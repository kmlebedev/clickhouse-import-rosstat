package bank

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Общий разбор CSV-выгрузок раздела «Потребители» СберИндекса: месячный
// consumper-spending-index-sa и недельный ver-izmenenie-trat-po-kategoriyam отдают один
// формат `date;category;value` (UTF-8 с BOM, разделитель `;`, LF), поэтому разбор — один на
// оба импортёра. Функции ниже не ходят в сеть (стандарт §6.1): скачивание остаётся в
// CrawFunc импортёра, а разбор проверяется офлайн на фикстурах bank/testdata.

// sberPoint — одна точка выгрузки раздела «Потребители» СберИндекса.
type sberPoint struct {
	Series string
	Date   time.Time
	Value  float64
}

// sberCSVTimeLayout — формат даты обеих выгрузок: у месячной это первое число месяца, у
// недельной — воскресенье ISO-недели.
const sberCSVTimeLayout = "2006-01-02"

// sberCSVHeader — шапка выгрузки. Сверяется целиком, а не по числу колонок: смена шапки
// источником (другая раскладка, HTML-страница вместо CSV) должна валить прогон ошибкой,
// а не сдвигать значения в чужие колонки.
var sberCSVHeader = []string{"date", "category", "value"}

// parseSberRows — чистый разбор выгрузки (сеть — в CrawFunc импортёра): шапка сверяется
// целиком (BOM снимается), счёт — с контекстом строки, пустой результат — ошибка.
func parseSberRows(records [][]string) ([]sberPoint, error) {
	if len(records) == 0 {
		return nil, errors.New("пустая выгрузка: нет ни одной строки")
	}
	// Файл источника начинается с BOM (ef bb bf), и без его снятия шапка не совпала бы с
	// ожидаемой — то есть живой файл падал бы на сверке шапки.
	header := records[0]
	if len(header) == len(sberCSVHeader) {
		header = slices.Clone(header)
		header[0] = strings.TrimPrefix(header[0], "\ufeff")
	}
	if !slices.Equal(header, sberCSVHeader) {
		// Мёртвый эндпоинт отвечал HTTP 200 с HTML-оболочкой SPA: текст ошибки должен
		// называть эту причину, а не выглядеть «сбоем разбора даты».
		return nil, fmt.Errorf("неожиданная шапка %q, ожидалась %q: вероятно, источник отдал HTML-заглушку, а не CSV",
			records[0], sberCSVHeader)
	}

	points := make([]sberPoint, 0, len(records)-1)
	for i, row := range records[1:] {
		// Номер записи выгрузки (шапка — первая), а не номер строки файла: csv.Reader
		// пропускает пустые строки, поэтому строка файла могла бы не совпасть с индексом.
		record := i + 2
		if len(row) != len(sberCSVHeader) {
			return nil, fmt.Errorf("запись %d: полей %d, ожидалось %d: %q",
				record, len(row), len(sberCSVHeader), row)
		}
		// «Все категории » источника — та же категория, что «Все категории»: без TrimSpace
		// опечатка источника стала бы отдельным рядом, а её будущее исправление «оборвало» бы
		// накопленную историю.
		series := strings.TrimSpace(row[1])
		if series == "" {
			return nil, fmt.Errorf("запись %d: пустое имя категории: %q", record, row)
		}
		date, err := time.Parse(sberCSVTimeLayout, row[0])
		if err != nil {
			return nil, fmt.Errorf("запись %d: дата %q: %w", record, row[0], err)
		}
		value, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			return nil, fmt.Errorf("запись %d: значение %q не число: %w", record, row[2], err)
		}
		// ParseFloat принимает литералы NaN/Inf/infinity без ошибки: это тоже порча выгрузки,
		// но её не видно ни в разборе, ни в логе, пока NaN не доедет до ClickHouse и не начнёт
		// портить агрегаты витрины.
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("запись %d: значение %q не конечно (NaN/Inf): %q", record, row[2], row)
		}
		points = append(points, sberPoint{Series: series, Date: date, Value: value})
	}
	if len(points) == 0 {
		// Шапка верна, но данных нет — для шаблона это молчаливый «0 строк», то есть ровно
		// тот дефект, ради которого заведён пункт.
		return nil, fmt.Errorf("пустая выгрузка: строк %d, точек нет", len(records))
	}
	return points, nil
}

// sberUndescribed — имена точек, отсутствующие в каталоге: вызывающий журналирует warn и
// импортирует точку (стандарт §6.6 — новый товар источника не валит цепочку). Имена
// возвращаются в порядке первого появления и без дублей: одну и ту же новую категорию
// недельный файл отдаёт многими неделями, и предупреждений должно быть по одному на имя,
// а не на точку.
func sberUndescribed(points []sberPoint, meta []util.SeriesMeta) []string {
	described := make(map[string]struct{}, len(meta))
	for _, m := range meta {
		described[m.Series] = struct{}{}
	}
	var (
		names []string
		seen  = make(map[string]struct{})
	)
	for _, p := range points {
		if _, ok := described[p.Series]; ok {
			continue
		}
		if _, ok := seen[p.Series]; ok {
			continue
		}
		seen[p.Series] = struct{}{}
		names = append(names, p.Series)
	}
	return names
}
