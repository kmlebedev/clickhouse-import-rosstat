package minfin

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// Сведения о формировании и использовании дополнительных нефтегазовых доходов
// федерального бюджета: https://minfin.gov.ru/ru/statistics/fedbud/
// Файл Svedeniya_<YYYYMMDD>.xlsx: лист «Информационное сообщение», шапка с якорем
// «Показатели» в первом столбце. Имя файла содержит дату публикации, а каталог
// library/<год>/<месяц>/main/ меняется от выпуска к выпуску, поэтому URL не строится
// по дате — ссылка берётся со страницы каталога (стандарт §4.5).
const (
	oilGasPageUrl      = "https://minfin.gov.ru/ru/statistics/fedbud/"
	oilGasLinkSelector = `a.file_item[href*="Svedeniya_"]`
	oilGasTable        = "minfin_oil_gas_mes"
	oilGasSheet        = "Информационное сообщение"
	oilGasHeaderAnchor = "Показатели"
	// oilGasCountPoints — количественный чек версии (стандарт §9.2): FINAL обязателен,
	// иначе count() считал бы несмёрженные части ReplacingMergeTree.
	oilGasCountPoints = "SELECT count() FROM minfin_oil_gas_mes FINAL"

	// Имена рядов объявлены кодом (стандарт §6.8): подпись строки источника — ключ
	// карты, а имя ряда не зависит от редакции и года файла.
	oilGasSeriesTotal = "Нефтегазовые доходы, всего"
	oilGasSeriesNdpi  = "НДПИ"
)

// oilGasSeriesByRowLabel — подпись строки источника → стабильное имя ряда.
// Подстроки НДПИ («Нефть», «Газ», «Газовый конденсат») не импортируются: точное
// совпадение после нормализации пробелов отбирает только агрегаты верхнего уровня.
var oilGasSeriesByRowLabel = map[string]string{
	"Нефтегазовые доходы, всего":          oilGasSeriesTotal,
	"Налог на добычу полезных ископаемых": oilGasSeriesNdpi,
}

// normalizeOilGasLabel убирает внешние пробелы и схлопывает внутренние: подписи
// источника могут различаться неразрывными пробелами и переносами строки, а карта
// обязана находить строку независимо от этого.
func normalizeOilGasLabel(label string) string {
	return strings.Join(strings.Fields(label), " ")
}

// oilGasNumberReplacer удаляет пробелы, включая неразрывные: raw-значение книги
// приходит с точкой-разделителем, а запятая в локали может быть десятичной —
// поэтому она не удаляется, иначе «661,3» превратилось бы в 6613.
var oilGasNumberReplacer = strings.NewReplacer(" ", "", "\u00a0", "")

// oilGasImport разбирает лист «Информационное сообщение»: строки — показатели,
// колонки — месяцы (raw-сериалы Excel) и годы (целые 2018…2025). Значения — уже
// помесячные потоки, а не накопление с начала года, поэтому разности не считаются.
// Годовые колонки пропускаются: ряд хранит только месячные точки. Пустая ячейка и
// литеральный «-» — не точка (стандарт §6.7). Fail-fast: нет листа, нет шапки, нет
// ожидаемой строки или ноль точек — ошибка с именем таблицы и причиной.
func oilGasImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows(oilGasSheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return fmt.Errorf("%s: лист «%s»: %w", oilGasTable, oilGasSheet, err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("%s: лист «%s» пуст", oilGasTable, oilGasSheet)
	}

	headerRow := -1
	for i, row := range rows {
		if len(row) == 0 {
			continue
		}
		if normalizeOilGasLabel(row[0]) == oilGasHeaderAnchor {
			headerRow = i
			break
		}
	}
	if headerRow < 0 {
		return fmt.Errorf("%s: лист «%s»: не найдена шапка с якорем «%s» в первом столбце",
			oilGasTable, oilGasSheet, oilGasHeaderAnchor)
	}

	// Индексы колонок выводятся из шапки, а не пишутся числом: сериал Excel ≥ 20000 —
	// месяц, целое 1900…2100 — годовая колонка. Первая колонка занята подписями,
	// нечисловые ячейки шапки пропускаются.
	columns := make(map[int]time.Time)
	for j, cell := range rows[headerRow] {
		if j == 0 {
			continue
		}
		raw := strings.TrimSpace(cell)
		if raw == "" {
			continue
		}
		serial, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			continue
		}
		switch {
		case serial >= 20000:
			date, err := excelize.ExcelDateToTime(serial, false)
			if err != nil {
				return fmt.Errorf("%s: лист «%s», колонка %d: сериал даты %q: %w",
					oilGasTable, oilGasSheet, j+1, raw, err)
			}
			// Дата нормализуется к 1-му числу месяца: точка ряда — отчётный месяц.
			columns[j] = time.Date(date.Year(), date.Month(), 1, 0, 0, 0, 0, time.UTC)
		case serial == math.Trunc(serial) && serial >= 1900 && serial <= 2100:
			// Годовая колонка (накопленный итог за год) не импортируется: ряд хранит
			// только месячные потоки, иначе годовая сумма стала бы точкой января.
		}
	}
	if len(columns) == 0 {
		return fmt.Errorf("%s: лист «%s»: в шапке нет ни одной месячной колонки", oilGasTable, oilGasSheet)
	}

	found := make(map[string]bool, len(oilGasSeriesByRowLabel))
	points := 0
	for _, row := range rows[headerRow+1:] {
		if len(row) == 0 {
			continue
		}
		label := normalizeOilGasLabel(row[0])
		if label == "" {
			continue
		}
		series, ok := oilGasSeriesByRowLabel[label]
		if !ok {
			// Строки-составляющие НДПИ («Нефть», «Газ», «Газовый конденсат») и
			// прочие показатели листа не импортируются: карта хранит только
			// агрегаты верхнего уровня с точной подписью.
			continue
		}
		found[series] = true
		for column, date := range columns {
			if column >= len(row) {
				continue
			}
			raw := strings.TrimSpace(row[column])
			// Пустая ячейка (в выпуске 05.10.2026 так закрыт ещё не отчётный
			// 2026-10) и литеральный «-» — не точка (стандарт §6.7).
			if raw == "" || raw == "-" {
				continue
			}
			value, err := strconv.ParseFloat(oilGasNumberReplacer.Replace(raw), 64)
			if err != nil {
				return fmt.Errorf("%s: лист «%s», строка «%s», колонка %d: значение %q не число: %w",
					oilGasTable, oilGasSheet, label, column+1, raw, err)
			}
			if err = batch.Append(series, date, value); err != nil {
				return fmt.Errorf("%s: лист «%s», строка «%s», колонка %d: запись точки: %w",
					oilGasTable, oilGasSheet, label, column+1, err)
			}
			points++
		}
	}

	// Fail-fast: смена подписи строки источника — ошибка с именем таблицы, листа и
	// подписи, а не молчаливое «0 rows» (стандарт §6.4).
	for label, series := range oilGasSeriesByRowLabel {
		if !found[series] {
			return fmt.Errorf("%s: лист «%s»: не найдена ожидаемая строка «%s»", oilGasTable, oilGasSheet, label)
		}
	}
	if points == 0 {
		return fmt.Errorf("%s: лист «%s»: разбор не дал ни одной точки", oilGasTable, oilGasSheet)
	}
	return nil
}

// oilGasMes — нефтегазовые доходы и НДПИ: таблица, DDL и разбор
// Svedeniya_<YYYYMMDD>.xlsx. Значения — Float64 (правило 5 AGENTS.md). Каталог рядов
// и витрину v_minfin_budget публикует шаблон через AfterImport после точек и до
// строки версии.
var oilGasMes = util.ClickHouseImport{
	TableName: oilGasTable,
	CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (
              name LowCardinality(String)
			, date Date
			, value Float64
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);`},
	ImportFunc: oilGasImport,
	AfterImport: func(ctx context.Context, conn driver.Conn) error {
		return publishFedbud(ctx, conn, oilGasSeriesMeta)
	},
	PublicationCountPoints: oilGasCountPoints,
}

type oilGasMesStat struct {
	util.ClickHouseImport
}

// Import берёт ссылку на актуальный Svedeniya_<YYYYMMDD>.xlsx со страницы каталога
// федерального бюджета: URL не строится по дате (имя содержит дату публикации, а
// каталог library/<год>/<месяц>/main/ меняется от выпуска к выпуску).
func (s *oilGasMesStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	page, err := util.GetPage(oilGasPageUrl)
	if err != nil {
		return count, fmt.Errorf("%s: страница %s: %w", oilGasTable, oilGasPageUrl, err)
	}
	href, err := util.FindLink(page, oilGasLinkSelector)
	if err != nil {
		return count, fmt.Errorf("%s: не найдена ссылка на Svedeniya_<YYYYMMDD>.xlsx на %s: %w",
			oilGasTable, oilGasPageUrl, err)
	}
	s.DataUrl = minfinUrl + href
	return s.ClickHouseImport.Import(ctx, conn)
}

// oilGasMesImporter — нефтегазовые доходы и НДПИ: адрес файла берётся со страницы
// каталога в Import() (стандарт §5.4), регистрация — реестром minfin/importers.go.
var oilGasMesImporter = &oilGasMesStat{ClickHouseImport: oilGasMes}
