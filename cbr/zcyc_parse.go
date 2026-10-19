package cbr

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

const (
	// cbrZcycTableSelector — таблица «Кривая бескупонной доходности
	// государственных облигаций» на странице cbr.ru/hd_base/zcyc_params.
	cbrZcycTableSelector = ".table-wrapper table.data"

	// cbrZcycChunkYears — максимальная длина окна запроса к UniDbQuery: ЦБ
	// ограничивает диапазон, поэтому бэкфилл с 2003 года режется на смежные окна.
	cbrZcycChunkYears = 4

	// cbrZcycTimeLayout — формат даты в строках таблицы (16.10.2026).
	cbrZcycTimeLayout = "02.01.2006"
)

// cbrZcycTenors — канонические теноры в порядке источника.
var cbrZcycTenors = []string{"0.25", "0.5", "0.75", "1", "2", "3", "5", "7", "10", "15", "20", "30"}

// zcycPoint — одна точка кривой бескупонной доходности: тенор в канонической
// записи (0.25, 1, 10), торговый день публикации и доходность, % годовых.
type zcycPoint struct {
	tenor string
	date  time.Time
	value float64
}

// zcycRows разбирает HTML-таблицу КБД ЦБ в точки. Вторая (последняя) строка
// заголовка — 12 теноров; известные теноры привязываются к колонкам, неизвестные
// пропускаются с предупреждением, а их отсутствие целиком — ошибка: смена
// разметки не должна выглядеть пустым рядом. Пустая выборка ЦБ (страницы-формы
// UniDbQuery без таблицы) даёт nil, nil; заглушка WAF/сломанная страница — ошибку.
// Пустые ячейки значений — пропуск (не ноль, стандарт §6.7); непустые
// непарсящиеся дата или значение — ошибка. Порядок точек — как в источнике
// (строки от свежей к старой, теноры слева направо), сортировка не выполняется.
func zcycRows(body []byte) ([]zcycPoint, error) {
	table, err := util.ParseHTMLTable(body, cbrZcycTableSelector)
	if err != nil {
		if errors.Is(err, util.ErrHTMLTableNotFound) {
			if util.IsUniDbQueryPage(body) {
				return nil, nil
			}
			return nil, fmt.Errorf("cbr_zcyc: таблица %q не найдена, страница не похожа на форму UniDbQuery (WAF-заглушка или сломанная страница): %w", cbrZcycTableSelector, err)
		}
		return nil, fmt.Errorf("cbr_zcyc: %w", err)
	}

	if len(table.Headers) == 0 {
		return nil, fmt.Errorf("cbr_zcyc: у таблицы %q нет строки заголовка — разметка изменилась", cbrZcycTableSelector)
	}

	// Заголовок теноров — последняя строка <th>: первая несёт rowspan/colspan
	// («Дата», «Срок до погашения, лет») и для привязки колонок не годится.
	type tenorColumn struct {
		col   int
		tenor string
	}
	header := table.Headers[len(table.Headers)-1]
	columns := make([]tenorColumn, 0, len(header))
	// seen — нормализованный тенор → исходный заголовок первой колонки: две
	// колонки одного тенора (1 и 1,00) — ошибка разметки, а не дубли точек.
	seen := make(map[string]string, len(header))
	for col, cell := range header {
		label := strings.TrimSpace(cell)
		number, err := strconv.ParseFloat(strings.ReplaceAll(label, ",", "."), 64)
		if err != nil {
			// Нечисловая ячейка в строке теноров — не тенор (например, «Дата»).
			continue
		}
		// FormatFloat нормализует запись: 0,50 → 0.5, 1,00 → 1.
		tenor := strconv.FormatFloat(number, 'f', -1, 64)
		if !slices.Contains(cbrZcycTenors, tenor) {
			log.Warnf("cbr_zcyc: колонка заголовка %q (нормализованный тенор %q) не входит в канонический список — колонка пропущена", label, tenor)
			continue
		}
		if first, ok := seen[tenor]; ok {
			return nil, fmt.Errorf("cbr_zcyc: тенор %q встречается в заголовке дважды (колонки %q и %q) — разметка изменилась", tenor, first, label)
		}
		seen[tenor] = label
		columns = append(columns, tenorColumn{col: col, tenor: tenor})
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("cbr_zcyc: в заголовке таблицы нет ни одного канонического тенора (%s) — разметка изменилась", strings.Join(cbrZcycTenors, ", "))
	}

	points := make([]zcycPoint, 0, len(table.Rows)*len(columns))
	for _, row := range table.Rows {
		if len(row) == 0 {
			continue
		}
		rawDate := strings.TrimSpace(row[0])
		if rawDate == "" {
			continue
		}
		date, err := time.Parse(cbrZcycTimeLayout, rawDate)
		if err != nil {
			return nil, fmt.Errorf("cbr_zcyc: дата %q: %w", rawDate, err)
		}
		for _, column := range columns {
			idx := column.col + 1
			if idx >= len(row) {
				continue
			}
			rawValue := strings.TrimSpace(row[idx])
			if rawValue == "" {
				// Пустая ячейка — отсутствие значения, а не ноль.
				continue
			}
			value, err := strconv.ParseFloat(strings.ReplaceAll(rawValue, ",", "."), 64)
			if err != nil {
				return nil, fmt.Errorf("cbr_zcyc: значение %q (дата %s, тенор %s): %w", rawValue, rawDate, column.tenor, err)
			}
			// ParseFloat принимает NaN/Inf без ошибки, но доходность ими быть не может:
			// это битая страница, и она не должна попасть в ряд (как любое нечисло).
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, fmt.Errorf("cbr_zcyc: значение %q (дата %s, тенор %s) не является конечным числом (NaN/Inf) — разметка или страница битая", rawValue, rawDate, column.tenor)
			}
			points = append(points, zcycPoint{tenor: column.tenor, date: date, value: value})
		}
	}

	if len(table.Rows) > 0 && len(points) == 0 {
		return nil, fmt.Errorf("cbr_zcyc: в таблице %d строк, но ни одной точки — разметка изменилась", len(table.Rows))
	}
	return points, nil
}

// zcycPointsInRange оставляет точки, попавшие в запрошенное окно [from, to] (обе
// границы включительно), сохраняя порядок источника. Фильтр обязателен, потому что
// ЦБ игнорирует диапазон, когда он пуст или лежит в будущем, и отдаёт таблицу с
// последней доступной строкой: без фильтра такая строка попала бы в чужое окно.
// Дефект был незаметен из-за ReplacingMergeTree — повторная вставка той же точки не
// меняет count(), хотя импортёр каждый раз писал батч.
func zcycPointsInRange(points []zcycPoint, from, to time.Time) []zcycPoint {
	if from.After(to) {
		return nil
	}
	var kept []zcycPoint
	for _, p := range points {
		if p.date.Before(from) || p.date.After(to) {
			continue
		}
		kept = append(kept, p)
	}
	return kept
}

// zcycChunks режет [from, to] на последовательные непересекающиеся окна не
// длиннее cbrZcycChunkYears лет; последнее окно обрезано по to. Окна смежные:
// следующее начинается на день позже конца предыдущего.
func zcycChunks(from, to time.Time) [][2]time.Time {
	if from.After(to) {
		return nil
	}
	var chunks [][2]time.Time
	for start := from; !start.After(to); {
		end := start.AddDate(cbrZcycChunkYears, 0, -1)
		if end.After(to) {
			end = to
		}
		chunks = append(chunks, [2]time.Time{start, end})
		start = end.AddDate(0, 0, 1)
	}
	return chunks
}
