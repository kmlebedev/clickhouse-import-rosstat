// Package bold тянет ежедневные T+1 данные из публичного JSON API bold.report (ByteTree):
// агрегат 73 золотых ETF (≈83 % мировой величины WGC) и цена золота. Здесь — только чистый
// разбор ответов и конверсии; сеть и запись в ClickHouse живут в импортёрах (bold.go).
package bold

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Эндпоинты источника. Формат истории — {version, updated, data:[…]}, формат «последнего
// дня» — объект верхнего уровня {version, updated, date, …поля}.
//
// Суффикс `.json` в пути ОБЯЗАТЕЛЕН: источник — статика на S3 за CloudFront, объекта без
// расширения там просто нет, а CDN без права ListBucket маскирует отсутствующий ключ
// отказом в доступе (не 404, а 403 AccessDenied). Проверено живым curl 2026-10-12:
// `gold/price-latest` → 403 AccessDenied, `gold/price-latest.json` → 200. Офлайн-тесты
// (httptest) этого не видят — они отвечают на любой путь, поэтому суффикс пришпилен
// отдельной проверкой в TestBoldEndpointsPinPaths: путь без `.json` — ошибка теста.
const (
	boldBaseURL = "https://bold.report/api/v1/"

	boldPathPrice       = "gold/price.json"
	boldPathPriceLatest = "gold/price-latest.json"
	boldPathUnits       = "gold/funds/units.json"
	boldPathUnitsLatest = "gold/funds/units-latest.json"
	boldPathAum         = "gold/funds/aum.json"
	boldPathAumLatest   = "gold/funds/aum-latest.json"
	boldPathFlows       = "gold/flows/all.json"
	boldPathFlowsLatest = "gold/flows/all-latest.json"
	boldPathSummary     = "gold/summary.json"
)

// Имена семи рядов ETF-контура (source='bold' в macro_series).
const (
	boldSeriesFlowTonnes     = "etf_bold_flow_tonnes"
	boldSeriesFlowUsdMln     = "etf_bold_flow_usd_mln"
	boldSeriesFlow7dTonnes   = "etf_bold_flow_7d_tonnes"
	boldSeriesFlow30dTonnes  = "etf_bold_flow_30d_tonnes"
	boldSeriesFlow90dTonnes  = "etf_bold_flow_90d_tonnes"
	boldSeriesHoldingsTonnes = "etf_bold_holdings_tonnes"
	boldSeriesAumUsdBn       = "etf_bold_aum_usd_bn"
)

const (
	// boldDateLayout — даты источника («2026-10-09»); строки «последнего дня» и истории
	// уже конец дня, поэтому далее никакой нормализации дат нет (в ClickHouse — Date32).
	boldDateLayout = "2006-01-02"
	// troyOunceGrams — тройская унция в граммах: та же константа, что в gold/gold.go.
	troyOunceGrams = 31.1034768
	// ouncesPerTonne — унций в тонне: 1e6/31.1034768 = 32150.74656862…
	ouncesPerTonne = 1e6 / troyOunceGrams
	// aumUsdDivisor — AUM источника в USD, ряд etf_bold_aum_usd_bn — в млрд USD.
	aumUsdDivisor = 1e9
	// flowUsdDivisor — поток в USD, ряд etf_bold_flow_usd_mln — в млн USD.
	flowUsdDivisor = 1e6
)

// point — точка ряда: имя ряда, дата (конец дня, 00:00 UTC) и значение в единицах ряда.
// Объявлен локально: пакеты источников не зависят друг от друга (аналог wgc.seriesPoint).
type point struct {
	Series string
	Date   time.Time
	Value  float64
}

// boldRow — строка истории/«последнего дня»; отсутствующие поля источника остаются nil
// (у 64 первых строк истории есть только price — это отсутствие данных, а не ноль).
type boldRow struct {
	Date   string   `json:"date"`
	Value  *float64 `json:"value"`
	Units  *float64 `json:"units"`
	Aum    *float64 `json:"aum"`
	Culm7  *float64 `json:"culm7day"`
	Culm30 *float64 `json:"culm30day"`
	Culm90 *float64 `json:"culm90day"`
	Price  *float64 `json:"price"`
}

// boldEnvelope — обёртка файлов истории.
type boldEnvelope struct {
	Version string    `json:"version"`
	Updated string    `json:"updated"`
	Data    []boldRow `json:"data"`
}

// boldFundRow — строка пер-фондового среза (у среза нет поля даты: дата берётся из
// ответа «последнего дня», поэтому расхождение дат обязано быть ошибкой, а не записью).
type boldFundRow struct {
	Ticker       string   `json:"ticker"`
	Name         string   `json:"name"`
	Units        *float64 `json:"units"`
	UnitsChange  *float64 `json:"unitsChange"`
	Aum          *float64 `json:"aum"`
	AumChange    *float64 `json:"aumChange"`
	Price        *float64 `json:"price"`
	PriceChange  *float64 `json:"priceChange"`
	Shares       *float64 `json:"shares"`
	SharesChange *float64 `json:"sharesChange"`
}

// boldFundSnapshot — ответ gold/summary: срез 73 фондов за последний день.
type boldFundSnapshot struct {
	Version string        `json:"version"`
	Updated string        `json:"updated"`
	Data    []boldFundRow `json:"data"`
}

// boldFundPoint — точка пер-фондового среза для bold_fund_snapshot.
type boldFundPoint struct {
	Date         time.Time
	Ticker       string
	Name         string
	Units        float64
	UnitsChange  float64
	Aum          float64
	AumChange    float64
	Price        float64
	PriceChange  float64
	Shares       float64
	SharesChange float64
}

// boldMetric — метрика эндпоинта: какие поля строки обязаны быть заполнены.
type boldMetric int

const (
	boldMetricPrice boldMetric = iota
	boldMetricUnits
	boldMetricAum
	boldMetricFlows
)

// name — имя метрики в терминах источника (для сообщений об ошибках).
func (m boldMetric) name() string {
	switch m {
	case boldMetricPrice:
		return "price"
	case boldMetricUnits:
		return "units"
	case boldMetricAum:
		return "aum"
	case boldMetricFlows:
		return "flows"
	}
	return fmt.Sprintf("boldMetric(%d)", int(m))
}

// missing — первое обязательное поле метрики, которого нет в строке ("" — строка полна).
// У flows обязательны все три окна: источник публикует их вместе, и одиночный nil —
// сигнал смены формы ответа, а не отсутствие данных.
func (m boldMetric) missing(r boldRow) string {
	switch m {
	case boldMetricPrice:
		if r.Value == nil {
			return "value"
		}
	case boldMetricUnits:
		if r.Units == nil {
			return "units"
		}
	case boldMetricAum:
		if r.Aum == nil {
			return "aum"
		}
	case boldMetricFlows:
		switch {
		case r.Culm7 == nil:
			return "culm7day"
		case r.Culm30 == nil:
			return "culm30day"
		case r.Culm90 == nil:
			return "culm90day"
		}
	default:
		return "обязательные поля (метрика не распознана)"
	}
	return ""
}

// require проверяет, что строка несёт обязательные поля метрики; иначе — ошибка с именем
// метрики и даты (пустое поле не должно доехать до БД нулём).
func (m boldMetric) require(r boldRow) error {
	switch m {
	case boldMetricPrice, boldMetricUnits, boldMetricAum, boldMetricFlows:
	default:
		return fmt.Errorf("bold: неизвестная метрика boldMetric(%d)", int(m))
	}
	if field := m.missing(r); field != "" {
		return fmt.Errorf("bold: в ответе метрики %s за %s нет обязательного поля %s", m.name(), r.Date, field)
	}
	return nil
}

// present — то же, что require, но без ошибки: есть ли в строке поля метрики.
func (m boldMetric) present(r boldRow) bool { return m.missing(r) == "" }

// parseBoldDate разбирает дату источника («2026-10-09») в 00:00 UTC.
func parseBoldDate(s string) (time.Time, error) {
	d, err := time.ParseInLocation(boldDateLayout, s, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("bold: дата %q не разобрана (нужен формат %s): %w", s, boldDateLayout, err)
	}
	return d, nil
}

// parseLatest разбирает ответ «последнего дня»: это объект верхнего уровня, а не список,
// поэтому unmarshal идёт сразу в boldRow. Дата и обязательные поля метрики проверяются.
func parseLatest(body []byte, metric boldMetric) (boldRow, error) {
	var row boldRow
	if err := json.Unmarshal(body, &row); err != nil {
		return boldRow{}, fmt.Errorf("bold: разбор ответа метрики %s: %w", metric.name(), err)
	}
	if _, err := parseBoldDate(row.Date); err != nil {
		return boldRow{}, err
	}
	if err := metric.require(row); err != nil {
		return boldRow{}, err
	}
	return row, nil
}

// parseHistory разбирает файл истории. Строки без полей метрики остаются в срезе с nil —
// решение о точке принимает boldETFSets (нулевой сток не должен попасть в ряды как факт);
// ошибка тогда, когда метрики нет ни в одной строке (не тот эндпоинт), когда дата повторяется
// (дубли точек молча) и когда окна culm* заполнены частично (правило «все три или ни одного»).
func parseHistory(body []byte, metric boldMetric) ([]boldRow, error) {
	var envelope boldEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("bold: разбор истории метрики %s: %w", metric.name(), err)
	}
	if len(envelope.Data) == 0 {
		return nil, fmt.Errorf("bold: история метрики %s пуста", metric.name())
	}
	full := 0
	seen := make(map[string]int, len(envelope.Data))
	for i, row := range envelope.Data {
		if _, err := parseBoldDate(row.Date); err != nil {
			return nil, fmt.Errorf("bold: история метрики %s, строка %d: %w", metric.name(), i+1, err)
		}
		if prev, ok := seen[row.Date]; ok {
			return nil, fmt.Errorf("bold: история метрики %s: дата %s повторяется (строки %d и %d) — это дало бы дубли точек",
				metric.name(), row.Date, prev, i+1)
		}
		seen[row.Date] = i + 1
		if err := requireCulmWindows(row); err != nil {
			return nil, fmt.Errorf("bold: история метрики %s, строка %d: %w", metric.name(), i+1, err)
		}
		if metric.present(row) {
			full++
		}
	}
	if full == 0 {
		return nil, fmt.Errorf("bold: в истории метрики %s нет ни одной строки с её полями", metric.name())
	}
	return envelope.Data, nil
}

// parseFundSnapshot разбирает пер-фондовый срез. Пустой срез и строка с любым незаполненным
// полем — ошибка: срез обязан быть полным, отсутствие поля означает смену формы ответа.
func parseFundSnapshot(body []byte) ([]boldFundRow, error) {
	var snapshot boldFundSnapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return nil, fmt.Errorf("bold: разбор среза фондов (%s): %w", boldPathSummary, err)
	}
	if len(snapshot.Data) == 0 {
		return nil, fmt.Errorf("bold: срез фондов (%s) пуст", boldPathSummary)
	}
	for i, row := range snapshot.Data {
		if err := requireFundRow(row); err != nil {
			return nil, fmt.Errorf("bold: срез фондов, строка %d (%s): %w", i+1, row.Ticker, err)
		}
	}
	return snapshot.Data, nil
}

// requireFundRow проверяет полноту строки среза. Два уровня проверки: здесь (parseFundSnapshot)
// смена формы ответа ловится раньше и с номером строки, а boldFundPoints вызывает ту же функцию
// сам — прямой вызов без разбора тоже не должен молча получить нули.
func requireFundRow(f boldFundRow) error {
	if f.Ticker == "" {
		return errors.New("пустой ticker")
	}
	if f.Name == "" {
		return errors.New("пустое name")
	}
	fields := []struct {
		name  string
		value *float64
	}{
		{"units", f.Units},
		{"unitsChange", f.UnitsChange},
		{"aum", f.Aum},
		{"aumChange", f.AumChange},
		{"price", f.Price},
		{"priceChange", f.PriceChange},
		{"shares", f.Shares},
		{"sharesChange", f.SharesChange},
	}
	for _, field := range fields {
		if field.value == nil {
			return fmt.Errorf("нет обязательного поля %s", field.name)
		}
	}
	return nil
}

// requireCulmWindows проверяет правило источника «все три окна или ни одного»: строки истории
// до 2015-04-01 не несут ни одного culm*, с 2015-04-01 — все три (проверено на живом файле
// flows/all, 3138 строк). Частично заполненная строка — смена формы ответа, и без ошибки ряд
// окон молча получил бы дыру: одну точку с частью семантики.
func requireCulmWindows(r boldRow) error {
	windows := []struct {
		name  string
		value *float64
	}{
		{"culm7day", r.Culm7},
		{"culm30day", r.Culm30},
		{"culm90day", r.Culm90},
	}
	present := make([]string, 0, len(windows))
	absent := make([]string, 0, len(windows))
	for _, w := range windows {
		if w.value != nil {
			present = append(present, w.name)
		} else {
			absent = append(absent, w.name)
		}
	}
	if len(present) == 0 || len(present) == len(windows) {
		return nil
	}
	return fmt.Errorf("окна заполнены частично: есть %s; нет %s — в источнике либо все три, либо ни одного",
		strings.Join(present, ", "), strings.Join(absent, ", "))
}

// ozToTonnes переводит тройские унции в тонны (1 т = 1e6/31.1034768 = 32150.74656862… унции).
func ozToTonnes(oz float64) float64 { return oz / ouncesPerTonne }

// boldETFSets строит точки семи рядов ETF-контура по строкам истории (слитым по дате) или по
// одной строке инкремента. prevUnits/prevDate — сток предыдущего торгового дня, прочитанный
// из ClickHouse (на полном пути не нужен). Строки сортируются по дате: порядок точек не должен
// зависеть от порядка входа. Пустой результат — ошибка «нет точек», а не тихий ноль.
func boldETFSets(rows []boldRow, prevUnits *float64, prevDate time.Time) ([]point, error) {
	type datedRow struct {
		date time.Time
		row  boldRow
	}
	dated := make([]datedRow, 0, len(rows))
	for i, row := range rows {
		d, err := parseBoldDate(row.Date)
		if err != nil {
			return nil, fmt.Errorf("bold: строка %d: %w", i+1, err)
		}
		dated = append(dated, datedRow{date: d, row: row})
	}
	sort.SliceStable(dated, func(i, j int) bool { return dated[i].date.Before(dated[j].date) })

	var points []point
	// base/baseDate — сток и его дата для дельты: внутри среза берётся из предыдущей строки,
	// для первой строки — из prevUnits/prevDate (ClickHouse).
	var base *float64
	var baseDate time.Time
	for i, item := range dated {
		row, date := item.row, item.date
		if i > 0 && dated[i-1].date.Equal(date) {
			return nil, fmt.Errorf("bold: дата %s повторяется в срезе — это дало бы дубли точек", date.Format(boldDateLayout))
		}
		if err := requireCulmWindows(row); err != nil {
			return nil, fmt.Errorf("bold: строка %s: %w", date.Format(boldDateLayout), err)
		}
		if row.Units != nil {
			points = append(points, point{boldSeriesHoldingsTonnes, date, ozToTonnes(*row.Units)})
		}
		if row.Aum != nil {
			points = append(points, point{boldSeriesAumUsdBn, date, *row.Aum / aumUsdDivisor})
		}
		if row.Culm7 != nil {
			points = append(points, point{boldSeriesFlow7dTonnes, date, ozToTonnes(*row.Culm7)})
		}
		if row.Culm30 != nil {
			points = append(points, point{boldSeriesFlow30dTonnes, date, ozToTonnes(*row.Culm30)})
		}
		if row.Culm90 != nil {
			points = append(points, point{boldSeriesFlow90dTonnes, date, ozToTonnes(*row.Culm90)})
		}

		if i == 0 {
			base, baseDate = prevUnits, prevDate
		}
		if base != nil && row.Units != nil {
			if !baseDate.Before(date) {
				return nil, fmt.Errorf("bold: предыдущий сток за %s не старше строки %s — дельта потока посчиталась бы неверно",
					baseDate.Format(boldDateLayout), date.Format(boldDateLayout))
			}
			delta := *row.Units - *base
			points = append(points, point{boldSeriesFlowTonnes, date, ozToTonnes(delta)})
			if row.Price != nil {
				points = append(points, point{boldSeriesFlowUsdMln, date, delta * *row.Price / flowUsdDivisor})
			}
		}
		// Предыдущий сток строки: nil значит «в этой строке запасов нет», и потока для
		// следующей строки тоже нет (нет предыдущего — нет дельты).
		base, baseDate = row.Units, date
	}

	if len(points) == 0 {
		return nil, errors.New("bold: нет точек: ни в одной строке нет units, aum или окон culm*")
	}
	return points, nil
}

// boldFundPoints переводит пер-фондовый срез в точки за date (у среза своего поля даты нет —
// дата приходит из ответа «последнего дня»). nil в любом числовом поле — ошибка с тикером,
// именем и полем: срез обязан быть полным, а ноль вместо отсутствия данных — та самая
// молчаливая подмена, которую пункт исключает. parseFundSnapshot проверяет то же раньше
// (и с номером строки), здесь функция защищается при прямом вызове.
func boldFundPoints(snapshot []boldFundRow, date time.Time) ([]boldFundPoint, error) {
	points := make([]boldFundPoint, 0, len(snapshot))
	for _, fund := range snapshot {
		if err := requireFundRow(fund); err != nil {
			return nil, fmt.Errorf("bold: фонд %s (%s): %w", fund.Ticker, fund.Name, err)
		}
		points = append(points, boldFundPoint{
			Date:         date,
			Ticker:       fund.Ticker,
			Name:         fund.Name,
			Units:        *fund.Units,
			UnitsChange:  *fund.UnitsChange,
			Aum:          *fund.Aum,
			AumChange:    *fund.AumChange,
			Price:        *fund.Price,
			PriceChange:  *fund.PriceChange,
			Shares:       *fund.Shares,
			SharesChange: *fund.SharesChange,
		})
	}
	return points, nil
}
