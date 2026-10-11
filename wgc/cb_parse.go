package wgc

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

// Раскладка выгрузок ЦБ (Changes_latest_as_of_*.xlsx и World_official_gold_holdings_as_of_*.xlsx),
// метрики и сдвиги блока топ-100.
const (
	// cbChangesSheet — лист выгрузки Changes с месячными изменениями резервов в тоннах.
	cbChangesSheet = "Monthly"
	// cbHoldingsSheet — единственный лист выгрузки holdings: таблица топ-100.
	cbHoldingsSheet = "PDF"

	// cbChangesHeaderRow — строка-заголовок листа Monthly: serial (начало месяца) на каждую
	// колонку-месяц.
	cbChangesHeaderRow = 8
	// cbChangesFirstDataCol — колонка D: в A полное имя, в B короткое (по нему словарь стран),
	// в C комментарий (не импортируем).
	cbChangesFirstDataCol = 4
	// cbChangesNameCol — колонка B: короткое имя страны для словаря.
	cbChangesNameCol = 2
	// cbChangesFirstDataRow — данные со строки 9: строки 1..7 служебные.
	cbChangesFirstDataRow = 9

	// cbHoldingsTonnesHeader — заголовок блока таблицы топ-100: по нему находятся блоки.
	cbHoldingsTonnesHeader = "Tonnes"
	// cbHoldingsReservesHeaderPrefix — начало подписи колонки справа от Tonnes («% of reserves**»
	// в выгрузке; номер сноски меняется, префикс — нет).
	cbHoldingsReservesHeaderPrefix = "% of reserves"
	// cbHoldingsAsofHeader — подпись колонки даты уровня (Tonnes+2; в файле — с хвостовым пробелом).
	cbHoldingsAsofHeader = "Holdings as of"

	// cbMetricChangeTonnes — метрика месячных изменений резервов (знак — направление).
	cbMetricChangeTonnes = "gold_change_tonnes"
	// cbMetricHoldingsTonnes — метрика уровня резервов (сток).
	cbMetricHoldingsTonnes = "gold_holdings_tonnes"
)

// CbCountry — страна набора ЦБ: ISO-код, русское имя витрины и короткое имя в выгрузках WGC
// (колонка B листа Monthly и колонка имени листа PDF).
type CbCountry struct {
	ISO     string
	Country string
	Name    string
}

// cbCountries — страны набора в фиксированном порядке POL, CZE, TUR, RUS, KAZ, UZB, CHN
// (Task 5 читает витрину в нём же). Имена — короткие имена файлов: «Turkey» — net-ряд, рядом в
// выгрузке есть «Turkey*» (gross, включая банки) — её брать нельзя.
var cbCountries = []CbCountry{
	{ISO: "POL", Country: "Польша", Name: "Poland, Rep. of"},
	{ISO: "CZE", Country: "Чехия", Name: "Czech Rep."},
	{ISO: "TUR", Country: "Турция", Name: "Turkey"},
	{ISO: "RUS", Country: "Россия", Name: "Russian Federation"},
	{ISO: "KAZ", Country: "Казахстан", Name: "Kazakhstan, Rep. of"},
	{ISO: "UZB", Country: "Узбекистан", Name: "Uzbekistan, Rep. of"},
	{ISO: "CHN", Country: "Китай", Name: "China, P.R.: Mainland"},
}

// cbCountriesByName — словарь стран по короткому имени файла (после чистки сносок holdings).
var cbCountriesByName = func() map[string]CbCountry {
	byName := make(map[string]CbCountry, len(cbCountries))
	for _, c := range cbCountries {
		byName[c.Name] = c
	}
	return byName
}()

// cbMetrics — метрики набора в порядке имён рядов внутри страны.
var cbMetrics = []string{cbMetricChangeTonnes, cbMetricHoldingsTonnes}

// CbSeriesNames — 14 кодов рядов набора (2 метрики × 7 стран) для витрины v_cb_gold_dynamics
// (Task 5): страны — в порядке словаря, внутри страны сначала дельты, затем уровни.
func CbSeriesNames() []string {
	names := make([]string, 0, len(cbCountries)*len(cbMetrics))
	for _, c := range cbCountries {
		for _, metric := range cbMetrics {
			names = append(names, wgcCbSeriesName(metric, c.ISO))
		}
	}
	return names
}

// CbCountries — словарь стран набора в фиксированном порядке (POL, CZE, TUR, RUS, KAZ, UZB,
// CHN). Возвращается копия: витрина не должна менять словарь парсера.
func CbCountries() []CbCountry {
	return append([]CbCountry(nil), cbCountries...)
}

// wgcCbSeriesName — имя ряда ЦБ по метрике и ISO-коду страны.
func wgcCbSeriesName(metric, iso string) string {
	return "cb_" + metric + "_" + iso
}

// cbFootnoteSuffix — сноска в имени страны листа PDF («Turkey5)», «Belarus, Rep. of4)»).
var cbFootnoteSuffix = regexp.MustCompile(`\d+\)$`)

// cbChangesMonth — колонка листа Monthly с датой месяца (конец месяца, UTC).
type cbChangesMonth struct {
	col  int
	date time.Time
}

// parseCbChanges разбирает лист Monthly выгрузки Changes: по каждой стране словаря — месячные
// изменения резервов в тоннах с 2002-01. Пустая ячейка — нет данных (пропуск), «0» — значение.
func parseCbChanges(f *excelize.File) ([]seriesPoint, error) {
	maxCol, maxRow, err := sheetSize(f, cbChangesSheet)
	if err != nil {
		return nil, fmt.Errorf("лист %s: размеры: %w", cbChangesSheet, err)
	}
	if maxCol < cbChangesFirstDataCol || maxRow < cbChangesFirstDataRow {
		return nil, fmt.Errorf("лист %s: нет колонок-месяцев (с %d) или строк данных (с %d): раскладка выгрузки изменилась",
			cbChangesSheet, cbChangesFirstDataCol, cbChangesFirstDataRow)
	}

	months, err := cbChangesMonths(f, maxCol, maxRow)
	if err != nil {
		return nil, err
	}
	rows, err := cbChangesCountryRows(f, maxRow)
	if err != nil {
		return nil, err
	}

	points := make([]seriesPoint, 0, len(cbCountries)*len(months))
	for _, c := range cbCountries {
		row := rows[c.ISO]
		for _, month := range months {
			ref := cbCellRef(month.col, row)
			raw, err := cellRaw(f, cbChangesSheet, month.col, row)
			if err != nil {
				return nil, fmt.Errorf("лист %s: %s: %w", cbChangesSheet, ref, err)
			}
			value, ok, err := cellFloat(raw)
			if err != nil {
				return nil, fmt.Errorf("лист %s: %s: %w", cbChangesSheet, ref, err)
			}
			if !ok {
				continue // пустая ячейка — за месяц данных нет; это не ноль
			}
			points = append(points, seriesPoint{
				Series: wgcCbSeriesName(cbMetricChangeTonnes, c.ISO),
				Date:   month.date,
				Value:  value,
			})
		}
	}
	return points, nil
}

// cbChangesMonths читает строку-заголовок 8: колонки D и далее — месяцы (serial). Непустой
// не-числовой заголовок — ошибка; пустой заголовок допустим только пока колонка пуста, иначе
// её значения остались бы без даты (fail-fast, как в ETF-парсере).
func cbChangesMonths(f *excelize.File, maxCol, maxRow int) ([]cbChangesMonth, error) {
	months := make([]cbChangesMonth, 0, maxCol-cbChangesFirstDataCol+1)
	for col := cbChangesFirstDataCol; col <= maxCol; col++ {
		ref := cbCellRef(col, cbChangesHeaderRow)
		raw, err := cellRaw(f, cbChangesSheet, col, cbChangesHeaderRow)
		if err != nil {
			return nil, fmt.Errorf("лист %s: заголовок-дата %s: %w", cbChangesSheet, ref, err)
		}
		if raw == "" {
			if err := cbColumnStaysEmpty(f, col, maxRow); err != nil {
				return nil, err
			}
			continue
		}
		date, err := cellDate(raw)
		if err != nil {
			return nil, fmt.Errorf("лист %s: заголовок-дата %s = %q: %w", cbChangesSheet, ref, raw, err)
		}
		months = append(months, cbChangesMonth{col: col, date: monthEnd(date)})
	}
	if len(months) == 0 {
		return nil, fmt.Errorf("лист %s: в строке %d нет ни одной колонки-месяца: раскладка выгрузки изменилась",
			cbChangesSheet, cbChangesHeaderRow)
	}
	return months, nil
}

// cbColumnStaysEmpty — колонка без заголовка-даты допустима только пока в ней нет данных:
// значение осталось бы без даты и потерялось бы молча.
func cbColumnStaysEmpty(f *excelize.File, col, maxRow int) error {
	for row := cbChangesFirstDataRow; row <= maxRow; row++ {
		raw, err := cellRaw(f, cbChangesSheet, col, row)
		if err != nil {
			return fmt.Errorf("лист %s: %s: %w", cbChangesSheet, cbCellRef(col, row), err)
		}
		if raw != "" {
			return fmt.Errorf("лист %s: %s = %q, но заголовок-дата колонки %d пуст: месяц не к чему привязать",
				cbChangesSheet, cbCellRef(col, row), raw, col)
		}
	}
	return nil
}

// cbChangesCountryRows — строка каждой страны словаря на листе Monthly. Поиск по колонке B —
// строгое равенство, поэтому Turkey (net) не путается с Turkey* (gross). Ноль совпадений
// (страна пропала из выгрузки) или больше одного (неясно, какая строка верна) — ошибка.
func cbChangesCountryRows(f *excelize.File, maxRow int) (map[string]int, error) {
	rowsByName := make(map[string][]int, maxRow-cbChangesFirstDataRow+1)
	for row := cbChangesFirstDataRow; row <= maxRow; row++ {
		raw, err := cellRaw(f, cbChangesSheet, cbChangesNameCol, row)
		if err != nil {
			return nil, fmt.Errorf("лист %s: %s: %w", cbChangesSheet, cbCellRef(cbChangesNameCol, row), err)
		}
		name := strings.TrimSpace(raw)
		if name == "" {
			continue // строки-итоги и пустые хвосты: приписать их стране словаря нельзя
		}
		rowsByName[name] = append(rowsByName[name], row)
	}

	rows := make(map[string]int, len(cbCountries))
	for _, c := range cbCountries {
		found := rowsByName[c.Name]
		switch len(found) {
		case 0:
			return nil, fmt.Errorf("лист %s: страна %q (%s) не найдена в колонке B: словарь и выгрузка разошлись",
				cbChangesSheet, c.Name, c.ISO)
		case 1:
			rows[c.ISO] = found[0]
		default:
			return nil, fmt.Errorf("лист %s: страна %q (%s) встречается больше одного раза (строки %v): неясно, какая строка верна",
				cbChangesSheet, c.Name, c.ISO, found)
		}
	}
	return rows, nil
}

// cbHoldingsBlock — блок таблицы топ-100 листа PDF: строки заголовка и колонки имени страны,
// тонн и даты уровня.
type cbHoldingsBlock struct {
	headerRow int
	nameCol   int
	tonnesCol int
	asofCol   int
}

// parseCbHoldings разбирает лист PDF выгрузки holdings: блоки таблицы топ-100 находятся по
// заголовку Tonnes (Tonnes-1 — имя страны, Tonnes+2 — «Holdings as of»), сноски в именах
// («Turkey5)») чистятся. Уровень пишется только странам словаря. Страна словаря, не попавшая в
// топ-100, — warn (топ-100 меняется, уровень за релиз просто отсутствует); дубль — ошибка (две
// точки на одну дату ReplacingMergeTree заменил бы случайной).
func parseCbHoldings(f *excelize.File) ([]seriesPoint, error) {
	maxCol, maxRow, err := sheetSize(f, cbHoldingsSheet)
	if err != nil {
		return nil, fmt.Errorf("лист %s: размеры: %w", cbHoldingsSheet, err)
	}
	blocks, err := cbHoldingsBlocks(f, maxCol, maxRow)
	if err != nil {
		return nil, err
	}

	levels := make(map[string]seriesPoint, len(cbCountries))
	for _, b := range blocks {
		for row := b.headerRow + 1; row <= maxRow; row++ {
			nameRef := cbCellRef(b.nameCol, row)
			raw, err := cellRaw(f, cbHoldingsSheet, b.nameCol, row)
			if err != nil {
				return nil, fmt.Errorf("лист %s: %s: %w", cbHoldingsSheet, nameRef, err)
			}
			name := cbStripFootnote(raw)
			if name == "" {
				break // таблица блока кончилась: ниже пустые строки, итоги и сноски
			}
			country, ok := cbCountriesByName[name]
			if !ok {
				continue // остальные страны топ-100 (США, Германия, ...) в набор не входят
			}
			if _, dup := levels[country.ISO]; dup {
				return nil, fmt.Errorf("лист %s: страна %q (%s) встречается больше одного раза: неясно, какой уровень верен",
					cbHoldingsSheet, country.Name, country.ISO)
			}

			tonnesRef := cbCellRef(b.tonnesCol, row)
			tonnesRaw, err := cellRaw(f, cbHoldingsSheet, b.tonnesCol, row)
			if err != nil {
				return nil, fmt.Errorf("лист %s: %s: %w", cbHoldingsSheet, tonnesRef, err)
			}
			tonnes, ok, err := cellFloat(tonnesRaw)
			if err != nil {
				return nil, fmt.Errorf("лист %s: %s: %w", cbHoldingsSheet, tonnesRef, err)
			}
			if !ok {
				return nil, fmt.Errorf("лист %s: %s: у страны %q пустая колонка %q",
					cbHoldingsSheet, tonnesRef, country.Name, cbHoldingsTonnesHeader)
			}

			asofRef := cbCellRef(b.asofCol, row)
			asofRaw, err := cellRaw(f, cbHoldingsSheet, b.asofCol, row)
			if err != nil {
				return nil, fmt.Errorf("лист %s: %s: %w", cbHoldingsSheet, asofRef, err)
			}
			asof, err := cellDate(asofRaw)
			if err != nil {
				return nil, fmt.Errorf("лист %s: %s: дата «Holdings as of» страны %q = %q: %w",
					cbHoldingsSheet, asofRef, country.Name, asofRaw, err)
			}

			levels[country.ISO] = seriesPoint{
				Series: wgcCbSeriesName(cbMetricHoldingsTonnes, country.ISO),
				Date:   monthEnd(asof),
				Value:  tonnes,
			}
		}
	}

	points := make([]seriesPoint, 0, len(levels))
	for _, c := range cbCountries {
		if p, ok := levels[c.ISO]; ok {
			points = append(points, p)
			continue
		}
		log.Warnf("wgc: лист %s: страна %q (%s) не найдена в топ-100 — уровень за релиз не записан",
			cbHoldingsSheet, c.Name, c.ISO)
	}
	return points, nil
}

// cbHoldingsBlocks ищет блоки таблицы топ-100: ячейка-заголовок сравнивается строго (после
// TrimSpace), поэтому подписи вроде «WORLD OFFICIAL GOLD HOLDINGS» за заголовок не считаются.
// В блоке: Tonnes-2 — ранг (не читаем), Tonnes-1 — имя страны, +1 — «% of reserves» (не
// импортируем), +2 — «Holdings as of». Соседние заголовки проверяются при привязке блока
// (cbVerifyHoldingsHeaders): раскладка, найденная по одному якорю, на веру не принимается.
func cbHoldingsBlocks(f *excelize.File, maxCol, maxRow int) ([]cbHoldingsBlock, error) {
	var blocks []cbHoldingsBlock
	for row := 1; row <= maxRow; row++ {
		for col := 1; col <= maxCol; col++ {
			raw, err := cellRaw(f, cbHoldingsSheet, col, row)
			if err != nil {
				return nil, fmt.Errorf("лист %s: %s: %w", cbHoldingsSheet, cbCellRef(col, row), err)
			}
			if strings.TrimSpace(raw) != cbHoldingsTonnesHeader {
				continue
			}
			if col < 2 || col+2 > maxCol {
				return nil, fmt.Errorf("лист %s: заголовок %q в %s: нет места под колонки блока (имя, %% of reserves, Holdings as of)",
					cbHoldingsSheet, cbHoldingsTonnesHeader, cbCellRef(col, row))
			}
			if err := cbVerifyHoldingsHeaders(f, row, col); err != nil {
				return nil, err
			}
			blocks = append(blocks, cbHoldingsBlock{
				headerRow: row,
				nameCol:   col - 1,
				tonnesCol: col,
				asofCol:   col + 2,
			})
		}
	}
	if len(blocks) == 0 {
		return nil, fmt.Errorf("лист %s: не найден заголовок %q: раскладка выгрузки изменилась", cbHoldingsSheet, cbHoldingsTonnesHeader)
	}
	return blocks, nil
}

// cbVerifyHoldingsHeaders проверяет соседние заголовки блока при его привязке — якорь Tonnes
// один раскладку не подтверждает. Tonnes+1 должен начинаться с «% of reserves» («% of
// reserves**» в выгрузке), Tonnes+2 — равняться «Holdings as of» (в файле — с хвостовым
// пробелом, сравнение после TrimSpace). Колонка имени (Tonnes-1) в выгрузке октября 2026
// подписи не несёт (клетки B6/G6 пусты; хвостовой блок стран вне топ-100 подписан «Other»),
// поэтому для неё проверяется только «не число»: число слева от Tonnes означало бы, что якорь
// встал над колонкой данных, и страны молча выпали бы из импорта. Несовпадение — ошибка с
// адресом ячейки: у дрейфа раскладки должен быть видимый след, а не молча испорченная
// «Holdings as of» (число из колонки резервов превратилось бы в дату 1900-01-31).
func cbVerifyHoldingsHeaders(f *excelize.File, headerRow, tonnesCol int) error {
	nameRef := cbCellRef(tonnesCol-1, headerRow)
	nameRaw, err := cellRaw(f, cbHoldingsSheet, tonnesCol-1, headerRow)
	if err != nil {
		return fmt.Errorf("лист %s: заголовок колонки имени %s: %w", cbHoldingsSheet, nameRef, err)
	}
	if _, ok, numErr := cellFloat(nameRaw); numErr == nil && ok {
		return fmt.Errorf("лист %s: %s = %q: слева от %q ожидалась колонка имени, а не число — раскладка выгрузки изменилась",
			cbHoldingsSheet, nameRef, nameRaw, cbHoldingsTonnesHeader)
	}

	reservesRef := cbCellRef(tonnesCol+1, headerRow)
	reservesRaw, err := cellRaw(f, cbHoldingsSheet, tonnesCol+1, headerRow)
	if err != nil {
		return fmt.Errorf("лист %s: заголовок колонки резервов %s: %w", cbHoldingsSheet, reservesRef, err)
	}
	if !strings.HasPrefix(strings.TrimSpace(reservesRaw), cbHoldingsReservesHeaderPrefix) {
		return fmt.Errorf("лист %s: %s = %q: ожидалась подпись, начинающаяся с %q — раскладка выгрузки изменилась",
			cbHoldingsSheet, reservesRef, reservesRaw, cbHoldingsReservesHeaderPrefix)
	}

	asofRef := cbCellRef(tonnesCol+2, headerRow)
	asofRaw, err := cellRaw(f, cbHoldingsSheet, tonnesCol+2, headerRow)
	if err != nil {
		return fmt.Errorf("лист %s: заголовок колонки %q %s: %w", cbHoldingsSheet, cbHoldingsAsofHeader, asofRef, err)
	}
	if strings.TrimSpace(asofRaw) != cbHoldingsAsofHeader {
		return fmt.Errorf("лист %s: %s = %q: ожидался заголовок %q — раскладка выгрузки изменилась",
			cbHoldingsSheet, asofRef, asofRaw, cbHoldingsAsofHeader)
	}
	return nil
}

// cbStripFootnote убирает сноску в конце имени страны («Turkey5)» → «Turkey») и обрезает
// пробелы — в выгрузке встречаются имена со сносками и заголовки с хвостовыми пробелами.
func cbStripFootnote(raw string) string {
	return strings.TrimSpace(cbFootnoteSuffix.ReplaceAllString(strings.TrimSpace(raw), ""))
}

// cbCellRef — адрес ячейки для ошибок: дефект выгрузки должен быть виден по месту.
func cbCellRef(col, row int) string {
	ref, err := excelize.CoordinatesToCellName(col, row)
	if err != nil {
		return fmt.Sprintf("колонка %d строка %d", col, row)
	}
	return ref
}
