package cbr

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"strconv"
	"strings"
	"time"
)

// ruoniaRow — одна строка листа «RC» после разбора. Метрики, которых в исторических
// строках нет (2010–2018: только дата, ставка и объём), остаются nil: отсутствие
// значения — не ноль (тот же принцип, что у util.ParseHTMLTable, стандарт §6.7).
type ruoniaRow struct {
	date    time.Time
	ruo     float32
	vol     *float32
	trans   *uint16
	minRate *float32
	maxRate *float32
	p25     *float32
	p75     *float32
}

// ruoniaParseRow разбирает строку листа «RC». Ширина строки в выгрузке переменная:
// 2010–2018 — три значения (date, ruo, vol), 2017–2019 — четыре (последнее — метка
// времени выгрузки), с 2020 — все одиннадцать (date, ruo, vol, trans, C, MinRate,
// P25, P75, MaxRate, StatusXML, DateUpdate). Проверено 2026-11-14 на выгрузке
// FromDate=01/01/2010 (docs/research/2026-11-14-ruania-history-source.md §4).
// Короткая строка — не ошибка: исторические метрики источник не публикует, и писать
// за них ноль значило бы выдумать данные.
func ruoniaParseRow(row []string) (ruoniaRow, error) {
	var r ruoniaRow
	if len(row) < 3 {
		return r, fmt.Errorf("%s: строка из %d значений, ожидалось не меньше 3 (дата, ставка, объём)", cbrRuoniaTable, len(row))
	}
	date, err := time.Parse("01-02-06", row[0])
	if err != nil {
		return r, fmt.Errorf("%s: дата %q: %w", cbrRuoniaTable, row[0], err)
	}
	r.date = date
	if r.ruo, err = ruoniaFloat(row[1], "ruo"); err != nil {
		return r, err
	}
	// Объём сделок тоже бывает пустым: в выгрузке есть дни с одной ставкой и
	// StatusXML=1 (17 строк в фикстуре cbr/testdata/ruonia.xlsx) — это «ставка есть,
	// оборот не рассчитан», а не ноль.
	if r.vol, err = ruoniaFloatOpt(row[2], "vol"); err != nil {
		return r, err
	}
	if len(row) < 9 {
		return r, nil
	}
	if r.trans, err = ruoniaUint(row[3], "trans"); err != nil {
		return r, err
	}
	if r.minRate, err = ruoniaFloatOpt(row[5], "minRate"); err != nil {
		return r, err
	}
	if r.p25, err = ruoniaFloatOpt(row[6], "percentile25"); err != nil {
		return r, err
	}
	if r.p75, err = ruoniaFloatOpt(row[7], "Percentile75"); err != nil {
		return r, err
	}
	if r.maxRate, err = ruoniaFloatOpt(row[8], "maxRate"); err != nil {
		return r, err
	}
	return r, nil
}

// ruoniaFloat — обязательное непустое значение метрики. Десятичный разделитель
// принимается и запятой, и точкой: формат выгрузки может смениться.
func ruoniaFloat(raw, column string) (float32, error) {
	value, err := strconv.ParseFloat(ruoniaNumber(raw), 32)
	if err != nil {
		return 0, fmt.Errorf("%s: %s=%q: %w", cbrRuoniaTable, column, raw, err)
	}
	return float32(value), nil
}

// ruoniaNumber приводит число выгрузки к виду strconv.ParseFloat. Форматы двух эпох
// разные: современные строки — «1,073.1000» (точка — десятичная, запятая — тысячи),
// исторические — «2.9800». Поэтому запятая снимается, только когда рядом есть точка;
// одинокая запятая считается десятичным разделителем.
func ruoniaNumber(raw string) string {
	trimmed := strings.TrimSpace(raw)
	switch {
	case strings.Contains(trimmed, ",") && strings.Contains(trimmed, "."):
		return strings.ReplaceAll(trimmed, ",", "")
	case strings.Contains(trimmed, ","):
		return strings.ReplaceAll(trimmed, ",", ".")
	default:
		return trimmed
	}
}

// ruoniaFloatOpt — необязательное значение: пустая ячейка даёт nil (метрики нет),
// нечисловая — ошибку.
func ruoniaFloatOpt(raw, column string) (*float32, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	value, err := ruoniaFloat(raw, column)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

// ruoniaUint — необязательное целое (число сделок): пустая ячейка — nil.
func ruoniaUint(raw, column string) (*uint16, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 16)
	if err != nil {
		return nil, fmt.Errorf("%s: %s=%q: %w", cbrRuoniaTable, column, raw, err)
	}
	parsed := uint16(value)
	return &parsed, nil
}

// ruoniaImport разбирает лист «RC»: одна широкая строка на торговый день (date + 7 метрик,
// порядок колонок — как в DDL cbr_ruania). Пустой лист/ноль строк — ошибка (fail-fast):
// молчаливый «0 rows» выглядел бы успешным прогоном, а все семь метрик остались бы без
// обновления.
func ruoniaImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows("RC")
	if err != nil {
		return err
	}
	// Пустой лист — сразу ошибка: rows[1:] ниже паникует на пустом срезе, а паника в
	// импортёре — не «0 rows», а упавший шаг DAG без понятного сообщения.
	if len(rows) == 0 {
		return fmt.Errorf("%s: лист «RC» пуст", cbrRuoniaTable)
	}
	count := 0
	for _, row := range rows[1:] {
		if len(row) == 0 {
			continue
		}
		parsed, err := ruoniaParseRow(row)
		if err != nil {
			return err
		}
		if err = batch.Append(parsed.date, parsed.ruo, parsed.vol, parsed.trans,
			parsed.minRate, parsed.maxRate, parsed.p25, parsed.p75); err != nil {
			return err
		}
		count++
	}
	if count == 0 {
		return fmt.Errorf("%s: разбор листа «RC» не дал ни одной точки", cbrRuoniaTable)
	}
	return nil
}

// cbrRuoniaHistoryFrom — начало индекса RUONIA по методологии ЦБ: «первое значение
// индекса RUONIA устанавливается для 11 января 2010 года». До 2026-11-14 выгрузка
// начиналась с 01/01/2019, и девять лет денежного рынка (2010–2018) — ровно цикл
// ужесточения 2014–2015 — в БД отсутствовали, хотя источник их отдаёт
// (docs/research/2026-11-14-ruania-history-source.md, проба 2026-11-14: 4 150 строк,
// 2010-01-11 … 2026-11-12 при FromDate=01/01/2010).
const cbrRuoniaHistoryFrom = "2010-01-11"

const (
	cbrRuoniaTable = "cbr_ruania"
	// cbrRuoniaFromDate — начало выгрузки. Дата намеренно не 11.01.2010 ровно, а начало
	// месяца: источник отдаёт с методологического начала индекса, а сдвиг границы на
	// день вперёд отрезал бы точку 11.01.2010, если экспорт когда-нибудь начнёт
	// отдавать и 10.01.2010.
	cbrRuoniaFromDate = "01/01/2010"
	// cbrRuoniaUrl — выгрузка формы UniDbQuery. ToDate обязан быть «сегодня»:
	// экспорт с ToDate в прошлом отдаёт HTTP 200 и пустой лист (проверено
	// 2026-11-14 на 01/02/2019 — 0 строк), поэтому дата подставляется в рантайме.
	cbrRuoniaUrl = "https://www.cbr.ru/Queries/UniDbQuery/DownloadExcel/14315?Posted=True&FromDate=" + cbrRuoniaFromDate + "&ToDate=%s"
)

// cbrRuoniaDdl — канонический DDL таблицы. Пять метрик Nullable: источник публикует
// их не за всю историю (в выгрузке 2010–2018 у строки только date, ruo, vol —
// проверено 2026-11-14), а отсутствие значения — не ноль.
const cbrRuoniaDdl = "CREATE TABLE IF NOT EXISTS %s (" +
	" date Date," +
	" ruo Float32," +
	" vol Nullable(Float32)," +
	" trans Nullable(UInt16)," +
	" minRate Nullable(Float32)," +
	" maxRate Nullable(Float32)," +
	" percentile25 Nullable(Float32)," +
	" Percentile75 Nullable(Float32)" +
	") ENGINE = ReplacingMergeTree ORDER BY (date);"

// cbrRuoniaAlters — миграция уже созданной таблицы к Nullable-схеме: CREATE IF NOT
// EXISTS существующую таблицу не меняет, а прежний DDL объявлял пять метрик
// обязательными. MODIFY COLUMN идемпотентен; локальный прецедент — cbr/psk.go
// (cbrPskAlterMaxNullable).
var cbrRuoniaAlters = []string{
	"ALTER TABLE %s MODIFY COLUMN vol Nullable(Float32)",
	"ALTER TABLE %s MODIFY COLUMN trans Nullable(UInt16)",
	"ALTER TABLE %s MODIFY COLUMN minRate Nullable(Float32)",
	"ALTER TABLE %s MODIFY COLUMN maxRate Nullable(Float32)",
	"ALTER TABLE %s MODIFY COLUMN percentile25 Nullable(Float32)",
	"ALTER TABLE %s MODIFY COLUMN Percentile75 Nullable(Float32)",
}

type ruoniaStat struct {
	util.ClickHouseImport
}

// Import вычисляет DataUrl в рантайме: в шаблон %s подставляется текущая
// дата в формате 01/02/2006 (layout совпадает с прежним DataUrlTimeFormat).
func (s *ruoniaStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	s.DataUrl = fmt.Sprintf(cbrRuoniaUrl, time.Now().Format("01/02/2006"))
	return s.ClickHouseImport.Import(ctx, conn)
}

// cbrRuoniaImporter — RUONIA: дата подставляется в URL в Import(); регистрируется
// реестром cbr/importers.go.
var cbrRuoniaImporter = &ruoniaStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   cbrRuoniaTable,
	CreateTable: append([]string{cbrRuoniaDdl}, cbrRuoniaAlters...),
	ImportFunc:  ruoniaImport,
	AfterImport: afterCbrMacro(cbrRuaniaSeriesMeta),
}}
