package cbr

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
)

// ruoniaFixture — выгрузка RUONIA, скачанная с URL из cbrRuoniaUrl
// (FromDate=01/01/2019, ToDate=13.10.2026 — сегодняшняя дата в формате Import). В книге
// ровно один лист «RC», поэтому zip-хирургия не нужна: фикстура закоммичена тем же
// файлом, что отдал ЦБ.
const ruoniaFixture = "testdata/ruonia.xlsx"

// ruoniaCatalogColumns — метрики каталога cbr_ruania в порядке колонок Append у
// ruoniaImport (date, ruo, vol, trans, minRate, maxRate, p25, p75) — тот же порядок, что в
// DDL cbr_ruania и в раскладке views.go. Таблица широкая, колонки name у неё нет,
// поэтому имя метрики задаёт позиция в строке, а не значение.
var ruoniaCatalogColumns = []string{
	"cbr_ruania_ruo",
	"cbr_ruania_vol",
	"cbr_ruania_trans",
	"cbr_ruania_min_rate",
	"cbr_ruania_max_rate",
	"cbr_ruania_p25",
	"cbr_ruania_p75",
}

// ruoniaNamedBatch переводит широкую строку ruoniaImport (date + 7 метрик) в именованные
// строки каталога: приёмник checkCbrSeriesMeta знает формы (date, value) и
// (name, date, value), а у cbr_ruania имя метрики задаёт позиция колонки — ровно так же,
// как это делает views.go. Сам разбор строки остаётся за ruoniaImport: приёмник только
// перекладывает уже разобранные значения.
type ruoniaNamedBatch struct {
	driver.Batch
	rec *appendRecorder
}

func (b ruoniaNamedBatch) Append(v ...any) error {
	if len(v) != len(ruoniaCatalogColumns)+1 {
		return fmt.Errorf("строка RUONIA из %d значений, ожидалось %d (date + %d метрик)",
			len(v), len(ruoniaCatalogColumns)+1, len(ruoniaCatalogColumns))
	}
	date, ok := v[0].(time.Time)
	if !ok {
		return fmt.Errorf("дата RUONIA пришла как %T, ожидался time.Time", v[0])
	}
	for i, name := range ruoniaCatalogColumns {
		value := v[i+1]
		// Метрики, которых нет в исторической строке, приходят указателями (Nullable в
		// DDL): nil — значения нет, и строки каталога у такой точки быть не должно.
		switch typed := value.(type) {
		case *uint16:
			if typed == nil {
				continue
			}
			value = uint64(*typed)
		case *float32:
			if typed == nil {
				continue
			}
			value = float64(*typed)
		}
		if err := b.rec.Append(name, date, value); err != nil {
			return err
		}
	}
	return nil
}

// ruoniaRows разбирает фикстуру тем же кодом, что и импортёр (ruoniaImport), и раскладывает
// метрики по именам каталога. Ошибка разбора, расхождение формы строки со схемой приёмника
// и пустой разбор — падение теста: фикстура и есть контракт.
func ruoniaRows(t *testing.T) []appendedRow {
	t.Helper()
	f, err := excelize.OpenFile(ruoniaFixture)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", ruoniaFixture, err)
	}
	defer func() { _ = f.Close() }()
	rec := newCbrRecorder()
	if err = ruoniaImport(f, ruoniaNamedBatch{rec: rec}); err != nil {
		t.Fatalf("разбор фикстуры %s: %v", ruoniaFixture, err)
	}
	if rec.err != nil {
		t.Fatalf("разбор фикстуры %s: схема строк не совпала с разбором: %v", ruoniaFixture, rec.err)
	}
	if len(rec.rows) == 0 {
		t.Fatalf("разбор фикстуры %s не дал ни одной точки", ruoniaFixture)
	}
	return rec.rows
}

// TestCbrRuaniaSeriesMetaCoversParsedRows: каталог cbr_ruania (7 метрик) и разбор фикстуры
// листа «RC» совпадают в обе стороны. Строка файла широкая (date + 7 метрик), поэтому
// разбор раскладывается по именам каталога в порядке колонок DDL (ruoniaNamedBatch).
func TestCbrRuaniaSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrRuaniaSeriesMeta, func(rec *appendRecorder) error {
		f, err := excelize.OpenFile(ruoniaFixture)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return ruoniaImport(f, ruoniaNamedBatch{rec: rec})
	})
}

// TestCbrRuaniaFixtureRows: разбор фикстуры даёт 1908 точек ставки и меньше — по
// остальным метрикам: источник публикует их не в каждой строке (17 дней без оборота,
// 496 — без min/max/перцентилей и числа сделок). Ноль за отсутствующую метрику не
// пишется: до перевода на Nullable (2026-11-14) эти 496 строк уезжали в БД нулями —
// нулевая минимальная ставка читалась как «0 % годовых».
// Числа и порядок пинят фикстуру: молчаливая потеря или переворот части листа «RC» не пройдёт.
func TestCbrRuaniaFixtureRows(t *testing.T) {
	want := map[string]int{
		"cbr_ruania_ruo":      1908,
		"cbr_ruania_vol":      1891,
		"cbr_ruania_trans":    1412,
		"cbr_ruania_min_rate": 1412,
		"cbr_ruania_max_rate": 1412,
		"cbr_ruania_p25":      1412,
		"cbr_ruania_p75":      1412,
	}
	rows := ruoniaRows(t)
	dates := map[string][]time.Time{}
	for _, row := range rows {
		dates[row.name] = append(dates[row.name], row.date)
	}
	if len(rows) != 10859 {
		t.Errorf("строк разбора %d, want 10859 (1908 ставок + 1891 объёмов + 5 × 1412 метрик)", len(rows))
	}
	for name, count := range want {
		if got := len(dates[name]); got != count {
			t.Errorf("метрика %q: точек %d, want %d", name, got, count)
		}
	}
	newest, oldest := time.Date(2026, time.October, 12, 0, 0, 0, 0, time.UTC), time.Date(2019, time.January, 9, 0, 0, 0, 0, time.UTC)
	got := dates["cbr_ruania_ruo"]
	if len(got) == 0 || !got[0].Equal(newest) || !got[len(got)-1].Equal(oldest) {
		t.Errorf("ряд ставки не 2026-10-12..2019-01-09: %v..%v (порядок файла — от свежей к старой)", got[0], got[len(got)-1])
	}
}

// TestCbrRuaniaSpotValues: значения всех 7 метрик на 2026-10-08 из baseline T1 (снимок
// витрины v_cbr_macro) совпадают с разбором фикстуры в точности Float32. Семь спотов
// держат и привязку метрики к колонке: перестановка p25/p75 или min/max ловится здесь.
func TestCbrRuaniaSpotValues(t *testing.T) {
	rows := ruoniaRows(t)
	date := time.Date(2026, time.October, 8, 0, 0, 0, 0, time.UTC)
	cbrCheckSpot(t, rows, "cbr_ruania_ruo", date, 14.010000228881836)
	cbrCheckSpot(t, rows, "cbr_ruania_vol", date, 826.1799926757812)
	cbrCheckSpot(t, rows, "cbr_ruania_trans", date, 67.0)
	cbrCheckSpot(t, rows, "cbr_ruania_min_rate", date, 13.649999618530273)
	cbrCheckSpot(t, rows, "cbr_ruania_max_rate", date, 14.149999618530273)
	cbrCheckSpot(t, rows, "cbr_ruania_p25", date, 13.75)
	cbrCheckSpot(t, rows, "cbr_ruania_p75", date, 14.050000190734863)
}

// TestCbrRuaniaMissingSheetFails: книга без листа «RC» — ошибка, а не «0 rows»: имя листа
// в ImportFunc захардкожено, и его смена у ЦБ не должна выглядеть успешным прогоном.
func TestCbrRuaniaMissingSheetFails(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	rec := newCbrRecorder()
	if err := ruoniaImport(f, ruoniaNamedBatch{rec: rec}); err == nil {
		t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
	}
	if len(rec.raw) != 0 {
		t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
	}
}

// TestCbrRuaniaEmptySheetFails: лист «RC» есть, но строк данных нет — ошибка, а не
// «0 rows» с зелёным dagu: пустая выгрузка ЦБ не должна выглядеть успешным прогоном (в
// таблице широких строк ни одной точки, а зелёный шаг DAG сообщил бы об обратном). Имя
// таблицы в тексте ошибки сверяется литералом: оно — часть контракта для дежурного по логу.
func TestCbrRuaniaEmptySheetFails(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", "RC"); err != nil {
		t.Fatalf("SetSheetName: %v", err)
	}
	rec := newCbrRecorder()
	err := ruoniaImport(f, rec)
	if err == nil {
		t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
	}
	if !strings.Contains(err.Error(), "cbr_ruania") {
		t.Errorf("ошибка %q не называет таблицу cbr_ruania", err)
	}
	if len(rec.raw) != 0 {
		t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
	}
}

// TestCbrRuaniaUrlStartsAtHistoryStart: выгрузка начинается с начала индекса RUONIA
// (11.01.2010), а не с 2019 года. До 2026-11-14 вечером FromDate в URL стоял 01/01/2019,
// и девять лет денежного рынка терялись только из-за этой константы: источник отдаёт
// историю с 2010-01-11 (research/2026-11-14-ruania-history-source.md).
func TestCbrRuaniaUrlStartsAtHistoryStart(t *testing.T) {
	if got, want := cbrRuoniaFromDate, "01/01/2010"; got != want {
		t.Errorf("FromDate выгрузки RUONIA = %q, want %q", got, want)
	}
	if got, want := cbrRuoniaHistoryFrom, "2010-01-11"; got != want {
		t.Errorf("начало индекса RUONIA = %q, want %q (методология ЦБ)", got, want)
	}
	url := fmt.Sprintf(cbrRuoniaUrl, "02.11.2026")
	if !strings.Contains(url, "FromDate="+cbrRuoniaFromDate) {
		t.Errorf("URL выгрузки %q не содержит FromDate=%s", url, cbrRuoniaFromDate)
	}
	if !strings.Contains(url, "ToDate=02.11.2026") {
		t.Errorf("URL выгрузки %q не подставляет ToDate: экспорт с ToDate в прошлом отдаёт пустой лист", url)
	}
	if strings.Contains(url, "FromDate=01/01/2019") {
		t.Errorf("URL выгрузки %q всё ещё начинается с 2019 года — потеряны 2010–2018", url)
	}
}

// TestRuoniaParseRowLegacy — историческая строка выгрузки (2010–2018) приходит с тремя
// значениями: дата, ставка, объём. Пять метрик источника нет, и они обязаны остаться nil,
// а не нулями: нулевой minRate выглядел бы как настоящая ставка 0 % годовых.
func TestRuoniaParseRowLegacy(t *testing.T) {
	row, err := ruoniaParseRow([]string{"01-11-10", "2.9800", "64.5600"})
	if err != nil {
		t.Fatalf("разбор исторической строки: %v", err)
	}
	if got, want := row.date.Format("2006-01-02"), "2010-01-11"; got != want {
		t.Errorf("дата %q, want %q", got, want)
	}
	if row.ruo != 2.98 || row.vol == nil || *row.vol != 64.56 {
		t.Errorf("ставка/объём %v/%v, want 2.98/64.56", row.ruo, row.vol)
	}
	if row.trans != nil || row.minRate != nil || row.maxRate != nil || row.p25 != nil || row.p75 != nil {
		t.Errorf("метрики исторической строки должны быть nil: trans=%v min=%v max=%v p25=%v p75=%v",
			row.trans, row.minRate, row.maxRate, row.p25, row.p75)
	}
}

// TestRuoniaParseRowModern — полная строка выгрузки (с 2020) разбирается по колонкам
// DDL: trans=4-я, minRate=6-я, p25=7-я, p75=8-я, maxRate=9-я. Перестановка p25/p75 или
// min/max ловится здесь.
func TestRuoniaParseRowModern(t *testing.T) {
	row, err := ruoniaParseRow([]string{"01-30-26", "13,92", "987,57", "84", "5", "13,65", "13,80", "14,05", "14,15", "", "46142"})
	if err != nil {
		t.Fatalf("разбор полной строки: %v", err)
	}
	if row.trans == nil || *row.trans != 84 {
		t.Errorf("trans=%v, want 84", row.trans)
	}
	want := map[string]struct {
		got *float32
		val float32
	}{
		"minRate": {row.minRate, 13.65}, "p25": {row.p25, 13.80}, "p75": {row.p75, 14.05}, "maxRate": {row.maxRate, 14.15},
	}
	for name, pair := range want {
		if pair.got == nil || *pair.got != pair.val {
			t.Errorf("%s=%v, want %v", name, pair.got, pair.val)
		}
	}
}

// TestRuoniaParseRowRejectsShortRow — строка короче трёх значений не является точкой:
// импортёр обязан упасть с понятным сообщением, а не молча пропустить день.
func TestRuoniaParseRowRejectsShortRow(t *testing.T) {
	if _, err := ruoniaParseRow([]string{"01-11-10", "2.9800"}); err == nil {
		t.Fatal("строка из двух значений разобрана без ошибки")
	}
	if _, err := ruoniaParseRow([]string{"01-11-10", "", "64.5600"}); err == nil {
		t.Fatal("пустая ставка разобрана без ошибки")
	}
}
