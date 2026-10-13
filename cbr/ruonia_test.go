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

// ruoniaFixtureRows — строк данных в фикстуре: 1909 строк листа минус строка заголовка.
const ruoniaFixtureRows = 1908

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
		if err := b.rec.Append(name, date, v[i+1]); err != nil {
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

// TestCbrRuaniaFixtureRows: разбор фикстуры даёт по 1908 точек на каждую из 7 метрик
// (1909 строк листа минус заголовок) в порядке файла — от свежей к старой, от 2026-10-12 до
// 2019-01-09. В baseline T1 было 1906 точек начиная с 2019-01-09; фикстура скачана
// 13.10.2026 и несёт ещё два торговых дня (09.10.2026 и 12.10.2026). Число строк и порядок
// пинят фикстуру: молчаливая потеря или переворот части листа «RC» не пройдёт.
func TestCbrRuaniaFixtureRows(t *testing.T) {
	rows := ruoniaRows(t)
	if len(rows) != len(ruoniaCatalogColumns)*ruoniaFixtureRows {
		t.Errorf("строк разбора %d, want %d (7 метрик × %d строк фикстуры)",
			len(rows), len(ruoniaCatalogColumns)*ruoniaFixtureRows, ruoniaFixtureRows)
	}
	newest, oldest := time.Date(2026, time.October, 12, 0, 0, 0, 0, time.UTC), time.Date(2019, time.January, 9, 0, 0, 0, 0, time.UTC)
	dates := map[string][]time.Time{}
	for _, row := range rows {
		dates[row.name] = append(dates[row.name], row.date)
	}
	for _, name := range ruoniaCatalogColumns {
		got := dates[name]
		if len(got) != ruoniaFixtureRows {
			t.Errorf("метрика %q: точек %d, want %d", name, len(got), ruoniaFixtureRows)
			continue
		}
		if !got[0].Equal(newest) || !got[len(got)-1].Equal(oldest) {
			t.Errorf("метрика %q: ряд %s..%s, want %s..%s (порядок файла — от свежей к старой)", name,
				got[0].Format("2006-01-02"), got[len(got)-1].Format("2006-01-02"),
				newest.Format("2006-01-02"), oldest.Format("2006-01-02"))
		}
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
