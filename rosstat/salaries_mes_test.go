package rosstat

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// openSalariesMesFixture открывает реальную выгрузку Росстата (testdata/salaries_mes.xlsx).
// Фикстура — не синтетическая книга (§11.1): в ней те же листы, шапка, якорь и числовой
// формат `#,##0`, на которых импортёр падал в продакшене.
func openSalariesMesFixture(t *testing.T) *excelize.File {
	t.Helper()
	xlsx, err := excelize.OpenFile("testdata/salaries_mes.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	return xlsx
}

// importSalariesMesFixture прогоняет импортёр по фикстуре и отдаёт записанные точки:
// ни базы, ни сети тесту не нужно (§11.6).
func importSalariesMesFixture(t *testing.T) []appendedRow {
	t.Helper()
	rec := &appendRecorder{}
	if err := salariesMesImport(openSalariesMesFixture(t), rec); err != nil {
		t.Fatalf("salariesMesImport: %v", err)
	}
	return rec.rows
}

func TestSalariesMesReadsFixture(t *testing.T) {
	rows := importSalariesMesFixture(t)
	// 418 — число ненулевых числовых ячеек месяцев в выгрузке (июль 1991 — июнь 2026).
	// Шесть нулей января–июня 1991 («данных не имеется») и пять пустых августа–декабря
	// 2026 точками не становятся.
	if len(rows) != 418 {
		t.Fatalf("точек %d, ожидалось 418", len(rows))
	}
	for _, row := range rows {
		if row.series != salariesMesSeriesName {
			t.Fatalf("%s: ряд %q, ожидался %q", row.date.Format("2006-01-02"), row.series, salariesMesSeriesName)
		}
	}
}

func TestSalariesMesSkipsZeroCells(t *testing.T) {
	rows := importSalariesMesFixture(t)
	for _, row := range rows {
		if row.value == 0 {
			t.Errorf("%s: записана нулевая точка — в источнике нуль означает «данных не имеется»", row.date.Format("2006-01-02"))
		}
	}
	// Первая точка — июль 1991: нули января–июня 1991 пропущены, дата сдвинута на месяц
	// вперёд (июль 1991 записан на 1 августа 1991).
	first := rows[0]
	for _, row := range rows {
		if row.date.Before(first.date) {
			first = row
		}
	}
	if want := time.Date(1991, 8, 1, 0, 0, 0, 0, time.UTC); !first.date.Equal(want) {
		t.Errorf("первая точка %s, ожидалась %s", first.date.Format("2006-01-02"), want.Format("2006-01-02"))
	}
}

func TestSalariesMesSpotValues(t *testing.T) {
	rows := importSalariesMesFixture(t)
	spots := []struct {
		date  time.Time
		value float32
	}{
		// Январь 1992 (A9 = 1992, G9 = 1) записан на 1 февраля 1992 — сдвиг AddDate(0, 1, 0).
		{time.Date(1992, 2, 1, 0, 0, 0, 0, time.UTC), 1},
		// Июнь 2026 (R43 = 114743) записан на 1 июля 2026 — последняя точка выгрузки.
		{time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), 114743},
	}
	for _, spot := range spots {
		found := false
		for _, row := range rows {
			if !row.date.Equal(spot.date) {
				continue
			}
			found = true
			if row.value != spot.value {
				t.Errorf("%s: значение %v, ожидалось %v", spot.date.Format("2006-01-02"), row.value, spot.value)
			}
		}
		if !found {
			t.Errorf("точка %s не найдена", spot.date.Format("2006-01-02"))
		}
	}
}

// TestSalariesMesSeriesMetaCoversParsedRows — двусторонний контракт каталога (§11.4):
// ряд, имя которого объявлено кодом (§6.8), обязан быть описан в salariesMesSeriesMeta,
// а каждый описанный ряд — разобран. Имя ряда в каталоге записано литералом, а не ссылкой
// на salariesMesSeriesName: иначе тест сравнивал бы константу с самой собой.
func TestSalariesMesSeriesMetaCoversParsedRows(t *testing.T) {
	rows := importSalariesMesFixture(t)
	names := make([]string, 0, len(rows))
	for _, row := range rows {
		names = append(names, row.series)
	}
	requireParsedDescribed(t, names, describedSeries(t, salariesMesSeriesMeta))
}

// TestSalariesMesMissingSheetFails — fail-fast (§6.4): книга без данных — ошибка с именем
// таблицы, а не «0 строк» и не пустой результат.
func TestSalariesMesMissingSheetFails(t *testing.T) {
	rec := &appendRecorder{}
	err := salariesMesImport(newTestWorkbook(t), rec)
	if err == nil {
		t.Fatal("пустая книга: ожидалась ошибка, получен nil")
	}
	if !strings.Contains(err.Error(), rosstatSalariesMesTable) {
		t.Errorf("в ошибке нет имени таблицы %s: %v", rosstatSalariesMesTable, err)
	}
	if len(rec.rows) != 0 {
		t.Errorf("в батч записано %d строк, ожидалось 0", len(rec.rows))
	}
}

// TestSalariesMesFindLink — селектор ссылки проверяется на HTML-фикстуре тем же
// util.FindLink, что и в Import() (§11.3): ссылка на файл ищется, а не собирается из
// угаданного имени tab1-zpl_ММ-ГГГГ.xlsx.
func TestSalariesMesFindLink(t *testing.T) {
	page, err := os.ReadFile("testdata/salaries_page.html")
	if err != nil {
		t.Fatal(err)
	}
	href, err := util.FindLink(page, salariesMesLinkSelector)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(href, "tab1-zpl") {
		t.Errorf("href %q не содержит tab1-zpl", href)
	}
}
