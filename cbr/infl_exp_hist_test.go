package cbr

import (
	"math"
	"os"
	"testing"
	"time"
)

func cbrInflExpHistFixture(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", path, err)
	}
	return body
}

// TestCbrInflExpHistTable2023 — таблица волны 2023-01: последние значения строк совпадают с
// точками книги в БД (наблюдаемая 14,9, ожидаемая 11,6 на 2023-01-14). Это главный якорь
// разбора: правило «последнее значение строки» сверено с независимым источником.
func TestCbrInflExpHistTable2023(t *testing.T) {
	rows, err := inflExpHistRows(cbrInflExpHistFixture(t, "testdata/infl_exp_hist_2023_table.txt"), time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("разбор таблицы 2023-01: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("строк %d, want 2 (наблюдаемая и ожидаемая)", len(rows))
	}
	want := map[string]float64{inflExpObservedName: 14.9, inflExpExpectedName: 11.6}
	for _, row := range rows {
		if !row.Date.Equal(time.Date(2023, time.January, 14, 0, 0, 0, 0, time.UTC)) {
			t.Errorf("дата %s, want 2023-01-14", row.Date.Format("2006-01-02"))
		}
		if expected, ok := want[row.Series]; !ok || math.Abs(row.Value-expected) > 1e-4 {
			t.Errorf("%s = %v, want %v", row.Series, row.Value, expected)
		}
	}
}

// TestCbrInflExpHistTable2019 — волна 2019-01: подпись строки подробнее («ФОМ (медиана,
// наблюдаемая инфляция)»), значения 10,1 и 10,4 — хвост пятимесячного ряда.
func TestCbrInflExpHistTable2019(t *testing.T) {
	rows, err := inflExpHistRows(cbrInflExpHistFixture(t, "testdata/infl_exp_hist_2019_table.txt"), time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("разбор таблицы 2019-01: %v", err)
	}
	want := map[string]float64{inflExpObservedName: 10.1, inflExpExpectedName: 10.4}
	if len(rows) != len(want) {
		t.Fatalf("строк %d, want %d", len(rows), len(want))
	}
	for _, row := range rows {
		if expected, ok := want[row.Series]; !ok || math.Abs(row.Value-expected) > 1e-4 {
			t.Errorf("%s = %v, want %v", row.Series, row.Value, expected)
		}
	}
}

// TestCbrInflExpHistNoTable2017 — в выпуске 2017-01 таблицы нет: числа напечатаны прозой в
// двухколоночной вёрстке. Это не ошибка разбора, а граница корпуса: волна пропускается.
func TestCbrInflExpHistNoTable2017(t *testing.T) {
	rows, err := inflExpHistRows(cbrInflExpHistFixture(t, "testdata/infl_exp_hist_2017_prose.txt"), time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("разбор текста 2017-01: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("строк %d, want 0: в выпуске 2017-01 таблицы нет", len(rows))
	}
}

// TestCbrInflExpHistWaves — реестр волн со страницы ожиданий: не меньше 140 месяцев, свежая
// волна — 2026-10, а волна 2022-01 приходит страницей (PDF лежит внутри неё).
func TestCbrInflExpHistWaves(t *testing.T) {
	waves, err := inflExpHistWaves(cbrInflExpHistFixture(t, "testdata/infl_exp_index.html"))
	if err != nil {
		t.Fatalf("разбор страницы волн: %v", err)
	}
	if len(waves) < 140 {
		t.Fatalf("волн %d, want не меньше 140", len(waves))
	}
	if got := waves[0].month.Format("2006-01"); got != "2026-10" {
		t.Errorf("свежая волна %s, want 2026-10", got)
	}
	byMonth := map[string]inflExpHistWave{}
	for _, wave := range waves {
		byMonth[wave.month.Format("2006-01")] = wave
	}
	if wave, ok := byMonth["2022-01"]; !ok || wave.url[len(wave.url)-1:] != "/" {
		t.Errorf("волна 2022-01: %+v, want ссылку на страницу волны", wave)
	}
	if wave, ok := byMonth["2019-01"]; !ok || wave.url[len(wave.url)-4:] != ".pdf" {
		t.Errorf("волна 2019-01: %+v, want прямой PDF", wave)
	}
}
