package minfin

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// Фикстуры — реальные артефакты источника (стандарт §11.1): выпуск 05.10.2026 и
// страница каталога федерального бюджета, с которой на него ведёт ссылка.
const (
	oilGasFixtureXlsx = "Svedeniya_20261005.xlsx"
	oilGasFixturePage = "fedbud_page.html"
	oilGasFixtureHref = "/common/upload/library/2026/10/main/Svedeniya_20261005.xlsx"
)

func openOilGasFixture(t *testing.T) *excelize.File {
	t.Helper()
	xlsx, err := excelize.OpenFile(filepath.Join("testdata", oilGasFixtureXlsx))
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", oilGasFixtureXlsx, err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	return xlsx
}

// newOilGasTestWorkbook — синтетическая книга только для веток fail-fast: основной
// разбор проверяется на реальном файле, потому что синтетика повторяет допущения
// парсера и остаётся зелёной, когда источник разошёлся с кодом (стандарт §11.1).
func newOilGasTestWorkbook(t *testing.T, sheet string, rows [][]any) *excelize.File {
	t.Helper()
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", sheet); err != nil {
		t.Fatal(err)
	}
	for r, row := range rows {
		for c, value := range row {
			cell, err := excelize.CoordinatesToCellName(c+1, r+1)
			if err != nil {
				t.Fatal(err)
			}
			if err = f.SetCellValue(sheet, cell, value); err != nil {
				t.Fatal(err)
			}
		}
	}
	return f
}

// TestOilGasLinkSelectorFindsSvedeniya — тот же селектор, что и в Import(), находит
// на реальной странице каталога ссылку на актуальный Svedeniya_<YYYYMMDD>.xlsx.
func TestOilGasLinkSelectorFindsSvedeniya(t *testing.T) {
	page, err := os.ReadFile(filepath.Join("testdata", oilGasFixturePage))
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", oilGasFixturePage, err)
	}
	href, err := util.FindLink(page, oilGasLinkSelector)
	if err != nil {
		t.Fatalf("FindLink по селектору %q: %v", oilGasLinkSelector, err)
	}
	if href != oilGasFixtureHref {
		t.Errorf("FindLink вернул %q, want %q", href, oilGasFixtureHref)
	}
}

// TestOilGasImportParsesSvedeniya — разбор реального выпуска: два агрегата верхнего
// уровня по 105 месяцев (2018-01 … 2026-09); строка НДПИ-подстрок, годовые колонки и
// пустая последняя месячная колонка (2026-10) точек не дают.
func TestOilGasImportParsesSvedeniya(t *testing.T) {
	rec := &appendRecorder{}
	if err := oilGasImport(openOilGasFixture(t), rec); err != nil {
		t.Fatalf("разбор фикстуры %s: %v", oilGasFixtureXlsx, err)
	}
	if len(rec.rows) != 210 {
		t.Fatalf("точек: %d, want 210 (2 ряда × 105 месяцев)", len(rec.rows))
	}

	bySeries := make(map[string]map[string]float64)
	firstMonth := time.Date(2018, time.January, 1, 0, 0, 0, 0, time.UTC)
	lastMonth := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	for _, row := range rec.rows {
		if row.date.Day() != 1 {
			t.Errorf("ряд %q: дата %s не нормализована к 1-му числу месяца", row.name, row.date.Format("2006-01-02"))
		}
		if row.date.Before(firstMonth) || row.date.After(lastMonth) {
			t.Errorf("ряд %q: точка %s вне месячного диапазона 2018-01 … 2026-09: годовая колонка стала точкой",
				row.name, row.date.Format("2006-01-02"))
		}
		month := row.date.Format("2006-01-02")
		if bySeries[row.name] == nil {
			bySeries[row.name] = make(map[string]float64)
		}
		if _, dup := bySeries[row.name][month]; dup {
			t.Errorf("ряд %q: дата %s встречается дважды", row.name, month)
		}
		bySeries[row.name][month] = row.value
	}
	if len(bySeries) != 2 {
		t.Errorf("разобрано рядов: %d (%v), want 2: подстроки НДПИ не импортируются", len(bySeries), bySeries)
	}
	for _, series := range []string{"Нефтегазовые доходы, всего", "НДПИ"} {
		dates := bySeries[series]
		if len(dates) != 105 {
			t.Errorf("ряд %q: уникальных дат %d, want 105 (пустая 2026-10 и годовые колонки не дают точек)", series, len(dates))
		}
		var seriesMin, seriesMax time.Time
		for month := range dates {
			date, err := time.Parse("2006-01-02", month)
			if err != nil {
				t.Fatalf("ряд %q: дата %q: %v", series, month, err)
			}
			if seriesMin.IsZero() || date.Before(seriesMin) {
				seriesMin = date
			}
			if date.After(seriesMax) {
				seriesMax = date
			}
		}
		if !seriesMin.Equal(firstMonth) || !seriesMax.Equal(lastMonth) {
			t.Errorf("ряд %q: границы %s … %s, want %s … %s",
				series, seriesMin.Format("2006-01-02"), seriesMax.Format("2006-01-02"),
				firstMonth.Format("2006-01-02"), lastMonth.Format("2006-01-02"))
		}
	}

	spots := []struct {
		series string
		month  string
		want   float64
	}{
		{"Нефтегазовые доходы, всего", "2018-01-01", 661.3},
		{"Нефтегазовые доходы, всего", "2026-09-01", 452.4},
		{"НДПИ", "2018-01-01", 442.9},
		{"НДПИ", "2026-09-01", 807.0},
	}
	for _, spot := range spots {
		got, ok := bySeries[spot.series][spot.month]
		if !ok {
			t.Errorf("ряд %q: нет точки за %s", spot.series, spot.month)
			continue
		}
		if math.Abs(got-spot.want) > 1e-9 {
			t.Errorf("ряд %q за %s = %v, want %v", spot.series, spot.month, got, spot.want)
		}
	}
}

func TestOilGasImportFailsWithoutSheet(t *testing.T) {
	rec := &appendRecorder{}
	err := oilGasImport(newOilGasTestWorkbook(t, "Другой лист", [][]any{{"Показатели", 43101}}), rec)
	if err == nil {
		t.Fatal("разбор книги без листа «Информационное сообщение» прошёл без ошибки")
	}
	if !strings.Contains(err.Error(), oilGasTable) || !strings.Contains(err.Error(), oilGasSheet) {
		t.Errorf("ошибка %q не называет таблицу %q и лист %q", err, oilGasTable, oilGasSheet)
	}
}

func TestOilGasImportFailsWithoutHeader(t *testing.T) {
	rec := &appendRecorder{}
	rows := [][]any{
		{"Не показатели", 43101},
		{"Нефтегазовые доходы, всего", 661.3},
	}
	err := oilGasImport(newOilGasTestWorkbook(t, oilGasSheet, rows), rec)
	if err == nil {
		t.Fatal("разбор книги без шапки «Показатели» прошёл без ошибки")
	}
	if !strings.Contains(err.Error(), "шапк") {
		t.Errorf("ошибка %q не называет причину «нет шапки»", err)
	}
}

func TestOilGasImportFailsWithoutExpectedRows(t *testing.T) {
	rec := &appendRecorder{}
	rows := [][]any{
		{"Показатели", 43101, 43132},
		{"Нефть", 368.5, 387.1},
		{"Газ", 74.4, 57.0},
	}
	err := oilGasImport(newOilGasTestWorkbook(t, oilGasSheet, rows), rec)
	if err == nil {
		t.Fatal("разбор книги без ожидаемых строк прошёл без ошибки")
	}
	if !strings.Contains(err.Error(), "не найдена ожидаемая строка") {
		t.Errorf("ошибка %q не называет пропавшую подпись строки", err)
	}
}

func TestOilGasImportFailsWithoutPoints(t *testing.T) {
	rec := &appendRecorder{}
	rows := [][]any{
		{"Показатели", 43101, 43132},
		{"Нефтегазовые доходы, всего", "-", ""},
		{"Налог на добычу полезных ископаемых", "", "-"},
	}
	err := oilGasImport(newOilGasTestWorkbook(t, oilGasSheet, rows), rec)
	if err == nil {
		t.Fatal("разбор книги без единой точки прошёл без ошибки")
	}
	if !strings.Contains(err.Error(), "ни одной точки") {
		t.Errorf("ошибка %q не называет причину «ноль точек»", err)
	}
}

func TestOilGasImportFailsOnEmptySheet(t *testing.T) {
	rec := &appendRecorder{}
	err := oilGasImport(newOilGasTestWorkbook(t, oilGasSheet, nil), rec)
	if err == nil {
		t.Fatal("разбор пустого листа прошёл без ошибки")
	}
	if !strings.Contains(err.Error(), oilGasTable) || !strings.Contains(err.Error(), oilGasSheet) {
		t.Errorf("ошибка %q не называет таблицу %q и лист %q", err, oilGasTable, oilGasSheet)
	}
}

// TestOilGasPublishesDerivatives держит подключение производных: каталог и витрина
// публикуются шаблоном только через AfterImport, и его удаление оставило бы все
// офлайн-тесты зелёными, а агент — без описаний рядов и свежей витрины.
func TestOilGasPublishesDerivatives(t *testing.T) {
	if oilGasMes.AfterImport == nil {
		t.Error("у oilGasMes не подключён AfterImport: каталог и v_minfin_budget не будут опубликованы")
	}
	if oilGasMes.PublicationCountPoints != oilGasCountPoints {
		t.Errorf("PublicationCountPoints = %q, want %q", oilGasMes.PublicationCountPoints, oilGasCountPoints)
	}
}

func TestOilGasImportFailsWhenOneSeriesEmpty(t *testing.T) {
	rec := &appendRecorder{}
	rows := [][]any{
		{"Показатели", 43101, 43132},
		{"Нефтегазовые доходы, всего", 661.3, 628.2},
		{"Налог на добычу полезных ископаемых", "", "-"},
	}
	err := oilGasImport(newOilGasTestWorkbook(t, oilGasSheet, rows), rec)
	if err == nil {
		t.Fatal("разбор с пустым рядом прошёл без ошибки: старые точки ряда остались бы молча")
	}
	if !strings.Contains(err.Error(), "не дала ни одной точки") {
		t.Errorf("ошибка %q не называет ряд, не давший точек", err)
	}
}

// TestOilGasSeriesMetaCoversParsedRows — двусторонний контракт каталога (стандарт
// §11.4): имена рядов объявлены кодом, поэтому каждый разобранный ряд обязан быть
// описан, а каждый описанный — встретиться при разборе.
func TestOilGasSeriesMetaCoversParsedRows(t *testing.T) {
	described := describedFedbud(t, minfinOilGasSource, oilGasSeriesMeta)
	rec := &appendRecorder{}
	if err := oilGasImport(openOilGasFixture(t), rec); err != nil {
		t.Fatalf("разбор фикстуры %s: %v", oilGasFixtureXlsx, err)
	}
	parsed := make(map[string]bool)
	for _, row := range rec.rows {
		parsed[row.name] = true
		if !described[row.name] {
			t.Errorf("ряд %q из разбора не описан в oilGasSeriesMeta", row.name)
		}
	}
	for name := range described {
		if !parsed[name] {
			t.Errorf("ряд %q описан в oilGasSeriesMeta, но не встретился при разборе", name)
		}
	}
}
