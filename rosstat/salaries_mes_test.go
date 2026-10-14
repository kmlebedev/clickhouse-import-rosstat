package rosstat

import (
	"errors"
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
	// 421 — число ненулевых ячеек месяцев в выгрузке (июль 1991 — июль 2026): 6 точек 1991
	// года + 408 за 1992–2025 + 7 за январь–июль 2026. Шесть нулей января–июня 1991
	// («данных не имеется») и пять пустых августа–декабря 2026 точками не становятся.
	if len(rows) != 421 {
		t.Fatalf("точек %d, ожидалось 421", len(rows))
	}
	for _, row := range rows {
		if row.series != salariesMesSeriesName {
			t.Fatalf("%s: ряд %q, ожидался %q", row.date.Format("2006-01-02"), row.series, salariesMesSeriesName)
		}
	}
	// Границы серии: первая точка — июль 1991 (дата сдвинута на 1 августа 1991), последняя —
	// июль 2026 (1 августа 2026).
	if want := time.Date(1991, 8, 1, 0, 0, 0, 0, time.UTC); !rows[0].date.Equal(want) {
		t.Errorf("первая точка %s, ожидалась %s", rows[0].date.Format("2006-01-02"), want.Format("2006-01-02"))
	}
	last := rows[len(rows)-1]
	if want := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC); !last.date.Equal(want) {
		t.Errorf("последняя точка %s, ожидалась %s", last.date.Format("2006-01-02"), want.Format("2006-01-02"))
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
		// Июнь 2026 (L43 = 114743) записан на 1 июля 2026 (последняя точка — июль 2026).
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

// TestSalariesMesReadsFootnoteCells — значения октября–декабря 2022 г. источник отдаёт
// текстом со сноской («62 470(2)»: пробел — разделитель тысяч, «(2)» — номер сноски о
// покрытии). Это опубликованные значения того же ряда, и они обязаны быть точками: пропуск
// оставил бы дыру в три месяца головного ряда зарплат.
func TestSalariesMesReadsFootnoteCells(t *testing.T) {
	rows := importSalariesMesFixture(t)
	spots := []struct {
		date  time.Time
		value float32
	}{
		// P39 = «62 470(2)» — октябрь 2022, точка на 1 ноября 2022.
		{time.Date(2022, 11, 1, 0, 0, 0, 0, time.UTC), 62470},
		// Q39 = «63 060(2)» — ноябрь 2022, точка на 1 декабря 2022.
		{time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), 63060},
		// R39 = «88 468(2)» — декабрь 2022, точка на 1 января 2023.
		{time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), 88468},
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

// TestSalariesMesEmptyWorkbookFails — fail-fast (§6.4): книга без листов с данными не даёт
// ни одной точки, и это ошибка с именем таблицы, а не «0 строк». Проверяется ветка «разбор
// дал 0 точек» (якорь года не найден), а не отсутствие конкретного листа: книга создаётся
// пустой, листа «Лист1» в ней нет вовсе.
func TestSalariesMesEmptyWorkbookFails(t *testing.T) {
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

// TestSalariesMesValueBranches — табличный тест самих хелперов значения: ветки ошибок
// (нечисловой текст, испорченная сноска, NaN/Inf) в фикстуре не встречаются, а без них
// разбор молча пропустил бы мусор или записал бы его точкой (§6.4). Книга здесь не нужна —
// хелперы работают со строкой ячейки.
func TestSalariesMesValueBranches(t *testing.T) {
	tests := []struct {
		name      string
		cell      string
		wantValue float64
		wantPoint bool
		wantErr   bool
	}{
		{name: "пустая ячейка — не точка", cell: ""},
		{name: "пробелы — не точка", cell: "   "},
		{name: "литеральный нуль — не точка", cell: "0"},
		{name: "нуль с дробью — не точка", cell: "0.0"},
		{name: "сырое число", cell: "1074", wantValue: 1074, wantPoint: true},
		{name: "сырое число с копейками", cell: "1017.35", wantValue: 1017.35, wantPoint: true},
		{name: "значение со сноской", cell: "62 470(2)", wantValue: 62470, wantPoint: true},
		{name: "значение со сноской и неразрывным пробелом", cell: "62\u00a0470(2)", wantValue: 62470, wantPoint: true},
		{name: "текст — ошибка", cell: "abc", wantErr: true},
		{name: "многоточие — ошибка", cell: "…", wantErr: true},
		{name: "сноска без числа — ошибка", cell: "(2)", wantErr: true},
		{name: "незакрытая сноска — ошибка", cell: "62 470(x", wantErr: true},
		{name: "двойная сноска — ошибка", cell: "62 470(2)(3)", wantErr: true},
		{name: "сноска без числа, пробелы — ошибка", cell: "  (2)  ", wantErr: true},
		{name: "NaN — ошибка", cell: "NaN", wantErr: true},
		{name: "Inf — ошибка", cell: "Inf", wantErr: true},
		{name: "-Inf — ошибка", cell: "-Inf", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, point, err := salariesMesValue(tt.cell)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ячейка %q: ожидалась ошибка, получено значение %v (точка=%v)", tt.cell, value, point)
				}
				if point {
					t.Errorf("ячейка %q: ошибка и точка одновременно (value=%v)", tt.cell, value)
				}
				return
			}
			if err != nil {
				t.Fatalf("ячейка %q: неожиданная ошибка: %v", tt.cell, err)
			}
			if value != tt.wantValue || point != tt.wantPoint {
				t.Errorf("ячейка %q: получено (%v, point=%v), ожидалось (%v, point=%v)",
					tt.cell, value, point, tt.wantValue, tt.wantPoint)
			}
		})
	}
}

// TestSalariesMesFootnoteValueBranches — табличный тест распознавания сноски: разделитель
// тысяч пробелом снимается, а испорченная сноска («62 470(x», «(2)», двойная) не
// распознаётся — иначе значение ушло бы точкой или, наоборот, пропало молча.
func TestSalariesMesFootnoteValueBranches(t *testing.T) {
	tests := []struct {
		name      string
		cell      string
		wantValue float64
		wantOK    bool
	}{
		{name: "пробел как разделитель тысяч", cell: "62 470(2)", wantValue: 62470, wantOK: true},
		{name: "неразрывный пробел", cell: "62\u00a0470(2)", wantValue: 62470, wantOK: true},
		{name: "сноска без разделителей", cell: "62470(2)", wantValue: 62470, wantOK: true},
		{name: "без сноски", cell: "62 470"},
		{name: "сноска без числа", cell: "(2)"},
		{name: "незакрытая сноска", cell: "62 470(x"},
		{name: "двойная сноска", cell: "62 470(2)(3)"},
		{name: "текст со сноской", cell: "abc(2)"},
		{name: "пустая ячейка", cell: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, ok := salariesMesFootnoteValue(tt.cell)
			if ok != tt.wantOK {
				t.Fatalf("ячейка %q: распознано=%v, ожидалось %v", tt.cell, ok, tt.wantOK)
			}
			if ok && value != tt.wantValue {
				t.Errorf("ячейка %q: значение %v, ожидалось %v", tt.cell, value, tt.wantValue)
			}
		})
	}
}

// TestSalariesMesAppendErrorNotSwallowed — ошибка записи доходит до вызывающего (§5.5, §11.6):
// сбой Append в середине батча обязан вернуть ошибку, а не выглядеть успешным импортом.
func TestSalariesMesAppendErrorNotSwallowed(t *testing.T) {
	wantErr := errors.New("append сломан")
	rec := &appendRecorder{appendErr: wantErr}
	err := salariesMesImport(openSalariesMesFixture(t), rec)
	if !errors.Is(err, wantErr) {
		t.Fatalf("ошибка Append не вернулась: err = %v", err)
	}
	if len(rec.rows) != 0 {
		t.Errorf("в батч записано %d строк, ожидалось 0 (первый Append упал)", len(rec.rows))
	}
}
