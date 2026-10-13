package cbr

import (
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// householdsBMesFixture — фикстура листа «Балансы» (та же выгрузка ЦБ, из которой снят
// baseline; см. отчёт Task 6).
const householdsBMesFixture = "testdata/households_bm.xlsx"

// householdsBMesFormerLegacyNames — 7 имён, которые до Task 6 числились в каталоге как
// legacy с пометкой «в текущем файле отсутствуют» (бывшая householdsBMesLegacyNames).
// Сверка с фикстурой households_bm.xlsx показала, что все 7 присутствуют в текущем файле
// и обновляются из него, поэтому в Task 6 они внесены в householdsBMesNames, а legacy-запись
// удалена. Тест держит это знание на фикстуре: если ЦБ перестанет публиковать строку,
// упадёт здесь, и каталог нужно будет развести снова.
var householdsBMesFormerLegacyNames = []string{
	"Депозиты в банках-нерезидентах в рублях",
	"Котируемые акции нерезидентов в рублях",
	"Котируемые акции резидентов в иностранной валюте",
	"Паи и акции инвестиционных фондов - резидентов в иностранной валюте",
	"Переводные депозиты в кредитных организациях в иностранной валюте",
	"автокредиты в иностранной валюте",
	"потребительские ссуды в иностранной валюте",
}

// TestHouseholdsBMesSeriesMetaCoversParsedRows: двусторонний контракт каталога и разбора
// фикстуры: разбор ⊆ каталог и каталог ⊆ разбор (70 имён, включая 7 бывших legacy).
func TestHouseholdsBMesSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, householdsBMesSeriesMeta, func(rec *appendRecorder) error {
		f, err := excelize.OpenFile(householdsBMesFixture)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return householdsBMesImport(f, rec)
	})
}

// TestHouseholdsBMesFixtureRows: разбор фикстуры даёт 70 имён и 7350 уникальных пар
// (name, date) с 2018-01-01 по 2026-09-01 — ровно как сырая таблица домохозяйств в baseline
// T1 (.superpowers/sdd/2026-10-13-cbr-refactor/cbr-baseline.md: n = 7350, names = 70,
// d0 = 2018-01-01, d1 = 2026-09-01). Строк в разборе больше (9030):
// 16 строк файла повторяют названия («кредитных организаций» 5 раз, «других финансовых
// организаций» 6, «нефинансовых организаций» 5, «государственного управления» 3,
// «cтраховщиков» 2), а ReplacingMergeTree оставляет одну строку на пару (name, date) —
// это и описано в каталоге («в таблице остаётся одна строка на название и дату»).
// Числа пинят фикстуру: молчаливая потеря листа или строки разбора не пройдёт.
func TestHouseholdsBMesFixtureRows(t *testing.T) {
	rows := cbrFixtureRows(t, householdsBMesFixture, householdsBMesImport)
	if len(rows) != 9030 {
		t.Errorf("строк в разборе %d, want 9030 (структура фикстуры)", len(rows))
	}
	names := map[string]bool{}
	pairs := map[string]bool{}
	for _, row := range rows {
		names[row.name] = true
		pairs[row.name+"\x00"+row.date.Format("2006-01-02")] = true
	}
	if len(names) != 70 {
		t.Errorf("имён в разборе %d, want 70 (baseline T1)", len(names))
	}
	if len(pairs) != 7350 {
		t.Errorf("уникальных пар (name, date) %d, want 7350 (baseline T1)", len(pairs))
	}
}

// TestHouseholdsBMesFormerLegacyNamesParsed: все 7 бывших legacy-имён реально есть в
// текущем файле — у каждого полный ряд 2018-01-01..2026-09-01 (105 точек), как в сырой
// таблице домохозяйств (n = 105 на каждое имя). Это и есть основание для обновления каталога.
func TestHouseholdsBMesFormerLegacyNamesParsed(t *testing.T) {
	rows := cbrFixtureRows(t, householdsBMesFixture, householdsBMesImport)
	perName := map[string][]time.Time{}
	for _, row := range rows {
		perName[row.name] = append(perName[row.name], row.date)
	}
	first, last := time.Date(2018, time.January, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	for _, name := range householdsBMesFormerLegacyNames {
		dates := perName[name]
		if len(dates) != 105 {
			t.Errorf("имя %q: точек %d, want 105 (n = 105 в сырой таблице)", name, len(dates))
			continue
		}
		if !dates[0].Equal(first) || !dates[len(dates)-1].Equal(last) {
			t.Errorf("имя %q: ряд %s..%s, want %s..%s", name, dates[0].Format("2006-01-02"),
				dates[len(dates)-1].Format("2006-01-02"), first.Format("2006-01-02"), last.Format("2006-01-02"))
		}
	}
}

// TestHouseholdsBMesSpotValues: спот-значения из baseline T1 (последняя точка 2026-09-01,
// снята из витрины v_cbr_macro) совпадают с разбором фикстуры в точности Float32.
func TestHouseholdsBMesSpotValues(t *testing.T) {
	rows := cbrFixtureRows(t, householdsBMesFixture, householdsBMesImport)
	date := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	cbrCheckSpot(t, rows, "Наличная валюта(2)", date, 27606.400390625)
	cbrCheckSpot(t, rows, "Котируемые акции резидентов - всего", date, 4668.2998046875)
}

// TestHouseholdsBMesEmptySheetFails: пустой лист, отсутствующий лист и лист без раздела
// «АКТИВЫ» — ошибка, а не «0 rows»: иначе молчаливый прогон сотрёт ряд нулём строк.
func TestHouseholdsBMesEmptySheetFails(t *testing.T) {
	empty := func(t *testing.T) *excelize.File {
		t.Helper()
		f := excelize.NewFile()
		t.Cleanup(func() { _ = f.Close() })
		return f
	}
	for name, build := range map[string]func(t *testing.T) *excelize.File{
		"нет листа «Балансы»": func(t *testing.T) *excelize.File { return empty(t) },
		"лист пуст": func(t *testing.T) *excelize.File {
			f := empty(t)
			if err := f.SetSheetName("Sheet1", "Балансы"); err != nil {
				t.Fatalf("SetSheetName: %v", err)
			}
			return f
		},
		"нет раздела «АКТИВЫ»": func(t *testing.T) *excelize.File {
			f := empty(t)
			if err := f.SetSheetName("Sheet1", "Балансы"); err != nil {
				t.Fatalf("SetSheetName: %v", err)
			}
			_ = f.SetCellValue("Балансы", "A1", "Финансовые активы и обязательства")
			_ = f.SetCellValue("Балансы", "A2", "Наличная валюта(2)")
			_ = f.SetCellValue("Балансы", "B2", "11 508.8")
			return f
		},
		"после «АКТИВЫ» нет строк данных": func(t *testing.T) *excelize.File {
			f := empty(t)
			if err := f.SetSheetName("Sheet1", "Балансы"); err != nil {
				t.Fatalf("SetSheetName: %v", err)
			}
			_ = f.SetCellValue("Балансы", "A1", "01-01-18")
			_ = f.SetCellValue("Балансы", "A2", "АКТИВЫ")
			return f
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			if err := householdsBMesImport(build(t), rec); err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
			}
			if len(rec.rows) != 0 {
				t.Errorf("при ошибке записано %d строк, want 0", len(rec.rows))
			}
		})
	}
}
