package rosstat

import (
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

func newTestWorkbook(t *testing.T, sheets ...string) *excelize.File {
	t.Helper()
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	for i, sheet := range sheets {
		if i == 0 {
			if err := f.SetSheetName("Sheet1", sheet); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, err := f.NewSheet(sheet); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func setSheetRows(t *testing.T, f *excelize.File, sheet string, rows [][]any) {
	t.Helper()
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
}

func describedSeries(t *testing.T, meta []util.SeriesMeta) map[string]bool {
	t.Helper()
	described := make(map[string]bool, len(meta))
	for _, m := range meta {
		if m.Source != rosstatSource || m.Series == "" || m.Title == "" || m.Unit == "" || m.Frequency == "" || m.Origin == "" || m.Description == "" {
			t.Errorf("описание ряда %q заполнено не полностью", m.Series)
		}
		if described[m.Series] {
			t.Errorf("ряд %q описан дважды", m.Series)
		}
		described[m.Series] = true
	}
	return described
}

func requireParsedDescribed(t *testing.T, parsed []string, described map[string]bool) {
	t.Helper()
	seen := make(map[string]bool, len(parsed))
	for _, name := range parsed {
		if !described[name] {
			t.Errorf("ряд %q не описан в series_meta", name)
		}
		seen[name] = true
	}
	for name := range described {
		if !seen[name] {
			t.Errorf("описанный ряд %q не встретился при разборе", name)
		}
	}
}

func TestIpcWeeksSeriesMetaCoversFixture(t *testing.T) {
	xlsx, err := excelize.OpenFile("data/nedel_ipc_2023.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = xlsx.Close() }()
	table, err := parseIpcWeeks(xlsx)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(*table))
	for _, row := range *table {
		names = append(names, row[0])
	}
	described := describedSeries(t, ipcWeeksSeriesMeta())
	for _, name := range names {
		if !described[name] {
			t.Errorf("ряд %q из выгрузки не описан в series_meta", name)
		}
	}
}

func TestIpcMesSeriesMetaCoversParsedSheets(t *testing.T) {
	sheets := []string{"01", "02", "03", "04"}
	f := newTestWorkbook(t, sheets...)
	for i, sheet := range sheets {
		setSheetRows(t, f, sheet, [][]any{
			{ipcMesSeriesMeta[i].Series},
			{ipcMesField},
			{"январь", "100.5", "101.2"},
		})
	}
	table, err := parseIpcMes(f)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(*table))
	for _, row := range *table {
		names = append(names, row[0])
	}
	requireParsedDescribed(t, names, describedSeries(t, ipcMesSeriesMeta))
}

func TestVvpKvartalSeriesMetaCoversParsedSheets(t *testing.T) {
	titles := map[string]string{
		"2":  "Валовой внутрений продукт",
		"9":  "Валовой внутрений продукт",
		"10": "Валовой внутренний продукт",
		"12": "Индексы физического объема валового внутреннего продукта",
		"14": "Индексы - дефляторы валового внутреннего продукта",
	}
	sheets := []string{"2", "9", "10", "12", "14"}
	f := newTestWorkbook(t, sheets...)
	for _, sheet := range sheets {
		setSheetRows(t, f, sheet, [][]any{
			{"Национальные счета"},
			{"", titles[sheet] + " 1) (в текущих ценах)"},
			{"", "2011 г."},
			{"", "I квартал", "II квартал", "III квартал", "IV квартал"},
			{"", "15000", "15500", "16000", "16500"},
		})
	}
	table, err := parseVvpKvartal(f)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(*table))
	for _, r := range *table {
		names = append(names, r.name)
	}
	requireParsedDescribed(t, names, describedSeries(t, vvpKvartalSeriesMeta))
}

func TestSalariesMesSeriesMetaCoversParsedSheet(t *testing.T) {
	f := newTestWorkbook(t, "Лист1")
	setSheetRows(t, f, "Лист1", [][]any{
		{salariesMesSeriesMeta[0].Series},
		{salariesMesField, "", "", "", "", "", "202", "203"},
	})
	table, err := parseSalariesMes(f)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(*table))
	for _, row := range *table {
		names = append(names, row[0])
	}
	requireParsedDescribed(t, names, describedSeries(t, salariesMesSeriesMeta))
}
