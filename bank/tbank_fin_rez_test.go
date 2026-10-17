package bank

import (
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// TestTbIfrsDdlShape: DDL импортёра Т-Банка — своя %s-форма (стандарт §5.1): шаблон исполняет её
// как fmt.Sprintf(sql, TableName), поэтому имя таблицы в литерале обязано быть ровно одно и только
// в позиции подстановки, а колонка значения — Float64 (спека §3.5: таблица новая, legacy-таблицы
// пакета остаются Float32). Имя общей таблицы ВТБ (rus_vtb_group_ifrs) в литерале означало бы, что
// развязка не сделана: две непохожие отчётности писались бы в одну таблицу, а префлайт и count()
// одного импортёра считали бы точки другого.
//
// Разбор книги этим тестом не покрыт: источника-книги нет (ссылка ведёт на PDF), фикстуры нет —
// разбор переведён на батч механически и не верифицирован (спека §3.5, отступление О4).
//
// Имена таблиц — литералами, а не константами импортёра (§8.5): ссылка на константу сделала бы
// проверку тавтологичной и прошла бы ровно на той подмене имени, от которой обязана падать.
func TestTbIfrsDdlShape(t *testing.T) {
	if got := strings.Count(tbIfrsDdl, "%s"); got != 1 {
		t.Errorf("в tbIfrsDdl %d вхождений %%s, want 1 (имя таблицы подставляет шаблон):\n%s", got, tbIfrsDdl)
	}
	if !strings.Contains(tbIfrsDdl, "Float64") {
		t.Errorf("в tbIfrsDdl нет Float64: колонка значения новой таблицы обязана быть Float64 (спека §3.5):\n%s", tbIfrsDdl)
	}
	if strings.Contains(tbIfrsDdl, "rus_vtb_group_ifrs") {
		t.Errorf("tbIfrsDdl содержит имя общей таблицы ВТБ rus_vtb_group_ifrs — DDL обязан быть своим:\n%s", tbIfrsDdl)
	}
	if strings.Contains(tbIfrsDdl, "tbank_group_ifrs") {
		t.Errorf("tbIfrsDdl содержит литерал имени своей таблицы tbank_group_ifrs — имя приходит подстановкой:\n%s", tbIfrsDdl)
	}
}

// TestTbIfrsPointBounds: tbIfrsPoint владеет извлечением точки (границы шапки, строки данных и
// колонки, значение, дата), поэтому его границы проверяются синтетическими строками: фикстуры
// источника нет, полный разбор не верифицирован (О4). Каждый кейс — значение или ошибка, но не
// паника (§6.5).
func TestTbIfrsPointBounds(t *testing.T) {
	// Шапка — строка 0, данные — строка 1: значение лежит в той же колонке, что и дата шапки.
	rows := [][]string{
		{"", "Ряд", "Новостройка", "01-02-25", "01-03-25"},
		{"", "Ряд", "Новостройка", "100.5", "1,234"},
	}
	value, date, err := tbIfrsPoint(rows, 0, 1, 3)
	if err != nil {
		t.Fatalf("корректная точка: %v", err)
	}
	if value != 100.5 {
		t.Errorf("значение = %v, want 100.5", value)
	}
	if want := time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC); !date.Equal(want) {
		t.Errorf("дата = %s, want %s (layout %q)", date.Format(time.RFC3339), want.Format(time.RFC3339), tbIfrsTimeLayout)
	}
	// Запятая — тысячный разделитель прежней семантики Т-Банка, а не десятичный: «1,234» — 1234.
	if value, _, err := tbIfrsPoint(rows, 0, 1, 4); err != nil || value != 1234 {
		t.Errorf("значение «1,234» = %v, err = %v; want 1234, nil (запятая — тысячный разделитель)", value, err)
	}

	for name, tc := range map[string]struct {
		rows      [][]string
		headerRow int
		dataRow   int
		col       int
		wantErr   string
	}{
		"нет строки шапки": {
			rows: rows, headerRow: -1, dataRow: 1, col: 3, wantErr: "шапки",
		},
		"шапка за концом книги": {
			rows: rows, headerRow: 2, dataRow: 1, col: 3, wantErr: "шапки",
		},
		"нет строки данных": {
			rows: rows, headerRow: 0, dataRow: 2, col: 3, wantErr: "данных",
		},
		"отрицательная строка данных": {
			rows: rows, headerRow: 0, dataRow: -1, col: 3, wantErr: "данных",
		},
		"колонка за границей шапки": {
			rows:      [][]string{{"", "Ряд"}, {"", "Ряд", "Новостройка", "100"}},
			headerRow: 0, dataRow: 1, col: 3, wantErr: "шапки",
		},
		"колонка за границей строки данных": {
			rows:      [][]string{{"", "Ряд", "Новостройка", "01-02-25"}, {"", "Ряд", "Новостройка"}},
			headerRow: 0, dataRow: 1, col: 3, wantErr: "данных",
		},
		"отрицательная колонка": {
			rows: rows, headerRow: 0, dataRow: 1, col: -1, wantErr: "шапки",
		},
		"значение не число": {
			rows: [][]string{
				{"", "Ряд", "Новостройка", "01-02-25"},
				{"", "Ряд", "Новостройка", "н/д"},
			},
			headerRow: 0, dataRow: 1, col: 3, wantErr: "не число",
		},
		"битая дата": {
			rows: [][]string{
				{"", "Ряд", "Новостройка", "31-12-22"},
				{"", "Ряд", "Новостройка", "100"},
			},
			headerRow: 0, dataRow: 1, col: 3, wantErr: "дата",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := tbIfrsPoint(tc.rows, tc.headerRow, tc.dataRow, tc.col)
			if err == nil {
				t.Fatal("ожидалась ошибка, а точка разобрана")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ошибка %q не называет %q", err, tc.wantErr)
			}
			// Таблицу и лист в ошибку добавляет вызывающий: хелпер их не знает.
			for _, foreign := range []string{tbIfrsTable, tbIfrsSheet} {
				if strings.Contains(err.Error(), foreign) {
					t.Errorf("ошибка %q называет %q — этот контекст добавляет вызывающий", err, foreign)
				}
			}
		})
	}
}

// TestTbIfrsValueBeyondHeader: непустая ячейка правее шапки дат — ошибка с таблицей, листом,
// строкой, колонкой и значением, а не молчаливый обрыв (T6). Фикстуры источника нет, поэтому
// книга строится в памяти — полный разбор остаётся неверифицированным (О4). Разметка выведена из
// цикла tbIfrsImportFunc: строка 1 — заголовок (короче трёх ячеек — пропуск); строка 2 — шапка
// (даты с C2); строка 3 — якорь «Новостройка» в C3 и литеральный «0» в D3 (страж литерального нуля
// пропускает строку якоря — иначе «Новостройка» ушла бы в разбор значения); строка 4 — данные
// (значения с C4). Основной кейс: шапка покрывает только C — первую колонку цикла row[2:] (j=0), а
// значение лежит в D (j=1), за шапкой.
func TestTbIfrsValueBeyondHeader(t *testing.T) {
	t.Run("значение за шапкой — ошибка", func(t *testing.T) {
		rec := newAppendRecorder("name", "date", "balance")
		err := tbIfrsImportFunc(tbIfrsBeyondHeaderBook(t,
			[]string{"01-02-25"}, // шапка: только колонка C (row[2], j=0)
			[]string{"", "999"},  // данные: C4 пуста, D4 — за шапкой (j=1)
		), rec)
		if err == nil {
			t.Fatal("значение правее шапки: ожидалась ошибка, а разбор прошёл")
		}
		// Ожидания — литералами (§8.5): ссылка на константы импортёра сделала бы проверку
		// тавтологичной и прошла бы на подмене, от которой обязана падать.
		for _, want := range []string{
			"tbank_group_ifrs", "Interest", "строка 4", "колонка 4", `"999"`, "без даты в шапке",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("ошибка %q не называет %q", err, want)
			}
		}
		if len(rec.rows) != 0 {
			t.Errorf("до ошибки записано строк %d, want 0", len(rec.rows))
		}
	})
	// Пробельная ячейка — не значение: она не должна давать «значение "" без даты в шапке» и не
	// мешать уже записанной покрытой точке D4.
	t.Run("пробельная ячейка за шапкой — пропуск", func(t *testing.T) {
		rec := newAppendRecorder("name", "date", "balance")
		err := tbIfrsImportFunc(tbIfrsBeyondHeaderBook(t,
			[]string{"01-02-25", "01-03-25"}, // шапка: C и D
			[]string{"", "100", " "},         // D4 покрыта шапкой, E4 — пробел за шапкой (j=2)
		), rec)
		if err != nil {
			t.Fatalf("пробельная ячейка — не значение: %v", err)
		}
		if len(rec.rows) != 1 {
			t.Errorf("записано строк %d, want 1 (покрытая точка D4 до пробельной ячейки)", len(rec.rows))
		}
	})
}

// tbIfrsBeyondHeaderBook — книга в памяти для TestTbIfrsValueBeyondHeader. header пишется с C2,
// data — с C4 (индекс ячейки 2 — колонка C); пустые значения пропускаются, чтобы GetRows не тянул
// хвост. Строки 1, 3 и колонка B — служебная разметка, описанная в докблоке теста.
func tbIfrsBeyondHeaderBook(t *testing.T, header, data []string) *excelize.File {
	t.Helper()
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", tbIfrsSheet); err != nil {
		t.Fatalf("SetSheetName(%q): %v", tbIfrsSheet, err)
	}
	for cell, cellValue := range map[string]any{
		"A1": "Т-Банк, МСФО",
		"B3": "Ряд",
		"C3": tbIfrsDataField,
		"D3": "0",
		"B4": "Ряд",
	} {
		if err := f.SetCellValue(tbIfrsSheet, cell, cellValue); err != nil {
			t.Fatalf("SetCellValue(%s!%s): %v", tbIfrsSheet, cell, err)
		}
	}
	for _, row := range []struct {
		n     int
		cells []string
	}{{2, header}, {4, data}} {
		for i, value := range row.cells {
			if value == "" {
				continue
			}
			cell, err := excelize.CoordinatesToCellName(i+3, row.n) // индекс ячейки 2 — колонка C
			if err != nil {
				t.Fatalf("CoordinatesToCellName(%d, %d): %v", i+3, row.n, err)
			}
			if err := f.SetCellValue(tbIfrsSheet, cell, value); err != nil {
				t.Fatalf("SetCellValue(%s!%s): %v", tbIfrsSheet, cell, err)
			}
		}
	}
	return f
}
