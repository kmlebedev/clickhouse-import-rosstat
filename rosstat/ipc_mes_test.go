package rosstat

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Стабильные имена рядов импортёра: в тесте они записаны литералами, поэтому совпадение с
// ipcMesSeriesNames и с ipcMesSeriesMeta не тавтологично (§6.8, §11.4).
const (
	ipcMesSeriesGoods    = "Индексы потребительских цен на товары и услуги по Российской Федерации"
	ipcMesSeriesFood     = "Индексы потребительских цен на продовольственные товары по Российской Федерации"
	ipcMesSeriesNonFood  = "Индексы потребительских цен на непродовольственные товары по Российской Федерации"
	ipcMesSeriesServices = "Индексы потребительских цен на услуги по Российской Федерации"
)

var ipcMesStableNames = []string{ipcMesSeriesGoods, ipcMesSeriesFood, ipcMesSeriesNonFood, ipcMesSeriesServices}

// openIpcMesFixture открывает реальную выгрузку Росстата (testdata/ipc_mes.xlsx): листы
// «Содержание», «01»…«04». В каждом числовом листе A1 — заголовок со сноской «1)» и годами
// выгрузки («…в 1991-2026*)гг.»), строка 4 — годы с колонки B, строка 5 — якорь
// «к концу предыдущего месяца», строки 6…17 — январь…декабрь, строка 18 — второй якорь
// «к декабрю предыдущего года» с годовым декабрём, ниже — сноски.
func openIpcMesFixture(t *testing.T) *excelize.File {
	t.Helper()
	xlsx, err := excelize.OpenFile("testdata/ipc_mes.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	return xlsx
}

// importIpcMesFixture прогоняет импортёр по фикстуре и отдаёт записанные точки: ни базы,
// ни сети тесту не нужно (§11.6).
func importIpcMesFixture(t *testing.T) []appendedRow {
	t.Helper()
	rec := &appendRecorder{}
	if err := ipcMesImport(openIpcMesFixture(t), rec); err != nil {
		t.Fatalf("ipcMesImport: %v", err)
	}
	return rec.rows
}

// ipcMesSheetFixtureRows — лист в разметке выгрузки, но с годами 2019–2020: год берётся из
// строки листа, и кодовая константа 1991 дала бы на такой книге даты начала девяностых
// (TestIpcMesYearsComeFromFileRow). Ниже месяцев стоит второй якорь «к декабрю предыдущего
// года» с годовым декабрём — он в ряд попасть не должен (TestIpcMesStopsAtEndOfMonthBlock).
func ipcMesSheetFixtureRows() [][]any {
	return [][]any{
		{"Индексы потребительских цен на товары и услуги1) по Российской Федерации в 1991-2026*)гг."},
		{"К содержанию"},
		{"на конец периода, в %"},
		{"", "2019", "2020"},
		{ipcMesField},
		{"январь", "100.5", "101.2"},
		{"февраль", "100.6", "101.3"},
		{"к декабрю предыдущего года"},
		{"декабрь", "200", "300"},
	}
}

// setIpcMesSheetRows наполняет лист построчной разметкой теста.
func setIpcMesSheetRows(t *testing.T, f *excelize.File, sheets []string, rows [][]any) {
	t.Helper()
	for _, sheet := range sheets {
		setSheetRows(t, f, sheet, rows)
	}
}

// TestIpcMesReadsFixture — разбор реальной выгрузки даёт четыре ряда по 429 точек. Число точное:
// январь–сентябрь доступны все 36 лет (36 × 9), а октябрь–декабрь выгрузки сентября 2026 кончаются
// 2025 годом (35 × 3). Сдвиг строки годов или чтение второго блока сдвинули бы число точек.
func TestIpcMesReadsFixture(t *testing.T) {
	rows := importIpcMesFixture(t)
	if len(rows) != 1716 {
		t.Fatalf("точек %d, ожидалось 1716", len(rows))
	}
	counts := countBySeries(rows)
	if len(counts) != 4 {
		t.Fatalf("рядов %d, ожидалось 4: %v", len(counts), counts)
	}
	for _, name := range ipcMesStableNames {
		if counts[name] != 429 {
			t.Errorf("ряд %q: точек %d, ожидалось 429", name, counts[name])
		}
	}
}

// TestIpcMesStableSeriesNames — имя ряда объявлено кодом (§6.8), а не взято из заголовка листа:
// заголовок несёт сноску «1)» и годы выгрузки («…в 1991-2026*)гг.»), и имя из заголовка оборвало
// бы головной ряд инфляции на первой же редакции файла Росстата.
func TestIpcMesStableSeriesNames(t *testing.T) {
	rows := importIpcMesFixture(t)
	seen := make(map[string]bool, len(ipcMesStableNames))
	for _, row := range rows {
		if strings.Contains(row.series, "1991-2026") || strings.Contains(row.series, ")") {
			t.Errorf("имя ряда %q содержит годы или сноску заголовка листа", row.series)
		}
		seen[row.series] = true
	}
	for _, name := range ipcMesStableNames {
		if !seen[name] {
			t.Errorf("ряда %q нет среди разобранных имён", name)
		}
	}
}

// TestIpcMesStopsAtEndOfMonthBlock — блок месяцев кончается до второго якоря «к декабрю
// предыдущего года» (строка 18 листа «01»): под ним лежит годовой декабрь — другой показатель
// («01», 1992 год: 2608.84 против месячного 125.2). Попади он в ряд, у декабря 1992 было бы две
// точки, а годовой индекс стоял бы датой 1992-12-31.
func TestIpcMesStopsAtEndOfMonthBlock(t *testing.T) {
	rows := importIpcMesFixture(t)
	in1992 := 0
	for _, row := range rows {
		if row.series != ipcMesSeriesGoods {
			continue
		}
		if row.value == float32(2608.84) {
			t.Errorf("в ряд «01» попало годовое значение 2608.84 датой %s", row.date.Format("2006-01-02"))
		}
		if row.date.Year() == 1992 {
			in1992++
		}
	}
	if in1992 != 12 {
		t.Errorf("точек ряда «01» за 1992 год %d, ожидалось 12", in1992)
	}
}

// TestIpcMesSpotValue — январь 1992 листа «01» = 345.3, дата — последний день отчётного месяца
// (1992-01-31). Константа 1991 на этой фикстуре совпала бы с годом из строки 4 случайно, поэтому
// происхождение года проверяет отдельная книга с чужими годами (TestIpcMesYearsComeFromFileRow).
func TestIpcMesSpotValue(t *testing.T) {
	const want = float32(345.3)
	date := time.Date(1992, time.January, 31, 0, 0, 0, 0, time.UTC)
	found := 0
	for _, row := range importIpcMesFixture(t) {
		if row.series != ipcMesSeriesGoods || !row.date.Equal(date) {
			continue
		}
		found++
		if row.value != want {
			t.Errorf("%s: значение %v, ожидалось %v", date.Format("2006-01-02"), row.value, want)
		}
	}
	if found != 1 {
		t.Fatalf("точек ряда «01» на %s — %d, ожидалась 1", date.Format("2006-01-02"), found)
	}
}

// TestIpcMesYearsComeFromFileRow — годы берутся из строки листа над якорем, а не из константы:
// на книге с годами 2019–2020 кодовый год начала ряда дал бы даты 1991–1992, и точки встали бы
// в конец чужого ряда (в таблице с ключом (name, date) это тихая подмена данных).
func TestIpcMesYearsComeFromFileRow(t *testing.T) {
	sheets := []string{"01", "02", "03", "04"}
	f := newTestWorkbook(t, sheets...)
	setIpcMesSheetRows(t, f, sheets, ipcMesSheetFixtureRows())
	rec := &appendRecorder{}
	if err := ipcMesImport(f, rec); err != nil {
		t.Fatalf("ipcMesImport: %v", err)
	}
	dates := seriesDates(rec.rows, ipcMesSeriesGoods)
	want := []string{"2019-01-31", "2020-01-31", "2019-02-28", "2020-02-29"}
	if len(dates) != len(want) {
		t.Fatalf("точек ряда «01» %d, ожидалось %d", len(dates), len(want))
	}
	for i, date := range dates {
		if got := date.Format("2006-01-02"); got != want[i] {
			t.Errorf("точка %d: дата %s, ожидалась %s", i+1, got, want[i])
		}
	}
}

// TestIpcMesSeriesMetaCoversParsedRows — двусторонний контракт каталога (§11.4): имя ряда
// объявлено кодом (§6.8), поэтому проверяются оба направления — каждый разобранный ряд описан в
// ipcMesSeriesMeta и каждый описанный ряд разбирается. Имена каталога записаны литералами, так
// что правка только одного из двух мест тест ломает.
func TestIpcMesSeriesMetaCoversParsedRows(t *testing.T) {
	rows := importIpcMesFixture(t)
	names := make([]string, 0, len(rows))
	for _, row := range rows {
		names = append(names, row.series)
	}
	requireParsedDescribed(t, names, describedSeries(t, ipcMesSeriesMeta))
}

// TestIpcMesUnknownSheetWarns — добавленный источником числовой лист («05») — ожидаемое
// изменение выгрузки (§6.6): импортёр пишет предупреждение с именем листа и продолжает разбор
// листов «01»–«04», а не валит недельную цепочку. Молчаливый пропуск тоже неверен: оператор
// должен видеть, что новый лист не разбирается.
func TestIpcMesUnknownSheetWarns(t *testing.T) {
	var logged bytes.Buffer
	prevOut := log.StandardLogger().Out
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	sheets := []string{"01", "02", "03", "04", "05"}
	f := newTestWorkbook(t, sheets...)
	setIpcMesSheetRows(t, f, sheets, ipcMesSheetFixtureRows())
	rec := &appendRecorder{}
	if err := ipcMesImport(f, rec); err != nil {
		t.Fatalf("лист «05» — ожидаемое изменение источника, ошибки быть не должно: %v", err)
	}
	if !strings.Contains(logged.String(), "05") {
		t.Errorf("в предупреждении нет имени листа «05»: %q", logged.String())
	}
	counts := countBySeries(rec.rows)
	if len(counts) != 4 {
		t.Errorf("рядов %d, ожидалось 4: %v", len(counts), counts)
	}
	for _, name := range ipcMesStableNames {
		if counts[name] != 4 {
			t.Errorf("ряд %q: точек %d, ожидалось 4", name, counts[name])
		}
	}
}

// TestIpcMesMissingSheetFails — fail-fast (§6.4): книга без ожидаемого листа — ошибка с именем
// листа и таблицы, а не «0 точек» и не тихий пропуск. Разбор идёт целиком до первой записи, и
// батч обязан остаться пустым: частичная запись книги выглядела бы успешным импортом с
// оборванным рядом.
func TestIpcMesMissingSheetFails(t *testing.T) {
	sheets := []string{"01", "02", "04"}
	f := newTestWorkbook(t, sheets...)
	setIpcMesSheetRows(t, f, sheets, ipcMesSheetFixtureRows())
	rec := &appendRecorder{}
	err := ipcMesImport(f, rec)
	if err == nil {
		t.Fatal("книга без листа «03»: ожидалась ошибка, получен nil")
	}
	if !strings.Contains(err.Error(), "03") {
		t.Errorf("в ошибке нет номера листа «03»: %v", err)
	}
	if !strings.Contains(err.Error(), rosstatIPCMesTable) {
		t.Errorf("в ошибке нет имени таблицы %s: %v", rosstatIPCMesTable, err)
	}
	if len(rec.rows) != 0 {
		t.Errorf("в батч записано %d строк, ожидалось 0", len(rec.rows))
	}
}

// TestIpcMesFindLink — селектор проверяется на HTML-фикстуре страницы цен тем же util.FindLink,
// что и в Import() (§11.3): на странице лежат и другие выгрузки `ipc_*`, поэтому селектор
// ограничен `ipc_mes_`, а ссылка не собирается из угаданного имени файла ipc_mes_ММ-ГГГГ.xlsx.
func TestIpcMesFindLink(t *testing.T) {
	page, err := os.ReadFile("testdata/price_page.html")
	if err != nil {
		t.Fatal(err)
	}
	href, err := util.FindLink(page, ipcMesLinkSelector)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(href, "ipc_mes_") {
		t.Errorf("href %q не содержит ipc_mes_", href)
	}
}

// TestIpcMesSheetErrorBranches — ветки отказа разбора листа (§6.4), которых в полной выгрузке
// нет: без них сдвиг разметки дал бы «ноль точек» вместо ошибки, а мусор в ячейке — точку ряда.
// Книга у каждого случая своя, но листы в ней все четыре: проверяется ровно одна ветка, а не
// полнота книги.
func TestIpcMesSheetErrorBranches(t *testing.T) {
	sheets := []string{"01", "02", "03", "04"}
	tests := []struct {
		name string
		rows [][]any
		want string
	}{
		{
			name: "якорь не найден",
			rows: [][]any{{"Заголовок"}, {"", "2019", "2020"}, {"январь", "100"}},
			want: "не найдена",
		},
		{
			name: "строка годов пуста",
			rows: [][]any{{"Заголовок"}, {ipcMesField}, {"январь", "100"}},
			want: "строка годов пуста",
		},
		{
			name: "якорь в первой строке листа",
			rows: [][]any{{ipcMesField}, {"январь", "100"}},
			want: "строки годов нет над",
		},
		{
			name: "год не число",
			rows: [][]any{{"Заголовок"}, {"", "20хх", "2020"}, {ipcMesField}, {"январь", "100", "101"}},
			want: "20хх",
		},
		{
			name: "месяцы не по возрастанию",
			rows: [][]any{{"Заголовок"}, {"", "2019", "2020"}, {ipcMesField}, {"февраль", "100", "101"}, {"январь", "102", "103"}},
			want: "по возрастанию",
		},
		{
			name: "значение не число",
			rows: [][]any{{"Заголовок"}, {"", "2019", "2020"}, {ipcMesField}, {"январь", "нет", "101"}},
			want: "не число",
		},
		{
			name: "значение не конечное число",
			rows: [][]any{{"Заголовок"}, {"", "2019"}, {ipcMesField}, {"январь", "NaN"}},
			want: "не конечное",
		},
		{
			name: "колонка вне строки годов",
			rows: [][]any{{"Заголовок"}, {"", "2019"}, {ipcMesField}, {"январь", "100", "101"}},
			want: "вне строки годов",
		},
		{
			name: "точек нет",
			rows: [][]any{{"Заголовок"}, {"", "2019"}, {ipcMesField}, {"январь", ""}},
			want: "0 точек",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTestWorkbook(t, sheets...)
			setIpcMesSheetRows(t, f, sheets, tt.rows)
			rec := &appendRecorder{}
			err := ipcMesImport(f, rec)
			if err == nil {
				t.Fatalf("ожидалась ошибка, получено %d строк", len(rec.rows))
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("в ошибке нет %q: %v", tt.want, err)
			}
			if !strings.Contains(err.Error(), rosstatIPCMesTable) {
				t.Errorf("в ошибке нет имени таблицы %s: %v", rosstatIPCMesTable, err)
			}
			if len(rec.rows) != 0 {
				t.Errorf("при ошибке в батч записано %d строк, ожидалось 0", len(rec.rows))
			}
		})
	}
}
