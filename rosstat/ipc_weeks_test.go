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

// openIpcWeeksFixture открывает реальную выгрузку Росстата (testdata/ipc_weeks.xlsx): лист
// «Содержание» и листы-годы «2022»…«2026». Лист года устроен так: A4 — «Наименование»,
// B4… — подписи колонок («на 12 января**» — «**» помечена первая колонка года, у последней
// колонки 2022 года подпись без «на »), строки 5… — товары (значение колонки — процент к
// предыдущей дате регистрации), ниже — сноски.
func openIpcWeeksFixture(t *testing.T) *excelize.File {
	t.Helper()
	xlsx, err := excelize.OpenFile("testdata/ipc_weeks.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	return xlsx
}

// importIpcWeeksFixture прогоняет импортёр по фикстуре и отдаёт записанные точки: ни базы,
// ни сети тесту не нужно (§11.6).
func importIpcWeeksFixture(t *testing.T) []appendedRow {
	t.Helper()
	rec := &appendRecorder{}
	if err := ipcWeeksImport(openIpcWeeksFixture(t), rec); err != nil {
		t.Fatalf("ipcWeeksImport: %v", err)
	}
	return rec.rows
}

// TestIpcWeeksReadsFixture — разбор реальной выгрузки. Числа точные: 550 строк данных
// (108 + 110 + 110 + 110 + 112 — листы 2022…2026; таблицу листа замыкает сноска с одной
// заполненной ячейкой) и 131 различный товар.
//
// Точки: 26 611. От прежней таблицы ipc_weeks и числа в rosstat-baseline.md (25 691) разбор
// отличается нетто на +920, и все три составляющие разницы — исправления дефектов разбора:
//   - −28: 25 691 — все ячейки значений разобранных строк, но 28 из них — «…» («данных не
//     имеется», так подписана сноска листа; лист 2026, строка «Поездка на отдых в ОАЭ,
//     поездка»). Прежний разбор глотал ошибку strconv.ParseFloat и писал по «…» ноль — в
//     живой v_rosstat_macro таких нулевых точек было ровно 28 (из 27 407 точек пакета).
//     Теперь ячейка без данных точкой не становится (§6.7).
//   - +66: значения строк, у которых «…» стоит в первой колонке значений: лист 2022,
//     «Полет в салоне экономического класса самолета» (39 точек) и лист 2026, «Поездки в
//     отдельные страны Юго-Восточной Азии» (27 точек). Прежний разбор обрывал по такой
//     строке лист целиком и терял саму строку.
//   - +882: строки ниже такой строки, которые прежний обрыв терял вместе с ней: 2022 —
//     726 точек (15 строк: 14 полных по 51 значению и «Поездка на отдых в Турцию» с 12),
//     2026 — 156 (4 строки по 39). Все они — значения реальных товаров, перечисленных
//     в каталоге.
//
// Сумма составляющих: −28 + 66 + 882 = +920 (нетто). Число 948 — не разница, а грубая сумма
// возвращённых ячеек (66 + 882): нетто меньше неё ровно на 28 исчезнувших нулей старого разбора.
func TestIpcWeeksReadsFixture(t *testing.T) {
	rows := importIpcWeeksFixture(t)
	if len(rows) != 26611 {
		t.Fatalf("точек %d, ожидалось 26611", len(rows))
	}
	counts := countBySeries(rows)
	if len(counts) != 131 {
		t.Fatalf("рядов %d, ожидалось 131", len(counts))
	}
	for _, row := range rows {
		if row.value == 0 {
			t.Errorf("ряд %q, %s: точка со значением 0 — «…» источника не значение (§6.7)",
				row.series, row.date.Format("2006-01-02"))
		}
	}
}

// TestIpcWeeksMetaCoversParsedRows — односторонний контракт каталога (§11.4): каждый
// разобранный товар описан в ipcWeeksSeriesMeta.
//
// Обратное направление («описано ⊆ разобрано») не проверяется сознательно: имена товаров даёт
// источник, а не код, поэтому ряд, пропавший из обновлённой или урезанной фикстуры, не должен
// ломать тест и остаётся описанным в каталоге (§10.6 — у агента не должно быть ряда без
// описания, но товар, переставший публиковаться, — не повод удалять описание). На этой
// фикстуре оба множества совпадают (131 разобранный товар = 131 описание ipcWeeksProducts),
// но это свойство конкретного файла, а не контракта.
func TestIpcWeeksMetaCoversParsedRows(t *testing.T) {
	described := describedSeries(t, ipcWeeksSeriesMeta())
	for _, row := range importIpcWeeksFixture(t) {
		if !described[row.series] {
			t.Errorf("разобранный товар %q не описан в ipcWeeksSeriesMeta", row.series)
		}
	}
}

// TestIpcWeeksReadsRowsAroundNoDataRow — строки, которые прежний разбор терял: лист обрывался
// на первой строке, у которой значение в первой колонке не число (в выгрузке это «…» —
// «данных не имеется» у товара за отдельные недели), и вместе с ней пропадали и её собственные
// значения, и все строки ниже. Здесь закреплены обе половины потери: у товаров, чьи строки
// обрывали лист, точки есть, и у товаров из строк ниже них — тоже.
func TestIpcWeeksReadsRowsAroundNoDataRow(t *testing.T) {
	byYear := make(map[string]map[int]int)
	for _, row := range importIpcWeeksFixture(t) {
		if byYear[row.series] == nil {
			byYear[row.series] = make(map[int]int)
		}
		byYear[row.series][row.date.Year()]++
	}
	// Строки с «…» в первой колонке значений: у них читаются значения остальных колонок.
	// «Полет в салоне…» есть и в следующих годах, поэтому проверяется счёт 2022 года;
	// «Поездки в отдельные страны Юго-Восточной Азии» есть только в листе 2026.
	for _, tt := range []struct {
		series string
		year   int
		points int
	}{
		{"Полет в салоне экономического класса самолета, в расчете на 1000 км пути", 2022, 39},
		{"Поездки в отдельные страны Юго-Восточной Азии, поездка", 2026, 27},
		// Строки ниже обрыва: в 2022 — «Поездка на отдых в Турцию» (12 значений, остальные
		// колонки её строки помечены «…»), в 2026 — строка ниже «Поездки в отдельные страны…».
		{"Поездка на отдых в Турцию, поездка", 2022, 12},
		{"Поездка на отдых на Черноморское побережье России, поездка", 2026, 39},
	} {
		got := byYear[tt.series][tt.year]
		if got != tt.points {
			t.Errorf("ряд %q, %d год: точек %d, ожидалось %d", tt.series, tt.year, got, tt.points)
		}
	}
}

// TestIpcWeeksNoDataCellDoesNotEndSheet — ячейка «…» не значение и не терминатор: строка
// читается целиком (пропускается только сама ячейка), строки ниже остаются таблицей. Концом
// таблицы листа служит строка-сноска — одна заполненная ячейка (§6.3: терминатор — признак
// разметки, а не первое нечисловое значение), пустой первый столбец — тоже конец.
func TestIpcWeeksNoDataCellDoesNotEndSheet(t *testing.T) {
	f := newTestWorkbook(t, "2022")
	setSheetRows(t, f, "2022", [][]any{
		{"Наименование", "на 12 января**", "на 19 января", "на 26 января"},
		{"Говядина (кроме бескостного мяса), кг", "…", "100.44", "101"},
		{"Сахар-песок, кг", "100.1", "100.2", "100.3"},
		{"* Без учета статистической информации"},
		{"Яйца куриные, 10 шт.", "100.4", "100.5", "100.6"},
	})
	rec := &appendRecorder{}
	if err := ipcWeeksImport(f, rec); err != nil {
		t.Fatalf("ipcWeeksImport: %v", err)
	}
	counts := countBySeries(rec.rows)
	// 2 + 3 = 5: строка с «…» даёт две точки (значения 19 и 26 января), строка ниже — три,
	// а строка после сноски-терминатора не читается вовсе.
	if len(rec.rows) != 5 {
		t.Fatalf("точек %d, ожидалось 5: %v", len(rec.rows), rec.rows)
	}
	if counts["Яйца куриные, 10 шт."] != 0 {
		t.Errorf("строка после сноски-терминатора прочитана: %v", counts)
	}
	wantDates := map[string][]string{
		"Говядина (кроме бескостного мяса), кг": {"2022-01-19", "2022-01-26"},
		"Сахар-песок, кг":                       {"2022-01-12", "2022-01-19", "2022-01-26"},
	}
	for series, want := range wantDates {
		dates := seriesDates(rec.rows, series)
		if len(dates) != len(want) {
			t.Fatalf("ряд %q: точек %d, ожидалось %d", series, len(dates), len(want))
		}
		for i, date := range dates {
			if got := date.Format("2006-01-02"); got != want[i] {
				t.Errorf("ряд %q, точка %d: дата %s, ожидалась %s", series, i+1, got, want[i])
			}
		}
	}
	for _, row := range rec.rows {
		if row.value == 0 {
			t.Errorf("ряд %q, %s: точка со значением 0 — «…» не значение (§6.7)",
				row.series, row.date.Format("2006-01-02"))
		}
	}
}

// TestIpcWeeksEmptyNameEndsSheet — строка с пустым первым столбцом таблицей не является:
// под ней лежат сноски и пустые строки листа, а точка ушла бы в ряд с пустым именем, которого
// каталог не описывает (§10.6). Разбор на такой строке кончается, а не пишет её значения.
func TestIpcWeeksEmptyNameEndsSheet(t *testing.T) {
	f := newTestWorkbook(t, "2022")
	setSheetRows(t, f, "2022", [][]any{
		{"Наименование", "на 12 января**", "на 19 января"},
		{"Говядина (кроме бескостного мяса), кг", "100.65", "100.44"},
		{"", "100.1", "100.2"},
		{"Сахар-песок, кг", "100.3", "100.4"},
	})
	rec := &appendRecorder{}
	if err := ipcWeeksImport(f, rec); err != nil {
		t.Fatalf("ipcWeeksImport: %v", err)
	}
	if len(rec.rows) != 2 {
		t.Fatalf("точек %d, ожидалось 2 (строка без имени кончает таблицу): %v", len(rec.rows), rec.rows)
	}
	counts := countBySeries(rec.rows)
	if counts["Сахар-песок, кг"] != 0 || len(counts) != 1 {
		t.Errorf("ряды %v: ожидался только «Говядина (кроме бескостного мяса), кг»", counts)
	}
}

// TestIpcWeeksSpotValue — точка из известной ячейки выгрузки: лист «2026», товар «Говядина
// (кроме бескостного мяса), кг», колонка «на 12 января**» = 100.65.
func TestIpcWeeksSpotValue(t *testing.T) {
	const product = "Говядина (кроме бескостного мяса), кг"
	const want = float32(100.65)
	date := time.Date(2026, time.January, 12, 0, 0, 0, 0, time.UTC)
	found := 0
	for _, row := range importIpcWeeksFixture(t) {
		if row.series != product || !row.date.Equal(date) {
			continue
		}
		found++
		if row.value != want {
			t.Errorf("%s: значение %v, ожидалось %v", date.Format("2006-01-02"), row.value, want)
		}
	}
	if found != 1 {
		t.Fatalf("точек товара %q на %s — %d, ожидалась 1", product, date.Format("2006-01-02"), found)
	}
}

// TestIpcWeeksUnknownProductWarns — товар вне ipcWeeksProducts — ожидаемое изменение источника
// (§6.6): предупреждение с именем товара, точка при этом пишется. Ошибкой это быть не может:
// недельный шаг первый в цепочке dagu/rosstat.yaml, и его падение лишило бы данных все
// последующие шаги (§12.3). Предупреждение — один раз на товар, а не на каждую его точку:
// у недельного ряда точек сотни.
func TestIpcWeeksUnknownProductWarns(t *testing.T) {
	var logged bytes.Buffer
	prevOut := log.StandardLogger().Out
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	const unknown = "Товар, которого нет в каталоге, кг"
	f := newTestWorkbook(t, "2022")
	setSheetRows(t, f, "2022", [][]any{
		{"Наименование", "на 12 января**", "на 19 января"},
		{unknown, "100.65", "100.44"},
		{"Говядина (кроме бескостного мяса), кг", "100.1", "100.2"},
	})
	rec := &appendRecorder{}
	if err := ipcWeeksImport(f, rec); err != nil {
		t.Fatalf("товар вне каталога — ожидаемое изменение источника, ошибки быть не должно: %v", err)
	}
	if !strings.Contains(logged.String(), unknown) {
		t.Errorf("в предупреждении нет имени товара %q: %q", unknown, logged.String())
	}
	if n := strings.Count(logged.String(), unknown); n != 1 {
		t.Errorf("о товаре %q предупреждений %d, ожидалось одно на товар: %q", unknown, n, logged.String())
	}
	counts := countBySeries(rec.rows)
	if counts[unknown] != 2 {
		t.Errorf("точек товара вне каталога %d, ожидалось 2: %v", counts[unknown], counts)
	}
}

// TestIpcWeeksMissingHeaderFails — fail-fast (§6.4): лист-год без строки «Наименование» —
// ошибка с именем таблицы и листа, а не «0 точек» и не тихий пропуск. При ошибке в батч не
// записано ничего: разбор идёт целиком до первой записи.
func TestIpcWeeksMissingHeaderFails(t *testing.T) {
	f := newTestWorkbook(t, "2022")
	setSheetRows(t, f, "2022", [][]any{
		{"К содержанию"},
		{"", "Индекс потребительских цен"},
		{"", "в % к предыдущей дате регистрации"},
	})
	rec := &appendRecorder{}
	err := ipcWeeksImport(f, rec)
	if err == nil {
		t.Fatal("лист-год без строки «Наименование»: ожидалась ошибка, получен nil")
	}
	if !strings.Contains(err.Error(), rosstatIPCWeeksTable) {
		t.Errorf("в ошибке нет имени таблицы %s: %v", rosstatIPCWeeksTable, err)
	}
	if !strings.Contains(err.Error(), "2022") {
		t.Errorf("в ошибке нет имени листа «2022»: %v", err)
	}
	if len(rec.rows) != 0 {
		t.Errorf("при ошибке в батч записано %d строк, ожидалось 0", len(rec.rows))
	}
}

// TestIpcWeeksShortRowDoesNotPanic — строка данных короче шапки: значений в такой строке
// меньше, чем колонок, разбор не паникует и читает то, что есть. Строка короче двух ячеек
// замыкает таблицу листа (ниже неё — сноски), а строка в две ячейки — законная точка.
func TestIpcWeeksShortRowDoesNotPanic(t *testing.T) {
	f := newTestWorkbook(t, "2022")
	setSheetRows(t, f, "2022", [][]any{
		{"Наименование", "на 12 января**", "на 19 января", "на 26 января"},
		{"Говядина (кроме бескостного мяса), кг", "100.65", "100.44", "101"},
		{"Сахар-песок, кг", "99.9"},
	})
	rec := &appendRecorder{}
	if err := ipcWeeksImport(f, rec); err != nil {
		t.Fatalf("ipcWeeksImport: %v", err)
	}
	if len(rec.rows) != 4 {
		t.Fatalf("точек %d, ожидалось 4 (3 полных + 1 из короткой строки)", len(rec.rows))
	}
	if got := countBySeries(rec.rows)["Сахар-песок, кг"]; got != 1 {
		t.Errorf("точек короткой строки %d, ожидалась 1", got)
	}
}

// TestIpcWeeksRowLongerThanHeaderIsGuarded — строка данных длиннее шапки: ячеек за последней
// колонкой шапки дата не описывает, точками они не становятся. Прежний разбор индексировал
// шапку без проверки границы (rows[fieldIdx][j+1]) и паниковал — паника в разборе запрещена
// (§6.5), а недельный шаг, упав паникой, остановил бы цепочку dagu (§12.3).
func TestIpcWeeksRowLongerThanHeaderIsGuarded(t *testing.T) {
	f := newTestWorkbook(t, "2022")
	setSheetRows(t, f, "2022", [][]any{
		{"Наименование", "на 12 января**", "на 19 января"},
		{"Говядина (кроме бескостного мяса), кг", "100.65", "100.44", "101", "102"},
	})
	rec := &appendRecorder{}
	if err := ipcWeeksImport(f, rec); err != nil {
		t.Fatalf("ipcWeeksImport: %v", err)
	}
	if len(rec.rows) != 2 {
		t.Fatalf("точек %d, ожидалось 2 (по числу колонок шапки): %v", len(rec.rows), rec.rows)
	}
	want := []string{"2022-01-12", "2022-01-19"}
	for i, row := range rec.rows {
		if got := row.date.Format("2006-01-02"); got != want[i] {
			t.Errorf("точка %d: дата %s, ожидалась %s", i+1, got, want[i])
		}
	}
}

// TestIpcWeeksReadsRawCellValues — значения читаются сырыми (§6.2). Формат выгрузки `0.00`
// округлил бы 100.126 до «100.13»: форматированная строка — представление, а не данные.
// Ловушка здесь не теоретическая: `GetRows` по умолчанию отдаёт именно представление.
func TestIpcWeeksReadsRawCellValues(t *testing.T) {
	f := newTestWorkbook(t, "2022")
	setSheetRows(t, f, "2022", [][]any{
		{"Наименование", "на 12 января**"},
		{"Говядина (кроме бескостного мяса), кг", 100.126},
	})
	style, err := f.NewStyle(&excelize.Style{NumFmt: 2}) // 2 — встроенный формат `0.00`
	if err != nil {
		t.Fatal(err)
	}
	if err = f.SetCellStyle("2022", "B2", "B2", style); err != nil {
		t.Fatal(err)
	}
	rec := &appendRecorder{}
	if err = ipcWeeksImport(f, rec); err != nil {
		t.Fatalf("ipcWeeksImport: %v", err)
	}
	if len(rec.rows) != 1 {
		t.Fatalf("точек %d, ожидалась 1: %v", len(rec.rows), rec.rows)
	}
	if got := rec.rows[0].value; got != float32(100.126) {
		t.Errorf("значение %v, ожидалось 100.126 (формат `0.00` дал бы 100.13)", got)
	}
}

// TestIpcWeeksSkipsNoDataCells — «…» — подпись источника «данных не имеется» (так названа
// сноска листа), а не значение: точкой такая ячейка не становится (§6.7), и разбор строки на
// ней не обрывается — значение следующей колонки читается. Прежний разбор писал по «…» ноль,
// проглотив ошибку strconv.ParseFloat (на фикстуре — 28 нулевых точек листа 2026).
func TestIpcWeeksSkipsNoDataCells(t *testing.T) {
	f := newTestWorkbook(t, "2022")
	setSheetRows(t, f, "2022", [][]any{
		{"Наименование", "на 12 января**", "на 19 января", "на 26 января"},
		{"Говядина (кроме бескостного мяса), кг", "101.63", "…", "100.11"},
	})
	rec := &appendRecorder{}
	if err := ipcWeeksImport(f, rec); err != nil {
		t.Fatalf("«…» — подпись «данных не имеется», ошибки быть не должно: %v", err)
	}
	if len(rec.rows) != 2 {
		t.Fatalf("точек %d, ожидалось 2 (ячейка «…» точкой не становится): %v", len(rec.rows), rec.rows)
	}
	// Значения проверяются вместе с датами: пропуск «…» обязан не оставить точки с нулём
	// (прежний разбор глотал ошибку ParseFloat и писал по «…» ровно такой ноль).
	wantDate := []string{"2022-01-12", "2022-01-26"}
	wantValue := []float32{101.63, 100.11}
	for i, row := range rec.rows {
		if got := row.date.Format("2006-01-02"); got != wantDate[i] {
			t.Errorf("точка %d: дата %s, ожидалась %s", i+1, got, wantDate[i])
		}
		if row.value != wantValue[i] {
			t.Errorf("точка %d: значение %v, ожидалось %v", i+1, row.value, wantValue[i])
		}
	}
}

// TestIpcWeeksErrorBranches — ветки отказа разбора (§6.4), которых в полной выгрузке нет:
// без них сдвиг разметки дал бы «ноль точек» вместо внятной ошибки, а мусор в ячейке — точку
// ряда. Разбор идёт целиком до первой записи в батч, поэтому при любой из ошибок батч пуст.
func TestIpcWeeksErrorBranches(t *testing.T) {
	tests := []struct {
		name   string
		sheets []string
		rows   [][]any
		want   string
	}{
		{
			name:   "значение не число",
			sheets: []string{"2022"},
			rows: [][]any{
				{"Наименование", "на 12 января**", "на 19 января"},
				{"Говядина (кроме бескостного мяса), кг", "100.65", "нет"},
			},
			want: "не число",
		},
		{
			name:   "значение не конечное число",
			sheets: []string{"2022"},
			rows: [][]any{
				{"Наименование", "на 12 января**", "на 19 января"},
				{"Говядина (кроме бескостного мяса), кг", "100.65", "NaN"},
			},
			want: "не конечное",
		},
		{
			name:   "месяц подписи не распознан",
			sheets: []string{"2022"},
			rows: [][]any{
				{"Наименование", "на 12 месяца**"},
				{"Говядина (кроме бескостного мяса), кг", "100.65"},
			},
			want: "не распознан",
		},
		{
			name:   "день подписи не число",
			sheets: []string{"2022"},
			rows: [][]any{
				{"Наименование", "на двенадцать января**"},
				{"Говядина (кроме бескостного мяса), кг", "100.65"},
			},
			want: "не число",
		},
		{
			name:   "подпись колонки не «день месяц»",
			sheets: []string{"2022"},
			rows: [][]any{
				{"Наименование", "Итого"},
				{"Говядина (кроме бескостного мяса), кг", "100.65"},
			},
			want: "подпись колонки",
		},
		{
			name:   "шапка есть, точек нет",
			sheets: []string{"2022"},
			rows: [][]any{
				{"Наименование", "на 12 января**"},
			},
			want: "0 точек",
		},
		{
			name:   "в книге нет листов года",
			sheets: []string{"Содержание"},
			rows: [][]any{
				{"Еженедельные индексы потребительских цен"},
			},
			want: "0 точек",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTestWorkbook(t, tt.sheets...)
			for _, sheet := range tt.sheets {
				setSheetRows(t, f, sheet, tt.rows)
			}
			rec := &appendRecorder{}
			err := ipcWeeksImport(f, rec)
			if err == nil {
				t.Fatalf("ожидалась ошибка, получено %d строк", len(rec.rows))
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("в ошибке нет %q: %v", tt.want, err)
			}
			if !strings.Contains(err.Error(), rosstatIPCWeeksTable) {
				t.Errorf("в ошибке нет имени таблицы %s: %v", rosstatIPCWeeksTable, err)
			}
			if len(rec.rows) != 0 {
				t.Errorf("при ошибке в батч записано %d строк, ожидалось 0", len(rec.rows))
			}
		})
	}
}

// TestIpcWeeksFindLink — селектор ссылки проверяется на HTML-фикстуре страницы цен тем же
// util.FindLink, что и в Import() (§11.3): прежняя позиционная CSS-цепочка из номеров
// вложенных элементов ломалась от любой правки вёрстки, а имя файла (Nedel_ipc.xlsx)
// постоянное, поэтому селектор — по фрагменту href.
func TestIpcWeeksFindLink(t *testing.T) {
	page, err := os.ReadFile("testdata/price_page.html")
	if err != nil {
		t.Fatal(err)
	}
	href, err := util.FindLink(page, ipcWeeksLinkSelector)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(href, "Nedel_ipc") {
		t.Errorf("href %q не содержит Nedel_ipc", href)
	}
}
