package bank

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// Фикстура — живой файл книги Сбера «sber_finansovie_rezultaty_ras_2022-2024.xlsx» (скачан
// 2026-10-17, 56 947 Б): коммитится как есть — книга в десятки килобайт (§11.2), урезания не
// требует. Разбор проверяется офлайн на ней (§11.3).
const sberFinFixture = "sber_finansovie_rezultaty.xlsx"

// Даты книги (разведка живого файла 2026-10-17, выкладка sheet1/sheet2/sheet4.xml + styles.xml).
// Листы «Баланс» и «Капитал» несут колонки-остатки серийными номерами Excel (44926…46203,
// числовой формат mm-dd-yy) — это месячные концы 2022-12-31…2026-06-30, 43 колонки. Листы
// «Отчет о прибылях и убытках» и «Показатели» — месячные подписи («янв23»…«июн26»), 42 колонки;
// дата точки — последний день месяца подписи.
var (
	sberFixtureDec22 = time.Date(2022, time.December, 31, 0, 0, 0, 0, time.UTC)
	sberFixtureJan23 = time.Date(2023, time.January, 31, 0, 0, 0, 0, time.UTC)
	sberFixtureDec25 = time.Date(2025, time.December, 31, 0, 0, 0, 0, time.UTC)
	sberFixtureMay26 = time.Date(2026, time.May, 31, 0, 0, 0, 0, time.UTC)
	sberFixtureJun26 = time.Date(2026, time.June, 30, 0, 0, 0, 0, time.UTC)
)

// sberFixtureBalanceSeries — 13 рядов листа «Баланс» (строки 3–10, 12–15, 17 источника, млрд ₽).
// Имена — подписи источника дословно, включая хвостовой пробел у двух подписей: ровно те же 13
// имён лежат в таблице базовой линии от 2026-10-16 (сверка `make ch-sql`: длины 'Кредитные карты
// и овердрафты ' и 'Резервы под кредитные убытки ' — 54 байта, то есть с пробелом), и нормализация
// имени в разборе создала бы при повторном импорте второй ряд вместо обновления первого (§15.2).
var sberFixtureBalanceSeries = []string{
	"Активы",
	"Кредиты и авансы клиентам (нетто)",
	"Кредиты корпоративным клиентам (гросс)",
	"Кредиты физическим лицам (гросс)",
	"Жилищное кредитование",
	"Потребительские и прочие ссуды",
	"Кредитные карты и овердрафты ",
	"Резервы под кредитные убытки ",
	"Обязательства",
	"Средства клиентов",
	"Средства корпоративных клиентов",
	"Средства физических лиц",
	"Собственные средства",
}

// sberFixturePnlSeries — 7 рядов листа «Отчет о прибылях и убытках» (строки 3, 5–10 источника,
// млрд ₽; порядок источника). В подписи «Расходы на резервы…» — двойной пробел источника.
var sberFixturePnlSeries = []string{
	"Прибыль за период",
	"Чистые процентные доходы",
	"Чистые комиссионные доходы",
	"Расходы на резервы и переоценка кредитов, оцениваемых  по справедливой стоимости",
	"Операционные расходы",
	"Прибыль до налогообложения",
	"Расход по налогу на прибыль",
}

// sberFixtureCapitalSeries — 6 рядов листа «Капитал»: строки капитала (млрд ₽, строки 3–5) и
// нормативы достаточности (доли: 0.146 = 14,6 %, строки 8–10).
var sberFixtureCapitalSeries = []string{
	"Базовый капитал",
	"Основной капитал",
	"Общий капитал",
	"Достаточность базового капитала Н1.1",
	"Достаточность основного капитала Н1.2",
	"Достаточность общего капитала Н1.0",
}

// sberFixtureRatioSeries — 4 ряды листа «Показатели» (доли; строки 3, 4, 6, 8 источника). Блок
// строки 10 («1М23»…«6М26») — накопительный: его значения (строка 11, «Стоимость кредитного
// риска…») считаются с начала года, а не за месяц, и точками не являются.
var sberFixtureRatioSeries = []string{
	"Рентабельность капитала",
	"Рентабельность активов",
	"Доля просроченной задолженности",
	"Отношение операционных расходов к доходам",
}

// sberOpenFixture открывает книгу-фикстуру: разбор получает *excelize.File — тот же вход, что у
// ImportFunc шаблона (стандарт §6.1), поэтому тест открывает файл сам и в сеть не ходит (§11.3).
func sberOpenFixture(t *testing.T) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile("testdata/" + sberFinFixture)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", sberFinFixture, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// sberRenameSheet — переименование листа копии фикстуры: ветка «в книге нет листа» проверяется на
// живой разметке остальных листов, а не на синтетической книге (§11.1).
func sberRenameSheet(t *testing.T, f *excelize.File, from, to string) {
	t.Helper()
	if err := f.SetSheetName(from, to); err != nil {
		t.Fatalf("SetSheetName(%q, %q): %v", from, to, err)
	}
}

// sberSetCell — правка ячейки копии фикстуры для негативных веток: ошибку excelize глотать
// нельзя, иначе тест проверит немутнутый лист.
func sberSetCell(t *testing.T, f *excelize.File, sheet, cell string, value any) {
	t.Helper()
	if err := f.SetCellValue(sheet, cell, value); err != nil {
		t.Fatalf("SetCellValue(%s!%s): %v", sheet, cell, err)
	}
}

// sberSeriesNames — имена рядов разбора без повторов, в порядке первого появления.
func sberSeriesNames(points []sberFinPoint) []string {
	var names []string
	seen := map[string]bool{}
	for _, p := range points {
		if !seen[p.Series] {
			seen[p.Series] = true
			names = append(names, p.Series)
		}
	}
	return names
}

// sberValue — значение точки (series, date); отсутствие точки — фатальная ошибка теста.
func sberValue(t *testing.T, points []sberFinPoint, series string, date time.Time) float64 {
	t.Helper()
	for _, p := range points {
		if p.Series == series && p.Date.Equal(date) {
			return p.Value
		}
	}
	t.Fatalf("в разборе нет точки %q за %s", series, date.Format(sberFinTimeLayout))
	return 0
}

// TestParseSberFinBook: книга даёт точки четырёх листов — «Баланс» (13 рядов × 43 колонки-даты,
// разбор прежней семантики), «Отчет о прибылях и убытках» (7 рядов × 42 месячные колонки),
// «Капитал» (6 × 43) и «Показатели» (4 × 42). Числа ожиданий — из независимой выкладки листов
// (unzip sharedStrings.xml + styles.xml + worksheets, 2026-10-17), а не из реализации.
// Накопительные колонки листов P&L («6М25»/«6М26») и колонки-годы («2023»–«2025») точками не
// являются: ключ таблицы (name, date) держит одно значение на месяц, а накопительный итог за тот
// же месяц затёр бы месячное значение (решение задачи 3 по листу P&L ВТБ).
func TestParseSberFinBook(t *testing.T) {
	points, err := parseSberFinBook(sberOpenFixture(t))
	if err != nil {
		t.Fatalf("parseSberFinBook: %v", err)
	}
	if len(points) != 1279 {
		t.Errorf("точек %d, want 1279 (559 баланса + 294 P&L + 258 капитала + 168 коэффициентов)", len(points))
	}
	// Ряды — обе стороны: ни пропущенных, ни лишних (13 + 7 + 6 + 4 = 30).
	want := slices.Clone(sberFixtureBalanceSeries)
	want = append(want, sberFixturePnlSeries...)
	want = append(want, sberFixtureCapitalSeries...)
	want = append(want, sberFixtureRatioSeries...)
	got := sberSeriesNames(points)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("ряды разбора %q, want %q", got, want)
	}
	// Ключ таблицы — (name, date): дубль в одном разборе означал бы, что один и тот же месяц
	// приехал дважды (например, месячная колонка и накопительная колонка-блок одной подписи) —
	// ReplacingMergeTree оставил бы из них одно значение молча. Тест ловит дубль до записи.
	seen := map[string]bool{}
	for _, p := range points {
		key := p.Series + "|" + p.Date.Format(sberFinTimeLayout)
		if seen[key] {
			t.Errorf("точка %q за %s пришла дважды (ключ (name, date) не переживёт дубля)",
				p.Series, p.Date.Format(sberFinTimeLayout))
		}
		seen[key] = true
	}
	// Накопительного блока листа «Показатели» в точках нет: его значения («Стоимость кредитного
	// риска…» под шапкой «1М23») считаются с начала года, а не за месяц.
	for _, p := range points {
		if strings.HasPrefix(p.Series, "Стоимость кредитного риска") {
			t.Errorf("ряд %q за %s: значения накопительного блока точками не являются", p.Series, p.Date.Format(sberFinTimeLayout))
		}
	}
	// Даты — по блокам: листы с серийными колонками («Баланс», «Капитал») несут 43 месячных
	// остатка, листы с месячными подписями («Отчет о прибылях и убытках», «Показатели») — 42; и
	// каждая точка — последний день своего месяца (день после неё — первое число).
	datesPerSeries := map[string]map[string]bool{}
	for _, p := range points {
		if p.Date.AddDate(0, 0, 1).Day() != 1 {
			t.Errorf("ряд %q: дата %s — не последний день месяца", p.Series, p.Date.Format("2006-01-02"))
		}
		if datesPerSeries[p.Series] == nil {
			datesPerSeries[p.Series] = map[string]bool{}
		}
		datesPerSeries[p.Series][p.Date.Format("2006-01-02")] = true
	}
	for series, dates := range datesPerSeries {
		want := 42
		if slices.Contains(sberFixtureBalanceSeries, series) || slices.Contains(sberFixtureCapitalSeries, series) {
			want = 43
		}
		if len(dates) != want {
			t.Errorf("ряд %q: дат %d, want %d", series, len(dates), want)
		}
	}
	// Spot-значения четырёх листов (стандарт §11.5): остатки баланса, статья P&L, капитал и
	// норматив Н1.0, коэффициенты. Значение первой точки P&L — за январь 2023 («янв23»), а не
	// накопительное «1М23» из блока коэффициентов.
	for _, spot := range []struct {
		series string
		date   time.Time
		want   float64
	}{
		{series: "Активы", date: sberFixtureDec22, want: 40622.1},
		{series: "Активы", date: sberFixtureMay26, want: 66227.2},
		{series: "Активы", date: sberFixtureJun26, want: 65355.1},
		{series: "Собственные средства", date: sberFixtureJun26, want: 9113.5},
		{series: "Средства физических лиц", date: sberFixtureJun26, want: 34221.5},
		{series: "Кредитные карты и овердрафты ", date: sberFixtureDec22, want: 1113},
		{series: "Резервы под кредитные убытки ", date: sberFixtureDec22, want: -1672.9},
		{series: "Прибыль за период", date: sberFixtureJan23, want: 110.1},
		{series: "Прибыль за период", date: sberFixtureDec25, want: 114.5},
		{series: "Прибыль за период", date: sberFixtureJun26, want: 168.1},
		{series: "Чистые процентные доходы", date: sberFixtureJan23, want: 173.8},
		{series: "Расход по налогу на прибыль", date: sberFixtureJan23, want: -25.8},
		{series: "Общий капитал", date: sberFixtureJun26, want: 8727.1},
		{series: "Достаточность базового капитала Н1.1", date: sberFixtureDec22, want: 0.13},
		{series: "Достаточность общего капитала Н1.0", date: sberFixtureJun26, want: 0.146},
		{series: "Рентабельность капитала", date: sberFixtureJan23, want: 0.223},
		{series: "Рентабельность активов", date: sberFixtureJun26, want: 0.031},
		{series: "Отношение операционных расходов к доходам", date: sberFixtureJan23, want: 0.241},
	} {
		if got := sberValue(t, points, spot.series, spot.date); math.Abs(got-spot.want) > sberSpotEps {
			t.Errorf("%q за %s = %v, want %v (значение ячейки листа)",
				spot.series, spot.date.Format(sberFinTimeLayout), got, spot.want)
		}
	}
}

// TestParseSberFinBookFailFast: ветки, где разбор обязан вернуть ошибку, а не «0 точек»
// (стандарт §6.4): нет любого из четырёх листов, нет шапки, нет колонок с датами, подпись колонки
// не распознана, значение не число, ячейка «NaN» (ParseFloat принимает её без ошибки, а в
// Float32-колонку она уехала бы порчей), значения без подписи ряда и лист без точек. Мутации — на
// копии живой фикстуры в памяти (§11.1), а текст ошибки обязан называть таблицу (§6.4).
func TestParseSberFinBookFailFast(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(t *testing.T, f *excelize.File)
		want   string
	}{
		"нет листа «Баланс»": {
			mutate: func(t *testing.T, f *excelize.File) {
				sberRenameSheet(t, f, sberFinBalanceSheet, "Активы и пассивы")
			},
			want: sberFinBalanceSheet,
		},
		"нет листа «Отчет о прибылях и убытках»": {
			mutate: func(t *testing.T, f *excelize.File) { sberRenameSheet(t, f, sberFinPnlSheet, "ОПиУ") },
			want:   sberFinPnlSheet,
		},
		"нет листа «Капитал»": {
			mutate: func(t *testing.T, f *excelize.File) { sberRenameSheet(t, f, sberFinCapitalSheet, "Нормативы") },
			want:   sberFinCapitalSheet,
		},
		"нет листа «Показатели»": {
			mutate: func(t *testing.T, f *excelize.File) { sberRenameSheet(t, f, sberFinRatiosSheet, "Метрики") },
			want:   sberFinRatiosSheet,
		},
		"нет шапки листа": {
			mutate: func(t *testing.T, f *excelize.File) { sberSetCell(t, f, sberFinBalanceSheet, "B2", "Статья") },
			want:   "строка шапки",
		},
		"в шапке нет колонок с датами": {
			mutate: func(t *testing.T, f *excelize.File) { sberClearRow(t, f, sberFinBalanceSheet, 2, 3, 45) },
			want:   "колонок с датами",
		},
		"подпись колонки не распознана": {
			mutate: func(t *testing.T, f *excelize.File) { sberSetCell(t, f, sberFinBalanceSheet, "C2", "31-12-22") },
			want:   "не распознана",
		},
		"месяц подписи не распознан": {
			mutate: func(t *testing.T, f *excelize.File) { sberSetCell(t, f, sberFinRatiosSheet, "C2", "абв23") },
			want:   "не распознана",
		},
		"значения без подписи ряда": {
			mutate: func(t *testing.T, f *excelize.File) { sberSetCell(t, f, sberFinBalanceSheet, "B3", " ") },
			want:   "без подписи",
		},
		"значение не число": {
			mutate: func(t *testing.T, f *excelize.File) { sberSetCell(t, f, sberFinBalanceSheet, "C3", "н/д") },
			want:   "не число",
		},
		"значение не конечно": {
			mutate: func(t *testing.T, f *excelize.File) { sberSetCell(t, f, sberFinBalanceSheet, "C3", "NaN") },
			want:   "не конечно",
		},
		"лист без точек": {
			mutate: func(t *testing.T, f *excelize.File) {
				for _, row := range []int{3, 4, 6, 8} {
					sberClearRow(t, f, sberFinRatiosSheet, row, 3, 44)
				}
			},
			want: "ни одной точки",
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := sberOpenFixture(t)
			tc.mutate(t, f)
			_, err := parseSberFinBook(f)
			if err == nil {
				t.Fatal("ожидалась ошибка, а разбор прошёл")
			}
			// Ожидания — литералами, не константами импортёра (§8.5): ссылка на константу
			// сделала бы проверку тавтологичной и прошла бы ровно на той опечатке, от которой
			// обязана падать.
			for _, want := range []string{"sber_finansovie_rezultaty", tc.want} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("ошибка %q не называет %q", err, want)
				}
			}
		})
	}
}

// sberClearRow — обнуление ячеек строки листа: значение « » читается пустым (разбор снимает
// пробелы), поэтому точка из такой ячейки не появится.
func sberClearRow(t *testing.T, f *excelize.File, sheet string, row, col0, col1 int) {
	t.Helper()
	for col := col0; col <= col1; col++ {
		name, err := excelize.ColumnNumberToName(col)
		if err != nil {
			t.Fatalf("excelize.ColumnNumberToName(%d): %v", col, err)
		}
		sberSetCell(t, f, sheet, fmt.Sprintf("%s%d", name, row), " ")
	}
}

// TestAppendSberFinPoints: запись идёт по одному Append на точку в форме (Series, Date, Value) —
// схема таблицы (name, date, balance) — и значение приводится к точности Float32-колонки
// (legacy-схема, спека §4.5). Ошибка Append не проглатывается (§5.5): расхождение схемы обязано
// вернуться вызывающему, а не пропасть в логе.
func TestAppendSberFinPoints(t *testing.T) {
	points := []sberFinPoint{
		{Series: "Прибыль за период", Date: sberFixtureJan23, Value: 110.1},
		{Series: "Активы", Date: sberFixtureDec22, Value: 40622.1},
	}
	rec := newAppendRecorder("name", "date", "balance")
	if err := appendSberFinPoints(rec, points); err != nil {
		t.Fatalf("appendSberFinPoints: %v", err)
	}
	if len(rec.rows) != len(points) {
		t.Fatalf("записано строк %d, want %d", len(rec.rows), len(points))
	}
	for i, want := range points {
		got := rec.rows[i]
		if got.name != want.Series || !got.date.Equal(want.Date) || got.value != float64(float32(want.Value)) {
			t.Errorf("строка %d = %+v, want (Series, Date, Float32-значение) = %q, %s, %v",
				i, got, want.Series, want.Date.Format(sberFinTimeLayout), float64(float32(want.Value)))
		}
	}
	// Чужая схема приёмника — ошибка, которую разбор обязан вернуть вызывающему.
	err := appendSberFinPoints(newAppendRecorder("name", "date"), points)
	if err == nil {
		t.Fatal("Append с чужой схемой: ожидалась ошибка, а она проглочена")
	}
	if !strings.Contains(err.Error(), "Прибыль за период") {
		t.Errorf("ошибка %q не называет точку, на которой запись остановилась", err)
	}
}

// TestSberFinImportFunc: ImportFunc шаблона — книга из памяти → точки → батч; ошибку разбора он не
// оборачивает вторым префиксом (parseSberFinBook уже называет таблицу), а ошибка записи
// возвращается вызывающему.
func TestSberFinImportFunc(t *testing.T) {
	rec := newAppendRecorder("name", "date", "balance")
	if err := sberFinImportFunc(sberOpenFixture(t), rec); err != nil {
		t.Fatalf("sberFinImportFunc: %v", err)
	}
	if len(rec.rows) != 1279 {
		t.Errorf("записано строк %d, want 1279", len(rec.rows))
	}
	// Книга без листа «Баланс»: ошибка ImportFunc называет таблицу и лист ровно один раз.
	f := sberOpenFixture(t)
	sberRenameSheet(t, f, sberFinBalanceSheet, "Активы и пассивы")
	err := sberFinImportFunc(f, newAppendRecorder("name", "date", "balance"))
	if err == nil {
		t.Fatal("книга без листа «Баланс»: ожидалась ошибка, а разбор прошёл")
	}
	if got := strings.Count(err.Error(), "sber_finansovie_rezultaty"); got != 1 {
		t.Errorf("в ошибке %q имя таблицы %d раз, want 1 (второй префикс — дубль)", err, got)
	}
}

// TestSberFinImporterOnTemplate: импортёр стоит на шаблонном пути (ImportFunc, не CrawFunc) —
// следствие ImportFunc: тот же срез книги, что разобран, уходит в винтажный архив версии
// (стандарт §9.6); DataUrl статичен, точка контроля публикации сохранена литералом.
func TestSberFinImporterOnTemplate(t *testing.T) {
	if sberFinRezImporter.CrawFunc != nil {
		t.Error("у импортёра остался CrawFunc: байты должен качать шаблон — иначе версия в архив не попадает")
	}
	if sberFinRezImporter.ImportFunc == nil {
		t.Error("у импортёра нет ImportFunc: путь разбора книги не подключён")
	}
	if got := sberFinRezImporter.DataUrl; got != "https://www.sberbank.com/common/img/uploaded/redirected/com/investor-relations/groupresults/sber_finansovie_rezultaty_ras_2022-2024.xlsx" {
		t.Errorf("DataUrl = %q, want адрес файла источника (статичная ссылка)", got)
	}
	if got := sberFinRezImporter.PublicationCountPoints; got != "SELECT count() FROM sber_finansovie_rezultaty FINAL" {
		t.Errorf("PublicationCountPoints = %q, want литерал чека версии (с FINAL)", got)
	}
}

// TestSberFinDdlShape: DDL — %s-форма (стандарт §5.1): шаблон исполняет её как
// fmt.Sprintf(sql, TableName), поэтому имя таблицы в литерале обязано быть ровно одно и только в
// позиции подстановки. Литерал без %s дал бы «%!(EXTRA string=…)» и ошибку ClickHouse про
// multi-statement, а имя в литерале — двойное имя таблицы в запросе.
func TestSberFinDdlShape(t *testing.T) {
	if got := strings.Count(sberRpbuDataUrlDdl, "%s"); got != 1 {
		t.Errorf("в sberRpbuDataUrlDdl %d вхождений %%s, want 1 (имя таблицы подставляет шаблон):\n%s", got, sberRpbuDataUrlDdl)
	}
	if strings.Contains(sberRpbuDataUrlDdl, sberRpbuDataUrlTable) {
		t.Errorf("sberRpbuDataUrlDdl содержит литерал имени таблицы %q — имя приходит подстановкой:\n%s",
			sberRpbuDataUrlTable, sberRpbuDataUrlDdl)
	}
}
