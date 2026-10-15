package cbr

import (
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/xuri/excelize/v2"
)

// Разбор книги cbr_dip_bulletin проверяется на трёх живых выпусках (спека §3.8): современная
// вёрстка (26-06: лист SAAR и лист безработицы), свежий выпуск без листа безработицы (26-07:
// лист занятости вместо него) и старая вёрстка (2021-06: шапка без ячейки «Дата» и сноска
// «Источник: Росстат, расчет ДИП» ниже данных). Фикстуры — урезанные zip-хирургией реальные
// файлы ЦБ: разметка, шапки и значения в них те же, что у источника (стандарт §11.1–11.2).
const (
	dipFixture2606 = "testdata/dip_bulletin_26-06.xlsx"
	dipFixture2607 = "testdata/dip_bulletin_26-07.xlsx"
	dipFixture2106 = "testdata/dip_bulletin_2021-06.xlsx"
)

// dipFixtureReleases — выпуски фикстур: id, адрес и дата публикации взяты со живой страницы
// (разбор страницы — cbr/dip_page.go, тест — dip_page_test.go), а не выдуманы. Разбору книги они
// нужны только для сообщений, но тест обязан подавать то, что подаст импортёр.
var dipFixtureReleases = map[string]dipRelease{
	dipFixture2606: {
		ID:              "62343",
		URL:             dipUrlPrefix + "/Collection/Collection/File/62343/bulletin_26-06.xlsx",
		PublicationDate: dipDate(2026, time.September, 1),
		Release:         "26-06",
	},
	dipFixture2607: {
		ID:              "62444",
		URL:             dipUrlPrefix + "/Collection/Collection/File/62444/bulletin_26-07.xlsx",
		PublicationDate: dipDate(2026, time.October, 14),
		Release:         "26-07",
	},
	dipFixture2106: {
		ID:              "36593",
		URL:             dipUrlPrefix + "/Collection/Collection/File/36593/stat_bulletin_21-06.xlsx",
		PublicationDate: dipDate(2021, time.September, 1),
		Release:         "21-06",
	},
}

// dipDate — дата в UTC: ожидания выписаны литералами из файлов источника.
func dipDate(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// dipLastDay — последний день месяца: сутки назад от первого числа следующего месяца. Правило
// привязки даты (спека §3.3) повторено в тесте намеренно: ожидания не должны считаться тем же
// кодом, что разбирает книгу.
func dipLastDay(year int, month time.Month) time.Time {
	return time.Date(year, month+1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
}

// dipMonthEnds — последние дни месяцев подряд, начиная с месяца даты first: ряд источника идёт
// месяцами без пропусков (спека §3.4), и проверка полной последовательности ловит потерянную или
// сдвинутую строку, которую первый/последний спот не заметили бы.
func dipMonthEnds(first time.Time, count int) []time.Time {
	dates := make([]time.Time, 0, count)
	for i := 0; i < count; i++ {
		month := time.Date(first.Year(), first.Month()+time.Month(i), 1, 0, 0, 0, 0, time.UTC)
		dates = append(dates, dipLastDay(month.Year(), month.Month()))
	}
	return dates
}

// dipOpenFixture — копия живой фикстуры в памяти под мутацию: ветки fail-fast и семантика ячеек
// проверяются на ней, а не на синтетической книге (стандарт §11.1). Правки идут по копии в
// памяти — файл на диске не меняется.
func dipOpenFixture(t *testing.T, file string, mutate func(t *testing.T, f *excelize.File)) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile(file)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", file, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	mutate(t, f)
	return f
}

// dipSetCell — правка ячейки мутируемой фикстуры: excelize сообщает о негодной правке ошибкой, и
// проглотить её значило бы проверять немодифицированный лист.
func dipSetCell(t *testing.T, f *excelize.File, sheet, cell string, value any) {
	t.Helper()
	if err := f.SetCellValue(sheet, cell, value); err != nil {
		t.Fatalf("SetCellValue(%s!%s, %v): %v", sheet, cell, value, err)
	}
}

// dipCellName — адрес ячейки по колонке и строке (1-based) — так же, как адресует ячейки разбор.
func dipCellName(t *testing.T, column, row int) string {
	t.Helper()
	cell, err := excelize.CoordinatesToCellName(column, row)
	if err != nil {
		t.Fatalf("CoordinatesToCellName(%d, %d): %v", column, row, err)
	}
	return cell
}

// dipParseFixture — разбор фикстуры тем же кодом, что и импортёр: ошибка и пустой разбор —
// падение теста, потому что фикстура и есть контракт разбора (стандарт §11.1).
func dipParseFixture(t *testing.T, file string) []cbrDipPoint {
	t.Helper()
	release, ok := dipFixtureReleases[file]
	if !ok {
		t.Fatalf("для фикстуры %s не объявлен выпуск", file)
	}
	f, err := excelize.OpenFile(file)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", file, err)
	}
	defer func() { _ = f.Close() }()
	points, err := cbrDipParseBook(f, release)
	if err != nil {
		t.Fatalf("разбор фикстуры %s: %v", file, err)
	}
	return points
}

// dipGroupPoints — точки по рядам: состав и порядок рядов проверяются отдельно от чтения.
func dipGroupPoints(points []cbrDipPoint) map[string][]cbrDipPoint {
	bySeries := make(map[string][]cbrDipPoint)
	for _, point := range points {
		bySeries[point.Series] = append(bySeries[point.Series], point)
	}
	return bySeries
}

// dipSpotValue — значение точки (ряд, дата) в разборе; отсутствие точки фатально: спот,
// которого нет, ничего не проверяет.
func dipSpotValue(t *testing.T, points []cbrDipPoint, series string, date time.Time) float64 {
	t.Helper()
	for _, point := range points {
		if point.Series == series && point.Date.Equal(date) {
			return point.Value
		}
	}
	t.Fatalf("в разборе нет точки (%q, %s)", series, date.Format("2006-01-02"))
	return 0
}

// dipCheckSpot — сверка одного значения с литералом из файла источника. Допуск берётся общий с
// платёжным балансом (cbrSpotEqualFloat64, 1e-9): xlsx хранит число десятичной записью, и
// ParseFloat даёт то же ближайшее float64, что и литерал теста.
func dipCheckSpot(t *testing.T, points []cbrDipPoint, series string, date time.Time, want float64) {
	t.Helper()
	got := dipSpotValue(t, points, series, date)
	if !cbrSpotEqualFloat64(got, want) {
		t.Errorf("точка (%q, %s) = %v, want %v (значение из файла источника)",
			series, date.Format("2006-01-02"), got, want)
	}
}

// dipWarnHook перехватывает предупреждения logrus на время теста: «предупредить и пропустить» —
// контракт с оператором (стандарт §6.6, §12.3), и тест обязан видеть, что разбор предупредил, а
// не просто промолчал.
func dipWarnHook(t *testing.T) *test.Hook {
	t.Helper()
	hook := test.NewGlobal()
	t.Cleanup(func() { log.StandardLogger().ReplaceHooks(make(log.LevelHooks)) })
	return hook
}

// dipWarnMessages — сообщения уровня warn, собранные хуком.
func dipWarnMessages(hook *test.Hook) []string {
	var messages []string
	for _, entry := range hook.AllEntries() {
		if entry.Level == log.WarnLevel {
			messages = append(messages, entry.Message)
		}
	}
	return messages
}

// dipAssertWarn — хотя бы одно предупреждение содержит каждую из ожидаемых подстрок.
func dipAssertWarn(t *testing.T, hook *test.Hook, wants ...string) {
	t.Helper()
	messages := dipWarnMessages(hook)
	if len(messages) == 0 {
		t.Fatalf("предупреждений нет, want сообщение про %v", wants)
	}
	for _, want := range wants {
		found := false
		for _, message := range messages {
			if strings.Contains(message, want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("ни одно предупреждение не содержит %q (сообщения: %v)", want, messages)
		}
	}
}

// dipWantSeries — литерал объявленного ряда для сверки реестра.
type dipWantSeries struct {
	name   string
	labels []string
}

// dipWantSheet — литерал объявленного листа: варианты заголовка A1 и ряды его колонок (спека
// §3.2). Литералы, а не ссылки на реестр: сверка со спекой — единственное место, где видно
// переименование ряда или подписи, а не код, читающий сам себя.
type dipWantSheet struct {
	titles []string
	series []dipWantSeries
}

// dipSeriesWant — ожидание по ряду фикстуры: число точек и крайние даты.
type dipSeriesWant struct {
	points int
	first  time.Time
	last   time.Time
}

// dipSpot — спот-значение выпуска: точка, которую тест сверяет с литералом из файла источника.
type dipSpot struct {
	series string
	date   time.Time
	value  float64
}

// dipVintage — выпуск-фикстура и ожидания его разбора: состав рядов (ровно он и без лишних),
// число точек, крайние даты и спот-значения.
type dipVintage struct {
	file   string
	total  int
	series map[string]dipSeriesWant
	spots  []dipSpot
}

// dipVintages — три живых выпуска. 26-06: современная вёрстка, SAAR 2021-01 … 2026-07 (67
// точек) и безработица 2015-01 … 2026-06 (138); 26-07: SAAR 2021-01 … 2026-09 (69) и занятость
// 2017-01 … 2026-08 (116), листа безработицы нет; 2021-06: старая вёрстка, SAAR 2017-01 …
// 2021-07 (55) и безработица 2015-01 … 2021-06 (78).
var dipVintages = []dipVintage{
	{
		file:  dipFixture2606,
		total: 544,
		series: map[string]dipSeriesWant{
			"saar_all_goods":   {points: 67, first: dipDate(2021, time.January, 31), last: dipDate(2026, time.July, 31)},
			"saar_food":        {points: 67, first: dipDate(2021, time.January, 31), last: dipDate(2026, time.July, 31)},
			"saar_nonfood":     {points: 67, first: dipDate(2021, time.January, 31), last: dipDate(2026, time.July, 31)},
			"saar_services":    {points: 67, first: dipDate(2021, time.January, 31), last: dipDate(2026, time.July, 31)},
			"unemployment_nsa": {points: 138, first: dipDate(2015, time.January, 31), last: dipDate(2026, time.June, 30)},
			"unemployment_sa":  {points: 138, first: dipDate(2015, time.January, 31), last: dipDate(2026, time.June, 30)},
		},
		spots: []dipSpot{
			{"saar_all_goods", dipDate(2021, time.January, 31), 4.781574875641297},
			{"saar_food", dipDate(2021, time.January, 31), 3.7864708443726727},
			{"saar_all_goods", dipDate(2026, time.July, 31), 11.591788035567617},
			{"unemployment_nsa", dipDate(2015, time.January, 31), 5.455578448855274},
			{"unemployment_nsa", dipDate(2021, time.January, 31), 5.767823278912974},
			{"unemployment_sa", dipDate(2015, time.January, 31), 5.233359},
			{"unemployment_sa", dipDate(2026, time.June, 30), 2.239751},
		},
	},
	{
		file:  dipFixture2607,
		total: 508,
		series: map[string]dipSeriesWant{
			"saar_all_goods": {points: 69, first: dipDate(2021, time.January, 31), last: dipDate(2026, time.September, 30)},
			"saar_food":      {points: 69, first: dipDate(2021, time.January, 31), last: dipDate(2026, time.September, 30)},
			"saar_nonfood":   {points: 69, first: dipDate(2021, time.January, 31), last: dipDate(2026, time.September, 30)},
			"saar_services":  {points: 69, first: dipDate(2021, time.January, 31), last: dipDate(2026, time.September, 30)},
			"employment_nsa": {points: 116, first: dipDate(2017, time.January, 31), last: dipDate(2026, time.August, 31)},
			"employment_sa":  {points: 116, first: dipDate(2017, time.January, 31), last: dipDate(2026, time.August, 31)},
		},
		spots: []dipSpot{
			{"saar_all_goods", dipDate(2021, time.January, 31), 4.786428626998557},
			{"saar_all_goods", dipDate(2026, time.July, 31), 9.998797020040357},
			{"employment_nsa", dipDate(2017, time.January, 31), 59.1},
			{"employment_sa", dipDate(2017, time.January, 31), 59.67240922577325},
			{"employment_nsa", dipDate(2021, time.January, 31), 58.4},
			{"employment_sa", dipDate(2021, time.January, 31), 58.875962309501496},
		},
	},
	{
		file:  dipFixture2106,
		total: 376,
		series: map[string]dipSeriesWant{
			"saar_all_goods":   {points: 55, first: dipDate(2017, time.January, 31), last: dipDate(2021, time.July, 31)},
			"saar_food":        {points: 55, first: dipDate(2017, time.January, 31), last: dipDate(2021, time.July, 31)},
			"saar_nonfood":     {points: 55, first: dipDate(2017, time.January, 31), last: dipDate(2021, time.July, 31)},
			"saar_services":    {points: 55, first: dipDate(2017, time.January, 31), last: dipDate(2021, time.July, 31)},
			"unemployment_nsa": {points: 78, first: dipDate(2015, time.January, 31), last: dipDate(2021, time.June, 30)},
			"unemployment_sa":  {points: 78, first: dipDate(2015, time.January, 31), last: dipDate(2021, time.June, 30)},
		},
		spots: []dipSpot{
			{"saar_all_goods", dipDate(2017, time.January, 31), 4.49},
			{"unemployment_nsa", dipDate(2015, time.January, 31), 5.4891410816903825},
			{"unemployment_sa", dipDate(2015, time.January, 31), 5.27924038461241},
			{"unemployment_nsa", dipDate(2021, time.June, 30), 4.7518965200418766},
			{"unemployment_sa", dipDate(2021, time.June, 30), 4.9346175550654197},
		},
	},
}

// dipSameStrings — поэлементное сравнение двух списков строк.
func dipSameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestDipSeriesRegistry: объявленный реестр совпадает со спекой §3.2 дословно — три листа,
// восемь рядов, варианты заголовков A1 и подписей колонок в объявленном порядке. Имена рядов
// объявлены кодом (стандарт §6.8), поэтому ошибка в этой таблице создала бы ряд-двойник или
// оборвала ряд на смене вёрстки — а разбор при этом остался бы зелёным.
func TestDipSeriesRegistry(t *testing.T) {
	want := []dipWantSheet{
		{
			titles: []string{"Сезонно сглаженный рост цен, % м/м SAAR", "Сезонно сглаженный рост цен, % SAAR"},
			series: []dipWantSeries{
				{"saar_all_goods", []string{"Все товары и услуги"}},
				{"saar_food", []string{"Продовольственные товары"}},
				{"saar_nonfood", []string{"Непродовольственные товары"}},
				{"saar_services", []string{"Услуги"}},
			},
		},
		{
			titles: []string{"Уровень безработицы, %"},
			series: []dipWantSeries{
				{"unemployment_sa", []string{"С корректировкой на сезонность", "Скорректированный на сезонность", "Сезонно сглаженный"}},
				{"unemployment_nsa", []string{"Без корректировки на сезонность", "Нескорректированный на сезонность", "Уровень безработицы"}},
			},
		},
		{
			titles: []string{"Уровень занятости, %"},
			series: []dipWantSeries{
				{"employment_sa", []string{"С корректировкой на сезонность", "Уровень занятости, % SA"}},
				{"employment_nsa", []string{"Без корректировки на сезонность", "Уровень занятости, %"}},
			},
		},
	}
	if len(cbrDipSheets) != len(want) {
		t.Fatalf("объявленных листов %d, want %d", len(cbrDipSheets), len(want))
	}
	names := make(map[string]bool)
	for i, sheet := range want {
		got := cbrDipSheets[i]
		if !dipSameStrings(got.Titles, sheet.titles) {
			t.Errorf("лист %d: заголовки A1 %v, want %v", i, got.Titles, sheet.titles)
		}
		if len(got.Series) != len(sheet.series) {
			t.Errorf("лист %d: рядов %d, want %d", i, len(got.Series), len(sheet.series))
			continue
		}
		for j, series := range sheet.series {
			gotSeries := got.Series[j]
			if gotSeries.Name != series.name {
				t.Errorf("лист %d, ряд %d: имя %q, want %q", i, j, gotSeries.Name, series.name)
			}
			if !dipSameStrings(gotSeries.Labels, series.labels) {
				t.Errorf("ряд %q: подписи %v, want %v", series.name, gotSeries.Labels, series.labels)
			}
			if names[gotSeries.Name] {
				t.Errorf("ряд %q объявлен дважды: имя ряда — ключ витрины, дубль слил бы две метрики", gotSeries.Name)
			}
			names[gotSeries.Name] = true
		}
	}
	if len(names) != 8 {
		t.Errorf("рядов объявлено %d, want 8 (спека §3.2)", len(names))
	}

	// Чужие метрики выпусков 22-04 и 22-05 («(без …)», «(пр. шк.)») не должны совпасть ни с одной
	// объявленной подписью: совпадение склеило бы две разные редакции метрики в один ряд (спека §3.2).
	foreign := []string{
		"Прод. (без плодоовощей)",
		"Непрод. (без нефтепродуктов и табака)",
		"Услуги (без ЖКУ)",
		"Все товары и услуги (пр. шк.)",
		"Услуги (без ЖКУ)*",
	}
	for _, sheet := range cbrDipSheets {
		for _, series := range sheet.Series {
			for _, label := range foreign {
				if cbrDipLabelMatches(label, series.Labels) {
					t.Errorf("чужая метрика %q совпала с рядом %q — метрики разных редакций слились бы", label, series.Name)
				}
			}
		}
	}
}

// TestDipLabelNormalization: подписи источника приводятся к сравнимому виду до точного сравнения
// (спека §3.2) — кавычки-ёлочки и сноски-звёздочки снимаются, переносы, неразрывные пробелы и
// тире нормализуются; чужая метрика и пустая ячейка совпадением не считаются.
func TestDipLabelNormalization(t *testing.T) {
	for name, tc := range map[string]struct {
		cell  string
		label string
		want  bool
	}{
		"ёлочки и сноска": {"«Все товары и услуги»*", "Все товары и услуги", true},
		"лапки":           {"“Услуги”", "Услуги", true},
		"перенос и неразрывный пробел": {"Продовольственные\u00a0\nтовары", "Продовольственные товары", true},
		"чужая метрика с «(без …)»":    {"Услуги (без ЖКУ)", "Услуги", false},
		"чужая метрика с «(пр. шк.)»":  {"Все товары и услуги (пр. шк.)", "Все товары и услуги", false},
		"пустая ячейка":                {"", "Услуги", false},
		"сноска без кавычек":           {"Непродовольственные товары*", "Непродовольственные товары", true},
	} {
		t.Run(name, func(t *testing.T) {
			if got := cbrDipLabelMatches(tc.cell, []string{tc.label}); got != tc.want {
				t.Errorf("cbrDipLabelMatches(%q, %q) = %v, want %v", tc.cell, tc.label, got, tc.want)
			}
		})
	}
	if !cbrDipSameLabel("Непродовольственные – товары", "Непродовольственные - товары") {
		t.Error("короткое тире источника не приравнено дефису объявленной подписи")
	}
	if !cbrDipSameLabel("Услуги", "услуги") {
		t.Error("регистр подписи значим: «Услуги» и «услуги» — одна подпись")
	}
}

// TestDipFixtureRows: разбор каждой фикстуры даёт ожидаемое число точек, ровно ожидаемый состав
// рядов (ни забытого, ни лишнего) и по каждому ряду — непрерывную последовательность последних
// дней месяцев с объявленными крайними датами (спека §3.3, §3.4).
func TestDipFixtureRows(t *testing.T) {
	for _, vintage := range dipVintages {
		t.Run(vintage.file, func(t *testing.T) {
			points := dipParseFixture(t, vintage.file)
			if len(points) != vintage.total {
				t.Errorf("точек %d, want %d", len(points), vintage.total)
			}
			bySeries := dipGroupPoints(points)
			if len(bySeries) != len(vintage.series) {
				t.Errorf("рядов %d, want %d: состав разобранных рядов разошёлся со спекой §3.2",
					len(bySeries), len(vintage.series))
			}
			for name := range vintage.series {
				got, ok := bySeries[name]
				if !ok {
					t.Errorf("ряд %q не разобран", name)
					continue
				}
				want := vintage.series[name]
				if len(got) != want.points {
					t.Errorf("ряд %q: точек %d, want %d", name, len(got), want.points)
					continue
				}
				for i, date := range dipMonthEnds(want.first, want.points) {
					if !got[i].Date.Equal(date) {
						t.Errorf("ряд %q: точка %d = %s, want %s (последний день месяца подряд, спека §3.3)",
							name, i, got[i].Date.Format("2006-01-02"), date.Format("2006-01-02"))
						break
					}
				}
				if last := got[len(got)-1].Date; !last.Equal(want.last) {
					t.Errorf("ряд %q: последняя дата %s, want %s", name, last.Format("2006-01-02"), want.last.Format("2006-01-02"))
				}
			}
			for name := range bySeries {
				if _, ok := vintage.series[name]; !ok {
					t.Errorf("ряд %q разобран, но в ожиданиях фикстуры его нет", name)
				}
			}
		})
	}
}

// TestDipSpotValues: значения из известных ячеек каждого выпуска — первые и последние точки
// рядов, SA- и NSA-колонки (спека §3.8: два-три спота на ряд). Значения литералами: сверка идёт с
// разбором, а не с константой кода.
func TestDipSpotValues(t *testing.T) {
	for _, vintage := range dipVintages {
		t.Run(vintage.file, func(t *testing.T) {
			points := dipParseFixture(t, vintage.file)
			for _, spot := range vintage.spots {
				dipCheckSpot(t, points, spot.series, spot.date, spot.value)
			}
		})
	}
}

// TestDipRevisionAcrossReleases: один и тот же месяц в двух выпусках — две разные точки с разными
// значениями (спека §1: выпуск это точка во времени, а не «последняя версия ряда»). Оба выпуска
// разбираются одним кодом, и ревизия SAAR 2026-07 видна числом, а не только текстом каталога.
func TestDipRevisionAcrossReleases(t *testing.T) {
	const series = "saar_all_goods"
	date := dipDate(2026, time.July, 31)
	julyInJune := dipSpotValue(t, dipParseFixture(t, dipFixture2606), series, date)
	julyInJuly := dipSpotValue(t, dipParseFixture(t, dipFixture2607), series, date)
	if cbrSpotEqualFloat64(julyInJune, julyInJuly) {
		t.Fatalf("значение %q за %s в выпусках 26-06 и 26-07 совпало (%v): ревизия не видна",
			series, date.Format("2006-01-02"), julyInJune)
	}
	dipCheckSpot(t, dipParseFixture(t, dipFixture2606), series, date, 11.591788035567617)
	dipCheckSpot(t, dipParseFixture(t, dipFixture2607), series, date, 9.998797020040357)
}

// TestDipTextDates: дата в колонке A читается и текстом «ГГГГ-ММ-ДД» и «ДД.ММ.ГГГГ», а не только
// серийным числом Excel (спека §3.4): подмена ячейки первого месяца обеими формами даёт те же
// 544 точки и ту же точку-спот.
func TestDipTextDates(t *testing.T) {
	release := dipFixtureReleases[dipFixture2606]
	for name, text := range map[string]string{
		"ISO":        "2021-01-01",
		"ДД.ММ.ГГГГ": "01.01.2021",
	} {
		t.Run(name, func(t *testing.T) {
			f := dipOpenFixture(t, dipFixture2606, func(t *testing.T, f *excelize.File) {
				dipSetCell(t, f, "2", "A4", text)
			})
			points, err := cbrDipParseBook(f, release)
			if err != nil {
				t.Fatalf("разбор мутированной фикстуры: %v", err)
			}
			if len(points) != 544 {
				t.Errorf("точек %d, want 544 (текстовая дата — та же точка)", len(points))
			}
			dipCheckSpot(t, points, "saar_all_goods", dipDate(2021, time.January, 31), 4.781574875641297)
		})
	}
}

// TestDipCellSemantics — семантика ячеек на мутированной копии живой фикстуры: пустая ячейка —
// не точка (стандарт §6.7), литеральный ноль — точка (спека §3.3), значение пишется float64.
// Пары проверок противопоставлены: разбор обязан различать пустое значение и ноль.
func TestDipCellSemantics(t *testing.T) {
	release := dipFixtureReleases[dipFixture2606]
	for name, tc := range map[string]struct {
		value    any
		wantRows int
		wantZero bool
	}{
		"пустая ячейка точки не даёт": {value: "", wantRows: 543},
		"литеральный ноль — точка":    {value: 0, wantRows: 544, wantZero: true},
	} {
		t.Run(name, func(t *testing.T) {
			f := dipOpenFixture(t, dipFixture2606, func(t *testing.T, f *excelize.File) {
				dipSetCell(t, f, "2", "B4", tc.value)
			})
			points, err := cbrDipParseBook(f, release)
			if err != nil {
				t.Fatalf("разбор мутированной фикстуры: %v", err)
			}
			if len(points) != tc.wantRows {
				t.Errorf("точек %d, want %d", len(points), tc.wantRows)
			}
			if !tc.wantZero {
				return
			}
			value := dipSpotValue(t, points, "saar_all_goods", dipDate(2021, time.January, 31))
			if value != 0 {
				t.Errorf("значение точки = %v, want 0 (литеральный ноль — настоящее значение)", value)
			}
		})
	}
}

// TestDipFailFastOnFixture — ветки, где разбор обязан вернуть ошибку с именем таблицы, а не «0
// точек» (стандарт §6.4, спека §3.4), — на мутированной копии живой фикстуры 26-06 (стандарт
// §11.1). Копия, а не синтетическая книга: якоря, шапка и ячейки в ней те же, что у источника,
// поэтому ошибка приходит от мутации, а не от сборки книги. Возврат nil на ошибке — контракт: при
// ошибке разбор не отдаёт ни одной точки, частично разобранный выпуск хуже пустого.
func TestDipFailFastOnFixture(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate  func(t *testing.T, f *excelize.File)
		wantErr string
	}{
		"значение не число": {
			mutate:  func(t *testing.T, f *excelize.File) { dipSetCell(t, f, "2", "B4", "н/д") },
			wantErr: "значение \"н/д\"",
		},
		"значение NaN": {
			// ParseFloat принимает «NaN» молча: точка с NaN отравила бы ряд (суммы — NaN).
			mutate:  func(t *testing.T, f *excelize.File) { dipSetCell(t, f, "2", "B4", "NaN") },
			wantErr: "не конечно",
		},
		"дубль даты": {
			// Вторая строка получает серийное число первой: (series, date) встречается дважды —
			// какая из точек настоящая, разбор не знает (спека §3.1).
			mutate:  func(t *testing.T, f *excelize.File) { dipSetCell(t, f, "2", "A5", 44197) },
			wantErr: "дубль (series, date)",
		},
		"значение ниже данных": {
			// Под последней строкой данных появляется значение в объявленной колонке: подпись
			// строки не распознана — ряд неполон, а выглядел бы полным (спека §3.4).
			mutate:  func(t *testing.T, f *excelize.File) { dipSetCell(t, f, "2", "E71", 1) },
			wantErr: "ниже последней разобранной даты",
		},
		"значение без даты": {
			// Дата первой строки данных испорчена, значения остались: ни одной даты в колонке A не
			// разобрано, и записать значения нечем — ошибка, а не молча усечённый ряд. Сообщение
			// называет причину («без даты»), а не «ниже последней даты», которой нет.
			mutate:  func(t *testing.T, f *excelize.File) { dipSetCell(t, f, "2", "A4", "итого") },
			wantErr: "без даты",
		},
		"лист с переименованными колонками": {
			// Все четыре объявленные подписи SAAR заменены на чужие метрики выпусков 22-04 и 22-05:
			// лист найден по заголовку, но не содержит ни одной объявленной подписи — «лист не то,
			// чем кажется», а не пропуск ряда (спека §3.4).
			mutate: func(t *testing.T, f *excelize.File) {
				dipSetCell(t, f, "2", "B3", "Прод. (без плодоовощей)")
				dipSetCell(t, f, "2", "C3", "Непрод. (без нефтепродуктов и табака)")
				dipSetCell(t, f, "2", "D3", "Услуги (без ЖКУ)")
				dipSetCell(t, f, "2", "E3", "Все товары и услуги (пр. шк.)")
			},
			wantErr: "нет ни одной объявленной подписи колонки",
		},
		"книга без объявленных листов": {
			// Заголовки A1 обоих листов переименованы: источник сменил разметку — ошибка до чтения
			// данных, а не «0 точек».
			mutate: func(t *testing.T, f *excelize.File) {
				dipSetCell(t, f, "2", "A1", "Приложение")
				dipSetCell(t, f, "23", "A1", "Приложение 2")
			},
			wantErr: "ни одного объявленного листа",
		},
		"книга без точек": {
			// Шапки и подписи целы, а значений нет: нулевой разбор обязан валить прогон — «0 rows» с
			// зелёным dagu оставил бы все восемь рядов без обновления (стандарт §6.4).
			mutate: func(t *testing.T, f *excelize.File) {
				for row := 4; row <= 70; row++ {
					for column := 2; column <= 5; column++ {
						dipSetCell(t, f, "2", dipCellName(t, column, row), "")
					}
				}
				for row := 4; row <= 141; row++ {
					for column := 2; column <= 3; column++ {
						dipSetCell(t, f, "23", dipCellName(t, column, row), "")
					}
				}
			},
			wantErr: "не дал ни одной точки",
		},
	} {
		t.Run(name, func(t *testing.T) {
			points, err := cbrDipParseBook(dipOpenFixture(t, dipFixture2606, tc.mutate), dipFixtureReleases[dipFixture2606])
			if err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d точек", len(points))
			}
			if !strings.Contains(err.Error(), "cbr_dip_bulletin") {
				t.Errorf("ошибка %q не называет таблицу cbr_dip_bulletin", err)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ошибка %q не содержит %q", err, tc.wantErr)
			}
			if !strings.Contains(err.Error(), "26-06") {
				t.Errorf("ошибка %q не называет выпуск 26-06 (id 62343)", err)
			}
			if len(points) != 0 {
				t.Errorf("при ошибке разбор отдал %d точек — частичный выпуск хуже пустого", len(points))
			}
		})
	}
}

// TestDipEmptyBookFails: пустая книга (лист Sheet1 без заголовка) — та же ветка «нет ни одного
// объявленного листа», что и переименованная книга: «0 rows» по нераспознанной разметке выглядел
// бы успешным прогоном (стандарт §6.4).
func TestDipEmptyBookFails(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	points, err := cbrDipParseBook(f, dipFixtureReleases[dipFixture2606])
	if err == nil {
		t.Fatalf("ожидалась ошибка, разбор дал %d точек", len(points))
	}
	if !strings.Contains(err.Error(), "cbr_dip_bulletin") {
		t.Errorf("ошибка %q не называет таблицу cbr_dip_bulletin", err)
	}
	if !strings.Contains(err.Error(), "ни одного объявленного листа") {
		t.Errorf("ошибка %q не называет причину (нет объявленных листов)", err)
	}
}

// TestDipMissingSheetWarns: в выпуске 26-07 листа безработицы нет вовсе — штатный случай спеки
// §2: разбор предупреждает и пропускает ряды этого листа, а SAAR и занятость импортируются.
// Ошибка здесь означала бы вечно красный шаг dagu и потерю всех последующих выпусков цепочки
// (стандарт §12.3).
func TestDipMissingSheetWarns(t *testing.T) {
	hook := dipWarnHook(t)
	points := dipParseFixture(t, dipFixture2607)
	if len(points) != 508 {
		t.Errorf("точек %d, want 508: без листа безработицы разбираются SAAR и занятость", len(points))
	}
	bySeries := dipGroupPoints(points)
	for _, name := range []string{"unemployment_sa", "unemployment_nsa"} {
		if _, ok := bySeries[name]; ok {
			t.Errorf("ряд %q разобран, хотя листа безработицы в выпуске нет", name)
		}
	}
	for _, name := range []string{"employment_sa", "employment_nsa", "saar_all_goods"} {
		if len(bySeries[name]) == 0 {
			t.Errorf("ряд %q не разобран, хотя его лист в выпуске есть", name)
		}
	}
	dipAssertWarn(t, hook, "Уровень безработицы, %", "unemployment_sa", "unemployment_nsa", "пропущены")
}

// TestDipMissingColumnWarns: отдельная объявленная колонка не найдена в найденном листе —
// предупреждение и пропуск, остальные колонки листа импортируются (спека §3.4; так выглядят
// выпуски 22-04 и 22-05 с компонентами SAAR «(без …)»). Подпись заменена на живую подпись
// выпуска 22-05 — не выдуманную.
func TestDipMissingColumnWarns(t *testing.T) {
	hook := dipWarnHook(t)
	f := dipOpenFixture(t, dipFixture2606, func(t *testing.T, f *excelize.File) {
		dipSetCell(t, f, "2", "E3", "Услуги (без ЖКУ)")
	})
	points, err := cbrDipParseBook(f, dipFixtureReleases[dipFixture2606])
	if err != nil {
		t.Fatalf("разбор мутированной фикстуры: %v", err)
	}
	// 3 компоненты SAAR × 67 + 2 ряда безработицы × 138: пропущен только ряд «Услуги».
	if len(points) != 477 {
		t.Errorf("точек %d, want 477 (3×67 + 2×138)", len(points))
	}
	bySeries := dipGroupPoints(points)
	if _, ok := bySeries["saar_services"]; ok {
		t.Error("ряд saar_services разобран, хотя его колонка переименована в чужую метрику")
	}
	for _, name := range []string{"saar_all_goods", "saar_food", "saar_nonfood"} {
		if len(bySeries[name]) != 67 {
			t.Errorf("ряд %q: точек %d, want 67 (остальные колонки листа импортируются)", name, len(bySeries[name]))
		}
	}
	if len(bySeries["unemployment_sa"]) != 138 {
		t.Errorf("ряд unemployment_sa: точек %d, want 138 (чужой лист разбором не задет)",
			len(bySeries["unemployment_sa"]))
	}
	dipAssertWarn(t, hook, "saar_services", "не найдена", "Услуги")
}
