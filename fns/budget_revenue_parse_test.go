package fns

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// fnsFixture — чтение фикстуры целиком: фикстуры реальные архивы источника, урезанные
// только по составу внутренних файлов (стандарт §11.1–11.2).
func fnsFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", name, err)
	}
	return data
}

// TestFnsParseTerritoryArchives — разбор двух архивов разных лет: число точек,
// классы территорий, точечные значения и привязка даты. Две фикстуры держатся
// потому, что разметка листа между 2020 и 2026 разошлась (строка-якорь на другой
// строке, имена файлов с префиксом карточки, написания субъектов), и парсер обязан
// пережить это обоими путями.
func TestFnsParseTerritoryArchives(t *testing.T) {
	cases := []struct {
		file  string
		month string
		spots map[string]float64
	}{
		{
			file:  "1nm010926reg.zip",
			month: "2026-08",
			spots: map[string]float64{
				"Российская Федерация|admin_revenues_total":          31696942340,
				"Российская Федерация|admin_revenues_federal":        18129844310,
				"Российская Федерация|admin_revenues_regional":       13567098030,
				"Российская Федерация|admin_revenues_local":          1750370040,
				"Центральный федеральный округ|admin_revenues_total": 11404955871,
				"Белгородская область|admin_revenues_total":          162727827,
				"Российская Федерация|ndfl_regional":                 5962573249,
				"Российская Федерация|vat_domestic_federal":          8379296315,
				"Российская Федерация|profit_tax_regional":           3755294292,
			},
		},
		{
			file:  "1nm010220reg.zip",
			month: "2020-01",
			spots: map[string]float64{
				"Российская Федерация|admin_revenues_total":    1486948864,
				"Российская Федерация|admin_revenues_federal":  987773073,
				"Российская Федерация|admin_revenues_regional": 499175791,
				"Российская Федерация|admin_revenues_local":    85148272,
				"Белгородская область|admin_revenues_total":    4879524,
				"Российская Федерация|ndfl_regional":           216230324,
				"Российская Федерация|vat_domestic_federal":    366046161,
				"Российская Федерация|profit_tax_regional":     126680789,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			month, err := fnsParseMonth(tc.month)
			if err != nil {
				t.Fatalf("разбор месяца %s: %v", tc.month, err)
			}
			rec := newFnsRecorder()
			if err := fnsParseBudgetRevenue(fnsFixture(t, tc.file), rec, month); err != nil {
				t.Fatalf("разбор %s: %v", tc.file, err)
			}
			if rec.err != nil {
				t.Fatalf("форма строк нарушена: %v", rec.err)
			}
			// 47 объявленных рядов × 94 территории: в этих архивах пустых ячеек нет.
			if len(rec.rows) != len(fnsSeriesRows)*fnsTerritoriesInArchive {
				t.Errorf("точек %d, ожидалось %d (%d рядов × %d территорий)",
					len(rec.rows), len(fnsSeriesRows)*fnsTerritoriesInArchive, len(fnsSeriesRows), fnsTerritoriesInArchive)
			}
			for _, row := range rec.rows {
				if !row.date.Equal(month) {
					t.Fatalf("точка %s/%s: дата %s, ожидался отчётный месяц %s",
						row.region, row.series, row.date.Format("2006-01-02"), month.Format("2006-01-02"))
				}
			}
			index := rec.index()
			for key, want := range tc.spots {
				got, ok := index[key]
				if !ok {
					t.Errorf("нет точки %s", key)
					continue
				}
				if got.value != want {
					t.Errorf("%s = %v, ожидалось %v", key, got.value, want)
				}
			}
			regions := rec.regionSet()
			if len(regions) != fnsTerritoriesInArchive {
				t.Errorf("территорий %d, ожидалось %d", len(regions), fnsTerritoriesInArchive)
			}
			counts := map[string]int{}
			for _, kind := range regions {
				counts[kind]++
			}
			if counts[fnsKindRF] != 1 || counts[fnsKindFederalDistrict] != 8 || counts[fnsKindSubject] != 85 {
				t.Errorf("классы территорий %v, ожидалось rf=1, federal_district=8, subject=85", counts)
			}
			// Написания разных лет обязаны свестись к одному канону: сокращений АО
			// и капса источника в таблице быть не должно.
			for region := range regions {
				if strings.Contains(region, "АО") || strings.ToUpper(region) == region {
					t.Errorf("территория %q осталась в написании источника, а не канона", region)
				}
			}
		})
	}
}

// TestFnsCatalogMatchesParsedSeries — двусторонний контракт каталога (стандарт §11.4):
// имена рядов объявлены кодом, поэтому каждый разобранный ряд описан и каждый
// описанный ряд встречается в разборе.
func TestFnsCatalogMatchesParsedSeries(t *testing.T) {
	meta := fnsBudgetRevenueSeriesMeta()
	declared := make(map[string]bool, len(meta))
	for _, row := range meta {
		if row.Source != fnsSource {
			t.Errorf("ряд %q: source %q, ожидался %q", row.Series, row.Source, fnsSource)
		}
		if row.Title == "" || row.Unit == "" || row.Frequency != "M" || row.Origin == "" || row.Description == "" {
			t.Errorf("ряд %q: каталог неполный — title/unit/frequency/origin/description обязательны", row.Series)
		}
		if !strings.Contains(row.Description, "накопленным итогом") {
			t.Errorf("ряд %q: в описании нет правила чтения накопленного — агент прочитает значение как месячное", row.Series)
		}
		if declared[row.Series] {
			t.Errorf("ряд %q объявлен в каталоге дважды", row.Series)
		}
		declared[row.Series] = true
	}
	if len(meta) != len(fnsSeriesRows) {
		t.Errorf("каталог %d рядов, а колонок объявлено %d", len(meta), len(fnsSeriesRows))
	}

	rec := newFnsRecorder()
	if err := fnsParseBudgetRevenue(fnsFixture(t, "1nm010926reg.zip"), rec, time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("разбор фикстуры: %v", err)
	}
	parsed := rec.seriesSet()
	for series := range parsed {
		if !declared[series] {
			t.Errorf("разобран ряд %q, которого нет в каталоге: агент увидит ряд без описания", series)
		}
	}
	for series := range declared {
		if parsed[series] == 0 {
			t.Errorf("ряд %q описан в каталоге, но не разобран из фикстуры", series)
		}
	}
}

// TestFnsReportingMonth — имена архивов, которые встретились на живых страницах
// годов: ddmmyy, ddmmyyyy, подчёркивание после 1nm, маркер ревизии до и после даты.
func TestFnsReportingMonth(t *testing.T) {
	cases := []struct {
		name     string
		month    string
		revision bool
		ok       bool
	}{
		{"1nm010926reg.zip", "2026-08", false, true},
		{"1nm010126reg.zip", "2025-12", false, true},
		{"1nm01062021reg.zip", "2021-05", false, true},
		{"1nm_010322reg.zip", "2022-02", false, true},
		{"ut_1nm01022021reg.zip", "2021-01", true, true},
		{"1nm010124ut_reg.zip", "2023-12", true, true},
		{"1nm011124m_reg.zip", "2024-10", false, true},
		// as-of обязан приходиться на первое число: другой день — не месячный отчёт.
		{"1nm150926reg.zip", "", false, false},
		{"readme_reg.zip", "", false, false},
		{"1nm011326reg.zip", "", false, false},
		// Дата из будущего — испорченная ссылка, а не отчёт: приняв её, гейт
		// отсёк бы все настоящие месяцы стартом max(date)+1.
		{"1nm010199reg.zip", "", false, false},
	}
	for _, tc := range cases {
		month, revision, ok := fnsReportingMonth(tc.name)
		if ok != tc.ok {
			t.Errorf("%s: распознан %v, ожидалось %v", tc.name, ok, tc.ok)
			continue
		}
		if !tc.ok {
			continue
		}
		if got := month.Format("2006-01"); got != tc.month {
			t.Errorf("%s: отчётный месяц %s, ожидалось %s", tc.name, got, tc.month)
		}
		if revision != tc.revision {
			t.Errorf("%s: revision=%v, ожидалось %v", tc.name, revision, tc.revision)
		}
	}
}

// TestFnsResolveTerritory — канон и классы, включая написания архивов 2020 года и
// опечатку латинской h в «Ямало-Hенецкий АО».
func TestFnsResolveTerritory(t *testing.T) {
	cases := []struct{ raw, want, kind string }{
		{"РОССИЙСКАЯ ФЕДЕРАЦИЯ", fnsRfCanonical, fnsKindRF},
		{"Российская Федерация", fnsRfCanonical, fnsKindRF},
		{"ЦЕНТРАЛЬНЫЙ ФЕДЕРАЛЬНЫЙ ОКРУГ", "Центральный федеральный округ", fnsKindFederalDistrict},
		{"город Москва", "г. Москва", fnsKindSubject},
		{"город Санкт-Петербург", "г. Санкт-Петербург", fnsKindSubject},
		{"город Севастополь", "г. Севастополь", fnsKindSubject},
		{"Чукотский АО", "Чукотский автономный округ", fnsKindSubject},
		{"Ненецкий АО", "Ненецкий автономный округ", fnsKindSubject},
		{"Ханты-Мансийский АО - Югра", "Ханты-Мансийский автономный округ - Югра", fnsKindSubject},
		{"Ямало-Hенецкий АО", "Ямало-Ненецкий автономный округ", fnsKindSubject},
		{"Республика Марий Эл", "Республика Марий-Эл", fnsKindSubject},
	}
	for _, tc := range cases {
		name, kind, err := fnsResolveTerritory(tc.raw)
		if err != nil {
			t.Errorf("%q: %v", tc.raw, err)
			continue
		}
		if name != tc.want || kind != tc.kind {
			t.Errorf("%q → %q/%s, ожидалось %q/%s", tc.raw, name, kind, tc.want, tc.kind)
		}
	}
	for _, raw := range []string{"", "   ", "Область Неведомая", "Республика Западная"} {
		if _, _, err := fnsResolveTerritory(raw); err == nil {
			t.Errorf("%q: неизвестная территория принята без ошибки — субъект пропал бы молча", raw)
		}
	}
}

// TestFnsParseNumber — пустая ячейка, прочерк и литеральный X не точки; разделители
// разрядов и десятичная запятая разбираются; мусор даёт ошибку.
func TestFnsParseNumber(t *testing.T) {
	// Маркер «нет данных» источник пишет и кириллицей, и латиницей: оба начертания
	// обязаны остаться без точки, иначе «х» превратится в ошибку разбора.
	for _, raw := range []string{"", "  ", "-", "—", "X", "x", "Х", "х"} {
		if value, has, err := fnsParseNumber(raw); err != nil || has {
			t.Errorf("%q: has=%v value=%v err=%v, ожидалось отсутствие точки", raw, has, value, err)
		}
	}
	cases := []struct {
		raw  string
		want float64
	}{
		{"1486948864", 1486948864},
		{"1\u00a0486\u202f948 864", 1486948864},
		{"-7948763", -7948763},
		{"12,5", 12.5},
	}
	for _, tc := range cases {
		value, has, err := fnsParseNumber(tc.raw)
		if err != nil || !has || value != tc.want {
			t.Errorf("%q → %v/%v/%v, ожидалось %v", tc.raw, value, has, err, tc.want)
		}
	}
	if _, _, err := fnsParseNumber("12 34,56,78"); err == nil {
		t.Error("мусорное значение разобрано без ошибки")
	}
}

// TestFnsIsTerritoryFile — внутренний файл архива ищется по признаку: лист 11О
// подходит с «ЕНС» и без него, с числовым префиксом карточки сайта, а листы
// начислений и справочные — нет.
func TestFnsIsTerritoryFile(t *testing.T) {
	yes := []string{
		"11О Поступление налогов по территориям.xlsx",
		"11О Поступление налогов по территориям1.xlsx",
		"79732178_11О ЕНС Поступление налогов по территориям.xlsx",
	}
	no := []string{
		"12О Начислено по территориям.xlsx",
		"SP1-Справочно1 по разделу 1 по территориям.xlsx",
		"Раздел4 Начисление и поступление по страховым взносам.xlsx",
		"11О Поступление налогов по территориям.xls",
	}
	for _, name := range yes {
		if !fnsIsTerritoryFile(name) {
			t.Errorf("%q не опознан как лист поступлений", name)
		}
	}
	for _, name := range no {
		if fnsIsTerritoryFile(name) {
			t.Errorf("%q ошибочно принят за лист поступлений", name)
		}
	}
}

// TestFnsOpenWorkbookFailFast — состав архива проверяется до разбора: без листа
// поступлений и при двух подходящих файлах импортёр обязан упасть, а не выбрать
// наугад.
func TestFnsOpenWorkbookFailFast(t *testing.T) {
	body := fnsFixture(t, "1nm010926reg.zip")

	empty := fnsRebuildZip(t, map[string][]byte{"Раздел4 Страховые взносы.xlsx": []byte("не книга")})
	if _, _, err := fnsOpenTerritoryWorkbook(empty); err == nil {
		t.Error("архив без листа поступлений принят без ошибки")
	} else if !strings.Contains(err.Error(), "11О") {
		t.Errorf("текст ошибки не называет ожидаемый лист: %v", err)
	}

	duplicated := fnsRebuildZip(t, map[string][]byte{
		"11О Поступление налогов по территориям.xlsx":     body,
		"11О ЕНС Поступление налогов по территориям.xlsx": body,
	})
	if _, _, err := fnsOpenTerritoryWorkbook(duplicated); err == nil {
		t.Error("архив с двумя подходящими листами принят без ошибки")
	}

	if _, _, err := fnsOpenTerritoryWorkbook([]byte("не архив")); err == nil {
		t.Error("не-архив принят без ошибки")
	}
}

// TestFnsCollectPointsFailFastOnMissingColumn — объявленный ряд, которого в книге нет,
// должен остановить прогон: молча потерять строку отчёта нельзя (стандарт §6.4).
func TestFnsCollectPointsFailFastOnMissingColumn(t *testing.T) {
	book := excelize.NewFile()
	defer func() { _ = book.Close() }()
	// Книга без шапки кодов вообще и без объявленных колонок в частности.
	if err := book.SetCellValue("Sheet1", "A1", "Форма № 1-НМ"); err != nil {
		t.Fatalf("заготовка книги: %v", err)
	}
	if _, err := fnsCollectPoints(book, time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("книга без объявленных колонок разобрана успешно — потеря ряда прошла бы молча")
	}
}

// TestFnsParseRevisionWithExtraRows — ревизия отчёта за декабрь 2023 года
// (1nm010124ut_reg.zip): в листе появились четыре новых субъекта отдельными
// строками без значений, строка-группировка «Юго-Западные субъекты» и строка
// «МИ ФНС России по управлению долгом» со значениями по всем 47 рядам. Без
// объявленного исключения эти 51 точка попали бы в региональный разрез как
// территории (или задвоили бы итог), а незнакомые строки уронили бы прогон.
func TestFnsParseRevisionWithExtraRows(t *testing.T) {
	month, revision, ok := fnsReportingMonth("1nm010124ut_reg.zip")
	if !ok || !revision {
		t.Fatalf("имя ревизии не распознано: ok=%v revision=%v", ok, revision)
	}
	if got := month.Format("2006-01"); got != "2023-12" {
		t.Fatalf("отчётный месяц %s, ожидался 2023-12", got)
	}
	rec := newFnsRecorder()
	if err := fnsParseBudgetRevenue(fnsFixture(t, "1nm010124ut_reg.zip"), rec, month); err != nil {
		t.Fatalf("разбор ревизии: %v", err)
	}
	if rec.err != nil {
		t.Fatalf("форма строк нарушена: %v", rec.err)
	}
	if len(rec.rows) != len(fnsSeriesRows)*fnsTerritoriesInArchive {
		t.Errorf("точек %d, ожидалось %d: группы и администратор не должны давать точек",
			len(rec.rows), len(fnsSeriesRows)*fnsTerritoriesInArchive)
	}
	for _, row := range rec.rows {
		switch row.region {
		case "Юго-Западные субъекты", "МИ ФНС России по управлению долгом":
			t.Errorf("исключённая строка попала в точки: %s / %s", row.region, row.series)
		}
	}
	if got := rec.index()["Российская Федерация|admin_revenues_total"].value; got != 36182868968 {
		t.Errorf("РФ admin_revenues_total 2023-12 = %v, ожидалось 36182868968", got)
	}
	for _, name := range []string{"Донецкая Народная Республика", "Луганская Народная Республика",
		"Запорожская область", "Херсонская область"} {
		region, kind, err := fnsResolveTerritory(name)
		if err != nil || region != name || kind != fnsKindSubject {
			t.Errorf("%q: разрешено в %q/%v, ошибка %v, ожидался субъект того же имени", name, region, kind, err)
		}
	}
}

// TestFnsTerritoryTablesAreNormalized — таблицы написаний нормализуются тем же
// правилом, что разбор применяет к ячейке: алиас обязан вести на канон из списка,
// его нормализованный ключ не должен совпадать с каноническим написанием (тогда
// таблица избыточна), а исключённая строка не должна пересекаться с территориями и
// обязана оставаться неразрешимой — иначе проверка исключений не сработает.
func TestFnsTerritoryTablesAreNormalized(t *testing.T) {
	for spelling, canonical := range fnsTerritoryAliasSpellings {
		key := fnsNormalizeTerritory(spelling)
		if got, ok := fnsTerritoryAliases[key]; !ok || got != canonical {
			t.Errorf("алиас %q: ключ %q не найден или ведёт в %q", spelling, key, got)
		}
		if _, isCanonical := fnsTerritoryByNorm[key]; isCanonical {
			t.Errorf("алиас %q совпадает с каноническим написанием: таблица избыточна", spelling)
		}
		if _, excluded := fnsTerritoryExcluded[key]; excluded {
			t.Errorf("алиас %q числится и в списке исключений", spelling)
		}
		if region, _, err := fnsResolveTerritory(spelling); err != nil || region != canonical {
			t.Errorf("алиас %q: разрешено %q, ошибка %v", spelling, region, err)
		}
		if _, kind, err := fnsResolveTerritory(canonical); err != nil || kind != fnsKindSubject {
			t.Errorf("канон алиаса %q: класс %q, ошибка %v — города и АО обязаны оставаться субъектами", canonical, kind, err)
		}
	}
	for spelling, reason := range fnsTerritoryExcludedSpellings {
		key := fnsNormalizeTerritory(spelling)
		if got, ok := fnsTerritoryExcluded[key]; !ok || got != reason {
			t.Errorf("исключение %q: ключ %q не найден", spelling, key)
		}
		if _, isCanonical := fnsTerritoryByNorm[key]; isCanonical {
			t.Errorf("исключённая строка %q пересекается с канонической территорией", spelling)
		}
		if _, _, err := fnsResolveTerritory(spelling); err == nil {
			t.Errorf("%q разрешается как территория: без срабатывания исключения в разрез попал бы не субъект", spelling)
		}
	}
}

// fnsRebuildZip — архив из пары имя → содержимое: нужны только негативные ветки
// состава архива, положительные разбираются с реальными фикстурами.
func fnsRebuildZip(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, data := range files {
		writer, err := zw.Create(name)
		if err != nil {
			t.Fatalf("создание %q: %v", name, err)
		}
		if _, err = io.Copy(writer, bytes.NewReader(data)); err != nil {
			t.Fatalf("запись %q: %v", name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("закрытие архива: %v", err)
	}
	return buf.Bytes()
}
