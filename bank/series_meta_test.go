package bank

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// TestBankSeriesMetaCoversFixtures — контракт каталога трёх отчётных источников пакета на живых
// фикстурах (стандарт §11.4): каждый ряд, разобранный из книги, описан в каталоге своего
// источника — банки ДОМ.РФ в domrfSeriesMeta, ряды ВТБ в vtbSeriesMeta, ряды Сбера по РСБУ в
// sberFinSeriesMeta. Контракт односторонний («разобрано ⊆ описано»): имена рядов даёт источник, а
// фикстура — снимок живого файла, и она не доказывает, что описанный ряд ещё публикуется. Разбор
// идёт теми же чистыми функциями, что и у импортёров (§11.3): тест не ходит в сеть.
//
// Кроме покрытия сверяются: заполненность описания (пустое поле — ряд, о котором агенту нечего
// прочитать), источник каждого описания, уникальность имени внутри источника (дубль схлопнул бы
// два разных ряда в один ключ каталога) и число описаний — текущий набор фикстур. Отдельно
// проверяется состав bankSeriesMeta, который получает AfterImport всех пяти импортёров: каталог
// собирается конкатенацией, и потерянный при сборке ряд или лишний дубль не поймал бы больше ни
// один тест (стандарт §10.1, §10.6).
func TestBankSeriesMetaCoversFixtures(t *testing.T) {
	cases := []struct {
		fixture string
		meta    []util.SeriesMeta
		source  string
		series  int
		// parsed — имена рядов, разобранные из фикстуры: у ВТБ и Сбера — общие с тестами разбора
		// хелперы без повторов (vtbSeriesNames/sberSeriesNames), у ДОМ.РФ — точки листа.
		parsed func(t *testing.T) []string
	}{
		{
			fixture: domrfBookFixture,
			meta:    domrfSeriesMeta,
			source:  domrfMortgageTable,
			series:  15,
			parsed: func(t *testing.T) []string {
				points, err := parseDomrfSheet(domrfOpenFixture(t))
				if err != nil {
					t.Fatalf("parseDomrfSheet(%s): %v", domrfBookFixture, err)
				}
				names := make([]string, 0, len(points))
				for _, p := range points {
					names = append(names, p.Series)
				}
				return names
			},
		},
		{
			fixture: vtbBookFixture,
			meta:    vtbSeriesMeta,
			source:  vtbIfrsTable,
			series:  36,
			parsed: func(t *testing.T) []string {
				points, err := parseVtbBook(vtbOpenFixture(t, vtbBookFixture))
				if err != nil {
					t.Fatalf("parseVtbBook(%s): %v", vtbBookFixture, err)
				}
				return vtbSeriesNames(points)
			},
		},
		{
			fixture: sberFinFixture,
			meta:    sberFinSeriesMeta,
			source:  sberRpbuDataUrlTable,
			series:  30,
			parsed: func(t *testing.T) []string {
				points, err := parseSberFinBook(sberOpenFixture(t))
				if err != nil {
					t.Fatalf("parseSberFinBook(%s): %v", sberFinFixture, err)
				}
				return sberSeriesNames(points)
			},
		},
	}

	for _, c := range cases {
		if len(c.meta) != c.series {
			t.Errorf("каталог %s: описаний %d, ожидалось %d (набор фикстуры %s)",
				c.source, len(c.meta), c.series, c.fixture)
		}
		own := make(map[string]bool, len(c.meta))
		for _, m := range c.meta {
			if m.Source != c.source {
				t.Errorf("ряд %q: источник %q, ожидался %q", m.Series, m.Source, c.source)
			}
			if m.Series == "" || m.Title == "" || m.Unit == "" || m.Frequency == "" || m.Origin == "" || m.Description == "" {
				t.Errorf("описание ряда %q заполнено не полностью: %+v", m.Series, m)
			}
			// Заголовок собирает fmt.Sprintf: незакрытый знак процента в шаблоне превратил бы текст
			// в «%!(NOVERB)» — в каталоге у агента оказался бы мусор формата, а не фраза.
			if strings.Contains(m.Title+m.Unit+m.Description, "%!") {
				t.Errorf("ряд %q: в описании остался мусор формата (%q)", m.Series, m.Title)
			}
			if own[m.Series] {
				t.Errorf("ряд %q описан в %s дважды", m.Series, c.source)
			}
			own[m.Series] = true
		}
		for _, series := range c.parsed(t) {
			if !own[series] {
				t.Errorf("ряд %q из фикстуры %s не описан в каталоге источника %s", series, c.fixture, c.source)
			}
		}
	}

	// Состав bankSeriesMeta — по источникам: 5 месячных и 46 недельных СберИндекса, 15 ДОМ.РФ, 36
	// ВТБ, 30 Сбера по РСБУ (спека §3.6). Дубль ключа (source, series) в каталоге тоже ошибка:
	// ReplacingMergeTree оставил бы из двух описаний одно молча.
	wantSources := map[string]int{
		sberSourceIndex:      5,
		sberSourceWeek:       46,
		domrfMortgageTable:   15,
		vtbIfrsTable:         36,
		sberRpbuDataUrlTable: 30,
	}
	gotSources := make(map[string]int, len(wantSources))
	seen := make(map[string]bool, len(bankSeriesMeta))
	for _, m := range bankSeriesMeta {
		gotSources[m.Source]++
		if key := m.Source + "/" + m.Series; seen[key] {
			t.Errorf("ряд %s описан в bankSeriesMeta дважды: ключ каталога (source, series) не уникален", key)
		} else {
			seen[key] = true
		}
	}
	if got := len(bankSeriesMeta); got != 132 {
		t.Errorf("bankSeriesMeta: описаний %d, ожидалось 132 (51 СберИндекса + 15 ДОМ.РФ + 36 ВТБ + 30 Сбера по РСБУ)", got)
	}
	for source, want := range wantSources {
		if got := gotSources[source]; got != want {
			t.Errorf("bankSeriesMeta: рядов источника %s %d, ожидалось %d", source, got, want)
		}
	}
}

// TestBankMacroViewContract — форма витрины v_bank_macro (bank/views.go) закреплена значениями, а
// не константами пакета: имя, все пять таблиц-источников в Tables и пять source-литералов в
// Select — публичный контракт для агента (правило 11 AGENTS.md, спека §3.6, стандарт §10.3).
// Случайная правка витрины (выпавшая из Tables таблица, забытая ветка UNION ALL, подменённый
// source, потерянный каст) обязана краснить тест, а не менять форму витрины молча: агент читает
// пять наборов одним запросом, и без таблицы в Tables витрина собиралась бы не из всех данных, а
// без литерала её набор исчез бы из витрины или слился с другим ключом каталога. Литералы здесь
// намеренно записаны, а не взяты из констант пакета: проверяется то, что реально увидит агент
// (прецедент — TestSberSpendingViewContract).
func TestBankMacroViewContract(t *testing.T) {
	if got := bankMacroView.Name; got != "v_bank_macro" {
		t.Errorf("имя витрины %q, ожидалось v_bank_macro", got)
	}
	sources := []string{
		"domrf_mortgage",
		"rus_vtb_group_ifrs",
		"sber_finansovie_rezultaty",
		"sber_consumper_spending_index",
		"sber_izmenenie_trat",
	}
	if got := len(bankMacroView.Tables); got != len(sources) {
		t.Errorf("в Tables %d таблиц, ожидалось %d (%v): util.CreateView пропустит витрину, пока не "+
			"существует каждой из них, а лишняя таблица удерживала бы её от сборки", got, len(sources), bankMacroView.Tables)
	}
	for _, table := range sources {
		if !slices.Contains(bankMacroView.Tables, table) {
			t.Errorf("таблица %q отсутствует в Tables (%v): её набор не попадёт в витрину",
				table, bankMacroView.Tables)
		}
		if got := strings.Count(bankMacroView.Select, "'"+table+"'"); got != 1 {
			t.Errorf("source-литерал %q встречается в Select %d раз, ожидался 1: набор исчезнет из "+
				"витрины или сольётся с другим ключом каталога", table, got)
		}
		if !strings.Contains(bankMacroView.Select, "FROM "+table+" FINAL") {
			t.Errorf("в Select нет чтения таблицы литералом (ожидалась форма «FROM %s FINAL»): "+
				"витрина обязана называть таблицу так же, как её увидит агент", table)
		}
	}
	// Значения приводятся к Float64: в таблицах пакета legacy Float32 (спека §4.5) — без каста
	// агент получил бы float32-точность разных наборов в одном столбце value.
	if got := strings.Count(bankMacroView.Select, "toFloat64"); got != len(sources) {
		t.Errorf("в Select %d приведений toFloat64, ожидалось %d (по одному на каждую ветку UNION ALL)",
			got, len(sources))
	}
	if bankMacroView.Comment == "" {
		t.Error("у витрины нет комментария таблицы: агент не узнает, что лежит в v_bank_macro")
	}
	for _, col := range []string{"source", "series", "date", "value"} {
		if bankMacroView.Columns[col] == "" {
			t.Errorf("у колонки %q витрины нет комментария", col)
		}
	}
}

// TestBankAfterImportWired — производные (каталог рядов пакета и обе витрины) обязаны быть
// подключены у всех пяти импортёров отчётностей: забытый AfterImport не уронил бы прогон, а
// оставил бы ряды импортёра без описания у агента и витрины без его набора, и заметить это можно
// было бы только запросом к БД. У ВТБ тот же хелпер зовёт собственный Import() после цикла файлов
// — он публикует производные и на пустой дельте, когда гейт пропустил все файлы; сам вызов
// тестом не проверяется: Import ходит в сеть (§11.3), поэтому здесь проверяется подключение.
func TestBankAfterImportWired(t *testing.T) {
	for _, imp := range []struct {
		name  string
		after func(context.Context, driver.Conn) error
	}{
		{sberRpbuDataUrlTable, sberFinRezImporter.AfterImport},
		{sberCSITable, sberCSIImporter.AfterImport},
		{sberCSIWeekTable, sberCSIWeekImporter.AfterImport},
		{vtbIfrsTable, vtbImporter.AfterImport},
		{domrfMortgageTable, domrfImporter.AfterImport},
	} {
		if imp.after == nil {
			t.Errorf("у импортёра %s не подключены производные (AfterImport): каталог рядов и витрины "+
				"v_bank_macro/v_bank_sber_spending после его прогона не публикуются", imp.name)
		}
	}
}
