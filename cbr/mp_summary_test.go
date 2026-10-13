package cbr

import (
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Ожидаемые значения ниже — литералами, а не константами пакета: тест, сверяющий
// mpSummaryName с самим собой, прошёл бы и после переименования таблицы или шага dagu,
// то есть ровно на той правке, от которой он и должен падать.
//
// Контур резюме обсуждений пишется в три свои таблицы и читается агентом через три
// витрины, поэтому пины идут по всем четырём контрактам: имя импортёра (шаг расписания),
// DDL (схема, которую ждут витрины и агент) и Select витрин (что именно увидит агент).

// TestMPSummaryImporterNameAndTable — имя импортёра и таблица-шапка: первое — контракт
// CLICKHOUSE_IMPORT_STAT и id шага dagu/cbr.yaml, второе — контракт данных (витрина
// v_cbr_mp_summary и агент MCP). У импортёров с собственными таблицами пакет держит
// конвенцию «Name() == TableName», и расхождение означало бы прогон, который пишет в одну
// таблицу, а витрина читает другую.
func TestMPSummaryImporterNameAndTable(t *testing.T) {
	const name = "cbr_mp_summary"

	if got := mpSummaryImporter.Name(); got != name {
		t.Errorf("Name() = %q, want %q", got, name)
	}
	if got := mpSummaryImporter.TableName; got != name {
		t.Errorf("TableName = %q, want %q: у контура своя таблица-шапка, имя импортёра — её же имя", got, name)
	}
}

// TestMPSummaryDdlPins — три DDL-литерала контура: имена таблиц, ReplacingMergeTree (без
// него повторный прогон плодил бы копии строк резюме) и ключи ORDER BY, по которым
// перезаписываются строки. Ключи — часть контракта с витринами: по ним агент видит одну
// строку на заседание (шапка), на абзац (блоки) и на фрагмент канала у каждой пары
// (model, prompt_version) у каналов. Колонки тоже пинятся: Nullable у дельты и даты
// вступления — не деталь стиля, а различие «нет предыдущего решения» от «шаг 0».
func TestMPSummaryDdlPins(t *testing.T) {
	cases := []struct {
		table   string
		ddl     string
		orderBy string
		columns []string
	}{
		{
			table:   "cbr_mp_summary",
			ddl:     mpSummaryDdl,
			orderBy: "ORDER BY meeting_date;",
			columns: []string{
				"meeting_date Date", "published Date", "url String",
				"decision_action LowCardinality(String)", "decision_rate Float64",
				"decision_delta_bp Nullable(Int16)", "effective_from Nullable(Date)", "signal String",
			},
		},
		{
			table:   "cbr_mp_summary_block",
			ddl:     mpSummaryBlockDdl,
			orderBy: "ORDER BY (meeting_date, block, subsection, kind, seq);",
			columns: []string{
				"meeting_date Date", "block LowCardinality(String)", "subsection LowCardinality(String)",
				"kind LowCardinality(String)", "seq UInt16", "text String",
			},
		},
		{
			table:   "cbr_mp_summary_channel",
			ddl:     mpSummaryChannelDdl,
			orderBy: "ORDER BY (meeting_date, channel, kind, method, model, prompt_version, seq);",
			columns: []string{
				"meeting_date Date", "channel LowCardinality(String)", "kind LowCardinality(String)",
				"method LowCardinality(String)", "model LowCardinality(String)", "prompt_version UInt16",
				"seq UInt16", "text String",
			},
		},
	}

	for _, c := range cases {
		if !strings.Contains(c.ddl, "CREATE TABLE IF NOT EXISTS "+c.table+" (") {
			t.Errorf("DDL не создаёт таблицу %q", c.table)
		}
		if !strings.Contains(c.ddl, "ENGINE = ReplacingMergeTree") {
			t.Errorf("DDL %s: нет ReplacingMergeTree: повторный прогон плодил бы копии строк", c.table)
		}
		if !strings.Contains(c.ddl, c.orderBy) {
			t.Errorf("DDL %s: нет ключа %q", c.table, c.orderBy)
		}
		for _, col := range c.columns {
			if !strings.Contains(c.ddl, col) {
				t.Errorf("DDL %s: нет колонки %q", c.table, col)
			}
		}
	}
}

// TestMPSummaryViewPins — три витрины контура: выборка целиком (а не отдельные подстроки),
// потому что витрина — это и есть контракт с агентом. Select — голая выборка из своей
// таблицы: FINAL обязателен (ключ ReplacingMergeTree, без него агент увидел бы
// несмёрженные части), фильтров source нет (таблицы свои, не общая macro_series), а
// колонки перечислены явно, чтобы витрина не поехала за порядком колонок DDL. У каждой
// колонки обязан быть комментарий: по ним агент понимает, что значит число (правило 11).
func TestMPSummaryViewPins(t *testing.T) {
	cases := []struct {
		name    string
		view    util.View
		table   string
		columns []string
	}{
		{
			name:  "v_cbr_mp_summary",
			view:  mpSummaryView,
			table: "cbr_mp_summary",
			columns: []string{
				"meeting_date", "published", "url", "decision_action", "decision_rate",
				"decision_delta_bp", "effective_from", "signal",
			},
		},
		{
			name:    "v_cbr_mp_summary_blocks",
			view:    mpSummaryBlocksView,
			table:   "cbr_mp_summary_block",
			columns: []string{"meeting_date", "block", "subsection", "kind", "seq", "text"},
		},
		{
			name:  "v_cbr_mp_summary_channels",
			view:  mpSummaryChannelsView,
			table: "cbr_mp_summary_channel",
			columns: []string{
				"meeting_date", "channel", "kind", "method", "model", "prompt_version", "seq", "text",
			},
		},
	}

	for _, c := range cases {
		if c.view.Name != c.name {
			t.Errorf("витрина называется %q, want %q", c.view.Name, c.name)
		}
		if len(c.view.Tables) != 1 || c.view.Tables[0] != c.table {
			t.Errorf("витрина %s читает %v, want [%s]", c.name, c.view.Tables, c.table)
		}
		want := "SELECT " + strings.Join(c.columns, ", ") + " FROM " + c.table + " FINAL"
		if c.view.Select != want {
			t.Errorf("Select витрины %s = %q, want %q", c.name, c.view.Select, want)
		}
		if c.view.Comment == "" {
			t.Errorf("витрина %s без комментария таблицы: агент не поймёт, что это за данные", c.name)
		}
		for _, col := range c.columns {
			if c.view.Columns[col] == "" {
				t.Errorf("витрина %s: колонка %s без комментария", c.name, col)
			}
		}
	}
}

// TestMPSummaryDecisionDeltas — шаг ставки в базисных пунктах: NULL у самого раннего
// решения ряда, 0 у hold (ставка не менялась) и round((rate − предыдущая rate) × 100) у
// изменений. Проверка отдельная от Import(): дельта — единственная арифметика импортёра,
// а её ошибка тихая (число выглядит правдоподобно), и живой прогон — не место, где её
// искать. Значение нового документа той же даты сильнее прошлого: повторный прогон
// пересчитывает шаг, а не теряет ставку.
func TestMPSummaryDecisionDeltas(t *testing.T) {
	day := func(d int, m time.Month) time.Time { return time.Date(2026, m, d, 0, 0, 0, 0, time.UTC) }
	bp := func(v int16) *int16 { return &v }

	past := []mpPastSummary{
		{MeetingDate: day(13, time.February), Rate: 16},
		{MeetingDate: day(24, time.April), Rate: 16},
	}
	docs := []mpFetchedSummary{
		{Doc: mpSummaryDoc{MeetingDate: day(20, time.June), Rate: 15.5}},
		{Doc: mpSummaryDoc{MeetingDate: day(24, time.July), Rate: 15}},
	}

	want := map[time.Time]*int16{
		day(13, time.February): nil,     // самое раннее решение: предыдущего нет
		day(24, time.April):    bp(0),   // hold: ставка та же
		day(20, time.June):     bp(-50), // снижение 15,5 % после 16 %
		day(24, time.July):     bp(-50), // снижение 15 % после 15,5 %
	}
	deltas := mpDecisionDeltas(past, docs)
	if len(deltas) != len(want) {
		t.Fatalf("шагов %d, want %d: ряд заседаний собран неверно", len(deltas), len(want))
	}
	for date, w := range want {
		got, ok := deltas[date]
		switch {
		case !ok:
			t.Errorf("нет шага для %s", date.Format(mpSummaryDayLayout))
		case w == nil && got != nil:
			t.Errorf("шаг %s = %d, want NULL (у самого раннего решения предыдущего нет)", date.Format(mpSummaryDayLayout), *got)
		case w != nil && got == nil:
			t.Errorf("шаг %s = NULL, want %d: предыдущее заседание потеряно", date.Format(mpSummaryDayLayout), *w)
		case w != nil && *got != *w:
			t.Errorf("шаг %s = %d, want %d", date.Format(mpSummaryDayLayout), *got, *w)
		}
	}

	// Дата уже лежит в таблице — новое значение перекрывает прошлое, и шаг следующего
	// заседания считается от него: иначе повторный прогон исправил бы ставку, оставив
	// дельту прежней.
	again := mpDecisionDeltas(past, []mpFetchedSummary{
		{Doc: mpSummaryDoc{MeetingDate: day(24, time.April), Rate: 15.5}},
	})
	got := again[day(24, time.April)]
	if got == nil {
		t.Errorf("шаг 2026-04-24 после уточнения ставки = NULL (или строки нет), want -50: новый документ сильнее прошлого")
	} else if *got != -50 {
		t.Errorf("шаг 2026-04-24 после уточнения ставки = %d, want -50 (новый документ сильнее прошлого)", *got)
	}
}
