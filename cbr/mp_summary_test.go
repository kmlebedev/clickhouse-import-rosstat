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
// витрины, поэтому пины идут по всем контрактам контура: имя импортёра (шаг расписания),
// DDL (схема, которую ждут витрины и агент), Select витрин (что именно увидит агент),
// решение «известен/не известен» (что перекачивается повторным прогоном) и дельта ставки.

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

// TestMPSummaryMissingRefs — что прогон считает новым документом. Известен только документ,
// записанный целиком: адрес в шапке, хотя бы одна строка блоков по дате его заседания и хотя
// бы одна regex-строка каналов по той же дате (слой 1 — три батча, и сбой любого из них
// оставляет документ неполным). По одному адресу недописанный документ ушёл бы в no-op, и
// потерянное не вернулось бы никогда (спека §3.1 п. 2: сбой батчей слоя 1 = следующий прогон
// повторит). Недописанный документ обязан попадать в missing: он перекачивается и
// перезаписывается целиком, а ключи ReplacingMergeTree делают перезапись идемпотентной.
// Порядок индекса (от новых к старым) при этом сохраняется — он же порядок скачивания.
func TestMPSummaryMissingRefs(t *testing.T) {
	day := func(d int, m time.Month) time.Time { return time.Date(2026, m, d, 0, 0, 0, 0, time.UTC) }

	// Пять карточек индекса: полный документ; документ с одной шапкой (блоков нет);
	// документ с шапкой и блоками, но без regex-разметки (сбой третьего батча слоя 1);
	// документ с шапкой и regex-строками, но без блоков (сбой второго батча — случай
	// «известность по шапке и разметке» маскировал бы пропавший текст резюме);
	// и вовсе неизвестный. Даты заседания четырёх первых — из шапок таблицы, они же ключи
	// блоков и каналов.
	full := mpSummaryRef{Published: day(23, time.September), Href: "/dkp/mp_dec/decision_key_rate/summary_key_rate_23092026/"}
	headerOnly := mpSummaryRef{Published: day(5, time.August), Href: "/dkp/mp_dec/decision_key_rate/summary_key_rate_05082026/"}
	noRegex := mpSummaryRef{Published: day(1, time.July), Href: "/dkp/mp_dec/decision_key_rate/summary_key_rate_01072026/"}
	regexOnly := mpSummaryRef{Published: day(7, time.May), Href: "/dkp/mp_dec/decision_key_rate/summary_key_rate_07052026/"}
	unknown := mpSummaryRef{Published: day(1, time.April), Href: "/dkp/mp_dec/decision_key_rate/summary_key_rate_01042026/"}
	refs := []mpSummaryRef{full, headerOnly, noRegex, regexOnly, unknown}

	imported := map[string]time.Time{
		cbrUrl + full.Href:       day(11, time.September),
		cbrUrl + headerOnly.Href: day(24, time.July),  // шапка есть — блоков и regex-строк нет
		cbrUrl + noRegex.Href:    day(5, time.June),   // шапка и блоки есть — regex-строк нет
		cbrUrl + regexOnly.Href:  day(24, time.April), // шапка и regex-строки есть — блоков нет
	}
	blockDates := map[time.Time]bool{
		day(11, time.September): true,
		day(5, time.June):       true,
	}
	regexDates := map[time.Time]bool{
		day(11, time.September): true,
		day(24, time.April):     true,
	}

	got := mpMissingRefs(refs, imported, blockDates, regexDates)
	want := []mpSummaryRef{headerOnly, noRegex, regexOnly, unknown}
	if len(got) != len(want) {
		t.Fatalf("missing: %d карточек %v, want %d %v: неполный документ слоя 1 (шапка, блоки, regex-строки — все три уровня) обязан перекачиваться",
			len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("missing[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}

	// Каждое из трёх условий проверяется отдельным кейсом, и одного «полного» документа для
	// этого мало: без regexOnly условие по blockDates можно было бы выбросить незамеченным
	// (документ с шапкой и разметкой, но без текста считался бы готовым).
	if again := mpMissingRefs([]mpSummaryRef{regexOnly}, imported, blockDates, regexDates); len(again) != 1 {
		t.Errorf("missing документа с шапкой и regex-строками, но без блоков = %v, want его же: без блоков документ неполон", again)
	}

	// Обратное направление: у документа со всеми тремя уровнями повторный прогон не скачивает
	// ничего — иначе ежедневный шаг заново разбирал бы все 22 резюме.
	if again := mpMissingRefs(refs[:1], imported, blockDates, regexDates); len(again) != 0 {
		t.Errorf("missing полного документа = %v, want пусто: известный документ не перекачивается", again)
	}

	// Признак третьего уровня — именно method='regex': строки LLM-разметки лежат в той же
	// таблице и догоняются отдельно, поэтому по ним потерянный regex-слой (сбой третьего
	// батча) выглядел бы записанным и не вернулся бы никогда.
	const wantRegexQuery = "SELECT DISTINCT meeting_date FROM cbr_mp_summary_channel FINAL WHERE method = 'regex'"
	if mpSummaryRegexDatesQuery != wantRegexQuery {
		t.Errorf("запрос regex-уровня = %q, want %q", mpSummaryRegexDatesQuery, wantRegexQuery)
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

// TestMPSummaryLLMPendingDates — отстающие LLM-документы: известные заседания МИНУС уже
// размеченные пары (model, prompt_version). Живая приёмка 2026-10-14 нашла здесь инверсию
// (выборкой «даты СО строками» догон размечал уже размеченные документы и никогда — те, у
// которых строк нет), поэтому оба направления пинятся явно: «размечено всё» → пусто (иначе
// no-op недостижим и каждый прогон заново спрашивает LLM по всем 22 документам), «не
// размечено ничего» → весь ряд (иначе догон не запускается вовсе).
func TestMPSummaryLLMPendingDates(t *testing.T) {
	day := func(d int, m time.Month) time.Time { return time.Date(2025, m, d, 0, 0, 0, 0, time.UTC) }
	past := []mpPastSummary{
		{MeetingDate: day(14, time.February), Url: "u1"},
		{MeetingDate: day(21, time.March), Url: "u2"},
		{MeetingDate: day(25, time.April), Url: "u3"},
	}

	t.Run("AllTagged", func(t *testing.T) {
		tagged := map[time.Time]bool{
			day(14, time.February): true,
			day(21, time.March):    true,
			day(25, time.April):    true,
		}
		if pending := mpLLMPendingDates(past, tagged); len(pending) != 0 {
			t.Errorf("pending = %v, want пусто: все документы пары размечены, догонять нечего", pending)
		}
	})

	t.Run("NoneTagged", func(t *testing.T) {
		pending := mpLLMPendingDates(past, map[time.Time]bool{})
		if len(pending) != len(past) {
			t.Fatalf("pending = %v, want все %d даты: ни одна не размечена", pending, len(past))
		}
		for _, p := range past {
			if !pending[p.MeetingDate] {
				t.Errorf("pending без %s: неразмеченный документ не догоняется", p.MeetingDate.Format(mpSummaryDayLayout))
			}
		}
	})

	t.Run("PartlyTagged", func(t *testing.T) {
		pending := mpLLMPendingDates(past, map[time.Time]bool{day(21, time.March): true})
		if len(pending) != 2 {
			t.Fatalf("pending = %v, want 2 даты (размеченная 2025-03-21 исключена)", pending)
		}
		if pending[day(21, time.March)] {
			t.Errorf("pending содержит размеченную 2025-03-21: документ будет размечен повторно")
		}
	})

	// Разметка другой пары (другая модель / версия промпта) отстающих не снимает: в наборе
	// «размечено» — строки только нужной пары (model, prompt_version): смена модели или
	// версии промпта добавляет строки рядом со старыми и догон не пропускает (спека §3.5).
	// Условие пары — часть запроса набора, поэтому пинится литералом (как запрос regex-уровня).
	const wantTaggedQuery = "SELECT DISTINCT meeting_date FROM cbr_mp_summary_channel FINAL " +
		"WHERE method = 'llm' AND model = ? AND prompt_version = ?"
	if mpLLMTaggedQuery != wantTaggedQuery {
		t.Errorf("запрос набора «размечено» = %q, want %q: без пары (model, prompt_version) "+
			"отстающие считались бы размеченными после смены модели или версии промпта",
			mpLLMTaggedQuery, wantTaggedQuery)
	}
}

// TestMPSummaryLLMDocs — сборка списка документов для модели: объединение скачанных этим
// прогоном и отстающих МИНУС уже размеченные пары (model, prompt_version). Пинятся оба
// подслучая, которые нашло scoped-ревью фикс-волны: (а) дата есть и в docs, и в pending →
// документ уходит в LLM ОДИН раз (прежняя реализация добавляла его ещё раз через
// justFetched-ветку mpFetchPending, и на одну дату получалось два вызова модели); (б) дата в
// docs, но строки пары уже есть (документ перекачан из-за смены url) → в LLM не уходит вовсе.
// Список, а не набор, поэтому дубль виден длиной — ровно так его и ловит проба ревьюера.
func TestMPSummaryLLMDocs(t *testing.T) {
	day := func(d int, m time.Month) time.Time { return time.Date(2025, m, d, 0, 0, 0, 0, time.UTC) }
	fetched := func(d int, m time.Month, url string) mpFetchedSummary {
		return mpFetchedSummary{
			Url: url,
			Doc: mpSummaryDoc{MeetingDate: day(d, m), Action: "hold", Rate: 16},
		}
	}

	docs := []mpFetchedSummary{
		fetched(14, time.February, "u1"), // и в docs, и в pending, и не размечена
		fetched(21, time.March, "u2"),    // только в docs
		fetched(25, time.April, "u3"),    // в docs, но пара уже размечена (смена url)
	}
	pending := map[time.Time]bool{
		day(14, time.February): true, // тот же документ, что в docs
		day(14, time.March):    true, // отстающий: в этом прогоне не скачивался
	}
	tagged := map[time.Time]bool{
		day(25, time.April): true,
	}

	ready, fetch := mpLLMDocs(docs, pending, tagged)

	wantReady := []time.Time{day(14, time.February), day(21, time.March)}
	if len(ready) != len(wantReady) {
		t.Errorf("ready: %d документов, want %d (2025-02-14 и 2025-03-21): лишний элемент — "+
			"либо дубль даты, попавшей и в docs, и в pending (модель вызывается дважды на документ), "+
			"либо невычтенная размеченная пара", len(ready), len(wantReady))
	}
	for i, want := range wantReady {
		if i >= len(ready) {
			break
		}
		if !ready[i].Date.Equal(want) {
			t.Errorf("ready[%d] = %s, want %s", i, ready[i].Date.Format(mpSummaryDayLayout), want.Format(mpSummaryDayLayout))
		}
	}
	if len(fetch) != 1 || !fetch[day(14, time.March)] {
		t.Errorf("fetch = %v, want только 2025-03-14: скачанный документ повторно не берётся, "+
			"размеченная пара не догоняется", fetch)
	}
	for _, d := range ready {
		if d.Date.Equal(day(25, time.April)) {
			t.Errorf("ready содержит 2025-04-25: у документа уже есть LLM-строки пары — повторная разметка лишняя")
		}
	}

	// Ни pending, ни docs — размечать нечего; пустой вход не должен давать ни документов,
	// ни загрузок (иначе прогон без новых и отстающих звал бы модель на пустом списке).
	emptyReady, emptyFetch := mpLLMDocs(nil, nil, nil)
	if len(emptyReady) != 0 || len(emptyFetch) != 0 {
		t.Errorf("на пустом входе ready=%v fetch=%v, want оба пустыми", emptyReady, emptyFetch)
	}
}
