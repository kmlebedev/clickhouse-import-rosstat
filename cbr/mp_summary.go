package cbr

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// mpSummaryIndexUrl — индекс материалов заседаний ЦБ: карточки «Резюме обсуждения
	// ключевой ставки» с датой публикации и относительным href. Домен навешивает
	// импортёр (cbrUrl) — как в findLink; страница резюме = cbrUrl + href.
	mpSummaryIndexUrl = "https://www.cbr.ru/dkp/mp_dec/decision_key_rate/"

	// mpSummaryName — имя импортёра в CLICKHOUSE_IMPORT_STAT и id шага dagu/cbr.yaml;
	// оно же — имя таблицы-шапки: у импортёров со своими таблицами пакет держит
	// конвенцию «Name() == TableName» (v_cbr_mp_summary* и агент MCP ищут данные по
	// нему). Блоки и каналы — отдельные таблицы того же контура.
	mpSummaryName         = "cbr_mp_summary"
	mpSummaryTable        = "cbr_mp_summary"
	mpSummaryBlockTable   = "cbr_mp_summary_block"
	mpSummaryChannelTable = "cbr_mp_summary_channel"

	// mpMethodRegex/mpMethodLLM — значения колонки method таблицы каналов: слой 1
	// (детерминированные регексы, работают всегда) и слой 2 (разметка моделью LLM).
	// Строки — контракт с DDL и витриной v_cbr_mp_summary_channels.
	mpMethodRegex = "regex"
	mpMethodLLM   = "llm"

	// mpPromptVersionRegex — версия промпта у строк regex-слоя: у них промпта нет,
	// и 0 отличает их от любой настоящей редакции промпта (mpPromptVersion).
	mpPromptVersionRegex = 0

	// mpSummaryDayLayout — формат дат в сообщениях импортёра (и в url, и в отчётах).
	mpSummaryDayLayout = "2006-01-02"
)

// DDL трёх таблиц контура — дословные копии канонического DDL (спека §3.3,
// ARCHITECTURE §6.2): копии обязаны совпадать. Имена таблиц внутри литералов — тоже
// литералы: CREATE ... IF NOT EXISTS фиксирует схему, а не подставляет TableName, и
// расхождение с mpSummaryTable поймано тестом TestMPSummaryDdlPins.
const (
	mpSummaryDdl = `CREATE TABLE IF NOT EXISTS cbr_mp_summary (
    meeting_date Date,
    published Date,                          -- дата публикации (из индекса; = дата в слаге URL)
    url String,
    decision_action LowCardinality(String),  -- 'hold'|'cut'|'hike'
    decision_rate Float64,                   -- % годовых
    decision_delta_bp Nullable(Int16),       -- шаг к предыдущему решению по meeting_date; NULL — нет предыдущего
    effective_from Nullable(Date),           -- «с …» для cut/hike; NULL для hold
    signal String                            -- дословные предложения про сигнал
) ENGINE = ReplacingMergeTree ORDER BY meeting_date;`

	mpSummaryBlockDdl = `CREATE TABLE IF NOT EXISTS cbr_mp_summary_block (
    meeting_date Date,
    block LowCardinality(String),       -- русское имя блока
    subsection LowCardinality(String),  -- 'Основные факты'|'Дискуссия'|''
    kind LowCardinality(String),        -- 'paragraph'|'bullet'
    seq UInt16,
    text String
) ENGINE = ReplacingMergeTree ORDER BY (meeting_date, block, subsection, kind, seq);`

	mpSummaryChannelDdl = `CREATE TABLE IF NOT EXISTS cbr_mp_summary_channel (
    meeting_date Date,
    channel LowCardinality(String),   -- slug из реестра 11 каналов
    kind LowCardinality(String),      -- 'risk_pro'|'risk_dez'|'prose'|'note'
    method LowCardinality(String),    -- 'regex'|'llm'
    model LowCardinality(String),     -- '' для regex; фактическое имя модели для llm
    prompt_version UInt16,            -- 0 для regex; версия промпта для llm
    seq UInt16,
    text String                       -- дословная цитата; для 'note' — краткий вывод модели по каналу
) ENGINE = ReplacingMergeTree ORDER BY (meeting_date, channel, kind, method, model, prompt_version, seq);`
)

// Порядок колонок в INSERT — контракт с порядком Append в Import (проверка ошибок
// Append обязательна: правило проекта, без неё строка молча теряется).
const (
	mpSummaryInsert        = "INSERT INTO " + mpSummaryTable + " (meeting_date, published, url, decision_action, decision_rate, decision_delta_bp, effective_from, signal)"
	mpSummaryBlockInsert   = "INSERT INTO " + mpSummaryBlockTable + " (meeting_date, block, subsection, kind, seq, text)"
	mpSummaryChannelInsert = "INSERT INTO " + mpSummaryChannelTable + " (meeting_date, channel, kind, method, model, prompt_version, seq, text)"
)

// Запросы чтения. FINAL обязателен: ключи таблиц — ReplacingMergeTree, и без него
// несмёрженные части дали бы дубли дат и повторные матчи каналов.
const (
	// mpSummaryImportedQuery — шапки уже импортированных резюме: адрес страницы и дата
	// заседания. Адрес нужен дельте по индексу (что в индексе ново), дата — проверке
	// полноты документа: шапка пишется первым батчем, и по одному адресу недописанный
	// документ (сбой батча блоков или каналов) считался бы готовым — потерянное не
	// вернулось бы следующим прогоном (спека §3.1).
	mpSummaryImportedQuery = "SELECT url, meeting_date FROM " + mpSummaryTable + " FINAL"

	// mpSummaryBlockDatesQuery — даты заседаний, у которых в таблице блоков есть хотя бы
	// одна строка: вместе с адресом шапки это и есть признак записанного целиком документа.
	mpSummaryBlockDatesQuery = "SELECT DISTINCT meeting_date FROM " + mpSummaryBlockTable + " FINAL"

	// mpSummaryPastQuery — прошлые решения: ставки для decision_delta_bp и адреса
	// страниц для LLM-догона. ORDER BY meeting_date — дельта идёт по порядку заседаний.
	mpSummaryPastQuery = "SELECT meeting_date, decision_rate, url FROM " + mpSummaryTable + " FINAL ORDER BY meeting_date"

	// mpSummaryCountQuery — число резюме таблицы: сообщение no-op называет его
	// (прогон без новых документов обязан отчитаться, а сколько всего на месте).
	mpSummaryCountQuery = "SELECT count() FROM " + mpSummaryTable + " FINAL"

	// mpLLMPendingQuery — даты заседаний, у которых нет LLM-строк текущей пары
	// (model, prompt_version): они доразмечаются следующим прогоном. Пара в ключе
	// таблицы, поэтому смена модели или промпта добавляет новые строки, не затирая
	// старые (спека §3.5), а «разметки нет вовсе» и «разметка устарела» — один запрос.
	mpLLMPendingQuery = "SELECT DISTINCT meeting_date FROM " + mpSummaryChannelTable +
		" FINAL WHERE method = '" + mpMethodLLM + "' AND model = ? AND prompt_version = ?"
)

// mpSummaryStat — импортёр контура резюме обсуждений. Свой Import() (не шаблон
// util.ClickHouseImport): у контура три таблицы, три витрины и дельта по индексу.
type mpSummaryStat struct {
	util.ClickHouseImport
}

func (s *mpSummaryStat) Name() string { return mpSummaryName }

// mpPastSummary — решение, уже лежащее в таблице: ставка нужна дельте, адрес страницы —
// LLM-догону (в таблице лежат строки разметки, а не текст документа, поэтому отстающее
// резюме приходится скачивать заново).
type mpPastSummary struct {
	MeetingDate time.Time
	Rate        float64
	Url         string
}

// mpFetchedSummary — разобранное новое резюме вместе с атрибутами его карточки в
// индексе: абсолютный адрес страницы и дата публикации (шапка таблицы).
type mpFetchedSummary struct {
	Url       string
	Published time.Time
	Doc       mpSummaryDoc
}

// mpLLMDoc — документ, которому нужна LLM-разметка: дата заседания (ключ строк), адрес
// страницы (только для сообщений о пропуске) и разобранное резюме.
type mpLLMDoc struct {
	Date time.Time
	Url  string
	Doc  mpSummaryDoc
}

// Import — полный прогон импортёра (спека §3.1, §3.8). Порядок шагов — часть контракта:
//  1. DDL трёх таблиц;
//  2. индекс материалов заседаний → карточки резюме;
//  3. дельта по таблицам: новые документы (адреса нет в шапке ИЛИ нет блоков по дате его
//     заседания — недописанный документ) и отстающая LLM-разметка;
//  4. no-op, если ни того, ни другого нет (витрины не трогаем — они созданы ранее);
//  5. прошлые решения таблицы: ставки для дельты и адреса для LLM-догона;
//  6. скачивание и разбор новых резюме — fail-fast: ошибка до записи, иначе
//     decision_delta_bp посчитался бы по неполному ряду заседаний;
//  7. батчи по таблицам (шапка, блоки, regex-каналы), Send каждого — ошибка наверх;
//  8. LLM-догон: разметка новых документов и документов без строк текущей пары
//     (model, prompt_version): ошибка одного документа — warn и продолжение
//     (деградация до regex-слоя), ошибка базы — наверх;
//  9. витрины v_cbr_mp_summary* — производные, после данных.
//
// Префлайта (PublicationCountPoints/PublicationSource) и AfterImport у импортёра нет
// сознательно: дельта считается по индексу и полноте документа в таблицах, а не по
// отпечатку артефакта, и само-исцеление «известная версия → no-op» здесь заменено
// проверкой адресов и блоков (спека §3.1).
func (s *mpSummaryStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	// 1.
	for _, ddl := range []string{mpSummaryDdl, mpSummaryBlockDdl, mpSummaryChannelDdl} {
		if err = conn.Exec(ctx, ddl); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", mpSummaryName, err)
		}
	}

	// 2.
	index, err := getPage(mpSummaryIndexUrl)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", mpSummaryName, mpSummaryIndexUrl, err)
	}
	refs, err := parseMPDecIndex(index)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", mpSummaryName, err)
	}

	// 3.
	imported, err := mpImportedSummaries(ctx, conn)
	if err != nil {
		return 0, err
	}
	blockDates, err := mpBlockDates(ctx, conn)
	if err != nil {
		return 0, err
	}
	missing := mpMissingRefs(refs, imported, blockDates)

	baseURL, apiKey, model, llmOK := mpLLMConfigFromEnv()
	var pending map[time.Time]bool
	if llmOK {
		if pending, err = mpLLMPending(ctx, conn, model, mpPromptVersion); err != nil {
			return 0, err
		}
	}

	// 4.
	if len(missing) == 0 && len(pending) == 0 {
		var knownCount uint64
		if err = conn.QueryRow(ctx, mpSummaryCountQuery).Scan(&knownCount); err != nil {
			return 0, fmt.Errorf("%s: число резюме таблицы не прочитано: %w", mpSummaryName, err)
		}
		log.Infof("%s: no-op, все %d резюме на месте", mpSummaryName, knownCount)
		return 0, nil
	}

	// 5.
	past, err := mpPastSummaries(ctx, conn)
	if err != nil {
		return 0, err
	}

	// 6.
	docs := make([]mpFetchedSummary, 0, len(missing))
	for _, ref := range missing {
		url := cbrUrl + ref.Href
		page, pageErr := getPage(url)
		if pageErr != nil {
			return 0, fmt.Errorf("%s: %s: %w", mpSummaryName, url, pageErr)
		}
		doc, parseErr := parseMPSummary(page)
		if parseErr != nil {
			return 0, fmt.Errorf("%s: %s: %w", mpSummaryName, url, parseErr)
		}
		docs = append(docs, mpFetchedSummary{Url: url, Published: ref.Published, Doc: doc})
	}

	// 7. Дельта считается по всему ряду (прошлые решения + новые документы), поэтому
	// ряд собирается до первых записей — иначе шаг у нового резюме не с чем было бы
	// сравнить.
	deltas := mpDecisionDeltas(past, docs)

	header := &mpBatch{ctx: ctx, conn: conn, table: mpSummaryTable, insert: mpSummaryInsert}
	blockRows := &mpBatch{ctx: ctx, conn: conn, table: mpSummaryBlockTable, insert: mpSummaryBlockInsert}
	regexRows := &mpBatch{ctx: ctx, conn: conn, table: mpSummaryChannelTable, insert: mpSummaryChannelInsert}
	for _, d := range docs {
		date := mpDay(d.Doc.MeetingDate)
		if err = header.append(date, d.Published, d.Url, d.Doc.Action, d.Doc.Rate,
			deltas[date], d.Doc.EffectiveFrom, d.Doc.Signal); err != nil {
			return 0, err
		}
		for _, b := range d.Doc.Blocks {
			if err = blockRows.append(date, b.Block, b.Subsection, b.Kind, uint16(b.Seq), b.Text); err != nil {
				return 0, err
			}
		}
		// Regex-слой: каналов реестра 11, матчи одного документа нумеруются сквозным
		// Seq (один номер у всех каналов одного фрагмента), метод и модель — служебные.
		for _, m := range tagMPChannels(d.Doc.Blocks) {
			if err = regexRows.append(date, m.Channel, m.Kind, mpMethodRegex, "",
				uint16(mpPromptVersionRegex), uint16(m.Seq), m.Text); err != nil {
				return 0, err
			}
		}
	}
	for _, batch := range []*mpBatch{header, blockRows, regexRows} {
		if err = batch.send(); err != nil {
			return 0, err
		}
		count += batch.rows
	}

	// 8.
	var llmCount int64
	if !llmOK {
		log.Warnf("%s: LLM-разметка пропущена (нет %s/%s/%s)", mpSummaryName,
			mpLLMBaseURLEnv, mpLLMAPIKeyEnv, mpLLMModelEnv)
	} else {
		llmDocs := make([]mpLLMDoc, 0, len(docs)+len(pending))
		for _, d := range docs {
			llmDocs = append(llmDocs, mpLLMDoc{Date: mpDay(d.Doc.MeetingDate), Url: d.Url, Doc: d.Doc})
		}
		llmDocs = append(llmDocs, mpFetchPending(past, pending)...)

		llmRows := &mpBatch{ctx: ctx, conn: conn, table: mpSummaryChannelTable, insert: mpSummaryChannelInsert}
		for _, d := range llmDocs {
			if err = mpTagLLM(llmRows, d, baseURL, apiKey, model); err != nil {
				return 0, err
			}
		}
		if err = llmRows.send(); err != nil {
			return 0, err
		}
		llmCount = llmRows.rows
		count += llmCount
	}

	// 9.
	for _, view := range []util.View{mpSummaryView, mpSummaryBlocksView, mpSummaryChannelsView} {
		if _, err = util.CreateView(ctx, conn, view); err != nil {
			return 0, err
		}
	}

	log.Infof("%s: резюме %d новых, блоков %d, каналов regex %d, LLM %d", mpSummaryName,
		len(docs), blockRows.rows, regexRows.rows, llmCount)
	return count, nil
}

// mpMissingRefs — карточки индекса, которых в таблице нет целиком. Документ считается
// известным, только если его адрес есть в шапке И по дате его заседания есть хотя бы одна
// строка блоков: шапка пишется первым батчем, и сбой среднего или последнего батча оставил
// бы документ без текста — по одному адресу следующий прогон ушёл бы в no-op, и потерянное
// не вернулось бы никогда (спека §3.1 п. 2: сбой батчей слоя 1 = следующий прогон повторит).
// Недописанный документ попадает в missing, перекачивается и перезаписывается целиком;
// ключи ReplacingMergeTree делают повторную запись идемпотентной. Порядок индекса
// (от новых к старым) сохраняется: он же порядок скачивания и записи.
//
// Чистая функция без обращений к БД: сценарий «шапка есть, блоков нет» пинится тестом
// без драйвера и сети.
func mpMissingRefs(refs []mpSummaryRef, imported map[string]time.Time, blockDates map[time.Time]bool) []mpSummaryRef {
	missing := make([]mpSummaryRef, 0, len(refs))
	for _, ref := range refs {
		date, ok := imported[cbrUrl+ref.Href]
		if ok && blockDates[date] {
			continue
		}
		missing = append(missing, ref)
	}
	return missing
}

// mpImportedSummaries — шапки таблицы: адрес страницы → дата заседания (день в UTC, mpDay).
// Дата нормализуется тем же mpDay, что и даты блоков: иначе проверка полноты документа не
// нашла бы блоки (драйвер отдаёт Date в зоне сессии). Ключ таблицы — meeting_date, поэтому
// один адрес встречается один раз; если адрес всё же попадёт под две даты, в карте останется
// последняя прочитанная — документ может лишний раз попасть в missing (лишнее скачивание и
// идемпотентная перезапись), но не потеряется: это безопасное направление ошибки.
func mpImportedSummaries(ctx context.Context, conn driver.Conn) (map[string]time.Time, error) {
	rows, err := conn.Query(ctx, mpSummaryImportedQuery)
	if err != nil {
		return nil, fmt.Errorf("%s: шапки таблицы не прочитаны: %w", mpSummaryName, err)
	}
	defer func() { _ = rows.Close() }()

	imported := make(map[string]time.Time)
	for rows.Next() {
		var (
			url  string
			date time.Time
		)
		if err = rows.Scan(&url, &date); err != nil {
			return nil, fmt.Errorf("%s: строка шапок таблицы не прочитана: %w", mpSummaryName, err)
		}
		imported[url] = mpDay(date)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: шапки таблицы не дочитаны: %w", mpSummaryName, err)
	}
	return imported, nil
}

// mpBlockDates — даты заседаний, у которых в таблице блоков есть хотя бы одна строка:
// признак того, что документ записан не одной шапкой.
func mpBlockDates(ctx context.Context, conn driver.Conn) (map[time.Time]bool, error) {
	rows, err := conn.Query(ctx, mpSummaryBlockDatesQuery)
	if err != nil {
		return nil, fmt.Errorf("%s: даты блоков не прочитаны: %w", mpSummaryName, err)
	}
	defer func() { _ = rows.Close() }()

	dated := make(map[time.Time]bool)
	for rows.Next() {
		var date time.Time
		if err = rows.Scan(&date); err != nil {
			return nil, fmt.Errorf("%s: дата блоков не прочитана: %w", mpSummaryName, err)
		}
		dated[mpDay(date)] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: даты блоков не дочитаны: %w", mpSummaryName, err)
	}
	return dated, nil
}

// mpPastSummaries — прошлые решения таблицы. Дата приводится к календарному дню в UTC
// (mpDay): clickhouse-go отдаёт Date с таймзоной сессии, и без приведения дата таблицы не
// совпала бы с датой разобранного документа (она всегда UTC-полночь) — дельта считалась бы
// по двум разным рядам, а LLM-догон не нашёл бы отстающих документов.
func mpPastSummaries(ctx context.Context, conn driver.Conn) ([]mpPastSummary, error) {
	rows, err := conn.Query(ctx, mpSummaryPastQuery)
	if err != nil {
		return nil, fmt.Errorf("%s: прошлые решения не прочитаны: %w", mpSummaryName, err)
	}
	defer func() { _ = rows.Close() }()

	var out []mpPastSummary
	for rows.Next() {
		var p mpPastSummary
		if err = rows.Scan(&p.MeetingDate, &p.Rate, &p.Url); err != nil {
			return nil, fmt.Errorf("%s: строка прошлых решений не прочитана: %w", mpSummaryName, err)
		}
		p.MeetingDate = mpDay(p.MeetingDate)
		out = append(out, p)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: прошлые решения не дочитаны: %w", mpSummaryName, err)
	}
	return out, nil
}

// mpLLMPending — даты заседаний без строк разметки текущей пары (model, prompt_version).
func mpLLMPending(ctx context.Context, conn driver.Conn, model string, promptVersion int) (map[time.Time]bool, error) {
	rows, err := conn.Query(ctx, mpLLMPendingQuery, model, promptVersion)
	if err != nil {
		return nil, fmt.Errorf("%s: отстающие LLM-документы не прочитаны: %w", mpSummaryName, err)
	}
	defer func() { _ = rows.Close() }()

	pending := make(map[time.Time]bool)
	for rows.Next() {
		var date time.Time
		if err = rows.Scan(&date); err != nil {
			return nil, fmt.Errorf("%s: дата отстающего LLM-документа не прочитана: %w", mpSummaryName, err)
		}
		pending[mpDay(date)] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: отстающие LLM-документы не дочитаны: %w", mpSummaryName, err)
	}
	return pending, nil
}

// mpDay — календарный день значения в UTC: год/месяц/день берутся у значения КАК ЕСТЬ.
// Приведение через .UTC() сдвинуло бы день назад (дата, отданная драйвером в зоне сессии
// Москвы, — это 21:00 предыдущего дня по UTC), а вместе с днём поехал бы и ключ
// ReplacingMergeTree, и порядок дельты (прецедент — forecast/resolve.go:utcDate).
func mpDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// mpDecisionDeltas — шаг ключевой ставки в базисных пунктах по каждой дате заседания.
// Прошлые решения таблицы сливаются с новыми документами (одна строка на meeting_date,
// новое значение сильнее: повторный прогон того же резюме не должен менять ряд), ряд
// сортируется по дате, и шаг считается как round((rate − предыдущая rate) × 100). У
// самого раннего решения предыдущего нет — NULL; у hold шаг получается 0 сам собой
// (ставка не менялась). Считается по всему ряду, а не по строке: шаг «через заседание»
// читатель принял бы за решение.
func mpDecisionDeltas(past []mpPastSummary, docs []mpFetchedSummary) map[time.Time]*int16 {
	rates := make(map[time.Time]float64, len(past)+len(docs))
	for _, p := range past {
		rates[p.MeetingDate] = p.Rate
	}
	for _, d := range docs {
		rates[mpDay(d.Doc.MeetingDate)] = d.Doc.Rate
	}

	dates := make([]time.Time, 0, len(rates))
	for date := range rates {
		dates = append(dates, date)
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })

	deltas := make(map[time.Time]*int16, len(dates))
	for i, date := range dates {
		if i == 0 {
			deltas[date] = nil
			continue
		}
		bp := int16(math.Round((rates[date] - rates[dates[i-1]]) * 100))
		deltas[date] = &bp
	}
	return deltas
}

// mpFetchPending — документы отстающих резюме для LLM-догона. Адрес страницы берётся из
// таблицы: дата заседания известна, а url карточки записан шапкой при импорте. Недоступная
// или неразобранная страница — warn и продолжение: это деградация одного документа до
// regex-слоя (спека §3.5), а не сбой прогона; следующий прогон повторит догон.
func mpFetchPending(past []mpPastSummary, pending map[time.Time]bool) []mpLLMDoc {
	var out []mpLLMDoc
	for _, p := range past {
		if !pending[p.MeetingDate] || p.Url == "" {
			continue
		}
		page, err := getPage(p.Url)
		if err != nil {
			log.Warnf("%s: LLM-догон %s: страница %s не скачана: %v", mpSummaryName,
				p.MeetingDate.Format(mpSummaryDayLayout), p.Url, err)
			continue
		}
		doc, err := parseMPSummary(page)
		if err != nil {
			log.Warnf("%s: LLM-догон %s: страница %s не разобрана: %v", mpSummaryName,
				p.MeetingDate.Format(mpSummaryDayLayout), p.Url, err)
			continue
		}
		out = append(out, mpLLMDoc{Date: p.MeetingDate, Url: p.Url, Doc: doc})
	}
	return out
}

// mpTagLLM размечает один документ моделью и складывает матчи в батч каналов. Ошибка
// документа (отказ провайдера, неразобранный ответ после ретрая) — warn и продолжение:
// документ остаётся с regex-слоем, догон повторится (спека §3.5). Ошибка батча (БД) —
// наверх: прогон обязан узнать, что разметка не записана.
func mpTagLLM(llmRows *mpBatch, d mpLLMDoc, baseURL, apiKey, model string) error {
	matches, err := tagMPChannelsLLM(d.Doc, baseURL, apiKey, model)
	if err != nil {
		log.Warnf("%s: LLM-разметка %s (%s) пропущена, документ остаётся с regex-слоем: %v",
			mpSummaryName, d.Date.Format(mpSummaryDayLayout), d.Url, err)
		return nil
	}
	for _, m := range matches {
		// Model и PromptVersion — часть ключа ReplacingMergeTree: разметка другой модели
		// или редакции промпта ложится рядом со старой, а не затирает её.
		if err = llmRows.append(d.Date, m.Channel, m.Kind, mpMethodLLM, model,
			uint16(mpPromptVersion), uint16(m.Seq), m.Text); err != nil {
			return err
		}
	}
	return nil
}

// mpBatch — ленивый батч одной таблицы: открывается на первой строке и не открывается
// вовсе, если строк не нашлось (LLM-догон без новых резюме оставляет первые три батча
// пустыми, прогон без отстающих — батч LLM). Пустой PrepareBatch/Send — лишний круг к базе
// и запись в лог сервера на каждом прогоне (прецедент — forecast/resolve.go).
type mpBatch struct {
	ctx    context.Context
	conn   driver.Conn
	table  string
	insert string
	rows   int64
	batch  driver.Batch
}

// append добавляет строку батча. Ошибка Append закрывает открытый батч: соединение,
// оставшееся с незакрытым батчем, сломает следующий импортёр («уже есть незакрытый батч»).
func (b *mpBatch) append(vals ...any) error {
	if b.batch == nil {
		batch, err := b.conn.PrepareBatch(b.ctx, b.insert)
		if err != nil {
			return fmt.Errorf("%s %s: %w", mpSummaryName, b.table, err)
		}
		b.batch = batch
	}
	if err := b.batch.Append(vals...); err != nil {
		if b.batch != nil {
			mpAbortBatch(b.batch, b.table)
			b.batch = nil
		}
		return fmt.Errorf("%s %s: append: %w", mpSummaryName, b.table, err)
	}
	b.rows++
	return nil
}

// send отправляет накопленное. Батч не открывался — отправлять нечего: пустой Send не
// нужен ни базе (лишняя транзакция), ни отчёту (строк ноль).
func (b *mpBatch) send() error {
	if b.batch == nil {
		return nil
	}
	if err := b.batch.Send(); err != nil {
		return fmt.Errorf("%s %s: send: %w", mpSummaryName, b.table, err)
	}
	b.batch = nil
	return nil
}

// mpAbortBatch закрывает батч на ошибке Append и не теряет сбой закрытия: `_ =` скрыл бы
// причину, по которой батч всё же остался открытым. Возвращается первопричина (Append) —
// она точнее сбоя Abort, случившегося уже после неё (прецедент — forecast/resolve.go).
func mpAbortBatch(batch driver.Batch, what string) {
	if err := batch.Abort(); err != nil {
		log.Warnf("%s %s: не удалось закрыть батч после ошибки: %v", mpSummaryName, what, err)
	}
}

// Витрины контура (спека §3.6): шапка, полный текст и разметка каналов. Таблицы свои,
// поэтому фильтров source в Select нет — только FINAL; комментарии колонок обязательны
// (агент читает контур через них, правило 11).
var (
	mpSummaryView = util.View{
		Name:   "v_cbr_mp_summary",
		Tables: []string{mpSummaryTable},
		Select: `SELECT meeting_date, published, url, decision_action, decision_rate, decision_delta_bp, effective_from, signal FROM ` +
			mpSummaryTable + " FINAL",
		Comment: "Резюме обсуждения ключевой ставки Банка России (материалы заседаний ЦБ; импортёр cbr_mp_summary): дата заседания, дата публикации, решение по ставке, шаг к предыдущему решению в базисных пунктах, дата вступления в силу и сигнал. " +
			"Дата ряда — дата заседания (meeting_date), дата публикации — атрибут (published) и приходит позже заседания. " +
			"Решение (действие, ставка, дата вступления) — факт ЦБ из пятого блока резюме; signal — дословные предложения ЦБ про сигнал, а не пересказ, поэтому формулировка меняется от заседания к заседанию. " +
			"Полный текст — в v_cbr_mp_summary_blocks, разметка каналов — в v_cbr_mp_summary_channels",
		Columns: map[string]string{
			"meeting_date":      "Дата заседания Совета директоров по ключевой ставке; ключ ряда (одно решение на дату)",
			"published":         "Дата публикации резюме на сайте ЦБ (позже даты заседания)",
			"url":               "Страница резюме на сайте ЦБ (провенанс)",
			"decision_action":   "Действие: hold (сохранил)|cut (снизил)|hike (повысил)",
			"decision_rate":     "Ключевая ставка после решения, % годовых",
			"decision_delta_bp": "Шаг к предыдущему решению по meeting_date, базисные пункты; hold даёт 0, у самого раннего резюме NULL",
			"effective_from":    "Дата вступления ставки в силу («с …») для cut/hike; NULL для hold и там, где ЦБ дату не назвал",
			"signal":            "Дословные предложения резюме про сигнал ДКП (пустая строка, если сигнал не сформулирован)",
		},
	}

	mpSummaryBlocksView = util.View{
		Name:   "v_cbr_mp_summary_blocks",
		Tables: []string{mpSummaryBlockTable},
		Select: `SELECT meeting_date, block, subsection, kind, seq, text FROM ` +
			mpSummaryBlockTable + " FINAL",
		Comment: "Полный текст резюме обсуждения ключевой ставки: по строке на абзац или пункт списка, с русским именем блока и подзаголовком. " +
			"Текст дословный (нормализованы только пробелы и неразрывные дефисы), цитаты разметки каналов берутся отсюда: v_cbr_mp_summary_channels ссылается на те же фрагменты. " +
			"Блоки фиксированы (пять обязательных: ситуация и инфляция, денежно-кредитные условия, внешние условия, инфляционные риски, выводы и решение)",
		Columns: map[string]string{
			"meeting_date": "Дата заседания; часть ключа (вместе с block/subsection/kind/seq — порядок текста внутри резюме)",
			"block":        "Русское имя блока резюме (пять фиксированных)",
			"subsection":   "Подзаголовок внутри блока («Основные факты»/«Дискуссия») или пустая строка, если подзаголовков нет",
			"kind":         "paragraph — абзац, bullet — пункт списка",
			"seq":          "Сквозной номер абзаца или пункта внутри блока (дельта по meeting_date)",
			"text":         "Дословный текст абзаца или пункта",
		},
	}

	mpSummaryChannelsView = util.View{
		Name:   "v_cbr_mp_summary_channels",
		Tables: []string{mpSummaryChannelTable},
		Select: `SELECT meeting_date, channel, kind, method, model, prompt_version, seq, text FROM ` +
			mpSummaryChannelTable + " FINAL",
		Comment: "Разметка резюме обсуждения по 11 каналам трансмиссии ДКП (реестр и определения — docs/KEY_RATE_CHANNELS.md): канал, вид фрагмента, метод разметки, модель и версия промпта. " +
			"Разметка эвристическая (regex) и модельная (llm), не позиция ЦБ: text — дословная цитата документа (у слоя llm сверена с текстом резюме), note — краткий вывод модели по каналу, а не цитата. " +
			"Один фрагмент может попасть в несколько каналов; решение о «главных каналах» принимает читающий (агент), опираясь на цитаты. " +
			"Для одного (meeting_date, channel, kind) рядом лежат строки обоих слоёв и всех размеченных моделей — сравнивать их между собой осмысленно, путать нельзя",
		Columns: map[string]string{
			"meeting_date":   "Дата заседания; часть ключа (вместе с channel/kind/method/model/prompt_version/seq)",
			"channel":        "Slug канала из реестра 11 каналов (current_inflation, core_inflation, expectations, labor_market, lending_transmission, output_gap, budget_impulse, money_supply, external_fx, savings_behavior, autonomous_factors)",
			"kind":           "risk_pro — проинфляционный риск, risk_dez — дезинфляционный, prose — прочее утверждение про канал, note — краткий вывод модели по каналу",
			"method":         "regex — детерминированные регексы (слой 1, работает всегда), llm — разметка моделью (слой 2)",
			"model":          "Имя модели LLM для method='llm'; пустая строка для regex",
			"prompt_version": "Версия промпта LLM; 0 для regex",
			"seq":            "Сквозной номер фрагмента в порядке обхода документа; у llm — порядок ответа модели",
			"text":           "Дословная цитата фрагмента; для kind='note' — краткий вывод модели по каналу (не цитата)",
		},
	}
)

// mpSummaryImporter — резюме обсуждения ключевой ставки: Name() = cbr_mp_summary и три
// свои таблицы, поэтому Import() свой (шаблон util.ClickHouseImport ведёт одну таблицу,
// один DataUrl и префлайт). Поля шаблона не используются: CreateTable пуст — DDL трёх
// таблиц исполняет Import(), а PublicationCountPoints/PublicationSource/AfterImport не
// объявлены сознательно (префлайта нет: дельта по индексу, спека §3.1). Регистрация —
// реестром cbr/importers.go; сетевых вызовов в init() нет.
var mpSummaryImporter = &mpSummaryStat{ClickHouseImport: util.ClickHouseImport{
	TableName: mpSummaryTable,
}}
