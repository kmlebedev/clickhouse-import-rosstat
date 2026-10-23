package cbr

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// Импортёр пресс-релизов ЦБ по ключевой ставке — контур cbr_key_rate_press: шапка
// релиза, дословные абзацы и отдельный слой квартальных блоков с числами
// (спека docs/superpowers/specs/2026-10-23-cbr-key-rate-press-design.md).
//
// Артефакт — набор выпусков (8 в год), поэтому Import() свой (прецедент —
// cbr_mp_summary): шаблон util.ClickHouseImport ведёт одну таблицу. Даты заседаний и
// ссылки на релизы берутся из календаря cbr.ru/dkp/cal_mp/ (у 2024+ в карточке есть
// прямой href), текст — со страницы релиза. Гейт — по meeting_date: известность
// выпуска и есть его дата; префлайта нет, потому что count() всей таблицы у известной
// версии не сошёлся бы ни разу (спека §5, отступление от стандарта §9.1 — как у
// cbr_mt_forecast и cbr_mp_summary).
//
// История начинается с 16.02.2024 — первого релиза с квартальным блоком «текущий
// рост цен с поправкой на сезонность»; до 2024 таких блоков в релизах нет (проверено
// на 2019–2023, спека §2).

const (
	// pressName — имя импортёра в CLICKHOUSE_IMPORT_STAT и id шага dagu/cbr.yaml;
	// оно же имя таблицы-шапки (конвенция пакета «Name() == TableName»).
	pressName = "cbr_key_rate_press"

	pressTable        = "cbr_key_rate_press"
	pressBlockTable   = "cbr_key_rate_press_block"
	pressQuarterTable = "cbr_key_rate_press_quarter"

	// pressDayLayout — формат дат в логе и сообщениях импортёра.
	pressDayLayout = "2006-01-02"
)

// DDL трёх таблиц контура — дословные копии канонического DDL (ARCHITECTURE §6.2);
// копии обязаны совпадать, имена таблиц внутри литералов — тоже (тест-пин
// TestCbrKeyRatePressDdlPins).
const (
	pressDdl = `CREATE TABLE IF NOT EXISTS cbr_key_rate_press (
    meeting_date Date,
    published Date,                          -- дата публикации (у релиза = дата заседания, 13:30 МСК)
    url String,
    decision_action LowCardinality(String),  -- 'hold'|'cut'|'hike'
    decision_rate Float64,                   -- % годовых
    decision_delta_bp Nullable(Int16),       -- шаг к предыдущему решению своего ряда; у первого NULL
    effective_from Nullable(Date),           -- «с …» для cut/hike; NULL для hold
    next_meeting_date Nullable(Date),
    signal String                            -- дословные предложения про сигнал
) ENGINE = ReplacingMergeTree ORDER BY meeting_date;`

	pressBlockDdl = `CREATE TABLE IF NOT EXISTS cbr_key_rate_press_block (
    meeting_date Date,
    block LowCardinality(String),  -- decision|guidance|inflation_quarter|expectations|activity|labor|monetary|credit|risks|budget|forecast|publications|next_meeting|other
    seq UInt16,                    -- порядок абзаца в релизе (истина; block — эвристика)
    text String
) ENGINE = ReplacingMergeTree ORDER BY (meeting_date, seq);`

	pressQuarterDdl = `CREATE TABLE IF NOT EXISTS cbr_key_rate_press_quarter (
    meeting_date Date,
    seq UInt16,                    -- seq того же абзаца в block-таблице
    published Date,
    url String,
    period LowCardinality(String), -- '3к26' | 'июль' | 'октябрь — ноябрь' | 'II квартал 2024'
    form LowCardinality(String),   -- 'quarter' | 'month' | 'none'
    headline Nullable(Float64),            -- текущий рост цен, % SAAR
    headline_prev Nullable(Float64),
    headline_prev2 Nullable(Float64),      -- второй базис («… и 4,3% в 4к25»)
    core Nullable(Float64),                -- базовая инфляция, % SAAR
    core_prev Nullable(Float64),
    core_prev2 Nullable(Float64),
    annual_inflation Nullable(Float64),    -- годовая инфляция, оценка ЦБ
    annual_asof Nullable(Date),            -- «по оценке на 19 октября»
    parsed UInt8,                          -- 1 — числа разобраны; 0 — только текст
    text String                            -- дословный абзац
) ENGINE = ReplacingMergeTree ORDER BY (meeting_date, seq);`
)

// Порядок колонок в INSERT — контракт с порядком Append (проверка ошибок Append
// обязательна: без неё строка молча теряется).
const (
	pressInsert        = "INSERT INTO " + pressTable + " (meeting_date, published, url, decision_action, decision_rate, decision_delta_bp, effective_from, next_meeting_date, signal)"
	pressBlockInsert   = "INSERT INTO " + pressBlockTable + " (meeting_date, block, seq, text)"
	pressQuarterInsert = "INSERT INTO " + pressQuarterTable + " (meeting_date, seq, published, url, period, form, " +
		"headline, headline_prev, headline_prev2, core, core_prev, core_prev2, annual_inflation, annual_asof, parsed, text)"
)

// Запросы чтения. FINAL обязателен: ключи таблиц — ReplacingMergeTree, и без него
// несмёрженные части дали бы дубли дат и повторные матчи.
const (
	// pressKnownQuery — даты уже импортированных релизов: гейт прогона.
	pressKnownQuery = "SELECT meeting_date FROM " + pressTable + " FINAL"

	// pressBlockDatesQuery — даты, у которых есть хотя бы один абзац: шапка без
	// блоков — недописанный документ (сбой батча блоков), и он перекачивается.
	pressBlockDatesQuery = "SELECT DISTINCT meeting_date FROM " + pressBlockTable + " FINAL"

	// pressQuarterBlockPairsQuery — пары (дата, seq) квартальных абзацев: по ним
	// проверяется, что у каждого такого абзаца есть строка квартала (самолечение
	// сбоя третьего батча).
	pressQuarterBlockPairsQuery = "SELECT meeting_date, seq FROM " + pressBlockTable + " FINAL WHERE block = '" + pressQuarterBlock + "'"

	// pressQuarterPairsQuery — пары (дата, seq), записанные в таблицу кварталов.
	pressQuarterPairsQuery = "SELECT meeting_date, seq FROM " + pressQuarterTable + " FINAL"

	// pressPastQuery — прошлые решения для decision_delta_bp: ставки по порядку заседаний.
	pressPastQuery = "SELECT meeting_date, decision_rate FROM " + pressTable + " FINAL ORDER BY meeting_date"

	// pressCountQuery — число релизов таблицы: сообщение no-op называет его.
	pressCountQuery = "SELECT count() FROM " + pressTable + " FINAL"
)

var (
	// pressHistoryStart — первый релиз с квартальным блоком (16.02.2024). Дата
	// литералом, а не вычислением: история источника — решение, названное в спеке §2.
	pressHistoryStart = time.Date(2024, time.February, 16, 0, 0, 0, 0, time.UTC)

	// pressMoscow — МСК фиксированным смещением UTC+3 (без DST): «сегодня» для
	// правила «релиз сегодняшнего дня до 13:30 ещё не опубликован» не должно
	// зависеть от часового пояса хоста (dev-машина живёт в UTC+5, staging — в UTC).
	pressMoscow = time.FixedZone("MSK", 3*60*60)
)

// pressStat — импортёр контура пресс-релизов: Name() = cbr_key_rate_press, три свои
// таблицы, поэтому Import() свой.
type pressStat struct {
	util.ClickHouseImport
}

func (s *pressStat) Name() string { return pressName }

// pressFetched — скачанный и разобранный выпуск: адрес (провенанс) и документ.
type pressFetched struct {
	Meeting pressMeeting
	URL     string
	Doc     *pressDoc
}

// pressPast — решение, уже лежащее в таблице: ставка для дельты.
type pressPast struct {
	Date time.Time
	Rate float64
}

// Import — полный цикл прогона (спека §5): DDL → витрины и каталог (самолечение на
// no-op) → календарь → отбор дат → гейт (шапки + неполные документы) → скачивание и
// разбор новых выпусков → три батча. Сбой одного выпуска не отменяет остальные:
// ошибки копятся errors.Join, прогон завершается ошибкой — dagu видит красный шаг.
func (s *pressStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, ddl := range []string{pressDdl, pressBlockDdl, pressQuarterDdl} {
		if err = conn.Exec(ctx, ddl); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", pressName, err)
		}
	}

	for _, view := range []util.View{pressView, pressBlocksView, pressQuartersView} {
		if _, err = util.CreateView(ctx, conn, view); err != nil {
			return 0, err
		}
	}
	if err = util.UpsertSeriesCatalog(ctx, conn, pressSeriesMeta); err != nil {
		return 0, fmt.Errorf("%s: каталог рядов: %w", pressName, err)
	}

	calendarURL := pressSiteURL + pressCalendarPath
	page, err := util.GetPage(calendarURL)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", pressName, calendarURL, err)
	}
	meetings, err := pressCalendarMeetings(page)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", pressName, err)
	}

	today := pressToday()
	wanted := pressWantedMeetings(meetings, pressHistoryStart, today)
	if len(wanted) == 0 {
		return 0, fmt.Errorf("%s: календарь не дал ни одного заседания с %s — вёрстка или даты источника изменились",
			pressName, pressHistoryStart.Format(pressDayLayout))
	}

	known, err := pressDateSet(ctx, conn, pressKnownQuery)
	if err != nil {
		return 0, err
	}
	blockDates, err := pressDateSet(ctx, conn, pressBlockDatesQuery)
	if err != nil {
		return 0, err
	}
	quarterGaps, err := pressQuarterGaps(ctx, conn)
	if err != nil {
		return 0, err
	}

	fresh := pressMissingMeetings(wanted, known, blockDates, quarterGaps)
	if len(fresh) == 0 {
		var knownCount uint64
		if err = conn.QueryRow(ctx, pressCountQuery).Scan(&knownCount); err != nil {
			return 0, fmt.Errorf("%s: число релизов таблицы не прочитано: %w", pressName, err)
		}
		log.Infof("%s: новых выпусков нет (релизов в таблице: %d; заседаний в календаре: %d с %s)",
			pressName, knownCount, len(wanted), pressHistoryStart.Format(pressDayLayout))
		return 0, nil
	}

	past, err := pressPastDecisions(ctx, conn)
	if err != nil {
		return 0, err
	}

	var (
		docs []pressFetched
		errs []error
	)
	for _, meeting := range fresh {
		if meeting.Href == "" {
			// Ссылки нет у будущих заседаний; у прошлого это смена вёрстки календаря.
			if meeting.Date.Before(today) {
				errs = append(errs, fmt.Errorf("%s: заседание %s: в календаре нет ссылки на пресс-релиз — вёрстка календаря изменилась",
					pressName, meeting.Date.Format(pressDayLayout)))
			}
			continue
		}
		url := pressSiteURL + meeting.Href
		releasePage, pageErr := util.GetPage(url)
		if pageErr != nil {
			errs = append(errs, fmt.Errorf("%s: %s: %w", pressName, url, pageErr))
			continue
		}
		doc, parseErr := pressParseDoc(releasePage, meeting.Date)
		if parseErr != nil {
			// Релиз сегодняшнего заседания выходит в 13:30 МСК: ежедневный шаг dagu
			// в 09:17 видит страницу-заглушку. Это не сбой источника — warn и до
			// следующего прогона; для прошлых дат то же состояние — ошибка.
			if errors.Is(parseErr, errPressNotPublished) && pressSameDay(meeting.Date, today) {
				log.Warnf("%s: %s: релиз ещё не опубликован (пресс-релиз выходит в 13:30 МСК) — пропуск до следующего прогона",
					pressName, meeting.Date.Format(pressDayLayout))
				continue
			}
			errs = append(errs, fmt.Errorf("%s: %s: %w", pressName, url, parseErr))
			continue
		}
		docs = append(docs, pressFetched{Meeting: meeting, URL: url, Doc: doc})
	}
	if len(docs) == 0 {
		if len(errs) > 0 {
			return 0, errors.Join(errs...)
		}
		log.Infof("%s: новых выпусков нет (сегодняшний релиз ещё не опубликован)", pressName)
		return 0, nil
	}

	deltas := pressDeltas(past, docs)

	header := &pressBatch{ctx: ctx, conn: conn, table: pressTable, insert: pressInsert}
	blockRows := &pressBatch{ctx: ctx, conn: conn, table: pressBlockTable, insert: pressBlockInsert}
	quarterRows := &pressBatch{ctx: ctx, conn: conn, table: pressQuarterTable, insert: pressQuarterInsert}
	for _, d := range docs {
		date := mpDay(d.Meeting.Date)
		if err = header.append(date, date, d.URL, d.Doc.Action, d.Doc.Rate, deltas[date],
			d.Doc.EffectiveFrom, d.Doc.NextMeetingDate, d.Doc.Signal); err != nil {
			return 0, err
		}
		for _, b := range d.Doc.Blocks {
			if err = blockRows.append(date, b.Block, uint16(b.Seq), b.Text); err != nil {
				return 0, err
			}
		}
		for _, q := range d.Doc.Quarters {
			if err = quarterRows.append(date, uint16(q.Seq), date, d.URL, q.Period, q.Form,
				q.Headline, q.HeadlinePrev, q.HeadlinePrev2, q.Core, q.CorePrev, q.CorePrev2,
				q.AnnualInflation, q.AnnualAsof, pressBoolUint8(q.Parsed), q.Text); err != nil {
				return 0, err
			}
		}
	}
	if err = header.send(); err != nil {
		blockRows.abort()
		quarterRows.abort()
		return 0, err
	}
	if err = blockRows.send(); err != nil {
		quarterRows.abort()
		return 0, err
	}
	if err = quarterRows.send(); err != nil {
		return 0, err
	}

	count = int64(len(docs))
	log.Infof("%s: импортировано релизов: %d", pressName, count)
	return count, errors.Join(errs...)
}

// pressWantedMeetings — заседания в окне истории: от pressHistoryStart до сегодня
// включительно. Будущие заседания календаря не импортируются: релиза у них ещё нет.
func pressWantedMeetings(meetings []pressMeeting, start, today time.Time) []pressMeeting {
	out := make([]pressMeeting, 0, len(meetings))
	for _, m := range meetings {
		date := mpDay(m.Date)
		if date.Before(start) || date.After(today) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// pressMissingMeetings — что перекачать: неизвестные даты, шапки без блоков и даты,
// у которых квартальный абзац остался без строки квартала (самолечение сбоя батча).
func pressMissingMeetings(wanted []pressMeeting, known, blockDates, quarterGaps map[time.Time]bool) []pressMeeting {
	out := make([]pressMeeting, 0, len(wanted))
	for _, m := range wanted {
		date := mpDay(m.Date)
		if !known[date] || !blockDates[date] || quarterGaps[date] {
			out = append(out, m)
		}
	}
	return out
}

// pressDateSet читает множество дат из запроса. distinct — запрос уже отдаёт
// DISTINCT (у блочной таблицы ключ шире даты); дубли не мешают, множество одно.
func pressDateSet(ctx context.Context, conn driver.Conn, query string) (map[time.Time]bool, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("%s: даты не прочитаны: %w", pressName, err)
	}
	defer func() { _ = rows.Close() }()
	out := make(map[time.Time]bool)
	for rows.Next() {
		var date time.Time
		if err = rows.Scan(&date); err != nil {
			return nil, fmt.Errorf("%s: строка дат не прочитана: %w", pressName, err)
		}
		out[mpDay(date)] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: даты не дочитаны: %w", pressName, err)
	}
	return out, nil
}

// pressQuarterGaps — даты, у которых квартальный абзац есть, а строки квартала нет:
// признак недописанного третьего батча. Пары сравниваются по (meeting_date, seq).
func pressQuarterGaps(ctx context.Context, conn driver.Conn) (map[time.Time]bool, error) {
	blocks, err := pressPairsSet(ctx, conn, pressQuarterBlockPairsQuery)
	if err != nil {
		return nil, err
	}
	quarters, err := pressPairsSet(ctx, conn, pressQuarterPairsQuery)
	if err != nil {
		return nil, err
	}
	gaps := make(map[time.Time]bool)
	for key := range blocks {
		if !quarters[key] {
			gaps[key.Date] = true
		}
	}
	return gaps, nil
}

// pressPairKey — ключ пары (дата, seq) для проверки полноты документа.
type pressPairKey struct {
	Date time.Time
	Seq  uint16
}

// pressPairsSet читает пары (meeting_date, seq).
func pressPairsSet(ctx context.Context, conn driver.Conn, query string) (map[pressPairKey]bool, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("%s: пары (дата, seq) не прочитаны: %w", pressName, err)
	}
	defer func() { _ = rows.Close() }()
	out := make(map[pressPairKey]bool)
	for rows.Next() {
		var (
			date time.Time
			seq  uint16
		)
		if err = rows.Scan(&date, &seq); err != nil {
			return nil, fmt.Errorf("%s: пара (дата, seq) не прочитана: %w", pressName, err)
		}
		out[pressPairKey{Date: mpDay(date), Seq: seq}] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: пары (дата, seq) не дочитаны: %w", pressName, err)
	}
	return out, nil
}

// pressPastDecisions — прошлые решения таблицы для дельты шага.
func pressPastDecisions(ctx context.Context, conn driver.Conn) ([]pressPast, error) {
	rows, err := conn.Query(ctx, pressPastQuery)
	if err != nil {
		return nil, fmt.Errorf("%s: прошлые решения не прочитаны: %w", pressName, err)
	}
	defer func() { _ = rows.Close() }()
	var out []pressPast
	for rows.Next() {
		var (
			date time.Time
			rate float64
		)
		if err = rows.Scan(&date, &rate); err != nil {
			return nil, fmt.Errorf("%s: прошлое решение не прочитано: %w", pressName, err)
		}
		out = append(out, pressPast{Date: mpDay(date), Rate: rate})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: прошлые решения не дочитаны: %w", pressName, err)
	}
	return out, nil
}

// pressDeltas — decision_delta_bp новых выпусков: шаг к предыдущему решению всего
// ряда (прошлые строки + новые), по порядку заседаний. У самого раннего — NULL:
// предыдущего решения в истории источника нет (до 2024-02 релизы не импортируются).
func pressDeltas(past []pressPast, docs []pressFetched) map[time.Time]*int16 {
	rates := make(map[time.Time]float64, len(past)+len(docs))
	for _, p := range past {
		rates[p.Date] = p.Rate
	}
	for _, d := range docs {
		rates[mpDay(d.Meeting.Date)] = d.Doc.Rate
	}
	dates := make([]time.Time, 0, len(rates))
	for date := range rates {
		dates = append(dates, date)
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })

	out := make(map[time.Time]*int16, len(docs))
	for _, d := range docs {
		date := mpDay(d.Meeting.Date)
		idx := sort.Search(len(dates), func(i int) bool { return !dates[i].Before(date) })
		if idx <= 0 || idx >= len(dates) || !dates[idx].Equal(date) {
			continue
		}
		delta := int16(math.Round((rates[date] - rates[dates[idx-1]]) * 100))
		out[date] = &delta
	}
	return out
}

// pressToday — сегодняшняя дата по Москве, нормализованная к суткам UTC (как mpDay):
// сравнение с датами календаря идёт по календарному дню.
func pressToday() time.Time {
	now := time.Now().In(pressMoscow)
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
}

// pressSameDay — одна и та же календарная дата.
func pressSameDay(a, b time.Time) bool { return mpDay(a).Equal(mpDay(b)) }

// pressBoolUint8 — bool в UInt8 для ClickHouse (parsed).
func pressBoolUint8(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}

// pressBatch — ленивый батч одной таблицы: открывается на первой строке и не
// открывается вовсе, если строк не нашлось. Копия mpBatch с именем своего импортёра
// в ошибках: mpBatch подписывает сбой cbr_mp_summary, и чужое имя в логе сбивало бы
// оператора (тот же приём, что у per-importer батчей пакета).
type pressBatch struct {
	ctx    context.Context
	conn   driver.Conn
	table  string
	insert string
	rows   int64
	batch  driver.Batch
}

// append добавляет строку батча; ошибка Append закрывает открытый батч (иначе
// соединение не вернулось бы в пул до конца прогона).
func (b *pressBatch) append(vals ...any) error {
	if b.batch == nil {
		batch, err := b.conn.PrepareBatch(b.ctx, b.insert)
		if err != nil {
			return fmt.Errorf("%s %s: %w", pressName, b.table, err)
		}
		b.batch = batch
	}
	if err := b.batch.Append(vals...); err != nil {
		b.abort()
		return fmt.Errorf("%s %s: append: %w", pressName, b.table, err)
	}
	b.rows++
	return nil
}

// send отправляет накопленное; пустой батч — no-op (лишняя транзакция не нужна).
// На сбое Send батч помечается использованным, Abort не вызывается (ErrBatchAlreadySent).
func (b *pressBatch) send() error {
	if b.batch == nil {
		return nil
	}
	if err := b.batch.Send(); err != nil {
		b.batch = nil
		return fmt.Errorf("%s %s: send: %w", pressName, b.table, err)
	}
	b.batch = nil
	return nil
}

// abort закрывает НЕ отправлявшийся открытый батч: на пути ошибки соединение должно
// вернуться в пул. Идемпотентен: закрытый или неоткрывавшийся батч — no-op.
func (b *pressBatch) abort() {
	if b.batch == nil {
		return
	}
	if err := b.batch.Abort(); err != nil {
		log.Warnf("%s %s: не удалось закрыть батч после ошибки: %v", pressName, b.table, err)
	}
	b.batch = nil
}

// Витрины контура (спека §6): шапка, абзацы и квартальные блоки. Таблицы свои,
// поэтому фильтров source в Select нет — только FINAL; комментарии колонок
// обязательны (агент читает контур через них, правило 11).
var (
	pressView = util.View{
		Name:   "v_cbr_key_rate_press",
		Tables: []string{pressTable},
		Select: `SELECT meeting_date, published, url, decision_action, decision_rate, decision_delta_bp, effective_from, next_meeting_date, signal FROM ` +
			pressTable + " FINAL",
		Comment: "Пресс-релизы Банка России по ключевой ставке (импортёр cbr_key_rate_press): дата заседания, решение по ставке, шаг к предыдущему решению, дата вступления в силу, следующее заседание и дословный сигнал. " +
			"Дата ряда — дата заседания (meeting_date); у пресс-релиза она же дата публикации (13:30 МСК), поэтому published повторяет meeting_date. " +
			"Решение — факт ЦБ из первого абзаца релиза; signal — дословные предложения про дальнейшую политику, а не пересказ. " +
			"Полный текст — в v_cbr_key_rate_press_blocks, квартальные блоки с числами — в v_cbr_key_rate_press_quarters. История с 16.02.2024",
		Columns: map[string]string{
			"meeting_date":      "Дата заседания Совета директоров по ключевой ставке; ключ ряда (один релиз на дату)",
			"published":         "Дата публикации пресс-релиза; у релиза совпадает с датой заседания (публикация в 13:30 МСК)",
			"url":               "Страница пресс-релиза на сайте ЦБ (провенанс)",
			"decision_action":   "Действие: hold (сохранил)|cut (снизил)|hike (повысил)",
			"decision_rate":     "Ключевая ставка после решения, % годовых",
			"decision_delta_bp": "Шаг к предыдущему решению по meeting_date, базисные пункты; hold даёт 0, у первого релиза истории NULL",
			"effective_from":    "Дата вступления ставки в силу («с …») для cut/hike; NULL для hold и там, где ЦБ дату не назвал",
			"next_meeting_date": "Дата следующего заседания по ключевой ставке, названная в релизе",
			"signal":            "Дословные предложения релиза про сигнал ДКП (пустая строка, если сигнал не сформулирован)",
		},
	}

	pressBlocksView = util.View{
		Name:   "v_cbr_key_rate_press_blocks",
		Tables: []string{pressBlockTable},
		Select: `SELECT meeting_date, block, seq, text FROM ` + pressBlockTable + " FINAL",
		Comment: "Полный текст пресс-релиза ЦБ по ключевой ставке: по строке на абзац, в порядке релиза. Текст дословный (нормализованы только пробелы и неразрывные дефисы). " +
			"block — эвристический слаг абзаца, истина — seq и текст: decision, guidance (сигнал), inflation_quarter (квартальный блок), expectations, activity, labor, monetary, credit, risks, budget, forecast, publications, next_meeting, other. " +
			"Квартальные блоки с разобранными числами — в v_cbr_key_rate_press_quarters",
		Columns: map[string]string{
			"meeting_date": "Дата заседания; часть ключа (вместе с seq — порядок абзаца в релизе)",
			"block":        "Слаг абзаца (эвристика по ключевым словам; истина — seq и текст)",
			"seq":          "Порядковый номер абзаца в релизе с 1",
			"text":         "Дословный текст абзаца",
		},
	}

	pressQuartersView = util.View{
		Name:   "v_cbr_key_rate_press_quarters",
		Tables: []string{pressQuarterTable},
		Select: `SELECT meeting_date, seq, published, url, period, form, headline, headline_prev, headline_prev2, core, core_prev, core_prev2, annual_inflation, annual_asof, parsed, text FROM ` +
			pressQuarterTable + " FINAL",
		Comment: "Квартальные блоки пресс-релиза ЦБ по ключевой ставке — числа, которые ЦБ сравнивает в тексте решения (KEY_RATE_PUBLICATIONS.md §4.7): текущий рост цен и базовая инфляция с поправкой на сезонность, предыдущие значения и второй базис, годовая инфляция с датой оценки. " +
			"Форма сравнения — form: quarter (квартал к кварталу: «В 3к26 … после 4,8% в 2к26») или month (месяц/месяцы к среднему за прошлый квартал: «В июле … после 5,3% в среднем в 2к26»); period — период в тексте релиза. " +
			"Это числа ЦБ, а не расчёт проекта: ревизия между выпусками — свойство ряда, а не ошибка. parsed=0 — стандартная пара не разобрана, числа NULL, источник истины — text. " +
			"Годовая инфляция — оценка ЦБ на annual_asof, а не месячный ИПЦ Росстата. Текст дословный, как в v_cbr_key_rate_press_blocks",
		Columns: map[string]string{
			"meeting_date":     "Дата заседания; часть ключа (вместе с seq — номер абзаца)",
			"seq":              "Номер квартального абзаца в релизе (совпадает с seq в v_cbr_key_rate_press_blocks)",
			"published":        "Дата публикации релиза (совпадает с датой заседания)",
			"url":              "Страница пресс-релиза (провенанс)",
			"period":           "Период сравнения в тексте: «3к26», «июль», «октябрь — ноябрь», «II квартал 2024»",
			"form":             "Форма сравнения: quarter — квартал к кварталу; month — месяц/месяцы к среднему за прошлый квартал; none — чисел нет",
			"headline":         "«Текущий рост цен с поправкой на сезонность», % в пересчёте на год (SAAR)",
			"headline_prev":    "Предыдущее значение текущего роста цен, % SAAR",
			"headline_prev2":   "Второй предыдущий базис, если ЦБ назвал два («… и 4,3% в 4к25»)",
			"core":             "«Показатель базовой инфляции», % SAAR",
			"core_prev":        "Предыдущее значение базовой инфляции, % SAAR",
			"core_prev2":       "Второй предыдущий базис базовой инфляции, если назван",
			"annual_inflation": "Годовая инфляция, оценка ЦБ на annual_asof, %",
			"annual_asof":      "Дата оценки годовой инфляции («по оценке на 19 октября»); NULL, если ЦБ дату не назвал",
			"parsed":           "1 — числа разобраны; 0 — стандартная пара не найдена, числа NULL, читать text",
			"text":             "Дословный квартальный абзац",
		},
	}
)

// pressSeriesMeta — каталог пяти рядов контура: агент читает его перед запросом
// (unit/frequency/origin/description). Frequency — irregular: релизы выходят
// 8 раз в год, привязка — дата заседания.
var pressSeriesMeta = []util.SeriesMeta{
	{
		Source:      pressName,
		Series:      "decision_rate",
		Title:       "Ключевая ставка после решения (пресс-релиз ЦБ)",
		Unit:        "% годовых",
		Frequency:   "irregular",
		Origin:      "cbr.ru/press/pr/?file=… — первый абзац пресс-релиза по ключевой ставке",
		Description: "Ключевая ставка после решения, % годовых; дата точки — дата заседания. Одна точка на заседание: hold — ставка не изменилась.",
	},
	{
		Source:      pressName,
		Series:      "decision_delta_bp",
		Title:       "Шаг ключевой ставки к предыдущему решению",
		Unit:        "б.п.",
		Frequency:   "irregular",
		Origin:      "cbr.ru/press/pr/?file=… — разность decision_rate с предыдущим релизом ряда",
		Description: "Изменение ключевой ставки к предыдущему решению, базисные пункты; hold даёт 0. У первого релиза истории (16.02.2024) NULL: предыдущего решения в ряду нет.",
	},
	{
		Source:      pressName,
		Series:      "headline_saar",
		Title:       "Текущий рост цен с поправкой на сезонность (квартальный блок)",
		Unit:        "% в пересчёте на год",
		Frequency:   "irregular",
		Origin:      "cbr.ru/press/pr/?file=… — квартальный блок «текущий рост цен с поправкой на сезонность»",
		Description: "Число ЦБ из квартального блока: SAAR закрытого квартала (form=quarter) или месяц/месяцы к среднему за прошлый квартал (form=month). Это не расчёт проекта и не недельный SAAR; ревизия между выпусками — свойство ряда. Предыдущие значения — headline_prev/headline_prev2.",
	},
	{
		Source:      pressName,
		Series:      "core_saar",
		Title:       "Базовая инфляция с поправкой на сезонность (квартальный блок)",
		Unit:        "% в пересчёте на год",
		Frequency:   "irregular",
		Origin:      "cbr.ru/press/pr/?file=… — тот же абзац, «показатель базовой инфляции»",
		Description: "Базовая инфляция SAAR из квартального блока пресс-релиза: форма и период совпадают с headline_saar. Предыдущие значения — core_prev/core_prev2. Сравнивать с «Базовый ИПЦ» (v_cbr_macro) на дату релиза, а не с последним винтажом.",
	},
	{
		Source:      pressName,
		Series:      "annual_inflation",
		Title:       "Годовая инфляция, оценка ЦБ на дату релиза",
		Unit:        "%",
		Frequency:   "irregular",
		Origin:      "cbr.ru/press/pr/?file=… — «годовая инфляция, по оценке на …»",
		Description: "Оценка годовой инфляции ЦБ по оперативным данным на annual_asof, %. Это не месячный ИПЦ Росстата и не расчёт проекта: дата оценки может быть позже последней месячной публикации.",
	},
}

// cbrKeyRatePressImporter — импортёр контура: Name() = cbr_key_rate_press и три свои
// таблицы, поэтому Import() свой. Поля шаблона не используются: CreateTable пуст —
// DDL трёх таблиц исполняет Import(); PublicationCountPoints/PublicationSource/
// AfterImport не объявлены сознательно (префлайта нет: гейт по meeting_date, спека §5;
// витрины и каталог публикует Import). Регистрация — реестром cbr/importers.go;
// сетевых вызовов в init() нет.
var cbrKeyRatePressImporter = &pressStat{ClickHouseImport: util.ClickHouseImport{
	TableName: pressTable,
}}
