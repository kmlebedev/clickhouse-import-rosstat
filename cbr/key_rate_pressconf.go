package cbr

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// Импортёр стенограмм пресс-конференций ЦБ по ключевой ставке — контур
// cbr_key_rate_pressconf: шапка видео и реплики с таймкодами (спека
// docs/superpowers/specs/2026-10-23-cbr-key-rate-pressconf-design.md).
//
// Источник — плейлист YouTube ЦБ (23 конференции с 16.02.2024) и player API с
// русскими автосубтитрами. Артефакт — набор видео, а не файл, поэтому Import() свой
// (прецедент — cbr_key_rate_press): шаблон util.ClickHouseImport ведёт одну таблицу.
// Гейт — по meeting_date: известность выпуска и есть его дата; префлайта нет, потому
// что count() всей таблицы у известной версии не сошёлся бы ни разу (спека §5).

const (
	// pressconfName — имя импортёра в CLICKHOUSE_IMPORT_STAT и id шага dagu/cbr.yaml;
	// оно же имя таблицы-шапки (конвенция пакета «Name() == TableName»).
	pressconfName = "cbr_key_rate_pressconf"

	pressconfTable    = "cbr_key_rate_pressconf"
	pressconfCueTable = "cbr_key_rate_pressconf_cue"

	// pressconfDayLayout — формат дат в логе и сообщениях импортёра.
	pressconfDayLayout = "2006-01-02"
)

// DDL двух таблиц контура — дословные копии канонического DDL (ARCHITECTURE §6.2);
// копии обязаны совпадать, имена таблиц внутри литералов — тоже (тест-пин
// TestCbrKeyRatePressconfDdlPins).
const (
	pressconfDdl = `CREATE TABLE IF NOT EXISTS cbr_key_rate_pressconf (
    meeting_date Date,
    published Date,                          -- = meeting_date: эфир в день решения
    video_id String,
    url String,                              -- https://www.youtube.com/watch?v=<video_id>
    title String,
    duration_ms UInt32,
    caption_lang LowCardinality(String),     -- 'ru'
    caption_kind LowCardinality(String),     -- 'asr' | 'manual'
    cues UInt16,                             -- число записанных реплик
    loaded_at DateTime('Europe/Moscow') DEFAULT now()  -- версия ReplacingMergeTree
) ENGINE = ReplacingMergeTree(loaded_at) ORDER BY meeting_date;`

	pressconfCueDdl = `CREATE TABLE IF NOT EXISTS cbr_key_rate_pressconf_cue (
    meeting_date Date,
    seq UInt16,                              -- порядок реплики с 1
    start_ms UInt32,
    end_ms UInt32,
    text String                              -- дословная реплика ASR
) ENGINE = ReplacingMergeTree ORDER BY (meeting_date, seq);`
)

// Порядок колонок в INSERT — контракт с порядком Append (проверка ошибок Append
// обязательна: без неё строка молча теряется). loaded_at заполняет DEFAULT now().
const (
	pressconfInsert    = "INSERT INTO " + pressconfTable + " (meeting_date, published, video_id, url, title, duration_ms, caption_lang, caption_kind, cues)"
	pressconfCueInsert = "INSERT INTO " + pressconfCueTable + " (meeting_date, seq, start_ms, end_ms, text)"
)

// Запросы чтения. FINAL обязателен: ключи таблиц — ReplacingMergeTree, и без него
// несмёрженные части дали бы дубли дат и повторные матчи.
const (
	// pressconfHeadersQuery — шапки уже импортированных конференций и ожидаемое число
	// реплик: по нему проверяется полнота документа.
	pressconfHeadersQuery = "SELECT meeting_date, cues FROM " + pressconfTable + " FINAL"

	// pressconfCueCountsQuery — сколько реплик реально лежит по каждой дате.
	pressconfCueCountsQuery = "SELECT meeting_date, count() FROM " + pressconfCueTable + " FINAL GROUP BY meeting_date"

	// pressconfCountQuery / pressconfCueTotalQuery — числа для сообщения no-op.
	pressconfCountQuery    = "SELECT count() FROM " + pressconfTable + " FINAL"
	pressconfCueTotalQuery = "SELECT count() FROM " + pressconfCueTable + " FINAL"
)

var (
	// pressconfHistoryStart — первая пресс-конференция истории контура (16.02.2024).
	// Дата литералом, а не вычислением: история источника — решение, названное в
	// спеке §2 (как у cbr_key_rate_press и cbr_mp_summary).
	pressconfHistoryStart = time.Date(2024, time.February, 16, 0, 0, 0, 0, time.UTC)

	// pressconfMoscow — МСК фиксированным смещением UTC+3 (без DST): «сегодня» для
	// правила «субтитры сегодняшней конференции могут быть ещё не готовы» не должно
	// зависеть от часового пояса хоста.
	pressconfMoscow = time.FixedZone("MSK", 3*60*60)
)

// pressconfStat — импортёр контура: Name() = cbr_key_rate_pressconf, две свои таблицы,
// поэтому Import() свой.
type pressconfStat struct {
	util.ClickHouseImport
}

func (s *pressconfStat) Name() string { return pressconfName }

// pressconfDoc — скачанная и разобранная конференция: позиция плейлиста, метаданные
// видео и реплики субтитров.
type pressconfDoc struct {
	Item        pressconfItem
	URL         string
	Title       string
	DurationMs  uint32
	CaptionLang string
	CaptionKind string
	Cues        []pressconfCue
}

// Import — полный цикл прогона (спека §5): DDL → витрины → плейлист → разбор →
// окно истории → гейт (шапки + неполные документы) → player/субтитры → два батча.
// Сбой одной конференции не отменяет остальные: ошибки копятся errors.Join, прогон
// завершается ошибкой — dagu видит красный шаг.
func (s *pressconfStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, ddl := range []string{pressconfDdl, pressconfCueDdl} {
		if err = conn.Exec(ctx, ddl); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", pressconfName, err)
		}
	}

	for _, view := range []util.View{pressconfView, pressconfCuesView, pressconfTextView} {
		if _, err = util.CreateView(ctx, conn, view); err != nil {
			return 0, err
		}
	}

	page, err := pressconfGetPage(pressconfPlaylistURL)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", pressconfName, pressconfPlaylistURL, err)
	}
	visitor, apiKey, err := pressconfPageConfig(page)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", pressconfName, err)
	}
	items, err := pressconfPlaylistItems(page)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", pressconfName, err)
	}

	today := pressconfToday()
	wanted := pressconfWanted(items, pressconfHistoryStart, today)
	if len(wanted) == 0 {
		return 0, fmt.Errorf("%s: плейлист не дал ни одной пресс-конференции с %s — вёрстка или заголовки изменились",
			pressconfName, pressconfHistoryStart.Format(pressconfDayLayout))
	}

	headers, err := pressconfHeaders(ctx, conn)
	if err != nil {
		return 0, err
	}
	cueCounts, err := pressconfCueCounts(ctx, conn)
	if err != nil {
		return 0, err
	}

	missing := pressconfMissing(wanted, headers, cueCounts)
	if len(missing) == 0 {
		var headerCount, cueCount uint64
		if err = conn.QueryRow(ctx, pressconfCountQuery).Scan(&headerCount); err != nil {
			return 0, fmt.Errorf("%s: число конференций таблицы не прочитано: %w", pressconfName, err)
		}
		if err = conn.QueryRow(ctx, pressconfCueTotalQuery).Scan(&cueCount); err != nil {
			return 0, fmt.Errorf("%s: число реплик таблицы не прочитано: %w", pressconfName, err)
		}
		log.Infof("%s: новых пресс-конференций нет (в таблице: %d; реплик: %d; в плейлисте: %d с %s)",
			pressconfName, headerCount, cueCount, len(wanted), pressconfHistoryStart.Format(pressconfDayLayout))
		return 0, nil
	}

	var (
		docs []pressconfDoc
		errs []error
	)
	for _, item := range missing {
		doc, fetchErr := pressconfFetch(item, visitor, apiKey)
		if fetchErr != nil {
			// Субтитры сегодняшней конференции YouTube генерирует с задержкой: ежедневный
			// шаг dagu в 09:17 видит вчерашнюю уже готовой, а ручной прогон сразу после
			// эфира — ещё нет. Это не сбой источника: warn и до следующего прогона.
			if errors.Is(fetchErr, errPressconfNoCaptions) && pressconfSameDay(item.Date, today) {
				log.Warnf("%s: %s: субтитры ещё не готовы — пропуск до следующего прогона",
					pressconfName, item.Date.Format(pressconfDayLayout))
				continue
			}
			errs = append(errs, fetchErr)
			continue
		}
		docs = append(docs, doc)
	}
	if len(docs) == 0 {
		if len(errs) > 0 {
			return 0, errors.Join(errs...)
		}
		log.Infof("%s: новых пресс-конференций нет (сегодняшние субтитры ещё не готовы)", pressconfName)
		return 0, nil
	}

	header := &pressconfBatch{ctx: ctx, conn: conn, table: pressconfTable, insert: pressconfInsert}
	cueRows := &pressconfBatch{ctx: ctx, conn: conn, table: pressconfCueTable, insert: pressconfCueInsert}
	var cueTotal int64
	for _, doc := range docs {
		date := mpDay(doc.Item.Date)
		if err = header.append(date, date, doc.Item.VideoID, doc.URL, doc.Title, doc.DurationMs,
			doc.CaptionLang, doc.CaptionKind, uint16(len(doc.Cues))); err != nil {
			cueRows.abort()
			return 0, err
		}
		for _, cue := range doc.Cues {
			if err = cueRows.append(date, cue.Seq, cue.StartMs, cue.EndMs, cue.Text); err != nil {
				header.abort()
				return 0, err
			}
			cueTotal++
		}
	}
	if err = header.send(); err != nil {
		cueRows.abort()
		return 0, err
	}
	if err = cueRows.send(); err != nil {
		return 0, err
	}

	count = int64(len(docs))
	log.Infof("%s: импортировано пресс-конференций: %d, реплик: %d", pressconfName, count, cueTotal)
	return count, errors.Join(errs...)
}

// pressconfFetch скачивает player response, выбирает русскую дорожку и разбирает JSON3
// в реплики. Ошибки оборачиваются датой: в dagu видно, какая конференция не далась.
func pressconfFetch(item pressconfItem, visitor, apiKey string) (pressconfDoc, error) {
	player, err := pressconfPlayer(item.VideoID, visitor, apiKey)
	if err != nil {
		return pressconfDoc{}, err
	}
	if err = pressconfVerifyPlayer(player, item); err != nil {
		return pressconfDoc{}, err
	}
	duration, err := pressconfDuration(player)
	if err != nil {
		return pressconfDoc{}, err
	}
	track, err := pressconfPickTrack(player.Captions.PlayerCaptionsTracklistRenderer.CaptionTracks)
	if err != nil {
		return pressconfDoc{}, fmt.Errorf("%s: %s: %w", pressconfName, item.Date.Format(pressconfDayLayout), err)
	}
	var captions pressconfCaptionDoc
	if err = util.GetJSON(pressconfCaptionURL(track.BaseURL), &captions); err != nil {
		return pressconfDoc{}, fmt.Errorf("%s: %s: субтитры: %w", pressconfName, item.Date.Format(pressconfDayLayout), err)
	}
	cues, err := pressconfParseCues(&captions)
	if err != nil {
		return pressconfDoc{}, fmt.Errorf("%s: %s: %w", pressconfName, item.Date.Format(pressconfDayLayout), err)
	}
	return pressconfDoc{
		Item:        item,
		URL:         "https://www.youtube.com/watch?v=" + item.VideoID,
		Title:       player.VideoDetails.Title,
		DurationMs:  duration,
		CaptionLang: track.LanguageCode,
		CaptionKind: pressconfCaptionKind(track.Kind),
		Cues:        cues,
	}, nil
}

// pressconfHeaders читает шапки: дата → ожидаемое число реплик.
func pressconfHeaders(ctx context.Context, conn driver.Conn) (map[time.Time]pressconfHeader, error) {
	rows, err := conn.Query(ctx, pressconfHeadersQuery)
	if err != nil {
		return nil, fmt.Errorf("%s: шапки не прочитаны: %w", pressconfName, err)
	}
	defer func() { _ = rows.Close() }()
	out := make(map[time.Time]pressconfHeader)
	for rows.Next() {
		var (
			date  time.Time
			count uint16
		)
		if err = rows.Scan(&date, &count); err != nil {
			return nil, fmt.Errorf("%s: строка шапки не прочитана: %w", pressconfName, err)
		}
		out[mpDay(date)] = pressconfHeader{Cues: count}
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: шапки не дочитаны: %w", pressconfName, err)
	}
	return out, nil
}

// pressconfCueCounts читает фактическое число реплик по датам.
func pressconfCueCounts(ctx context.Context, conn driver.Conn) (map[time.Time]uint64, error) {
	rows, err := conn.Query(ctx, pressconfCueCountsQuery)
	if err != nil {
		return nil, fmt.Errorf("%s: число реплик не прочитано: %w", pressconfName, err)
	}
	defer func() { _ = rows.Close() }()
	out := make(map[time.Time]uint64)
	for rows.Next() {
		var (
			date  time.Time
			count uint64
		)
		if err = rows.Scan(&date, &count); err != nil {
			return nil, fmt.Errorf("%s: строка числа реплик не прочитана: %w", pressconfName, err)
		}
		out[mpDay(date)] = count
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: число реплик не дочитано: %w", pressconfName, err)
	}
	return out, nil
}

// pressconfToday — сегодняшняя дата по Москве, нормализованная к суткам UTC (как
// mpDay): сравнение с датами плейлиста идёт по календарному дню.
func pressconfToday() time.Time {
	now := time.Now().In(pressconfMoscow)
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
}

// pressconfSameDay — одна и та же календарная дата.
func pressconfSameDay(a, b time.Time) bool { return mpDay(a).Equal(mpDay(b)) }

// pressconfBatch — ленивый батч одной таблицы: открывается на первой строке и не
// открывается вовсе, если строк не нашлось. Копия pressBatch с именем своего
// импортёра в ошибках: pressBatch подписывает сбой cbr_key_rate_press, и чужое имя в
// логе сбивало бы оператора (тот же приём, что у per-importer батчей пакета).
type pressconfBatch struct {
	ctx    context.Context
	conn   driver.Conn
	table  string
	insert string
	rows   int64
	batch  driver.Batch
}

// append добавляет строку батча; ошибка Append закрывает открытый батч (иначе
// соединение не вернулось бы в пул до конца прогона).
func (b *pressconfBatch) append(vals ...any) error {
	if b.batch == nil {
		batch, err := b.conn.PrepareBatch(b.ctx, b.insert)
		if err != nil {
			return fmt.Errorf("%s %s: %w", pressconfName, b.table, err)
		}
		b.batch = batch
	}
	if err := b.batch.Append(vals...); err != nil {
		b.abort()
		return fmt.Errorf("%s %s: append: %w", pressconfName, b.table, err)
	}
	b.rows++
	return nil
}

// send отправляет накопленное; пустой батч — no-op (лишняя транзакция не нужна).
// На сбое Send батч помечается использованным, Abort не вызывается (ErrBatchAlreadySent).
func (b *pressconfBatch) send() error {
	if b.batch == nil {
		return nil
	}
	if err := b.batch.Send(); err != nil {
		b.batch = nil
		return fmt.Errorf("%s %s: send: %w", pressconfName, b.table, err)
	}
	b.batch = nil
	return nil
}

// abort закрывает НЕ отправлявшийся открытый батч: на пути ошибки соединение должно
// вернуться в пул. Идемпотентен: закрытый или неоткрывавшийся батч — no-op.
func (b *pressconfBatch) abort() {
	if b.batch == nil {
		return
	}
	if err := b.batch.Abort(); err != nil {
		log.Warnf("%s %s: не удалось закрыть батч после ошибки: %v", pressconfName, b.table, err)
	}
	b.batch = nil
}

// Витрины контура (спека §6): шапка, реплики и полный текст. Таблицы свои, поэтому
// фильтров source в Select нет — только FINAL; комментарии колонок обязательны
// (агент читает контур через них, правило 11).
var (
	pressconfView = util.View{
		Name:   "v_cbr_key_rate_pressconf",
		Tables: []string{pressconfTable},
		Select: `SELECT meeting_date, published, video_id, url, title, duration_ms, caption_lang, caption_kind, cues FROM ` +
			pressconfTable + " FINAL",
		Comment: "Стенограммы пресс-конференций Банка России по ключевой ставке (импортёр cbr_key_rate_pressconf): шапка видео — дата заседания, ссылка, заголовок, длительность, язык и тип дорожки, число реплик. " +
			"Текст — автосубтитры YouTube (ASR), а не официальная стенограмма: возможны ошибки распознавания, повторы и маркеры смены говорящего. " +
			"Реплики с таймкодами — в v_cbr_key_rate_pressconf_cues, полный текст одной строкой — в v_cbr_key_rate_pressconf_text. История с 16.02.2024 (23 конференции на 23.10.2026)",
		Columns: map[string]string{
			"meeting_date": "Дата заседания Совета директоров по ключевой ставке; ключ ряда (одна конференция на дату)",
			"published":    "Дата публикации видео; совпадает с датой заседания (эфир в день решения)",
			"video_id":     "Идентификатор видео YouTube",
			"url":          "Страница видео на YouTube (провенанс)",
			"title":        "Заголовок видео",
			"duration_ms":  "Длительность видео, миллисекунды",
			"caption_lang": "Язык дорожки субтитров: ru (допускается региональный ru-*)",
			"caption_kind": "Тип дорожки: asr — автосубтитры YouTube; manual — ручная дорожка",
			"cues":         "Число реплик, записанных в v_cbr_key_rate_pressconf_cues для этого заседания",
		},
	}

	pressconfCuesView = util.View{
		Name:   "v_cbr_key_rate_pressconf_cues",
		Tables: []string{pressconfCueTable},
		Select: `SELECT meeting_date, seq, start_ms, end_ms, text FROM ` + pressconfCueTable + " FINAL",
		Comment: "Реплики пресс-конференции ЦБ по ключевой ставке: по строке на событие автосубтитров, в порядке видео. " +
			"text — дословный текст ASR (нормализованы только пробелы), возможны ошибки распознавания и маркеры смены говорящего >>. " +
			"start_ms/end_ms — таймкоды от начала видео; seq — порядок реплики. Полный текст одной строкой — v_cbr_key_rate_pressconf_text",
		Columns: map[string]string{
			"meeting_date": "Дата заседания; часть ключа (вместе с seq — порядок реплики)",
			"seq":          "Порядковый номер реплики с 1",
			"start_ms":     "Начало реплики, миллисекунды от начала видео",
			"end_ms":       "Конец реплики, миллисекунды от начала видео; может совпадать с start_ms у коротких событий",
			"text":         "Дословный текст реплики ASR",
		},
	}

	pressconfTextView = util.View{
		Name:   "v_cbr_key_rate_pressconf_text",
		Tables: []string{pressconfTable, pressconfCueTable},
		Select: `SELECT h.meeting_date, h.published, h.video_id, h.url, h.title, h.duration_ms, h.caption_lang, h.caption_kind, h.cues, ` +
			`arrayStringConcat(arrayMap(x -> x.2, arraySort(c.cues)), ' ') AS text ` +
			`FROM (SELECT * FROM ` + pressconfTable + ` FINAL) AS h ` +
			`LEFT JOIN (SELECT meeting_date, groupArray((seq, text)) AS cues FROM ` + pressconfCueTable + ` FINAL GROUP BY meeting_date) AS c ` +
			`ON h.meeting_date = c.meeting_date`,
		Comment: "Полный текст пресс-конференции ЦБ по ключевой ставке: одна строка на заседание, реплики v_cbr_key_rate_pressconf_cues склеены в порядке seq. " +
			"Это автосубтитры ASR, а не официальная стенограмма; для цитаты с таймкодом используйте v_cbr_key_rate_pressconf_cues. " +
			"Пустой text означает, что реплики ещё не записаны (сбой шага cbr_key_rate_pressconf), а не отсутствие конференции",
		Columns: map[string]string{
			"meeting_date": "Дата заседания Совета директоров по ключевой ставке; ключ ряда",
			"published":    "Дата публикации видео; совпадает с датой заседания",
			"video_id":     "Идентификатор видео YouTube",
			"url":          "Страница видео на YouTube (провенанс)",
			"title":        "Заголовок видео",
			"duration_ms":  "Длительность видео, миллисекунды",
			"caption_lang": "Язык дорожки субтитров: ru",
			"caption_kind": "Тип дорожки: asr — автосубтитры; manual — ручная",
			"cues":         "Число реплик, склеенных в text",
			"text":         "Полный текст пресс-конференции (автосубтитры ASR), реплики через пробел в порядке seq",
		},
	}
)

// cbrKeyRatePressconfImporter — импортёр контура: Name() = cbr_key_rate_pressconf и
// две свои таблицы, поэтому Import() свой. Поля шаблона не используются: CreateTable
// пуст — DDL двух таблиц исполняет Import(); PublicationCountPoints/PublicationSource/
// AfterImport не объявлены сознательно (префлайта нет: гейт по meeting_date, спека §5;
// витрины публикует Import). Регистрация — реестром cbr/importers.go; сетевых вызовов
// в init() нет.
var cbrKeyRatePressconfImporter = &pressconfStat{ClickHouseImport: util.ClickHouseImport{
	TableName: pressconfTable,
}}
