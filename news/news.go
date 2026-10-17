package news

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// feedKind — транспорт ленты. RSS и Telegram различаются и получением (XML против
// HTML-превью), и разбором (itemsFromDoc против parseTelegramPage), поэтому Kind
// диспетчеризует fetchFeeds, а не угадывается по URL.
type feedKind int

const (
	feedRSS feedKind = iota
	feedTelegram
)

// feed — одна лента конфига: Key — имя фикстуры в testdata (офлайн-прогоны),
// Source — метка источника в news_events, URL — адрес ленты, Kind — транспорт.
type feed struct {
	Key    string
	Source string
	URL    string
	Kind   feedKind
}

// URL-ы лент — переменные (а не константы) ради подмены в тестах: httptest
// подставляет свой адрес, и TestFetchFeeds проверяет изоляцию сбоя, не выходя в сеть.
var (
	feedInterfaxURL = "https://www.interfax.ru/rss"
	// Публичное веб-превью Telegram-канала Интерфакса: без авторизации и ключей.
	// Окно — последние 20 сообщений, поэтому 30-минутный опрос перекрывает его
	// с запасом, а простой дольше окна теряет позиции (check покажет дыру).
	feedInterfaxTelegramURL = "https://t.me/s/interfaxonline"
	feedRBCURL              = "https://rssexport.rbc.ru/rbcnews/news/30/full.rss"
	feedCBRNewsURL          = "https://www.cbr.ru/rss/RssNews"
	feedCBRPressURL         = "https://www.cbr.ru/rss/eventrss"
	feedTASSURL             = "https://tass.ru/rss/v2.xml"
	// Ленты smart-lab — персональные блоги; добавлены ради контура ключевой ставки
	// (тексты про инфляцию и политику ЦБ).
	feedSmartlabLebedevURL = "https://smart-lab.ru/rss/personal_blog/KonstantinLebedev/"
	feedSmartlabSpydellURL = "https://smart-lab.ru/rss/personal_blog/spydell-telegram/"
)

// feeds собирает конфиг восьми лент в фиксированном порядке. Порядок — часть
// контракта: лог прогона и отчёт check идут построчно, и разный порядок лент
// от запуска к запуску сделал бы диффы лога нечитаемыми. Парные ленты несут общий
// Source: у двух фидов ЦБ это «cbr» (источник один — Банк России), у двух лент
// smart-lab — «smartlab», у RSS и Telegram Интерфакса — «interfax»; в news_events
// такие ленты различаются url. interfax_tg идёт сразу после interfax: это второй
// транспорт того же источника, и в отчёте покрытия их окна складываются.
func feeds() []feed {
	return []feed{
		{Key: "interfax", Source: "interfax", URL: feedInterfaxURL, Kind: feedRSS},
		{Key: "interfax_tg", Source: "interfax", URL: feedInterfaxTelegramURL, Kind: feedTelegram},
		{Key: "rbc", Source: "rbc", URL: feedRBCURL, Kind: feedRSS},
		{Key: "cbr_news", Source: "cbr", URL: feedCBRNewsURL, Kind: feedRSS},
		{Key: "cbr_press", Source: "cbr", URL: feedCBRPressURL, Kind: feedRSS},
		{Key: "tass", Source: "tass", URL: feedTASSURL, Kind: feedRSS},
		{Key: "smartlab_lebedev", Source: "smartlab", URL: feedSmartlabLebedevURL, Kind: feedRSS},
		{Key: "smartlab_spydell", Source: "smartlab", URL: feedSmartlabSpydellURL, Kind: feedRSS},
	}
}

// Режимы прогона. modeSince — режим расписания dagu: пишутся только позиции,
// которых ещё нет в news_events (ключ date/source/title), поэтому 30-минутный
// прогон не переписывает окно лент целиком. modeAll — ручной полный upsert окна
// (пересборка): идемпотентен по тому же ключу. modeCheck — отчёт покрытия без
// записи: дыру в окне иначе не увидеть, она уже не влезает в ленту.
const (
	modeAll   = "all"
	modeSince = "since"
	modeCheck = "check"
	modeEnv   = "NEWS_WATCH_MODE"
)

// parseMode переводит значение NEWS_WATCH_MODE в режим прогона. Пустое значение —
// all: ручной запуск без переменной делает полный upsert окна (безопасный дефолт
// для нечастого ручного прогона), а не падает. Всё остальное — ошибка, и
// вызывающий получает её до сети и до БД: «ALL» или опечатка в слове, принятые
// за дефолт all, молча перечитали бы окно целиком там, где ждали since. В тексте
// ошибки — имя переменной: по нему понятно, что править.
func parseMode(v string) (string, error) {
	switch v {
	case "", modeAll:
		return modeAll, nil
	case modeSince, modeCheck:
		return v, nil
	}
	return "", fmt.Errorf("%s=%q: неизвестный режим (all|since|check)", modeEnv, v)
}

// sourceSpan — окно покрытия источника: сколько позиций сохранено и от какого
// момента до какого. По нему считаются дыры (пропущенные прогоны) и отчёт check.
//
// FailedFeeds — ключи лент источника, не прочитанных в этом прогоне (порядок
// feeds()). Непустой список помечает окно неполным: у источника ЦБ два фида, и по
// одному выжившему о разрыве покрытия судить нельзя — промежуток могла закрывать
// упавшая лента, поэтому дыра по неполному окну не объявляется. Заполняется только
// feed-стороной (feedSpans); окно, прочитанное из БД, поле не несёт — сохранённые
// строки уже собраны прошлыми прогонами целиком.
type sourceSpan struct {
	Count       uint64
	Min, Max    time.Time
	FailedFeeds []string
}

// feedResult — итог чтения одной ленты. Прочитанные позиции и ошибку ленты
// вызывающий видит отдельно: сбойная лента не должна прятать успех остальных.
type feedResult struct {
	Feed    feed
	Items   []newsItem
	Skipped int
	// SeenIDs — ID сообщений Telegram-окна, включая медиа без текста. Заполняется
	// только Telegram-лентой; T2 отличает по нему вышедшее из окна сообщение
	// от сообщения, у которого просто нет текста.
	SeenIDs map[uint64]struct{}
	Err     error
}

// fetchFeeds читает ленты по очереди и возвращает результат на каждую. Ошибка
// ленты попадает в её feedResult.Err, а цикл продолжается: восемь лент независимы,
// и обрыв одной (WAF, 500, таймаут) не повод не собрать остальные семь.
// Агрегация ошибок — дело вызывающего (Import): он решает, чем отчитаться в dagu.
//
// Транспорт выбирается по Kind, а не по виду URL: RSS разбирается в rssDoc,
// Telegram-превью — в telegramPage. Ошибка разбора Telegram (чужая страница,
// сменившиеся селекторы) так же локальна, как ошибка RSS.
func fetchFeeds(feeds []feed) []feedResult {
	results := make([]feedResult, 0, len(feeds))
	for _, f := range feeds {
		res := feedResult{Feed: f}
		switch f.Kind {
		case feedRSS:
			var doc rssDoc
			// URL в ошибке транспорта (util.GetXML его не маскирует) — здесь ключей
			// в query нет, ленты публичные, поэтому ошибка уезжает в лог как есть.
			if err := util.GetXML(f.URL, &doc); err != nil {
				res.Err = err
				break
			}
			res.Items, res.Skipped, res.Err = itemsFromDoc(f.Source, doc)
		case feedTelegram:
			page, err := util.GetPage(f.URL)
			if err != nil {
				res.Err = err
				break
			}
			parsed, err := parseTelegramPage(f.Key, page)
			if err != nil {
				res.Err = err
				break
			}
			res.Items = telegramItems(f.Source, parsed)
			res.SeenIDs, res.Skipped = parsed.SeenIDs, parsed.Skipped
		default:
			// Новая лента без транспорта — ошибка ленты, а не пропуск: иначе она
			// молча выпала бы из прогона, и check не увидел бы дыру.
			res.Err = fmt.Errorf("%s: неизвестный вид ленты %d", f.Key, f.Kind)
		}
		results = append(results, res)
	}
	return results
}

// feedSpans сворачивает результаты лент в окна по Source. Оба фида ЦБ дают один
// ключ «cbr»: окно покрытия — свойство источника, а не ленты, и по отдельным
// фидам дыра между ними выглядела бы пропущенным прогоном, которого не было.
// Поэтому Count суммируется, а Min/Max берутся крайними.
//
// Упавшая лента не выбрасывается из окна молча: её ключ попадает в FailedFeeds
// (спан создаётся, даже если у источника не прочиталась ни одна лента). Иначе
// частичный сбой ЦБ выглядел бы сужением окна: cbr_news упала, cbr_press началась
// позже сохранённого максимума — и detectGaps объявил бы дыру, которую на деле
// закрывала упавшая лента. Ошибка ленты так и отчитывается ошибкой (FailedFeeds),
// а не пустым окном: результат без ошибки без позиций невозможен — itemsFromDoc
// считает пустую ленту ошибкой.
func feedSpans(results []feedResult) map[string]sourceSpan {
	spans := make(map[string]sourceSpan)
	for _, r := range results {
		span := spans[r.Feed.Source]
		if r.Err != nil {
			span.FailedFeeds = append(span.FailedFeeds, r.Feed.Key)
			spans[r.Feed.Source] = span
			continue
		}
		span.Count += uint64(len(r.Items))
		for _, it := range r.Items {
			if span.Min.IsZero() || it.Date.Before(span.Min) {
				span.Min = it.Date
			}
			if span.Max.IsZero() || it.Date.After(span.Max) {
				span.Max = it.Date
			}
		}
		spans[r.Feed.Source] = span
	}
	return spans
}

// newsKey — ключ позиции news_events: момент публикации в Unix-секундах (DateTime
// хранит секунды) и заголовок. Source в ключ не входит: существующие позиции и так
// читаются по одному источнику (WHERE source = ?), и пара сравнивается в его
// пределах. Пара (date, title) — это канонический ORDER BY (date, source, title) без
// source. Сравнение идёт по Unix-секундам, а не по настенным часам: клиент отдаёт
// DateTime со своим офсетом, а момент публикации при этом один и тот же.
type newsKey struct {
	date  int64
	title string
}

// readStoredKeys читает ключи (date, title) уже сохранённых позиций источника не
// старее since — нижней границы окна этого прогона. Пустая таблица или источник без
// строк — пустая map без ошибки: «первый прогон» и «всё уже сохранено» различает
// вызывающий по составу items, а не по ошибке. Порядок Scan — порядок колонок
// storedKeysQuery: date, title. FINAL в запросе обязателен: без него ReplacingMergeTree
// вернул бы и вытесненные копии позиций (на состав map это не влияет — ключ один, —
// но чтение было бы нечестным).
func readStoredKeys(ctx context.Context, conn driver.Conn, source string, since time.Time) (map[newsKey]struct{}, error) {
	rows, err := conn.Query(ctx, storedKeysQuery, source, since)
	if err != nil {
		return nil, fmt.Errorf("news_watch: читать ключи %s (source=%s): %w", newsTable, source, err)
	}
	defer func() { _ = rows.Close() }()

	keys := make(map[newsKey]struct{})
	for rows.Next() {
		var (
			date  time.Time
			title string
		)
		if err = rows.Scan(&date, &title); err != nil {
			return nil, fmt.Errorf("news_watch: читать строку ключа %s (source=%s): %w", newsTable, source, err)
		}
		keys[newsKey{date: date.Unix(), title: title}] = struct{}{}
	}
	// Ошибка обхода (обрыв соединения на середине) обязана быть видна: молча
	// вернувшаяся половина ключей записала бы уже сохранённые позиции повторно.
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("news_watch: читать ключи %s (source=%s): %w", newsTable, source, err)
	}
	return keys, nil
}

// selectNewItems оставляет из окна лент только позиции, которых ещё нет в
// news_events, — по ключу (date, source, title), а не по дате-курсору.
//
// Почему наличие, а не watermark (max(date) источника). RSS не обязан быть строго
// упорядочен по времени: позиция с датой старее уже сохранённого максимума всё равно
// может впервые появиться в окне (опоздавшая публикация, отложенный анонс) — курсор по
// дате молча выбросил бы её, и в news_events осталась бы дыра, которую check прочитает
// как «всё сохранено». Хуже у ЦБ: под одним source = 'cbr' лежат ДВА фида (RssNews и
// eventrss), и их фронты независимы — пресс-релиз с датой раньше фронта новостного фида
// не записался бы никогда. Проверка наличия по ключу таких позиций не теряет: пишутся
// ровно строки, которых в таблице нет, независимо от их порядка по дате.
//
// Окно лент при этом вычитывается целиком: у RSS нет «since»-параметра, лента — это
// всегда последние N позиций (Интерфакс 25, РБК 30, ЦБ и TASS по 100). Меняется только
// состав записи — читаем всё окно, пишем отсутствующее.
//
// Группировка — по item.Source в порядке первого появления (обход map в Go случаен, а
// порядок лент — часть контракта лога и батча). Для каждого источника since — минимум
// дат его позиций окна: запрос существующих ключей ниже этой границы не нужен, раньше
// неё в окне этого прогона ничего нет. Порядок результата — исходный порядок items
// (лента за лентой, позиция за позицией), а ключ, продублированный внутри прогона,
// пишется один раз: второй раз это та же строка ReplacingMergeTree.
func selectNewItems(ctx context.Context, conn driver.Conn, items []newsItem) ([]newsItem, error) {
	var order []string
	bySource := make(map[string][]newsItem)
	for _, it := range items {
		if _, ok := bySource[it.Source]; !ok {
			order = append(order, it.Source)
		}
		bySource[it.Source] = append(bySource[it.Source], it)
	}

	stored := make(map[string]map[newsKey]struct{}, len(order))
	for _, src := range order {
		since := bySource[src][0].Date
		for _, it := range bySource[src][1:] {
			if it.Date.Before(since) {
				since = it.Date
			}
		}
		keys, err := readStoredKeys(ctx, conn, src, since)
		if err != nil {
			return nil, err
		}
		stored[src] = keys
	}

	// Дедуп — в пределах источника: только (date, source, title) целиком образует ключ
	// news_events, и одна и та же пара (date, title) у двух источников — две разные
	// строки, обе законные.
	written := make(map[string]map[newsKey]struct{}, len(order))
	out := make([]newsItem, 0, len(items))
	for _, it := range items {
		key := newsKey{date: it.Date.Unix(), title: it.Title}
		if _, ok := stored[it.Source][key]; ok {
			continue // позиция уже в news_events — писать нечего
		}
		seen := written[it.Source]
		if seen == nil {
			seen = make(map[newsKey]struct{})
			written[it.Source] = seen
		}
		if _, ok := seen[key]; ok {
			continue // дубликат того же ключа внутри прогона
		}
		seen[key] = struct{}{}
		out = append(out, it)
	}
	return out, nil
}

// sortedSources — ключи окон в лексикографическом порядке. Обход map в Go случаен,
// поэтому отчёт (дыры и покрытие) без сортировки менял бы порядок строк от прогона
// к прогону: живой лог нельзя ни сравнить с предыдущим, ни продиффить глазами.
func sortedSources(spans map[string]sourceSpan) []string {
	srcs := make([]string, 0, len(spans))
	for src := range spans {
		srcs = append(srcs, src)
	}
	sort.Strings(srcs)
	return srcs
}

// detectGaps ищет дыры покрытия: сохранённый максимум источника старше минимума
// его ленты — значит, между двумя прогонами окно ленты проскочило целиком и часть
// новостей не собрана (лента конечна и «залечить» её задним числом нечем; в
// news_events зияет окно). Сравнение строгое (Min.After(Max)): стык встык — это
// продолжение покрытия, а не разрыв.
//
// Неполное окно (FailedFeeds) дыры не объявляет: у ЦБ две ленты под одним
// источником, и если одна из них упала, окно выжившей короче реального — разрыв
// между сохранённым максимумом и её началом могла закрывать именно упавшая лента.
// Ложная тревога в check дороже молчания: она гонит разбираться с дырой, которой
// нет. Источник без сохранённых строк и источник с пустой лентой дырой тоже не
// считаются: первого ещё не было, у второго ошибка ленты.
// По каждому источнику — одна строка, отсортировано по имени источника.
func detectGaps(feed, stored map[string]sourceSpan) []string {
	var gaps []string
	for _, src := range sortedSources(feed) {
		f, s := feed[src], stored[src]
		if len(f.FailedFeeds) > 0 {
			continue
		}
		if s.Count > 0 && f.Count > 0 && f.Min.After(s.Max) {
			gaps = append(gaps, fmt.Sprintf("%s: дыра %s … %s (окно не собрано: пропущенный прогон)",
				src, s.Max.Format(time.RFC3339), f.Min.Format(time.RFC3339)))
		}
	}
	return gaps
}

// coverageLines описывает окна прогона для режима check (ручная сверка покрытия):
// сколько строк источника уже сохранено и в каком окне, против того, что сейчас
// в ленте. Строки читает человек, поэтому источник без сохранённых строк назван
// словами («первый прогон не выполнялся»), а не нулевым окном 0001-01-01, а окно
// источника, у которого упали все ленты, — «нет прочитанных позиций»: нулевые
// времена выглядели бы как настоящее окно от 0001 года.
//
// Неполное окно (FailedFeeds) помечается суффиксом с именами непрочитанных лент:
// в check важно не только сколько собрано, но и по чему судить нельзя — иначе
// выжившая лента ЦБ читалась бы как весь источник.
func coverageLines(feed, stored map[string]sourceSpan) []string {
	lines := make([]string, 0, len(feed))
	for _, src := range sortedSources(feed) {
		f, s := feed[src], stored[src]
		window := fmt.Sprintf("в ленте %d (%s…%s)",
			f.Count, f.Min.Format(time.RFC3339), f.Max.Format(time.RFC3339))
		if f.Count == 0 {
			window = fmt.Sprintf("в ленте %d (нет прочитанных позиций)", f.Count)
		}
		var line string
		if s.Count == 0 {
			line = fmt.Sprintf("%s: сохранённых строк нет (первый прогон не выполнялся)", src)
		} else {
			line = fmt.Sprintf("%s: сохранено %d (%s…%s), %s",
				src, s.Count, s.Min.Format(time.RFC3339), s.Max.Format(time.RFC3339), window)
		}
		if len(f.FailedFeeds) > 0 {
			line += fmt.Sprintf("; неполно: не прочитаны ленты %s", strings.Join(f.FailedFeeds, ", "))
		}
		lines = append(lines, line)
	}
	return lines
}

// newsImport — импортёр контура новостей. Имя news_watch — контракт с расписанием:
// его ставит CLICKHOUSE_IMPORT_STAT в dagu/news.yaml, по нему же main.go выбирает
// импортёр из chimport.Stats (промах в имени — не ошибка, а предупреждение
// «no importer named» и зелёный прогон, поэтому имя закреплено тестом).
type newsImport struct{}

func (s *newsImport) Name() string {
	return "news_watch"
}

const (
	newsTable = "news_events"
	// newsInsert — запись позиции. Список колонок явный: на живой таблице description и
	// content_hash добавлены ALTER-ами, и позиционный INSERT клал бы значения в порядке
	// физических колонок (миграции двигают только описание и hash), а не в порядке DDL.
	// Порядок колонок здесь — контракт с порядком аргументов Append в insertItems.
	newsInsert = "INSERT INTO " + newsTable + " (date, source, title, url, description, content_hash, sentiment)"
	// storedSpansQuery читает сохранённые окна источников. FINAL обязателен:
	// news_events — ReplacingMergeTree, и без него повторно записанные позиции
	// (ключ date/source/title) считались бы дважды, а счётчик покрытия check
	// завышался бы на каждом прогоне.
	storedSpansQuery = "SELECT source, count(), min(date), max(date) FROM " + newsTable + " FINAL GROUP BY source"
	// storedKeysQuery читает ключи (date, title) сохранённых позиций одного источника
	// не старее нижней границы окна этого прогона — вход режима since. FINAL по той же
	// причине, что и у storedSpansQuery. Плейсхолдеры позиционные: ? #1 — source,
	// ? #2 — граница (time.Time); clickhouse-go биндит аргументы по порядку.
	storedKeysQuery = "SELECT date, title FROM " + newsTable + " FINAL WHERE source = ? AND date >= ?"

	// newsAnnotationInsert — запись LLM-разметки. Список колонок явный, и его порядок —
	// контракт с порядком аргументов Append в insertNewsAnnotations (порядок канонического DDL).
	newsAnnotationInsert = "INSERT INTO " + newsAnnotationTable + " (url, content_hash, kind, method, model, prompt_version, dimension, tag, direction, confidence, evidence, sentiment, relevance, summary, annotated_at)"
)

// newsAnnotationCandidatesQuery читает бэклог разметки: Telegram-сообщения за 7 дней, для
// которых ещё нет строки method='llm' этой пары (model, prompt_version). Пара входит в ключ
// ReplacingMergeTree, поэтому смена модели/промпта автоматически делает старые сообщения
// кандидатами, не затирая прежнюю разметку. FINAL обязателен: news_events и news_annotation —
// ReplacingMergeTree, и без него вытесненные копии дали бы дубли кандидатов / ложные пропуски.
// LIMIT — из newsLLMBacklogLimit: стоимость LLM ограничена и после долгого простоя.
var newsAnnotationCandidatesQuery = `SELECT date, source, title, url, description, content_hash FROM ` + newsTable + ` FINAL WHERE url LIKE 'https://t.me/%' AND content_hash != '' AND date >= now() - INTERVAL 7 DAY AND (url, content_hash) NOT IN (SELECT url, content_hash FROM ` + newsAnnotationTable + ` FINAL WHERE method = 'llm' AND model = ? AND prompt_version = ?) ORDER BY date DESC LIMIT ` + strconv.Itoa(newsLLMBacklogLimit)

// Import выполняет прогон контура новостей: DDL → витрина → ленты → батч.
// Порядок шагов — часть контракта, а не стиль.
//
// Режим разбирается первым действием, до БД и до сети: опечатка в NEWS_WATCH_MODE
// обязана падать сразу. Прими «ALL» за дефолт all — и прогон успел бы перечитать
// окно целиком, прежде чем выяснилось, что режим не тот; проверка до всего делает
// ошибку дешёвой и одинаковой в любом окружении.
//
// Дальше — DDL и витрина до чтения лент: импортёр обязан отработать на чистой базе,
// а витрина публикуется в начале прогона (как у eia), чтобы правка определения
// уезжала вместе с импортом. Сбойная лента не отменяет остальные: её ошибка
// копится в feedErrs и возвращается в конце — успешные ленты уже записаны, DAG
// краснеет, а ретрай идемпотентен (ReplacingMergeTree по date/source/title).
func (s *newsImport) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	mode, err := parseMode(os.Getenv(modeEnv))
	if err != nil {
		return 0, err
	}
	// Схема — CREATE плюс идемпотентные ALTER-ы миграции (newsSchemaStatements), и в режиме
	// check тоже: чтение обязано работать на живой таблице, дожившей до колонки description.
	for _, stmt := range newsSchemaStatements() {
		if err = conn.Exec(ctx, stmt); err != nil {
			return 0, err
		}
	}
	// check — режим чтения: он ходит только в сырую news_events и не публикует
	// витрину, поэтому шаг пропускается, а не выполняется вхолостую.
	if mode != modeCheck {
		for _, stmt := range newsViewStatements {
			if err = conn.Exec(ctx, stmt); err != nil {
				return 0, err
			}
		}
	}

	results := fetchFeeds(feeds())
	var feedErrs []error
	for _, r := range results {
		if r.Err != nil {
			log.Errorf("news_watch: %s: %v", r.Feed.Key, r.Err)
			feedErrs = append(feedErrs, fmt.Errorf("%s: %w", r.Feed.Key, r.Err))
			continue
		}
		// skipped печатается не для красоты: ненулевое значение — единственный
		// сигнал, что лента сменила форму и позиции начали теряться молча.
		log.Infof("news_watch: %s: позиций %d, пропущено %d", r.Feed.Key, len(r.Items), r.Skipped)
	}
	spans := feedSpans(results)

	if mode == modeCheck {
		stored, err := readStoredSpans(ctx, conn)
		if err != nil {
			// Ошибка чтения покрытия не вытесняет ошибки лент: dagu печатает одну
			// строку на прогон, и диагноз обязан быть полным — видно и сбой базы,
			// и то, какие ленты не прочитались в этом прогоне.
			return 0, errors.Join(err, errors.Join(feedErrs...))
		}
		for _, line := range coverageLines(spans, stored) {
			log.Infof("news_watch check: %s", line)
		}
		// Дыра — ошибка: режим ручной, и «зелёный» check с дырой в покрытии никто
		// не перечитает. Отказ ленты тоже виден: оба списка съезжаются в feedErrs.
		if gaps := detectGaps(spans, stored); len(gaps) > 0 {
			feedErrs = append(feedErrs, fmt.Errorf("дыры покрытия: %s", strings.Join(gaps, "; ")))
		}
		return 0, errors.Join(feedErrs...)
	}

	// Слоты — позиции успешных лент в порядке feeds(). RSS-слот несёт окно ленты как есть,
	// Telegram-слот — результат diffTelegram: текущий текст с зафиксированным title версии 1.
	// Порядок слотов сохраняется до батча, поэтому лог и запись детерминированы.
	type feedSlot struct {
		kind  feedKind
		items []newsItem
	}
	slots := make([]feedSlot, 0, len(results))
	var newVersions []telegramVersion
	now := time.Now()
	for _, r := range results {
		if r.Err != nil {
			continue
		}
		if r.Feed.Kind != feedTelegram {
			slots = append(slots, feedSlot{kind: feedRSS, items: r.Items})
			continue
		}
		if len(r.Items) == 0 {
			// Успешная Telegram-лента без позиций невозможна: parseTelegramPage считает
			// пустое окно ошибкой. Страховка, а не штатная ветка.
			continue
		}
		// Версии читаются до записи: без истории diffTelegram принял бы все текущие
		// сообщения за v1 и затёр нумерацию. Чтение — строго по каналу и нижней границе
		// окна этого прогона (минимум дат публикации).
		channel := r.Items[0].Channel
		windowMin, windowMax := r.Items[0].Date, r.Items[0].Date
		for _, it := range r.Items[1:] {
			if it.Date.Before(windowMin) {
				windowMin = it.Date
			}
			if it.Date.After(windowMax) {
				windowMax = it.Date
			}
		}
		existing, readErr := readTelegramVersions(ctx, conn, channel, windowMin)
		if readErr != nil {
			// Ошибка чтения версий отменяет запись этого прогона: без истории версий
			// текущий текст записался бы с неверным номером. Повтор идемпотентен.
			return 0, errors.Join(readErr, errors.Join(feedErrs...))
		}
		telegramItems, versions, stats := diffTelegram(existing, r.Items, r.SeenIDs, windowMin, windowMax, now)
		// Те же позиции, что печатает лог ленты выше, но с итогом версий: M — новые
		// версии текущих сообщений, K — tombstone (удаления из окна).
		log.Infof("news_watch: %s: позиций %d, новых версий %d, удалений %d",
			r.Feed.Key, len(telegramItems), stats.NewVersions, stats.Deleted)
		slots = append(slots, feedSlot{kind: feedTelegram, items: telegramItems})
		newVersions = append(newVersions, versions...)
	}

	var items []newsItem
	if mode == modeSince {
		// since — «только отсутствующие» для RSS: пишутся позиции, которых нет в
		// news_events по ключу (date, source, title), без опоры на порядок дат (см.
		// selectNewItems). Telegram-текущие пишутся всегда: их ключ стабилен по title v1,
		// а upsert держит в news_events свежий текст и кандидатов LLM-разметки.
		var rss []newsItem
		for _, s := range slots {
			if s.kind == feedRSS {
				rss = append(rss, s.items...)
			}
		}
		filtered, err := selectNewItems(ctx, conn, rss)
		if err != nil {
			// Тот же полный диагноз, что и в check: отказ лент не должен теряться
			// за ошибкой базы — иначе в dagu видно только её.
			return 0, errors.Join(err, errors.Join(feedErrs...))
		}
		// selectNewItems сохраняет исходный порядок и выбрасывает уже сохранённые позиции;
		// раскладываем результат обратно по слотам по ключу (source, date, title), чтобы
		// батч шёл в порядке feeds(). Счётчик, а не set: дубль ключа внутри прогона
		// selectNewItems уже схлопнул, и второй экземпляр писать нельзя.
		type slotKey struct {
			source string
			key    newsKey
		}
		kept := make(map[slotKey]int, len(filtered))
		for _, it := range filtered {
			kept[slotKey{source: it.Source, key: newsKey{date: it.Date.Unix(), title: it.Title}}]++
		}
		items = make([]newsItem, 0, len(filtered)+len(newVersions))
		for _, s := range slots {
			for _, it := range s.items {
				if s.kind == feedTelegram {
					items = append(items, it)
					continue
				}
				k := slotKey{source: it.Source, key: newsKey{date: it.Date.Unix(), title: it.Title}}
				if kept[k] == 0 {
					continue
				}
				kept[k]--
				items = append(items, it)
			}
		}
	} else {
		// all — полный upsert окна: пишутся все позиции всех успешных лент.
		for _, s := range slots {
			items = append(items, s.items...)
		}
	}
	if len(items) == 0 {
		// Пустой список в since — норма (всё окно уже собрано, повторный прогон по
		// расписанию). В all то же самое означает «упали все ленты», и там молчать
		// правильно: каждая такая лента уже сказана в логе и попала в feedErrs.
		if mode == modeSince {
			log.Infof("news_watch: новых позиций нет — нечего обновлять (0 строк)")
		}
	} else {
		if count, err = insertItems(ctx, conn, items); err != nil {
			// К ошибке батча добавляются отказы лент: иначе прогон в dagu отчитывается
			// только «запись упала», и сбойные ленты этого прогона из вывода выпадают.
			return count, errors.Join(err, errors.Join(feedErrs...))
		}
	}
	// Версии пишутся после news_events: сбой второго батча не теряет текущий текст,
	// а повтор прогона дописывает недостающие версии (ключ ReplacingMergeTree).
	if _, verr := insertTelegramVersions(ctx, conn, newVersions); verr != nil {
		return count, errors.Join(verr, errors.Join(feedErrs...))
	}

	// LLM-разметка — после записи текущего текста и версий, но до выхода из прогона:
	// пустой items (since при покрытом окне или упавшая Telegram-лента) не повод пропускать
	// бэклог news_annotation, иначе после простоя разметка не догналась бы никогда.
	// Любая ошибка LLM/валидации/записи — warn и продолжение: импорт остаётся зелёным,
	// догон — следующим прогоном. check сюда не доходит (возврат выше).
	baseURL, apiKey, model, llmOK := newsLLMConfigFromEnv()
	if !llmOK {
		// Один warn за прогон: без него отсутствие разметки выглядело бы тихим дефектом.
		log.Warnf("news_watch: LLM-разметка пропущена (нет LLM_BASE_URL/LLM_API_KEY/NEWS_LLM_MODEL)")
	} else {
		candidates, readErr := readNewsAnnotationCandidates(ctx, conn, model, newsPromptVersion)
		switch {
		case readErr != nil:
			log.Warnf("news_watch: LLM-разметка (%s): %v", model, readErr)
		case len(candidates) == 0:
			// Бэклога нет — провайдер не вызывается.
		default:
			rows, tagErr := tagTelegramLLM(ctx, newsLLMConfig{BaseURL: baseURL, APIKey: apiKey, Model: model}, candidates)
			if tagErr != nil {
				log.Warnf("news_watch: LLM-разметка (%s): %v", model, tagErr)
				break
			}
			var docs, tags int
			for _, row := range rows {
				switch row.Kind {
				case newsKindDoc:
					docs++
				case newsKindTag:
					tags++
				}
			}
			if _, insErr := insertNewsAnnotations(ctx, conn, rows); insErr != nil {
				log.Warnf("news_watch: LLM-разметка: запись %s: %v", newsAnnotationTable, insErr)
				break
			}
			log.Infof("news_watch: LLM %s: размечено %d сообщений, тегов %d", model, docs, tags)
		}
	}
	// Успешные ленты записаны, сбойная видна в логе и в итоговой ошибке: dagu
	// перезапустит прогон, а повторная запись тех же позиций безвредна.
	return count, errors.Join(feedErrs...)
}

// readStoredSpans читает сохранённые окна источников: сколько строк по каждому
// source и в каком они диапазоне дат. Это вход режима check (сверка покрытия).
// Пустая таблица — не ошибка, а первый прогон: вызывающий различает «источник ещё
// не писался» по отсутствию ключа, а не по нулевому окну.
// FailedFeeds здесь не заполняется: сохранённые строки собраны прошлыми прогонами
// целиком, и непрочитанных лент у них нет.
func readStoredSpans(ctx context.Context, conn driver.Conn) (map[string]sourceSpan, error) {
	rows, err := conn.Query(ctx, storedSpansQuery)
	if err != nil {
		return nil, fmt.Errorf("news_watch: читать покрытие %s: %w", newsTable, err)
	}
	defer func() { _ = rows.Close() }()

	spans := make(map[string]sourceSpan)
	for rows.Next() {
		var (
			src  string
			span sourceSpan
		)
		// Порядок Scan — порядок колонок storedSpansQuery: source, count(), min(date), max(date).
		if err = rows.Scan(&src, &span.Count, &span.Min, &span.Max); err != nil {
			return nil, fmt.Errorf("news_watch: читать строку покрытия %s: %w", newsTable, err)
		}
		spans[src] = span
	}
	// Ошибка обхода (обрыв соединения на середине) обязана быть видна: запрос агрегирует
	// по источникам (GROUP BY source), и не доехавшие строки — это целые потерянные
	// источники. Молча вернувшаяся половина окон выдала бы их за «первый прогон не
	// выполнялся» (coverageLines), а detectGaps по ним молча пропустил бы проверку дыр
	// (дыра считается только при сохранённом Count > 0). Обрыв — ошибка, а не «нет данных».
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("news_watch: читать покрытие %s: %w", newsTable, err)
	}
	return spans, nil
}

// insertItems пишет позиции одним батчем — построчный Exec в цикле запрещён
// (правило 2 AGENTS.md). ContentHash заполняется у обеих лент (у RSS тоже: разметка
// generic по url+content_hash). Sentiment передаётся явным (*float32)(nil): колонку
// заполняет LLM-сессия (follow-up), и типизированный nil фиксирует, что поле
// оставлено пустым осознанно. Ошибка Append прерывает батч через Abort: частично
// собранный батч Send не отправляет, поэтому половина прогона в таблицу не уедет.
//
// Порядок аргументов Append — контракт с явным списком колонок newsInsert
// (date, source, title, url, description, content_hash, sentiment); менять его можно
// только вместе с ним.
func insertItems(ctx context.Context, conn driver.Conn, items []newsItem) (int64, error) {
	batch, err := conn.PrepareBatch(ctx, newsInsert)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, it := range items {
		if err = batch.Append(it.Date, it.Source, it.Title, it.URL, it.Description,
			it.ContentHash, (*float32)(nil)); err != nil {
			_ = batch.Abort()
			return count, err
		}
		count++
	}
	err = batch.Send()
	return count, err
}

// readNewsAnnotationCandidates читает бэклог LLM-разметки: Telegram-сообщения за 7 дней,
// у которых нет doc/tag-строк этой пары (model, prompt_version). Пустой результат — не
// ошибка: разметить нечего. Порядок Scan — порядок колонок newsAnnotationCandidatesQuery:
// date, source, title, url, description, content_hash. Ошибка обхода проверяется: молча
// вернувшаяся половина бэклога оставила бы часть сообщений без разметки до следующего прогона.
func readNewsAnnotationCandidates(ctx context.Context, conn driver.Conn, model string, promptVersion uint16) ([]newsItem, error) {
	rows, err := conn.Query(ctx, newsAnnotationCandidatesQuery, model, promptVersion)
	if err != nil {
		return nil, fmt.Errorf("news_watch: читать кандидатов %s (model=%s, prompt_version=%d): %w",
			newsAnnotationTable, model, promptVersion, err)
	}
	defer func() { _ = rows.Close() }()

	var candidates []newsItem
	for rows.Next() {
		var it newsItem
		if err = rows.Scan(&it.Date, &it.Source, &it.Title, &it.URL, &it.Description, &it.ContentHash); err != nil {
			return nil, fmt.Errorf("news_watch: читать кандидата %s (model=%s): %w",
				newsAnnotationTable, model, err)
		}
		candidates = append(candidates, it)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("news_watch: читать кандидатов %s (model=%s): %w", newsAnnotationTable, model, err)
	}
	return candidates, nil
}

// insertNewsAnnotations пишет разметку одним батчем: построчный Exec в цикле запрещён
// (правило 2 AGENTS.md). Пустой список не открывает батч вовсе — прогон без кандидатов не
// должен слать пустой Send. Ошибка Append прерывает батч через Abort: частично собранный
// батч Send не отправляет, поэтому половина разметки в таблицу не уедет.
//
// Порядок аргументов Append — контракт с явным списком колонок newsAnnotationInsert
// (url, content_hash, kind, method, model, prompt_version, dimension, tag, direction,
// confidence, evidence, sentiment, relevance, summary, annotated_at).
func insertNewsAnnotations(ctx context.Context, conn driver.Conn, rows []newsAnnotationRow) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx, newsAnnotationInsert)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, row := range rows {
		if err = batch.Append(row.URL, row.ContentHash, row.Kind, row.Method, row.Model,
			row.PromptVersion, row.Dimension, row.Tag, row.Direction, row.Confidence,
			row.Evidence, row.Sentiment, row.Relevance, row.Summary, row.AnnotatedAt); err != nil {
			_ = batch.Abort()
			return count, err
		}
		count++
	}
	err = batch.Send()
	return count, err
}

func init() {
	chimport.Stats = append(chimport.Stats, &newsImport{})
}
