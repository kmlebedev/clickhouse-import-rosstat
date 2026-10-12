package news

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// feed — одна лента конфига: Key — имя фикстуры в testdata (офлайн-прогоны),
// Source — метка источника в news_events, URL — адрес ленты.
type feed struct {
	Key    string
	Source string
	URL    string
}

// URL-ы лент — переменные (а не константы) ради подмены в тестах: httptest
// подставляет свой адрес, и TestFetchFeeds проверяет изоляцию сбоя, не выходя в сеть.
var (
	feedInterfaxURL = "https://www.interfax.ru/rss"
	feedRBCURL      = "https://rssexport.rbc.ru/rbcnews/news/30/full.rss"
	feedCBRNewsURL  = "https://www.cbr.ru/rss/RssNews"
	feedCBRPressURL = "https://www.cbr.ru/rss/eventrss"
	feedTASSURL     = "https://tass.ru/rss/v2.xml"
)

// feeds собирает конфиг пяти лент в фиксированном порядке. Порядок — часть
// контракта: лог прогона и отчёт check идут построчно, и разный порядок лент
// от запуска к запуску сделал бы диффы лога нечитаемыми. Обе ленты ЦБ несут
// Source «cbr»: источник один — Банк России, а различает их url в news_events.
func feeds() []feed {
	return []feed{
		{Key: "interfax", Source: "interfax", URL: feedInterfaxURL},
		{Key: "rbc", Source: "rbc", URL: feedRBCURL},
		{Key: "cbr_news", Source: "cbr", URL: feedCBRNewsURL},
		{Key: "cbr_press", Source: "cbr", URL: feedCBRPressURL},
		{Key: "tass", Source: "tass", URL: feedTASSURL},
	}
}

// Режимы прогона. modeAll — дефолт расписания dagu: окно лент конечно (25–100
// позиций), и полное перечитывание окна идемпотентно (ключ date/source/title),
// а пропущенный прогон из-за этого не теряет данные. modeSince — догон по
// курсорам из news_events для ручного запуска. modeCheck — отчёт покрытия без
// записи: дыру в окне иначе не увидеть, она уже не влезает в ленту.
const (
	modeAll   = "all"
	modeSince = "since"
	modeCheck = "check"
	modeEnv   = "NEWS_WATCH_MODE"
)

// parseMode переводит значение NEWS_WATCH_MODE в режим прогона. Пустое значение —
// all: ручной запуск без переменной обязан работать как расписание, а не падать.
// Всё остальное — ошибка, и вызывающий получает её до сети и до БД: «ALL» или
// опечатка в слове, принятые за дефолт all, молча перечитали бы окно целиком там,
// где ждали догон since. В тексте ошибки — имя переменной: по нему понятно, что править.
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
	Err     error
}

// fetchFeeds читает ленты по очереди и возвращает результат на каждую. Ошибка
// ленты попадает в её feedResult.Err, а цикл продолжается: пять лент независимы,
// и обрыв одной (WAF, 500, таймаут) не повод не собрать четыре остальные.
// Агрегация ошибок — дело вызывающего (Import): он решает, чем отчитаться в dagu.
func fetchFeeds(feeds []feed) []feedResult {
	results := make([]feedResult, 0, len(feeds))
	for _, f := range feeds {
		var doc rssDoc
		res := feedResult{Feed: f}
		// URL в ошибке транспорта (util.GetXML его не маскирует) — здесь ключей
		// в query нет, ленты публичные, поэтому ошибка уезжает в лог как есть.
		if err := util.GetXML(f.URL, &doc); err != nil {
			res.Err = err
			results = append(results, res)
			continue
		}
		items, skipped, err := itemsFromDoc(f.Source, doc)
		res.Items, res.Skipped, res.Err = items, skipped, err
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

// selectSince отбирает позиции строго новее курсора своего источника. Курсор —
// max(date) сохранённых строк источника, и позиция ровно на нём уже в news_events:
// нестрогое сравнение писало бы её заново каждый прогон (для ReplacingMergeTree это
// лишний батч, а для отчёта «сколько прибавилось» — враньё). Отсутствующий или
// нулевой курсор означает первый прогон источника — тогда проходит всё окно.
func selectSince(items []newsItem, cursors map[string]time.Time) []newsItem {
	var out []newsItem
	for _, it := range items {
		if it.Date.After(cursors[it.Source]) {
			out = append(out, it)
		}
	}
	return out
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
	newsTable  = "news_events"
	newsInsert = "INSERT INTO " + newsTable
	// storedSpansQuery читает сохранённые окна источников. FINAL обязателен:
	// news_events — ReplacingMergeTree, и без него повторно записанные позиции
	// (ключ date/source/title) считались бы дважды, а курсор since и счётчик
	// покрытия check завышались бы на каждом прогоне.
	storedSpansQuery = "SELECT source, count(), min(date), max(date) FROM " + newsTable + " FINAL GROUP BY source"
)

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
	if err = conn.Exec(ctx, newsEventsDdl); err != nil {
		return 0, err
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

	var items []newsItem
	for _, r := range results {
		if r.Err == nil {
			items = append(items, r.Items...)
		}
	}
	if mode == modeSince {
		// Курсор источника — max(date) сохранённых строк. Пустая таблица даёт пустую
		// map без ошибки, и это верное поведение для первого прогона: курсоров нет,
		// проходит всё окно.
		stored, err := readStoredSpans(ctx, conn)
		if err != nil {
			// Тот же полный диагноз, что и в check: отказ лент не должен теряться
			// за ошибкой базы — иначе в dagu видно только её.
			return 0, errors.Join(err, errors.Join(feedErrs...))
		}
		cursors := make(map[string]time.Time, len(stored))
		for src, span := range stored {
			cursors[src] = span.Max
		}
		items = selectSince(items, cursors)
	}
	if len(items) == 0 {
		// Пустой список в since — норма (всё окно уже собрано, ручной повторный
		// запуск). В all то же самое означает «упали все ленты», и там молчать
		// правильно: каждая такая лента уже сказана в логе и попала в feedErrs.
		if mode == modeSince {
			log.Infof("news_watch: новых позиций нет — нечего обновлять (0 строк)")
		}
		return 0, errors.Join(feedErrs...)
	}

	if count, err = insertItems(ctx, conn, items); err != nil {
		// К ошибке батча добавляются отказы лент: иначе прогон в dagu отчитывается
		// только «запись упала», и сбойные ленты этого прогона из вывода выпадают.
		return count, errors.Join(err, errors.Join(feedErrs...))
	}
	// Успешные ленты записаны, сбойная видна в логе и в итоговой ошибке: dagu
	// перезапустит прогон, а повторная запись тех же позиций безвредна.
	return count, errors.Join(feedErrs...)
}

// readStoredSpans читает сохранённые окна источников: сколько строк по каждому
// source и в каком они диапазоне дат. Это вход режимов since (курсоры) и check
// (сверка покрытия). Пустая таблица — не ошибка, а первый прогон: вызывающий
// различает «источник ещё не писался» по отсутствию ключа, а не по нулевому окну.
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
	// Ошибка обхода (обрыв соединения на середине) обязана быть видна: молча
	// вернувшаяся половина окон дала бы ложный отчёт check и неполные курсоры.
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("news_watch: читать покрытие %s: %w", newsTable, err)
	}
	return spans, nil
}

// insertItems пишет позиции одним батчем — построчный Exec в цикле запрещён
// (правило 2 AGENTS.md). Sentiment передаётся явным (*float32)(nil): колонку
// заполняет LLM-сессия (follow-up), и типизированный nil фиксирует, что поле
// оставлено пустым осознанно. Ошибка Append прерывает батч через Abort: частично
// собранный батч Send не отправляет, поэтому половина прогона в таблицу не уедет.
func insertItems(ctx context.Context, conn driver.Conn, items []newsItem) (int64, error) {
	batch, err := conn.PrepareBatch(ctx, newsInsert)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, it := range items {
		if err = batch.Append(it.Date, it.Source, it.Title, it.URL, (*float32)(nil)); err != nil {
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
