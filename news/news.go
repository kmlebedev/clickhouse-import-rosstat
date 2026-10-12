package news

import (
	"fmt"
	"sort"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
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
type sourceSpan struct {
	Count    uint64
	Min, Max time.Time
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
// Поэтому Count суммируется, а Min/Max берутся крайними. Результаты с ошибкой
// и без позиций пропускаются: пустое окно (нулевое время) — это не «дыра от 1970 года»,
// а отсутствие данных, и отчитывается о нём не покрытие, а ошибка ленты.
func feedSpans(results []feedResult) map[string]sourceSpan {
	spans := make(map[string]sourceSpan)
	for _, r := range results {
		if r.Err != nil || len(r.Items) == 0 {
			continue
		}
		span := spans[r.Feed.Source]
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
// продолжение покрытия, а не разрыв. Источник без сохранённых строк и источник
// с пустой лентой дырой не считаются: первого ещё не было, у второго ошибка ленты.
// По каждому источнику — одна строка, отсортировано по имени источника.
func detectGaps(feed, stored map[string]sourceSpan) []string {
	var gaps []string
	for _, src := range sortedSources(feed) {
		f, s := feed[src], stored[src]
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
// словами («первый прогон не выполнялся»), а не нулевым окном 0001-01-01.
func coverageLines(feed, stored map[string]sourceSpan) []string {
	lines := make([]string, 0, len(feed))
	for _, src := range sortedSources(feed) {
		f, s := feed[src], stored[src]
		if s.Count == 0 {
			lines = append(lines, fmt.Sprintf("%s: сохранённых строк нет (первый прогон не выполнялся)", src))
			continue
		}
		lines = append(lines, fmt.Sprintf("%s: сохранено %d (%s…%s), в ленте %d (%s…%s)",
			src, s.Count, s.Min.Format(time.RFC3339), s.Max.Format(time.RFC3339),
			f.Count, f.Min.Format(time.RFC3339), f.Max.Format(time.RFC3339)))
	}
	return lines
}
