package minec

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// minecImporterName — Name(), CLICKHOUSE_IMPORT_STAT и id шага dagu.
	minecImporterName = "minec_urals_price"
	// minecTable — общая macro_series: пакет владеет только своим source.
	minecTable  = "macro_series"
	minecSource = "minec"
	// minecUralsSeries — имя ряда в macro_series и series_catalog.
	minecUralsSeries = "urals_avg_price"
	// minecListingURL — страница материалов раздела; номер страницы дописывается
	// в minecPageURL.
	minecListingURL = "https://www.economy.gov.ru/material/departments/d12/konyunktura_mirovyh_tovarnyh_rynkov/materials.html"
	// minecFromEnv — ремонт/бэкфилл с месяца включительно (YYYY-MM): гейт по
	// max(date) не перечитывает прошлое, а ревизии источника это иногда требуют.
	minecFromEnv = "MINEC_URALS_FROM"
	// minecMaxPages — предохранитель обхода: живой листинг кончается на 22-й
	// странице, 60 оставляет запас на годы вперёд и не даёт уйти в бесконечный
	// цикл при «всегда непустой» разметке.
	minecMaxPages = 60
	// minecSelectLast — гейт: число точек и последний отчётный месяц своего ряда.
	// FINAL и WHERE source обязательны: иначе чек считал бы несмёрженные части и
	// чужие ряды общей таблицы (стандарт §9.2).
	minecSelectLast = "SELECT count(), max(date) FROM " + minecTable + " FINAL WHERE source = '" + minecSource + "'"
	minecInsert     = "INSERT INTO " + minecTable
	minecMacroDdl   = `CREATE TABLE IF NOT EXISTS %s (
				  source LowCardinality(String)
				, series LowCardinality(String)
				, date Date32
				, value Float64
			) ENGINE = ReplacingMergeTree ORDER BY (source, series, date);
		`
)

var (
	// minecPagePause — пауза между запросами к сайту Минэка: бэкфилл делает ~126
	// запросов подряд, и без паузы источник отвечает заметно медленнее. Переменная,
	// чтобы тесты не спали.
	minecPagePause = 300 * time.Millisecond
	// minecGetPage — транспорт одной страницы; переменная, чтобы тест подменил
	// сеть картой фикстур (стандарт §11.3: тесты офлайн).
	minecGetPage = util.GetFileBytesChecked
)

type minecUrals struct {
	util.ClickHouseImport
}

// Name — имя импортёра: таблица у него общая (macro_series), поэтому Name() !=
// TableName и продублирован методом на внешней структуре (как cbr_mo_br).
func (s *minecUrals) Name() string { return minecImporterName }

// minecUralsImporter — импортёр-краулер: один ряд, точки собираются в память и
// пишутся одним батчем. Точки контроля публикации нет (стандарт §9.3): артефакт —
// набор из 104 страниц, и отпечаток одного URL не отвечает на вопрос «что уже
// разобрано»; no-op держит гейт по max(date) внутри CrawFunc. Винтажного архива
// нет по той же причине и потому, что CrawFunc в архив не попадает (§9.6) —
// отступление названо в спеке.
var minecUralsImporter = &minecUrals{ClickHouseImport: util.ClickHouseImport{
	TableName:   minecTable,
	CreateTable: []string{minecMacroDdl},
	DataUrl:     minecListingURL,
	CrawFunc:    minecUralsCraw,
	AfterImport: afterMinecMacro,
}}

// minecPoint — точка ряда: отчётный месяц и средняя цена за него.
type minecPoint struct {
	date  time.Time
	value float64
}

// minecUralsCraw — полный цикл импортёра поверх шаблона: гейт → обход листинга и
// статей → один батч. Пустая дельта — не ошибка: повтор в тот же день штатен.
func minecUralsCraw(crawUrl string, conn driver.Conn) (count int64, err error) {
	ctx := context.Background()
	start, err := minecStartMonth(ctx, conn)
	if err != nil {
		return 0, err
	}
	points, err := minecCollect(crawUrl, start)
	if err != nil {
		return 0, err
	}
	if len(points) == 0 {
		log.Infof("%s: новых месяцев нет (%s)", minecImporterName, minecStartLabel(start))
		return 0, nil
	}
	return minecInsertPoints(ctx, conn, points)
}

// minecStartMonth — с какого месяца импортировать. Пустая таблица — с начала
// истории источника; иначе следующий месяц после max(date). MINEC_URALS_FROM
// переопределяет гейт (включительно) и служит единственным путём ремонта:
// ревизия уже импортированного месяца гейтом не видна.
func minecStartMonth(ctx context.Context, conn driver.Conn) (time.Time, error) {
	if from := strings.TrimSpace(os.Getenv(minecFromEnv)); from != "" {
		start, err := time.Parse("2006-01", from)
		if err != nil {
			return time.Time{}, fmt.Errorf("%s: %s=%q: ожидается YYYY-MM: %w", minecImporterName, minecFromEnv, from, err)
		}
		return start, nil
	}
	var rows uint64
	var last time.Time
	if err := conn.QueryRow(ctx, minecSelectLast).Scan(&rows, &last); err != nil {
		return time.Time{}, fmt.Errorf("%s: max(date): %w", minecImporterName, err)
	}
	return minecGate(rows, last), nil
}

// minecGate — начало импорта по состоянию таблицы: пустая — нулевая дата (вся
// история), иначе следующий месяц после последнего записанного.
func minecGate(rows uint64, last time.Time) time.Time {
	if rows == 0 {
		return time.Time{}
	}
	return time.Date(last.Year(), last.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)
}

// minecStartLabel — начало истории для лога: нулевая дата читалась бы как
// «0001-01».
func minecStartLabel(start time.Time) string {
	if start.IsZero() {
		return "с начала истории"
	}
	return "с " + start.Format("2006-01")
}

// minecCollect обходит листинг с первой страницы и тянет статьи только тех
// месяцев, которых ещё нет в таблице. Обход прекращается, как только на странице
// встретился месяц раньше start: листинг упорядочен по дате публикации по
// убыванию. Известные месяцы не запрашиваются вовсе — в этом инкремент.
func minecCollect(listingURL string, start time.Time) ([]minecPoint, error) {
	var points []minecPoint
	seen := make(map[time.Time]string)
	reachedEnd := false
	for page := 1; page <= minecMaxPages; page++ {
		body, err := minecFetch(minecPageURL(listingURL, page))
		if err != nil {
			return nil, fmt.Errorf("%s: листинг, страница %d: %w", minecImporterName, page, err)
		}
		first := page == 1
		parsed, err := minecParseListing(body, first)
		if err != nil {
			return nil, fmt.Errorf("%s: листинг, страница %d: %w", minecImporterName, page, err)
		}
		if parsed.items == 0 {
			// Пустое тело — страницы за последней сайт отдаёт как «\n\n».
			if first {
				return nil, fmt.Errorf("%s: листинг, страница 1: ни одного материала — разметка источника изменилась или страница подменена", minecImporterName)
			}
			reachedEnd = true
			break
		}
		oldest := time.Time{}
		for _, link := range parsed.urals {
			if oldest.IsZero() || link.month.Before(oldest) {
				oldest = link.month
			}
			if link.month.Before(start) {
				continue
			}
			if prev, ok := seen[link.month]; ok {
				if prev != link.href {
					// Исправленная публикация месяца под другим слагом не должна
					// блокировать ряд: берём первую (более свежую) ссылку листинга.
					log.Warnf("%s: месяц %s встречается дважды: %s и %s; берётся первая", minecImporterName, link.month.Format("2006-01"), prev, link.href)
				}
				continue
			}
			point, err := minecFetchArticle(listingURL, link)
			if err != nil {
				return nil, err
			}
			seen[link.month] = link.href
			points = append(points, point)
		}
		if !oldest.IsZero() && oldest.Before(start) {
			break
		}
	}
	if start.IsZero() && !reachedEnd {
		// Бэкфилл упёрся в cap, не дойдя до пустой страницы: это не «нет данных»,
		// а сломанный листинг — иначе история молча обрезалась бы.
		return nil, fmt.Errorf("%s: листинг не кончился за %d страниц", minecImporterName, minecMaxPages)
	}
	return points, nil
}

// minecFetchArticle скачивает статью месяца и сверяет её отчётный месяц с
// листингом: расхождение означает, что источник противоречит сам себе, и писать
// точку «не туда» нельзя.
func minecFetchArticle(listingURL string, link minecLink) (minecPoint, error) {
	articleURL, err := minecArticleURL(listingURL, link.href)
	if err != nil {
		return minecPoint{}, err
	}
	body, err := minecFetch(articleURL)
	if err != nil {
		return minecPoint{}, fmt.Errorf("%s: статья %s: %w", minecImporterName, articleURL, err)
	}
	price, month, err := minecParseArticle(body)
	if err != nil {
		return minecPoint{}, fmt.Errorf("%s: статья %s: %w", minecImporterName, articleURL, err)
	}
	if !month.Equal(link.month) {
		return minecPoint{}, fmt.Errorf("%s: статья %s: месяц в заголовке %s не совпал с листингом %s", minecImporterName, articleURL, month.Format("2006-01"), link.month.Format("2006-01"))
	}
	return minecPoint{date: month, value: price}, nil
}

// minecArticleURL достраивает абсолютный адрес статьи от адреса листинга: тест
// подменяет хост на httptest, а прод берёт домен источника.
func minecArticleURL(listingURL, href string) (string, error) {
	if strings.HasPrefix(href, "http://") || strings.HasPrefix(href, "https://") {
		return href, nil
	}
	base, err := url.Parse(listingURL)
	if err != nil {
		return "", fmt.Errorf("%s: разбор адреса листинга %q: %w", minecImporterName, listingURL, err)
	}
	if base.Scheme == "" || base.Host == "" {
		return "", fmt.Errorf("%s: в адресе листинга %q нет схемы и хоста", minecImporterName, listingURL)
	}
	if !strings.HasPrefix(href, "/") {
		href = "/" + href
	}
	return base.Scheme + "://" + base.Host + href, nil
}

// minecInsertPoints пишет все новые точки одним батчем: построчный Exec в цикле
// запрещён (AGENTS.md, правило 2), а недописанный батч закрывается Abort — иначе
// соединение не вернётся в пул.
func minecInsertPoints(ctx context.Context, conn driver.Conn, points []minecPoint) (count int64, err error) {
	batch, err := conn.PrepareBatch(ctx, minecInsert)
	if err != nil {
		return 0, err
	}
	for _, point := range points {
		if err = batch.Append(minecSource, minecUralsSeries, point.date, point.value); err != nil {
			if aerr := batch.Abort(); aerr != nil {
				log.Warnf("%s: не удалось закрыть батч после ошибки: %v", minecImporterName, aerr)
			}
			return 0, fmt.Errorf("%s: append %s: %w", minecImporterName, point.date.Format("2006-01"), err)
		}
	}
	if err = batch.Send(); err != nil {
		return 0, fmt.Errorf("%s: send: %w", minecImporterName, err)
	}
	return int64(len(points)), nil
}

// minecPageURL — адрес страницы листинга; разделитель учитывает уже имеющийся
// query.
func minecPageURL(listingURL string, page int) string {
	sep := "?"
	if strings.Contains(listingURL, "?") {
		sep = "&"
	}
	return fmt.Sprintf("%s%spage=%d", listingURL, sep, page)
}

// minecFetch — единственная точка исходящих запросов: пауза перед каждым,
// чтобы бэкфилл не забивал источник; тест отключает её через minecPagePause.
func minecFetch(url string) ([]byte, error) {
	if minecPagePause > 0 {
		time.Sleep(minecPagePause)
	}
	return minecGetPage(url)
}
