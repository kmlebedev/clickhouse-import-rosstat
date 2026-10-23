// Package domrf — машиночитаемая аналитика ДОМ.РФ о рынке жилья: месячный рынок ипотеки
// (выдачи, ДДУ, портфель, просрочка, ставки), недельная ставка предложения по продуктам и
// индекс цен на первичном рынке по регионам.
//
// Пакет отвечает на вопрос «идёт ли кредит в недвижимость как в инвестиционный актив», а не
// «сколько семей купили квартиру»: первичка (ДДУ) отделена от вторички, цена кредита дана в
// недельном темпе, цена актива — по регионам и в паре с числом сделок.
//
// Пакет `bank/` не трогается: там остаётся `domrf_mortgage` (месячная ставка предложения по
// банкам, в долях). Новые таблицы хранят проценты, как их отдаёт источник, — смешивать единицы в
// одной витрине запрещено (см. комментарий v_domrf_mortgage).
//
// Источник закрыт вероятностным антиботом ServicePipe: на часть запросов страница приходит
// HTTP 200 с JS-оболочкой вместо HTML. Поэтому страница качается только через domrfGetPage
// (повторы + детект оболочки), а файлы — обычным GET: на них антибот не распространяется
// (проверено 2026-10-23, docs/research/2026-10-23-domrf-analytics-sources.md).
package domrf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

const (
	// domrfSite — домен источника (punycode дом.рф). Литерал: тесты сверяют сборку адресов.
	domrfSite = "https://xn--d1aqf.xn--p1ai"
	// domrfAnalyticsPage — страница, в атрибутах main#app которой лежат оба блока: ссылка на
	// книгу «Общая таблица по РФ» и недельные ставки. JSON-API раздела отдаёт только листинги
	// этих блоков (section=838 — «Топ ипотечных банков»), поэтому страница остаётся входом.
	domrfAnalyticsPage = domrfSite + "/analytics/mortgage/"
	// domrfPriceIndexUrl — JSON-API индекса цен: POST с фильтром регионов, антиботом не закрыт.
	domrfPriceIndexUrl = domrfSite + "/api/v2/priceindex/living/mainchart/"

	// domrfAppSelector — элемент, в атрибутах которого страница несёт JSON-блоки (имена
	// атрибутов — коды блоков: statistical_series, mortgage-dinamika-stavok, …).
	domrfAppSelector = "main#app"

	// domrfMaxAttempts / domrfRetryDelay — политика повторов страницы: антибот вероятностный
	// (2026-10-23: 1–5 успехов из 8–20 попыток), пауза между попытками снижает раздражение WAF.
	domrfMaxAttempts = 8
	domrfRetryDelay  = time.Second
	// domrfChallengeMaxBytes — верхняя граница размера JS-оболочки (1 787–1 795 байт на
	// 2026-10-23); реальная страница — 110–130 КБ. Порог отделяет оболочку от любой настоящей
	// страницы с запасом в 25 раз.
	domrfChallengeMaxBytes = 4096
	// domrfChallengeMarkers — маркеры JS-оболочки ServicePipe: внешний скрипт загрузчика и
	// собственный тег-заглушка. Одного размера мало: пустая страница-ошибка тоже короткая, но
	// её должен увидеть разбор, а не ретрай.
	domrfChallengeMarkers = "servicepipe js-challenge-loader"
)

// domrfIsChallenge — страница является JS-оболочкой антибота, а не содержимым. Чистая функция
// (§6.1): разбор не ходит в сеть, поэтому детект проверяется офлайн на фикстуре.
func domrfIsChallenge(page []byte) bool {
	if len(page) == 0 || len(page) >= domrfChallengeMaxBytes {
		return false
	}
	body := string(page)
	for _, marker := range strings.Fields(domrfChallengeMarkers) {
		if strings.Contains(body, marker) {
			return true
		}
	}
	return false
}

// domrfGetPage качает страницу, повторяя запрос, пока не придёт содержимое вместо оболочки.
// Ошибка последней попытки возвращается вызывающему: «страница не получена» — это сбой прогона,
// а не «0 строк» (иначе пустая таблица выглядела бы успешным импортом).
func domrfGetPage(pageUrl string) ([]byte, error) {
	var lastErr error
	for attempt := 1; attempt <= domrfMaxAttempts; attempt++ {
		page, err := util.GetPage(pageUrl)
		switch {
		case err != nil:
			lastErr = err
		case domrfIsChallenge(page):
			lastErr = fmt.Errorf("JS-оболочка антибота ServicePipe (%d байт)", len(page))
		default:
			return page, nil
		}
		if attempt < domrfMaxAttempts {
			time.Sleep(domrfRetryDelay)
		}
	}
	return nil, fmt.Errorf("%s: страница не получена за %d попыток: %w", pageUrl, domrfMaxAttempts, lastErr)
}

// domrfBlocks возвращает атрибуты main#app: имя атрибута — код блока, значение — JSON блока.
// Отсутствие элемента — ошибка: страница пришла без данных (сменилась вёрстка или источник
// отдал другую страницу), и молчаливый пустой набор блоков скрыл бы это.
func domrfBlocks(page []byte) (map[string]string, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, fmt.Errorf("разбор страницы: %w", err)
	}
	selection := doc.Find(domrfAppSelector).First()
	if selection.Length() == 0 {
		return nil, fmt.Errorf("элемент %s не найден: страница без блоков данных", domrfAppSelector)
	}
	attrs := selection.Get(0).Attr
	blocks := make(map[string]string, len(attrs))
	for _, attr := range attrs {
		if attr.Key == "id" || attr.Key == "class" {
			continue
		}
		blocks[attr.Key] = attr.Val
	}
	if len(blocks) == 0 {
		return nil, fmt.Errorf("у %s нет блоков данных", domrfAppSelector)
	}
	return blocks, nil
}

// domrfBlock разбирает JSON блока по его имени. Ошибка называет блок: у страницы их больше
// десяти, и «invalid character» без имени не подсказывает, какой из них сломался.
func domrfBlock[T any](blocks map[string]string, name string) (out T, err error) {
	raw, ok := blocks[name]
	if !ok {
		return out, fmt.Errorf("блок %q не найден среди атрибутов %s", name, domrfAppSelector)
	}
	if err = json.Unmarshal([]byte(raw), &out); err != nil {
		return out, fmt.Errorf("блок %q: разбор JSON: %w", name, err)
	}
	return out, nil
}

// domrfPriceIndexRequest — тело POST индекса цен. Поля повторяют запрос фронта источника:
// date_from/date_to существуют в контракте, но API их не принимает (400 на любой формат),
// поэтому всегда nil — история отдаётся целиком.
type domrfPriceIndexRequest struct {
	Regions  []domrfPriceIndexRequestRegion `json:"regions"`
	DateTo   *string                        `json:"date_to"`
	DateFrom *string                        `json:"date_from"`
}

// domrfPriceIndexRequestRegion — один слот фильтра: регион, класс жилья и разрез по комнатам.
// Класс источник игнорирует (в ответе всегда «Общий»), но поле обязательно в контракте.
type domrfPriceIndexRequestRegion struct {
	Region      string `json:"region"`
	EstateClass string `json:"estateClass"`
	Rooms       string `json:"rooms"`
	ID          int    `json:"id"`
}

// domrfPostPriceIndex отправляет запрос индекса цен и возвращает тело ответа. Повтор — тот же,
// что у страницы: антибот на API не наблюдался, но ответ-оболочка с кодом 200 неотличима от
// данных без этой проверки. Ошибка статуса приходит из util.PostFileBytes с началом тела: 400
// «поле date_from» — диагностика контракта, а не сетевой сбой.
func domrfPostPriceIndex(payload []byte) ([]byte, error) {
	headers := map[string]string{"Content-Type": "application/json", "Accept": "application/json"}
	var lastErr error
	for attempt := 1; attempt <= domrfMaxAttempts; attempt++ {
		body, err := util.PostFileBytes(domrfPriceIndexUrl, payload, headers)
		switch {
		case err != nil:
			lastErr = err
		case domrfIsChallenge(body):
			lastErr = fmt.Errorf("JS-оболочка антибота ServicePipe (%d байт)", len(body))
		default:
			return body, nil
		}
		if attempt < domrfMaxAttempts {
			time.Sleep(domrfRetryDelay)
		}
	}
	return nil, fmt.Errorf("%s: ответ не получен за %d попыток: %w", domrfPriceIndexUrl, domrfMaxAttempts, lastErr)
}
