package minec

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const (
	// minecPageMarker — заголовок раздела в <title> первой страницы листинга: по
	// нему полная страница отличается от заглушки WAF. Страницы 2+ сайт отдаёт
	// AJAX-фрагментом карточек без <title>, поэтому маркер проверяется только у
	// первой.
	minecPageMarker = "Конъюнктура мировых товарных рынков"
	// minecSectionPath — путь раздела в href материалов.
	minecSectionPath = "/konyunktura_mirovyh_tovarnyh_rynkov/"
	// minecUralsMarker — слово в заголовке материала: слаг URL у источника менялся
	// трижды (o_srednem_urovne_cen_nefti_…, o_srednem_urovne_ceny_…,
	// o_sredney_cene_na_neft_…), поэтому ряд опознаётся по заголовку.
	minecUralsMarker = "Юралс"
	// minecLinkSelector — карточка материала: ссылка с title, ведущая в раздел.
	minecLinkSelector    = `a[href*="` + minecSectionPath + `"][title]`
	minecTitleSelector   = "header.e-title.e-title_main"
	minecContentSelector = "section.e-material__content"
	// minecMaxPrice — верхняя граница разумной цены, долл. за баррель: значение
	// выше — сбой разбора, а не рынок.
	minecMaxPrice = 1000.0
)

var (
	// minecTitleMonthRe — «за октябрь 2026 года»; регистр и «ё» нормализуются
	// до применения.
	minecTitleMonthRe = regexp.MustCompile(`(?:^|\s)за\s+([а-я]+)\s+(\d{4})`)
	// minecPriceRe — «101,46 долл. США за баррель». Левая граница — не цифра: без
	// неё «1101,46» читалось бы как «101,46». Разрядов тысяч нет: цена нефти в
	// долларах за баррель — двух-трёхзначное число (minecMaxPrice страхует выше).
	// Порядок «долл. США за баррель» закреплён 104 живыми страницами 2018–2026.
	minecPriceRe = regexp.MustCompile(`(?:^|\D)(\d{1,3},\d+)\s*долл\.\s*США\s+за\s+баррель`)
)

// minecMonths — русские месяцы в формах, которые встречаются в заголовках
// («за январь», «за мая»): после «за» источник использует именительный падеж,
// но карта принимает и родительный — смена формулировки не должна давать новый
// ряд или пропуск.
var minecMonths = map[string]time.Month{
	"январь": time.January, "января": time.January,
	"февраль": time.February, "февраля": time.February,
	"март": time.March, "марта": time.March,
	"апрель": time.April, "апреля": time.April,
	"май": time.May, "мая": time.May,
	"июнь": time.June, "июня": time.June,
	"июль": time.July, "июля": time.July,
	"август": time.August, "августа": time.August,
	"сентябрь": time.September, "сентября": time.September,
	"октябрь": time.October, "октября": time.October,
	"ноябрь": time.November, "ноября": time.November,
	"декабрь": time.December, "декабря": time.December,
}

// minecLink — материал листинга с отчётным месяцем, взятым из заголовка.
type minecLink struct {
	href  string
	title string
	month time.Time
}

// minecListingPage — разобранная страница листинга: число всех материалов
// (нужно, чтобы отличить конец списка от страницы без Urals) и ссылки на Urals.
type minecListingPage struct {
	items int
	urals []minecLink
}

// minecParseListing разбирает страницу материалов. first — первая страница:
// сайт отдаёт её полной страницей с <title> раздела, а страницы 2+ — AJAX-
// фрагментом карточек, поэтому маркер требуется только у первой.
//
// Пустое тело — конец списка (страницы за последней отдают «\n\n»), вызывающий
// решает, ошибка это или нет. Непустое тело без единого материала — ошибка:
// иначе заглушка WAF обрезала бы историю молча (стандарт §6.4).
func minecParseListing(body []byte, first bool) (minecListingPage, error) {
	var page minecListingPage
	if len(bytes.TrimSpace(body)) == 0 {
		return page, nil
	}
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return page, fmt.Errorf("разбор страницы: %w", err)
	}
	if first {
		title := minecNormalize(doc.Find("title").First().Text())
		if !strings.Contains(title, minecPageMarker) {
			return page, fmt.Errorf("страница не похожа на список материалов раздела (нет %q в заголовке: %q)", minecPageMarker, title)
		}
	}
	var parseErrs []error
	doc.Find(minecLinkSelector).Each(func(_ int, s *goquery.Selection) {
		href := strings.TrimSpace(s.AttrOr("href", ""))
		linkTitle := strings.TrimSpace(s.AttrOr("title", ""))
		if href == "" || linkTitle == "" {
			return
		}
		page.items++
		if !strings.Contains(linkTitle, minecUralsMarker) {
			return
		}
		month, err := minecParseTitleMonth(linkTitle)
		if err != nil {
			parseErrs = append(parseErrs, fmt.Errorf("%s: %w", href, err))
			return
		}
		page.urals = append(page.urals, minecLink{href: href, title: linkTitle, month: month})
	})
	if len(parseErrs) > 0 {
		return page, errors.Join(parseErrs...)
	}
	if page.items == 0 {
		return page, fmt.Errorf("на странице нет ни одного материала — разметка источника изменилась или страница подменена")
	}
	return page, nil
}

// minecParseArticle разбирает статью: цену в «долл. США за баррель» и отчётный
// месяц из заголовка. Ровно одно совпадение цены — контракт: ноль означает
// смену подписи или вёрстки, два — неоднозначность, какую цифру писать.
func minecParseArticle(body []byte) (price float64, month time.Time, err error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("разбор страницы: %w", err)
	}
	title := minecNormalize(doc.Find(minecTitleSelector).First().Text())
	if title == "" {
		return 0, time.Time{}, fmt.Errorf("заголовок страницы не найден (%s)", minecTitleSelector)
	}
	if !strings.Contains(title, minecUralsMarker) {
		return 0, time.Time{}, fmt.Errorf("страница не про Urals: %q", title)
	}
	month, err = minecParseTitleMonth(title)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("%q: %w", title, err)
	}
	content := minecNormalize(doc.Find(minecContentSelector).First().Text())
	if content == "" {
		return 0, time.Time{}, fmt.Errorf("%q: содержимое страницы не найдено (%s)", title, minecContentSelector)
	}
	matches := minecPriceRe.FindAllStringSubmatch(content, -1)
	if len(matches) != 1 {
		return 0, time.Time{}, fmt.Errorf("%q: цена «долл. США за баррель» найдена %d раз (ожидалась ровно одна)", title, len(matches))
	}
	price, err = minecParsePrice(matches[0][1])
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("%q: %w", title, err)
	}
	return price, month, nil
}

// minecParsePrice переводит «101,46» (возможные разделители разрядов — пробел,
// NBSP, узкий NBSP) в число.
func minecParsePrice(raw string) (float64, error) {
	clean := strings.NewReplacer(" ", "", "\u00a0", "", "\u202f", "", ",", ".").Replace(raw)
	price, err := strconv.ParseFloat(clean, 64)
	if err != nil {
		return 0, fmt.Errorf("значение %q не число: %w", raw, err)
	}
	if price <= 0 || price > minecMaxPrice || math.IsNaN(price) || math.IsInf(price, 0) {
		return 0, fmt.Errorf("значение %q вне допустимого диапазона", raw)
	}
	return price, nil
}

// minecParseTitleMonth вынимает отчётный месяц из заголовка «… за <месяц> <год>
// года» и возвращает 1-е число месяца в UTC: дата в macro_series — отчётный
// период, а не дата публикации.
func minecParseTitleMonth(title string) (time.Time, error) {
	normalized := strings.ToLower(strings.ReplaceAll(title, "ё", "е"))
	m := minecTitleMonthRe.FindStringSubmatch(normalized)
	if m == nil {
		return time.Time{}, fmt.Errorf("в заголовке %q нет месяца и года («за <месяц> <год> года»)", title)
	}
	month, ok := minecMonths[m[1]]
	if !ok {
		return time.Time{}, fmt.Errorf("неизвестный месяц %q в заголовке %q", m[1], title)
	}
	year, err := strconv.Atoi(m[2])
	if err != nil {
		return time.Time{}, fmt.Errorf("год %q в заголовке %q: %w", m[2], title, err)
	}
	return time.Date(year, month, 1, 0, 0, 0, 0, time.UTC), nil
}

// minecNormalize схлопывает пробелы, включая NBSP из HTML, в один: разбор ищет
// подпись цены как непрерывный текст.
func minecNormalize(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
