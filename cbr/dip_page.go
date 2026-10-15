package cbr

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Селекторы и признаки страницы выпусков приложений к бюллетеню «О чем говорят тренды»
// (https://www.cbr.ru/ec_research/mb/, блок «Статистические данные к бюллетеню»). Селектор —
// константа пакета, а не литерал в теле: тот же селектор использует тест на живой
// HTML-фикстуре (стандарт §4.5, §11.4).
const (
	// dipPageUrl — страница выпусков бюллетеня. Сеть делает импортёр (util.GetPage в
	// Import, спека §3.5 шаг 2): разбор обязан оставаться чистым и тестируемым офлайн.
	dipPageUrl = "https://www.cbr.ru/ec_research/mb/"

	// dipLinkSelector — ссылки на приложения к бюллетеню внутри групп годов
	// (div.versions_items). Класс a.versions_item носят и ссылки на PDF-выпуски
	// (284 на странице) и на индексы новостей, поэтому отбор идёт по href,
	// заканчивающемуся на .xlsx, — только приложения XLSX разбираются импортёром.
	dipLinkSelector = "div.versions_items a.versions_item"

	// dipTooltipAttr — атрибут ссылки с датой публикации выпуска («Опубликовано
	// 14 октября 2026»). Дата публикации — часть винтажа, поэтому ссылка без неё —
	// ошибка разбора, а не пропуск (стандарт §6.4).
	dipTooltipAttr = "data-tooltip-content"

	// dipGroupAttr — атрибут группы года: div.versions_items[data-versions-items="2026 г."]
	// содержит ссылки всех выпусков одного года. Номер выпуска в тексте ссылки года
	// не называет («№ 7 / Октябрь»), а в старых записях называет и прошлый год
	// («№ 6 / Октябрь 2020»), поэтому год берётся только отсюда.
	dipGroupAttr = "data-versions-items"

	// dipUrlPrefix — домен для относительных href (/Collection/Collection/File/<id>/…xlsx):
	// href в разметке относительный, абсолютный адрес файла = cbrUrl + href (как в
	// util.FindLink и parseMPDecIndex). Своя константа, а не cbrUrl, чтобы тест мог
	// сверить сборку адреса с литералом.
	dipUrlPrefix = "https://www.cbr.ru"
)

// dipRelease — выпуск приложений к бюллетеню, разобранный со страницы: id из пути
// (/Collection/Collection/File/<id>/…xlsx — у всех выпусков свой, ключ винтажа),
// абсолютный адрес файла, дата публикации (из подсказки ссылки) и номер выпуска
// («26-07»: две цифры года группы + номер из текста «№ 7 / Октябрь»). Имя файла
// ключом не служит сознательно: stat_bulletin_23-07.xlsx на странице встречается
// дважды под разными id (спека §2).
type dipRelease struct {
	ID              string
	URL             string
	PublicationDate time.Time
	Release         string
}

// dipReleasePathRe — путь ссылки приложения: /Collection/Collection/File/<id>/<файл>.xlsx.
// id — только цифры: он же ключ винтажа (release_id таблицы), нецифровой путь означает
// не тот источник, что разбирается.
var dipReleasePathRe = regexp.MustCompile(`^/Collection/Collection/File/(\d+)/[^/]*\.xlsx$`)

// dipTooltipRe — дата публикации в подсказке ссылки: «Опубликовано 14 октября 2026».
// Регекс вместо time.Parse с локалью: русские названия месяцев в родительном падеже
// не входят в стандартные layouts, а util.MonthsToNum их знает.
var dipTooltipRe = regexp.MustCompile(`Опубликовано\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})`)

// dipReleaseNumRe — номер выпуска в тексте ссылки: «№ 7 / Октябрь» (в старых записях —
// «№ 6 / Октябрь 2020»). Номер — первая цифра после «№»; месяц текста разбору не нужен:
// дата отчётного периода лежит в книге выпуска, а не в подписи. Неразрывный пробел
// (&#160; в разметке, U+00A0 после декодирования) приравнен к обычному: ЦБ ставит его
// между «№» и номером.
var dipReleaseNumRe = regexp.MustCompile(`№[\s\x{00A0}]*(\d{1,2})`)

// dipGroupYearRe — год из атрибута группы («2026 г.»): две последние цифры идут в release.
var dipGroupYearRe = regexp.MustCompile(`(\d{4})\s*г\.`)

// dipParseReleases разбирает страницу выпусков приложений к бюллетеню
// (https://www.cbr.ru/ec_research/mb/) в записи выпусков в порядке страницы (свежие
// первыми; сортировку по дате делает импортёр). Ссылка отбирается по href, заканчивающемуся
// на .xlsx: класс a.versions_item носят и ссылки на PDF-выпуски (284 на живой странице), и
// на индексы новостей — без фильтра в разбор попали бы PDF-выпуски и чужие файлы.
//
// Fail-fast (стандарт §6.4): страница без блока выпусков; ни одной ссылки с .xlsx; ссылка
// без подсказки с датой публикации; дата не разбирается; id не цифры; номер выпуска не
// найден в тексте; год группы не найден; дубликат id. Ошибка, а не «0 выпусков»: структура
// страницы изменилась, тихий пустой список означал бы «новых выпусков нет» при живом ЦБ.
// Сеть здесь не трогается — страница скачана вызывающим (util.GetPage), разбор чистый и
// тестируется на HTML-строке.
func dipParseReleases(page *goquery.Document) ([]dipRelease, error) {
	var (
		releases []dipRelease
		seen     = make(map[string]bool)
		parseErr error
	)
	page.Find(dipLinkSelector).Each(func(_ int, a *goquery.Selection) {
		if parseErr != nil {
			return
		}
		href, ok := a.Attr("href")
		if !ok || !strings.HasSuffix(strings.TrimSpace(href), ".xlsx") {
			return
		}
		m := dipReleasePathRe.FindStringSubmatch(strings.TrimSpace(href))
		if m == nil {
			parseErr = fmt.Errorf("страница %s: ссылка %q не похожа на путь приложения (/Collection/Collection/File/<id>/<файл>.xlsx)",
				dipPageUrl, href)
			return
		}
		id := m[1]

		// Дубликат id — ошибка: id — ключ винтажа (release_id), и два выпуска с одним id
		// дали бы в таблице две версии одного ключа, из которых гейт выбрал бы произвольную.
		if seen[id] {
			parseErr = fmt.Errorf("страница %s: дубликат id %q у ссылки %q: ключ винтажа не уникален",
				dipPageUrl, id, href)
			return
		}
		seen[id] = true

		// Дата публикации — атрибут той же ссылки: она часть винтажа, и ссылки без неё
		// в разбор не годятся (пропуск сделал бы выпуск без даты публикации).
		tooltip, ok := a.Attr(dipTooltipAttr)
		if !ok || strings.TrimSpace(tooltip) == "" {
			parseErr = fmt.Errorf("страница %s: ссылка %q без атрибута %s (дата публикации — часть винтажа)",
				dipPageUrl, href, dipTooltipAttr)
			return
		}
		published, err := dipParsePublished(tooltip)
		if err != nil {
			parseErr = err
			return
		}

		// Год выпуска — из ближайшей группы с data-versions-items («2026 г.»): в тексте
		// ссылки год есть не всегда, а в старых записях он и вовсе прошлый («№ 6 / Октябрь 2020»).
		group := a.Closest("[" + dipGroupAttr + "]")
		yearRaw, ok := group.Attr(dipGroupAttr)
		if !ok {
			parseErr = fmt.Errorf("страница %s: ссылка %q вне группы с атрибутом %s: год выпуска не найден",
				dipPageUrl, href, dipGroupAttr)
			return
		}
		ym := dipGroupYearRe.FindStringSubmatch(yearRaw)
		if ym == nil {
			parseErr = fmt.Errorf("страница %s: группа %q: год выпуска не найден", dipPageUrl, yearRaw)
			return
		}
		year, err := strconv.Atoi(ym[1])
		if err != nil {
			parseErr = fmt.Errorf("страница %s: группа %q: год %q: %w", dipPageUrl, yearRaw, ym[1], err)
			return
		}

		// Номер выпуска — из текста ссылки («№ 7 / Октябрь»); месяц текста не нужен:
		// номер выпуска в году может не совпадать с месяцем (№ 5 / Июль).
		text := strings.TrimSpace(a.Text())
		num := dipReleaseNumRe.FindStringSubmatch(text)
		if num == nil {
			parseErr = fmt.Errorf("страница %s: ссылка %q: номер выпуска не найден в тексте %q",
				dipPageUrl, href, text)
			return
		}
		numVal, err := strconv.Atoi(num[1])
		if err != nil {
			parseErr = fmt.Errorf("страница %s: ссылка %q: номер выпуска %q: %w", dipPageUrl, href, num[1], err)
			return
		}
		// Обе части release — по две цифры («26-07»): строка версии сортируется лексически
		// и сравнивается с именами файлов источников (bulletin_NN-NN.xlsx).
		release := fmt.Sprintf("%02d-%02d", year%100, numVal)

		releases = append(releases, dipRelease{
			ID:              id,
			URL:             dipUrlPrefix + strings.TrimSpace(href),
			PublicationDate: published,
			Release:         release,
		})
	})
	if parseErr != nil {
		return nil, parseErr
	}
	if len(releases) == 0 {
		// Fail-fast: страница без блока выпусков или без ссылок на приложения —
		// структура источника изменилась, и пустая дельта выглядела бы успешным прогоном.
		return nil, fmt.Errorf("страница %s: не найдено ни одной ссылки на приложение (%s с href на .xlsx)",
			dipPageUrl, dipLinkSelector)
	}
	return releases, nil
}

// dipParsePublished — дата публикации из подсказки ссылки («Опубликовано 14 октября 2026»):
// день и год цифрами, месяц — русское название в родительном падеже (util.MonthsToNum).
// Ошибка оборачивается с исходным текстом подсказки: ЦБ может сменить формулировку, и без
// исходного текста причина не называла бы источник.
func dipParsePublished(tooltip string) (time.Time, error) {
	m := dipTooltipRe.FindStringSubmatch(tooltip)
	if m == nil {
		return time.Time{}, fmt.Errorf("страница %s: подсказка %q не вида «Опубликовано <день> <месяц> <год>»",
			dipPageUrl, tooltip)
	}
	day, err := strconv.Atoi(m[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("страница %s: подсказка %q: день %q: %w", dipPageUrl, tooltip, m[1], err)
	}
	month, ok := util.MonthsToNum[m[2]]
	if !ok {
		return time.Time{}, fmt.Errorf("страница %s: подсказка %q: месяц %q не разобран", dipPageUrl, tooltip, m[2])
	}
	year, err := strconv.Atoi(m[3])
	if err != nil {
		return time.Time{}, fmt.Errorf("страница %s: подсказка %q: год %q: %w", dipPageUrl, tooltip, m[3], err)
	}
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC), nil
}
