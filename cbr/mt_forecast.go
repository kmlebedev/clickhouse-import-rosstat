package cbr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/PuerkitoBio/goquery"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// Импортёр «Комментария к среднесрочному прогнозу» Банка России — таблица cbr_mt_forecast:
// диапазоны среднегодовой ключевой ставки и прогноза инфляции вместе с остальными строками листа
// «Табл 1» (16 показателей, спека §3.2). Артефакт импортёра — не один файл, а набор выпусков
// (4 в год), поэтому у импортёра свой Import() (прецедент — cbr_dip_bulletin, спека §3.5):
// карточки выпусков разбирает mtForecastIndexReleases (индекс тот же, что читает cbr_mp_summary),
// ссылку на книгу — mtForecastPageXlsxURL, книгу — mtForecastParseTabl1
// (cbr/mt_forecast_parse.go), а файл каждого выпуска качает шаблон util.ClickHouseImport
// (DataUrl + ImportFunc для страничной книги, DataUrl + BodyFunc для PDF) — иначе не получились бы
// ни архив версии файла выпуска, ни строка source_publications (стандарт §9.6). Индекс отдаёт два
// типа карточек (спека §3.4): комментарии (на 2026-10-19 — 2 страничных xlsx и 9 PDF) и
// «Среднесрочные прогнозы» (11 PDF). Оба корпуса импортируются одним циклом: различает их
// транспорт, а не гейт (спека §2, §3.2).

const (
	// mtForecastIndexUrl — индекс материалов заседаний ЦБ: карточки «Комментария» и
	// «Среднесрочного прогноза» с датой публикации и ссылкой — страницей выпуска (у новейших
	// комментариев) или PDF. Адрес тот же, что читает cbr_mp_summary (mpSummaryIndexUrl), но
	// константа своя: импортёры делят страницу, а не код (прецедент — два импортёра macro_series
	// со своими копиями DDL), и смена адреса одного контура не должна молча уводить другой.
	// Страница выпуска = cbrUrl + href карточки.
	mtForecastIndexUrl = "https://www.cbr.ru/dkp/mp_dec/decision_key_rate/"

	// mtForecastCardTitle — титул карточки «Комментария» в видимой подписи
	// (span.document-regular_name_visible): по нему карточка отличается от соседей по индексу —
	// «Резюме обсуждения ключевой ставки» и «Среднесрочного прогноза»: у материалов один блок,
	// одни селекторы карточки/подписи/даты (mpSummaryCardSel/mpSummaryNameSel/mpSummaryDateSel
	// того же пакета, спека §3.5) и одна дата публикации.
	mtForecastCardTitle = "Комментарий к среднесрочному прогнозу Банка России"

	// mtForecastForecastCardTitle — титул карточки «Среднесрочного прогноза»: второй материал
	// заседания, который читается из того же индекса (спека §3.4). Пара «маркер href + титул»
	// отличает его от комментария и от соседей по блоку.
	mtForecastForecastCardTitle = "Среднесрочный прогноз Банка России"

	// mtForecastHrefMarker — фрагмент href карточки: comment_<DDMMYYYY>.pdf у PDF-выпуска или
	// comment_<DDMMYYYY>/ у страницы выпуска (спека §3.4). Отсекает чужие ссылки карточки.
	mtForecastHrefMarker = "comment_"

	// mtForecastForecastHrefMarker — фрагмент href карточки «Среднесрочного прогноза»:
	// forecast_<ДДММГГ>.pdf — у прогноза всегда PDF (спека §3.4); дата в имени — дата заседания.
	mtForecastForecastHrefMarker = "forecast_"

	// mtForecastXlsxSel — селектор ссылки на машиночитаемую книгу выпуска на его странице
	// («Графики и таблицы в XLSX», стандарт §4.5: константа — тот же селектор использует тест на
	// живой фикстуре). На странице ровно один xlsx-URL; имя файла (у источника опечатка
	// «commet_graph», см. спека §2) и <id> в пути меняются от выпуска к выпуску, поэтому ссылка
	// берётся со страницы, а не собирается.
	mtForecastXlsxSel = `a[href$=".xlsx"]`

	// mtForecastDdl — DDL таблицы cbr_mt_forecast (спека §3.1) в %s-форме шаблона: %s — TableName.
	// Ключ ReplacingMergeTree (indicator, year, release_date) — это и есть винтаж: одна и та же
	// пара (показатель, год) в разных выпусках — разные строки (следующий выпуск ревизует
	// диапазон), повторный прогон идемпотентен. meeting_date и url — атрибуты выпуска, не части
	// ключа (как publication_date у дипа): оговорка — подмена файла под тем же release_date
	// гейтом не отслеживается (лечение — удалить строки выпуска и повторить прогон).
	mtForecastDdl = `CREATE TABLE IF NOT EXISTS %s (
    meeting_date Date,                 -- дата опорного заседания (из шапки блока индекса)
    release_date Date,                 -- дата публикации «Комментария» (= дата в слаге URL)
    url String,                        -- страница комментария (провенанс)
    indicator LowCardinality(String),  -- стабильный код показателя (16, объявляет код, §3.2)
    year UInt16,                       -- прогнозный год
    lo Float64,                        -- нижняя граница диапазона; у точечного прогноза lo == hi
    hi Float64                         -- верхняя граница диапазона
) ENGINE = ReplacingMergeTree ORDER BY (indicator, year, release_date);
`

	// mtForecastCountPoints — количественный чек версии (стандарт §9.2): число точек своей
	// таблицы. FINAL обязателен — иначе count() считал бы несмёрженные части. Число растёт с
	// каждым выпуском, поэтому решением о no-op оно не служит (это делает гейт, спека §5.1), но
	// объявлено: без него не включатся архив версий и строки source_publications (§9.6).
	mtForecastCountPoints = "SELECT count() FROM cbr_mt_forecast FINAL"

	// mtForecastKnownReleasesQuery — гейт прогона (спека §3.5, шаг 4): даты публикации выпусков,
	// уже лежащих в таблице. Гейт заменяет HEAD-префлайт набора артефактов: PublicationCountPoints
	// считает точки всей таблицы, и чек известной версии не сошёлся бы ни разу — каждый прогон
	// пере-импортировал бы все файлы. SELECT DISTINCT отвечает на тот же вопрос («этот выпуск уже
	// импортирован?») по своим данным и не делает ни одного HEAD на повторном прогоне.
	// Самолечение «выпуск пропал из таблицы» работает: гейт смотрит на данные, а не на реестр
	// версий (спека §3.6). Следствие названо и там: подмена файла под тем же release_date гейтом
	// не отслеживается — лечение — удалить строки выпуска и повторить прогон.
	mtForecastKnownReleasesQuery = "SELECT DISTINCT release_date FROM cbr_mt_forecast FINAL"

	// mtForecastReleaseCountQuery — число строк выпуска: проверка, что успешный вызов шаблона
	// записал выпуск (спека §3.5, шаг 6). FINAL обязателен (стандарт §9.2) — как у
	// mtForecastCountPoints. Дата выпуска уходит плейсхолдером драйвера, а не литералом: это
	// данные страницы, и в тексте запроса им не место (прецедент — cbrDipReleaseCountQuery).
	mtForecastReleaseCountQuery = "SELECT count() FROM cbr_mt_forecast FINAL WHERE release_date = ?"

	// mtForecastDayLayout — формат дат в сообщениях импортёра: даты выпуска и заседания — дни,
	// и в логе они называются в том же виде, в каком лежат в таблице.
	mtForecastDayLayout = "2006-01-02"
)

// mtForecastKind — тип карточки индекса (спека §3.4): в одном блоке заседания лежат два
// материала с общими селекторами карточки, подписи и даты, но разными титулами и маркерами
// ссылок. Тип в mtForecastRelease называет, какой это выпуск, — от него зависит текст ошибок и
// счётчики, а от признака PDF — транспорт выпуска в цикле импорта.
type mtForecastKind string

const (
	// mtForecastKindComment — «Комментарий к среднесрочному прогнозу Банка России»: страница с
	// книгой «Табл 1» или PDF-выпуск.
	mtForecastKindComment mtForecastKind = "comment"
	// mtForecastKindForecast — «Среднесрочный прогноз Банка России»: в индексе только PDF.
	mtForecastKindForecast mtForecastKind = "forecast"
)

var (
	// mtForecastSlugRe — слага страницы выпуска: .../comment_<DDMMYYYY>/ (спека §3.4). Дата
	// публикации берётся отсюда — единственный источник идентичности страничного выпуска; у
	// PDF-карточек слага нет вовсе (их дата — только из карточки), а совпадение слага с датой
	// карточки проверяется явно — страница, переехавшая под другую дату, обязана быть ошибкой, а
	// не вторым винтажём.
	mtForecastSlugRe = regexp.MustCompile(`comment_(\d{8})/?$`)

	// mtForecastSlugLayout — формат даты слага: DDMMYYYY (07.05.2026 в слаге — 07052026).
	mtForecastSlugLayout = "02012006"

	// mtForecastMeetingRe — дата заседания в шапке блока индекса: «Заседание Совета директоров от
	// DD.MM.YYYY» (текст a.dropdown_title ближайшего предка div.dropdown карточки, спека §3.4).
	// Дата заседания — атрибут выпуска, не часть ключа, но без неё агенту не с чем сопоставить
	// прогноз, и разбор её не угадывает: не нашлась — ошибка.
	mtForecastMeetingRe = regexp.MustCompile(`Заседание\s+Совета\s+директоров\s+от\s+(\d{2}\.\d{2}\.\d{4})`)
)

// mtForecastIndexReleases разбирает индекс материалов заседаний ЦБ в выпуски двух типов
// (спека §3.4): комментарии (mtForecastKindComment) и «Среднесрочные прогнозы»
// (mtForecastKindForecast). Карточки отбираются одним проходом по общим селекторам индекса
// (mpSummaryCardSel/mpSummaryNameSel/mpSummaryDateSel): тип задаёт маркер href (comment_ или
// forecast_), а титул карточки подтверждает его — карточка с чужим титулом пропускается, поэтому
// «Резюме» и «Заявление» того же блока в выпуски не попадают. Порядок — порядок страницы (свежие
// первыми; хронологическую сортировку делает mtForecastNewReleases).
//
// У страничной карточки комментария release_date — из слага URL (единственный источник
// идентичности, спека §3.4) и сверяется с датой карточки (расхождение — ошибка: страница
// переехала под другую дату). У PDF-карточек и прогнозов дата публикации — только из карточки; у
// прогноза она совпадает с датой заседания. meeting_date — из шапки ближайшего блока-дропдауна
// для обоих типов: без неё агенту не с чем сопоставить прогноз. PDF-выпуски возвращаются с
// признаком PDF (файл у них — сам PDF; страницы выпуска нет) и проходят тот же гейт по дате
// публикации, что и страничные: признак PDF выбирает транспорт в цикле импорта.
//
// Fail-fast (стандарт §6.4): ни одной карточки «Комментария» (источник сломан или подменён);
// карточка без даты публикации; дата карточки не разбирается; ссылка ни PDF, ни страница вида
// comment_<DDMMYYYY>/; дата слага не разбирается; расхождение даты карточки и слага; нет шапки
// с датой заседания. Отсутствие прогнозов — warn, не ошибка: комментарии импортируются, а пробел
// виден в логе и счётчиках (спека §3.4). Сеть не трогается — страница скачана вызывающим
// (util.GetPage), разбор чистый и тестируется на HTML-байтах.
func mtForecastIndexReleases(page []byte) ([]mtForecastRelease, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, fmt.Errorf("индекс %s: разбор html: %w", mtForecastIndexUrl, err)
	}
	var (
		releases []mtForecastRelease
		parseErr error
	)
	doc.Find(mpSummaryCardSel).Each(func(_ int, card *goquery.Selection) {
		if parseErr != nil {
			return
		}
		// Маркер href называет тип выпуска: у комментария — comment_ (PDF или страница), у
		// прогноза — forecast_ (всегда PDF). Титул подтверждает пару: карточка с маркером одного
		// типа и титулом другого — не наш выпуск и пропускается, а не угадывается.
		link := card.Find(`a[href*="` + mtForecastHrefMarker + `"]`).First()
		kind, title := mtForecastKindComment, mtForecastCardTitle
		if link.Length() == 0 {
			link = card.Find(`a[href*="` + mtForecastForecastHrefMarker + `"]`).First()
			kind, title = mtForecastKindForecast, mtForecastForecastCardTitle
		}
		href, ok := link.Attr("href")
		if !ok || strings.TrimSpace(href) == "" {
			return
		}
		if normMPText(card.Find(mpSummaryNameSel).First().Text()) != title {
			return
		}
		href = strings.TrimSpace(href)

		// Дата карточки — у всех карточек: карточка без неё не называет публикацию, и структура
		// индекса изменилась. Нулевая дата молча записала бы выпуск под 1970-01-01.
		raw := normMPText(card.Find(mpSummaryDateSel).First().Text())
		published, err := time.Parse(mpIndexDateLayout, raw)
		if err != nil {
			parseErr = fmt.Errorf("индекс %s: карточка %s: дата публикации %q: %w", mtForecastIndexUrl, href, raw, err)
			return
		}

		// Дата заседания — из шапки ближайшего блока-дропдауна: у всех карточек заседания общий
		// блок, и заголовок берётся у того, внутри которого стоит карточка (спека §3.4). Нужна
		// обоим типам: комментарию — как атрибут выпуска, прогнозу — это дата его карточки.
		header := normMPText(card.Closest("div.dropdown").Find("a.dropdown_title").First().Text())
		meeting := mtForecastMeetingRe.FindStringSubmatch(header)
		if meeting == nil {
			parseErr = fmt.Errorf("индекс %s: карточка %s: в шапке блока %q нет даты заседания (ждалась «Заседание Совета директоров от DD.MM.YYYY»)",
				mtForecastIndexUrl, href, header)
			return
		}
		meetingDate, err := time.Parse(mpIndexDateLayout, meeting[1])
		if err != nil {
			parseErr = fmt.Errorf("индекс %s: карточка %s: дата заседания %q: %w", mtForecastIndexUrl, href, meeting[1], err)
			return
		}

		release := mtForecastRelease{
			Kind:        kind,
			MeetingDate: meetingDate,
			ReleaseDate: published,
			URL:         cbrUrl + href,
		}

		// PDF-выпуск: страницы у него нет, файл — сам PDF. Признак PDF выбирает транспорт в цикле
		// импорта (BodyFunc вместо страницы с книгой), а счётчики разбора делят комментарии на
		// страницы и PDF; прогноз в индексе всегда PDF (спека §3.2, §3.4). Per-card лога нет:
		// выпуск назовётся в цикле импорта (mtForecastReleaseContext), а число выпусков каждого
		// вида видно в счётчиках no-op-прогона.
		if strings.HasSuffix(href, ".pdf") {
			release.PDF = true
			releases = append(releases, release)
			return
		}

		// Страничный выпуск бывает только у комментария: у прогноза страницы нет, и не-PDF ссылка
		// означает смену вёрстки, которую разбор не угадывает.
		if kind != mtForecastKindComment {
			parseErr = fmt.Errorf("индекс %s: карточка %s: у прогноза ссылка не PDF — ожидалась forecast_<ДДММГГ>.pdf, разбор не знает, что это за выпуск",
				mtForecastIndexUrl, href)
			return
		}

		// Дата слага — идентичность страничного выпуска, дата карточки — её подтверждение.
		slug := mtForecastSlugRe.FindStringSubmatch(href)
		if slug == nil {
			parseErr = fmt.Errorf("индекс %s: карточка %s: ссылка ни PDF, ни страница выпуска вида comment_<DDMMYYYY>/ — разбор не знает, что это за выпуск",
				mtForecastIndexUrl, href)
			return
		}
		slugDate, err := time.Parse(mtForecastSlugLayout, slug[1])
		if err != nil {
			parseErr = fmt.Errorf("индекс %s: карточка %s: дата слага %q: %w", mtForecastIndexUrl, href, slug[1], err)
			return
		}
		if !slugDate.Equal(published) {
			parseErr = fmt.Errorf("индекс %s: карточка %s: дата публикации карточки %s расходится с датой слага %s — страница переехала под другую дату",
				mtForecastIndexUrl, href, published.Format(mpIndexDateLayout), slugDate.Format(mpIndexDateLayout))
			return
		}
		releases = append(releases, release)
	})
	if parseErr != nil {
		return nil, parseErr
	}
	counts := mtForecastReleaseCounts(releases)
	if counts.Comments == 0 {
		return nil, fmt.Errorf("индекс %s: не найдено ни одной карточки «%s» — комментариев 0: источник сломан или подменён (спека §3.4)",
			mtForecastIndexUrl, mtForecastCardTitle)
	}
	if counts.Forecasts == 0 {
		log.Warnf("%s: индекс %s: ни одной карточки «%s» — комментарии импортируются, прогнозы отсутствуют; проверьте вёрстку индекса",
			mtForecastTable, mtForecastIndexUrl, mtForecastForecastCardTitle)
	}
	return releases, nil
}

// mtForecastCounts — счётчики разбора индекса по типам и форматам (спека §3.4): числа входят в
// no-op-сообщение прогона «комментариев N (страниц A, PDF B), прогнозов M, в таблице K выпусков»
// и считаются из того же среза, что решает гейт, — счётчики не разъезжаются с разбором. Comments —
// все карточки комментария, из них CommentPages — страничные (xlsx-выпуски), CommentPDFs — PDF;
// Forecasts — карточки «Среднесрочного прогноза».
type mtForecastCounts struct {
	Comments     int
	CommentPages int
	CommentPDFs  int
	Forecasts    int
}

// mtForecastReleaseCounts считает выпуски разбора по типам и форматам. Неизвестный Kind не
// относится ни к одной карточке индекса и в счётчики не попадает: разбор таких не производит, а
// молчаливое причисление к комментариям скрыло бы ошибку типа.
func mtForecastReleaseCounts(releases []mtForecastRelease) mtForecastCounts {
	var counts mtForecastCounts
	for _, release := range releases {
		switch release.Kind {
		case mtForecastKindComment:
			counts.Comments++
			if release.PDF {
				counts.CommentPDFs++
				continue
			}
			counts.CommentPages++
		case mtForecastKindForecast:
			counts.Forecasts++
		}
	}
	return counts
}

// mtForecastNewReleases — выпуски, которых нет в таблице, в хронологическом порядке (старые →
// новые): первый прогон наполняет историю детерминированным префиксом, а порядок не зависит от
// порядка страницы (та отдаёт свежие первыми, спека §3.5, шаг 5). Гейт общий для обоих транспортов:
// PDF-выпуски проходят его наравне со страничными — файл у них сам PDF, и отсекать их нечем
// (признак PDF выбирает ветку Import, а не известность; числа разбора — mtForecastReleaseCounts).
// Известность — по дате выпуска (ключ винтажа): подмена файла под тем же release_date не
// отслеживается, а «выпуск пропал из таблицы» самолечится (спека §3.6). Сортировка устойчивая: у
// двух выпусков одной даты порядок страницы сохраняется — результат детерминирован и без
// уникальности дат. Чистая функция без обращений к БД и сети: сценарии «все выпуски известны»
// (пусто → no-op прогона) и «все выпуски новые» пинятся тестом.
func mtForecastNewReleases(releases []mtForecastRelease, known map[time.Time]bool) []mtForecastRelease {
	fresh := make([]mtForecastRelease, 0, len(releases))
	for _, release := range releases {
		if known[mpDay(release.ReleaseDate)] {
			continue
		}
		fresh = append(fresh, release)
	}
	sort.SliceStable(fresh, func(i, j int) bool { return fresh[i].ReleaseDate.Before(fresh[j].ReleaseDate) })
	return fresh
}

// mtForecastPageXlsxURL — ссылка на книгу выпуска с его страницы: util.FindLink по
// mtForecastXlsxSel (стандарт §4.4, §4.5). Дата выпуска входит в текст ошибки ожидаемым именем
// файла (commet_graph_<DDMMYYYY>.xlsx — опечатка «commet» настоящая, из источника, спека §2):
// без имени файла оператор видел бы «ссылка не найдена» и не знал, что именно искать на
// изменившейся странице (§4.4 требует домен и ожидаемое имя; домен даёт адрес страницы в
// обёртке Import). Ошибка при отсутствии ссылки — не «0 точек»: страница без xlsx значит, что
// ЦБ сменил вёрстку выпуска, и молчаливый пропуск оставил бы выпуск неимпортированным
// (fail-fast, стандарт §6.4). href приходит относительным — префикс домена навешивает Import
// (cbrUrl + href), как у mo_br. Страница выпуска здесь байтами, а не URL: разбор чистый, тест
// подаёт живую фикстуру (стандарт §11.3).
func mtForecastPageXlsxURL(page []byte, date time.Time) (string, error) {
	href, err := util.FindLink(page, mtForecastXlsxSel)
	if err != nil {
		return "", fmt.Errorf("страница выпуска: ожидался commet_graph_%s.xlsx (селектор %q): %w",
			date.Format(mtForecastSlugLayout), mtForecastXlsxSel, err)
	}
	return href, nil
}

// mtForecastAppendPDFText — приёмник текста PDF-выпуска: разбор «Табл. 1» и запись точек строками
// cbr_mt_forecast (спека §3.2). Отделён от mtForecastPDFAppendPoints, потому что извлечение текста
// вызывает pdftotext, а запись обязана тестироваться офлайн на уже извлечённой фикстуре (спека §7):
// сток знает только текст. Атрибуты выпуска (meeting_date, release_date, url) — первые три колонки
// DDL, порядок колонок тот же, что у xlsx-ветки (mtForecastAppendPoints). Ошибка Append называет
// таблицу, дату выпуска и точку (indicator, year), но не выпуск целиком: контекст выпуска
// (mtForecastReleaseContext) добавляет цикл Import — иначе один сбой назывался бы дважды.
func mtForecastAppendPDFText(release mtForecastRelease, text []byte, batch driver.Batch) error {
	points, err := mtForecastParsePDF(text)
	if err != nil {
		return err
	}
	for _, p := range points {
		if err := batch.Append(release.MeetingDate, release.ReleaseDate, release.URL, p.Indicator, p.Year, p.Lo, p.Hi); err != nil {
			return fmt.Errorf("%s: %s: точка (%s, %d): %w",
				mtForecastTable, release.ReleaseDate.Format(mtForecastDayLayout), p.Indicator, p.Year, err)
		}
	}
	return nil
}

// mtForecastPDFAppendPoints — BodyFunc одного PDF-выпуска: шаблон сам скачивает байты PDF и тот же
// срез кладёт в винтажный архив версии (стандарт §9.6), поэтому сети здесь нет — только извлечение
// текста из уже скачанного файла и запись точек (спека §3.2, §3.3). Страница выпуска не читается:
// файл у PDF-выпуска — сам PDF. ctx ограничивает pdftotext (mtForecastPDFTimeout), но не отменяет
// родительский контекст прогона.
func mtForecastPDFAppendPoints(ctx context.Context, release mtForecastRelease) func(body []byte, batch driver.Batch) error {
	return func(body []byte, batch driver.Batch) error {
		text, err := mtForecastPDFText(ctx, body)
		if err != nil {
			return err
		}
		return mtForecastAppendPDFText(release, text, batch)
	}
}

// mtForecastReleaseContext — выпуск в тексте ошибки и лога: тип (комментарий/прогноз), дата
// выпуска и дата заседания. Дата выпуска — ключ винтажа, и по ней выпуск ищется в таблице при
// разборе сбоя (у дипа ту же роль играет release_id); тип называет, какой материал заседания
// сломался.
func mtForecastReleaseContext(release mtForecastRelease) string {
	kind := "комментарий"
	if release.Kind == mtForecastKindForecast {
		kind = "прогноз"
	}
	return fmt.Sprintf("%s: %s: выпуск %s (заседание %s)",
		mtForecastTable, kind, release.ReleaseDate.Format(mtForecastDayLayout), release.MeetingDate.Format(mtForecastDayLayout))
}

// mtForecastStat — импортёр cbr_mt_forecast: встроенный util.ClickHouseImport ведёт таблицу, DDL
// и объявляет точку контроля публикации; Import переопределён — у импортёра не один файл, а
// набор выпусков (спека §3.5). Name() не переопределён: имя импортёра совпадает с именем
// таблицы, как у большинства пакета.
type mtForecastStat struct {
	util.ClickHouseImport
}

// Import — полный цикл одного прогона (спека §3.5): DDL → производные (каталог и витрина) →
// индекс → гейт по release_date → цикл по новым выпускам в хронологическом порядке (старые
// первыми): для страничного выпуска — страница и ссылка на книгу, для PDF — сразу адрес файла;
// вызов шаблона на каждый выпуск и проверка, что строки выпуска в таблице есть (успех шаблона
// записи не гарантирует — он умеет ответить no-op).
// Порядок шагов — часть контракта: DDL выполняется до сети и до гейта, потому что гейт
// обращается к таблице, а шаблонный DDL внутри цикла не отработал бы при нуле новых выпусков
// (прогон на чистой базе обязан отработать целиком). Сбой одного выпуска не отменяет остальные:
// ошибки копятся errors.Join с названием выпуска в тексте, прогон завершается ошибкой — dagu
// видит сбой и не пускает цепочку дальше (спека §3.5, шаг 7). Если новых выпусков нет — 0 строк
// и ни одного сетевого обращения к файлам, кроме индекса (его чтение — цена обнаружения нового
// выпуска, спека §3.5, шаг 4).
func (s *mtForecastStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	// 1. DDL — до сети: шаблонный DDL внутри цикла отработал бы только при непустом списке
	// новых выпусков, а гейт уже обращается к таблице.
	for _, ddl := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(ddl, s.TableName)); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", mtForecastTable, err)
		}
	}

	// 2. Производные — в начале прогона, до сети и гейта: сбой CreateView или каталога в прошлом
	// прогоне должен исправиться и на no-op-пути (выпуск выходит 4 раза в год — иначе устаревшая
	// витрина или каталог пролежали бы до следующего выпуска месяцами). Отступление от буквы
	// §10.2 названо в спеке §5.3; прецедент — rosstat_ipc_weeks_bulletins. AfterImport на
	// шаблонных копиях ниже не выставляется: производные публикует этот вызов.
	if err = afterCbrMtForecast(cbrMtForecastSeriesMeta)(ctx, conn); err != nil {
		return 0, err
	}

	// 3. Индекс материалов заседаний: карточки «Комментария» и «Среднесрочного прогноза» в порядке
	// страницы (свежие первыми); разбор — fail-fast (ноль карточек комментария при живом ЦБ —
	// ошибка, а не «новых выпусков нет»; отсутствие прогнозов — warn).
	page, err := util.GetPage(mtForecastIndexUrl)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", mtForecastTable, mtForecastIndexUrl, err)
	}
	releases, err := mtForecastIndexReleases(page)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", mtForecastTable, err)
	}

	// 4. Гейт: известность — по дате выпуска (ключу винтажа), а не по имени файла (в имени — дата
	// публикации, но <id> файла меняется, а дата выпуска и есть идентичность, спека §3.4).
	known, err := mtForecastKnownReleases(ctx, conn)
	if err != nil {
		return 0, err
	}
	fresh := mtForecastNewReleases(releases, known)
	if len(fresh) == 0 {
		counts := mtForecastReleaseCounts(releases)
		// Счётчики — часть сообщения: «комментариев 11: страниц 2, PDF 9; прогнозов 11» объясняет
		// оператору, почему прогон пуст (спека §3.4, §3.5, шаг 4). Сообщение называет оба корпуса:
		// отсутствие прогнозов не должно прятаться за числом комментариев.
		log.Infof("%s: новых выпусков нет (комментариев %d: страниц %d, PDF %d; прогнозов %d; в таблице %d выпусков)",
			mtForecastTable, counts.Comments, counts.CommentPages, counts.CommentPDFs, counts.Forecasts, len(known))
		return 0, nil
	}

	// 5. Цикл по новым выпускам. Ошибки копятся — сбой одного выпуска не отменяет остальные:
	// выпуски независимы (у каждого своя страница или PDF, файл и дата версии), а пропущенный
	// подхватился бы следующим прогоном, если бы прогон молча прекращался на первом сбое.
	var errs []error
	for _, release := range fresh {
		// Локальная копия встроенного шаблона: DataUrl и импорт-функция зависят от выпуска, а
		// мутировать общую структуру импортёра нельзя (прецедент — moBrStat, cbr/mo_br.go). Копия
		// приносит объявленную точку контроля публикации: без неё не было бы ни архива версии файла
		// выпуска, ни строки source_publications (стандарт §9.6), а судьбу прогона она не решает —
		// no-op считает гейт (спека §5.1). AfterImport у копии не выставляется: производные
		// публикует шаг 2 (спека §5.3).
		local := s.ClickHouseImport
		if release.PDF {
			// PDF-выпуск: страницы у него нет, файл — сам PDF. Байты качает шаблон и тот же срез
			// кладёт в винтажный архив версии (стандарт §9.6) — отдельного GET до шаблона нет.
			local.DataUrl = release.URL
			local.BodyFunc = mtForecastPDFAppendPoints(ctx, release)
		} else {
			releasePage, pageErr := util.GetPage(release.URL)
			if pageErr != nil {
				errs = append(errs, fmt.Errorf("%s: %s: %w", mtForecastReleaseContext(release), release.URL, pageErr))
				continue
			}
			href, linkErr := mtForecastPageXlsxURL(releasePage, release.ReleaseDate)
			if linkErr != nil {
				errs = append(errs, fmt.Errorf("%s: %s: %w", mtForecastReleaseContext(release), release.URL, linkErr))
				continue
			}
			local.DataUrl = cbrUrl + href
			local.ImportFunc = mtForecastAppendPoints(release)
		}
		points, importErr := local.Import(ctx, conn)
		if importErr != nil {
			errs = append(errs, fmt.Errorf("%s: %w", mtForecastReleaseContext(release), importErr))
			continue
		}
		// 6. Успешный вызов шаблона — ещё не «выпуск записан» (спека §3.5, шаг 6): его префлайт
		// умеет вернуть no-op («версия известна и число точек совпало»), и тогда новый по гейту
		// выпуск остался бы молча неимпортированным — следующий прогон снова счёл бы его новым.
		// Проверка делает эту дыру громкой ошибкой выпуска, а continue сохраняет независимость
		// остальных выпусков (прецедент — cbrDipReleaseWritten).
		if writeErr := mtForecastReleaseWritten(ctx, conn, release); writeErr != nil {
			errs = append(errs, writeErr)
			continue
		}
		count += points
		log.Infof("%s: импортировано %d точек", mtForecastReleaseContext(release), points)
	}
	// 7. Итог: число точек записанных выпусков и агрегированная ошибка (errors.Join без ошибок —
	// nil, то есть успешный прогон); ошибка прогона возвращается наверх — dagu видит красный шаг.
	return count, errors.Join(errs...)
}

// mtForecastKnownReleases — множество дат выпусков, уже лежащих в таблице (гейт, спека §3.5,
// шаг 4). Множество, а не список: в цикле важен только ответ «выпуск известен?». Драйвер отдаёт
// Date как time.Time, но ключ нормализуется mpDay (сутки UTC): сравнение должно идти по
// календарному дню, а не по моменту времени. Ошибка чтения — ошибка прогона, а не «считаем, что
// известных нет»: пустое множество пере-импортировало бы все выпуски, и сбой диагностики
// обернулся бы полным прогоном.
func mtForecastKnownReleases(ctx context.Context, conn driver.Conn) (map[time.Time]bool, error) {
	rows, err := conn.Query(ctx, mtForecastKnownReleasesQuery)
	if err != nil {
		return nil, fmt.Errorf("%s: выпуски таблицы не прочитаны: %w", mtForecastTable, err)
	}
	defer func() { _ = rows.Close() }()

	known := make(map[time.Time]bool)
	for rows.Next() {
		var date time.Time
		if err = rows.Scan(&date); err != nil {
			return nil, fmt.Errorf("%s: строка выпусков таблицы не прочитана: %w", mtForecastTable, err)
		}
		known[mpDay(date)] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: выпуски таблицы не дочитаны: %w", mtForecastTable, err)
	}
	return known, nil
}

// mtForecastReleaseWritten — проверка, что строки выпуска в таблице есть: число строк по
// release_date после успешного вызова шаблона (спека §3.5, шаг 6). Одного успеха шаблона мало:
// решения о составе выпусков его префлайт не принимает (гейт сильнее), но умеет вернуть no-op,
// если версия известна и число точек совпало, — тогда новый по гейту выпуск остался бы молча
// неимпортированным, а следующий прогон снова счёл бы его новым. Ноль строк — ошибка, и сбой
// запроса — ошибка: «считаем, что записано» пропустило бы дыру в обоих случаях. Дата выпуска
// передаётся плейсхолдером драйвера, а не литералом: это данные страницы.
func mtForecastReleaseWritten(ctx context.Context, conn driver.Conn, release mtForecastRelease) error {
	var written uint64
	if err := conn.QueryRow(ctx, mtForecastReleaseCountQuery, release.ReleaseDate).Scan(&written); err != nil {
		return fmt.Errorf("%s: число строк выпуска не прочитано: %w", mtForecastReleaseContext(release), err)
	}
	if written == 0 {
		return fmt.Errorf("%s: шаблон вернул no-op (версия известна), но строк выпуска в таблице нет — удалите строку выпуска из source_publications и повторите прогон", mtForecastReleaseContext(release))
	}
	return nil
}

// cbrMtForecastImporter — «Комментарий к среднесрочному прогнозу» Банка России: Name() =
// TableName = cbr_mt_forecast, 16 объявленных рядов (cbr/mt_forecast_meta.go). Объявление точки
// контроля публикации живёт здесь, а не в Import(): URL артефакта — не поле структуры, а адрес
// выпуска, вычисляемый в Import() (прецедент — cbrDipBulletinImporter), и тест-энфорсер реестра
// читает объявление у зарегистрированного импортёра. AfterImport не выставляется: производные
// (каталог 16 рядов и витрина v_cbr_mt_forecast, cbr/views.go) публикует Import() на каждом
// прогоне, включая no-op-путь (самолечение, спека §5.3) — шаблонной копии эта обязанность не
// передаётся. Регистрация — реестром cbr/importers.go; сетевых вызовов в init() нет.
var cbrMtForecastImporter = &mtForecastStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              mtForecastTable,
	CreateTable:            []string{mtForecastDdl},
	PublicationCountPoints: mtForecastCountPoints,
}}
