package cbr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/shakinm/xlsReader/xls"
	"github.com/shakinm/xlsReader/xls/structure"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

// Квартальный корпус среднерыночных значений полной стоимости потребительских кредитов (займов)
// (ПСК) кредитных организаций — таблица cbr_psk (спека 2026-11-03 §2.3/§3.4/§3.5 C).
// https://www.cbr.ru/statistics/bank_sector/psk/
//
// Артефакт — не один файл, а лента из 48 выпусков трёх эр формата (23 PDF 2014-11 … 2020-05,
// 13 XLS 2020-08 … 2023-08, 12 XLSX 2023-11 … 2026-08), поэтому у импортёра свой Import()
// (прецеденты — cbr_dip_bulletin, cbr_mt_forecast): лента страницы отдаёт ссылки
// /Collection/Collection/File/<id>/<DDMMYYYY>_ko.<ext> и актуальных выпусков, и версионных
// списков, поэтому regex по всему HTML видит полный набор, а дедупликация идёт по <id>.
// Тип ko — только кредитные организации; mfo/kpk/skpk/lomb — вне объёма (спека §6).
// Файлы качает шаблон util.ClickHouseImport (BodyFunc: байты приходят срезом, он же уходит в
// винтажный архив версии, стандарт §9.6), а разбор — три адаптера одной нормализованной формы
// «код | категория | среднерыночное | предельное | период».

const (
	// cbrPskTable — таблица импортёра: её называют сообщения разбора и ошибки. Имя то же, что у
	// реестра cbr/importers.go (там — литералом, стандарт §8.2).
	cbrPskTable = "cbr_psk"

	// cbrPskPageURL — лента выпусков ПСК: блоки пяти типов кредиторов (ko, mfo, kpk, skpk, lomb) и
	// версионные списки каждого выпуска. Импортёр читает страницу каждый прогон — это цена
	// обнаружения нового выпуска, а не скачивание файлов (спека §3.4).
	cbrPskPageURL = "https://www.cbr.ru/statistics/bank_sector/psk/"

	// cbrPskSheet — имя листа XLSX-эры («AVG»): у XLSX-книг лист стабилен, и смена имени обязана
	// упасть с именем листа, а не вернуть ноль точек. XLS-эра лист данных выбирает по содержимому:
	// в её книгах встречаются «AVG» (шаблон без чисел), «Лист1» и «Лист2» (спека §3.4).
	cbrPskSheet = "AVG"

	// cbrPskCountPoints — количественный чек версии (стандарт §9.2): число точек своей таблицы.
	// У самого импортёра точка контроля публикации НЕ объявлена (спека §3.5 C: префлайт шаблона
	// не подменяет ленту, а чек известной версии по счёту всей таблицы не сошёлся бы ни разу).
	// Запрос нужен локальным копиям шаблона в Import(): объявление точки контроля у копии включает
	// общую механику версии — строку source_publications и, если хранилище настроено, винтажный
	// архив файла выпуска. Судьбу прогона решает гейт по ленте, а не этот чек.
	cbrPskCountPoints = "SELECT count() FROM cbr_psk FINAL"

	// cbrPskKnownReleasesQuery — гейт прогона (спека §3.4): имена файлов выпусков, уже
	// импортированных (ключ версии — имя файла <DDMMYYYY>_ko.<ext>: оно однозначно называет выпуск
	// ленты, а <id> файла в имени не участвует). DISTINCT не даёт множеству раздуться версиями
	// одного выпуска (файл мог перепубликоваться под новым отпечатком). Источник — общая
	// source_publications: имя файла пишет общая механика версии локальной копии шаблона.
	cbrPskKnownReleasesQuery = "SELECT DISTINCT name FROM source_publications FINAL WHERE source = 'cbr_psk'"

	// cbrPskReleaseCountQuery — число строк выпуска по source_file: проверка, что успешный вызов
	// шаблона записал выпуск. Имя файла уходит плейсхолдером драйвера, а не литералом: это данные
	// ленты. FINAL обязателен — как у cbrPskCountPoints.
	cbrPskReleaseCountQuery = "SELECT count() FROM cbr_psk FINAL WHERE source_file = ?"

	// cbrPskCatalogQuery — динамический каталог рядов (спека §3.5 C): по каждому коду категории —
	// наименование из самого позднего выпуска. Идентичность ряда — (код + наименование как
	// опубликовано); argMax по date даёт актуальное имя, а расхождение исторических имён одного
	// кода названо в description каталога (код менял смысл между редакциями таксономии).
	cbrPskCatalogQuery = "SELECT category_code, argMax(category, date) FROM cbr_psk FINAL GROUP BY category_code ORDER BY category_code"

	// cbrPskPeriodAnchor — якорь строки периода выпуска: «за период с … по …». Строка есть во всех
	// трёх эрах (PDF — без кавычек, XLS/XLSX — с «»); дата точки — её конец (спека §3.4).
	cbrPskPeriodAnchor = "за период с"

	// cbrPskHeaderAnchor — подпись шапки машиночитаемых выпусков: «Номер строки». От неё
	// отсчитываются строка номеров колонок и строки категорий; номер строки в разборе не
	// фиксируется числом (стандарт §6.3).
	cbrPskHeaderAnchor = "Номер строки"

	// cbrPskDayLayout — формат дат в сообщениях импортёра: даты точек и публикаций — дни.
	cbrPskDayLayout = "2006-01-02"

	// cbrPskPDFHeaderSize — сколько первых байт ответа проверяется на сигнатуру `%PDF-` до exec
	// (WAF отдаёт HTML-заглушку с кодом 200; спецификация PDF допускает мусор перед заголовком,
	// но не больше 1024 байт).
	cbrPskPDFHeaderSize = 1024

	// cbrPskPDFTimeout — потолок одного извлечения текста PDF: файлы корпуса ~120 КБ разбираются
	// за доли секунды, две минуты — запас на медленный хост, а не рабочее время.
	cbrPskPDFTimeout = 2 * time.Minute
)

// cbrPskDdl — DDL таблицы cbr_psk (спека §3.2) в %s-форме шаблона: %s — TableName. Ключ
// ReplacingMergeTree (category_code, date): ревизия категории новым выпуском того же периода
// заменяет строку, а не дублирует её. Обе величины одной строкой — это одна публикация и одна
// семантика (среднерыночное значение и его предельный кап); psk_max Nullable: в ранних выпусках
// (PDF-эра 2014) предельное значение ещё не публиковалось, и NULL значит «в выпуске его не было»,
// а не ноль. source_file — винтажный след выпуска.
// «%%» в комментариях колонок — экранирование для fmt.Sprintf шаблона (шаблон форматирует DDL
// именем таблицы): без него комментарий превратился бы в «%!г(MISSING)».
const cbrPskDdl = `CREATE TABLE IF NOT EXISTS %s (
    date Date,                         -- конец квартала расчёта (конец периода «за период с … по …»)
    category_code String,              -- код категории из выпуска (смысл кода менялся между редакциями таксономии)
    category LowCardinality(String),   -- наименование категории как опубликовано в выпуске
    psk_avg Float64,                   -- среднерыночное значение ПСК, %% годовых
    psk_max Nullable(Float64),         -- предельное значение ПСК (среднерыночное × 4/3, кап по ч. 11 ст. 6 353-ФЗ), %% годовых; NULL — в выпуске не публиковалось
    source_file LowCardinality(String) -- имя файла выпуска (14082026_ko.xlsx) — провенанс
) ENGINE = ReplacingMergeTree ORDER BY (category_code, date);
`

// cbrPskAlterMaxNullable — идемпотентная миграция уже созданной таблицы: живой прогон
// 2026-11-03 создал psk_max как Float64, а ранние PDF предельного значения не публикуют. ALTER
// выполняется раз за прогон (в списке CreateTable импортёра) и сохраняет 746 уже импортированных
// строк; на свежей таблице это no-op-изменение типа на тот же Nullable(Float64).
const cbrPskAlterMaxNullable = "ALTER TABLE %s MODIFY COLUMN psk_max Nullable(Float64)"

var (
	// cbrPskReleaseRe — ссылка на файл выпуска ko в ленте: и актуальные файлы, и версионные списки
	// (data-versions-item) лежат в одном HTML, поэтому regex видит полный корпус. Тип зашит в
	// шаблон: mfo/kpk/skpk/lomb не совпадают вовсе (спека §2.3, §6).
	cbrPskReleaseRe = regexp.MustCompile(`href="(/Collection/Collection/File/(\d+)/(\d{8})_ko\.(xlsx|xls|pdf))"`)

	// cbrPskReleaseLayout — формат даты публикации в имени файла: DDMMYYYY (14.08.2026 —
	// 14082026). Дата публикации — атрибут выпуска и порядок первого прогона; дата точки — конец
	// периода из заголовка, а не дата файла (сдвиг на квартал запрещён, спека §3.4).
	cbrPskReleaseLayout = "02012006"

	// cbrPskCodeRe — код категории: точка-разделённые числа («1», «2.1.1», «8.3»). Строка с кодом
	// без чисел — секция таксономии, а не точка (спека §3.4).
	cbrPskCodeRe = regexp.MustCompile(`^\d+(?:\.\d+)*$`)

	// cbrPskFootnoteRe — сноска XLSX-выпуска: номер сноски и пробел в начале первой ячейки
	// («1 Среднерыночные значения …»). Сноска стоит после данных и завершает таблицу; в XLS и PDF
	// сноска начинается со звёздочки.
	cbrPskFootnoteRe = regexp.MustCompile(`^\d+\s`)

	// cbrPskPeriodEndRe — конец периода из заголовка выпуска: «за период с 1 июля по 30 сентября
	// 2018 г.», «за период с «01» апреля 2026 г. по «30» июня 2026 г.». Кавычки снимаются до
	// разбора, поэтому шаблон общий для всех трёх эр; месяц — в родительном падеже (свой словарь,
	// util.MonthsToNum — для именительного).
	cbrPskPeriodEndRe = regexp.MustCompile(`за\s+период\s+с\s+.*?\s+по\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})`)

	// cbrPskPDFRowRe — строка таблицы PDF: код в начале строки и остаток (наименование и значения).
	// Многострочные наименования секций не имеют кода в начале и пропускаются.
	cbrPskPDFRowRe = regexp.MustCompile(`^(\d+(?:\.\d+)*)\s+(.*)$`)

	// cbrPskPDFValueRe — значение PDF-эры: десятичная запятая источника («12,673»), точка тоже
	// принимается. Дробная часть обязательна: шапка колонок («1 2 3 4») и номера строк —
	// целые токены, и без этого требования они стали бы значениями.
	cbrPskPDFValueRe = regexp.MustCompile(`^-?\d+[,.]\d+$`)

	// cbrPskPDFTool — утилита извлечения текста (poppler-utils). Переменная, а не константа, ради
	// тестов: подмена заведомо отсутствующим именем проверяет ветку ошибки, не запуская pdftotext.
	cbrPskPDFTool = "pdftotext"

	// cbrPskPDFSpaceReplacer — типографика PDF-строки: NBSP и служебные пробелы становятся
	// обычными, soft hyphen удаляется (он режет слова в заголовках ЦБ). Переводы строк не
	// заменяются: строка уже разрезана по \n.
	cbrPskPDFSpaceReplacer = strings.NewReplacer(
		"\u00a0", " ", "\u2002", " ", "\u2009", " ", "\u200a", " ", "\u202f", " ",
		"\t", " ", "\f", " ",
		"\u00ad", "",
	)

	// cbrPskMonthsGenitive — месяцы в родительном падеже: так их печатает заголовок периода во всех
	// трёх эрах («по 30 сентября 2018 г.»). Словарь свой, а не util.MonthsToNum: тот обслуживает
	// именительный падеж меток «Январь 2014» и не должен обрастать формами ради одного заголовка.
	cbrPskMonthsGenitive = map[string]time.Month{
		"января": time.January, "февраля": time.February, "марта": time.March,
		"апреля": time.April, "мая": time.May, "июня": time.June,
		"июля": time.July, "августа": time.August, "сентября": time.September,
		"октября": time.October, "ноября": time.November, "декабря": time.December,
	}

	// cbrPskPeriodQuoteReplacer — кавычки заголовка периода (ёлочки XLSX-эры, типографские
	// двойные PDF) снимаются до разбора: шаблон периода один на все эры.
	cbrPskPeriodQuoteReplacer = strings.NewReplacer(
		"«", "", "»", "", "\"", "", "“", "", "”", "", "„", "",
	)

	// cbrPskPDFStarGapRe — межбуквенный «**»-разделитель повреждённого текстового слоя PDF:
	// двойная звёздочка между буквами или цифрами. Обычная сноска источника («**Расчет…») стоит
	// в начале строки и под шаблон не попадает (слева от неё нет буквы/цифры).
	cbrPskPDFStarGapRe = regexp.MustCompile(`[\p{L}\p{N}]\*\*[\p{L}\p{N}]`)

	// errCbrPskUnreadablePDF — типизированный признак «текстовый слой PDF нечитаем»: pdftotext
	// вернул текст, но буквы и цифры в нём потеряны или перемешаны (живой выпуск 13022015_ko.pdf:
	// значения вида «1,», месяцы в чужой кодировке, управляющие байты на месте цифр). Такой выпуск
	// не разбирается ни одной опцией pdftotext, поэтому Import пропускает его с предупреждением и
	// сохраняет версию с 0 точек, чтобы гейт не скачивал файл каждый прогон.
	errCbrPskUnreadablePDF = errors.New("нечитаемый текстовый слой PDF")

	// errCbrPskNoPoints — признак «в разобранной таблице нет ни одной точки». Для PDF это значит,
	// что текстовый слой не дал таблицы (проверяется вместе с errCbrPskUnreadablePDF), а для
	// XLS/XLSX — что лист не содержит чисел и не является листом данных (перебор листов по
	// содержимому продолжается).
	errCbrPskNoPoints = errors.New("точек нет")

	// errCbrPskNoPeriod — в листе нет строки периода «за период с … по …»: для XLS это признак
	// служебного/шаблонного листа (например, AVG без чисел в выпусках 2022 года), а не ошибка
	// книги — перебор листов по содержимому идёт дальше.
	errCbrPskNoPeriod = errors.New("нет строки периода")
)

// cbrPskRelease — выпуск ленты: id файла (ключ версии в URL), имя файла (ключ версии в
// source_publications), расширение (выбирает адаптер разбора), дата публикации из имени файла
// (порядок прогона и атрибут выпуска) и адрес файла.
type cbrPskRelease struct {
	ID        string
	FileName  string
	Ext       string
	Published time.Time
	URL       string
}

// cbrPskRow — строка таблицы выпуска до нормализации: код и наименование категории, сырые ячейки
// среднерыночного и предельного значений. Пустые значения допустимы: секции таксономии и листья
// без чисел различаются по наличию дочерних кодов (cbrPskPoints).
type cbrPskRow struct {
	Code     string
	Category string
	Avg      string
	Max      string
}

// cbrPskPoint — точка разбора: категория, среднерыночное значение и предельное (NULL, если в
// выпуске его не публиковали: PDF-эра 2014 печатала только среднерыночное). Период в точку не
// входит: он общий для выпуска и добавляется при записи строки таблицы.
type cbrPskPoint struct {
	Code     string
	Category string
	Avg      float64
	Max      *float64
}

// cbrPskParseReleases разбирает ленту выпусков ko со страницы ПСК: regex по всему HTML (актуальные
// файлы и версионные списки), дедупликация по <id>, фильтр типа ko зашит в шаблон ссылки. Ошибки —
// fail-fast (стандарт §6.4): ни одной ko-ссылки (источник сломан или подменён); дата в имени файла
// не разбирается; один id встретился с разными путями/датами (структура ленты изменилась).
// Порядок результата — хронологический (старые → новые): первый прогон наполняет историю
// детерминированным префиксом, а порядок не зависит от порядка страницы. Сеть не трогается —
// страница скачана вызывающим, разбор чистый и тестируется на HTML-байтах.
func cbrPskParseReleases(page []byte) ([]cbrPskRelease, error) {
	matches := cbrPskReleaseRe.FindAllSubmatch(page, -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("%s: лента %s: ни одной ссылки вида /Collection/Collection/File/<id>/<DDMMYYYY>_ko.<ext> — источник сломан или подменён",
			cbrPskTable, cbrPskPageURL)
	}
	seen := make(map[string]cbrPskRelease, len(matches))
	names := make(map[string]string, len(matches))
	releases := make([]cbrPskRelease, 0, len(matches))
	for _, match := range matches {
		path, id, dateText, ext := string(match[1]), string(match[2]), string(match[3]), strings.ToLower(string(match[4]))
		published, err := time.Parse(cbrPskReleaseLayout, dateText)
		if err != nil {
			return nil, fmt.Errorf("%s: лента %s: дата публикации %q в ссылке %s: %w",
				cbrPskTable, cbrPskPageURL, dateText, path, err)
		}
		release := cbrPskRelease{
			ID:        id,
			FileName:  fmt.Sprintf("%s_ko.%s", dateText, ext),
			Ext:       ext,
			Published: published,
			URL:       cbrUrl + path,
		}
		if prev, ok := seen[id]; ok {
			// Один id — один файл: расхождение пути, даты или расширения означает, что лента
			// перестала быть набором выпусков, и угадывать, какой из двух верен, нельзя.
			if prev.URL != release.URL || !prev.Published.Equal(release.Published) {
				return nil, fmt.Errorf("%s: лента %s: id %s встретился дважды с разными файлами (%s и %s) — структура ленты изменилась",
					cbrPskTable, cbrPskPageURL, id, prev.URL, release.URL)
			}
			continue
		}
		// Ключ гейта и версии — имя файла; два id с одним именем (дата публикации и расширение)
		// сделали бы известность одного выпуска ответом за другой, а no-op — ложным.
		if prevID, ok := names[release.FileName]; ok {
			return nil, fmt.Errorf("%s: лента %s: имя файла %s у двух выпусков (id %s и %s) — ключ версии перестал быть уникальным",
				cbrPskTable, cbrPskPageURL, release.FileName, prevID, id)
		}
		names[release.FileName] = id
		seen[id] = release
		releases = append(releases, release)
	}
	sort.SliceStable(releases, func(i, j int) bool {
		if !releases[i].Published.Equal(releases[j].Published) {
			return releases[i].Published.Before(releases[j].Published)
		}
		return releases[i].ID < releases[j].ID
	})
	return releases, nil
}

// cbrPskKnownReleases — множество имён файлов выпусков, уже импортированных (ключ версии в
// source_publications, гейт спеки §3.4). Множество, а не список: в цикле важен только ответ
// «выпуск известен?». Ошибка чтения — ошибка прогона, а не «считаем, что известных нет»: пустое
// множество пере-импортировало бы весь корпус и вместо no-op устроило бы полный прогон.
func cbrPskKnownReleases(ctx context.Context, conn driver.Conn) (map[string]bool, error) {
	rows, err := conn.Query(ctx, cbrPskKnownReleasesQuery)
	if err != nil {
		return nil, fmt.Errorf("%s: выпуски таблицы не прочитаны: %w", cbrPskTable, err)
	}
	defer func() { _ = rows.Close() }()

	known := make(map[string]bool)
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("%s: строка выпусков таблицы не прочитана: %w", cbrPskTable, err)
		}
		known[name] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: выпуски таблицы не дочитаны: %w", cbrPskTable, err)
	}
	return known, nil
}

// cbrPskNewReleases — выпуски ленты, которых нет в source_publications, в хронологическом порядке
// (старые → новые). Чистая функция без обращений к БД и сети: сценарии «все выпуски известны»
// (пусто → no-op) и «все выпуски новые» пинятся тестом, а цикл скачивания в Import идёт только по
// результату этой функции — известный выпуск не скачивается вовсе.
func cbrPskNewReleases(releases []cbrPskRelease, known map[string]bool) []cbrPskRelease {
	fresh := make([]cbrPskRelease, 0, len(releases))
	for _, release := range releases {
		if known[release.FileName] {
			continue
		}
		fresh = append(fresh, release)
	}
	sort.SliceStable(fresh, func(i, j int) bool {
		if !fresh[i].Published.Equal(fresh[j].Published) {
			return fresh[i].Published.Before(fresh[j].Published)
		}
		return fresh[i].ID < fresh[j].ID
	})
	return fresh
}

// cbrPskPeriodEnd разбирает конец периода из заголовка выпуска: «за период с 1 июля по 30 сентября
// 2018 г.» → 2018-09-30, «за период с «01» апреля 2026 г. по «30» июня 2026 г.» → 2026-06-30.
// Дата точки — конец периода расчёта, а не дата публикации: fallback по дате файла запрещён
// (сдвиг на квартал), непарсящийся заголовок — fail-fast (спека §3.4). Кавычки, NBSP и лишние
// пробелы снимаются до разбора; месяц — в родительном падеже.
func cbrPskPeriodEnd(text string) (time.Time, error) {
	normalized := cbrPskNormalizeText(cbrPskPeriodQuoteReplacer.Replace(strings.ToLower(text)))
	match := cbrPskPeriodEndRe.FindStringSubmatch(normalized)
	if match == nil {
		return time.Time{}, fmt.Errorf("заголовок периода %q не разобран как «за период с … по <день> <месяц> <год> г.»", text)
	}
	day, err := strconv.Atoi(match[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("день %q в заголовке периода %q: %w", match[1], text, err)
	}
	month, ok := cbrPskMonthsGenitive[match[2]]
	if !ok {
		return time.Time{}, fmt.Errorf("неизвестный месяц %q в заголовке периода %q", match[2], text)
	}
	year, err := strconv.Atoi(match[3])
	if err != nil {
		return time.Time{}, fmt.Errorf("год %q в заголовке периода %q: %w", match[3], text, err)
	}
	if year < 2000 || year > 2100 {
		return time.Time{}, fmt.Errorf("год %d в заголовке периода %q вне диапазона 2000–2100", year, text)
	}
	date := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	// time.Date нормализует 31 июня в 1 июля; такой заголовок — смена формы, а не точка.
	if date.Day() != day || date.Month() != month || date.Year() != year {
		return time.Time{}, fmt.Errorf("дата %02d.%02d.%d в заголовке периода %q не существует", day, int(month), year, text)
	}
	return date, nil
}

// cbrPskCleanControls снимает управляющие символы ячейки: NUL-байты и прочие коды < 0x20 (кроме
// табуляции/перевода строки/возврата каретки, которые становятся пробелом). Живой выпуск
// 16112022_ko.xls хранит коды категорий с хвостом NUL-байтов («5\x00\x00…»), и без очистки код не
// совпал бы с шаблоном; наименования и значения чистятся тем же правилом.
func cbrPskCleanControls(raw string) string {
	return strings.Map(func(symbol rune) rune {
		switch symbol {
		case '\t', '\n', '\r':
			return ' '
		}
		if symbol < 0x20 || symbol == 0x7f {
			return -1
		}
		return symbol
	}, raw)
}

// cbrPskNormalizeText приводит ячейку или строку к сравнимому виду: управляющие символы снимаются,
// NBSP и служебные пробелы становятся обычными, soft hyphen удаляется, серии пробелов схлопываются,
// края обрезаются.
func cbrPskNormalizeText(raw string) string {
	raw = cbrPskPDFSpaceReplacer.Replace(cbrPskCleanControls(raw))
	return strings.Join(strings.Fields(raw), " ")
}

// cbrPskCell — ячейка строки по индексу колонки: GetRows обрезает хвостовые пустые ячейки, короткая
// строка — норма; отсутствующая ячейка равна пустой (стандарт §6.5, §6.7). Управляющие символы и
// NUL-байты снимаются здесь же: книги XLS-эры приходят с ними в кодах и значениях.
func cbrPskCell(row []string, column int) string {
	if column < 0 || column >= len(row) {
		return ""
	}
	return strings.TrimSpace(strings.ReplaceAll(cbrPskCleanControls(row[column]), "\u00a0", " "))
}

// cbrPskColumnNumbersRow — строка номеров колонок машиночитаемого выпуска («1 2 3 4»): она стоит
// сразу под шапкой и не является строкой категорий. Проверяются первые четыре ячейки: набор шире
// четырёх колонок в объём не входит, а строка данных с кодом «1» имеет в колонке B наименование,
// а не «2».
func cbrPskColumnNumbersRow(row []string) bool {
	if len(row) < 4 {
		return false
	}
	for i := 0; i < 4; i++ {
		number, err := strconv.Atoi(cbrPskCell(row, i))
		if err != nil || number != i+1 {
			return false
		}
	}
	return true
}

// cbrPskPeriodFromGrid находит строку периода в листе и разбирает её конец. Якорь — подстрока
// «за период с», а не номер строки: у эр разметка сдвинута (XLSX — строка 2, XLS — строка 2, но
// заголовки книги разные), и добавленная строка подписи не должна сдвигать разбор. found = false —
// лист не похож на лист данных (шаблонный AVG без периода в выпусках 2022 года): перебор листов по
// содержимому продолжается. Ошибка разбора заголовка возвращается отдельно: вызывающий сам решает,
// пропустить лист или упасть.
func cbrPskPeriodFromGrid(rows [][]string) (date time.Time, found bool, err error) {
	for _, row := range rows {
		for _, cell := range row {
			if strings.Contains(strings.ToLower(cbrPskNormalizeText(cell)), cbrPskPeriodAnchor) {
				date, err = cbrPskPeriodEnd(cell)
				return date, true, err
			}
		}
	}
	return time.Time{}, false, nil
}

// cbrPskRowsFromGrid разбирает строки категорий листа в нормализованные строки: код в колонке A,
// наименование в B, среднерыночное в C, предельное в D (спека §3.4). Шапка ищется по подписи
// «Номер строки», строка номеров колонок под ней пропускается. Сноска после данных завершает
// разбор (номер сноски с пробелом или звёздочка в первой колонке); любая другая непустая строка
// без кода категории — ошибка: это смена структуры листа, а не конец данных. where называет лист:
// в XLS лист данных выбирается по содержимому и не всегда называется AVG.
func cbrPskRowsFromGrid(rows [][]string, where string) ([]cbrPskRow, error) {
	header := -1
	for i, row := range rows {
		if len(row) > 0 && strings.EqualFold(cbrPskNormalizeText(cbrPskCell(row, 0)), cbrPskHeaderAnchor) {
			header = i
			break
		}
	}
	if header < 0 {
		return nil, fmt.Errorf("%s: %s: не найдена шапка «%s» — источник сменил структуру листа",
			cbrPskTable, where, cbrPskHeaderAnchor)
	}
	first := header + 1
	if first < len(rows) && cbrPskColumnNumbersRow(rows[first]) {
		first++
	}
	out := make([]cbrPskRow, 0, len(rows)-first)
	for i := first; i < len(rows); i++ {
		row := rows[i]
		code := cbrPskCell(row, 0)
		if code == "" {
			continue
		}
		if !cbrPskCodeRe.MatchString(code) {
			// Сноска после данных: номер сноски с пробелом (XLSX) или звёздочка (XLS/PDF-форма в
			// книге). Подпись «Опубликовано …» тоже стоит ниже таблицы и завершает разбор.
			lower := strings.ToLower(code)
			if strings.HasPrefix(code, "*") || cbrPskFootnoteRe.MatchString(code) || strings.HasPrefix(lower, "опубликовано") {
				break
			}
			return nil, fmt.Errorf("%s: %s, строка %d: код %q не похож на код категории — структура листа изменилась",
				cbrPskTable, where, i+1, code)
		}
		out = append(out, cbrPskRow{
			Code:     code,
			Category: cbrPskNormalizeText(cbrPskCell(row, 1)),
			Avg:      cbrPskCell(row, 2),
			Max:      cbrPskCell(row, 3),
		})
	}
	return out, nil
}

// cbrPskParseGrid разбирает один лист книги XLS/XLSX: период, шапка, строки категорий и точки.
// Нет строки периода — errCbrPskNoPeriod, нет точек — errCbrPskNoPoints: по этим двум признакам
// cbrPskParseXls отличает служебный лист (шаблон AVG без чисел) от листа данных и продолжает
// перебор. Остальные ошибки — смена разметки листа и наверх.
func cbrPskParseGrid(rows [][]string, where string) (time.Time, []cbrPskPoint, error) {
	period, found, err := cbrPskPeriodFromGrid(rows)
	if err != nil {
		return time.Time{}, nil, fmt.Errorf("%s: %s: %w", cbrPskTable, where, err)
	}
	if !found {
		return time.Time{}, nil, fmt.Errorf("%s: %s: не найдена строка периода «%s … по …»: %w",
			cbrPskTable, where, cbrPskPeriodAnchor, errCbrPskNoPeriod)
	}
	raw, err := cbrPskRowsFromGrid(rows, where)
	if err != nil {
		return time.Time{}, nil, err
	}
	points, err := cbrPskPoints(raw, where)
	if err != nil {
		return time.Time{}, nil, err
	}
	return period, points, nil
}

// cbrPskValue — значение ячейки: пустая ячейка, литеральный «-» и X/х/X X (в том числе
// разнесённые по двум колонкам в секциях XLS-выпусков) — «значения нет» (ok = false, не ошибка);
// непустой произвольный текст — ошибка (смена структуры листа, стандарт §6.4); «NaN»/«Inf»
// ParseFloat принимает, но значением ПСК они не являются и отравили бы колонку Float64.
// Десятичный разделитель — запятая PDF-эры или точка машиночитаемых выпусков.
func cbrPskValue(raw string) (value float64, ok bool, err error) {
	cell := strings.TrimSpace(cbrPskCleanControls(raw))
	if cell == "" || cell == "-" {
		return 0, false, nil
	}
	switch strings.ToLower(strings.Join(strings.Fields(cell), " ")) {
	case "x", "х", "x x", "х х", "x х", "х x":
		return 0, false, nil
	}
	value, err = strconv.ParseFloat(strings.ReplaceAll(cell, ",", "."), 64)
	if err != nil {
		return 0, false, fmt.Errorf("значение %q: %w", cell, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false, fmt.Errorf("значение %q не конечно (NaN/Inf)", cell)
	}
	return value, true, nil
}

// cbrPskNullable — значение nullable-колонки psk_max для batch.Append: nil вместо указателя,
// иначе драйвер получил бы *float64 и отверг вставку.
func cbrPskNullable(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

// cbrPskHasChild — есть ли у кода дочерний код (code + «.»): секция таксономии отличается от листа
// без значений тем, что под ней стоят строки более глубокого кода. Структурный признак, а не
// глубина кода: в XLSX-эре код «2.1» — секция с детьми 2.1.1/2.1.2, а в XLS-эре «2.1» — лист с
// числами, и жёсткая глубина ошиблась бы на одной из эр.
func cbrPskHasChild(codes map[string]bool, code string) bool {
	prefix := code + "."
	for other := range codes {
		if strings.HasPrefix(other, prefix) {
			return true
		}
	}
	return false
}

// cbrPskPoints нормализует строки выпуска в точки: среднерыночное значение — точка, предельное
// необязательно (в PDF-эре 2014 его ещё не публиковали — psk_max = NULL); строка без обоих
// значений — секция (если у кода есть дети) или лист без чисел (тогда пропуск с предупреждением,
// спека §3.4); предельное без среднерыночного — ошибка: такой лист не имеет смысла и означает
// сдвиг колонок. Ноль точек после разбора — ошибка-признак errCbrPskNoPoints: «0 rows» выглядели
// бы успешным прогоном, а выпуск остался бы непрочитанным. where называет разобранную таблицу
// (лист книги или PDF-таблицу): ошибка обязана назвать место, где искать смену разметки.
func cbrPskPoints(rows []cbrPskRow, where string) ([]cbrPskPoint, error) {
	codes := make(map[string]bool, len(rows))
	for _, row := range rows {
		codes[row.Code] = true
	}
	points := make([]cbrPskPoint, 0, len(rows))
	for _, row := range rows {
		avg, hasAvg, err := cbrPskValue(row.Avg)
		if err != nil {
			return nil, fmt.Errorf("%s: %s, категория %q (%s), среднерыночное значение: %w",
				cbrPskTable, where, row.Category, row.Code, err)
		}
		maxValue, hasMax, err := cbrPskValue(row.Max)
		if err != nil {
			return nil, fmt.Errorf("%s: %s, категория %q (%s), предельное значение: %w",
				cbrPskTable, where, row.Category, row.Code, err)
		}
		switch {
		case hasAvg:
			if row.Category == "" {
				return nil, fmt.Errorf("%s: %s, код %q: точка без наименования категории — структура выпуска изменилась",
					cbrPskTable, where, row.Code)
			}
			point := cbrPskPoint{Code: row.Code, Category: row.Category, Avg: avg}
			if hasMax {
				point.Max = &maxValue
			}
			points = append(points, point)
		case hasMax:
			return nil, fmt.Errorf("%s: %s, категория %q (%s): предельное значение без среднерыночного — сдвиг колонок или смена структуры выпуска",
				cbrPskTable, where, row.Category, row.Code)
		default:
			if cbrPskHasChild(codes, row.Code) {
				continue // секция таксономии: код есть, значений нет
			}
			log.Warnf("%s: %s, категория %q (%s) без значений — точка пропущена (стандарт §6.6)",
				cbrPskTable, where, row.Category, row.Code)
		}
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: разбор %s не дал ни одной точки: %w", cbrPskTable, where, errCbrPskNoPoints)
	}
	return points, nil
}

// cbrPskSheetXlsx находит лист «AVG» книги XLSX-эры по имени: листов в книге немного, но порядок
// не контракт, а смена имени обязана упасть с именем листа (стандарт §6.4).
func cbrPskSheetXlsx(xlsx *excelize.File) (string, error) {
	sheets := xlsx.GetSheetList()
	for _, sheet := range sheets {
		if sheet == cbrPskSheet {
			return sheet, nil
		}
	}
	return "", fmt.Errorf("%s: в книге нет листа «%s» (листы книги: %v) — источник сменил имя листа или разбирается чужая книга",
		cbrPskTable, cbrPskSheet, sheets)
}

// cbrPskParseXlsx разбирает выпуск XLSX-эры (2023-11 …): лист «AVG», строка периода, шапка и
// строки категорий. Значения — точка-десятичные (RawCellValue), поэтому ParseFloat читает их как
// есть; форматированная строка потеряла бы точность.
func cbrPskParseXlsx(body []byte) (time.Time, []cbrPskPoint, error) {
	xlsx, err := excelize.OpenReader(bytes.NewReader(body))
	if err != nil {
		return time.Time{}, nil, fmt.Errorf("%s: excelize: %w", cbrPskTable, err)
	}
	defer func() { _ = xlsx.Close() }()
	sheet, err := cbrPskSheetXlsx(xlsx)
	if err != nil {
		return time.Time{}, nil, err
	}
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return time.Time{}, nil, fmt.Errorf("%s: лист «%s»: %w", cbrPskTable, sheet, err)
	}
	return cbrPskParseGrid(rows, fmt.Sprintf("лист «%s»", sheet))
}

// cbrPskXlsCell приводит ячейку BIFF к строке: числа — без потери точности (FormatFloat -1),
// подписи — очищенными от управляющих символов и NUL-байтов (XLS-выпуск 16112022 хранит коды с
// хвостом NUL) и обрезанными. GetString числовой ячейки зависит от формата книги, поэтому числа
// читаются через GetFloat64 — так же, как это делает fedstat/excel.go.
func cbrPskXlsCell(cell structure.CellData) string {
	typ := cell.GetType()
	if strings.HasSuffix(typ, "Number") || strings.HasSuffix(typ, "Rk") {
		return strconv.FormatFloat(cell.GetFloat64(), 'f', -1, 64)
	}
	return strings.TrimSpace(cbrPskCleanControls(cell.GetString()))
}

// cbrPskXlsGrid переводит строки листа BIFF в строковую сетку: ячейки — cbrPskXlsCell, длина строк
// сохраняется (короткие строки добиваются пустыми значениями при чтении cbrPskCell).
func cbrPskXlsGrid(sheet xls.Sheet) [][]string {
	rows := sheet.GetRows()
	grid := make([][]string, 0, len(rows))
	for _, row := range rows {
		cells := row.GetCols()
		converted := make([]string, len(cells))
		for i, cell := range cells {
			converted[i] = cbrPskXlsCell(cell)
		}
		grid = append(grid, converted)
	}
	return grid
}

// cbrPskParseXls разбирает выпуск XLS-эры (2020-08 … 2023-08): BIFF-книга, лист данных выбирается
// по содержимому, а не по имени (спека §3.4): в выпусках 2022 года лист «AVG» — шаблон с одними
// названиями категорий, числа лежат в «Лист2»; в выпусках 2023 года лист называется «Лист1».
// Кандидат — лист, где есть строка периода «за период с … по …» И хотя бы одна точка с числом;
// служебные листы (нет периода или нет чисел) пропускаются. Ни одного кандидата — ошибка с первой
// причиной и списком листов. X/х/X X/пусто/«-» в числовых колонках — «данных нет», не ошибка.
func cbrPskParseXls(body []byte) (time.Time, []cbrPskPoint, error) {
	workbook, err := xls.OpenReader(bytes.NewReader(body))
	if err != nil {
		return time.Time{}, nil, fmt.Errorf("%s: открытие .xls: %w", cbrPskTable, err)
	}
	sheets := workbook.GetSheets()
	names := make([]string, 0, len(sheets))
	var firstErr error
	for _, sheet := range sheets {
		name := strings.TrimSpace(cbrPskCleanControls(sheet.GetName()))
		names = append(names, name)
		grid := cbrPskXlsGrid(sheet)
		period, points, parseErr := cbrPskParseGrid(grid, fmt.Sprintf("лист %q", name))
		if parseErr == nil {
			return period, points, nil
		}
		if errors.Is(parseErr, errCbrPskNoPeriod) || errors.Is(parseErr, errCbrPskNoPoints) {
			// Служебный лист: шаблон без периода или без чисел. Перебор продолжается.
			continue
		}
		if firstErr == nil {
			firstErr = parseErr
		}
	}
	if firstErr != nil {
		return time.Time{}, nil, fmt.Errorf("%s: ни одного листа с периодом и числами (листы книги: %v); первая ошибка: %w",
			cbrPskTable, names, firstErr)
	}
	return time.Time{}, nil, fmt.Errorf("%s: ни в одном листе книги нет строки периода «%s … по …» и чисел (листы книги: %v) — источник сменил структуру книги",
		cbrPskTable, cbrPskPeriodAnchor, names)
}

// cbrPskPDFUnreadable распознаёт повреждённый текстовый слой PDF: управляющие байты (кроме
// табуляции, переводов строк и form feed) на месте букв/цифр или межбуквенные «**»-разделители.
// Живой выпуск 13022015_ko.pdf даёт значения «1,» и месяцы в чужой кодировке с байтами 0x13–0x1c;
// ни одна опция pdftotext (-raw, -enc UTF-8/Latin1, -nopgbrk) его не восстанавливает.
func cbrPskPDFUnreadable(text []byte) bool {
	for _, symbol := range text {
		if symbol < 0x20 && symbol != '\t' && symbol != '\n' && symbol != '\r' && symbol != '\f' {
			return true
		}
	}
	return cbrPskPDFStarGapRe.Match(text)
}

// cbrPskParsePDFText разбирает PDF-выпуск (2014-11 … 2020-05) из текста `pdftotext -layout`:
// строка периода, затем строки таблицы «код  наименование  [среднерыночное  предельное]».
// Многострочные наименования секций не имеют кода в начале и пропускаются; строка сноски
// «*Среднерыночные…» завершает таблицу (спека §3.4). Значения — с десятичной запятой. Две
// величины — среднерыночное и предельное; одна (PDF-эра 2014) — только среднерыночное, предельное
// не публиковалось (psk_max = NULL). Ноль точек — признак нечитаемого текстового слоя: валидный
// PDF с таблицей обязан дать хотя бы одну точку, поэтому выпуск пропускается, а не валит прогон.
func cbrPskParsePDFText(text []byte) (time.Time, []cbrPskPoint, error) {
	if cbrPskPDFUnreadable(text) {
		return time.Time{}, nil, fmt.Errorf("%s: PDF-выпуск: %w: в тексте потеряны буквы/цифры (управляющие байты или межбуквенные **), разбор невозможен",
			cbrPskTable, errCbrPskUnreadablePDF)
	}
	lines := strings.Split(string(text), "\n")
	for i := range lines {
		lines[i] = strings.TrimSpace(cbrPskPDFSpaceReplacer.Replace(lines[i]))
	}

	periodIndex := -1
	var period time.Time
	for i, line := range lines {
		if !strings.Contains(strings.ToLower(cbrPskNormalizeText(line)), cbrPskPeriodAnchor) {
			continue
		}
		date, err := cbrPskPeriodEnd(line)
		if err != nil {
			return time.Time{}, nil, fmt.Errorf("%s: PDF-выпуск: %w", cbrPskTable, err)
		}
		period, periodIndex = date, i
		break
	}
	if periodIndex < 0 {
		return time.Time{}, nil, fmt.Errorf("%s: PDF-выпуск: не найдена строка периода «%s … по …» — вёрстка PDF изменилась",
			cbrPskTable, cbrPskPeriodAnchor)
	}

	var rows []cbrPskRow
	for i := periodIndex + 1; i < len(lines); i++ {
		line := lines[i]
		if line == "" {
			continue
		}
		// Сноска источника: «*Среднерыночные значения …» — таблица кончилась.
		if strings.HasPrefix(line, "*") {
			break
		}
		match := cbrPskPDFRowRe.FindStringSubmatch(line)
		if match == nil {
			// Продолжение многострочного наименования секции или текст шапки: кода в начале нет.
			continue
		}
		code := match[1]
		fields := strings.Fields(match[2])
		if len(fields) == 0 {
			continue
		}
		lastValue := cbrPskPDFValueRe.MatchString(fields[len(fields)-1])
		prevValue := len(fields) >= 2 && cbrPskPDFValueRe.MatchString(fields[len(fields)-2])
		switch {
		case lastValue && prevValue:
			if len(fields) < 3 {
				return time.Time{}, nil, fmt.Errorf("%s: PDF-выпуск, строка %d, код %q: значения без наименования категории — вёрстка PDF изменилась",
					cbrPskTable, i+1, code)
			}
			rows = append(rows, cbrPskRow{
				Code:     code,
				Category: strings.Join(fields[:len(fields)-2], " "),
				Avg:      fields[len(fields)-2],
				Max:      fields[len(fields)-1],
			})
		case lastValue:
			if len(fields) < 2 {
				return time.Time{}, nil, fmt.Errorf("%s: PDF-выпуск, строка %d, код %q: значение без наименования категории — вёрстка PDF изменилась",
					cbrPskTable, i+1, code)
			}
			// Ранние PDF печатали только среднерыночное значение; предельного в выпуске нет.
			rows = append(rows, cbrPskRow{
				Code:     code,
				Category: strings.Join(fields[:len(fields)-1], " "),
				Avg:      fields[len(fields)-1],
			})
		default:
			rows = append(rows, cbrPskRow{Code: code, Category: strings.Join(fields, " ")})
		}
	}

	points, err := cbrPskPoints(rows, "PDF-таблица")
	if err != nil {
		if errors.Is(err, errCbrPskNoPoints) {
			return time.Time{}, nil, fmt.Errorf("%s: PDF-выпуск: %w: разбор не дал ни одной точки — текст повреждён или таблица не распознана",
				cbrPskTable, errCbrPskUnreadablePDF)
		}
		return time.Time{}, nil, err
	}
	return period, points, nil
}

// cbrPskPDFText извлекает текст PDF-выпуска утилитой `pdftotext` (poppler-utils). Байты приходят
// срезом от шаблона: util.ClickHouseImport сам качает файл и кладёт тот же срез в винтажный архив,
// поэтому сети здесь нет — только локальное преобразование уже скачанного PDF. Fail-fast: первые
// 1024 байта обязаны содержать сигнатуру `%PDF-` (WAF/HTML-заглушка приходит с кодом 200); ошибка
// запуска называет утилиту и подсказывает poppler-utils; пустой stdout — тоже ошибка (PDF без
// текстового слоя не разобрать).
func cbrPskPDFText(ctx context.Context, body []byte) ([]byte, error) {
	head := body[:min(len(body), cbrPskPDFHeaderSize)]
	if !bytes.Contains(head, []byte("%PDF-")) {
		return nil, fmt.Errorf("%s: ответ не PDF (заголовок %q) — WAF/HTML вместо файла", cbrPskTable, "%PDF-")
	}
	ctx, cancel := context.WithTimeout(ctx, cbrPskPDFTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, cbrPskPDFTool, "-layout", "-enc", "UTF-8", "-nopgbrk", "-", "-")
	cmd.Stdin = bytes.NewReader(body)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		stderrText := strings.TrimSpace(stderr.String())
		if stderrText == "" {
			stderrText = "stderr пуст"
		}
		return nil, fmt.Errorf("%s: %s: %w: %s (нужен poppler-utils: pdftotext)",
			cbrPskTable, cbrPskPDFTool, err, stderrText)
	}
	if len(bytes.TrimSpace(output)) == 0 {
		return nil, fmt.Errorf("%s: %s: пустой stdout — в PDF нет текстового слоя (скан?) или pdftotext не смог его извлечь",
			cbrPskTable, cbrPskPDFTool)
	}
	return output, nil
}

// cbrPskParseRelease выбирает адаптер по расширению выпуска и возвращает период с точками. PDF
// сначала извлекается в текст (внешняя утилита), книги разбираются excelize/xlsReader напрямую.
func cbrPskParseRelease(ctx context.Context, body []byte, release cbrPskRelease) (time.Time, []cbrPskPoint, error) {
	switch release.Ext {
	case "xlsx":
		return cbrPskParseXlsx(body)
	case "xls":
		return cbrPskParseXls(body)
	case "pdf":
		text, err := cbrPskPDFText(ctx, body)
		if err != nil {
			return time.Time{}, nil, err
		}
		return cbrPskParsePDFText(text)
	default:
		return time.Time{}, nil, fmt.Errorf("%s: неизвестное расширение выпуска %q (ожидались xlsx, xls или pdf)",
			cbrPskTable, release.Ext)
	}
}

// cbrPskReleaseContext — выпуск в тексте ошибки и лога: имя файла, id и дата публикации. Имя файла —
// ключ версии в source_publications, по нему выпуск ищется в реестре при разборе сбоя.
func cbrPskReleaseContext(release cbrPskRelease) string {
	return fmt.Sprintf("%s: выпуск %s (id %s, опубликован %s)",
		cbrPskTable, release.FileName, release.ID, release.Published.Format(cbrPskDayLayout))
}

// cbrPskAppendBody — BodyFunc одного выпуска: разбор скачанных шаблоном байтов и запись точек
// строками cbr_psk. Период общий для выпуска и уходит в каждую строку (порядок колонок — порядок
// DDL: date, category_code, category, psk_avg, psk_max, source_file). Контекст выпуска добавляет
// цикл Import — здесь он назван один раз, иначе сбой разбора назывался бы дважды. Ошибка Append
// называет точку: без неё строка терялась бы молча (стандарт §5.5).
func cbrPskAppendBody(ctx context.Context, release cbrPskRelease) func(body []byte, batch driver.Batch) error {
	return func(body []byte, batch driver.Batch) error {
		period, points, err := cbrPskParseRelease(ctx, body, release)
		if err != nil {
			return err
		}
		for _, point := range points {
			if err = batch.Append(period, point.Code, point.Category, point.Avg, cbrPskNullable(point.Max), release.FileName); err != nil {
				return fmt.Errorf("%s: точка (%s, %s): %w",
					cbrPskTable, point.Code, period.Format(cbrPskDayLayout), err)
			}
		}
		return nil
	}
}

// cbrPskSkipUnreadable — пропуск выпуска с нечитаемым текстовым слоем PDF: версия сохраняется в
// source_publications с 0 точек, чтобы гейт по имени файла не скачивал его каждый прогон (спека
// §3.4). Fingerprint пустой: сеть за отпечатком не ходим, а гейт ищет по (source, name) —
// cbrPskKnownReleases читает DISTINCT name. Плата названа: исправленный файл под тем же именем
// гейтом не отслеживается (лечение — удалить строку версии и повторить прогон).
func cbrPskSkipUnreadable(ctx context.Context, conn driver.Conn, release cbrPskRelease) error {
	if err := util.SavePublication(ctx, conn, util.Publication{
		Source:    cbrPskTable,
		Name:      release.FileName,
		Points:    0,
		Url:       release.URL,
		FirstSeen: time.Now(),
	}); err != nil {
		return fmt.Errorf("%s: версия нечитаемого выпуска не сохранена: %w", cbrPskReleaseContext(release), err)
	}
	log.Warnf("%s: не машиночитаем (повреждённый текстовый слой PDF) — пропуск, версия сохранена с 0 точек",
		cbrPskReleaseContext(release))
	return nil
}

// cbrPskReleaseWritten — проверка, что строки выпуска в таблице есть: число строк по source_file
// после успешного вызова шаблона. Одного успеха шаблона мало: его префлайт умеет вернуть no-op,
// если версия известна и число точек совпало, — тогда новый по гейту выпуск остался бы молча
// неимпортированным, а следующий прогон снова счёл бы его новым. Ноль строк — ошибка с именем
// выпуска и указанием, что делать; сбой запроса — тоже ошибка.
func cbrPskReleaseWritten(ctx context.Context, conn driver.Conn, release cbrPskRelease) error {
	var written uint64
	if err := conn.QueryRow(ctx, cbrPskReleaseCountQuery, release.FileName).Scan(&written); err != nil {
		return fmt.Errorf("%s: число строк выпуска не прочитано: %w", cbrPskReleaseContext(release), err)
	}
	if written == 0 {
		return fmt.Errorf("%s: шаблон вернул no-op (версия известна), но строк выпуска в таблице нет — удалите строку выпуска из source_publications и повторите прогон",
			cbrPskReleaseContext(release))
	}
	return nil
}

// cbrPskSeriesMeta — описание одного ряда каталога по категории: имя «ПСК <код>: <наименование>»,
// unit «% годовых», частота Q. Наименование — из argMax по date (грубая нормализация каталога):
// историческое имя категории с тем же кодом может отличаться, и это названо в description —
// код категории менял смысл между редакциями таксономии, сверять нужно наименование.
func cbrPskSeriesMeta(code, category string) util.SeriesMeta {
	series := fmt.Sprintf("ПСК %s: %s", code, category)
	return util.SeriesMeta{
		Source:    cbrSource,
		Series:    series,
		Title:     "Среднерыночное значение ПСК: " + category,
		Unit:      "% годовых",
		Frequency: "Q",
		Origin:    fmt.Sprintf("cbr.ru/statistics/bank_sector/psk, тип ko, категория %s", code),
		Description: fmt.Sprintf("Среднерыночное значение полной стоимости потребительских кредитов (займов) (ПСК) по категории %s «%s», %% годовых. "+
			"Это не фактическая ставка по выданным кредитам, а установленный законом предел: по ч. 11 ст. 6 Федерального закона № 353-ФЗ ПСК по договору не может превышать среднерыночное значение более чем на одну треть. "+
			"Значение применяется в следующем календарном квартале после периода расчёта; дата точки — конец квартала расчёта (сопоставлять по toStartOfQuarter(date)). "+
			"Предельное значение (psk_max в v_cbr_psk) — NULL в выпусках, где оно не публиковалось (PDF-эра 2014), а не ноль. "+
			"Код категории менял смысл между редакциями таксономии (1–4 в PDF-эре 2014–2018, 1–5 с 2019, 1–8 с 2023: в XLS 2023 — укрупнённые 6–8, в XLSX с 2023-11 — 6.1.1/6.1.2, 7.1.x, 8.x): сверяйте наименование категории, а не только код. "+
			"Ипотека — коды 6.1.1/6.1.2 с 2023-11; в XLS-выпусках 2023 года она представлена укрупнёнными 6–8.", code, category),
	}
}

// cbrPskCatalogMeta читает из таблицы коды категорий с наименованием последнего выпуска и собирает
// каталог рядов. Каталог динамический: набор категорий зависит от винтажа (у трёх эр таксономии
// разные), поэтому коды объявляет не код, а данные (спека §3.5 C). Ошибка чтения — ошибка прогона:
// пустой каталог скрыл бы от агента уже записанные ряды, а писать его «на всякий случай» нулём
// строк нельзя.
func cbrPskCatalogMeta(ctx context.Context, conn driver.Conn) ([]util.SeriesMeta, error) {
	rows, err := conn.Query(ctx, cbrPskCatalogQuery)
	if err != nil {
		return nil, fmt.Errorf("%s: каталог категорий не прочитан: %w", cbrPskTable, err)
	}
	defer func() { _ = rows.Close() }()

	meta := make([]util.SeriesMeta, 0)
	for rows.Next() {
		var code, category string
		if err = rows.Scan(&code, &category); err != nil {
			return nil, fmt.Errorf("%s: строка каталога категорий не прочитана: %w", cbrPskTable, err)
		}
		if code == "" || category == "" {
			return nil, fmt.Errorf("%s: категория с пустым кодом или наименованием (%q, %q) — таблица повреждена разбором",
				cbrPskTable, code, category)
		}
		meta = append(meta, cbrPskSeriesMeta(code, category))
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: каталог категорий не дочитан: %w", cbrPskTable, err)
	}
	return meta, nil
}

// cbrPskStat — импортёр cbr_psk: встроенный util.ClickHouseImport ведёт таблицу и DDL; Import
// переопределён — у импортёра не один файл, а лента выпусков (спека §3.5 C), и точка контроля
// публикации не объявлена: префлайт шаблона не подменяет ленту. Name() не переопределён: имя
// импортёра совпадает с именем таблицы, как у большинства пакета.
type cbrPskStat struct {
	util.ClickHouseImport
}

// Import — полный цикл одного прогона (спека §3.4/§3.5 C): DDL → лента страницы → гейт по
// source_publications → цикл по новым выпускам в хронологическом порядке (старые первыми) →
// производные (витрина и динамический каталог). Порядок шагов — контракт: DDL выполняется до сети
// и до гейта, потому что гейт обращается к source_publications, а шаблонный DDL внутри цикла не
// отработал бы при нуле новых выпусков. Лента читается каждый прогон — это цена обнаружения
// нового выпуска; известные выпуски не скачиваются вовсе. Новый выпуск качается шаблоном
// (BodyFunc): тот же срез байтов уходит в разбор и в винтажный архив версии, а объявление точки
// контроля у локальной копии включает общую механику версии (source_publications). Выпуск с
// нечитаемым текстовым слоем PDF не валит прогон: он пропускается с предупреждением, а версия
// сохраняется с 0 точек, чтобы гейт не повторял скачивание (cbrPskSkipUnreadable). Сбой остальных
// выпусков не отменяет соседние: ошибки копятся errors.Join с именем выпуска, прогон завершается
// ошибкой — dagu видит сбой и не пускает цепочку дальше. Витрина и каталог публикуются и на
// no-op-пути (самолечение): каталог динамический, и его сбой прошлого прогона обязан исправиться
// без нового выпуска.
func (s *cbrPskStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	// 1. DDL — до сети: шаблонный DDL внутри цикла отработал бы только при непустом списке
	// новых выпусков, а гейт уже обращается к source_publications.
	for _, ddl := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(ddl, s.TableName)); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", cbrPskTable, err)
		}
	}

	// 2. Лента выпусков: ссылки ko на странице (актуальные и версионные списки) в хронологическом
	// порядке. Разбор — fail-fast: пропавшая лента или неразбираемая дата — ошибка, а не «новых
	// выпусков нет» при живом ЦБ.
	page, err := util.GetPage(cbrPskPageURL)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", cbrPskTable, cbrPskPageURL, err)
	}
	releases, err := cbrPskParseReleases(page)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", cbrPskTable, err)
	}

	// 3. Гейт: известность выпуска — по имени файла в source_publications, ключ версии — имя файла
	// <DDMMYYYY>_ko.<ext> (оно однозначно называет выпуск ленты). Таблицу реестра заводит общий
	// механизм; EnsurePublications вызывается до чтения, потому что на чистой базе гейт обратился
	// бы к несуществующей таблице раньше шаблона.
	if err = util.EnsurePublications(ctx, conn); err != nil {
		return 0, err
	}
	known, err := cbrPskKnownReleases(ctx, conn)
	if err != nil {
		return 0, err
	}
	fresh := cbrPskNewReleases(releases, known)
	if len(fresh) == 0 {
		if err = afterCbrPsk(ctx, conn); err != nil {
			return 0, err
		}
		log.Infof("%s: новых выпусков нет (в ленте %d, в таблице %d)", cbrPskTable, len(releases), len(known))
		return 0, nil
	}

	// 4. Цикл по новым выпускам. Ошибки копятся — сбой одного выпуска не отменяет остальные:
	// выпуски независимы (у каждого свой файл и строка версии), а пропущенный подхватился бы
	// следующим прогоном, если бы прогон молча прекращался на первом сбое.
	var errs []error
	for _, release := range fresh {
		// Локальная копия встроенного шаблона: DataUrl и разбор зависят от выпуска, а мутировать
		// общую структуру импортёра нельзя (прецедент — mtForecastStat). PublicationCountPoints у
		// копии объявлен: это включает общую механику версии (source_publications и архив при
		// настроенном хранилище), но судьбу прогона решает гейт по ленте, а не префлайт копии.
		local := s.ClickHouseImport
		local.DataUrl = release.URL
		local.BodyFunc = cbrPskAppendBody(ctx, release)
		local.PublicationCountPoints = cbrPskCountPoints
		// DDL и миграция psk_max уже выполнены шагом 1; копии они не нужны и не должны гоняться
		// на каждый выпуск (48 ALTER/CREATE на первом прогоне).
		local.CreateTable = nil
		points, importErr := local.Import(ctx, conn)
		if importErr != nil {
			// Нечитаемый текстовый слой PDF — штатный пропуск: выпуск не разбирается ни одной
			// опцией pdftotext, но версия с 0 точек сохраняется, чтобы гейт не повторял скачивание.
			if errors.Is(importErr, errCbrPskUnreadablePDF) {
				if skipErr := cbrPskSkipUnreadable(ctx, conn, release); skipErr != nil {
					errs = append(errs, skipErr)
				}
				continue
			}
			errs = append(errs, fmt.Errorf("%s: %w", cbrPskReleaseContext(release), importErr))
			continue
		}
		// 5. Успешный вызов шаблона — ещё не «выпуск записан»: его префлайт умеет вернуть no-op, и
		// тогда новый по гейту выпуск остался бы молча неимпортированным — следующий прогон снова
		// счёл бы его новым. Ноль точек по новому выпуску — отдельная ошибка (спека §3.4).
		if points == 0 {
			errs = append(errs, fmt.Errorf("%s: ноль точек — новый выпуск не записан", cbrPskReleaseContext(release)))
			continue
		}
		if writeErr := cbrPskReleaseWritten(ctx, conn, release); writeErr != nil {
			errs = append(errs, writeErr)
			continue
		}
		count += points
		log.Infof("%s: импортировано %d точек", cbrPskReleaseContext(release), points)
	}

	// 6. Производные после цикла: витрина и каталог видят записанные выпуски, а на no-op-пути
	// (выше) самолечатся. Сбой производных данные не откатывает, но прогон завершается ошибкой.
	if derr := afterCbrPsk(ctx, conn); derr != nil {
		errs = append(errs, derr)
	}
	return count, errors.Join(errs...)
}

// cbrPskImporter — квартальный корпус среднерыночных значений ПСК кредитных организаций:
// Name() = TableName = cbr_psk. Точка контроля публикации НЕ объявлена (спека §3.5 C): артефакт —
// лента из 48 выпусков, и чек известной версии по счёту всей таблицы не сошёлся бы ни разу; no-op
// держит гейт по source_publications (cbrPskKnownReleasesQuery). Производные (витрина v_cbr_psk и
// динамический каталог) публикует собственный Import() на каждом прогоне, включая no-op, поэтому
// поле AfterImport шаблона не выставляется. Регистрация — реестром cbr/importers.go; сетевых
// вызовов в init() нет.
var cbrPskImporter = &cbrPskStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   cbrPskTable,
	CreateTable: []string{cbrPskDdl, cbrPskAlterMaxNullable},
}}
