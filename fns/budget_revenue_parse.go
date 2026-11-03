package fns

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
	"golang.org/x/text/encoding/charmap"
)

const (
	// fnsSheetAnchor — первая колонка строки-якоря листа поступлений по территориям:
	// в ней буква «А», а в этой же строке — коды колонок (`<код>` или `<код>.<n>`).
	// Поиск по признаку, а не по номеру строки, нужен потому, что в архиве 2020 года
	// якорь лежит на 10-й строке, а в 2026 — на 8-й (стандарт §6.3).
	fnsSheetAnchor = "А"
	// fnsHeaderScan — за сколько верхних строк искать якорь: шапка всегда в начале
	// листа, ниже идут только строки территорий.
	fnsHeaderScan = 20
	// fnsTerritoryCount — 1 РФ + 8 федеральных округов + 85 субъектов. Столько строк
	// обязан отдать каждый разбираемый лист: меньше — книга не та (стандарт §6.4).
	fnsTerritoryCount = 94
	// fnsSubtotalLabel — служебная строка между РФ и округами, территорией не является.
	fnsSubtotalLabel = "в том числе:"
	// fnsRfCanonical — каноническое имя агрегата: источник пишет его капсом, а в базе
	// к регионам обращаются по названию в обычном регистре (ср. v_fedstat_ipp).
	fnsRfCanonical = "Российская Федерация"
)

// Классы территорий: федеральный округ — группировка, а не слагаемое РФ, поэтому
// субъекты и округа различимы в одном запросе.
const (
	fnsKindRF              = "rf"
	fnsKindFederalDistrict = "federal_district"
	fnsKindSubject         = "subject"
)

// fnsLevel — уровень бюджета колонки листа. Суффикс ячейки — собственная нумерация
// колонок источника: 1 = всего (федеральный + консолидированный бюджет субъекта),
// 2 = федеральный бюджет, 3 = консолидированный бюджет субъекта РФ, 4 = из графы 3
// в доходы местных бюджетов (часть графы 3, к итогу не прибавляется).
type fnsLevel struct {
	suffix  string // «1»…«4»
	name    string // часть имени ряда: total|federal|regional|local
	keyword string // ключевое слово подписи колонки — контроль суффикса
}

var fnsLevels = []fnsLevel{
	{suffix: "1", name: "total", keyword: "всего"},
	{suffix: "2", name: "federal", keyword: "федеральн"},
	{suffix: "3", name: "regional", keyword: "консолидирован"},
	{suffix: "4", name: "local", keyword: "местн"},
}

// fnsSeriesRow — объявленный ряд: код строки источника, суффикс колонки и стабильное
// имя. Имена объявляет код, а не заголовок листа (стандарт §6.8): заголовки содержат
// годы, сноски и редакции, и их смена создала бы новый ряд вместо продолжения
// старого. Набор курируемый (спека §3.4): прочие колонки листа разбор игнорирует.
type fnsSeriesRow struct {
	code   string // код строки листа: «1040»
	suffix string // суффикс колонки: «1»…«4»
	stub   string // основа имени ряда: «profit_tax»
	title  string // название вида налога по строке источника
}

var fnsSeriesRows = []fnsSeriesRow{
	{"1000", "1", "admin_revenues", "Доходы, администрируемые налоговыми органами"},
	{"1000", "2", "admin_revenues", "Доходы, администрируемые налоговыми органами"},
	{"1000", "3", "admin_revenues", "Доходы, администрируемые налоговыми органами"},
	{"1000", "4", "admin_revenues", "Доходы, администрируемые налоговыми органами"},
	{"1010", "1", "tax_nontax_revenues", "Налоговые и неналоговые доходы"},
	{"1010", "2", "tax_nontax_revenues", "Налоговые и неналоговые доходы"},
	{"1010", "3", "tax_nontax_revenues", "Налоговые и неналоговые доходы"},
	{"1010", "4", "tax_nontax_revenues", "Налоговые и неналоговые доходы"},
	{"1020", "1", "tax_revenues", "Налоговые доходы"},
	{"1020", "2", "tax_revenues", "Налоговые доходы"},
	{"1020", "3", "tax_revenues", "Налоговые доходы"},
	{"1020", "4", "tax_revenues", "Налоговые доходы"},
	{"1030", "1", "profit_income_taxes", "Налоги на прибыль, доходы"},
	{"1030", "2", "profit_income_taxes", "Налоги на прибыль, доходы"},
	{"1030", "3", "profit_income_taxes", "Налоги на прибыль, доходы"},
	{"1030", "4", "profit_income_taxes", "Налоги на прибыль, доходы"},
	{"1040", "1", "profit_tax", "Налог на прибыль организаций"},
	{"1040", "2", "profit_tax", "Налог на прибыль организаций"},
	{"1040", "3", "profit_tax", "Налог на прибыль организаций"},
	{"1040", "4", "profit_tax", "Налог на прибыль организаций"},
	{"1130", "3", "ndfl", "Налог на доходы физических лиц"},
	{"1130", "4", "ndfl", "Налог на доходы физических лиц"},
	{"1200", "1", "goods_taxes", "Налоги на товары (работы, услуги), реализуемые на территории Российской Федерации"},
	{"1200", "2", "goods_taxes", "Налоги на товары (работы, услуги), реализуемые на территории Российской Федерации"},
	{"1200", "3", "goods_taxes", "Налоги на товары (работы, услуги), реализуемые на территории Российской Федерации"},
	{"1200", "4", "goods_taxes", "Налоги на товары (работы, услуги), реализуемые на территории Российской Федерации"},
	{"1210", "2", "vat_domestic", "Налог на добавленную стоимость на товары (работы, услуги), реализуемые на территории Российской Федерации"},
	{"1220", "1", "excise_domestic", "Акцизы по подакцизным товарам (продукции), производимым на территории Российской Федерации"},
	{"1220", "2", "excise_domestic", "Акцизы по подакцизным товарам (продукции), производимым на территории Российской Федерации"},
	{"1220", "3", "excise_domestic", "Акцизы по подакцизным товарам (продукции), производимым на территории Российской Федерации"},
	{"1220", "4", "excise_domestic", "Акцизы по подакцизным товарам (продукции), производимым на территории Российской Федерации"},
	{"1720", "1", "resource_taxes", "Налоги, сборы и регулярные платежи за пользование природными ресурсами"},
	{"1720", "2", "resource_taxes", "Налоги, сборы и регулярные платежи за пользование природными ресурсами"},
	{"1720", "3", "resource_taxes", "Налоги, сборы и регулярные платежи за пользование природными ресурсами"},
	{"1720", "4", "resource_taxes", "Налоги, сборы и регулярные платежи за пользование природными ресурсами"},
	{"1730", "1", "ndpi", "Налог на добычу полезных ископаемых"},
	{"1730", "2", "ndpi", "Налог на добычу полезных ископаемых"},
	{"1730", "3", "ndpi", "Налог на добычу полезных ископаемых"},
	{"1730", "4", "ndpi", "Налог на добычу полезных ископаемых"},
	{"1970", "1", "abolished_taxes", "Задолженность и перерасчёты по отменённым налогам, сборам и иным обязательным платежам"},
	{"1970", "2", "abolished_taxes", "Задолженность и перерасчёты по отменённым налогам, сборам и иным обязательным платежам"},
	{"1970", "3", "abolished_taxes", "Задолженность и перерасчёты по отменённым налогам, сборам и иным обязательным платежам"},
	{"1970", "4", "abolished_taxes", "Задолженность и перерасчёты по отменённым налогам, сборам и иным обязательным платежам"},
	{"2370", "1", "nontax_revenues", "Неналоговые доходы, администрируемые налоговыми органами"},
	{"2370", "2", "nontax_revenues", "Неналоговые доходы, администрируемые налоговыми органами"},
	{"2370", "3", "nontax_revenues", "Неналоговые доходы, администрируемые налоговыми органами"},
	{"2370", "4", "nontax_revenues", "Неналоговые доходы, администрируемые налоговыми органами"},
}

// fnsColumnKey — ключ колонки листа: «1040.3». Итоговая колонка источника пишется без
// суффикса («1040»), поэтому она получает суффикс «1» в fnsParseHeaderColumn.
func fnsColumnKey(code, suffix string) string { return code + "." + suffix }

// fnsLevel — уровень колонки по её суффиксу.
func (r fnsSeriesRow) fnsLevel() fnsLevel {
	for _, level := range fnsLevels {
		if level.suffix == r.suffix {
			return level
		}
	}
	// Суффикс вне 1..4 в индекс не попадает: регулярка шапки его не принимает.
	return fnsLevels[0]
}

// series — имя ряда в таблице, каталоге и витрине: вид налога + уровень бюджета.
func (r fnsSeriesRow) series() string { return r.stub + "_" + r.fnsLevel().name }

// fnsSeriesByColumn — индекс объявленных колонок; вычисляется при инициализации
// пакета и сети не трогает (правило 3 AGENTS.md запрещает сетевые вызовы, а не
// таблицу).
var fnsSeriesByColumn = func() map[string]fnsSeriesRow {
	index := make(map[string]fnsSeriesRow, len(fnsSeriesRows))
	for _, row := range fnsSeriesRows {
		index[fnsColumnKey(row.code, row.suffix)] = row
	}
	return index
}()

// fnsTerritoryNames — канонические названия территорий листа поступлений в написании
// текущего источника, в порядке строк листа: РФ, затем федеральные округа вместе со
// своими субъектами. Список декларирован, потому что архивы разных лет пишут одни и
// те же территории по-разному (спека §2.4), а ряд обязан оставаться тем же рядом.
var fnsTerritoryNames = []string{
	fnsRfCanonical,
	"Центральный федеральный округ",
	"Белгородская область",
	"Брянская область",
	"Владимирская область",
	"Воронежская область",
	"Ивановская область",
	"Калужская область",
	"Костромская область",
	"Курская область",
	"Липецкая область",
	"Московская область",
	"Орловская область",
	"Рязанская область",
	"Смоленская область",
	"Тамбовская область",
	"Тверская область",
	"Тульская область",
	"Ярославская область",
	"г. Москва",
	"Северо-Западный федеральный округ",
	"Республика Карелия",
	"Республика Коми",
	"Архангельская область",
	"Вологодская область",
	"Калининградская область",
	"Ленинградская область",
	"Мурманская область",
	"Новгородская область",
	"Псковская область",
	"г. Санкт-Петербург",
	"Ненецкий автономный округ",
	"Северо-Кавказский федеральный округ",
	"Республика Дагестан",
	"Республика Ингушетия",
	"Кабардино-Балкарская Республика",
	"Карачаево-Черкесская Республика",
	"Республика Северная Осетия - Алания",
	"Чеченская Республика",
	"Ставропольский край",
	"Южный федеральный округ",
	"Республика Адыгея",
	"Республика Калмыкия",
	"Республика Крым",
	"Краснодарский край",
	"Астраханская область",
	"Волгоградская область",
	"Ростовская область",
	"г. Севастополь",
	"Приволжский федеральный округ",
	"Республика Башкортостан",
	"Республика Марий-Эл",
	"Республика Мордовия",
	"Республика Татарстан",
	"Удмуртская Республика",
	"Чувашская Республика",
	"Кировская область",
	"Нижегородская область",
	"Оренбургская область",
	"Пензенская область",
	"Пермский край",
	"Самарская область",
	"Саратовская область",
	"Ульяновская область",
	"Уральский федеральный округ",
	"Курганская область",
	"Свердловская область",
	"Тюменская область",
	"Челябинская область",
	"Ханты-Мансийский автономный округ - Югра",
	"Ямало-Ненецкий автономный округ",
	"Сибирский федеральный округ",
	"Республика Алтай",
	"Республика Тыва",
	"Республика Хакасия",
	"Алтайский край",
	"Красноярский край",
	"Иркутская область",
	"Кемеровская область - Кузбасс",
	"Новосибирская область",
	"Омская область",
	"Томская область",
	"Дальневосточный федеральный округ",
	"Республика Бурятия",
	"Республика Саха (Якутия)",
	"Приморский край",
	"Хабаровский край",
	"Амурская область",
	"Камчатский край",
	"Магаданская область",
	"Сахалинская область",
	"Забайкальский край",
	"Еврейская автономная область",
	"Чукотский автономный округ",
}

// fnsTerritoryAliases — написания тех же территорий в архивах других лет. Ключ —
// нормализованное написание (fnsNormalizeTerritory). «ямалоhенецкийао» — с латинской
// h: именно так напечатано «Ямало-Hенецкий АО» в архиве 2020 года, и это не ошибка
// нормализации, а особенность источника. Парные написания 2020 года наблюдены
// живым разбором архивов 2020 и 2026.
var fnsTerritoryAliases = map[string]string{
	"городмосква":           "г. Москва",
	"москва":                "г. Москва",
	"городсанктпетербург":   "г. Санкт-Петербург",
	"санктпетербург":        "г. Санкт-Петербург",
	"городсевастополь":      "г. Севастополь",
	"севастополь":           "г. Севастополь",
	"ненецкийао":            "Ненецкий автономный округ",
	"чукотскийао":           "Чукотский автономный округ",
	"хантымансийскийаоюгра": "Ханты-Мансийский автономный округ - Югра",
	"ямалоhенецкийао":       "Ямало-Ненецкий автономный округ",
	"ямалоненецкийао":       "Ямало-Ненецкий автономный округ",
}

// fnsNormalizeTerritory сводит название к сопоставимому виду: нижний регистр,
// «ё» → «е», без пробелов, дефисов, точек, скобок и прочей пунктуации. Латиница
// сохраняется намеренно: homoglyph «h» в «Ямало-Hенецкий» — часть написания, и её
// ловит таблица алиасов, а не тихая подмена.
func fnsNormalizeTerritory(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.ReplaceAll(name, "ё", "е")) {
		if (r >= 'а' && r <= 'я') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// fnsTerritoryByNorm — нормализованное каноническое имя → каноническое имя.
var fnsTerritoryByNorm = func() map[string]string {
	norms := make(map[string]string, len(fnsTerritoryNames))
	for _, name := range fnsTerritoryNames {
		norms[fnsNormalizeTerritory(name)] = name
	}
	return norms
}()

// fnsTerritoryByKind — каноническое имя → класс территории.
var fnsTerritoryByKind = func() map[string]string {
	kinds := make(map[string]string, len(fnsTerritoryNames))
	for _, name := range fnsTerritoryNames {
		switch {
		case name == fnsRfCanonical:
			kinds[name] = fnsKindRF
		case strings.HasSuffix(name, "федеральный округ"):
			kinds[name] = fnsKindFederalDistrict
		default:
			kinds[name] = fnsKindSubject
		}
	}
	return kinds
}()

// fnsResolveTerritory превращает название из листа в каноническое имя и класс.
// Неизвестное название — ошибка прогона, а не warn: потерять целый субъект в
// фискальной таблице хуже, чем упавший шаг изолированного DAG, а правка — одна
// строка в списке или в таблице алиасов (спека §3.5.5).
func fnsResolveTerritory(raw string) (name string, kind string, err error) {
	norm := fnsNormalizeTerritory(raw)
	if norm == "" {
		return "", "", fmt.Errorf("пустое название территории %q", raw)
	}
	candidate, ok := fnsTerritoryByNorm[norm]
	if !ok {
		candidate, ok = fnsTerritoryAliases[norm]
	}
	if !ok {
		return "", "", fmt.Errorf("неизвестная территория %q: добавь её в fnsTerritoryNames или fnsTerritoryAliases", raw)
	}
	kind, ok = fnsTerritoryByKind[candidate]
	if !ok {
		return "", "", fmt.Errorf("территория %q не отнесена к классу rf|federal_district|subject", candidate)
	}
	return candidate, kind, nil
}

// fnsPoint — строка таблицы: территория, ряд, отчётный месяц и накопленное значение.
type fnsPoint struct {
	date   time.Time
	region string
	kind   string
	series string
	value  float64
}

// fnsNumberReplacer убирает разделители разрядов: в raw-режиме excelize отдаёт
// обычные, неразрывные и узкие пробелы внутри числа.
var fnsNumberReplacer = strings.NewReplacer(" ", "", "\u00a0", "", "\u202f", "", "\t", "")

// fnsParseNumber разбирает сырое значение ячейки. Пустая ячейка, «-» и «X» — не
// точка (стандарт §6.7): у источника это отсутствие данных, а не ноль.
func fnsParseNumber(raw string) (value float64, has bool, err error) {
	trimmed := strings.TrimSpace(raw)
	switch trimmed {
	case "", "-", "—", "x", "X":
		return 0, false, nil
	}
	parsed, err := strconv.ParseFloat(strings.ReplaceAll(fnsNumberReplacer.Replace(trimmed), ",", "."), 64)
	if err != nil {
		return 0, false, fmt.Errorf("значение %q не число", raw)
	}
	return parsed, true, nil
}

// fnsColumnCode — ячейка шапки: код строки и необязательный суффикс уровня.
var fnsColumnCode = regexp.MustCompile(`^(\d{3,4})(?:\.(\d))?$`)

// fnsOpenTerritoryWorkbook открывает из архива внутренний файл поступлений по
// территориям и возвращает его имя. Файл ищется по признаку: с 2023 года источник
// вставил в имя «ЕНС», а часть архивов приходит с числовым префиксом карточки сайта.
func fnsOpenTerritoryWorkbook(body []byte) (*excelize.File, string, error) {
	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, "", fmt.Errorf("разбор архива: %w", err)
	}
	var found *zip.File
	var foundName string
	for _, f := range zr.File {
		name := fnsZipName(f)
		if !fnsIsTerritoryFile(name) {
			continue
		}
		if found != nil {
			return nil, "", fmt.Errorf("в архиве два подходящих файла территорий: %q и %q", foundName, name)
		}
		found, foundName = f, name
	}
	if found == nil {
		return nil, "", fmt.Errorf("в архиве нет файла вида «11О … поступление … по территориям.xlsx» — состав архива изменился")
	}
	rc, err := found.Open()
	if err != nil {
		return nil, foundName, fmt.Errorf("чтение %q из архива: %w", foundName, err)
	}
	data, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	if readErr != nil {
		return nil, foundName, fmt.Errorf("чтение %q из архива: %w", foundName, readErr)
	}
	if closeErr != nil {
		return nil, foundName, fmt.Errorf("закрытие %q из архива: %w", foundName, closeErr)
	}
	xf, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		return nil, foundName, fmt.Errorf("excelize %q: %w (страница сайта вместо файла?)", foundName, err)
	}
	return xf, foundName, nil
}

// fnsZipName отдаёт имя элемента архива в юникоде: FNS пишет имена в cp866, а Go
// передаёт байты как есть.
func fnsZipName(f *zip.File) string {
	if !f.NonUTF8 {
		return f.Name
	}
	decoded, err := charmap.CodePage866.NewDecoder().String(f.Name)
	if err != nil {
		return f.Name
	}
	return decoded
}

// fnsIsTerritoryFile — признак нужного внутреннего файла архива: лист 11О о
// поступлении налогов по территориям. «12О Начислено…» и справочные листы под
// признаки не попадают.
func fnsIsTerritoryFile(name string) bool {
	lowered := strings.ToLower(name)
	return strings.Contains(lowered, "11") &&
		strings.Contains(lowered, "поступлен") &&
		strings.Contains(lowered, "территори") &&
		strings.HasSuffix(lowered, ".xlsx")
}

// fnsParseBudgetRevenue — BodyFunc шаблона: архив → лист территорий → точки.
// Разбор от сети отделён (стандарт §6.1); month приходит из имени файла, потому что
// строка «по состоянию на …» есть не во всех листах и не в одном формате.
func fnsParseBudgetRevenue(body []byte, batch driver.Batch, month time.Time) error {
	xf, fileName, err := fnsOpenTerritoryWorkbook(body)
	if err != nil {
		return fmt.Errorf("%s: %w", fnsImporterName, err)
	}
	defer func() { _ = xf.Close() }()

	points, err := fnsCollectPoints(xf, month)
	if err != nil {
		return fmt.Errorf("%s: файл %q: %w", fnsImporterName, fileName, err)
	}
	if len(points) == 0 {
		return fmt.Errorf("%s: файл %q: разбор не дал ни одной точки", fnsImporterName, fileName)
	}
	for _, point := range points {
		if err = batch.Append(point.date, point.region, point.kind, point.series, point.value); err != nil {
			return fmt.Errorf("%s: append %s / %s / %s: %w",
				fnsImporterName, point.date.Format("2006-01"), point.region, point.series, err)
		}
	}
	return nil
}

// fnsCollectPoints обходит видимые листы книги и собирает объявленные колонки.
// Листы без объявленных колонок (служебные, скрытые срезы, группы мелких КБК)
// пропускаются; если во всей книге не найдена хоть одна объявленная колонка —
// это ошибка: источник убрал строку отчёта, и набор рядов надо пересматривать явно.
func fnsCollectPoints(xf *excelize.File, month time.Time) ([]fnsPoint, error) {
	var points []fnsPoint
	claimed := make(map[string]bool, len(fnsSeriesRows))
	for _, sheet := range xf.GetSheetList() {
		visible, err := xf.GetSheetVisible(sheet)
		if err != nil {
			return nil, fmt.Errorf("лист «%s»: видимость: %w", sheet, err)
		}
		if !visible {
			continue
		}
		sheetPoints, err := fnsCollectSheetPoints(xf, sheet, month, claimed)
		if err != nil {
			return nil, fmt.Errorf("лист «%s»: %w", sheet, err)
		}
		points = append(points, sheetPoints...)
	}
	var missing []string
	for _, row := range fnsSeriesRows {
		if !claimed[fnsColumnKey(row.code, row.suffix)] {
			missing = append(missing, fnsColumnKey(row.code, row.suffix))
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("в книге нет объявленных колонки(ов) %s — строка отчёта пропала из источника", strings.Join(missing, ", "))
	}
	return points, nil
}

// fnsSheetColumn — объявленная колонка листа: ряд и его позиция.
type fnsSheetColumn struct {
	row   fnsSeriesRow
	index int
}

// fnsCollectSheetPoints разбирает один лист: шапка по якорю «А», колонки по кодам,
// строки по территориям. Точка добавляется только если значение в ячейке есть.
func fnsCollectSheetPoints(xf *excelize.File, sheet string, month time.Time, claimed map[string]bool) ([]fnsPoint, error) {
	rows, err := xf.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, err
	}
	anchor := fnsFindAnchor(rows)
	if anchor < 0 {
		return nil, nil // лист без шапки кодов: служебный, не ошибка
	}

	var columns []fnsSheetColumn
	for index, cell := range rows[anchor] {
		if index == 0 {
			continue // колонка территорий
		}
		declared, ok := fnsParseHeaderCell(strings.TrimSpace(cell))
		if !ok {
			continue
		}
		key := fnsColumnKey(declared.code, declared.suffix)
		if claimed[key] {
			// Повтор той же колонки в книге — ожидаемое изменение разметки, а не
			// поломка: берется первая, дубликат не удваивает точки (стандарт §6.6).
			log.Warnf("%s: лист «%s»: колонка %s встречается повторно — берётся первая", fnsImporterName, sheet, key)
			continue
		}
		fnsCheckColumnLabel(sheet, key, rows, anchor, index, declared.fnsLevel().keyword)
		claimed[key] = true
		columns = append(columns, fnsSheetColumn{row: declared, index: index})
	}
	if len(columns) == 0 {
		return nil, nil
	}

	var points []fnsPoint
	territories := 0
	for i := anchor + 1; i < len(rows); i++ {
		row := rows[i]
		if len(row) == 0 {
			continue
		}
		raw := strings.TrimSpace(row[0])
		if raw == "" || strings.EqualFold(raw, fnsSubtotalLabel) {
			continue
		}
		region, kind, err := fnsResolveTerritory(raw)
		if err != nil {
			return nil, err
		}
		territories++
		for _, column := range columns {
			if column.index >= len(row) {
				continue
			}
			value, has, err := fnsParseNumber(row[column.index])
			if err != nil {
				return nil, fmt.Errorf("территория %q, лист «%s», колонка %s: %w", raw, sheet, column.row.series(), err)
			}
			if !has {
				continue
			}
			points = append(points, fnsPoint{
				date:   month,
				region: region,
				kind:   kind,
				series: column.row.series(),
				value:  value,
			})
		}
	}
	if territories != fnsTerritoryCount {
		return nil, fmt.Errorf("территорий %d, ожидалось %d — структура книги изменилась", territories, fnsTerritoryCount)
	}
	return points, nil
}

// fnsFindAnchor — строка-якорь шапки: её первая колонка содержит букву «А».
func fnsFindAnchor(rows [][]string) int {
	limit := fnsHeaderScan
	if limit > len(rows) {
		limit = len(rows)
	}
	for i := 0; i < limit; i++ {
		if len(rows[i]) > 0 && strings.TrimSpace(rows[i][0]) == fnsSheetAnchor {
			return i
		}
	}
	return -1
}

// fnsParseHeaderCell — объявленная колонка по ячейке шапки. Итоговая колонка
// источника пишется без суффикса («1040») и получает суффикс «1».
func fnsParseHeaderCell(cell string) (fnsSeriesRow, bool) {
	match := fnsColumnCode.FindStringSubmatch(cell)
	if match == nil {
		return fnsSeriesRow{}, false
	}
	suffix := match[2]
	if suffix == "" {
		suffix = "1"
	}
	declared, ok := fnsSeriesByColumn[fnsColumnKey(match[1], suffix)]
	return declared, ok
}

// fnsCheckColumnLabel сверяет суффикс колонки с её подписью: ближайшей непустой
// ячейкой того же столбца выше якоря. Конфликт — warn, а не ошибка:
// идентификатором столбца в файле остаётся суффикс, а подписи источник путает
// (у колонки «1610.2» в архиве 2026 года напечатано «в федеральный бюджет»).
func fnsCheckColumnLabel(sheet, key string, rows [][]string, anchor, index int, keyword string) {
	if keyword == "" {
		return
	}
	for i := anchor - 1; i >= 0; i-- {
		if index >= len(rows[i]) {
			continue
		}
		label := strings.TrimSpace(rows[i][index])
		if label == "" {
			continue
		}
		if !strings.Contains(fnsNormalizeTerritory(label), keyword) {
			log.Warnf("%s: лист «%s»: колонка %s подписана %q, а суффикс обещает %q",
				fnsImporterName, sheet, key, label, keyword)
		}
		return
	}
	log.Warnf("%s: лист «%s»: у колонки %s нет подписи уровня", fnsImporterName, sheet, key)
}
