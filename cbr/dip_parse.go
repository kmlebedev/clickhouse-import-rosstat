package cbr

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

// Разбор книги выпуска приложений к бюллетеню ЦБ «О чём говорят тренды» (таблица
// cbr_dip_bulletin): объявленный реестр рядов и чтение точек. Страницу выпусков разбирает
// cbr/dip_page.go, качает файл выпуска и пишет точки импортёр cbr/dip_bulletin.go; здесь ни
// сети, ни БД (стандарт §6.1) — на входе открытая книга и выпуск со страницы, на выходе плоский
// список точек, имена рядов которого объявлены кодом (стандарт §6.8, спека §3.2).
//
// Вёрстка книги дрейфует между выпусками (спека §2): лист SAAR-инфляции называется по-разному,
// шапка листа бывает без ячейки «Дата», SA- и NSA-подписи колонок меняются, номера листов плавают
// (2/3/8/17/20/23/64), а листов рынка труда нет в 13 выпусках из 49 — поэтому лист ищется по
// заголовку A1, шапка — по объявленным подписям, а отсутствие листа или отдельной колонки не
// валит прогон (стандарт §6.6, §12.3).

const (
	// cbrDipTable — таблица импортёра: её называют сообщения разбора и предупреждения (стандарт
	// §6.4). Имя то же, что у реестра cbr/importers.go (там — литералом, стандарт §8.2).
	cbrDipTable = "cbr_dip_bulletin"

	// cbrDipHeaderRows — глубина поиска строки шапки (спека §3.4): шапка — первая строка из
	// первых десяти, где есть хотя бы одна объявленная подпись колонки. Глубже искать нельзя:
	// в книге десятки листов, и подпись, случайно совпавшая с объявленной ниже шапки, привязала
	// бы точки к чужой разметке.
	cbrDipHeaderRows = 10

	// cbrDipDateColumn — колонка даты (0-based): дата точки всегда в A (спека §3.4), поэтому
	// объявленные подписи колонок ищутся только правее. Без этого правила заголовок A1 листа
	// «Уровень занятости, %» (выпуск 26-07) совпал бы с объявленной подписью NSA-колонки старой
	// вёрстки («Уровень занятости, %») и разбор принял бы заголовок листа за шапку.
	cbrDipDateColumn = 0

	// cbrDipTitlesInError — сколько заголовков A1 входит в сообщение «нет объявленных листов»:
	// в книге до 47 листов, и полный список превратил бы ошибку в свалку.
	cbrDipTitlesInError = 5
)

// cbrDipDateLayouts — текстовые формы даты в колонке A (спека §3.4): ISO «ГГГГ-ММ-ДД» и
// «ДД.ММ.ГГГГ». Серийное число Excel обрабатывает cbrDipDate отдельно: GetRows с RawCellValue
// отдаёт дату именно сырым серийным числом (стандарт §6.2), а не форматированной строкой.
var cbrDipDateLayouts = []string{"2006-01-02", "02.01.2006"}

// cbrDipLabelReplacer — типографика подписей источника, не влияющая на смысл: кавычки-ёлочки и
// лапки, сноски-звёздочки (подпись со сноской — та же подпись), тире трёх видов (короткое,
// длинное и минус) — дефис. Переносы строк и неразрывные пробелы снимает cbrDipNormalizeLabel:
// их схлопывает strings.Fields (U+00A0 для него пробел).
var cbrDipLabelReplacer = strings.NewReplacer(
	"«", "", "»", "",
	"„", "", "“", "", "”", "", "‟", "",
	"\"", "", "'", "",
	"*", "",
	"–", "-", "—", "-", "−", "-",
)

// cbrDipSeriesSpec — объявленный ряд: имя и варианты подписи его колонки в шапке листа. Имя
// объявляет код (стандарт §6.8): подпись источника дрейфует («Скорректированный на сезонность»,
// «Сезонно сглаженный» — один и тот же ряд в разных выпусках), и имя, собранное из подписи,
// оборвало бы ряд на первой смене вёрстки. Подписи сравниваются точным совпадением после
// нормализации: варианты перечислены кодом, а не выводятся из текста.
type cbrDipSeriesSpec struct {
	Name   string
	Labels []string
}

// cbrDipSheetSpec — объявленный лист: варианты заголовка A1 и ряды его колонок. Лист ищется по
// нормализованному заголовку, а не по номеру: номер плавает (2/3/8/17/20/23/64/…, спека §3.4),
// а заголовок называет саму таблицу источника.
type cbrDipSheetSpec struct {
	Titles []string
	Series []cbrDipSeriesSpec
}

// cbrDipSheets — объявленный набор рядов импортёра (спека §3.2): три листа — восемь рядов.
// Состав намеренно узкий: «Цель по инфляции» — константа-ориентир, а не измерение; компоненты
// SAAR с «(без …)» в выпусках 22-04 и 22-05 — другие метрики, склеивать их с основными нельзя
// (они не совпадут ни с одной объявленной подписью — §3.2); листы-графики без подписей колонок
// не разбираются.
var cbrDipSheets = []cbrDipSheetSpec{
	{
		// Лист SAAR-инфляции: современный заголовок и заголовок пяти старых выпусков
		// (20-06 … 21-02, спека §2). Четыре компоненты — колонки B…E; колонка «Цель по инфляции»
		// не объявлена и не читается.
		Titles: []string{
			"Сезонно сглаженный рост цен, % м/м SAAR",
			"Сезонно сглаженный рост цен, % SAAR",
		},
		Series: []cbrDipSeriesSpec{
			{Name: "saar_all_goods", Labels: []string{"Все товары и услуги"}},
			{Name: "saar_food", Labels: []string{"Продовольственные товары"}},
			{Name: "saar_nonfood", Labels: []string{"Непродовольственные товары"}},
			{Name: "saar_services", Labels: []string{"Услуги"}},
		},
	},
	{
		// Лист безработицы. Подписи SA- и NSA-колонок менялись от выпуска к выпуску: заголовки
		// A1 у листа один, а варианты колонок — из спеки §3.2 (21-03 писал «Скорректированный на
		// сезонность», 21-06 … 23-01 — «Сезонно сглаженный», в 20-07 и 20-08 безработица шла
		// одной колонкой «Уровень безработицы» без SA-пары).
		Titles: []string{"Уровень безработицы, %"},
		Series: []cbrDipSeriesSpec{
			{Name: "unemployment_sa", Labels: []string{
				"С корректировкой на сезонность", "Скорректированный на сезонность", "Сезонно сглаженный",
			}},
			{Name: "unemployment_nsa", Labels: []string{
				"Без корректировки на сезонность", "Нескорректированный на сезонность", "Уровень безработицы",
			}},
		},
	},
	{
		// Лист занятости — штатная замена листа безработицы в свежих выпусках (25-03 … 26-07):
		// в 26-07 листа безработицы нет вовсе, и это не поломка источника (спека §2, §3.4).
		Titles: []string{"Уровень занятости, %"},
		Series: []cbrDipSeriesSpec{
			{Name: "employment_sa", Labels: []string{
				"С корректировкой на сезонность", "Уровень занятости, % SA",
			}},
			{Name: "employment_nsa", Labels: []string{
				"Без корректировки на сезонность", "Уровень занятости, %",
			}},
		},
	},
}

// cbrDipPoint — точка разбора: имя ряда, дата (последний день отчётного месяца, спека §3.3) и
// значение. Выпуск в точку не входит: release_id, release и publication_date добавляет импортёр
// при записи строки таблицы — они свойства выпуска, а не измерения.
type cbrDipPoint struct {
	Series string
	Date   time.Time
	Value  float64
}

// cbrDipColumn — найденная колонка листа: её индекс в строке (0-based; A — 0) и имя ряда.
type cbrDipColumn struct {
	Index  int
	Series string
}

// cbrDipContext — общая часть сообщений разбора: таблица и выпуск. ID выпуска (ключ винтажа)
// в сообщении обязателен: release у двух выпусков на странице может совпасть по имени файла-
// предшественника (stat_bulletin_23-07.xlsx дважды, спека §2), и без id выпуск в логе не отличим.
func cbrDipContext(release dipRelease) string {
	return fmt.Sprintf("%s: выпуск %s (id %s)", cbrDipTable, release.Release, release.ID)
}

// cbrDipNormalizeLabel приводит подпись листа, колонки или объявленного ряда к сравнимому виду:
// снимаются кавычки и сноски-звёздочки, тире становится дефисом, переносы строк и неразрывные
// пробелы схлопываются в один пробел. Тот же вид получает объявленная подпись из кода, поэтому
// сравнение идёт по нормализованным строкам (стандарт §6.3), а ошибки называют исходный текст.
func cbrDipNormalizeLabel(raw string) string {
	return strings.Join(strings.Fields(cbrDipLabelReplacer.Replace(raw)), " ")
}

// cbrDipSameLabel — совпадают ли подписи после нормализации; регистр не значим (в заголовках
// источника аббревиатуры пишутся и капсом). Пустые подписи равными не считаются: пустая ячейка
// шапки — не подпись колонки, и совпадением она бы её объявила.
func cbrDipSameLabel(a, b string) bool {
	normalizedA, normalizedB := cbrDipNormalizeLabel(a), cbrDipNormalizeLabel(b)
	return normalizedA != "" && normalizedB != "" && strings.EqualFold(normalizedA, normalizedB)
}

// cbrDipLabelMatches — совпадает ли подпись ячейки хотя бы с одним из объявленных вариантов.
// Подпись со сноской («Услуги*») совпадает с объявленной: сноску снимает нормализация; чужая
// метрика с «(без …)» не совпадает ни с чем — в этом и смысл точного сравнения (спека §3.2).
func cbrDipLabelMatches(cell string, labels []string) bool {
	for _, label := range labels {
		if cbrDipSameLabel(cell, label) {
			return true
		}
	}
	return false
}

// cbrDipLabelsText — перечень подписей для текста ошибки или предупреждения: по нему видно,
// чего именно не дождался разбор, — подписи источника меняются от выпуска к выпуску, и у ряда
// их может быть несколько.
func cbrDipLabelsText(labels []string) string {
	quoted := make([]string, 0, len(labels))
	for _, label := range labels {
		quoted = append(quoted, "«"+label+"»")
	}
	return strings.Join(quoted, " / ")
}

// cbrDipNamesText — перечень имён рядов для текста предупреждения о пропущенном листе.
func cbrDipNamesText(series []cbrDipSeriesSpec) string {
	quoted := make([]string, 0, len(series))
	for _, spec := range series {
		quoted = append(quoted, "«"+spec.Name+"»")
	}
	return strings.Join(quoted, ", ")
}

// cbrDipTitlesText — заголовки A1 книги для сообщения о пропавших листах: пустые пропускаются,
// в текст входят первые cbrDipTitlesInError, остальные — счётчиком (в книге до 47 листов).
func cbrDipTitlesText(titles []string) string {
	quoted := make([]string, 0, cbrDipTitlesInError)
	rest := 0
	for _, title := range titles {
		if strings.TrimSpace(title) == "" {
			continue
		}
		if len(quoted) >= cbrDipTitlesInError {
			rest++
			continue
		}
		quoted = append(quoted, "«"+strings.TrimSpace(title)+"»")
	}
	if len(quoted) == 0 {
		return "(пусто)"
	}
	text := strings.Join(quoted, ", ")
	if rest > 0 {
		text += fmt.Sprintf(", …(+%d)", rest)
	}
	return text
}

// cbrDipSheetLabels — все объявленные подписи колонок листа: по ним ищется шапка (достаточно
// любой) и строится текст ошибки о ненайденной шапке.
func cbrDipSheetLabels(spec cbrDipSheetSpec) []string {
	labels := make([]string, 0, len(spec.Series))
	for _, series := range spec.Series {
		labels = append(labels, series.Labels...)
	}
	return labels
}

// cbrDipCell — ячейка строки по индексу колонки: GetRows обрезает хвостовые пустые ячейки, и
// короткая строка — норма; отсутствующая ячейка равна пустой (стандарт §6.5, §6.7). Пробелы
// снимаются: « 1 » — то же значение.
func cbrDipCell(row []string, column int) string {
	if column < 0 || column >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[column])
}

// cbrDipColumnName — буква колонки по индексу (0-based): ошибки называют колонку так, как её
// видит человек в Excel, а не «колонка 4».
func cbrDipColumnName(index int) string {
	name, err := excelize.ColumnNumberToName(index + 1)
	if err != nil {
		return strconv.Itoa(index + 1)
	}
	return name
}

// cbrDipMonthEnd — последний день месяца даты: нулевой день от первого числа следующего месяца
// (time.Date нормализует его в последний день предыдущего). В источнике стоит 1-е число
// отчётного месяца, точка ряда — конец месяца (спека §3.3).
func cbrDipMonthEnd(date time.Time) time.Time {
	return time.Date(date.Year(), date.Month()+1, 0, 0, 0, 0, 0, time.UTC)
}

// cbrDipDate — дата из ячейки колонки A: серийное число Excel или текст «ГГГГ-ММ-ДД» /
// «ДД.ММ.ГГГГ» (спека §3.4). Неразобранная ячейка возвращает ok == false — этим разбор
// завершает данные, а не падает: ниже таблицы лежат сноски источника. use1904 — система дат
// книги: серийное число без неё сдвинуло бы все даты на четыре года молча.
func cbrDipDate(cell string, use1904 bool) (time.Time, bool) {
	cell = strings.TrimSpace(cell)
	if cell == "" {
		return time.Time{}, false
	}
	for _, layout := range cbrDipDateLayouts {
		if parsed, err := time.Parse(layout, cell); err == nil {
			return cbrDipMonthEnd(parsed), true
		}
	}
	serial, err := strconv.ParseFloat(cell, 64)
	if err != nil {
		return time.Time{}, false
	}
	parsed, err := excelize.ExcelDateToTime(serial, use1904)
	if err != nil {
		return time.Time{}, false
	}
	return cbrDipMonthEnd(parsed), true
}

// cbrDipValue — значение ячейки: ParseFloat по сырой строке (стандарт §6.2, спека §3.3).
// Нечисловая ячейка и NaN/Inf — ошибка: ParseFloat принимает «NaN» молча, а точка с NaN
// отравила бы ряд (сравнения с NaN ложны, суммы — NaN). Литеральный 0 — настоящее значение
// (стандарт §6.7); пустая ячейка сюда не доходит.
func cbrDipValue(cell string) (float64, error) {
	value, err := strconv.ParseFloat(cell, 64)
	if err != nil {
		return 0, fmt.Errorf("значение %q не число: %w", cell, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("значение %q не конечно (NaN/Inf)", cell)
	}
	return value, nil
}

// cbrDipHeaderRow — индекс строки шапки (0-based): первая из первых cbrDipHeaderRows строк, где
// хотя бы одна колонка правее A несёт объявленную подпись (спека §3.4). Колонка A не участвует:
// там всегда дата, а заголовок листа A1 сам может совпасть с объявленной подписью (лист
// «Уровень занятости, %» в 26-07). -1 — шапки нет.
func cbrDipHeaderRow(rows [][]string, labels []string) int {
	limit := len(rows)
	if limit > cbrDipHeaderRows {
		limit = cbrDipHeaderRows
	}
	for i := 0; i < limit; i++ {
		for j := cbrDipDateColumn + 1; j < len(rows[i]); j++ {
			if cbrDipLabelMatches(rows[i][j], labels) {
				return i
			}
		}
	}
	return -1
}

// cbrDipColumns — колонки объявленных рядов в найденной шапке, в порядке листа. Подпись может
// быть любым вариантом ряда; подпись, встретившаяся в двух колонках, — ошибка: какую из колонок
// считать рядом, разбор не знает (спека §3.2), и молча выбранная первая записала бы данные не
// той метрики. Колонка, не совпавшая ни с одной объявленной подписью («Дата», «Цель по
// инфляции», чужие метрики с «(без …)»), не читается.
func cbrDipColumns(header []string, spec cbrDipSheetSpec) ([]cbrDipColumn, error) {
	columns := make([]cbrDipColumn, 0, len(spec.Series))
	taken := make(map[string]bool, len(spec.Series))
	for j := cbrDipDateColumn + 1; j < len(header); j++ {
		for _, series := range spec.Series {
			if !cbrDipLabelMatches(header[j], series.Labels) {
				continue
			}
			if taken[series.Name] {
				return nil, fmt.Errorf("колонка %s: подпись %q повторяется — одна подпись у двух колонок, ряд %q не различим",
					cbrDipColumnName(j), strings.TrimSpace(header[j]), series.Name)
			}
			taken[series.Name] = true
			columns = append(columns, cbrDipColumn{Index: j, Series: series.Name})
			break
		}
	}
	return columns, nil
}

// cbrDipParseSheet разбирает один найденный лист: шапку, колонки объявленных рядов и точки.
// seen — общий по книге набор ключей (series, date): дубль точки внутри файла — ошибка разбора
// (спека §3.1), а не молча выбранная последняя запись.
func cbrDipParseSheet(xlsx *excelize.File, sheet string, spec cbrDipSheetSpec, release dipRelease, use1904 bool, seen map[string]bool) ([]cbrDipPoint, error) {
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, fmt.Errorf("%s, лист «%s»: %w", cbrDipContext(release), sheet, err)
	}
	labels := cbrDipSheetLabels(spec)
	header := cbrDipHeaderRow(rows, labels)
	if header < 0 {
		return nil, fmt.Errorf("%s, лист «%s»: в первых %d строках нет ни одной объявленной подписи колонки (варианты: %s) — лист не то, чем кажется",
			cbrDipContext(release), sheet, cbrDipHeaderRows, cbrDipLabelsText(labels))
	}
	columns, err := cbrDipColumns(rows[header], spec)
	if err != nil {
		return nil, fmt.Errorf("%s, лист «%s»: %w", cbrDipContext(release), sheet, err)
	}
	matched := make(map[string]bool, len(columns))
	for _, column := range columns {
		matched[column.Series] = true
	}
	for _, series := range spec.Series {
		if !matched[series.Name] {
			// Ожидаемое изменение источника (другая редакция метрики, переименованная колонка):
			// предупреждение и пропуск ряда, а не падение — иначе выпуск 22-04 с чужими
			// компонентами SAAR остановил бы цепочку dagu для всех последующих выпусков (§3.4).
			log.Warnf("%s, лист «%s»: колонка ряда %q не найдена (варианты подписи: %s): ряд пропущен, остальные колонки листа импортируются",
				cbrDipContext(release), sheet, series.Name, cbrDipLabelsText(series.Labels))
		}
	}

	var points []cbrDipPoint
	lastData := -1
	for i := header + 1; i < len(rows); i++ {
		date, ok := cbrDipDate(cbrDipCell(rows[i], cbrDipDateColumn), use1904)
		if !ok {
			// Пустая или неразобранная как дата колонка A завершает данные (спека §3.4): ниже —
			// сноски источника или пустота. Значения в объявленных колонках ниже последней даты
			// ловит проверка хвоста под циклом.
			break
		}
		lastData = i
		for _, column := range columns {
			cell := cbrDipCell(rows[i], column.Index)
			if cell == "" {
				// Пустая ячейка — не точка (стандарт §6.7); литеральный ноль — точка.
				continue
			}
			value, err := cbrDipValue(cell)
			if err != nil {
				return nil, fmt.Errorf("%s, лист «%s», строка %d, колонка %s: %w",
					cbrDipContext(release), sheet, i+1, cbrDipColumnName(column.Index), err)
			}
			key := column.Series + "\x00" + date.Format("2006-01-02")
			if seen[key] {
				return nil, fmt.Errorf("%s, лист «%s»: точка (%q, %s) повторяется — дубль (series, date) в книге",
					cbrDipContext(release), sheet, column.Series, date.Format("2006-01-02"))
			}
			seen[key] = true
			points = append(points, cbrDipPoint{Series: column.Series, Date: date, Value: value})
		}
	}

	// Хвост ниже данных: непустая ячейка объявленной колонки под последней разобранной датой
	// означает, что часть ряда не разобрана (подпись строки не распознана), — неполный ряд не
	// должен выглядеть полным. Текст только в колонке A (сноска «Источник: Росстат, расчет ДИП»
	// в выпуске 21-06) — штатный случай: A не объявленная колонка. Прецедент проверки —
	// rosstat_labour_force_mes, который по той же причине падает на нераспознанной подписи
	// месяца, под которой остались значения (спека §3.4).
	if lastData < 0 {
		// Ни одной даты в колонке A не разобрано: значения в объявленных колонках — данные без
		// даты, и записать их нечем (стандарт §6.4). Отдельная ветка, а не общий хвост: без неё
		// сообщение говорило бы «ниже последней даты», которой нет, и причину пришлось бы искать
		// глазами по книге.
		for i := header + 1; i < len(rows); i++ {
			for _, column := range columns {
				if cell := cbrDipCell(rows[i], column.Index); cell != "" {
					return nil, fmt.Errorf("%s, лист «%s», строка %d, колонка %s: значение %q без даты — ни одной даты в колонке A не разобрано",
						cbrDipContext(release), sheet, i+1, cbrDipColumnName(column.Index), cell)
				}
			}
		}
		return points, nil
	}
	for i := lastData + 1; i < len(rows); i++ {
		for _, column := range columns {
			if cell := cbrDipCell(rows[i], column.Index); cell != "" {
				return nil, fmt.Errorf("%s, лист «%s», строка %d, колонка %s: значение %q ниже последней разобранной даты — данные продолжаются ниже разобранных (подпись строки не распознана?)",
					cbrDipContext(release), sheet, i+1, cbrDipColumnName(column.Index), cell)
			}
		}
	}
	return points, nil
}

// cbrDipParseBook разбирает книгу выпуска: ищет объявленные листы по A1, в каждом — шапку по
// объявленным подписям колонок, читает точки и отдаёт плоский список.
//
// Fail-fast (стандарт §6.4, спека §3.4): в книге нет ни одного объявленного листа; найденный
// лист не содержит ни одной объявленной подписи колонки в первых cbrDipHeaderRows строках; одна
// подпись встретилась в двух колонках; нечисловое или неконечное значение; дубль (series, date)
// внутри одного разбираемого листа; непустая ячейка объявленной колонки ниже разобранных данных;
// ни одной точки во всей книге («нулевой разбор» — «0 rows» выглядели бы успешным прогоном).
// Предупреждение и пропуск (§6.6, §12.3): объявленного листа нет в книге (в 13 выпусках нет
// листов рынка труда, включая свежий 26-07), объявленной колонки нет в найденном листе
// (компоненты SAAR в 22-04 и 22-05) и один объявленный заголовок A1 несут несколько листов
// (22-05: «Уровень безработицы, %» на листах «22» и «42» с теми же данными) — у листов-дублей
// разбирается первый по порядку книги, остальные пропускаются, а остальные листы и колонки
// импортируются.
//
// Ни сети, ни БД (стандарт §6.1): книга приходит открытой, выпуск — со страницы; ошибки
// называет cbrDipContext — таблицу и выпуск.
func cbrDipParseBook(xlsx *excelize.File, release dipRelease) ([]cbrDipPoint, error) {
	// Система дат книги: серийное число Excel само по себе не дата — без Date1904 (Mac-книги)
	// все точки сдвинулись бы на четыре года, и заметила бы это только живая сверка.
	props, err := xlsx.GetWorkbookProps()
	if err != nil {
		return nil, fmt.Errorf("%s: свойства книги: %w", cbrDipContext(release), err)
	}
	use1904 := props.Date1904 != nil && *props.Date1904

	sheetNames := xlsx.GetSheetList()
	titles := make([]string, len(sheetNames))
	for i, sheet := range sheetNames {
		title, err := xlsx.GetCellValue(sheet, "A1", excelize.Options{RawCellValue: true})
		if err != nil {
			return nil, fmt.Errorf("%s, лист «%s»: заголовок A1: %w", cbrDipContext(release), sheet, err)
		}
		titles[i] = title
	}

	var points []cbrDipPoint
	seen := make(map[string]bool)
	sheetFound := false
	for _, spec := range cbrDipSheets {
		matched := false
		parsedSheet := ""
		for i, sheet := range sheetNames {
			if !cbrDipLabelMatches(titles[i], spec.Titles) {
				continue
			}
			if matched {
				// Заголовок объявлен, но этот лист — дубль уже разобранного. Лист-дубль это свойство
				// вёрстки выпуска, а не поломка источника (22-05: «Уровень безработицы, %» на листах
				// «22» и «42», данные те же, различие лишь в строке со сноской «Источники: Росстат,
				// расчеты ДИП.»): точки обоих листов совпали бы, и разбор упал бы дублем (series,
				// date) — так и уронил живой прогон 2026-10-16. Разбирается первый лист по порядку
				// книги, остальные пропускаются с предупреждением (спека §3.4); ошибкой остаётся
				// дубль точки внутри одного разбираемого листа.
				log.Warnf("%s: заголовок %s несут листы «%s» и «%s» — разобран первый по порядку книги (лист «%s»), лист «%s» пропущен: точки листа-дубля не читаются (данные в нём те же, а разбор обоих дал бы дубль (series, date))",
					cbrDipContext(release), cbrDipLabelsText(spec.Titles), parsedSheet, sheet, parsedSheet, sheet)
				continue
			}
			matched, sheetFound = true, true
			parsedSheet = sheet
			sheetPoints, err := cbrDipParseSheet(xlsx, sheet, spec, release, use1904, seen)
			if err != nil {
				return nil, err
			}
			points = append(points, sheetPoints...)
		}
		if !matched {
			// Лист ожидаемый, но его нет в этом выпуске. Это свойство выпуска, а не поломка
			// источника: падение шага лишило бы данных все последующие выпуски цепочки dagu
			// (стандарт §12.3, спека §3.4).
			log.Warnf("%s: в книге нет листа с заголовком %s: ряды %s пропущены, остальные листы книги разобраны",
				cbrDipContext(release), cbrDipLabelsText(spec.Titles), cbrDipNamesText(spec.Series))
		}
	}
	if !sheetFound {
		return nil, fmt.Errorf("%s: в книге нет ни одного объявленного листа (заголовки A1: %s) — источник сменил разметку или разбирается не приложение ДИП",
			cbrDipContext(release), cbrDipTitlesText(titles))
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: разбор книги не дал ни одной точки — ни один объявленный ряд не разобран", cbrDipContext(release))
	}
	return points, nil
}
