package cbr

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// Общие хелперы разбора выгрузок платёжного баланса ЦБ РФ (cbr/bop_est.go — оценка ключевых
// агрегатов, cbr/bop_standard.go — стандартные компоненты): якоря листа, подписи периодов и
// привязка даты, стандарт §6 и спека пункта §3.5. Хелперы не знают ни имён рядов, ни имён
// таблиц: их задаёт вызывающий импортёр, поэтому один код разбора обслуживает оба файла ЦБ.

// Виды периода в имени ряда (спека §3.2): вид идёт в имя, а не в колонку таблицы — каталог и
// витрина пакета устроены как (source, series) и (source, series, date, value), и измерение
// «вид» в колонке разрушило бы и то и другое. Даты точек вида «квартал» и «последний месяц
// квартала» совпадают, и различает их только имя ряда.
const (
	cbrBopKindMonth      = "месяц"
	cbrBopKindQuarter    = "квартал"
	cbrBopKindYear       = "год"
	cbrBopKindCumulative = "накопленным итогом"
)

// cbrBopUnitAnchor — признак строки единиц: подпись «(млрд долларов США)» стоит в строке над
// шапкой (в файле оценки — правее последней колонки данных: у винтажа 15.10.2026 это K, у
// 14.09.2026 — O, и колонка переезжает вместе с числом колонок). От неё отсчитываются строки шапки
// и первая строка данных, поэтому магических индексов строк в разборе нет (стандарт §6.3).
const cbrBopUnitAnchor = "долларов США"

// cbrBopLabelReplacer — типографика подписей источника, не влияющая на смысл периода: ЦБ
// ставит между месяцами накопленного итога короткое и длинное тире (в спеке — дефис).
// Переносы строк и неразрывные пробелы отдельным правилом не нужны: их схлопывает
// strings.Fields в cbrBopNormalizeLabel.
var cbrBopLabelReplacer = strings.NewReplacer("–", "-", "—", "-")

// cbrBopNormalizeLabel приводит подпись строки или колонки источника к сравнимому виду:
// снимается пометка «(оценка)» (она относится к точке, а не к периоду: следующим выпуском ЦБ
// её снимает, и подпись не должна от этого меняться), переносы строк и неразрывные пробелы
// схлопываются в один пробел, тире становится дефисом, у формы «… 2026 г.» снимается хвост
// «г.». Тот же вид получает ожидаемая подпись из кода, поэтому сравнение идёт по
// нормализованным строкам (стандарт §6.3), а ошибки называют исходный текст ячейки.
func cbrBopNormalizeLabel(raw string) string {
	label := strings.ReplaceAll(raw, "(оценка)", "")
	label = cbrBopLabelReplacer.Replace(label)
	label = strings.Join(strings.Fields(label), " ")
	return strings.TrimSpace(strings.TrimSuffix(label, "г."))
}

// cbrBopSameLabel — совпадают ли подписи после нормализации; регистр не значим. Пустая строка
// не равна непустой подписи, поэтому пустые ячейки метки периода в поиске не участвуют.
func cbrBopSameLabel(a, b string) bool {
	return a != "" && b != "" && strings.EqualFold(cbrBopNormalizeLabel(a), cbrBopNormalizeLabel(b))
}

// cbrBopRowMatches — совпадает ли подпись строки хотя бы с одним из ожидаемых вариантов позиции.
// Вариантов у позиции бывает больше одного: ЦБ переименовывает строки листа от выпуска к выпуску
// (выпуск 15.10.2026 переименовал половину агрегатов оценки), и разбор принимает оба набора
// подписей по позиции строки, не заводя второй ряд (спека §3.3). Сравнение остаётся точным
// совпадением нормализованных подписей: варианты перечислены кодом, а не выводятся из текста.
func cbrBopRowMatches(cell string, labels []string) bool {
	for _, label := range labels {
		if cbrBopSameLabel(cell, label) {
			return true
		}
	}
	return false
}

// cbrBopLabelsText — перечень ожидаемых подписей для текста ошибки: по нему видно, чего именно не
// дождался разбор, — подписи строк меняются от выпуска к выпуску, и у позиции их может быть
// несколько.
func cbrBopLabelsText(labels []string) string {
	quoted := make([]string, 0, len(labels))
	for _, label := range labels {
		quoted = append(quoted, "«"+label+"»")
	}
	return strings.Join(quoted, " / ")
}

// cbrBopPeriod — разобранная подпись колонки: вид периода (он идёт в имя ряда) и дата точки.
type cbrBopPeriod struct {
	Kind string
	Date time.Time
}

// cbrBopParsePeriod разбирает подпись периода в одну из четырёх форм (спека §3.5):
// «месяц ГГГГ» (апрель 2026), «квартал ГГГГ» (II квартал 2026 в оценке и 2 квартал 2026 в
// стандартной презентации — запись номера у двух файлов ЦБ разная, см. cbrBopQuarter),
// «месяц - месяц ГГГГ» (январь - июль 2026; первый месяц обязан быть январь — иначе это не итог с
// начала года) и «ГГГГ» (2026). Дата точки — по спеке §3.4: последний день месяца, последний день
// квартала, 31 декабря, у накопленного итога — последний день последнего покрытого месяца.
// Неразобранная подпись — ошибка, а не пропуск колонки: значение без периода записать нечем
// (стандарт §6.4).
func cbrBopParsePeriod(raw string) (cbrBopPeriod, error) {
	label := cbrBopNormalizeLabel(raw)
	fields := strings.Fields(label)
	switch len(fields) {
	case 1:
		year, err := cbrBopYear(fields[0], label)
		if err != nil {
			return cbrBopPeriod{}, err
		}
		return cbrBopPeriod{Kind: cbrBopKindYear, Date: time.Date(year, time.December, 31, 0, 0, 0, 0, time.UTC)}, nil
	case 2:
		month, err := cbrBopMonth(fields[0], label)
		if err != nil {
			return cbrBopPeriod{}, err
		}
		year, err := cbrBopYear(fields[1], label)
		if err != nil {
			return cbrBopPeriod{}, err
		}
		return cbrBopPeriod{Kind: cbrBopKindMonth, Date: cbrBopLastDayOfMonth(year, month)}, nil
	case 3:
		if !strings.EqualFold(fields[1], "квартал") {
			return cbrBopPeriod{}, cbrBopPeriodFormError(label)
		}
		quarter, err := cbrBopQuarter(fields[0], label)
		if err != nil {
			return cbrBopPeriod{}, err
		}
		year, err := cbrBopYear(fields[2], label)
		if err != nil {
			return cbrBopPeriod{}, err
		}
		return cbrBopPeriod{Kind: cbrBopKindQuarter, Date: cbrBopLastDayOfQuarter(year, quarter)}, nil
	case 4:
		if fields[1] != "-" {
			return cbrBopPeriod{}, cbrBopPeriodFormError(label)
		}
		first, err := cbrBopMonth(fields[0], label)
		if err != nil {
			return cbrBopPeriod{}, err
		}
		if first != time.January {
			return cbrBopPeriod{}, fmt.Errorf("подпись %q: первый месяц накопленного итога %q не январь — ЦБ считает итог с начала года", label, fields[0])
		}
		last, err := cbrBopMonth(fields[2], label)
		if err != nil {
			return cbrBopPeriod{}, err
		}
		year, err := cbrBopYear(fields[3], label)
		if err != nil {
			return cbrBopPeriod{}, err
		}
		return cbrBopPeriod{Kind: cbrBopKindCumulative, Date: cbrBopLastDayOfMonth(year, last)}, nil
	default:
		return cbrBopPeriod{}, cbrBopPeriodFormError(label)
	}
}

// cbrBopPeriodFormError — отказ разбора с перечнем поддерживаемых форм: по тексту ошибки
// видно, какую именно подпись источника разбор не знает.
func cbrBopPeriodFormError(label string) error {
	return fmt.Errorf("подпись %q не разобрана как период: ожидается «месяц ГГГГ», «[IVX]+ квартал ГГГГ» (или «1 квартал ГГГГ»), «месяц - месяц ГГГГ» или «ГГГГ»", label)
}

// cbrBopMonth — месяц по русскому названию. Словарь месяцев один на пакет (util.MonthsToNum,
// правило 7 AGENTS.md): он знает и именительный падеж («июль»), и родительный («июля»).
func cbrBopMonth(name, label string) (time.Month, error) {
	month, ok := util.MonthsToNum[strings.ToLower(name)]
	if !ok {
		return 0, fmt.Errorf("подпись %q: неизвестный месяц %q", label, name)
	}
	return month, nil
}

// cbrBopYear — год подписи. Ошибка называется вместе с подписью: в листе десятки колонок, и
// «invalid syntax» без контекста не говорит, какую правку ждать от ЦБ.
func cbrBopYear(token, label string) (int, error) {
	year, err := strconv.Atoi(token)
	if err != nil {
		return 0, fmt.Errorf("подпись %q: год %q: %w", label, token, err)
	}
	return year, nil
}

// cbrBopRomanDigits — римские цифры квартала. В файлах ЦБ они латинские (I — 0x49): похожие
// кириллические «І» и «Х» цифрами не считаются и дают ошибку разбора — это смена формы
// подписи источником, а не квартал (стандарт §6.4).
var cbrBopRomanDigits = map[byte]int{'I': 1, 'V': 5, 'X': 10}

// cbrBopQuarter — номер квартала подписи. Запись номера у двух файлов ЦБ разная: оценка пишет
// квартал латинской римской цифрой («I квартал 2026 г.»), стандартная презентация — арабской
// («1 квартал 1994 г.»), и оба файла обслуживает один разбор, поэтому принимаются обе записи — и
// только они. Сначала пробуется римская; отказ римской записи у токена из одних римских цифр
// возвращается как есть (это содержательный отказ: «V квартал» — номер вне диапазона 1…4), а у
// остальных токенов принимается ровно одна арабская цифра 1…4. Посторонняя форма обязана валить
// прогон, а не превращаться в квартал догадкой (стандарт §6.4).
func cbrBopQuarter(token, label string) (int, error) {
	roman, romanErr := cbrBopRomanQuarter(token, label)
	if romanErr == nil {
		return roman, nil
	}
	if cbrBopRomanToken(token) {
		return 0, romanErr
	}
	// Арабская запись — ровно один символ из «1»…«4»: strconv.Atoi принял бы ещё «+2», «02» и « 2»
	// (ведущий пробел не доживает до сюда — его снимает cbrBopNormalizeLabel), а это уже не форма
	// спеки §3.5, и такая подпись обязана валить прогон с исходным текстом, а не становиться точкой.
	if len(token) != 1 || token[0] < '1' || token[0] > '4' {
		return 0, fmt.Errorf("подпись %q: %q не номер квартала (арабская цифра 1…4 или латинская римская I, V, X)", label, token)
	}
	return int(token[0] - '0'), nil
}

// cbrBopRomanToken — состоит ли токен целиком из римских цифр квартала. Отделяет отказ римской
// записи по диапазону («V квартал», «IX квартал») от токена, римским не являющегося вовсе: у
// второго ошибку обязана назвать арабская попытка, у первого арабская запись ни при чём.
func cbrBopRomanToken(token string) bool {
	if token == "" {
		return false
	}
	for i := 0; i < len(token); i++ {
		if _, ok := cbrBopRomanDigits[token[i]]; !ok {
			return false
		}
	}
	return true
}

// cbrBopRomanQuarter разбирает римскую цифру квартала — так номер пишет оценка (в стандартной
// презентации он арабский, и вызывает её cbrBopQuarter) — и проверяет диапазон: «V квартал»
// цифрой разбирается, но кварталом не является, и молча пропустить это нельзя.
func cbrBopRomanQuarter(token, label string) (int, error) {
	quarter := 0
	for i := 0; i < len(token); i++ {
		value, ok := cbrBopRomanDigits[token[i]]
		if !ok {
			return 0, fmt.Errorf("подпись %q: %q не римская цифра квартала (латинские I, V, X)", label, token)
		}
		// Вычитающее правило римской записи (IV = 4): следующая цифра больше текущей. За концом
		// токена следующей цифры нет — сравнение с нулём ничего не вычитает.
		next := 0
		if i+1 < len(token) {
			next = cbrBopRomanDigits[token[i+1]]
		}
		if value < next {
			quarter -= value
			continue
		}
		quarter += value
	}
	if quarter < 1 || quarter > 4 {
		return 0, fmt.Errorf("подпись %q: номер квартала %d вне диапазона 1…4", label, quarter)
	}
	return quarter, nil
}

// cbrBopLastDayOfMonth — последний день месяца: нулевой день от первого числа следующего
// месяца (time.Date нормализует день 0 в последний день предыдущего месяца). У привязки даты
// платёжного баланса конвенция — конец периода (спека §3.4), поэтому день 0, а не -1, как в
// cbrIntRateLoansMonthDate: там точка ряда — предпоследний день месяца.
func cbrBopLastDayOfMonth(year int, month time.Month) time.Time {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC)
}

// cbrBopLastDayOfQuarter — последний день квартала: его третий месяц, затем последний день
// месяца. Четвёртый квартал даёт time.Date(year, 13, 0) — это 31 декабря, как и ожидается.
func cbrBopLastDayOfQuarter(year, quarter int) time.Time {
	return cbrBopLastDayOfMonth(year, time.Month(quarter*3))
}

// cbrBopColumn — колонка данных листа: её индекс в строке (0-based; колонка 0 — метка периода)
// и разобранный период подписи.
type cbrBopColumn struct {
	Index  int
	Period cbrBopPeriod
}

// cbrBopLayout — разметка листа, выведенная из якорей: номер первой строки данных и колонки с
// разобранными периодами.
type cbrBopLayout struct {
	FirstDataRow int
	Columns      []cbrBopColumn
}

// cbrBopColumnName — буква колонки листа по индексу (0-based): ошибки разбора называют колонку
// так же, как её видит человек в Excel, а не «колонка 14».
func cbrBopColumnName(index int) string {
	name, err := excelize.ColumnNumberToName(index + 1)
	if err != nil {
		return strconv.Itoa(index + 1)
	}
	return name
}

// cbrBopSheetByPrefix ищет лист книги по префиксу имени и возвращает его точное имя: у выгрузки
// оценки дата выпуска стоит в имени листа («Оценка ПБ (15.10.2026)») и меняется с выпуском,
// поэтому имя листа — признак, а не константа (стандарт §6.3). Отсутствие листа — ошибка с
// именем таблицы и списком листов книги: «0 rows» на переименованном листе выглядели бы
// успешным прогоном (стандарт §6.4).
func cbrBopSheetByPrefix(xlsx *excelize.File, table, prefix string) (string, error) {
	sheets := xlsx.GetSheetList()
	for _, name := range sheets {
		if strings.HasPrefix(name, prefix) {
			return name, nil
		}
	}
	return "", fmt.Errorf("%s: в книге нет листа с префиксом %q (листы книги: %s)", table, prefix, strings.Join(sheets, ", "))
}

// cbrBopSheetByName ищет лист книги по точному имени и возвращает его же: у стандартной
// презентации имена листов — константы («Кварталы», «Годы»), даты выпуска в них нет, поэтому
// префиксный поиск здесь был бы слабее признака — он принял бы «Кварталы (30.09.2026)» за тот же
// лист и молча разобрал бы не то, что объявлено. Отсутствие листа — ошибка с именем таблицы и
// списком листов книги: «0 rows» на переименованном листе выглядели бы успешным прогоном
// (стандарт §6.4, спека §3.5).
func cbrBopSheetByName(xlsx *excelize.File, table, name string) (string, error) {
	sheets := xlsx.GetSheetList()
	for _, sheet := range sheets {
		if sheet == name {
			return sheet, nil
		}
	}
	return "", fmt.Errorf("%s: в книге нет листа «%s» (листы книги: %s)", table, name, strings.Join(sheets, ", "))
}

// cbrBopRowContains — есть ли в строке ячейка, содержащая подстроку.
func cbrBopRowContains(row []string, substr string) bool {
	for _, cell := range row {
		if strings.Contains(cell, substr) {
			return true
		}
	}
	return false
}

// cbrBopRowEmpty — строка без единой непустой ячейки: пустые строки шапки её строками не
// считаются, иначе шапка «нашлась» бы и в пустом листе.
func cbrBopRowEmpty(row []string) bool {
	for _, cell := range row {
		if strings.TrimSpace(cell) != "" {
			return false
		}
	}
	return true
}

// cbrBopColumnPeriod возвращает подпись колонки: это самая нижняя непустая ячейка её строк шапки,
// и она обязана разобраться как период. Спека §3.5 допускает мягкий разбор — «первая,
// разобравшаяся как период, при обходе снизу вверх»: неразобравшуюся ячейку можно пропустить и
// взять следующую выше. Код строже намеренно: мягкий разбор молча пропустил бы строку-аннотацию,
// добавленную источником под периодами, взял бы период из чужой строки, и значения уехали бы в
// (name, date) сдвинутой шапки, тогда как смена шапки обязана валить прогон громко (стандарт §6.4,
// §12.3). На живом листе оба варианта дают одно и то же: самая нижняя непустая ячейка колонки
// данных и есть подпись периода (в листе оценки над периодами стоит только «Справочно:» —
// периодом оно не является, а под ним в строке ниже лежит подпись прошлого года, поэтому разбором
// оно не берётся). Возвращает разобранный период и текст, на котором разбор остановился: если он
// не разобрался, это та самая нижняя непустая ячейка — ею ошибка называет текст источника. Пустой
// текст означает, что подписей в колонке нет вовсе, то есть колонка данных не она.
func cbrBopColumnPeriod(headerRows [][]string, column int) (cbrBopPeriod, string, bool) {
	for i := len(headerRows) - 1; i >= 0; i-- {
		row := headerRows[i]
		if column >= len(row) {
			continue
		}
		cell := strings.TrimSpace(row[column])
		if cell == "" {
			continue
		}
		period, err := cbrBopParsePeriod(cell)
		if err != nil {
			return cbrBopPeriod{}, cell, false
		}
		return period, cell, true
	}
	return cbrBopPeriod{}, "", false
}

// cbrBopSheetLayout выводит разметку листа из якорей (стандарт §6.3): строка единиц — ячейка с
// «долларов США»; первая строка данных — первая ниже неё, где в колонке A стоит первая ожидаемая
// подпись агрегата (firstLabels — её варианты: у оценки подписей два набора, у стандартной
// презентации один); строки шапки — непустые строки между ними. Колонки данных — колонки 1…
// (колонка A — метка периода), у каждой подпись берётся снизу вверх. Колонка с подписью, не
// разобравшейся как период, и колонка со значением, но без подписи, — ошибка (спека §3.5): первая
// означает смену формы подписи источником, вторая — значение, которому нечем назначить период.
// Ошибки называют таблицу, лист, колонку буквой и строку.
func cbrBopSheetLayout(rows [][]string, table, sheet string, firstLabels []string) (cbrBopLayout, error) {
	layout := cbrBopLayout{}
	unitRow := -1
	for i, row := range rows {
		if cbrBopRowContains(row, cbrBopUnitAnchor) {
			unitRow = i
			break
		}
	}
	if unitRow < 0 {
		return layout, fmt.Errorf("%s: лист «%s»: не найдена строка единиц (ячейка с %q) — шапка источника не распознана",
			table, sheet, cbrBopUnitAnchor)
	}
	firstDataRow := -1
	for i := unitRow + 1; i < len(rows); i++ {
		if len(rows[i]) > 0 && cbrBopRowMatches(rows[i][0], firstLabels) {
			firstDataRow = i
			break
		}
	}
	if firstDataRow < 0 {
		return layout, fmt.Errorf("%s: лист «%s»: ниже строки единиц (строка %d) нет первой ожидаемой подписи агрегата (варианты: %s) — источник изменил состав листа",
			table, sheet, unitRow+1, cbrBopLabelsText(firstLabels))
	}
	headerRows := make([][]string, 0, firstDataRow-unitRow)
	for i := unitRow + 1; i < firstDataRow; i++ {
		if !cbrBopRowEmpty(rows[i]) {
			headerRows = append(headerRows, rows[i])
		}
	}
	if len(headerRows) == 0 {
		return layout, fmt.Errorf("%s: лист «%s»: между строкой единиц и первой строкой данных нет строк с подписями периодов — колонки привязать не к чему",
			table, sheet)
	}
	width := 0
	for _, row := range headerRows {
		if len(row) > width {
			width = len(row)
		}
	}
	columns := make([]cbrBopColumn, 0, width)
	labeled := make(map[int]bool, width)
	for c := 1; c < width; c++ {
		period, text, ok := cbrBopColumnPeriod(headerRows, c)
		if text == "" {
			continue
		}
		if !ok {
			// Нижняя непустая ячейка колонки не разобралась как период — ошибка, а не «поискать
			// выше»: иначе аннотация, добавленная источником под периодами, была бы молча
			// пропущена, а период взят из чужой строки (разбор подписи — cbrBopColumnPeriod).
			return layout, fmt.Errorf("%s: лист «%s», колонка %s: подпись %q не разобрана как период — источник изменил шапку листа",
				table, sheet, cbrBopColumnName(c), text)
		}
		columns = append(columns, cbrBopColumn{Index: c, Period: period})
		labeled[c] = true
	}
	if len(columns) == 0 {
		return layout, fmt.Errorf("%s: лист «%s»: в строках шапки нет ни одной колонки с разобранной подписью периода", table, sheet)
	}
	// Колонка со значением, но без разобравшейся подписи, — ошибка (спека §3.5): иначе точка
	// молча не попала бы в таблицу, а ряд остался бы без обновления (стандарт §6.4).
	for i := firstDataRow; i < len(rows); i++ {
		for c, cell := range rows[i] {
			if c == 0 || labeled[c] || strings.TrimSpace(cell) == "" {
				continue
			}
			return layout, fmt.Errorf("%s: лист «%s», колонка %s строки %d: есть значение %q, но подпись периода не разобрана",
				table, sheet, cbrBopColumnName(c), i+1, strings.TrimSpace(cell))
		}
	}
	layout.FirstDataRow = firstDataRow
	layout.Columns = columns
	return layout, nil
}
