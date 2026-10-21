package cbr

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Разбор PDF «Оперативная справка» (inFOM_ГГ-ММ.pdf) — источника, который ЦБ публикует
// раньше книги Infl_exp_ГГ-ММ.xlsx. На рис. 1 «Прямые оценки годовой инфляции» значения
// подписаны у точек, поэтому извлекаются не из текста подряд, а по координатам
// (`pdftotext -bbox`): колонка = месяц оси, порядок подписей сверху вниз = наблюдаемая →
// ожидаемая через год → ожидаемая через пять лет. Проверено на PDF 26-10: 33 исторических
// месяца совпадают с книгой 26-09 до 0.1 п.п., октябрь — 14,5 / 13,2 / 12,2.
//
// Сеть и exec остаются снаружи: разбор — чистая функция «bbox XHTML → колонки» (стандарт
// §6.1), поэтому тесты офлайн и не требуют poppler. Сам PDF в архив версий попадает через
// шаблон util.ClickHouseImport (BodyFunc), как у cbr_mt_forecast.
const (
	// inflExpPdfLinkSelector — ссылка на актуальный PDF в блоке версий страницы
	// инфляционных ожиданий. Имена inFOM_ГГ-ММ.pdf меняются каждый выпуск, а ссылок
	// .versions_item на странице сотни (старые Infl_exp_*.pdf и чужие файлы), поэтому
	// селектор сужен до inFOM-имени: первая ссылка в DOM — свежая.
	inflExpPdfLinkSelector = `a.versions_item[href*="inFOM_"][href$=".pdf"]`

	// inflExpPdfHeaderSize — сколько первых байт проверяется на сигнатуру `%PDF-`:
	// WAF отдаёт HTML-заглушку с кодом 200, и проверка стоит до exec.
	inflExpPdfHeaderSize = 1024

	// inflExpPdfTimeout — потолок одного извлечения: PDF 26-10 (381 КБ) разбирается за
	// доли секунды, 2 минуты — запас на медленный хост, а не рабочее время.
	inflExpPdfTimeout = 2 * time.Minute
)

// inflExpPdfContext — общая часть сообщений разбора (стандарт §6.4).
const inflExpPdfContext = cbrInflExpOpName + ", PDF «Оперативная справка»"

var (
	// inflExpPdfTool — утилита извлечения текста: `pdftotext` из poppler-utils.
	// Переменная, а не константа, ради теста отсутствующего бинарника.
	inflExpPdfTool = "pdftotext"

	// inflExpPdfWordRe — слово `-bbox`-вывода с рамкой. Атрибуты и их порядок печатает
	// сам poppler; текст подписи — ASCII-цифры и кириллица месяцев, поэтому `[^<]*`.
	inflExpPdfWordRe = regexp.MustCompile(`<word xMin="(-?[\d.]+)" yMin="(-?[\d.]+)" xMax="(-?[\d.]+)" yMax="(-?[\d.]+)">([^<]*)</word>`)

	// inflExpPdfAxisRe — подпись месяца оси: «окт.26». Год двузначный, век — 2000-е:
	// история ряда начинается с 2023, а ось PDF 26-10 — с янв.24.
	inflExpPdfAxisRe = regexp.MustCompile(`^(янв|фев|мар|апр|май|июн|июл|авг|сен|окт|ноя|дек)\.(\d{2})$`)

	// inflExpPdfValueRe — подпись значения у точки: одна цифра после запятой,
	// десятичный разделитель русский. Целые числа (номера страниц, доли рис. 2) не
	// подходят, а числа с точкой в PDF не встречаются.
	inflExpPdfValueRe = regexp.MustCompile(`^\d{1,2},\d$`)

	// inflExpPdfMonths — короткие подписи оси в нижнем регистре.
	inflExpPdfMonths = map[string]time.Month{
		"янв": time.January, "фев": time.February, "мар": time.March, "апр": time.April,
		"май": time.May, "июн": time.June, "июл": time.July, "авг": time.August,
		"сен": time.September, "окт": time.October, "ноя": time.November, "дек": time.December,
	}
)

// inflExpPdfWord — слово страницы с рамкой в координатах PDF (pt, y растёт вниз).
type inflExpPdfWord struct {
	xMin, yMin, xMax, yMax float64
	text                   string
}

// centerX — середина рамки: подпись центрирована на точке графика.
func (w inflExpPdfWord) centerX() float64 { return (w.xMin + w.xMax) / 2 }

// inflExpPdfPoint — месячная колонка рис. 1: месяц (первое число) и три медианы.
type inflExpPdfPoint struct {
	Month      time.Time
	Observed   float64
	Expected1y float64
	Expected5y float64
}

// inflExpPdfPoints разбирает `-bbox`-вывод страницы 1: по оси месяцев находит колонки,
// привязывает к ним подписи значений по x и сортирует их по y (сверху вниз). Возвращает
// по точке на колонку в порядке оси; импортируются последние два значения (первые два ряда).
//
// Fail-fast (стандарт §6.4): пустой текстовый слой; меньше inflExpPdfAxisMin месяцев оси;
// месяцы оси не подряд; подпись значения вне оси; в последней колонке не ровно три подписи;
// порядок подписей нарушен (v0 < v1 < v2); значение вне (0, 100).
func inflExpPdfPoints(bbox []byte) ([]inflExpPdfPoint, error) {
	words, err := inflExpPdfWords(bbox)
	if err != nil {
		return nil, err
	}
	axis, err := inflExpPdfAxis(words)
	if err != nil {
		return nil, err
	}
	points, err := inflExpPdfColumns(words, axis)
	if err != nil {
		return nil, err
	}
	last := points[len(points)-1]
	if last.Observed < last.Expected1y || last.Expected1y < last.Expected5y {
		return nil, fmt.Errorf("%s: порядок подписей последней колонки %s нарушен (%v < %v < %v): сверху вниз ожидались наблюдаемая, ожидаемая через год и через пять лет — вёрстка графика изменилась",
			inflExpPdfContext, last.Month.Format("2006-01"), last.Observed, last.Expected1y, last.Expected5y)
	}
	return points, nil
}

// inflExpPdfWords собирает слова `-bbox`-вывода. Пустой результат — текстового слоя нет:
// так выглядят старые выпуски-сканы (inFOM_26-09.pdf, inFOM_25-10.pdf), и молчаливое
// «0 строк» скрыло бы пропажу оперативной точки.
func inflExpPdfWords(bbox []byte) ([]inflExpPdfWord, error) {
	matches := inflExpPdfWordRe.FindAllSubmatch(bbox, -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("%s: текстовый слой PDF пуст — выпуск отсканирован или pdftotext не извлёк подписи", inflExpPdfContext)
	}
	words := make([]inflExpPdfWord, 0, len(matches))
	for _, m := range matches {
		coords := make([]float64, 4)
		for i := range coords {
			value, err := strconv.ParseFloat(string(m[i+1]), 64)
			if err != nil {
				return nil, fmt.Errorf("%s: координата %q слова %q: %w", inflExpPdfContext, m[i+1], m[5], err)
			}
			coords[i] = value
		}
		words = append(words, inflExpPdfWord{xMin: coords[0], yMin: coords[1], xMax: coords[2], yMax: coords[3], text: string(m[5])})
	}
	return words, nil
}

// inflExpPdfAxisMin — минимум подписей на оси, чтобы её можно было опознать и вычислить шаг:
// на странице две оси (рис. 1 и рис. 2), и верхняя должна быть полной.
const inflExpPdfAxisMin = 6

// inflExpPdfAxis находит ось рис. 1 — верхнюю группу подписей месяцев — и возвращает её,
// отсортированную по x, с уникальными x. Группы разделяются по y с допуском
// inflExpPdfAxisYTolerance: повёрнутые подписи одной оси печатаются на 373.0…373.6 pt,
// а ось рис. 2 стоит на ~330 pt ниже.
func inflExpPdfAxis(words []inflExpPdfWord) ([]inflExpPdfWord, error) {
	var candidates []inflExpPdfWord
	for _, w := range words {
		if inflExpPdfAxisRe.MatchString(w.text) {
			candidates = append(candidates, w)
		}
	}
	if len(candidates) < inflExpPdfAxisMin {
		return nil, fmt.Errorf("%s: подписей месяцев оси %d, want не меньше %d — ось графика не найдена", inflExpPdfContext, len(candidates), inflExpPdfAxisMin)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].yMin < candidates[j].yMin })
	top := candidates[0].yMin
	axis := make([]inflExpPdfWord, 0, len(candidates))
	for _, w := range candidates {
		if w.yMin-top <= inflExpPdfAxisYTolerance {
			axis = append(axis, w)
		}
	}
	if len(axis) < inflExpPdfAxisMin {
		return nil, fmt.Errorf("%s: на верхней оси %d подписей месяцев, want не меньше %d", inflExpPdfContext, len(axis), inflExpPdfAxisMin)
	}
	sort.Slice(axis, func(i, j int) bool { return axis[i].centerX() < axis[j].centerX() })
	unique := axis[:1]
	for _, w := range axis[1:] {
		if w.centerX()-unique[len(unique)-1].centerX() < inflExpPdfAxisXTolerance {
			continue
		}
		unique = append(unique, w)
	}
	if len(unique) < inflExpPdfAxisMin {
		return nil, fmt.Errorf("%s: после склейки дублей на оси осталось %d месяцев, want не меньше %d", inflExpPdfContext, len(unique), inflExpPdfAxisMin)
	}
	for i := 1; i < len(unique); i++ {
		prev, err := inflExpPdfAxisMonth(unique[i-1].text)
		if err != nil {
			return nil, err
		}
		next, err := inflExpPdfAxisMonth(unique[i].text)
		if err != nil {
			return nil, err
		}
		if !next.Equal(prev.AddDate(0, 1, 0)) {
			return nil, fmt.Errorf("%s: месяцы оси идут не подряд: %s после %s — подписи оси потеряны или склеены",
				inflExpPdfContext, unique[i].text, unique[i-1].text)
		}
	}
	return unique, nil
}

const (
	// inflExpPdfAxisYTolerance — допуск одной оси по y (pt): повёрнутые подписи
	// печатаются с разбросом меньше 1 pt, а вторая ось страницы ниже на сотни pt.
	inflExpPdfAxisYTolerance = 2
	// inflExpPdfAxisXTolerance — допуск дубля подписи по x (pt): шаг колонок ~12.5 pt.
	inflExpPdfAxisXTolerance = 1
)

// inflExpPdfAxisMonth разбирает подпись «окт.26» в первое число месяца.
func inflExpPdfAxisMonth(label string) (time.Time, error) {
	m := inflExpPdfAxisRe.FindStringSubmatch(label)
	if m == nil {
		return time.Time{}, fmt.Errorf("%s: подпись оси %q не разобрана", inflExpPdfContext, label)
	}
	month, ok := inflExpPdfMonths[m[1]]
	if !ok {
		return time.Time{}, fmt.Errorf("%s: месяц %q подписи оси %q не объявлен", inflExpPdfContext, m[1], label)
	}
	year, err := strconv.Atoi(m[2])
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: год %q подписи оси %q: %w", inflExpPdfContext, m[2], label, err)
	}
	return time.Date(2000+year, month, 1, 0, 0, 0, 0, time.UTC), nil
}

// inflExpPdfColumns раскладывает подписи значений по колонкам оси. Значение выше оси
// (y меньше, чем у подписей месяцев) и не дальше половины шага от колонки; порядок в
// колонке — сверху вниз. Колонки без подписей дают точку с нулями (историю импортёр
// не пишет), а последняя обязана иметь ровно три: по ней и строится оперативная точка.
func inflExpPdfColumns(words []inflExpPdfWord, axis []inflExpPdfWord) ([]inflExpPdfPoint, error) {
	axisY := axis[0].yMin
	for _, w := range axis[1:] {
		if w.yMin < axisY {
			axisY = w.yMin
		}
	}
	step := (axis[len(axis)-1].centerX() - axis[0].centerX()) / float64(len(axis)-1)
	if step <= 0 {
		return nil, fmt.Errorf("%s: шаг оси месяцев %v — колонки не различены", inflExpPdfContext, step)
	}
	columns := make([][]inflExpPdfWord, len(axis))
	for _, w := range words {
		if !inflExpPdfValueRe.MatchString(w.text) || w.yMin >= axisY {
			continue
		}
		index := int(math.Round((w.centerX() - axis[0].centerX()) / step))
		if index < 0 || index >= len(axis) {
			return nil, fmt.Errorf("%s: подпись %q (x=%.1f) вне оси месяцев — полоса графика сдвинулась", inflExpPdfContext, w.text, w.centerX())
		}
		columns[index] = append(columns[index], w)
	}
	points := make([]inflExpPdfPoint, 0, len(axis))
	for i, tick := range axis {
		month, err := inflExpPdfAxisMonth(tick.text)
		if err != nil {
			return nil, err
		}
		sort.Slice(columns[i], func(a, b int) bool { return columns[i][a].yMin < columns[i][b].yMin })
		if len(columns[i]) == 0 {
			points = append(points, inflExpPdfPoint{Month: month})
			continue
		}
		if i == len(axis)-1 && len(columns[i]) != 3 {
			return nil, fmt.Errorf("%s: подписей в последней колонке %s %d, want 3 — точка оперативного месяца не собрана",
				inflExpPdfContext, tick.text, len(columns[i]))
		}
		if len(columns[i]) > 3 {
			return nil, fmt.Errorf("%s: подписей в колонке %s %d, want не больше 3 — в полосу графика попало лишнее число",
				inflExpPdfContext, tick.text, len(columns[i]))
		}
		values := make([]float64, 0, len(columns[i]))
		for _, w := range columns[i] {
			value, err := strconv.ParseFloat(strings.Replace(w.text, ",", ".", 1), 64)
			if err != nil {
				return nil, fmt.Errorf("%s: значение %q колонки %s: %w", inflExpPdfContext, w.text, tick.text, err)
			}
			if value <= 0 || value >= 100 {
				return nil, fmt.Errorf("%s: значение %q колонки %s вне (0, 100) — подпись не является медианой", inflExpPdfContext, w.text, tick.text)
			}
			values = append(values, value)
		}
		point := inflExpPdfPoint{Month: month, Observed: values[0]}
		if len(values) > 1 {
			point.Expected1y = values[1]
		}
		if len(values) > 2 {
			point.Expected5y = values[2]
		}
		points = append(points, point)
	}
	return points, nil
}

// inflExpPdfBBox извлекает `-bbox`-XHTML первой страницы PDF утилитой pdftotext. Байты
// приходят срезом от шаблона импорта: util.ClickHouseImport сам качает файл и кладёт тот
// же срез в винтажный архив, поэтому сети здесь нет. BodyFunc шаблона не получает ctx,
// поэтому таймаут — свой, от context.Background(): зависший pdftotext не должен держать
// прогон импортёра.
//
// Fail-fast: в первых inflExpPdfHeaderSize байтах нет `%PDF-` (WAF/HTML-заглушка) —
// ошибка до exec; пустой stdout — тоже ошибка (скан без текстового слоя).
func inflExpPdfBBox(body []byte) ([]byte, error) {
	head := body[:min(len(body), inflExpPdfHeaderSize)]
	if !bytes.Contains(head, []byte("%PDF-")) {
		return nil, fmt.Errorf("%s: ответ не PDF (нет сигнатуры %%PDF-) — WAF/HTML вместо файла", inflExpPdfContext)
	}
	ctx, cancel := context.WithTimeout(context.Background(), inflExpPdfTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, inflExpPdfTool, "-bbox", "-enc", "UTF-8", "-f", "1", "-l", "1", "-", "-")
	cmd.Stdin = bytes.NewReader(body)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		stderrText := strings.TrimSpace(stderr.String())
		if stderrText == "" {
			stderrText = "stderr пуст"
		}
		return nil, fmt.Errorf("%s: %s: %w: %s (нужен poppler-utils: pdftotext)", inflExpPdfContext, inflExpPdfTool, err, stderrText)
	}
	if len(bytes.TrimSpace(output)) == 0 {
		return nil, fmt.Errorf("%s: %s: пустой stdout — в PDF нет текстового слоя (скан?)", inflExpPdfContext, inflExpPdfTool)
	}
	return output, nil
}

// inflExpPdfMonth — месяц выпуска из имени inFOM_ГГ-ММ.pdf: он же месяц последней точки
// графика (проверено на 26-09 и 26-10). Для книги и чужих файлов месяца не вывести — ошибка.
func inflExpPdfMonth(href string) (time.Time, error) {
	m := inflExpArchiveRe.FindStringSubmatch(path.Base(href))
	if m == nil {
		return time.Time{}, fmt.Errorf("%s: в ссылке %q нет имени inFOM_ГГ-ММ.pdf", inflExpPdfContext, href)
	}
	if !strings.EqualFold(m[3], "pdf") {
		return time.Time{}, fmt.Errorf("%s: ссылка %q — не PDF (расширение %q)", inflExpPdfContext, href, m[3])
	}
	year, err := strconv.Atoi(m[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: год %q ссылки %q: %w", inflExpPdfContext, m[1], href, err)
	}
	month, err := strconv.Atoi(m[2])
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: месяц %q ссылки %q: %w", inflExpPdfContext, m[2], href, err)
	}
	if month < 1 || month > 12 {
		return time.Time{}, fmt.Errorf("%s: месяц %d ссылки %q вне 1..12", inflExpPdfContext, month, href)
	}
	return time.Date(2000+year, time.Month(month), 1, 0, 0, 0, 0, time.UTC), nil
}

// inflExpPdfPointDate — дата точки в cbr_infl_exp: 14-е число месяца опроса, как у
// xlsx-импортёра (1-е число книги + 13 дней).
func inflExpPdfPointDate(month time.Time) time.Time {
	return time.Date(month.Year(), month.Month(), 14, 0, 0, 0, 0, time.UTC)
}

// inflExpPdfNewer — гейт прогона: месяц PDF строго новее уже записанной даты. Пустая
// таблица (нулевая дата) новее любого месяца; сравнение идёт по суткам, а не по моменту.
func inflExpPdfNewer(month, known time.Time) bool {
	day := time.Date(known.Year(), known.Month(), known.Day(), 0, 0, 0, 0, time.UTC)
	return inflExpPdfPointDate(month).After(day)
}

// inflExpPdfAppend пишет оперативную точку двумя рядами: наблюдаемая инфляция и ожидаемая
// через год. Пятилетний ряд из PDF не импортируется (решение владельца) — он остаётся
// индикатором порядка подписей в разборе.
func inflExpPdfAppend(point inflExpPdfPoint, batch driver.Batch) error {
	date := inflExpPdfPointDate(point.Month)
	for _, row := range []struct {
		name  string
		value float64
	}{
		{inflExpObservedName, point.Observed},
		{inflExpExpectedName, point.Expected1y},
	} {
		if err := batch.Append(row.name, date, row.value); err != nil {
			return fmt.Errorf("%s: запись %q за %s: %w", cbrInflExpOpName, row.name, date.Format("2006-01-02"), err)
		}
	}
	return nil
}
