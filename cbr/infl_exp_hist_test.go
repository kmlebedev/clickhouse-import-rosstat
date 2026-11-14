package cbr

import (
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

func cbrInflExpHistFixture(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", path, err)
	}
	return body
}

// TestCbrInflExpHistTable2023 — таблица волны 2023-01: последние значения строк совпадают с
// точками книги в БД (наблюдаемая 14,9, ожидаемая 11,6 на 2023-01-14). Это главный якорь
// разбора: правило «последнее значение строки» сверено с независимым источником.
func TestCbrInflExpHistTable2023(t *testing.T) {
	rows, err := inflExpHistRows(cbrInflExpHistFixture(t, "testdata/infl_exp_hist_2023_table.txt"), time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("разбор таблицы 2023-01: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("строк %d, want 2 (наблюдаемая и ожидаемая)", len(rows))
	}
	want := map[string]float64{inflExpObservedName: 14.9, inflExpExpectedName: 11.6}
	for _, row := range rows {
		if !row.Date.Equal(time.Date(2023, time.January, 14, 0, 0, 0, 0, time.UTC)) {
			t.Errorf("дата %s, want 2023-01-14", row.Date.Format("2006-01-02"))
		}
		if expected, ok := want[row.Series]; !ok || math.Abs(row.Value-expected) > 1e-4 {
			t.Errorf("%s = %v, want %v", row.Series, row.Value, expected)
		}
	}
}

// TestCbrInflExpHistTable2019 — волна 2019-01: подпись строки подробнее («ФОМ (медиана,
// наблюдаемая инфляция)»), значения 10,1 и 10,4 — хвост пятимесячного ряда.
func TestCbrInflExpHistTable2019(t *testing.T) {
	rows, err := inflExpHistRows(cbrInflExpHistFixture(t, "testdata/infl_exp_hist_2019_table.txt"), time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("разбор таблицы 2019-01: %v", err)
	}
	want := map[string]float64{inflExpObservedName: 10.1, inflExpExpectedName: 10.4}
	if len(rows) != len(want) {
		t.Fatalf("строк %d, want %d", len(rows), len(want))
	}
	for _, row := range rows {
		if expected, ok := want[row.Series]; !ok || math.Abs(row.Value-expected) > 1e-4 {
			t.Errorf("%s = %v, want %v", row.Series, row.Value, expected)
		}
	}
}

// TestCbrInflExpHistNoTable2017 — в выпуске 2017-01 таблицы нет: числа напечатаны прозой в
// двухколоночной вёрстке. Это не ошибка разбора, а граница корпуса: волна пропускается.
func TestCbrInflExpHistNoTable2017(t *testing.T) {
	rows, err := inflExpHistRows(cbrInflExpHistFixture(t, "testdata/infl_exp_hist_2017_prose.txt"), time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("разбор текста 2017-01: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("строк %d, want 0: в выпуске 2017-01 таблицы нет", len(rows))
	}
}

// TestCbrInflExpHistWaves — реестр волн со страницы ожиданий: не меньше 140 месяцев, свежая
// волна — 2026-10, а волна 2022-01 приходит страницей (PDF лежит внутри неё).
func TestCbrInflExpHistWaves(t *testing.T) {
	waves, err := inflExpHistWaves(cbrInflExpHistFixture(t, "testdata/infl_exp_index.html"))
	if err != nil {
		t.Fatalf("разбор страницы волн: %v", err)
	}
	if len(waves) < 140 {
		t.Fatalf("волн %d, want не меньше 140", len(waves))
	}
	if got := waves[0].month.Format("2006-01"); got != "2026-10" {
		t.Errorf("свежая волна %s, want 2026-10", got)
	}
	byMonth := map[string]inflExpHistWave{}
	for _, wave := range waves {
		byMonth[wave.month.Format("2006-01")] = wave
	}
	if wave, ok := byMonth["2022-01"]; !ok || wave.url[len(wave.url)-1:] != "/" {
		t.Errorf("волна 2022-01: %+v, want ссылку на страницу волны", wave)
	}
	if wave, ok := byMonth["2019-01"]; !ok || wave.url[len(wave.url)-4:] != ".pdf" {
		t.Errorf("волна 2019-01: %+v, want прямой PDF", wave)
	}
	// Ноябрь 2019 назван в архиве файлом Infl_exp_19-10.pdf (как и октябрь): месяц волны
	// обязан браться из подписи ссылки, иначе дедупликация по имени файла выбрасывает
	// ноябрь и оставляет неверную октябрьскую ссылку.
	november, ok := byMonth["2019-11"]
	if !ok {
		t.Fatalf("волна 2019-11 потеряна: архив назвал её файлом Infl_exp_19-10.pdf")
	}
	if got := november.month.Format("2006-01"); got != "2019-11" {
		t.Errorf("месяц волны %s, want 2019-11", got)
	}
	if !strings.HasSuffix(november.url, "Infl_exp_19-10.pdf") {
		t.Errorf("документ волны 2019-11 = %q, want ссылку из подписи «Ноябрь»", november.url)
	}
	if november.annex == "" {
		t.Errorf("у волны 2019-11 нет статистического приложения: refetch приложения закрывает ноябрь машиночитаемо")
	}
	if wave, ok := byMonth["2019-01"]; !ok || wave.annex == "" {
		t.Errorf("волна 2019-01: приложение %q, want stat_Infl_exp_*.xlsx", wave.annex)
	}
}

// inflExpHistAnnexFixture открывает статистическое приложение волны тем же кодом, что и
// импортёр: фикстура — реальный файл ЦБ, урезанный удалением посторонних листов.
func inflExpHistAnnexFixture(t *testing.T, path string) *excelize.File {
	t.Helper()
	xlsx, err := excelize.OpenFile(path)
	if err != nil {
		t.Fatalf("открытие приложения %s: %v", path, err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	return xlsx
}

// TestCbrInflExpHistAnnexVertical — приложение волны 2017-01 печатает таблицу медиан
// вертикально: колонка дат плюс по колонке на ряд, история с 2010-04. Именно этот файл
// закрывает 2017 год, который раньше считался недоступным без разбора прозы.
func TestCbrInflExpHistAnnexVertical(t *testing.T) {
	rows, err := inflExpHistAnnexRows(inflExpHistAnnexFixture(t, "testdata/infl_exp_hist_annex_17-01.xlsx"))
	if err != nil {
		t.Fatalf("разбор приложения 2017-01: %v", err)
	}
	counts := inflExpHistAnnexCounts(rows)
	if counts[inflExpObservedName] != 50 || counts[inflExpExpectedName] != 50 {
		t.Errorf("точек по рядам %v, want по 50 (2010-04 … 2017-01)", counts)
	}
	inflExpHistAnnexCheck(t, rows, inflExpObservedName, "2010-04-14", 17.355263157894736)
	inflExpHistAnnexCheck(t, rows, inflExpExpectedName, "2010-04-14", 14.4)
	inflExpHistAnnexCheck(t, rows, inflExpObservedName, "2017-01-14", 13.9828)
	inflExpHistAnnexCheck(t, rows, inflExpExpectedName, "2017-01-14", 11.4855)
}

// TestCbrInflExpHistAnnexHorizontal — приложение волны 2019-11 печатает ту же таблицу
// строкой дат: 2017-01 … 2019-11. Последняя точка — значение самой волны, поэтому
// ноябрь 2019 приходит приложением, даже если PDF волны назван файлом октября.
func TestCbrInflExpHistAnnexHorizontal(t *testing.T) {
	rows, err := inflExpHistAnnexRows(inflExpHistAnnexFixture(t, "testdata/infl_exp_hist_annex_19-11.xlsx"))
	if err != nil {
		t.Fatalf("разбор приложения 2019-11: %v", err)
	}
	counts := inflExpHistAnnexCounts(rows)
	if counts[inflExpObservedName] != 35 || counts[inflExpExpectedName] != 35 {
		t.Errorf("точек по рядам %v, want по 35 (2017-01 … 2019-11)", counts)
	}
	inflExpHistAnnexCheck(t, rows, inflExpObservedName, "2019-11-14", 8.6797)
	inflExpHistAnnexCheck(t, rows, inflExpExpectedName, "2019-11-14", 8.2566)
	inflExpHistAnnexCheck(t, rows, inflExpObservedName, "2017-01-14", 13.9828)
	inflExpHistAnnexCheck(t, rows, inflExpExpectedName, "2017-01-14", 11.4855)
}

// TestCbrInflExpHistRowsNoTable — заголовок «Прямые оценки…» в PDF — подпись графика, а не
// таблица: волна без строки «ФОМ (медиана)» пропускается, а не валит прогон. Это штатное
// ограничение PDF 2017–2018, машиночитаемая история берётся из приложения волны.
func TestCbrInflExpHistRowsNoTable(t *testing.T) {
	text := []byte("Прямые оценки годовой инфляции: медианные значения\nиные подписи строк\n")
	rows, err := inflExpHistRows(text, time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("подпись графика без таблицы: %v, want nil", err)
	}
	if len(rows) != 0 {
		t.Errorf("строк %d, want 0", len(rows))
	}
}

// TestCbrInflExpHistRowsHeaderWithoutValuesFails — шапка строки «ФОМ (медиана)» в тексте
// есть, а значений нет: это смена вёрстки, и она обязана быть ошибкой волны, а не
// «в выпуске нет таблицы» (иначе волна тихо выпадает из ряда).
func TestCbrInflExpHistRowsHeaderWithoutValuesFails(t *testing.T) {
	text := []byte("ФОМ (медиана) предыдущие 12 месяцев —\n")
	if _, err := inflExpHistRows(text, time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("шапка таблицы без значений разобрана без ошибки")
	}
}

// TestCbrInflExpHistAnnexHorizontalShiftedDates — приложение 2022+ печатает строку дат с
// первой колонки, а значения — со второй: привязка по абсолютному номеру колонки
// записала бы январь значением февраля. Тест пинит порядок, а не номера колонок.
func TestCbrInflExpHistAnnexHorizontalShiftedDates(t *testing.T) {
	grid := [][]string{
		{"Прямые оценки годовой инфляции: медианные значения"},
		{"2022-01-01", "2022-02-01", "2022-03-01"},
		{"наблюдаемая инфляция", "16.705", "17.1443", "16.313"},
		{"ожидаемая инфляция", "13.7279", "13.5372", "18.3345"},
	}
	rows, ok := inflExpHistAnnexHorizontal(grid, 0)
	if !ok {
		t.Fatal("таблица 2022+ не разобрана")
	}
	inflExpHistAnnexCheck(t, rows, inflExpObservedName, "2022-01-14", 16.705)
	inflExpHistAnnexCheck(t, rows, inflExpObservedName, "2022-02-14", 17.1443)
	inflExpHistAnnexCheck(t, rows, inflExpExpectedName, "2022-01-14", 13.7279)
}

// inflExpHistAnnexCounts — число точек по рядам: ловит тихую потерю половины таблицы.
func inflExpHistAnnexCounts(rows []util.FallbackRow) map[string]int {
	counts := map[string]int{}
	for _, row := range rows {
		counts[row.Series]++
	}
	return counts
}

// inflExpHistAnnexCheck сверяет точку приложения с известной ячейкой источника.
func inflExpHistAnnexCheck(t *testing.T, rows []util.FallbackRow, series, date string, want float64) {
	t.Helper()
	for _, row := range rows {
		if row.Series != series || row.Date.Format("2006-01-02") != date {
			continue
		}
		if math.Abs(row.Value-want) > 1e-4 {
			t.Errorf("%s на %s = %v, want %v", series, date, row.Value, want)
		}
		return
	}
	t.Errorf("точка %s на %s не разобрана", series, date)
}

// TestCbrInflExpHistAnnexSharedHeader — лист «Данные за все годы» приложения 2022+: строка
// дат стоит в начале листа, а под заголовком таблицы сразу идут значения. Без поиска шапки
// выше заголовка этот лист не разбирался бы, и история до 2022 оставалась бы недостижимой.
func TestCbrInflExpHistAnnexSharedHeader(t *testing.T) {
	grid := [][]string{
		{"2017-01-01", "2017-02-01", "2017-03-01"},
		{"иной блок", "1", "2", "3"},
		{"Прямые оценки годовой инфляции: медианные значения"},
		{"наблюдаемая инфляция (в %)", "13.9828", "15.0564", "14.03747"},
		{"ожидаемая инфляция (в %)", "11.4855", "12.8791", "11.16207"},
	}
	rows, ok := inflExpHistAnnexHorizontal(grid, 2)
	if !ok {
		t.Fatal("таблица с общей шапкой листа не разобрана")
	}
	inflExpHistAnnexCheck(t, rows, inflExpObservedName, "2017-01-14", 13.9828)
	inflExpHistAnnexCheck(t, rows, inflExpExpectedName, "2017-03-14", 11.16207)
}

// TestCbrInflExpHistAnnexIgnoresSubgroups — рядом с основной таблицей приложение печатает
// подгруппы («наблюдаемая инфляция среди тех, кто имеет сбережения»): вхождение подстроки
// записало бы их значения в основной ряд и удвоило точки.
func TestCbrInflExpHistAnnexIgnoresSubgroups(t *testing.T) {
	grid := [][]string{
		{"Прямые оценки годовой инфляции: медианные значения"},
		{"", "2019-01-01", "2019-02-01"},
		{"наблюдаемая инфляция", "10.1", "10.2"},
		{"наблюдаемая инфляция среди тех, кто имеет сбережения", "8.0", "8.1"},
		{"ожидаемая инфляция", "10.4", "10.5"},
	}
	rows, ok := inflExpHistAnnexHorizontal(grid, 0)
	if !ok {
		t.Fatal("таблица не разобрана")
	}
	counts := inflExpHistAnnexCounts(rows)
	if counts[inflExpObservedName] != 2 || counts[inflExpExpectedName] != 2 {
		t.Fatalf("точек по рядам %v, want по 2: подгруппы не должны попадать в основной ряд", counts)
	}
	inflExpHistAnnexCheck(t, rows, inflExpObservedName, "2019-01-14", 10.1)
}
