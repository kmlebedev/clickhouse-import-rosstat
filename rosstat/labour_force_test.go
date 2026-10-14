package rosstat

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// labourForceFixturePath — реальная (урезанная zip-хирургией) выгрузка Росстата со листами
// «Содержание» и «5»: шапка в строках 5–6, подписи месяцев со сносками («январь1)»,
// «октябрь1)2)»), перевод строки внутри подписи «Уровень занятости, \nв %», текст сносок в
// строках 132–133. Разметка урезания не меняет (§11.2), поэтому фикстура проверяет разметку
// источника, а не допущения парсера (§11.1).
const labourForceFixturePath = "testdata/labour_force.xlsx"

// openLabourForceFixture открывает книгу фикстуры — так же, как salariesMesImport в
// rosstat/salaries_mes_test.go:31.
func openLabourForceFixture(t *testing.T) *excelize.File {
	t.Helper()
	xlsx, err := excelize.OpenFile(labourForceFixturePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	return xlsx
}

// parseLabourForceFixture разбирает реальную фикстуру: ни сети, ни базы тесту не нужно (§11.3).
func parseLabourForceFixture(t *testing.T) []labourForcePoint {
	t.Helper()
	points, err := parseLabourForce(openLabourForceFixture(t))
	if err != nil {
		t.Fatalf("parseLabourForce: %v", err)
	}
	return points
}

// mutateLabourForceFixture собирает книгу-мутанта из копии реальной фикстуры: копия кладётся в
// t.TempDir(), один элемент меняется через excelize, копия сохраняется и открывается заново —
// разбор видит ровно тот файл, что записан на диск. Синтетическая книга (newTestWorkbook) здесь
// не годится: она кодировала бы допущения парсера о разметке (сноски в подписях месяцев, перевод
// строки в шапке, строка «в том числе»), а не разметку источника (§11.1).
func mutateLabourForceFixture(t *testing.T, mutate func(f *excelize.File)) *excelize.File {
	t.Helper()
	data, err := os.ReadFile(labourForceFixturePath)
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "labour_force.xlsx")
	if err = os.WriteFile(copyPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	mutated, err := excelize.OpenFile(copyPath)
	if err != nil {
		t.Fatal(err)
	}
	mutate(mutated)
	if err = mutated.Save(); err != nil {
		t.Fatal(err)
	}
	if err = mutated.Close(); err != nil {
		t.Fatal(err)
	}
	saved, err := excelize.OpenFile(copyPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = saved.Close() })
	return saved
}

// TestLabourForceReadsFixture — разбор реальной выгрузки: лист «5», месяцы 2017-01 … 2026-07
// (115 месяцев) по шести колонкам шапки — 690 точек; дата точки — последний день отчётного
// месяца (спека §3.4), поэтому крайние даты 2017-01-31 и 2026-07-31.
func TestLabourForceReadsFixture(t *testing.T) {
	points := parseLabourForceFixture(t)
	if len(points) != 690 {
		t.Fatalf("точек %d, ожидалось 690", len(points))
	}
	series := make(map[string]int, 6)
	first, last := points[0].date, points[0].date
	for _, point := range points {
		series[point.series]++
		if point.date.Before(first) {
			first = point.date
		}
		if point.date.After(last) {
			last = point.date
		}
	}
	if len(series) != 6 {
		t.Errorf("рядов %d, ожидалось 6: %v", len(series), series)
	}
	for name, count := range series {
		if count != 115 {
			t.Errorf("ряд %q: точек %d, ожидалось 115 (2017-01 … 2026-07)", name, count)
		}
	}
	if want := time.Date(2017, time.January, 31, 0, 0, 0, 0, time.UTC); !first.Equal(want) {
		t.Errorf("минимальная дата %s, ожидалась %s", first.Format("2006-01-02"), want.Format("2006-01-02"))
	}
	if want := time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC); !last.Equal(want) {
		t.Errorf("максимальная дата %s, ожидалась %s", last.Format("2006-01-02"), want.Format("2006-01-02"))
	}
}

// TestLabourForceSpotValues — значения из известных ячеек источника (спека §2): значение
// читается сырым (§6.2), дата — последний день отчётного месяца.
func TestLabourForceSpotValues(t *testing.T) {
	points := parseLabourForceFixture(t)
	spots := []struct {
		series string
		date   time.Time
		value  float64
	}{
		// B8 = 76489.193 — январь 2017.
		{"Рабочая сила", time.Date(2017, time.January, 31, 0, 0, 0, 0, time.UTC), 76489.193},
		// G8 = 5.6 — уровень безработицы января 2017.
		{"Уровень безработицы", time.Date(2017, time.January, 31, 0, 0, 0, 0, time.UTC), 5.6},
		// B131 = 76389.482 — последний месяц выгрузки, июль 2026.
		{"Рабочая сила", time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC), 76389.482},
		// C131 = 74660.473, D131 = 1729.009 и E131 = 62.9 — занятые, безработные и уровень
		// участия июля 2026. Рабочая сила за этот месяц пинтится строкой выше, но остальные
		// колонки последнего месяца иначе не закреплены тестом, а урезание фикстуры или сдвиг
		// строки-секции года меняет именно их.
		{"Занятые", time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC), 74660.473},
		{"Безработные", time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC), 1729.009},
		{"Уровень участия в составе рабочей силы", time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC), 62.9},
		// F131 = 61.5 и G131 = 2.3 — уровни занятости и безработицы июля 2026.
		{"Уровень занятости", time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC), 61.5},
		{"Уровень безработицы", time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC), 2.3},
	}
	for _, spot := range spots {
		found := 0
		for _, point := range points {
			if point.series != spot.series || !point.date.Equal(spot.date) {
				continue
			}
			found++
			if point.value != spot.value {
				t.Errorf("%s %s: значение %v, ожидалось %v",
					spot.series, spot.date.Format("2006-01-02"), point.value, spot.value)
			}
		}
		if found != 1 {
			t.Errorf("%s %s: точек %d, ожидалась 1", spot.series, spot.date.Format("2006-01-02"), found)
		}
	}
}

// TestLabourForceFootnoteLabels — подписи источника приходят со сносками и переводом строки:
// «январь1)», «октябрь1)2)» (с октября 2022 г. источник ставит обе сноски) и шапка «Уровень
// занятости, \nв %». util.MonthsToNum сноски не понимает (util/xls.go:25), поэтому разбор
// обязан снять хвостовые сноски и сжать пробелы — иначе месяц не распознаётся вовсе.
func TestLabourForceFootnoteLabels(t *testing.T) {
	tests := []struct{ raw, want string }{
		{"январь1)", "январь"},
		{"октябрь1)2)", "октябрь"},
		{"Уровень занятости, \nв %", "Уровень занятости, в %"},
	}
	for _, test := range tests {
		if got := labourForceLabel(test.raw); got != test.want {
			t.Errorf("labourForceLabel(%q) = %q, ожидалось %q", test.raw, got, test.want)
		}
	}
	// Подписи месяцев разбора и года-секции сносок не несут и метки не меняют.
	if got := labourForceLabel("2017"); got != "2017" {
		t.Errorf("labourForceLabel(%q) = %q, ожидалось %q", "2017", got, "2017")
	}
}

// TestLabourForceMissingSheetFails — книга без листа «5»: отсутствие ожидаемого листа означает
// сломанную или подменённую выгрузку, а не «0 точек» (§6.4). Книга — копия реальной фикстуры с
// удалённым листом.
func TestLabourForceMissingSheetFails(t *testing.T) {
	xlsx := mutateLabourForceFixture(t, func(f *excelize.File) {
		if err := f.DeleteSheet(labourForceSheet); err != nil {
			t.Fatal(err)
		}
	})
	points, err := parseLabourForce(xlsx)
	if err == nil {
		t.Fatalf("книга без листа %q разобрана без ошибки: %d точек", labourForceSheet, len(points))
	}
	if !strings.Contains(err.Error(), rosstatLabourForceTable) || !strings.Contains(err.Error(), labourForceSheet) {
		t.Errorf("ошибка %q не называет таблицу %q и лист %q", err, rosstatLabourForceTable, labourForceSheet)
	}
	if points != nil {
		t.Errorf("на ошибке вернулось %d точек, ожидалось nil", len(points))
	}
}

// TestLabourForceMissingHeaderFails — лист «5» без строки шапки: якорь «Рабочая сила» не
// найден, разбор обязан вернуть ошибку с именем листа, а не молча дать 0 точек (§6.4).
func TestLabourForceMissingHeaderFails(t *testing.T) {
	xlsx := mutateLabourForceFixture(t, func(f *excelize.File) {
		if err := f.SetCellValue(labourForceSheet, "B5", ""); err != nil {
			t.Fatal(err)
		}
	})
	if _, err := parseLabourForce(xlsx); err == nil {
		t.Fatalf("лист %q без строки шапки разобран без ошибки", labourForceSheet)
	} else if !strings.Contains(err.Error(), labourForceSheet) {
		t.Errorf("ошибка %q не называет лист %q", err, labourForceSheet)
	}
}

// TestLabourForceNonNumericCellFails — ячейка значения не число и не пуста (Review Focus 2):
// текст обязан дать ошибку разбора с именем листа и строки, а не пропуск — пропуск выглядел бы
// в витрине как отсутствие месяца.
func TestLabourForceNonNumericCellFails(t *testing.T) {
	const broken = "н/д"
	xlsx := mutateLabourForceFixture(t, func(f *excelize.File) {
		// B8 — «Рабочая сила» января 2017.
		if err := f.SetCellValue(labourForceSheet, "B8", broken); err != nil {
			t.Fatal(err)
		}
	})
	points, err := parseLabourForce(xlsx)
	if err == nil {
		t.Fatalf("ячейка %q разобрана без ошибки: %d точек", broken, len(points))
	}
	if !strings.Contains(err.Error(), "не число") || !strings.Contains(err.Error(), broken) {
		t.Errorf("ошибка %q не называет нечисловое значение %q", err, broken)
	}
}

// TestLabourForceEmptyCellIsNotPoint — пустые ячейки последнего месяца выгрузки (данных ещё
// нет, источник печатает месяц заранее) точками не становятся: Review Focus 1. Книга — копия
// реальной фикстуры с очищенными ячейками июля 2026; нулей в ряду при этом быть не должно (§6.7).
func TestLabourForceEmptyCellIsNotPoint(t *testing.T) {
	// Ожидание — литерал (690 точек), а не пересчёт по фикстуре: иначе потерянный при разборе
	// месяц уехал бы вместе с ожиданием и тест остался бы зелёным.
	const baseline = 690
	const emptied = 6
	xlsx := mutateLabourForceFixture(t, func(f *excelize.File) {
		for _, cell := range []string{"B131", "C131", "D131", "E131", "F131", "G131"} {
			if err := f.SetCellValue(labourForceSheet, cell, ""); err != nil {
				t.Fatal(err)
			}
		}
	})
	points, err := parseLabourForce(xlsx)
	if err != nil {
		t.Fatalf("parseLabourForce: %v", err)
	}
	if len(points) != baseline-emptied {
		t.Errorf("точек %d, ожидалось %d: пустая ячейка — не точка", len(points), baseline-emptied)
	}
	july := time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC)
	for _, point := range points {
		if point.value == 0 {
			t.Errorf("%s %s: записана нулевая точка — пустая ячейка означает «данных нет»",
				point.series, point.date.Format("2006-01-02"))
		}
		if point.date.Equal(july) {
			t.Errorf("%s: точка %s записана по пустой ячейке", point.series, july.Format("2006-01-02"))
		}
	}
}

// TestLabourForceMisspelledMonthFails — опечатка в подписи месяца («феврал» вместо «февраль»)
// даёт нераспознанную подпись в середине таблицы. Раньше разбор на ней молча замыкался и отдавал
// 6 точек: неполный ряд в витрине выглядит полным, поэтому теперь замыкание проверяется — строка с
// нераспознанной подписью, несущая значения в колонках рядов, сама является ошибкой (§6.4).
func TestLabourForceMisspelledMonthFails(t *testing.T) {
	const broken = "феврал"
	xlsx := mutateLabourForceFixture(t, func(f *excelize.File) {
		// A9 — подпись февраля 2017; значения февраля (B9:G9) остаются на месте.
		if err := f.SetCellValue(labourForceSheet, "A9", broken); err != nil {
			t.Fatal(err)
		}
	})
	points, err := parseLabourForce(xlsx)
	if err == nil {
		t.Fatalf("подпись %q разобрана без ошибки: %d точек вместо 690", broken, len(points))
	}
	if !strings.Contains(err.Error(), rosstatLabourForceTable) || !strings.Contains(err.Error(), labourForceSheet) {
		t.Errorf("ошибка %q не называет таблицу %q и лист %q", err, rosstatLabourForceTable, labourForceSheet)
	}
	if !strings.Contains(err.Error(), broken) || !strings.Contains(err.Error(), "не распознана") {
		t.Errorf("ошибка %q не называет нераспознанную подпись %q", err, broken)
	}
	if !strings.Contains(err.Error(), "строка 9") {
		t.Errorf("ошибка %q не называет строку с нераспознанной подписью", err)
	}
}

// TestLabourForceGarbageLabelOnLastMonthFails — та же поломка в последней строке таблицы: подпись
// июля 2026 (A131) заменена мусором при заполненных значениях. Прежняя проверка начиналась со
// следующей строки и пропускала саму строку-замыкание, поэтому июль исчезал молча (684 точки,
// err=nil). Теперь строка-замыкание проверяется наравне с остальными, и ошибка называет её номер.
func TestLabourForceGarbageLabelOnLastMonthFails(t *testing.T) {
	const broken = "xxx"
	xlsx := mutateLabourForceFixture(t, func(f *excelize.File) {
		// A131 — подпись июля 2026; значения июля (B131:G131) остаются на месте.
		if err := f.SetCellValue(labourForceSheet, "A131", broken); err != nil {
			t.Fatal(err)
		}
	})
	points, err := parseLabourForce(xlsx)
	if err == nil {
		t.Fatalf("подпись %q последней строки разобрана без ошибки: %d точек вместо 690", broken, len(points))
	}
	if !strings.Contains(err.Error(), rosstatLabourForceTable) || !strings.Contains(err.Error(), labourForceSheet) {
		t.Errorf("ошибка %q не называет таблицу %q и лист %q", err, rosstatLabourForceTable, labourForceSheet)
	}
	if !strings.Contains(err.Error(), "строка 131") {
		t.Errorf("ошибка %q не называет строку-замыкание 131", err)
	}
	if !strings.Contains(err.Error(), broken) || !strings.Contains(err.Error(), "не распознана") {
		t.Errorf("ошибка %q не называет нераспознанную подпись %q", err, broken)
	}
}

// TestLabourForceUnparsedLabelWithDataBelowFails — вторая ветка проверки замыкания: строка с
// нераспознанной подписью значений не несёт (слетела только подпись), но данные идут ниже неё.
// Без этой ветки разбор вернул бы пустые точки вместо ошибки при потерянной строке-подписи.
func TestLabourForceUnparsedLabelWithDataBelowFails(t *testing.T) {
	const broken = "феврал"
	xlsx := mutateLabourForceFixture(t, func(f *excelize.File) {
		if err := f.SetCellValue(labourForceSheet, "A9", broken); err != nil {
			t.Fatal(err)
		}
		for _, cell := range []string{"B9", "C9", "D9", "E9", "F9", "G9"} {
			if err := f.SetCellValue(labourForceSheet, cell, ""); err != nil {
				t.Fatal(err)
			}
		}
	})
	points, err := parseLabourForce(xlsx)
	if err == nil {
		t.Fatalf("подпись %q без значений разобрана без ошибки: %d точек вместо 690", broken, len(points))
	}
	if !strings.Contains(err.Error(), "данные продолжаются ниже строки 9") {
		t.Errorf("ошибка %q не называет строку с нераспознанной подписью и продолжение данных ниже", err)
	}
	if !strings.Contains(err.Error(), "строка 10") || !strings.Contains(err.Error(), broken) {
		t.Errorf("ошибка %q не называет строку данных 10 и подпись %q", err, broken)
	}
}

// TestLabourForceLostMonthLabelFails — потерянная подпись месяца (пустая A9) при заполненных
// значениях: строка-разделитель без значений — норма, но строка значений без подписи означает
// сломанную разметку, и пропуск такой строки терял бы шесть точек молча (§6.4).
func TestLabourForceLostMonthLabelFails(t *testing.T) {
	xlsx := mutateLabourForceFixture(t, func(f *excelize.File) {
		if err := f.SetCellValue(labourForceSheet, "A9", ""); err != nil {
			t.Fatal(err)
		}
	})
	points, err := parseLabourForce(xlsx)
	if err == nil {
		t.Fatalf("строка значений без подписи месяца разобрана без ошибки: %d точек вместо 690", len(points))
	}
	if !strings.Contains(err.Error(), rosstatLabourForceTable) || !strings.Contains(err.Error(), labourForceSheet) {
		t.Errorf("ошибка %q не называет таблицу %q и лист %q", err, rosstatLabourForceTable, labourForceSheet)
	}
	if !strings.Contains(err.Error(), "подпись месяца пуста") {
		t.Errorf("ошибка %q не называет потерянную подпись месяца", err)
	}
}

// TestLabourForceRemovedMonthRowFails — источник потерял строку месяца (RemoveRow удалил февраль
// 2017), поэтому март идёт сразу за январём. До проверки последовательности разбор отдавал 684
// точки и err=nil, префлайт записывал points=684, и месячная дыра в головном ряду канала
// оставалась навсегда без сигнала (версия известна — дальше вечный no-op). Теперь разбор обязан
// упасть и назвать строку, сбитый месяц и предыдущий — по-русски.
func TestLabourForceRemovedMonthRowFails(t *testing.T) {
	xlsx := mutateLabourForceFixture(t, func(f *excelize.File) {
		// Строка 9 — февраль 2017; строки ниже сдвигаются, и март оказывается строкой 9.
		if err := f.RemoveRow(labourForceSheet, 9); err != nil {
			t.Fatal(err)
		}
	})
	points, err := parseLabourForce(xlsx)
	if err == nil {
		t.Fatalf("потеря строки месяца разобрана без ошибки: %d точек вместо ошибки", len(points))
	}
	if points != nil {
		t.Errorf("на ошибке вернулось %d точек, ожидалось nil", len(points))
	}
	if !strings.Contains(err.Error(), rosstatLabourForceTable) || !strings.Contains(err.Error(), labourForceSheet) {
		t.Errorf("ошибка %q не называет таблицу %q и лист %q", err, rosstatLabourForceTable, labourForceSheet)
	}
	if !strings.Contains(err.Error(), "строка 9") {
		t.Errorf("ошибка %q не называет строку со сбитой последовательностью месяцев", err)
	}
	if !strings.Contains(err.Error(), `месяц "март" не следует за месяцем "январь"`) {
		t.Errorf("ошибка %q не называет сбитый март и предыдущий январь", err)
	}
	if strings.Contains(err.Error(), "March") || strings.Contains(err.Error(), "January") {
		t.Errorf("ошибка %q печатает месяц по-английски: оператор должен видеть русскую подпись", err)
	}
}

// TestLabourForceMismatchedYearSectionFails — в строке-секции года напечатано «2025» вместо
// «2026» (A124): январь–июль 2026 записались бы датами 2025-01-31 … 2025-07-31 и по ключу
// (name, date) затёрли бы корректные точки тех же месяцев 2025 — тихая порча, а не ошибка.
// До проверки последовательности разбор отдавал 690 точек и err=nil.
func TestLabourForceMismatchedYearSectionFails(t *testing.T) {
	xlsx := mutateLabourForceFixture(t, func(f *excelize.File) {
		if err := f.SetCellValue(labourForceSheet, "A124", "2025"); err != nil {
			t.Fatal(err)
		}
	})
	points, err := parseLabourForce(xlsx)
	if err == nil {
		t.Fatalf("повтор года в строке-секции разобран без ошибки: %d точек вместо ошибки", len(points))
	}
	if points != nil {
		t.Errorf("на ошибке вернулось %d точек, ожидалось nil", len(points))
	}
	if !strings.Contains(err.Error(), rosstatLabourForceTable) || !strings.Contains(err.Error(), labourForceSheet) {
		t.Errorf("ошибка %q не называет таблицу %q и лист %q", err, rosstatLabourForceTable, labourForceSheet)
	}
	if !strings.Contains(err.Error(), "строка 124") {
		t.Errorf("ошибка %q не называет строку-секцию года", err)
	}
	if !strings.Contains(err.Error(), "год 2025 идёт после года 2025") {
		t.Errorf("ошибка %q не называет повтор года 2025", err)
	}
}

// TestLabourForceEmptyParseFails — все строки месяцев на месте, но значения в шести колонках
// пусты: разбор обязан вернуть ошибку «0 точек» (§6.4, §11.5), а не пустой батч — пустая
// выгрузка означает сломанный или подменённый источник, а не отсутствие показателя.
func TestLabourForceEmptyParseFails(t *testing.T) {
	xlsx := mutateLabourForceFixture(t, func(f *excelize.File) {
		rows, err := f.GetRows(labourForceSheet, excelize.Options{RawCellValue: true})
		if err != nil {
			t.Fatal(err)
		}
		for i, row := range rows {
			if len(row) == 0 {
				continue
			}
			// Подписи месяцев остаются на месте — очищаются только ячейки значений шести колонок.
			if _, ok := util.MonthsToNum[strings.ToLower(labourForceLabel(row[0]))]; !ok {
				continue
			}
			for col := 1; col <= 6; col++ {
				cell, err := excelize.CoordinatesToCellName(col+1, i+1)
				if err != nil {
					t.Fatal(err)
				}
				if err = f.SetCellValue(labourForceSheet, cell, ""); err != nil {
					t.Fatal(err)
				}
			}
		}
	})
	points, err := parseLabourForce(xlsx)
	if err == nil {
		t.Fatalf("лист без значений разобран без ошибки: %d точек", len(points))
	}
	if !strings.Contains(err.Error(), rosstatLabourForceTable) || !strings.Contains(err.Error(), labourForceSheet) {
		t.Errorf("ошибка %q не называет таблицу %q и лист %q", err, rosstatLabourForceTable, labourForceSheet)
	}
	if !strings.Contains(err.Error(), "разбор дал 0 точек") {
		t.Errorf("ошибка %q не называет пустой разбор", err)
	}
}

// TestLabourForceIgnoresTrailingFootnoteRows — после последней строки месяца (131) идут строки
// 132–133 с текстом сносок источника: разбор на них завершается, а не падает и не читает их
// точками (Review Focus 4). Ожидания — литералы (115 строк-месяцев, 690 точек), а не пересчёт
// через тот же util.MonthsToNum, что и разбор: иначе исчезнувший из выгрузки месяц уехал бы
// вместе с ожиданием и тест остался бы зелёным. Число строк-месяцев в книге считается отдельно —
// так тест ловит и «лишнюю» точку из строки сносок, и потерянный месяц.
func TestLabourForceIgnoresTrailingFootnoteRows(t *testing.T) {
	const wantMonths = 115
	const wantPoints = 690
	xlsx := openLabourForceFixture(t)
	points, err := parseLabourForce(xlsx)
	if err != nil {
		t.Fatalf("parseLabourForce: %v", err)
	}
	rows, err := xlsx.GetRows(labourForceSheet, excelize.Options{RawCellValue: true})
	if err != nil {
		t.Fatal(err)
	}
	months := 0
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		if _, ok := util.MonthsToNum[strings.ToLower(labourForceLabel(row[0]))]; ok {
			months++
		}
	}
	if months != wantMonths {
		t.Errorf("строк-месяцев в книге %d, ожидалось %d", months, wantMonths)
	}
	if len(points) != wantPoints {
		t.Errorf("точек %d, ожидалось %d (%d месяцев × 6 рядов)", len(points), wantPoints, wantMonths)
	}
	last := time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC)
	for _, point := range points {
		if point.date.After(last) {
			t.Errorf("%s: точка %s позже последнего месяца выгрузки — строка сноски прочитана точкой",
				point.series, point.date.Format("2006-01-02"))
		}
	}
}

// TestLabourForceFindLink — селектор ссылки проверяется на HTML-фикстуре тем же util.FindLink,
// что и в Import() (§11.3, Review Focus 5). В фикстуре лежат обе соседние выгрузки: «trud-1_15-s»
// (дефис) и «trud_1_15-72» (подчёркивание), поэтому селектор с суффиксом действительно
// проверяется — общая маска по «trud» вернула бы не тот файл. На странице без ссылки разбор
// обязан упасть, а не отдать пустую строку в GetXlsx("").
func TestLabourForceFindLink(t *testing.T) {
	// Адрес страницы закреплён литералом: с неё снята HTML-фикстура, с неё же Import()
	// (следующая задача) берёт ссылку на файл. Сравнение константы с литералом, а не с самой
	// собой: молчаливая правка адреса иначе ушла бы вместе со «свежей» фикстурой.
	if labourForcePage != "https://rosstat.gov.ru/labour_force" {
		t.Errorf("labourForcePage = %q, ожидалось %q", labourForcePage, "https://rosstat.gov.ru/labour_force")
	}
	page, err := os.ReadFile("testdata/labour_force_page.html")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(page, []byte("/storage/mediabank/trud_1_15-72.xlsx")) {
		t.Fatal("в HTML-фикстуре нет соседнего файла trud_1_15-72.xlsx — селектор ничем не проверяется")
	}
	href, err := util.FindLink(page, labourForceLinkSelector)
	if err != nil {
		t.Fatalf("FindLink: %v", err)
	}
	if href != "/storage/mediabank/trud-1_15-s.xlsx" {
		t.Errorf("FindLink() = %q, ожидалось %q", href, "/storage/mediabank/trud-1_15-s.xlsx")
	}
	// Реальная фикстура другой страницы Росстата (рынок труда и зарплаты) ссылки на файл не несёт.
	other, err := os.ReadFile("testdata/salaries_page.html")
	if err != nil {
		t.Fatal(err)
	}
	if href, err = util.FindLink(other, labourForceLinkSelector); err == nil {
		t.Errorf("на странице без выгрузки найдена ссылка %q, ожидалась ошибка", href)
	}
}

// TestLabourForceImportAppendsFloat64 — импортёр на реальной фикстуре пишет 690 точек шести рядов,
// и каждая запись — float64: это пинит схему колонки Float64 новой таблицы (§5.3). Запись float32
// прошла бы в колонку Float64 молча, потеряв точность на шаге представления, и заметен такой
// сдвиг только по типу значения в харнессе (§11.6).
func TestLabourForceImportAppendsFloat64(t *testing.T) {
	rec := &appendRecorder{}
	if err := labourForceImport(openLabourForceFixture(t), rec); err != nil {
		t.Fatalf("labourForceImport: %v", err)
	}
	if len(rec.rows) != 690 {
		t.Fatalf("точек записано %d, ожидалось 690", len(rec.rows))
	}
	series := make(map[string]int, 6)
	for _, row := range rec.rows {
		series[row.series]++
		if row.valueType != "float64" {
			t.Errorf("%s %s: запись типа %s, ожидался float64 (колонка value таблицы %s)",
				row.series, row.date.Format("2006-01-02"), row.valueType, rosstatLabourForceTable)
		}
	}
	if len(series) != 6 {
		t.Errorf("записано рядов %d, ожидалось 6: %v", len(series), series)
	}
	for name, count := range series {
		if count != 115 {
			t.Errorf("ряд %q: точек %d, ожидалось 115 (2017-01 … 2026-07)", name, count)
		}
	}
}

// TestLabourForceAppendErrorNotSwallowed — ошибка записи доходит до вызывающего (§5.5, §11.6):
// сбой Append в середине батча обязан вернуть ошибку, а не выглядеть успешным импортом.
func TestLabourForceAppendErrorNotSwallowed(t *testing.T) {
	wantErr := errors.New("append сломан")
	rec := &appendRecorder{appendErr: wantErr}
	err := labourForceImport(openLabourForceFixture(t), rec)
	if !errors.Is(err, wantErr) {
		t.Fatalf("ошибка Append не вернулась: err = %v", err)
	}
	if len(rec.rows) != 0 {
		t.Errorf("в батч записано %d строк, ожидалось 0 (первый Append упал)", len(rec.rows))
	}
}

// TestLabourForceSeriesMetaCoversParsedRows — двусторонний контракт каталога (§11.4): имена рядов
// объявлены кодом (§6.8), поэтому проверяются оба направления — «разобрано ⊆ описано» и «описано ⊆
// разобрано». Односторонней проверки здесь мало: имя, забытое в каталоге, оставило бы агента MCP
// без описания ряда, а имя, забытое в разборе, — описание ряда, которого нет в данных; сверка
// каталога с самим собой не поймала бы ни то, ни другое.
func TestLabourForceSeriesMetaCoversParsedRows(t *testing.T) {
	described := describedSeries(t, labourForceSeriesMeta)
	for _, meta := range labourForceSeriesMeta {
		// Частота — часть контракта каталога: ряд помесячный, и «M» проверяется литералом.
		if meta.Frequency != "M" {
			t.Errorf("ряд %q: частота %q, ожидалась %q", meta.Series, meta.Frequency, "M")
		}
	}
	points := parseLabourForceFixture(t)
	parsed := make([]string, 0, len(points))
	for _, point := range points {
		parsed = append(parsed, point.series)
	}
	requireParsedDescribed(t, parsed, described)
}

// TestLabourForcePublicationDeclaration — точка контроля публикации объявлена (§9.1): файл
// перезаписывается на месте под стабильным именем и обновляется раз в месяц при недельном шаге
// расписания. Url объявления пуст намеренно: ссылка вычисляется внутри Import(), и отпечаток
// префлайта шаблон строит по DataUrl, а не по этой копии (§5.4, util.PublicationDecl).
// CountPoints сверяется с литералом, а не с константой labourForceCountPoints: объявление
// собирается из той же константы, и сравнение с ней не могло бы упасть — настоящий литерал
// живёт здесь и в колонке countPoints реестра (rosstat/importers.go), которую сверяет энфорсер
// TestRosstatImportersPrefixedAndRegistered.
func TestLabourForcePublicationDeclaration(t *testing.T) {
	pub := labourForceImporter.Publication()
	if want := "SELECT count() FROM rosstat_labour_force_mes FINAL"; pub.CountPoints != want {
		t.Errorf("объявление импортёра даёт запрос чека %q, ожидался %q", pub.CountPoints, want)
	}
	if pub.Source != rosstatLabourForceTable {
		t.Errorf("объявление импортёра даёт ключ версии %q, ожидался %q (Name() == TableName)", pub.Source, rosstatLabourForceTable)
	}
	if pub.Url != "" {
		t.Errorf("объявление импортёра несёт Url %q, ожидалась пустая строка: ссылка вычисляется в Import()", pub.Url)
	}
}
