package wgc

import (
	"strconv"
	"time"

	"github.com/xuri/excelize/v2"
)

// seriesPoint — точка ряда: имя ряда (etf_<метрика>_<регион>), дата (конец отчётного месяца,
// UTC) и значение в единицах ряда.
type seriesPoint struct {
	Series string
	Date   time.Time
	Value  float64
}

// monthEnd — последний день месяца t (UTC 00:00): WGC публикует месячные итоги, и датой точки
// в проекте принят конец отчётного месяца.
func monthEnd(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, time.UTC)
}

// cellRaw читает ячейку в raw-виде: без применения number format, поэтому serial-даты приходят
// числами («46295»), а не строками в формате ячейки.
func cellRaw(f *excelize.File, sheet string, col, row int) (string, error) {
	ref, err := excelize.CoordinatesToCellName(col, row)
	if err != nil {
		return "", err
	}
	return f.GetCellValue(sheet, ref, excelize.Options{RawCellValue: true})
}

// cellFloat разбирает значение ячейки: пустая ячейка — это отсутствие данных, а не ноль,
// поэтому она возвращается как (0, false, nil); непустой мусор — ошибка.
func cellFloat(v string) (value float64, ok bool, err error) {
	if v == "" {
		return 0, false, nil
	}
	value, err = strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, false, err
	}
	return value, true, nil
}

// cellDate переводит Excel-serial в дату (выгрузки WGC — 1900-система).
func cellDate(v string) (time.Time, error) {
	serial, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return time.Time{}, err
	}
	return excelize.ExcelDateToTime(serial, false)
}

// sheetSize возвращает границы листа (последняя колонка и строка) по длинам строк: GetRows
// здесь — источник размеров, значения ячеек читаются отдельно в raw-виде.
func sheetSize(f *excelize.File, sheet string) (maxCol, maxRow int, err error) {
	rows, err := f.GetRows(sheet)
	if err != nil {
		return 0, 0, err
	}
	for _, row := range rows {
		if len(row) > maxCol {
			maxCol = len(row)
		}
	}
	return maxCol, len(rows), nil
}
