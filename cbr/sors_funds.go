package cbr

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

// Сведения о размещенных и привлеченных средствах
// https://www.cbr.ru/statistics/bank_sector/sors/
// Прямая стабильная выгрузка: «Средства на счетах организаций, банковские депозиты (вклады) и
// другие привлеченные средства юридических лиц, физических лиц и индивидуальных
// предпринимателей (в целом по Российской Федерации), млн руб.» — помесячно с 01.02.2012.
const (
	cbrSorsFundsUrl   = "https://www.cbr.ru/vfs/statistics/BankSector/Borrowings/02_01_Funds_all.xlsx"
	cbrSorsFundsTable = "cbr_sors_funds"
	cbrSorsFundsDdl   = `CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, balance Float64
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	// cbrSorsFundsDateLayout — формат дат шапки листа: первое число отчётного месяца текстом.
	cbrSorsFundsDateLayout = "02.01.2006"
	// cbrSorsFundsMinDates — минимум месячных дат, при котором строка признаётся шапкой с датами.
	// Якорь, а не индекс строки (стандарт §6.3): заголовок книги — merged-строка, и её добавление
	// или удаление не должно сдвигать разбор.
	cbrSorsFundsMinDates = 12
	// cbrSorsFundsCountPoints — количественный чек версии (стандарт §9.1): число точек своего
	// ряда. Литерал, как и таблица: запрос — часть утверждения «эти точки мои». FINAL обязателен —
	// без него count() считает несмёрженные части и растёт после каждого повторного импорта.
	cbrSorsFundsCountPoints = "SELECT count() FROM cbr_sors_funds FINAL"
)

// CbrSorsFundsStat — импортёр средств клиентов банков; регистрируется реестром
// cbr/importers.go в хвосте (шаг dagu/cbr.yaml тоже в хвосте цепочки).
type CbrSorsFundsStat struct {
	util.ClickHouseImport
}

// sorsFundsLabel нормализует подпись строки источника: снимает отступ (ведущие пробелы) и
// хвостовую сноску `*`/`**`. Подписи в файле несут и то и другое («   депозиты юридических лиц*»),
// а карта имён объявлена кодом (стандарт §6.8).
func sorsFundsLabel(cell string) string {
	return strings.TrimSpace(strings.TrimRight(strings.TrimSpace(cell), "*"))
}

// sorsFundsDateRow ищет строку дат листа: первая строка, где не меньше cbrSorsFundsMinDates
// подряд идущих месячных дат в формате 02.01.2006 (колонка B и дальше). Разрыв шапки — ошибка,
// а не молчаливое усечение: иначе часть истории выпала бы из ряда без следа (§6.4).
func sorsFundsDateRow(rows [][]string) (int, []time.Time, error) {
	for i, row := range rows {
		if len(row) < 2 {
			continue
		}
		var dates []time.Time
		for _, cell := range row[1:] {
			date, err := time.Parse(cbrSorsFundsDateLayout, strings.TrimSpace(cell))
			if err != nil {
				break
			}
			dates = append(dates, date)
		}
		if len(dates) < cbrSorsFundsMinDates {
			continue
		}
		for _, cell := range row[len(dates)+1:] {
			if strings.TrimSpace(cell) != "" {
				return 0, nil, fmt.Errorf("шапка дат разорвана: после %d месячных дат есть непустая ячейка %q", len(dates), cell)
			}
		}
		return i, dates, nil
	}
	return 0, nil, fmt.Errorf("не найдена строка дат: ожидалось не меньше %d месячных дат в формате %s", cbrSorsFundsMinDates, cbrSorsFundsDateLayout)
}

// sorsFundsCode сопоставляет подпись строки стабильному коду ряда; вторая величина — найдена ли
// подпись в карте. Незнакомые строки (сноски, пустые) не ошибка: их пропускает разбор.
func sorsFundsCode(row []string) (string, bool) {
	if len(row) == 0 {
		return "", false
	}
	label := sorsFundsLabel(row[0])
	if label == "" {
		return "", false
	}
	for _, series := range cbrSorsFundsSeries {
		if series.Label == label {
			return series.Code, true
		}
	}
	return "", false
}

// sorsFundsImport разбирает три листа книги и пишет точки в batch: имя ряда — стабильный код с
// суффиксом валюты (`_rub`/`_fx`), дата — первое число отчётного месяца, значение — остаток на
// эту дату, млн руб. Пустая ячейка и литерал `-` — «данных нет», точкой не становятся (§6.7);
// ноль — значение (в валютном листе строка эскроу — нули: счета эскроу рублёвые). Пропала любая
// из семи подписей, нечисловая ячейка или ноль точек — ошибка (§6.4), а не молчаливый прогон.
func sorsFundsImport(xlsx *excelize.File, batch driver.Batch) error {
	expected := make(map[string]bool, len(cbrSorsFundsSheets))
	for _, sheet := range cbrSorsFundsSheets {
		expected[sheet.Name] = true
	}
	for _, name := range xlsx.GetSheetList() {
		if !expected[name] {
			log.Warnf("%s: в книге неизвестный лист «%s» — пропущен", cbrSorsFundsTable, name)
		}
	}
	count := 0
	for _, sheet := range cbrSorsFundsSheets {
		rows, err := xlsx.GetRows(sheet.Name, excelize.Options{RawCellValue: true})
		if err != nil {
			return fmt.Errorf("%s: лист «%s»: %w", cbrSorsFundsTable, sheet.Name, err)
		}
		headerRow, dates, err := sorsFundsDateRow(rows)
		if err != nil {
			return fmt.Errorf("%s: лист «%s»: %w", cbrSorsFundsTable, sheet.Name, err)
		}
		rowByCode := make(map[string]int, len(cbrSorsFundsSeries))
		for i := headerRow + 1; i < len(rows); i++ {
			code, ok := sorsFundsCode(rows[i])
			if !ok {
				continue
			}
			if prev, dup := rowByCode[code]; dup {
				return fmt.Errorf("%s: лист «%s»: подпись строки «%s» встречается дважды (строки %d и %d)",
					cbrSorsFundsTable, sheet.Name, sorsFundsLabel(rows[i][0]), prev+1, i+1)
			}
			rowByCode[code] = i
		}
		var missing []string
		for _, series := range cbrSorsFundsSeries {
			if _, ok := rowByCode[series.Code]; !ok {
				missing = append(missing, "«"+series.Label+"»")
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("%s: лист «%s»: не найдены строки %s", cbrSorsFundsTable, sheet.Name, strings.Join(missing, ", "))
		}
		for _, series := range cbrSorsFundsSeries {
			row := rows[rowByCode[series.Code]]
			name := series.Code + sheet.Suffix
			for j, date := range dates {
				col := j + 1
				if col >= len(row) {
					break
				}
				cell := strings.TrimSpace(row[col])
				if cell == "" || cell == "-" {
					continue
				}
				value, err := strconv.ParseFloat(strings.ReplaceAll(cell, "\u00a0", ""), 64)
				if err != nil {
					return fmt.Errorf("%s: лист «%s», строка «%s», %s: значение %q: %w",
						cbrSorsFundsTable, sheet.Name, series.Label, date.Format("2006-01-02"), cell, err)
				}
				if err = batch.Append(name, date, value); err != nil {
					return fmt.Errorf("%s: %s: %w", cbrSorsFundsTable, name, err)
				}
				count++
			}
		}
	}
	if count == 0 {
		return fmt.Errorf("%s: разбор трёх листов не дал ни одной точки", cbrSorsFundsTable)
	}
	return nil
}

// cbrSorsFundsImporter — средства клиентов банков РФ: 21 ряд (7 строк × 3 листа), помесячно с
// 2012-02. Файл обновляется раз в месяц при ежедневном шаге, поэтому объявлена точка контроля
// публикации: будничный прогон — HEAD-префлайт и no-op без скачивания.
var cbrSorsFundsImporter = &CbrSorsFundsStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              cbrSorsFundsTable,
	CreateTable:            []string{cbrSorsFundsDdl},
	DataUrl:                cbrSorsFundsUrl,
	ImportFunc:             sorsFundsImport,
	AfterImport:            afterCbrMacro(cbrSorsFundsSeriesMeta),
	PublicationCountPoints: cbrSorsFundsCountPoints,
}}
