package minfin

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// Краткая ежемесячная информация об исполнении консолидированного бюджета Российской Федерации и государственных внебюджетных фондов (млрд. руб., накоплено с начала года)
// https://minfin.gov.ru/ru/document?id_4=93447-kratkaya_ezhemesyachnaya_informatsiya_ob_ispolnenii_konsolidirovannogo_byudzheta_rossiiskoi_federatsii_i_gosudarstvennykh_vnebyudzhetnykh_fondov_mlrd._rub._nakopleno_s_nachala_goda
// https://minfin.gov.ru/common/upload/library/2025/08/main/Prilozhenie_8_dannye_115-117_%E2%80%94_mesyats.xlsx
// https://minfin.gov.ru/common/upload/library/2025/10/main/Prilozhenie_8_dannye_115-117_%E2%80%94_mesyats.xlsx
// https://minfin.gov.ru/common/upload/library/2026/02/main/Prilozhenie_8_dannye_115-117_%E2%80%94_mesyats.xlsx
const (
	minfinUrl          = "https://minfin.gov.ru"
	conbudExecutePath  = "/ru/statistics/conbud/execute"
	fedbudMesyatsTable = "minfin_fed_bud_mesyats"
	// fedbudMesyatsLinkSelector — ссылка на актуальный XLSX консолидированного
	// бюджета. Имя файла содержит эм-даш и меняется от выпуска к выпуску,
	// поэтому ссылка берётся со страницы, а не строится по дате.
	fedbudMesyatsLinkSelector = `a[href*="_mesyats.xlsx"]`
)

var fedbudMesyatsFields = []string{"Доходы, всего", "Расходы, всего", "Акцизы", "Национальная оборона", "Дефицит (-)/Профицит (+)"}

type fedbudMesyatsStat struct {
	util.ClickHouseImport
}

// fedbudMesyats — консолидированный бюджет: таблица, DDL и разбор листа «месяц»
// (накопление переводится в месячные разности). Каталог рядов и витрину
// v_minfin_budget публикует Import() обёртки после полного цикла шаблона.
// Регистрация — реестром minfin/importers.go (стандарт §8).
var fedbudMesyats = util.ClickHouseImport{
	TableName: fedbudMesyatsTable,
	CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (
              name LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);`},
	ImportFunc: fedbudMesyatsImport,
}

// fedbudMesyatsImporter — консолидированный бюджет; адрес файла вычисляется в
// Import() через util.GetPage/FindLink (стандарт §4.3: сеть — внутри Import,
// ссылка — со страницы источника).
var fedbudMesyatsImporter = &fedbudMesyatsStat{ClickHouseImport: fedbudMesyats}

func getFedbudMesyatDataUrl() (string, error) {
	page, err := util.GetPage(minfinUrl + conbudExecutePath)
	if err != nil {
		return "", fmt.Errorf("minfin_fed_bud_mesyats: страница %s: %w", minfinUrl+conbudExecutePath, err)
	}
	href, err := util.FindLink(page, fedbudMesyatsLinkSelector)
	if err != nil {
		return "", fmt.Errorf("minfin_fed_bud_mesyats: не найдена ссылка на *_mesyats.xlsx на %s: %w", minfinUrl+conbudExecutePath, err)
	}
	return minfinUrl + href, nil
}

func (s *fedbudMesyatsStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if s.DataUrl, err = getFedbudMesyatDataUrl(); err != nil {
		return 0, err
	}
	if count, err = s.ClickHouseImport.Import(ctx, conn); err != nil {
		return count, err
	}
	return count, publishFedbud(ctx, conn, fedbudMesyatsSeriesMeta)
}

func fedbudMesyatsImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows("месяц", excelize.Options{RawCellValue: true})
	if err != nil {
		return fmt.Errorf("%s: лист «месяц»: %w", fedbudMesyatsTable, err)
	}
	tableIsFoundRowNum := -1
	var valuePrev, value float64
	for i, row := range rows {
		if len(row) < 3 || strings.TrimSpace(row[1]) == "" {
			continue
		}
		if strings.TrimSpace(row[1]) == "Показатель" {
			tableIsFoundRowNum = i
			continue
		}
		if tableIsFoundRowNum < 0 {
			continue
		}
		// В выпуске 2026/10 строка называется «Акцизы ****»: звёздочки —
		// сноска источника. Снимаем хвостовые * и пробелы, иначе ряд молча
		// пропускается, а в таблице остаются старые точки.
		label := strings.TrimRight(strings.TrimSpace(row[1]), "* ")
		if !slices.Contains(fedbudMesyatsFields, label) {
			continue
		}
		for j, rowCol := range row[2:] {
			col := j + 2
			if col >= len(rows[tableIsFoundRowNum]) {
				return fmt.Errorf("%s: строка %q: в шапке нет колонки %d", fedbudMesyatsTable, label, col)
			}
			dateStr := strings.TrimSpace(rows[tableIsFoundRowNum][col])
			date, err := fedbudMesyatsDate(dateStr)
			if err != nil {
				return fmt.Errorf("%s: строка %q: дата %q: %w", fedbudMesyatsTable, label, dateStr, err)
			}
			valueNew, err := strconv.ParseFloat(strings.ReplaceAll(rowCol, ",", ""), 64)
			if err != nil {
				return fmt.Errorf("%s: строка %q: значение %q не число: %w", fedbudMesyatsTable, label, rowCol, err)
			}
			if date.Month() == time.January {
				value = valueNew
			} else {
				value = valueNew - valuePrev
			}
			valuePrev = valueNew
			if err = batch.Append(label, date, value); err != nil {
				return err
			}
		}
	}
	return nil
}

// fedbudMesyatsDate разбирает дату шапки. В raw-режиме excelize отдаёт
// сериал (число), в форматированном — строку «Jan-11» или «янв.23 ***».
// RawCellValue обязателен (§6.2): без него значения округляются до одного
// знака, и в таблицу попадает 1115,9 вместо 1115,85.
func fedbudMesyatsDate(raw string) (time.Time, error) {
	if serial, err := strconv.ParseFloat(strings.TrimSpace(raw), 64); err == nil && serial > 0 {
		t, err := excelize.ExcelDateToTime(serial, false)
		if err != nil {
			return time.Time{}, err
		}
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC), nil
	}
	dateStr := raw
	if dateArr := strings.Split(dateStr, " "); len(dateArr) > 1 {
		dateStr = dateReplacer.Replace(dateArr[0])
	}
	t, err := time.Parse("Jan-06", dateStr)
	if err != nil {
		return time.Time{}, err
	}
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC), nil
}
