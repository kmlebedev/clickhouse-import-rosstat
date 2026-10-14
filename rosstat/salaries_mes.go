package rosstat

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

const (
	// Рынок труда, занятость и заработная плата https://rosstat.gov.ru/labor_market_employment_salaries
	// Среднемесячная номинальная начисленная заработная плата работников в целом по экономике Российской Федерации
	// Имя файла содержит месяц и год (tab1-zpl_ММ-ГГГГ.xlsx) и меняется при обновлении,
	// поэтому ссылка берётся со страницы.
	rosstatSalariesMesTable = "rosstat_salaries_mes"
	salariesMesDdl          = `CREATE TABLE IF NOT EXISTS %s (
				  name LowCardinality(String)
				, date Date
				, salary Float32
			) ENGINE = ReplacingMergeTree ORDER BY (name, date);
		`
	salariesMesField      = "1991"
	salariesMesYearStart  = 1991
	salariesMesTimeLayout = "2006-01"
	// salariesMesLinkSelector — селектор ссылки на актуальный xlsx: константа рядом с
	// импортёром, а не литерал в теле функции (§4.5) — тот же селектор проверяет тест на
	// HTML-фикстуре.
	salariesMesLinkSelector = `a[href*="tab1-zpl"]`
	// salariesMesMonthColumn — индекс первой колонки месяцев. Лист «Лист1» устроен так:
	// A — год, B — «В среднем за год», C–F — «В среднем за кварталы», G…R — январь…декабрь,
	// поэтому row[6:] — ровно 12 месяцев, а строка короче колонки января (в выгрузке это
	// «… данных не имеется») обрывает таблицу листа.
	salariesMesMonthColumn = 6
	// salariesMesSeriesName — стабильное имя ряда, объявленное кодом (§6.8): заголовок
	// источника («Среднемесячная номинальная начисленная заработная плата работников по
	// полному кругу организаций», A1; годы выгрузки — в A2) меняется при обновлении файла,
	// и ряд из заголовка оборвался бы на первой же редакции. Значение — сегодняшний
	// заголовок листа «Лист1»; в каталоге ряд записан литералом независимо (series_meta.go),
	// а совпадение двух записей проверяют тесты.
	salariesMesSeriesName = "Среднемесячная номинальная начисленная заработная плата работников по полному кругу организаций"
	// salariesMesCountPoints — количественный чек версии (спека §3.5): число точек своего
	// ряда. Литерал, как и таблица: запрос — часть утверждения «эти точки мои». FINAL
	// обязателен — иначе count() считал бы несмёрженные части.
	salariesMesCountPoints = "SELECT count() FROM rosstat_salaries_mes FINAL"
)

// getSalariesMesXlsDataUrl возвращает адрес актуального tab1-zpl_*.xlsx со страницы рынка
// труда Росстата: имя файла содержит месяц и год (tab1-zpl_ММ-ГГГГ.xlsx) и меняется при
// обновлении, поэтому ссылка берётся со страницы. Транспорт — через util (§4.1), а ошибка
// не проглатывается (§4.4): без ссылки импорт обязан упасть до записи в БД, а не писать
// «0 строк» по устаревшему адресу.
func getSalariesMesXlsDataUrl() (string, error) {
	pageUrl := fmt.Sprintf("%s/labor_market_employment_salaries", rosstatUrl)
	page, err := util.GetPage(pageUrl)
	if err != nil {
		return "", fmt.Errorf("salariesMes: %s: %w", pageUrl, err)
	}
	href, err := util.FindLink(page, salariesMesLinkSelector)
	if err != nil {
		return "", fmt.Errorf("salariesMes: ссылка на tab1-zpl_*.xlsx не найдена на %s: %w", pageUrl, err)
	}
	return rosstatUrl + href, nil
}

type SalariesMesStat struct {
	util.ClickHouseImport
}

// salariesMesPoint — точка ряда: значение месяца и дата её записи — 1-е число месяца,
// следующего за отчётным (AddDate(0, 1, 0)).
type salariesMesPoint struct {
	date  time.Time
	value float64
}

// salariesMesValue разбирает ячейку месяца. point == false — ячейка точкой не является:
// пустая ячейка и литеральный 0 в этом источнике означают «данных не имеется» (§6.7; нули
// января–июня 1991 квартальные колонки той же строки помечают «…»). Значение, помеченное
// сноской источника («62 470(2)» — октябрь–декабрь 2022), читается точкой: это
// опубликованное значение того же ряда, где пробел — разделитель тысяч, а «(2)» — номер
// сноски о покрытии (без учёта статистической информации по ДНР, ЛНР, Запорожской и
// Херсонской областям), а не другой показатель. Пропуск такой ячейки оставил бы дыру в три
// месяца головного ряда. Любой другой текст — ошибка: «значение не число там, где ожидалось
// число» (§6.4).
func salariesMesValue(cell string) (value float64, point bool, err error) {
	cell = strings.TrimSpace(cell)
	if cell == "" {
		return 0, false, nil
	}
	value, err = strconv.ParseFloat(cell, 64)
	if err == nil {
		return value, value != 0, nil
	}
	footnote, ok := salariesMesFootnoteValue(cell)
	if !ok {
		return 0, false, fmt.Errorf("значение ячейки %q не число: %w", cell, err)
	}
	return footnote, footnote != 0, nil
}

// salariesMesFootnoteValue разбирает ячейку, у которой значение записано текстом со сноской
// источника: «62 470(2)». Разделитель тысяч — пробел (в том числе неразрывный), сноска —
// «(N)» в конце ячейки.
func salariesMesFootnoteValue(cell string) (float64, bool) {
	i := strings.LastIndex(cell, "(")
	if i < 1 || !strings.HasSuffix(cell, ")") {
		return 0, false
	}
	value, err := strconv.ParseFloat(strings.Join(strings.Fields(cell[:i]), ""), 64)
	return value, err == nil
}

// parseSalariesMes читает листы книги и отдаёт точки ряда. Ячейки берутся сырыми
// значениями (§6.2): числовой формат выгрузки — `#,##0`, и форматированная строка дала бы
// «1,074» вместо 1074 (а в локали с запятой — и ошибку разбора; на ней прежний разбор и
// падал). Имя ряда — константа (§6.8), заголовок листа не читается вовсе.
func parseSalariesMes(xlsx *excelize.File) ([]salariesMesPoint, error) {
	var points []salariesMesPoint
	for _, sheet := range xlsx.GetSheetList() {
		rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
		if err != nil {
			return nil, fmt.Errorf("%s: лист %q: %w", rosstatSalariesMesTable, sheet, err)
		}
		// Якорь — строка первого года в первой колонке; год остальных строк считается от
		// него смещением, а не полем каждой строки (§6.3). Год у поздних лет несёт сноску
		// («2026(2)»), поэтому строку не сверяют с годом целиком — только якорь.
		anchor := -1
		for i, row := range rows {
			if len(row) == 0 {
				continue
			}
			if row[0] == salariesMesField {
				anchor = i
			}
			if anchor < 0 {
				continue
			}
			// «… данных не имеется» и сноски внизу листа короче колонки января: таблица кончилась.
			if len(row) <= salariesMesMonthColumn {
				break
			}
			year := salariesMesYearStart + i - anchor
			for mes, cell := range row[salariesMesMonthColumn:] {
				value, point, err := salariesMesValue(cell)
				if err != nil {
					return nil, fmt.Errorf("%s: %d-%02d: %w", rosstatSalariesMesTable, year, mes+1, err)
				}
				if !point {
					continue
				}
				month, err := time.Parse(salariesMesTimeLayout, fmt.Sprintf("%d-%02d", year, mes+1))
				if err != nil {
					return nil, fmt.Errorf("%s: %d-%02d: %w", rosstatSalariesMesTable, year, mes+1, err)
				}
				points = append(points, salariesMesPoint{date: month.AddDate(0, 1, 0), value: value})
			}
		}
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: разбор дал 0 точек — якорь %q не найден или выгрузка пуста", rosstatSalariesMesTable, salariesMesField)
	}
	return points, nil
}

// salariesMesImport разбирает файл и пишет точки в batch. Семантика прежних export() и цикла
// вставки Import сохранена: имя ряда — константа salariesMesSeriesName (§6.8), дата «год-месяц»
// строки сдвинута на 1-е число следующего месяца, значение — зарплата месяца. Повторный
// ParseFloat убран: разбор отдаёт уже разобранные значения.
func salariesMesImport(xlsx *excelize.File, batch driver.Batch) error {
	points, err := parseSalariesMes(xlsx)
	if err != nil {
		return err
	}
	for _, point := range points {
		if err = batch.Append(salariesMesSeriesName, point.date, float32(point.value)); err != nil {
			return err
		}
	}
	return nil
}

// Import берёт адрес актуального xlsx со страницы рынка труда Росстата: имя файла содержит
// месяц и год (tab1-zpl_ММ-ГГГГ.xlsx) и меняется при обновлении, а сетевое обращение в
// init() запрещено (ARCHITECTURE.md §8.6). Дальше — полный цикл шаблона: DDL → префлайт по
// HEAD (файл обновляется раз в месяц, шаг расписания — недельный) → точки → производные
// (AfterImport) → строка версии.
func (s *SalariesMesStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	xlsDataUrl, err := getSalariesMesXlsDataUrl()
	if err != nil {
		return count, err
	}
	s.DataUrl = xlsDataUrl
	return s.ClickHouseImport.Import(ctx, conn)
}

// salariesMesImporter — среднемесячная начисленная зарплата по РФ; регистрируется реестром rosstat/importers.go.
var salariesMesImporter = &SalariesMesStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              rosstatSalariesMesTable,
	CreateTable:            []string{salariesMesDdl},
	ImportFunc:             salariesMesImport,
	AfterImport:            afterRosstatMacro(salariesMesSeriesMeta),
	PublicationCountPoints: salariesMesCountPoints,
}}
