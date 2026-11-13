package rosstat

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

const (
	// rosstatCPISPRTable — месячная публикация Росстата «Индексы потребительских цен
	// и структура потребительских расходов по РФ (с 2025 г.)»: структура расходов
	// (веса ИПЦ) и индексы по каждой позиции за месяц.
	rosstatCPISPRTable = "rosstat_cpi_spr"

	// cpiSPRDdl — DDL в %s-форме шаблона. Ключ включает месяц: публикация кумулятивная
	// (каждый выпуск повторяет историю с 2025-01), и структура внутри года может
	// меняться — месячные снимки хранятся отдельно.
	cpiSPRDdl = `CREATE TABLE IF NOT EXISTS %s (
      year       UInt16
    , month      UInt8
    , code       LowCardinality(String)
    , name       LowCardinality(String)
    , weight     Float64
    , index_prev Nullable(Float64)
    , index_ytd  Nullable(Float64)
    , index_yoy  Nullable(Float64)
) ENGINE = ReplacingMergeTree
ORDER BY (year, month, code);
`

	// cpiSPRPage — страница цен Росстата: на ней лежит месячная публикация.
	cpiSPRPage = "https://rosstat.gov.ru/statistics/price"
	// cpiSPRLinkSelector — имя файла несёт месяц и год (ipc_spr_09-2026.xlsx), поэтому
	// адрес берётся со страницы. Флаг i — регистр имён Росстат меняет между релизами
	// (у книг весов это уже наблюдалось: CPR_Tov_RF_… / CPR_tov_RF_…).
	cpiSPRLinkSelector = `a[href*="ipc_spr_" i]`

	// cpiSPRCountPoints — количественный чек версии: число точек всей таблицы.
	// Файл месячный, шаг недельный, поэтому повтор того же выпуска — HEAD-префлайт
	// и no-op без скачивания.
	cpiSPRCountPoints = "SELECT count() FROM rosstat_cpi_spr FINAL"

	// cpiSPRCatalogQuery — каталог из записанных точек; argMax по (year, month) даёт
	// название из последнего месяца.
	cpiSPRCatalogQuery = `SELECT code, argMax(name, (year, month)) AS name, min(year) AS first_year, max(year) AS last_year
FROM rosstat_cpi_spr FINAL
GROUP BY code
ORDER BY code`
)

// cpiSPRCatalogRow — строка запроса каталога.
type cpiSPRCatalogRow struct {
	code      string
	name      string
	firstYear uint16
	lastYear  uint16
}

// cpiSPRDataURL берёт адрес актуального выпуска со страницы цен. Без ссылки импорт
// обязан упасть до записи, а не писать «0 строк».
func cpiSPRDataURL() (string, error) {
	page, err := util.GetPage(cpiSPRPage)
	if err != nil {
		return "", fmt.Errorf("cpiSPR: %s: %w", cpiSPRPage, err)
	}
	href, err := util.FindLink(page, cpiSPRLinkSelector)
	if err != nil {
		return "", fmt.Errorf("cpiSPR: ссылка ipc_spr_*.xlsx не найдена на %s: %w", cpiSPRPage, err)
	}
	if strings.HasPrefix(href, "http://") || strings.HasPrefix(href, "https://") {
		return href, nil
	}
	return rosstatUrl + href, nil
}

// CPISprStat — импортёр структуры расходов и индексов ИПЦ по месяцам.
type CPISprStat struct {
	util.ClickHouseImport
}

// Import берёт адрес выпуска со страницы цен (сетевое обращение в init() запрещено).
// Режим восстановления отклоняется: версия архива привязана к месячному URL.
func (s *CPISprStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if util.ArchiveRestoreMode() {
		return 0, fmt.Errorf("%s: режим восстановления (%s=%s) не поддержан: DataUrl вычисляется со страницы, версия архива привязана к месячному URL",
			rosstatCPISPRTable, util.EnvArchiveAction, util.ArchiveActionRestore)
	}
	dataURL, err := cpiSPRDataURL()
	if err != nil {
		return 0, err
	}
	s.DataUrl = dataURL
	return s.ClickHouseImport.Import(ctx, conn)
}

// cpiSPRImport разбирает книгу и пишет точки одним батчем. Индексы — указатели:
// у 9–11 строк на лист индекса нет, и NULL не должен превращаться в 0.
func cpiSPRImport(xlsx *excelize.File, batch driver.Batch) error {
	points, err := parseCPISPR(xlsx)
	if err != nil {
		return err
	}
	for _, point := range points {
		if err = batch.Append(point.year, point.month, point.code, point.name, point.weight,
			cpiSPRNullable(point.indexPrev), cpiSPRNullable(point.indexYTD), cpiSPRNullable(point.indexYOY)); err != nil {
			return err
		}
	}
	return nil
}

// cpiSPRNullable — значение индекса для Nullable(Float64): nil остаётся NULL.
func cpiSPRNullable(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

// cpiSPRCatalog публикует каталог рядов: четыре метрики на код.
func cpiSPRCatalog(ctx context.Context, conn driver.Conn) error {
	rows, err := conn.Query(ctx, cpiSPRCatalogQuery)
	if err != nil {
		return fmt.Errorf("%s: %w", rosstatCPISPRTable, err)
	}
	defer func() { _ = rows.Close() }()

	var catalog []cpiSPRCatalogRow
	for rows.Next() {
		var row cpiSPRCatalogRow
		if err = rows.Scan(&row.code, &row.name, &row.firstYear, &row.lastYear); err != nil {
			return fmt.Errorf("%s: %w", rosstatCPISPRTable, err)
		}
		catalog = append(catalog, row)
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("%s: %w", rosstatCPISPRTable, err)
	}
	return util.UpsertSeriesCatalog(ctx, conn, cpiSPRSeriesMeta(catalog))
}

func cpiSPRAfterImport(ctx context.Context, conn driver.Conn) error {
	if err := cpiSPRCatalog(ctx, conn); err != nil {
		return err
	}
	_, err := util.CreateView(ctx, conn, cpiSPRView)
	return err
}

// cpiSPRView — витрина месячной публикации: структура расходов и индексы по позициям.
var cpiSPRView = util.View{
	Name:   "v_rosstat_cpi_spr",
	Tables: []string{rosstatCPISPRTable},
	Select: `SELECT year, month, code, name, weight, index_prev, index_ytd, index_yoy
FROM rosstat_cpi_spr FINAL`,
	Comment: "Месячная публикация Росстата «Индексы потребительских цен и структура потребительских расходов по РФ (с 2025 г.)» (XLSX ipc_spr_MM-YYYY.xlsx со страницы rosstat.gov.ru/statistics/price): по каждой позиции — структура потребительских расходов (вес ИПЦ, % к итогу) и три индекса потребительских цен. Ключ — (год, месяц, код): публикация кумулятивная (каждый выпуск повторяет историю с 2025-01), структура внутри года может меняться, поэтому месячные снимки хранятся отдельно. В файле все уровни (товары, группы, агрегаты), сумма весов листа ~987–989, а не 100. Лист января — годовая структура (для 2026 сверено с книгой групп 2026: 742/742 кода). Месяц и год берутся из заголовка листа («… за сентябрь 2025 г.»), потому что лист 09.2025 назван 0922025.",
	Columns: map[string]string{
		"year":       "Год месяца публикации (из заголовка листа)",
		"month":      "Месяц публикации, 1–12 (из заголовка листа)",
		"code":       "Локальный код позиции Росстата: товары/услуги-представители (например, 111) или группы и агрегаты (2, 6, 9000, 930.АГ)",
		"name":       "Наименование позиции из источника на этот месяц",
		"weight":     "Структура потребительских расходов, % к итогу (вес ИПЦ); в файле все уровни, поэтому сумма листа ~987–989, не 100",
		"index_prev": "Индекс потребительских цен, % к предыдущему месяцу (100 = без изменений); NULL — индекс не публиковался",
		"index_ytd":  "Индекс потребительских цен, % к декабрю предыдущего года (100 = без изменений); NULL — индекс не публиковался",
		"index_yoy":  "Индекс потребительских цен, % к соответствующему месяцу предыдущего года (100 = без изменений); NULL — индекс не публиковался",
	},
}

// cpiSPRMetric — метрика каталога: суффикс ряда и единица.
type cpiSPRMetric struct {
	key, unit, title string
}

var cpiSPRMetrics = []cpiSPRMetric{
	{"weight", "% (структура потребительских расходов, вес ИПЦ)", "вес ИПЦ"},
	{"index_prev", "%, индекс потребительских цен к предыдущему месяцу", "ИПЦ к предыдущему месяцу"},
	{"index_ytd", "%, индекс потребительских цен к декабрю предыдущего года", "ИПЦ к декабрю предыдущего года"},
	{"index_yoy", "%, индекс потребительских цен к соответствующему месяцу предыдущего года", "ИПЦ к соответствующему месяцу предыдущего года"},
}

// cpiSPRSeriesMeta строит каталог: четыре ряда на позицию.
func cpiSPRSeriesMeta(rows []cpiSPRCatalogRow) []util.SeriesMeta {
	metas := make([]util.SeriesMeta, 0, len(rows)*len(cpiSPRMetrics))
	for _, row := range rows {
		for _, metric := range cpiSPRMetrics {
			metas = append(metas, util.SeriesMeta{
				Source:    rosstatSource,
				Series:    "cpi-spr:" + row.code + ":" + metric.key,
				Title:     fmt.Sprintf("%s — %s", row.name, metric.title),
				Unit:      metric.unit,
				Frequency: "M",
				Origin:    fmt.Sprintf("rosstat.gov.ru/statistics/price, файл ipc_spr_MM-YYYY.xlsx, лист = месяц, код %s", row.code),
				Description: fmt.Sprintf("Месячная публикация Росстата по позиции %s: %d–%d. "+
					"Структура потребительских расходов — вес ИПЦ в %% к итогу (в файле все уровни, сумма листа ~987–989); "+
					"индексы — проценты, 100 = без изменений, NULL — индекс не публиковался. Лист января — годовая структура: "+
					"для 2026 сверено с книгой групп 2026 (742 из 742 кодов). Ключ точки — (год, месяц, код).",
					row.code, row.firstYear, row.lastYear),
			})
		}
	}
	return metas
}

// cpiSprImporter — структура расходов и индексы ИПЦ Росстата по месяцам (с 2025-01).
var cpiSprImporter = &CPISprStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              rosstatCPISPRTable,
	CreateTable:            []string{cpiSPRDdl},
	ImportFunc:             cpiSPRImport,
	AfterImport:            cpiSPRAfterImport,
	PublicationCountPoints: cpiSPRCountPoints,
}}
