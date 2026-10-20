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
	// rosstatPPIProductTable — ИЦП Росстата по товарам и товарным группам (ОКПД2).
	rosstatPPIProductTable = "rosstat_ppi_product"

	// ppiProductDdl — DDL в %s-форме шаблона. Дата — последний день отчётного
	// месяца; ключ (date, okpd2, index_type) перезаписывает ревизию месяца.
	ppiProductDdl = `CREATE TABLE IF NOT EXISTS %s (
      date Date
    , okpd2 LowCardinality(String)
    , name LowCardinality(String)
    , index_type LowCardinality(String)
    , value Float64
) ENGINE = ReplacingMergeTree
ORDER BY (date, okpd2, index_type);
`

	// ppiProductPage — страница цен Росстата. Ссылка на книгу берётся с неё:
	// имя файла содержит месяц и меняется каждый выпуск.
	ppiProductPage = "https://rosstat.gov.ru/statistics/price"
	// ppiProductLinkSelector — месячная выгрузка товаров. Квартальной пары у
	// этого файла нет, поэтому :not не нужен.
	ppiProductLinkSelector = `a[href*="Proizvoditeli_Ind_tov_"]`
	// ppiProductCountPoints — количественный чек версии: файл месячный, шаг
	// недельный, повтор — HEAD-префлайт и no-op без скачивания.
	ppiProductCountPoints = "SELECT count() FROM rosstat_ppi_product FINAL"

	// ppiProductCatalogQuery — каталог строится из фактически записанных точек:
	// argMax(name, date) даёт название из последнего выпуска, в котором товар
	// встречался. Так каталог переживает переименования кодов.
	ppiProductCatalogQuery = `SELECT okpd2, index_type, argMax(name, date) AS name
FROM rosstat_ppi_product FINAL
GROUP BY okpd2, index_type
ORDER BY okpd2, index_type`
)

// ppiProductCatalogRow — строка запроса каталога: код, тип индекса и последнее
// известное название товара.
type ppiProductCatalogRow struct {
	okpd2     string
	indexType string
	name      string
}

// ppiProductDataURL берёт адрес актуальной книги товаров со страницы цен.
// Транспорт — через util; без ссылки импорт обязан упасть до записи в БД.
func ppiProductDataURL() (string, error) {
	page, err := util.GetPage(ppiProductPage)
	if err != nil {
		return "", fmt.Errorf("ppiProduct: %s: %w", ppiProductPage, err)
	}
	href, err := util.FindLink(page, ppiProductLinkSelector)
	if err != nil {
		return "", fmt.Errorf("ppiProduct: ссылка на Proizvoditeli_Ind_tov_*.xlsx не найдена на %s: %w", ppiProductPage, err)
	}
	if strings.HasPrefix(href, "http://") || strings.HasPrefix(href, "https://") {
		return href, nil
	}
	return rosstatUrl + href, nil
}

// PPIProductStat — импортёр ИЦП по товарам/ОКПД2; регистрируется реестром
// rosstat/importers.go.
type PPIProductStat struct {
	util.ClickHouseImport
}

// Import берёт адрес книги со страницы цен: сетевое обращение в init() запрещено.
// Дальше — полный цикл шаблона: DDL → префлайт → точки → каталог → строка версии.
func (s *PPIProductStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if util.ArchiveRestoreMode() {
		// URL содержит месяц и меняется каждый выпуск, поэтому версия в архиве
		// привязана к конкретному месячному адресу; восстановление по текущей
		// странице вернуло бы только последний выпуск. Явный отказ лучше
		// молчаливого «restore не нашёл версию» (прецедент rosstat_ppi_ved).
		return 0, fmt.Errorf("%s: режим восстановления (%s=%s) не поддержан: DataUrl вычисляется со страницы, версия архива привязана к месячному URL",
			rosstatPPIProductTable, util.EnvArchiveAction, util.ArchiveActionRestore)
	}
	dataURL, err := ppiProductDataURL()
	if err != nil {
		return 0, err
	}
	s.DataUrl = dataURL
	return s.ClickHouseImport.Import(ctx, conn)
}

// ppiProductImport разбирает книгу и пишет точки одним батчем.
func ppiProductImport(xlsx *excelize.File, batch driver.Batch) error {
	points, err := parsePPIProduct(xlsx)
	if err != nil {
		return err
	}
	return appendPPIProduct(points, batch)
}

// appendPPIProduct — запись точек в батч. Вынесена из ppiProductImport, чтобы
// тест на урезанной фикстуре проверял тот же контракт колонок, не проходя
// проверку непрерывности покрытия, которой в фикстуре нет.
func appendPPIProduct(points []ppiProductPoint, batch driver.Batch) error {
	for _, point := range points {
		if err := batch.Append(point.date, point.okpd2, point.name, point.indexType, point.value); err != nil {
			return err
		}
	}
	return nil
}

// ppiProductCatalog публикует каталог товарных рядов. Витрина v_rosstat_ppi
// строится в T5 (#280); T2, как и T1, публикует только каталог.
func ppiProductCatalog(ctx context.Context, conn driver.Conn) error {
	rows, err := conn.Query(ctx, ppiProductCatalogQuery)
	if err != nil {
		return fmt.Errorf("%s: %w", rosstatPPIProductTable, err)
	}
	defer func() { _ = rows.Close() }()

	var catalog []ppiProductCatalogRow
	for rows.Next() {
		var row ppiProductCatalogRow
		if err = rows.Scan(&row.okpd2, &row.indexType, &row.name); err != nil {
			return fmt.Errorf("%s: %w", rosstatPPIProductTable, err)
		}
		catalog = append(catalog, row)
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("%s: %w", rosstatPPIProductTable, err)
	}
	return util.UpsertSeriesCatalog(ctx, conn, ppiProductSeriesMeta(catalog))
}

func ppiProductAfterImport(ctx context.Context, conn driver.Conn) error {
	return ppiProductCatalog(ctx, conn)
}

// ppiProductImporter — ИЦП Росстата по товарам и товарным группам (ОКПД2).
var ppiProductImporter = &PPIProductStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              rosstatPPIProductTable,
	CreateTable:            []string{ppiProductDdl},
	ImportFunc:             ppiProductImport,
	AfterImport:            ppiProductAfterImport,
	PublicationCountPoints: ppiProductCountPoints,
}}
