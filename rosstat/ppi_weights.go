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
	// rosstatPPIWeightsTable — официальные веса ИЦП по видам деятельности.
	rosstatPPIWeightsTable = "rosstat_ppi_weights"

	// ppiWeightsDdl — DDL в %s-форме шаблона. Ключ включает weight_type:
	// у одного (year, code) два разных веса — domestic и domestic_export.
	ppiWeightsDdl = `CREATE TABLE IF NOT EXISTS %s (
      year UInt16
    , code LowCardinality(String)
    , name LowCardinality(String)
    , weight_type LowCardinality(String)
    , value Float64
) ENGINE = ReplacingMergeTree
ORDER BY (year, code, weight_type);
`

	// ppiWeightsPage — та же страница цен Росстата.
	ppiWeightsPage = "https://rosstat.gov.ru/statistics/price"
	// ppiWeightsLinkSelector — файл весов. Имя содержит горизонт (vesa_2006-2026),
	// при сдвиге года меняется, поэтому ссылка берётся со страницы.
	ppiWeightsLinkSelector = `a[href*="vesa_"]`
	// ppiWeightsCountPoints — количественный чек версии: веса обновляются раз в
	// год, шаг недельный, повтор — HEAD-префлайт и no-op.
	ppiWeightsCountPoints = "SELECT count() FROM rosstat_ppi_weights FINAL"

	// ppiWeightsCatalogQuery — каталог из фактически записанных весов;
	// argMax(name, year) даёт название из последнего года.
	ppiWeightsCatalogQuery = `SELECT code, weight_type, argMax(name, year) AS name
FROM rosstat_ppi_weights FINAL
GROUP BY code, weight_type
ORDER BY code, weight_type`
)

// ppiWeightCatalogRow — строка запроса каталога: код ВЭД, база и последнее
// известное название.
type ppiWeightCatalogRow struct {
	code       string
	weightType string
	name       string
}

// ppiWeightsDataURL берёт адрес актуальной книги весов со страницы цен.
func ppiWeightsDataURL() (string, error) {
	page, err := util.GetPage(ppiWeightsPage)
	if err != nil {
		return "", fmt.Errorf("ppiWeights: %s: %w", ppiWeightsPage, err)
	}
	href, err := util.FindLink(page, ppiWeightsLinkSelector)
	if err != nil {
		return "", fmt.Errorf("ppiWeights: ссылка на vesa_*.xlsx не найдена на %s: %w", ppiWeightsPage, err)
	}
	if strings.HasPrefix(href, "http://") || strings.HasPrefix(href, "https://") {
		return href, nil
	}
	return rosstatUrl + href, nil
}

// PPIWeightsStat — импортёр весов ИЦП; регистрируется реестром rosstat/importers.go.
type PPIWeightsStat struct {
	util.ClickHouseImport
}

// Import берёт адрес книги весов со страницы цен. Режим восстановления явно
// отклоняется: DataUrl вычисляется со страницы, версия архива привязана к
// basename файла с горизонтом (прецедент rosstat_ppi_ved).
func (s *PPIWeightsStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if util.ArchiveRestoreMode() {
		return 0, fmt.Errorf("%s: режим восстановления (%s=%s) не поддержан: DataUrl вычисляется со страницы, версия архива привязана к файлу с горизонтом",
			rosstatPPIWeightsTable, util.EnvArchiveAction, util.ArchiveActionRestore)
	}
	dataURL, err := ppiWeightsDataURL()
	if err != nil {
		return 0, err
	}
	s.DataUrl = dataURL
	return s.ClickHouseImport.Import(ctx, conn)
}

// ppiWeightsImport разбирает книгу и пишет точки одним батчем.
func ppiWeightsImport(xlsx *excelize.File, batch driver.Batch) error {
	points, err := parsePPIWeights(xlsx)
	if err != nil {
		return err
	}
	for _, point := range points {
		if err = batch.Append(point.year, point.code, point.name, point.weightType, point.value); err != nil {
			return err
		}
	}
	return nil
}

// ppiWeightsCatalog публикует каталог весовых рядов. Витрина v_rosstat_ppi —
// T5 (#280); T2 публикует только каталог.
func ppiWeightsCatalog(ctx context.Context, conn driver.Conn) error {
	rows, err := conn.Query(ctx, ppiWeightsCatalogQuery)
	if err != nil {
		return fmt.Errorf("%s: %w", rosstatPPIWeightsTable, err)
	}
	defer func() { _ = rows.Close() }()

	var catalog []ppiWeightCatalogRow
	for rows.Next() {
		var row ppiWeightCatalogRow
		if err = rows.Scan(&row.code, &row.weightType, &row.name); err != nil {
			return fmt.Errorf("%s: %w", rosstatPPIWeightsTable, err)
		}
		catalog = append(catalog, row)
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("%s: %w", rosstatPPIWeightsTable, err)
	}
	return util.UpsertSeriesCatalog(ctx, conn, ppiWeightsSeriesMeta(catalog))
}

func ppiWeightsAfterImport(ctx context.Context, conn driver.Conn) error {
	return ppiWeightsCatalog(ctx, conn)
}

// ppiWeightsImporter — веса ИЦП Росстата по видам экономической деятельности.
var ppiWeightsImporter = &PPIWeightsStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              rosstatPPIWeightsTable,
	CreateTable:            []string{ppiWeightsDdl},
	ImportFunc:             ppiWeightsImport,
	AfterImport:            ppiWeightsAfterImport,
	PublicationCountPoints: ppiWeightsCountPoints,
}}
