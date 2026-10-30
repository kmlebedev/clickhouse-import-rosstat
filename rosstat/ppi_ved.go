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
	// rosstatPPIVedTable — ИЦП Росстата по ВЭД, совокупный ряд «Промышленность».
	rosstatPPIVedTable = "rosstat_ppi_ved"

	// ppiVedDdl — DDL в %s-форме шаблона. Дата — последний день отчётного месяца;
	// ключ (date, code, index_type) перезаписывает ревизию месяца.
	ppiVedDdl = `CREATE TABLE IF NOT EXISTS %s (
      date Date
    , code LowCardinality(String)
    , name LowCardinality(String)
    , index_type LowCardinality(String)
    , value Float64
) ENGINE = ReplacingMergeTree
ORDER BY (date, code, index_type);
`

	// ppiVedPage — страница цен Росстата. Ссылка на файл берётся с неё, а не собирается:
	// имя файла содержит месяц и год, адрес хранения Росстат уже менял.
	ppiVedPage = "https://rosstat.gov.ru/statistics/price"
	// ppiVedLinkSelector — месячная выгрузка. На странице лежат две ссылки:
	// Proizvoditeli_Ind_VED_08-2026.xlsx и Proizvoditeli_Ind_VED_2kv-2026.xlsx;
	// :not отсекает квартальную, иначе FindLink вернул бы первый документ по порядку.
	ppiVedLinkSelector = `a[href*="Proizvoditeli_Ind_VED_"]:not([href*="2kv-"])`
	// ppiVedCountPoints — количественный чек версии (§9.1): число точек всех четырёх
	// типов индексов. Файл обновляется раз в месяц, шаг недельный, поэтому повтор —
	// HEAD-префлайт и no-op без скачивания.
	ppiVedCountPoints = "SELECT count() FROM rosstat_ppi_ved FINAL"
)

// ppiVedDataURL берёт адрес актуальной книги со страницы цен. Транспорт — через util;
// без ссылки импорт обязан упасть до записи в БД, а не писать «0 строк».
func ppiVedDataURL() (string, error) {
	page, err := util.GetPage(ppiVedPage)
	if err != nil {
		return "", fmt.Errorf("ppiVed: %s: %w", ppiVedPage, err)
	}
	href, err := util.FindLink(page, ppiVedLinkSelector)
	if err != nil {
		return "", fmt.Errorf("ppiVed: ссылка на Proizvoditeli_Ind_VED_*.xlsx не найдена на %s: %w", ppiVedPage, err)
	}
	if strings.HasPrefix(href, "http://") || strings.HasPrefix(href, "https://") {
		return href, nil
	}
	return rosstatUrl + href, nil
}

// PPIVedStat — импортёр ИЦП по ВЭД; регистрируется реестром rosstat/importers.go.
type PPIVedStat struct {
	util.ClickHouseImport
}

// Import берёт адрес книги со страницы цен: сетевое обращение в init() запрещено.
// Дальше — полный цикл шаблона: DDL → префлайт → точки → каталог → строка версии.
func (s *PPIVedStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if util.ArchiveRestoreMode() {
		// URL содержит месяц и меняется каждый выпуск, поэтому версия в архиве привязана
		// к конкретному месячному адресу. Восстановление по текущей странице вернуло бы
		// только последний выпуск (а при недоступном источнике не вернуло бы ничего);
		// явный отказ лучше молчаливого «restore не нашёл версию» — прецедент okved2.
		return 0, fmt.Errorf("%s: режим восстановления (%s=%s) не поддержан: DataUrl вычисляется со страницы, версия архива привязана к месячному URL",
			rosstatPPIVedTable, util.EnvArchiveAction, util.ArchiveActionRestore)
	}
	dataURL, err := ppiVedDataURL()
	if err != nil {
		return 0, err
	}
	s.DataUrl = dataURL
	return s.ClickHouseImport.Import(ctx, conn)
}

// ppiVedImport разбирает книгу и пишет точки одним батчем. Значение — float64:
// колонка value объявлена Float64, и float32 потерял бы точность.
func ppiVedImport(xlsx *excelize.File, batch driver.Batch) error {
	points, err := parsePPIVed(xlsx)
	if err != nil {
		return err
	}
	for _, point := range points {
		if err = batch.Append(point.date, point.code, point.name, point.indexType, point.value); err != nil {
			return err
		}
	}
	return nil
}

// ppiVedAfterImport публикует только каталог: общая витрина v_rosstat_ppi строится
// в T5 (#280) над rosstat_ppi_ved, rosstat_ppi_product, rosstat_ppi_weights и ppi_sa,
// которых в T1 ещё нет. Отступление от §10.2 стандарта названо в спеке §6.1.
func ppiVedAfterImport(ctx context.Context, conn driver.Conn) error {
	return util.UpsertSeriesCatalog(ctx, conn, ppiVedSeriesMeta)
}

// ppiVedImporter — ИЦП Росстата по ВЭД, совокупный ряд «Промышленность».
var ppiVedImporter = &PPIVedStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              rosstatPPIVedTable,
	CreateTable:            []string{ppiVedDdl},
	ImportFunc:             ppiVedImport,
	AfterImport:            ppiVedAfterImport,
	PublicationCountPoints: ppiVedCountPoints,
}}
