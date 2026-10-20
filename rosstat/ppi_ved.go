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

// ppiVedSeriesMeta — каталог четырёх рядов ИЦП по ВЭД. Имена совпадают с name в
// точках: они объявлены кодом (§6.8), а не взяты из подписи строки источника.
var ppiVedSeriesMeta = []util.SeriesMeta{
	{
		Source:    rosstatSource,
		Series:    ppiVedSeriesMM,
		Title:     "ИЦП по ВЭД, РФ, м/м",
		Unit:      "индекс, % к предыдущему месяцу (100 = без изменения)",
		Frequency: "M",
		Origin:    "rosstat.gov.ru/statistics/price, файл Proizvoditeli_Ind_VED_*.xlsx: листы 1.1 (1998–2016), 2.1 (2013–2025) и 3.1–8.1 (2021–2026), строка «Российская Федерация» / код 1323500.029.31",
		Description: "Индекс цен производителей промышленной продукции по РФ в целом за месяц: 100 — цены не изменились, 103,5 — рост на 3,5% за месяц. " +
			"Дата точки — последний день отчётного месяца. Ряд непрерывен с 1998-01: до 2013 г. берётся лист 1.1 (ОКВЭД-2007), с 2013 г. — 2.1 (ОКВЭД2), с 2021 г. — годовые листы 3.x–8.x; на перекрытиях приоритет у более новой классификации. " +
			"Публикация — 12–16-й рабочий день после отчётного месяца; в течение года возможны ревизии, повторная запись перезаписывает точку по ключу. " +
			"Пустая ячейка и «…»/«...» — пропуск источника, а не ноль.",
	},
	{
		Source:    rosstatSource,
		Series:    ppiVedSeriesYTD,
		Title:     "ИЦП по ВЭД, РФ, с начала года",
		Unit:      "индекс, % к декабрю предыдущего года (100 = без изменения)",
		Frequency: "M",
		Origin:    "rosstat.gov.ru/statistics/price, файл Proizvoditeli_Ind_VED_*.xlsx: листы 1.1/2.1 — строка «Декабрь в % к декабрю предыдущего года»; листы 3.2–8.2 — помесячно, строка «Российская Федерация» / код 1323500.029.31",
		Description: "Индекс цен производителей промышленной продукции по РФ накопленным итогом с начала года: 100 — цены не изменились с декабря предыдущего года, 107,9 — рост на 7,9% с начала года. " +
			"Дата точки — последний день отчётного месяца. Покрытие неоднородно по источнику: до 2021 г. есть только декабрьские значения (строка «Декабрь в % к декабрю предыдущего года»), с 2021-01 — помесячные значения листов 3.2–8.2. " +
			"Пустая ячейка и «…»/«...» — пропуск источника, а не ноль.",
	},
	{
		Source:    rosstatSource,
		Series:    ppiVedSeriesYY,
		Title:     "ИЦП по ВЭД, РФ, г/г",
		Unit:      "индекс, % к соответствующему месяцу предыдущего года (100 = без изменения)",
		Frequency: "M",
		Origin:    "rosstat.gov.ru/statistics/price, файл Proizvoditeli_Ind_VED_*.xlsx: листы 3.3–8.3, строка «Российская Федерация» / код 1323500.029.31",
		Description: "Индекс цен производителей промышленной продукции по РФ в % к соответствующему месяцу предыдущего года: 106,5 — рост на 6,5% год к году. " +
			"Дата точки — последний день отчётного месяца. Источник публикует этот разрез только с 2021 г.; более ранних годовых темпов в файле нет. " +
			"Пустая ячейка и «…»/«...» — пропуск источника, а не ноль.",
	},
	{
		Source:    rosstatSource,
		Series:    ppiVedSeriesPeriod,
		Title:     "ИЦП по ВЭД, РФ, период с начала года к прошлому году",
		Unit:      "индекс, % к соответствующему периоду предыдущего года (100 = без изменения)",
		Frequency: "M",
		Origin:    "rosstat.gov.ru/statistics/price, файл Proizvoditeli_Ind_VED_*.xlsx: листы 3.4–8.4, строка «Российская Федерация» / код 1323500.029.31",
		Description: "Индекс цен производителей промышленной продукции по РФ за период с начала года в % к соответствующему периоду предыдущего года: 103,2 — рост на 3,2% к тому же периоду прошлого года. " +
			"Дата точки — последний день отчётного месяца. Источник публикует этот разрез только с 2021 г. " +
			"Пустая ячейка и «…»/«...» — пропуск источника, а не ноль.",
	},
}
