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
	// rosstatCPIWeightsTable — публичные веса ИПЦ Росстата: товары (услуги)-представители
	// и группы. Одна строка на (год, вариант листа, файл, код).
	rosstatCPIWeightsTable = "rosstat_cpi_weights"

	// cpiWeightsDdl — DDL в %s-форме шаблона. variant входит в ключ: лист 2022
	// публикуется дважды («до 01.04» и «с 01.04»), без него точки затирали бы друг друга.
	cpiWeightsDdl = `CREATE TABLE IF NOT EXISTS %s (
      year        UInt16
    , variant     LowCardinality(String)
    , source_file LowCardinality(String)
    , level       UInt8
    , code        LowCardinality(String)
    , name        LowCardinality(String)
    , weight      Float64
) ENGINE = ReplacingMergeTree
ORDER BY (year, variant, source_file, code);
`

	// cpiWeightsPage — страница цен Росстата: на ней обе книги структуры расходов.
	cpiWeightsPage = "https://rosstat.gov.ru/statistics/price"
	// Селекторы ссылок: имена книг несут горизонт (2004–2026 / 2001–2026) и при
	// сдвиге года меняются, поэтому адреса берутся со страницы. Начало горизонта
	// (2004/2001) в селекторе — отделять полную книгу групп от годовой
	// CPR_Gr-Tov-RF_2026.xlsx: её ссылка стоит на странице раньше.
	cpiWeightsItemsSelector  = `a[href*="CPR_tov_RF_2004-"]`
	cpiWeightsGroupsSelector = `a[href*="CPR_Gr-Tov-RF_2001-"]`

	// Число точек версии — по файлу: у двух файлов разные версии, общий count()
	// сделал бы каждую известную версию «неполной» после записи второго файла.
	cpiWeightsCountItems  = "SELECT count() FROM rosstat_cpi_weights FINAL WHERE source_file = 'CPR_tov_RF'"
	cpiWeightsCountGroups = "SELECT count() FROM rosstat_cpi_weights FINAL WHERE source_file = 'CPR_Gr-Tov-RF'"
	// Объявление точки контроля для реестра и тестов: суммарный count() таблицы.
	cpiWeightsCountAll = "SELECT count() FROM rosstat_cpi_weights FINAL"

	// cpiWeightsCatalogQuery — каталог из фактически записанных весов; argMax(name, year)
	// даёт название из последнего года.
	// Строки группового файла 2016–2024 без кода получают синтетический ключ
	// name:<название>; это не код Росстата, поэтому в каталог они не входят.
	cpiWeightsCatalogQuery = `SELECT code, source_file, argMax(name, year) AS name, min(year) AS first_year, max(year) AS last_year
FROM rosstat_cpi_weights FINAL
WHERE NOT startsWith(code, 'name:')
GROUP BY code, source_file
ORDER BY code, source_file`
)

// cpiWeightCatalogRow — строка запроса каталога.
type cpiWeightCatalogRow struct {
	code      string
	source    string
	name      string
	firstYear uint16
	lastYear  uint16
}

// cpiWeightsLinks берёт со страницы цен обе ссылки на книги весов. Страница
// скачивается один раз: это ~0,9 МБ, а ссылки нужны обе.
func cpiWeightsLinks() (items, groups string, err error) {
	page, err := util.GetPage(cpiWeightsPage)
	if err != nil {
		return "", "", fmt.Errorf("cpiWeights: %s: %w", cpiWeightsPage, err)
	}
	resolve := func(selector string) (string, error) {
		href, linkErr := util.FindLink(page, selector)
		if linkErr != nil {
			return "", fmt.Errorf("cpiWeights: ссылка %s не найдена на %s: %w", selector, cpiWeightsPage, linkErr)
		}
		if strings.HasPrefix(href, "http://") || strings.HasPrefix(href, "https://") {
			return href, nil
		}
		return rosstatUrl + href, nil
	}
	if items, err = resolve(cpiWeightsItemsSelector); err != nil {
		return "", "", err
	}
	if groups, err = resolve(cpiWeightsGroupsSelector); err != nil {
		return "", "", err
	}
	return items, groups, nil
}

// CPIWeightsStat — импортёр весов ИПЦ; регистрируется реестром rosstat/importers.go.
type CPIWeightsStat struct {
	util.ClickHouseImport
}

// cpiWeightsImporterFile собирает шаблонный импортёр одного файла: шаблон даёт DDL,
// HEAD-префлайт, скачивание, архив версии и публикацию каталога/витрины, свой
// ImportFunc — разбор конкретной книги.
func cpiWeightsImporterFile(file, dataURL, countPoints string) *util.ClickHouseImport {
	return &util.ClickHouseImport{
		TableName:   rosstatCPIWeightsTable,
		CreateTable: []string{cpiWeightsDdl},
		DataUrl:     dataURL,
		ImportFunc: func(xlsx *excelize.File, batch driver.Batch) error {
			return cpiWeightsImport(xlsx, batch, file)
		},
		AfterImport:            cpiWeightsAfterImport,
		PublicationCountPoints: countPoints,
		PublicationSource:      rosstatCPIWeightsTable,
	}
}

// Import берёт адреса двух книг со страницы цен и импортирует их по очереди.
// Режим восстановления отклоняется: URL вычисляется со страницы, а версия архива
// привязана к имени файла с горизонтом (прецедент rosstat_ppi_weights).
func (s *CPIWeightsStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if util.ArchiveRestoreMode() {
		return 0, fmt.Errorf("%s: режим восстановления (%s=%s) не поддержан: DataUrl вычисляется со страницы, версия архива привязана к файлу с горизонтом",
			rosstatCPIWeightsTable, util.EnvArchiveAction, util.ArchiveActionRestore)
	}
	// DDL — до сети, как в шаблоне: если страница источника недоступна, таблица всё
	// равно создана и следующий прогон не начинает с нуля.
	for _, sql := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(sql, s.TableName)); err != nil {
			return 0, err
		}
	}
	itemsURL, groupsURL, err := cpiWeightsLinks()
	if err != nil {
		return 0, err
	}
	items := cpiWeightsImporterFile(cpiWeightsFileItems, itemsURL, cpiWeightsCountItems)
	groups := cpiWeightsImporterFile(cpiWeightsFileGroups, groupsURL, cpiWeightsCountGroups)
	itemsCount, err := items.Import(ctx, conn)
	if err != nil {
		return itemsCount, err
	}
	// Второй файл публикует каталог уже по обоим файлам: AfterImport шаблона стоит
	// после Send и до строки версии. Повторная публикация ниже страхует случай, когда
	// оба файла в no-op, а витрина или каталог отсутствуют (self-heal).
	groupsCount, err := groups.Import(ctx, conn)
	if err != nil {
		return itemsCount + groupsCount, err
	}
	if err = cpiWeightsAfterImport(ctx, conn); err != nil {
		return itemsCount + groupsCount, err
	}
	return itemsCount + groupsCount, nil
}

// cpiWeightsImport разбирает книгу и пишет точки одним батчем.
func cpiWeightsImport(xlsx *excelize.File, batch driver.Batch, file string) error {
	points, err := parseCPIWeights(xlsx, file)
	if err != nil {
		return err
	}
	for _, point := range points {
		if err = batch.Append(point.year, point.variant, point.file, point.level, point.code, point.name, point.weight); err != nil {
			return err
		}
	}
	return nil
}

// cpiWeightsCatalog публикует каталог весовых рядов: одна запись на (код, файл).
func cpiWeightsCatalog(ctx context.Context, conn driver.Conn) error {
	rows, err := conn.Query(ctx, cpiWeightsCatalogQuery)
	if err != nil {
		return fmt.Errorf("%s: %w", rosstatCPIWeightsTable, err)
	}
	defer func() { _ = rows.Close() }()

	var catalog []cpiWeightCatalogRow
	for rows.Next() {
		var row cpiWeightCatalogRow
		if err = rows.Scan(&row.code, &row.source, &row.name, &row.firstYear, &row.lastYear); err != nil {
			return fmt.Errorf("%s: %w", rosstatCPIWeightsTable, err)
		}
		catalog = append(catalog, row)
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("%s: %w", rosstatCPIWeightsTable, err)
	}
	return util.UpsertSeriesCatalog(ctx, conn, cpiWeightsSeriesMeta(catalog))
}

func cpiWeightsAfterImport(ctx context.Context, conn driver.Conn) error {
	if err := cpiWeightsCatalog(ctx, conn); err != nil {
		return err
	}
	_, err := util.CreateView(ctx, conn, cpiWeightsView)
	return err
}

// cpiWeightsView — витрина весов: широкая, потому что вес — свойство (год, код),
// а не датированный ряд; комментарии колонок — для MCP-агента.
var cpiWeightsView = util.View{
	Name:   "v_rosstat_cpi_weights",
	Tables: []string{rosstatCPIWeightsTable},
	Select: `SELECT year, variant, source_file, level, code, name, weight
FROM rosstat_cpi_weights FINAL`,
	Comment: "Структура потребительских расходов населения Росстата для расчёта ИПЦ: годовой вес каждой позиции (товара/услуги-представителя и группы) в процентах. Источник — два публичных XLSX страницы rosstat.gov.ru/statistics/price: CPR_tov_RF_* (товары, 2004–2026) и CPR_Gr-Tov-RF_* (группы и товары, 2001–2026). В групповом файле 2016–2024 кода нет — такие строки хранятся с синтетическим code='name:<название>' (не код Росстата); у 2025–2026 есть локальные коды. Сумма товарного листа без итоговой строки «Все товары и услуги» — 90…100,5: объединённая группа «Воздушный и железнодорожный транспорт» публикуется в групповом файле и не разбита на 9260/9290; у группового файла могут быть все уровни, поэтому его сумма не нормируется. Лист 2022 хранится в двух вариантах (до 01.04 и с 01.04) — колонка variant. Веса 2016–2024 совпадают с архивом CB_COMP на пересечении кодов (9260/9290 — архивный фолбэк); 2025–26 — live-источник. Единицы — проценты, не доли.",
	Columns: map[string]string{
		"year":        "Год структуры расходов (лист книги); применяется к месяцам следующего года",
		"variant":     "Вариант листа года: пусто — единственный лист; before-2022-04-01 / from-2022-04-01 — две публикации 2022",
		"source_file": "Файл источника: CPR_tov_RF — товары (услуги)-представители; CPR_Gr-Tov-RF — группы и товары",
		"level":       "Уровень иерархии из файла (0 — файл уровня не даёт)",
		"code":        "Код позиции Росстата (например, 10, 803, 930.АГ) или синтетический name:<название> у строк группового файла 2016–2024, где код не публикуется; ключ вместе с годом, вариантом и файлом",
		"name":        "Название позиции из источника на этот год",
		"weight":      "Вес позиции, % структуры потребительских расходов (сумма пары «товарный лист + объединённая группа транспорта» = 100)",
	},
}

func cpiWeightFileKind(source string) string {
	if source == cpiWeightsFileGroups {
		return "groups"
	}
	return "items"
}

func cpiWeightFileTitle(source string) string {
	if source == cpiWeightsFileGroups {
		return "группа/товар"
	}
	return "товар (услуга)-представитель"
}

// cpiWeightsSeriesMeta строит каталог весов: одна запись на пару (код, файл).
func cpiWeightsSeriesMeta(rows []cpiWeightCatalogRow) []util.SeriesMeta {
	metas := make([]util.SeriesMeta, 0, len(rows))
	for _, row := range rows {
		kind := cpiWeightFileKind(row.source)
		metas = append(metas, util.SeriesMeta{
			Source:    rosstatSource,
			Series:    "cpi-weight:" + row.code + ":" + kind,
			Title:     fmt.Sprintf("%s — вес, %s", row.name, cpiWeightFileTitle(row.source)),
			Unit:      "% (доля в структуре потребительских расходов ИПЦ)",
			Frequency: "A",
			Origin:    fmt.Sprintf("rosstat.gov.ru/statistics/price, файл %s_*.xlsx, лист = год, код %s", row.source, row.code),
			Description: fmt.Sprintf("Годовой вес позиции в структуре потребительских расходов для расчёта ИПЦ: %d–%d. "+
				"Вес 2016–2024 совпадает с архивом CB_COMP точно, 2025–26 — live-источник; лист 2022 хранится в двух вариантах (до 01.04 и с 01.04). "+
				"Сумма пары «товарный лист + объединённая группа транспорта» = 100 ± 0,5. Дата точки — год, а не месяц; ревизия перезаписывается по ключу (year, variant, source_file, code).",
				row.firstYear, row.lastYear),
		})
	}
	return metas
}

// cpiWeightsImporter — веса ИПЦ Росстата: товары и группы, 2001/2004–2026.
var cpiWeightsImporter = &CPIWeightsStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              rosstatCPIWeightsTable,
	CreateTable:            []string{cpiWeightsDdl},
	AfterImport:            cpiWeightsAfterImport,
	PublicationCountPoints: cpiWeightsCountAll,
}}
