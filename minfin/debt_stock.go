package minfin

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	debtInternalPageURL  = minfinURL + "/ru/document?id_4=93479-tablitsy_ezhemesyachnykh_znachenii_obema_gosudarstvennogo_vnutrennego_dolga_rossiiskoi_federatsii"
	debtInternalSelector = `a[href$=".xls"]`
	debtExternalPageURL  = minfinURL + "/ru/document?id_4=69444-gosudarstvennyi_vneshnii_dolg_rossiiskoi_federatsii_2011-2026_gg."
	debtExternalSelector = `a[href$=".xlsx"]`

	ofzStructureTagURL  = minfinURL + "/ru/document?TAG_ID_4%5B%5D=76"
	ofzStructureDocSel  = `a[href*="struktura_i_dinamika_gosudarstvennogo_vnutrennego_dolga_rossiiskoi_federatsii_v_chasti_gosudarstvennykh_tsennykh_bumag_rossiiskoi_federatsii_nominalnaya_stoimost_kotorykh_ukazana_v_valyute_rossiiskoi_federatsii_v_period_s_01"]`
	ofzStructureFileSel = `a[href*="debtDynamicsMonth_"]`

	ofzStructure2022URL = "https://minfin.gov.ru/common/upload/library/2023/01/main/Struktura_gos_vnutr._dolga_po_srokam_pogasheniya_i_vidam_obyazatelstv_ezhemesyachnye_na_rus._na_1_yanvarya_23.xls"
)

// minfinDebtStock — мультифайловый импортёр: внутренний .xls, внешний .xlsx,
// годовая структура 2022 и месячные debtDynamicsMonth. Шаблон util.ClickHouseImport
// вызывается по одному файлу; внешний Import() нужен потому, что DataUrl у шаблона
// один. Префлайт не объявлен: файлы обновляются с той же частотой, что шаг DAG
// (monthly), повтор перезаписывает точки и не меняет count().
type minfinDebtStock struct {
	util.ClickHouseImport
}

var debtStockImporter = &minfinDebtStock{ClickHouseImport: util.ClickHouseImport{
	TableName:   debtStockTable,
	CreateTable: minfinSeriesCreate(false),
	AfterImport: func(ctx context.Context, conn driver.Conn) error {
		return publishOfzStress(ctx, conn, debtStockSeriesMeta)
	},
}}

func (s *minfinDebtStock) Import(ctx context.Context, conn driver.Conn) (int64, error) {
	for _, stmt := range s.CreateTable {
		if err := conn.Exec(ctx, fmt.Sprintf(stmt, s.TableName)); err != nil {
			return 0, err
		}
	}

	var total int64

	internalURL, err := resolveMinfinFile(debtInternalPageURL, debtInternalSelector, debtStockTable)
	if err != nil {
		return total, err
	}
	internal := util.ClickHouseImport{
		TableName:   s.TableName,
		CreateTable: s.CreateTable,
		DataUrl:     internalURL,
		BodyFunc:    parseInternalDebtXls,
		AfterImport: s.AfterImport,
	}
	imported, err := internal.Import(ctx, conn)
	if err != nil {
		return total, fmt.Errorf("%s: внутренний долг: %w", debtStockTable, err)
	}
	total += imported

	externalURL, err := resolveMinfinFile(debtExternalPageURL, debtExternalSelector, debtStockTable)
	if err != nil {
		return total, err
	}
	external := util.ClickHouseImport{
		TableName:   s.TableName,
		CreateTable: s.CreateTable,
		DataUrl:     externalURL,
		ImportFunc:  parseExternalDebtXlsx,
		AfterImport: s.AfterImport,
	}
	imported, err = external.Import(ctx, conn)
	if err != nil {
		return total, fmt.Errorf("%s: внешний долг: %w", debtStockTable, err)
	}
	total += imported

	annualPoints, err := util.CountPoints(ctx, conn,
		fmt.Sprintf("SELECT count() FROM %s FINAL WHERE name = '%s' AND date <= '2023-01-01'", debtStockTable, debtStockOfzTotal))
	if err != nil {
		return total, fmt.Errorf("%s: гейт структуры 2022: %w", debtStockTable, err)
	}
	if annualPoints == 0 {
		annual := util.ClickHouseImport{
			TableName:   s.TableName,
			CreateTable: s.CreateTable,
			DataUrl:     ofzStructure2022URL,
			BodyFunc:    parseOfzStructure2022Xls,
			AfterImport: s.AfterImport,
		}
		imported, err = annual.Import(ctx, conn)
		if err != nil {
			return total, fmt.Errorf("%s: структура 2022: %w", debtStockTable, err)
		}
		total += imported
	}

	monthlyPoints, err := util.CountPoints(ctx, conn,
		fmt.Sprintf("SELECT count() FROM %s FINAL WHERE name = '%s' AND date >= '2023-02-01'", debtStockTable, debtStockOfzTotal))
	if err != nil {
		return total, fmt.Errorf("%s: гейт месячной структуры: %w", debtStockTable, err)
	}
	forceBackfill := strings.EqualFold(os.Getenv("MINFIN_DEBT_STOCK_BACKFILL"), "1")
	if forceBackfill || monthlyPoints == 0 {
		imported, err = importOfzStructureHistory(ctx, conn, s)
		if err != nil {
			return total, err
		}
		total += imported
	}

	latestURL, err := resolveOfzStructureFileURL()
	if err != nil {
		return total, err
	}
	latest := util.ClickHouseImport{
		TableName:   s.TableName,
		CreateTable: s.CreateTable,
		DataUrl:     latestURL,
		ImportFunc:  parseOfzStructureMonthXlsx,
		AfterImport: s.AfterImport,
	}
	imported, err = latest.Import(ctx, conn)
	if err != nil {
		return total, fmt.Errorf("%s: последняя структура %s: %w", debtStockTable, latestURL, err)
	}
	return total + imported, nil
}

// importOfzStructureHistory — первичный бэкфилл месячных структур: с 2023-05-01
// по текущий месяц по шаблону URL. Отсутствующий файл (HEAD 404) — не ошибка:
// публикации 2023-02…04 и 2025-04…08 на сайте отсутствуют (source report §5).
func importOfzStructureHistory(ctx context.Context, conn driver.Conn, s *minfinDebtStock) (int64, error) {
	var total int64
	start := time.Date(2023, time.May, 1, 0, 0, 0, 0, time.UTC)
	now := time.Now()
	end := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	for date := start; !date.After(end); date = date.AddDate(0, 1, 0) {
		dataURL := parseOfzStructureMonthURL(date.Year(), int(date.Month()))
		// HEAD на этом пути Minfin отдаёт 404 даже для существующего файла (WAF),
		// поэтому проба — GET: XLSX начинается с zip-сигнатуры PK, а 404-заглушка
		// приходит HTML. Тело валидного файла затем качает шаблон ещё раз — 10 КБ
		// на месяц, зато пропуск отсутствующей публикации не зависит от HEAD.
		body, err := util.GetFileBytes(dataURL)
		if err != nil {
			return total, fmt.Errorf("%s: проба структуры %s: %w", debtStockTable, dataURL, err)
		}
		if !bytes.HasPrefix(body, []byte("PK")) {
			log.Debugf("%s: нет публикации структуры на %s — пропуск (ответ не XLSX)", debtStockTable, date.Format("2006-01-02"))
			continue
		}
		inner := util.ClickHouseImport{
			TableName:   s.TableName,
			CreateTable: s.CreateTable,
			DataUrl:     dataURL,
			ImportFunc:  parseOfzStructureMonthXlsx,
			AfterImport: s.AfterImport,
		}
		imported, err := inner.Import(ctx, conn)
		if err != nil {
			return total, fmt.Errorf("%s: структура %s: %w", debtStockTable, dataURL, err)
		}
		total += imported
		time.Sleep(500 * time.Millisecond)
	}
	return total, nil
}

func resolveMinfinFile(pageURL, selector, table string) (string, error) {
	page, err := util.GetPage(pageURL)
	if err != nil {
		return "", fmt.Errorf("%s: страница %s: %w", table, pageURL, err)
	}
	return minfinLink(page, pageURL, selector, table)
}

func resolveOfzStructureFileURL() (string, error) {
	docURL, err := resolveFirstTagDoc(ofzStructureTagURL, ofzStructureDocSel, debtStockTable, 5)
	if err != nil {
		return "", err
	}
	docPage, err := util.GetPage(docURL)
	if err != nil {
		return "", fmt.Errorf("%s: страница документа %s: %w", debtStockTable, docURL, err)
	}
	return minfinLink(docPage, docURL, ofzStructureFileSel, debtStockTable)
}

// resolveFirstTagDoc — первая ссылка селектора на первых страницах тега. Тег
// «Госдолг» (76) показывает документы не строго на первой странице: актуальная
// месячная структура и «Основные показатели…» лежат на второй, поэтому
// резолв идёт по страницам, а не только по page_4=1.
func resolveFirstTagDoc(tagURL, selector, table string, maxPages int) (string, error) {
	var lastErr error
	for page := 1; page <= maxPages; page++ {
		pageURL := tagURL
		if page > 1 {
			pageURL = fmt.Sprintf("%s&page_4=%d", tagURL, page)
		}
		pageBytes, err := util.GetPage(pageURL)
		if err != nil {
			return "", fmt.Errorf("%s: страница %s: %w", table, pageURL, err)
		}
		href, err := minfinLink(pageBytes, pageURL, selector, table)
		if err == nil {
			return href, nil
		}
		lastErr = err
	}
	return "", fmt.Errorf("%s: в первых %d страницах тега не найдена ссылка: %w", table, maxPages, lastErr)
}
