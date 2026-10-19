package cbr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	// cbrMPLTable — макропруденциальные лимиты: одна строка на правило/полосу.
	cbrMPLTable = "cbr_mpl"
	// cbrMPLUrl — landing с 10 xlsx-файлами: лимиты банков и МФО + границы
	// диапазонов старых форматов.
	cbrMPLUrl = "https://www.cbr.ru/finstab/instruments/makroprudencialnye-limity/"
	// cbrMPLCountPoints — глобальный чек версии для реестра; на каждый limit-файл
	// локальная копия шаблона объявляет per-file count (см. Import).
	cbrMPLCountPoints = "SELECT count() FROM cbr_mpl FINAL"
	cbrMPLDdl         = `CREATE TABLE IF NOT EXISTS %s (
    segment LowCardinality(String),
    loan_class LowCardinality(String),
    period_from Date,
    period_to Date,
    source_file LowCardinality(String),
    source_sheet LowCardinality(String),
    row_no UInt32,
    subrow_no LowCardinality(String),
    band_idx UInt8,
    loan_kind String,
    pdn_band LowCardinality(String),
    pdn_min Nullable(Float64),
    pdn_max Nullable(Float64),
    term_band LowCardinality(String),
    term_min Nullable(Float64),
    term_max Nullable(Float64),
    down_payment_band LowCardinality(String),
    down_payment_min Nullable(Float64),
    down_payment_max Nullable(Float64),
    ltv_band LowCardinality(String),
    ltv_min Nullable(Float64),
    ltv_max Nullable(Float64),
    amount_band LowCardinality(String),
    amount_min Nullable(Float64),
    amount_max Nullable(Float64),
    limit_pct Float64,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (segment, loan_class, period_from, period_to, source_file, row_no, subrow_no, band_idx);`
)

// cbrMPLStat — импортёр cbr_mpl. Артефакт — набор из 10 файлов, поэтому
// Import() свой (прецедент — cbr_risk_weight_addons): landing → ranges в память
// → limit-файлы через локальные копии шаблона → version-проверка → каталог и
// витрина. Ranges-файлы в таблицу не пишутся: они нужны только для числовых
// границ v1/v2.
type cbrMPLStat struct {
	util.ClickHouseImport
}

func (s *cbrMPLStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	// Список limit-файлов берётся с landing, а ranges-файлы вообще не пишутся в
	// таблицу и в архив не попадают; восстановить набор без источника нельзя.
	if util.ArchiveRestoreMode() {
		return 0, fmt.Errorf("%s: режим восстановления не поддержан: артефакт — набор limit-файлов с landing %s и ranges-файлы, которые не архивируются",
			cbrMPLTable, cbrMPLUrl)
	}
	for _, ddl := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(ddl, s.TableName)); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", cbrMPLTable, err)
		}
	}

	page, err := util.GetPage(cbrMPLUrl)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", cbrMPLTable, cbrMPLUrl, err)
	}
	limits, rangesFiles, err := cbrMPLParsePage(page)
	if err != nil {
		return 0, err
	}

	ranges := make(mplRanges)
	for _, file := range rangesFiles {
		xlsx, loadErr := util.GetXlsx(file.URL)
		if loadErr != nil {
			return 0, fmt.Errorf("%s: файл границ %s: %w", cbrMPLTable, file.Name, loadErr)
		}
		parsed, parseErr := cbrMPLParseRanges(xlsx, file)
		closeErr := xlsx.Close()
		if parseErr != nil {
			return 0, parseErr
		}
		if closeErr != nil {
			return 0, fmt.Errorf("%s: файл границ %s: закрытие книги: %w", cbrMPLTable, file.Name, closeErr)
		}
		for key, value := range parsed {
			ranges[key] = value
		}
	}

	var errs []error
	for _, file := range limits {
		version := uint64(time.Now().UnixNano())
		local := s.ClickHouseImport
		local.DataUrl = file.URL
		local.ImportFunc = cbrMPLImportFunc(file, ranges, version)
		local.PublicationSource = cbrMPLTable
		local.PublicationCountPoints = fmt.Sprintf(
			"SELECT count() FROM %s FINAL WHERE source_file = '%s'",
			cbrMPLTable, mplSQLQuote(file.Name))
		local.AfterImport = nil

		if _, importErr := local.Import(ctx, conn); importErr != nil {
			errs = append(errs, fmt.Errorf("%s: файл %s: %w", cbrMPLTable, file.Name, importErr))
			continue
		}
		var written uint64
		if queryErr := conn.QueryRow(ctx,
			"SELECT count() FROM "+cbrMPLTable+" WHERE version = ? AND source_file = ?",
			version, file.Name).Scan(&written); queryErr != nil {
			errs = append(errs, fmt.Errorf("%s: файл %s: число строк версии не прочитано: %w",
				cbrMPLTable, file.Name, queryErr))
			continue
		}
		if written == 0 {
			log.Infof("%s: файл %s — no-op, версия уже импортирована", cbrMPLTable, file.Name)
			continue
		}
		count += int64(written)
		log.Infof("%s: файл %s (%s, %s): %d строк", cbrMPLTable, file.Name,
			file.Segment, file.DocumentFrom.Format("2006-01-02"), written)
	}

	if publishErr := afterCbrMPL(cbrMPLSeriesMeta)(ctx, conn); publishErr != nil {
		errs = append(errs, publishErr)
	}
	return count, errors.Join(errs...)
}

func cbrMPLImportFunc(file mplFile, ranges mplRanges, version uint64) func(*excelize.File, driver.Batch) error {
	return func(xlsx *excelize.File, batch driver.Batch) error {
		points, err := cbrMPLParseBook(xlsx, file, ranges)
		if err != nil {
			return err
		}
		for _, p := range points {
			if err = batch.Append(
				p.segment, p.loanClass, p.periodFrom, p.periodTo, p.sourceFile, p.sourceSheet,
				p.rowNo, p.subrowNo, p.bandIdx, p.loanKind,
				p.pdnBand, mplNullable(p.pdnMin), mplNullable(p.pdnMax),
				p.termBand, mplNullable(p.termMin), mplNullable(p.termMax),
				p.downPaymentBand, mplNullable(p.downPaymentMin), mplNullable(p.downPaymentMax),
				p.ltvBand, mplNullable(p.ltvMin), mplNullable(p.ltvMax),
				p.amountBand, mplNullable(p.amountMin), mplNullable(p.amountMax),
				p.limitPct, version,
			); err != nil {
				return fmt.Errorf("%s: файл %s, лист %s, строка %d: %w",
					cbrMPLTable, p.sourceFile, p.sourceSheet, p.rowNo, err)
			}
		}
		return nil
	}
}

func mplSQLQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

var cbrMPLImporter = &cbrMPLStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              cbrMPLTable,
	CreateTable:            []string{cbrMPLDdl},
	DataUrl:                cbrMPLUrl,
	PublicationCountPoints: cbrMPLCountPoints,
}}
