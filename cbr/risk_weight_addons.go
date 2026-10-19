package cbr

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	// cbrRiskWeightAddonsTable — матрицы надбавок к коэффициентам риска: одна
	// строка на (вид матрицы, период, код строки, код столбца, файл-версию).
	cbrRiskWeightAddonsTable = "cbr_risk_weight_addons"
	// cbrRiskWeightAddonsUrl — landing с 33 xlsx-матрицами (действующие + архив).
	cbrRiskWeightAddonsUrl = "https://www.cbr.ru/finstab/instruments/risk_weight_add_ons/"
	// cbrRiskWeightAddonsCountPoints — глобальный чек версии для реестра; на
	// каждый файл локальная копия шаблона объявляет per-file count (см. Import).
	cbrRiskWeightAddonsCountPoints = "SELECT count() FROM cbr_risk_weight_addons FINAL"
	cbrRiskWeightAddonsDdl         = `CREATE TABLE IF NOT EXISTS %s (
    matrix LowCardinality(String),
    valid_from Date,
    valid_to Date,
    table_no LowCardinality(String),
    row_code LowCardinality(String),
    col_code LowCardinality(String),
    value Float64,
    is_applicable UInt8,
    source_file LowCardinality(String),
    source_sheet LowCardinality(String),
    source_caption String,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (matrix, valid_from, valid_to, row_code, col_code, source_file);`
)

// cbrRiskWeightAddonsStat — импортёр cbr_risk_weight_addons. Артефакт — не один
// файл, а 33 матрицы, поэтому Import() свой (прецедент — cbr_dip_bulletin):
// страница → файлы → локальная копия шаблона на каждый файл. Локальная копия
// приносит per-file preflight и архив версии; AfterImport пуст, каталог и
// витрину публикует основной Import() после цикла (в том числе на no-op).
type cbrRiskWeightAddonsStat struct {
	util.ClickHouseImport
}

// Import — DDL → landing → 33 файла в порядке даты начала действия документа
// (старые первыми) → каждый файл через шаблон → подсчёт строк версии → каталог
// и витрина. Ошибка одного файла не отменяет остальные: files независимы, а
// пропущенный файл подхватится следующим прогоном.
func (s *cbrRiskWeightAddonsStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	// Набор файлов и их адреса берутся с landing; в режиме восстановления
	// источник не спрашивается, а списка файлов без landing нет. Отказываем до
	// сети с понятным текстом (прецедент — bank/vtb_fin_rez.go).
	if util.ArchiveRestoreMode() {
		return 0, fmt.Errorf("%s: режим восстановления не поддержан: артефакт — набор из 33 файлов, список берётся с landing %s",
			cbrRiskWeightAddonsTable, cbrRiskWeightAddonsUrl)
	}
	for _, ddl := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(ddl, s.TableName)); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", cbrRiskWeightAddonsTable, err)
		}
	}

	page, err := util.GetPage(cbrRiskWeightAddonsUrl)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", cbrRiskWeightAddonsTable, cbrRiskWeightAddonsUrl, err)
	}
	files, err := cbrRiskWeightAddonsParsePage(page)
	if err != nil {
		return 0, err
	}
	known, err := riskAddonKnownVersions(ctx, conn)
	if err != nil {
		return 0, err
	}

	var errs []error
	for idx, file := range files {
		version := riskAddonVersion(file, idx, known[file.Name])
		local := s.ClickHouseImport
		local.DataUrl = file.URL
		local.ImportFunc = cbrRiskWeightAddonsImportFunc(file, version)
		local.PublicationSource = cbrRiskWeightAddonsTable
		local.PublicationCountPoints = fmt.Sprintf(
			"SELECT count() FROM %s FINAL WHERE source_file = '%s'",
			cbrRiskWeightAddonsTable, riskAddonSQLQuote(file.Name))
		local.AfterImport = nil

		if _, importErr := local.Import(ctx, conn); importErr != nil {
			errs = append(errs, fmt.Errorf("%s: файл %s: %w", cbrRiskWeightAddonsTable, file.Name, importErr))
			continue
		}
		var written uint64
		if queryErr := conn.QueryRow(ctx,
			"SELECT count() FROM "+cbrRiskWeightAddonsTable+" WHERE version = ? AND source_file = ?",
			version, file.Name).Scan(&written); queryErr != nil {
			errs = append(errs, fmt.Errorf("%s: файл %s: число строк версии не прочитано: %w",
				cbrRiskWeightAddonsTable, file.Name, queryErr))
			continue
		}
		if written == 0 {
			log.Infof("%s: файл %s — no-op, версия уже импортирована", cbrRiskWeightAddonsTable, file.Name)
			continue
		}
		count += int64(written)
		log.Infof("%s: файл %s (%s): %d ячеек", cbrRiskWeightAddonsTable, file.Name,
			file.DocumentFrom.Format("2006-01-02"), written)
	}

	if publishErr := afterCbrRiskWeightAddons(cbrRiskWeightAddonsSeriesMeta)(ctx, conn); publishErr != nil {
		errs = append(errs, publishErr)
	}
	return count, errors.Join(errs...)
}

// cbrRiskWeightAddonsImportFunc — ImportFunc шаблона для одного файла: разбор
// книги и запись ячеек одной версией. Файл попадает в винтажный архив через
// шаблон: байты, которые он разбирает, — те же, что сохраняет архив.
func cbrRiskWeightAddonsImportFunc(file riskAddonFile, version uint64) func(*excelize.File, driver.Batch) error {
	return func(xlsx *excelize.File, batch driver.Batch) error {
		points, err := cbrRiskWeightAddonsParseBook(xlsx, file)
		if err != nil {
			return err
		}
		for _, p := range points {
			if err = batch.Append(p.matrix, p.validFrom, p.validTo, p.tableNo, p.rowCode, p.colCode,
				p.value, p.isApplicable, p.sourceFile, p.sourceSheet, p.sourceCaption, version); err != nil {
				return fmt.Errorf("%s: файл %s, лист %s, %s×%s: %w",
					cbrRiskWeightAddonsTable, p.sourceFile, p.sourceSheet, p.rowCode, p.colCode, err)
			}
		}
		return nil
	}
}

// riskAddonVersion — версия строки для ReplacingMergeTree: старшие 32 бита —
// дата начала действия документа, младшие — детерминированный номер файла в
// отсортированном списке. Так более новый документ всегда выигрывает у старого
// в argMax витрины, даже если старый файл переимпортировали позже (сбой архива,
// backfill). existing (max(version) этого source_file) двигает младшие биты
// вперёд, чтобы изменённое содержимое того же файла перезаписало прежние строки.
func riskAddonVersion(file riskAddonFile, index int, existing uint64) uint64 {
	high := uint64(file.DocumentFrom.Unix()) << 32
	version := high | uint64(index+1)
	if version <= existing {
		version = existing + 1
	}
	return version
}

// riskAddonKnownVersions — max(version) по каждому уже записанному source_file:
// нужно только для монотонности повторного импорта того же файла.
func riskAddonKnownVersions(ctx context.Context, conn driver.Conn) (map[string]uint64, error) {
	rows, err := conn.Query(ctx,
		"SELECT source_file, max(version) FROM "+cbrRiskWeightAddonsTable+" GROUP BY source_file")
	if err != nil {
		return nil, fmt.Errorf("%s: версии файлов не прочитаны: %w", cbrRiskWeightAddonsTable, err)
	}
	defer func() { _ = rows.Close() }()

	known := make(map[string]uint64)
	for rows.Next() {
		var name string
		var version uint64
		if err = rows.Scan(&name, &version); err != nil {
			return nil, fmt.Errorf("%s: строка версий не прочитана: %w", cbrRiskWeightAddonsTable, err)
		}
		known[name] = version
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: версии файлов не дочитаны: %w", cbrRiskWeightAddonsTable, err)
	}
	return known, nil
}

// riskAddonSQLQuote — basename файла в SQL-литерале. Имена приходят со страницы
// ЦБ, но кавычка в имени не должна ломать запрос.
func riskAddonSQLQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// cbrRiskWeightAddonsImporter — запись реестра cbr/importers.go. Базовая точка
// контроля объявлена глобальным count: она — контракт реестра; per-file count
// подставляется в локальные копии внутри Import().
var cbrRiskWeightAddonsImporter = &cbrRiskWeightAddonsStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              cbrRiskWeightAddonsTable,
	CreateTable:            []string{cbrRiskWeightAddonsDdl},
	DataUrl:                cbrRiskWeightAddonsUrl,
	PublicationCountPoints: cbrRiskWeightAddonsCountPoints,
}}
