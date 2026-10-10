package polyus

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	log "github.com/sirupsen/logrus"
)

// financialMetricsTable — витрина с метриками Polyus. Схема перенесена без
// изменений из legacy-импортёра financial/gold_polyus_finance.go (коммит
// 108f4ca): таблица уже наполнена в ClickHouse.
const financialMetricsTable = "polyus_financial_metrics"

// financialMetricsCompany — тикер компании в записях витрины.
const financialMetricsCompany = "PLZL"

// financialMetricsCreateTable — DDL витрины, дословно из legacy-импортёра.
// Формат с %s сохранён ради совместимости с util.ClickHouseImport: подстановка
// имени таблицы идёт через fmt.Sprintf.
const financialMetricsCreateTable = `CREATE TABLE IF NOT EXISTS %s
(
    company LowCardinality(String),
    metric LowCardinality(String),
    period String,
    period_type Enum8('Q' = 1, 'H' = 2, 'FY' = 3, 'LTM' = 4),
    value Nullable(Float64),
    unit LowCardinality(String),
    source_url LowCardinality(String),
    source_page UInt16,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (company, metric, period)`

// financialMetricsImport импортирует метрики Polyus из PDF-отчётов в
// polyus_financial_metrics.
type financialMetricsImport struct{}

func (s *financialMetricsImport) Name() string {
	return financialMetricsTable
}

func (s *financialMetricsImport) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(
		ctx,
		fmt.Sprintf(financialMetricsCreateTable, financialMetricsTable),
	); err != nil {
		return 0, err
	}

	batch, err := conn.PrepareBatch(
		ctx,
		"INSERT INTO "+financialMetricsTable,
	)
	if err != nil {
		return 0, err
	}

	var failed, skipped, dropped, duplicates int

	// Одно время загрузки на весь батч: loaded_at объявлен с DEFAULT now(), но
	// batch-вставка ClickHouse всё равно требует значение на каждую из девяти
	// колонок. Одинаковая метка времени и делает строки одной загрузки одной
	// версией в ReplacingMergeTree(loaded_at).
	loadedAt := time.Now()

	// Ключи уже добавленных записей. Таблица с ORDER BY (company, metric, period)
	// молча схлопывает строки с одинаковым ключом: батч из 28 записей ClickHouse
	// записал как 25 строк, а лог импортёра отчитался о 28. Ключ тут считается
	// повторно, потому что ORDER BY таблицы — часть её DDL, и связь между ними
	// должна быть видна там же, где ведётся вставка.
	seen := make(map[batchKey]bool)

	for _, report := range reports {
		if !report.Enabled {
			// Отключённый отчёт не ошибка: адрес и причина остаются в pages.go,
			// и в следующий раз включать его будет тот, кто увидит эту строку.
			skipped++
			log.Infof("skip report %s (%s): disabled", report.URL, report.Period)

			continue
		}

		records, parseErr := parseReport(ctx, report)
		if parseErr != nil {
			// Один недоступный или нечитаемый отчёт не роняет импорт целиком:
			// остальные отчёты по-прежнему попадают в витрину.
			failed++
			log.Warnf("report %s (%s) failed: %v", report.URL, report.Period, parseErr)

			continue
		}

		// Записи, чей период parseKPIPage не отличает от колонки-изменения,
		// отбрасываются здесь: в витрину попадают только выверенные периоды.
		if report.Kind == "kpi" {
			var guardDropped int
			records, guardDropped = guardKPIRecords(records)
			dropped += guardDropped
		}

		for _, record := range records {
			key := batchKey{record.Metric, record.Period}
			if seen[key] {
				duplicates++
				log.Warnf(
					"duplicate %s %s from %s: already in batch, skipping to keep the batch key unique",
					record.Metric,
					record.Period,
					record.SourceURL,
				)

				continue
			}
			seen[key] = true

			if err = batch.Append(
				financialMetricsCompany,
				record.Metric,
				record.Period,
				record.PeriodType,
				record.Value,
				record.Unit,
				record.SourceURL,
				uint16(record.SourcePage),
				loadedAt,
			); err != nil {
				return count, err
			}

			count++
		}
	}

	if err = batch.Send(); err != nil {
		return 0, err
	}

	log.Infof(
		"Imported %d rows of %s (%d reports skipped as disabled, %d failed, %d change-column records dropped, %d duplicate keys skipped)",
		count,
		financialMetricsTable,
		skipped,
		failed,
		dropped,
		duplicates,
	)

	return count, nil
}

// batchKey — ключ ORDER BY витрины: по нему ReplacingMergeTree схлопывает
// строки, поэтому две записи с одним ключом в одном батче неразличимы после
// вставки.
//
// Оба поля обязаны быть непустыми: пустой период дал бы один и тот же ключ
// (metric, "") для всех записей отчёта, который не удалось бы разобрать, и
// ClickHouse оставил бы ровно одну строку вместо всего отчёта.
type batchKey struct {
	Metric string
	Period string
}

// uniqueBatchKeys оставляет по одному ключу на каждую пару (метрика, период),
// сохраняя порядок появления, и возвращает число отброшенных повторов.
//
// Функция служит спецификацией слияния ключей для Import: сам он ведёт seen
// потоком по отчётам, чтобы не держать в памяти список из одних ключей. Здесь
// же проверяется само правило — без него дубли дошли бы до batch.Append и
// ClickHouse схлопнул бы их уже после вставки, оставив лог импортёра с
// завышенным счётчиком строк.
func uniqueBatchKeys(keys [][2]string) (kept [][2]string, duplicates int) {
	seen := make(map[[2]string]bool, len(keys))

	for _, key := range keys {
		if seen[key] {
			duplicates++

			continue
		}

		seen[key] = true
		kept = append(kept, key)
	}

	return kept, duplicates
}

// parseReport скачивает PDF отчёта во временный каталог, извлекает из него
// указанные страницы и разбирает их парсером соответствующего типа.
//
// Каталог удаляется целиком при выходе: textPath передаётся в extractPDF с
// компонентом каталога (filepath.Join(dir, ...)), потому что extractPDF кладёт
// промежуточные файлы страниц рядом с ним через filepath.Dir.
func parseReport(ctx context.Context, report Report) ([]MetricRecord, error) {
	dir, err := os.MkdirTemp("", "polyus-report-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer func() {
		if removeErr := os.RemoveAll(dir); removeErr != nil {
			log.Warnf("remove temp dir %s: %v", dir, removeErr)
		}
	}()

	pdfPath := filepath.Join(dir, "report.pdf")
	if err = downloadPDF(ctx, report.URL, pdfPath); err != nil {
		return nil, fmt.Errorf("download %s: %w", report.URL, err)
	}

	textPath := filepath.Join(dir, "report.txt")
	if err = extractPDF(ctx, pdfPath, textPath, report.Pages); err != nil {
		return nil, fmt.Errorf("extract %s: %w", report.URL, err)
	}

	switch report.Kind {
	case "kpi":
		// KPI-парсер берёт период из шапки страницы, а не из метаданных отчёта.
		return parseKPIPage(textPath, report.URL, report.Pages[0])
	case "ifrs":
		return parseIFRSPage(
			textPath,
			report.URL,
			report.Pages[0],
			report.Period,
		)
	default:
		return nil, fmt.Errorf("unknown report Kind %q", report.Kind)
	}
}

func init() {
	chimport.Stats = append(chimport.Stats, &financialMetricsImport{})
}
