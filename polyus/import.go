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

	var failed, skipped, dropped int

	// unassigned собирает по отчёту счётчики значений, не ставших ни одной
	// записью (см. guard.go). Guard их не отбрасывает — они и должны остаться за
	// бортом, — но сумма уходит в итоговую строку: по ней видно, что шапка и
	// разбор разошлись.
	var unassigned []int

	// Одно время загрузки на весь батч: loaded_at объявлен с DEFAULT now(), но
	// batch-вставка ClickHouse всё равно требует значение на каждую из девяти
	// колонок. Одинаковая метка времени и делает строки одной загрузки одной
	// версией в ReplacingMergeTree(loaded_at).
	loadedAt := time.Now()

	// Записи с ключом, который уже был добавлен в этот же батч, до вставки не
	// доходят: см. batchDedup.add.
	var dedup batchDedup

	for _, report := range reports {
		if !report.Enabled {
			// Отключённый отчёт не ошибка: адрес и причина остаются в pages.go,
			// и в следующий раз включать его будет тот, кто увидит эту строку.
			skipped++
			log.Infof("skip report %s (%s): disabled", report.URL, report.Period)

			continue
		}

		records, pageUnassigned, headerless, parseErr := parseReport(ctx, report)
		if parseErr != nil {
			// Один недоступный или нечитаемый отчёт не роняет импорт целиком:
			// остальные отчёты по-прежнему попадают в витрину.
			failed++
			log.Warnf("report %s (%s) failed: %v", report.URL, report.Period, parseErr)

			continue
		}

		// Пустой разбор включённого отчёта — ошибка, а не отсутствие данных: у
		// этого класса отчётов (пресс-релиз или форма МСФО с метриками) пустая
		// страница означает, что шапка не распознана, а не что метрик нет.
		// Нераспознанная шапка даёт ровно тот же результат, что и пустой отчёт, —
		// ноль записей и nil вместо ошибки, — поэтому отказ нужно объявлять здесь.
		// Сигнал о нём несёт счётчик строк до первой шапки: unassigned при этом
		// отказе нулевой и о нем молчит (см. parseKPILines).
		if !reportParsed(records, headerless) {
			failed++
			log.Warnf(
				"report %s (%s) parsed to zero records with no error (%d metric line(s) before the first recognised header): the header was not recognised, so the page shape is the problem, not the absence of data",
				report.URL,
				report.Period,
				headerless,
			)

			continue
		}

		// Guard применяется к отчёту ЛЮБОГО типа: инвариант «в витрину не
		// попадает запись без периода» не зависит от того, откуда период взялся —
		// из шапки страницы (KPI) или из параметра отчёта (МСФО). Оставленный
		// только для KPI, он пропускал бы МСФО-записи с пустым периодом прямо в
		// batch.Append, а batchDedup их не ловит: ключ (metric, "") у них разный.
		applied, guardDropped := applyGuard(report.Kind, records)
		records = applied
		dropped += guardDropped

		if report.Kind == "kpi" {
			// Нераспределённые значения считает только KPI-разбор: у МСФО-страницы
			// такого понятия нет — период приходит параметром, а сравнительный
			// столбец не нераспределённое значение, а другая величина (см.
			// parseReport).
			unassigned = append(unassigned, pageUnassigned)
		}

		for _, record := range records {
			if !dedup.add(record) {
				log.Warnf(
					"duplicate %s %s from %s: already in batch, skipping to keep the batch key unique",
					record.Metric,
					record.Period,
					record.SourceURL,
				)

				continue
			}

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

	warnUnassignedCounts(unassigned, len(unassigned))

	log.Infof(
		"Imported %d rows of %s (%d reports skipped as disabled, %d failed, %d periodless records dropped, %d duplicate keys skipped, %d values outside period columns)",
		count,
		financialMetricsTable,
		skipped,
		failed,
		dropped,
		dedup.duplicates,
		countUnassigned(unassigned),
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

// batchDedup решает, попадает ли запись в batch, и ведёт учёт повторных ключей.
//
// Таблица с ключом ORDER BY (company, metric, period) и движком
// ReplacingMergeTree молча схлопывает строки с одинаковым ключом внутри одной
// вставки: батч из 28 записей ClickHouse записал как 25 строк, а лог импортёра
// отчитался о 28. Записи с одним ключом приходят из разных отчётов (eps_basic за
// 2026H1 печатают и KPI-релиз, и МСФО), так что проверка нужна до batch.Append —
// тогда счётчик импортированных строк совпадает с числом сохранённых.
//
// Возвращает false, если ключ уже встречался; каждый повтор считает сам.
func (d *batchDedup) add(record MetricRecord) bool {
	if d.seen == nil {
		d.seen = make(map[batchKey]bool)
	}

	key := batchKey{record.Metric, record.Period}
	if d.seen[key] {
		d.duplicates++

		return false
	}

	d.seen[key] = true

	return true
}

// batchDedup ведёт ключи, уже добавленные в batch, и считает повторы.
type batchDedup struct {
	seen       map[batchKey]bool
	duplicates int
}

// parseReport скачивает PDF отчёта во временный каталог, извлекает из него
// указанные страницы и разбирает их парсером соответствующего типа.
//
// Возвращает также число значений, не ставших ни одной записью, — сигнал для
// guard'а (см. guardRecords) и импортёра, — и число строк до первой распознанной
// шапки, по которому видно нераспознанную страницу (см. reportParsed). У
// МСФО-страницы нет ни того, ни другого: её разбор берёт из строки только первое
// значение, а сравнительный столбец отчётного периода — не нераспределённое
// значение, а другая величина того же показателя, и в счётчик она не попадает.
// Поэтому МСФО-ветка возвращает оба нуля.
//
// Каталог удаляется целиком при выходе: textPath передаётся в extractPDF с
// компонентом каталога (filepath.Join(dir, ...)), потому что extractPDF кладёт
// промежуточные файлы страниц рядом с ним через filepath.Dir.
func parseReport(ctx context.Context, report Report) ([]MetricRecord, int, int, error) {
	dir, err := os.MkdirTemp("", "polyus-report-*")
	if err != nil {
		return nil, 0, 0, fmt.Errorf("create temp dir: %w", err)
	}
	defer func() {
		if removeErr := os.RemoveAll(dir); removeErr != nil {
			log.Warnf("remove temp dir %s: %v", dir, removeErr)
		}
	}()

	pdfPath := filepath.Join(dir, "report.pdf")
	if err = downloadPDF(ctx, report.URL, pdfPath); err != nil {
		return nil, 0, 0, fmt.Errorf("download %s: %w", report.URL, err)
	}

	textPath := filepath.Join(dir, "report.txt")
	if err = extractPDF(ctx, pdfPath, textPath, report.Pages); err != nil {
		return nil, 0, 0, fmt.Errorf("extract %s: %w", report.URL, err)
	}

	switch report.Kind {
	case "kpi":
		// KPI-парсер берёт период из шапки страницы, а не из метаданных отчёта.
		// Запасной номер страницы — первая из запрошенных: снимок склеен из
		// report.Pages подряд, и он верен ровно для неё (см. parseReportKPIPage).
		records, unassigned, headerless, err := parseReportKPIPage(
			textPath,
			report.URL,
			report.Pages[0],
		)
		if err != nil {
			return nil, 0, 0, err
		}

		return records, unassigned, headerless, nil
	case "ifrs":
		records, err := parseIFRSPage(
			textPath,
			report.URL,
			report.Pages[0],
			report.Period,
		)
		if err != nil {
			return nil, 0, 0, err
		}

		return records, 0, 0, nil
	default:
		return nil, 0, 0, fmt.Errorf("unknown report Kind %q", report.Kind)
	}
}

// reportParsed сообщает, дал ли разбор отчёта хоть одну запись. Пустой разбор —
// отказ, и вот почему.
//
// Нераспознанная шапка неотличима от успеха по возврату: страница, у которой не
// нашлась шапка финансовой таблицы, разбирается в ноль записей с err == nil.
// Счётчик нераспределённых значений этот отказ не ловит: колонок нет, поэтому
// ни одно значение не может быть отнесено к непериодной колонке, и unassigned
// равен нулю именно тогда, когда сигнал нужен. Направление счётчика поэтому
// обратное — см. parseKPILines: строки до первой шапки считает headerless, и
// ненулевое значение подтверждает, что страница несла строки метрик, которым не
// досталось ни одной колонки.
//
// Строка с метрикой выше первой шапки бывает и на здоровой странице: на трёх
// включённых фикстурах с шапкой таких ноль, но английская МСФО-страница 6 без
// шапки финансовой таблицы даёт три («Total revenue», «Profit for the period»,
// «Profit for the period attributable to:»). Поэтому наличие headerless само по
// себе отказом не делает; отказ — ноль записей, потому что включённый отчёт без
// единой строки означает нераспознанную шапку, а не отсутствие данных.
func reportParsed(records []MetricRecord, headerless int) bool {
	return len(records) > 0 || headerless == 0
}

// applyGuard применяет guard к записям отчёта и возвращает оставшиеся вместе с
// числом отброшенных.
//
// Вынесено отдельной функцией, потому что от неё требуется свойство, которое
// вызовом в одной ветке не выражается: guard обязан выполняться для отчёта ЛЮБОГО
// типа. У KPI и МСФО различается только источник периода (шапка страницы против
// параметра отчёта, см. kind), а инвариант «в витрину не попадает запись без
// периода» общий. У МСФО период приходит параметром, но при period == "" все
// девять метрик страницы дошли бы до batch.Append, а batchDedup их не ловит:
// ключ витрины — (metric, period), и у разных метрик с пустым периодом он разный.
// Поэтому вызов, оставленный внутри ветки `if report.Kind == "kpi"`, терял бы
// эту гарантию молча, и kind передаётся сюда только затем, чтобы такое сужение
// было видно в сигнатуре и не проходило тест.
func applyGuard(kind string, records []MetricRecord) ([]MetricRecord, int) {
	return guardRecords(records)
}

// parseReportKPIPage разбирает склеенный снимок KPI-отчёта и возвращает записи
// вместе со счётчиком нераспределённых значений и числом строк до первой
// распознанной шапки (см. parseKPILines).
//
// Период каждой записи берётся из шапки страницы, а номер страницы — из самой
// строки снимка (linePage): extractPDF склеивает все Report.Pages в один файл, и
// номер, переданный параметром, верен только для первой страницы. Именно первая
// страница и передаётся из parseReport: номер попадает в записи лишь на
// синтетическом входе, у которого колонки page_num нет.
func parseReportKPIPage(
	textPath string,
	sourceURL string,
	page int,
) ([]MetricRecord, int, int, error) {
	lines, err := readTSVLines(textPath)
	if err != nil {
		return nil, 0, 0, err
	}

	records, unassigned, headerless := parseKPILines(lines, sourceURL, page)

	return records, unassigned, headerless, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &financialMetricsImport{})
}
