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

// financialMetricsTable — общая для сектора таблица метрик компаний. Схема взята
// из legacy-таблицы polyus_financial_metrics и расширена измерением источника
// (source_kind), которое входит в ORDER BY — см. financialMetricsCreateTable.
//
// Legacy-таблица polyus_financial_metrics остаётся на месте и больше этим
// импортёром не наполняется: её читает Grafana-дашборд dashboard/finance-polyus.json,
// поэтому ни переименовать её, ни пересоздать нельзя, не сломав дашборд.
const financialMetricsTable = "company_financials"

// financialMetricsStatName — имя импортёра в реестре chimport.Stats, то есть
// значение CLICKHOUSE_IMPORT_STAT и имя шага в dagu/financial.yaml. Это
// отдельная константа, и склеивать её с financialMetricsTable нельзя.
//
// Имя в реестре — стабильный контракт, на который ссылаются расписания
// (dagu/financial.yaml, шаг polyus_financial_metrics) и операторы, набирающие
// make import STAT=polyus_financial_metrics. Имя таблицы — не контракт: оно уже
// менялось (polyus_financial_metrics → company_financials, когда таблица стала
// общей для сектора) и может измениться снова.
//
// Пока обе величины были одной константой, смена имени таблицы молча
// переименовывала и импортёр: make import STAT=polyus_financial_metrics отвечал
// "no importer named ...", а недельный DAG-шаг перестал совпадать с импортёром.
// Поэтому Name() возвращает эту константу, а не имя таблицы.
const financialMetricsStatName = "polyus_financial_metrics"

// financialMetricsCompany — тикер компании в записях витрины.
const financialMetricsCompany = "PLZL"

// financialMetricsCreateTable — DDL витрины, канонический (ARCHITECTURE.md §6.2).
// Формат с %s сохранён ради совместимости с util.ClickHouseImport: подстановка
// имени таблицы идёт через fmt.Sprintf.
//
// Отличие от legacy-схемы одно — source_kind в списке колонок и в ORDER BY.
// Именно оно и делает таблицу пригодной для нескольких документов: один и тот же
// показатель за один и тот же период печатают и KPI-релиз, и МСФО-отчёт, и без
// измерения источника в ключе ReplacingMergeTree оставил бы из двух значений одно,
// молча потеряв второе (у Полюса так расходятся 17 из 79 общих ключей). Значения
// source_kind: "kpi" (пресс-релиз), "ifrs" (аудированная форма), "legacy"
// (строки, перенесённые из polyus_financial_metrics).
//
// Порядок колонок не произвольный: source_kind стоит между period_type и value —
// ровно там, где его передаёт batch.Append.
const financialMetricsCreateTable = `CREATE TABLE IF NOT EXISTS %s
(
    company     LowCardinality(String),
    metric      LowCardinality(String),
    period      String,
    period_type Enum8('Q' = 1, 'H' = 2, 'FY' = 3, 'LTM' = 4),
    source_kind LowCardinality(String),
    value       Nullable(Float64),
    unit        LowCardinality(String),
    source_url  LowCardinality(String),
    source_page UInt16,
    loaded_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (company, metric, period, source_kind, source_url)`

// financialMetricsImport импортирует метрики Polyus из PDF-отчётов в
// company_financials.
type financialMetricsImport struct{}

// Name возвращает имя импортёра в реестре — контракт CLI и расписания, а не имя
// таблицы. Расходиться с financialMetricsTable оно обязано: см.
// financialMetricsStatName.
func (s *financialMetricsImport) Name() string {
	return financialMetricsStatName
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

		// Пустой разбор включённого отчёта — ошибка, а не отсутствие данных:
		// включённые в pages.go отчёты указывают на страницы с данными, а не на
		// титул или содержание, поэтому ноль записей означает нераспознанную или
		// пустую страницу, а не «в отчёте нет метрик». Ошибка направления
		// несимметрична: ложная тревога стоит строки в логе, пропущенная — тихой
		// потери данных.
		if !reportParsed(records, headerless) {
			failed++
			log.Warnf("report %s (%s) %s", report.URL, report.Period, zeroRecordReason(headerless))

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
				record.SourceKind,
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
// Ключ обязан опознавать ДОКУМЕНТ, а не только его вид. SourceKind различает
// пресс-релиз и аудированную отчётность — ось приоритета в v_company_financials,
// но не документ: два KPI-релиза, FY2023 и FY2024, несут один и тот же
// source_kind = "kpi", и на ключе из четырёх полей они по-прежнему сталкивались.
// Тогда ровно тот случай, ради которого витрина и заводилась, оставался
// невидимым: FY2023 печатает за 2023FY золото 2902, FY2024 за тот же период —
// 2799, и второе молча пропадало (79 повторов в батче вместо единиц). SourceURL
// и есть признак документа — он приходит в каждой записи (см. MetricRecord) и
// различает два релиза, у которых совпали все остальные поля.
//
// Порядок полей — порядок ORDER BY витрины: (company, metric, period, source_kind,
// source_url). Расходиться эти два места не имеют права: ключ, оставшийся уже
// ORDER BY, пропускает в batch строки, которые ClickHouse потом схлопнет, — и
// счётчик импортированных строк перестаёт совпадать с числом сохранённых (ровно
// та ошибка, от которой этот ключ и защищает).
//
// Metric и Period обязаны быть непустыми: пустой период дал бы один и тот же
// ключ (metric, "") для всех записей отчёта, который не удалось бы разобрать, и
// ClickHouse оставил бы ровно одну строку вместо всего отчёта.
type batchKey struct {
	Company    string
	Metric     string
	Period     string
	SourceKind string
	SourceURL  string
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
// Ключ здесь совпадает с ORDER BY витрины (см. batchKey): пропустить через
// проверку всё, что ClickHouse не схлопнет, — единственный способ довести обе
// версии до вставки, а ключ уже ORDER BY оставил бы в батче строки, которые
// ReplacingMergeTree потом схлопывает молча.
//
// Возвращает false, если ключ уже встречался; каждый повтор считает сам.
func (d *batchDedup) add(record MetricRecord) bool {
	if d.seen == nil {
		d.seen = make(map[batchKey]bool)
	}

	key := batchKey{record.Company, record.Metric, record.Period, record.SourceKind, record.SourceURL}
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

// reportParsed сообщает, разобран ли отчёт. Отчёт с нулём записей не разобран —
// и вот почему правило именно такое.
//
// Первая причина нуля записей — нераспознанная шапка. Она неотличима от успеха по
// возврату: страница, у которой не нашлась шапка финансовой таблицы, разбирается в
// ноль записей с err == nil. Счётчик нераспределённых значений этот отказ не
// ловит: колонок нет, поэтому ни одно значение не может быть отнесено к
// непериодной колонке, и unassigned равен нулю именно тогда, когда сигнал нужен.
// Направление счётчика поэтому обратное — см. parseKPILines: строки до первой
// шапки считает headerless, и ненулевое значение подтверждает, что страница несла
// строки метрик, которым не досталось ни одной колонки.
//
// Вторая причина — шапка найдена, но ни одна строка к ней не отнесена
// (headerless == 0). Это тоже отказ, хотя и другого вида: колонки есть, а метрик
// страница не дала. Отличать его от «в отчёте метрик нет» здесь нечем и не нужно:
// включённый отчёт в pages.go указывает на страницы с данными (Pages: {1} у
// русского релиза, {6, 7} у МСФО, {4} у FY2014), ни один — не на титул или
// содержание. Поэтому для включённого отчёта ноль записей аномален всегда, а
// ошибка направления несимметрична: ложная тревога стоит одной строки в логе,
// пропущенная — тихой потери данных. headerless при этом не условие отказа, а
// подробность для предупреждения: ноль записей ловится независимо от него, а его
// значение говорит читателю, какая из двух причин сработала.
//
// Строка с метрикой выше первой шапки бывает и на здоровой странице: на трёх
// включённых фикстурах с шапкой таких ноль, но английская МСФО-страница 6 без
// шапки финансовой таблицы даёт три («Total revenue», «Profit for the period»,
// «Profit for the period attributable to:»). Поэтому headerless и не может быть
// условием отказа: он не отделяет неразобранную страницу от здоровой, а лишь
// описывает её.
func reportParsed(records []MetricRecord, headerless int) bool {
	return len(records) > 0
}

// zeroRecordReason описывает предупреждение о пустом разборе включённого отчёта и
// различает две его причины.
//
// Ноль записей ловится независимо от headerless (см. reportParsed), но читателю
// лога важно, какая из двух причин сработала: у них разные следствия. Первая —
// шапка не распознана, то есть дело в форме страницы (метка единиц, форма
// периодов в шапке); вторая — шапка найдена, но ни одна строка к ней не отнесена,
// то есть дело в строках (метки не совпали со словарём метрик, значения не легли
// ни в одну колонку-период). Обе — отказ, и обе печатаются одной строкой Warn,
// потому что различаются только хвостом сообщения.
func zeroRecordReason(headerless int) string {
	if headerless > 0 {
		return fmt.Sprintf(
			"parsed to zero records with no error (%d metric line(s) before the first recognised header): its header was not recognised, so the page shape is the problem, not the absence of data",
			headerless,
		)
	}

	return "parsed to zero records with no error (0 metric lines before the first recognised header): its header was recognised but no metric rows were attributed to it, so the rows are the problem, not the absence of data"
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
