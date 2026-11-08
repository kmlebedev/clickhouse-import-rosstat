package util

// Общий резервный источник: набор документов (PDF/HTML), из которых в таблицу
// основного импортёра добираются периоды, которых в ней ещё нет. Прецеденты —
// cbr_infl_exp_op (оперативная точка из inFOM_ГГ-ММ.pdf до выхода книги) и
// rosstat_ipc_weeks (PDF-выпуски новостной ленты при 404/устаревшем Nedel_ipc.xlsx).
//
// Механизм не решает, «свежий ли основной файл»: основной импортёр пишет свои точки
// первым, а Fallback смотрит на множество дат, уже лежащих в таблице. Поэтому один и
// тот же движок работает и отдельным шагом (CBR), и внутри шага основного импортёра
// (rosstat, где отдельный шаг после падающего XLSX в type: chain не запустится).

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
)

// FallbackDocument — документ резервного источника: дата периода, к которой относятся
// его точки (последний день недели/месяца, а не дата публикации), и адрес файла.
type FallbackDocument struct {
	Date time.Time
	URL  string
}

// FallbackRow — строка резервного источника. Имена и единицы задаёт Parse; в какие
// колонки таблицы её писать, решает Append.
type FallbackRow struct {
	Series string
	Date   time.Time
	Value  float64
}

// FallbackResult — итог прогона: сколько документов вернул источник, сколько дат не
// было в таблице, сколько точек записано и какие ошибки накопились. Published — признак,
// что AfterImport уже вызван (шаблоном на документе или механизмом на no-op-пути).
type FallbackResult struct {
	Documents int
	Missing   int
	Imported  int64
	Published bool
	Errors    []error
}

// Err — накопленные ошибки одним значением; errors.Join(nil...) == nil.
func (r FallbackResult) Err() error {
	return errors.Join(r.Errors...)
}

// Fallback — общий движок резервного источника.
type Fallback struct {
	// Template — шаблон целевой таблицы: TableName, CreateTable, PublicationSource,
	// PublicationCountPoints и AfterImport. Fallback подменяет DataUrl и BodyFunc на
	// каждый документ, поэтому ImportFunc/CrawFunc/BeforeImport у шаблона не используются.
	Template ClickHouseImport
	// Documents возвращает документы резерва; порядок не важен — Fallback сортирует по дате.
	Documents func(ctx context.Context) ([]FallbackDocument, error)
	// Parse разбирает байты документа в строки. doc.Date — ожидаемая дата; Fallback
	// проверяет, что каждая строка вернулась именно с ней.
	Parse func(ctx context.Context, doc FallbackDocument, body []byte) ([]FallbackRow, error)
	// KnownDatesQuery — SQL дат, уже лежащих в таблице. Обязателен FINAL: иначе
	// count/даты считали бы несмёрженные части.
	KnownDatesQuery string
	// Append пишет строку в батч: колонки и типы у таблиц разные.
	Append func(batch driver.Batch, row FallbackRow) error
}

// FallbackKnownDates читает даты целевой таблицы. Множество, а не max(date): у набора
// документов возможны разрывы, и пропущенную середину тоже надо лечить. Ошибка чтения —
// ошибка прогона, а не «дат нет»: иначе каждый прогон пере-импортировал бы весь набор.
func FallbackKnownDates(ctx context.Context, conn driver.Conn, query string) (map[string]bool, error) {
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("fallback: пустой запрос дат")
	}
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("fallback: даты таблицы не прочитаны: %w", err)
	}
	defer func() { _ = rows.Close() }()

	known := make(map[string]bool)
	for rows.Next() {
		var date time.Time
		if err = rows.Scan(&date); err != nil {
			return nil, fmt.Errorf("fallback: строка дат таблицы не прочитана: %w", err)
		}
		known[date.Format("2006-01-02")] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("fallback: даты таблицы не дочитаны: %w", err)
	}
	return known, nil
}

// FallbackMissing — документы, дат которых нет в known, в хронологическом порядке.
// Дедуп по дате: повторная публикация того же периода (пере-нумерованный файл) не должна
// давать двух скачиваний; остаётся первый документ входного порядка.
func FallbackMissing(docs []FallbackDocument, known map[string]bool) []FallbackDocument {
	seen := make(map[string]bool, len(docs))
	missing := make([]FallbackDocument, 0, len(docs))
	for _, doc := range docs {
		key := doc.Date.Format("2006-01-02")
		if known[key] || seen[key] {
			continue
		}
		seen[key] = true
		missing = append(missing, doc)
	}
	sort.SliceStable(missing, func(i, j int) bool {
		return missing[i].Date.Before(missing[j].Date)
	})
	return missing
}

// Import — цикл резервного источника: DDL → документы → known → недостающие документы
// по одному через шаблон (DataUrl + BodyFunc) → проверка, что даты записались →
// производные. Сбой одного документа не отменяет остальные: ошибки копятся errors.Join.
//
// AfterImport на локальной копии шаблона сохраняется: шаблон вызывает его после точек и
// до строки версии (ARCHITECTURE §8 п. 13), поэтому версия CBR и винтажный архив PDF
// остаются в прежнем порядке. На no-op-пути (все даты известны) AfterImport вызывается
// один раз здесь же — идемпотентная публикация лечит каталог и витрину прошлого сбоя.
func (f *Fallback) Import(ctx context.Context, conn driver.Conn) (FallbackResult, error) {
	var res FallbackResult
	if f.Documents == nil || f.Parse == nil || f.Append == nil {
		res.Errors = append(res.Errors, fmt.Errorf("fallback: не заданы Documents/Parse/Append"))
		return res, res.Err()
	}
	if f.Template.TableName == "" {
		res.Errors = append(res.Errors, fmt.Errorf("fallback: не задан TableName"))
		return res, res.Err()
	}
	for _, ddl := range f.Template.CreateTable {
		if err := conn.Exec(ctx, fmt.Sprintf(ddl, f.Template.TableName)); err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("%s: DDL: %w", f.Template.PublicationKey(), err))
			return res, res.Err()
		}
	}

	docs, err := f.Documents(ctx)
	if err != nil {
		res.Errors = append(res.Errors, err)
		return res, res.Err()
	}
	res.Documents = len(docs)
	for _, doc := range docs {
		if doc.Date.IsZero() || strings.TrimSpace(doc.URL) == "" {
			res.Errors = append(res.Errors, fmt.Errorf("fallback: документ с пустой датой или URL (%q)", doc.URL))
		}
	}
	if err = res.Err(); err != nil {
		return res, err
	}

	known, err := FallbackKnownDates(ctx, conn, f.KnownDatesQuery)
	if err != nil {
		res.Errors = append(res.Errors, err)
		return res, res.Err()
	}
	missing := FallbackMissing(docs, known)
	res.Missing = len(missing)
	if len(missing) == 0 {
		log.Infof("%s: резервные периоды на месте (%d документов, %d дат)",
			f.Template.PublicationKey(), len(docs), len(known))
		if f.Template.AfterImport != nil {
			if err = f.Template.AfterImport(ctx, conn); err != nil {
				res.Errors = append(res.Errors, err)
				return res, res.Err()
			}
			res.Published = true
		}
		return res, nil
	}

	failed := make(map[string]bool, len(missing))
	for _, doc := range missing {
		doc := doc
		key := doc.Date.Format("2006-01-02")
		local := f.Template
		local.DataUrl = doc.URL
		local.CreateTable = nil
		local.ImportFunc = nil
		local.CrawFunc = nil
		local.BeforeImport = nil
		local.BodyFunc = func(body []byte, batch driver.Batch) error {
			rows, err := f.Parse(ctx, doc, body)
			if err != nil {
				return fmt.Errorf("%s: %w", doc.URL, err)
			}
			if len(rows) == 0 {
				return fmt.Errorf("%s: разбор дал 0 строк", doc.URL)
			}
			for _, row := range rows {
				if !row.Date.Equal(doc.Date) {
					return fmt.Errorf("%s: дата строки %s не совпала с датой документа %s",
						doc.URL, row.Date.Format("2006-01-02"), doc.Date.Format("2006-01-02"))
				}
				if err = f.Append(batch, row); err != nil {
					return fmt.Errorf("%s: ряд %q, %s: %w",
						doc.URL, row.Series, row.Date.Format("2006-01-02"), err)
				}
			}
			return nil
		}
		n, importErr := local.Import(ctx, conn)
		if importErr != nil {
			failed[key] = true
			res.Errors = append(res.Errors, fmt.Errorf("%s: %w", doc.URL, importErr))
			continue
		}
		res.Imported += n
		if f.Template.AfterImport != nil {
			res.Published = true
		}
		log.Infof("%s: резервный документ %s (%s): импортировано %d точек",
			f.Template.PublicationKey(), key, doc.URL, n)
	}

	// Успешный вызов шаблона записи не гарантирует: префлайт умеет вернуть no-op
	// («версия известна»), и тогда дата осталась бы молча незаписанной. Сверяем даты
	// после цикла и делаем такую дыру громкой. Документы, упавшие с ошибкой, уже
	// названы в res.Errors — для них отдельная диагностика была бы шумом.
	after, err := FallbackKnownDates(ctx, conn, f.KnownDatesQuery)
	if err != nil {
		res.Errors = append(res.Errors, err)
		return res, res.Err()
	}
	for _, doc := range missing {
		key := doc.Date.Format("2006-01-02")
		if failed[key] || after[key] {
			continue
		}
		res.Errors = append(res.Errors, fmt.Errorf(
			"%s: дата %s не появилась в таблице — шаблон вернул no-op; удалите строку версии из source_publications и повторите прогон",
			doc.URL, key))
	}
	return res, res.Err()
}
