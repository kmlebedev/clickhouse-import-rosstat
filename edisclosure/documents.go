package edisclosure

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/archive"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// envInputDir — каталог снапшота коллектора (спека §5.3): manifest.json и
	// documents.jsonl, записанные scripts/edisclosure_collect.py.
	envInputDir = "EDISCLOSURE_INPUT_DIR"

	// envRecheck — глубокий прогон (значение «1»): документы скачиваются заново,
	// но строка переписывается только при смене содержимого или метаданных.
	envRecheck = "EDISCLOSURE_RECHECK"

	// envAllowMissing — пропустить прогон, если каталога входа нет: флаг ставит
	// только шаг dagu, чтобы ежемесячное расписание не падало на хосте, куда
	// коллектор ещё не доставил снапшот (спека §5.3).
	envAllowMissing = "EDISCLOSURE_ALLOW_MISSING"

	// defaultInputDir — каталог снапшота по умолчанию: выход make edisclosure-collect.
	defaultInputDir = "build/edisclosure"

	// edisclosureInsert — единственный INSERT прогона (спека §5.3 п. 6): порядок
	// колонок совпадает с appendDocument.
	edisclosureInsert = "INSERT INTO edisclosure_documents (ticker, company_id, section, doc_type, period, basis_date, publication_date, fileid, url, size_bytes, sha256, archive_key, first_seen)"
)

// getFileBytes и archiveFromEnv — точки подмены в тестах: офлайн-прогон не
// должен ни ходить в сеть, ни читать конфигурацию архива из окружения.
var (
	getFileBytes   = util.GetFileBytes
	archiveFromEnv = archive.FromEnv
)

// documentsImporter — импортёр edisclosure_documents: набор документов карточек
// e-disclosure (манифест коллектора + documents.jsonl), а не один артефакт с
// DataUrl, поэтому у него собственный Import() (прецедент cbr_dip_bulletin):
// гейт по таблице, скачивание каждого документа, винтажный архив и один батч.
type documentsImporter struct{}

// Name — имя импортёра: совпадает с таблицей и значением CLICKHOUSE_IMPORT_STAT
// в dagu/edisclosure.yaml. Оно же — source винтажного архива версий.
func (documentsImporter) Name() string {
	return edisclosureTable
}

// Import — полный цикл одного прогона (спека §5.3): чтение снапшота до сети и
// БД → DDL → витрина (на каждом прогоне, в том числе no-op) → гейт по таблице →
// единственный батч скачанных версий. Ошибка на любом шаге откатывает батч:
// частичных строк в таблице нет, а строка не объявляет версию, которой нет в
// архиве. Прогон без изменений не делает ни одного сетевого вызова.
func (documentsImporter) Import(ctx context.Context, conn driver.Conn) (int64, error) {
	inputDir := os.Getenv(envInputDir)
	if inputDir == "" {
		inputDir = defaultInputDir
	}
	recheck := os.Getenv(envRecheck) == "1"

	// 1. Снапшот читается и валидируется до сети и БД: ошибка любой строки —
	// fail-fast, ни одного запроса и ни одной записи.
	_, docs, err := readInput(inputDir)
	if err != nil {
		// errors.Is, а не os.IsNotExist: readInput оборачивает ошибку файла
		// через %w, и предикат os не разворачивает произвольную обёртку.
		if errors.Is(err, os.ErrNotExist) && os.Getenv(envAllowMissing) == "1" {
			log.Warnf("%s: снапшот %s не найден (%v), прогон пропущен: %s=1",
				edisclosureTable, inputDir, err, envAllowMissing)
			return 0, nil
		}
		return 0, fmt.Errorf("%s: вход %s: %w", edisclosureTable, inputDir, err)
	}

	// 2. DDL до гейта: прогон обязан отработать на чистой базе, где таблицы нет.
	if err = conn.Exec(ctx, edisclosureDdl); err != nil {
		return 0, fmt.Errorf("%s: DDL: %w", edisclosureTable, err)
	}

	// 3. Витрина публикуется на каждом прогоне: её сбой не должен застревать за
	// гейтом данных (спека §5.3 п. 3).
	if err = publishEdisclosureView(ctx, conn); err != nil {
		return 0, fmt.Errorf("%s: витрина: %w", edisclosureTable, err)
	}

	existing, err := loadExisting(ctx, conn)
	if err != nil {
		return 0, err
	}
	store, registry := openArchive()
	archiveEnabled := store != nil
	plan := planFetches(docs, existing, recheck, archiveEnabled)
	if len(plan) == 0 {
		log.Infof("%s: документов в манифесте %d, к загрузке 0, пропущено %d: no-op",
			edisclosureTable, len(docs), len(docs))
		return 0, nil
	}

	// 6. Один батч на весь план: Append с проверкой ошибки, Send в конце,
	// Abort при любом сбое (скачивание, архив, Append, Send).
	batch, err := conn.PrepareBatch(ctx, edisclosureInsert)
	if err != nil {
		return 0, fmt.Errorf("%s: батч не подготовлен: %w", edisclosureTable, err)
	}
	var written int64
	for _, d := range plan {
		body, err := getFileBytes(d.URL)
		if err != nil {
			return 0, abortBatch(batch, fmt.Errorf("%s: %s: скачивание: %w", edisclosureTable, d.URL, err))
		}
		if err = validateBody(body); err != nil {
			return 0, abortBatch(batch, fmt.Errorf("%s: %s: %w", edisclosureTable, d.URL, err))
		}
		sum := sha256.Sum256(body)
		sha := hex.EncodeToString(sum[:])

		// Версия сохраняется в архив до решения о записи строки: needsWrite
		// смотрит на archive_key, а строка обязана объявлять объект, который
		// уже лежит в хранилище (порядок Put → Ensure → Append, спека §5.5).
		key := ""
		if archiveEnabled {
			name := archiveName(d, bodyExt(body))
			key = archive.Key(edisclosureArchiveSource, name, sha, sum)
			if err = store.Put(ctx, key, body); err != nil {
				return 0, abortBatch(batch, fmt.Errorf("%s: %s: архив %s: %w",
					edisclosureTable, d.URL, name, err))
			}
			if err = registry.Ensure(ctx, archive.Entry{
				Source:      edisclosureArchiveSource,
				Name:        name,
				Fingerprint: sha,
				SHA256:      sha,
				Size:        int64(len(body)),
				URL:         d.URL,
				Key:         key,
				FirstSeen:   time.Now(),
			}); err != nil {
				return 0, abortBatch(batch, fmt.Errorf("%s: %s: реестр архива: %w",
					edisclosureTable, d.URL, err))
			}
		}

		e, ok := existing[docKey(d)]
		if !needsWrite(d, e, sha, key, archiveEnabled) {
			continue
		}
		// first_seen существующей строки сохраняется: это дата первой встречи
		// документа конвейером, а не дата текущего прогона.
		firstSeen := time.Now()
		if ok {
			firstSeen = e.FirstSeen
		}
		if err = appendDocument(batch, d, int64(len(body)), sha, key, firstSeen); err != nil {
			return 0, abortBatch(batch, fmt.Errorf("%s: fileid %s: %w",
				edisclosureTable, d.FileID, err))
		}
		written++
	}
	if err = batch.Send(); err != nil {
		return 0, abortBatch(batch, fmt.Errorf("%s: отправка батча: %w", edisclosureTable, err))
	}
	log.Infof("%s: документов в манифесте %d, к загрузке %d, пропущено %d",
		edisclosureTable, len(docs), len(plan), len(docs)-len(plan))
	return written, nil
}

// edisclosureImporter — единственный импортёр пакета; edisclosureImporters
// перечисляет его для регистрации в chimport.Stats.
var edisclosureImporter = documentsImporter{}

// existingDoc — строка edisclosure_documents, как её видит гейт: метаданные
// карточки, версия содержимого (sha256) и ключ объекта архива. Ключа
// идентичности (ticker, section, fileid) в структуре нет: карта loadExisting
// хранит строки под docKey, и дублировать его в значении незачем. FirstSeen
// нужен, чтобы при перезаписи сохранить дату первой встречи документа.
type existingDoc struct {
	DocType         string
	Period          string
	PublicationDate time.Time
	URL             string
	SHA256          string
	ArchiveKey      string
	FirstSeen       time.Time
}

// metadataMatches — совпали ли метаданные карточки (doc_type, period,
// publication_date, url): ровно они, по спеке §5.3 п. 4, делают документ
// изменившимся. Ticker/section/fileid — ключ, по которому строка найдена, их
// сравнивать не нужно. Даты сравниваются как моменты (Equal): zero time равен
// zero time, поэтому пустая дата в карточке не выглядит изменением.
func (d doc) metadataMatches(e existingDoc) bool {
	return d.DocType == e.DocType &&
		d.Period == e.Period &&
		d.URL == e.URL &&
		d.PublicationDate.Equal(e.PublicationDate)
}

// planFetches — план скачивания: документы, для которых нужен сетевой запрос.
// Пропуск без сети допустим, только если строка уже есть, RECHECK не задан,
// метаданные карточки совпали и архив либо выключен, либо версия уже лежит в
// архиве (archive_key непуст). Пустой archive_key при включённом архиве —
// повод скачать заново: так строка, записанная при выключенном архиве,
// самолечится до полноценной версии (спека §5.3 п. 4). Порядок документов
// сохраняется: батч и лог идут в порядке манифеста.
func planFetches(docs []doc, existing map[string]existingDoc, recheck, archiveEnabled bool) []doc {
	plan := make([]doc, 0, len(docs))
	for _, d := range docs {
		e, ok := existing[docKey(d)]
		if ok && !recheck && d.metadataMatches(e) && (!archiveEnabled || e.ArchiveKey != "") {
			continue
		}
		plan = append(plan, d)
	}
	return plan
}

// needsWrite — нужна ли запись строки после скачивания. Документ с RECHECK
// скачивается всегда, но строка переписывается только при смене версии
// (sha256) или метаданных карточки: иначе глубокий прогон каждый раз плодил бы
// версии ReplacingMergeTree без изменения содержимого. Отдельный случай —
// заполнение пустого archive_key при включённом архиве: содержимое то же, но
// строка ещё не объявила версию, сохранённую в архиве. Нулевая existingDoc
// означает отсутствие строки: валидная строка таблицы нулевой быть не может
// (doc_type, url и sha256 непусты, first_seen заполнен).
func needsWrite(d doc, e existingDoc, sha, archiveKey string, archiveEnabled bool) bool {
	if e == (existingDoc{}) {
		return true
	}
	if !d.metadataMatches(e) {
		return true
	}
	if sha != e.SHA256 {
		return true
	}
	if archiveEnabled && archiveKey != "" && e.ArchiveKey == "" {
		return true
	}
	return false
}

// validateBody — проверка скачанного тела до sha256 и архива: пустое тело и
// HTML вместо файла означают, что FileLoad.ashx ответил страницей ошибки или
// заглушкой, а не документом. Проверяется первый непробельный байт: HTML может
// начинаться с пробелов, поэтому смотреть на body[0] недостаточно. Zip, rar,
// pdf и прочие бинарные тела допустимы — расширение определит bodyExt.
func validateBody(body []byte) error {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return errors.New("тело ответа пусто: документ не скачан")
	}
	if trimmed[0] == '<' {
		return errors.New("тело ответа: HTML вместо файла")
	}
	return nil
}

// loadExisting — гейт прогона (спека §5.3 п. 4): существующие строки под docKey.
// FINAL обязателен: без него ReplacingMergeTree отдал бы и устаревшие версии, и
// гейт увидел бы не ту строку. publication_date — Nullable(Date), поэтому
// читается в sql.NullTime, а нулевое время в existingDoc означает «даты нет».
func loadExisting(ctx context.Context, conn driver.Conn) (map[string]existingDoc, error) {
	rows, err := conn.Query(ctx, "SELECT ticker, section, fileid, doc_type, period, publication_date, url, sha256, archive_key, first_seen FROM edisclosure_documents FINAL")
	if err != nil {
		return nil, fmt.Errorf("%s: существующие строки не прочитаны: %w", edisclosureTable, err)
	}
	defer func() { _ = rows.Close() }()

	existing := make(map[string]existingDoc)
	for rows.Next() {
		var (
			ticker, section, fileid string
			docType, period         string
			publication             sql.NullTime
			url, sha, archiveKey    string
			firstSeen               time.Time
		)
		if err = rows.Scan(&ticker, &section, &fileid, &docType, &period, &publication,
			&url, &sha, &archiveKey, &firstSeen); err != nil {
			return nil, fmt.Errorf("%s: строка таблицы не прочитана: %w", edisclosureTable, err)
		}
		e := existingDoc{
			DocType:    docType,
			Period:     period,
			URL:        url,
			SHA256:     sha,
			ArchiveKey: archiveKey,
			FirstSeen:  firstSeen,
		}
		if publication.Valid {
			e.PublicationDate = publication.Time
		}
		existing[docKey(doc{Ticker: ticker, Section: section, FileID: fileid})] = e
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: существующие строки не дочитаны: %w", edisclosureTable, err)
	}
	return existing, nil
}

// openArchive — винтажный архив версий (спека §5.5). Ошибка конфигурации не
// валит прогон: недоступный архив не должен останавливать данные, поэтому она
// логируется, а архив считается выключенным (nil, nil) — строки уйдут с пустым
// archive_key. Пустой store — штатное «архив выключен» (ARCHIVE_BACKEND пуст).
func openArchive() (archive.Store, *archive.Registry) {
	store, err := archiveFromEnv(util.HttpClient)
	if err != nil {
		log.Errorf("%s: архив версий выключен: %v", edisclosureTable, err)
		return nil, nil
	}
	if store == nil {
		log.Warnf("%s: архив версий выключен (%s пуст): archive_key будет пустым",
			edisclosureTable, archive.EnvBackend)
		return nil, nil
	}
	return store, archive.NewRegistry(store)
}

// appendDocument — строка батча в порядке колонок edisclosureInsert. Нулевые
// даты уходят как nil: basis_date и publication_date — Nullable(Date), и
// time.Time{} драйвер записал бы как 1970-01-01, а не NULL. size_bytes —
// UInt64, поэтому размер конвертируется из int64 без знака.
func appendDocument(batch driver.Batch, d doc, size int64, sha, key string, firstSeen time.Time) error {
	return batch.Append(
		d.Ticker, d.CompanyID, d.Section, d.DocType, d.Period,
		nullDate(d.BasisDate), nullDate(d.PublicationDate),
		d.FileID, d.URL, uint64(size), sha, key, firstSeen,
	)
}

// nullDate — значение Nullable(Date): nil для отсутствующей даты.
func nullDate(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// abortBatch откатывает батч и возвращает исходную ошибку. Ошибка отката не
// заменяет причину сбоя, а присоединяется к ней: дежурный должен видеть и
// скачивание/архив, и то, что батч не удалось отменить.
func abortBatch(batch driver.Batch, err error) error {
	if abortErr := batch.Abort(); abortErr != nil {
		return errors.Join(err, fmt.Errorf("%s: откат батча: %w", edisclosureTable, abortErr))
	}
	return err
}
