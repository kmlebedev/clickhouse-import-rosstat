package edisclosure

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// documentsImporter — импортёр edisclosure_documents: набор документов карточек
// e-disclosure (манифест коллектора + documents.jsonl), а не один артефакт с
// DataUrl, поэтому у него собственный Import() (прецедент cbr_dip_bulletin).
// В Task 2 это каркас: DDL, витрина и реестр уже объявлены, а тело Import
// заменяется в Task 5 (гейт по таблице, скачивание, архив, один батч).
type documentsImporter struct{}

// Name — имя импортёра: совпадает с таблицей и значением CLICKHOUSE_IMPORT_STAT
// в dagu/edisclosure.yaml. Оно же — source винтажного архива версий.
func (documentsImporter) Name() string {
	return edisclosureTable
}

// Import — заглушка Task 2: тело появится в Task 5. Возвращать ошибку, а не
// тихий no-op, обязательно: прогон по расписанию должен падать громко, пока
// импортёр ничего не умеет.
func (documentsImporter) Import(ctx context.Context, conn driver.Conn) (int64, error) {
	return 0, errors.New("edisclosure_documents: not implemented in task 2")
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
