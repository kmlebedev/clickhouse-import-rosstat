package edisclosure

import (
	"context"
	"errors"

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
