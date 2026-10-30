package rosstat

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ppiRecordingBatch — общая подмена driver.Batch для тестов T2: записывает
// аргументы Append как есть, чтобы тест мог проверить и порядок колонок, и
// типы. T1 использует собственный ppiVedBatchRecorder с пятью именованными
// полями; здесь важен общий контракт для двух новых таблиц.
type ppiRecordingBatch struct {
	rows [][]any
}

var _ driver.Batch = (*ppiRecordingBatch)(nil)

func (r *ppiRecordingBatch) Append(v ...any) error {
	r.rows = append(r.rows, append([]any(nil), v...))
	return nil
}

func (r *ppiRecordingBatch) Abort() error { return nil }

func (r *ppiRecordingBatch) AppendStruct(any) error {
	return fmt.Errorf("ppiRecordingBatch: AppendStruct не поддерживается")
}

func (r *ppiRecordingBatch) Column(int) driver.BatchColumn { return nil }

func (r *ppiRecordingBatch) Columns() []column.Interface { return nil }

func (r *ppiRecordingBatch) Flush() error { return nil }

func (r *ppiRecordingBatch) Send() error { return nil }

func (r *ppiRecordingBatch) IsSent() bool { return false }

func (r *ppiRecordingBatch) Rows() int { return len(r.rows) }
