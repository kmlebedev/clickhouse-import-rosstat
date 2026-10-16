package util

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
)

// abortFakeBatch — батч под контракт путей ошибки после PrepareBatch: неотправленный батч
// обязан быть закрыт через Abort, иначе соединение не вернётся в пул драйвера, и серия ошибок
// разбора подвесит прогон в acquire() (утечка из диагноза rosstat_ipc_weeks_bulletins: 429 от
// Wayback, каждый сбой оставлял батч открытым). Фейк отвечает только на вопрос «позвали ли
// Abort» — возврат соединения в пул проверяется живым прогоном, но без первого недостижим.
type abortFakeBatch struct {
	driver.Batch
	appendErr error
	sendErr   error
	aborts    int
}

func (b *abortFakeBatch) Append(...any) error { return b.appendErr }

func (b *abortFakeBatch) Send() error { return b.sendErr }

func (b *abortFakeBatch) Rows() int { return 0 }

func (b *abortFakeBatch) Abort() error {
	b.aborts++
	return nil
}

// abortFakeConn отдаёт один заранее собранный батч. Exec нужен только UpsertSeriesCatalog
// (DDL и витрина) и всегда успешен: тест про батч, а не про DDL.
type abortFakeConn struct {
	driver.Conn
	batch *abortFakeBatch
}

func (c *abortFakeConn) Exec(context.Context, string, ...any) error { return nil }

func (c *abortFakeConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return c.batch, nil
}

func TestImportBodyAbortsBatchOnError(t *testing.T) {
	bodyErr := errors.New("BodyFunc: HTML заглушки WAF вместо бюллетеня")
	sendErr := errors.New("Send: сбой отправки")

	tests := []struct {
		name    string
		bodyErr error
		sendErr error
	}{
		{name: "ошибка BodyFunc", bodyErr: bodyErr},
		{name: "ошибка Send", sendErr: sendErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := &abortFakeBatch{sendErr: tt.sendErr}
			s := &ClickHouseImport{
				TableName: "tbl",
				BodyFunc:  func([]byte, driver.Batch) error { return tt.bodyErr },
			}
			_, err := s.importBody(context.Background(), []byte("body"), &abortFakeConn{batch: batch})
			want := tt.bodyErr
			if want == nil {
				want = tt.sendErr
			}
			if !errors.Is(err, want) {
				t.Fatalf("importBody вернула %v, ожидалась первопричина %v", err, want)
			}
			if batch.aborts != 1 {
				t.Fatalf("Abort вызван %d раз, ожидался один: неотправленный батч держит соединение вне пула", batch.aborts)
			}
		})
	}
}

func TestImportXlsBodyAbortsBatchOnImportFuncError(t *testing.T) {
	wb := excelize.NewFile()
	buf, err := wb.WriteToBuffer()
	if err != nil {
		t.Fatalf("excelize.WriteToBuffer: %v", err)
	}
	importErr := errors.New("ImportFunc: разбор книги")
	batch := &abortFakeBatch{}
	s := &ClickHouseImport{
		TableName:  "tbl",
		ImportFunc: func(*excelize.File, driver.Batch) error { return importErr },
	}
	if _, err = s.importXlsBody(context.Background(), buf.Bytes(), &abortFakeConn{batch: batch}); !errors.Is(err, importErr) {
		t.Fatalf("importXlsBody вернула %v, ожидалась первопричина %v", err, importErr)
	}
	if batch.aborts != 1 {
		t.Fatalf("Abort вызван %d раз, ожидался один", batch.aborts)
	}
}

func TestSavePublicationAbortsBatchOnError(t *testing.T) {
	appendErr := errors.New("Append: несовместимый тип")
	sendErr := errors.New("Send: сбой отправки")

	tests := []struct {
		name      string
		appendErr error
		sendErr   error
	}{
		{name: "ошибка Append", appendErr: appendErr},
		{name: "ошибка Send", sendErr: sendErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := &abortFakeBatch{appendErr: tt.appendErr, sendErr: tt.sendErr}
			err := SavePublication(context.Background(), &abortFakeConn{batch: batch}, Publication{Source: "s", Name: "n"})
			want := tt.appendErr
			if want == nil {
				want = tt.sendErr
			}
			if !errors.Is(err, want) {
				t.Fatalf("SavePublication вернула %v, ожидалась первопричина %v", err, want)
			}
			if batch.aborts != 1 {
				t.Fatalf("Abort вызван %d раз, ожидался один", batch.aborts)
			}
		})
	}
}

func TestUpsertSeriesCatalogAbortsBatchOnAppendError(t *testing.T) {
	appendErr := errors.New("Append: несовместимый тип")
	batch := &abortFakeBatch{appendErr: appendErr}
	err := UpsertSeriesCatalog(context.Background(), &abortFakeConn{batch: batch}, []SeriesMeta{{Source: "s", Series: "x"}})
	if !errors.Is(err, appendErr) {
		t.Fatalf("UpsertSeriesCatalog вернула %v, ожидалась первопричина %v", err, appendErr)
	}
	if batch.aborts != 1 {
		t.Fatalf("Abort вызван %d раз, ожидался один", batch.aborts)
	}
}
