package util

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
)

// abortFakeBatch — батч под контракт путей ошибки после PrepareBatch: Abort обязателен только на
// путях ДО Send (ImportFunc/BodyFunc/Append): неотправленный батч держит соединение вне пула
// драйвера, и серия ошибок разбора подвесит прогон в acquire() (утечка из диагноза
// rosstat_ipc_weeks_bulletins: 429 от Wayback, каждый сбой оставлял батч открытым). После Send
// абортить не нужно: Send возвращает соединение в пул дефером независимо от исхода
// (clickhouse-go/v2, conn_batch.go), а Abort после Send отвечает ErrBatchAlreadySent — то есть
// дал бы ложный warn «батч не закрыт», а не лечение утечки. Фейк отвечает только на вопрос
// «позвали ли Abort» — возврат соединения в пул проверяется живым прогоном, но без первого
// недостижим.
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

// TestImportBodyAbortsBatchOnError — пути ошибки importBody: сбой разбора (до Send) закрывает
// батч через Abort, а сбой Send — нет: соединение возвращает в пул сам Send, и Abort после него
// дал бы ErrBatchAlreadySent с ложным warn.
func TestImportBodyAbortsBatchOnError(t *testing.T) {
	bodyErr := errors.New("BodyFunc: HTML заглушки WAF вместо бюллетеня")
	sendErr := errors.New("Send: сбой отправки")

	tests := []struct {
		name       string
		bodyErr    error
		sendErr    error
		wantAborts int
	}{
		{name: "ошибка BodyFunc", bodyErr: bodyErr, wantAborts: 1},
		{name: "ошибка Send", sendErr: sendErr, wantAborts: 0},
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
			if batch.aborts != tt.wantAborts {
				t.Fatalf("Abort вызван %d раз, ожидалось %d: до Send неотправленный батч держит соединение вне пула, после Send его возвращает сам Send", batch.aborts, tt.wantAborts)
			}
		})
	}
}

// TestImportXlsBodyAbortsBatchOnError — пути ошибки importXlsBody: сбой ImportFunc (до Send)
// закрывает батч через Abort, сбой Send — нет (соединение вернул сам Send).
func TestImportXlsBodyAbortsBatchOnError(t *testing.T) {
	wb := excelize.NewFile()
	buf, err := wb.WriteToBuffer()
	if err != nil {
		t.Fatalf("excelize.WriteToBuffer: %v", err)
	}
	importErr := errors.New("ImportFunc: разбор книги")
	sendErr := errors.New("Send: сбой отправки")
	tests := []struct {
		name       string
		importErr  error
		sendErr    error
		wantErr    error
		wantAborts int
	}{
		{name: "ошибка ImportFunc", importErr: importErr, wantErr: importErr, wantAborts: 1},
		{name: "ошибка Send", sendErr: sendErr, wantErr: sendErr, wantAborts: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := &abortFakeBatch{sendErr: tt.sendErr}
			s := &ClickHouseImport{
				TableName:  "tbl",
				ImportFunc: func(*excelize.File, driver.Batch) error { return tt.importErr },
			}
			if _, err := s.importXlsBody(context.Background(), buf.Bytes(), &abortFakeConn{batch: batch}); !errors.Is(err, tt.wantErr) {
				t.Fatalf("importXlsBody вернула %v, ожидалась первопричина %v", err, tt.wantErr)
			}
			if batch.aborts != tt.wantAborts {
				t.Fatalf("Abort вызван %d раз, ожидалось %d", batch.aborts, tt.wantAborts)
			}
		})
	}
}

// TestSavePublicationAbortsBatchOnError — пути ошибки SavePublication: сбой Append (до Send)
// закрывает батч через Abort, сбой Send — нет (соединение вернул сам Send, Abort после него —
// ErrBatchAlreadySent с ложным warn).
func TestSavePublicationAbortsBatchOnError(t *testing.T) {
	appendErr := errors.New("Append: несовместимый тип")
	sendErr := errors.New("Send: сбой отправки")

	tests := []struct {
		name       string
		appendErr  error
		sendErr    error
		wantAborts int
	}{
		{name: "ошибка Append", appendErr: appendErr, wantAborts: 1},
		{name: "ошибка Send", sendErr: sendErr, wantAborts: 0},
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
			if batch.aborts != tt.wantAborts {
				t.Fatalf("Abort вызван %d раз, ожидалось %d: до Send неотправленный батч держит соединение вне пула, после Send его возвращает сам Send", batch.aborts, tt.wantAborts)
			}
		})
	}
}

// TestUpsertSeriesCatalogAbortsBatchOnError — пути ошибки UpsertSeriesCatalog: сбой Append (до
// Send) закрывает батч через Abort, сбой Send — нет.
func TestUpsertSeriesCatalogAbortsBatchOnError(t *testing.T) {
	appendErr := errors.New("Append: несовместимый тип")
	sendErr := errors.New("Send: сбой отправки")
	tests := []struct {
		name       string
		appendErr  error
		sendErr    error
		wantErr    error
		wantAborts int
	}{
		{name: "ошибка Append", appendErr: appendErr, wantErr: appendErr, wantAborts: 1},
		{name: "ошибка Send", sendErr: sendErr, wantErr: sendErr, wantAborts: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := &abortFakeBatch{appendErr: tt.appendErr, sendErr: tt.sendErr}
			err := UpsertSeriesCatalog(context.Background(), &abortFakeConn{batch: batch}, []SeriesMeta{{Source: "s", Series: "x"}})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("UpsertSeriesCatalog вернула %v, ожидалась первопричина %v", err, tt.wantErr)
			}
			if batch.aborts != tt.wantAborts {
				t.Fatalf("Abort вызван %d раз, ожидалось %d", batch.aborts, tt.wantAborts)
			}
		})
	}
}
