package edisclosure

import (
	"context"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Контракт объявления публикатора витрины: витрина публикуется на каждом прогоне
// импортёра, в том числе на no-op (спека §5.3). Присваивание пустой переменной
// ожидаемой сигнатуры держит символ используемым до Task 5 и не соберётся, если
// publishEdisclosureView пропадёт из пакета или сменит сигнатуру. Сравнить функцию
// с nil нельзя: go vet (nilfunc) и staticcheck (SA4031) отвергают такую проверку
// как заведомо ложную для объявленной функции.
var _ func(context.Context, driver.Conn) error = publishEdisclosureView

// TestEdisclosureViewShape держит контракт витрины: имя, единственный источник,
// FINAL и комментарий каждой из 14 колонок DDL (спека §5.6). Список колонок —
// литералами: ссылка на Columns сделала бы проверку тавтологичной.
func TestEdisclosureViewShape(t *testing.T) {
	if edisclosureView.Name != "v_edisclosure_documents" {
		t.Fatalf("view = %q", edisclosureView.Name)
	}
	if len(edisclosureView.Tables) != 1 || edisclosureView.Tables[0] != "edisclosure_documents" {
		t.Fatalf("Tables = %v", edisclosureView.Tables)
	}
	if !strings.Contains(edisclosureView.Select, "FROM edisclosure_documents FINAL") {
		t.Fatalf("Select = %q", edisclosureView.Select)
	}
	wantColumns := []string{"ticker", "company_id", "section", "doc_type", "period",
		"basis_date", "publication_date", "fileid", "url", "size_bytes", "sha256",
		"archive_key", "first_seen", "loaded_at"}
	if len(edisclosureView.Columns) != 14 {
		t.Fatalf("Columns = %d, want 14 (все колонки DDL спеки §5.2)", len(edisclosureView.Columns))
	}
	for _, col := range wantColumns {
		if _, ok := edisclosureView.Columns[col]; !ok {
			t.Errorf("нет комментария колонки %q", col)
		}
	}
}
