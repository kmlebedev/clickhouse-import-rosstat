package edisclosure

import (
	"strings"
	"testing"
)

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
	for _, col := range []string{"ticker", "company_id", "section", "doc_type", "period",
		"basis_date", "publication_date", "fileid", "url", "size_bytes", "sha256",
		"archive_key", "first_seen", "loaded_at"} {
		if _, ok := edisclosureView.Columns[col]; !ok {
			t.Errorf("нет комментария колонки %q", col)
		}
	}
}
