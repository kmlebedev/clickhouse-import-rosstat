package edisclosure

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

var daguStatRef = regexp.MustCompile(`(?m)^[ \t]*-[ \t]+CLICKHOUSE_IMPORT_STAT=([A-Za-z0-9_]+)[ \t]*$`)

func TestImportersRegistry(t *testing.T) {
	if got := edisclosureImporter.Name(); got != "edisclosure_documents" {
		t.Fatalf("Name() = %q, want edisclosure_documents", got)
	}
	// source винтажного архива равен имени импортёра и таблицы (спека §5.5):
	// ключи archive.Key начинаются с edisclosure_documents/. Проверка литералом,
	// а не ссылкой на edisclosureTable: иначе расхождение констант было бы не видно.
	if got := edisclosureArchiveSource; got != "edisclosure_documents" {
		t.Fatalf("archive source = %q, want edisclosure_documents", got)
	}
	found := false
	for _, stat := range chimport.Stats {
		if stat.Name() == "edisclosure_documents" {
			found = true
		}
	}
	if !found {
		t.Fatal("edisclosure_documents не зарегистрирован в chimport.Stats")
	}
	data, err := os.ReadFile(filepath.Join("..", "dagu", "edisclosure.yaml"))
	if err != nil {
		t.Fatalf("чтение dagu/edisclosure.yaml: %v", err)
	}
	steps := daguStatRef.FindAllStringSubmatch(string(data), -1)
	if len(steps) != 1 || steps[0][1] != "edisclosure_documents" {
		t.Fatalf("шаги dagu = %v, want один edisclosure_documents", steps)
	}
}

// TestEdisclosureDdlShape держит дословную копию спеки §5.2: движок с версией
// loaded_at, ключ идентичности документа и колонки версии/архива. Проверяются
// литералы, а не константы пакета: сравнение константы с самой собой прошло бы
// и после правки DDL мимо спеки.
func TestEdisclosureDdlShape(t *testing.T) {
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS edisclosure_documents",
		"ENGINE = ReplacingMergeTree(loaded_at)",
		"ORDER BY (ticker, section, fileid)",
		"archive_key String", "sha256 String", "first_seen DateTime",
	} {
		if !strings.Contains(edisclosureDdl, want) {
			t.Errorf("DDL не содержит %q", want)
		}
	}
}
