package polyus

import (
	"os"
	"path/filepath"
	"testing"
)

func TestJoinFiles(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.txt")
	b := filepath.Join(dir, "b.txt")
	if err := os.WriteFile(a, []byte("Gold sales\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, []byte("Total revenue\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "joined.txt")
	if err := joinFiles([]string{a, b}, out); err != nil {
		t.Fatalf("joinFiles: %v", err)
	}
	text, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(text) != "Gold sales\nTotal revenue\n" {
		t.Errorf("joined text = %q", text)
	}
}

func TestJoinFilesMissingFileErrors(t *testing.T) {
	dir := t.TempDir()
	if err := joinFiles([]string{filepath.Join(dir, "absent.txt")}, filepath.Join(dir, "out.txt")); err == nil {
		t.Error("expected error for missing input file")
	}
}
