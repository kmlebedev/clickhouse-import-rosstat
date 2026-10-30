package util

import (
	"context"
	"strings"
	"testing"
)

func TestREnvironmentExcludesSecrets(t *testing.T) {
	t.Setenv("X13_TEST_SECRET", "super-secret")
	t.Setenv("R_LIBS_USER", "/tmp/r-lib")
	joined := strings.Join(rEnvironment(), "\n")
	if strings.Contains(joined, "X13_TEST_SECRET") || strings.Contains(joined, "super-secret") {
		t.Fatal("окружение R содержит секрет конвейера")
	}
	if !strings.Contains(joined, "R_LIBS_USER=/tmp/r-lib") {
		t.Fatalf("окружение R не содержит R_LIBS_USER: %q", joined)
	}
}

func TestRunX13MissingRscript(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	if _, err := RunX13(context.Background(), []byte("script"), []byte("input"), "test"); err == nil {
		t.Fatal("ожидалась ошибка отсутствия Rscript")
	} else if !strings.Contains(err.Error(), "Rscript") {
		t.Fatalf("ошибка %q не упоминает Rscript", err)
	}
}

func TestTail(t *testing.T) {
	if got := tail("  short  ", 100); got != "short" {
		t.Fatalf("tail короткой строки %q", got)
	}
	if got := tail("abcdef", 3); got != "def" {
		t.Fatalf("tail длинной строки %q", got)
	}
}
