package wgc

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeDataFile создаёт файл каталога данных с заданным mtime: findLatestFile ищет
// свежайшую выгрузку по времени изменения, а не по имени.
func writeDataFile(t *testing.T, path string, mtime time.Time) {
	t.Helper()
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatalf("Chtimes(%s): %v", path, err)
	}
}

// Имена выгрузок WGC содержат месяц прописью (Oct2026 лексикографически больше
// Nov2026), поэтому выбор обязан идти по mtime: «сентябрьский» файл, тронутый
// позже, должен победить файл с более поздним именем. Файл вне маски
// (Changes_latest_* — выгрузка ЦБ) не участвует, даже если он самый свежий.
func TestFindLatestFilePicksNewestByMtime(t *testing.T) {
	dir := t.TempDir()
	sep := filepath.Join(dir, "ETF_Flows_2026-09-01_0900.xlsx")
	oct := filepath.Join(dir, "ETF_Flows_2026-10-02_1401.xlsx")
	changes := filepath.Join(dir, "Changes_latest_as_of_Oct2026_IFS.xlsx")
	writeDataFile(t, sep, time.Date(2026, time.October, 3, 10, 0, 0, 0, time.UTC))
	writeDataFile(t, oct, time.Date(2026, time.October, 2, 14, 1, 0, 0, time.UTC))
	writeDataFile(t, changes, time.Date(2026, time.October, 4, 0, 0, 0, 0, time.UTC))

	got, err := findLatestFile(dir, etfFileMask)
	if err != nil {
		t.Fatalf("findLatestFile: %v", err)
	}
	if got != sep {
		t.Errorf("findLatestFile = %q, ожидался %q (выбор по mtime, а не по имени)", got, sep)
	}
}

// Одинаковый mtime — тай-брейк по имени по убыванию: иначе выбор зависел бы от
// порядка обхода каталога и импортёр читал бы то один файл, то другой.
func TestFindLatestFileTieBreaksByNameDesc(t *testing.T) {
	dir := t.TempDir()
	early := filepath.Join(dir, "ETF_Flows_a.xlsx")
	late := filepath.Join(dir, "ETF_Flows_b.xlsx")
	mtime := time.Date(2026, time.October, 2, 14, 1, 0, 0, time.UTC)
	writeDataFile(t, early, mtime)
	writeDataFile(t, late, mtime)

	got, err := findLatestFile(dir, etfFileMask)
	if err != nil {
		t.Fatalf("findLatestFile: %v", err)
	}
	if got != late {
		t.Errorf("findLatestFile = %q, ожидался %q (тай-брейк — имя по убыванию)", got, late)
	}
}

// Отсутствие файла — не ошибка: вызывающий сам решает, что это warn + 0 строк
// (dagu не должен краснеть до первого скачивания выгрузки).
func TestFindLatestFileNone(t *testing.T) {
	got, err := findLatestFile(t.TempDir(), etfFileMask)
	if err != nil {
		t.Fatalf("findLatestFile(пустой каталог): %v", err)
	}
	if got != "" {
		t.Errorf("findLatestFile(пустой каталог) = %q, ожидалось пусто", got)
	}
}
