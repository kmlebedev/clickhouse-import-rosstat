package wgc

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Имя импортёра — контракт расписания (dagu/wgc.yaml), ручного запуска
// (make import STAT=wgc_etf_flows) и source строк в macro_series/series_catalog.
func TestEtfFlowsStatName(t *testing.T) {
	if got := (&etfFlowsStat{}).Name(); got != "wgc_etf_flows" {
		t.Fatalf("Name() = %q, ожидалось wgc_etf_flows", got)
	}
}

// Пустая папка данных — не ошибка (warn + 0 строк): dagu не должен краснеть до
// первого скачивания выгрузки. conn при этом не трогается вовсе — тест зовёт
// Import с nil и упал бы паникой уже на первом обращении к соединению.
func TestEtfImportEmptyDirIsNoop(t *testing.T) {
	stat := etfFlowsStat{dir: t.TempDir()}
	count, err := stat.Import(context.Background(), nil)
	if err != nil {
		t.Fatalf("Import пустого каталога: %v", err)
	}
	if count != 0 {
		t.Errorf("Import пустого каталога = %d строк, ожидалось 0", count)
	}
}

// Битый файл — ошибка импорта (fail-fast, урок eia): тихий ноль оставил бы dagu
// зелёным, а ряды — без обновления. Ошибка обязана называть файл: по каталогу
// wgc/data лежат выгрузки разных задач, и «что-то не открылось» неотличимо.
func TestEtfImportBrokenFileFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ETF_Flows_broken.xlsx")
	if err := os.WriteFile(path, []byte("не xlsx"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	stat := etfFlowsStat{dir: dir}
	_, err := stat.Import(context.Background(), nil)
	if err == nil {
		t.Fatal("битый файл должен давать ошибку, а не тихий ноль")
	}
	if !strings.Contains(err.Error(), "ETF_Flows_broken.xlsx") {
		t.Errorf("ошибка не называет файл: %v", err)
	}
}
