package wgc

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
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

// newHeaderOnlyEtfWorkbook собирает воркбук со всеми четырьмя листами и шапками,
// но без строк данных: так выглядит выгрузка, если данные не попали в файл.
// Колонка B листов фондов (справочные итоги, парсер её не читает) заполнена в
// строках 1..7, чтобы GetRows вернул непрерывный диапазон и размер листа дошёл до
// проверки «данные с 7-й строки»; с дырой в шапке парсер упал бы раньше guard-а,
// и тест закреплял бы не тот fail-fast путь.
func newHeaderOnlyEtfWorkbook(t *testing.T) *excelize.File {
	t.Helper()

	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })

	sheets := []string{etfFlowsSheet, etfDemandSheet, etfHoldingsSheet, etfChartsSheet}
	if err := f.SetSheetName("Sheet1", sheets[0]); err != nil {
		t.Fatalf("SetSheetName(%s): %v", sheets[0], err)
	}
	for _, sheet := range sheets[1:] {
		if _, err := f.NewSheet(sheet); err != nil {
			t.Fatalf("NewSheet(%s): %v", sheet, err)
		}
	}

	for _, sheet := range sheets[:3] {
		refs := map[string]string{
			"F4": "North America", "G4": "Europe", "H4": "Asia", "I4": "Other",
			"B1": "0", "B2": "0", "B3": "0", "B4": "0", "B5": "0", "B6": "0", "B7": "0",
		}
		for ref, value := range refs {
			setEtfCell(t, f, sheet, ref, value)
		}
	}

	for ref, value := range map[string]string{
		"A1": etfAumChartsHeader,
		"A2": etfDateHeader, "B2": "North America", "C2": "Europe", "D2": "Asia", "E2": "Other",
	} {
		setEtfCell(t, f, etfChartsSheet, ref, value)
	}

	return f
}

// Воркбук с шапками, но без точек — ошибка до PrepareBatch: пустой срез прошёл бы
// в Send, а каталог описал бы ряды, которых в таблице нет. Тест зовёт Import с nil
// conn: guard срабатывает раньше соединения, и обращение к conn упало бы паникой.
func TestEtfImportWorkbookWithoutDataFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ETF_Flows_2026-10-02_1401.xlsx")
	if err := newHeaderOnlyEtfWorkbook(t).SaveAs(path); err != nil {
		t.Fatalf("SaveAs: %v", err)
	}

	stat := etfFlowsStat{dir: dir}
	count, err := stat.Import(context.Background(), nil)
	if err == nil {
		t.Fatal("воркбук без строк данных должен давать ошибку, а не тихий ноль с публикацией каталога")
	}
	if count != 0 {
		t.Errorf("Import = %d строк, ожидалось 0", count)
	}
	if !strings.Contains(err.Error(), "без точек") {
		t.Errorf("ошибка не фиксирует причину (выгрузка без точек): %v", err)
	}
}
