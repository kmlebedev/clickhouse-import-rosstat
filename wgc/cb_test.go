package wgc

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	// cbChangesFile, cbHoldingsFile — имена выгрузок ЦБ в тестовых каталогах: обязаны
	// подходить под маски selectCbFiles, иначе тест проверял бы не выбор файла.
	cbChangesFile  = "Changes_latest_as_of_Oct2026_IFS.xlsx"
	cbHoldingsFile = "World_official_gold_holdings_as_of_Oct2026_IFS.xlsx"
)

// Имя импортёра — контракт расписания (dagu/wgc.yaml), ручного запуска
// (make import STAT=wgc_cb_reserves) и source строк в macro_series/series_catalog.
func TestCbReservesStatName(t *testing.T) {
	if got := (&cbReservesStat{}).Name(); got != "wgc_cb_reserves" {
		t.Fatalf("Name() = %q, ожидалось wgc_cb_reserves", got)
	}
}

// Пустая папка данных — не ошибка (warn + 0 строк): dagu не должен краснеть до
// первого скачивания выгрузок. conn при этом не трогается вовсе — тест зовёт
// Import с nil и упал бы паникой уже на первом обращении к соединению.
func TestCbImportEmptyDirIsNoop(t *testing.T) {
	stat := cbReservesStat{dir: t.TempDir()}
	count, err := stat.Import(context.Background(), nil)
	if err != nil {
		t.Fatalf("Import пустого каталога: %v", err)
	}
	if count != 0 {
		t.Errorf("Import пустого каталога = %d строк, ожидалось 0", count)
	}
}

// Выгрузки ЦБ скачиваются вручную и появляются в папке по отдельности, поэтому
// частичный набор — норма: "" = файла нет, и он не мешает разобрать вторую выгрузку.
func TestCbSelectFilesPartial(t *testing.T) {
	mtime := time.Date(2026, time.October, 4, 12, 0, 0, 0, time.UTC)

	t.Run("только changes", func(t *testing.T) {
		dir := t.TempDir()
		writeDataFile(t, filepath.Join(dir, cbChangesFile), mtime)

		changes, holdings, err := selectCbFiles(dir)
		if err != nil {
			t.Fatalf("selectCbFiles: %v", err)
		}
		if changes != filepath.Join(dir, cbChangesFile) {
			t.Errorf("changes = %q, ожидался %q", changes, filepath.Join(dir, cbChangesFile))
		}
		if holdings != "" {
			t.Errorf("holdings = %q, ожидался пустой путь (файла нет)", holdings)
		}
	})

	t.Run("только holdings", func(t *testing.T) {
		dir := t.TempDir()
		writeDataFile(t, filepath.Join(dir, cbHoldingsFile), mtime)

		changes, holdings, err := selectCbFiles(dir)
		if err != nil {
			t.Fatalf("selectCbFiles: %v", err)
		}
		if changes != "" {
			t.Errorf("changes = %q, ожидался пустой путь (файла нет)", changes)
		}
		if holdings != filepath.Join(dir, cbHoldingsFile) {
			t.Errorf("holdings = %q, ожидался %q", holdings, filepath.Join(dir, cbHoldingsFile))
		}
	})

	t.Run("пустая папка", func(t *testing.T) {
		changes, holdings, err := selectCbFiles(t.TempDir())
		if err != nil {
			t.Fatalf("selectCbFiles(пустой каталог): %v", err)
		}
		if changes != "" || holdings != "" {
			t.Errorf("selectCbFiles(пустой каталог) = (%q, %q), ожидались пустые пути", changes, holdings)
		}
	})
}

// writeCbChangesWithoutValues собирает выгрузку Changes, в которой все страны словаря на
// месте, а значений за месяц нет: разбор проходит, точек — ноль. Так выглядит файл, если
// данные в него не попали.
func writeCbChangesWithoutValues(t *testing.T, dir string) {
	t.Helper()

	f := newEtfSheet(t, cbChangesSheet)
	setEtfDate(t, f, cbChangesSheet, "D8", cbDate(2026, time.August, 31))
	for i, c := range CbCountries() {
		setEtfCell(t, f, cbChangesSheet, "B"+strconv.Itoa(cbChangesFirstDataRow+i), c.Name)
	}
	if err := f.SaveAs(filepath.Join(dir, cbChangesFile)); err != nil {
		t.Fatalf("SaveAs(%s): %v", cbChangesFile, err)
	}
}

// writeCbHoldingsWithoutTargets собирает выгрузку holdings с блоком Tonnes, но без стран
// словаря в топ-100: разбор проходит (топ-100 меняется, пропажа — warn), точек — ноль.
func writeCbHoldingsWithoutTargets(t *testing.T, dir string) {
	t.Helper()

	f := newEtfSheet(t, cbHoldingsSheet)
	setEtfCell(t, f, cbHoldingsSheet, "C6", cbHoldingsTonnesHeader)
	setEtfCell(t, f, cbHoldingsSheet, "E6", "Holdings as of")
	setEtfCell(t, f, cbHoldingsSheet, "B7", "Germany")
	setEtfCell(t, f, cbHoldingsSheet, "C7", 3349.29)
	setEtfDate(t, f, cbHoldingsSheet, "E7", cbDate(2026, time.August, 5))
	if err := f.SaveAs(filepath.Join(dir, cbHoldingsFile)); err != nil {
		t.Fatalf("SaveAs(%s): %v", cbHoldingsFile, err)
	}
}

// Разобравшиеся в ноль точек выгрузки — ошибка до PrepareBatch: пустой срез прошёл бы в
// Send, а каталог описал бы ряды, которых в таблице нет (fail-fast, урок eia). Ошибка
// обязана наступать и на частичном наборе: «нет дельт, но есть уровни» — не повод молча
// записать пустоту. Тест зовёт Import с nil conn: guard срабатывает раньше соединения,
// обращение к conn упало бы паникой.
func TestCbImportWorkbooksWithoutPointsFail(t *testing.T) {
	for _, tc := range []struct {
		name     string
		changes  bool
		holdings bool
	}{
		{"обе выгрузки", true, true},
		{"только changes", true, false},
		{"только holdings", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.changes {
				writeCbChangesWithoutValues(t, dir)
			}
			if tc.holdings {
				writeCbHoldingsWithoutTargets(t, dir)
			}

			stat := cbReservesStat{dir: dir}
			count, err := stat.Import(context.Background(), nil)
			if err == nil {
				t.Fatal("выгрузки без точек должны давать ошибку, а не пустой батч с публикацией каталога")
			}
			if count != 0 {
				t.Errorf("Import = %d строк, ожидалось 0", count)
			}
			if !strings.Contains(err.Error(), "без точек") {
				t.Errorf("ошибка не фиксирует причину (выгрузки без точек): %v", err)
			}
		})
	}
}
