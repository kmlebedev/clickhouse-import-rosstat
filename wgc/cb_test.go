package wgc

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
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

// silenceCbLogs глушит ожидаемые warn-и теста (пустая папка, holdings без стран словаря в
// топ-100): logrus пишет в stderr, и без перехвата вывод засорялся бы сообщениями, которые
// тест не проверяет.
func silenceCbLogs(t *testing.T) {
	t.Helper()

	var buffered bytes.Buffer
	prevOut := log.StandardLogger().Out
	log.SetOutput(&buffered)
	t.Cleanup(func() { log.SetOutput(prevOut) })
}

// Пустая папка данных — не ошибка (warn + 0 строк): dagu не должен краснеть до
// первого скачивания выгрузок. conn при этом не трогается вовсе — тест зовёт
// Import с nil и упал бы паникой уже на первом обращении к соединению.
// Флаг DAG снимается явно: режим ручного прогона не должен зависеть от окружения оболочки.
func TestCbImportEmptyDirIsNoop(t *testing.T) {
	silenceCbLogs(t)
	t.Setenv(wgcRequireDataEnv, "")

	stat := cbReservesStat{dir: t.TempDir()}
	count, err := stat.Import(context.Background(), nil)
	if err != nil {
		t.Fatalf("Import пустого каталога: %v", err)
	}
	if count != 0 {
		t.Errorf("Import пустого каталога = %d строк, ожидалось 0", count)
	}
}

// С WGC_REQUIRE_DATA=1 (так шаг идёт в DAG) пустая папка — ошибка, а не warn + 0: тихий
// ноль на staging оставлял DAG зелёным при пустой витрине v_cb_gold_dynamics (#507).
// Ошибка наступает до разбора и до PrepareBatch — Import зовётся с nil, обращение к conn
// упало бы паникой. В тексте обязаны быть флаг, каталог и обе маски: по нему видно, какие
// файлы не доехали.
func TestCbImportRequireDataEmptyDir(t *testing.T) {
	silenceCbLogs(t)
	t.Setenv(wgcRequireDataEnv, "1")

	dir := t.TempDir()
	stat := cbReservesStat{dir: dir}

	count, err := stat.Import(context.Background(), nil)
	if err == nil {
		t.Fatal("WGC_REQUIRE_DATA=1 и пустой каталог должны давать ошибку")
	}
	if count != 0 {
		t.Errorf("Import = %d строк, ожидалось 0", count)
	}
	for _, want := range []string{"WGC_REQUIRE_DATA=1", dir, cbChangesMask, cbHoldingsMask} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка не содержит %q: %v", want, err)
		}
	}
}

// Частичный набор при WGC_REQUIRE_DATA=1 — тоже ошибка: без holdings витрина осталась бы
// без уровней (часть колонок NULL), а DAG — зелёным. Файл-заглушка writeDataFile не xlsx,
// поэтому разбор упал бы на excelize: ошибка обязана называть флаг и отсутствующую маску,
// то есть наступать раньше открытия файла (и соединения — conn здесь nil).
func TestCbImportRequireDataPartial(t *testing.T) {
	silenceCbLogs(t)
	t.Setenv(wgcRequireDataEnv, "1")

	dir := t.TempDir()
	writeDataFile(t, filepath.Join(dir, cbChangesFile), time.Date(2026, time.October, 4, 12, 0, 0, 0, time.UTC))

	stat := cbReservesStat{dir: dir}
	count, err := stat.Import(context.Background(), nil)
	if err == nil {
		t.Fatal("WGC_REQUIRE_DATA=1 и только changes должны давать ошибку")
	}
	if count != 0 {
		t.Errorf("Import = %d строк, ожидалось 0", count)
	}
	if !strings.Contains(err.Error(), "WGC_REQUIRE_DATA=1") {
		t.Errorf("ошибка не фиксирует режим WGC_REQUIRE_DATA=1: %v", err)
	}
	if !strings.Contains(err.Error(), cbHoldingsMask) {
		t.Errorf("ошибка не называет отсутствующую маску %s: %v", cbHoldingsMask, err)
	}
	if strings.Contains(err.Error(), cbChangesMask) {
		t.Errorf("ошибка называет присутствующую маску %s: %v", cbChangesMask, err)
	}
}

// copyCbFixture кладёт копию фикстуры винтажа под именем выгрузки в тестовый каталог:
// Import ищет файлы по маскам, а не по путям фикстур.
func copyCbFixture(t *testing.T, dst, src string) {
	t.Helper()

	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", src, err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", dst, err)
	}
}

// Оба файла при WGC_REQUIRE_DATA=1 — не ошибка: разбор идёт, и прогон доходит до
// соединения. Фикстуры винтажа 2026-10 (те же, что у parse-тестов) дают точки, а
// etfFakeConn останавливает Import на PrepareBatch — значит, проверка файлов и разбор
// пройдены, и флаг не даёт ложного fail-loud «нет выгрузок» при полном наборе.
func TestCbImportRequireDataBothPresent(t *testing.T) {
	silenceCbLogs(t)
	t.Setenv(wgcRequireDataEnv, "1")

	dir := t.TempDir()
	copyCbFixture(t, filepath.Join(dir, cbChangesFile), cbChangesFixturePath)
	copyCbFixture(t, filepath.Join(dir, cbHoldingsFile), cbHoldingsFixturePath)

	conn := &etfFakeConn{}
	stat := cbReservesStat{dir: dir}
	count, err := stat.Import(context.Background(), conn)
	if err == nil {
		t.Fatal("фейковый PrepareBatch обязан вернуть ошибку: молчаливого нуля без записи быть не должно")
	}
	if count != 0 {
		t.Errorf("Import = %d строк, ожидалось 0 (ошибка до Send)", count)
	}
	for _, banned := range []string{wgcRequireDataEnv, "нет выгрузок"} {
		if strings.Contains(err.Error(), banned) {
			t.Errorf("ошибка похожа на fail-loud по файлам (%q): %v", banned, err)
		}
	}
	if len(conn.calls) != 2 || !strings.HasPrefix(conn.calls[1], "preparebatch: ") {
		t.Errorf("обращения к соединению = %v, ожидались DDL и PrepareBatch (разбор обоих файлов пройден)", conn.calls)
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
	setEtfCell(t, f, cbHoldingsSheet, "D6", "% of reserves**")
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
	silenceCbLogs(t)
	// Флаг DAG снимается явно: тест проверяет разбор выгрузок, а не режим WGC_REQUIRE_DATA,
	// и с флагом из окружения частичные подкейсы падали бы на отсутствующей второй маске.
	t.Setenv(wgcRequireDataEnv, "")

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
