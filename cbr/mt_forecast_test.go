package cbr

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Импортёр cbr_mt_forecast — второй материал контура заседаний ЦБ после cbr_mp_summary:
// карточки «Комментария к среднесрочному прогнозу» живут в том же индексе и под теми же
// селекторами (mpSummaryCardSel/mpSummaryNameSel/mpSummaryDateSel), поэтому проверяются все
// контракты обнаружения и идентичности выпуска: титул карточки, дата публикации против слага
// страницы, дата заседания из шапки блока, ссылка на книгу и схема таблицы. Ожидаемые значения
// ниже — литералами, а не константами пакета: тест, сверяющий mtForecastCardTitle с самим
// собой, прошёл бы и после правки константы, то есть ровно на той правке, от которой обязан
// падать (стандарт §8.5).

const (
	// mtForecastIndexFixture — живой индекс материалов заседаний ЦБ (та же страница, что
	// читает cbr_mp_summary): 11 карточек «Комментария» — 2 страничные (07052026 и 05082026,
	// xlsx появился с редизайном 2026) и 9 PDF-выпусков 2024-02 … 2026-02 (спека §2).
	mtForecastIndexFixture = "testdata/mp_summary_index.html"

	// mtForecastPageFixture — живая страница выпуска 05.08.2026: на ней кнопка «Графики и
	// таблицы в XLSX» — единственный xlsx-URL страницы (спека §3.9).
	mtForecastPageFixture = "testdata/mt_forecast_page_05082026.html"

	// mtForecastPageAnchor — id ссылки карточки выпуска 05.08.2026 в индексе: по нему тест
	// находит именно эту карточку для подмены её даты (id ссылок ЦБ стабильны в разметке).
	mtForecastPageAnchor = "a_194262page"

	// mtForecastTestDayLayout — формат дат в сообщениях теста (годы-месяцы-дни).
	mtForecastTestDayLayout = "2006-01-02"
)

// mtForecastUTCDay — дата-литерал в сутках UTC: release_date и meeting_date — даты без
// времени, и сравнение должно идти по календарному дню (как у mpDay в контуре резюме).
func mtForecastUTCDay(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// mtForecastFixture читает HTML-фикстуру теста. Файл на диске тест не меняет никогда: мутации
// веток fail-fast живут в памяти и только на время подтеста (стандарт §11.3 — тесты офлайн,
// фикстура остаётся живой).
func mtForecastFixture(t *testing.T, path string) []byte {
	t.Helper()
	page, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", path, err)
	}
	return page
}

// mtForecastIndexWithCardDate — копия страницы индекса с подменённой датой карточки, найденной
// по id её ссылки: дата публикации карточки — часть идентичности выпуска, и её расхождение со
// слагом страницы обязано падать (спека §3.4). Поиск идёт назад от якорной ссылки до ближайшего
// блока document-regular_date: у соседних карточек того же заседания («Резюме», «Среднесрочный
// прогноз») даты могут совпадать, и подмена «первого вхождения» правила бы не ту карточку.
func mtForecastIndexWithCardDate(t *testing.T, page []byte, anchorID, date string) []byte {
	t.Helper()
	anchor := bytes.Index(page, []byte(`id="`+anchorID+`"`))
	if anchor < 0 {
		t.Fatalf("в фикстуре индекса нет ссылки с id=%q: фикстура обновлена?", anchorID)
	}
	const dateMarker = `<div class="document-regular_date">`
	start := bytes.LastIndex(page[:anchor], []byte(dateMarker))
	if start < 0 {
		t.Fatalf("перед ссылкой id=%q нет блока даты %q: разметка индекса изменилась", anchorID, dateMarker)
	}
	start += len(dateMarker)
	end := start + bytes.Index(page[start:anchor], []byte(`</div>`))
	if end < start {
		t.Fatalf("в карточке id=%q не найден конец блока даты", anchorID)
	}
	mutated := make([]byte, 0, len(page))
	mutated = append(mutated, page[:start]...)
	mutated = append(mutated, date...)
	mutated = append(mutated, page[end:]...)
	return mutated
}

// TestMtForecastIndexReleases — разбор живого индекса: 11 карточек «Комментария» — 2 страничные
// (07052026: публикация 07.05.2026 при заседании 24.04.2026; 05082026: 05.08.2026 при
// 24.07.2026) и 9 PDF-пропусков. PDF-выпуски возвращаются с признаком PDF и без даты заседания:
// страницы у них нет, и в цикл импорта они не попадут (спека §3.4). Девять PDF названы счётчиком,
// а не выброшены молча: «источник отдаёт выпуск без машиночитаемого файла» должно быть видно.
func TestMtForecastIndexReleases(t *testing.T) {
	page := mtForecastFixture(t, mtForecastIndexFixture)
	releases, err := mtForecastIndexReleases(page)
	if err != nil {
		t.Fatalf("разбор живого индекса: %v", err)
	}
	if len(releases) != 11 {
		t.Fatalf("выпусков %d, want 11: 2 страничных (07052026, 05082026) + 9 PDF", len(releases))
	}

	pages, pdfs := mtForecastReleaseCounts(releases)
	if pages != 2 || pdfs != 9 {
		t.Fatalf("страничных выпусков %d, PDF %d, want 2 и 9: числа — часть no-op-сообщения прогона (спека §3.5)", pages, pdfs)
	}

	wantPages := map[time.Time]struct {
		meeting time.Time
		url     string
	}{
		mtForecastUTCDay(2026, time.May, 7): {
			meeting: mtForecastUTCDay(2026, time.April, 24),
			url:     "https://www.cbr.ru/dkp/mp_dec/decision_key_rate/comment_07052026/",
		},
		mtForecastUTCDay(2026, time.August, 5): {
			meeting: mtForecastUTCDay(2026, time.July, 24),
			url:     "https://www.cbr.ru/dkp/mp_dec/decision_key_rate/comment_05082026/",
		},
	}
	for _, release := range releases {
		if release.PDF {
			if !strings.HasSuffix(release.URL, ".pdf") {
				t.Errorf("PDF-выпуск %s: url %q не оканчивается на .pdf", release.ReleaseDate.Format(mtForecastTestDayLayout), release.URL)
			}
			if !release.MeetingDate.IsZero() {
				t.Errorf("PDF-выпуск %s: разобрана дата заседания %s, а PDF-карточки пропускаются до шапки блока (спека §3.4)",
					release.ReleaseDate.Format(mtForecastTestDayLayout), release.MeetingDate.Format(mtForecastTestDayLayout))
			}
			if release.ReleaseDate.IsZero() {
				t.Error("PDF-выпуск без даты публикации: карточка не называет публикацию")
			}
			continue
		}
		want, ok := wantPages[release.ReleaseDate]
		if !ok {
			t.Errorf("страничный выпуск %s не ожидался: want 2026-05-07 или 2026-08-05", release.ReleaseDate.Format(mtForecastTestDayLayout))
			continue
		}
		if !release.MeetingDate.Equal(want.meeting) {
			t.Errorf("выпуск %s: дата заседания %s, want %s (из шапки блока индекса)",
				release.ReleaseDate.Format(mtForecastTestDayLayout), release.MeetingDate.Format(mtForecastTestDayLayout), want.meeting.Format(mtForecastTestDayLayout))
		}
		if release.URL != want.url {
			t.Errorf("выпуск %s: url %q, want %q (страница выпуска — провенанс строк)",
				release.ReleaseDate.Format(mtForecastTestDayLayout), release.URL, want.url)
		}
	}

	t.Run("Дата карточки расходится со слагом", func(t *testing.T) {
		// 05082026 в слаге против 06.08.2026 в карточке: страница переехала под другую дату, и
		// запись выпуска под датой карточки создала бы второй винтаж одного выпуска.
		mutated := mtForecastIndexWithCardDate(t, page, mtForecastPageAnchor, "06.08.2026")
		_, err := mtForecastIndexReleases(mutated)
		if err == nil {
			t.Fatal("дата карточки 06.08.2026 против слага 05082026: ожидалась ошибка, а выпуск разобран")
		}
		if !strings.Contains(err.Error(), "comment_05082026") {
			t.Errorf("ошибка %q не называет карточку выпуска: по тексту не найти, какая карточка разошлась", err)
		}
	})

	t.Run("Карточка без даты публикации", func(t *testing.T) {
		// Структура индекса изменилась: карточка без даты не называет публикацию, и молчаливое
		// «дата нулевая» записало бы выпуск под 1970-01-01.
		mutated := mtForecastIndexWithCardDate(t, page, mtForecastPageAnchor, "")
		if _, err := mtForecastIndexReleases(mutated); err == nil {
			t.Error("карточка без даты публикации: ожидалась ошибка, а выпуск разобран")
		}
	})

	t.Run("Индекс без страничных карточек", func(t *testing.T) {
		// Обе страничные карточки стали PDF — источник отдаёт только PDF (так было до 2026).
		// Импортировать нечего навсегда, и это ошибка, а не «новых выпусков нет»: тихий пустой
		// список выглядел бы успешным прогоном при живом ЦБ (спека §3.5, шаг 3).
		mutated := bytes.ReplaceAll(page, []byte(`comment_05082026/"`), []byte(`comment_05082026.pdf"`))
		mutated = bytes.ReplaceAll(mutated, []byte(`comment_07052026/"`), []byte(`comment_07052026.pdf"`))
		_, err := mtForecastIndexReleases(mutated)
		if err == nil {
			t.Fatal("индекс только из PDF-карточек: ожидалась ошибка, а разбор вернул пустой список как успех")
		}
		if !strings.Contains(err.Error(), "странич") {
			t.Errorf("ошибка %q не говорит, что страничных карточек нет: причина не называется", err)
		}
	})
}

// TestMtForecastPageXlsxURL — ссылка на книгу со страницы выпуска: util.FindLink по
// mtForecastXlsxSel находит ровно тот xlsx, что лежит на живой странице 05.08.2026 (имя файла
// «commet_graph» — опечатка источника; <id> меняется от выпуска к выпуску, поэтому ссылка
// берётся со страницы, а не собирается). Страница без xlsx-ссылки — ошибка, а не пустой href.
func TestMtForecastPageXlsxURL(t *testing.T) {
	page := mtForecastFixture(t, mtForecastPageFixture)
	href, err := mtForecastPageXlsxURL(page)
	if err != nil {
		t.Fatalf("разбор живой страницы выпуска: %v", err)
	}
	const want = "/Content/Document/File/194307/commet_graph_05082026.xlsx"
	if href != want {
		t.Errorf("ссылка на книгу = %q, want %q (живой xlsx выпуска 05.08.2026)", href, want)
	}

	// Страница без xlsx-ссылки: ЦБ сменил вёрстку выпуска, и молчаливый пропуск оставил бы
	// выпуск неимпортированным (fail-fast, стандарт §6.4). Мутация в памяти: оба вхождения
	// имени файла (кнопка шапки и карточка материала) переименованы в PDF.
	broken := bytes.ReplaceAll(page, []byte("commet_graph_05082026.xlsx"), []byte("commet_graph_05082026.pdf"))
	if _, err := mtForecastPageXlsxURL(broken); err == nil {
		t.Error("страница без ссылки на xlsx: ожидалась ошибка, а ссылка не найдена не была")
	}
}

// TestMtForecastNewReleases — гейт прогона: из выпусков индекса остаются те, чьей даты публикации
// нет в таблице, в хронологическом порядке (старые первыми) — первый прогон наполняет историю
// детерминированным префиксом. PDF-выпуски не проходят гейт никогда: у них нет ни страницы, ни
// файла. Пустое множество известных даёт все страничные выпуски, полное — пусто (повторный
// прогон — no-op гейта, спека §3.6).
func TestMtForecastNewReleases(t *testing.T) {
	releases, err := mtForecastIndexReleases(mtForecastFixture(t, mtForecastIndexFixture))
	if err != nil {
		t.Fatalf("разбор живого индекса: %v", err)
	}

	t.Run("Известных нет", func(t *testing.T) {
		fresh := mtForecastNewReleases(releases, map[time.Time]bool{})
		if len(fresh) != 2 {
			t.Fatalf("новых выпусков %d, want 2: PDF-выпуски в цикл не попадают — у них нет ни страницы, ни xlsx", len(fresh))
		}
		if !fresh[0].ReleaseDate.Equal(mtForecastUTCDay(2026, time.May, 7)) || !fresh[1].ReleaseDate.Equal(mtForecastUTCDay(2026, time.August, 5)) {
			t.Errorf("порядок новых выпусков [%s, %s], want [2026-05-07, 2026-08-05]: старые первыми, а не в порядке страницы (та отдаёт свежие первыми)",
				fresh[0].ReleaseDate.Format(mtForecastTestDayLayout), fresh[1].ReleaseDate.Format(mtForecastTestDayLayout))
		}
	})

	t.Run("Все известны", func(t *testing.T) {
		known := map[time.Time]bool{
			mpDay(mtForecastUTCDay(2026, time.May, 7)):    true,
			mpDay(mtForecastUTCDay(2026, time.August, 5)): true,
		}
		if fresh := mtForecastNewReleases(releases, known); len(fresh) != 0 {
			t.Errorf("новых выпусков %d (%v), want 0: повторный прогон — no-op гейта", len(fresh), fresh)
		}
	})

	t.Run("Известен только свежий", func(t *testing.T) {
		// Гейт вычитает по дате выпуска поштучно: известность свежего не должна прятать старый.
		known := map[time.Time]bool{mpDay(mtForecastUTCDay(2026, time.August, 5)): true}
		fresh := mtForecastNewReleases(releases, known)
		if len(fresh) != 1 || !fresh[0].ReleaseDate.Equal(mtForecastUTCDay(2026, time.May, 7)) {
			t.Errorf("новые выпуски %v, want только 2026-05-07: гейт вычитает известное по одному выпуску", fresh)
		}
	})
}

// TestMtForecastImporterNameAndTable — имя импортёра и таблица: первое — контракт
// CLICKHOUSE_IMPORT_STAT и id шага dagu/cbr.yaml, второе — контракт данных (витрина
// v_cbr_mt_forecast и агент MCP). У импортёров со своими таблицами пакет держит конвенцию
// «Name() == TableName» (стандарт §7.2).
func TestMtForecastImporterNameAndTable(t *testing.T) {
	const name = "cbr_mt_forecast"
	if got := cbrMtForecastImporter.Name(); got != name {
		t.Errorf("Name() = %q, want %q", got, name)
	}
	if got := cbrMtForecastImporter.TableName; got != name {
		t.Errorf("TableName = %q, want %q", got, name)
	}
}

// TestMtForecastDdlPins — DDL определяет схему, которую ждут витрина и агент: колонки DDL
// (спека §3.1) в порядке INSERT, ключ винтажа (indicator, year, release_date) — по нему
// перезаписывается точка одного выпуска и сосуществуют ревизии разных (следующий выпуск ревизует
// диапазон), Float64 у границ (правило 5 AGENTS.md) и ReplacingMergeTree. DDL — в %s-форме:
// имя таблицы подставляет шаблон, и потеря %s оставила бы запрос без имени таблицы.
func TestMtForecastDdlPins(t *testing.T) {
	if len(cbrMtForecastImporter.CreateTable) != 1 {
		t.Fatalf("DDL-инструкций %d, want 1", len(cbrMtForecastImporter.CreateTable))
	}
	if !strings.Contains(cbrMtForecastImporter.CreateTable[0], "%s") {
		t.Error("DDL без плейсхолдера имени таблицы: имя в шаблон не подставляется (стандарт §5.1)")
	}
	ddl := fmt.Sprintf(cbrMtForecastImporter.CreateTable[0], "cbr_mt_forecast")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS cbr_mt_forecast (",
		"meeting_date Date",
		"release_date Date",
		"url String",
		"indicator LowCardinality(String)",
		"year UInt16",
		"lo Float64",
		"hi Float64",
		"ENGINE = ReplacingMergeTree ORDER BY (indicator, year, release_date)",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL %q не содержит %q", ddl, want)
		}
	}
	if strings.Contains(ddl, "Float32") {
		t.Error("DDL объявляет Float32: правило 5 AGENTS.md требует Float64")
	}
}

// TestMtForecastContractPins — запросы и селекторы импортёра литералами: запросы — часть
// утверждения «эти точки мои» (FINAL обязателен, иначе чек считал бы несмёрженные части,
// стандарт §9.2; дата выпуска в поста-чеке — плейсхолдер, а не литерал: она приходит со
// страницы), селекторы — контракт с живыми фикстурами тестов (стандарт §4.5).
func TestMtForecastContractPins(t *testing.T) {
	for _, c := range []struct {
		what, got, want string
	}{
		{"число точек (количественный чек версии)", mtForecastCountPoints, "SELECT count() FROM cbr_mt_forecast FINAL"},
		{"известные выпуски (гейт прогона)", mtForecastKnownReleasesQuery, "SELECT DISTINCT release_date FROM cbr_mt_forecast FINAL"},
		{"число строк выпуска (поста-чек)", mtForecastReleaseCountQuery, "SELECT count() FROM cbr_mt_forecast FINAL WHERE release_date = ?"},
		{"адрес индекса материалов заседаний", mtForecastIndexUrl, "https://www.cbr.ru/dkp/mp_dec/decision_key_rate/"},
		{"титул карточки индекса", mtForecastCardTitle, "Комментарий к среднесрочному прогнозу Банка России"},
		{"маркер ссылки карточки", mtForecastHrefMarker, "comment_"},
		{"селектор книги на странице выпуска", mtForecastXlsxSel, `a[href$=".xlsx"]`},
	} {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.what, c.got, c.want)
		}
	}
}

// TestMtForecastViewPins — витрина v_cbr_mt_forecast: выборка целиком (а не отдельные подстроки),
// потому что витрина — это и есть контракт с агентом. Select — расширенная форма dedicated-колонок
// lo/hi (отступление от long-формы §10.3 названо в спеке §5.2: диапазон не сводится к паре
// «дата → значение» без распада на две строки), FINAL обязателен (ключ ReplacingMergeTree), а
// колонки перечислены явно, чтобы витрина не поехала за порядком колонок DDL. У каждой колонки
// обязан быть комментарий (правило 11 AGENTS.md).
func TestMtForecastViewPins(t *testing.T) {
	const name = "v_cbr_mt_forecast"
	columns := []string{"source", "meeting_date", "release_date", "url", "indicator", "year", "lo", "hi"}

	if cbrMtForecastView.Name != name {
		t.Errorf("витрина называется %q, want %q", cbrMtForecastView.Name, name)
	}
	if len(cbrMtForecastView.Tables) != 1 || cbrMtForecastView.Tables[0] != "cbr_mt_forecast" {
		t.Errorf("витрина читает %v, want [cbr_mt_forecast]", cbrMtForecastView.Tables)
	}
	const want = "SELECT 'cbr_mt_forecast' AS source, meeting_date, release_date, url, indicator, year, lo, hi FROM cbr_mt_forecast FINAL"
	if cbrMtForecastView.Select != want {
		t.Errorf("Select витрины = %q, want %q", cbrMtForecastView.Select, want)
	}
	if cbrMtForecastView.Comment == "" {
		t.Error("витрина без комментария таблицы: агент не поймёт, что это за данные")
	}
	if len(cbrMtForecastView.Columns) != len(columns) {
		t.Errorf("комментариев колонок %d, want %d", len(cbrMtForecastView.Columns), len(columns))
	}
	for _, col := range columns {
		if cbrMtForecastView.Columns[col] == "" {
			t.Errorf("витрина: колонка %s без комментария", col)
		}
	}
}
