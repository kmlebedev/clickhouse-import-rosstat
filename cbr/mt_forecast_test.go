package cbr

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Импортёр cbr_mt_forecast — материал контура заседаний ЦБ: карточки «Комментария к
// среднесрочному прогнозу» и «Среднесрочного прогноза Банка России» живут в том же индексе, что
// читает cbr_mp_summary, и под теми же селекторами
// (mpSummaryCardSel/mpSummaryNameSel/mpSummaryDateSel), поэтому проверяются все контракты
// обнаружения и идентичности выпуска: титул карточки, дата публикации против слага страницы,
// дата заседания из шапки блока, адрес PDF, ссылка на книгу и схема таблицы. Ожидаемые значения
// ниже — литералами, а не константами пакета: тест, сверяющий mtForecastCardTitle с самим
// собой, прошёл бы и после правки константы, то есть ровно на той правке, от которой обязан
// падать (стандарт §8.5).

const (
	// mtForecastIndexFixture — живой индекс материалов заседаний ЦБ (та же страница, что
	// читает cbr_mp_summary): 11 карточек «Комментария» — 2 страничные (07052026 и 05082026,
	// xlsx появился с редизайном 2026) и 9 PDF-выпусков 2024-02 … 2026-02, плюс 11 PDF-карточек
	// «Среднесрочного прогноза» (спека §2, §3.4).
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

// TestMtForecastIndexReleases — разбор живого индекса: 22 карточки — 11 «Комментариев»
// (2 страничные: 07052026 при заседании 24.04.2026 и 05082026 при 24.07.2026; 9 PDF 2024-02 …
// 2026-02) и 11 «Среднесрочных прогнозов» (PDF, спека §3.4). Оба типа читаются одним проходом:
// маркер href (comment_/forecast_) задаёт тип, титул карточки его подтверждает. Дата заседания
// разбирается у обоих типов: у комментария она раньше даты публикации, у прогноза дата карточки
// и есть дата заседания. PDF-выпуски возвращаются с признаком PDF и попадут в цикл импорта только
// после гейта (сейчас mtForecastNewReleases отсеивает все PDF), но счётчики и даты видны уже здесь.
func TestMtForecastIndexReleases(t *testing.T) {
	page := mtForecastFixture(t, mtForecastIndexFixture)
	releases, err := mtForecastIndexReleases(page)
	if err != nil {
		t.Fatalf("разбор живого индекса: %v", err)
	}
	if len(releases) != 22 {
		t.Fatalf("выпусков %d, want 22: 11 комментариев (2 страницы + 9 PDF) и 11 прогнозов", len(releases))
	}

	want := mtForecastCounts{Comments: 11, CommentPages: 2, CommentPDFs: 9, Forecasts: 11}
	if counts := mtForecastReleaseCounts(releases); counts != want {
		t.Fatalf("счётчики %+v, want %+v: числа — часть no-op-сообщения прогона (спека §3.4)", counts, want)
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

	// Даты заседания PDF-комментариев — из шапок блоков фикстуры: публикация приходит позже
	// заседания, и дата карточки её не заменяет. Литералы, а не константы пакета: разбор обязан
	// читать шапку блока, а не выводить дату из карточки.
	wantCommentPDFs := map[time.Time]time.Time{
		mtForecastUTCDay(2026, time.February, 26): mtForecastUTCDay(2026, time.February, 13),
		mtForecastUTCDay(2025, time.November, 6):  mtForecastUTCDay(2025, time.October, 24),
		mtForecastUTCDay(2025, time.August, 6):    mtForecastUTCDay(2025, time.July, 25),
		mtForecastUTCDay(2025, time.May, 12):      mtForecastUTCDay(2025, time.April, 25),
		mtForecastUTCDay(2025, time.February, 26): mtForecastUTCDay(2025, time.February, 14),
		mtForecastUTCDay(2024, time.November, 6):  mtForecastUTCDay(2024, time.October, 25),
		mtForecastUTCDay(2024, time.August, 7):    mtForecastUTCDay(2024, time.July, 26),
		mtForecastUTCDay(2024, time.May, 13):      mtForecastUTCDay(2024, time.April, 26),
		mtForecastUTCDay(2024, time.February, 27): mtForecastUTCDay(2024, time.February, 16),
	}

	var pages, commentPDFs, forecasts int
	for _, release := range releases {
		switch release.Kind {
		case mtForecastKindComment:
			if release.PDF {
				commentPDFs++
				meeting, ok := wantCommentPDFs[release.ReleaseDate]
				if !ok {
					t.Errorf("PDF-комментарий %s не ожидался: want одну из дат 2024-02-27 … 2026-02-26",
						release.ReleaseDate.Format(mtForecastTestDayLayout))
					continue
				}
				if !release.MeetingDate.Equal(meeting) {
					t.Errorf("PDF-комментарий %s: дата заседания %s, want %s (из шапки блока индекса)",
						release.ReleaseDate.Format(mtForecastTestDayLayout), release.MeetingDate.Format(mtForecastTestDayLayout), meeting.Format(mtForecastTestDayLayout))
				}
				if !strings.HasSuffix(release.URL, ".pdf") {
					t.Errorf("PDF-комментарий %s: url %q не оканчивается на .pdf", release.ReleaseDate.Format(mtForecastTestDayLayout), release.URL)
				}
				continue
			}
			pages++
			expected, ok := wantPages[release.ReleaseDate]
			if !ok {
				t.Errorf("страничный комментарий %s не ожидался: want 2026-05-07 или 2026-08-05", release.ReleaseDate.Format(mtForecastTestDayLayout))
				continue
			}
			if !release.MeetingDate.Equal(expected.meeting) {
				t.Errorf("комментарий %s: дата заседания %s, want %s (из шапки блока индекса)",
					release.ReleaseDate.Format(mtForecastTestDayLayout), release.MeetingDate.Format(mtForecastTestDayLayout), expected.meeting.Format(mtForecastTestDayLayout))
			}
			if release.URL != expected.url {
				t.Errorf("комментарий %s: url %q, want %q (страница выпуска — провенанс строк)",
					release.ReleaseDate.Format(mtForecastTestDayLayout), release.URL, expected.url)
			}
		case mtForecastKindForecast:
			forecasts++
			if !release.PDF {
				t.Errorf("прогноз %s: PDF = false, want true: «Среднесрочный прогноз» в индексе — PDF-карточка",
					release.ReleaseDate.Format(mtForecastTestDayLayout))
			}
			if !release.ReleaseDate.Equal(release.MeetingDate) {
				t.Errorf("прогноз %s: дата карточки не равна дате заседания %s: у прогноза дата карточки — дата заседания",
					release.ReleaseDate.Format(mtForecastTestDayLayout), release.MeetingDate.Format(mtForecastTestDayLayout))
			}
			if !strings.Contains(release.URL, "forecast_") || !strings.HasSuffix(release.URL, ".pdf") {
				t.Errorf("прогноз %s: url %q не содержит forecast_ или не оканчивается на .pdf",
					release.ReleaseDate.Format(mtForecastTestDayLayout), release.URL)
			}
		default:
			t.Errorf("выпуск %s: неизвестный Kind %q, want %q или %q",
				release.ReleaseDate.Format(mtForecastTestDayLayout), release.Kind, mtForecastKindComment, mtForecastKindForecast)
		}
	}
	if pages != 2 || commentPDFs != 9 || forecasts != 11 {
		t.Errorf("разобрано страничных комментариев %d, PDF-комментариев %d, прогнозов %d, want 2, 9 и 11",
			pages, commentPDFs, forecasts)
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

	t.Run("Индекс без комментариев", func(t *testing.T) {
		// Карточки комментария пропали из индекса (источник сломан или подменён): прогнозы без
		// комментариев импортировать нечем, и это ошибка, а не «новых выпусков нет».
		mutated := bytes.ReplaceAll(page, []byte(`Комментарий к среднесрочному прогнозу Банка России`), []byte(`Комментарий`))
		_, err := mtForecastIndexReleases(mutated)
		if err == nil {
			t.Fatal("индекс без карточек «Комментария»: ожидалась ошибка, а разбор вернул прогнозы как успех")
		}
		if !strings.Contains(err.Error(), "комментар") {
			t.Errorf("ошибка %q не говорит, что карточек комментария нет: причина не называется", err)
		}
	})

	t.Run("Индекс без прогнозов", func(t *testing.T) {
		// Прогнозы пропали из индекса: комментарии импортируются, пробел виден в логе и счётчиках,
		// поэтому разбор обязан ответить успехом с Forecasts == 0, а не ошибкой (спека §3.4).
		mutated := bytes.ReplaceAll(page, []byte(`Среднесрочный прогноз Банка России`), []byte(`Прогноз`))
		releases, err := mtForecastIndexReleases(mutated)
		if err != nil {
			t.Fatalf("индекс без карточек прогноза: разбор вернул ошибку %v, want warn (комментарии импортируются)", err)
		}
		counts := mtForecastReleaseCounts(releases)
		if counts.Forecasts != 0 {
			t.Errorf("прогнозов %d, want 0: карточки без титула не должны разбираться", counts.Forecasts)
		}
		if counts.Comments != 11 || counts.CommentPages != 2 || counts.CommentPDFs != 9 {
			t.Errorf("счётчики комментариев %+v, want {Comments:11, CommentPages:2, CommentPDFs:9}: отсутствие прогнозов не должно скрывать комментарии", counts)
		}
	})
}

// TestMtForecastPageXlsxURL — ссылка на книгу со страницы выпуска: util.FindLink по
// mtForecastXlsxSel находит ровно тот xlsx, что лежит на живой странице 05.08.2026 (имя файла
// «commet_graph» — опечатка источника; <id> меняется от выпуска к выпуску, поэтому ссылка
// берётся со страницы, а не собирается). Страница без xlsx-ссылки — ошибка, а не пустой href, и
// ошибка называет ожидаемое имя файла (стандарт §4.4): «ссылка не найдена» без него не сказало бы
// оператору, что именно искать на изменившейся странице.
func TestMtForecastPageXlsxURL(t *testing.T) {
	page := mtForecastFixture(t, mtForecastPageFixture)
	releaseDate := time.Date(2026, time.August, 5, 0, 0, 0, 0, time.UTC)
	href, err := mtForecastPageXlsxURL(page, releaseDate)
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
	_, err = mtForecastPageXlsxURL(broken, releaseDate)
	if err == nil {
		t.Fatal("страница без ссылки на xlsx: ожидалась ошибка, а ссылка не найдена не была")
	}
	if !strings.Contains(err.Error(), "commet_graph_05082026.xlsx") {
		t.Errorf("ошибка %q не называет ожидаемое имя файла: оператору не видно, что искать (стандарт §4.4)", err)
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
		{"титул карточки прогноза", mtForecastForecastCardTitle, "Среднесрочный прогноз Банка России"},
		{"маркер ссылки карточки", mtForecastHrefMarker, "comment_"},
		{"маркер ссылки карточки прогноза", mtForecastForecastHrefMarker, "forecast_"},
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
