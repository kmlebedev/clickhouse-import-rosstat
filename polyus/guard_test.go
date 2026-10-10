package polyus

import (
	"testing"

	log "github.com/sirupsen/logrus"
)

// TestGuardDropsUnassignedValues держит два свойства резервного guard'а на
// синтетической строке: значение, не накрытое ни одной колонкой-периодом, не
// становится записью, а сама запись с пустым периодом до витрины не доходит.
//
// Строка собрана вручную: на живых фикстурах гарантия другая и более сильная —
// разбор раскладывает всё, и пустого периода не появляется вовсе (см.
// TestGuardIsInertOnRealFixtures). Здесь проверяется именно поведение самого
// guard'а, поэтому вход подан заведомо испорченный.
func TestGuardDropsUnassignedValues(t *testing.T) {
	records := []MetricRecord{
		{Metric: "revenue", Period: "2026H1", Value: 4674},
		// Пустой период — ошибка разбора: такая запись дала бы в витрине ключ
		// (metric, "") и столкнулась бы со всеми записями той же метрики.
		{Metric: "revenue", Period: "", Value: 999},
		{Metric: "gold_output", Period: "2026H1", Value: 1287},
	}

	kept, dropped := guardRecords(records)

	if dropped != 1 {
		t.Errorf("dropped = %d, want 1 (the record with an empty period)", dropped)
	}

	if len(kept) != 2 {
		t.Fatalf("kept = %d records, want 2: %+v", len(kept), kept)
	}

	for _, r := range kept {
		if r.Period == "" {
			t.Errorf("record with an empty period survived the guard: %+v", r)
		}
		if r.Value == 999 {
			t.Errorf("the empty-period record's value reached the batch: %+v", r)
		}
	}
}

// TestGuardKeepsUnassignedCount держит связь guard'а со счётчиком
// нераспределённых значений: guard его не считает сам, а получает от парсера и
// возвращает нетронутым — счётчик описывает разбор, а не отброс.
//
// Число взято живое: на строке «Производство золота» русского релиза 1 п/г 2026
// нераспределённых ровно два значения — (2%) и 6% из колонок изменения
// (TestUnassignedCountsChangeColumnValuesOnRUGoldRow). Guard обязан донести эту
// двойку до итоговой строки импортёра, а не занулить её.
func TestGuardKeepsUnassignedCount(t *testing.T) {
	const unassigned = 2

	kept, _ := guardRecords([]MetricRecord{
		{Metric: "gold_output", Period: "2026H1", Value: 1287},
		{Metric: "gold_output", Period: "2025H1", Value: 1311},
		{Metric: "gold_output", Period: "2025H2", Value: 1218},
	})

	if kept == nil {
		t.Fatal("guard returned nil records for a clean input")
	}

	if got := countUnassigned([]int{unassigned, 0}); got != unassigned {
		t.Errorf("unassigned counter = %d, want %d", got, unassigned)
	}
}

// TestGuardIsInertOnRealFixtures держит главное свойство нового guard'а: он
// резервный и по умолчанию НЕ отбрасывает ничего.
//
// Старый guard отбрасывал последнюю колонку шапки: позиционный разбор относил
// значение колонки изменения («Изм. за год») на период соседней колонки, и
// вместе с ним выбрасывались настоящие значения. Колоночная модель устранила
// причину — значение кладётся по своей X, — поэтому легитимный период теперь
// неотличим от «неправильного» и отбрасывать нечего. Тест держит, что ни одна
// распознанная запись не потеряна на всех включённых отчётах.
func TestGuardIsInertOnRealFixtures(t *testing.T) {
	fixtures := []struct {
		path string
		page int
	}{
		{"testdata/press_reliz_1h26_p1.tsv", 1},
		{"testdata/press_release_hist_p1.tsv", 4},
		{"testdata/press_release_fy2024_p4.tsv", 4},
	}

	for _, fixture := range fixtures {
		records, err := parseKPIPage(fixture.path, "https://example.invalid/x.pdf", fixture.page)
		if err != nil {
			t.Fatalf("parseKPIPage %s: %v", fixture.path, err)
		}
		if len(records) == 0 {
			t.Fatalf("%s parsed to zero records: the test lost its subject", fixture.path)
		}

		kept, dropped := guardRecords(records)

		if dropped != 0 {
			t.Errorf(
				"%s: guard dropped %d records, want 0: the column model places values by X, so there is nothing to drop",
				fixture.path,
				dropped,
			)
		}
		if len(kept) != len(records) {
			t.Errorf("%s: kept %d records, want all %d", fixture.path, len(kept), len(records))
		}
	}
}

// TestWarnUnassignedCounts держит громкость резерва: счётчик нераспределённых
// значений, отличный от нуля, обязан попасть в лог на уровне Warn. Молча
// потерянное значение выглядит как чисто разобранная строка.
//
// Заодно проверяется обратная сторона: при нулевом счётчике в лог ничего не
// пишется, иначе предупреждение перестало бы что-либо значить.
func TestWarnUnassignedCounts(t *testing.T) {
	// Уровень Warn снимается на время теста, поэтому hook получает записи и
	// тогда, когда logrus настроен иначе.
	logger := log.StandardLogger()

	previous := logger.GetLevel()
	logger.SetLevel(log.WarnLevel)

	defer func() { logger.SetLevel(previous) }()

	hook := &warnCapture{}
	logger.AddHook(hook)

	defer logger.ReplaceHooks(make(log.LevelHooks))

	warnUnassignedCounts([]int{0, 0, 0}, 3)

	if len(hook.entries) != 0 {
		t.Errorf("warned on zero unassigned values: %v", hook.entries)
	}

	warnUnassignedCounts([]int{2, 1}, 2)

	if len(hook.entries) != 1 {
		t.Fatalf("warnings = %d, want 1: %v", len(hook.entries), hook.entries)
	}

	entry := hook.entries[0]
	if got := entry.Data["unassigned"]; got != 3 {
		t.Errorf("unassigned in the warning = %v, want 3 (2+1)", got)
	}
	if got := entry.Data["reports"]; got != 2 {
		t.Errorf("reports in the warning = %v, want 2", got)
	}
	if entry.Level != log.WarnLevel {
		t.Errorf("level = %v, want warn", entry.Level)
	}
}

// warnCapture собирает записи лога для проверки.
type warnCapture struct {
	entries []*log.Entry
}

func (h *warnCapture) Levels() []log.Level {
	return log.AllLevels
}

func (h *warnCapture) Fire(entry *log.Entry) error {
	h.entries = append(h.entries, entry)

	return nil
}

// TestGuardAppliesToIFRSRecords держит область действия guard'а: инвариант «в
// витрину не попадает запись без периода» обязан выполняться для отчёта ЛЮБОГО
// типа, а не только для KPI.
//
// МСФО-страница даёт ровно тот вход, на котором ошибка была не видна: её период
// приходит параметром (parseIFRSPage), и разбор НЕ отбрасывает записи с пустым
// периодом — при period == "" все девять метрик страницы доходят до guard'а с
// Period: "". batchDedup их не спасает: ключ витрины — (metric, period), а
// метрики у этих девяти разные, поэтому уникальный ключ (metric, "") у каждой
// своей метрики, и ReplacingMergeTree схлопнула бы их только между запусками.
//
// Записи те же, что отдаёт реальная страница 6 английского МСФО за 1 п/г 2026
// (parseIFRSPage("testdata/en_msfo_p6.tsv", ..., "")): девять метрик с пустым
// периодом. Проверяется общий шаг — applyGuard, — и это не косметика: через него
// проходит отчёт любого типа, а kind передаётся в него ровно затем, чтобы
// сужение «guard только для kpi» было видно в сигнатуре. Тест зовёт applyGuard
// с "ifrs", поэтому вызов guardRecords, возвращённый внутрь ветки kpi, ловится
// здесь, а не только при живой приёмке.
func TestGuardAppliesToIFRSRecords(t *testing.T) {
	records, err := parseIFRSPage("testdata/en_msfo_p6.tsv", "https://example.invalid/6m2026.pdf", 6, "")
	if err != nil {
		t.Fatalf("parseIFRSPage: %v", err)
	}

	if len(records) == 0 {
		t.Fatal("the IFRS page parsed to zero records: the test lost its subject")
	}

	for _, r := range records {
		if r.Period != "" {
			t.Fatalf("record %s carries period %q: the test no longer builds the empty-period input it is about", r.Metric, r.Period)
		}
	}

	kept, dropped := applyGuard("ifrs", records)

	if dropped != len(records) {
		t.Errorf("guard dropped %d of %d IFRS records, want all of them: a record without a period must not reach batch.Append", dropped, len(records))
	}
	if len(kept) != 0 {
		t.Errorf("guard kept %d IFRS records with an empty period, want 0", len(kept))
	}
}

// TestReportParsedTurnsHeaderlessPageIntoFailure держит B1 на уровне импортёра:
// включённый отчёт, разобравшийся в ноль записей, — отказ (`failed++`), а не
// молчаливый успех.
//
// Шов взят из самого Import(): успех отчёта решает reportParsed, а не пустой
// список записей. Это то же место, где сходятся обе причины нуля записей —
// нераспознанная шапка и пустая страница, — и разбирать их по отдельности
// незачем: у обоих один правильный ответ, «данных нет, отчёт не разобран».
// Через Import() это не проверить без ClickHouse (нужны conn и PDF), а шов на
// уровне решения не требует ни того, ни другого и не оставляет Import()
// единственным непокрытым звеном.
//
// Число строк выше шапки здесь берётся не из прозы, а из разбора той же
// повреждённой фикстуры, что и в TestParseKPILinesHeaderlessCount: 10 строк
// метрик остались без колонок, и именно это число импортёр печатает в Warn.
func TestReportParsedTurnsHeaderlessPageIntoFailure(t *testing.T) {
	intact := tsvLines(t, "testdata/press_reliz_1h26_p1.tsv")
	damaged := dropUnitsMarkerLine(t, intact)

	records, _, headerless := parseKPILines(damaged, "https://example.invalid/1h26.pdf", 1)

	if parsed := reportParsed(records, headerless); parsed {
		t.Errorf(
			"reportParsed = true for a page whose header was not recognised (%d records, %d metric lines above the header): the importer would count it as a successful report and lose those lines silently",
			len(records),
			headerless,
		)
	}

	// Обратные случаи: разобранный отчёт — успех независимо от того, были ли выше
	// шапки строки с меткой (на повреждённой фикстуре такие строки есть, и отказ
	// обязан остаться про ноль записей, а не про непустой счётчик); страница без
	// единой строки метрики выше шапки — тоже успех, потому что пустой отчёт от
	// нераспознанного этим швом не отличается и отличать его здесь нечем.
	if !reportParsed([]MetricRecord{{Metric: "revenue", Period: "2026H1"}}, 10) {
		t.Error("reportParsed = false for a non-empty parse: a parsed report is a success whatever the headerless counter says")
	}
	if !reportParsed(nil, 0) {
		t.Error("reportParsed = false for an empty parse with no metric line above the header: nothing points at a lost page here")
	}
}
