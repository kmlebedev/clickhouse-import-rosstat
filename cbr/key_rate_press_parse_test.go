package cbr

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Фикстуры — живые страницы ЦБ (календарь и релизы 2024–2026), снятые 2026-10-23.
// Тесты офлайн: сеть не трогается, разбор получает те же байты, что отдал источник.

func pressFixture(t *testing.T, name string) []byte {
	t.Helper()
	page, err := os.ReadFile(filepath.Join("testdata", "key_rate_press", name))
	if err != nil {
		t.Fatalf("фикстура %s: %v", name, err)
	}
	return page
}

func pressDay(t *testing.T, raw string) time.Time {
	t.Helper()
	date, err := time.Parse(pressDayLayout, raw)
	if err != nil {
		t.Fatalf("дата %q: %v", raw, err)
	}
	return date
}

// TestPressCalendarMeetings — календарь заседаний: 56 заседаний 2021-02 … 2027-12,
// распознаваемых по текущей формулировке (2020 год использует старую «по
// денежно-кредитной политике» и для истории источника не нужен). У заседаний 2024+
// есть прямые ссылки на релизы, у будущих — нет. Даты и ссылки берутся из одного
// блока main-events_day, поэтому ручной конструкции URL нет.
func TestPressCalendarMeetings(t *testing.T) {
	meetings, err := pressCalendarMeetings(pressFixture(t, "cal_mp.html"))
	if err != nil {
		t.Fatalf("календарь: %v", err)
	}
	if len(meetings) != 56 {
		t.Fatalf("заседаний %d, want 56: вёрстка календаря изменилась", len(meetings))
	}
	if got := meetings[0].Date.Format(pressDayLayout); got != "2021-02-12" {
		t.Errorf("первое заседание %s, want 2021-02-12", got)
	}
	if got := meetings[len(meetings)-1].Date.Format(pressDayLayout); got != "2027-12-17" {
		t.Errorf("последнее заседание %s, want 2027-12-17", got)
	}

	byDate := make(map[string]pressMeeting, len(meetings))
	for _, m := range meetings {
		byDate[m.Date.Format(pressDayLayout)] = m
	}
	if m := byDate["2024-02-16"]; m.Href != "/press/pr/?file=16022024_133000key.htm" {
		t.Errorf("2024-02-16 href = %q, want /press/pr/?file=16022024_133000key.htm", m.Href)
	}
	if m := byDate["2026-10-23"]; m.Href != "/press/pr/?file=23102026_133000key.htm" {
		t.Errorf("2026-10-23 href = %q, want /press/pr/?file=23102026_133000key.htm", m.Href)
	}
	if m := byDate["2027-02-12"]; m.Href != "" {
		t.Errorf("2027-02-12 href = %q, want пусто: релиза будущего заседания ещё нет", m.Href)
	}
}

// TestPressCalendarMeetingsEmpty — пустой документ или чужая вёрстка — ошибка,
// а не пустая дельта: тихий ноль оставил бы источник неимпортированным.
func TestPressCalendarMeetingsEmpty(t *testing.T) {
	if _, err := pressCalendarMeetings([]byte("<html><body>нет календаря</body></html>")); err == nil {
		t.Fatal("пустой календарь разобран без ошибки")
	}
}

// TestPressParseDoc — разбор шести живых релизов: решение и шапка, число абзацев,
// слаги ключевых блоков и квартальные числа. Ожидания — из текста источника.
func TestPressParseDoc(t *testing.T) {
	tests := []struct {
		name       string
		file       string
		date       string
		action     string
		rate       float64
		next       string
		signal     bool
		blocks     int
		quarter    pressQuarterWant
		wantBlocks []string
	}{
		{
			name: "2024-02-16 «по сравнению с», два месяца",
			file: "press_16022024.html", date: "2024-02-16",
			action: "hold", rate: 16.0, next: "2024-03-22", signal: true, blocks: 12,
			quarter: pressQuarterWant{
				period: "декабрь — январь", form: pressFormMonth,
				headline: 6.6, headlinePrev: 11.5, core: 7.0, corePrev: 10.2,
				annual: 7.4, asof: "2024-02-12", parsed: true,
			},
			wantBlocks: []string{"decision", pressQuarterBlock, "next_meeting"},
		},
		{
			name: "2024-03-22 без чисел",
			file: "press_22032024.html", date: "2024-03-22",
			action: "hold", rate: 16.0, next: "2024-04-26", signal: true, blocks: 12,
			quarter: pressQuarterWant{
				form: pressFormNone, annual: 7.7, asof: "2024-03-18", parsed: false,
			},
			wantBlocks: []string{"decision", pressQuarterBlock, "next_meeting"},
		},
		{
			name: "2024-07-26 римский квартал",
			file: "press_26072024.html", date: "2024-07-26",
			action: "hike", rate: 18.0, next: "2024-09-13", signal: true, blocks: 12,
			quarter: pressQuarterWant{
				period: "II квартал 2024", form: pressFormQuarter,
				headline: 8.6, headlinePrev: 5.8, core: 9.2, corePrev: 6.8,
				annual: 9.0, asof: "2024-07-22", parsed: true,
			},
			wantBlocks: []string{"decision", pressQuarterBlock, "next_meeting"},
		},
		{
			name: "2025-10-24 квартал 3к25",
			file: "press_24102025.html", date: "2025-10-24",
			action: "cut", rate: 16.5, next: "2025-12-19", signal: true, blocks: 16,
			quarter: pressQuarterWant{
				period: "3к25", form: pressFormQuarter,
				headline: 6.4, headlinePrev: 4.4, core: 4.3, corePrev: 4.4,
				annual: 8.2, asof: "2025-10-20", parsed: true,
			},
			wantBlocks: []string{"decision", pressQuarterBlock, "next_meeting"},
		},
		{
			name: "2026-09-11 месяц к кварталу",
			file: "press_11092026.html", date: "2026-09-11",
			action: "hold", rate: 14.0, next: "2026-10-23", signal: true, blocks: 13,
			quarter: pressQuarterWant{
				period: "июль", form: pressFormMonth,
				headline: 11.6, headlinePrev: 5.3, core: 7.0, corePrev: 4.6,
				annual: 6.3, asof: "2026-09-07", parsed: true,
			},
			wantBlocks: []string{"decision", pressQuarterBlock, "next_meeting"},
		},
		{
			name: "2026-10-23 квартал 3к26",
			file: "press_23102026.html", date: "2026-10-23",
			action: "hold", rate: 14.0, next: "2026-12-18", signal: true, blocks: 13,
			quarter: pressQuarterWant{
				period: "3к26", form: pressFormQuarter,
				headline: 8.0, headlinePrev: 4.8, core: 6.2, corePrev: 4.5,
				annual: 6.8, asof: "2026-10-19", parsed: true,
			},
			wantBlocks: []string{"decision", pressQuarterBlock, "next_meeting"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc, err := pressParseDoc(pressFixture(t, tt.file), pressDay(t, tt.date))
			if err != nil {
				t.Fatalf("разбор: %v", err)
			}
			if doc.Action != tt.action || doc.Rate != tt.rate {
				t.Errorf("решение %s %.2f, want %s %.2f", doc.Action, doc.Rate, tt.action, tt.rate)
			}
			if doc.EffectiveFrom != nil {
				t.Errorf("effective_from = %s, want NULL: в этом релизе даты вступления нет",
					doc.EffectiveFrom.Format(pressDayLayout))
			}
			if doc.NextMeetingDate == nil || doc.NextMeetingDate.Format(pressDayLayout) != tt.next {
				t.Errorf("следующее заседание %v, want %s", doc.NextMeetingDate, tt.next)
			}
			if tt.signal && doc.Signal == "" {
				t.Error("signal пуст: фраза про дальнейшую политику не найдена")
			}
			if !tt.signal && doc.Signal != "" {
				t.Errorf("signal = %q, want пусто: в релизе нет формулировки сигнала", doc.Signal)
			}
			if tt.blocks > 0 && len(doc.Blocks) != tt.blocks {
				t.Errorf("абзацев %d, want %d", len(doc.Blocks), tt.blocks)
			}
			t.Logf("абзацев %d, кварталов %d", len(doc.Blocks), len(doc.Quarters))

			slugs := make(map[string]bool, len(doc.Blocks))
			for _, b := range doc.Blocks {
				slugs[b.Block] = true
			}
			for _, want := range tt.wantBlocks {
				if !slugs[want] {
					t.Errorf("блок %q не найден среди слагов %v", want, slugs)
				}
			}
			if len(doc.Quarters) != 1 {
				t.Fatalf("квартальных блоков %d, want 1", len(doc.Quarters))
			}
			pressAssertQuarter(t, doc.Quarters[0], tt.quarter)
		})
	}
}

// pressQuarterWant — ожидание квартального блока: числа указателями-значениями,
// nil — NULL.
type pressQuarterWant struct {
	period           string
	form             string
	headline         float64
	headlinePrev     float64
	headlinePrev2    float64
	headlinePrev2Set bool
	core             float64
	corePrev         float64
	corePrev2        float64
	corePrev2Set     bool
	annual           float64
	annualSet        bool
	asof             string
	parsed           bool
}

// pressAssertQuarter сверяет разобранный квартал с ожиданием. Нулевые ожидания
// headline/core означают «числа нет» только при parsed=false; для релизов с числами
// тест задаёт их явно (в корпусе нет квартала с нулевой инфляцией).
func pressAssertQuarter(t *testing.T, got pressQuarter, want pressQuarterWant) {
	t.Helper()
	if got.Period != want.period {
		t.Errorf("period = %q, want %q", got.Period, want.period)
	}
	if got.Form != want.form {
		t.Errorf("form = %q, want %q", got.Form, want.form)
	}
	if got.Parsed != want.parsed {
		t.Errorf("parsed = %v, want %v", got.Parsed, want.parsed)
	}
	pressAssertFloat(t, "headline", got.Headline, want.headline, want.parsed)
	pressAssertFloat(t, "headline_prev", got.HeadlinePrev, want.headlinePrev, want.parsed)
	pressAssertFloat(t, "core", got.Core, want.core, want.parsed)
	pressAssertFloat(t, "core_prev", got.CorePrev, want.corePrev, want.parsed)
	if want.headlinePrev2Set {
		pressAssertFloat(t, "headline_prev2", got.HeadlinePrev2, want.headlinePrev2, true)
	} else if got.HeadlinePrev2 != nil {
		t.Errorf("headline_prev2 = %v, want NULL", *got.HeadlinePrev2)
	}
	if want.corePrev2Set {
		pressAssertFloat(t, "core_prev2", got.CorePrev2, want.corePrev2, true)
	} else if got.CorePrev2 != nil {
		t.Errorf("core_prev2 = %v, want NULL", *got.CorePrev2)
	}
	if want.annualSet || want.annual != 0 {
		pressAssertFloat(t, "annual_inflation", got.AnnualInflation, want.annual, true)
	} else if got.AnnualInflation != nil {
		t.Errorf("annual_inflation = %v, want NULL", *got.AnnualInflation)
	}
	if want.asof == "" {
		if got.AnnualAsof != nil {
			t.Errorf("annual_asof = %s, want NULL", got.AnnualAsof.Format(pressDayLayout))
		}
		return
	}
	if got.AnnualAsof == nil || got.AnnualAsof.Format(pressDayLayout) != want.asof {
		t.Errorf("annual_asof = %v, want %s", got.AnnualAsof, want.asof)
	}
}

func pressAssertFloat(t *testing.T, name string, got *float64, want float64, set bool) {
	t.Helper()
	if !set {
		if got != nil {
			t.Errorf("%s = %v, want NULL", name, *got)
		}
		return
	}
	if got == nil {
		t.Errorf("%s = NULL, want %v", name, want)
		return
	}
	if *got != want {
		t.Errorf("%s = %v, want %v", name, *got, want)
	}
}

// TestPressParseQuarterTwoBases — два базиса и «после в среднем»: формы, которые
// встречаются в корпусе, но не в шести фикстурах.
func TestPressParseQuarterTwoBases(t *testing.T) {
	text := "В 2к26 текущий рост цен с поправкой на сезонность составил в среднем 5,0% в пересчете на год " +
		"после 8,7% в 1к26 и 4,3% в 4к25. Аналогичный показатель базовой инфляции в среднем замедлился " +
		"до 4,2% после 6,2% в предыдущем квартале и 4,9% в 4к25."
	got := pressParseQuarter(text, 3, pressDay(t, "2026-07-24"))
	want := pressQuarterWant{
		period: "2к26", form: pressFormQuarter,
		headline: 5.0, headlinePrev: 8.7, headlinePrev2: 4.3, headlinePrev2Set: true,
		core: 4.2, corePrev: 6.2, corePrev2: 4.9, corePrev2Set: true,
		parsed: true,
	}
	pressAssertQuarter(t, got, want)

	text = "В апреле 2025 года текущий рост цен с поправкой на сезонность снизился до 6,2% в пересчете " +
		"на год после в среднем 8,2% в 1к25. Аналогичный показатель базовой инфляции составил 4,4% " +
		"после в среднем 8,9% в предыдущем квартале."
	got = pressParseQuarter(text, 2, pressDay(t, "2025-06-06"))
	pressAssertQuarter(t, got, pressQuarterWant{
		period: "апрель 2025", form: pressFormMonth,
		headline: 6.2, headlinePrev: 8.2, core: 4.4, corePrev: 8.9,
		parsed: true,
	})
}

// TestPressParseQuarterNoNumbers — «в феврале оставался на уровне января»: чисел
// стандартной пары нет, parsed=0, годовая инфляция при этом есть.
func TestPressParseQuarterNoNumbers(t *testing.T) {
	text := "Текущий рост цен с поправкой на сезонность в феврале оставался на уровне января, а в марте, " +
		"по предварительным данным, снижается. Показатель годовой инфляции пока сохраняется на уровне " +
		"февраля и, по оценке на 18 марта, составил 7,7%."
	got := pressParseQuarter(text, 2, pressDay(t, "2024-03-22"))
	if got.Parsed {
		t.Error("parsed = true, want false: чисел текущего роста и базовой нет")
	}
	if got.Headline != nil || got.Core != nil {
		t.Errorf("headline/core = %v/%v, want NULL", got.Headline, got.Core)
	}
	pressAssertFloat(t, "annual_inflation", got.AnnualInflation, 7.7, true)
	if got.AnnualAsof == nil || got.AnnualAsof.Format(pressDayLayout) != "2024-03-18" {
		t.Errorf("annual_asof = %v, want 2024-03-18", got.AnnualAsof)
	}
}

// TestPressBlockSlug — словарь слагов: порядок проверок не даёт сигналу стать
// ожиданиями, а денежно-кредитным условиям — кредитной активностью.
func TestPressBlockSlug(t *testing.T) {
	tests := []struct{ text, want string }{
		{"Следующее заседание Совета директоров запланировано на 18 декабря 2026 года.", "next_meeting"},
		{"5 ноября 2026 года Банк России опубликует Резюме обсуждения ключевой ставки.", "publications"},
		{"По итогам заседания Банк России обновил среднесрочный прогноз.", "forecast"},
		{"В 3к26 текущий рост цен с поправкой на сезонность составил 8,0%.", pressQuarterBlock},
		{"Некоторое время текущий рост цен будет оставаться повышенным из-за инерции.", "other"},
		{"Банк России будет принимать дальнейшие решения по ключевой ставке в зависимости от инфляционных ожиданий.", "guidance"},
		{"Денежно-кредитные условия оцениваются как умеренно жесткие.", "monetary"},
		{"Кредитная активность в среднем за 3к26 была повышенной.", "credit"},
		{"Напряженность на рынке труда постепенно снижается.", "labor"},
		{"Проинфляционные риски преобладают над дезинфляционными.", "risks"},
		{"Банк России исходит из объявленных параметров бюджетной политики.", "budget"},
		{"Инфляционные ожидания населения остаются высокими.", "expectations"},
		{"По оперативным данным, экономика растет умеренными темпами.", "activity"},
		{"Устойчивые показатели инфляции находятся в диапазоне 4–6%.", "other"},
	}
	for _, tt := range tests {
		if got := pressBlockSlug(tt.text); got != tt.want {
			t.Errorf("pressBlockSlug(%q) = %q, want %q", tt.text, got, tt.want)
		}
	}
}

// TestPressParseDate — даты ЦБ: NBSP нормализуется, неизвестный месяц — ошибка.
func TestPressParseDate(t *testing.T) {
	got, err := pressParseDate("16\u00a0февраля 2024 года")
	if err != nil || got.Format(pressDayLayout) != "2024-02-16" {
		t.Errorf("дата = %v, %v; want 2024-02-16", got, err)
	}
	if _, err = pressParseDate("16 фев 2024"); err == nil {
		t.Error("нераспознанная дата разобрана без ошибки")
	}
}

// TestPressParseDocNotPublished — страница без абзаца решения — sentinel
// errPressNotPublished: импортёр отличает «ещё не вышел» от сбоя разбора.
func TestPressParseDocNotPublished(t *testing.T) {
	page := []byte(`<html><body><div class="landing-text"><p>Страница обновляется</p></div></body></html>`)
	_, err := pressParseDoc(page, pressDay(t, "2026-10-23"))
	if !errors.Is(err, errPressNotPublished) {
		t.Fatalf("ошибка %v, want errPressNotPublished", err)
	}
}

// TestCbrKeyRatePressDdlPins — DDL-литералы называют свои таблицы (копия
// канонического DDL в ARCHITECTURE §6.2 не должна разъехаться с константами).
func TestCbrKeyRatePressDdlPins(t *testing.T) {
	for _, tt := range []struct{ ddl, table string }{
		{pressDdl, pressTable},
		{pressBlockDdl, pressBlockTable},
		{pressQuarterDdl, pressQuarterTable},
	} {
		if !containsTable(tt.ddl, tt.table) {
			t.Errorf("DDL не создаёт таблицу %q:\n%s", tt.table, tt.ddl)
		}
	}
	if cbrKeyRatePressImporter.Name() != pressTable {
		t.Errorf("Name() = %q, want %q", cbrKeyRatePressImporter.Name(), pressTable)
	}
}

// containsTable — в DDL есть «CREATE TABLE IF NOT EXISTS <table> (».
func containsTable(ddl, table string) bool {
	return strings.Contains(ddl, "CREATE TABLE IF NOT EXISTS "+table+" (")
}

// TestPressWantedMeetings — окно истории: с 2024-02-16 по сегодня включительно,
// будущие заседания не импортируются.
func TestPressWantedMeetings(t *testing.T) {
	meetings := []pressMeeting{
		{Date: pressDay(t, "2024-01-01")},
		{Date: pressDay(t, "2024-02-16")},
		{Date: pressDay(t, "2026-10-23")},
		{Date: pressDay(t, "2026-12-18")},
	}
	got := pressWantedMeetings(meetings, pressHistoryStart, pressDay(t, "2026-10-23"))
	if len(got) != 2 || got[0].Date.Format(pressDayLayout) != "2024-02-16" || got[1].Date.Format(pressDayLayout) != "2026-10-23" {
		t.Errorf("окно = %v, want 2024-02-16 и 2026-10-23", got)
	}
}

// TestPressMissingMeetings — перекачиваются неизвестные даты, шапки без блоков и
// даты с квартальным абзацем без строки квартала.
func TestPressMissingMeetings(t *testing.T) {
	d1 := pressDay(t, "2024-02-16")
	d2 := pressDay(t, "2024-03-22")
	d3 := pressDay(t, "2024-04-26")
	wanted := []pressMeeting{{Date: d1}, {Date: d2}, {Date: d3}}
	known := map[time.Time]bool{d1: true, d2: true, d3: true}
	blocks := map[time.Time]bool{d1: true, d2: true, d3: true}
	gaps := map[time.Time]bool{d3: true}
	got := pressMissingMeetings(wanted, known, blocks, gaps)
	if len(got) != 1 || got[0].Date != d3 {
		t.Errorf("перекачка = %v, want только %s (дыра квартала)", got, d3.Format(pressDayLayout))
	}
	blocks[d2] = false
	got = pressMissingMeetings(wanted, known, blocks, gaps)
	if len(got) != 2 {
		t.Errorf("перекачка = %v, want две даты (шапка без блоков + дыра квартала)", got)
	}
}

// TestPressDeltas — шаг к предыдущему решению: NULL у первого, 0 у hold, ± б.п. у
// cut/hike; считает по всему ряду, а не по порядку поступления.
func TestPressDeltas(t *testing.T) {
	d1 := pressDay(t, "2024-02-16")
	d2 := pressDay(t, "2024-03-22")
	d3 := pressDay(t, "2024-04-26")
	docs := []pressFetched{
		{Meeting: pressMeeting{Date: d1}, Doc: &pressDoc{Rate: 16.0}},
		{Meeting: pressMeeting{Date: d2}, Doc: &pressDoc{Rate: 16.0}},
		{Meeting: pressMeeting{Date: d3}, Doc: &pressDoc{Rate: 16.5}},
	}
	got := pressDeltas(nil, docs)
	if got[d1] != nil {
		t.Errorf("дельта первого решения = %v, want NULL", *got[d1])
	}
	if got[d2] == nil || *got[d2] != 0 {
		t.Errorf("дельта hold = %v, want 0", got[d2])
	}
	if got[d3] == nil || *got[d3] != 50 {
		t.Errorf("дельта повышения = %v, want +50", got[d3])
	}

	docs = docs[:1]
	got = pressDeltas([]pressPast{{Date: pressDay(t, "2023-12-15"), Rate: 15.0}}, docs)
	if got[d1] == nil || *got[d1] != 100 {
		t.Errorf("дельта к прошлой ставке = %v, want +100", got[d1])
	}
}
