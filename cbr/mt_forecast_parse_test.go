package cbr

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/xuri/excelize/v2"
)

// Разбор «Табл 1» книги «Комментария к среднесрочному прогнозу» (cbr_mt_forecast) проверяется на
// двух живых выпусках (спека §3.9): свежем 05.08.2026 — горизонт 2026–2029, 64 точки, —
// и предыдущем 07.05.2026 — горизонт 2026–2028, 48 точек. Фикстуры — урезанные zip-хирургией
// реальные книги ЦБ (scripts/xlsx_trim.py, стандарт §11.1–11.2): XML оставленного листа «Табл 1»
// в них побайтово тот же, что у источника, поэтому споты сверяются с литералами живого файла.
const (
	mtForecastFixture05082026 = "testdata/mt_forecast_05082026.xlsx"
	mtForecastFixture07052026 = "testdata/mt_forecast_07052026.xlsx"

	// mtForecastFixtureSheet — имя листа в фикстурах: в книгах источника 24–25 листов, урезание
	// оставило один (стандарт §11.2).
	mtForecastFixtureSheet = "Табл 1"
)

// mtForecastSourceLabels — подписи 16 строк «Табл 1» дословно из книги источника — сырые: NBSP,
// EN SPACE, ведущие пробелы, приклеенный номер сноски («…валюте3») — → код реестра. Литералы, а
// не ссылка на реестр: сверка идёт с текстом источника, поэтому тихая правка подписи в реестре
// (или в нормализации) ловится здесь, а не на следующем выпуске.
var mtForecastSourceLabels = map[string]string{
	"Инфляция, в\u00a0%, декабрь к\u00a0декабрю предыдущего года":            "cpi_dec",
	"Инфляция, в\u00a0среднем за\u00a0год, в\u00a0% к\u00a0предыдущему году": "cpi_avg",
	"Ключевая ставка, в среднем за год, в % годовых":                         "key_rate_avg",
	"Валовой внутренний продукт":                                             "gdp_yoy",
	"–\u00a0в %, IV квартал к IV кварталу предыдущего года":                  "gdp_q4_yoy",
	"Расходы на\u00a0конечное потребление":                                   "consumption_total",
	"– домашних хозяйств":                                                    "consumption_households",
	"Валовое накопление":                                                     "gross_capital_formation",
	"– основного капитала":                                                   "fixed_capital_investment",
	"Экспорт": "exports",
	"Импорт":  "imports",
	"Денежная масса в\u00a0национальном определении":                           "m2",
	"Требования банковской системы к экономике в рублях и иностранной валюте3": "bank_claims_total",
	"–\u2002к организациям":           "bank_claims_organizations",
	"–\u2002к населению, в том числе": "bank_claims_households",
	"   ипотечные жилищные кредиты":   "bank_claims_mortgage",
}

// mtForecastBookOrder — порядок строк «Табл 1» в книге (спека §3.2). Разбор обходит строки сверху
// вниз, поэтому порядок точек — этот список × прогнозные годы по возрастанию. Литералы, а не
// ссылка на реестр: перестановка строк источника или порядка обхода видна здесь.
var mtForecastBookOrder = []string{
	"cpi_dec", "cpi_avg", "key_rate_avg", "gdp_yoy", "gdp_q4_yoy", "consumption_total",
	"consumption_households", "gross_capital_formation", "fixed_capital_investment",
	"exports", "imports", "m2", "bank_claims_total", "bank_claims_organizations",
	"bank_claims_households", "bank_claims_mortgage",
}

// mtForecastSpot — ожидаемая точка выпуска: код показателя, прогнозный год и границы диапазона.
type mtForecastSpot struct {
	indicator string
	year      uint16
	lo, hi    float64
}

// mtForecastVintage — выпуск-фикстура и ожидания его разбора: число точек, прогнозные годы шапки
// и спот-значения из спеки §3.9.
type mtForecastVintage struct {
	file  string
	total int
	years []uint16
	spots []mtForecastSpot
}

// mtForecastVintages — два живых выпуска с xlsx (спека §2, §3.9). Горизонт у выпусков разный —
// 4 и 3 прогнозных года, — и он берётся из шапки, а не из числа колонок: число точек 64 и 48
// ловит потерянный или лишний год. Споты: все 16 строк на первый прогнозный год (границы каждой
// строки) плюс ячейки особых форм — приклеенная сноска («14,5-14,61»), точка («4,0»), скобочные
// отрицательные, целые через en dash.
var mtForecastVintages = []mtForecastVintage{
	{
		file:  mtForecastFixture05082026,
		total: 64,
		years: []uint16{2026, 2027, 2028, 2029},
		spots: []mtForecastSpot{
			{"cpi_dec", 2026, 6.0, 7.0}, {"cpi_dec", 2028, 4.0, 4.0}, {"cpi_dec", 2029, 4.0, 4.0},
			{"cpi_avg", 2026, 5.9, 6.2}, {"cpi_avg", 2027, 4.3, 5.2},
			{"key_rate_avg", 2026, 14.5, 14.6}, {"key_rate_avg", 2027, 10.5, 12.5},
			{"key_rate_avg", 2028, 8.0, 9.0}, {"key_rate_avg", 2029, 7.5, 8.5},
			{"gdp_yoy", 2026, 0.0, 1.0},
			{"gdp_q4_yoy", 2026, 0.0, 1.5},
			{"consumption_total", 2026, 1.5, 2.5},
			{"consumption_households", 2026, 1.5, 2.5},
			{"gross_capital_formation", 2026, -3.5, -1.5},
			{"fixed_capital_investment", 2026, -1.5, 0.5},
			{"exports", 2026, 0.0, 2.0},
			{"imports", 2026, 1.0, 3.0},
			{"m2", 2026, 7.0, 12.0}, {"m2", 2027, 6.0, 11.0},
			{"bank_claims_total", 2026, 6.0, 10.0},
			{"bank_claims_organizations", 2026, 7.0, 11.0},
			{"bank_claims_households", 2026, 5.0, 9.0},
			{"bank_claims_mortgage", 2026, 6.0, 10.0}, {"bank_claims_mortgage", 2028, 10.0, 15.0},
		},
	},
	{
		file:  mtForecastFixture07052026,
		total: 48,
		years: []uint16{2026, 2027, 2028},
		spots: []mtForecastSpot{
			{"cpi_dec", 2026, 4.5, 5.5}, {"cpi_dec", 2028, 4.0, 4.0},
			{"cpi_avg", 2026, 5.1, 5.6},
			{"key_rate_avg", 2026, 14.0, 14.5}, {"key_rate_avg", 2027, 8.0, 10.0},
			{"key_rate_avg", 2028, 7.5, 8.5},
			{"gdp_yoy", 2026, 0.5, 1.5},
			{"gdp_q4_yoy", 2026, 1.0, 2.0},
			{"consumption_total", 2026, 0.5, 1.5},
			{"consumption_households", 2026, 0.5, 1.5},
			{"gross_capital_formation", 2026, 1.0, 3.0},
			{"fixed_capital_investment", 2026, 0.0, 2.0},
			{"exports", 2026, 0.5, 2.5},
			{"imports", 2026, 0.5, 2.5},
			{"m2", 2026, 5.0, 10.0},
			{"bank_claims_total", 2026, 6.0, 10.0},
			{"bank_claims_organizations", 2026, 7.0, 11.0},
			{"bank_claims_households", 2026, 5.0, 9.0},
			{"bank_claims_mortgage", 2026, 6.0, 10.0}, {"bank_claims_mortgage", 2027, 10.0, 15.0},
		},
	},
}

// mtForecastFixtureReleases — выпуски фикстур: даты заседания и публикации и адрес страницы взяты
// из живой разведки (спека §2, source-report §6), а не выдуманы. Разбору книги выпуск нужен
// только для строк батча (mtForecastAppendPoints), но тест обязан подавать то, что подаст импортёр.
var mtForecastFixtureReleases = map[string]mtForecastRelease{
	mtForecastFixture05082026: {
		MeetingDate: mtForecastFixtureDate(2026, time.July, 24),
		ReleaseDate: mtForecastFixtureDate(2026, time.August, 5),
		URL:         "https://www.cbr.ru/dkp/mp_dec/decision_key_rate/comment_05082026/",
	},
	mtForecastFixture07052026: {
		MeetingDate: mtForecastFixtureDate(2026, time.April, 24),
		ReleaseDate: mtForecastFixtureDate(2026, time.May, 7),
		URL:         "https://www.cbr.ru/dkp/mp_dec/decision_key_rate/comment_07052026/",
	},
}

// mtForecastFixtureDate — дата в UTC: ожидания выписаны литералами из источника.
func mtForecastFixtureDate(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// mtForecastOpenFixture — копия живой фикстуры в памяти под мутацию: ветки fail-fast и warn
// проверяются на ней, а не на синтетической книге (стандарт §11.1). Правки идут по копии в
// памяти — файл на диске не меняется.
func mtForecastOpenFixture(t *testing.T, file string, mutate func(t *testing.T, f *excelize.File)) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile(file)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", file, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	mutate(t, f)
	return f
}

// mtForecastSetCell — правка ячейки мутируемой фикстуры: excelize сообщает о негодной правке
// ошибкой, и проглотить её значило бы проверять немодифицированный лист.
func mtForecastSetCell(t *testing.T, f *excelize.File, cell string, value any) {
	t.Helper()
	if err := f.SetCellValue(mtForecastFixtureSheet, cell, value); err != nil {
		t.Fatalf("SetCellValue(%s!%s, %v): %v", mtForecastFixtureSheet, cell, value, err)
	}
}

// mtForecastParseFixture — разбор фикстуры тем же кодом, что и импортёр: ошибка и пустой разбор —
// падение теста, потому что фикстура и есть контракт разбора (стандарт §11.1).
func mtForecastParseFixture(t *testing.T, file string) []mtForecastPoint {
	t.Helper()
	f := mtForecastOpenFixture(t, file, func(*testing.T, *excelize.File) {})
	points, err := mtForecastParseTabl1(f)
	if err != nil {
		t.Fatalf("разбор фикстуры %s: %v", file, err)
	}
	if len(points) == 0 {
		t.Fatalf("разбор фикстуры %s не дал ни одной точки", file)
	}
	return points
}

// mtForecastPointValue — точка (показатель, год) в разборе: отсутствие точки фатально — спот,
// которого нет, ничего не проверяет.
func mtForecastPointValue(t *testing.T, points []mtForecastPoint, indicator string, year uint16) mtForecastPoint {
	t.Helper()
	for _, point := range points {
		if point.Indicator == indicator && point.Year == year {
			return point
		}
	}
	t.Fatalf("в разборе нет точки (%q, %d)", indicator, year)
	return mtForecastPoint{}
}

// mtForecastCheckSpot — сверка границ точки с литералами из файла источника. Допуск общий с
// Float64-таблицами пакета (cbrSpotEqualFloat64, 1e-9): разбор идёт через ParseFloat десятичной
// записи ячейки, поэтому границы сверяются как значения, а не как текст.
func mtForecastCheckSpot(t *testing.T, points []mtForecastPoint, indicator string, year uint16, lo, hi float64) {
	t.Helper()
	got := mtForecastPointValue(t, points, indicator, year)
	if !cbrSpotEqualFloat64(got.Lo, lo) || !cbrSpotEqualFloat64(got.Hi, hi) {
		t.Errorf("точка (%q, %d) = (%v, %v), want (%v, %v) (значения из файла источника)",
			indicator, year, got.Lo, got.Hi, lo, hi)
	}
}

// mtForecastWarnHook перехватывает предупреждения logrus на время теста: «предупредить и
// пропустить» — контракт с оператором (стандарт §6.6, §12.3), и тест обязан видеть, что разбор
// предупредил, а не просто промолчал.
func mtForecastWarnHook(t *testing.T) *test.Hook {
	t.Helper()
	hook := test.NewGlobal()
	t.Cleanup(func() { log.StandardLogger().ReplaceHooks(make(log.LevelHooks)) })
	return hook
}

// mtForecastWarnMessages — сообщения уровня warn, собранные хуком.
func mtForecastWarnMessages(hook *test.Hook) []string {
	var messages []string
	for _, entry := range hook.AllEntries() {
		if entry.Level == log.WarnLevel {
			messages = append(messages, entry.Message)
		}
	}
	return messages
}

// mtForecastAssertWarn — хотя бы одно предупреждение содержит каждую из ожидаемых подстрок.
func mtForecastAssertWarn(t *testing.T, hook *test.Hook, wants ...string) {
	t.Helper()
	messages := mtForecastWarnMessages(hook)
	if len(messages) == 0 {
		t.Fatalf("предупреждений нет, want сообщение про %v", wants)
	}
	for _, want := range wants {
		found := false
		for _, message := range messages {
			if strings.Contains(message, want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("ни одно предупреждение не содержит %q (сообщения: %v)", want, messages)
		}
	}
}

// TestMtForecastIndicatorRegistry: реестр 16 показателей совпадает с §3.2 спеки — коды и подписи
// уникальны, поля заполнены, Core стоит ровно у трёх показателей (cpi_dec, cpi_avg, key_rate_avg:
// ради них пункт существует), поиск по подписи находит каждый показатель и не находит чужой.
// Отдельно проверяются подписи источника: сырые строки книги (с NBSP, EN SPACE и приклеенной
// сноской) после нормализации обязаны дать объявленные подписи — иначе разбор молча пропустит
// строку, а пропажа core-показателя уронит прогон.
func TestMtForecastIndicatorRegistry(t *testing.T) {
	if len(mtForecastIndicators) != len(mtForecastBookOrder) {
		t.Fatalf("показателей в реестре %d, want %d (спека §3.2)", len(mtForecastIndicators), len(mtForecastBookOrder))
	}
	core := map[string]bool{"cpi_dec": true, "cpi_avg": true, "key_rate_avg": true}
	seenCodes := make(map[string]bool, len(mtForecastIndicators))
	seenLabels := make(map[string]string, len(mtForecastIndicators))
	for _, indicator := range mtForecastIndicators {
		for field, value := range map[string]string{
			"code": indicator.Code, "label": indicator.Label, "title": indicator.Title, "unit": indicator.Unit,
		} {
			if strings.TrimSpace(value) == "" {
				t.Errorf("показатель %q: пустое поле %s", indicator.Code, field)
			}
		}
		if seenCodes[indicator.Code] {
			t.Errorf("код %q объявлен дважды", indicator.Code)
		}
		seenCodes[indicator.Code] = true
		if other, dup := seenLabels[indicator.Label]; dup {
			t.Errorf("подпись %q назначена и %q, и %q: подпись — ключ поиска строки", indicator.Label, other, indicator.Code)
		}
		seenLabels[indicator.Label] = indicator.Code
		if indicator.Core != core[indicator.Code] {
			t.Errorf("показатель %q: Core=%v, ожидалось %v (core — три показателя спеки §3.2)",
				indicator.Code, indicator.Core, core[indicator.Code])
		}
		got, ok := mtForecastIndicatorByLabel(indicator.Label)
		if !ok || got.Code != indicator.Code {
			t.Errorf("подпись %q: поиск вернул (%q, %v), ожидался код %q", indicator.Label, got.Code, ok, indicator.Code)
		}
	}
	for code := range core {
		if !seenCodes[code] {
			t.Errorf("core-показатель %q не объявлен в реестре", code)
		}
	}
	if _, ok := mtForecastIndicatorByLabel("Инвестиции в основной капитал"); ok {
		t.Error("незнакомая подпись найдена в реестре — реестр шире 16 показателей")
	}
	for raw, want := range mtForecastSourceLabels {
		got, ok := mtForecastIndicatorByLabel(mtForecastNormalizeLabel(raw))
		if !ok {
			t.Errorf("подпись источника %q после нормализации не найдена в реестре", raw)
			continue
		}
		if got.Code != want {
			t.Errorf("подпись источника %q → код %q, ожидался %q", raw, got.Code, want)
		}
	}
}

// TestMtForecastNormalizeCell: базовая нормализация (спека §3.3, слой 1) приводит ячейку к
// сравнимому виду — пробелы-двойники (NBSP, EN SPACE, узкий и hair space) и переводы строк
// становятся пробелом, soft hyphen удаляется, серии пробелов схлопываются, края обрезаются.
// Ни цифры, ни тире не трогаются: сноску-цифру и тире снимает слой подписи, а не базовая
// нормализация (иначе базовая форма перестала бы быть общей для подписей и значений).
func TestMtForecastNormalizeCell(t *testing.T) {
	for name, tc := range map[string]struct{ raw, want string }{
		"перенос строки шапки":      {"2025\n(факт)", "2025 (факт)"},
		"NBSP":                      {"Инфляция, в\u00a0%, декабрь", "Инфляция, в %, декабрь"},
		"EN SPACE":                  {"–\u2002к организациям", "– к организациям"},
		"узкий пробел и hair space": {"13,3 –\u200a14,0", "13,3 – 14,0"},
		"табуляция":                 {"1\tС учетом", "1 С учетом"},
		"серии пробелов":            {"2   Данные", "2 Данные"},
		"ведущие и хвостовые":       {"  1,0 ", "1,0"},
		"soft hyphen удаляется":     {"фор\u00adмат", "формат"},
		"цифра сноски не снимается": {"валюте3", "валюте3"},
		"тире не трогается":         {"– домашних хозяйств", "– домашних хозяйств"},
		"пустая ячейка":             {"", ""},
		"только пробелы-двойники":   {"\u00a0\u00a0", ""},
	} {
		t.Run(name, func(t *testing.T) {
			if got := mtForecastNormalizeCell(tc.raw); got != tc.want {
				t.Errorf("mtForecastNormalizeCell(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

// TestMtForecastNormalizeLabel: слой подписи (спека §3.3, слой 2) — к базовой нормализации
// добавляются оба тире (en dash U+2013, em dash U+2014) → ASCII-дефис и срезание хвостовых цифр
// (приклеенный номер сноски у строки «…валюте3»). Точное совпадение с картой — после этого слоя:
// чужая метрика и пустая подпись показателем не становятся.
func TestMtForecastNormalizeLabel(t *testing.T) {
	for name, tc := range map[string]struct{ raw, want string }{
		"en dash": {"– к организациям", "- к организациям"},
		"em dash": {"— основного капитала", "- основного капитала"},
		"хвостовая цифра сноски":  {"Требования банковской системы…валюте3", "Требования банковской системы…валюте"},
		"две хвостовые цифры":     {"строка сноски11", "строка сноски"},
		"цифра не с хвоста":       {"1 С учетом", "1 С учетом"},
		"подпись с NBSP и цифрой": {"Расходы на\u00a0конечное потребление2", "Расходы на конечное потребление"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := mtForecastNormalizeLabel(tc.raw); got != tc.want {
				t.Errorf("mtForecastNormalizeLabel(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

// TestMtForecastFixtureRows: разбор каждой фикстуры даёт ожидаемое число точек (16 показателей ×
// число прогнозных лет) и ровно ожидаемую последовательность (показатель, год): строки книги
// сверху вниз, внутри строки — прогнозные годы по возрастанию (спека §3.3). Последовательность,
// а не только счёт: потерянная, лишняя или перевёрнутая колонка года не проходит, как и
// переставленная строка показателя.
func TestMtForecastFixtureRows(t *testing.T) {
	for _, vintage := range mtForecastVintages {
		t.Run(vintage.file, func(t *testing.T) {
			points := mtForecastParseFixture(t, vintage.file)
			if len(points) != vintage.total {
				t.Errorf("точек %d, want %d (16 показателей × %d прогнозных лет)",
					len(points), vintage.total, len(vintage.years))
			}
			i := 0
			var mismatches int
			for _, indicator := range mtForecastBookOrder {
				for _, year := range vintage.years {
					if i >= len(points) {
						t.Fatalf("точек меньше ожидаемых: последовательность оборвалась на (%s, %d)", indicator, year)
					}
					if got := points[i]; got.Indicator != indicator || got.Year != year {
						if mismatches == 0 {
							t.Errorf("точка %d = (%q, %d), want (%q, %d) — порядок разбора построчный (спека §3.3)",
								i, got.Indicator, got.Year, indicator, year)
						}
						mismatches++
					}
					i++
				}
			}
			if i != len(points) {
				t.Errorf("ожидаемая последовательность покрывает %d точек, разбор дал %d", i, len(points))
			}
		})
	}
}

// TestMtForecastSpotValues: значения из известных ячеек каждого выпуска (спека §3.9) — все 16
// строк на первый прогнозный год плюс ячейки особых форм: приклеенная сноска у верхней границы
// («14,5-14,61» → 14,6), точечный прогноз («4,0» → lo == hi), скобочные отрицательные
// («(-3,5)-(-1,5)»), целые диапазоны через en dash («7–12»). Значения литералами: сверка идёт с
// разбором, а не с константой кода.
func TestMtForecastSpotValues(t *testing.T) {
	for _, vintage := range mtForecastVintages {
		t.Run(vintage.file, func(t *testing.T) {
			points := mtForecastParseFixture(t, vintage.file)
			for _, spot := range vintage.spots {
				mtForecastCheckSpot(t, points, spot.indicator, spot.year, spot.lo, spot.hi)
			}
		})
	}
}

// TestMtForecastValueForms: формы прогнозной ячейки (спека §3.3) — точка и диапазон, оба тире
// (дефис, en dash, em dash), скобочные отрицательные, целые, пробелы вокруг тире, приклеенный
// номер сноски у ровно 2-знаковой дроби и юникод-надстрочные цифры в любой позиции.
func TestMtForecastValueForms(t *testing.T) {
	forms := map[string]struct {
		cell string
		lo   float64
		hi   float64
	}{
		"точка": {"4,0", 4.0, 4.0},
		"точка с точкой-разделителем":    {"4.0", 4.0, 4.0},
		"отрицательная точка":            {"-4,9", -4.9, -4.9},
		"диапазон дефисом":               {"6,0-7,0", 6.0, 7.0},
		"диапазон en dash":               {"7\u201312", 7.0, 12.0},
		"диапазон em dash":               {"7\u201412", 7.0, 12.0},
		"пробелы вокруг тире":            {"6 – 10", 6.0, 10.0},
		"отрицательные в скобках":        {"(-3,5)-(-1,5)", -3.5, -1.5},
		"скобка и положительная":         {"(-1,5)-0,5", -1.5, 0.5},
		"приклеенная сноска в диапазоне": {"14,5-14,61", 14.5, 14.6},
		"приклеенная сноска в точке":     {"14,61", 14.6, 14.6},
		"надстрочная сноска":             {"14,5-14,6\u00b9", 14.5, 14.6},
		"надстрочная сноска U+2075":      {"4,0\u2075", 4.0, 4.0},
		"NBSP и хвостовой пробел":        {"\u00a06,0-7,0 ", 6.0, 7.0},
	}
	for name, tc := range forms {
		t.Run(name, func(t *testing.T) {
			lo, hi, err := mtForecastBounds(tc.cell)
			if err != nil {
				t.Fatalf("mtForecastBounds(%q): %v", tc.cell, err)
			}
			if !cbrSpotEqualFloat64(lo, tc.lo) || !cbrSpotEqualFloat64(hi, tc.hi) {
				t.Errorf("mtForecastBounds(%q) = (%v, %v), want (%v, %v)", tc.cell, lo, hi, tc.lo, tc.hi)
			}
		})
	}
}

// TestMtForecastValueErrors: форма, которой у источника нет, — ошибка, а не пропуск (спека §3.3):
// пустая ячейка, текст вместо числа, границы в обратном порядке (lo > hi — страховка от искажения
// приклеенной сноской), недописанный диапазон, факт-ячейка со сноской («– 2»).
func TestMtForecastValueErrors(t *testing.T) {
	for name, cell := range map[string]string{
		"пустая":       "",
		"текст":        "около 5",
		"ло больше хи": "5,0-4,0",
		"недописанный диапазон":   "6,0-",
		"два разделителя":         "6,0-7,0-8,0",
		"факт-ячейка со сноской":  "– 2",
		"проценты в ячейке":       "4,0 %",
		"только номер сноски":     "\u00b9",
		"только пробелы-двойники": "\u00a0\u00a0",
	} {
		t.Run(name, func(t *testing.T) {
			lo, hi, err := mtForecastBounds(cell)
			if err == nil {
				t.Fatalf("mtForecastBounds(%q) = (%v, %v), ожидалась ошибка: незнакомая форма — не пропуск", cell, lo, hi)
			}
		})
	}
	// Границы в обратном порядке — отдельная причина: lo > hi означает, что разбор исказил
	// значение (например, снял приклеенную сноску не с той границы), и молча записанный
	// «диапазон» перевернул бы смысл прогноза.
	if _, _, err := mtForecastBounds("5,0-4,0"); err == nil || !strings.Contains(err.Error(), "lo") {
		t.Errorf("ошибка для «5,0-4,0» не называет порядок границ: %v", err)
	}
}

// TestMtForecastFailFastOnFixture — ветки, где разбор обязан вернуть ошибку с именем таблицы и
// адресом ячейки, а не «0 точек» (стандарт §6.4, спека §3.3), — на мутированной копии живой
// фикстуры 05.08.2026 (стандарт §11.1). Копия, а не синтетическая книга: якоря, шапка и ячейки в
// ней те же, что у источника, поэтому ошибка приходит от мутации, а не от сборки книги. Возврат
// nil на ошибке — контракт: при ошибке разбор не отдаёт ни одной точки, частично разобранный
// выпуск хуже пустого.
func TestMtForecastFailFastOnFixture(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate  func(t *testing.T, f *excelize.File)
		wantErr []string
	}{
		"книга без листа «Табл 1»": {
			// Переименованный лист — источник сменил имя таблицы или это чужая книга: ошибка до
			// чтения данных, со списком листов книги.
			mutate:  func(t *testing.T, f *excelize.File) { mtForecastRenameSheet(t, f, "Лист1") },
			wantErr: []string{"cbr_mt_forecast", "нет листа «Табл 1»", "Лист1"},
		},
		"лист без строки-показателя": {
			// Подписи всех 16 строк заменены на незнакомую: якорь разбора пропал — ошибка, а не
			// «0 точек» (стандарт §6.4).
			mutate: func(t *testing.T, f *excelize.File) {
				for row := 8; row <= 23; row++ {
					mtForecastSetCell(t, f, fmt.Sprintf("A%d", row), "показатель")
				}
			},
			wantErr: []string{"cbr_mt_forecast", "лист «Табл 1»", "ни одной строки-показателя"},
		},
		"шапка без годов": {
			// Годы шапки подменены текстом: выше первой строки-показателя нет строки с двумя
			// годами — разбор не знает, какие колонки прогнозные.
			mutate: func(t *testing.T, f *excelize.File) {
				for _, cell := range []string{"C6", "D6", "E6", "F6"} {
					mtForecastSetCell(t, f, cell, "прогноз")
				}
			},
			wantErr: []string{"cbr_mt_forecast", "шапк", "строка 8"},
		},
		"колонка шапки ни год, ни факт": {
			// Ячейка шапки не год и не факт-пометка: разбор не знает, что это за колонка, и
			// истолковать её молча значило бы приписать значения не тому году.
			mutate:  func(t *testing.T, f *excelize.File) { mtForecastSetCell(t, f, "D6", "IV кв.") },
			wantErr: []string{"cbr_mt_forecast", "Табл 1!D6", "ни год, ни факт"},
		},
		"дубль года в шапке": {
			// Два столбца с одним годом: две точки (показатель, год) в одной книге — какая из них
			// настоящая, разбор не знает (спека §3.3).
			mutate:  func(t *testing.T, f *excelize.File) { mtForecastSetCell(t, f, "D6", 2026) },
			wantErr: []string{"cbr_mt_forecast", "Табл 1!D6", "2026", "повторя"},
		},
		"пустая прогнозная ячейка": {
			// Пустая ячейка прогноза — «значение, где ожидалось число» (стандарт §6.4): молча
			// пропущенная, она оставила бы (показатель, год) без прогноза.
			mutate:  func(t *testing.T, f *excelize.File) { mtForecastSetCell(t, f, "C8", "") },
			wantErr: []string{"cbr_mt_forecast", "Табл 1!C8", "пуст"},
		},
		"неразбираемая ячейка": {
			mutate:  func(t *testing.T, f *excelize.File) { mtForecastSetCell(t, f, "C8", "около 5") },
			wantErr: []string{"cbr_mt_forecast", "Табл 1!C8", `"около 5"`},
		},
		"границы в обратном порядке": {
			mutate:  func(t *testing.T, f *excelize.File) { mtForecastSetCell(t, f, "C8", "5,0-4,0") },
			wantErr: []string{"cbr_mt_forecast", "Табл 1!C8", "lo"},
		},
		"нет core-строки": {
			// Строка core-показателя исчезла из книги (подпись снята): точки остальных 15 строк
			// разбираемы, но ради cpi_avg и собратьев импортёр и заведён (спека §3.2) — ошибка, а
			// не предупреждение.
			mutate:  func(t *testing.T, f *excelize.File) { mtForecastSetCell(t, f, "A9", "") },
			wantErr: []string{"cbr_mt_forecast", "cpi_avg", "core"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			points, err := mtForecastParseTabl1(mtForecastOpenFixture(t, mtForecastFixture05082026, tc.mutate))
			if err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d точек", len(points))
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("ошибка %q не содержит %q", err, want)
				}
			}
			if len(points) != 0 {
				t.Errorf("при ошибке разбор отдал %d точек — частичный выпуск хуже пустого", len(points))
			}
		})
	}
}

// TestMtForecastUnknownLabelWarns: незнакомая подпись строки — предупреждение и пропуск, а не
// ошибка (спека §3.3, стандарт §6.6): новые строки источника — ожидаемое изменение, и падение
// шага лишило бы данных все последующие выпуски цепочки dagu (стандарт §12.3). Подпись заменена
// на живую строку прогноза ЦБ, которой в «Табл 1» нет, — не выдуманный текст. Предупреждений
// два, и оба штатные: переименованная строка неизвестна реестру, а показатель m2 после этого не
// разобран — второй факт виден из того же разбора, и молчать о нём нельзя.
func TestMtForecastUnknownLabelWarns(t *testing.T) {
	hook := mtForecastWarnHook(t)
	f := mtForecastOpenFixture(t, mtForecastFixture05082026, func(t *testing.T, f *excelize.File) {
		mtForecastSetCell(t, f, "A19", "Инвестиции в основной капитал")
	})
	points, err := mtForecastParseTabl1(f)
	if err != nil {
		t.Fatalf("незнакомая подпись не должна валить разбор (стандарт §6.6): %v", err)
	}
	if len(points) != 60 {
		t.Errorf("точек %d, want 60 (64 − 4: строка m2 пропущена)", len(points))
	}
	for _, point := range points {
		if point.Indicator == "m2" {
			t.Error("ряд m2 разобран, хотя его строка переименована в незнакомую подпись")
		}
	}
	messages := mtForecastWarnMessages(hook)
	if len(messages) != 2 {
		t.Fatalf("предупреждений %d, want 2 (незнакомая подпись строки и пропавший из-за неё m2): %v",
			len(messages), messages)
	}
	mtForecastAssertWarn(t, hook, "Инвестиции в основной капитал", "строка 19", "пропущен")
	mtForecastAssertWarn(t, hook, "m2", "не найдены")
}

// TestMtForecastMissingNonCoreWarns: строка не-core показателя исчезла из книги — предупреждение
// со списком кодов и разбор остальных 15 строк (спека §3.3): полнота набора важна, но не блокирует
// цепочку. Ошибкой остаётся только пропажа core-показателя.
func TestMtForecastMissingNonCoreWarns(t *testing.T) {
	hook := mtForecastWarnHook(t)
	f := mtForecastOpenFixture(t, mtForecastFixture05082026, func(t *testing.T, f *excelize.File) {
		mtForecastSetCell(t, f, "A17", "")
	})
	points, err := mtForecastParseTabl1(f)
	if err != nil {
		t.Fatalf("пропавшая не-core строка — предупреждение, а не ошибка (спека §3.3): %v", err)
	}
	if len(points) != 60 {
		t.Errorf("точек %d, want 60 (64 − 4: строка exports пропущена)", len(points))
	}
	for _, point := range points {
		if point.Indicator == "exports" {
			t.Error("ряд exports разобран, хотя его строка снята из книги")
		}
	}
	messages := mtForecastWarnMessages(hook)
	if len(messages) != 1 {
		t.Fatalf("предупреждений %d, want 1 (пропавшая строка не-core показателя): %v", len(messages), messages)
	}
	mtForecastAssertWarn(t, hook, "exports", "пропущен")
}

// mtForecastAppendColumns — колонки строки батча cbr_mt_forecast в порядке DDL (спека §3.1):
// meeting_date, release_date, url, indicator, year, lo, hi. Литералы, а не ссылка на DDL:
// перестановка соседних дат (заседание ↔ публикация) записала бы их наоборот, а обе — Date, и
// заметить это в таблице было бы нечем.
var mtForecastAppendColumns = []string{"meeting_date", "release_date", "url", "indicator", "year", "lo", "hi"}

// mtForecastBatch — приёмник широкой строки выпуска: строка cbr_mt_forecast — семь значений, а
// appendedRow харнесса знает формы (date, value) и (name, date, value) — прецедент разложения
// широкой строки dipVintageBatch (cbr/dip_bulletin_test.go). Точка (indicator, release_date, lo)
// уходит харнессу: на ней работает общий двусторонний контракт каталога (checkCbrSeriesMeta,
// стандарт §11.4), — а полная строка остаётся у теста: порядок колонок DDL и атрибуты выпуска
// сверяются с ней. Расхождение формы — ошибка Append, а не паника.
type mtForecastBatch struct {
	driver.Batch
	rec  *appendRecorder
	rows [][]any // принятые строки: порядок значений — порядок колонок DDL
}

// Append проверяет форму строки до записи и запоминает её только после успеха харнесса:
// отвергнутая строка записанной не считается.
func (b *mtForecastBatch) Append(v ...any) error {
	if len(v) != len(mtForecastAppendColumns) {
		return fmt.Errorf("строка выпуска из %d значений, ожидалось %d (%v)",
			len(v), len(mtForecastAppendColumns), mtForecastAppendColumns)
	}
	indicator, ok := v[3].(string)
	if !ok {
		return fmt.Errorf("имя показателя пришло как %T, ожидалась строка (колонка indicator)", v[3])
	}
	releaseDate, ok := v[1].(time.Time)
	if !ok {
		return fmt.Errorf("дата выпуска пришла как %T, ожидался time.Time (колонка release_date)", v[1])
	}
	if err := b.rec.Append(indicator, releaseDate, v[5]); err != nil {
		return err
	}
	b.rows = append(b.rows, append([]any(nil), v...))
	return nil
}

// TestMtForecastAppendPoints: обёртка записи одного выпуска (ImportFunc шаблона) пишет по строке на
// точку разбора, атрибуты выпуска — в колонки DDL, а порядок записи — порядок разбора: строки
// книги сверху вниз, внутри строки прогнозные годы по возрастанию. Фикстура 05.08.2026
// разбирается в 64 точки (TestMtForecastFixtureRows), поэтому первая строка батча — cpi_dec 2026,
// последняя — bank_claims_mortgage 2029: перевёрнутый обход или потерянная колонка винтажа
// оставили бы выпуск неотличимым от другого.
func TestMtForecastAppendPoints(t *testing.T) {
	release := mtForecastFixtureReleases[mtForecastFixture05082026]
	batch := &mtForecastBatch{rec: newCbrRecorder()}
	f := mtForecastOpenFixture(t, mtForecastFixture05082026, func(*testing.T, *excelize.File) {})
	if err := mtForecastAppendPoints(release)(f, batch); err != nil {
		t.Fatalf("запись выпуска %s: %v", release.ReleaseDate.Format("2006-01-02"), err)
	}
	if batch.rec.err != nil {
		t.Fatalf("схема строк не совпала с разбором: %v", batch.rec.err)
	}
	if len(batch.rows) != 64 {
		t.Errorf("строк батча %d, want 64 (16 показателей × 4 прогнозных года)", len(batch.rows))
	}
	for i, row := range batch.rows {
		if meeting, ok := row[0].(time.Time); !ok || !meeting.Equal(release.MeetingDate) {
			t.Fatalf("строка %d: meeting_date = %v, ожидалась %s", i, row[0], release.MeetingDate.Format("2006-01-02"))
		}
		if published, ok := row[1].(time.Time); !ok || !published.Equal(release.ReleaseDate) {
			t.Fatalf("строка %d: release_date = %v, ожидалась %s (ключ винтажа)",
				i, row[1], release.ReleaseDate.Format("2006-01-02"))
		}
		if row[2] != release.URL {
			t.Fatalf("строка %d: url = %v, ожидался %q", i, row[2], release.URL)
		}
		if _, ok := row[3].(string); !ok {
			t.Fatalf("строка %d: показатель пришёл как %T (позиция 3 — колонка indicator)", i, row[3])
		}
		if _, ok := row[4].(uint16); !ok {
			t.Fatalf("строка %d: год пришёл как %T, ожидался uint16 (колонка year — UInt16)", i, row[4])
		}
		if _, ok := row[5].(float64); !ok {
			t.Fatalf("строка %d: нижняя граница пришла как %T, ожидался float64 (колонка lo)", i, row[5])
		}
		if _, ok := row[6].(float64); !ok {
			t.Fatalf("строка %d: верхняя граница пришла как %T, ожидался float64 (колонка hi)", i, row[6])
		}
	}
	if len(batch.rows) > 0 {
		first, last := batch.rows[0], batch.rows[len(batch.rows)-1]
		if first[3] != "cpi_dec" || first[4] != uint16(2026) || first[5] != 6.0 || first[6] != 7.0 {
			t.Errorf("первая строка = (%v, %v, %v, %v), want (cpi_dec, 2026, 6, 7)",
				first[3], first[4], first[5], first[6])
		}
		if last[3] != "bank_claims_mortgage" || last[4] != uint16(2029) || last[5] != 10.0 || last[6] != 15.0 {
			t.Errorf("последняя строка = (%v, %v, %v, %v), want (bank_claims_mortgage, 2029, 10, 15)",
				last[3], last[4], last[5], last[6])
		}
	}
}

// TestMtForecastAppendPointsParseError: разбор книги не удался — обёртка возвращает ошибку и не
// пишет ни одной строки: частично записанный выпуск хуже неписанного (стандарт §6.4). Книга —
// копия живой фикстуры 05.08.2026 с пустой прогнозной ячейкой первой строки, файл на диске не
// меняется (mtForecastOpenFixture, стандарт §11.1).
func TestMtForecastAppendPointsParseError(t *testing.T) {
	release := mtForecastFixtureReleases[mtForecastFixture05082026]
	batch := &mtForecastBatch{rec: newCbrRecorder()}
	f := mtForecastOpenFixture(t, mtForecastFixture05082026, func(t *testing.T, f *excelize.File) {
		mtForecastSetCell(t, f, "C8", "")
	})
	err := mtForecastAppendPoints(release)(f, batch)
	if err == nil {
		t.Fatalf("ожидалась ошибка разбора, записано строк %d", len(batch.rows))
	}
	for _, want := range []string{"cbr_mt_forecast", "Табл 1!C8"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
	if len(batch.rows) != 0 || len(batch.rec.raw) != 0 {
		t.Errorf("при ошибке разбора записано строк %d (харнесс: %d), want 0",
			len(batch.rows), len(batch.rec.raw))
	}
}

// TestMtForecastAppendPointsAppendError: сбой Append не проглатывается — без проверки err строка
// выпуска терялась бы молча, — а ошибка называет выпуск и точку (mtForecastAppendPoints). Роль
// «батч отверг строку» играет харнесс со схемой из одной колонки: тройку (indicator, date, value)
// он отвергает так же, как настоящий батч — негодную строку (прецедент —
// TestDipAppendPointsAppendError).
func TestMtForecastAppendPointsAppendError(t *testing.T) {
	release := mtForecastFixtureReleases[mtForecastFixture05082026]
	batch := &mtForecastBatch{rec: newCbrRecorder("indicator")}
	f := mtForecastOpenFixture(t, mtForecastFixture05082026, func(*testing.T, *excelize.File) {})
	err := mtForecastAppendPoints(release)(f, batch)
	if err == nil {
		t.Fatalf("ожидалась ошибка записи, строк принято %d", len(batch.rows))
	}
	for _, want := range []string{"cbr_mt_forecast", "2026-08-05", "cpi_dec", "2026"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
}

// TestMtForecastSeriesMeta: каталог описывает ровно 16 объявленных кодов — Series = Code, source и
// частота одинаковы, заголовок и единица — из реестра, origin называет строку «Табл 1», а
// description — что измеряет показатель и свойства диапазона и винтажа (спека §3.7, стандарт
// §10.1). Запись каталога — контракт с агентом MCP: пустое поле или ряд, которого нет в реестре,
// оставили бы ряд без описания.
func TestMtForecastSeriesMeta(t *testing.T) {
	if len(cbrMtForecastSeriesMeta) != len(mtForecastIndicators) {
		t.Fatalf("каталог описывает %d рядов, want %d (16 показателей спеки §3.2)",
			len(cbrMtForecastSeriesMeta), len(mtForecastIndicators))
	}
	byCode := make(map[string]int, len(cbrMtForecastSeriesMeta))
	for i, meta := range cbrMtForecastSeriesMeta {
		if other, dup := byCode[meta.Series]; dup {
			t.Errorf("ряд %q описан дважды (записи %d и %d)", meta.Series, other, i)
		}
		byCode[meta.Series] = i
	}
	for _, indicator := range mtForecastIndicators {
		i, ok := byCode[indicator.Code]
		if !ok {
			t.Errorf("код %q не описан в каталоге", indicator.Code)
			continue
		}
		meta := cbrMtForecastSeriesMeta[i]
		if meta.Source != mtForecastSource {
			t.Errorf("ряд %q: source %q, ожидался %q", meta.Series, meta.Source, mtForecastSource)
		}
		if meta.Frequency != mtForecastFrequency {
			t.Errorf("ряд %q: частота %q, ожидалась %q (прогнозные годы — годовая)",
				meta.Series, meta.Frequency, mtForecastFrequency)
		}
		if meta.Title != indicator.Title {
			t.Errorf("ряд %q: title %q, ожидался %q (заголовок берётся из реестра)", meta.Series, meta.Title, indicator.Title)
		}
		if meta.Unit != indicator.Unit {
			t.Errorf("ряд %q: unit %q, ожидался %q (единица — из таблицы §3.2 спеки)", meta.Series, meta.Unit, indicator.Unit)
		}
		for field, value := range map[string]string{
			"series": meta.Series, "title": meta.Title, "unit": meta.Unit,
			"frequency": meta.Frequency, "origin": meta.Origin, "description": meta.Description,
		} {
			if strings.TrimSpace(value) == "" {
				t.Errorf("ряд %q: пустое поле %s", meta.Series, field)
			}
		}
		if !strings.Contains(meta.Origin, "„Табл 1“") || !strings.Contains(meta.Origin, "строка „"+indicator.Label+"“") {
			t.Errorf("ряд %q: origin %q не называет таблицу и строку источника", meta.Series, meta.Origin)
		}
		if strings.TrimSpace(mtForecastDescriptions[indicator.Code]) == "" {
			t.Errorf("ряд %q: в карте описаний нет текста показателя", meta.Series)
			continue
		}
		if !strings.Contains(meta.Description, mtForecastDescriptions[indicator.Code]) {
			t.Errorf("ряд %q: description %q не содержит описания показателя", meta.Series, meta.Description)
		}
		if !strings.Contains(meta.Description, "винтаж") || !strings.Contains(meta.Description, "lo = hi") {
			t.Errorf("ряд %q: description %q не называет свойства диапазона и винтажа (спека §3.7)",
				meta.Series, meta.Description)
		}
	}
	for code := range mtForecastDescriptions {
		if _, ok := byCode[code]; !ok {
			t.Errorf("описание показателя %q не соответствует ни одному ряду каталога", code)
		}
	}
}

// TestMtForecastSeriesMetaCoversParsedRows: двусторонний контракт каталога и разбора на живой
// фикстуре (стандарт §11.4): каждый разобранный показатель описан в каталоге и каждый описанный
// ряд разбирается — на выпуске 05.08.2026 в книге есть все 16 строк, поэтому оба направления
// проверяемы. Строка батча широкая, поэтому разбор раскладывается по именам каталога в порядке
// колонок DDL (mtForecastBatch).
func TestMtForecastSeriesMetaCoversParsedRows(t *testing.T) {
	release := mtForecastFixtureReleases[mtForecastFixture05082026]
	checkCbrSeriesMeta(t, mtForecastSource, cbrMtForecastSeriesMeta, func(rec *appendRecorder) error {
		f, err := excelize.OpenFile(mtForecastFixture05082026)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return mtForecastAppendPoints(release)(f, &mtForecastBatch{rec: rec})
	})
}

// TestMtForecastCoreIndicatorSetsUnionOfFixtures: объединение разбора двух выпусков даёт ровно
// реестр из 16 кодов — ни лишнего, ни забытого (стандарт §11.4). Проверка нужна потому, что один
// выпуск полного контракта не доказывает: строка, исчезнувшая из книги и заменённая похожей,
// осталась бы в каталоге, а прогон записал бы меньше рядов молча.
func TestMtForecastCoreIndicatorSetsUnionOfFixtures(t *testing.T) {
	parsed := make(map[string]bool, len(mtForecastIndicators))
	for _, vintage := range mtForecastVintages {
		for _, point := range mtForecastParseFixture(t, vintage.file) {
			parsed[point.Indicator] = true
		}
	}
	if len(parsed) != len(mtForecastIndicators) {
		t.Errorf("объединение выпусков дало %d кодов, want %d", len(parsed), len(mtForecastIndicators))
	}
	declared := make(map[string]bool, len(mtForecastIndicators))
	for _, indicator := range mtForecastIndicators {
		declared[indicator.Code] = true
		if !parsed[indicator.Code] {
			t.Errorf("код %q объявлен, но не разобран ни на одной фикстуре", indicator.Code)
		}
	}
	for code := range parsed {
		if !declared[code] {
			t.Errorf("код %q разобран, но не объявлен в реестре", code)
		}
	}
}

// mtForecastRenameSheet — переименование листа мутируемой фикстуры: так воспроизводится книга,
// в которой «Табл 1» называется иначе (или это чужая книга).
func mtForecastRenameSheet(t *testing.T, f *excelize.File, name string) {
	t.Helper()
	if err := f.SetSheetName(mtForecastFixtureSheet, name); err != nil {
		t.Fatalf("SetSheetName(%s, %s): %v", mtForecastFixtureSheet, name, err)
	}
}
