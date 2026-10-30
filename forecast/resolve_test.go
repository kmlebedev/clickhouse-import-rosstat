package forecast

import (
	"context"
	"errors"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

// TestMain глушит логи импортёра: Import печатает Infof на каждую записанную
// строку и Warnf на каждый пропуск, а проверяются здесь счётчик записанных строк
// и тело батча. Без этого вывод `go test -v` состоял бы из строк лога, к предмету
// проверки отношения не имеющих, и падение теста в них тонуло бы.
func TestMain(m *testing.M) {
	log.SetLevel(log.PanicLevel)

	os.Exit(m.Run())
}

// date — дата в UTC: и входные target_date, и ожидаемые границы окна в тестах
// задаются в одной зоне, потому что резолвер обязан работать с календарными
// датами, а не с моментами времени.
func date(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// TestQuarterWindow — окно сверки: [первый день квартала, содержащего
// target_date, сам target_date].
//
// Границы года и середины кварталов проверяются потому, что арифметика квартала —
// единственное место резолвера, где ошибка не видна в логе: сдвинутое окно даёт
// ДРУГОЕ среднее (в Q3-2026 COMEX: 4300.80 за квартал против 4240.32 у MOEX и
// иных значений на подокнах), то есть ненулевой actual и правдоподобный
// error_pct — ошибку, которую невозможно заметить по одному числу в журнале.
func TestQuarterWindow(t *testing.T) {
	cases := []struct {
		name      string
		target    time.Time
		wantStart time.Time
		wantEnd   time.Time
	}{
		{"конец четвёртого квартала", date(2026, time.December, 31), date(2026, time.October, 1), date(2026, time.December, 31)},
		{"середина третьего квартала", date(2026, time.July, 15), date(2026, time.July, 1), date(2026, time.July, 15)},
		{"первый день года", date(2026, time.January, 1), date(2026, time.January, 1), date(2026, time.January, 1)},
		{"конец первого квартала", date(2026, time.March, 31), date(2026, time.January, 1), date(2026, time.March, 31)},
		{"середина первого квартала следующего года", date(2027, time.February, 10), date(2027, time.January, 1), date(2027, time.February, 10)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start, end := quarterWindow(tc.target)

			if !start.Equal(tc.wantStart) || !end.Equal(tc.wantEnd) {
				t.Fatalf("quarterWindow(%s) = [%s, %s], want [%s, %s]",
					tc.target.Format(dateLayout), start.Format(dateLayout), end.Format(dateLayout),
					tc.wantStart.Format(dateLayout), tc.wantEnd.Format(dateLayout))
			}
		})
	}
}

// TestPickActual — выбор площадки: comex_front при наличии точек по нему, иначе
// moex_fix_usd, иначе «факта нет».
//
// Приоритет COMEX проверяется и при НЕПУСТОМ MOEX: подмена приоритета даёт другое
// среднее (в Q3-2026 — 4240.32 вместо 4300.80) с правдоподобным error_pct, то есть
// молча другую ошибку прогноза в витрине. Гейт идёт по КОЛИЧЕСТВУ точек, а не по
// значению среднего: пустой avgIf даёт NaN (проверено на живом ClickHouse), а NaN
// не «пусто», и любое сравнение с ним ложно.
func TestPickActual(t *testing.T) {
	cases := []struct {
		name       string
		stats      windowStats
		wantVenue  string
		wantActual float64
		wantOK     bool
	}{
		{
			"comex и moex — приоритет comex",
			windowStats{ComexCount: 64, ComexAvg: 4300.80, MoexCount: 66, MoexAvg: 4240.32},
			comexVenue, 4300.80, true,
		},
		{
			"только moex — фолбэк",
			windowStats{MoexCount: 66, MoexAvg: 4240.32},
			moexVenue, 4240.32, true,
		},
		{
			"окно пусто — факта нет",
			windowStats{},
			"", 0, false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			venue, actual, ok := pickActual(tc.stats)

			if ok != tc.wantOK {
				t.Fatalf("pickActual(%+v) ok = %t, want %t", tc.stats, ok, tc.wantOK)
			}
			if venue != tc.wantVenue || actual != tc.wantActual {
				t.Fatalf("pickActual(%+v) = (%q, %v), want (%q, %v)",
					tc.stats, venue, actual, tc.wantVenue, tc.wantActual)
			}
		})
	}
}

// TestErrorPct — error_pct = (predicted − actual) / actual × 100.
//
// Число закреплено литералом (4450 против среднего Q3-2026 4300.80 даёт
// 3.469122…%): это ровно та величина, которую приёмка задачи читает в
// v_forecast_accuracy, и равенство «формула = формула» в тесте ничего бы не
// проверило. Знак проверяется отдельным кейсом: прогноз ниже факта даёт
// отрицательную ошибку, и перевёрнутый знак молча перевернул бы смысл витрины
// (переоценка/недооценка золота читаются по знаку error_pct).
func TestErrorPct(t *testing.T) {
	cases := []struct {
		name      string
		predicted float64
		actual    float64
		want      float64
	}{
		{"прогноз выше факта — положительная ошибка", 4450, 4300.80, 3.469122},
		{"прогноз ниже факта — отрицательная ошибка", 4000, 4300.80, -6.994048},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := errorPct(tc.predicted, tc.actual)

			if math.Abs(got-tc.want) > 1e-6 {
				t.Fatalf("errorPct(%v, %v) = %v, want %v", tc.predicted, tc.actual, got, tc.want)
			}
		})
	}
}

// TestErrorBp — error_bp = (predicted − actual) × 100: уровни в % годовых,
// ошибка в базисных пунктах. Числа закреплены литералами: 14.35 против 14.00 —
// +35 б.п. (прогноз выше факта), 13.00 против 14.00 — −100 б.п. (прогноз ниже).
// Знак — часть контракта витрины: по нему читается переоценка/недооценка ставки.
func TestErrorBp(t *testing.T) {
	cases := []struct {
		name      string
		predicted float64
		actual    float64
		want      float64
	}{
		{"прогноз выше факта — положительная ошибка", 14.35, 14.00, 35},
		{"прогноз ниже факта — отрицательная ошибка", 13.00, 14.00, -100},
		{"прогноз совпал с фактом", 14.00, 14.00, 0},
		{"дробный уровень", 14.345, 14.25, 9.5},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := errorBp(tc.predicted, tc.actual)

			if math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("errorBp(%v, %v) = %v, want %v", tc.predicted, tc.actual, got, tc.want)
			}
		})
	}
}

// TestNeedsWrite — перезапись только при отсутствующем или отличающемся факте.
//
// Проверка «нужен ли батч» стоит до PrepareBatch: без неё резолвер писал бы ту же
// строку на каждом прогоне DAG-а (144 записи в сутки), а сравнение float64 в лоб
// (`!=`) засчитало бы NaN фактического среднего как «отличается» — то есть
// сломанный запрос статистики выглядел бы как постоянная нужда переписать факт.
func TestNeedsWrite(t *testing.T) {
	const actual = 4300.80

	equal := actual
	different := 4240.32

	cases := []struct {
		name string
		row  forecastRow
		want bool
	}{
		{"факта нет", forecastRow{Actual: nil}, true},
		{"факт совпадает", forecastRow{Actual: &equal}, false},
		{"факт отличается", forecastRow{Actual: &different}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := needsWrite(tc.row, actual); got != tc.want {
				t.Fatalf("needsWrite(%+v, %v) = %t, want %t", tc.row, actual, got, tc.want)
			}
		})
	}
}

// TestUTCDate — календарная дата значения сохраняется при нормализации.
//
// clickhouse-go отдаёт Date с таймзоной сессии (moex/moex_iss.go:97), и у даты в
// +03:00 полночь по Москве — это 21:00 ПРЕДЫДУЩЕГО дня UTC: `t.UTC()` сдвинул бы
// день назад. Сдвиг менял бы и окно сверки (среднее за не тот день), и — у
// target_date, попавшего на первое число квартала, — сам квартал.
func TestUTCDate(t *testing.T) {
	moscow := time.FixedZone("MSK", 3*60*60)
	got := utcDate(time.Date(2026, time.July, 1, 0, 0, 0, 0, moscow))

	if want := date(2026, time.July, 1); !got.Equal(want) {
		t.Fatalf("utcDate = %s, want %s (календарный день сдвинулся)", got, want)
	}
	if got.Location() != time.UTC {
		t.Fatalf("utcDate вернула дату в зоне %s, want UTC", got.Location())
	}
}

// TestImportWritesOnlyChanged — Import (Review Focus 3): батч собирается только из
// строк, которым факт нужен, и при отсутствии таких строк не отправляется вовсе.
//
// Фейк-стор, а не живой ClickHouse: проверяется оркестрация — окно, приоритет
// площадки, решение «писать/не писать», состав батча, — а не SQL. Тексты запросов
// закреплены отдельно (TestResolvableQueryFilters, TestWindowStatsGatesEmptyAverage).
type fakeResolveStore struct {
	rows          []forecastRow
	rateRows      []forecastRow
	stats         windowStats
	rates         map[string]float64 // target_date (YYYY-MM-DD) → ставка после заседания
	resolvableErr error
	rateReadErr   error
	windowErr     error
	rateAfterErr  error
	writeErr      error

	windows     [][2]time.Time
	rateTargets []time.Time
	writes      [][]forecastResolution
}

func (f *fakeResolveStore) resolvable(context.Context) ([]forecastRow, error) {
	if f.resolvableErr != nil {
		return nil, f.resolvableErr
	}

	return f.rows, nil
}

func (f *fakeResolveStore) rateResolvable(context.Context) ([]forecastRow, error) {
	if f.rateReadErr != nil {
		return nil, f.rateReadErr
	}

	return f.rateRows, nil
}

func (f *fakeResolveStore) rateAfter(_ context.Context, target time.Time) (float64, bool, error) {
	f.rateTargets = append(f.rateTargets, target)

	if f.rateAfterErr != nil {
		return 0, false, f.rateAfterErr
	}
	rate, ok := f.rates[target.Format(dateLayout)]

	return rate, ok, nil
}

func (f *fakeResolveStore) windowStats(_ context.Context, start, end time.Time) (windowStats, error) {
	f.windows = append(f.windows, [2]time.Time{start, end})

	if f.windowErr != nil {
		return windowStats{}, f.windowErr
	}

	return f.stats, nil
}

func (f *fakeResolveStore) writeResolutions(_ context.Context, rows []forecastResolution) error {
	if f.writeErr != nil {
		return f.writeErr
	}

	f.writes = append(f.writes, rows)

	return nil
}

func TestImportWritesOnlyChanged(t *testing.T) {
	const (
		predicted = 4450.0
		actual    = 4300.80
	)

	target := date(2026, time.September, 30)
	alreadyResolved := actual

	t.Run("пишет только строку без факта", func(t *testing.T) {
		store := &fakeResolveStore{
			rows: []forecastRow{
				{ForecastDate: date(2026, time.July, 31), TargetDate: target, Predicted: predicted},
				{ForecastDate: date(2026, time.August, 31), TargetDate: target, Predicted: predicted, Actual: &alreadyResolved},
			},
			stats: windowStats{ComexCount: 64, ComexAvg: actual, MoexCount: 66, MoexAvg: 4240.32},
		}

		count, err := resolveForecasts(context.Background(), store)
		if err != nil {
			t.Fatalf("resolveForecasts: %v", err)
		}

		if count != 1 {
			t.Fatalf("count = %d, want 1 (вторая строка уже имеет тот же факт)", count)
		}

		if len(store.writes) != 1 {
			t.Fatalf("writeResolutions вызван %d раз, want 1", len(store.writes))
		}

		batch := store.writes[0]
		if len(batch) != 1 {
			t.Fatalf("в батче %d строк, want 1", len(batch))
		}

		got := batch[0]
		wantPct := errorPct(predicted, actual)
		want := forecastResolution{
			ForecastDate: date(2026, time.July, 31),
			TargetDate:   target,
			Metric:       forecastMetric,
			Predicted:    predicted,
			Actual:       actual,
			ErrorPct:     &wantPct,
		}
		if !got.ForecastDate.Equal(want.ForecastDate) || !got.TargetDate.Equal(want.TargetDate) ||
			got.Metric != want.Metric || got.Predicted != want.Predicted || got.Actual != want.Actual ||
			got.ErrorPct == nil || *got.ErrorPct != *want.ErrorPct || got.ErrorBp != nil {
			t.Fatalf("записана строка %+v, want %+v", got, want)
		}

		// Окно — квартал target_date: строка прогноза от 31.07.2026 на 30.09.2026
		// обязана сверяться со средним за 01.07–30.09, а не за «недавнее» окно.
		if len(store.windows) != 2 {
			t.Fatalf("windowStats вызван %d раз, want 2 (по разу на строку)", len(store.windows))
		}
		wantWindow := [2]time.Time{date(2026, time.July, 1), target}
		for i, window := range store.windows {
			if !window[0].Equal(wantWindow[0]) || !window[1].Equal(wantWindow[1]) {
				t.Fatalf("окно %d = [%s, %s], want [%s, %s]", i,
					window[0].Format(dateLayout), window[1].Format(dateLayout),
					wantWindow[0].Format(dateLayout), wantWindow[1].Format(dateLayout))
			}
		}
	})

	t.Run("писать нечего — батча нет", func(t *testing.T) {
		store := &fakeResolveStore{
			rows: []forecastRow{
				{ForecastDate: date(2026, time.July, 31), TargetDate: target, Predicted: predicted, Actual: &alreadyResolved},
			},
			stats: windowStats{ComexCount: 64, ComexAvg: actual},
		}

		count, err := resolveForecasts(context.Background(), store)
		if err != nil {
			t.Fatalf("resolveForecasts: %v", err)
		}

		if count != 0 {
			t.Fatalf("count = %d, want 0", count)
		}
		if len(store.writes) != 0 {
			t.Fatalf("writeResolutions вызван %d раз, want 0 (пустой батч — это лишняя запись в журнал)", len(store.writes))
		}
	})

	t.Run("окно без точек — пропуск, не ошибка", func(t *testing.T) {
		store := &fakeResolveStore{
			rows:  []forecastRow{{ForecastDate: date(2026, time.July, 31), TargetDate: target, Predicted: predicted}},
			stats: windowStats{},
		}

		count, err := resolveForecasts(context.Background(), store)
		if err != nil {
			t.Fatalf("resolveForecasts: %v (окно без точек — штатное состояние, а не сбой)", err)
		}
		if count != 0 || len(store.writes) != 0 {
			t.Fatalf("count = %d, батчей %d, want 0 и 0", count, len(store.writes))
		}
	})
}

// TestResolveRateWritesErrorBp — ветка cbr_rate*: факт — ставка после заседания,
// error_bp в базисных пунктах, error_pct пуст, метрика трека переносится в батч.
//
// Две строки разных треков на одну дату заседания проверяются вместе нарочно:
// именно суффикс metric отличает их в ключе (metric, forecast_date), и потеря
// суффикса схлопнула бы taylor и ordered_logit в одну строку ReplacingMergeTree.
func TestResolveRateWritesErrorBp(t *testing.T) {
	target := date(2026, time.September, 11)
	store := &fakeResolveStore{
		rateRows: []forecastRow{
			{
				ForecastDate: date(2026, time.September, 1), TargetDate: target, Metric: "cbr_rate:taylor",
				Predicted: 14.35, Probabilities: "{}", Model: "taylor", DataVersion: "v-taylor", PromptVersion: 0,
			},
			{
				ForecastDate: date(2026, time.September, 1), TargetDate: target, Metric: "cbr_rate:ordered_logit",
				Predicted: 13.00, Probabilities: `{"cut":80,"hike":5,"hold":15}`,
				Model: "ordered_logit", DataVersion: "v-logit", PromptVersion: 0,
			},
		},
		rates: map[string]float64{"2026-09-11": 14.00},
	}

	count, err := resolveForecasts(context.Background(), store)
	if err != nil {
		t.Fatalf("resolveForecasts: %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if len(store.writes) != 1 || len(store.writes[0]) != 2 {
		t.Fatalf("writes = %+v, want one batch with two rows", store.writes)
	}
	if len(store.rateTargets) != 2 {
		t.Fatalf("rateAfter вызван %d раз, want 2 (по разу на строку)", len(store.rateTargets))
	}
	for _, called := range store.rateTargets {
		if !called.Equal(target) {
			t.Fatalf("rateAfter(%s), want %s", called.Format(dateLayout), target.Format(dateLayout))
		}
	}

	want := []struct {
		metric        string
		bp            float64
		probabilities string
		model         string
		dataVersion   string
		promptVersion uint16
	}{
		{"cbr_rate:taylor", 35, "{}", "taylor", "v-taylor", 0},
		{"cbr_rate:ordered_logit", -100, `{"cut":80,"hike":5,"hold":15}`, "ordered_logit", "v-logit", 0},
	}
	for index, resolution := range store.writes[0] {
		if index >= len(want) {
			break
		}
		if resolution.Metric != want[index].metric {
			t.Fatalf("строка %d: metric %q, want %q", index, resolution.Metric, want[index].metric)
		}
		if resolution.Actual != 14.00 {
			t.Fatalf("строка %d: actual %v, want 14.00", index, resolution.Actual)
		}
		if resolution.ErrorPct != nil {
			t.Fatalf("строка %d: error_pct = %v, want nil (ставка измеряется в б.п.)", index, *resolution.ErrorPct)
		}
		if resolution.ErrorBp == nil || math.Abs(*resolution.ErrorBp-want[index].bp) > 1e-9 {
			t.Fatalf("строка %d: error_bp = %v, want %v", index, resolution.ErrorBp, want[index].bp)
		}
		// Резолвер переписывает строку целиком: метаданные публикации обязаны
		// переехать в новую версию, иначе ReplacingMergeTree потеряет распределение
		// и версию данных после первого же резолва.
		if resolution.Probabilities != want[index].probabilities || resolution.Model != want[index].model ||
			resolution.DataVersion != want[index].dataVersion || resolution.PromptVersion != want[index].promptVersion {
			t.Fatalf("строка %d: metadata = (%q, %q, %q, %d), want (%q, %q, %q, %d)", index,
				resolution.Probabilities, resolution.Model, resolution.DataVersion, resolution.PromptVersion,
				want[index].probabilities, want[index].model, want[index].dataVersion, want[index].promptVersion)
		}
	}
}

// TestResolveRateKeepsStageSuffix — staged-строка (infra/4.3) резолвится той же
// веткой `metric LIKE 'cbr_rate%'`, а суффикс стадии и метаданные переносятся в
// новую версию без потерь: иначе shadow/canary схлопнулись бы с production.
func TestResolveRateKeepsStageSuffix(t *testing.T) {
	target := date(2026, time.September, 11)
	store := &fakeResolveStore{
		rateRows: []forecastRow{
			{
				ForecastDate: date(2026, time.September, 1), TargetDate: target,
				Metric: "cbr_rate:ordered_logit:shadow", Predicted: 13.00,
				Probabilities: `{"cut":80,"hike":5,"hold":15}`,
				Model:         "ordered_logit", DataVersion: "v-shadow", PromptVersion: 0,
			},
		},
		rates: map[string]float64{"2026-09-11": 14.00},
	}

	count, err := resolveForecasts(context.Background(), store)
	if err != nil {
		t.Fatalf("resolveForecasts: %v", err)
	}
	if count != 1 || len(store.writes) != 1 || len(store.writes[0]) != 1 {
		t.Fatalf("count = %d, writes = %+v, want one staged row", count, store.writes)
	}
	row := store.writes[0][0]
	if row.Metric != "cbr_rate:ordered_logit:shadow" {
		t.Fatalf("metric = %q, want cbr_rate:ordered_logit:shadow", row.Metric)
	}
	if row.ErrorBp == nil || math.Abs(*row.ErrorBp+100) > 1e-9 {
		t.Fatalf("error_bp = %v, want -100", row.ErrorBp)
	}
	if row.Probabilities != `{"cut":80,"hike":5,"hold":15}` ||
		row.Model != "ordered_logit" || row.DataVersion != "v-shadow" {
		t.Fatalf("metadata = (%q, %q, %q), want staged metadata preserved",
			row.Probabilities, row.Model, row.DataVersion)
	}
}

// TestResolveRateMissingFact — точки после заседания ещё нет: строка остаётся
// нерезолвленной, прогон не падает. Так выглядит заседание, решение которого
// объявлено сегодня, а ставка вступает в силу завтра.
func TestResolveRateMissingFact(t *testing.T) {
	store := &fakeResolveStore{
		rateRows: []forecastRow{{
			ForecastDate: date(2026, time.October, 18), TargetDate: date(2026, time.October, 23),
			Metric: "cbr_rate:taylor", Predicted: 13.35,
		}},
		rates: map[string]float64{},
	}

	count, err := resolveForecasts(context.Background(), store)
	if err != nil {
		t.Fatalf("resolveForecasts: %v (нет факта — штатное состояние, а не сбой)", err)
	}
	if count != 0 || len(store.writes) != 0 {
		t.Fatalf("count = %d, батчей %d, want 0 и 0", count, len(store.writes))
	}
}

// TestResolveRateAlreadyResolved — тот же факт уже записан: батч не отправляется
// и error_bp не пересчитывается заново на каждом ежедневном прогоне.
func TestResolveRateAlreadyResolved(t *testing.T) {
	actual := 14.00
	store := &fakeResolveStore{
		rateRows: []forecastRow{{
			ForecastDate: date(2026, time.September, 1), TargetDate: date(2026, time.September, 11),
			Metric: "cbr_rate:taylor", Predicted: 14.35, Actual: &actual,
		}},
		rates: map[string]float64{"2026-09-11": 14.00},
	}

	count, err := resolveForecasts(context.Background(), store)
	if err != nil {
		t.Fatalf("resolveForecasts: %v", err)
	}
	if count != 0 || len(store.writes) != 0 {
		t.Fatalf("count = %d, батчей %d, want 0 и 0", count, len(store.writes))
	}
}

// TestResolveKeepsXauBehaviour — обе ветки в одном прогоне: xau_q_avg остаётся
// с квартальным окном и error_pct, cbr_rate* — со ставкой после заседания и
// error_bp. Регрессия золотого контура была бы невидима в отдельном тесте ветки
// ставки, поэтому строки проверяются в одном батче.
func TestResolveKeepsXauBehaviour(t *testing.T) {
	const (
		xauPredicted = 4450.0
		xauActual    = 4300.80
	)
	target := date(2026, time.September, 30)
	store := &fakeResolveStore{
		rows: []forecastRow{{
			ForecastDate: date(2026, time.July, 31), TargetDate: target, Predicted: xauPredicted,
		}},
		rateRows: []forecastRow{{
			ForecastDate: date(2026, time.September, 1), TargetDate: target,
			Metric: "cbr_rate:ordered_logit", Predicted: 13.00,
			Probabilities: `{"cut":80,"hike":5,"hold":15}`, Model: "ordered_logit",
			DataVersion: "v-logit", PromptVersion: 0,
		}},
		stats: windowStats{ComexCount: 64, ComexAvg: xauActual},
		rates: map[string]float64{"2026-09-30": 14.00},
	}

	count, err := resolveForecasts(context.Background(), store)
	if err != nil {
		t.Fatalf("resolveForecasts: %v", err)
	}
	if count != 2 || len(store.writes) != 1 || len(store.writes[0]) != 2 {
		t.Fatalf("count = %d, writes = %+v, want 2 строки одним батчем", count, store.writes)
	}

	byMetric := map[string]forecastResolution{}
	for _, resolution := range store.writes[0] {
		byMetric[resolution.Metric] = resolution
	}

	xau, ok := byMetric[forecastMetric]
	if !ok {
		t.Fatalf("в батче нет %s: %+v", forecastMetric, store.writes[0])
	}
	if xau.ErrorPct == nil || math.Abs(*xau.ErrorPct-errorPct(xauPredicted, xauActual)) > 1e-9 {
		t.Fatalf("xau error_pct = %v, want %v", xau.ErrorPct, errorPct(xauPredicted, xauActual))
	}
	if xau.ErrorBp != nil {
		t.Fatalf("xau error_bp = %v, want nil", *xau.ErrorBp)
	}

	rate, ok := byMetric["cbr_rate:ordered_logit"]
	if !ok {
		t.Fatalf("в батче нет cbr_rate:ordered_logit: %+v", store.writes[0])
	}
	if rate.ErrorPct != nil {
		t.Fatalf("rate error_pct = %v, want nil", *rate.ErrorPct)
	}
	if rate.ErrorBp == nil || math.Abs(*rate.ErrorBp-(-100)) > 1e-9 {
		t.Fatalf("rate error_bp = %v, want -100", rate.ErrorBp)
	}
	if rate.Probabilities != `{"cut":80,"hike":5,"hold":15}` || rate.Model != "ordered_logit" ||
		rate.DataVersion != "v-logit" || rate.PromptVersion != 0 {
		t.Fatalf("rate metadata = (%q, %q, %q, %d), want probabilities/model/data_version preserved", rate.Probabilities,
			rate.Model, rate.DataVersion, rate.PromptVersion)
	}
}

// TestResolveRateSkipsNonPositiveFact — ставка 0 или отрицательная — порча
// ряда, а не измерение: строка пропускается с warn, иначе в журнал попала бы
// ошибка, посчитанная от несуществующего уровня.
func TestResolveRateSkipsNonPositiveFact(t *testing.T) {
	store := &fakeResolveStore{
		rateRows: []forecastRow{{
			ForecastDate: date(2026, time.September, 1), TargetDate: date(2026, time.September, 11),
			Metric: "cbr_rate:taylor", Predicted: 14.35,
		}},
		rates: map[string]float64{"2026-09-11": 0},
	}

	count, err := resolveForecasts(context.Background(), store)
	if err != nil {
		t.Fatalf("resolveForecasts: %v", err)
	}
	if count != 0 || len(store.writes) != 0 {
		t.Fatalf("count = %d, батчей %d, want 0 и 0", count, len(store.writes))
	}
}

// TestImportPropagatesStoreError — сбой стора возвращается вызывающему на каждой
// из трёх операций.
//
// Ошибка, проглоченная ради «нулевого счётчика», выглядела бы как успешный прогон
// DAG-а без записей: main.go напечатал бы «Imported 0 rows of forecast_resolve», и
// упавший резолвер был бы неотличим от «резолвить нечего». Отдельно проверяется,
// что при сбое статистики окна батч НЕ отправляется: частично сверенный журнал
// (часть строк с фактом, часть без) выглядел бы как полный.
func TestImportPropagatesStoreError(t *testing.T) {
	failure := errors.New("нет соединения с ClickHouse")
	target := date(2026, time.September, 30)
	rows := []forecastRow{{ForecastDate: date(2026, time.July, 31), TargetDate: target, Predicted: 4450}}
	rateRows := []forecastRow{{
		ForecastDate: date(2026, time.September, 1), TargetDate: target,
		Metric: "cbr_rate:taylor", Predicted: 14.35,
	}}

	cases := []struct {
		name  string
		store *fakeResolveStore
	}{
		{"сбой выборки журнала", &fakeResolveStore{resolvableErr: failure}},
		{"сбой выборки cbr_rate*", &fakeResolveStore{rateReadErr: failure}},
		{"сбой ставки после заседания", &fakeResolveStore{rateRows: rateRows, rateAfterErr: failure}},
		{"сбой статистики окна", &fakeResolveStore{rows: rows, windowErr: failure}},
		{"сбой записи батча", &fakeResolveStore{rows: rows, stats: windowStats{ComexCount: 64, ComexAvg: 4300.80}, writeErr: failure}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			count, err := resolveForecasts(context.Background(), tc.store)
			if !errors.Is(err, failure) {
				t.Fatalf("resolveForecasts вернул err = %v, want %v", err, failure)
			}
			if count != 0 {
				t.Fatalf("count = %d, want 0 (сбой не считается записанной строкой)", count)
			}
			if len(tc.store.writes) != 0 {
				t.Fatalf("writeResolutions вызван %d раз, want 0", len(tc.store.writes))
			}
		})
	}
}

// TestName — имя импортёра в реестре chimport.Stats, то есть значение
// CLICKHOUSE_IMPORT_STAT и id шага в dagu/forecast.yaml.
//
// Имя — стабильный контракт расписания: переименование разорвало бы связку
// «шаг DAG ↔ импортёр» молча — dagu перестал бы находить импортёр, а оператор
// `make import STAT=forecast_resolve` получил бы «no importer named», при этом
// прогон DAG-а выглядел бы успешным.
func TestName(t *testing.T) {
	if got := (&resolver{}).Name(); got != "forecast_resolve" {
		t.Fatalf("Name() = %q, want forecast_resolve", got)
	}
}

// TestResolvableQueryFilters — фильтр выборки (Review Focus 2).
//
// Проверяется ТЕКСТ SQL, а не результат: живой ClickHouse здесь не нужен, а
// проверяется ровно то, что нельзя увидеть в логе. Без `metric = 'xau_q_avg'`
// резолвер пошёл бы резолвить `nav` — метрику, у которой факта не существует
// (verify.md плагина gold-nav: «nav — факт не определён»), и записал бы в журнал
// ошибку прогноза NAV, посчитанную по цене золота. Без `target_date <= today()`
// сверялись бы прогнозы на будущее: неполный квартал, усреднённый по нескольким
// дням, дал бы правдоподобный actual и «ошибку» прогноза задним числом.
func TestResolvableQueryFilters(t *testing.T) {
	for _, want := range []string{"metric = 'xau_q_avg'", "target_date <= today()"} {
		if !strings.Contains(resolvableSelect, want) {
			t.Errorf("resolvableSelect не содержит %q: %q", want, resolvableSelect)
		}
	}
}

// TestRateResolvableQueryFilters — выборка контура ставки (Review Focus 2).
//
// `metric LIKE 'cbr_rate%'` — единственный фильтр, который приводит в резолвер
// треки cbr_rate:taylor/ordered_logit/ml/llm и не трогает xau_q_avg/nav; без
// `target_date <= today()` резолвились бы прогнозы на будущее — ставка «после
// заседания» тогда была бы взята из прошлого режима, и в журнале появилась бы
// правдоподобная ошибка задним числом.
func TestRateResolvableQueryFilters(t *testing.T) {
	for _, want := range []string{"metric LIKE 'cbr_rate%'", "target_date <= today()"} {
		if !strings.Contains(rateResolvableSelect, want) {
			t.Errorf("rateResolvableSelect не содержит %q: %q", want, rateResolvableSelect)
		}
	}
}

// TestRateAfterQueryUsesStrictlyLaterDate — факт ставки: первая точка ПОСЛЕ
// target_date, а не «на дату». Заседание объявляет ставку со следующего дня, и
// точка самого дня заседания ещё несёт прежнюю ставку; `>=` дал бы нулевую
// ошибку у любого прогноза и сделал бы резолвер бессмысленным. FINAL обязателен
// для ReplacingMergeTree, ORDER BY + LIMIT 1 фиксирует «первую».
func TestRateAfterQueryUsesStrictlyLaterDate(t *testing.T) {
	for _, want := range []string{
		"FROM cbr_key_rate FINAL",
		"date > {target:Date}",
		"ORDER BY date",
		"LIMIT 1",
	} {
		if !strings.Contains(rateAfterSelect, want) {
			t.Errorf("rateAfterSelect не содержит %q: %q", want, rateAfterSelect)
		}
	}
}

// TestWindowStatsGatesEmptyAverage — пустое среднее не должно покидать запрос
// ни NaN, ни нулём-заглушкой.
//
// Гейт по количеству точек обязателен: `avgIf` на пустом множестве возвращает NaN
// (проверено на живом ClickHouse), а NaN ломает резолвер дважды — (1) `actual <= 0`
// ложно для NaN, поэтому строка не пропускается, (2) `NaN != NaN`, поэтому
// needsWrite всегда истинно: каждый прогон DAG-а писал бы строку заново, а в
// v_forecast_accuracy появился бы resolved-факт NaN при отсутствии данных.
// `coalesce` поверх avgIf дал бы 0 — уже не NaN, но тогда пустое окно
// записывалось бы как нулевой факт и тонуло в warn'е про `actual <= 0` вместо
// явного «точек в окне нет».
func TestWindowStatsGatesEmptyAverage(t *testing.T) {
	for _, want := range []string{
		"if(comex_n > 0, avgIf(usd, venue = 'comex_front'), 0)",
		"if(moex_n > 0, avgIf(usd, venue = 'moex_fix_usd'), 0)",
	} {
		if !strings.Contains(windowStatsSelect, want) {
			t.Errorf("windowStatsSelect не содержит %q: %q", want, windowStatsSelect)
		}
	}

	if strings.Contains(windowStatsSelect, "coalesce") {
		t.Errorf("windowStatsSelect подменяет пустое среднее через coalesce: %q", windowStatsSelect)
	}
}
