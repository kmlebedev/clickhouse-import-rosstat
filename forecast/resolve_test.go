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
	stats         windowStats
	resolvableErr error
	windowErr     error
	writeErr      error

	windows [][2]time.Time
	writes  [][]forecastResolution
}

func (f *fakeResolveStore) resolvable(context.Context) ([]forecastRow, error) {
	if f.resolvableErr != nil {
		return nil, f.resolvableErr
	}

	return f.rows, nil
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
		want := forecastResolution{
			ForecastDate: date(2026, time.July, 31),
			TargetDate:   target,
			Predicted:    predicted,
			Actual:       actual,
			ErrorPct:     errorPct(predicted, actual),
		}
		if !got.ForecastDate.Equal(want.ForecastDate) || !got.TargetDate.Equal(want.TargetDate) ||
			got.Predicted != want.Predicted || got.Actual != want.Actual || got.ErrorPct != want.ErrorPct {
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

	cases := []struct {
		name  string
		store *fakeResolveStore
	}{
		{"сбой выборки журнала", &fakeResolveStore{resolvableErr: failure}},
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
