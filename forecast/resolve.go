// Package forecast резолвит журнал прогнозов: заполняет forecast_log.actual и
// ошибку сверкой прогноза с фактом. Две ветки:
//
//   - xau_q_avg — среднее gold_prices за квартал target_date, error_pct;
//   - cbr_rate* — ставка после заседания из cbr_key_rate, error_bp (базисные
//     пункты), error_pct = NULL: относительная ошибка ставки зависит от уровня
//     (100 б.п. — это ≈7 % при 14 % и ≈25 % при 4 %), поэтому шкала несопоставима
//     между режимами.
//
// ПОЧЕМУ `nav` НЕ РЕЗОЛВИТСЯ. Факта для NAV на акцию не существует. Расчётный
// NAV sum-of-parts и рыночная цена PLZL — разные величины (цена — мнение рынка о
// будущем, NAV — сумма дисконтированных потоков по плану производства), поэтому
// подстановка market_price в actual записала бы в журнал «ошибку прогноза» там,
// где прогноз вообще не этой величиной проверяется. Ровно это зафиксировано в
// verify.md плагина gold-nav: «nav — факт не определён, оставь actual пустым».
// Поэтому выборки строк идут по metric = 'xau_q_avg' и metric LIKE 'cbr_rate%',
// а прочие метрики (nav и будущие) остаются нетронутыми — не ошибкой и не warn'ом,
// а состоянием журнала.
//
// ПОЧЕМУ ОКНО СВЕРКИ xau_q_avg — КВАРТАЛ, СОДЕРЖАЩИЙ target_date. Прогноз xau_q_avg —
// это СРЕДНЯЯ цена золота за квартал горизонта, а не цена на дату (ingest/clickhouse.go:
// target_date — конец горизонта сценария, «Q4-2026» → 31.12.2026), поэтому и факт
// берётся средним за тот же квартал: verify.md плагина gold-nav — «факт xau_q_avg
// — среднее GC=F за квартал target_date». Фактически читается среднее
// venue='comex_front' (фронт-месяц GC=F — тот самый GC=F из verify.md);
// moex_fix_usd — фолбэк, когда по COMEX в окне точек нет (ряд начинается
// 2024-10-11, а MOEX-проксия идёт с 2024-08-05). Верхняя граница окна — сам
// target_date, а не конец квартала: резолвер не знает, откуда пришла строка, и у
// ручного ввода target_date бывает серединой квартала (2026-07-15), где среднее
// «по кварталу целиком» означало бы факт из будущего. У штатных target_date =
// последний день квартала окно и так равно кварталу целиком.
//
// ПОЧЕМУ ФАКТ cbr_rate — ПЕРВАЯ ТОЧКА ПОСЛЕ target_date. Заседание объявляет
// ставку, действующую со следующего дня; ряд cbr_key_rate дневной с пропусками
// выходных, поэтому первая точка строго после даты заседания — и есть ставка
// сразу после решения (пятничное заседание → понедельник). Окно d+7 из
// key_rate_model.py здесь не нужно: оно служит ярлыку выборки и могло бы
// захватить следующее заседание.
package forecast

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	log "github.com/sirupsen/logrus"
)

const (
	// forecastResolveStat — имя импортёра в реестре chimport.Stats, то есть
	// значение CLICKHOUSE_IMPORT_STAT и id шага в dagu/forecast.yaml. Отдельная
	// константа: имя в реестре — стабильный контракт расписания, и склеивать его с
	// именем таблицы нельзя (прецедент — polyus/financialMetricsStatName).
	forecastResolveStat = "forecast_resolve"

	// forecastMetric — метрика золотого контура: единственная, у которой факт —
	// среднее gold_prices (см. комментарий пакета про nav).
	forecastMetric = "xau_q_avg"

	// rateMetricPrefix — префикс метрик контура ключевой ставки. Треки
	// различаются суффиксом (`cbr_rate:taylor`, `cbr_rate:ordered_logit`, ...),
	// и резолвер переносит metric строки в батч как есть, чтобы треки не
	// схлопнулись ключом (metric, forecast_date).
	rateMetricPrefix = "cbr_rate"

	// comexVenue, moexVenue — площадки gold_prices, из которых берётся факт.
	// Порядок приоритета задан pickActual: comex_front — целевой ряд verify.md
	// (GC=F), moex_fix_usd — производная проксия (MOEX-фикс ÷ курс ЦБ).
	comexVenue = "comex_front"
	moexVenue  = "moex_fix_usd"

	// dateLayout — формат дат в параметрах запроса и в логе.
	dateLayout = "2006-01-02"

	// forecastLogInsert — вставка резолвленных строк. Порядок колонок — как в
	// forecastLogDdl (ingest/schema.go): forecast_date, target_date, metric,
	// predicted, actual, error_pct, error_bp, probabilities, model, data_version,
	// prompt_version. Сверять глазами обязательно: ClickHouse примет перестановку
	// однотипных Date/Float64 и молча запишет actual в error_pct.
	forecastLogInsert = "INSERT INTO forecast_log (forecast_date, target_date, metric, predicted, actual, error_pct, error_bp, probabilities, model, data_version, prompt_version)"
)

// resolvableSelect — строки журнала, которым факт уже можно посчитать: только
// xau_q_avg и только с наступившей target_date. ORDER BY forecast_date — порядок
// батча вставки (воспроизводимость при разборе прогона).
//
// actual читается НЕ как Nullable: драйвер строго типизирован, и Scan в
// sql.NullFloat64 (Nullable-скан) — отдельная ветка драйвера, которой здесь
// сознательно избегаем. Надёжнее прочитать признак NULL отдельной колонкой
// (if(actual IS NULL, 1, 0)) и само значение через coalesce(actual, 0). NULL
// превращается в *float64 уже в Go (connStore.resolvable):
// у coalesce-нуля и у настоящего нуля фактической цены один и тот же вид, и
// различать их обязан признак, а не значение.
const resolvableSelect = `SELECT forecast_date, target_date, predicted, if(actual IS NULL, 1, 0) AS actual_is_null, coalesce(actual, 0) AS actual, probabilities, model, data_version, prompt_version FROM forecast_log FINAL WHERE metric = '` + forecastMetric + `' AND target_date <= today() ORDER BY forecast_date`

// rateResolvableSelect — строки контура ключевой ставки. metric читается в
// выборку: трек (`cbr_rate:taylor` и т.п.) обязан вернуться в батч тем же
// значением, иначе строки разных треков одной даты схлопнулись бы ключом
// (metric, forecast_date).
//
// actual_is_null/coalesce — тот же приём, что у resolvableSelect: драйвер
// строго типизирован, а признак NULL надёжнее отдельной ветки сканирования.
const rateResolvableSelect = `SELECT forecast_date, target_date, metric, predicted, if(actual IS NULL, 1, 0) AS actual_is_null, coalesce(actual, 0) AS actual, probabilities, model, data_version, prompt_version FROM forecast_log FINAL WHERE metric LIKE '` + rateMetricPrefix + `%' AND target_date <= today() ORDER BY metric, forecast_date`

// rateAfterSelect — ставка сразу после заседания: первая точка cbr_key_rate
// строго позже target_date. FINAL обязателен (ReplacingMergeTree по (date)): без
// него перезаписанная точка дня могла бы прийти дважды, и «первая» зависела бы
// от порядка строк. Параметр — строка в формате Date (клиент принимает в
// NamedValue для {name:Date} только string).
const rateAfterSelect = `SELECT rate FROM cbr_key_rate FINAL WHERE date > {target:Date} ORDER BY date LIMIT 1`

// windowStatsSelect — статистика окна сверки по площадкам gold_prices.
//
// FINAL обязателен: gold_prices — ReplacingMergeTree по (venue, date), и без FINAL
// одна и та же дата могла бы прийти несколько раз (перезапись точки через
// POST /v1/gold_prices — норма), исказив среднее в пользу перезаписанных версий.
//
// Ветки avgIf гейтятся по КОЛИЧЕСТВУ точек (if(countIf > 0, avgIf(...), 0)), а не
// через coalesce: avgIf на пустом множестве возвращает NaN, а не NULL. NaN не
// ловится ни `actual <= 0` (сравнение с NaN ложно), ни `needsWrite` (`NaN != NaN`
// всегда истинно), поэтому пустое окно писалось бы как resolved-факт NaN — и заново
// на каждом прогоне. Ноль вместо NaN — это отсутствие точек, названное явно:
// вызывающий видит нулевой счётчик и пропускает строку с warn'ом, не разбирая
// значение среднего.
//
// Параметры — в форме ClickHouse-параметров {name:Type}: объявленный тип Date
// заставляет сервер разобрать пришедшую строку по типу. Значение обязано быть
// СТРОКОЙ — клиент принимает в NamedValue для {name:Type} только string
// (bindQueryOrAppendParameters → ErrExpectedStringValueInNamedValueForQueryParameter),
// поэтому time.Time здесь не подходит и конвертируется в connStore.windowStats.
const windowStatsSelect = `SELECT countIf(venue = '` + comexVenue + `') AS comex_n, if(comex_n > 0, avgIf(usd, venue = '` + comexVenue + `'), 0) AS comex_avg, countIf(venue = '` + moexVenue + `') AS moex_n, if(moex_n > 0, avgIf(usd, venue = '` + moexVenue + `'), 0) AS moex_avg FROM gold_prices FINAL WHERE date >= {start:Date} AND date <= {end:Date}`

// forecastRow — строка forecast_log, которую предстоит сверить с фактом.
// Metric заполнен у строк cbr_rate* (у xau_q_avg он известен константой).
// Actual == nil означает, что факт ещё не записан (в SQL это actual IS NULL).
type forecastRow struct {
	ForecastDate  time.Time
	TargetDate    time.Time
	Metric        string
	Predicted     float64
	Actual        *float64
	Probabilities string
	Model         string
	DataVersion   string
	PromptVersion uint16
}

// windowStats — статистика окна сверки по площадкам: количество точек и их
// средняя цена. Нулевой счётчик означает «точек нет» — и только он: среднее при
// нулевом счётчике нулевое по построению запроса, а не измеренное.
type windowStats struct {
	ComexCount uint64
	ComexAvg   float64
	MoexCount  uint64
	MoexAvg    float64
}

// forecastResolution — строка forecast_log с посчитанным фактом: то, что уходит в
// батч. Actual не указатель: резолвленная строка всегда имеет факт, а «пусто» в
// этой структуре выражается отсутствием элемента в срезе. ErrorPct и ErrorBp —
// взаимоисключающие: у xau_q_avg заполнена относительная ошибка, у cbr_rate* —
// ошибка в базисных пунктах; вторая остаётся nil и пишется в NULL.
//
// Метаданные прогноза (probabilities/model/data_version/prompt_version) входят в
// ключ строки журнала и в контракт публикации: резолвер обязан перенести их в
// батч как есть, иначе ReplacingMergeTree заменит строку версией без распределения
// и версии данных.
type forecastResolution struct {
	ForecastDate  time.Time
	TargetDate    time.Time
	Metric        string
	Predicted     float64
	Actual        float64
	ErrorPct      *float64
	ErrorBp       *float64
	Probabilities string
	Model         string
	DataVersion   string
	PromptVersion uint16
}

// resolveStore — узкий срез стора, который нужен оркестрации Import; в тестах
// подменяется фейком (паттерн chStore из ingest/clickhouse.go).
type resolveStore interface {
	resolvable(ctx context.Context) ([]forecastRow, error)
	rateResolvable(ctx context.Context) ([]forecastRow, error)
	windowStats(ctx context.Context, start, end time.Time) (windowStats, error)
	rateAfter(ctx context.Context, target time.Time) (float64, bool, error)
	writeResolutions(ctx context.Context, rows []forecastResolution) error
}

// quarterWindow — окно сверки для target_date: [первый день квартала, target_date].
//
// Нижняя граница строится по номеру квартала ((месяц−1)/3), а не таблицей месяцев:
// таблица рано или поздно разошлась бы с календарём. Верхняя граница — сам
// target_date (см. комментарий пакета), а не конец квартала.
func quarterWindow(target time.Time) (time.Time, time.Time) {
	firstMonth := time.Month((int(target.Month())-1)/3*3 + 1)
	start := time.Date(target.Year(), firstMonth, 1, 0, 0, 0, 0, time.UTC)

	return start, utcDate(target)
}

// pickActual — площадка и средняя цена для факта: comex_front, если по нему есть
// точки, иначе moex_fix_usd, если есть по нему. Решение принимается по КОЛИЧЕСТВУ
// точек, а не по значению среднего: нулевое среднее при непустом окне невозможно
// (цены золота положительны), а вот нулевой счётчик — обычное состояние (ряд COMEX
// начинается 2024-10-11, недельные пропуски бывают).
func pickActual(stats windowStats) (venue string, actual float64, ok bool) {
	switch {
	case stats.ComexCount > 0:
		return comexVenue, stats.ComexAvg, true
	case stats.MoexCount > 0:
		return moexVenue, stats.MoexAvg, true
	default:
		return "", 0, false
	}
}

// errorPct — ошибка прогноза в процентах: (predicted − actual) / actual × 100.
// Знак сохраняется (прогноз ниже факта даёт отрицательную ошибку) — по нему
// витрина читает переоценку/недооценку. Деление на actual безопасно: вызывающий
// отсекает actual <= 0 отдельной веткой, поэтому NaN и ±Inf здесь появиться не
// могут.
func errorPct(predicted, actual float64) float64 {
	return (predicted - actual) / actual * 100
}

// errorBp — ошибка прогноза ставки в базисных пунктах: (predicted − actual) × 100.
// predicted и actual — уровни ставки в % годовых, поэтому 14.35 против 14.00 даёт
// +35 б.п. Знак сохраняется (прогноз ниже факта — отрицательная ошибка). Деления
// нет: нулевой/отрицательный факт здесь не ломает формулу, но вызывающий всё равно
// отсекает actual <= 0 как порчу ряда.
func errorBp(predicted, actual float64) float64 {
	return (predicted - actual) * 100
}

// needsWrite — нужна ли перезапись строки: факта нет или он отличается от
// посчитанного. Совпадение заканчивается молчанием вызывающего (log.Debugf):
// прогон DAG-а идёт ежедневно, и перезапись того же значения — это 365 лишних
// батчей в год и неотличимая от изменения строка в журнале.
func needsWrite(row forecastRow, actual float64) bool {
	return row.Actual == nil || *row.Actual != actual
}

// resolver — импортёр forecast_resolve (Name() — значение CLICKHOUSE_IMPORT_STAT
// и id шага dagu/forecast.yaml).
//
// Почему резолвер отдельным импортёром, а не частью gold_prices: сверка не зависит
// от источника цены — она читает то, что уже лежит в gold_prices, и одинаково
// работает и по COMEX-сессии, и по MOEX-проксии. Привязка к одному из DAG-ов
// расписания (gold.yaml, 18:47) сломала бы сверку при переносе записи цены в
// другой шаг; собственное расписание (`23 20 * * *` = ежедневно 20:23 МСК,
// dagu/forecast.yaml) идёт после обоих источников дня.
type resolver struct{}

func (r *resolver) Name() string { return forecastResolveStat }

// Import читает нерезолвленные прогнозы xau_q_avg и cbr_rate*, считает факт по
// золоту за квартал target_date и по ставке после заседания соответственно и
// записывает ОДНИМ батчем только изменившиеся строки. Возвращает число записанных
// строк: 0 — норма (менять нечего), а не сбой; main.go печатает его сам.
func (r *resolver) Import(ctx context.Context, conn driver.Conn) (int64, error) {
	return resolveForecasts(ctx, connStore{conn: conn})
}

// resolveForecasts — оркестрация: выборки → факт → решение → батч. Вынесена из
// Import, чтобы проверяться офлайн на фейковом сторе.
//
// Две ветки не пересекаются по метрикам: cbr_rate* резолвится ставкой из
// cbr_key_rate (error_bp, error_pct = NULL), xau_q_avg — средним gold_prices за
// квартал (error_pct, error_bp = NULL). Оба набора строк уходят одним батчем.
//
// Пустая выборка и «все строки уже сходятся» — не ошибки: и то и другое
// возвращает 0 строк (журнал прогнозов заполняется ingest'ом, и на свежей БД
// строк с наступившей target_date может не быть вовсе).
func resolveForecasts(ctx context.Context, store resolveStore) (int64, error) {
	rateRows, err := store.rateResolvable(ctx)
	if err != nil {
		return 0, err
	}

	rows, err := store.resolvable(ctx)
	if err != nil {
		return 0, err
	}

	var resolutions []forecastResolution

	for _, row := range rateRows {
		actual, ok, err := store.rateAfter(ctx, row.TargetDate)
		if err != nil {
			return 0, err
		}
		if !ok {
			// Точки после заседания ещё нет — штатное состояние: решение объявлено,
			// ставка вступает в силу со следующего дня, и ряд cbr_key_rate пополнится
			// завтра. Строку не пишем, actual остаётся NULL, прогон не падает.
			log.Warnf("forecast_resolve: прогноз %s на %s (%s) — в cbr_key_rate нет точки после %s, "+
				"факт не записан, строка остаётся нерезолвленной",
				row.ForecastDate.Format(dateLayout), row.TargetDate.Format(dateLayout), row.Metric,
				row.TargetDate.Format(dateLayout))

			continue
		}

		if actual <= 0 {
			// Ставка <= 0 — не измерение, а признак испорченного ряда: error_bp
			// посчитался бы, но сверять не с чем. Пропуск с warn, как у золота.
			log.Warnf("forecast_resolve: прогноз %s на %s (%s) — ставка после заседания %v, "+
				"факт не записан (ставка должна быть положительной)",
				row.ForecastDate.Format(dateLayout), row.TargetDate.Format(dateLayout), row.Metric, actual)

			continue
		}

		if !needsWrite(row, actual) {
			log.Debugf("forecast_resolve: прогноз %s на %s (%s) уже сверен (%v), записи нет",
				row.ForecastDate.Format(dateLayout), row.TargetDate.Format(dateLayout), row.Metric, actual)

			continue
		}

		// error_bp — единственная причина перезаписи строки cbr_rate*: actual сам по
		// себе мог бы быть витриной, но публикуется именно ошибка в б.п. (KEY_RATE_
		// PUBLICATIONS §8), и держать формулу в SQL значило бы иметь две реализации
		// одного правила. error_pct здесь NULL — относительная ошибка ставки
		// несопоставима между режимами.
		bp := errorBp(row.Predicted, actual)
		resolution := forecastResolution{
			ForecastDate:  row.ForecastDate,
			TargetDate:    row.TargetDate,
			Metric:        row.Metric,
			Predicted:     row.Predicted,
			Actual:        actual,
			ErrorBp:       &bp,
			Probabilities: row.Probabilities,
			Model:         row.Model,
			DataVersion:   row.DataVersion,
			PromptVersion: row.PromptVersion,
		}

		log.Infof("forecast_resolve: прогноз %s на %s (%s) — ставка после заседания %.4f, predicted %.4f, error_bp %.4f",
			row.ForecastDate.Format(dateLayout), row.TargetDate.Format(dateLayout), row.Metric,
			actual, row.Predicted, bp)

		resolutions = append(resolutions, resolution)
	}

	for _, row := range rows {
		start, end := quarterWindow(row.TargetDate)

		stats, err := store.windowStats(ctx, start, end)
		if err != nil {
			return 0, err
		}

		venue, actual, ok := pickActual(stats)
		if !ok {
			// Окно без точек — состояние источника, а не сбой: строку пропускаем,
			// оставляя actual пустым, и говорим об этом в логе. Ошибкой это было бы
			// равносильно «резолвер красный, пока COMEX-ряд не начался».
			log.Warnf("forecast_resolve: прогноз %s на %s — в gold_prices нет точек за %s..%s, "+
				"факт не записан, строка остаётся нерезолвленной",
				row.ForecastDate.Format(dateLayout), row.TargetDate.Format(dateLayout),
				start.Format(dateLayout), end.Format(dateLayout))

			continue
		}

		if actual <= 0 {
			// Среднее <= 0 у цены золота — не измерение, а признак испорченного ряда
			// (точки-нули, отрицательная цена). error_pct в этом случае или не имеет
			// смысла (0), или переворачивает знак, поэтому строку пропускаем так же,
			// как пустое окно: warn виден в логе прогона, actual остаётся NULL.
			log.Warnf("forecast_resolve: прогноз %s на %s — среднее %s за %s..%s равно %v, "+
				"факт не записан (error_pct при нулевом факте не определена)",
				row.ForecastDate.Format(dateLayout), row.TargetDate.Format(dateLayout), venue,
				start.Format(dateLayout), end.Format(dateLayout), actual)

			continue
		}

		if !needsWrite(row, actual) {
			// Строка без изменений — не «пропуск», а норма: debug, чтобы ежедневный
			// прогон не печатал по строке на каждый уже резолвленный прогноз.
			log.Debugf("forecast_resolve: прогноз %s на %s уже сверен (%s %v), записи нет",
				row.ForecastDate.Format(dateLayout), row.TargetDate.Format(dateLayout), venue, actual)

			continue
		}

		points := stats.ComexCount
		if venue != comexVenue {
			points = stats.MoexCount
		}

		// error_pct считается здесь и попадает в батч: это единственная причина, по
		// которой строка вообще перезаписывается (actual сам по себе мог бы быть
		// витриной), и держать формулу в SQL значило бы иметь две реализации одного
		// правила — в резолвере и в v_forecast_accuracy (error_pct = (predicted −
		// actual) / actual × 100).
		pct := errorPct(row.Predicted, actual)
		resolution := forecastResolution{
			ForecastDate:  row.ForecastDate,
			TargetDate:    row.TargetDate,
			Metric:        forecastMetric,
			Predicted:     row.Predicted,
			Actual:        actual,
			ErrorPct:      &pct,
			Probabilities: row.Probabilities,
			Model:         row.Model,
			DataVersion:   row.DataVersion,
			PromptVersion: row.PromptVersion,
		}

		log.Infof("forecast_resolve: прогноз %s на %s — %s, точек %d, actual %.4f, error_pct %.4f",
			row.ForecastDate.Format(dateLayout), row.TargetDate.Format(dateLayout), venue,
			points, actual, pct)

		resolutions = append(resolutions, resolution)
	}

	// Пустой батч не отправляется: PrepareBatch/Send без строк — это запись в
	// журнал сервера и лишняя транзакция на каждом ежедневном прогоне, при том что
	// менять нечего.
	if len(resolutions) == 0 {
		return 0, nil
	}

	if err = store.writeResolutions(ctx, resolutions); err != nil {
		return 0, err
	}

	return int64(len(resolutions)), nil
}

// connStore — реализация resolveStore поверх driver.Conn.
type connStore struct {
	conn driver.Conn
}

func (s connStore) resolvable(ctx context.Context) ([]forecastRow, error) {
	rows, err := s.conn.Query(ctx, resolvableSelect)
	if err != nil {
		return nil, fmt.Errorf("резолв прогнозов: читать forecast_log: %w", err)
	}

	defer func() { _ = rows.Close() }()

	var forecasts []forecastRow

	for rows.Next() {
		var (
			forecastDate  time.Time
			targetDate    time.Time
			predicted     float64
			actualIsNull  uint8
			actual        float64
			probabilities string
			model         string
			dataVersion   string
			promptVersion uint16
		)

		// Порядок Scan — порядок колонок resolvableSelect. actual_is_null читается
		// в uint8 (if(...) имеет тип UInt8), actual — во float64 (coalesce(Float64)),
		// prompt_version — в uint16 (UInt16).
		if err = rows.Scan(&forecastDate, &targetDate, &predicted, &actualIsNull, &actual,
			&probabilities, &model, &dataVersion, &promptVersion); err != nil {
			return nil, fmt.Errorf("резолв прогнозов: читать строку forecast_log: %w", err)
		}

		row := forecastRow{
			ForecastDate:  utcDate(forecastDate),
			TargetDate:    utcDate(targetDate),
			Metric:        forecastMetric,
			Predicted:     predicted,
			Probabilities: probabilities,
			Model:         model,
			DataVersion:   dataVersion,
			PromptVersion: promptVersion,
		}
		if actualIsNull == 0 {
			row.Actual = &actual
		}

		forecasts = append(forecasts, row)
	}

	// rows.Err() обязателен: Next() возвращает false и при ошибке чтения, поэтому
	// без этой проверки недочитанный журнал выглядел бы как полный — часть прогнозов
	// просто осталась бы нерезолвленной до следующего прогона, без единого признака
	// сбоя в логе.
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("резолв прогнозов: читать forecast_log: %w", err)
	}

	return forecasts, nil
}

// rateResolvable — строки cbr_rate*: тот же разбор, что у resolvable, плюс
// metric (трек возвращается в батч как есть). Ошибки обёрнуты отдельно, чтобы
// в логе прогона было видно, какая из двух выборок упала.
func (s connStore) rateResolvable(ctx context.Context) ([]forecastRow, error) {
	rows, err := s.conn.Query(ctx, rateResolvableSelect)
	if err != nil {
		return nil, fmt.Errorf("резолв прогнозов: читать forecast_log cbr_rate*: %w", err)
	}

	defer func() { _ = rows.Close() }()

	var forecasts []forecastRow

	for rows.Next() {
		var (
			forecastDate  time.Time
			targetDate    time.Time
			metric        string
			predicted     float64
			actualIsNull  uint8
			actual        float64
			probabilities string
			model         string
			dataVersion   string
			promptVersion uint16
		)

		// Порядок Scan — порядок колонок rateResolvableSelect.
		if err = rows.Scan(&forecastDate, &targetDate, &metric, &predicted, &actualIsNull, &actual,
			&probabilities, &model, &dataVersion, &promptVersion); err != nil {
			return nil, fmt.Errorf("резолв прогнозов: читать строку forecast_log cbr_rate*: %w", err)
		}

		row := forecastRow{
			ForecastDate:  utcDate(forecastDate),
			TargetDate:    utcDate(targetDate),
			Metric:        metric,
			Predicted:     predicted,
			Probabilities: probabilities,
			Model:         model,
			DataVersion:   dataVersion,
			PromptVersion: promptVersion,
		}
		if actualIsNull == 0 {
			row.Actual = &actual
		}

		forecasts = append(forecasts, row)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("резолв прогнозов: читать forecast_log cbr_rate*: %w", err)
	}

	return forecasts, nil
}

// rateAfter — ставка сразу после заседания: первая точка cbr_key_rate строго
// позже target_date. «Нет строки» — не ошибка, а ненаблюдённый исход (заседание
// сегодня, ставка вступает в силу завтра): возвращается ok = false, вызывающий
// пропускает строку с warn. Прочие ошибки — ошибка прогона.
func (s connStore) rateAfter(ctx context.Context, target time.Time) (float64, bool, error) {
	// cbr_key_rate.rate — legacy Float32: драйвер строго типизирован и отказывает
	// «converting Float32 to *float64 is unsupported» (поймано живым прогоном
	// приёмки). Сканируем в float32 и расширяем; точность ставки (шаг 0.25 п.п.)
	// Float32 хранит без потерь.
	var rate float32

	err := s.conn.QueryRow(ctx, rateAfterSelect,
		clickhouse.Named("target", target.Format(dateLayout)),
	).Scan(&rate)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("резолв прогнозов: ставка после %s из cbr_key_rate: %w",
			target.Format(dateLayout), err)
	}

	return float64(rate), true, nil
}

func (s connStore) windowStats(ctx context.Context, start, end time.Time) (windowStats, error) {
	var stats windowStats

	// Даты уходят строками dateLayout: параметры {start:Date}/{end:Date} клиент
	// принимает только как string, а сервер разбирает их по объявленному типу Date
	// (см. windowStatsSelect).
	err := s.conn.QueryRow(ctx, windowStatsSelect,
		clickhouse.Named("start", start.Format(dateLayout)),
		clickhouse.Named("end", end.Format(dateLayout)),
	).Scan(&stats.ComexCount, &stats.ComexAvg, &stats.MoexCount, &stats.MoexAvg)
	if err != nil {
		return windowStats{}, fmt.Errorf("резолв прогнозов: статистика gold_prices за %s..%s: %w",
			start.Format(dateLayout), end.Format(dateLayout), err)
	}

	return stats, nil
}

// writeResolutions пишет резолвленные строки одним батчем
// (PrepareBatch → Append (err checked) → Send). Abort вызывается только в ветке
// ошибки Append, где батч ещё открыт; на ошибке Send — не вызывается, как и в
// остальных батч-писателях репозитория (Send завершает и закрывает батч сам).
func (s connStore) writeResolutions(ctx context.Context, rows []forecastResolution) error {
	batch, err := s.conn.PrepareBatch(ctx, forecastLogInsert)
	if err != nil {
		return fmt.Errorf("резолв прогнозов: %w", err)
	}

	for _, row := range rows {
		// Порядок позиций — порядок колонок forecastLogInsert. В DDL actual,
		// error_pct и error_bp Nullable; nil в интерфейсе даёт NULL (у xau_q_avg
		// error_bp пуст, у cbr_rate* — error_pct). Передаём float64 или nil, а не
		// указатель: драйверу так не нужно выбирать ветку Nullable-скана.
		var errorPct, errorBp any
		if row.ErrorPct != nil {
			errorPct = *row.ErrorPct
		}
		if row.ErrorBp != nil {
			errorBp = *row.ErrorBp
		}
		// Порядок позиций — порядок колонок forecastLogInsert. Метаданные прогноза
		// переносятся из прочитанной строки как есть: ReplacingMergeTree заменит
		// строку этой версией, и распределение/модель/версия данных не должны
		// исчезнуть после резолва.
		if err = batch.Append(row.ForecastDate, row.TargetDate, row.Metric, row.Predicted, row.Actual,
			errorPct, errorBp, row.Probabilities, row.Model, row.DataVersion, row.PromptVersion); err != nil {
			abortBatch(batch, "резолв прогнозов")

			return fmt.Errorf("резолв прогнозов %s на %s: %w",
				row.ForecastDate.Format(dateLayout), row.TargetDate.Format(dateLayout), err)
		}
	}

	if err = batch.Send(); err != nil {
		return fmt.Errorf("резолв прогнозов: %w", err)
	}

	return nil
}

// abortBatch закрывает батч на ошибке Append и не даёт потерять сбой закрытия.
//
// Abort — не украшение: без него соединение остаётся с открытым батчем, и
// следующий импортёр получит «уже есть незакрытый батч». Но и проглотить ошибку
// Abort нельзя (`_ =`): тогда причина, по которой батч всё же остался открытым,
// не попадёт в лог. Возвращаемой ошибкой остаётся ПЕРВОПРИЧИНА (Append) — она
// точнее сбоя Abort, случившегося уже после неё.
func abortBatch(batch driver.Batch, what string) {
	if aerr := batch.Abort(); aerr != nil {
		log.Warnf("%s: не удалось закрыть батч после ошибки: %v", what, aerr)
	}
}

// utcDate — календарная дата значения в UTC.
//
// Год/месяц/день берутся у значения КАК ЕСТЬ, а не через t.UTC(): clickhouse-go
// отдаёт Date с таймзоной сессии (moex/moex_iss.go:97), и полночь по Москве —
// это 21:00 предыдущего дня UTC, то есть .UTC() сдвинул бы день назад. Сдвиг
// менял бы окно сверки (среднее не за тот день), а у target_date = первого числа
// квартала — сам квартал. Тот же приём — gold/gold.go (loadUsdRates).
func utcDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func init() {
	chimport.Stats = append(chimport.Stats, &resolver{})
}
