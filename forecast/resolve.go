// Package forecast резолвит журнал прогнозов золотого контура: заполняет
// forecast_log.actual и error_pct сверкой прогноза с фактом из gold_prices.
// Единственный импортёр пакета — forecast_resolve.
//
// ПОЧЕМУ `nav` НЕ РЕЗОЛВИТСЯ. Факта для NAV на акцию не существует. Расчётный
// NAV sum-of-parts и рыночная цена PLZL — разные величины (цена — мнение рынка о
// будущем, NAV — сумма дисконтированных потоков по плану производства), поэтому
// подстановка market_price в actual записала бы в журнал «ошибку прогноза» там,
// где прогноз вообще не этой величиной проверяется. Ровно это зафиксировано в
// verify.md плагина gold-nav: «nav — факт не определён, оставь actual пустым».
// Поэтому выборка строк идёт по metric = 'xau_q_avg', а прочие метрики (nav и
// будущие) остаются нетронутыми — не ошибкой и не warn'ом, а состоянием журнала.
//
// ПОЧЕМУ ОКНО СВЕРКИ — КВАРТАЛ, СОДЕРЖАЩИЙ target_date. Прогноз xau_q_avg — это
// СРЕДНЯЯ цена золота за квартал горизонта, а не цена на дату (ingest/clickhouse.go:
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
package forecast

import (
	"context"
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

	// forecastMetric — метрика, которую резолвер сверяет с фактом: единственная,
	// у которой факт в этом контуре определён (см. комментарий пакета про nav).
	forecastMetric = "xau_q_avg"

	// comexVenue, moexVenue — площадки gold_prices, из которых берётся факт.
	// Порядок приоритета задан pickActual: comex_front — целевой ряд verify.md
	// (GC=F), moex_fix_usd — производная проксия (MOEX-фикс ÷ курс ЦБ).
	comexVenue = "comex_front"
	moexVenue  = "moex_fix_usd"

	// dateLayout — формат дат в параметрах запроса и в логе.
	dateLayout = "2006-01-02"

	// forecastLogInsert — вставка резолвленных строк. Порядок колонок — как в
	// forecastLogDdl (ingest/schema.go): forecast_date, target_date, metric,
	// predicted, actual, error_pct. Сверять глазами обязательно: ClickHouse примет
	// перестановку однотипных Date/Float64 и молча запишет actual в error_pct.
	forecastLogInsert = "INSERT INTO forecast_log (forecast_date, target_date, metric, predicted, actual, error_pct)"
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
const resolvableSelect = `SELECT forecast_date, target_date, predicted, if(actual IS NULL, 1, 0) AS actual_is_null, coalesce(actual, 0) AS actual FROM forecast_log FINAL WHERE metric = '` + forecastMetric + `' AND target_date <= today() ORDER BY forecast_date`

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
// Actual == nil означает, что факт ещё не записан (в SQL это actual IS NULL).
type forecastRow struct {
	ForecastDate time.Time
	TargetDate   time.Time
	Predicted    float64
	Actual       *float64
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
// батч. Actual и ErrorPct здесь не указатели: резолвленная строка всегда имеет оба
// значения, а «пусто» в этой структуре выражается отсутствием элемента в срезе.
type forecastResolution struct {
	ForecastDate time.Time
	TargetDate   time.Time
	Predicted    float64
	Actual       float64
	ErrorPct     float64
}

// resolveStore — узкий срез стора, который нужен оркестрации Import; в тестах
// подменяется фейком (паттерн chStore из ingest/clickhouse.go).
type resolveStore interface {
	resolvable(ctx context.Context) ([]forecastRow, error)
	windowStats(ctx context.Context, start, end time.Time) (windowStats, error)
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
// другой шаг; собственное расписание (23:20) идёт после обоих источников дня.
type resolver struct{}

func (r *resolver) Name() string { return forecastResolveStat }

// Import читает нерезолвленные прогнозы xau_q_avg, считает факт по золоту за
// квартал каждого target_date и записывает ОДНИМ батчем только изменившиеся
// строки. Возвращает число записанных строк: 0 — норма (менять нечего), а не сбой;
// main.go печатает его сам.
func (r *resolver) Import(ctx context.Context, conn driver.Conn) (int64, error) {
	return resolveForecasts(ctx, connStore{conn: conn})
}

// resolveForecasts — оркестрация: выборка → окно → статистика → решение →
// батч. Вынесена из Import, чтобы проверяться офлайн на фейковом сторе.
//
// Пустая выборка и «все строки уже сходятся» — не ошибки: и то и другое
// возвращает 0 строк (журнал прогнозов заполняется ingest'ом, и на свежей БД
// строк xau_q_avg с наступившей target_date может не быть вовсе).
func resolveForecasts(ctx context.Context, store resolveStore) (int64, error) {
	rows, err := store.resolvable(ctx)
	if err != nil {
		return 0, err
	}

	var resolutions []forecastResolution

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
		resolution := forecastResolution{
			ForecastDate: row.ForecastDate,
			TargetDate:   row.TargetDate,
			Predicted:    row.Predicted,
			Actual:       actual,
			ErrorPct:     errorPct(row.Predicted, actual),
		}

		log.Infof("forecast_resolve: прогноз %s на %s — %s, точек %d, actual %.4f, error_pct %.4f",
			row.ForecastDate.Format(dateLayout), row.TargetDate.Format(dateLayout), venue,
			points, actual, resolution.ErrorPct)

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
			forecastDate time.Time
			targetDate   time.Time
			predicted    float64
			actualIsNull uint8
			actual       float64
		)

		// Порядок Scan — порядок колонок resolvableSelect. actual_is_null читается
		// в uint8 (if(...) имеет тип UInt8), actual — во float64 (coalesce(Float64)).
		if err = rows.Scan(&forecastDate, &targetDate, &predicted, &actualIsNull, &actual); err != nil {
			return nil, fmt.Errorf("резолв прогнозов: читать строку forecast_log: %w", err)
		}

		row := forecastRow{
			ForecastDate: utcDate(forecastDate),
			TargetDate:   utcDate(targetDate),
			Predicted:    predicted,
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
		// Порядок позиций — порядок колонок forecastLogInsert. actual и error_pct
		// уходят как float64: в DDL они Nullable, но резолвленная строка всегда
		// имеет оба значения, и NULL в резолве — это отсутствие строки в батче.
		if err = batch.Append(row.ForecastDate, row.TargetDate, forecastMetric, row.Predicted, row.Actual, row.ErrorPct); err != nil {
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
