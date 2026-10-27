package weeklysa

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// weeklySABulletinsQuery — агрегат 2009–2021 из бюллетеней «Оперативной
	// информации»: в этой таблице имя ряда лежит в колонке name.
	weeklySABulletinsQuery = `SELECT date, percent
FROM rosstat_ipc_weeks_bulletins FINAL
WHERE name = ?
ORDER BY date`
	// weeklySABIQuery — та же серия 2019+ из BI Росстата.
	weeklySABIQuery = `SELECT date, value
FROM rosstat_bi_cpi_weekly FINAL
WHERE series = ?
ORDER BY date`
	// weeklySAPublishedQuery — as-of входа: когда конвейер впервые увидел версии
	// обоих источников. maxOrNull обязателен: max() по пустой выборке в ClickHouse
	// даёт NULL, а toDate(NULL)-семантика разных версий различна. У BI
	// source_publications может не быть вовсе (динамический export-URL), поэтому
	// published_at — Nullable и NULL здесь штатен.
	weeklySAPublishedQuery = `SELECT toDate(maxOrNull(first_seen))
FROM source_publications FINAL
WHERE source IN (?, ?)`
)

// weeklySARow — строка weekly_cpi_sa в порядке колонок INSERT.
type weeklySARow struct {
	Date        time.Time
	Series      string
	Method      string
	SpecVersion string
	SAIndex     float64
	SA4W        float64
	SAAR        float64
	AsOf        time.Time
	Partial     bool
	PublishedAt sql.NullTime
}

// weeklySAImporter — производный слой SA. runner — точка подмены X-13 в тестах;
// в реестре стоит runX13.
type weeklySAImporter struct {
	runner x13Runner
}

func (s *weeklySAImporter) Name() string { return weeklySATable }

// Import — цикл производного импортёра: DDL → входы → слияние → отпечаток →
// гейт версии → сетка/месяцы → X-13 → строки → каталог/витрина → версия.
func (s *weeklySAImporter) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if pinErr := checkEmbeddedPins(); pinErr != nil {
		return 0, pinErr
	}
	if execErr := conn.Exec(ctx, weeklySADdl); execErr != nil {
		return 0, fmt.Errorf("%s: DDL: %w", weeklySATable, execErr)
	}
	for _, alter := range weeklySAAlters {
		if execErr := conn.Exec(ctx, alter); execErr != nil {
			return 0, fmt.Errorf("%s: миграция колонок: %w", weeklySATable, execErr)
		}
	}

	bulletin, bi, readErr := readInputs(ctx, conn)
	if readErr != nil {
		return 0, readErr
	}
	merged, mergeErr := mergeSources(bulletin, bi)
	if mergeErr != nil {
		return 0, fmt.Errorf("%s: %w", weeklySATable, mergeErr)
	}
	if validateErr := validateSeries(merged, weeklySAFirstDate, weeklySAMinInputPoints); validateErr != nil {
		return 0, fmt.Errorf("%s: %w", weeklySATable, validateErr)
	}

	fingerprint := inputFingerprint(merged)
	if ensureErr := util.EnsurePublications(ctx, conn); ensureErr != nil {
		return 0, ensureErr
	}
	knownPoints, known, knownErr := util.PublicationKnown(ctx, conn, weeklySATable, weeklySATable, fingerprint)
	if knownErr != nil {
		return 0, knownErr
	}
	if known {
		points, pointsErr := util.CountPoints(ctx, conn, weeklySACountPoints)
		if pointsErr != nil {
			return 0, pointsErr
		}
		if shouldSkip(known, knownPoints, points) {
			if catalogErr := publishWeeklyDerived(ctx, conn); catalogErr != nil {
				return 0, catalogErr
			}
			log.Infof("%s: no-op, версия %s уже импортирована, %d точек на месте", weeklySATable, fingerprint, points)
			return 0, nil
		}
		log.Warnf("%s: версия %s известна, но в %s %d точек вместо %d — полный прогон", weeklySATable, fingerprint, weeklySATable, points, knownPoints)
	}

	grid, gridErr := buildGrid(merged)
	if gridErr != nil {
		return 0, fmt.Errorf("%s: %w", weeklySATable, gridErr)
	}
	index, indexErr := fourWeekIndex(grid)
	if indexErr != nil {
		return 0, fmt.Errorf("%s: %w", weeklySATable, indexErr)
	}
	lastGridDate := grid[len(grid)-1].Date
	months, monthsErr := monthlyLastInMonth(index, lastGridDate, weeklySAFirstMonth, weeklySAMinMonths)
	if monthsErr != nil {
		return 0, fmt.Errorf("%s: %w", weeklySATable, monthsErr)
	}
	partial, partialErr := partialMonth(index, lastGridDate)
	if partialErr != nil {
		return 0, fmt.Errorf("%s: %w", weeklySATable, partialErr)
	}

	runner := s.runner
	if runner == nil {
		runner = runX13
	}
	completeInput, encodeErr := encodeX13Input(months)
	if encodeErr != nil {
		return 0, encodeErr
	}
	completeOutput, runErr := runner(ctx, completeInput)
	if runErr != nil {
		return 0, runErr
	}
	sa, parseErr := parseX13Output(completeOutput, months)
	if parseErr != nil {
		return 0, parseErr
	}

	published, publishedErr := readPublishedAt(ctx, conn)
	if publishedErr != nil {
		return 0, publishedErr
	}
	rows, rowsErr := buildRows(months, sa, published)
	if rowsErr != nil {
		return 0, fmt.Errorf("%s: %w", weeklySATable, rowsErr)
	}
	if partial != nil {
		nowcastMonths := append(append([]monthPoint{}, months...), *partial)
		nowcastInput, nowcastEncodeErr := encodeX13Input(nowcastMonths)
		if nowcastEncodeErr != nil {
			return 0, nowcastEncodeErr
		}
		nowcastOutput, nowcastRunErr := runner(ctx, nowcastInput)
		if nowcastRunErr != nil {
			return 0, nowcastRunErr
		}
		nowcastSA, nowcastParseErr := parseX13Output(nowcastOutput, nowcastMonths)
		if nowcastParseErr != nil {
			return 0, nowcastParseErr
		}
		partialRows, partialRowsErr := buildRows([]monthPoint{*partial}, []saPoint{nowcastSA[len(nowcastSA)-1]}, published)
		if partialRowsErr != nil {
			return 0, fmt.Errorf("%s: %w", weeklySATable, partialRowsErr)
		}
		rows = append(rows, partialRows...)
	}
	count, insertErr := insertRows(ctx, conn, rows)
	if insertErr != nil {
		return count, insertErr
	}

	if catalogErr := publishWeeklyDerived(ctx, conn); catalogErr != nil {
		return count, catalogErr
	}
	points, pointsErr := util.CountPoints(ctx, conn, weeklySACountPoints)
	if pointsErr != nil {
		return count, pointsErr
	}
	if saveErr := util.SavePublication(ctx, conn, util.Publication{
		Source:      weeklySATable,
		Name:        weeklySATable,
		Fingerprint: fingerprint,
		Url:         weeklySADerivedURL,
		FirstSeen:   time.Now(),
		Points:      points,
	}); saveErr != nil {
		return count, saveErr
	}
	log.Infof("%s: %d rows, %s…%s, spec %s", weeklySATable, count,
		rows[0].Date.Format("2006-01-02"), rows[len(rows)-1].Date.Format("2006-01-02"), weeklySASpecVersion)
	return count, nil
}

// readInputs читает оба источника с FINAL. Пустой вход — ошибка: производный
// слой не должен молча писать пустую таблицу.
func readInputs(ctx context.Context, conn driver.Conn) (bulletin, bi []rawPoint, err error) {
	bulletin, err = readPoints(ctx, conn, weeklySABulletinsQuery, weeklySABulletinsSource)
	if err != nil {
		return nil, nil, err
	}
	bi, err = readPoints(ctx, conn, weeklySABIQuery, weeklySABISource)
	if err != nil {
		return nil, nil, err
	}
	if len(bulletin) == 0 {
		return nil, nil, fmt.Errorf("%s: нет точек агрегата %q — сначала rosstat_ipc_weeks_bulletins", weeklySABulletinsSource, weeklySAAggregateSeries)
	}
	if len(bi) == 0 {
		return nil, nil, fmt.Errorf("%s: нет точек агрегата %q — сначала rosstat_bi_cpi_weekly", weeklySABISource, weeklySAAggregateSeries)
	}
	return bulletin, bi, nil
}

func readPoints(ctx context.Context, conn driver.Conn, query, source string) ([]rawPoint, error) {
	rows, err := conn.Query(ctx, query, weeklySAAggregateSeries)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", source, err)
	}
	points := make([]rawPoint, 0, 512)
	for rows.Next() {
		var point rawPoint
		if scanErr := rows.Scan(&point.Date, &point.Value); scanErr != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("%s: %w", source, scanErr)
		}
		point.Date = normalizeDate(point.Date)
		points = append(points, point)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("%s: %w", source, rowsErr)
	}
	if closeErr := rows.Close(); closeErr != nil {
		return nil, fmt.Errorf("%s: %w", source, closeErr)
	}
	return points, nil
}

// inputFingerprint — SHA-256 канонически отсортированных входов, версии
// спецификации и пина R-скрипта. Правка скрипта без bump spec_version всё равно
// меняет отпечаток, поэтому повтор станет полным прогоном, а не ложным no-op.
func inputFingerprint(points []rawPoint) string {
	return inputFingerprintWithScript(points, weeklyX13ScriptSHA256)
}

// inputFingerprintWithScript — та же функция с явным пином: тест проверяет,
// что смена пина меняет отпечаток.
func inputFingerprintWithScript(points []rawPoint, scriptSHA string) string {
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "spec=%s\nfeature=%s\nx13=%s\n", weeklySASpecVersion, weeklySAFeatureVersion, scriptSHA)
	sorted := append([]rawPoint(nil), points...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Date.Before(sorted[j].Date) })
	for _, point := range sorted {
		_, _ = fmt.Fprintf(hash, "w|%s|%s\n",
			normalizeDate(point.Date).Format("2006-01-02"),
			strconv.FormatFloat(point.Value, 'g', -1, 64))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// shouldSkip — решение no-op: версия известна и число точек в таблице совпало с
// записанным при её импорте. Вынесено отдельно, чтобы ветка проверялась тестом
// без подмены соединения ClickHouse.
func shouldSkip(known bool, knownPoints, points uint64) bool {
	return known && points == knownPoints
}

// readPublishedAt — as-of входа из source_publications по обоим источникам;
// NULL означает, что строк версий ещё нет (первый прогон / BI без preflight).
func readPublishedAt(ctx context.Context, conn driver.Conn) (sql.NullTime, error) {
	var value sql.NullTime
	if err := conn.QueryRow(ctx, weeklySAPublishedQuery, weeklySABulletinsSource, weeklySABISource).Scan(&value); err != nil {
		return sql.NullTime{}, fmt.Errorf("%s: published_at: %w", weeklySATable, err)
	}
	return value, nil
}

// buildRows проверяет совпадение SA-ряда с месяцами и собирает строки с версией
// в каждой. AsOf/Partial переносятся из monthPoint: так полные месяцы и
// nowcast-строка различаются без отдельного SQL-признака по дате.
func buildRows(months []monthPoint, sa []saPoint, published sql.NullTime) ([]weeklySARow, error) {
	metrics, err := computeMetrics(months, sa)
	if err != nil {
		return nil, err
	}
	rows := make([]weeklySARow, len(metrics))
	for i, metric := range metrics {
		rows[i] = weeklySARow{
			Date:        metric.Date,
			Series:      weeklySASeries,
			Method:      weeklySAMethod,
			SpecVersion: weeklySASpecVersion,
			SAIndex:     metric.SAIndex,
			SA4W:        metric.SA4W,
			SAAR:        metric.SAAR,
			AsOf:        metric.AsOf,
			Partial:     metric.Partial,
			PublishedAt: published,
		}
	}
	return rows, nil
}

// insertRows — один PrepareBatch на прогон, Append с проверкой, Send.
func insertRows(ctx context.Context, conn driver.Conn, rows []weeklySARow) (int64, error) {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO weekly_cpi_sa (date, series, method, spec_version, sa_index, sa_4w, saar, as_of, partial, published_at)")
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		if appendErr := batch.Append(
			row.Date,
			row.Series,
			row.Method,
			row.SpecVersion,
			row.SAIndex,
			row.SA4W,
			row.SAAR,
			row.AsOf,
			row.Partial,
			nullableDate(row.PublishedAt),
		); appendErr != nil {
			if abortErr := batch.Abort(); abortErr != nil {
				log.Warnf("%s: не удалось закрыть батч после ошибки: %v", weeklySATable, abortErr)
			}
			return 0, appendErr
		}
	}
	if sendErr := batch.Send(); sendErr != nil {
		return 0, sendErr
	}
	return int64(len(rows)), nil
}

func nullableDate(value sql.NullTime) any {
	if !value.Valid {
		return nil
	}
	return value.Time
}

// publishWeeklyDerived — 3 ряда каталога и витрина v_weekly_cpi_sa. Вызывается и
// на no-op-пути: каталог и витрина self-heal, даже если строки уже на месте.
func publishWeeklyDerived(ctx context.Context, conn driver.Conn) error {
	if err := util.UpsertSeriesCatalog(ctx, conn, weeklySASeriesMeta()); err != nil {
		return err
	}
	_, err := util.CreateView(ctx, conn, weeklySAView)
	return err
}
