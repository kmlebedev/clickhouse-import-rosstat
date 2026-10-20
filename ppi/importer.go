package ppi

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
	// ppiSAVEDQuery — только совокупный код и только m/m: остальные типы
	// индексов для цепочки уровней не нужны.
	ppiSAVEDQuery = `SELECT date, code, value
FROM rosstat_ppi_ved FINAL
WHERE index_type = 'm/m' AND code = ?
ORDER BY date`
	// ppiSAProductQuery — все m/m товары; фильтрация по mapped-листьям идёт в Go,
	// потому что mapping версионируется в коде, а не в SQL.
	ppiSAProductQuery = `SELECT date, okpd2, value
FROM rosstat_ppi_product FINAL
WHERE index_type = 'm/m'
ORDER BY okpd2, date`
	// ppiSAPublishedQuery — as-of входного винтажа: когда конвейер впервые увидел
	// файл источника. maxOrNull обязателен: max() по пустой выборке в ClickHouse
	// даёт NULL, но toDate(NULL)-семантика разных версий различна, а 1970-01-01 в
	// published_at был бы ложной датой. Это не официальная дата публикации
	// Росстата (её в машиночитаемом виде нет), поэтому колонка и оговорка названы явно.
	ppiSAPublishedQuery = `SELECT toDate(maxOrNull(first_seen))
FROM source_publications FINAL
WHERE source = ?`
)

// ppiSARow — строка ppi_sa в порядке колонок INSERT.
type ppiSARow struct {
	Date            time.Time
	Series          string
	Method          string
	SpecVersion     string
	TaxonomyVersion string
	SA              float64
	MMSA            *float64
	SAAR            *float64
	SAAR3           *float64
	PublishedAt     sql.NullTime
}

// ppiSAImporter — производный слой SA. runner — точка подмены X-13 в тестах;
// в реестре стоит runX13.
type ppiSAImporter struct {
	runner x13Runner
}

func (s *ppiSAImporter) Name() string { return ppiSATable }

// Import — цикл производного импортёра: DDL → входы → агрегация → отпечаток →
// гейт версии → X-13 → строки → каталог → версия.
func (s *ppiSAImporter) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if pinErr := checkEmbeddedPins(); pinErr != nil {
		return 0, pinErr
	}
	if execErr := conn.Exec(ctx, ppiSADdl); execErr != nil {
		return 0, fmt.Errorf("%s: DDL: %w", ppiSATable, execErr)
	}

	mapping, parseErr := parseMapping(ppiMappingCSV)
	if parseErr != nil {
		return 0, parseErr
	}
	if mapping.Version != ppiSATaxonomyVersion {
		return 0, fmt.Errorf("%s: mapping %q, ожидалась %q: обновите ppi/data и константу версии", ppiSATable, mapping.Version, ppiSATaxonomyVersion)
	}
	weights, weightsErr := parseWeights(ppiWeightsCSV)
	if weightsErr != nil {
		return 0, weightsErr
	}
	if checkErr := checkWeights(mapping, weights); checkErr != nil {
		return 0, checkErr
	}

	ved, product, readErr := readInputs(ctx, conn)
	if readErr != nil {
		return 0, readErr
	}
	series, seriesErr := buildSeries(ved, product, mapping, weights)
	if seriesErr != nil {
		return 0, fmt.Errorf("%s: %w", ppiSATable, seriesErr)
	}

	fingerprint := inputFingerprint(ved, product)
	if ensureErr := util.EnsurePublications(ctx, conn); ensureErr != nil {
		return 0, ensureErr
	}
	knownPoints, known, knownErr := util.PublicationKnown(ctx, conn, ppiSATable, ppiSATable, fingerprint)
	if knownErr != nil {
		return 0, knownErr
	}
	if known {
		points, pointsErr := util.CountPoints(ctx, conn, ppiSACountPoints)
		if pointsErr != nil {
			return 0, pointsErr
		}
		if points == knownPoints {
			if catalogErr := util.UpsertSeriesCatalog(ctx, conn, ppiSASeriesMeta()); catalogErr != nil {
				return 0, catalogErr
			}
			log.Infof("%s: no-op, версия %s уже импортирована, %d точек на месте", ppiSATable, fingerprint, points)
			return int64(points), nil
		}
		log.Warnf("%s: версия %s известна, но в ppi_sa %d точек вместо %d — полный прогон", ppiSATable, fingerprint, points, knownPoints)
	}

	input, encodeErr := encodeSeriesCSV(series)
	if encodeErr != nil {
		return 0, encodeErr
	}
	runner := s.runner
	if runner == nil {
		runner = runX13
	}
	output, runErr := runner(ctx, input)
	if runErr != nil {
		return 0, runErr
	}
	sa, parseOutputErr := parseX13Output(output, series)
	if parseOutputErr != nil {
		return 0, parseOutputErr
	}

	vedPublished, publishedErr := readPublishedAt(ctx, conn, ppiSAVEDSource)
	if publishedErr != nil {
		return 0, publishedErr
	}
	productPublished, productPublishedErr := readPublishedAt(ctx, conn, ppiSAProductSource)
	if productPublishedErr != nil {
		return 0, productPublishedErr
	}
	published := map[string]sql.NullTime{
		"total":   vedPublished,
		"food":    productPublished,
		"nonfood": productPublished,
		"all":     productPublished,
		"core":    productPublished,
	}
	rows, rowsErr := buildRows(series, sa, published)
	if rowsErr != nil {
		return 0, rowsErr
	}
	count, insertErr := insertRows(ctx, conn, rows)
	if insertErr != nil {
		return count, insertErr
	}

	if catalogErr := util.UpsertSeriesCatalog(ctx, conn, ppiSASeriesMeta()); catalogErr != nil {
		return count, catalogErr
	}
	points, pointsErr := util.CountPoints(ctx, conn, ppiSACountPoints)
	if pointsErr != nil {
		return count, pointsErr
	}
	if saveErr := util.SavePublication(ctx, conn, util.Publication{
		Source:      ppiSATable,
		Name:        ppiSATable,
		Fingerprint: fingerprint,
		Url:         ppiSADerivedURL,
		FirstSeen:   time.Now(),
		Points:      points,
	}); saveErr != nil {
		return count, saveErr
	}
	log.Infof("%s: %d series, %d rows, spec %s, taxonomy %s", ppiSATable, len(ppiSAOrder), count, ppiSASpecVersion, ppiSATaxonomyVersion)
	return count, nil
}

// readInputs читает оба сырых ряда с FINAL. Пустой вход — ошибка: производный
// слой не должен молча писать пустую таблицу.
func readInputs(ctx context.Context, conn driver.Conn) (ved, product []rawPoint, err error) {
	rows, err := conn.Query(ctx, ppiSAVEDQuery, ppiSATotalCode)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", ppiSAVEDSource, err)
	}
	for rows.Next() {
		var point rawPoint
		if scanErr := rows.Scan(&point.Date, &point.Code, &point.Value); scanErr != nil {
			_ = rows.Close()
			return nil, nil, fmt.Errorf("%s: %w", ppiSAVEDSource, scanErr)
		}
		point.Date = normalizeDate(point.Date)
		ved = append(ved, point)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		_ = rows.Close()
		return nil, nil, fmt.Errorf("%s: %w", ppiSAVEDSource, rowsErr)
	}
	if closeErr := rows.Close(); closeErr != nil {
		return nil, nil, fmt.Errorf("%s: %w", ppiSAVEDSource, closeErr)
	}

	rows, err = conn.Query(ctx, ppiSAProductQuery)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", ppiSAProductSource, err)
	}
	for rows.Next() {
		var point rawPoint
		if scanErr := rows.Scan(&point.Date, &point.Code, &point.Value); scanErr != nil {
			_ = rows.Close()
			return nil, nil, fmt.Errorf("%s: %w", ppiSAProductSource, scanErr)
		}
		point.Date = normalizeDate(point.Date)
		product = append(product, point)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		_ = rows.Close()
		return nil, nil, fmt.Errorf("%s: %w", ppiSAProductSource, rowsErr)
	}
	if closeErr := rows.Close(); closeErr != nil {
		return nil, nil, fmt.Errorf("%s: %w", ppiSAProductSource, closeErr)
	}

	if len(ved) == 0 {
		return nil, nil, fmt.Errorf("%s: нет m/m точек кода %s — сначала rosstat_ppi_ved", ppiSAVEDSource, ppiSATotalCode)
	}
	if len(product) == 0 {
		return nil, nil, fmt.Errorf("%s: нет m/m точек — сначала rosstat_ppi_product", ppiSAProductSource)
	}
	return ved, product, nil
}

// inputFingerprint — SHA-256 канонически отсортированных входов, версий и
// пинов встроенных данных/скрипта. Правка R-скрипта или CSV без bump версии
// всё равно меняет отпечаток (пины входят в хеш), поэтому повтор станет полным
// прогоном, а не ложным no-op.
func inputFingerprint(ved, product []rawPoint) string {
	return inputFingerprintWithPins(ved, product, [3]string{
		ppiX13ScriptSHA256, ppiMappingSHA256, ppiWeightsSHA256,
	})
}

// inputFingerprintWithPins — та же функция с явными пинами: тест проверяет,
// что смена любого пина меняет отпечаток.
func inputFingerprintWithPins(ved, product []rawPoint, pins [3]string) string {
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "spec=%s\ntaxonomy=%s\nx13=%s\nmapping=%s\nweights=%s\n",
		ppiSASpecVersion, ppiSATaxonomyVersion, pins[0], pins[1], pins[2])

	vedSorted := append([]rawPoint(nil), ved...)
	sort.Slice(vedSorted, func(i, j int) bool {
		return vedSorted[i].Date.Before(vedSorted[j].Date)
	})
	for _, point := range vedSorted {
		_, _ = fmt.Fprintf(hash, "ved|%s|%s|%s\n",
			normalizeDate(point.Date).Format("2006-01-02"), point.Code,
			strconv.FormatFloat(point.Value, 'g', -1, 64))
	}

	productSorted := append([]rawPoint(nil), product...)
	sort.Slice(productSorted, func(i, j int) bool {
		if productSorted[i].Date.Equal(productSorted[j].Date) {
			return productSorted[i].Code < productSorted[j].Code
		}
		return productSorted[i].Date.Before(productSorted[j].Date)
	})
	for _, point := range productSorted {
		_, _ = fmt.Fprintf(hash, "ppi|%s|%s|%s\n",
			normalizeDate(point.Date).Format("2006-01-02"), point.Code,
			strconv.FormatFloat(point.Value, 'g', -1, 64))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// readPublishedAt — as-of входа из source_publications; NULL означает, что
// версия ещё не записана (первый прогон до появления строки).
func readPublishedAt(ctx context.Context, conn driver.Conn, source string) (sql.NullTime, error) {
	var value sql.NullTime
	if err := conn.QueryRow(ctx, ppiSAPublishedQuery, source).Scan(&value); err != nil {
		return sql.NullTime{}, fmt.Errorf("%s: published_at: %w", source, err)
	}
	return value, nil
}

// buildRows проверяет, что SA-ряд совпадает с уровнем по датам, и собирает
// строки с версиями в каждой. Версия — не атрибут прогона, а часть строки.
func buildRows(series ppiSeries, sa map[string][]saPoint, published map[string]sql.NullTime) ([]ppiSARow, error) {
	rows := make([]ppiSARow, 0, 436)
	for _, name := range ppiSAOrder {
		points, ok := series[name]
		if !ok || len(points) == 0 {
			return nil, fmt.Errorf("нет уровня ряда %q", name)
		}
		saPoints, ok := sa[name]
		if !ok || len(saPoints) == 0 {
			return nil, fmt.Errorf("нет SA ряда %q", name)
		}
		metrics, err := computeMetrics(saPoints)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		if len(metrics) != len(points) {
			return nil, fmt.Errorf("%s: %d SA-точек против %d точек уровня", name, len(metrics), len(points))
		}
		publishedAt := published[name]
		for i, metric := range metrics {
			expected := normalizeDate(points[i].Date)
			if !metric.Date.Equal(expected) {
				return nil, fmt.Errorf("%s: дата SA %s, ожидалась %s", name, metric.Date.Format("2006-01"), expected.Format("2006-01"))
			}
			rows = append(rows, ppiSARow{
				Date:            metric.Date,
				Series:          name,
				Method:          ppiSAMethod,
				SpecVersion:     ppiSASpecVersion,
				TaxonomyVersion: ppiSATaxonomyVersion,
				SA:              metric.SA,
				MMSA:            metric.MMSA,
				SAAR:            metric.SAAR,
				SAAR3:           metric.SAAR3,
				PublishedAt:     publishedAt,
			})
		}
	}
	return rows, nil
}

// insertRows — один PrepareBatch на прогон, Append с проверкой, Send.
func insertRows(ctx context.Context, conn driver.Conn, rows []ppiSARow) (int64, error) {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO ppi_sa (date, series, method, spec_version, taxonomy_version, sa, mm_sa, saar, saar3, published_at)")
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		if appendErr := batch.Append(
			row.Date,
			row.Series,
			row.Method,
			row.SpecVersion,
			row.TaxonomyVersion,
			row.SA,
			nullableFloat(row.MMSA),
			nullableFloat(row.SAAR),
			nullableFloat(row.SAAR3),
			nullableDate(row.PublishedAt),
		); appendErr != nil {
			if abortErr := batch.Abort(); abortErr != nil {
				log.Warnf("%s: не удалось закрыть батч после ошибки: %v", ppiSATable, abortErr)
			}
			return 0, appendErr
		}
	}
	if sendErr := batch.Send(); sendErr != nil {
		return 0, sendErr
	}
	return int64(len(rows)), nil
}

func nullableFloat(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableDate(value sql.NullTime) any {
	if !value.Valid {
		return nil
	}
	return value.Time
}
