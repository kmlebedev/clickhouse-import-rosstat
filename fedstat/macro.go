package fedstat

// Общий раннер макро-импортёров: конфиг источника, сборка позиционного тела
// POST, выбор кода РФ, пауза между запросами, батч-вставка. Импортёры отличаются
// только DDL, маппингом разобранных записей в точки и каталогом/витриной.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
)

// fedstatFilter — фиксированное значение измерения в форме.
type fedstatFilter struct {
	dim  string
	code string
}

// fedstatSourceSpec — один индикатор ЕМИСС внутри импортёра. Layout-массивы
// (left/top/filter) зашиты по research-конфигурации 2026-10-21: они стабильны,
// а коды all-dims парсятся со страницы, потому что справочники меняются.
type fedstatSourceSpec struct {
	indicator    string
	title        string
	layout       fedstatLayout
	labelCount   int
	left         []string
	top          []string
	filter       []string
	unitDim      string
	unitCode     string
	regionDim    string
	fixed        []fedstatFilter
	allDims      []string
	years        []int
	batch        bool // один POST на весь диапазон (колонки-годы)
	regionSwitch bool // 2023+: пробовать 1849012, затем 1688487
	pause        time.Duration
}

func fedstatYearRange(from, to int) []int {
	years := make([]int, 0, to-from+1)
	for year := from; year <= to; year++ {
		years = append(years, year)
	}
	return years
}

// fedstatRegionsFor — 2023+ сначала новое определение РФ, затем старое: 58089
// за 2023 г. отдаётся только под 1688487, а 57806/31074 — под 1849012.
func fedstatRegionsFor(year int, switchRegion bool) []string {
	if switchRegion && year >= 2023 {
		return []string{fedstatRegionRFNew, fedstatRegionRF}
	}
	return []string{fedstatRegionRF}
}

func fedstatRegionName(code string) string {
	if code == fedstatRegionRFNew {
		return fedstatRegionRFNewName
	}
	return fedstatRegionRFName
}

// fedstatPacer — пауза между POST. Серия без паузы получает 503; у 31074
// пауза 30 с, у остальных 10 с.
type fedstatPacer struct {
	pause time.Duration
	last  time.Time
}

func (p *fedstatPacer) wait(ctx context.Context) error {
	if p.pause <= 0 {
		return nil
	}
	if !p.last.IsZero() {
		delay := p.pause
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	p.last = time.Now()
	return nil
}

type fedstatYearResult struct {
	records []fedstatRecord
	region  string
}

// fedstatFetchYear — GET страницы, токен, конфиг справочников, POST excel,
// разбор. Ошибка «нет данных» по одному коду РФ — повод попробовать второй;
// остальные ошибки не глотаются.
func fedstatFetchYear(ctx context.Context, client *http.Client, source fedstatSourceSpec, year int, pacer *fedstatPacer) (fedstatYearResult, error) {
	var lastNoData error
	for _, region := range fedstatRegionsFor(year, source.regionSwitch) {
		if err := pacer.wait(ctx); err != nil {
			return fedstatYearResult{}, err
		}
		result, err := fedstatFetchYearRegion(ctx, client, source, year, region)
		if err == nil {
			return result, nil
		}
		if errors.Is(err, errFedstatNoData) {
			lastNoData = err
			log.Warnf("%s: %d, регион %s: данных нет, пробую следующий код РФ", source.title, year, region)
			continue
		}
		return fedstatYearResult{}, err
	}
	return fedstatYearResult{}, lastNoData
}

func fedstatFetchYearRegion(ctx context.Context, client *http.Client, source fedstatSourceSpec, year int, region string) (fedstatYearResult, error) {
	page, err := fedstatGetPage(ctx, client, source.indicator)
	if err != nil {
		return fedstatYearResult{}, err
	}
	token, err := fedstatToken(page, source.indicator)
	if err != nil {
		return fedstatYearResult{}, err
	}
	config, err := fedstatSourceSpecConfig(page, source)
	if err != nil {
		return fedstatYearResult{}, err
	}
	body, err := fedstatBuildForm(source, token, []int{year}, region, config)
	if err != nil {
		return fedstatYearResult{}, err
	}
	data, err := fedstatPostExcel(ctx, client, source.indicator, body)
	if err != nil {
		return fedstatYearResult{}, fmt.Errorf("%s %d/%s: %w", source.indicator, year, region, err)
	}
	records, err := fedstatParseExcel(data, source.layout, year, source.labelCount)
	if err != nil {
		return fedstatYearResult{}, fmt.Errorf("%s %d/%s: %w", source.indicator, year, region, err)
	}
	return fedstatYearResult{records: records, region: region}, nil
}

// fedstatFetchBatch — один POST на весь диапазон лет (колонки-годы). Регион —
// только старое определение РФ: исторические ряды до 2023 г. иначе не отдаются.
func fedstatFetchBatch(ctx context.Context, client *http.Client, source fedstatSourceSpec, pacer *fedstatPacer) (fedstatYearResult, error) {
	if err := pacer.wait(ctx); err != nil {
		return fedstatYearResult{}, err
	}
	page, err := fedstatGetPage(ctx, client, source.indicator)
	if err != nil {
		return fedstatYearResult{}, err
	}
	token, err := fedstatToken(page, source.indicator)
	if err != nil {
		return fedstatYearResult{}, err
	}
	config, err := fedstatSourceSpecConfig(page, source)
	if err != nil {
		return fedstatYearResult{}, err
	}
	body, err := fedstatBuildForm(source, token, source.years, fedstatRegionRF, config)
	if err != nil {
		return fedstatYearResult{}, err
	}
	data, err := fedstatPostExcel(ctx, client, source.indicator, body)
	if err != nil {
		return fedstatYearResult{}, fmt.Errorf("%s %d–%d: %w", source.indicator, source.years[0], source.years[len(source.years)-1], err)
	}
	records, err := fedstatParseExcel(data, source.layout, 0, source.labelCount)
	if err != nil {
		return fedstatYearResult{}, fmt.Errorf("%s %d–%d: %w", source.indicator, source.years[0], source.years[len(source.years)-1], err)
	}
	return fedstatYearResult{records: records, region: fedstatRegionRF}, nil
}

func fedstatSourceSpecConfig(page []byte, source fedstatSourceSpec) (map[string][]string, error) {
	if len(source.allDims) == 0 {
		return nil, nil
	}
	return fedstatPageConfig(page, source.allDims)
}

// fedstatBuildForm — тело POST в порядке осей. selectedFilterIds идут строго
// indicator -> unit -> регион -> годы -> fixed -> all-dims; алфавитный порядок
// url.Values.Encode даёт пустой конверт (проверено live 2026-10-21).
func fedstatBuildForm(source fedstatSourceSpec, token string, years []int, region string, config map[string][]string) ([]byte, error) {
	fields := []fedstatField{{"id", source.indicator}}
	for _, dim := range source.left {
		fields = append(fields, fedstatField{"lineObjectIds", dim})
	}
	for _, dim := range source.top {
		fields = append(fields, fedstatField{"columnObjectIds", dim})
	}
	for _, dim := range source.filter {
		fields = append(fields, fedstatField{"filterObjectIds", dim})
	}
	fields = append(fields,
		fedstatField{"struts.token.name", "token"},
		fedstatField{"token", token},
		fedstatField{"title", source.title},
	)
	selected := []string{"0_" + source.indicator}
	if source.unitDim != "" && source.unitCode != "" {
		selected = append(selected, source.unitDim+"_"+source.unitCode)
	}
	if source.regionDim != "" {
		selected = append(selected, source.regionDim+"_"+region)
	}
	for _, year := range years {
		selected = append(selected, "3_"+strconv.Itoa(year))
	}
	for _, fixed := range source.fixed {
		selected = append(selected, fixed.dim+"_"+fixed.code)
	}
	for _, dim := range source.allDims {
		codes, ok := config[dim]
		if !ok || len(codes) == 0 {
			return nil, fmt.Errorf("%s: нет кодов измерения %s", source.indicator, dim)
		}
		for _, code := range codes {
			selected = append(selected, dim+"_"+code)
		}
	}
	for _, value := range selected {
		fields = append(fields, fedstatField{"selectedFilterIds", value})
	}
	return fedstatEncodeFields(fields), nil
}

// fedstatFetchYears — последовательная загрузка годов с маппингом записей в
// точки импортёра. «Нет данных» по году — warn и пропуск; остальные ошибки
// возвращаются. Пустой результат — повод импортёру сообщить Imported 0 rows.
func fedstatFetchYears[T any](ctx context.Context, client *http.Client, pacer *fedstatPacer, table string, source fedstatSourceSpec, years []int, mapRecord func(fedstatRecord, string) (T, error)) ([]T, error) {
	points := make([]T, 0, 12*len(years))
	for _, year := range years {
		result, err := fedstatFetchYear(ctx, client, source, year, pacer)
		if errors.Is(err, errFedstatNoData) {
			log.Warnf("%s: %s/%d: данных нет ни по одному коду РФ", table, source.indicator, year)
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, record := range result.records {
			point, err := mapRecord(record, result.region)
			if err != nil {
				return nil, fmt.Errorf("%s: %s/%d: %w", table, source.indicator, year, err)
			}
			points = append(points, point)
		}
		log.Infof("%s: %s/%d: %d точек", table, source.indicator, year, len(result.records))
	}
	return points, nil
}

// fedstatDedupPoints — дедуп по ключу с проверкой конфликта значений и
// детерминированной сортировкой. Одинаковый ключ с разным значением — ошибка:
// это признак склейки двух определений или сбоя разметки, а не ревизии.
func fedstatDedupPoints[T any](table string, points []T, keyFn func(T) string, valueFn func(T) float64) ([]T, error) {
	seen := make(map[string]float64, len(points))
	out := make([]T, 0, len(points))
	for _, point := range points {
		key := keyFn(point)
		if previous, ok := seen[key]; ok {
			if previous != valueFn(point) {
				return nil, fmt.Errorf("%s: дубль %s с разными значениями %v и %v", table, key, previous, valueFn(point))
			}
			continue
		}
		seen[key] = valueFn(point)
		out = append(out, point)
	}
	sort.Slice(out, func(i, j int) bool { return keyFn(out[i]) < keyFn(out[j]) })
	return out, nil
}

// fedstatRequireFullFetch — полный прогон без единой точки означает, что источник
// недодал историю (смена разметки/фильтра), а не «пустой ряд»: молчаливый
// Imported 0 rows на первом прогоне скрыл бы сломанный источник.
func fedstatRequireFullFetch(table, source string, full bool, points int) error {
	if full && points == 0 {
		return fmt.Errorf("%s: %s: полный прогон не дал точек — источник недодал историю", table, source)
	}
	return nil
}

// fedstatTableState — count() и max(date) по таблице с фильтром источника.
// Пустая таблица: has=false, max не используется (ClickHouse на пустом max(date)
// отдаёт 1970-01-01).
func fedstatTableState(ctx context.Context, conn driver.Conn, table, where string) (count int64, maxDate time.Time, has bool, err error) {
	query := fmt.Sprintf("SELECT count(), max(date) FROM %s FINAL", table)
	if where != "" {
		query += " WHERE " + where
	}
	var stored uint64
	if err = conn.QueryRow(ctx, query).Scan(&stored, &maxDate); err != nil {
		return 0, time.Time{}, false, fmt.Errorf("%s: состояние таблицы: %w", table, err)
	}
	count = int64(stored)
	return count, maxDate, count > 0, nil
}

// fedstatInsert — один PrepareBatch на прогон, Append с проверкой, Abort на
// пути ошибки до Send (правило 2 AGENTS.md).
func fedstatInsert[T any](ctx context.Context, conn driver.Conn, table string, points []T, appendPoint func(driver.Batch, T) error) (int64, error) {
	if len(points) == 0 {
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
	if err != nil {
		return 0, fmt.Errorf("%s: PrepareBatch: %w", table, err)
	}
	for _, point := range points {
		if err = appendPoint(batch, point); err != nil {
			fedstatAbortBatch(batch, table)
			return 0, fmt.Errorf("%s: Append: %w", table, err)
		}
	}
	count := int64(batch.Rows())
	if err = batch.Send(); err != nil {
		return count, fmt.Errorf("%s: Send: %w", table, err)
	}
	return count, nil
}

// fedstatPointKey — ключ точки для дедупа и сравнения с уже записанными строками.
// Разделитель \x1f не встречается в подписях источника.
func fedstatPointKey(parts ...string) string {
	return strings.Join(parts, "\x1f")
}
