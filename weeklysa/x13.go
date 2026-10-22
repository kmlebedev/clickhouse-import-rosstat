package weeklysa

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

//go:embed data/weekly_cpi_sa_x13.R
var weeklyX13Script []byte

// x13Runner — точка подмены в тестах: R в CI нет, а разбор и запись надо
// проверять офлайн.
type x13Runner func(ctx context.Context, input []byte) ([]byte, error)

// runX13 делегирует общий мост util.RunX13: временный каталог, whitelist
// окружения, таймаут и хвост stderr живут в одном месте для ppi_sa и
// weekly_cpi_sa.
func runX13(ctx context.Context, input []byte) ([]byte, error) {
	return util.RunX13(ctx, weeklyX13Script, input, weeklySATable)
}

// encodeX13Input — вход R: date,level, конец месяца и 4-нед. индекс.
func encodeX13Input(months []monthPoint) ([]byte, error) {
	if len(months) == 0 {
		return nil, fmt.Errorf("CSV входа X-13: пустой месячный ряд")
	}
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	writer.UseCRLF = false
	if err := writer.Write([]string{"date", "level"}); err != nil {
		return nil, fmt.Errorf("CSV входа X-13: %w", err)
	}
	for _, point := range months {
		date := normalizeDate(point.Date).Format("2006-01-02")
		if err := writer.Write([]string{date, strconv.FormatFloat(point.Value, 'g', -1, 64)}); err != nil {
			return nil, fmt.Errorf("CSV входа X-13: %w", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, fmt.Errorf("CSV входа X-13: %w", err)
	}
	return buf.Bytes(), nil
}

// parseX13Output разбирает date,sa_level. Полнота проверяется по входу: каждый
// ожидаемый месяц обязан быть, лишние/дубли/NA/неположительные — ошибка.
func parseX13Output(data []byte, expected []monthPoint) ([]saPoint, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("X-13: чтение шапки: %w", err)
	}
	idx := make(map[string]int, len(header))
	for i, name := range header {
		idx[strings.TrimSpace(name)] = i
	}
	for _, col := range []string{"date", "sa_level"} {
		if _, ok := idx[col]; !ok {
			return nil, fmt.Errorf("X-13: нет колонки %q", col)
		}
	}

	byDate := make(map[time.Time]float64, len(expected))
	for line := 2; ; line++ {
		record, readErr := reader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("X-13: строка %d: %w", line, readErr)
		}
		date, dateErr := time.Parse("2006-01-02", strings.TrimSpace(record[idx["date"]]))
		if dateErr != nil {
			return nil, fmt.Errorf("X-13: строка %d: дата %q: %w", line, record[idx["date"]], dateErr)
		}
		date = normalizeDate(date)
		value, valueErr := strconv.ParseFloat(strings.TrimSpace(record[idx["sa_level"]]), 64)
		if valueErr != nil {
			return nil, fmt.Errorf("X-13: строка %d: sa_level %q: %w", line, record[idx["sa_level"]], valueErr)
		}
		if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("X-13: строка %d: неположительный или нечисловой sa_level %v", line, value)
		}
		if _, ok := byDate[date]; ok {
			return nil, fmt.Errorf("X-13: строка %d: дубль %s", line, date.Format("2006-01"))
		}
		byDate[date] = value
	}

	if len(byDate) != len(expected) {
		return nil, fmt.Errorf("X-13: %d точек, ожидалось %d", len(byDate), len(expected))
	}
	for _, point := range expected {
		date := normalizeDate(point.Date)
		if _, ok := byDate[date]; !ok {
			return nil, fmt.Errorf("X-13: нет точки %s", date.Format("2006-01"))
		}
	}

	dates := make([]time.Time, 0, len(byDate))
	for date := range byDate {
		dates = append(dates, date)
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })
	result := make([]saPoint, 0, len(dates))
	for _, date := range dates {
		result = append(result, saPoint{Date: date, SA: byDate[date]})
	}
	return result, nil
}

// normalizeDate отбрасывает время и часовой пояс: ключ точки — календарная дата.
func normalizeDate(value time.Time) time.Time {
	year, month, day := value.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}
