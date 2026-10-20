package ppi

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

//go:embed data/ppi_sa_x13.R
var ppiX13Script []byte

// ppiX13Timeout — предел одного прогона X-13: авто-ARIMA на 164 месяцах
// укладывается в секунды, но зависший R не должен держать dagu-шаг вечно.
const ppiX13Timeout = 15 * time.Minute

// x13Runner — точка подмены в тестах: R в CI нет, а разбор и запись надо
// проверять офлайн.
type x13Runner func(ctx context.Context, input []byte) ([]byte, error)

// runX13 пишет встроенный скрипт и вход во временный каталог и вызывает
// Rscript. Скрипт и данные встроены, поэтому бинарник на staging не зависит
// от scripts/ и docs/; внешняя зависимость — только R + seasonal.
func runX13(ctx context.Context, input []byte) ([]byte, error) {
	rscript, err := exec.LookPath("Rscript")
	if err != nil {
		return nil, fmt.Errorf("не найден Rscript в PATH: нужен R с пакетом seasonal: %w", err)
	}
	dir, err := os.MkdirTemp("", "ppi_sa_x13_")
	if err != nil {
		return nil, fmt.Errorf("X-13: временный каталог: %w", err)
	}
	defer func() {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			log.Warnf("ppi_sa: не удалось удалить временный каталог %s: %v", dir, rmErr)
		}
	}()

	scriptPath := filepath.Join(dir, "ppi_sa_x13.R")
	inputPath := filepath.Join(dir, "input.csv")
	outputPath := filepath.Join(dir, "output.csv")
	if writeErr := os.WriteFile(scriptPath, ppiX13Script, 0o600); writeErr != nil {
		return nil, fmt.Errorf("X-13: скрипт: %w", writeErr)
	}
	if writeErr := os.WriteFile(inputPath, input, 0o600); writeErr != nil {
		return nil, fmt.Errorf("X-13: вход: %w", writeErr)
	}

	runCtx, cancel := context.WithTimeout(ctx, ppiX13Timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, rscript, scriptPath, inputPath, outputPath)
	cmd.Env = os.Environ()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if runErr := cmd.Run(); runErr != nil {
		return nil, fmt.Errorf("запуск X-13 (Rscript): %w: %s", runErr, tail(stderr.String(), 2000))
	}
	result, readErr := os.ReadFile(outputPath)
	if readErr != nil {
		return nil, fmt.Errorf("X-13: результат: %w", readErr)
	}
	log.Infof("ppi_sa: X-13 %s", strings.TrimSpace(stdout.String()))
	return result, nil
}

// tail оставляет хвост вывода R: начало stderr часто занимает шум загрузки
// пакета, а причина — в конце.
func tail(text string, limit int) string {
	if len(text) <= limit {
		return strings.TrimSpace(text)
	}
	return strings.TrimSpace(text[len(text)-limit:])
}

// encodeSeriesCSV — вход R: series,date,level в фиксированном порядке рядов.
func encodeSeriesCSV(series ppiSeries) ([]byte, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	writer.UseCRLF = false
	if err := writer.Write([]string{"series", "date", "level"}); err != nil {
		return nil, fmt.Errorf("CSV входа: %w", err)
	}
	for _, name := range ppiSAOrder {
		points, ok := series[name]
		if !ok || len(points) == 0 {
			return nil, fmt.Errorf("CSV входа: пустой ряд %q", name)
		}
		for _, point := range points {
			if err := writer.Write([]string{
				name,
				normalizeDate(point.Date).Format("2006-01-02"),
				strconv.FormatFloat(point.Value, 'f', 10, 64),
			}); err != nil {
				return nil, fmt.Errorf("CSV входа: %w", err)
			}
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, fmt.Errorf("CSV входа: %w", err)
	}
	return buf.Bytes(), nil
}

// parseX13Output разбирает series,date,sa. Полнота проверяется по входу: каждый
// ожидаемый месяц каждой серии обязан быть, лишние/дубли — ошибка.
func parseX13Output(data []byte, expected ppiSeries) (map[string][]saPoint, error) {
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
	for _, col := range []string{"series", "date", "sa"} {
		if _, ok := idx[col]; !ok {
			return nil, fmt.Errorf("X-13: нет колонки %q", col)
		}
	}

	bySeries := make(map[string]map[time.Time]float64, len(expected))
	for line := 2; ; line++ {
		record, readErr := reader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("X-13: строка %d: %w", line, readErr)
		}
		name := strings.TrimSpace(record[idx["series"]])
		if _, ok := expected[name]; !ok {
			return nil, fmt.Errorf("X-13: строка %d: неизвестная серия %q", line, name)
		}
		date, dateErr := time.Parse("2006-01-02", strings.TrimSpace(record[idx["date"]]))
		if dateErr != nil {
			return nil, fmt.Errorf("X-13: строка %d: дата %q: %w", line, record[idx["date"]], dateErr)
		}
		date = normalizeDate(date)
		value, valueErr := strconv.ParseFloat(strings.TrimSpace(record[idx["sa"]]), 64)
		if valueErr != nil {
			return nil, fmt.Errorf("X-13: строка %d: sa %q: %w", line, record[idx["sa"]], valueErr)
		}
		if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("X-13: строка %d: неположительный или нечисловой sa %v", line, value)
		}
		if bySeries[name] == nil {
			bySeries[name] = map[time.Time]float64{}
		}
		if _, ok := bySeries[name][date]; ok {
			return nil, fmt.Errorf("X-13: строка %d: дубль %s/%s", line, name, date.Format("2006-01"))
		}
		bySeries[name][date] = value
	}

	for name, points := range expected {
		got := bySeries[name]
		if len(got) != len(points) {
			return nil, fmt.Errorf("X-13: серия %s: %d точек, ожидалось %d", name, len(got), len(points))
		}
		for _, point := range points {
			if _, ok := got[normalizeDate(point.Date)]; !ok {
				return nil, fmt.Errorf("X-13: серия %s: нет точки %s", name, point.Date.Format("2006-01"))
			}
		}
	}

	result := make(map[string][]saPoint, len(bySeries))
	for name, points := range bySeries {
		dates := make([]time.Time, 0, len(points))
		for date := range points {
			dates = append(dates, date)
		}
		sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })
		rows := make([]saPoint, 0, len(dates))
		for _, date := range dates {
			rows = append(rows, saPoint{Date: date, SA: points[date]})
		}
		result[name] = rows
	}
	return result, nil
}
