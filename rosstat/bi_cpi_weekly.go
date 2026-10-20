package rosstat

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// rosstatBICpiWeeklyTable — недельный агрегат ИПЦ РФ из BI Росстата. Отдельная таблица,
	// а не rosstat_ipc_weeks: XLSX 2022+ не содержит агрегатной строки, а смешивать два
	// источника в одной таблице значило бы ловить гонку с недельным XLSX-импортёром.
	rosstatBICpiWeeklyTable = "rosstat_bi_cpi_weekly"
	// biCpiWeeklyDdl — DDL в %s-форме: имя таблицы подставляет шаблон util.ClickHouseImport.
	biCpiWeeklyDdl = `CREATE TABLE IF NOT EXISTS %s (
					  series LowCardinality(String)
					, date Date
					, value Float64
				) ENGINE = ReplacingMergeTree ORDER BY (series, date);
			`
	// biCpiWeeklyDashboardURL — Referer, с которым браузер шлёт export-RPC.
	biCpiWeeklyDashboardURL = "https://bi.rosstat.gov.ru/biportal/contourbi.jsp?allsol=1&solution=Dashboard&project=%2FDashboard%2FPrices_week"
	// biCpiWeeklyPermutation и biCpiWeeklyModuleBase — заголовки GWT-RPC, снятые из
	// Network 2026-10-20. При обновлении BI их нужно перехватить заново (спека §2).
	biCpiWeeklyPermutation = "0D850A6CF67C443893E598332DD51877"
	biCpiWeeklyModuleBase  = "https://bi.rosstat.gov.ru/biportal/ddb/"
	// biCpiWeeklyExportPayload — тело GridViewService.exportGrid: карточка «Индекс
	// потребительских цен», slice1/view1/grid, формат csv. Литерал намеренно: это
	// контракт с конкретной сборкой BI, а не вычисляемый запрос. Ошибка RPC должна
	// падать явно (спека §2, §6).
	biCpiWeeklyExportPayload = `7|0|29|https://bi.rosstat.gov.ru/biportal/ddb/|4E54CCD3C262C2F21A605D6A0465BA40|com.contourcomponents.ddb.client.GridViewService|exportGrid|com.contourcomponents.ddb.client.DashboardRequest/2249903702|I|java.lang.String/2004016611|[Z/1413617015|java.lang.Long/4227064769|[I/2970817851|Индекс_потребительских_цен/slice1|java.util.HashMap/1797211028|report15|3|report18|allsol|1|solution|Dashboard|project|/Dashboard/Prices_week|report|Индекс_потребительских_цен|slice|slice1|tabindex|grid|view1|csv|1|2|3|4|3|5|6|7|5|0|0|0|0|8|2|0|0|0|0|0|9|cRD6OKFw|10|2|-1|-1|0|0|0|0|0|0|11|0|12|0|0|0|0|96|96|0|0|0|0|0|0|0|0|-1|-1|0|0|0|0|0|0|0|0|0|0|13|0|0|0|0|0|14|0|0|15|12|7|7|16|7|17|7|18|7|19|7|20|7|21|7|22|7|23|7|24|7|25|7|26|-8|7|27|7|28|0|2|0|0|0|aFYxoIL|0|15|0|0|0|0|0|0|0|0|27|0|0|0|0|0|1|29|`
	// biCpiWeeklyMinDate — первая публикация BI; более ранних недель источник не отдаёт.
	biCpiWeeklyMinDate = "2019-01-09"
)

// biCpiWeeklyExportURL — GWT-RPC endpoint BI, который готовит CSV по карточке
// «Consumer price index». Это не стабильный REST: ответ содержит новый tmp/exp<uuid>
// на каждый запрос (спека §2). Var, а не const: тест подменяет endpoint httptest-сервером.
var biCpiWeeklyExportURL = "https://bi.rosstat.gov.ru/biportal/grid"

// biCpiWeeklyPublicBase — схема и хост публичного tmp-CSV; var для теста.
var biCpiWeeklyPublicBase = "https://bi.rosstat.gov.ru"

// biCpiWeeklyPoint — точка ряда: дата регистрации и значение индекса.
type biCpiWeeklyPoint struct {
	date  time.Time
	value float64
}

// BICpiWeeklyStat — импортёр недельного агрегата ИПЦ из BI Росстата.
type BICpiWeeklyStat struct {
	util.ClickHouseImport
}

// biCpiWeeklyImporter регистрируется в rosstat/importers.go. PublicationCountPoints не
// заполняется: export-URL динамический (новый exp<uuid> на каждый POST), HEAD-префлайт и
// архив версий недостижимы; идемпотентность обеспечивает ReplacingMergeTree (спека §6).
var biCpiWeeklyImporter = &BICpiWeeklyStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   rosstatBICpiWeeklyTable,
	CreateTable: []string{biCpiWeeklyDdl},
	BodyFunc:    biCpiWeeklyImport,
	AfterImport: afterRosstatMacro(biCpiWeeklySeriesMeta()),
}}

// Import сначала готовит CSV через GWT-RPC BI, затем отдаёт скачивание шаблону: шаблон
// качает байты по DataUrl, BodyFunc их разбирает, AfterImport публикует каталог и витрину.
// Свой Import() — прецедент rosstat_ipc_weeks: DataUrl вычисляется во время прогона, а не
// в init(), и без сети в init().
func (s *BICpiWeeklyStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	csvURL, err := biCpiWeeklyCSVURL()
	if err != nil {
		return 0, err
	}
	s.DataUrl = csvURL
	return s.ClickHouseImport.Import(ctx, conn)
}

// biCpiWeeklyCSVURL выполняет export-RPC и превращает относительный tmp-путь в публичный
// URL. Ошибка RPC не проглатывается: без CSV прогон обязан упасть до записи, а не
// отчитаться «0 строк».
func biCpiWeeklyCSVURL() (string, error) {
	body, err := util.PostFileBytes(biCpiWeeklyExportURL, []byte(biCpiWeeklyExportPayload), map[string]string{
		"Content-Type":      "text/x-gwt-rpc; charset=UTF-8",
		"X-GWT-Permutation": biCpiWeeklyPermutation,
		"X-GWT-Module-Base": biCpiWeeklyModuleBase,
		"Referer":           biCpiWeeklyDashboardURL,
	})
	if err != nil {
		return "", fmt.Errorf("%s: export RPC: %w", rosstatBICpiWeeklyTable, err)
	}
	path, err := biCpiWeeklyExportPath(body)
	if err != nil {
		return "", fmt.Errorf("%s: export RPC: %w", rosstatBICpiWeeklyTable, err)
	}
	csvURL, err := biCpiWeeklyPublicURL(path)
	if err != nil {
		return "", fmt.Errorf("%s: export RPC: %w", rosstatBICpiWeeklyTable, err)
	}
	return csvURL, nil
}

// biCpiWeeklyExportPath разбирает ответ GWT-RPC `//OK[1,["tmp/exp.../....csv"],0,7]`.
// Формат — JSON после префикса //OK; путь лежит вторым элементом массива. Не-//OK и
// пустой/чужой путь — ошибка: молча вернуть пустой URL значило бы скачать HTML заглушки.
func biCpiWeeklyExportPath(body []byte) (string, error) {
	trimmed := strings.TrimSpace(string(body))
	if !strings.HasPrefix(trimmed, "//OK") {
		return "", fmt.Errorf("ответ не //OK: %q", truncateForError(trimmed, 256))
	}
	var arr []json.RawMessage
	if err := json.Unmarshal([]byte(strings.TrimPrefix(trimmed, "//OK")), &arr); err != nil {
		return "", fmt.Errorf("разбор //OK: %w", err)
	}
	if len(arr) < 2 {
		return "", fmt.Errorf("в //OK нет второго элемента: %q", truncateForError(trimmed, 256))
	}
	var paths []string
	if err := json.Unmarshal(arr[1], &paths); err != nil {
		return "", fmt.Errorf("разбор пути: %w", err)
	}
	if len(paths) != 1 || strings.TrimSpace(paths[0]) == "" {
		return "", fmt.Errorf("ожидался один непустой путь, получено %v", paths)
	}
	path := strings.TrimSpace(paths[0])
	if !strings.HasPrefix(path, "tmp/exp") || !strings.HasSuffix(path, ".csv") {
		return "", fmt.Errorf("неожиданный путь экспорта %q", path)
	}
	if strings.Contains(path, "..") {
		return "", fmt.Errorf("путь экспорта содержит ..: %q", path)
	}
	return path, nil
}

// biCpiWeeklyPublicURL собирает публичный URL файла: путь из RPC относителен /biportal/ и
// содержит пробелы/кириллицу, поэтому url.URL.String() кодирует его целиком.
func biCpiWeeklyPublicURL(path string) (string, error) {
	path = strings.TrimPrefix(path, "/")
	if !strings.HasPrefix(path, "tmp/exp") || !strings.HasSuffix(path, ".csv") || strings.Contains(path, "..") {
		return "", fmt.Errorf("неожиданный путь экспорта %q", path)
	}
	base, err := url.Parse(biCpiWeeklyPublicBase + "/biportal/")
	if err != nil {
		return "", fmt.Errorf("базовый URL %q: %w", biCpiWeeklyPublicBase, err)
	}
	base.Path = strings.TrimSuffix(base.Path, "/") + "/" + path
	escaped := base.String()
	if !strings.HasPrefix(escaped, biCpiWeeklyPublicBase+"/biportal/tmp/exp") {
		return "", fmt.Errorf("не удалось собрать URL из %q", path)
	}
	return escaped, nil
}

// biCpiWeeklyImport разбирает CSV и пишет точки в batch. Разбор идёт целиком до первой
// записи: ошибка оставляет батч пустым, а не пишет половину выгрузки.
func biCpiWeeklyImport(body []byte, batch driver.Batch) error {
	points, err := biCpiWeeklyParseCSV(body)
	if err != nil {
		return fmt.Errorf("%s: %w", rosstatBICpiWeeklyTable, err)
	}
	for _, point := range points {
		if err := batch.Append(ipcWeeksBulletinsAggregateSeries, point.date, point.value); err != nil {
			return err
		}
	}
	return nil
}

// biCpiWeeklyParseCSV разбирает CSV BI. Выгрузка меняет локаль: английская даёт M/D/YY,
// русская — DD.MM.YY; порядок строк не гарантирован, поэтому результат сортируется и
// дубликаты схлопываются (последнее значение). Семь новогодних перерывов — поведение
// источника, а не ошибка разбора; проверяется только первая дата и непустота.
func biCpiWeeklyParseCSV(body []byte) ([]biCpiWeeklyPoint, error) {
	body = bytes.TrimPrefix(body, []byte("\ufeff"))
	reader := csv.NewReader(bytes.NewReader(body))
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("CSV: %w", err)
	}
	headerIdx, periodIdx, valueIdx, indicatorIdx, territoryIdx := -1, -1, -1, -1, -1
	for i, record := range records {
		for j, cell := range record {
			if strings.EqualFold(strings.TrimSpace(cell), "period") {
				headerIdx = i
				periodIdx = j
			}
			if strings.EqualFold(strings.TrimSpace(cell), "value") {
				valueIdx = j
			}
			if strings.EqualFold(strings.TrimSpace(cell), "id_indicator") {
				indicatorIdx = j
			}
			if strings.EqualFold(strings.TrimSpace(cell), "territory") {
				territoryIdx = j
			}
		}
		if headerIdx >= 0 && periodIdx >= 0 && valueIdx >= 0 {
			break
		}
	}
	if headerIdx < 0 || periodIdx < 0 || valueIdx < 0 {
		return nil, fmt.Errorf("в CSV нет шапки с period/value")
	}
	if indicatorIdx < 0 || territoryIdx < 0 {
		return nil, fmt.Errorf("в CSV нет шапки с id_indicator/territory")
	}
	minimumCols := indicatorIdx
	if territoryIdx > minimumCols {
		minimumCols = territoryIdx
	}
	minimumCols++
	points := make([]biCpiWeeklyPoint, 0, len(records)-headerIdx-1)
	for line, record := range records[headerIdx+1:] {
		if len(record) <= periodIdx || len(record) <= valueIdx {
			empty := true
			for _, cell := range record {
				if strings.TrimSpace(cell) != "" {
					empty = false
					break
				}
			}
			if empty {
				continue
			}
			return nil, fmt.Errorf("строка %d: ожидалось минимум %d колонок, получено %d", headerIdx+line+2, minimumCols, len(record))
		}
		period := strings.TrimSpace(record[periodIdx])
		valueRaw := strings.TrimSpace(record[valueIdx])
		if period == "" && valueRaw == "" {
			continue
		}
		if len(record) < minimumCols {
			return nil, fmt.Errorf("строка %d: ожидалось минимум %d колонок, получено %d", headerIdx+line+2, minimumCols, len(record))
		}
		if !biCpiWeeklyIsAggregate(record[indicatorIdx], record[territoryIdx]) {
			return nil, fmt.Errorf("строка %d: выгрузка не является агрегатом ИПЦ РФ: indicator=%q territory=%q",
				headerIdx+line+2, record[indicatorIdx], record[territoryIdx])
		}
		date, err := biCpiWeeklyDate(period)
		if err != nil {
			return nil, fmt.Errorf("строка %d: %w", headerIdx+line+2, err)
		}
		value, err := biCpiWeeklyValue(valueRaw)
		if err != nil {
			return nil, fmt.Errorf("строка %d: %w", headerIdx+line+2, err)
		}
		points = append(points, biCpiWeeklyPoint{date: date, value: value})
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("CSV не содержит точек")
	}
	sort.SliceStable(points, func(i, j int) bool { return points[i].date.Before(points[j].date) })
	deduped := points[:0]
	for _, point := range points {
		if n := len(deduped); n > 0 && deduped[n-1].date.Equal(point.date) {
			deduped[n-1] = point
			continue
		}
		deduped = append(deduped, point)
	}
	if got := deduped[0].date.Format("2006-01-02"); got != biCpiWeeklyMinDate {
		return nil, fmt.Errorf("первая дата %s, ожидалась %s", got, biCpiWeeklyMinDate)
	}
	if last := deduped[len(deduped)-1].date; last.Before(time.Now().AddDate(0, 0, -21)) {
		log.Warnf("%s: последняя дата %s старше 21 дня — источник мог не обновиться", rosstatBICpiWeeklyTable, last.Format("2006-01-02"))
	}
	log.Infof("%s: parsed %d points %s..%s", rosstatBICpiWeeklyTable, len(deduped),
		deduped[0].date.Format("2006-01-02"), deduped[len(deduped)-1].date.Format("2006-01-02"))
	return deduped, nil
}

// biCpiWeeklyIsAggregate принимает обе локали выгрузки BI: английскую и русскую.
// Несовпадение означает, что payload вернул другую карточку, и импорт положил бы
// чужой ряд в агрегатную строку.
func biCpiWeeklyIsAggregate(indicator, territory string) bool {
	ind := strings.ToLower(indicator)
	ter := strings.ToLower(territory)
	indOK := strings.Contains(ind, "consumer price index") || strings.Contains(ind, "потребительских цен")
	terOK := strings.Contains(ter, "russian federation") || strings.Contains(ter, "российская федерация")
	return indOK && terOK
}

// biCpiWeeklyDate принимает M/D/YY (английская выгрузка), DD.MM.YY (русская) и ISO.
func biCpiWeeklyDate(raw string) (time.Time, error) {
	value := strings.TrimSpace(raw)
	for _, layout := range []string{"02.01.06", "1/2/06", "2006-01-02"} {
		if date, err := time.Parse(layout, value); err == nil {
			return date, nil
		}
	}
	return time.Time{}, fmt.Errorf("дата %q не распознана", raw)
}

// biCpiWeeklyValue разбирает значение; запятая как десятичный разделитель принимается
// на случай русской локали. NaN/Inf точкой ряда быть не могут.
func biCpiWeeklyValue(raw string) (float64, error) {
	value, err := strconv.ParseFloat(strings.ReplaceAll(strings.TrimSpace(raw), ",", "."), 64)
	if err != nil {
		return 0, fmt.Errorf("значение %q не число: %w", raw, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("значение %q не конечное число", raw)
	}
	return value, nil
}

func biCpiWeeklySeriesMeta() []util.SeriesMeta {
	return []util.SeriesMeta{ipcWeeksAggregateMeta}
}

func truncateForError(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
