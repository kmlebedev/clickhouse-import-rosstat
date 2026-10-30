package ppi

import (
	"bytes"
	_ "embed"
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed data/okpd2-kipc-2026-v2.csv
var ppiMappingCSV []byte

//go:embed data/kipc-weights-panel.csv
var ppiWeightsCSV []byte

// rawPoint — точка сырого ИЦП: дата (последний день отчётного месяца), код и
// значение m/m. Для rosstat_ppi_ved Code — совокупный код, для product — ОКПД2.
type rawPoint struct {
	Date  time.Time
	Code  string
	Value float64
}

// monthlyValue — точка производного ряда: дата и значение (m/m или уровень).
type monthlyValue struct {
	Date  time.Time
	Value float64
}

// ppiSeries — пять рядов слоя: total, food, nonfood, all, core.
type ppiSeries map[string][]monthlyValue

// ppiMapping — версия mapping и mapped-листья по группам КИПЦ.
type ppiMapping struct {
	Version       string
	LeavesByGroup map[string][]string
}

// ppiWeights — панель весов по периодам; primary читает период 2026.
type ppiWeights struct {
	ByPeriod map[string]map[string]float64
	Names    map[string]string
}

// parseMapping разбирает versioned CSV mapping. Листья — только is_leaf=1 и
// status=mapped; каждый лист обязан иметь группу, а группа — встречаться один раз.
func parseMapping(data []byte) (*ppiMapping, error) {
	r := csv.NewReader(bytes.NewReader(data))
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("mapping: чтение шапки: %w", err)
	}
	idx := make(map[string]int, len(header))
	for i, name := range header {
		idx[strings.TrimSpace(name)] = i
	}
	for _, col := range []string{"taxonomy_version", "okpd2", "is_leaf", "kipc_group", "status"} {
		if _, ok := idx[col]; !ok {
			return nil, fmt.Errorf("mapping: нет колонки %q", col)
		}
	}

	m := &ppiMapping{LeavesByGroup: map[string][]string{}}
	seen := map[string]string{}
	for line := 2; ; line++ {
		record, readErr := r.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("mapping: строка %d: %w", line, readErr)
		}
		version := strings.TrimSpace(record[idx["taxonomy_version"]])
		if version == "" {
			return nil, fmt.Errorf("mapping: строка %d: пустой taxonomy_version", line)
		}
		if m.Version == "" {
			m.Version = version
		} else if version != m.Version {
			return nil, fmt.Errorf("mapping: строка %d: taxonomy_version %q, ожидался %q", line, version, m.Version)
		}
		if strings.TrimSpace(record[idx["is_leaf"]]) != "1" {
			continue
		}
		if strings.TrimSpace(record[idx["status"]]) != "mapped" {
			continue
		}
		code := strings.TrimSpace(record[idx["okpd2"]])
		group := strings.TrimSpace(record[idx["kipc_group"]])
		if code == "" || group == "" {
			return nil, fmt.Errorf("mapping: строка %d: пустой okpd2 или kipc_group", line)
		}
		if prev, ok := seen[code]; ok {
			return nil, fmt.Errorf("mapping: код %s встречается дважды (группы %s и %s)", code, prev, group)
		}
		seen[code] = group
		m.LeavesByGroup[group] = append(m.LeavesByGroup[group], code)
	}
	if m.Version == "" || len(m.LeavesByGroup) == 0 {
		return nil, fmt.Errorf("mapping: пустой набор mapped-листьев")
	}
	for group, codes := range m.LeavesByGroup {
		sort.Strings(codes)
		m.LeavesByGroup[group] = codes
	}
	return m, nil
}

// parseWeights разбирает полную панель весов. Дубликат (period, code) — ошибка:
// два разных веса одного ключа сделали бы выбор веса недетерминированным.
func parseWeights(data []byte) (*ppiWeights, error) {
	r := csv.NewReader(bytes.NewReader(data))
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("weights: чтение шапки: %w", err)
	}
	idx := make(map[string]int, len(header))
	for i, name := range header {
		idx[strings.TrimSpace(name)] = i
	}
	for _, col := range []string{"period", "code", "name", "weight"} {
		if _, ok := idx[col]; !ok {
			return nil, fmt.Errorf("weights: нет колонки %q", col)
		}
	}

	w := &ppiWeights{ByPeriod: map[string]map[string]float64{}, Names: map[string]string{}}
	for line := 2; ; line++ {
		record, readErr := r.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("weights: строка %d: %w", line, readErr)
		}
		period := strings.TrimSpace(record[idx["period"]])
		code := strings.TrimSpace(record[idx["code"]])
		if period == "" || code == "" {
			return nil, fmt.Errorf("weights: строка %d: пустой period или code", line)
		}
		value, parseErr := strconv.ParseFloat(strings.TrimSpace(record[idx["weight"]]), 64)
		if parseErr != nil {
			return nil, fmt.Errorf("weights: строка %d: вес %q: %w", line, record[idx["weight"]], parseErr)
		}
		if value < 0 {
			return nil, fmt.Errorf("weights: строка %d: отрицательный вес %v", line, value)
		}
		if w.ByPeriod[period] == nil {
			w.ByPeriod[period] = map[string]float64{}
		}
		if _, ok := w.ByPeriod[period][code]; ok {
			return nil, fmt.Errorf("weights: строка %d: дубль (%s, %s)", line, period, code)
		}
		w.ByPeriod[period][code] = value
		w.Names[code] = strings.TrimSpace(record[idx["name"]])
	}
	if len(w.ByPeriod) == 0 {
		return nil, fmt.Errorf("weights: пустая панель")
	}
	return w, nil
}

// checkWeights требует вес primary-периода у каждой mapped-группы: пропуск
// означал бы молчаливое выпадение товаров из агрегата.
func checkWeights(m *ppiMapping, w *ppiWeights) error {
	primary := w.ByPeriod[ppiSAWeightPeriod]
	if primary == nil {
		return fmt.Errorf("weights: нет периода %s", ppiSAWeightPeriod)
	}
	missing := make([]string, 0)
	for group := range m.LeavesByGroup {
		if _, ok := primary[group]; !ok {
			missing = append(missing, group)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		return fmt.Errorf("weights: у групп %s нет веса %s", strings.Join(missing, ", "), ppiSAWeightPeriod)
	}
	return nil
}

// ppiScopeGroups — scope-группы mapping v2. Копия констант
// scripts/ppi_sa_mapping.py; изменение — только с новой taxonomy_version.
var ppiScopeGroups = map[string][]string{
	"food": {
		"01.1.1", "01.1.2", "01.1.3", "01.1.4", "01.1.5", "01.1.8", "01.1.9", "01.2",
	},
	"alcohol": {"02.1"},
	"tobacco": {"02.3"},
	"nonfood": {
		"04.3.1.1", "03.1", "03.2",
		"05.1", "05.2", "05.3", "05.4", "05.5", "05.6",
		"06.1", "07.1", "07.2.1", "08.1",
		"09.1", "09.2", "09.3", "09.7", "13.1.2", "13.2",
	},
	"fuel":      {"07.2.2"},
	"utilities": {"04.4", "04.5"},
}

// ppiSubtractGroups — услуги, вычитаемые из товарных групп (v2).
var ppiSubtractGroups = map[string][]string{
	"03.1": {"03.1.4"},
	"03.2": {"03.2.2"},
	"05.6": {"05.6.2"},
}

// isKIPCChild — иерархия КИПЦ только по точке: 04.3.1.1.1 и 04.3.1.1.10 — соседи,
// а не родитель/ребёнок (урок v2).
func isKIPCChild(code, parent string) bool {
	return strings.HasPrefix(code, parent+".")
}

// ppiAllGroups — широкий потребительский набор: food + alcohol + tobacco +
// nonfood + fuel. utilities в all не входят.
func ppiAllGroups() []string {
	groups := make([]string, 0, 64)
	for _, scope := range []string{"food", "alcohol", "tobacco", "nonfood", "fuel"} {
		groups = append(groups, ppiScopeGroups[scope]...)
	}
	return groups
}

// ppiCoreGroups — food + nonfood, без алкоголя, табака и топлива.
func ppiCoreGroups() []string {
	groups := make([]string, 0, 32)
	groups = append(groups, ppiScopeGroups["food"]...)
	groups = append(groups, ppiScopeGroups["nonfood"]...)
	return groups
}

// membersForGroups выбирает группы под scope и убирает дочерние, если выбрана
// родительская (v2 _dedupe_members): иначе листья посчитались бы дважды.
func membersForGroups(leaves map[string][]string, scope []string) map[string][]string {
	selected := make(map[string][]string)
	for group, codes := range leaves {
		for _, root := range scope {
			if group == root || isKIPCChild(group, root) {
				selected[group] = codes
				break
			}
		}
	}
	result := make(map[string][]string, len(selected))
	for group, codes := range selected {
		child := false
		for other := range selected {
			if other != group && isKIPCChild(group, other) {
				child = true
				break
			}
		}
		if !child {
			result[group] = codes
		}
	}
	return result
}

// aggregateScope считает m/m scope: среднее m/m внутри группы, затем взвешенное
// среднее групп с нормировкой на доступные в месяце веса.
func aggregateScope(members map[string][]string, weights map[string]float64, points map[string]map[time.Time]float64) ([]monthlyValue, error) {
	type groupPoint struct {
		date time.Time
		mean float64
	}
	perGroup := make(map[string][]groupPoint, len(members))
	netWeights := make(map[string]float64, len(members))
	for group, codes := range members {
		weight, ok := weights[group]
		if !ok {
			return nil, fmt.Errorf("нет веса %s для группы %s", ppiSAWeightPeriod, group)
		}
		for _, sub := range ppiSubtractGroups[group] {
			subWeight, subOK := weights[sub]
			if !subOK {
				return nil, fmt.Errorf("нет веса %s для вычитаемой группы %s", ppiSAWeightPeriod, sub)
			}
			weight -= subWeight
		}
		if weight < 0 {
			return nil, fmt.Errorf("отрицательный вес группы %s после вычитания услуг", group)
		}
		if weight == 0 {
			continue
		}
		netWeights[group] = weight
		byDate := make(map[time.Time][]float64)
		for _, code := range codes {
			for date, value := range points[code] {
				byDate[date] = append(byDate[date], value)
			}
		}
		dates := make([]time.Time, 0, len(byDate))
		for date := range byDate {
			dates = append(dates, date)
		}
		sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })
		for _, date := range dates {
			values := byDate[date]
			sum := 0.0
			for _, value := range values {
				sum += value
			}
			perGroup[group] = append(perGroup[group], groupPoint{date: date, mean: sum / float64(len(values))})
		}
	}

	type weighted struct {
		weight float64
		value  float64
	}
	buckets := make(map[time.Time][]weighted)
	for group, rows := range perGroup {
		weight := netWeights[group]
		for _, row := range rows {
			buckets[row.date] = append(buckets[row.date], weighted{weight: weight, value: row.mean})
		}
	}
	if len(buckets) == 0 {
		return nil, fmt.Errorf("пустой агрегат")
	}
	dates := make([]time.Time, 0, len(buckets))
	for date := range buckets {
		dates = append(dates, date)
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })

	result := make([]monthlyValue, 0, len(dates))
	for _, date := range dates {
		totalWeight := 0.0
		sum := 0.0
		for _, item := range buckets[date] {
			totalWeight += item.weight
			sum += item.weight * item.value
		}
		if totalWeight == 0 {
			continue
		}
		result = append(result, monthlyValue{Date: date, Value: sum / totalWeight})
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("пустой агрегат")
	}
	return result, nil
}

// chainSeries строит уровень от базы 100 в первом месяце start. m/m первого
// месяца не применяется (как в research); пропуск месяца — ошибка.
func chainSeries(points []monthlyValue, start time.Time) ([]monthlyValue, error) {
	sorted := append([]monthlyValue(nil), points...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Date.Before(sorted[j].Date) })

	result := make([]monthlyValue, 0, len(sorted))
	level := 100.0
	var prev time.Time
	for _, point := range sorted {
		if monthIndex(point.Date) < monthIndex(start) {
			continue
		}
		if point.Value <= 0 {
			return nil, fmt.Errorf("неположительный m/m %v на %s", point.Value, point.Date.Format("2006-01-02"))
		}
		if len(result) == 0 {
			if monthIndex(point.Date) != monthIndex(start) {
				return nil, fmt.Errorf("ряд начинается %s, ожидался %s", point.Date.Format("2006-01"), start.Format("2006-01"))
			}
			prev = point.Date
			result = append(result, monthlyValue{Date: normalizeDate(point.Date), Value: level})
			continue
		}
		if monthIndex(point.Date) != monthIndex(prev)+1 {
			return nil, fmt.Errorf("пропуск месяца после %s: следующая точка %s", prev.Format("2006-01"), point.Date.Format("2006-01"))
		}
		level *= point.Value / 100.0
		prev = point.Date
		result = append(result, monthlyValue{Date: normalizeDate(point.Date), Value: level})
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("пустой ряд")
	}
	return result, nil
}

// buildSeries собирает пять уровней из сырых m/m: total из ВЭД, потребительские
// из KIPC-агрегатов product. Входы уже прочитаны с FINAL.
func buildSeries(ved, product []rawPoint, m *ppiMapping, w *ppiWeights) (ppiSeries, error) {
	weights := w.ByPeriod[ppiSAWeightPeriod]
	if weights == nil {
		return nil, fmt.Errorf("нет периода весов %s", ppiSAWeightPeriod)
	}

	totalPoints := make([]monthlyValue, 0, len(ved))
	for _, point := range ved {
		if point.Code != ppiSATotalCode {
			continue
		}
		totalPoints = append(totalPoints, monthlyValue{Date: normalizeDate(point.Date), Value: point.Value})
	}
	total, err := chainSeries(totalPoints, ppiSATotalStart)
	if err != nil {
		return nil, fmt.Errorf("total: %w", err)
	}

	points := make(map[string]map[time.Time]float64, len(product))
	for _, point := range product {
		if points[point.Code] == nil {
			points[point.Code] = map[time.Time]float64{}
		}
		date := normalizeDate(point.Date)
		if _, exists := points[point.Code][date]; exists {
			return nil, fmt.Errorf("product: дубль %s/%s после FINAL", point.Code, date.Format("2006-01"))
		}
		points[point.Code][date] = point.Value
	}

	scopes := []struct {
		name   string
		groups []string
	}{
		{"food", ppiScopeGroups["food"]},
		{"nonfood", ppiScopeGroups["nonfood"]},
		{"all", ppiAllGroups()},
		{"core", ppiCoreGroups()},
	}
	result := ppiSeries{"total": total}
	for _, scope := range scopes {
		members := membersForGroups(m.LeavesByGroup, scope.groups)
		aggregate, aggErr := aggregateScope(members, weights, points)
		if aggErr != nil {
			return nil, fmt.Errorf("%s: %w", scope.name, aggErr)
		}
		chained, chainErr := chainSeries(aggregate, ppiSAConsumerStart)
		if chainErr != nil {
			return nil, fmt.Errorf("%s: %w", scope.name, chainErr)
		}
		result[scope.name] = chained
	}
	return result, nil
}

// monthIndex — порядковый номер месяца; дата внутри месяца не важна.
func monthIndex(date time.Time) int {
	return date.Year()*12 + int(date.Month()) - 1
}

// normalizeDate убирает время и локацию: ключи map и CSV должны совпадать
// независимо от того, в какой зоне драйвер вернул Date.
func normalizeDate(date time.Time) time.Time {
	return time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
}
