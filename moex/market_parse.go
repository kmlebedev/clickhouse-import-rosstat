package moex

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// marketSecidRe — допустимый код контракта: заглавные латинские буквы, цифры
// и подчёркивание (исторические серии BRZ9_2009). Код подставляется в путь
// запроса ISS, поэтому всё остальное отсекается до сети.
var marketSecidRe = regexp.MustCompile(`^[A-Z0-9_]{4,}$`)

func validMarketSecid(s string) bool {
	return marketSecidRe.MatchString(s)
}

// contractMonth — месяц поставки фьючерсного контракта; по нему выбирается
// фронт-месяц: контракт с минимальным (year, month) среди торгуемых в дату.
type contractMonth struct {
	year  int
	month time.Month
}

func (m contractMonth) before(other contractMonth) bool {
	if m.year != other.year {
		return m.year < other.year
	}
	return m.month < other.month
}

// parseContractMonth разбирает SHORTNAME вида "BR-10.09" или "COPPER-12.24"
// в месяц поставки. Двузначный год трактуется как 2000+YY: история BR/COPPER
// начинается в 2009/2024, контрактов XX века в источнике нет.
func parseContractMonth(shortname, asset string) (contractMonth, error) {
	prefix := asset + "-"
	if !strings.HasPrefix(shortname, prefix) {
		return contractMonth{}, fmt.Errorf("shortname %q does not start with %q", shortname, prefix)
	}
	rest := strings.TrimPrefix(shortname, prefix)
	parts := strings.Split(rest, ".")
	if len(parts) != 2 {
		return contractMonth{}, fmt.Errorf("bad contract month %q", shortname)
	}
	month, err := strconv.Atoi(parts[0])
	if err != nil || month < 1 || month > 12 {
		return contractMonth{}, fmt.Errorf("bad contract month %q", shortname)
	}
	yy, err := strconv.Atoi(parts[1])
	if err != nil || yy < 0 || yy > 99 {
		return contractMonth{}, fmt.Errorf("bad contract year %q", shortname)
	}
	return contractMonth{year: 2000 + yy, month: time.Month(month)}, nil
}

// futuresBar — дневная строка контракта: расчётная цена дня и закрытие
// (у дальних контрактов 2009–2010 CLOSE бывает null, а SETTLEPRICE заполнен).
type futuresBar struct {
	secid        string
	month        contractMonth
	date         time.Time
	settle       float64
	close        float64
	volume       uint64
	openPosition uint64
}

func (b futuresBar) value() float64 {
	if b.settle > 0 {
		return b.settle
	}
	if b.close > 0 {
		return b.close
	}
	return 0
}

type marketPoint struct {
	date  time.Time
	value float64
}

func rowFloatOrZero(row []any, idx int) (float64, error) {
	if idx < 0 || len(row) <= idx {
		return 0, fmt.Errorf("short row %v", row)
	}
	if row[idx] == nil {
		return 0, nil
	}
	v, ok := row[idx].(float64)
	if !ok {
		return 0, fmt.Errorf("not a number: %v", row[idx])
	}
	return v, nil
}

func rowUint(row []any, idx int) (uint64, error) {
	if idx < 0 || len(row) <= idx {
		return 0, fmt.Errorf("short row %v", row)
	}
	if row[idx] == nil {
		return 0, nil
	}
	v, ok := row[idx].(float64)
	if !ok {
		return 0, fmt.Errorf("not a number: %v", row[idx])
	}
	if v < 0 {
		return 0, fmt.Errorf("negative number: %v", row[idx])
	}
	return uint64(v), nil
}

// rowStringOrEmpty — строка или пустая для nil: ASSETCODE спредов и служебных
// инструментов ISS отдаёт null, и это не ошибка разбора.
func rowStringOrEmpty(row []any, idx int) (string, error) {
	if idx < 0 || len(row) <= idx {
		return "", fmt.Errorf("short row %v", row)
	}
	if row[idx] == nil {
		return "", nil
	}
	s, ok := row[idx].(string)
	if !ok {
		return "", fmt.Errorf("not a string: %v", row[idx])
	}
	return s, nil
}

// parseFuturesHistory разбирает блок history одного контракта. Календарные
// спреды приходят с пустым ASSETCODE и отбрасываются до разбора SHORTNAME.
func parseFuturesHistory(block issBlock, asset string) ([]futuresBar, error) {
	idx := map[string]int{}
	for _, name := range []string{
		"TRADEDATE", "SECID", "CLOSE", "SETTLEPRICE", "VOLUME", "OPENPOSITION", "SHORTNAME", "ASSETCODE",
	} {
		idx[name] = columnIndex(block.Columns, name)
		if idx[name] < 0 {
			return nil, fmt.Errorf("no column %q in %v", name, block.Columns)
		}
	}
	var bars []futuresBar
	for _, row := range block.Data {
		code, err := rowStringOrEmpty(row, idx["ASSETCODE"])
		if err != nil {
			return nil, err
		}
		if code != asset {
			continue
		}
		secid, err := rowString(row, idx["SECID"], "SECID")
		if err != nil || secid == "" {
			return nil, fmt.Errorf("%s: bad SECID %v", asset, row)
		}
		shortname, err := rowString(row, idx["SHORTNAME"], "SHORTNAME")
		if err != nil {
			return nil, err
		}
		month, err := parseContractMonth(shortname, asset)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", secid, err)
		}
		tradedate, err := rowString(row, idx["TRADEDATE"], "TRADEDATE")
		if err != nil {
			return nil, err
		}
		date, err := time.Parse(moexDate, tradedate)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", secid, err)
		}
		settle, err := rowFloatOrZero(row, idx["SETTLEPRICE"])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", secid, err)
		}
		close_, err := rowFloatOrZero(row, idx["CLOSE"])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", secid, err)
		}
		volume, err := rowUint(row, idx["VOLUME"])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", secid, err)
		}
		openPosition, err := rowUint(row, idx["OPENPOSITION"])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", secid, err)
		}
		bars = append(bars, futuresBar{
			secid:        secid,
			month:        month,
			date:         date,
			settle:       settle,
			close:        close_,
			volume:       volume,
			openPosition: openPosition,
		})
	}
	return bars, nil
}

// parseFuturesSecids — коды контрактов выбранного актива из блока history
// или securities. Календарные спреды (пустой ASSETCODE), опционы и прочие
// инструменты с чужим SHORTNAME отбрасываются.
func parseFuturesSecids(block issBlock, asset string) ([]string, error) {
	secidIdx := columnIndex(block.Columns, "SECID")
	assetIdx := columnIndex(block.Columns, "ASSETCODE")
	shortIdx := columnIndex(block.Columns, "SHORTNAME")
	if secidIdx < 0 || assetIdx < 0 || shortIdx < 0 {
		return nil, fmt.Errorf("no columns SECID/ASSETCODE/SHORTNAME in %v", block.Columns)
	}
	seen := make(map[string]bool, len(block.Data))
	var secids []string
	for _, row := range block.Data {
		code, err := rowStringOrEmpty(row, assetIdx)
		if err != nil {
			return nil, err
		}
		if code != asset {
			continue
		}
		shortname, err := rowString(row, shortIdx, "SHORTNAME")
		if err != nil {
			return nil, err
		}
		if _, err = parseContractMonth(shortname, asset); err != nil {
			continue // опцион или календарный спред — не контракт
		}
		secid, err := rowString(row, secidIdx, "SECID")
		if err != nil || secid == "" {
			return nil, fmt.Errorf("%s: bad SECID %v", asset, row)
		}
		if seen[secid] {
			continue
		}
		seen[secid] = true
		secids = append(secids, secid)
	}
	return secids, nil
}

// frontMonthPoints — непрерывный фронт-месяц: на каждую дату берётся контракт
// с минимальным месяцем поставки, у которого есть валидная цена (SETTLEPRICE,
// иначе CLOSE). Точки сортируются по дате; нулевые цены пропускаются.
func frontMonthPoints(bars []futuresBar) []marketPoint {
	byDate := make(map[time.Time]futuresBar, len(bars))
	for _, b := range bars {
		if b.value() <= 0 {
			continue
		}
		current, ok := byDate[b.date]
		if !ok || b.month.before(current.month) {
			byDate[b.date] = b
		}
	}
	points := make([]marketPoint, 0, len(byDate))
	for date, bar := range byDate {
		points = append(points, marketPoint{date: date, value: bar.value()})
	}
	sort.Slice(points, func(i, j int) bool { return points[i].date.Before(points[j].date) })
	return points
}
