package moex

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// keyrateContract — паспорт серии расчётного фьючерса на Индекс ключевой ставки MOEX (KEYRATE).
type keyrateContract struct {
	secid      string
	seriesName string
	firstTrade time.Time
	// lastTrade — дата экспирации контракта (LSTDELDATE): она же дата заседания Совета директоров ЦБ,
	// по ставке которого рассчитывается индекс KEYRATE.
	lastTrade time.Time
}

// keyrateDay — дневной итог торгов одной серии. Цена дня — settle (SETTLEPRICE из блока history):
// LAST у ISS равен нулю в дни без сделок (живая проба 2026-10-13) и ценой быть не может.
type keyrateDay struct {
	date         time.Time
	close        float64
	settle       float64
	waprice      float64
	volume       uint64
	numtrades    uint64
	openposition uint64
}

const (
	// keyrateBoard — площадка срочного рынка, на которой торгуются серии KEYRATE.
	keyrateBoard = "RFUD"
	// keyrateSeriesPrefix — код серии начинается с KK; отсекает прочие инструменты срочного рынка.
	keyrateSeriesPrefix = "KK"
)

// keyrateSecidRe — допустимый код серии: заглавные латинские буквы и цифры, не короче 4 символов
// (secid приходит из внешнего JSON и подставляется в SQL-условие инкремента).
var keyrateSecidRe = regexp.MustCompile(`^[A-Z0-9]{4,}$`)

func validSecid(s string) bool {
	return keyrateSecidRe.MatchString(s)
}

// parseKeyrateSecids — коды серий из блока securities запроса /iss/securities.json?q=KEYRATE.
// Параметр assetcode= ISS игнорирует (отдаёт все бумаги), поэтому отбор — здесь: площадка RFUD,
// код серии с префиксом KK, is_traded == 1 (календарные спреды и индекс KEYRATE отсеиваются).
func parseKeyrateSecids(b issBlock) ([]string, error) {
	secidIdx := keyrateColumnIndex(b.Columns, "secid")
	boardIdx := keyrateColumnIndex(b.Columns, "primary_boardid")
	tradedIdx := keyrateColumnIndex(b.Columns, "is_traded")
	if secidIdx < 0 || boardIdx < 0 || tradedIdx < 0 {
		return nil, errNoColumns(b.Columns)
	}
	var secids []string
	for _, row := range b.Data {
		secid, err := rowString(row, secidIdx, "secid")
		if err != nil {
			return nil, err
		}
		board, err := rowString(row, boardIdx, "primary_boardid")
		if err != nil {
			return nil, err
		}
		traded, err := rowTraded(row, tradedIdx)
		if err != nil {
			return nil, err
		}
		if board != keyrateBoard || !traded || !strings.HasPrefix(secid, keyrateSeriesPrefix) {
			continue
		}
		secids = append(secids, secid)
	}
	return secids, nil
}

// parseKeyrateContract — паспорт серии из блока description (пары name/value) запроса
// /iss/securities/<secid>.json. Обязательны SHORTNAME, FRSTTRADE и LSTDELDATE: молчаливый zero
// time недопустим. Пустое значение LSTDELDATE откатывается на LSTTRADE.
func parseKeyrateContract(b issBlock) (keyrateContract, error) {
	nameIdx := keyrateColumnIndex(b.Columns, "name")
	valueIdx := keyrateColumnIndex(b.Columns, "value")
	if nameIdx < 0 || valueIdx < 0 {
		return keyrateContract{}, errNoColumns(b.Columns)
	}
	fields := make(map[string]string, len(b.Data))
	for _, row := range b.Data {
		name, err := rowString(row, nameIdx, "name")
		if err != nil {
			return keyrateContract{}, err
		}
		value, err := rowString(row, valueIdx, "value")
		if err != nil {
			return keyrateContract{}, err
		}
		fields[name] = value
	}
	seriesName := fields["SHORTNAME"]
	if seriesName == "" {
		return keyrateContract{}, fmt.Errorf("empty SHORTNAME in description of %s", fields["SECID"])
	}
	firstTrade, err := parseKeyrateDate("FRSTTRADE", fields["FRSTTRADE"])
	if err != nil {
		return keyrateContract{}, err
	}
	lastRaw, ok := fields["LSTDELDATE"]
	if !ok {
		return keyrateContract{}, fmt.Errorf("no LSTDELDATE in description of %s", fields["SECID"])
	}
	if lastRaw == "" {
		lastRaw = fields["LSTTRADE"]
	}
	lastTrade, err := parseKeyrateDate("LSTDELDATE", lastRaw)
	if err != nil {
		return keyrateContract{}, err
	}
	return keyrateContract{
		secid:      fields["SECID"],
		seriesName: seriesName,
		firstTrade: firstTrade,
		lastTrade:  lastTrade,
	}, nil
}

// parseKeyrateHistory — дневные итоги из блока history запроса /iss/history/.../securities/<secid>.json.
// Читаются только tradedate, close, settleprice, waprice, volume, numtrades, openposition:
// колонка LAST не читается ни здесь, ни в импортёре.
func parseKeyrateHistory(b issBlock) ([]keyrateDay, error) {
	idx := make(map[string]int, 7)
	for _, name := range []string{"tradedate", "close", "settleprice", "waprice", "volume", "numtrades", "openposition"} {
		idx[name] = keyrateColumnIndex(b.Columns, name)
		if idx[name] < 0 {
			return nil, errNoColumns(b.Columns)
		}
	}
	var days []keyrateDay
	for _, row := range b.Data {
		tradedate, err := rowString(row, idx["tradedate"], "tradedate")
		if err != nil {
			return nil, err
		}
		date, err := time.Parse(moexDate, tradedate)
		if err != nil {
			return nil, err
		}
		close_, err := rowFloat(row, idx["close"])
		if err != nil {
			return nil, err
		}
		settle, err := rowFloat(row, idx["settleprice"])
		if err != nil {
			return nil, err
		}
		waprice, err := rowFloat(row, idx["waprice"])
		if err != nil {
			return nil, err
		}
		volume, err := rowCount(row, idx["volume"], "volume")
		if err != nil {
			return nil, err
		}
		numtrades, err := rowCount(row, idx["numtrades"], "numtrades")
		if err != nil {
			return nil, err
		}
		openposition, err := rowCount(row, idx["openposition"], "openposition")
		if err != nil {
			return nil, err
		}
		days = append(days, keyrateDay{
			date:         date,
			close:        close_,
			settle:       settle,
			waprice:      waprice,
			volume:       volume,
			numtrades:    numtrades,
			openposition: openposition,
		})
	}
	return days, nil
}

// parseHistoryTotal — TOTAL из блока history.cursor: сколько дней отдаёт ISS по запросу целиком,
// при PAGESIZE меньше TOTAL нужны страницы (параметр start).
func parseHistoryTotal(b issBlock) (int, error) {
	idx := keyrateColumnIndex(b.Columns, "TOTAL")
	if idx < 0 {
		return 0, errNoColumns(b.Columns)
	}
	if len(b.Data) == 0 {
		return 0, nil
	}
	total, err := rowFloat(b.Data[0], idx)
	if err != nil {
		return 0, err
	}
	if total < 0 {
		return 0, errBadCell("TOTAL", b.Data[0][idx])
	}
	return int(total), nil
}

// keyrateColumnIndex — поиск колонки без учёта регистра имени. Регистр у ISS разный от блока к блоку:
// в securities/description имена строчные (secid, primary_boardid, is_traded, name, value), в блоке
// history — заглавные (TRADEDATE, CLOSE, SETTLEPRICE, WAPRICE, VOLUME, NUMTRADES, OPENPOSITION).
func keyrateColumnIndex(columns []string, name string) int {
	for i, c := range columns {
		if strings.EqualFold(c, name) {
			return i
		}
	}
	return -1
}

// rowString — строковая ячейка строки блока ISS.
func rowString(row []any, idx int, name string) (string, error) {
	if idx < 0 || len(row) <= idx {
		return "", fmt.Errorf("short row %v", row)
	}
	v, ok := row[idx].(string)
	if !ok {
		return "", errBadCell(name, row[idx])
	}
	return v, nil
}

// rowTraded — колонка is_traded: у календарных спредов ISS отдаёт null (сведений о торгах нет),
// такую бумагу считаем неторгуемой, а не ошибкой разбора.
func rowTraded(row []any, idx int) (bool, error) {
	if idx < 0 || len(row) <= idx {
		return false, fmt.Errorf("short row %v", row)
	}
	switch v := row[idx].(type) {
	case nil:
		return false, nil
	case float64:
		return v == 1, nil
	default:
		return false, errBadCell("is_traded", row[idx])
	}
}

// rowCount — целочисленная колонка (объём, число сделок, открытые позиции): ISS отдаёт их числами,
// значение проверяется на неотрицательность перед приведением к uint64.
func rowCount(row []any, idx int, name string) (uint64, error) {
	v, err := rowFloat(row, idx)
	if err != nil {
		return 0, err
	}
	if v < 0 {
		return 0, errBadCell(name, row[idx])
	}
	return uint64(v), nil
}

// parseKeyrateDate — дата в формате ISS (moexDate); пустое значение — ошибка: zero time в паспорте
// серии означал бы «дата заседания неизвестна», и импортёр записал бы её молча.
func parseKeyrateDate(name, value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("empty %s in description", name)
	}
	date, err := time.Parse(moexDate, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: %w", name, err)
	}
	return date, nil
}
