package moex

import (
	"context"
	"fmt"
	"time"
)

// Транспорт серий расчётного фьючерса на Индекс ключевой ставки MOEX (KEYRATE):
// обнаружение серий, паспорт серии, дневные итоги с пагинацией по start.
const (
	keyrateSearchPath  = "/iss/securities.json?q=KEYRATE&iss.meta=off"
	keyrateDescPath    = "/iss/securities/%s.json?iss.meta=off"
	keyrateHistoryPath = "/iss/history/engines/futures/markets/forts/securities/%s.json"
	// блоки ответов ISS, которые читает транспорт
	keyrateSecuritiesBlock  = "securities"
	keyrateDescriptionBlock = "description"
	// история и её курсор приходят одним ответом — оба блока берёт один getBlocks
	keyrateHistoryBlock = "history"
	keyrateCursorBlock  = "history.cursor"
)

// fetchKeyrateSecids — коды торгуемых серий KEYRATE из поиска по бумагам ISS.
// Список не хардкодится: серии добавляются и истекают каждый квартал.
func fetchKeyrateSecids(ctx context.Context) ([]string, error) {
	block, err := getBlock(ctx, keyrateSearchPath, keyrateSecuritiesBlock)
	if err != nil {
		return nil, err
	}
	return parseKeyrateSecids(block)
}

// fetchKeyrateContract — паспорт серии: короткое имя, начало обращения и дата экспирации
// (= дата заседания Совета директоров ЦБ, по ставке которого рассчитывается индекс).
func fetchKeyrateContract(ctx context.Context, secid string) (keyrateContract, error) {
	path := fmt.Sprintf(keyrateDescPath, secid)
	block, err := getBlock(ctx, path, keyrateDescriptionBlock)
	if err != nil {
		return keyrateContract{}, err
	}
	contract, err := parseKeyrateContract(block)
	if err != nil {
		return keyrateContract{}, fmt.Errorf("%s: %w", path, err)
	}
	// secid берётся из запроса: описание его содержит, но идентификатор серии не должен
	// зависеть от тела ответа (и подставляется в SQL-условие инкремента в импортёре).
	contract.secid = secid
	return contract, nil
}

// fetchKeyrateHistory — дневные итоги серии с даты from включительно.
// ISS отдаёт страницы по moexPageLimit строк (PAGESIZE параметром не понижается), поэтому цикл
// идёт шагом moexPageLimit и завершается, когда страница пуста или собраны все дни по TOTAL
// из блока history.cursor.
func fetchKeyrateHistory(ctx context.Context, secid string, from time.Time) ([]keyrateDay, error) {
	path := fmt.Sprintf(keyrateHistoryPath, secid)
	var days []keyrateDay
	for start := 0; ; start += moexPageLimit {
		blocks, err := getBlocks(ctx, fmt.Sprintf("%s?iss.meta=off&from=%s&start=%d", path, from.Format(moexDate), start))
		if err != nil {
			return nil, err
		}
		page, err := parseKeyrateHistory(blocks[keyrateHistoryBlock])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if len(page) == 0 {
			break
		}
		days = append(days, page...)
		total, err := parseHistoryTotal(blocks[keyrateCursorBlock])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if len(days) >= total {
			break
		}
	}
	return days, nil
}
