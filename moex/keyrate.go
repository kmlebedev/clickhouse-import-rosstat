package moex

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
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
// Код серии приходит из тела ответа и подставляется в URL и в SQL-условие инкремента
// импортёра, поэтому на выходе остаются только коды, проходящие validSecid.
func fetchKeyrateSecids(ctx context.Context) ([]string, error) {
	block, err := getBlock(ctx, keyrateSearchPath, keyrateSecuritiesBlock)
	if err != nil {
		return nil, err
	}
	secids, err := parseKeyrateSecids(block)
	if err != nil {
		return nil, err
	}
	valid := make([]string, 0, len(secids))
	for _, secid := range secids {
		if !validSecid(secid) {
			log.Warnf("keyrate: серия %q отброшена: код не проходит проверку validSecid", secid)
			continue
		}
		valid = append(valid, secid)
	}
	return valid, nil
}

// fetchKeyrateContract — паспорт серии: короткое имя, начало обращения и дата экспирации
// (= дата заседания Совета директоров ЦБ, по ставке которого рассчитывается индекс).
func fetchKeyrateContract(ctx context.Context, secid string) (keyrateContract, error) {
	if !validSecid(secid) {
		return keyrateContract{}, fmt.Errorf("keyrate: недопустимый код серии %q", secid)
	}
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
	if !validSecid(secid) {
		return nil, fmt.Errorf("keyrate: недопустимый код серии %q", secid)
	}
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

// Импортёр keyrate_futures: дневные итоги торгов расчётного фьючерса на Индекс ключевой
// ставки МосБиржи (MOEX KEYRATE) по всем торгуемым сериям, паспорт каждой серии
// (дата экспирации = дата заседания Совета директоров ЦБ) и витрина вменённых ожиданий
// рынка по ставке. Цена серии — расчётная цена дня SETTLEPRICE в пунктах, равных п.п. ставки.
type keyrateFutures struct{}

const (
	keyrateTable          = "keyrate_futures"
	keyrateContractTable  = "keyrate_contract"
	keyrateInsert         = "INSERT INTO " + keyrateTable
	keyrateContractInsert = "INSERT INTO " + keyrateContractTable
	// cbrKeyRateTable — ряд фактической ставки (импортёр cbr, Name() = cbr_key_rate):
	// витрина считает вменённое изменение к последней известной ставке из него.
	cbrKeyRateTable = "cbr_key_rate"

	keyrateDdl = `CREATE TABLE IF NOT EXISTS keyrate_futures (
      secid LowCardinality(String)
    , date Date
    , close Float64
    , settle Float64
    , waprice Float64
    , volume UInt64
    , numtrades UInt64
    , openposition UInt64
) ENGINE = ReplacingMergeTree ORDER BY (secid, date);`

	keyrateContractDdl = `CREATE TABLE IF NOT EXISTS keyrate_contract (
      secid LowCardinality(String)
    , series_name String
    , first_trade Date
    , last_trade Date
    , updated_at DateTime
) ENGINE = ReplacingMergeTree ORDER BY (secid);`
)

var keyrateView = util.View{
	Name:   "v_keyrate_expectations",
	Tables: []string{keyrateTable, keyrateContractTable, cbrKeyRateTable},
	// Спот — скалярный подзапрос, а не CROSS JOIN: на одну связь меньше и нет зависимости от
	// семантики CROSS JOIN. Алиас обязан стоять ДО FINAL (`AS f FINAL`): форма
	// `keyrate_futures FINAL f` — SYNTAX_ERROR ClickHouse 26.10.
	// nullIf(…, 0) — страховка от пустой cbr_key_rate: argMax по пустой таблице отдаёт 0,
	// а не NULL, и без обёртки вменённое изменение выродилось бы в settle × 100 б.п.
	Select: `SELECT f.date AS date, f.secid AS secid, c.series_name AS series_name,
       c.last_trade AS decision_date, f.settle AS settle, f.close AS close,
       f.volume AS volume, f.openposition AS openposition,
       round(f.settle - nullIf((SELECT argMax(rate, date) FROM cbr_key_rate FINAL), 0), 4) AS expected_change_pp,
       round((f.settle - nullIf((SELECT argMax(rate, date) FROM cbr_key_rate FINAL), 0)) * 100, 1) AS expected_change_bp
FROM keyrate_futures AS f FINAL
INNER JOIN keyrate_contract AS c FINAL ON f.secid = c.secid`,
	Comment: "Вменённый рынком уровень ключевой ставки на дату заседания Совета директоров ЦБ по " +
		"расчётному фьючерсу MOEX KEYRATE (серии KKV6, KKZ6, KKG7, ...): settle — расчётная цена дня " +
		"(SETTLEPRICE), в п.п. ставки, decision_date — дата экспирации серии, она же дата заседания. " +
		"Инструмент низколиквиден (открытые позиции — десятки контрактов), поэтому значение " +
		"индикативно: это НЕ прогноз рынка и НЕ цена обязательства. LAST не используется: в дни без " +
		"сделок ISS отдаёт по нему ноль, ожидания читать по settle. expected_change_pp/bp равны NULL, " +
		"пока cbr_key_rate пуста (ставка ещё не загружена) — это «неизвестно», а не нулевое изменение. " +
		"Описания серий — в v_series_catalog по source='moex'",
	Columns: map[string]string{
		"date":          "Торговый день (Date)",
		"secid":         "Код серии фьючерса KEYRATE (KKV6, KKZ6, KKG7, ...): 1 п.п. ставки = 1 000 ₽ (шаг цены 0,01 п.п. = 10 ₽)",
		"series_name":   "Короткое имя серии по ISS (SHORTNAME), например KEYRATE-10.26 — вменённая ставка на заседание 10.2026",
		"decision_date": "Дата экспирации серии (LSTDELDATE); она же дата заседания Совета директоров ЦБ, по объявленной на нём ставке рассчитывается индекс KEYRATE",
		"settle":        "Расчётная цена дня (SETTLEPRICE), п.п. ставки: вменённый рынком уровень ключевой ставки на дату заседания decision_date",
		"close":         "Цена закрытия дня (CLOSE), п.п.; при отсутствии сделок не отражает ожиданий — читать settle",
		"volume":        "Количество контрактов в сделках за день, шт.; 0 означает, что сделок не было и settle серии за этот день — не рыночное ожидание",
		"openposition":  "Открытые позиции на конец дня, контрактов (индикатор ликвидности серии)",
		"expected_change_pp": "Вменённое изменение ключевой ставки к дате заседания: settle − последняя известная ставка cbr_key_rate, п.п.; " +
			"плюс — рынок ждёт повышения, минус — понижения. NULL означает, что ставка ещё не загружена (cbr_key_rate пуста): " +
			"вменённое значение неизвестно, нулём его считать нельзя",
		"expected_change_bp": "То же в базисных пунктах: (settle − последняя известная ставка cbr_key_rate) × 100. " +
			"Считается к последней ИЗВЕСТНОЙ ставке, а не к ставке на дату decision_date; при нулевом объёме серии значение " +
			"нерепрезентативно, а NULL (cbr_key_rate пуста, ставка ещё не загружена) значит «неизвестно», а не нулевое изменение. " +
			"Вменённое рынком значение, а не прогноз и не ценовое обязательство",
	},
}

func (s *keyrateFutures) Name() string {
	return keyrateTable
}

func (s *keyrateFutures) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	// DDL — до сети: импортёр обязан отработать на чистой базе.
	if err = conn.Exec(ctx, keyrateDdl); err != nil {
		return count, err
	}
	if err = conn.Exec(ctx, keyrateContractDdl); err != nil {
		return count, err
	}
	secids, err := fetchKeyrateSecids(ctx)
	if err != nil {
		return count, err
	}
	if len(secids) == 0 {
		return count, fmt.Errorf("keyrate_futures: ISS не отдал ни одной торгуемой серии KEYRATE")
	}
	// Паспорта серий: сбой одной серии не останавливает остальные (серии независимы, как ленты
	// в news_watch), но и не молчит — причина видна в логе по secid и в ошибке прогона.
	contracts := make([]keyrateContract, 0, len(secids))
	var seriesErrs []error
	for _, secid := range secids {
		if !validSecid(secid) {
			log.Warnf("keyrate_futures: серия %q пропущена: код не проходит проверку validSecid", secid)
			seriesErrs = append(seriesErrs, fmt.Errorf("%s: недопустимый код серии", secid))
			continue
		}
		contract, err := fetchKeyrateContract(ctx, secid)
		if err != nil {
			log.Warnf("keyrate_futures: паспорт серии %s не получен: %v", secid, err)
			seriesErrs = append(seriesErrs, fmt.Errorf("%s: %w", secid, err))
			continue
		}
		contracts = append(contracts, contract)
	}
	if len(contracts) == 0 {
		// Молчаливый нулевой прогон недопустим: сбой всех серий — ошибка импорта.
		return count, errors.Join(
			fmt.Errorf("keyrate_futures: паспорта не получены ни по одной из %d серий", len(secids)),
			errors.Join(seriesErrs...))
	}
	contractBatch, err := conn.PrepareBatch(ctx, keyrateContractInsert)
	if err != nil {
		return count, err
	}
	now := time.Now()
	for _, c := range contracts {
		if err = contractBatch.Append(c.secid, c.seriesName, c.firstTrade, c.lastTrade, now); err != nil {
			return count, err
		}
	}
	dayBatch, err := conn.PrepareBatch(ctx, keyrateInsert)
	if err != nil {
		return count, err
	}
	// Инкремент — по каждой серии от её max(date) + 1 день: серии появляются и истекают
	// в разные дни, общей даты у них нет. okSeries считает серии, по которым дни действительно
	// собраны, — по нему решается, публиковать ли прогон.
	okSeries := 0
	for _, c := range contracts {
		stored, err := keyrateStoredDate(ctx, conn, c)
		if err != nil {
			log.Warnf("keyrate_futures: последний день серии %s не прочитан: %v", c.secid, err)
			seriesErrs = append(seriesErrs, fmt.Errorf("%s: %w", c.secid, err))
			continue
		}
		days, err := fetchKeyrateHistory(ctx, c.secid, keyrateSince(c, stored))
		if err != nil {
			log.Warnf("keyrate_futures: история серии %s не получена: %v", c.secid, err)
			seriesErrs = append(seriesErrs, fmt.Errorf("%s: %w", c.secid, err))
			continue
		}
		okSeries++
		for _, d := range days {
			if err = dayBatch.Append(c.secid, d.date, d.close, d.settle, d.waprice, d.volume, d.numtrades, d.openposition); err != nil {
				return count, err
			}
			count++
		}
	}
	if okSeries == 0 {
		// Сбой всех серий — ошибка импорта, и она возвращается ДО публикации: пустой прогон
		// не должен отправлять батчи, писать каталог и пересоздавать витрину.
		return count, errors.Join(
			fmt.Errorf("keyrate_futures: дни не собраны ни по одной из %d серий", len(contracts)),
			errors.Join(seriesErrs...))
	}
	if err = contractBatch.Send(); err != nil {
		return count, err
	}
	if err = dayBatch.Send(); err != nil {
		return count, err
	}
	log.Infof("Собрано %d дней по %d сериям KEYRATE", count, okSeries)
	// Каталог — после успешного Send и на КАЖДУЮ обнаруженную серию (её паспорт), в том числе
	// на серию без дневных итогов: инструмент существует, его ряд должен быть виден агенту ещё
	// до первых сделок — так требует план («одна запись на обнаруженную серию»).
	if err = util.UpsertSeriesCatalog(ctx, conn, keyrateSeriesMeta(contracts)); err != nil {
		return count, err
	}
	if _, err = util.CreateView(ctx, conn, keyrateView); err != nil {
		return count, err
	}
	return count, errors.Join(seriesErrs...)
}

// keyrateSince — с какой даты запрашивать историю серии: день после последнего сохранённого
// (ключ таблицы — (secid, date)) или начало обращения, если по серии ещё ничего не сохранено.
func keyrateSince(c keyrateContract, stored time.Time) time.Time {
	if stored.IsZero() {
		return c.firstTrade
	}
	return stored.AddDate(0, 0, 1)
}

// keyrateStoredDate — дата последнего сохранённого дня серии. secid подставляется в SQL-условие,
// поэтому проверяется validSecid: код приходит из тела ответа ISS.
func keyrateStoredDate(ctx context.Context, conn driver.Conn, c keyrateContract) (time.Time, error) {
	if !validSecid(c.secid) {
		return time.Time{}, fmt.Errorf("keyrate: недопустимый код серии %q", c.secid)
	}
	return lastDate(ctx, conn, keyrateTable, "secid = '"+c.secid+"'")
}

// keyrateSeriesMeta — описания рядов в series_catalog: по одной записи на обнаруженную серию.
// Статического списка быть не может — серии добавляются и истекают каждый квартал.
func keyrateSeriesMeta(contracts []keyrateContract) []util.SeriesMeta {
	meta := make([]util.SeriesMeta, 0, len(contracts))
	for _, c := range contracts {
		expiry := c.lastTrade.Format(moexDate)
		meta = append(meta, util.SeriesMeta{
			Source:    moexSource,
			Series:    c.secid,
			Title:     fmt.Sprintf("%s (%s): вменённый уровень ключевой ставки на заседание ЦБ %s, дневной", c.seriesName, c.secid, expiry),
			Unit:      "п.п. ключевой ставки",
			Frequency: "D",
			Origin: "MOEX ISS: /iss/history/engines/futures/markets/forts/securities/" + c.secid +
				".json, поле SETTLEPRICE (цена дня); паспорт серии — /iss/securities/" + c.secid +
				".json: " + c.seriesName + ", FRSTTRADE " + c.firstTrade.Format(moexDate) +
				", экспирация (LSTDELDATE) " + expiry,
			Description: c.seriesName + " — расчётный фьючерс на Индекс ключевой ставки МосБиржи: цена серии " +
				"(settle, SETTLEPRICE) = вменённый рынком уровень ключевой ставки на дату экспирации " + expiry +
				" (она же дата заседания ЦБ), в п.п.; 1 п.п. = 1 000 ₽ (шаг 0,01 п.п. = 10 ₽). " +
				"Мнемоника перевода в ожидания: вменённое изменение к заседанию Δ = (settle − ставка) × 100 б.п., " +
				"где ставка — последнее известное значение cbr_key_rate; ожидаемый шаг между заседаниями — разность " +
				"settle соседних серий × 100 б.п.; вероятностная интерпретация при дискретных исходах по 25 б.п.: " +
				"P(−25) ≈ (ставка − settle) / 0.25, но полного распределения из одного числа не получить — опционов " +
				"на KEYRATE в ISS нет. Цена дня — SETTLEPRICE, LAST не используется (в дни без сделок ISS отдаёт по " +
				"нему ноль). Инструмент низколиквиден: значение индикативно, при нулевом объёме серии оно " +
				"нерепрезентативно. Витрина — v_keyrate_expectations",
		})
	}
	return meta
}
