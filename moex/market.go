package moex

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// Импортёр moex_market — непрерывные рыночные ряды MOEX для макро-контролей.
// В этом срезе — Brent: фронт-месяц расчётного фьючерса BR. RVI и медь —
// следующий пункт phase-2-moex-market.
type moexMarket struct{}

const (
	moexMarketTable  = "moex_market"
	moexMarketInsert = "INSERT INTO " + moexMarketTable
	moexMarketDdl    = `CREATE TABLE IF NOT EXISTS ` + moexMarketTable + ` (
			  series LowCardinality(String)
			, date Date
			, value Float64
		) ENGINE = ReplacingMergeTree ORDER BY (series, date)
	`

	brentAsset      = "BR"
	brentSeries     = "brent_front"
	brentMarketFrom = "2009-10-01"

	// Первый бэкфилл идёт от начала истории; повтор перечитывает окно,
	// чтобы поймать ролл контракта и правки расчётной цены.
	marketRewriteDays = 400

	futuresSecuritiesPath = "/iss/engines/futures/markets/forts/securities.json"
	futuresHistoryPath    = "/iss/history/engines/futures/markets/forts/securities/%s.json"
	futuresByDatePath     = "/iss/history/engines/futures/markets/forts/securities.json"
)

type futuresSpec struct {
	asset  string
	series string
	start  time.Time
}

var moexMarketView = util.View{
	Name:   "v_moex_market",
	Tables: []string{moexMarketTable},
	Select: `SELECT series, date, value FROM ` + moexMarketTable + ` FINAL`,
	Comment: "Рыночные ряды MOEX для макро-контролей: brent_front — непрерывный фронт-месяц " +
		"расчётного фьючерса BR (Brent, USD за баррель, SETTLEPRICE ближайшего по месяцу контракта). " +
		"Это фьючерсный фронт-месяц, а не спот и не back-adjusted continuous ряд: на роллах возможен " +
		"разрыв. RVI и медь добавляются следующим пунктом phase-2-moex-market; Urals/VIX/CRB на MOEX " +
		"отсутствуют (docs/research/2026-10-20-moex-market-series-audit.md). Описания рядов — в " +
		"v_series_catalog по source='moex'",
	Columns: map[string]string{
		"series": "Ряд: brent_front — Brent, USD за баррель",
		"date":   "Торговый день MOEX (Date)",
		"value":  "Расчётная цена дня (SETTLEPRICE) ближайшего по месяцу контракта BR, USD за баррель; не спот и не back-adjusted — на роллах возможен разрыв",
	},
}

func (s *moexMarket) Name() string {
	return moexMarketTable
}

func (s *moexMarket) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, moexMarketDdl); err != nil {
		return count, err
	}
	start, err := time.Parse(moexDate, brentMarketFrom)
	if err != nil {
		return count, err
	}
	specs := []futuresSpec{{asset: brentAsset, series: brentSeries, start: start}}
	batch, err := conn.PrepareBatch(ctx, moexMarketInsert)
	if err != nil {
		return count, err
	}
	for _, spec := range specs {
		from, err := marketFrom(ctx, conn, spec)
		if err != nil {
			_ = batch.Abort()
			return count, err
		}
		points, err := fetchMarketFutures(ctx, spec, from)
		if err != nil {
			_ = batch.Abort()
			return count, err
		}
		for _, p := range points {
			if err = batch.Append(spec.series, p.date, p.value); err != nil {
				_ = batch.Abort()
				return count, err
			}
			count++
		}
		log.Infof("moex_market: %s: %d points since %s", spec.series, len(points), from.Format(moexDate))
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	if err = util.UpsertSeriesCatalog(ctx, conn, moexSeriesMeta); err != nil {
		return count, err
	}
	if _, err = util.CreateView(ctx, conn, moexMarketView); err != nil {
		return count, err
	}
	log.Infof("Imported %d rows of %s", count, moexMarketTable)
	return count, nil
}

// marketFrom — начало окна: первый прогон от spec.start, повтор от последней
// даты минус marketRewriteDays (ролл и правки расчётной цены).
func marketFrom(ctx context.Context, conn driver.Conn, spec futuresSpec) (time.Time, error) {
	last, err := lastDate(ctx, conn, moexMarketTable, "series = '"+spec.series+"'")
	if err != nil {
		return time.Time{}, err
	}
	if last.IsZero() {
		return spec.start, nil
	}
	from := last.AddDate(0, 0, -marketRewriteDays)
	if from.Before(spec.start) {
		from = spec.start
	}
	return from, nil
}

func fetchMarketFutures(ctx context.Context, spec futuresSpec, from time.Time) ([]marketPoint, error) {
	secids, err := discoverFuturesSecids(ctx, spec.asset, from)
	if err != nil {
		return nil, err
	}
	if len(secids) == 0 {
		return nil, fmt.Errorf("moex_market: %s: no contracts discovered since %s", spec.asset, from.Format(moexDate))
	}
	var bars []futuresBar
	ok := 0
	for _, secid := range secids {
		history, err := fetchFuturesHistory(ctx, secid, spec.asset, from)
		if err != nil {
			log.Warnf("moex_market: %s: history %s not fetched: %v", spec.asset, secid, err)
			continue
		}
		ok++
		bars = append(bars, history...)
	}
	if ok == 0 {
		return nil, fmt.Errorf("moex_market: %s: history not fetched for any of %d contracts", spec.asset, len(secids))
	}
	points := frontMonthPoints(bars)
	if len(points) == 0 {
		return nil, fmt.Errorf("moex_market: %s: no front-month points", spec.asset)
	}
	log.Infof("moex_market: %s: %d contracts, %d bars, %d points", spec.asset, ok, len(bars), len(points))
	return points, nil
}

// discoverFuturesSecids — текущие контракты плюс исторические, найденные
// по одному запросу на месяц. Месячный запрос видит контракт, только если он
// торговался в выбранный день; 15→10-е и окно 400 дней компенсируют пропуск.
func discoverFuturesSecids(ctx context.Context, asset string, from time.Time) ([]string, error) {
	seen := make(map[string]bool)
	var secids []string
	add := func(list []string) {
		for _, secid := range list {
			if seen[secid] {
				continue
			}
			seen[secid] = true
			secids = append(secids, secid)
		}
	}
	block, err := getBlock(ctx, futuresSecuritiesPath+"?iss.meta=off&iss.only=securities", "securities")
	if err != nil {
		return nil, err
	}
	current, err := parseFuturesSecids(block, asset)
	if err != nil {
		return nil, err
	}
	add(current)
	now := time.Now().UTC()
	for month := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.UTC); !month.After(now); month = month.AddDate(0, 1, 0) {
		found, err := fetchFuturesSecidsForMonth(ctx, month, asset)
		if err != nil {
			return nil, err
		}
		add(found)
	}
	return secids, nil
}

// fetchFuturesSecidsForMonth — контракты, торговавшиеся в месяце: 15-е, при
// пустом ответе 14-е … 10-е. Пустой месяц — не ошибка.
func fetchFuturesSecidsForMonth(ctx context.Context, month time.Time, asset string) ([]string, error) {
	now := time.Now().UTC()
	for day := 15; day >= 10; day-- {
		date := time.Date(month.Year(), month.Month(), day, 0, 0, 0, 0, time.UTC)
		if date.After(now) {
			continue
		}
		secids, err := fetchFuturesSecidsOnDate(ctx, date, asset)
		if err != nil {
			return nil, err
		}
		if len(secids) > 0 {
			return secids, nil
		}
	}
	return nil, nil
}

func fetchFuturesSecidsOnDate(ctx context.Context, date time.Time, asset string) ([]string, error) {
	seen := make(map[string]bool)
	var secids []string
	for start := 0; ; start += moexPageLimit {
		blocks, err := getBlocks(ctx, fmt.Sprintf("%s?iss.meta=off&date=%s&assetcode=%s&start=%d",
			futuresByDatePath, date.Format(moexDate), asset, start))
		if err != nil {
			return nil, err
		}
		page, err := parseFuturesSecids(blocks["history"], asset)
		if err != nil {
			return nil, err
		}
		added := 0
		for _, secid := range page {
			if seen[secid] {
				continue
			}
			seen[secid] = true
			secids = append(secids, secid)
			added++
		}
		if len(blocks["history"].Data) == 0 {
			break
		}
		// Источник, игнорирующий start, отдаёт ту же страницу — без этого
		// условия цикл не завершится.
		if added == 0 && start > 0 {
			break
		}
		total, err := parseHistoryTotal(blocks["history.cursor"])
		if err != nil {
			return nil, err
		}
		if len(secids) >= total {
			break
		}
	}
	return secids, nil
}

// fetchFuturesHistory — дневная история контракта с from включительно.
// Пагинация start шагом moexPageLimit до пустой страницы или TOTAL из
// history.cursor (PAGESIZE параметром не понижается).
func fetchFuturesHistory(ctx context.Context, secid, asset string, from time.Time) ([]futuresBar, error) {
	if !validMarketSecid(secid) {
		return nil, fmt.Errorf("moex_market: invalid secid %q", secid)
	}
	path := fmt.Sprintf(futuresHistoryPath, secid)
	var bars []futuresBar
	for start := 0; ; start += moexPageLimit {
		blocks, err := getBlocks(ctx, fmt.Sprintf("%s?iss.meta=off&from=%s&start=%d",
			path, from.Format(moexDate), start))
		if err != nil {
			return nil, err
		}
		page, err := parseFuturesHistory(blocks["history"], asset)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if len(page) == 0 {
			break
		}
		bars = append(bars, page...)
		total, err := parseHistoryTotal(blocks["history.cursor"])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if len(bars) >= total {
			break
		}
	}
	return bars, nil
}
