package fred

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

type fredImport struct {
}

const (
	fredSource    = "fred"
	fredSeriesUrl = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=%s"
	fredTimeout   = 60 * time.Second
	fredTable     = "macro_series"
	fredDdl       = `CREATE TABLE IF NOT EXISTS ` + fredTable + ` (
			  source LowCardinality(String)
			, series LowCardinality(String)
			, date Date32
			, value Float64
		) ENGINE = ReplacingMergeTree ORDER BY (source, series, date);
	`
	fredInsert     = "INSERT INTO " + fredTable
	fredDateLayout = "2006-01-02"
)

var fredSeries = []string{"DFII10", "DGS10", "FEDFUNDS", "DTWEXBGS", "CPIAUCSL", "T5YIE"}

var fredViews = []string{
	`CREATE OR REPLACE VIEW v_fred_macro DEFINER = default SQL SECURITY DEFINER AS
		SELECT date, series, value FROM macro_series FINAL WHERE source = 'fred'`,
	`ALTER TABLE v_fred_macro MODIFY COMMENT 'Макро-ряды США из FRED: реальная доходность 10 лет (DFII10), номинальная 10 лет (DGS10), ставка ФРС (FEDFUNDS), широкий доллар (DTWEXBGS), CPI (CPIAUCSL), 5-летняя breakeven-инфляция (T5YIE). Описание каждого ряда и единиц — в v_series_catalog'`,
	`ALTER TABLE v_fred_macro COMMENT COLUMN date 'Дата наблюдения (для месячных рядов — первый день месяца)'`,
	`ALTER TABLE v_fred_macro COMMENT COLUMN series 'Код ряда FRED: DFII10, DGS10, FEDFUNDS, DTWEXBGS, CPIAUCSL, T5YIE'`,
	`ALTER TABLE v_fred_macro COMMENT COLUMN value 'Значение ряда в единицах из v_series_catalog (проценты, индекс или уровень индекса)'`,
}

var fredSeriesMeta = []util.SeriesMeta{
	{
		Source:      fredSource,
		Series:      "DFII10",
		Title:       "10-Year Treasury Inflation-Indexed Security yield (real), daily",
		Unit:        "percent, per year",
		Frequency:   "D",
		Origin:      "FRED fredgraph.csv?id=DFII10",
		Description: "Реальная доходность 10-летних TIPS. Ключевой драйвер золота: рост реальных доходностей удорожает безрисковое владение золотом. Значение — годовые проценты, не темп; пропуски в праздники FRED не публикует",
	},
	{
		Source:      fredSource,
		Series:      "DGS10",
		Title:       "Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity, daily",
		Unit:        "percent, per year",
		Frequency:   "D",
		Origin:      "FRED fredgraph.csv?id=DGS10",
		Description: "Номинальная доходность 10-летних казначейских облигаций США. Значение — годовые проценты; разница с DFII10 даёт breakeven-инфляцию",
	},
	{
		Source:      fredSource,
		Series:      "FEDFUNDS",
		Title:       "Effective Federal Funds Rate, monthly",
		Unit:        "percent, per year",
		Frequency:   "M",
		Origin:      "FRED fredgraph.csv?id=FEDFUNDS",
		Description: "Эффективная ставка по федеральным фондам (среднее за месяц) — ориентир денежной политики ФРС. Значение — годовые проценты; изменение ставки считать разностью уровней, не темпом",
	},
	{
		Source:      fredSource,
		Series:      "DTWEXBGS",
		Title:       "Nominal Broad U.S. Dollar Index, daily",
		Unit:        "index, Jan 2, 1997 = 100",
		Frequency:   "D",
		Origin:      "FRED fredgraph.csv?id=DTWEXBGS",
		Description: "Номинальный индекс широкого доллара США к корзине валют крупных торговых партнёров. Рост индекса — укрепление доллара. Уровень индекса, не темп: темп = value / value − 1 к нужной дате",
	},
	{
		Source:      fredSource,
		Series:      "CPIAUCSL",
		Title:       "Consumer Price Index for All Urban Consumers: All Items in U.S. City Average, SA, monthly",
		Unit:        "index, 1982-84 = 100",
		Frequency:   "M",
		Origin:      "FRED fredgraph.csv?id=CPIAUCSL",
		Description: "Индекс потребительских цен США, сезонно скорректированный. Уровень индекса, не темп: годовой темп инфляции = value / value 12 месяцев назад − 1",
	},
	{
		Source:      fredSource,
		Series:      "T5YIE",
		Title:       "5-Year Breakeven Inflation Rate, daily",
		Unit:        "percent, per year",
		Frequency:   "D",
		Origin:      "FRED fredgraph.csv?id=T5YIE",
		Description: "Рыночные инфляционные ожидания на 5 лет: разница номинальной и реальной доходности 5-летних облигаций. Значение — годовые проценты",
	},
}

type observation struct {
	date  time.Time
	value float64
}

func (s *fredImport) Name() string {
	return fredSource
}

func (s *fredImport) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, fredDdl); err != nil {
		return count, err
	}
	for _, stmt := range fredViews {
		if err = conn.Exec(ctx, stmt); err != nil {
			return count, err
		}
	}
	batch, err := conn.PrepareBatch(ctx, fredInsert)
	if err != nil {
		return count, err
	}
	for _, id := range fredSeries {
		observations, err := fetchSeries(ctx, id)
		if err != nil {
			return count, fmt.Errorf("fred %s: %w", id, err)
		}
		for _, o := range observations {
			if err = batch.Append(fredSource, id, o.date, o.value); err != nil {
				return count, err
			}
			count++
		}
		log.Infof("Fetched %d observations of %s", len(observations), id)
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	if err = util.UpsertSeriesCatalog(ctx, conn, fredSeriesMeta); err != nil {
		return count, err
	}
	return count, nil
}

func fetchSeries(ctx context.Context, id string) ([]observation, error) {
	ctx, cancel := context.WithTimeout(ctx, fredTimeout)
	defer cancel()
	// FRED стоит за Imperva: браузерный User-Agent рвёт соединение, поэтому UA не задаём
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf(fredSeriesUrl, id), nil)
	if err != nil {
		return nil, err
	}
	resp, err := util.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}
	return parseSeries(resp.Body)
}

func parseSeries(body io.Reader) ([]observation, error) {
	records, err := csv.NewReader(body).ReadAll()
	if err != nil {
		return nil, err
	}
	observations := make([]observation, 0, len(records))
	for i, record := range records {
		if i == 0 {
			continue
		}
		// FRED оставляет значение пустым (или ".") для дат без наблюдения: праздники, нет публикации
		if record[1] == "" || record[1] == "." {
			continue
		}
		date, err := time.Parse(fredDateLayout, record[0])
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		value, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		observations = append(observations, observation{date: date, value: value})
	}
	return observations, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &fredImport{})
}
