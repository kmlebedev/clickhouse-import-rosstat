package eia

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

type eiaImport struct {
}

const (
	eiaSource     = "eia"
	eiaKeyEnv     = "EIA_API_KEY"
	eiaTimeout    = 60 * time.Second
	eiaStart      = "2019-01-01"
	eiaFrequency  = "weekly"
	eiaPageLength = 5000
	eiaTimeLayout = "2006-01-02"
	eiaTable      = "macro_series"
	eiaDdl        = `CREATE TABLE IF NOT EXISTS ` + eiaTable + ` (
				  source LowCardinality(String)
				, series LowCardinality(String)
				, date Date32
				, value Float64
			) ENGINE = ReplacingMergeTree ORDER BY (source, series, date);
		`
	eiaInsert = "INSERT INTO " + eiaTable
)

var eiaViews = []string{
	`CREATE OR REPLACE VIEW v_eia_petroleum DEFINER = default SQL SECURITY DEFINER AS
		SELECT date, series, value FROM macro_series FINAL WHERE source = 'eia'`,
	`ALTER TABLE v_eia_petroleum MODIFY COMMENT 'Недельные ряды нефтяного канала США из EIA API v2: distillate_stocks — запасы дистиллятного топлива, refinery_utilization — загрузка мощностей НПЗ. Публикуются по средам (Weekly Petroleum Status Report); единицы и описание каждого ряда — в v_series_catalog'`,
	`ALTER TABLE v_eia_petroleum COMMENT COLUMN date 'Дата окончания отчётной недели (WPSR публикуется в среду вечером; период ряда — конец недели)'`,
	`ALTER TABLE v_eia_petroleum COMMENT COLUMN series 'Имя ряда: distillate_stocks (EIA WDISTUS1) или refinery_utilization (EIA WPULEUS3)'`,
	`ALTER TABLE v_eia_petroleum COMMENT COLUMN value 'Значение ряда в единицах из v_series_catalog: тыс. барр. (запасы дистиллятов) или проценты (загрузка НПЗ)'`,
}

// eiaSeriesDef — одна серия EIA: код в фасетах API, имя ряда в macro_series и маршрут API v2.
type eiaSeriesDef struct {
	code   string
	series string
	url    string
}

var (
	eiaSeries = []eiaSeriesDef{
		{
			code:   "WDISTUS1",
			series: "distillate_stocks",
			url:    "https://api.eia.gov/v2/petroleum/stoc/wstk/data/",
		},
		{
			code:   "WPULEUS3",
			series: "refinery_utilization",
			url:    "https://api.eia.gov/v2/petroleum/pnp/wiup/data/",
		},
	}
	eiaSeriesMeta = []util.SeriesMeta{
		{
			Source:      eiaSource,
			Series:      "distillate_stocks",
			Title:       "U.S. Ending Stocks of Distillate Fuel Oil (Thousand Barrels)",
			Unit:        "thousand barrels",
			Frequency:   "W",
			Origin:      "EIA API v2 /v2/petroleum/stoc/wstk/data/, series WDISTUS1 (Weekly Petroleum Status Report)",
			Description: "Запасы дистиллятного топлива (дизель и печное топливо) в США на конец недели, тыс. барр. Уровень запасов, не темп: недельное изменение = value минус value предыдущей недели; для оценки дефицита сравнивать с 5-летней средней для того же периода года (ряд сезонный). Рост запасов давит вниз на дизельный крэк",
		},
		{
			Source:      eiaSource,
			Series:      "refinery_utilization",
			Title:       "U.S. Percent Utilization of Refinery Operable Capacity",
			Unit:        "percent",
			Frequency:   "W",
			Origin:      "EIA API v2 /v2/petroleum/pnp/wiup/data/, series WPULEUS3 (Weekly Petroleum Status Report)",
			Description: "Загрузка мощностей НПЗ США, % от operable capacity. Проценты, не доли: рост загрузки = больше переработки сырья и предложения дистиллятов. Обычный уровень около 90%; падение ниже ~85% указывает на ремонты или сбои и работает в сторону роста крэка",
		},
	}
)

// eiaEnvelope — конверт ответа EIA API v2; total в API приходит строкой.
type eiaEnvelope struct {
	Response struct {
		Total string `json:"total"`
		Data  []struct {
			Period string `json:"period"`
			Series string `json:"series"`
			Value  string `json:"value"`
		} `json:"data"`
	} `json:"response"`
}

type observation struct {
	series string
	date   time.Time
	value  float64
}

// eiaPage — разобранная страница ответа: наблюдения, число строк data в ответе
// (по нему решается пагинация) и response.total.
type eiaPage struct {
	observations []observation
	rows         int
	total        int
}

func (s *eiaImport) Name() string {
	return eiaSource
}

func (s *eiaImport) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	key := os.Getenv(eiaKeyEnv)
	if key == "" {
		return count, fmt.Errorf("%s is not set", eiaKeyEnv)
	}
	if err = conn.Exec(ctx, eiaDdl); err != nil {
		return count, err
	}
	for _, stmt := range eiaViews {
		if err = conn.Exec(ctx, stmt); err != nil {
			return count, err
		}
	}
	batch, err := conn.PrepareBatch(ctx, eiaInsert)
	if err != nil {
		return count, err
	}
	for _, series := range eiaSeries {
		observations, err := fetchSeries(ctx, key, series)
		if err != nil {
			return count, fmt.Errorf("eia %s: %w", series.series, err)
		}
		for _, o := range observations {
			if err = batch.Append(eiaSource, o.series, o.date, o.value); err != nil {
				return count, err
			}
			count++
		}
		log.Infof("Fetched %d observations of %s (EIA series %s)", len(observations), series.series, series.code)
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	if err = util.UpsertSeriesCatalog(ctx, conn, eiaSeriesMeta); err != nil {
		return count, err
	}
	return count, nil
}

// fetchSeries вычитывает всю серию с пагинацией: страницы по eiaPageLength, пока
// API отдаёт полную страницу и offset не дошёл до response.total.
func fetchSeries(ctx context.Context, key string, series eiaSeriesDef) ([]observation, error) {
	ctx, cancel := context.WithTimeout(ctx, eiaTimeout)
	defer cancel()
	var observations []observation
	for offset := 0; ; {
		query := url.Values{}
		query.Set("api_key", key)
		query.Set("frequency", eiaFrequency)
		query.Set("data[0]", "value")
		query.Set("facets[series][]", series.code)
		query.Set("start", eiaStart)
		query.Set("sort[0][column]", "period")
		query.Set("sort[0][direction]", "asc")
		query.Set("length", strconv.Itoa(eiaPageLength))
		query.Set("offset", strconv.Itoa(offset))
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, series.url+"?"+query.Encode(), nil)
		if err != nil {
			return nil, err
		}
		resp, err := util.HttpClient.Do(req)
		if err != nil {
			return nil, maskKey(err, key)
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("http status %d", resp.StatusCode)
		}
		page, err := parseResponse(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("series %s: %w", series.code, err)
		}
		observations = append(observations, page.observations...)
		next, more := nextOffset(page.rows, offset, page.total, eiaPageLength)
		if !more {
			if page.rows < eiaPageLength && offset+page.rows < page.total {
				// страница короче запрошенной, но total обещает ещё строки: пагинация
				// с шагом length останавливается — сообщаем, чтобы не терять данные молча
				log.Warnf("EIA series %s: page of %d rows at offset %d while total %d — pagination stopped early",
					series.code, page.rows, offset, page.total)
			}
			break
		}
		offset = next
	}
	return observations, nil
}

// nextOffset решает, нужна ли следующая страница: только если пришла полная страница
// и следующая позиция ещё не покрывает response.total. Возвращает offset и признак «продолжать».
func nextOffset(rows, offset, total, length int) (int, bool) {
	if rows < length {
		return 0, false
	}
	next := offset + length
	if next >= total {
		return 0, false
	}
	return next, true
}

// maskKey убирает ключ API из текста ошибки: url.Error печатает полный URL, включая api_key.
func maskKey(err error, key string) error {
	if key == "" {
		return err
	}
	return errors.New(strings.ReplaceAll(err.Error(), key, "***"))
}

// eiaSeriesName сопоставляет код фасета EIA с именем ряда в macro_series.
func eiaSeriesName(code string) (string, bool) {
	for _, s := range eiaSeries {
		if s.code == code {
			return s.series, true
		}
	}
	return "", false
}

// parseResponse разбирает одну страницу ответа EIA: period и value переводятся в наблюдения,
// строки чужого кода серии и без значения пропускаются. rows — число строк data в ответе.
func parseResponse(body io.Reader) (eiaPage, error) {
	var envelope eiaEnvelope
	if err := json.NewDecoder(body).Decode(&envelope); err != nil {
		return eiaPage{}, err
	}
	total, err := strconv.Atoi(envelope.Response.Total)
	if err != nil {
		return eiaPage{}, fmt.Errorf("total %q: %w", envelope.Response.Total, err)
	}
	page := eiaPage{rows: len(envelope.Response.Data), total: total}
	for _, item := range envelope.Response.Data {
		series, ok := eiaSeriesName(item.Series)
		if !ok {
			continue
		}
		if item.Value == "" {
			continue
		}
		date, err := time.Parse(eiaTimeLayout, item.Period)
		if err != nil {
			return eiaPage{}, fmt.Errorf("series %s: period %q: %w", item.Series, item.Period, err)
		}
		value, err := strconv.ParseFloat(item.Value, 64)
		if err != nil {
			return eiaPage{}, fmt.Errorf("series %s: period %s: value %q: %w", item.Series, item.Period, item.Value, err)
		}
		page.observations = append(page.observations, observation{series: series, date: date, value: value})
	}
	return page, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &eiaImport{})
}
