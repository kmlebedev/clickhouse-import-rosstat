# Data map: views, queries, traps

All queries go through `mcp__clickhouse__run_query` as `kimi_reader`
(read-only, `v_*` views only). Everything below was run against the dev
instance on 2026-11-17; columns and series names are the real ones. Views
change, so when a query fails run `DESCRIBE v_<name>` and read the column
comments rather than guessing. The canonical registry is `ARCHITECTURE.md`
§6.3; if a view below does not exist on the instance, say so in the report
rather than substituting another.

Contents: [how to query](#how-to-query-without-losing-the-result) ·
[where each block lives](#where-each-block-lives) ·
[starter queries](#starter-queries) · [traps](#traps-found-on-live-data) ·
[further reading](#further-reading-in-the-repository)

## How to query without losing the result

Three mechanical things cost a run more time than any economics:

- **Cut Cyrillic text with `substringUTF8` / `leftUTF8`, never
  `substring`.** `substring` counts bytes, splits a two-byte letter, and the
  MCP then returns the whole cell as a hex string.
- **Keep results small.** A result above roughly 50 000 characters is
  written to a file instead of being returned. `v_cbr_macro` has 292 series
  and `v_rosstat_macro` 165, so never `GROUP BY series` without a filter;
  a full summary of discussion is about 50 paragraphs of 1 000 characters —
  pull it one `block` at a time.
- **Long-form views share `series, date, value`** but the fourth column
  differs (`source`, `kind`, `name`/`tenor`). Series are named either by
  table (`cbr_key_rate`) or by the Russian title of the source row
  (`ожидаемая инфляция`); find a name with
  `SELECT DISTINCT series FROM v_x WHERE series ILIKE '%…%'` or in
  `v_series_catalog` (`source, series, title, unit, frequency, origin,
  description`).

## Where each block lives

| Block | View | What to take |
|---|---|---|
| One-row-per-signal overview | `v_key_rate_dashboard` | `contour, indicator, value, unit, as_of, target_date, threshold, flag, stale, error_bp, probabilities, model, data_version`; contours `spot`, `fundamental`, `market`, `divergence`, `forecast`, `calendar` |
| Meeting dates, past and future | `v_events_calendar` | `category = 'cbr'` and title `СД ЦБ РФ: заседание по ключевой ставке`: every meeting since 2014 and the published schedule ahead; `emergency = 1` marks unscheduled ones; `source = 'canonical'` marks importer rows, `created_at` the row's first write |
| Key rate, RUONIA, FX | `v_cbr_macro` | `cbr_key_rate` (daily, working days, from 2013-09), `cbr_ruania_ruo`, `cbr_currency_usd` |
| Household expectations | `v_cbr_macro` | `ожидаемая инфляция`, `наблюдаемая инфляция` — monthly from 2017, dated the 14th of the survey month; the dashboard's `infl_exp_expected` is the last point |
| Money, credit, deposits | `v_cbr_macro` | `М2 SA`, `Денежная масса М2`, `Кредит экономике`, `Требования к организациям`, `Требования к населению*`, `Депозиты населения`; core CPI index `Базовый ИПЦ` |
| Lending and deposit rates | `v_cbr_macro` | bare maturity labels (`Всего: свыше 1 года`, `от 1 года до 3 лет`, `Всего (кредиты физлицам): по всем срокам`, `Средневзвешенная ставка по жилищным кредитам, РФ`) — resolve which is a loan and which a deposit through `v_series_catalog` before quoting |
| CPI monthly, weekly, labour, wages, GDP | `v_rosstat_macro` | Russian titles: `Индексы потребительских цен на товары и услуги…`, `Индекс потребительских цен (недельная оценка)`, `Уровень безработицы`, `Среднемесячная номинальная начисленная заработная плата…`; the rest of the view is weekly prices of individual goods |
| Weekly CPI, seasonally adjusted | `v_weekly_cpi_sa`, `v_weekly_cpi_rolling3m`, `v_weekly_cpi_sa_vintage`, `v_weekly_cpi_sa_asof` | series `weekly_cpi:saar`, `weekly_cpi:sa_4w`, `weekly_cpi:sa_index`, one row per month (month-end date), `as_of` = last week included, `partial = 1` for the unfinished month; `saar_3m` in the rolling view; for an as-of slice use `v_weekly_cpi_sa_asof(vintage_as_of='YYYY-MM-DD')`, whose history starts on 2026-11-09 |
| Business surveys | `v_cbr_survey` | `price_expectations_sa`, `price_growth_expected_sa`, `business_climate_sa`, `capacity_utilization_sa`, `headcount_expectations_sa`, `staff_availability_sa`, `credit_conditions`; 80 more `industry_*` series |
| Budget | `v_minfin_budget` | sources `minfin`, `minfin_mesyats`, `minfin_oil_gas`; `Доходы, всего`, `Расходы, всего`, `Дефицит (-)/Профицит (+)`, `Нефтегазовые доходы, всего`; defence closed since 2022 |
| External | `v_cbr_bop`, `v_moex_market`, `v_minec_macro` | `cbr_bop_est` (monthly estimate, 2025 on) and `cbr_bop_standard` (quarterly since 1994), period in the series name: `Счет текущих операций (месяц)`; Brent — `v_moex_market` `brent_front` (daily, 2009 on); Urals — `v_minec_macro` `urals_avg_price` (monthly, 2018 on) |
| Fuel prices | `v_rosstat_macro`, `v_fedstat_weekly_prices` | weekly retail petrol: `Бензин автомобильный марки АИ-92, л` (and АИ-95, АИ-98) in `v_rosstat_macro` from 2022, `Бензин автомобильный` from 2009; `v_fedstat_weekly_prices` from 2008 |
| Liquidity, money market | `v_cbr_liquidity`, `v_cbr_mirac` | `kind = 'daily'`: `deficit`, `corr_accounts`; `kind = 'averaging'`: `structural_deficit`, `ruonia_spread_bp`; MIACR by `name` and `tenor` |
| Market-implied rate | `v_keyrate_expectations` | `date, secid, series_name, decision_date, settle, close, volume, openposition, expected_change_bp` |
| OFZ | `v_ofz_signal`, dashboard `market` rows | `ofz_1y, ofz_2y, ofz_10y, key_rate, slope_10y_1y_pp, real_yield_10y_pp`; `v_ofz_curve_compare` by `tenor` |
| Press releases | `v_cbr_key_rate_press`, `_blocks`, `_quarters` | `meeting_date, published, decision_action (cut/hold/hike), decision_rate, decision_delta_bp, next_meeting_date, signal`; blocks: `block, seq, text` |
| Summaries of discussion | `v_cbr_mp_summary`, `_blocks`, `_channels` | blocks: `block, subsection (Основные факты / Дискуссия), kind (paragraph / bullet), seq, text`; `published` is about two weeks after `meeting_date` |
| Press conference | `v_cbr_key_rate_pressconf_text`, `_cues` | one long `text` per meeting — search it with `positionUTF8` / `extractAll`, do not select it whole |
| The Bank's forecast ranges | `v_cbr_mt_forecast` | `meeting_date, release_date, indicator, year, lo, hi`; indicators `key_rate_avg`, `cpi_dec`, `cpi_avg`, `gdp_yoy`, `gdp_q4_yoy`, `m2`, `bank_claims_*`, … |
| Annual policy guidelines | `v_cbr_ondkp_forecast` | `release_year, indicator (key_rate, …), year, lo, hi, announcement_date, publication_status` |
| Release calendar | `v_cbr_publication_calendar` | `publication_date, title, topic (analytics / statistics), period, planned` (1 = date from the Bank's schedule, publication not yet confirmed by the importer — past dates can still carry 1) |
| Other tracks and their live accuracy | `v_ml_live_accuracy` | `metric (cbr_rate:llm, cbr_rate:taylor, …), track, stage, model, forecast_date, target_date, predicted, actual, error_bp, is_resolved` |
| What was known on a date | `v_point_in_time_asof(as_of='YYYY-MM-DD')`, `v_point_in_time_version` | covers the DIP bulletin only; `data_version` for the record |

## Starter queries

```sql
-- 1. Preflight: everything in one screen, with staleness
SELECT contour, indicator, value, unit, as_of, target_date, flag, stale,
       error_bp, probabilities, model
FROM v_key_rate_dashboard ORDER BY contour, indicator;

-- 2. The next eight meetings (the trajectory's rows)
SELECT DISTINCT event_date FROM v_events_calendar
WHERE category = 'cbr' AND title LIKE '%заседание по ключевой ставке%'
  AND event_date >= today() ORDER BY event_date LIMIT 8;

-- 3. Decision history WITH holds, for base rates. cbr_key_rate alone shows
--    only changes, dated by the day they took effect (the Monday after).
WITH m0 AS (SELECT event_date AS meeting, max(emergency) AS emergency
            FROM v_events_calendar
            WHERE category = 'cbr' AND title LIKE '%заседание по ключевой ставке%'
              AND event_date < today() GROUP BY event_date),
     -- the decision is in force within six days (holidays stretch the usual
     -- three), but never later than the eve of the next meeting
     m AS (SELECT meeting, emergency,
                  least(meeting + 6, leadInFrame(meeting, 1, toDate('2100-01-01'))
                        OVER (ORDER BY meeting ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) - 1) AS settled
           FROM m0),
     r AS (SELECT date, value FROM v_cbr_macro WHERE series = 'cbr_key_rate'),
     x AS (SELECT meeting, emergency,
                  argMaxIf(value, date, date <  meeting) AS rate_before,
                  argMaxIf(value, date, date <= settled) AS rate_after
           FROM m CROSS JOIN r GROUP BY meeting, emergency)
SELECT meeting, emergency, rate_before, rate_after,
       round((rate_after - rate_before) * 100) AS step_bp,
       lagInFrame(step_bp) OVER (ORDER BY meeting) AS prev_step_bp
FROM x ORDER BY meeting;
-- A fixed "+4 days" window is wrong: the moves of 2014-10-31, 2015-04-30 and
-- 2022-04-29 took effect five days later, after holidays, and showed as holds.
-- Aggregate it for the prior, e.g. … GROUP BY sign(prev_step_bp), step_bp
-- WHERE emergency = 0. 109 meetings since 2014.

-- 4. Calendar-day average of the key rate this year, the --ytd-avg input.
--    The series has working days only; a plain avg() is biased (14.66
--    against the true 14.68 on 2026-11-15), so fill the gaps first.
WITH d AS (SELECT date, value FROM v_cbr_macro
           WHERE series = 'cbr_key_rate' AND date >= toStartOfYear(today()) - 15
           ORDER BY date WITH FILL FROM toStartOfYear(today()) - 15 TO today() + 1
           INTERPOLATE (value AS value))
SELECT max(date) AS through, count() AS days, round(avg(value), 4) AS ytd_avg,
       argMax(value, date) AS rate_now
FROM d WHERE date >= toStartOfYear(today());

-- 5. Market strip: latest settle for each future meeting
SELECT decision_date, secid,
       argMax(settle, date) AS implied_rate,
       argMax(expected_change_bp, date) AS implied_change_bp,
       argMax(volume, date) AS volume, argMax(openposition, date) AS oi,
       max(date) AS as_of
FROM v_keyrate_expectations WHERE decision_date >= today()
GROUP BY decision_date, secid ORDER BY decision_date;
-- For scoring a past meeting: same query with decision_date = the meeting
-- and date <= the run date of the forecast being scored.

-- 6. The Bank's ranges: latest vintage and the one before. Every vintage is
--    stored twice (release on the meeting day and again with the summary),
--    hence the GROUP BY.
SELECT meeting_date, indicator, year, any(lo) AS lo, any(hi) AS hi
FROM v_cbr_mt_forecast
WHERE indicator IN ('key_rate_avg', 'cpi_dec', 'cpi_avg', 'gdp_yoy')
  AND meeting_date IN (SELECT DISTINCT meeting_date FROM v_cbr_mt_forecast
                       ORDER BY meeting_date DESC LIMIT 2)
GROUP BY meeting_date, indicator, year ORDER BY indicator, year, meeting_date;

-- 7. Last decisions and the signal sentence
SELECT meeting_date, decision_action, decision_rate, decision_delta_bp,
       next_meeting_date, leftUTF8(signal, 600) AS signal
FROM v_cbr_key_rate_press ORDER BY meeting_date DESC LIMIT 4;

-- 8. The last summary: first the map, then one block at a time
SELECT block, subsection, count() AS paragraphs, sum(lengthUTF8(text)) AS chars
FROM v_cbr_mp_summary_blocks
WHERE meeting_date = (SELECT max(meeting_date) FROM v_cbr_mp_summary_blocks)
GROUP BY block, subsection ORDER BY block, subsection;

SELECT subsection, kind, seq, text FROM v_cbr_mp_summary_blocks
WHERE meeting_date = (SELECT max(meeting_date) FROM v_cbr_mp_summary_blocks)
  AND block LIKE 'Выводы для денежно-кредитной политики%' ORDER BY seq;
-- Blocks: 'Ситуация в экономике и инфляция', 'Денежно-кредитные условия',
-- 'Внешние условия', 'Инфляционные риски', 'Выводы для … и решение по
-- ключевой ставке' (the options discussed and the trajectory are here).

-- 9. Where the Bank spoke about temporary or one-off factors
SELECT meeting_date, channel, kind, leftUTF8(text, 500) AS text
FROM v_cbr_mp_summary_channels
WHERE method = 'regex' AND (text ILIKE '%разов%' OR text ILIKE '%временн%')
ORDER BY meeting_date DESC LIMIT 30;

-- 10. Household expectations and whether this month's wave is loaded
SELECT series, date, value FROM v_cbr_macro
WHERE series IN ('ожидаемая инфляция', 'наблюдаемая инфляция')
  AND date >= today() - 200 ORDER BY series, date;

SELECT publication_date, period, planned FROM v_cbr_publication_calendar
WHERE title ILIKE 'Оперативная справка%инфляционных ожиданий%'
  AND publication_date >= today() - 45 ORDER BY publication_date;
-- The title does not contain the word "инФОМ". The wave is loaded when the
-- month of the last 'ожидаемая инфляция' point equals `period` of the
-- latest note with planned = 0.

-- 11. Releases due before the next meeting that bear on the call
SELECT publication_date, title, period FROM v_cbr_publication_calendar
WHERE publication_date >= today() AND publication_date <= (SELECT min(event_date)
        FROM v_events_calendar WHERE category = 'cbr' AND event_date >= today())
  AND (title ILIKE '%инфляц%' OR title ILIKE 'Мониторинг предприятий%'
       OR title ILIKE '%кредит экономике%' OR title ILIKE '%Денежно-кредитные условия%'
       OR title ILIKE '%процентные ставки по кредит%')
ORDER BY publication_date;
-- Rosstat's own releases (weekly CPI on Wednesdays, monthly CPI around the
-- 10th–14th) are not in this calendar; name them from the usual schedule
-- and say the date is the usual one, not a confirmed one.

-- 12. How the other tracks did at past meetings
SELECT metric, stage, forecast_date, target_date, predicted, actual, error_bp
FROM v_ml_live_accuracy
WHERE metric LIKE 'cbr_rate:%' AND model != 'gate-selftest'
ORDER BY target_date DESC, metric, forecast_date DESC LIMIT 20;
-- Rows whose target_date is not the meeting being forecast are scores of
-- past calls. They say how reliable a track has been, not what it expects
-- now.

-- 13. Where today's real rate sits in its own history. Run this (or its
--     equivalent for another deflator) before calling a level "high",
--     "record" or "in the upper part of the distribution", and report the
--     period, the deflator and the percentile.
WITH h AS (SELECT date, key_rate - inflation_yoy AS real_rate FROM v_ofz_signal
           WHERE key_rate IS NOT NULL AND inflation_yoy IS NOT NULL)
SELECT min(date) AS from_day, max(date) AS to_day, count() AS days,
       round(quantile(0.5)(real_rate), 2) AS median,
       round(quantile(0.9)(real_rate), 2) AS p90,
       round(argMax(real_rate, date), 2) AS latest,
       round(100 * countIf(real_rate <= (SELECT argMax(real_rate, date) FROM h))
             / count(), 1) AS latest_percentile
FROM h;
-- On 2026-11-15: 2013-09 to 2026-11, 3 305 days, median 3.45, latest 7.65
-- against year-on-year CPI = 77th percentile — elevated, not extreme. The
-- deflator changes the answer (8.1 against the core annualised rate), and
-- for that one there is no ready history: build it or do not rank it.
```

## Traps found on live data

- **Staleness is common on dev.** Daily series lag by a week or more when
  imports have not run; monthly CPI, wages and labour arrive with their own
  lags. Report the date of every number. The check is against today's
  date, not against the last row of another series.
- **The dashboard's `stale` flag is not the whole story.** Its `forecast`
  and `divergence` rows keep the *previous* meeting as `target_date` until
  the tracks are re-run (on 2026-11-15 they still pointed at 2026-10-23 with
  `stale = 0`). Compare `target_date` with the next meeting before quoting
  another track's forecast; a row for a past meeting is a score, not a
  forecast. `infl_exp_expected` likewise shows `stale = 0` while holding the
  previous month's wave.
- **`as_of` of expectations is the survey date** (the 14th), not the day
  the note was published, so it never equals the calendar date of the note.
  Compare months.
- **`v_cbr_macro.cbr_key_rate` hides holds and shifts dates.** It changes
  on the day a decision takes effect (the Monday after a Friday meeting; the
  same day for emergency moves). Use query 3 for anything counted per
  meeting. From 2024-02 `v_cbr_key_rate_press` gives the same directly, but
  `effective_from` is empty and the first row has no `decision_delta_bp`.
- **`signal` is empty for nine of the 23 press releases** (February–June
  2026 among them). An empty `signal` means "not extracted", not "no
  signal": read the last paragraphs of `v_cbr_key_rate_press_blocks` for
  that meeting.
- **Forecast ranges are stored twice per vintage** (see query 6). Without
  the `GROUP BY` every row doubles and a careless `count()` or `avg()` is
  wrong.
- **A range can be a point.** For the current year the Bank narrows
  `key_rate_avg` as the year runs out (14.6–14.6 in October 2026). By then
  it is rounding, not information: `rate_math.py avg` shows that neither a
  hold nor a cut in December leaves it. The binding constraint late in the
  year is next year's range — see SKILL.md step 6.
- **Seasonally adjusted annualised rate.** `base_ipc_saar` in the dashboard
  is built from two neighbouring months of core CPI; Rosstat publishes no
  seasonally adjusted CPI. For headline use `v_weekly_cpi_sa`
  (`weekly_cpi:saar`, with `partial`) and `v_weekly_cpi_rolling3m`, and say
  which was used. A partial month annualised from one or two weeks swings
  wildly — the first week can show under 1% against 6% over three months —
  so quote the three-month figure next to it. `v_weekly_cpi_sa`
  holds the last vintage only and `v_weekly_cpi_sa_vintage` starts on
  2026-11-09, so a replay before that date cannot recover the SA value
  known then: use raw weekly CPI levels (`Индекс потребительских цен
  (недельная оценка)`) and compute the month-to-date change yourself, and
  record the gap in `data_gaps.json`.
- **Budget series exist in two sources.** `Доходы, всего` and the balance
  are in both `minfin` and `minfin_mesyats`; filter by `source` or every
  sum doubles. There is no cumulative series — sum the months of the year.
- **Two balance-of-payments sources with different coverage and units**;
  never splice `cbr_bop_est` onto `cbr_bop_standard`.
- **Market strip is short and thin.** On 2026-11-15 it held two series:
  the nearest (open interest 3 926, one contract traded that day) and the
  next (open interest 2, no trades). `settle` of a series nobody holds is
  the exchange's model price, not a view. Treat a series with open interest
  under about 100 contracts as "no market price" and compare the path
  beyond it with the OFZ curve only in direction.
- **`v_ofz_signal` can lag the dashboard's OFZ rows** by a week; take the
  fresher of the two and give its date. Its `inflation_yoy` is stamped on
  the CPI publication day (Rosstat's exact date where known, the Bank's
  "Consumer price dynamics" release otherwise, end of M+1 as a fallback) —
  the May 2025 CPI appears on 2025-06-11, not on the first yield days of
  June. For an intraday as-of slice use `date < cutoff`: the row date is
  the publication day. `v_ofz_curve` holds the G-curve back to 2010 (over
  4 000 days); a result of a handful of rows means the filter was too
  narrow.
- **Provenance of event-calendar rows.** `category = 'cbr'` belongs to the
  importer: analyst notes such as `СД ЦБ РФ: база — −25 б.п. до 13,75%`
  were retired on 2026-11-16 and `v_events_calendar` hides them. Filter
  meetings by the exact title or `source = 'canonical'`. `created_at` is
  the row's first write, but for every row that existed before 2026-11-16
  it is the migration moment, so `created_at <= as_of` is meaningful only
  for replays after that date; before it, limit by `event_date`.
- **Household-expectations publication date.** `v_series_publication`
  dates each wave by the inFOM note from 2025-01 and by the later full
  commentary before that (2024-07-14 → 2024-07-29); the Bank's calendar
  starts on 2025-01-10. Use `published_at < cutoff` as the as-of filter and
  cross-check the month against `v_cbr_publication_calendar` (query 10).
  For a replay before 2025 a wave dated after the cutoff may in fact have
  been out: do not rescue it with an assumed date — drop it, and record it
  in `data_gaps.json`.
- **Bank ranges are revised, not wrong.** A next-year `key_rate_avg` range
  that looks impossible against today's rate is a real vintage the Bank may
  later raise. Report it, run `bound` on it, and say it would need a
  revision; that need is itself information about the next core meeting.
- **Enterprise monitoring loads late.** The wave the Bank publishes around
  the 14th–24th can be missing for weeks (October 2026 wave absent on
  2026-11-16); list it as stale rather than using the previous wave as
  current.
- **Channel tags.** Eleven slugs; `kind` in (`prose`, `note`, `risk_pro`,
  `risk_dez`); `method` = `regex` or `llm`, and `llm` rows come from three
  different `model`s with different coverage (only `deepseek-chat` reaches
  the latest meeting). Four regex rows and one LLM row have confirmed wrong
  polarity; `autonomous_factors` mixes tariffs, liquidity and regulation;
  `budget_impulse` sometimes tags foreign fiscal policy. Never add up rows
  across `method`s or `model`s — they are parallel annotations of the same
  text.
- **`meeting_date` versus publication date.** Summaries appear about two
  weeks after the meeting (`published`); a run made between the two must
  not use the summary of the meeting it is still waiting for.
- **Selftest rows in the accuracy view.** `v_ml_live_accuracy` contains
  synthetic `model = 'gate-selftest'` rows with invented forecasts; exclude
  them. `v_forecast_accuracy` is a different view (gold and other metrics,
  errors in percent) and has no key-rate tracks.
- **Forecast-log rows may be unresolved.** `actual` and `error_bp` are
  filled by the `forecast_resolve` importer; if `is_resolved = 0` after the
  meeting, resolve the previous forecast yourself from query 3 and say that
  the log row was still open.
- **Policy guidelines.** `published_at` is empty in `v_cbr_ondkp_forecast`;
  use `announcement_date` where `publication_status` is not `unknown`, the
  document year otherwise, and do not use them for point-in-time claims
  within a year.
- **Text before 2024.** The press-release, summary and press-conference
  corpus starts at 2024-02-16 (23 meetings) unless the pre-2024 text
  importer has been loaded; check `min(meeting_date)` before making
  statements about "how the Board worded it in 2014–2015". Decisions
  themselves go back to 2014 through query 3.
- **Neutral rate, output gap, term premium.** Nobody publishes any of them
  as a series. Use the Bank's stated long-run range for neutral and survey
  proxies for the gap, and call them proxies; for the term premium there is
  not even a proxy, so the OFZ curve is read in direction only.
- **Wages lag by two months and CPI by two weeks.** On 2026-11-15 the last
  wage figure was for September, the last monthly CPI for September, the
  last expectations survey for October. Anything that happened in October
  cannot be seen in wages yet; check the date before using a series as
  evidence of what a shock did.

## Further reading in the repository

- `docs/research/2026-11-14-key-rate-data-coverage-audit.md` — freshness and
  depth of every channel.
- `docs/research/2026-11-15-key-rate-channel-audit.md` and
  `docs/research/key-rate-channel-audit/channels.csv` — the eleven channels,
  twelve sub-mechanisms, sign conditions and what cannot be identified.
- `docs/KEY_RATE_CHANNELS.md`, `docs/KEY_RATE_MODEL.md` — channel registry
  and the statistical tracks this skill is compared with.
- `docs/research/2026-10-20-cbr-publication-calendar.md` — when each input
  is published.
