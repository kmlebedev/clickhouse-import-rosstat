# Data map: views, queries, traps

All queries go through `mcp__clickhouse__run_query` as `kimi_reader`
(read-only, `v_*` views only). View and column descriptions live in the
database: run `DESCRIBE v_<name>` or read the table and column comments
before relying on a column that is not listed here. The canonical registry
is `ARCHITECTURE.md` §6.3; if a view below does not exist on the instance,
say so in the report rather than substituting another.

## Where each block lives

| Block | View | What to take |
|---|---|---|
| One-row-per-signal overview | `v_key_rate_dashboard` | `contour`, `indicator`, `value`, `unit`, `as_of`, `target_date`, `flag`, `stale`, `probabilities`, `model`; contours `spot`, `fundamental`, `market`, `divergence`, `forecast`, `calendar` |
| Key rate history, RUONIA, FX, M2, credit, lending and deposit rates, core CPI | `v_cbr_macro` | long form `series, date, value`; series names are Russian titles or table names (`cbr_key_rate`, `cbr_ruania_ruo`, `cbr_currency_usd`, `М2 SA`, `Кредит экономике`, `Базовый ИПЦ`) |
| CPI monthly and weekly, labour, wages, GDP | `v_rosstat_macro`, `v_weekly_cpi_sa` | weekly seasonally adjusted aggregate has `partial` and `as_of` |
| Household expectations | `v_key_rate_dashboard` (`infl_exp_expected`), `v_cbr_macro` | history before 2023 may be absent on older instances |
| Business surveys | `v_cbr_survey` | `price_expectations_sa`, `business_climate_sa`, `capacity_utilization` |
| Budget | `v_minfin_budget` | monthly; no cumulative series — sum it; defence closed since 2022 |
| External | `v_cbr_bop`, `v_moex_market` | two BoP sources with **different units**; `brent_front` |
| Liquidity, money market | `v_cbr_liquidity`, `v_cbr_mirac` | structural surplus/deficit; MIACR by term |
| Market-implied rate | `v_keyrate_expectations` | `date, secid, series_name, decision_date, settle, close, volume, openposition, expected_change_bp` |
| OFZ | `v_ofz_signal`, `v_ofz_curve_compare` | `ofz_1y`, `ofz_2y`, `ofz_10y`, `key_rate`, spreads |
| Press releases | `v_cbr_key_rate_press`, `_blocks`, `_quarters` | keyed by `meeting_date` |
| Summaries of discussion | `v_cbr_mp_summary`, `_blocks`, `_channels` | keyed by `meeting_date`; published about two weeks after the meeting |
| Press-conference | `v_cbr_key_rate_pressconf`, `_text`, `_cues` | Governor's statement and Q&A |
| The Bank's forecast ranges | `v_cbr_mt_forecast` | `meeting_date, release_date, indicator, year, lo, hi` — one vintage per core meeting |
| Annual policy guidelines | `v_cbr_ondkp_forecast` | scenario ranges by year; many rows lack a publication date |
| Release calendar | `v_cbr_publication_calendar` | `publication_date, title, topic, period, planned` |
| Other tracks and their live accuracy | `v_ml_live_accuracy`, `v_forecast_accuracy` | `metric`, `track`, `stage`, `predicted`, `actual`, `error_bp` |
| What was known on a date | `v_point_in_time_asof(as_of='YYYY-MM-DD')`, `v_point_in_time_version` | covers the DIP bulletin only; `data_version` for the record |

## Starter queries

```sql
-- Preflight: everything in one screen, with staleness
SELECT contour, indicator, value, unit, as_of, target_date, flag, stale
FROM v_key_rate_dashboard ORDER BY contour, indicator;

-- Decision history for base rates: one row per rate change
SELECT date, value AS rate, value - lagInFrame(value) OVER (ORDER BY date) AS step_pp
FROM v_cbr_macro WHERE series = 'cbr_key_rate'
QUALIFY step_pp != 0 ORDER BY date;

-- Average rate realised so far this year (input to rate_math.py avg).
-- The series has no rows on non-working days: fill forward before averaging
-- or pass the change dates to the script and let it do the day count.
SELECT min(date), max(date), count() FROM v_cbr_macro
WHERE series = 'cbr_key_rate' AND toYear(date) = toYear(today());

-- Market strip: latest settle for each future meeting
SELECT decision_date, secid,
       argMax(settle, date) AS implied_rate,
       argMax(expected_change_bp, date) AS implied_change_bp,
       argMax(volume, date) AS volume, argMax(openposition, date) AS oi,
       max(date) AS as_of
FROM v_keyrate_expectations WHERE decision_date >= today()
GROUP BY decision_date, secid ORDER BY decision_date;

-- The Bank's ranges, latest vintage and the one before (revision direction)
SELECT meeting_date, indicator, year, lo, hi FROM v_cbr_mt_forecast
WHERE meeting_date IN (SELECT DISTINCT meeting_date FROM v_cbr_mt_forecast
                       ORDER BY meeting_date DESC LIMIT 2)
ORDER BY indicator, year, meeting_date;

-- Paragraphs of the last summary; read them, do not count them
SELECT block, seq, text FROM v_cbr_mp_summary_blocks
WHERE meeting_date = (SELECT max(meeting_date) FROM v_cbr_mp_summary_blocks)
ORDER BY block, seq;

-- Where the Bank spoke about temporary or one-off factors
SELECT meeting_date, channel, kind, text FROM v_cbr_mp_summary_channels
WHERE method = 'regex' AND (text ILIKE '%разов%' OR text ILIKE '%временн%')
ORDER BY meeting_date DESC LIMIT 60;

-- Has this month's household-expectations note been published?
SELECT publication_date, title FROM v_cbr_publication_calendar
WHERE title ILIKE '%инФОМ%' AND publication_date >= toStartOfMonth(today())
ORDER BY publication_date;

-- Releases due before the next meeting
SELECT publication_date, title, period FROM v_cbr_publication_calendar
WHERE publication_date BETWEEN today() AND today() + 45 ORDER BY publication_date;
```

Adjust column names to what `DESCRIBE` returns — the long-form macro views
share `series, date, value` but some carry `source`, `unit`, `frequency`.

## Traps already found in this data

- **Staleness is common on dev.** Daily series lag by a week or more when
  imports have not run; monthly CPI, wages and labour arrive with their own
  lags. Report `as_of` for every number. The check is against today's date,
  not against the last row of another series.
- **Seasonally adjusted annualised rate.** `base_ipc_saar` in the dashboard
  is built from two neighbouring months of core CPI; Rosstat publishes no
  seasonally adjusted CPI. For headline use `v_weekly_cpi_sa` and monthly
  CPI with your own three-month annualisation, and say which was used.
- **Channel tags.** Eleven slugs, `kind` in (`risk_pro`, `risk_dez`, …),
  `method` = `regex` or an LLM model name. Four regex rows and one LLM row
  have confirmed wrong polarity; `autonomous_factors` mixes tariffs,
  liquidity and regulation; `budget_impulse` sometimes tags foreign fiscal
  policy. Never add up rows across `method`s — they are parallel
  annotations of the same text.
- **`meeting_date` versus publication date.** Summaries appear about two
  weeks after the meeting; a run made between the two must not use the
  summary of the meeting it is still waiting for.
- **Market strip.** `settle` is the implied rate on `decision_date`; a
  series with no trades has a `close` that reflects nothing. Check `volume`
  and `openposition` before reading a gap of a few basis points as a view.
- **Forecast-log rows may be unresolved.** `actual` and `error_bp` are
  filled by the `forecast_resolve` importer; if it has not run since the
  meeting, resolve the previous forecast yourself from `cbr_key_rate` and
  say that the log row was still open.
- **Policy guidelines.** Most `v_cbr_ondkp_forecast` rows have no
  publication date; use the document year as the vintage and do not use
  them for point-in-time claims within a year.
- **Text before 2024.** The press-release and summary corpus starts at
  2024-02-16 unless the pre-2024 text importer has been loaded; check
  `min(meeting_date)` before making statements about "how the Board worded
  it in 2014–2015".
- **Neutral rate and output gap.** Nobody publishes either as a series.
  Use the Bank's stated long-run range for neutral and survey proxies for
  the gap, and call them proxies.

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
