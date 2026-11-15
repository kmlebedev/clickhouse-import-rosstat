# Value against the market

A rate forecast is worth something only where it differs from what is
already priced and turns out closer to the outcome. This section measures
both. It describes exposures and keeps a paper record; it does not
recommend trades or sizes to anyone.

## 1. What the market prices

From `v_keyrate_expectations`, for each future meeting with a traded
series: `settle` is the market's expected rate after that meeting.

- **Expected change at the next meeting** = `settle` − current rate, in bp.
- **Implied two-outcome probability.** If the expected change lies between
  two neighbouring step sizes the Board uses (say 0 and −50), the weight on
  the larger step is `expected change / step`. This is a simplification —
  the market may be pricing three outcomes — so report it as "consistent
  with", and prefer comparing expected rates, which needs no assumption.
- **Liquidity check.** Note `volume` and `openposition`. A thin series
  gives a noisy price; widen the "no disagreement" band to 40 bp when open
  interest is a few hundred contracts. A series with open interest under
  about 100 (the second series of the strip often has single digits and no
  trades) carries the exchange's model price, not anyone's view: report it
  as "no market price" and compute no gap against it.
- **The market at the time, for scoring.** The previous run's
  `forecast.json` holds the market row it saw. If it is missing, rebuild it
  from `v_keyrate_expectations` with `decision_date` = that meeting and
  `date` ≤ that run date, and say it was reconstructed.
- **Further out,** use the later series of the strip where they trade, and
  the OFZ curve (`ofz_1y`, `ofz_2y` against the key rate) as a rough check
  of the path: a one-year yield below the key rate prices cuts over the
  year, but it includes a term premium, so do not read it to the basis
  point.

## 2. The gap

For each meeting on the strip:

`gap_bp = (forecast expected rate − market implied rate) × 100`

where the forecast expected rate is the probability-weighted rate from the
synthesis. Negative gap: the forecast is more dovish than the market.

| |gap| | Reading |
|---|---|
| under 25 bp | No disagreement. Say so. The dashboard uses the same threshold for the statistical tracks. |
| 25–75 bp | A disagreement of about one step in probability terms. Worth recording. |
| over 75 bp | Either a real edge or a mistake. Re-check the inputs and the meeting date mapping before writing it down, then state which lens produces the gap. |

Also show the gap for the other tracks (`taylor_vs_market_bp`,
`ordered_logit_vs_market_bp` in the dashboard) so the reader sees whether
this forecast disagrees with the market alone or with the models too — but
only when their `target_date` is the meeting being forecast. Between
meetings those rows still describe the previous one (and the divergence
values are empty); then report how the tracks did last time from
`v_ml_live_accuracy` instead and say they have not been re-run.

## 3. What a gap implies, in general terms

State direction and mechanism, not a recommendation:

- **KEYRATE futures.** The cleanest expression: one percentage point of
  rate equals 1 000 ₽ per contract, and the series settles on the rate
  announced at the meeting. A forecast 50 bp below the strip is worth
  500 ₽ per contract if exactly right.
- **Fixed-coupon OFZ.** A lower-than-priced path raises prices roughly by
  duration × the change in yield; the long end also depends on the term
  premium and supply, which this forecast says nothing about.
- **Floaters and money-market instruments** gain from a higher-than-priced
  path; fixed-term deposits and fixed-rate borrowing are the household and
  corporate versions of the same choice.
- **The rouble and equities** respond to the rate through channels with
  ambiguous sign; do not claim an implication for them from the rate view
  alone.

Always pair it with the losing case: what the same exposure loses if the
market's outcome occurs, and the forecast's own probability of that
outcome. A view with 60% confidence loses four times in ten.

## 4. The paper record

In `forecast.json`, under `paper`, record for the next meeting only:

- `instrument`: the KEYRATE series (`secid`) for that meeting;
- `entry`: its `settle` on the run date and that date;
- `direction`: `long` if the forecast expected rate is above the strip,
  `short` if below, `none` if |gap| is under the threshold;
- `size`: always 1 contract — the record measures the forecast, not
  position sizing.

On resolution (step 2 of the next run), the result is
`(announced rate − entry) × 1000 ₽ × (+1 long / −1 short)`, added to a
cumulative total in the report. Costs and margin are ignored and the report
says so.

## 5. Scoring

`scripts/rate_math.py score` returns, for a resolved meeting:

- the Brier score of this forecast (0 is perfect; lower is better);
- the Brier score of the market's distribution as recorded at the time;
- the Brier score of the naive forecast (100% on "no change");
- skill against each: `1 − own / reference`. Positive means the forecast
  beat the reference.

Keep the running table in the report. Three things are worth saying about
it honestly: eight meetings a year means a full year of records is still a
small sample; beating "no change" is easy in a cutting cycle and proves
little; beating the market's distribution over a dozen meetings is the only
result that indicates value.
