# The four lenses

Each lens is a complete argument with its own evidence, its own failure mode
and its own output: a probability row for the next decision and a base path.
Write each as its advocate would. Sections: §1 the Bank's own reaction
function, §2 Volcker, §3 asymmetric shocks, §4 the over-tightening dissent,
§5 how to combine them.

## 1. Lens A — the Bank's own reaction function

**Claim.** The Board is predictable to anyone who takes its published
forecast and wording literally. It forecasts inflation returning to the 4%
target, publishes the average key rate consistent with that, and moves in
steps that keep it on that path unless the data push the forecast.

**Evidence to read, in this order.**

1. *The arithmetic of the published range.* `v_cbr_mt_forecast` gives the
   range for the average key rate in the current and next years. With the
   rate realised so far, compute which combinations of decisions over the
   remaining meetings keep the annual average inside the range:
   `rate_math.py bound` for the whole set of paths, `rate_math.py avg` for
   one path you want to show. Late in the year this pins the remaining
   decisions to within one step; early in the year it bounds the year-end
   rate. For next year's range, report what `bound` returns — the least
   total easing that fits, the range of year-end rates, and the step sizes
   it assumed — rather than a minimum inferred from a few schedules.
2. *The signal sentence* in the last press release
   (`v_cbr_key_rate_press_blocks`): what the Board says it will assess, and
   whether the wording changed against the previous release. A changed
   sentence is information; an unchanged one is the default.
3. *The options on the table* in the last summary
   (`v_cbr_mp_summary_blocks`, the decision block): which alternatives were
   discussed, what arguments were given for each, and which conditions the
   Board named for a larger or smaller step. These conditions are the
   Board's own decision rule — check each against the data that arrived
   since.
4. *The balance of risks* and whether the Board called any current price
   pressure temporary (this feeds the one-off ledger).
5. *Data against the Bank's forecast.* Is inflation running above or below
   the path implied by the Bank's year-end range? Use the seasonally adjusted
   annualised rate, which is what the Board cites, not the year-on-year
   figure. The same for credit, expectations and the output-gap proxies.
6. *Meeting type.* Forecast ranges move only at core meetings; an interim
   meeting that departs from the path implied by the ranges is rare and has
   to be argued from a specific data surprise.

**Typical behaviour to hold in mind.** The Board prefers a sequence of equal
steps to a single large one, pauses when expectations rise for two
consecutive surveys, and explains reversals in advance through the signal.
Verify each of these against step 5 of the workflow (base rates) for the
current regime rather than taking them on trust.

**How this lens fails.** It extrapolates the Board's plan. The Board's own
forecasts have been revised upward more often than downward since 2021; a
lens that only reads the plan misses the meeting where the plan changes.
Compare earlier vintages in `v_cbr_mt_forecast` to see the size and
direction of past revisions.

## 2. Lens B — Volcker

**Claim.** Inflation ends when the public stops expecting it, and the public
stops expecting it only after policy has stayed visibly tight for longer
than is comfortable. The dangerous moment is not the peak rate but the
first easing: Volcker's Fed eased in the spring of 1980 as activity fell,
inflation and expectations came back within months, and the second
tightening was harsher than the first would have needed to be.

**What Volcker would look at.**

- *The real rate against what people expect, not against the target or the
  Bank's forecast.* Key rate minus household expected inflation, and key
  rate minus firms' price expectations. If that real rate is falling
  because nominal cuts outrun the decline in expectations, policy is easing
  faster than the nominal path shows.
- *Money and credit quantities.* M2 and credit growth relative to nominal
  income growth. Volcker distrusted the interest rate as a gauge of
  tightness and watched the aggregates; rapid deposit and credit growth
  alongside a "tight" rate means the rate is not tight.
- *Whether expectations have actually broken.* A fall for one or two
  surveys is not a break. Look for expectations below their range of the
  previous two years and falling while actual inflation is temporarily
  higher — the sign that they have stopped following current prices.
- *Fiscal stance.* A budget deficit above plan is demand the rate must
  offset; Volcker said repeatedly that monetary policy was carrying a load
  that fiscal policy should share.
- *Wages.* Wage growth above productivity plus the target is, in this view,
  inflation that has not yet shown up in prices.

**What the lens outputs.** A normative path — typically later and slower
cuts, or a hold — and, for the forecast itself, two things: the probability
that the Board pauses because it shares these worries (it often does; its
own wording about "the need to maintain tight conditions for a long
period" is this lens speaking), and the probability of a reversal within
three meetings if it eases against them.

**How this lens fails.** It reads every pause in disinflation as failure of
nerve and has no way to recognise over-tightening until credit stress is
obvious. The US in 1980 had demand-led inflation with indexed wages; where
price growth comes from administered prices and import costs, the same
medicine costs more output for less disinflation. That objection is lens D.

## 3. Lens C — asymmetric shocks

Source: Sber Centre for Macroeconomic Research, working paper on the
asymmetry of price shocks and the mirrored asymmetry of targets, November
2026 —
https://www.sberbank.com/common/img/uploaded/news/2026/cmi/assimetriya.html.
Re-read the paper at the start of a run if the page is reachable; the
summary below was taken from a partly garbled page render and the figures
should be confirmed against the original before they are quoted in a
report.

**Claim.** "One-off" price shocks are no longer one-off. Since 2021 supply
shocks in Russia have been larger (variance roughly 1.5–1.8 times that of
2017–2019), more frequent (large shocks around two or more a year against
well under one), and skewed upward. In 2026 they arrived every four to five
months — VAT in January, fuel in June, utility tariffs in October (indexed
above plan) — and the Bank cited temporary factors at every meeting of the
year. Household expectations now respond: about +0.6 pp six months after a
shock that adds 0.44 pp to monthly inflation, against no measurable
response in 2017–2019.

**The four-question test.** A central bank can look through a price shock
when all four hold:

1. the shock is small;
2. it does not recur;
3. expectations are anchored;
4. it does not pass into wages.

The paper argues the first three have not held in Russia since 2021. Apply
the test to every row of the one-off ledger with current data: size
relative to monthly inflation, whether a shock of the same kind occurred in
the previous twelve months, what expectations did in the two surveys after
it, what wages did after it.

"After it" is the whole point of questions 3 and 4. Wages arrive with a
lag of about two months, so for a recent shock there is usually no
post-shock wage figure yet: the honest answer is `unknown — latest data
<month>, before the shock`, and wage growth before the shock goes in the
evidence table as labour-market background. The same holds for
expectations when no survey has been taken since. A row can still be
classified *react* on questions 1–3; say that it rests on those.

**Consequences for the forecast.**

- While expectations stay sensitive, the Board reacts to any price shock
  whatever its nature — so a factor the Board itself labels temporary still
  slows or stops cuts. Do not assume "temporary" means "ignored".
- Because shocks are skewed upward, hitting 4% on average requires policy
  set as if aiming lower in the shock-free baseline (the paper's phrase is
  aiming at 3% to hit 4%). The path is therefore higher than the Bank's
  shock-free forecast implies; the paper puts the peak effect of shocks on
  the key rate at about 1–1.5 pp.
- For the trajectory: add scheduled and likely shocks to the calendar
  (tariff indexation dates, tax changes already legislated, excise changes)
  and mark the meetings that follow each one as likely pauses.

**How this lens fails.** It assumes the post-2021 shock regime persists. A
year without a large administered-price shock, or expectations that stop
reacting, would make it too hawkish. It is also an estimate from a short
sample with a structural break in 2022; treat the magnitudes as orders of
size.

## 4. Lens D — the over-tightening dissent

**Claim.** A large share of current price growth comes from supply and
administered prices, which the rate cannot reduce, while the rate is doing
visible damage elsewhere. The Board will be pushed to cut faster than it
signals — by the data on activity and credit quality, not by argument.

**What this lens looks at.**

- *Composition of inflation.* How much of the last three months' seasonally
  adjusted growth is tariffs, taxes, fuel and food against demand-sensitive
  services and non-food goods. If the demand-sensitive part is already at or
  below 4% annualised, the lens says policy has done its work.
- *The cost channel.* Interest expense as a cost that firms pass into
  prices; high rates on working-capital loans raising, not lowering, price
  pressure in sectors with thin margins.
- *Credit quality and payment discipline.* Payment delays, restructurings,
  the share of floating-rate corporate debt, problem loans, the consumer
  credit contraction. The summaries of late 2024 already recorded payment
  delays and postponed projects.
- *Activity.* Business climate, capacity utilisation, GDP, the gap between
  civilian and state-financed sectors. A falling civilian sector under an
  unchanged headline is the pattern to look for.
- *The level of the real rate in history.* Compare the current real rate
  with its own distribution since 2013 and with any estimate of neutral the
  Bank has published (it gives a range for the long-run neutral rate in the
  policy guidelines). A real rate in the top decile of its history for many
  months is the dissent's main exhibit.
- *Subsidised lending.* The larger the share of credit insulated from the
  key rate, the harder the rate hits the unsubsidised remainder.
- *A Taylor-rule cross-check.* r* + expected inflation + a response to the
  gap between expected inflation and target, with the Bank's own neutral
  range. Report the range it gives, not a point; the dashboard already
  carries `taylor_vs_market_bp`.

**What the lens outputs.** A faster path and a fatter downside tail (a
larger step than the signalled one). For the forecast, the question is
whether these arguments have started to appear in the Board's own summary —
once the Board lists financial-stability or activity concerns among its
arguments for a larger step, this lens has moved from dissent to input.

**How this lens fails.** It has been early in every cycle. Activity data in
Russia since 2022 have repeatedly looked weak while the labour market
stayed tight, and the Board has explicitly ranked returning inflation to
target above smoothing output.

## 5. Combining the lenses

There is no formula, and pretending to one hides the judgment. State the
weights in words and numbers and defend them with the current evidence:

- For the **next decision**, lens A carries most of the weight by default
  (the Board follows its own signal far more often than not). Reduce its
  weight when it is a core meeting, when the data since the last meeting
  contradict the Bank's forecast in the same direction on at least three
  channels, or when the signal sentence was changed.
- For the **path beyond two meetings**, the plan in lens A is weakest —
  this is where B, C and D earn their place. Use the one-off ledger to
  decide between them: rows classified *react* support C and B, rows
  classified *look through* with weak activity support D.
- For the **tails**, ask each lens for the outcome it considers most
  under-priced and keep at least 5% on it.
- Where two lenses give opposite paths and the evidence does not separate
  them, widen the low/high band instead of averaging the base path. An
  average of "cut 200 bp" and "hold" is a path nobody holds.

Record in the report which lens was closest to the outcome at each resolved
meeting. After six or more resolved meetings that tally — not any prior
conviction — is the main guide to the weights.
