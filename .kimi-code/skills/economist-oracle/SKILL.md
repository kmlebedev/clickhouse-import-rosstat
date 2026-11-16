---
name: economist-oracle
description: Use for the monthly Bank of Russia key-rate call in clickhouse-import-rosstat — the probability of a cut, hold or hike at the next Board meeting, the key-rate trajectory for the following meetings and years, and how that view differs from what the market prices. Trigger on "forecast the key rate", "what will the CBR do", "прогноз ключевой ставки", "траектория ставки", "снизит/сохранит/повысит", "monthly rate call", "update the rate forecast", "score last month's rate forecast", or any request to reason about the next CBR decision from the ClickHouse views, even when the user does not name this skill. Not for writing a publication (editor-in-chief) and not for training or backtesting ML models (bi-ml-models).
type: prompt
whenToUse: When the task is a judgment-based forecast of the next Bank of Russia key-rate decision and the rate path, built from the v_* views through the ClickHouse MCP, argued through several competing schools of thought and compared with market pricing; also when an earlier forecast of this kind has to be scored or revised
---

# Economist-oracle: the monthly key-rate call

Role: the economist who, once a month, says what the Bank of Russia will do
with the key rate, where the rate goes afterwards, and whether that differs
from what the market prices.

## Why this is a reasoning skill and not a model

The Board meets eight times a year. The text layer covers 23 meetings, the
current policy regime has seen one full tightening-and-easing cycle, and
there are eleven transmission channels plus one-off factors that never repeat
in the same form (a VAT rise, a fuel shock, a tariff indexation above plan).
A fitted model has more parameters than observations, so it either overfits
or collapses to "no change". The statistical tracks (`taylor`,
`ordered_logit`, `ml`) live in `bi-ml-models` and stay there.

What a small sample cannot estimate, a reader can still reason about: the
Bank publishes its reaction function in words and in forecast ranges, and
each school of monetary thought makes a different, checkable claim about what
the Bank will do next. This skill makes those claims explicit, turns each
into probabilities, and keeps score. The value is not in being right once —
it is in a dated, scored record of where the forecast differed from market
pricing and who turned out right.

## What one run produces

1. A probability distribution over the next decision (step size in basis
   points, not just the direction).
2. A trajectory: the rate after each of the next eight meetings (low / base /
   high) and the implied annual averages, comparable with the Bank's own
   ranges.
3. Four separately argued views — the Bank's own, Volcker's, the
   asymmetric-shock view, and the over-tightening dissent — and a synthesis
   that says how much weight each got and why.
4. The gap to market pricing in basis points, and what that gap would be
   worth if the forecast is right and cost if it is wrong.
5. A score for the previous run and a short note on what changed the view.

Files, by default: `docs/forecasts/key-rate/YYYY-MM/forecast.md` and
`forecast.json` next to it (template and schema in
[references/report-template.md](references/report-template.md)). The run date
is in the file, so a second run in the same month writes
`forecast-YYYY-MM-DD.md` instead of overwriting.

## When to run

The run is anchored to one release, not to a calendar number: **the first
working day after the inFOM quick note on household inflation expectations**
("Оперативная справка инФОМ" in the Bank's calendar,
https://www.cbr.ru/calendar/). Expectations are the input the Board itself
treats as decisive for pauses, and they are the last of the monthly inputs
to arrive.

What the Bank's calendar for January 2025 – December 2026 shows:

| Input | Published on (day of month) | Available on the run day? |
|---|---|---|
| Monetary conditions and transmission | 7–20 | yes |
| Rosstat monthly CPI; the Bank's "Consumer price dynamics" | about 10–14; 12–23 | yes (one outlier on the 30th) |
| Enterprise monitoring (firms' price expectations) | 14–24, rarely 26 | yes in most months |
| **inFOM quick note (household expectations)** | **16–22; in January 27–29** | **the anchor** |
| Full commentary on expectations | 24–29 | no — adds text, not the number |
| Credit to the economy and money supply | 19–29 | usually no — use the previous month and say so |

So the run falls on about the **20th–23rd** (in January about the 28th–30th).
If a scheduler needs a fixed number, use the **23rd** (January: the 30th),
moved back to the previous working day when it is a weekend — and moved
earlier to the day after the inFOM note whenever a Board meeting falls on or
before the 23rd (meetings of 20 March, 19 June, 23 October and 18 December
2026 were all like that: the note came out two days before the decision).

Two consequences to handle:

- **Check that the wave is in the database, not just published.** In the
  preflight compare the month of the last household-expectations point with
  the `period` of the latest note in `v_cbr_publication_calendar` (query 10
  in the data map; the point is dated by the survey, the 14th, so the dates
  themselves never match). If the note is out but the wave is not loaded,
  ask the owner to run the importer. Do not decide "not yet published" from
  `v_series_publication.published_at`: for household expectations it
  carries the later full commentary, about a week after the inFOM note
  (2026-07-21 → 2026-07-28), and would throw away the very wave this run is
  anchored on.
- **A run before the note is still a run.** When the user asks for the call
  before this month's note is out, do it — on last month's wave — and say
  so in the first line of the report, with the date the note is due. That
  note then becomes the natural "what would change the call" candidate.
- **Pre-meeting refresh.** When the meeting is more than ten days after the
  monthly run (for example the meetings of 13 February and 11 September
  2026, which preceded that month's note), do a short refresh two working
  days before the meeting: re-pull weekly CPI, the futures strip and
  anything released since, and either confirm the probabilities or revise
  them with a stated reason. The refresh is a new dated file, and both are
  scored.

Never run inside the meeting day after 13:30 Moscow time, and do not use
the decision itself or anything published after it.

## Ground rules

These exist because each one has already gone wrong in this repository or is
the classic way a rate forecast fools its author.

- **Numbers come only from the `v_*` views through the ClickHouse MCP**
  (`mcp__clickhouse__run_query`, user `kimi_reader`, read-only). No `curl` to
  ClickHouse, no reading `~/.config/rosstat/env`, no DDL or inserts. If the
  MCP is not connected, say so and stop after the preflight — a forecast
  built from memory of old numbers looks the same as a real one and is worse
  than none. A dated snapshot the user hands over is acceptable if the report
  says, at the top, that it was used and how old it is.
- **Use only what was published before the forecast date.** Record the
  latest date of every series used. A series flagged `stale` in
  `v_key_rate_dashboard` is reported as stale, not silently used as current;
  the audit of 2026-11-14 found 19 of 24 dashboard indicators stale on dev.
  The flag misses some cases — a row can show `stale = 0` and still describe
  the previous meeting or the previous month — so judge each number by its
  own date against the run date.
- **Read the Bank's paragraphs; do not count them.** Channel-tag frequency in
  `v_cbr_mp_summary_channels` is not a causal weight, and the tags contain
  known polarity errors (see the channel audit of 2026-11-15). Use the tags
  to find the paragraph, then read the paragraph in
  `v_cbr_mp_summary_blocks`.
- **Separate "will" from "should".** The forecast is of what the Board will
  do. Volcker and the dissent say what it should do. A "should" view moves
  the forecast only through a stated mechanism — for example, "if the Board
  eases against this view, the probability of a reversal within three
  meetings rises".
- **Every statement is observed, computed or assumed — and says which.**
  A first evaluation of this skill passed all 22 format checks and still
  contained six wrong arguments (issue #616), all of one kind: a sentence
  that sounded like evidence and was not. So before a claim goes into the
  report, know its status. *Observed*: a number from a view with its date.
  *Computed*: the method is shown and anyone can rerun it. *Assumed*:
  labelled as an assumption in the sentence itself, and nothing is
  concluded from it that the reader could mistake for a finding. The six
  ways this went wrong are the checklist in step 9.
- **Probabilities, in 5-point steps, summing to 100.** A forecast of "hold,
  probably" cannot be scored. Never give 0 or 100 to an outcome the Board has
  delivered at least once in a comparable situation.
- **No investment advice.** The value section describes what a view implies
  for rate-sensitive instruments in general and keeps a paper record. It
  does not tell anyone what to buy or how much.

## Workflow

Work through the steps in order; each feeds the next. The queries, the views
and their known traps are in [references/data-map.md](references/data-map.md)
— read it before the first query.

### 1. Preflight

Establish the run date, whether this month's inFOM wave is loaded (see
"When to run"), the current rate, the dates of the next eight meetings
(`v_events_calendar`, query 2) and whether the next one is a "core" meeting
(with a new medium-term forecast: February, April, July, October) or an
interim one. The Board changes its forecast
ranges only at core meetings, which makes large surprises more likely there.
List which inputs are stale. Read the previous `forecast.json` if one exists.

### 2. Score the previous run

If the meeting the previous run targeted has happened, compute its score
before forming a new view — doing it afterwards invites explaining the miss
away. Use `scripts/rate_math.py score` (Brier score of this forecast, of the
market-implied distribution recorded at the time, and of the naive
"no change" forecast). Write two or three sentences on what the miss or hit
teaches; if the miss came from one lens, say which.

### 3. Evidence pack

Pull one compact table per block; keep every number with its date and view.

- **Prices:** headline and core inflation — year on year and the seasonally
  adjusted annualised rate over the last one and three months; weekly CPI
  since the last monthly print.
- **Expectations:** households (`infl_exp`), firms' price expectations,
  and the break-even implied by the OFZ curve where available.
- **Demand and labour:** credit to the economy, M2, deposits, unemployment,
  wages, capacity utilisation and business climate as output-gap proxies.
- **Budget and external:** federal balance against plan, oil and gas
  revenue, the rouble, Brent, the balance of payments.
- **Monetary conditions:** key rate, RUONIA and its spread, lending and
  deposit rates, the real rate computed three ways (against current
  seasonally adjusted inflation, against household expectations, against the
  Bank's forecast).
- **The Bank's own words and numbers:** the signal sentence and risk balance
  from the last press release, the options discussed in the last summary,
  the medium-term forecast ranges (`v_cbr_mt_forecast`) and the annual policy
  guidelines (`v_cbr_ondkp_forecast`).
- **The market:** the KEYRATE futures strip (`v_keyrate_expectations`), the
  OFZ curve and slope (`v_ofz_signal`).

### 4. One-off ledger

List every factor the Bank or the data calls temporary, one-off or
administrative (tax changes, tariff indexation, fuel, harvest, a base
effect, a currency move). For each, answer the four questions in
[references/lenses.md](references/lenses.md) §3 — is it small, does it
recur, are expectations anchored, does it pass into wages — and classify it
as *look through*, *react*, or *already reacted*. Give each answer the date
of the observation it rests on, and answer `unknown` where the only data
predate the shock — a classification can still be argued from the other
questions, but an untested one must not be counted as failed. This ledger
is the single most important input: the three lenses disagree mainly about
these rows.

### 5. Base rates

Before any argument, write down what history alone says: from the
per-meeting decision table (query 3 in the data map — the raw `cbr_key_rate`
series shows only changes and would hide every hold), the distribution of
step sizes following the same previous move (cut after cut, hold after cut,
hold after two holds, and so on), the typical length of pauses, and how
often the Board reversed direction within three meetings. Leave emergency
meetings out of the counts and say how many meetings each number rests on. This is the prior. The lenses move it; if the final forecast
ends far from it, the report must say what evidence justified the distance.

### 6. Four lenses

Read [references/lenses.md](references/lenses.md) and argue each lens on its
own terms, as its best advocate would, before comparing them. Each lens
ends with its own probability row and its own base path. Arguing them
separately matters: a blended first draft quietly drops whichever view is
least comfortable.

| Lens | Question it answers | Carries most weight for |
|---|---|---|
| A. The Bank's own reaction function | Given what the Board said and forecast, what is it set up to do? | The next decision |
| B. Volcker | Is policy tight enough to break expectations, and is it being eased too early? | Reversal risk, length of the path |
| C. Asymmetric shocks (Sber CMI, Nov 2026) | Will "one-off" shocks keep arriving and force a reaction regardless of their nature? | Level of the path, terminal rate |
| D. Over-tightening dissent | Is the rate hitting supply and financial stability rather than inflation? | Speed of cuts, downside tail |

For lens A, run the arithmetic check with `scripts/rate_math.py avg`: the
Bank publishes a range for the average key rate of each year; given the
rate realised so far, only some paths over the remaining meetings keep the
average inside it. Through most of the year this turns the current-year
range into a hard constraint on the next one or two decisions and is the
strongest single piece of evidence available.

Check that the constraint actually binds before leaning on it. With one
meeting left the current-year range has usually collapsed to a point that
both a hold and a cut satisfy (the same rounded average either way) — then say
it is uninformative and move the same check to next year's range: run
`bound` for the next year without `--ytd-*`, with `--rate` set to the rate
each December outcome would leave in force on 1 January and all of next
year's meetings listed. It enumerates every path over the step sizes the
Board uses and reports how many stay inside and the least total easing
among them. That shows how much easing the Bank's own forecast leaves room
for, and therefore how urgent a December move is. Timing matters as much
as size — early steps move the average far more than late ones — which is
why trying a few schedules by hand misleads.

### 7. Synthesis

Combine the lenses into one distribution and one path. State the weights and
the reason for them in a sentence each — as a default, lens A dominates the
next decision, while B, C and D shape the path beyond two meetings and the
tails. Then check these:

- the path is internally consistent (the annual average of the low, base
  and high paths, from `rate_math.py avg`, is stated for every year the
  path touches and compared with the Bank's range);
- the distance from the base-rate prior is explained;
- the mode follows the direction of the Board's last signal, or the report
  names the condition from the last summary that the new data meet
  (lenses.md §1; a hypothesis under test — say so in the report);
- lenses B and C, which argue the same side, do not outweigh D by default
  (lenses.md §5);
- the single observation that would most change the call before the meeting
  is named, with its publication date from `v_cbr_publication_calendar`;
- nothing that supports the call was made for a different meeting. Another
  track's forecast, an old futures price or last run's own call whose
  `target_date` is not the meeting being forecast is history — it belongs
  in the score, and repeating it here as "the tracks also sit at 13.9"
  counts a resolved number as fresh independent support.

### 8. Value against the market

Follow [references/value.md](references/value.md). In short: convert the
futures strip into an implied expected rate per meeting, take the difference
with the forecast's expected rate, and report it in basis points. A gap
under 25 bp is "no disagreement" — say so plainly; most months the honest
finding is that the market already prices the view. Where the gap is
larger, describe what it implies for the main rate-sensitive exposures and
record a paper position so the claim can be checked later. The strip is
short — usually one series with real open interest — so beyond it there is
no market price to disagree with: leave the "Market" cell of later meetings
empty rather than filling it from a series nobody holds.

### 9. Audit the arguments, then write the record

Reread the draft once as a hostile reviewer, looking only for these six
things. Each is a real defect found in this skill's own output; a report
can have every table filled and still fail here.

1. **"At least", "only if", "requires", "impossible".** A statement about
   all paths needs all paths checked. A handful of `rate_math.py avg` runs
   shows what those schedules do, nothing more: four steps of 25 bp spread
   over the year looked like the minimum for 2027 until three early steps
   turned out to fit too. Run `rate_math.py bound` and quote its result
   with its conditions (which meetings, which step sizes), or write "of the
   schedules checked".
2. **Result if versus result on average.** What a position makes if the
   Board holds and what it makes in expectation over the whole distribution
   are different numbers; take both from `rate_math.py paper` and label
   each. Every probability quoted in prose must be the one in the table and
   in `forecast.json` — "a cut" (all negative steps) is not "−25 bp". With
   `direction: none` there is no position and its result is zero.
3. **Cause before effect.** An observation supports "the shock passed
   into X" only if it is dated after the shock. September wages say
   nothing about the pass-through of an October tariff rise; they are
   background. Until post-shock data exist the cell is `unknown`, or a
   hypothesis called a hypothesis.
4. **The right meeting.** See the last check of step 7.
5. **Numbers that came from nowhere.** A term premium, a neutral rate, an
   elasticity: if no view holds it and no computation produced it, it is an
   assumption. Label it, give the range, and do not derive "the market
   agrees with our path" from it — an assumed premium subtracted from a
   yield manufactures agreement. Beyond the liquid futures series the
   comparison with the curve is in direction only.
6. **"High by historical standards".** A rank needs the distribution:
   sample, period, method, result (query 13 in the data map does it for the
   real rate). Without it, give the level and drop the adjective.

Fix what the audit finds — downgrade the claim, or do the computation —
and note in "Limits of this run" what stayed an assumption.

Then fill the template, write `forecast.json`, and print the ingest payload for
the `cbr_rate:llm:shadow` track (fields in the template). Do not POST it:
writing to `forecast_log` goes through the ingest endpoint and its owner
decides when a track is recorded and at which stage.

Finish by telling the user, in this order: the call in one sentence with its
probability, the gap to the market, the one thing that would change the
call, and which inputs were stale.

## What a good run looks like

A reader should be able to disagree with it precisely. That means every
probability traces to a named piece of evidence, every lens could have been
written by someone who believes it, the one-off ledger shows where the
lenses part ways, and the report says what would prove it wrong and when
that will be known. If the four lenses all agree and match the market, the
report is short and says so — manufacturing a contrarian view to look useful
destroys the track record that gives the exercise its value.

## Helper

`scripts/rate_math.py` (standard library only; `avg`, `bound`, `paper`,
`score`):

```bash
python3 .kimi-code/skills/economist-oracle/scripts/rate_math.py avg \
  --year 2031 --ytd-avg 9.68 --ytd-through 2031-11-15 --rate 9.0 \
  --meeting 2031-12-19:8.75 --range 9.6:9.6
# a year that has not started: no --ytd-*, --rate is the rate on 1 January
python3 .kimi-code/skills/economist-oracle/scripts/rate_math.py avg \
  --year 2032 --rate 8.75 --meeting 2032-02-13:8.5 --meeting 2032-03-19:8.25 \
  --range 7.5:8.5
# every path over these meetings and step sizes: what the range really requires
python3 .kimi-code/skills/economist-oracle/scripts/rate_math.py bound \
  --year 2032 --rate 9.0 --meeting 2032-02-13 --meeting 2032-03-19 \
  --meeting 2032-04-30 --steps=-50,-25,0 --range 7.5:8.5
# one contract: result per outcome, and the expectation, kept apart
python3 .kimi-code/skills/economist-oracle/scripts/rate_math.py paper \
  --entry 8.94 --rate 9.0 --probs '{"-25":20,"0":75,"25":5}'
python3 .kimi-code/skills/economist-oracle/scripts/rate_math.py score \
  --probs '{"-100":0.2,"-50":0.6,"0":0.2}' --market '{"-50":0.5,"0":0.5}' \
  --outcome -50
```

The numbers in these examples are invented on purpose — real ones would tell
a replay of a past meeting how it ended. They show the argument format; take real values
from the views (`--ytd-avg` from query 4 of the data map — a calendar-day
average, not `avg()` over the working-day rows). `--meeting` takes the
meeting date; the script applies the change three days later, when it takes
effect.
