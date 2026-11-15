# Report template and record schema

## forecast.md

Keep it to what a reader needs to check the call. Tables for numbers, prose
for arguments. Every number carries its date; every quotation carries its
`meeting_date`.

```markdown
# Key rate: forecast for the meeting of <DD Month YYYY>

Run date <YYYY-MM-DD> · current rate <x>% · <core|interim> meeting ·
data version <as in the payload>
Stale inputs: <list, or "none">

## Call
<One sentence: the most likely decision with its probability, and the
year-end rate.> <One sentence: the gap to the market.> <One sentence: what
would change the call, and when it is published.>

## Decision probabilities
| Step, bp | <-100> | <-50> | 0 | <+…> | Expected rate |
|---|---|---|---|---|---|
| Base-rate prior | | | | | |
| A. Bank's reaction function | | | | | |
| B. Volcker | | | | | |
| C. Asymmetric shocks | | | | | |
| D. Over-tightening dissent | | | | | |
| **Forecast** | | | | | |
| Market-implied | | | | | |

Weights: A <..>, B <..>, C <..>, D <..> — <one sentence of reasons each>.

## Trajectory
| Meeting | Low | Base | High | Market | Gap, bp |
|---|---|---|---|---|---|
(eight meetings)

| Year | Average, base path | Bank's range | Inside? |
|---|---|---|---|

## Previous forecast: score
<Outcome, Brier scores (own / market / naive), paper result, which lens was
closest, what it teaches. "First run" if none.>
| Meeting | Outcome | Own | Market | Naive | Closest lens | Paper, ₽ |
|---|---|---|---|---|---|---|
(running table)

## Evidence
(one compact table per block: prices, expectations, demand and labour,
budget and external, monetary conditions, the Bank's words and numbers,
the market — indicator, value, date, view)

## One-off ledger
| Factor | Size | Recurred in 12m? | Expectations after | Wages | Bank's label | Classification |
|---|---|---|---|---|---|---|

## The four views
### A. The Bank's own reaction function
### B. Volcker
### C. Asymmetric shocks
### D. Over-tightening dissent
(each: the argument in its advocate's voice, the evidence it rests on, its
probability row and base path, what would prove it wrong)

## Synthesis
<Where the lenses part ways, how the ledger decided between them, distance
from the base-rate prior and why.>

## Value against the market
<Gap by meeting, reading of it, general implications with the losing case,
paper record.> Not investment advice.

## What to watch before the meeting
| Date | Release | Would move the call if |
|---|---|---|

## Limits of this run
<Stale or missing inputs, what was not checked, proxies used.>
```

## forecast.json

```json
{
  "run_date": "YYYY-MM-DD",
  "target_meeting": "YYYY-MM-DD",
  "meeting_type": "core | interim",
  "current_rate": 0.0,
  "data_version": "",
  "stale_inputs": [],
  "probabilities": {"-100": 0.0, "-50": 0.0, "0": 0.0},
  "expected_rate": 0.0,
  "lenses": {
    "prior":    {"probabilities": {}, "expected_rate": 0.0},
    "cbr":      {"probabilities": {}, "expected_rate": 0.0, "weight": 0.0},
    "volcker":  {"probabilities": {}, "expected_rate": 0.0, "weight": 0.0},
    "asymmetry":{"probabilities": {}, "expected_rate": 0.0, "weight": 0.0},
    "dissent":  {"probabilities": {}, "expected_rate": 0.0, "weight": 0.0}
  },
  "market": {"secid": "", "as_of": "YYYY-MM-DD", "implied_rate": 0.0,
             "probabilities": {}, "open_interest": 0},
  "gap_bp": 0,
  "path": [{"meeting": "YYYY-MM-DD", "low": 0.0, "base": 0.0, "high": 0.0,
            "market": null}],
  "annual_average": [{"year": 0, "base": 0.0, "cbr_lo": 0.0, "cbr_hi": 0.0}],
  "one_offs": [{"factor": "", "classification": "look_through | react | already_reacted"}],
  "change_trigger": {"release": "", "date": "YYYY-MM-DD", "condition": ""},
  "paper": {"instrument": "", "entry": 0.0, "entry_date": "YYYY-MM-DD",
            "direction": "long | short | none", "size": 1},
  "resolution": null
}
```

Probability keys are the step in basis points as strings. On the next run,
fill `resolution` in the previous file:
`{"outcome_bp": -50, "announced_rate": 0.0, "brier": {"own": 0.0, "market": 0.0, "naive": 0.0}, "paper_rub": 0, "closest_lens": ""}`.

## Ingest payload (print, do not send)

The `llm` track of `forecast_log` takes the same call. Print this block for
the owner; the endpoint is `POST /v1/key_rate_forecast` and is written by
`bi-ml-models: tools/forecast_log.py`. The contract is
`ingest/key_rate_forecast.go` (`ValidateKeyRateForecast`); re-read it before
printing, because a payload it rejects is of no use to the owner.

```json
{
  "forecast_date": "YYYY-MM-DD",
  "target_date": "YYYY-MM-DD",
  "metric": "cbr_rate:llm:shadow",
  "predicted": 0.0,
  "probabilities": {"cut": 0, "hold": 0, "hike": 0},
  "data_version": "",
  "model": "",
  "prompt_version": 1
}
```

What the endpoint checks, as of 2026-11-15:

- `predicted` is the expected **rate** after the meeting in percent (13.9),
  not the step.
- `probabilities` uses the three classes `cut`, `hold`, `hike` — all three
  present, in percent, summing to 100 ± 0.1. Collapse the step distribution
  of the report into them (all negative steps into `cut`, and so on); keys
  like `"-25"` are rejected. The step-level distribution stays in
  `forecast.json`.
- `data_version` must not be empty. Use the `data_version` of
  `v_point_in_time_version` followed by the run date
  (`F0B82DAD8D63D150@2026-11-15`) unless the owner has set another
  convention, and put the same string in `forecast.json`.
- `model` must not be empty: the model that produced this run.
- `prompt_version` must be at least 1 for the `llm` track: the version of
  this skill agreed with the track's owner. If none has been agreed, print
  1 and say that it is a placeholder.
