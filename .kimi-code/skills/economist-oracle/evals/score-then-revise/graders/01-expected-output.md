---
type: llm
focus: trace
weight: 1
---

The result matches this description: Scores the previous forecast before forming the new one: runs scripts/rate_math.py score, reports Brier scores for own, market and naive forecasts, the paper result in roubles, and which lens was closest; fills the resolution field of the previous forecast.json. Then produces the new forecast with a section on what changed the view. If the forecast_log row is still unresolved it says so and resolves from cbr_key_rate.
