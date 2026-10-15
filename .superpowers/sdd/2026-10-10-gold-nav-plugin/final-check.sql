SELECT 'v_model_inputs rows', count() FROM v_model_inputs;
SELECT * FROM v_model_inputs FORMAT Vertical;
SELECT indicator, value, threshold, flag, as_of, stale FROM v_gold_dashboard;
SELECT metric, forecast_date, target_date, predicted, is_resolved FROM v_forecast_accuracy;
SELECT category, count() FROM events_calendar FINAL WHERE event_date >= today() GROUP BY category ORDER BY category;
SELECT table, count() AS cols, countIf(comment != '') AS commented FROM system.columns WHERE table IN ('v_model_inputs','v_gold_dashboard','v_forecast_accuracy') GROUP BY table;
