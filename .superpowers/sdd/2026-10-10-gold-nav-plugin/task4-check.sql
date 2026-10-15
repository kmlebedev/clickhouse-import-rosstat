SELECT 'v_model_inputs rows', count() FROM v_model_inputs;
SELECT * FROM v_model_inputs FORMAT Vertical;
SELECT indicator, value, threshold, flag, as_of, stale FROM v_gold_dashboard;
SELECT table, count() AS cols, countIf(comment != '') AS commented FROM system.columns WHERE table IN ('v_model_inputs','v_gold_dashboard') GROUP BY table;
