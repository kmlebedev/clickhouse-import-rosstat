SELECT 'model_runs', count() FROM model_runs FINAL WHERE run_id = '7f3c2a10-5b1e-4c7d-9a2f-0e8d4b6c1a99';
SELECT 'forecast_log', metric, predicted FROM forecast_log FINAL WHERE forecast_date = today() AND metric IN ('nav','xau_q_avg') ORDER BY metric;
