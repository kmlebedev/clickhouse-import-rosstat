SELECT source, name, points, first_seen, url FROM source_publications FINAL WHERE source = 'cbr_int_rate_loans';
SELECT 'view_rows' AS check, toString(count()) AS value FROM v_cbr_macro FINAL WHERE series LIKE 'Всего:%' OR series LIKE 'СМП:%';
SELECT 'view_series' AS check, toString(uniqExact(series)) AS value FROM v_cbr_macro FINAL WHERE series LIKE 'Всего:%' OR series LIKE 'СМП:%';
