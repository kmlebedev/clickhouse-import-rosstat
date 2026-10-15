-- Приёмочный снимок cbr_int_rate_loans (пункт 4.2 №8). Read-only.
SELECT 'rows_total' AS check, toString(count()) AS value FROM cbr_int_rate_loans FINAL
UNION ALL SELECT 'dates', concat(toString(min(date)), ' .. ', toString(max(date))) FROM cbr_int_rate_loans FINAL
UNION ALL SELECT 'series', toString(uniqExact(name)) FROM cbr_int_rate_loans FINAL;

-- 16 рядов: точки и крайние даты
SELECT name, count() AS rows, min(date) AS d0, max(date) AS d1
FROM cbr_int_rate_loans FINAL GROUP BY name ORDER BY name;

-- spot-проверка против живого xlsx
SELECT 'spot' AS check, name, date, rate FROM cbr_int_rate_loans FINAL
WHERE (name LIKE 'Всего: до 30 дней%' AND date IN ('2014-01-30', '2026-08-30'))
   OR (name = 'СМП: свыше 1 года' AND date = '2026-08-30')
ORDER BY name, date;

-- каталог: 16 рядов источника
SELECT 'catalog_rows' AS check, toString(count()) AS value FROM series_catalog FINAL
WHERE source = 'cbr' AND origin LIKE '%loans_nonfin.xlsx%';

-- строка версии в source_publications
SELECT source, name, points, imported_at FROM source_publications FINAL WHERE source = 'cbr_int_rate_loans';

-- витрина: ряды кредитных ставок видны
SELECT 'view_rows' AS check, toString(count()) AS value FROM v_cbr_macro FINAL
WHERE series LIKE 'Всего:%' OR series LIKE 'СМП:%';
