# План: проверка рядов Росстата и подготовка к обновлению статьи

**Дата:** 2026-11-06. **Спека:** [2026-11-06-rosstat-source-recovery-prep-design.md](../specs/2026-11-06-rosstat-source-recovery-prep-design.md). **Ветка:** `chore/rosstat-source-check-2026-11`.

## Task 1. Baseline-прогон всех рядов Росстата

1. Makefile: переменная `STATS_rosstat_sources` — 11 имён в порядке `dagu/rosstat.yaml`, `unexport` по образцу `STATS_key_rate_sources`.
2. `make import STAT=rosstat_sources`; в логе — по строке на импортёр.
3. Зафиксировать `count() FINAL` по таблицам до и после прогона (raw-таблицы читаются под `default`; `kimi_reader` видит только `v_*`).
4. Результат — таблица «импортёр → код → причина» в research-заметке.

**Проверка:** успешных импортов данных нет, кроме no-op `rosstat_ipc_weeks_bulletins`; ни один импортёр не пишет «Imported 0 rows» вместо отказа.

## Task 2. Пересчёт производных слоёв

1. `make import STAT=weekly_cpi_sa` — ожидается 210 строк, `as_of=2026-10-26`.
2. `make import STAT=keyrate_futures,ofz_curve,cbr_key_rate,cbr_ruania` — рыночный контур и ставка.
3. Повтор первого прогона — no-op с тем же отпечатком.

**Проверка:** `v_weekly_cpi_sa` — 630 строк, `max(as_of)=2026-10-26`; `v_key_rate_dashboard` — `weekly_cpi_saar` и `keyrate_*` с новым `as_of`.

## Task 3. Документация источника

1. `docs/research/2026-11-06-rosstat-source-availability.md`: таблица проверок, эффект на импортёры, состояние dev, готовность статьи, рецепт перепроверки.
2. `ROADMAP_ROSSTAT.md` §3: строка доступности на 2026-11-06 и условие разблокировки «первый успешно скачанный файл».
3. `docs/publications/README.md`: раздел «Недельный SA-разбор» с командами прогона.

**Проверка:** `make docs-check` зелёный; в §3 нет утверждения «источник недоступен» без даты.

## Task 4. Перенос генератора графика

1. `scripts/publications/weekly_sa_chart.py`: чтение витрин через `scripts/lib/clickhouse.py`, CLI `--as-of/--out/--publication`, запись `chart-data.json` и SVG.
2. `scripts/publications/weekly_sa_chart_test.py`: фикстуры, форматирование чисел, nowcast-строка, пустой ряд.
3. Parity: прогон на `--as-of 2026-10-31` даёт файлы, побайтно равные артефактам предыдущего прогона — закоммиченному `weekly-sa-mm-vs-cbr.svg` и локальному `chart-data.json` (в git он не хранится).

**Проверка:** тест зелёный, parity подтверждён `cmp`.

## Task 5. Финальная приёмка

1. `make all`, `make docs-check`, `dagu validate dagu/rosstat.yaml`.
2. Коммиты: спека+план, группа+заметка+роадмап, генератор+тест.
3. PR один, мерж — за владельцем; протокол приёмки — комментарием в PR.
