# T3 #278: mapping ОКПД2→КИПЦ, веса ИПЦ и калибровка паритета

**Пункт:** `key-rate/phase-4-rosstat-ppi-sa` T3, issue [#278](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/278), parent [#266](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/266).
**Роль:** `research`. **Ветка:** `research/ppi-kipc-mapping`. **Worktree:** `.worktrees/ppi-kipc-mapping`.

## 1. Границы

- T2 (#277) ещё не смержен; T3 — research и по спеке **не пишет в ClickHouse**, поэтому работает напрямую с XLSX `Proizvoditeli_Ind_tov_08-2026.xlsx` и `Vesa-tov-KIPC_2012-2026.xlsx` (как research #275).
- Не меняются: Go-код, DDL, витрины, гранты, dagu.
- Артефакты: CSV/JSON в `docs/research/`, reproducible-скрипт `scripts/ppi_sa_mapping.py`, тесты, research-документ.

## 2. Единицы и веса

- Из `tov.xlsx` — 1 273 товарных кода; для агрегации используем листья (в срезе 2026 — 1 232 кода без дочерних). Родительские коды остаются в mapping-файле как `aggregate` и в веса не входят.
- Веса — `Vesa-tov-KIPC_2012-2026.xlsx`: лист `2026` для якорей; панель по годам — для handoff в T4.
- In-scope товарные группы КИПЦ: food `01.1.1`–`01.1.5`, `01.1.8`, `01.1.9`, `01.2`; alcohol/tobacco `02.1`, `02.3`; non-food goods `03.1`, `03.2` (без `03.1.4`, `03.2.2`), `05.1`–`05.6` (без `05.6.2`), `06.1`, `07.1`, `07.2.1`, `07.2.2`, `08.1`, `09.1`, `09.2`, `09.3`, `09.7`, `13.1.2`, `13.2`.
- Свежие фрукты/овощи `01.1.6`/`01.1.7` исключаются из food-веса: в ИЦП нет сопоставимого блока. Переработанные `10.31`/`10.39` идут в `01.1.9` с пометкой.
- In-scope вес 2026 ≈ 26,55 (food) + 26,03 (non-food goods) = ≈ 52,57 п.п. из 100; coverage — к этому знаменателю.
- Utilities (`35`→`04.5`, `36/38`→`04.4`) считаются отдельно и в `all/core` не входят.

## 3. Mapping

Seed `docs/research/2026-10-20-ppi-kipc-rules.csv`: `priority,match_type,pattern,kipc_group,status,note`; `match_type` = `code_exact` > `code_prefix` (самый длинный) > `name_regex`; статусы `mapped`, `excluded`, `out_of_scope`, `unmatched`. Ручные уточнения — `code_exact` с приоритетом 1000.

Черновая карта (финал — после ручной проверки unmatched/низкоуверенных):

- `10.1`–`10.9` → `01.1.*`/`01.1.9`/`01.2.*`; `10.92` → `09.3.2`, `10.91` → `excluded`;
- `11.01`–`11.05` → `02.1`, `11.06` → `excluded`, `11.07` → `01.2`; `12` → `02.3`;
- `13`–`15` → `03.1`/`03.2` (15.12 → `13.2`);
- `17.12`/`17.22`/`17.23` → `09.7`/`05.6`/`13.1.2`; `18` → `09.7`;
- `19.20.21`/`19.20.29` → `07.2.2`; `19.10` → `excluded`;
- `20.41` → `05.6`, `20.42` → `13.1.2`, `20.30` → `05.6`; остальные `20.*` → `excluded`;
- `21` → `06.1`; `22.11` → `07.2.1`; `22.19/22.29` → `05.6`/`13.2`; `22.2*` → `excluded`/`out_of_scope`;
- `23.1*/23.4*` → `05.4`; `23.5*`–`23.7*` → `excluded`;
- `25.7*` → `05.4`/`05.5`/`05.6`; `25.1*/25.2*` → `out_of_scope` (`04.3`); `24` → `excluded`;
- `26.2/26.3/26.4` → `08.1`; `26.52` → `13.2`; `26.11/26.12` → `excluded`;
- `27.2/27.4/27.5` → `05.3`/`05.5`; остальные `27.*` → `excluded`;
- `28.23/28.24/28.25` → `08.1`/`05.5`/`05.3`; остальные `28.*` → `excluded`;
- `29.10.2` → `07.1`, `29.32` → `07.2.1`; остальные `29.*` → `excluded`;
- `30.91/30.92` → `07.1`/`09.2`; остальные `30.*` → `excluded`;
- `31` → `05.1`; `32.12/32.13` → `13.2`, `32.2` → `09.1`, `32.3/32.4` → `09.2`, `32.5` → `06.1`, `32.91` → `05.6`, `32.99` → `13.2`;
- `33` → `excluded`; `35`/`36`/`38` → `out_of_scope`; `37` → `excluded`, `58` → `09.7`.

Правила меняются только по смыслу названия/класса, **не** по parity-ошибке.

## 4. Схемы и parity

1. **equal** — равные веса по mapped-листьям (baseline).
2. **kipc** — среднее m/m внутри группы КИПЦ, затем вес группы 2026, нормированный на доступные в месяце группы.
3. **narrow** — non-food только ОКПД2 `13–15, 17–18, 21, 31–32` (равные веса); food — как в equal; `all_narrow` = food + narrow non-food.

Ряды: `food` (без алкоголя и свежих плодоовощей), `nonfood` (без топлива и табака), `all` (широкий: food_broad + nonfood_broad, включая алкоголь/топливо/табак), `core` = `food + nonfood` (чёрная линия рис. 44). В parity якорь `all` сравниваем с `core` (primary) и `all` (secondary).

SA: `scripts/ppi_sa_x13.R` получает input/output позиционными аргументами; primary spec — `seats_log_cal`, robustness — `seats_level_cal`, `x11_log_cal`, `seats_log_nocal`. 10 якорей (4 food, 4 non-food, 2 all), primary metric — `mean3_saar`, secondary — `level3_annualized`. Цель MAE ≤ 0,3 п.п.; если не достигнута — фиксируем расхождение, не подгоняем веса.

## 5. Артефакты

- `docs/research/2026-10-20-ppi-kipc-rules.csv` — seed правил;
- `docs/research/2026-10-20-ppi-kipc-mapping.csv` — итоговый mapping;
- `docs/research/2026-10-20-ppi-kipc-coverage.json` — coverage;
- `docs/research/2026-10-20-ppi-kipc-parity.csv` и `...-parity-summary.json` — parity;
- `docs/research/2026-10-20-ppi-kipc-mapping.md` — research-документ (site-pages, секция «Исследования»);
- `scripts/ppi_sa_mapping.py`, `scripts/ppi_sa_mapping_test.py`.

## 6. Проверка

- `python3 scripts/ppi_sa_mapping_test.py -v`;
- `python3 scripts/ppi_sa_probe_test.py -v` (12 OK, регрессия #275);
- end-to-end: `mapping` → `series` → `pilot` (R `seasonal`) → `parity`; 10 якорей, 3 схемы, 4 ряда;
- `make docs-check`; `make all`; `git diff --stat`; grep на отсутствие записи в ClickHouse;
- mapping воспроизводится из seed детерминированно.

## 7. Решение T4/T7

- MAE ≤ 0,3 на food/non-food/core KIPC-схемы → T4 (#279) штатно, T7 (#282) P1.
- Улучшение есть, но > 0,3 → T4 с `taxonomy_version` и явным расхождением; T7 P0.
- Улучшения нет → T7 P0; T4 может публиковать собственный SA-ряд на baseline.
- Числа и решение — в issue #278 и `docs/ROADMAP_KEY_RATE.md`.
