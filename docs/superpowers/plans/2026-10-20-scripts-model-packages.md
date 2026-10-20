# План: раскладка Python-скриптов по семействам моделей (`infra/5.4`)

> Спека: [2026-10-20-scripts-model-packages-design.md](../specs/2026-10-20-scripts-model-packages-design.md).
> Issue: [#304](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/304).

## Инварианты

- Цели `make`, CLI, seed, таргеты и имена артефактов не меняются.
- `scripts/testdata/` остаётся на месте.
- В ClickHouse ничего не пишется; DDL/GRANT/витрины не трогаются.
- Исторические `docs/research`, `docs/plans`, `docs/publications`, старые `docs/superpowers`, `.superpowers/sdd`, `docs/archive` не переписываются.

## Фаза 1 — `scripts/lib/`

1. Создать `scripts/lib/__init__.py`.
2. `git mv scripts/eval_utils.py scripts/lib/eval_utils.py`.
3. `git mv scripts/ml_experiment.py scripts/lib/ml_experiment.py`; `ROOT = parents[2]`; `from lib.clickhouse import ClickHouseError, ch_exec`.
4. `git mv scripts/ml_experiment_test.py scripts/lib/ml_experiment_test.py`; bootstrap от `parents[2]`, package-импорт.
5. Создать `scripts/lib/clickhouse.py`: перенести из `extrapolation_aisc.py` транспорт (`mask`, `parse_clickhouse_url`, `ch_config`, `ClickHouseError`, `parse_ch_json`, `_error_body`, `ch_query`, `ch_exec`, `sql_literal`, `sql_batches`, `DEFAULT_BATCH_SIZE`, `DEFAULT_CH_HTTP_PORT`, `CH_QUERY_TIMEOUT_SECONDS`).
6. Создать `scripts/lib/clickhouse_test.py`: `TestConfig`, `TestQueryTransport`, `TestExecTransport`, хелперы `_isolated_env`, `_FakeResponse` из `extrapolation_aisc_test.py`.
7. Проверки: `python3 scripts/lib/ml_experiment_test.py -v`, `python3 scripts/lib/clickhouse_test.py -v`.

## Фаза 2 — `scripts/models/polyus/`

1. Создать `scripts/models/__init__.py`, `scripts/models/polyus/__init__.py`.
2. `git mv scripts/extrapolation_aisc.py scripts/models/polyus/aisc.py`; импортировать транспорт из `lib.clickhouse`; реэкспортировать `ch_query`, `ch_exec`, `ClickHouseError`, `sql_literal`, `sql_batches`; `ROOT = parents[3]`.
3. `git mv scripts/extrapolation_aisc_test.py scripts/models/polyus/aisc_test.py`; оставить модельные классы, импорт `from models.polyus import aisc as ex`, patch-строки `models.polyus.aisc.urllib.request.urlopen`; `FIXTURE_DIR = parents[2] / "testdata"`.
4. `git mv scripts/polyus_output_model.py scripts/models/polyus/polyus_output_model.py` и `polyus_output_model_test.py`; `ROOT = parents[3]`; `from models.polyus import aisc`.
5. Makefile: `dcf-aisc`, `polyus-output`.
6. Проверки: `aisc_test.py`, `polyus_output_model_test.py`, smoke `python3 scripts/models/polyus/aisc.py --help`.

## Фаза 3 — `scripts/models/cpi/`

1. `git mv` в `scripts/models/cpi/`: `cpi_nowcast`, `cpi_monthly`, `cpi_monthly_v3`, `cpi_monthly_industry`, `cpi_public_panel_audit`, `cpi_weekly_component_panel`, `cpi_live3_horizons` + одноимённые `_test.py`; `__init__.py`.
2. Импорты: `from models.cpi import ...`, `from models.cb_comp import cb_comp_live_compare` (для `cpi_live3_horizons`), `from lib import ...`; `ROOT = parents[3]`; `ch_query = ch.ch_query`.
3. Тесты: bootstrap `parents[2]`, package-импорты, `FIXTURE_DIR = parents[2] / "testdata" / "<name>"`.
4. Makefile: `cpi-nowcast`, `cpi-monthly`, `cpi-monthly-v3(-test)`, `cpi-monthly-industry(-test)`, `cpi-public-audit(-test)`, `cpi-weekly-panel(-test)`, `cpi-live3(-test)`.
5. Проверки: все семь наборов.

## Фаза 4 — `scripts/models/cb_comp/`

1. `git mv` в `scripts/models/cb_comp/`: `cb_comp_bottomup`, `cb_comp_parity`, `cb_comp_proxy`, `cb_comp_live_pipeline`, `cb_comp_live_compare`, `cb_comp_public_export`, `cb_comp_panel_export.R` + `_test.py`; `__init__.py`.
2. Импорты: `from models.cb_comp import ...`, `from models.cpi import cpi_monthly, cpi_monthly_v3, cpi_public_panel_audit`, `from lib import ...`.
3. Обновить usage в `cb_comp_panel_export.R`.
4. Makefile: `cb-comp-live(-test)`, `cb-comp-live-compare`, `cb-comp-public-export`, `cb-comp-parity(-test)`, `cb-comp-proxy`.
5. Проверки: четыре набора.

## Фаза 5 — `scripts/models/key_rate/`

1. `git mv` в `scripts/models/key_rate/`: `key_rate_model`, `shock_classifier`, `key_rate_llm_forecast`, `key_rate_cbr_rule`, `key_rate_scenario_fan`, `key_rate_scenario_fan_v2`, `key_rate_scenario_fan_v3`, `key_rate_cpi_v3_experiment` + `_test.py`; `__init__.py`.
2. Импорты: `from models.key_rate import ...`, `from models.cpi import cpi_live3_horizons, cpi_monthly, cpi_monthly_v3`, `from lib import ...`; `ch_query = ch.ch_query`; `aisc.ClickHouseError` → `ch.ClickHouseError`.
3. `shock_classifier.py`: константы фикстур — от `parents[3]` / `scripts/testdata`; тест — package-импорт.
4. `key_rate_forecast_log.py` и тест остаются в корне.
5. Makefile: `key-rate-model`, `shock-classifier(-test)`, `key-rate-llm-forecast`, `key-rate-cbr-rule(-test)`, `key-rate-scenario-fan(-test)`, `key-rate-scenario-fan-v2(-test)`, `key-rate-scenario-fan-v3(-test)`, `key-rate-cpi-v3-experiment(-test)`.
6. Проверки: все восемь наборов.

## Фаза 6 — хвосты

1. `mlflow_backfill.py`, `mlflow_export.py`, `ml_gate.py`, `ml_live_check.py`: `from lib.clickhouse import ch_query/ch_exec`, `from lib.eval_utils import ...`.
2. Проверить остальные корневые скрипты на импорты перемещённых модулей.
3. `scripts/testdata/README.md` — путь к `models/polyus/aisc.py`.
4. Обновить docstring/комментарии со старыми путями в перемещённых файлах.
5. `git grep` по старым путям в живом коде и живых доках.

## Фаза 7 — документация

- `ARCHITECTURE.md` §2 (блок `scripts/`), при необходимости §6/§7/§8.
- `README.md` — дерево и команды.
- `docs/DEVELOPMENT.md` — таблица целей и Python-тесты.
- `docs/CONFIGURATION.md`, `docs/OPERATIONS.md`, `docs/LIMITATIONS.md`, `docs/IMPORTERS.md`, `docs/DCF_DATA_COVERAGE.md`, `docs/KEY_RATE_MODEL.md`, `docs/guides/stat-ml-models.md`.
- `docs/ROADMAP_INFRA.md` — пункт 5.4; `docs/ROADMAP_KEY_RATE.md`, `docs/ROADMAP_DCF_POLYUS.md` — действующие инструкции.
- `.kimi-code/skills/python-engineer/SKILL.md`, `.kimi-code/skills/ml-engineer/SKILL.md`.

## Фаза 8 — приёмка

1. Все Python-наборы зелёные; число тестов не уменьшилось.
2. Smoke `--help`/импорт для каждого семейства.
3. `make docs-check`.
4. `make all`.
5. `git grep` — старых путей в живых файлах нет.
6. PR `Closes #304`; протокол — комментарием.
