# Спека: раскладка Python-скриптов по семействам моделей (`infra/5.4`)

> Issue: [#304](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/304).
> Дата: 2026-10-20. Роль: `shared` (Python + ML).

## 1. Контекст

`scripts/` — плоский каталог: 78 top-level `.py` смешивают модели, их версии, общий транспорт ClickHouse, MLflow-инструменты, коллекторы и docs-генератор. Любая новая версия модели (`cpi_monthly_v3`, `key_rate_scenario_fan_v2`, …) добавляется рядом с инфраструктурой, а общий код (`eval_utils`, `ml_experiment`, транспорт внутри `extrapolation_aisc`) импортируется по имени файла. Это затрудняет навигацию, обзор версий и повторное использование.

## 2. Решение

Разложить модельные скрипты по семействам, сохранив у каждой версии её CLI, таргеты и выходы:

```
scripts/
├── lib/
│   ├── clickhouse.py          # транспорт и SQL-хелперы (выделено из extrapolation_aisc)
│   ├── eval_utils.py
│   ├── ml_experiment.py
│   └── *_test.py
├── models/
│   ├── cpi/                   # nowcast ИПЦ: v1/v2/v3, industry, панели, live3
│   ├── cb_comp/               # паритет и варианты CB_COMP
│   ├── key_rate/              # модель ставки, шоки, LLM, cbr_rule, scenario_fan v1–v3
│   └── polyus/                # AISC-экстраполяция и модель выпуска
├── testdata/                  # не перемещается
└── <немодельные инструменты>   # harvest, mcp_*, docs_build, xlsx_*, mlflow_*,
                               # ml_gate, ml_live_check, grafana_*, ppi_sa_probe,
                               # key_rate_forecast_log, make_*_fixture
```

Пакеты обычные (`__init__.py`), запуск остаётся прямым (`python3 scripts/.../x.py`); `pyproject.toml` не вводится.

## 3. Состав семейств

| Семейство | Скрипты |
|---|---|
| `models/cpi` | `cpi_nowcast`, `cpi_monthly`, `cpi_monthly_v3`, `cpi_monthly_industry`, `cpi_public_panel_audit`, `cpi_weekly_component_panel`, `cpi_live3_horizons` |
| `models/cb_comp` | `cb_comp_bottomup`, `cb_comp_parity`, `cb_comp_proxy`, `cb_comp_live_pipeline`, `cb_comp_live_compare`, `cb_comp_public_export`, `cb_comp_panel_export.R` |
| `models/key_rate` | `key_rate_model`, `shock_classifier`, `key_rate_llm_forecast`, `key_rate_cbr_rule`, `key_rate_scenario_fan`, `key_rate_scenario_fan_v2`, `key_rate_scenario_fan_v3`, `key_rate_cpi_v3_experiment` |
| `models/polyus` | `aisc` (бывший `extrapolation_aisc`), `polyus_output_model` |

Тесты переезжают вместе со скриптами. `key_rate_forecast_log.py` — ingest-адаптер, не модель, остаётся в корне; `ml_gate`, `ml_live_check`, `grafana_*`, `ppi_sa_probe` — инструменты, остаются в корне.

## 4. Инварианты

- Имена файлов, цели `make`, CLI-флаги, seed, имена артефактов и SQL/CSV-выходы не меняются.
- `scripts/testdata/` не перемещается; `--export-fixtures scripts/testdata/...` работает как раньше.
- В ClickHouse ничего не пишется; DDL/GRANT/витрины не трогаются.
- Исторические документы (`docs/research`, `docs/plans`, `docs/publications`, старые `docs/superpowers`, `.superpowers/sdd`, `docs/archive`) не переписываются: это снимки состояния. Живые документы обновляются.

## 5. Импорты

Скрипт `scripts/models/<family>/x.py`:

```python
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
from lib import eval_utils as eu
from lib import clickhouse as ch
from models.cpi import cpi_monthly as cm
```

Тест `scripts/models/<family>/x_test.py`:

```python
SCRIPTS = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SCRIPTS))
from models.cpi import cpi_monthly as cm
```

В `lib/*.py` — `ROOT = Path(__file__).resolve().parents[2]`. Все соседние импорты — только package-qualified; плоский `import cpi_monthly` создал бы второй объект модуля и сломал `patch.object(cm, "ch_query")`.

Module-level алиасы `ch_query = ch.ch_query` сохраняются, как и имена `eu`/`mlx`. `aisc.py` реэкспортирует транспортные имена, чтобы `polyus_output_model` и тесты не меняли вызовы.

## 6. Разделение `extrapolation_aisc`

- `lib/clickhouse.py`: `mask`, `parse_clickhouse_url`, `ch_config`, `ClickHouseError`, `parse_ch_json`, `_error_body`, `ch_query`, `ch_exec`, `sql_literal`, `sql_batches`, `DEFAULT_BATCH_SIZE`, `DEFAULT_CH_HTTP_PORT`, `CH_QUERY_TIMEOUT_SECONDS`.
- `models/polyus/aisc.py`: константы Polyus, парсинг PDF, `build_series`, `calibrate`, `reconcile`, `build_plan_rows`, `validate`, `main`, `production_sql`, `read_actual_production`, `MINE_PLANS_COLUMNS`, `write_sql`, `write_csv`, `_display_path`.
- Тест 201 теста делится по классам: транспортные (`TestConfig`, `TestQueryTransport`, `TestExecTransport`) — в `lib/clickhouse_test.py`, остальные — в `models/polyus/aisc_test.py`.

## 7. Документация

Обновляются `ARCHITECTURE.md` §2, `README.md`, `docs/DEVELOPMENT.md`, `docs/CONFIGURATION.md`, `docs/OPERATIONS.md`, `docs/LIMITATIONS.md`, `docs/IMPORTERS.md`, `docs/DCF_DATA_COVERAGE.md`, `docs/KEY_RATE_MODEL.md`, `docs/guides/stat-ml-models.md`, `docs/ROADMAP_INFRA.md` (пункт 5.4), действующие инструкции `docs/ROADMAP_KEY_RATE.md`/`docs/ROADMAP_DCF_POLYUS.md`, скилы `python-engineer`/`ml-engineer`, `scripts/testdata/README.md`.

## 8. Приёмка

- Все Python-наборы зелёные; суммарное число тестов не уменьшилось.
- `make docs-check`, `make all` зелёные.
- `git grep` не находит старых путей в живом коде и живых доках.
- Нет секретов, записи в ClickHouse, DDL/GRANT и изменений Go-кода.

## 9. Отклонённые альтернативы

- **Оставить общий код в корне `scripts/`** — корень остаётся смешанным, а модели импортируют инфраструктуру по имени файла.
- **Оставить `extrapolation_aisc.py` целиком в `lib/`** — модель AISC не попадает в `models/polyus/`, а `lib/` получает модельный код.
- **Сократить имена файлов внутри папок** — потребовало бы переписать все упоминания в доках, тестах и артефактах без выигрыша для навигации.
