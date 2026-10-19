---
name: python-engineer
description: Use when the task is non-ML Python tooling in clickhouse-import-rosstat — collectors, harvest, docs generator, MCP setup/check, forecast-log sender, Grafana checks or ingest payloads — and you need the repo-specific test and write-boundary rules
type: prompt
whenToUse: When working on scripts/*.py outside ML, ingest payloads, collectors, docs generator, MCP checks or Grafana checks in clickhouse-import-rosstat
---

# Python-инженер в clickhouse-import-rosstat

**Назначение.** Маршрут не-ML Python-задачи: где граница записи, как писать офлайн-тесты, как пинить зависимости и что считается готовым результатом. ML-эксперименты — в [ml-engineer](../ml-engineer/SKILL.md), разведка источника — в [research-engineer](../research-engineer/SKILL.md).

**Канонические документы.** Команды и Python-тесты — [DEVELOPMENT.md](../../../docs/DEVELOPMENT.md); конфигурация — [CONFIGURATION.md](../../../docs/CONFIGURATION.md); граница записи — [IMPORTER_PACKAGE_STANDARD.md](../../../docs/IMPORTER_PACKAGE_STANDARD.md); роадмапы — [ROADMAP_INDEX.md](../../../docs/ROADMAP_INDEX.md).

## 1. Когда применять

- `scripts/*.py` вне ML: `edisclosure_collect.py`, `harvest.py`, `docs_build.py`, `mcp_setup_user.py`, `mcp_check.py`, `grafana_check.py`, `key_rate_forecast_log.py`, `extrapolation_aisc.py`, `polyus_output_model.py`.
- Ingest-payload: подготовка JSON для `POST /v1/...`, `--dry-run`, схема полей.
- Офлайн-тесты Python-инструментов (`*_test.py`), фикстуры, `uv`-пины.

**Когда не применять:** ML-модели/признаки/бэктест (→ `ml-engineer`); Go-импортёр/DDL/витрина/грант (→ `go-engineer`); разведка источника (→ `research-engineer`).

## 2. Что прочитать перед стартом

1. [AGENTS.md](../../../AGENTS.md) — общий контракт, правила 1–12.
2. Владеющий роадмап и issue пункта — [ROADMAP_INDEX.md](../../../docs/ROADMAP_INDEX.md).
3. [DEVELOPMENT.md](../../../docs/DEVELOPMENT.md) — разделы «Команды», «Python-тесты», «CI и релизы».
4. [CONFIGURATION.md](../../../docs/CONFIGURATION.md) — env-переменные и секреты.
5. [IMPORTER_PACKAGE_STANDARD.md](../../../docs/IMPORTER_PACKAGE_STANDARD.md) §10/§11 — граница записи и публикации.
6. Спеку/план пункта, если есть.

## 3. Границы роли

| Python-инженер делает | Не делает |
|---|---|
| Скрипты в `scripts/`, офлайн-тесты, фикстуры | Прямую запись в ClickHouse |
| Ingest-payload и `--dry-run` | DDL, `ALTER`, `GRANT`, `CREATE VIEW` |
| Коллекторы, harvest, docs-генератор | Go-импортёры и схему |
| MCP setup/check, Grafana check | ML-модели и признаки |

**Жёстко:** ClickHouse — только чтение через `ch_query`/MCP или запись через ingest endpoint/Go-импортёр. Если для задачи нужна новая таблица/витрина/грант — это отдельный пункт роадмапа и Go-импортёр по стандарту; Python готовит payload/спеку, применение — за Go-разработчиком.

## 4. Цикл

1. **Пункт и issue:** `<slug>/<item-id>`, критерий приёмки; `In Progress`, `Branch`.
2. **Спека/план**, если задача не однострочная; первые коммиты.
3. **Реализация:** скрипт + тесты; никаких сетевых вызовов в тестах.
4. **Офлайн-тесты:** `unittest`, моки транспорта, фикстуры в `scripts/testdata/`.
5. **Пины:** `uv run --with <pkg>==<version> --python 3.12`; версии — в Makefile/доках, не «плавающие» для официальных прогонов.
6. **Документация:** `DEVELOPMENT.md`/`CONFIGURATION.md`, если меняется команда или env.
7. **Проверки:** Python-тесты, `make docs-check`, `make all` (универсальный DoD); `make mcp-check`, если менялись витрины/гранты (но их меняет Go).
8. **PR:** один пункт, `Closes #N`, число тестов и команды — в протоколе.

## 5. Правила

1. **Секреты только из env/`make`**; не читать и не печатать `~/.config/rosstat/env`.
2. **Тесты без сети и живой БД:** моки `urllib`/`http.client` (например, `patch.object(module.urllib.request, "urlopen", ...)`), фикстуры; локальный HTTP-сервер — тоже сеть; `unittest`, не pytest.
3. **Детерминизм:** фиксированный seed, сортировка, стабильные имена артефактов.
4. **Запись — только payload:** `key_rate_forecast_log.py` готовит JSON для ingest; `--dry-run` обязателен для проверки.
5. **Артефакты прогонов не коммитить:** `build/`, `*.json`/`*.csv` результатов — в gitignore; фикстуры — явно.
6. **Не логировать секреты** и полные payload с токенами.
7. **Python 3.12** для официальных прогонов (`uv ... --python 3.12`); системный `python3` — только для stdlib-тестов.
8. **Не менять Go-код и схему** без отдельного пункта.

## 5.1. Контракт ingest и коллектора

- **Endpoints.** Типовые: `/v1/manual_series` (массив), `/v1/gold_prices`, `/v1/gold_forecasts`, `/v1/key_rate_forecast`, `/v1/model_run`; формы различаются. `source`/`venue` — закрытые списки; новый whitelist — Go-пункт, не Python.
- **Транспорт.** Явный timeout, User-Agent, `CERT_FILES` для гос-хостов, backoff; токен не в URL/логах.
- **Dry-run.** Без env и сети, детерминированный JSON, общий валидатор для dry-run и отправки, тело ≤ 1 MiB, коды 0/1.
- **Фикстуры.** Реальный урезанный XLSX в `scripts/testdata/`, не синтетическая книга.
- **Идемпотентность.** Ключи endpoint'ов (`(source, series, date)`, `(metric, forecast_date)`) делают повтор upsert'ом; проверь повтор.
- **Тестовая проводка.** Новый `scripts/<name>_test.py` добавь в `DEVELOPMENT.md` и §9 этого скила; `make all` Python-тесты не запускает.
- **Живая приёмка.** Локальный `make run-ingest INGEST_ADDR=:18081`, `inserted=N`, повтор — тот же N, тестовые строки убрать.
- **Новый ряд/таблица/витрина/грант.** Стоп: отдельный Go-пункт; Python готовит payload и спеку.
- **Снапшот для Go-импортёра.** Второй паттерн (`build/...` + `EDISCLOSURE_INPUT_DIR`) требует Go-импортёра; выбери паттерн в спеке.
- **Прод-скрипт.** Если нужен XLSX/HTTP, скрипт целиком идёт через `uv ... --python 3.12` с пином.
- **Env-пара клиента.** `INGEST_URL` + `INGEST_TOKEN`; в `CONFIGURATION.md` и `env-check` их пока нет — это отдельный docs-пункт, не повод хардкодить адрес.

## 6. Команды

| Цель | Команда |
|---|---|
| stdlib-тесты | `python3 scripts/<name>_test.py` |
| ML/тяжёлые тесты | `uv run --with ... --python 3.12 python3 scripts/<name>_test.py` |
| MCP end-to-end | `make mcp-check` |
| Docs | `make docs-check` |
| Универсальный CI | `make all` |
| Forecast payload | `make key-rate-forecast-log ARGS="--dry-run"` |
| e-disclosure сбор | `make edisclosure-collect` |
| Grafana smoke | `make grafana-check` (нужны токен/URL) |

## 7. Определение готовности

- [ ] Пункт/issue названы; спека/план — при необходимости.
- [ ] Тесты офлайн, число тестов указано в PR.
- [ ] Нет прямой записи в ClickHouse, DDL/GRANT и секретов.
- [ ] Зависимости запинены; `--python 3.12` для официальных прогонов.
- [ ] Документация обновлена.
- [ ] `make all` и `make docs-check` зелёные; Python-наборы — зелёные.
- [ ] Протокол — комментарием в PR; статус роадмапа — тем же PR.

## 8. Частые ошибки

| Ошибка | Почему плохо | Как правильно |
|---|---|---|
| Тест требует живую БД/сеть | Не работает в CI и у другого агента | Моки + фикстуры |
| Плавающие версии | Прогон не воспроизводится | `uv run --with pkg==x.y`, пин в Makefile |
| Запись в ClickHouse из скрипта | Ломает слой и гранты | Ingest endpoint / Go-импортёр |
| Коммит артефактов | Мусор в истории | `build/`, gitignore; фикстуры — отдельно |
| Секрет в логе | Утечка | Маскировать, не печатать env |
| Системный `python3` | Версия <3.10, statsmodels падает | `--python 3.12` |

## 9. CI-долг (`infra/5.2`)

- **Fast, stdlib-only:** `docs_build_test.py`, `harvest_test.py`, `ml_experiment_test.py`, `mlflow_export_test.py`, `mlflow_registry_test.py`, `key_rate_llm_forecast_test.py`, `key_rate_forecast_log_test.py`, `mcp_setup_user_test.py`, `cb_comp_proxy_test.py`, `edisclosure_collect_test.py`, `extrapolation_aisc_test.py`, `polyus_output_model_test.py`.
- **Тяжёлые (CatBoost/sklearn/statsmodels):** `key_rate_model_test.py`, `cpi_nowcast_test.py`, `cpi_monthly_test.py`, `shock_classifier_test.py`, `cb_comp_parity_test.py`, `cb_comp_bottomup_test.py` — path-filter/nightly.
- До появления workflow тесты гоняются вручную; `make all` их не включает.

## 10. Ссылки

- [AGENTS.md](../../../AGENTS.md), [DEVELOPMENT.md](../../../docs/DEVELOPMENT.md), [CONFIGURATION.md](../../../docs/CONFIGURATION.md).
- [ml-engineer](../ml-engineer/SKILL.md), [research-engineer](../research-engineer/SKILL.md), [github-tracker](../github-tracker/SKILL.md).
- [2026-10-19-engineering-issue-review.md](../../../docs/research/2026-10-19-engineering-issue-review.md) §4/§5.
