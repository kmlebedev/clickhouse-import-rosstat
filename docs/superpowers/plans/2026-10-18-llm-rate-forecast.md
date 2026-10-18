# Независимый LLM-прогноз ключевой ставки — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: use `superpowers:subagent-driven-development` (или `superpowers:executing-plans` только если владелец явно выбрал inline-исполнение). Шаги используют чекбоксы (`- [ ]`). Пункт роадмапа — `key-rate/phase-4-llm-track`, issue [#189](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/189); зависимость — #127.

**Goal:** добавить экспериментальный LLM-трек прогноза ключевой ставки: as-of снимок данных, фиксированный промпт, три OpenAI-совместимых запроса, медианный прогноз `{cut, hold, hike}` + `expected_step_bp`, JSON/markdown-артефакты и payload для `forecast_log` (#127), без прямой записи в ClickHouse.

**Architecture:** новый Python-скрипт `scripts/key_rate_llm_forecast.py` переиспользует транспорт `ch_query` из `scripts/extrapolation_aisc.py` (как `key_rate_model.py`) и читает только витрины `v_cbr_macro`, `v_rosstat_macro`, `v_cbr_survey`, `v_cbr_mo_br`, `v_keyrate_expectations`, `v_ofz_curve`, `v_cbr_mp_summary`, `v_cbr_mp_summary_channels`, `v_events_calendar`. Снимок фильтруется as-of правилами; промпт и ответ валидируются; три повтора агрегируются медианой. Артефакты — локальные, gitignored. `--log` формирует payload для будущего ingest #127 и ничего не пишет в БД. Тесты — офлайн, `unittest`, транспорт и HTTP замоканы.

**Tech Stack:** Python 3.12 (stdlib: `urllib`, `json`, `statistics`, `hashlib`, `unittest.mock`), `uv`, Makefile, ClickHouse HTTP 8123, OpenAI-совместимый `/v1/chat/completions`.

**Spec:** [2026-10-18-llm-rate-forecast-design.md](../specs/2026-10-18-llm-rate-forecast-design.md) — связывающая власть; этот файл — детализация.

## Ветка и предусловия

- Ветка `feat/key-rate-llm-forecast` от `origin/main`; worktree `.worktrees/key-rate-llm-forecast`. Чужие ветки/worktrees не трогаем.
- Первые коммиты ветки — спека и план (до кода). PR — один, в конце, после `make all`.
- ClickHouse поднимает владелец. Если `make ch-status` не отвечает — зафиксировать и не запускать `make ch-up`.
- LLM env: `LLM_BASE_URL`, `LLM_API_KEY`, `LLM_MODEL` (значения только в `~/.config/rosstat/env`, не печатать). Если их нет — живой прогон не выполняется, в PR фиксируется `DONE_WITH_CONCERNS` и команда для повторного прогона.
- Секреты — только `os.Getenv`; в логах и ошибках — только имена переменных.

## Решения, зафиксированные владельцем

| Вопрос | Решение |
|---|---|
| Запись в журнал | #127 не реализована: `--log` готовит payload и файл `key_rate_llm_forecast_log_payload.json`; прямой записи в `forecast_log` нет. |
| Число повторов | Три независимых вызова при `temperature=0`; медиана и разброс; меньше двух валидных — прогноз не строится. |
| Провайдер | OpenAI-совместимый `POST <base>/v1/chat/completions`, `Authorization: Bearer`; `response_format=json_object` с фолбэком. |
| Входы | Только витрины; резюме — последнее опубликованное к дате среза, каналы — regex-слой. Будущие данные не используются. |
| Артефакты | Локальные, gitignored, детерминированы при одинаковых данных/ответах/дате среза. |
| Расписание | Нет: ручной экспериментальный трек, без DAG. |

## Global Constraints

- Сетевые вызовы — только внутри функций, не в `init()`/импорте модуля.
- ClickHouse — только чтение через `ch_query`; скрипт не пишет в БД ни в одном режиме.
- Промпт и схема ответа — фиксированная редакция `PROMPT_VERSION = 1`.
- Все вероятности на выходе нормированы к 100 %; `expected_step_bp` — базисные пункты.
- Артефакты прошлого прогона удаляются при провале, чтобы диск не выдавал старый прогноз за новый.
- `make all` должен остаться зелёным; Python-тесты запускаются отдельно.

## Review Focus

1. **As-of:** ни одна точка позже `data_asof` не попадает в промпт; месячные лаги и `published` резюме проверены тестами.
2. **Схема:** сумма вероятностей ровно 100, отрицательные/NaN отвергаются, шаг в допустимом диапазоне, rationale/risks непустые.
3. **Три повтора:** ровно три вызова при успехе; медиана и разброс считаются по валидным; representative rationale детерминирован.
4. **Отсутствие env:** ошибка до сетевого вызова, в тексте только имена переменных, значения не утекают.
5. **`--log`:** нет записи в ClickHouse; payload содержит `metric='cbr_rate'`, `target_date`, `predicted`, вероятности, модель/`prompt_version`/`data_version` и TODO #127.
6. **Детерминизм артефактов:** два прогона с одинаковыми входами дают одинаковый JSON.

---

### Task 1: Спека и план — первые коммиты ветки

**Files:**
- Create: `docs/superpowers/specs/2026-10-18-llm-rate-forecast-design.md`
- Create: `docs/superpowers/plans/2026-10-18-llm-rate-forecast.md`

**Steps:**
- [ ] **Step 1:** Написать спеку и план (этот файл).
- [ ] **Step 2:** Самопроверка: нет нерешённых `TBD`, кроме явного TODO #127; охват совпадает с issue #189; критерий приёмки сформулирован проверяемо.
- [ ] **Step 3:** `git add` и коммит спеки и плана первыми коммитами ветки.

---

### Task 2: Снимок as-of и запросы к витринам

**Files:**
- Create: `scripts/key_rate_llm_forecast.py`
- Test: `scripts/key_rate_llm_forecast_test.py` (часть снимка)

**Interfaces:**
- Produces (используют Task 3–6):
  - `SNAPSHOT_SQL`/`load_*()` — чтение витрин через `ch_query`;
  - `available_from_*()` — правила доступности;
  - `snapshot(as_of: date) -> Snapshot` — dataclass с полями `meeting_date`, `current_rate`, `cpi`, `expectations`, `survey`, `keyrate`, `ofz`, `ruonia`, `consensus`, `summary`, `data_asof`;
  - `snapshot_payload(snapshot) -> dict` — канонический словарь для промпта и `data_version`;
  - `data_version(payload) -> str` — 12 hex SHA-256.

**Steps:**
- [ ] **Step 1:** Определить SQL для девяти блоков; каждый запрос содержит `FORMAT JSON` и `date`/`published`-фильтр не позже `data_asof`.
- [ ] **Step 2:** Реализовать `available_from_*` и `asof(points, available, on)`; для месячных рядов — конец `M+1`, для RUONIA/ОФЗ/KEYRATE — `date + 1`, для консенсуса — `date + 7`, для резюме — `published`.
- [ ] **Step 3:** Реализовать разбор строк (`row_date`, `row_float`, `row_optional`), проверки непустых обязательных входов, `Snapshot` и `snapshot_payload`.
- [ ] **Step 4:** Тесты: future-точка отбрасывается; месячная точка с неистёкшим лагом отбрасывается; резюме с `published > as_of` не выбирается; `data_version` стабильна и меняется при изменении данных.
- [ ] **Step 5:** Прогнать `python3 scripts/key_rate_llm_forecast_test.py -v` (часть снимка) — PASS.

---

### Task 3: Промпт, OpenAI-транспорт, валидация и три повтора

**Files:**
- Modify: `scripts/key_rate_llm_forecast.py`
- Test: `scripts/key_rate_llm_forecast_test.py`

**Interfaces:**
- Produces (используют Task 4–6):
  - `PROMPT_VERSION`, `REPEATS`, `MAX_FORMAT_ATTEMPTS`;
  - `system_prompt()`, `user_prompt(snapshot)`, `prompt_sha256(snapshot)`;
  - `llm_config_from_env() -> LLMConfig` и `MissingEnvError`;
  - `ask_once(config, system, user, *, json_mode=True) -> str`;
  - `parse_answer(content) -> Answer`; `normalize_probabilities(probs) -> dict`;
  - `forecast(snapshot, config, ask=ask_once) -> Forecast` — три повтора, медиана, разброс, representative.

**Steps:**
- [ ] **Step 1: Тесты сначала (офлайн).** Ответы провайдера — фиксированные строки; `ask` подменяется счётчиком вызовов.
  - `test_normalize_probabilities_*` — сумма 100, округление, отказ на NaN/отрицательных/нулевой сумме;
  - `test_parse_answer_*` — валидный ответ, отсутствие ключа, шаг вне диапазона, пустой rationale/risks;
  - `test_three_repeats_median_spread` — 3 вызова, ожидаемые медианы и разброс;
  - `test_format_error_retries_once` — первый ответ битый, второй валидный, `attempts=2`;
  - `test_two_invalid_samples_fail` — прогноз не строится;
  - `test_missing_env_lists_names` — `MissingEnvError`, имена переменных, ключ не в тексте;
  - `test_json_mode_fallback_on_unsupported` — 400 с упоминанием `response_format` → повтор без JSON-режима.
- [ ] **Step 2:** Реализовать `system_prompt` (редакция 1) и `user_prompt` (канонический JSON снимка).
- [ ] **Step 3:** Реализовать `llm_config_from_env`, `llm_url`, `ask_once`: `temperature=0`, `max_tokens=2048`, `response_format=json_object`, заголовок `Authorization: Bearer`, таймаут 180 с, извлечение `choices[0].message.content`; ошибки транспорта — компактные, тело ≤512 байт.
- [ ] **Step 4:** Реализовать `parse_answer`: вырезание JSON-объекта, проверка типов/ключей/диапазонов, нормализация; `normalize_probabilities` — целочисленные сотые доли, корректировка остатка.
- [ ] **Step 5:** Реализовать `forecast`: три повтора, до двух попыток на повтор при ошибке формата, отказ провайдера — сразу исключение; медиана, разброс, representative; `Forecast` dataclass.
- [ ] **Step 6:** Прогнать тесты — PASS.

---

### Task 4: Артефакты, отчёт, `--log` payload и CLI

**Files:**
- Modify: `scripts/key_rate_llm_forecast.py`
- Modify: `.gitignore`
- Test: `scripts/key_rate_llm_forecast_test.py`

**Interfaces:**
- Produces:
  - `forecast_json(forecast) -> str`, `write_artifact(path, forecast) -> bool`;
  - `render_report(forecast) -> str`;
  - `log_payload(forecast) -> dict`, `write_log_payload(path, forecast)`;
  - `run(as_of=None, out_dir=ROOT, log_mode=False) -> int`;
  - CLI: `--as-of`, `--out-dir`, `--log`.

**Steps:**
- [ ] **Step 1:** Тесты: JSON детерминирован (два вызова дают одинаковый текст), сумма вероятностей в JSON = 100, есть `model`/`prompt_version`/`data_version`/`spread`; отчёт содержит таблицу вероятностей и оговорку; `--log` payload содержит `metric='cbr_rate'`, `target_date`, `predicted`, TODO #127 и не вызывает `ch_exec`/запись.
- [ ] **Step 2:** Реализовать `forecast_json` (порядок ключей, `ensure_ascii=False`, indent=2, LF) и `render_report`.
- [ ] **Step 3:** Реализовать `--log` payload: `status`, `todo`, `row` с `metric`, `forecast_date`, `target_date`, `predicted`, `actual/error_pct/error_bp = null`, `probabilities`, `expected_step_bp`, `model`, `prompt_version`, `data_version`.
- [ ] **Step 4:** Реализовать CLI и `run`; при провале — удалить устаревшие артефакты (JSON, отчёт, payload) и вернуть 1; при успехе — записать JSON, отчёт, а в `--log` ещё payload.
- [ ] **Step 5:** Добавить в `.gitignore` три имени артефактов с комментарием.
- [ ] **Step 6:** Прогнать тесты — PASS.

---

### Task 5: Makefile, документация и роадмап

**Files:**
- Modify: `Makefile` (`.PHONY`, цель `key-rate-llm-forecast`)
- Modify: `README.md` (команды, конфигурация, ограничения)
- Modify: `ARCHITECTURE.md` (§2 scripts, §5 env)
- Modify: `docs/CONFIGURATION.md`, `docs/DEVELOPMENT.md`, `docs/LIMITATIONS.md`
- Modify: `docs/ROADMAP_KEY_RATE.md` (фаза 4, статус пункта и протокол)

**Steps:**
- [ ] **Step 1:** Добавить цель:
  `key-rate-llm-forecast: uv run --python 3.12 python3 scripts/key_rate_llm_forecast.py $(ARGS)`
  (зависимостей вне stdlib нет; `--python 3.12` — конвенция репозитория).
- [ ] **Step 2:** README: строка в таблице команд; `LLM_MODEL` в таблице конфигурации; ограничение «экспериментальный трек, без записи в журнал до #127».
- [ ] **Step 3:** ARCHITECTURE: `scripts/` в §2; `LLM_MODEL` в §5 с оговоркой, что `LLM_BASE_URL`/`LLM_API_KEY` общие, а `LLM_MODEL` — модель этого трека.
- [ ] **Step 4:** docs/CONFIGURATION.md — строка `LLM_MODEL`; docs/DEVELOPMENT.md — команда цели и тестов; docs/LIMITATIONS.md — ограничение as-of/экспериментальности/отсутствия записи.
- [ ] **Step 5:** Роадмап: в фазе 4 добавить подпункт `phase-4-llm-track` со спекой/планом и статусом ✅ после живой проверки; зафиксировать, что запись в `forecast_log` ждёт #127.
- [ ] **Step 6:** Прогнать `make all`, Python-тесты, `make docs-check`; проверить, что артефакты не видны в `git status`.

---

### Task 6: Живой прогон, commit/push, PR

**Steps:**
- [ ] **Step 1:** `make ch-status`; если ClickHouse не отвечает — зафиксировать и не поднимать.
- [ ] **Step 2:** Проверить наличие LLM env без печати значений (например, `make env-check` и наличие имён переменных); если нет — зафиксировать `DONE_WITH_CONCERNS`.
- [ ] **Step 3:** `make key-rate-llm-forecast` — артефакты; `make key-rate-llm-forecast ARGS="--log"` — payload; проверить вероятности и `git status`.
- [ ] **Step 4:** `make all` — зелёный; `python3 scripts/key_rate_llm_forecast_test.py` — зелёный.
- [ ] **Step 5:** Обновить issue #189 (`Acceptance`, `Spec`, `Plan`, Branch, Status); commit, push, PR с `Closes #189` вне backticks и протоколом проверок.
