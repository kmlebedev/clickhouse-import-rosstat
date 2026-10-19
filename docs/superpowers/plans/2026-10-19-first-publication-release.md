# #191 `key-rate/phase-4-first-publication` — релизная ревизия

> **For agentic workers:** план продолжает [2026-10-18-first-publication.md](2026-10-18-first-publication.md); спека — [2026-10-18-first-publication-design.md](../specs/2026-10-18-first-publication-design.md). Пункт — `key-rate/phase-4-first-publication`, issue [#191](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/191). Роль — `role:ml`.

**Goal:** довести первую публикацию прогноза ставки к заседанию 23.10.2026 до состояния «можно выпускать»: свежий снимок, пересчитанные треки, три строки `forecast_log`, обновлённые статья и чек-лист. Пост на smart-lab, ссылка в #191 и пост-апдейт после 23.10 — за владельцем.

**Architecture:** снимок — MCP ClickHouse (`kimi_reader`), только чтение витрин `v_*`; ряды обновляются штатными Go-импортёрами через `make import`; прогнозы — локальные JSON-артефакты Python-треков; запись — единственный путь `POST /v1/key_rate_forecast` (`cmd/ingest`), не прямой `INSERT`.

**Tech Stack:** Python 3.12 stdlib, Markdown/SVG, ClickHouse MCP (read-only), Go ingest, GitHub MCP.

## Ветка и предусловия

- Ветка `feat/key-rate-publication-release`, worktree `.worktrees/key-rate-publication-release` от `main`.
- Первый коммит — этот план и раздел «Релизная ревизия» в спеке.
- `#127`, `#189`, `#190` закрыты; статья влита PR #199; dev-строки `forecast_log` есть.
- Секреты — только `~/.config/rosstat/env` через `make`; значения не печатать. `make ch-up`/`ch-down`, `make deploy-staging*` не запускать.

## Global Constraints

- Каждое число — с источником и датой; устаревший вход — «нет обновления», а не последнее значение молча.
- Вероятности нормированы к 100 %; E[step] — взвешенное среднее исторических шагов классов.
- Три трека не усредняются; формальный прогноз — статистический, LLM и рынок — референсы.
- Паритет CB_COMP не заявляется: strict parity измерен, вердикт «не близок», TOST не пройден.
- Не давать инвестиционных рекомендаций и не утверждать превосходство над рынком/консенсусом без теста.
- `forecast_log` — live-журнал; fold-предсказания — в `ml_predictions`. Повторная запись идемпотентна.

## Review Focus

1. Числа статьи совпадают со снимком и артефактами; даты/единицы не перепутаны.
2. Новые строки журнала соответствуют артефактам: `predicted`, `probabilities`, `model`, `data_version`, `prompt_version`.
3. Расхождение контуров объяснено, треки не усреднены.
4. Пороги — число и условие; ликвидность KEYRATE приведена рядом.
5. В статье нет `PENDING`/`TBD`/`TODO`; статус «черновик» снят.
6. Чек-лист §9 отражает факт: готово / ждёт публикации владельцем / ждёт 23.10.
7. Тон smart-lab, без рекомендаций.

---

### Task 1: План и спека — первые коммиты

**Files:**
- Create: `docs/superpowers/plans/2026-10-19-first-publication-release.md`
- Modify: `docs/superpowers/specs/2026-10-18-first-publication-design.md` (раздел «Релизная ревизия 2026-10-19»)

**Steps:**
- [ ] **Step 1:** Зафиксировать план и границу «агент / владелец».
- [ ] **Step 2:** Самопроверка: нет `TBD`/`TODO`, охват — один PR, критерий #191 учтён.
- [ ] **Step 3:** Закоммитить до данных и статьи.

---

### Task 2: Обновить входы

**Steps:**
- [ ] **Step 1:** `make env-check`, `make ch-status` — стенд доступен, ключи заданы (значения не печатать).
- [ ] **Step 2:** `make import STAT=rosstat_ipc_weeks` — недельный ИПЦ; проверить `max(date)`.
- [ ] **Step 3:** `make import STAT=sber_izmenenie_trat`, `make import STAT=sber_consumper_spending_index` — СберИндекс.
- [ ] **Step 4:** `make import STAT=keyrate_futures`, `ofz_curve`, `cbr_ruania`, `cbr_key_rate`, `cbr_currency_usd`, `cbr_gold` — рыночный контур и ставка.
- [ ] **Step 5:** MCP-сверка max-дат и последних значений; если источник не обновился — «нет обновления» с датой; если данные есть, а импортёр их не взял — отдельный дефект, не маскировать.

---

### Task 3: Снимок и артефакты

**Files:**
- Create (gitignored): `build/key-rate-publication-2026-10-19/{snapshot.json,snapshot.md,chart-data.json,make_snapshot.py,make_charts.py}`
- Create (gitignored): `key_rate_model_forecast.json`, `key_rate_llm_forecast.json`, `key_rate_llm_forecast_log_payload.json`, `cpi_nowcast_forecast.json`

**Steps:**
- [ ] **Step 1:** Перенести `make_snapshot.py`/`make_charts.py` из `.worktrees/key-rate-first-publication/build/key-rate-publication-2026-10-18/`, обновить дату/каталог.
- [ ] **Step 2:** Снять снимок через MCP; записать `snapshot.json`/`snapshot.md`; посчитать `sha256` — `data_version` публикации.
- [ ] **Step 3:** `make key-rate-model`; проверить `forecast_date`, `meeting_date=2026-10-23`, `rate_before=14.0`, `sample_size=108`.
- [ ] **Step 4:** `make cpi-nowcast` (v1); v2 short-sample не перезапускать, если месячные входы и МП не менялись, — дату прогона v2 указать явно. Если изменились — полный `make cpi-monthly ARGS="--sample short"` (не `--quick`).
- [ ] **Step 5:** `make key-rate-llm-forecast ARGS="--log"`; проверить 3 повтора, медиану, разброс, `prompt_version=1`, `data_version`.
- [ ] **Step 6:** `make key-rate-forecast-log ARGS="--dry-run"` — 3 строки, суммы 100 ± 0.1, `predicted` в (0, 1000).

---

### Task 4: Статья, графики, чек-лист

**Files:**
- Modify: `docs/publications/2026-10-18-key-rate.md`
- Modify: `docs/publications/2026-10-18/checklist.md`
- Modify: `docs/publications/2026-10-18/{rate-inflation,keyrate-ofz,distributions}.svg`
- Modify: `docs/publications/2026-10-18/chart-data.json`

**Steps:**
- [ ] **Step 1:** Обновить шапку/подзаголовок, §1–§8, приложения А/Б, сноски, дисклеймер; снять «черновик».
- [ ] **Step 2:** Перегенерировать SVG и `chart-data.json`; проверить XML и совпадение чисел.
- [ ] **Step 3:** Обновить чек-лист §9: пункт 8 — «готово» после записи; снимок — «готово»; пост/пост-апдейт — «ждёт владельца».
- [ ] **Step 4:** Проверить, что нет `PENDING`/`TBD`/`TODO` и что паритет CB_COMP назван не достигнутым.

---

### Task 5: Запись в `forecast_log` и проверка

**Steps:**
- [ ] **Step 1:** `make build-ingest`; запустить `make run-ingest` в фоне (порт :8081; если занят — :18081, чужой процесс не убивать).
- [ ] **Step 2:** `INGEST_URL=http://127.0.0.1:8081 make key-rate-forecast-log` — 3 строки.
- [ ] **Step 3:** MCP `v_key_rate_dashboard`: `target_date=2026-10-23`, `stale=0`, три трека, вероятности, `data_version`, `prompt_version`; `error_bp IS NULL` до заседания.
- [ ] **Step 4:** Повторить запись — идемпотентно, без дублей.
- [ ] **Step 5:** Staging не трогать; если владелец считает рабочим контуром staging — передать точную команду и не считать проверенным.

---

### Task 6: Проверки, коммит, PR

**Steps:**
- [ ] **Step 1:** `make docs-check`.
- [ ] **Step 2:** `uv run --python 3.12 python3 scripts/key_rate_forecast_log_test.py`.
- [ ] **Step 3:** `make all`.
- [ ] **Step 4:** `make mcp-check` — зелёный или названная внешняя причина.
- [ ] **Step 5:** `git status` — без `build/`, артефактов и секретов; коммиты: план+спека, данные+статья, чек-лист+роадмап.
- [ ] **Step 6:** PR с `Refs #191` (не `Closes`), протокол приёмки — комментарием в PR; роадмап — `🔶` «релизная ревизия готова, ждёт публикации владельцем».

## Протокол проверки

Заполняется по факту:

- [ ] `make docs-check` — …
- [ ] `make all` — …
- [ ] `make mcp-check` — …
- [ ] Снимок: дата/время, ClickHouse, `sha256`, max-даты.
- [ ] `forecast_log`: 3 строки, значения, `data_version`, повтор.
- [ ] Статья: `PENDING`/`TBD`/`TODO` — нет; ссылки живы.
