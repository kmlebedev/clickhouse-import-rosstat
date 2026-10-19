# Классификатор шоков по каналам прогноза ставки — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** разметить явные шоки ключевой ставки из первоисточника, посчитать причинные веса наблюдений по каналам прогноза и научить `key_rate_model` применять их к правилу Тейлора и упорядоченному логиту.

**Architecture:** Go-импортёр календаря сохраняет признак внеочередного заседания в `events_calendar.emergency`; Python-скрипт `shock_classifier.py` собирает панель заседаний по каналам as-of, считает walk-forward аномальный скор (Махаланобис + Ledoit–Wolf) и веса, отдельным слоем обучает описательный логистический классификатор явных шоков; `key_rate_model.py` получает флаг `--shock-weights` и взвешенные версии обоих методов.

**Tech Stack:** Go 1.x (calendar/), ClickHouse, Python 3.12 через `uv`, `statsmodels>=0.15`, `scikit-learn`, `pandas`, `numpy`, `scipy`; тесты — `unittest` (Python) и `go test` (Go).

**Spec:** `docs/superpowers/specs/2026-10-19-key-rate-shock-classifier-design.md`

## Global Constraints

- Python запускается только через `uv run --with ... --python 3.12`; `statsmodels>=0.15` — обязательный пин.
- Секреты — только `os.Getenv`/Makefile; содержимое `~/.config/rosstat/env` не читать и не печатать.
- ClickHouse-транспорт — `ch_query` из `scripts/extrapolation_aisc.py`; в тестах сети и ClickHouse нет.
- Скрипт по умолчанию в ClickHouse не пишет; `ML_LOG=1` — только через `scripts/ml_experiment.py`.
- Новых таблиц/витрин ClickHouse нет; `events_calendar` — единственное изменение схемы (`emergency UInt8 DEFAULT 0`, идемпотентный `ALTER`).
- Без `--shock-weights` поведение и артефакты `key_rate_model` не меняются.
- Все числовые ряды — as-of: точка, опубликованная после даты заседания, не участвует.
- `make all` (gofmt, golangci-lint, `go vet`, `go test -race`, сборки) и `make docs-check` — зелёные.

## Review Focus

1. **Миграция `events_calendar` на живой таблице**: `ALTER ... ADD COLUMN IF NOT EXISTS` выполняется до вставки, `v_events_calendar` (`SELECT *`) видит колонку, повторный импорт не плодит строк.
2. **Причинность признаков**: ни одна точка, опубликованная после заседания, не попадает в его строку; статистики аномальной модели — только по прошлым заседаниям.
3. **Пропуски каналов**: отсутствующая точка — `None`, а не 0; покрытие и `missing_count` публикуются; `core`-вариант не падает на короткой истории.
4. **Эквивалентность взвешенных fit'ов**: при единичных весах `WLS` совпадает с `OLS`, `WeightedOrderedModel` — с `OrderedModel`; веса не ломают сходимость MLE.
5. **Неизменность официального пути**: без флага `key_rate_model` даёт прежние числа и артефакты; флаг не меняет бенчмарки B1–B3.

---

## Task 1: Признак внеочередного заседания в `events_calendar`

**Files:**
- Modify: `calendar/events.go`
- Modify: `calendar/events_test.go`
- Modify: `ARCHITECTURE.md` (§6.1/§6.2 — DDL и описание календаря)

**Interfaces:**
- Produces: колонка `events_calendar.emergency UInt8 DEFAULT 0`; `parseCalMP` возвращает `[]calMPMeeting` с полями `Date time.Time`, `Emergency bool`; `eventsView` содержит описание `emergency`.
- Consumes: существующие `eventsDdl`, `calMPInsert`, `calMPText`, `calMPIsMeeting`, `calMPIsMeetingTable`, `eventsView`.

- [ ] **Step 1: Написать падающий тест разметки**

В `calendar/events_test.go` добавить `TestCalMPEmergencyFlag`: на фикстуре `testdata/cal_mp.html` `parseCalMP` возвращает 118 заседаний, среди них ровно 6 с `Emergency=true`: `2014-03-03`, `2014-12-15`, `2022-02-28`, `2022-04-08`, `2022-05-26`, `2023-08-15`; остальные — `false`. Второй тест `TestEventsCalendarEmergencyColumn` проверяет, что `eventsDdl` содержит `emergency UInt8 DEFAULT 0`, `calMPInsert` содержит `emergency`, а `eventsView.Columns["emergency"]` непуст.

- [ ] **Step 2: Убедиться, что тест падает**

Run: `go test ./calendar/ -run 'TestCalMPEmergencyFlag|TestEventsCalendarEmergencyColumn' -v`
Expected: FAIL — `calMPMeeting`/`Emergency`/`emergency` ещё нет.

- [ ] **Step 3: Реализовать**

В `calendar/events.go`:

- добавить `emergency UInt8 DEFAULT 0` в `eventsDdl`;
- после `conn.Exec(ctx, eventsDdl)` выполнить `conn.Exec(ctx, "ALTER TABLE events_calendar ADD COLUMN IF NOT EXISTS emergency UInt8 DEFAULT 0")`;
- добавить `emergency` в `calMPInsert` и в `batch.Append`;
- ввести тип `calMPMeeting struct { Date time.Time; Emergency bool }`;
- `parseCalMP` возвращает `[]calMPMeeting`: для каждой карточки/строки флаг `strings.Contains(strings.ToLower(calMPText(text)), "внеочередн")`;
- в `Import` цикл по `calMPMeeting` пишет `meeting.Emergency` как `uint8` (0/1);
- в `eventsView.Columns` добавить `"emergency": "1 — внеочередное заседание Совета директоров ЦБ, 0 — плановое"`.

В `ARCHITECTURE.md` §6.2 обновить канонический DDL `events_calendar` и описание; при необходимости — строку §6.1 о календаре.

- [ ] **Step 4: Прогнать тесты**

Run: `go test ./calendar/... -v`
Expected: PASS, включая существующие тесты 118 дат.

- [ ] **Step 5: Живой импорт**

Run: `make import STAT=events_calendar`
Expected: `Cal_mp: 118 meeting dates inserted`; затем SQL `SELECT event_date, emergency FROM events_calendar FINAL WHERE category='cbr' AND emergency=1 ORDER BY event_date` — 6 строк; повторный импорт идемпотентен.

- [ ] **Step 6: Коммит**

```bash
git add calendar/events.go calendar/events_test.go ARCHITECTURE.md
git commit -m "calendar: keep emergency flag of cbr meetings"
```

---

## Task 2: Панель заседаний, метки и каналы

**Files:**
- Create: `scripts/shock_classifier.py`
- Create: `scripts/shock_classifier_test.py`
- Create: `scripts/testdata/shock_classifier/` (синтетические CSV: `meetings.csv`, `key_rate.csv`, `cpd.csv`, `channels.csv`)

**Interfaces:**
- Consumes: `key_rate_model.build_decisions`, `Decision`, `asof`, `annualize_saar`, `row_date`, `row_float`; таблицы/ряды из спеки §4.2.
- Produces: `PanelRow` (`decision: Decision`, `emergency: bool`, `explicit_shock: bool`, `features: dict[str, float|None]`, `missing_count: int`); `build_panel(meetings, key_rate, channel_points, ...) -> list[PanelRow]`; `load_channels() -> dict[str, list[tuple[date, float]]]`; `label_shock(emergency, step_bp) -> bool`.

- [ ] **Step 1: Написать падающие тесты разметки и as-of**

```python
def test_label_shock_requires_emergency_and_change():
    assert label_shock(True, -300) is True
    assert label_shock(True, 0) is False
    assert label_shock(False, 100) is False

def test_point_published_after_meeting_is_not_used():
    # точка за март 2022 опубликована 2022-04-20; заседание 2022-04-08 её не видит
    assert feature_asof(points, date(2022, 4, 8), monthly=True) == february_value

def test_missing_channel_is_none_not_zero():
    row = build_panel(...)[0]
    assert row.features["infl_exp_expected"] is None
    assert row.missing_count >= 1

def test_delta_is_to_previous_meeting():
    rows = build_panel(...)
    assert rows[1].features["pi_saar_chg"] == pytest.approx(rows[1].features["pi_saar"] - rows[0].features["pi_saar"])
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `uv run --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/shock_classifier_test.py -v`
Expected: FAIL — модуль/функции ещё не созданы.

- [ ] **Step 3: Реализовать панель**

В `scripts/shock_classifier.py`:

- константы `MIN_TRAIN=24`, `SEED=20261019`, имена артефактов, `SPEC_PATH`;
- `load_meetings_with_emergency()` — SQL к `events_calendar FINAL` (`event_date, category, title, emergency`) с фильтром канонического `title`;
- `load_key_rate()` и `load_mp_summary_deltas()` — переиспользовать из `key_rate_model`;
- `load_channels()` — по одному SQL на ряд; имена рядов и таблиц — из спеки §4.2;
- `PanelRow` и `build_panel(...)`: `Decision` из `key_rate_model.build_decisions`, флаг `emergency`, `explicit_shock=label_shock(...)`, признаки через `feature_asof` (месячные — доступны в конце M+1, дневные — в день), дельты к предыдущему заседанию, `missing_count`;
- `feature_asof(points, meeting_date, monthly: bool)` — чистая функция, тестируемая без ClickHouse;
- `export_fixtures(dir)` — запись синтетических CSV для офлайн-тестов.

- [ ] **Step 4: Прогнать тесты**

Run: та же команда.
Expected: PASS; тесты не обращаются к ClickHouse/сети.

- [ ] **Step 5: Коммит**

```bash
git add scripts/shock_classifier.py scripts/shock_classifier_test.py scripts/testdata/shock_classifier
git commit -m "shock classifier: meeting panel and as-of channels"
```

---

## Task 3: Аномальный скор, веса и описательный классификатор

**Files:**
- Modify: `scripts/shock_classifier.py`
- Modify: `scripts/shock_classifier_test.py`

**Interfaces:**
- Produces: `fit_anomaly(train_rows, features) -> AnomalyModel`; `anomaly_score(model, row) -> float`; `weight_from_score(score, median_train) -> float`; `walk_forward_weights(rows, min_train) -> list[float]`; `fit_shock_classifier(train_rows, features) -> LogisticRegression`; `walk_forward_classifier(rows, min_train) -> list[float|None]`; `classifier_metrics(labels, probs) -> dict`.
- Consumes: `PanelRow` из Task 2.

- [ ] **Step 1: Написать падающие тесты**

```python
def test_weights_are_bounded_and_monotone():
    assert all(0.05 <= w <= 1.0 for w in weights)
    assert weight_from_score(10.0, 1.0) <= weight_from_score(2.0, 1.0) <= weight_from_score(1.0, 1.0)

def test_first_min_train_weights_are_one():
    assert walk_forward_weights(rows, min_train=24)[:24] == [1.0] * 24

def test_test_row_does_not_change_training_statistics():
    before = walk_forward_weights(rows, 24)
    after = walk_forward_weights(rows[:-1] + [mutated_last_row], 24)
    assert before[:-1] == after[:-1]

def test_explicit_shocks_are_downweighted():
    assert median(weights_for_explicit_shocks) < median(weights_for_normal)

def test_classifier_separates_synthetic_shocks():
    assert classifier_metrics(labels, probs)["roc_auc"] > 0.9

def test_degenerate_classifier_fold_is_skipped():
    assert walk_forward_classifier(no_positive_rows, 24) == [None] * len(no_positive_rows)
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `uv run --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/shock_classifier_test.py -v`
Expected: FAIL — функций ещё нет.

- [ ] **Step 3: Реализовать**

- стандартизация: медиана/IQR по обучающему окну; `IQR=0` → std; затем 1; пропуск → 0;
- `LedoitWolf` на обучающем окне; `d_t` — расстояние Махаланобиса;
- `r_t = d_t / median(d_train)`; `w_t = 1/(1+max(0, r_t−1)^2)`, клип `[0.05, 1]`; первые `MIN_TRAIN` — 1.0;
- `walk_forward_weights` — только прошлые строки для каждой `t`;
- `LogisticRegression` (L2, `C=1.0`, `class_weight='balanced'`, `max_iter=1000`) на стандартизованных признаках; фолды без двух классов/положительных — `None`;
- `classifier_metrics` — ROC-AUC, PR-AUC, Brier, precision/recall при 0.5; при отсутствии положительных — `None`/`nan` с явной пометкой.

- [ ] **Step 4: Прогнать тесты**

Run: та же команда.
Expected: PASS.

- [ ] **Step 5: Коммит**

```bash
git add scripts/shock_classifier.py scripts/shock_classifier_test.py
git commit -m "shock classifier: causal anomaly weights and validation layer"
```

---

## Task 4: Веса в `key_rate_model`

**Files:**
- Modify: `scripts/key_rate_model.py`
- Modify: `scripts/key_rate_model_test.py`

**Interfaces:**
- Produces: `load_shock_weights(path) -> dict[date, float]`; `WeightedOrderedModel(OrderedModel)`; `_taylor_fit(train, weights=None)`, `_ordered_fit(train, weights=None)`; CLI `--shock-weights PATH`.
- Consumes: `Decision`, `Row`, `walk_forward`, `_hac_fit`, `OrderedModel`.

- [ ] **Step 1: Написать падающие тесты**

```python
def test_unit_weights_match_unweighted_wls():
    # WLS с weights=1 даёт те же params, что OLS
def test_weighted_ordered_model_matches_unweighted_at_unit_weights():
    # params совпадают в пределах 1e-6
def test_load_shock_weights_rejects_bad_values():
    # 0, >1, дубль даты -> ValueError
def test_missing_weight_defaults_to_one_with_warning():
    # дата вне CSV -> 1.0, счётчик предупреждений
def test_no_flag_artifacts_unchanged():
    # run_summary без флага даёт прежние строки отчёта/CSV
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy --python 3.12 python3 scripts/key_rate_model_test.py -v`
Expected: FAIL — функций/флага ещё нет.

- [ ] **Step 3: Реализовать**

- `load_shock_weights`: CSV `meeting_date,weight`; `date.fromisoformat`, уникальность, `0 < w <= 1`; отсутствующие даты — `1.0` + счётчик;
- `WeightedOrderedModel(OrderedModel)`:
  ```python
  def __init__(self, endog, exog, freq_weights=None, **kwds):
      self.freq_weights = None if freq_weights is None else np.asarray(freq_weights, dtype=float)
      super().__init__(endog, exog, **kwds)
  def loglikeobs(self, params):
      ll = super().loglikeobs(params)
      return ll if self.freq_weights is None else ll * self.freq_weights
  ```
- `_taylor_fit(train, weights=None)`: `sm.WLS(y, design, weights=weights)` при весах, иначе прежний `_hac_fit`;
- `_ordered_fit(train, weights=None)`: `WeightedOrderedModel` при весах, иначе `OrderedModel`;
- `walk_forward` пробрасывает веса по `meeting_date`; B1–B3 — без весов;
- CLI `--shock-weights`; при флаге — раздел «Веса шоков» в отчёте: покрытие, распределение, weighted/unweighted RMSE/MAE по `taylor`/`ordered_logit`, DM-HLN p, топ-10 пониженных;
- без флага — прежние артефакты и числа.

- [ ] **Step 4: Прогнать тесты**

Run: та же команда.
Expected: PASS; тест неизменности без флага — зелёный.

- [ ] **Step 5: Живая проверка**

Run: `make key-rate-model` (baseline), затем `make key-rate-model ARGS="--shock-weights shock_classifier_weights.csv"`.
Expected: baseline-числа не изменились; во втором прогоне появился раздел «Веса шоков» с DM-HLN.

- [ ] **Step 6: Коммит**

```bash
git add scripts/key_rate_model.py scripts/key_rate_model_test.py
git commit -m "key rate model: optional shock weights for taylor and ordered logit"
```

---

## Task 5: Артефакты, `ML_LOG`, Makefile и документы

**Files:**
- Modify: `scripts/shock_classifier.py`
- Modify: `scripts/shock_classifier_test.py`
- Modify: `Makefile`
- Modify: `README.md`, `docs/DEVELOPMENT.md`, `docs/OPERATIONS.md`, `docs/LIMITATIONS.md` (по `sync-readme-architecture`)
- Modify: `docs/ROADMAP_KEY_RATE.md` (статус пункта)

**Interfaces:**
- Produces: `write_report(...)`, `write_weights_csv(...)`, `write_forecast_json(...)`, `run(...)`; цели `make shock-classifier`, `make shock-classifier-test`; `ML_LOG`-прогон `key_rate_shock_classifier`.
- Consumes: `PanelRow`, `walk_forward_weights`, `walk_forward_classifier`, `load_shock_weights`.

- [ ] **Step 1: Написать падающие тесты артефактов**

```python
def test_weights_csv_has_causal_columns():
    # meeting_date, emergency, explicit_shock, step_bp, y3, anomaly_score, weight, shock_prob, missing_count
def test_forecast_json_is_written_for_next_meeting():
    # score/weight/features/as_of
def test_ml_log_payload_contains_metrics_and_predictions():
    # experiment='key_rate_shock_classifier', predictions двух видов
def test_report_contains_required_sections():
    # "Явные шоки", "Покрытие каналов", "Метрики классификатора", "Веса", "С весами / без", "Ограничения"
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `uv run --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/shock_classifier_test.py -v`
Expected: FAIL — писателей/цели ещё нет.

- [ ] **Step 3: Реализовать артефакты и журнал**

- `shock_classifier_report.md` — разделы из спеки §8;
- `shock_classifier_weights.csv` — колонки из спеки §8;
- `shock_classifier_forecast.json` — as-of срез следующего заседания;
- `ML_LOG=1` — `ml_experiment.start_run(experiment='key_rate_shock_classifier', ...)`, метрики/предсказания/артефакты; ошибка журнала — warning, не падение прогона;
- `Makefile`: `shock-classifier` и `shock-classifier-test` по конвенции `uv run --with ... --python 3.12`;
- документы: упоминание цели, артефактов, `--shock-weights`, `events_calendar.emergency`; статус пункта в роадмапе — ⏳ до приёмки, ✅ после.

- [ ] **Step 4: Прогнать тесты и документы**

Run: `make shock-classifier-test`, `make docs-check`.
Expected: PASS/зелёный.

- [ ] **Step 5: Коммит**

```bash
git add scripts/shock_classifier.py scripts/shock_classifier_test.py Makefile README.md docs/DEVELOPMENT.md docs/OPERATIONS.md docs/LIMITATIONS.md docs/ROADMAP_KEY_RATE.md
git commit -m "shock classifier: report, ml log and make target"
```

---

## Task 6: Живая приёмка, роадмап и PR

**Files:**
- Modify: `docs/ROADMAP_KEY_RATE.md` (статус ✅ + протокол)
- GitHub: issue #200, Project V2 #11, PR

- [ ] **Step 1: Полный набор проверок**

```bash
make all
make docs-check
make shock-classifier-test
make import STAT=events_calendar
make shock-classifier
make key-rate-model
make key-rate-model ARGS="--shock-weights shock_classifier_weights.csv"
ML_LOG=1 make shock-classifier
```

- [ ] **Step 2: Записать протокол**

В отчёт и комментарий PR внести фактические числа: заседаний, явных шоков, emergency-hold, покрытие, AUC/PR, min/median веса, weighted/unweighted RMSE/MAE и DM-HLN p, вес следующего заседания, подтверждение неизменности baseline.

- [ ] **Step 3: Обновить роадмап и issue**

Статус `phase-4-shock-classifier` — ✅ с датой и числами; issue #200 — заголовок/тело/поля Project V2 (`Status=In Review` до мержа, `Done` после); PR — `Closes #200` (вне backticks), протокол приёмки комментарием.

- [ ] **Step 4: Финальное ревью ветки**

`superpowers:requesting-code-review` на `deepseek/deepseek-flash`; исправить находки; затем `superpowers:verification-before-completion` — повторный прогон проверок.

- [ ] **Step 5: Коммит и PR**

```bash
git add docs/ROADMAP_KEY_RATE.md
git commit -m "docs: accept key rate shock classifier"
git push -u origin feat/key-rate-shock-classifier
```

Открыть один PR в `main` с описанием: пункт `key-rate/phase-4-shock-classifier`, источник, схема `events_calendar.emergency`, как запускать (`make shock-classifier`, `make key-rate-model ARGS="--shock-weights ..."`), пример проверки SQL, протокол приёмки.

---

## Self-review плана

- **Покрытие спеки:** §3 (Task 1), §4 (Task 2), §5–6 (Task 3), §7 (Task 4), §8 (Task 5), §10 (Task 6). Пропусков нет.
- **Шаги:** каждый шаг — одно действие с проверяемым результатом; тела моделей не переписаны, кроме формулы веса и подкласса `WeightedOrderedModel`, которые спека фиксирует.
- **Типы:** `PanelRow`, `Decision`, `AnomalyModel`, `load_shock_weights`, `WeightedOrderedModel` согласованы между задачами.
- **Review Focus:** пять пунктов покрыты тестами Task 1–4 (миграция, as-of, пропуски, эквивалентность весов, неизменность без флага).
- **Пропорция:** план короче спеки; код-блоки — только сигнатуры/формулы, которые нельзя вывести из тестов.
