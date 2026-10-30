# План: follow-up аналитической записки ЦБ — отраслевая sensitivity ИЦП и ценовые ожидания

**Пункт:** `key-rate/phase-4-ppi-note-followups`, issue [#316](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/316); задачи [#317](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/317) (research), [#318](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/318) (ML).
**Спека:** [2026-10-21-ppi-note-followups-design.md](../specs/2026-10-21-ppi-note-followups-design.md).
**Дата:** 21 октября 2026 г.
**Роль:** mixed: Task 1 — research, Task 2 — ML.

---

## 0. Ветки и worktrees

- Umbrella docs (эта спека/план + строка роадмапа): `docs/ppi-note-followups`, worktree `.worktrees/ppi-note-followups`.
- Task 1: `research/ppi-industry-sensitivity`, worktree `.worktrees/ppi-industry-sensitivity`; PR ссылается на #317.
- Task 2: `feat/ppi-expectations-forecast`, worktree `.worktrees/ppi-expectations-forecast`; PR ссылается на #318.
- WIP=1: Task 2 не начинать до мержа Task 1 (или явного решения владельца).

---

## Task 1. Отраслевой потребительский ИЦП vs product-level mapping

### Шаг 1. Аудит рядов `rosstat_ppi_ved`

1. Прочитать `rosstat_ppi_ved` (read-only, через `scripts/lib/clickhouse.py`; не `kimi_reader`, у него нет гранта на сырую таблицу). Запрос:

```sql
SELECT code, name, min(date) AS from, max(date) AS to, count() AS n
FROM rosstat_ppi_ved
WHERE index_type = 'm/m'
GROUP BY code, name
ORDER BY code
```

2. Отобрать кандидатов по названиям: «потребительск», «пищев», «напит», «одежд», «инвестицион», «промежуточн». Сохранить `docs/research/2026-10-21-ppi-ved-candidates.csv` (код, имя, период, n).
3. Сверить с `rosstat_ppi_weights` (38 кодов, 2017–2026): есть ли веса у кандидатов; если нет — equal weights, явно помеченные как прокси.
4. Зафиксировать: точный агрегат «производство потребительских товаров» найден / не найден; если не найден — как собран прокси (какие коды, веса).

### Шаг 2. SA-агрегат

1. Добавить `scripts/ppi_industry_sensitivity.py` (read-only):
   - читает m/m по выбранным кодам;
   - строит уровень (base 100) и SA:
     - **direct:** X-13 `seats_log_cal` на агрегате;
     - **indirect:** X-13 по компонентам → агрегат по весам (если веса есть);
   - считает `mm_sa`, `saar3`;
   - проверяет ≥36 точек, непрерывность, положительные уровни.
2. R-часть — переиспользовать `scripts/ppi_sa_x13.R`/`ppi/data/ppi_sa_x13.R`.
3. Артефакты: `docs/research/2026-10-21-ppi-industry-sensitivity.csv` (target, series, ours, anchor, diff) и `.json` (метрики).

### Шаг 3. Сверка

1. Взять 11 точных якорей и 14 качественных проверок из `docs/research/2026-10-21-ppi-sa-text-anchors-12.csv`.
2. Посчитать MAE/max/bias/corr по food/non-food/core и overall; pass/fail качественных.
3. Сравнить с текущим `ppi_sa`: exact MAE **1,538**; qualitative **6 pass / 8 fail**.
4. Проверить чувствительность: исключить алкоголь/табак, если отраслевой агрегат их включает; зафиксировать оба варианта.

### Шаг 4. Решение

- **go:** отраслевой агрегат даёт MAE < 1,538 и/или больше pass без ухудшения core → research-документ с кандидатом новой taxonomy; отдельный Go-follow-up (не в этом item).
- **no-go:** не лучше → подтверждаем product-level mapping; запись в #266.
- **Не заявлять паритет:** это измерение, не подгонка.

### Шаг 5. Документ

`docs/research/2026-10-21-ppi-industry-sensitivity.md`:
- источник/коды/покрытие;
- SA-агрегат (direct/indirect);
- таблица 11 exact + 14 qualitative;
- сравнение с `ppi_sa`;
- решение go/no-go;
- «что не проверено» (PIT, ревизии, веса, услуги).

---

## Task 2. Ценовые ожидания как признаки прогноза ИПЦ/ИЦП

Протокол ниже предрегистрирован до живых метрик. Реализация — research-only `scripts/models/ppi/ppi_monthly_forecast.py`; production CPI/PPI, `forecast_log`, registry/promotion, DDL, витрины, гранты, DAG и `ppi_sa` не меняются.

### Шаг 1. Таргеты, cutoff и фолды

1. CPI all — NSA m/m `rosstat_ipc_mes.percent - 100`, п.п.; PPI `total/food/nonfood/core` — `ppi_sa.mm_sa`, п.п. м/м, `spec_version='ppi-sa-2026-v1'`.
2. Origin `O`, target `O+h`; h1/h3/h6 — отдельные direct monthly targets, не `saar3`. Expanding train origins `t <= O-h-1`, `MIN_TRAIN=36`; первый test fold имеет 36 origins.
3. Cutoff — 8-е M+1 после `O`, 23:59:59 MSK. Survey берётся из `v_series_publication`, `source='cbr_survey'`, строго `published_at < cutoff`: `quality='date'` — точная дата с effective 23:59:59 MSK, `quality='fallback'` — конец M+1. Нет publication row — пропуск/fail closed, не фиксированный лаг.
4. Base и expectations получают одинаковые calendar boundaries; парные метрики — только на общих `(target,horizon,origin)`. Минимум 12 folds primary и 6 robustness, иначе `INSUFFICIENT`.

### Шаг 2. Признаки и парные модели

1. Aggregate: `price_expectations`, `price_expectations_sa`, `price_growth_expected`; лаги 0/1/2/3 от последнего опубликованного к cutoff месяца.
2. `b1_last` — последний доступный target, справочный benchmark. `b2_ar1` — OLS AR(1)+11 monthly dummies. `b3_enet` — ElasticNetCV на AR/month-dummy блоке; median imputation/scaling/CV строго внутри train, NaN не превращается в 0 заранее.
3. Primary pairs: `b2_ar1_expectations vs b2_ar1`, `b3_enet_expectations vs b3_enet`; парные модели отличаются только aggregate survey-блоком. Primary targets: CPI all и PPI `total/food/nonfood/core`.
4. Secondary consumer: B3 `industry-only` и `aggregate+industry` против того же `b3_enet`, без расширения primary-семейства. Consumer target — только проверенный pinned research-винтаж Task 1; нет industry publication calendar — secondary fail closed, aggregate primary не ломается.

### Шаг 3. Метрики и правило решения

1. RMSE/MAE для B1–B3; `delta=RMSE(base)-RMSE(expectations)`; DM-HLN с HAC `max(h-1,ceil(T^(1/3)))`; 95% paired circular block bootstrap CI delta, `block=3`, `n=2000`, `seed=20261027`.
2. MDE `2.802*sd(d)/sqrt(n)/(RMSE_A+RMSE_B)`; `n<2` и нулевая дисперсия differential не дают фиктивной значимости. Holm α=0.05 — только для primary DM p-values; secondary и robustness отдельно.
3. `promote` — только рекомендация открыть research follow-up: primary consumer PPI `food` хотя бы на h1/h3 имеет `delta>0`, Holm-adjusted DM p<0.05 и весь 95% CI delta выше 0. Secondary consumer подтверждает механизм, но сам не даёт `promote`. Production promotion запрещён.
4. Иначе `iterate` или `reject` по знаку эффекта и MDE; при n ниже минимума — `INSUFFICIENT`. Robustness 2022–2023 отчитывается отдельно и не меняет primary-вывод. После метрик не менять таргет, окна, признаки, primary-семейство или seed без нового post-hoc эксперимента.

### Шаг 4. Официальный run и документ

1. Выполнить `ML_LOG=1 MLFLOW=1 make ppi-monthly-forecast ARGS="--out-dir docs/research"`; сохранить run id, config/sample hashes, SHA-256 артефактов и проверить run/folds в `v_ml_runs`/`v_ml_error_by_fold`. Registry/promote не использовать.
2. Создать `docs/research/2026-10-21-ppi-expectations-forecast.md`, `.json` и `-predictions.csv`: данные/винтажи/SHA, окна и n по target×h, RMSE/MAE B1–B3, primary DM/Holm/CI/MDE, secondary consumer, robustness 2022–2023, сравнение с запиской и research-вердикт.
3. Явно указать: survey as-of соблюдён; CPI/PPI targets latest-vintage; полный PIT ревизий не проверен. Независимо пересчитать метрики из CSV и повторить CLI для проверки детерминизма без дублирования официального лога.
4. Записать результат в #318 и информативно в #76, не закрывая umbrella и не обещая production-интеграцию.

---

## Проверки

```bash
make docs-check
make ppi-monthly-forecast-test
make all
```

- Прод-код, DDL, витрины и гранты не меняются; `ppi_sa` не пересчитывается.
- `make docs-check`, offline Python-тесты и `make all` — зелёные; independent recalc и deterministic replay совпадают с официальными артефактами.
- Решения — в #266 (Task 1) и #318/#76 (Task 2).

---

## Review Focus

1. **Task 1:** точные коды/ряды Росстата, а не «похожие по названию»; веса/прокси явно названы; 11 exact + 14 qualitative — те же, что в T4/12-issue research; вывод не подгоняет веса.
2. **Task 2:** as-of без будущих ревизий; h1/h3/h6 предзарегистрированы; DM/CI/MDE; потребительский сектор не смешан с aggregate; baseline не переобучен.
3. **Границы:** записка — не официальная методика; паритет не заявляется; production не меняется.
4. **Трекер:** issue #316/#317/#318, Project V2, роадмап — согласованы.
