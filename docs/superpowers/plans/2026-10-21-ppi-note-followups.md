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

### Шаг 1. Baselines

1. **CPI:** использовать существующий nowcast #235/#76 (`scripts/models/cpi/cpi_monthly.py`, `cpi_monthly_v3.py`) как точку отсчёта; если модели слишком тяжёлые — упрощённый AR/ARIMA на `rosstat_ipc_mes` (m/m SAAR).
2. **PPI:** новый baseline `scripts/models/ppi/ppi_monthly_forecast.py`:
   - AR/ARIMA на `ppi_sa` (total, food, non-food, core) или VAR на `rosstat_ppi_ved`;
   - h1/h3/h6; target — `saar3` или m/m SA (зафиксировать до прогона).
3. Зафиксировать фолды, train/test, as-of; не использовать будущие ревизии.

### Шаг 2. Признаки `cbr_survey`

1. Агрегатные: `price_expectations`, `price_expectations_sa`, `price_growth_expected`.
2. Отраслевые: `industry_*_price_expectations`, `industry_*_price_expectations_sa` (16 листов).
3. Лаги 0/1/2/3 месяца; as-of по `series_publication`/`cbr_publication_calendar` (мониторинг ~10–15-е следующего месяца).
4. Для старых точек — `fallback` с `quality`; оговорка.

### Шаг 3. Walk-forward

1. Origin `O`, target `O+h`, train `≤ O−h−1`; h1/h3/h6.
2. Сравнить:
   - baseline;
   - +aggregate expectations;
   - +industry expectations;
   - +aggregate+industry.
3. Метрики: RMSE/MAE, DM-HLN, bootstrap CI, MDE.
4. Отдельно потребительский сектор: food, beverages, clothing / consumer goods.

### Шаг 4. Решение

- **go:** значимый выигрыш (DM p<0.05, CI исключает 0) хотя бы на h1/h3 в потребительском секторе → кандидат в фичи модели ставки/PPI-nowcast; отдельный follow-up.
- **no-go:** выигрыша нет → фиксируем; survey-ожидания не заменяем.
- **Оговорки:** 2022–2023 шоки, малая выборка, PIT/ревизии, записка предварительная.

### Шаг 5. Документ

`docs/research/2026-10-21-ppi-expectations-forecast.md` + JSON:
- данные/фолды/as-of;
- таблица RMSE/MAE/DM/CI по h1/h3/h6 и секторам;
- сравнение с baseline и с результатом записки;
- решение go/no-go;
- «что не проверено».

---

## Проверки

```bash
make docs-check
python3 scripts/ppi_industry_sensitivity_test.py -v   # Task 1, если добавлен
python3 scripts/models/ppi/ppi_monthly_forecast_test.py -v  # Task 2, если добавлен
make all  # только если менялись Go-файлы; в этих задачах не должны
```

- Прод-код, DDL, витрины и гранты не меняются; `ppi_sa` не пересчитывается.
- `make docs-check` — зелёный; Python-тесты — зелёные.
- Решения — в #266 (Task 1) и #318/#76 (Task 2).

---

## Review Focus

1. **Task 1:** точные коды/ряды Росстата, а не «похожие по названию»; веса/прокси явно названы; 11 exact + 14 qualitative — те же, что в T4/12-issue research; вывод не подгоняет веса.
2. **Task 2:** as-of без будущих ревизий; h1/h3/h6 предзарегистрированы; DM/CI/MDE; потребительский сектор не смешан с aggregate; baseline не переобучен.
3. **Границы:** записка — не официальная методика; паритет не заявляется; production не меняется.
4. **Трекер:** issue #316/#317/#318, Project V2, роадмап — согласованы.
