# Трёхступенчатый конвейер CPI → SA/expectations → ставка: результат (2026-10-21)

> **Пункт:** `key-rate/phase-4-cpi-key-rate-pipeline`, issue [#309](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/309), `role:ml`.
> **Спека:** [2026-10-20-cpi-key-rate-pipeline-design.md](../superpowers/specs/2026-10-20-cpi-key-rate-pipeline-design.md); **план:** [2026-10-20-cpi-key-rate-pipeline.md](../superpowers/plans/2026-10-20-cpi-key-rate-pipeline.md).
> **Код:** `scripts/lib/publication_calendar.py`, `scripts/models/cpi/cpi_pipeline_stage1.py`, `cpi_pipeline_sa.py`, `cpi_pipeline_stage2.py`; `scripts/models/key_rate/key_rate_pipeline.py` **не строился** из-за Gate 2.
> **Границы:** только чтение ClickHouse; в `forecast_log` не пишется; Go/DDL/витрины/гранты не менялись.

## 1. Короткий ответ

**Конвейер остановлен на Gate 2; stage 3 не строился.**

- **Stage 1** (CPI-nowcast `M0`/`M0+1`) проходит Gate 1 только по допуску «не хуже live3 +5 %»: RMSE **0,697780** против **0,754466** у live3 h1, n=80; DM p=**0,3062**, 95 % CI ΔRMSE [−0,116289; +0,050807] — **не значимо**.
- **Stage 2** (SA-путь + cumulative h1/h3/h6) **не проходит Gate 2**: на общих 52 фолдах h1 **1,008515** против **0,920523** (ratio 1,0956; DM p=0,0391; CI [+0,021860; +0,291029]), h3 **2,201192** против 2,027397 (ratio 1,0857; p=0,362; CI включает 0), h6 **2,964448** против 2,801349 (ratio 1,0582; p=0,513; CI включает 0). По pre-registered правилу +5 % — **FAIL на всех трёх горизонтах**; значимо хуже live3 только h1.
- **Stage 3** не начат: по спеке/плану Gate 2 — stop-условие. Заявлять пайплайн или промоушен нельзя.
- **Stage 1** при этом — рабочая заготовка: он не использует expectations/SA/МП, не течёт по календарю и на 2024+ target-месяцах даёт y0 RMSE ≈0,243 (диагностика, не Gate). Stage 2 сломался не из-за утечки (она исправлена), а из-за качества: строгий фильтр публикаций целей и SA-таргет уменьшают обучающую выборку и ухудшают h1/h3/h6.

## 2. Что проверялось

Три ступени:

1. **Stage 1:** NSA m/m для двух месяцев — `M0` (следующий неопубликованный) и `M0+1`. Входы: месячные лаги, weekly CPI, USD, FAO, ключевая ставка; expectations/SA/МП запрещены.
2. **Stage 2:** SA m/m path `M0…M0+5` и cumulative SA h1/h3/h6. Входы: OOF stage 1, real-time SA, CBR SA (когда вышел), ожидания населения/бизнеса, МП; политический блок запрещён.
3. **Stage 3:** ставка по OOF stage 2 + политическому блоку. **Не реализован** из-за Gate 2.

As-of — по `published_at` и времени заседания 13:30 МСК; месячный ИПЦ 6–13, weekly — среда 19:00, CBR SA — 15–22, ожидания — 21-е. Nested OOF: stage 2 учится на OOF stage 1; stage 3 не понадобился.

## 3. Stage 1: Gate 1

| Модель / target | n | RMSE | MAE |
|---|---:|---:|---:|
| CatBoost y0 (`M0`) | 84 | 0,762462 | 0,316230 |
| CatBoost y1 (`M0+1`) | 83 | 0,927318 | 0,449224 |
| B3 ElasticNet y0 | 84 | 0,838674 | 0,351130 |
| B1 last value y0 | 84 | 1,240410 | 0,479405 |

Gate 1 (same-fold live3 h1): n=80, stage 1 **0,697780**, live3 **0,754466**, threshold ×1,05 = **0,792189**, ratio **0,9249** → **PASS по допуску**, но DM p=0,3062 и CI включает 0; это «не хуже», а не доказанный выигрыш.

## 4. Stage 2: Gate 2

| h | stage 2 RMSE | live3 RMSE | threshold ×1,05 | ratio | 95 % CI ΔRMSE | DM p | решение |
|---:|---:|---:|---:|---:|---|---:|---|
| 1 | 1,008515 | 0,920523 | 0,966549 | 1,0956 | [+0,021860; +0,291029] | 0,0391 | **FAIL** |
| 3 | 2,201192 | 2,027397 | 2,128767 | 1,0857 | [−0,207; +0,467] | 0,362 | **FAIL** |
| 6 | 2,964448 | 2,801349 | 2,941416 | 1,0582 | [−0,411; +0,660] | 0,513 | **FAIL** |

n=52 общих фолда; исключены 2022-04-08 и 2023-06-09 (origin+1 ≠ `M0`). Строгий фильтр публикации целей сократил выборку: **54** оценённых фолда, 27 `fold_train_below_min`, 273 train-строки исключены как неопубликованные к заседанию. Это главный источник деградации относительно live3, у которого 89 фолдов и другая as-of-конвенция.

Tail-smoke на 5 фолдах (не Gate 2): direct h1/h3/h6 — 0,3213 / 1,1650 / 1,7927; bottom-up — 0,7913 / 1,4361 / 1,6699; ensemble — 0,5170 / 1,2969 / 1,7195; ensemble `not_worse=false` на всех h. Это только диагностика малого хвоста.

## 5. Почему Gate 2 провален

1. **Строгий publication-фильтр.** Train-строка включается, только если её target path end опубликован к тестовому заседанию. Для h6 это отсекает много строк; фолдов остаётся 54 вместо 89/86/82.
2. **SA-таргет и real-time SA.** Stage 2 прогнозирует SA m/m, а live3 — NSA cumulative; сравнение идёт по NSA back-transform, но SA-таргет и ревизии сезонных факторов добавляют ошибку.
3. **Ожидания/МП без точных дат.** MP as-of использует консервативный конец `P+1`; exact MP dates в календаре отсутствуют. Это может задерживать признаки.
4. **Ensemble не лучше баз** на tail-smoke; direct/bottom-up расходятся, а h3/h6 тяжёлые.

## 6. Вердикт

**`iterate`; stage 3 заблокирован.** Гипотеза «календарный as-of + раздельные информационные множества» не подтверждена на Gate 2. Stage 1 — перспективный компонент, но выигрыш над live3 не значим. Промоушен, `forecast_log` и production-путь не заявляются. Возврат к stage 3 возможен только после нового pre-registered решения владельца: (а) другой фильтр публикаций/больше фолдов, (б) NSA-таргет вместо SA, (в) точные даты MP/ожиданий, (г) отдельная модель h1 vs h3/h6.

## 7. Что не проверено

- PIT/винтажи ИПЦ и SA-ревизии; календарь за пределами 2026 — fallback.
- Точные даты MP/бизнес-опросов; production `publication_calendar` table/view — отдельный Go/DDL-пункт.
- `ML_LOG=1` паспорт/sha256 и `make all` на финальном коммите — проверяются в PR.
- Stage 3 не строился; его CRPS/coverage неизвестны.
- Gate 1/2 — только на общих фолдах; MDE и multiple testing не расширялись.

## 8. Воспроизводимость

```bash
make publication-calendar-test
make cpi-pipeline-stage1-test
make cpi-pipeline-sa-test
make cpi-pipeline-stage2-test
MLFLOW=0 make cpi-pipeline-stage1 ARGS="--as-of 2026-10-20 --out-dir build/cpi-pipeline-stage1"
MLFLOW=0 make cpi-pipeline-stage2 ARGS="--as-of 2026-10-20 --out-dir build/cpi-pipeline-stage2 --gate2"
```

Артефакты (gitignored): `build/cpi-pipeline-stage1/`, `build/cpi-pipeline-stage2/`. Ветка — `feat/key-rate-cpi-pipeline`; issue #309.
