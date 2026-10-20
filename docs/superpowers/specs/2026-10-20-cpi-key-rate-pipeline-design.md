# Дизайн: трёхступенчатый конвейер CPI → SA/expectations → ключевая ставка

**Пункт:** `key-rate/phase-4-cpi-key-rate-pipeline` (issue создать при старте; follow-up к #296/#272/#289)
**Роль:** `role:ml` для моделей; календарь/PIT-таблица — отдельная Go/DDL-часть
**Дата:** 2026-10-20
**Статус:** дизайн до реализации; спека и план — первые коммиты ветки
**Связанные документы:** [KEY_RATE_MODEL.md](../../KEY_RATE_MODEL.md), [KEY_RATE_CHANNELS.md](../../KEY_RATE_CHANNELS.md), [stat-ml-models.md](../../guides/stat-ml-models.md), [2026-10-20-cpi-v3-expectations.md](../../research/2026-10-20-cpi-v3-expectations.md)

---

## 1. Задача

Максимально использовать данные, доступные к дате заседания ЦБ, и построить прозрачный конвейер до конечной цели — **решения по ключевой ставке**:

1. **Stage 1 — CPI-nowcast:** по месячным лагам, недельному ИПЦ, недельным ценам, FX/FAO/ключевой ставке прогнозирует **NSA m/m** для двух месяцев: `M0` (следующий неопубликованный) и `M0+1`.
2. **Stage 2 — CPI-путь:** по OOF-выходу stage 1, real-time SA, официальному SA ЦБ (когда вышел), ожиданиям населения/бизнеса и МП строит **SA m/m path** на `M0…M0+5` и **cumulative SA** на h1/h3/h6.
3. **Stage 3 — ставка:** по OOF-пути stage 2 и политическому блоку (труд, кредит, бюджет, деньги, рынок, текст, сбережения, CCyB) прогнозирует решение по ставке на заседании `d`.

**Конечная цель:** stage 3 не хуже текущего `channel_fan_vol` по CRPS/coverage и RMSE, с DM/CI; иначе — честный `iterate`/`reject`.

**Почему не одна модель.** Текущий #296 показал, что CPI-прокси в `channel_fan_vol` даёт ΔCRPS −0,944, но 95 % CI включает 0. Одна модель с CPI/expectations/политическими каналами даёт double counting и слабую атрибуцию. Трёхступенчатый stack разделяет информационные множества и позволяет проверить вклад каждого блока.

---

## 2. Вне объёма

- Production-промоушен и `forecast_log` до gate #184.
- Замена primary Task 4 / `channel_fan_vol` без отдельного решения владельца.
- Новые Go-импортёры и DDL в ML-PR: календарь/PIT-таблица — отдельный Go/DDL-пункт.
- Per-sheet/88-групповой bottom-up и SA-вариант CB_COMP: только как robustness, если понадобится.
- Ретроспективное изменение результатов #296: текущий `iterate` остаётся результатом для консервативного as-of.

---

## 3. Календарь и as-of

### 3.1. Фактические публикации

| Ряд | Публикация | Комментарий |
|---|---|---|
| Месячный ИПЦ Росстата | 6–13 числа `M+1`, 19:00 МСК | пример: сентябрь 2026 — 9 октября |
| Недельный ИПЦ Росстата | среда, 19:00 МСК (редко четверг) | точка недели — понедельник; в БД доступна `date+2` |
| SA/тренд ЦБ | через 5–6 рабочих дней после месячного ИПЦ, обычно 15–22 | SA и тренд — в одном окне |
| Инфляционные ожидания населения (`cbr_infl_exp`) | ежемесячно около 20-го | октябрь 2026 — 21 октября |
| Ценовые ожидания/издержки бизнеса (`cbr_survey`) | по календарю мониторинга предприятий | не приравнивать к дате `cbr_infl_exp` |
| `cbr_mo_br` (консенсус) | 8 раз в год | отдельное расписание |
| Заседание ЦБ | дата/время; решение около 13:30 МСК | релиз в 19:00 в тот же день — уже после решения |

### 3.2. Правило as-of

Для каждого ряда хранится `published_at` (дата + время, если известно). На заседании `d` используется точка, только если:

```text
published_at(point) <= meeting_datetime(d)
```

Никаких фиксированных «8-е `M+1`» или «конец `M+1`» для признаков. Если историческая дата публикации неизвестна:

- fallback — консервативное правило, явно названное в отчёте;
- точка не считается доступной «по умолчанию»;
- в паспорте run фиксируется `calendar_version` и доля fallback-точек.

### 3.3. PIT-слой

- **Go/DDL (отдельный пункт):** таблица/витрина `publication_calendar` с `source`, `series`, `period`, `published_at`, `precision`, `source_url`; комментарии и `GRANT SELECT`.
- **ML-часть:** читает витрину; для эксперимента допускается версионированный CSV/YAML в `docs/` или `build/`, но production-путь — только таблица.
- Исторические даты: календарь Росстата/ЦБ, где доступен; иначе — консервативный fallback с записью в отчёт.
- Отдельно проверяется, что `published_at` не позже `meeting_datetime`; тест на look-ahead обязателен.

---

## 4. Часы и цели

### 4.1. Общий origin

`d` — дата/время заседания. По календарю:

- `latest_published(d)` — последний месячный ИПЦ, опубликованный до `d`;
- `M0 = latest_published(d) + 1 месяц` — следующий неопубликованный месяц;
- stage 1 цели: `M0` и `M0+1` (NSA m/m);
- stage 2 цели: SA m/m для `M0…M0+5`; cumulative SA:
  - h1 = `M0`;
  - h3 = `M0…M0+2`;
  - h6 = `M0…M0+5`;
- stage 3 цель: решение на `d` (шаг в б.п. и вероятности).

Пример 2026-10-23: сентябрь опубликован 9 октября → `M0 = октябрь`; stage 1 прогнозирует октябрь и ноябрь; stage 2 — SA-путь октябрь–март; h1 = октябрь, h3 = октябрь–декабрь, h6 = октябрь–март.

Пример 2026-09-11: август уже опубликован 9 сентября → `M0 = сентябрь`; stage 1 прогнозирует сентябрь и октябрь; stage 2 — сентябрь–февраль. Если бы заседание было 8 сентября (до релиза), `M0 = август`; это и есть граница календаря.

### 4.2. Целевые величины

- NSA m/m: `y_t = percent_t − 100` (п.п.).
- SA m/m: `s_t` — сезонно сглаженный m/m, п.п.
- Cumulative SA: `cum_h = 100 · (Π_{k=0..h−1}(1 + s_{M0+k}/100) − 1)`.
- Stage 2 отдаёт и месячный путь `s_t`, и `cum_h`; cumulative обязан быть согласован с путём (не отдельная несогласованная модель).
- NSA back-transform — только sanity: применить сезонные факторы к SA-пути и сравнить с фактическим Росстатом.

---

## 5. Информационные множества

| Stage | Входы | Не видит |
|---|---|---|
| 1 | месячные лаги NSA (3–6), weekly CPI, N недельных товарных/компонентных рядов, средние недельные цены (если есть), USD, FAO, ключевая ставка, month | expectations, SA, МП |
| 2 | stage 1 OOF (NSA `M0`, `M0+1`), real-time SA, CBR SA (когда вышел), ожидания населения, ценовые ожидания/издержки бизнеса, МП (IBK/price/cost), weekly diffusion, month | труд, кредит, бюджет, деньги, рынок, текст |
| 3 | stage 2 path + cumulative h1/h3/h6 + availability/uncertainty, труд, кредит, бюджет, деньги, KEYRATE/ОФЗ/RUONIA, текст, сбережения, CCyB | сырые CPI/expectations (кроме ablation) |

**Правила:**

- Stage 1 не использует expectations/SA/МП — они reserved for stage 2.
- Stage 2 не использует политический блок — он reserved for stage 3.
- FX/FAO в stage 1; stage 3 получает их эффект через stage 2, а прямой FX/Brent/BOP — отдельная ablation.
- Каждый признак имеет `available`/missing-индикатор; NaN ≠ 0.
- Для `cbr_infl_exp` и `cbr_survey` — разные `published_at`; нельзя применять один лаг.

---

## 6. Модели

### 6.1. Stage 1 — CPI nowcast

- **Targets:** NSA m/m для `M0` и `M0+1`.
- **Models:**
  - CatBoost direct на каждый месяц (дефолты, seed 20261018, `month` категориальным);
  - ElasticNetCV baseline на тех же признаках (медиана train внутри фолда);
  - опционально bottom-up по компонентам (food/nonfood/services) + NNLS.
- **Features:** месячные лаги, weekly CPI (4–8 недель), недельные товарные ряды (share rising, median, median4, gasoline4), средние недельные цены (если импортированы), USD (1m/3m), FAO, ключевая ставка, month.
- **As-of:** monthly CPI через `latest_published(d)`; weekly — через `published_at` (среда 19:00); FX/FAO/ставка — по своим датам.
- **Выход:** `cpi_nsa_m0`, `cpi_nsa_m1`, `cpi_nsa_available`.

### 6.2. Stage 2 — SA-путь и expectations

- **Targets:** SA m/m `s_t` для `M0…M0+5`; cumulative SA h1/h3/h6.
- **SA:**
  - real-time X-13ARIMA-SEATS (если доступен) или STL/MSTL на истории + stage 1 nowcast;
  - сезонные факторы оцениваются только на данных, доступных к `d`;
  - официальный CBR SA — **признак**, не таргет; при отсутствии — missing;
  - сверка модельного SA с CBR SA; расхождение публикуется.
- **Models:**
  - direct CatBoost на SA m/m по месяцам/горизонтам;
  - bottom-up по трём компонентам + NNLS (как `cb_comp_live_compare`/`cpi_monthly_v3`);
  - ensemble direct/bottom-up с фиксированными весами (0,5/0,5) — только если не хуже лучшей одиночной.
- **Features:** stage 1 OOF, SA stage 1, CBR SA (+available), `cbr_infl_exp` (уровень/изменение), `price_expectations`/`costs` (уровень/изменение), МП, weekly diffusion, month.
- **Выход:** `cpi_sa_path_m0…m5`, `cpi_sa_h1/h3/h6`, `cpi_sa_available`; NSA back-transform — diagnostics.

### 6.3. Stage 3 — ставка

- **Target:** решение на `d` — шаг в б.п. и вероятности (как в `key_rate_scenario_fan_v3`).
- **Features:**
  - stage 2: SA path, cumulative h1/h3/h6, availability, опционально uncertainty;
  - политический блок: труд (безработица, зарплаты), кредит (кредиты, ставки, домохозяйства), бюджет (баланс/импульс), деньги (M2), рынок (KEYRATE, ОФЗ, RUONIA), текст, сбережения, CCyB;
  - прямые FX/Brent/BOP — только ablation.
- **Models:** ElasticNet/ordered logit + channel fan; baseline — текущий `channel_fan_vol`; challenger — `channel_fan_vol` + stage 2.
- **Ablations:**
  1. baseline без stage 2 (сырые CPI/expectations);
  2. stage 2 + политический блок (primary);
  3. stage 2 без expectations;
  4. stage 2 без weekly;
  5. + прямые FX/Brent/BOP в stage 3.
- **Выход:** `key_rate_step_bp`, fan/CRPS/coverage; в `forecast_log` не пишется.

---

## 7. Stacking и защита от утечек

**Nested OOF обязателен.** Для каждого фолда заседания:

1. Train stage 1 на прошлых фолдах; получить OOF-прогнозы stage 1 для train-части stage 2.
2. Train stage 2 на OOF stage 1; получить OOF-прогнозы stage 2 для train-части stage 3.
3. Train stage 3 на OOF stage 2; предсказать текущее заседание.

Запрещено:

- обучать stage 2 на in-sample предсказаниях stage 1;
- обучать stage 3 на in-sample stage 2;
- использовать финальные сезонные факторы/ревизии;
- использовать точку, опубликованную после `d`;
- выбирать окно/модель по тесту.

Проверки:

- `assert_no_lookahead` для каждой стадии;
- тест «OOF-признаки не содержат будущих дат»;
- тест «stage 3 не видит сырые CPI/expectations»;
- тест «при отсутствии stage 2 — missing, не 0»;
- детерминизм при одном seed.

---

## 8. Валидация и приёмка

### 8.1. Общие правила

- Walk-forward по датам заседаний; expanding/rolling; purge/embargo; без random split.
- Общие фолды для сравниваемых моделей.
- Полный набор метрик: RMSE/MAE, DM-HLN, блочный бутстрап (блок 3, n=2000), MDE; для веера — CRPS/coverage 50/80/90, ширина, Brier/log loss.
- Эпизоды 2022–2023 и 2024–2026.
- `ML_LOG=1`, MLflow по умолчанию; паспорт run с `config_hash`/`sample_hash`, sha256, per-fold predictions.
- `quick`-прогоны не сравнивать с полными.

### 8.2. Stage 1

- RMSE/MAE для `M0` и `M0+1`; сравнение с B1/B3 и текущим `cpi_monthly_v3`.
- DM/CI; MDE; покрытие missing.
- Приёмка: stage 1 не хуже текущего v3/live3 на общих фолдах; иначе — `iterate`.

### 8.3. Stage 2

- RMSE/MAE для SA path/cumulative; SA path сравнивается с CBR SA, а не с live3 напрямую.
- DM/CI; SA-сверка с CBR; NSA back-transform — обязательная сверка.
- Приёмка: stage 2 **NSA back-transform cumulative h1/h3/h6** не хуже live3 на общих фолдах; SA path отдельно сверяется с CBR SA. Если NSA-сверка хуже — остановиться и не строить stage 3.

### 8.4. Stage 3

- Primary: CRPS `channel_fan_vol` expanding, ΔCRPS challenger − baseline.
- Приёмка: ΔCRPS < 0 и 95 % CI верх ≤ 0; coverage 80 % в `[0,70; 0,90]` и Wilson CI включает 0,8; RMSE не хуже baseline ×1,05; ablations опубликованы.
- Вердикт: `promote` / `iterate` / `reject`; без значимого выигрыша — не промоутить.

### 8.5. Multiple testing

- Primary — один (stage 3 CRPS); stage 1/2 — secondary.
- При >5 спецификациях — MCS/SPA; primary-контрасты — Holm.
- Никакого post-hoc выбора лучшей модели по тесту.

---

## 9. Фазы и артефакты

### Phase 0 — календарь/PIT и аудит данных

- Go/DDL: `publication_calendar` (или CSV до таблицы); `published_at` для Росстата/ЦБ/ожиданий.
- ML: аудит доступности, тесты as-of, отчёт о fallback-точках.
- Артефакт: `docs/research/2026-10-20-cpi-key-rate-calendar-audit.md` (создаётся в Phase 0).
- Критерий: все нужные ряды имеют `published_at` или явный fallback; look-ahead-тесты зелёные.

### Phase 1 — stage 1

- `scripts/cpi_pipeline_stage1.py`, `_test.py`; `make cpi-pipeline-stage1`.
- Отчёт: RMSE/MAE M0/M0+1, DM/CI, сравнение с v3/live3.
- Критерий: не хуже текущего CPI-nowcast на общих фолдах.

### Phase 2 — stage 2

- `scripts/cpi_pipeline_stage2.py`, `_test.py`; `make cpi-pipeline-stage2`.
- Отчёт: SA path/cumulative, SA-сверка, NSA sanity, DM/CI.
- Критерий: не хуже live3; если хуже — stop.

### Phase 3 — stage 3

- `scripts/key_rate_pipeline.py`, `_test.py`; `make key-rate-pipeline`.
- Отчёт: CRPS/coverage/RMSE, ablations, DM/CI.
- Критерий: stage 3 лучше `channel_fan_vol` по pre-registration или честный negative.

### Phase 4 — решение

- Research-отчёт, docs, roadmap status, PR.
- `forecast_log` — только после gate #184; production-промоушен — отдельным решением.

---

## 10. Риски

- **Stacking leakage** — главный; nested OOF и тесты обязательны.
- **SA-ревизии/endpoint** — real-time X-13/STL, без финальных факторов; расхождение с CBR SA публикуется.
- **Календарь** — исторические `published_at` могут отсутствовать; fallback явно назван, доля fallback в паспорте.
- **Малая выборка** — 108 заседаний; stage 2 89/86/82 фолда; MDE может быть >10 %.
- **Накопление ошибки** — stage 1→2→3; если stage 2 хуже live3, stage 3 не строим.
- **Double counting** — FX/бюджет в stage 1/2 и stage 3; прямые каналы в stage 3 — ablation.
- **Weekly prices** — рублёвые средние недельные цены могут быть не импортированы; stage 1 использует индексы, рубли — после импортёра.
- **Роли** — календарь/PIT-таблица и гранты — Go/DDL; ML не делает DDL.
- **Multiple testing** — primary один; secondary — Holm/MCS.

---

## 11. Открытые проверки Phase 0

- Точная спецификация SA ЦБ (X-13 vs STL; direct/indirect; outlier/trading-day настройки).
- Наличие исторических дат публикаций Росстата/ЦБ; где нет — fallback.
- Доступность рублёвых недельных цен (Fedstat/EMISS) и их маппинг на компоненты.
- Точная дата/время `cbr_infl_exp` и `cbr_survey`; различие расписаний.
- Достаточность 24+ train-origin для stage 2 на ранних заседаниях; политика missing.

---

## 12. Определение готовности дизайна

- [ ] Согласован трёхступенчатый stack и информационные множества.
- [ ] Зафиксированы target/clock: `M0`, два месяца stage 1, SA path h1/h3/h6.
- [ ] Зафиксирован as-of по `published_at`, а не по фиксированному 8-му/концу M+1.
- [ ] Зафиксированы nested OOF, метрики и критерий stage 3.
- [ ] Разделены ML- и Go/DDL-части.
- [ ] Phase 0/1 можно запускать без stage 2/3.
