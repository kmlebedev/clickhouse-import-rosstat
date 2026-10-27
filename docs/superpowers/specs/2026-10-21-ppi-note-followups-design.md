# Follow-up аналитической записки ЦБ: отраслевая sensitivity ИЦП и ценовые ожидания

**Пункт:** `key-rate/phase-4-ppi-note-followups`, issue [#316](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/316); задачи — [#317](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/317) (research) и [#318](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/318) (ML).
**Дата:** 21 октября 2026 г.
**Роль:** `role:mixed` — research (Task 1) + ML (Task 2).
**Зависимости:** #266 (`ppi_sa`, mapping, anchors), #235 (CPI industry MP features), #76 (CPI ML components). T7 #282 не блокирует.

---

## 1. Контекст

Аналитическая записка ЦБ «О прогнозной силе показателя ценовых ожиданий на основе мониторинга предприятий» (ноябрь 2024, Иркутск; [PDF](https://www.cbr.ru/content/document/file/167898/analytic_note_20241107_irkutsk.pdf)) — **не методика ИЦП SA** и не источник mapping/весов. В ней нет:

- mapping «товар/ОКПД2 → потребительская группа»;
- весов агрегатов и базы;
- PPI-спецификации X-13 (выборка, выбросы, ARIMA, календарь);
- определения корзины рис. 44 и машиночитаемого ИЦП SA.

Но записка даёт два проверяемых follow-up:

1. **Отраслевая sensitivity.** Автор использует ИЦП Росстата по отраслям и трёхсекторную разбивку «инвестиционные / потребительские / промежуточные товары», а также отдельные потребительские отрасли (пищевые продукты, напитки, одежда). Это альтернативный путь агрегации к нашему **product-level** mapping `okpd2-kipc-2026-v2`. Гипотеза: отраслевой потребительский агрегат может быть ближе к текстовым якорям ЦБ, чем наш product-level агрегат.
2. **Ценовые ожидания как признак.** Главный результат записки — ценовые ожидания предприятий улучшают краткосрочный прогноз ИПЦ и ИЦП, особенно в потребительском секторе. У нас есть `cbr_survey` (13 агрегатных + 368 отраслевых рядов, включая `price_expectations` и SA-пары), но для **ИЦП** такого эксперимента не было; для ИПЦ отраслевые МП-признаки проверялись в #235.

**Оговорка.** Записка — личная позиция автора, предварительные результаты; дисклеймер в PDF. Используем её как гипотезу, а не как нормативный источник паритета.

---

## 2. Объём

### Task 1 — research: отраслевой потребительский ИЦП vs product-level mapping

**Вопрос:** даёт ли отраслевой агрегат «производство потребительских товаров» (и потребительские отрасли) меньшее расхождение с 11 точными и 14 качественными якорями ЦБ, чем текущий `ppi_sa`?

**В объёме:**

- аудит `rosstat_ppi_ved` (243 кода ОКВЭД2, 1998+) и `rosstat_ppi_weights` (38 кодов, 2017–2026): найти точные ряды «производство потребительских товаров», пищевые продукты, напитки, одежду и др.;
- SA-агрегат: X-13 `seats_log_cal`, m/m; прямой (SA агрегата) и, если есть веса, непрямой (SA компонент → агрегат);
- сверка с 11 точными якорями (`docs/research/2026-10-21-ppi-sa-text-anchors-12.csv`) и 14 качественными проверками;
- сравнение с текущим `ppi_sa`: exact MAE 1,538; qualitative 6 pass / 8 fail;
- решение: кандидат новой taxonomy/sensitivity (отдельный Go-follow-up) или подтверждение product-level mapping.

**Вне объёма:** изменение `ppi_sa`, mapping, DDL, витрин, грантов; PPI SA spec; T7.

### Task 2 — ML: ценовые ожидания как признаки прогноза ИПЦ/ИЦП

**Вопрос:** улучшают ли ценовые ожидания предприятий (`cbr_survey`) краткосрочный прогноз ИПЦ и ИЦП, особенно в потребительском секторе?

**В объёме:**

- baselines:
  - CPI — существующий nowcast #235/#76 (или его упрощённый аналог) как точка отсчёта;
  - PPI — AR/ARIMA или VAR на `ppi_sa` / `rosstat_ppi_ved` (total, food, non-food, consumer);
- признаки `cbr_survey`: `price_expectations` / `_sa`, `price_growth_expected`, `industry_*_price_expectations(_sa)` (16 листов); лаги 0/1/2/3 месяца; as-of по публикации (~10–15-е следующего месяца);
- walk-forward h1/h3/h6: RMSE/MAE, DM-HLN, bootstrap CI, MDE; отдельно потребительский сектор;
- сравнение с baselines и с результатом записки; решение: кандидат в фичи модели ставки/PPI-nowcast или фиксация отсутствия выигрыша.

**Вне объёма:** production-модели, `forecast_log`, DDL/витрины; замена survey-ожиданий в модели ставки без значимого выигрыша; переписывание #235/#76.

---

## 3. Данные

| Данные | Источник | Использование |
|---|---|---|
| ИЦП по ВЭД | `rosstat_ppi_ved` (243 кода, 1998+) | Task 1: отраслевой потребительский агрегат |
| Веса ИЦП по ВЭД | `rosstat_ppi_weights` (38 кодов, 2017–2026) | Task 1: непрямой агрегат, если есть веса |
| ИЦП по товарам/ОКПД2 | `rosstat_ppi_product`, mapping `okpd2-kipc-2026-v2` | Task 1: текущий product-level baseline |
| `ppi_sa` | T4, `spec_version='ppi-sa-2026-v1'` | Task 1: current baseline; Task 2: PPI target |
| Текстовые якоря | `docs/research/2026-10-21-ppi-sa-text-anchors-12.*` | Task 1: 11 exact + 14 qualitative |
| Мониторинг предприятий | `cbr_survey` (`macro_series`, source `cbr_survey`) | Task 2: признаки |
| Публикационные даты | `series_publication` / `cbr_publication_calendar` | Task 2: as-of |
| CPI | `rosstat_ipc_mes` / nowcast #235/#76 | Task 2: CPI target/baseline |

---

## 4. Task 1: метод

1. **Идентификация рядов.** В `rosstat_ppi_ved` найти код/имя «производство потребительских товаров» и потребительские отрасли. Зафиксировать: код, имя, период, пропуски, единицы. Если агрегата нет — собрать из отраслей по весам/равным весам и явно назвать это прокси.
2. **SA.** Для агрегата и компонент — `seats_log_cal` (R `seasonal` 1.11.x), m/m, ≥36 точек, непрерывность. Варианты:
   - **direct:** SA агрегата;
   - **indirect:** SA компонент → агрегат по весам (если веса есть).
3. **Сверка.** Target — последний месяц 3-месячного окна; 11 точных якорей (4 food, 4 non-food, 3 core) и 14 качественных проверок. Метрики: MAE, max, bias, corr; pass/fail для качественных.
4. **Сравнение.** Текущий `ppi_sa`: exact MAE 1,538; qualitative 6 pass / 8 fail. Решение:
   - **go:** отраслевой агрегат даёт MAE < 1,538 и/или больше pass без ухудшения core → кандидат новой taxonomy, отдельный Go-follow-up;
   - **no-go:** не лучше → подтверждаем product-level mapping, фиксируем в #266.
5. **PIT-оговорка.** Текст as-of, `ppi_sa` — винтаж 08-2026; ревизии PPI не измерены.

---

## 5. Task 2: предрегистрированный метод

Протокол фиксируется до просмотра живых метрик. Это research-only эксперимент: production CPI/PPI, `forecast_log`, model registry/promotion, DDL, витрины, гранты, DAG и спецификация `ppi_sa` не меняются.

1. **Таргеты и горизонты.**
   - CPI all: NSA m/m из `rosstat_ipc_mes.percent`, значение `percent - 100`, п.п.;
   - PPI `total`, `food`, `nonfood`, `core`: `ppi_sa.mm_sa`, п.п. м/м, только `spec_version='ppi-sa-2026-v1'`;
   - origin `O`, таргет — месячное значение `O+h`; h1/h3/h6 — три отдельные direct-модели, не `saar3` и не накопленный темп.
2. **Cutoff и as-of survey.** Для origin `O` cutoff — 8-е число месяца M+1 после `O`, 23:59:59 MSK. Признак доступен только при `published_at < cutoff` в `v_series_publication` для `source='cbr_survey'`: публикация ровно в cutoff запрещена. `quality='date'` означает точную дату публикации с effective 23:59:59 MSK; `quality='fallback'` — конец M+1. Отсутствие publication row означает пропуск/fail closed, а не фиксированный лаг.
3. **Признаки.** Агрегатные `price_expectations`, `price_expectations_sa`, `price_growth_expected` с календарными лагами 0/1/2/3 от последнего доступного к cutoff месяца. Отраслевые `industry_*_price_expectations(_sa)` используются только в secondary. Изменение ещё не опубликованных или будущих survey-точек не должно влиять на прогноз.
4. **Walk-forward.** Expanding window; для test origin `O` и горизонта `h` supervised train origins строго `t <= O-h-1` (включая один месяц embargo). Первый test fold имеет `MIN_TRAIN=36` origins. Base и expectations в каждой паре используют одинаковый calendar boundary и общие origins; парные метрики считаются только на пересечении `(target,horizon,origin)`.
5. **Модели и primary-контрасты.**
   - `b1_last`: последний доступный таргет, справочный benchmark, не входит в primary-семейство;
   - `b2_ar1`: OLS AR(1) с 11 monthly dummies;
   - `b3_enet`: ElasticNetCV на AR/month-dummy блоке, median imputation и scaling подгоняются только на train; полностью пустая train-колонка получает 0 только после явного правила, NaN не заменяется нулём заранее;
   - primary pairs: `b2_ar1_expectations` против `b2_ar1` и `b3_enet_expectations` против `b3_enet`; парные версии отличаются только aggregate survey-блоком;
   - primary targets: CPI all и PPI `total/food/nonfood/core`; horizons h1/h3/h6.
6. **Secondary.** Consumer industry target и industry survey-признаки восстанавливаются только из проверенного pinned research-винтажа Task 1. B3 `industry-only` и `aggregate+industry` сравниваются с тем же `b3_enet`; primary-семейство не расширяется. При отсутствии industry publication calendar secondary fail closed с явным limitation; aggregate primary продолжается независимо.
7. **Метрики.** RMSE/MAE для B1–B3; для каждой primary-пары `delta=RMSE(base)-RMSE(expectations)`, DM-HLN и 95% paired circular block bootstrap CI delta с `block=3`, `n=2000`, `seed=20261027`. Библиотечный DM использует HAC `max(h-1,ceil(T^(1/3)))`. MDE: `2.802*sd(d)/sqrt(n)/(RMSE_A+RMSE_B)`; при `n<2` или вырожденном differential фиктивная значимость не создаётся. Holm с α=0.05 применяется только к primary DM p-values внутри заранее зафиксированного primary-семейства; secondary и robustness отчитываются отдельно.
8. **Мощность и robustness.** Минимум 12 общих folds для primary и 6 для robustness; при меньшем n вердикт `INSUFFICIENT`/недостаточная мощность, не содержательный `promote`/`reject`. Окно 2022–2023 анализируется отдельно как robustness и не меняет primary-вывод; шоки не удаляются.
9. **Правило решения.**
   - `promote` — только research-рекомендация открыть follow-up, не production promotion: в primary consumer-контрасте PPI `food` хотя бы на h1 или h3 одновременно `delta>0`, Holm-adjusted DM p<0.05 и весь 95% CI delta строго выше 0. Secondary consumer сам по себе не переводит вердикт в `promote`, а служит проверкой механизма;
   - иначе `iterate` при положительном, но недоказанном эффекте/недостаточной мощности либо `reject` при отсутствии положительного эффекта по совокупности RMSE/MDE; при недостатке folds — `INSUFFICIENT`;
   - нулевой или отрицательный результат допустим. После просмотра метрик таргет, окна, признаки, primary-семейство и seed не меняются без нового явно post-hoc эксперимента.
10. **Винтажи.** Survey строго as-of; CPI/PPI targets — latest-vintage. Полный real-time/PIT по ревизиям таргета не проверяется и не заявляется. Официальный run логируется с `ML_LOG=1 MLFLOW=1`; паспорт содержит run id, config/sample hashes и SHA-256 артефактов, без registry/promote.

---

## 6. Критерий приёмки

- [ ] Task 1: research-документ с точными кодами/рядами, покрытием, SA-агрегатом, таблицей 11 exact + 14 qualitative, решением go/no-go.
- [ ] Task 2: воспроизводимый expanding walk-forward h1/h3/h6 с `MIN_TRAIN=36`, strict survey as-of и общими origins у пар; отчёт содержит RMSE/MAE всех B1–B3, primary DM/Holm/CI/MDE, secondary и robustness 2022–2023.
- [ ] Официальный run имеет паспорт с run id, config/sample hashes, SHA-256 артефактов и детерминированный replay; verdict соответствует предрегистрированному правилу и минимальному числу folds.
- [ ] Явно раскрыты latest-vintage CPI/PPI targets и отсутствие полного PIT ревизий; publication coverage и качество survey dates задокументированы.
- [ ] `make docs-check`, offline Python-тесты и `make all` зелёные; независимый recalc из predictions CSV совпадает с JSON.
- [ ] Прод-код, DDL, витрины и гранты не менялись; `ppi_sa` не пересчитывался; production promotion не выполнялся.
- [ ] Решение записано в #266 (Task 1) и #76/#318 (Task 2).

---

## 7. Риски и ограничения

| Риск | Что делаем |
|---|---|
| Записка — не официальная методика | Используем как гипотезу; не заявляем паритет |
| Нет точного кода «потребительских товаров» | Собираем прокси из отраслей, явно помечаем |
| Отраслевой агрегат включает алкоголь/табак | Сверяем с определениями Fig. 44; при необходимости исключаем и фиксируем |
| PIT/ревизии PPI | Текст as-of, `ppi_sa` — последний винтаж; оговорка |
| 2022–2023 шоки | Отдельная разбивка/robustness; не подгоняем |
| ML-выигрыш случайен | DM/CI/MDE; pre-registered horizon; без production-изменений |
| Пересечение с #235/#76 | Task 2 — отдельный эксперимент, не переписывает существующие модели |

---

## 8. Handoff

| Задача | Роль | Артефакт |
|---|---|---|
| Task 1 | research | `docs/research/2026-10-21-ppi-industry-sensitivity.md` + CSV/JSON; решение в #266 |
| Task 2 | ML | `docs/research/2026-10-21-ppi-expectations-forecast.md` + JSON; скрипт под `scripts/models/`; решение в #318/#76 |
| Если Task 1 go | Go | отдельный пункт: новая taxonomy/sensitivity в `ppi_sa` (не в этом item) |
| Если Task 2 go | ML | отдельный пункт: фича в модели ставки/PPI-nowcast (не в этом item) |
