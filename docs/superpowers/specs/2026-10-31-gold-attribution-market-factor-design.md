# phase-4-followup-2a: рыночный фактор распродаж в `v_gold_attribution` — спека

Пункт `gold-thesis/phase-4-followup-2a` (issue #472). Часть split #144: (a) рыночный фактор — этот пункт, (b) новостной фактор — #144. Роль `go`. Связанные документы: [прототип и разбор моделей](../research/2026-10-31-gold-selloff-channel-models.md), [карта live-модели CB_COMP](../guides/cb-comp-live-model-map.md) §1.4 (почему `bs_econ16`/`bs_rozn16` не подходят), [GOLD_THESIS_DATA_ROADMAP.md](../GOLD_THESIS_DATA_ROADMAP.md) follow-up 2.

## 1. Зачем

В обновлении прогноза 31.10.2026 канал вынужденных продаж / тех-селла описан качественно: «Nasdaq −2,2%, Intel −30%, маржин-коллы, золото продали как ликвидный актив». Прототип показал, что линейные факторы «Nasdaq/VIX/кредит» не объясняют событие 29.10 (факт −1,26%, модель +0,26%, остаток −1,52 п.п.), а значимый дополнительный фактор — ETF-поток (недельная R² 0,118 → 0,234, t = 3,82).

Нужна витрина, которая:

1. показывает вклад каждого рыночного фактора в дневную доходность золота;
2. отделяет потоковый слой (ETF) от ставки/доллара;
3. маркирует стрессовые дни (тех-селл/кредит) и оставляет видимый остаток — кандидат в «ликвидностный» компонент;
4. даёт следующиему обновлению прогноза числа, а не нарратив.

Это **диагностическая** витрина, не production-вход прогноза: в `forecast_log` не пишет, вероятности сценариев не меняет.

## 2. Объём

Входит:

- три ряда FRED: `NASDAQCOM`, `VIXCLS`, `BAMLH0A0HYM2` (+ каталог, тест полноты, комментарий `v_fred_macro`);
- витрина `v_gold_attribution` (дневная, одна строка на дату): факторы, rolling-беты, вклады, остаток, стресс-флаг;
- панель `gold-thesis` «Рыночный фактор распродаж»: вклады и остаток за последние 90 дней;
- research-отчёт с живыми числами и ограничениями.

Не входит:

- новостной фактор (2b, #144) и исторический новостной слой (follow-up 5);
- многомерная rolling-регрессия/ridge (фаза 2; в этой фазе — одномерные rolling-беты, см. §4);
- CFTC COT/позиционирование CTA, intraday-данные, опционные skew;
- запись в `forecast_log` и изменение сценариев.

## 3. Данные

| Ряд | Источник | Частота | Глубина | Роль |
|---|---|---|---|---|
| `NASDAQCOM` | FRED, `fredgraph.csv?id=NASDAQCOM` | D | 1971+ | тех-селл |
| `VIXCLS` | FRED, `fredgraph.csv?id=VIXCLS` | D | 1990+ | волатильность/стресс |
| `BAMLH0A0HYM2` | FRED, `fredgraph.csv?id=BAMLH0A0HYM2` | D | 2023-10+ | кредитный стресс |
| `DFII10`, `DTWEXBGS` | FRED, уже есть | D | 2003+ / 2006+ | альтернативная стоимость |
| золото | `v_gold_prices`: `comex_front` → `bold` (приоритет) | D | 2014+ (`bold`) | цель |
| ETF-поток | `v_gold_etf_flows_daily.flow_tonnes` | D (торговые дни) | 2015+ | предельный покупатель |

FRED отстаёт на 1 торговый день; последняя точка витрины — по минимальной доступной дате факторов. `BAMLH0A0HYM2` ограничивает общую выборку 2023-11+; для более длинной истории кредитный фактор будет NULL, витрина это показывает, а не заполняет нулём.

## 4. Модель

### 4.1. Потоковый слой

Дневная лог-доходность золота:

`r_gold,t = α + β1·ΔDFII10_t + β2·Δlog(DTWEXBGS)_t + β3·flow_tonnes_t + ε_t`

В фазе 1 — **одномерные rolling-беты** для каждого фактора:

`β_f,t = covarPop(r_gold, Δf) / varPop(Δf)` по окну 180 наблюдений, `ROWS BETWEEN 180 PRECEDING AND 1 PRECEDING` (текущая точка не входит — без утечки).

Вклады: `contrib_f,t = β_f,t · Δf_t`. Остаток: `residual_t = r_gold,t − Σ contrib_f,t`.

Одномерные беты — сознательное упрощение: факторы коррелированы (Nasdaq–VIX–HY), вклады не ортогональны, остаток не равен «чистому» ликвидностному фактору. Это диагностика, а не структурная декомпозиция; многомерная модель — фаза 2.

### 4.2. Стрессовый слой

Предрегистрированные пороги (не подбираются на данных):

- `stress_equity = 1`, если `r_nasdaq < −1,5%` или `ΔVIX > +1,5` пункта;
- `stress_credit = 1`, если `ΔHY_OAS > +0,10 п.п.` (10 б.п.);
- `stress_flag = max(stress_equity, stress_credit)`;
- `stress_type` = `equity` / `credit` / `both` / `none`.

Стрессовый слой не добавляет коэффициент в сумму вкладов; он маркирует дни, в которых остаток трактуется как кандидат в ликвидностный компонент. Сравнение остатка в стрессовые и спокойные дни — описательное, не гейт.

### 4.3. Почему не `bs_econ16`/`bs_rozn16`

Это OLS-компоненты nowcast **ИПЦ РФ** на SA ценовых ожиданиях ЦБ; не ИЦП, не cross-asset, месячная частота. В канале «Nasdaq → маржин-коллы → продажа золота» у них нет ни одного релевантного регрессора. Разбор — [research note](../research/2026-10-31-gold-selloff-channel-models.md) §1.

## 5. Витрина `v_gold_attribution`

Одна строка на дату; источник — `v_fred_macro`, `v_gold_prices`, `v_gold_etf_flows_daily`. Создаётся импортёром `gold_views` после `v_gold_etf_flows_daily` (порядок в `goldViewsList`).

| Колонка | Тип | Смысл |
|---|---|---|
| `date` | Date | торговый день |
| `gold_ret` | Float64 | `ln(gold_t / gold_{t−1})` |
| `d_dfii10` | Float64 | изменение реальной 10y, п.п. |
| `d_log_dxy` | Float64 | `ln(DTWEXBGS_t / DTWEXBGS_{t−1})` |
| `r_nasdaq` | Float64 | `ln(NASDAQCOM_t / NASDAQCOM_{t−1})` |
| `d_vix` | Float64 | изменение VIX, пункты |
| `d_hy` | Float64 | изменение HY OAS, п.п. |
| `flow_t` | Nullable(Float64) | ETF-поток, т (NULL — нет точки) |
| `beta_dfii10` | Nullable(Float64) | rolling 180, одномерная |
| `beta_dxy` | Nullable(Float64) | то же |
| `beta_nasdaq` | Nullable(Float64) | то же |
| `beta_vix` | Nullable(Float64) | то же |
| `beta_hy` | Nullable(Float64) | то же |
| `beta_flow` | Nullable(Float64) | то же |
| `contrib_dfii10` | Nullable(Float64) | `beta × Δ` |
| `contrib_dxy` | Nullable(Float64) | то же |
| `contrib_nasdaq` | Nullable(Float64) | то же |
| `contrib_vix` | Nullable(Float64) | то же |
| `contrib_hy` | Nullable(Float64) | то же |
| `contrib_flow` | Nullable(Float64) | то же |
| `residual` | Nullable(Float64) | `gold_ret − Σ contrib` |
| `stress_flag` | UInt8 | 1 — стрессовый день |
| `stress_type` | LowCardinality(String) | `equity`/`credit`/`both`/`none` |

Комментарии таблицы и каждой колонки — по правилу 11 AGENTS.md. Грант `kimi_reader` — в `sql/mcp_kimi_reader.sql`; проверка — `scripts/mcp_check.py` (витрина читается, есть строки, `residual` не NULL на последней дате).

### 5.1. Реализация rolling-бет

ClickHouse 26.x поддерживает агрегатные оконные функции (`covarPop`, `varPop`). Если конкретная сборка не поддержит — fallback: self-join по диапазону дат (`b.date BETWEEN a.date − 180 AND a.date − 1`) с `covarPop`/`varPop` в подзапросе. В спеке фиксируется основной путь; fallback — в плане, если живой прогон упадёт.

## 6. Приёмка

1. `make import STAT=fred` — три новых ряда в `v_fred_macro`, каталог `v_series_catalog` полон, тест `fred` зелёный.
2. `make import STAT=gold_views` — `v_gold_attribution` создана; `kimi_reader` читает её через MCP.
3. Живые числа:
   - на `2026-10-29` `residual < −1 п.п.` (прототип: −1,52 п.п.);
   - `stress_flag = 1` на `2026-09-01` и `2026-02-04` (тех-селл/VIX);
   - `stress_flag = 0` на `2026-01-30` (gold-specific, без стресса в акциях).
4. Таблица вкладов за 27–30.10 в research-отчёте; числа витрины сверены с прототипом (допуск — разные окна/частота, объяснить расхождение).
5. Панель `gold-thesis` «Рыночный фактор распродаж» рендерится; `make grafana-build`, `make grafana-validate`.
6. `make all`, `make docs-check`, `make mcp-check` — зелёные.
7. `ARCHITECTURE.md` §6.2/§6.3, `README.md`, `docs/GOLD_CONTOUR.md` — обновлены тем же PR; `GOLD_THESIS_DATA_ROADMAP.md` — статус.

## 7. Что не доказывается

- Одномерные беты ≠ многомерная регрессия; вклады коррелированы, остаток не ортогонален.
- Дневная частота шумная; недельная версия — фаза 2.
- `bold` — прокси, не COMEX; `comex_front` короче (2024-10+).
- FRED отстаёт на 1 день; `HY OAS` только с 2023-10.
- Нет позиционирования CTA/COT, intraday и опционного skew — канал «маржин-коллы» виден только через остаток.
- Стресс-пороги предрегистрированы, но не калиброваны на holdout; это описание, не гейт.

## 8. Риски

- Оконные `covarPop`/`varPop` могут быть недоступны в сборке ClickHouse — fallback self-join.
- Витрина на 3 000+ строк с шестью rolling-бетами может быть тяжёлой; ограничить окно 5 годами или материализовать, если MCP-запрос > 2 с.
- Пропуски `flow_tonnes` (нет торгового дня) — NULL, не 0; это меняет выборку и остаток, фиксируется в комментарии.
- `v_gold_etf_flows_daily` — 73 фонда ≈83% WGC; не сравнивать с мировым WGC напрямую.
