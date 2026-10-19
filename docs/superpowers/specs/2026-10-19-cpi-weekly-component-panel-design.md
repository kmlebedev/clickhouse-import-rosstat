# Недельная компонентная панель ИПЦ: дизайн (2026-10-19)

**Пункт:** `key-rate/phase-4-ml-components`, Task 3 (issue [#236](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/236), родитель [#76](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/76)).
**Роль:** ML-инженер (панель данных; Go-код, DDL, витрины и гранты не меняются).
**Зависимость:** Task 1 (#234, PR #244, merged) — аудит и mapping публичной панели.
**Статус:** дизайн до реализации.

---

## 1. Задача

Собрать месячную панель m/m по товарам и услугам из публичных недельных индексов Росстата, сопоставить её с публичными весами ИПЦ, реконструировать `food`/`nonfood`/`services` и общий ИПЦ и сравнить с официальными `rosstat_ipc_mes`. Ответить на вопрос: достаточно ли покрытия весов для полного bottom-up (≥50 %) или допустим только hybrid «4 официальных компонента + признаки».

## 2. Входы

| Вход | Где | Объём |
|---|---|---|
| Недельные индексы Росстата | `v_rosstat_macro`, `source='rosstat'`, `frequency='W'` | 149 рядов: 148 товарных + агрегат «Индекс потребительских цен (недельная оценка)» |
| Официальные месячные компоненты | `v_rosstat_macro` (те же 4 ряда `rosstat_ipc_mes`) | 4 × 429 точек, 1991-01…2026-09 |
| Веса | `cpi_weights_tovar.xlsx` из архива CB_COMP | листы 2016–2024 |
| Словарь/группы | `dictionary_cpi.xlsx` из архива CB_COMP | листы 2010–2024 |

Сырые таблицы только читаются. Внутренняя панель ЦБ (87 кодов ОКВЭД2, `model.xlsx`, BSU, `panel.csv`) не используется; `mp_survey_data.xlsx` — вход Task 2, здесь не нужен.

## 3. Месячный m/m товара

- Недельное значение — индекс «% к предыдущей дате регистрации» (100 = без изменения).
- Для месяца `M` берутся точки с `month_start(date) == M` и `available_from_weekly(date) = date + 2 ≤ cutoff(M)`, где `cutoff(M)` — 8-е число `M+1`.
- `monthly_index = Π(value/100)`, `mm_pp = (monthly_index − 1) × 100`.
- **Полнота** (иначе `mm_pp = NaN`):
  1. `n_weeks ≥ 3`;
  2. есть предыдущая точка `prev` (дата регистрации до первой точки месяца);
  3. первая точка месяца — не позже 10-го числа;
  4. последняя точка — не раньше чем за 10 дней до конца месяца;
  5. максимальный разрыв (включая `prev → first`) `≤ 21` дня.
- Порог 21 день допускает новогодний/праздничный сдвиг 15–17 дней, но отсекает месяцы с двумя и более пропущенными неделями; 10-дневные границы не дают месяцу потерять начало или конец. Порог зафиксирован по календарю, а не по метрикам реконструкции.
- Статусы: `ok`, `no_points`, `no_previous`, `lt_min_weeks`, `incomplete_month`, `gap_too_large`.
- Значения `≤ 0`, `NaN`/`Inf`, дубли дат — `ValueError`: данные испорчены, а не «пропуск».
- Конвенция месяца — по дате регистрации недели, а не по началу интервала. Это не точный календарный месяц: цепочка идёт от предыдущей регистрации до последней в месяце. Ошибка аппроксимации измеряется сверкой с официальным агрегатом.
- Первый месяц серии (`no_previous`) и месяц после многолетнего разрыва (`gap_too_large`) — NaN; следующий месяц уже валиден.

## 4. Веса и покрытие

- Основной режим — вес года месяца: 2016–2024 — соответствующий лист; 2013–2015 → 2016; 2025–2026 → 2024. Код веса — из mapping Task 1 (`weight_code`).
- Робастность — фиксированные веса 2024 для всех месяцев.
- `component_weight` — сумма всех листовых весов компонента за год (включая несопоставленные коды); `matched_weight` — сумма весов сопоставленных серий; `covered_weight` — сумма весов серий с валидным `mm_pp`; `covered_share = covered_weight / component_weight`.
- Mapping — `cpi_public_panel_audit.build_mapping()`: extended-нормализатор, 118 совпадений, 30 несопоставленных; нормализатор не дублируется.

## 5. Реконструкция

Для месяца `M` и компонента `c`:
- `recon_c = Σ w_i·mm_i / Σ w_i` по валидным сопоставленным товарам компонента; NaN-товары не импутируются, веса ренормируются на покрытые.
- `all_products` — та же формула по всем валидным сопоставленным товарам.
- `all_components = Σ W_c·recon_c / Σ W_c`, где `W_c = component_weight`; если хотя бы один компонент NaN — `all_components = NaN` (не подменять нулём).
- Официальные значения — те же месяцы `rosstat_ipc_mes`; `error_pp = recon − official`.
- Метрики: `n`, `RMSE`, `MAE`, `max_abs`, `bias`, `mean_covered_share`, `mean_products`.
- Периоды: `dense_2022` (2022-01…2026-09), `model_2013_2026_08` (2013-10…2026-08), `pre2022` (2013-10…2021-12), `all` (2009-04…2026-09), `by_year` (2009–2026).
- Сверка компаундирования: агрегатный W-ряд 2009-04…2021-12 против `cpi_all`; ожидается 146 месяцев, MAE 0,1647 п.п., RMSE 0,2581, max 1,7354 (числа Task 1). Расхождение фиксируется как delta, не fail-fast.
- Сверка с диагностикой Task 1 (`product_panel_2022_plus`: MAE 0,4089, RMSE 0,5347, max 1,2580) — качественная; отличия объясняются годовыми весами и правилом полноты.

## 6. Гейт

- `gate_all_weights = coverage_2024 ≥ 0.5`.
- `gate_all_components = все три компонента ≥ 0.5` (дополнительная страховка).
- `bottomup_full = gate_all_weights and gate_all_components`; иначе `mode = hybrid_4_components_plus_features`, `partial_bottomup = robustness_only`.
- 3-компонентная панель — частичная реконструкция, не полный 88-групповой bottom-up и не замена официальных компонент.

## 7. As-of / PIT

- `cutoff(M)` — 8-е `M+1`; недельная точка доступна `date + 2`; месячная метка `M` — не раньше конца `M+1`.
- В панель месяца `M` не попадает точка с `available_from_weekly > cutoff(M)`.
- Официальные значения используются только ex post и проверяются на `available_from_mes(month_end(M)) > cutoff(M)`.
- Винтажей/ревизий нет: это правило cutoff, а не фактические даты публикаций; ограничение называется явно.

## 8. Выходы

- `scripts/cpi_weekly_component_panel.py` — воспроизводимая панель, только чтение ClickHouse.
- `scripts/cpi_weekly_component_panel_test.py` — офлайн-тесты.
- `docs/research/2026-10-19-cpi-weekly-component-panel.md` — отчёт.
- `docs/research/2026-10-19-cpi-weekly-component-panel.json` — summary, mapping, coverage, reconstruction, decision, паспорт данных.
- `docs/research/2026-10-19-cpi-weekly-component-panel-products.csv` — 148 товаров × месяцы.
- `docs/research/2026-10-19-cpi-weekly-component-panel-components.csv` — 5 рядов (food/nonfood/services/all_products/all_components) × месяцы.
- `make cpi-weekly-panel`, `make cpi-weekly-panel-test`.

## 9. Паспорт

Модель не обучается, поэтому MLflow/`ML_LOG` не используются; паспорт данных — блок `passport` в JSON: `run_id` (uuid5 от config/sample hash), `config_hash`, `sample_hash`, `data_asof`, `max_official_month`, git SHA/branch/dirty, spec/plan sha256, Python/openpyxl, status, artifacts sha256, decision. В ClickHouse скрипт не пишет.

## 10. Границы и приёмка

- Нет `CREATE/INSERT/ALTER/GRANT`, Go-кода, DDL, витрин и записей в ClickHouse.
- Нет зависимости от 87 кодов ОКВЭД2 и внутренней панели ЦБ.
- Компаундирование пинуется тестом; дата/месяц и as-of (`date + 2`, cutoff 8-е M+1) проверены.
- Реконструкция компонент сравнена с `rosstat_ipc_mes`; покрытие весов и периоды записаны.
- `make all`, `make docs-check`, `make cpi-weekly-panel-test` — зелёные.
- Отчёт, JSON и CSV приложены; статус роадмапа и Project V2 обновлены тем же PR.
