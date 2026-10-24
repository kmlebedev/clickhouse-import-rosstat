# Спека: `ofz-market/1.1` — аналитика выпусков ОФЗ

**Дата:** 2026-10-24
**Пункт:** `ofz-market/1.1` (issue #376)
**Роадмап:** [ROADMAP_OFZ_MARKET.md](../../ROADMAP_OFZ_MARKET.md)
**Статус:** реализовано; живая приёмка 2026-10-24 на MOEX ISS fallback (Transaq-путь ждёт `sec_info` от тестового аккаунта)

## 1. Цель и границы

Дневная выпуск-уровневая витрина `v_ofz_issue_analytics` по активным ОФЗ на TQCB/TQOB:
цена, YTM, дюрация, НКД, купон, ликвидность и спред к КБД ЦБ. Только данные и
расчётные метрики; никаких рекомендаций, позиций и стратегии.

В scope входит:

- новый пакет `ofz/` с импортёром `ofz_issue_analytics`;
- таблица `ofz_issue_analytics`, витрина `v_ofz_issue_analytics`;
- каталог рядов, комментарии, грант `kimi_reader`, DAG, MCP-check;
- расчёт YTM/дюрации для простых фиксированных купонных ОФЗ (ОФЗ-ПД).

Вне scope: ОФЗ-ПК, ОФЗ-АД, ОФЗ-ИН (переменный купон/амортизация/индексация), ВДО,
рекомендации, торговые сигналы. Для них витрина отдаёт явный `calculation_status` и
`NULL` в YTM/дюрации, а не выдуманное число.

## 2. Источники

| Источник | Что берём | Ограничение |
|---|---|---|
| `transaq_securities_info FINAL` | `sec_code`, `isin`, `sec_name`, `mat_date`, `coupon_date`, `coupon_value`, `coupon_period`, `facevalue`, `accruedint` | только следующий купон; нет полного графика амортизации |
| `transaq_securities FINAL` | `board` (`TQCB`/`TQOB`), `shortname`, `sectype`, `lotsize` | одна бумага может быть на двух board |
| `transaq_trades FINAL` | последняя цена, сделки за день, оборот | цена в % от номинала; `quantity` — количество бумаг |
| `cbr_zcyc FINAL` | КБД ЦБ на дату, 12 теноров | дневная публикация; точки может не быть |
| MOEX ISS `.../boards/{TQCB,TQOB}/securities.json` | цена последней сессии, `YIELDATPREVWAPRICE`, купон/НКД/погашение | fallback, когда `transaq_securities_info`/сделки пусты; статистики сделок нет |

Цена: последняя сделка на TQCB (приоритет) или TQOB. Если сделок нет — `NULL`, а не
ноль. `days_since_last_trade` показывает возраст последней сделки.

## 3. Расчёт

### 3.1. Календарь купонов

Для ОФЗ-ПД (фиксированный купон, погашение в конце):

- `coupon_date` — ближайший купон, `coupon_period` — период в днях, `coupon_value` —
  сумма купона на одну бумагу;
- даты будущих купонов: `coupon_date`, `coupon_date + period`, … до `mat_date`
  включительно; в `mat_date` добавляется погашение номинала;
- если `coupon_date` уже в прошлом или `period <= 0`, расчёт помечается
  `missing_data`/`unsupported`, YTM не считается.

Для ОФЗ-АД/ПК/ИН (определяется по коду выпуска: `SU46`/`SU29`/`SU52`, fallback —
`shortname`/`sec_name`: `ОФЗ-АД`, `ОФЗ-ПК`, `ОФЗ-ИН`) `calculation_status` =
`unsupported_amortizing`/`unsupported_variable`/`unsupported_indexed`, дюрация — `NULL`.
В MOEX fallback `ytm` остаётся справочной доходностью ISS (`YIELD`), поэтому для
`source='moex'` она заполнена и у `unsupported_*`; расчётной (ACT/365) она не является.

### 3.2. Цена и YTM

- `clean_price` — последняя сделка в % от номинала;
- `accrued_int` — НКД в рублях на бумагу;
- `dirty_price_pct = clean_price + accrued_int / facevalue * 100`;
- купоны в % от номинала: `coupon_value / facevalue * 100`;
- погашение: `100` (для ОФЗ-ПД);
- YTM `y` — решение `Σ CF_i / (1+y)^(t_i) = dirty_price_pct`, где `t_i` — дни от
  даты расчёта до платежа / 365 (ACT/365);
- Newton с ограниченным числом итераций и fallback на бисекцию; нет решения —
  `NULL` и `calculation_status='no_solution'`;
- `duration` — Macaulay: `Σ t_i·PV_i / dirty_price_pct`, годы;
- `modified_duration = duration / (1+y)`.

Конвенция ACT/365 фиксируется в комментариях и проверяется на 3–5 выпусках против
ручного расчёта MOEX/брокера. Если расхождение больше 1 б.п. — формула или данные
неверны, пункт не принимается.

### 3.3. Спред к кривой

Берётся последняя дата `cbr_zcyc` не позже даты расчёта. Доходности теноров
интерполируются линейно по сроку до погашения (годы). `spread_to_curve = ytm -
interpolated_yield`, п.п. Если точки кривой нет — `NULL`.

### 3.4. Ликвидность

За торговый день:

- `trade_count` — число сделок;
- `turnover` — `Σ price/100 * facevalue * quantity * lotsize`, руб.;
- `avg_trade_size = turnover / trade_count` (0 при отсутствии сделок);
- `days_since_last_trade` — дата расчёта минус дата последней сделки.

`quote_age_sec`/bid/ask — пункт `ofz-market/1.3`, сюда не входят.

## 4. Схема

Таблица `ofz_issue_analytics` (`ReplacingMergeTree(updated_at)`,
`ORDER BY (date, sec_code, board)`):

| Колонка | Тип | Смысл |
|---|---|---|
| `date` | Date | дата расчёта (МСК) |
| `sec_code` | LowCardinality(String) | код бумаги |
| `isin` | String | ISIN |
| `board` | LowCardinality(String) | TQCB/TQOB |
| `source` | LowCardinality(String) | `transaq` или `moex` (fallback) |
| `short_name` | String | короткое имя |
| `maturity_date` | Date | погашение |
| `days_to_maturity` | UInt32 | дней до погашения |
| `coupon_rate` | Nullable(Float64) | годовая ставка купона, % |
| `coupon_period` | Nullable(UInt16) | период купона, дни |
| `face_value` | Nullable(Float64) | номинал, руб. |
| `accrued_int` | Nullable(Float64) | НКД, руб. |
| `price` | Nullable(Float64) | последняя цена, % номинала |
| `ytm` | Nullable(Float64) | доходность к погашению, % годовых |
| `duration` | Nullable(Float64) | Macaulay, годы |
| `modified_duration` | Nullable(Float64) | модифицированная дюрация, годы |
| `turnover` | Nullable(Float64) | оборот за день, руб. |
| `trade_count` | Nullable(UInt32) | сделок за день |
| `avg_trade_size` | Nullable(Float64) | средний размер сделки, руб. |
| `days_since_last_trade` | Nullable(UInt32) | дней с последней сделки |
| `spread_to_curve` | Nullable(Float64) | YTM − КБД, п.п. |
| `curve_tenor` | LowCardinality(String) | ближайший тенор/интервал |
| `calculation_status` | LowCardinality(String) | `ok` / `unsupported_*` / `missing_data` / `no_solution` |
| `updated_at` | DateTime('Europe/Moscow') | время записи |

Витрина `v_ofz_issue_analytics` — `SELECT * FROM ofz_issue_analytics FINAL`.
Комментарии таблицы и каждой колонки обязательны.

## 5. Импортёр

`ofz/issue_analytics.go`:

1. DDL таблицы.
2. Запрос справочника и последних сделок/кривой; если `transaq_securities_info`/сделки пусты — fallback MOEX ISS (цена, `YIELDATPREVWAPRICE`, купон/НКД/погашение) с `source='moex'`.
3. Расчёт в Go (чистые функции в `ofz/calc.go`); приоритет — `source='transaq'` со статусом `ok`, fallback заполняет пропуски.
4. Один `PrepareBatch` + `Append` + `Send`.
5. `AfterImport`: `UpsertSeriesCatalog` + `CreateView`.
6. Ошибка любого источника — fail-fast с именем таблицы; пустой результат — 0 строк,
   не паника.

Импортёр не делает сетевых вызовов и не ходит в MOEX: пункт зависит только от
`transaq_*` и `cbr_zcyc`, как в роадмапе. Точный график амортизации/оферт — отдельный
пункт `ofz-market/1.4`, здесь его нет.

## 6. Каталог, грант, DAG, MCP

- `series_catalog`: по одной записи на метрику (`ytm`, `duration`, `modified_duration`,
  `price`, `accrued_int`, `spread_to_curve`, `turnover`, `trade_count`,
  `avg_trade_size`, `days_since_last_trade`, `days_to_maturity`, `coupon_rate`);
  `sec_code` — измерение, а не отдельный ряд.
- `sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_ofz_issue_analytics`.
- `dagu/ofz_analytics.yaml`: ежедневно после закрытия рынка, шаг `ofz_issue_analytics`.
- `scripts/mcp_check.py`: чтение витрины, число строк и последняя дата.

## 7. Тесты и приёмка

- Чистые тесты: календарь купонов, YTM на эталонном векторе, duration, интерполяция
  кривой, классификация ОФЗ-ПД/АД/ПК/ИН.
- Тест реестра: `Name()` = `ofz_issue_analytics`, таблица с префиксом `ofz_`.
- `make all`, `dagu validate dagu/ofz_analytics.yaml`.
- Живой прогон: `make import STAT=ofz_issue_analytics`, число строк, крайние даты,
  повторный прогон; `make mcp-check`.
- ✅ 2026-10-24 (dev-ClickHouse): `make import STAT=ofz_issue_analytics` — 61 строка,
  повтор — те же 61; YTM/спред — 46 из 61, дюрация — 33 (только fixed/ok); каталог — 12 метрик; блок OFZ в
  `make mcp-check` — OK. Тестовый Transaq-аккаунт не отдаёт `securities_info`, поэтому
  строки помечены `source='moex'` (MOEX ISS fallback: цена/YTM/купон/НКД/погашение).
  Transaq-путь остаётся основным; для его отдельной приёмки нужен `sec_info` от
  тестового аккаунта. `make docs-check` падает на уже существующем отсутствующем
  изображении `docs/publications/2026-10-22-weekly-sa-cpi/weekly-sa-mm-vs-cbr.svg`,
  к этому пункту отношения не имеет.

## 8. Риски

| Риск | Митигация |
|---|---|
| Нет полного графика амортизации/оферт | v1 только ОФЗ-ПД; остальные — явный `unsupported_*` |
| Устаревшая `securities_info` | `days_since_last_trade`, `calculation_status='missing_data'`, fail-fast по mat_date |
| Разные конвенции YTM | ACT/365 зафиксирована, live-сверка 3–5 выпусков |
| Тонкая ликвидность | `price=NULL` без сделки, `days_since_last_trade`, не подставлять ноль |
| Нет данных на dev | живая приёмка откладывается, пункт не ✅ |
