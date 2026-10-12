# clickhouse-import-rosstat

Go-конвейер импорта российской макроэкономической и финансовой статистики в ClickHouse. Данные используются дашбордами Grafana и аналитическими моделями (сценарный прогноз золота, DCF и NAV акций ПАО «Полюс»). Один бинарник, конфигурация только через переменные окружения.

Подробная архитектура, схемы таблиц и конвенции — в [ARCHITECTURE.md](ARCHITECTURE.md). План развития — в [docs/ROADMAP_DCF_POLYUS.md](docs/ROADMAP_DCF_POLYUS.md). Правила для AI-ассистентов — в [AGENTS.md](AGENTS.md). Контур прогноза золота (витрины, календарь, ingest-endpoint, плагин `gold-nav`) — в разделе «Контур прогноза золота» и в [спецификации](docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md).

## Что импортируется

Каждый источник — импортёр с `init()`-регистрацией. Расписание задаёт Dagu (файлы в `dagu/`).

| Домен (пакет) | Источник | Таблицы ClickHouse | Расписание (МСК) |
|---|---|---|---|
| `cbr` | Банк России: ключевая ставка, курс USD, M2, золото, кредиты, ОФЗ-индикаторы, инфляционные ожидания | `cbr_key_rate`, `cbr_currency_usd`, `cbr_m2`, `cbr_gold`, `cbr_bank_int_rate`, `cbr_loans_to_corporations`, `cbr_loans_to_individuals`, `cbr_infl_exp`, `cbr_credit_m2x`, `cbr_indicators_cpd`, `cbr_ruania`, `households_b_mes`, `cbr_queries_*`; каталог `series_catalog`, витрина `v_cbr_macro` | ежедневно 09:17 |
| `rosstat` | Росстат: ИПЦ (месяц, неделя), зарплаты, ВВП по кварталам | `ipc_mes`, `ipc_weeks`, `salaries_mes`, `vvp_kvartal`; каталог `series_catalog`, витрина `v_rosstat_macro` | четверг 12:17 |
| `minfin` | Минфин: исполнение федерального бюджета | `minfin_fed_bud_mes`, `minfin_fed_bud_mesyats`; каталог `series_catalog`, витрина `v_minfin_budget` | 10-е число 10:41 |
| `customs` | ФТС: внешняя торговля по странам | `customs_vneshn_torg` | 15-е число 11:41 |
| `fao` | ФАО: индексы продовольственных цен | `fao_food_price` | 5-е число 10:47 |
| `bank` | Банки: индексы потребительских расходов, финрезультаты (Сбер, ВТБ, Т-Банк), ипотека ДОМ.РФ | `sber_consumper_spending_index`, `sber_izmenenie_trat`, `sber_finansovie_rezultaty`, `rus_vtb_group_ifrs`, `tbank_group_ifrs`, `domrf_mortgage` | понедельник 09:23 |
| `craw` | Росстандарт: реестр СБКТС (свидетельства о безопасности конструкции ТС) — прокси импорта авто; транспорт — REST API портала `gost.ru` (заголовок `gtn.access.appid` из `GOST_APP_ID`), плоские дневные срезы по дате публикации | `gost_vehicle_safety_certificate` (10 колонок); витрины `v_gost_vehicle_safety_certificate` (строки) и `v_gost_vehicle_safety_daily` (день × марка, `certificates = uniqExact(certificate_number)`) | `dagu/craw.yaml`, раз в час (`:17` МСК), режим `since` (хвост 3 дня); вручную — `GOST_MODE=full` (история с 2016) и `GOST_MODE=check` (сверка полноты) |
| `fred` | FRED (CSV): DFII10, DGS10, FEDFUNDS, DTWEXBGS, CPIAUCSL, T5YIE | `macro_series` (`source = 'fred'`), каталог `series_catalog`, витрина `v_fred_macro` | ежедневно 11:41 |
| `bls` | BLS API v2: CPI (CUUR0000SA0, CUSR0000SA0), безработица, NFP, средняя зарплата, PPI, JOLTS | `macro_series` (`source = 'bls'`), каталог `series_catalog`, витрина `v_bls_macro` | ежедневно 12:23 |
| `bea` | BEA API: индексы PCE (headline, excluding food and energy), таблица NIPA T20804 | `macro_series` (`source = 'bea'`) | ежедневно 12:37 |
| `eia` | EIA API v2 (Weekly Petroleum Status): запасы дистиллятного топлива в США (WDISTUS1) и загрузка мощностей НПЗ (WPULEUS3) | `macro_series` (`source = 'eia'`), каталог `series_catalog`, витрина `v_eia_petroleum` | среда 19:43 |
| `wgc` | WGC (World Gold Council): месячные потоки и запасы золотых ETF (JSON API `fscdn.gold.org`; фолбэк — `ETF_Flows_*.xlsx`) и золотые резервы семи ЦБ (POL/CZE/TUR/RUS/KAZ/UZB/CHN; `Changes_latest_as_of_*.xlsx` + `World_official_gold_holdings_as_of_*.xlsx`) | `macro_series` (`source = 'wgc'`: 20 рядов `etf_<метрика>_<регион>` + 14 рядов `cb_gold_<метрика>_<iso3>`), каталог `series_catalog` (34 записи), витрины `v_gold_etf_flows` и `v_cb_gold_dynamics` | `dagu/wgc.yaml`, ежедневно 20:17; ETF — из API (автоматически), выгрузки ЦБ (и фолбэк ETF) кладутся в `wgc/data/` вручную |
| `bold` | bold.report (ByteTree), публичный JSON API без ключа: T+1 агрегат 73 золотых ETF (≈83 % мировой величины WGC) — дневной поток и его USD-оценка, окна 7/30/90 календарных дней, запасы и AUM, пер-фондовый срез; плюс цена дня источника («Futures Price», метод не раскрыт — не спот и не `GC=F`) | `macro_series` (`source = 'bold'`: 7 рядов `etf_bold_*`), `bold_fund_snapshot` (73 фонда, ключ `(date, ticker, name)`), `gold_prices` (`venue = 'bold'`); каталог `series_catalog` (7 записей), витрины `v_gold_etf_flows_daily` и `v_gold_etf_funds_daily` | `dagu/bold.yaml`, ежедневно 08:07 (шаг 1 — цена дня, шаг 2 — ETF-контур); вручную — `make import STAT=bold_gold_price`, `make import STAT=bold_etf_flows` |
| `news` | Новости РФ-канала: RSS-ленты Интерфакс, РБК, ЦБ РФ (RssNews + eventrss), TASS — события-решения (дизель/нефть, санкции, ставка, золото/резервы) | `news_events` (сырая, агенту не выдана); витрина `v_news_events` (только события по стемам тезисов) | `dagu/news.yaml`, каждые 30 минут (:07/:37 МСК), режим `since` — только отсутствующие позиции по ключу `(date, source, title)`; вручную — `make import STAT=news_watch` (`all` — полный upsert окна, `check` — диагностика покрытия) |
| `gold` | Золото: фиксинг MOEX GOLDFIXME (₽/г) пересчитан в USD/oz по курсу ЦБ | `gold_prices` (`venue = 'moex_fix_usd'`); каталог `series_catalog`, витрина `v_gold_prices` | ежедневно 18:47 |
| `moex` | МосБиржа ISS: свечи PLZL (OHLCV), индекс RGBI, доходности G-curve ОФЗ (1y/3y/5y/10y) | `stock_prices` (`code = 'PLZL'`), `ofz_curve`; каталог `series_catalog`, витрины `v_stock_prices`, `v_ofz_curve` | ежедневно 19:13 |
| `calendar` | Календарь событий-триггеров прогноза золота/NAV (Q4-2026; сид — `sql/events_calendar_q4_2026.sql`) | `events_calendar`; витрина `v_events_calendar` | вручную (`make import STAT=events_calendar`) |
| `forecast` | Резолвер журнала прогнозов: сверяет прогнозы `xau_q_avg` из `forecast_log` со средним `gold_prices` за квартал `target_date` (приоритет `comex_front > moex_fix_usd`), пишет `actual`/`error_pct`; `nav` не резолвится — факта нет | `forecast_log` (`actual`/`error_pct`; таблицу не создаёт — её создаёт ingest); витрина `v_forecast_accuracy` | ежедневно 20:23 |
| `views` | Витрины контура прогноза: `v_model_inputs`, `v_gold_dashboard`, `v_forecast_accuracy`, `v_imf_macro`, `v_gold_forecasts`, `v_gold_etf_flows`, `v_cb_gold_dynamics`, `v_gold_etf_flows_daily`, `v_gold_etf_funds_daily` (импортёр `gold_views`) | витрины; гранты `kimi_reader` — в `sql/mcp_kimi_reader.sql` | вручную, после первого запуска ingest: `make import STAT=gold_views` |
| `views` | Метрики компаний сектора: таблица `company_financials` + витрины `v_company_financials` (разрешённое значение, одна строка на компанию/метрику/период), `v_company_metric_sources` (все версии, включая проигравшую), `v_company_operating` (операционка по активам); каталог рядов Полюса в `series_catalog` (импортёр `company_views`) | таблица `company_financials`, витрины; гранты `kimi_reader` — в `sql/mcp_kimi_reader.sql` | `dagu/financial.yaml`, понедельник 10:23 (МСК), после `polyus_financial_metrics`; вручную — `make import STAT=company_views` |
| `dcf` | Сид LOM-планов Полюса из публичной отчётности (импортёр `dcf_mine_plans`): факт добычи последнего отчётного года из `databook_polyus` + версионные константы сида. **Идёт ДО расчётного ядра** | `mine_plans`; каталог `series_catalog` (source `dcf_mine_plans`, 8 рядов) | `dagu/dcf.yaml`, понедельник 11:29 (МСК), шаг 1 из 2 (до `dcf_engine`); вручную: `make import STAT=dcf_mine_plans` |
| `dcf` | Уточнённый поактивный AISC поверх Go-сида (скрипт `scripts/extrapolation_aisc.py`, цель `make dcf-aisc`): калибрует клин поддержания как долю от TCC по пресс-релизам 4Q/FY 2018–2021, экстраполирует на 2025–LOM и увязывает к групповому AISC. **В расписание НЕ входит — применяется вручную** | пишет `sql/dcf_mine_plans_seed.sql` (77 строк) — те же 5 активов × та же сетка годов, что у Go-сида, ключ `(company, asset, year)` → `ReplacingMergeTree` перезапишет 77 строк из 87 | вручную, после `dcf_mine_plans`: `make dcf-aisc` → `make ch-sql FILE=sql/dcf_mine_plans_seed.sql` → `DCF_RUN_ID=<uuid> make import STAT=dcf_engine` |
| `dcf` | Расчётное ядро sum-of-parts LOM-NAV Полюса (импортёр `dcf_engine`): НДПИ-функция, LOM-NPV с хвостом закрытия, эскалация по ИПЦ, два контура ставки. Читает `mine_plans` и `price_decks`, пишет `nav_by_asset` (активы × 3 дека × 2 контура). **Идёт ПОСЛЕ `dcf_mine_plans`** | `mine_plans`, `price_decks`, `nav_by_asset`; витрина `v_dcf_assumptions` (грант `kimi_reader`) | `dagu/dcf.yaml`, понедельник 11:29 (МСК), шаг 2 из 2 (после `dcf_mine_plans`); вручную: `make import STAT=dcf_mine_plans`, затем `DCF_RUN_ID=<uuid> make import STAT=dcf_engine` |
| `polyus` | Отчётность ПАО «Полюс»: xlsx-датапак (операционные результаты по активам с 2007) и PDF-отчёты (KPI-пресс-релизы EN/RU + МСФО-формы EN). Разбор PDF — колоночная модель: `pdftotext -tsv`, полоса шапки по ролям строк, колонки из неё по X | `databook_polyus`; PDF-отчёты → `company_financials` (общая таблица сектора) | понедельник 10:23 |
| `financial` | Legacy: корпоративные databook'и (ЮГК), investing.com, РЖД | `databook_ugk` и др. | понедельник 10:23 |

Импортёр `gold` — временный: производная цена, не LBMA. Официальный LBMA AM/PM пока не подключён, см. «Ограничения».

Импортёр `forecast_resolve` (пакет `forecast/`) закрывает обратную связь журнала прогнозов. Он читает `forecast_log FINAL` по `metric = 'xau_q_avg'` с наступившей `target_date`, берёт окно сверки — квартал, содержащий `target_date` (`[начало квартала, target_date]`), считает `actual` средним `gold_prices` в окне по приоритету `comex_front > moex_fix_usd` (площадка выбирается по количеству точек: среднее пустого окна — NaN, поэтому гейт по счётчику) и `error_pct = (predicted − actual) / actual × 100`. Записывает **одним батчем и только изменившиеся строки** — прогон ежедневный, перезапись того же значения не делается. `nav` и прочие метрики не резолвятся: факта для NAV нет, строка остаётся `is_resolved = 0`. Окно без точек цены — пропуск с предупреждением в логе; таблицу `forecast_log` резолвер не создаёт (её создаёт ingest при старте). Расписание — `dagu/forecast.yaml`, ежедневно 20:23 МСК (после `gold.yaml` и `moex.yaml`); разовый бэкфилл прогноза Q3-2026 — `sql/forecast_log_backfill.sql`. Результат виден в витрине `v_forecast_accuracy`.

Импортёр `dcf_mine_plans` (пакет `dcf/`) **сеет** `mine_plans` — LOM-планы активов Полюса: факт добычи последнего отчётного года он читает из `databook_polyus` (метрика `Total Dore gold output`, год берётся из колонки `date`), остальные числа (TCC, capex, срок службы, профиль проекта развития) берёт из версионных констант сида `dcf/mine_plans_seed.go` — снимка публичной отчётности Полюса. Актива с нулевой/отсутствующей добычей в датапаке пропускаются с предупреждением: сегодня так выпадают `TITIMUKHTA` и `ZAPADNOYE` (их россыпная добыча лежит в таблице `ALLUVIALS`), поэтому сид покрывает **6 активов из 8**. Ряды сида описаны в `series_catalog` (source `dcf_mine_plans`, 8 записей, частота `A`). Полная карта пробелов — [docs/DCF_DATA_COVERAGE.md](docs/DCF_DATA_COVERAGE.md).

**Поактивный AISC поверх Go-сида — ручной шаг `make dcf-aisc`.** Скрипт `scripts/extrapolation_aisc.py` (Python 3, только стандартная библиотека) разбирает поактивный AISC/TCC из четырёх закоммиченных PDF-пресс-релизов Полюса 4Q/FY 2018–2021 (`scripts/testdata/`), калибрует клин поддержания как **долю** от TCC за 2019–2021 (`r̄_i = mean((AISC − TCC)/TCC)`, грубые выбросы отсекаются правилом MAD), экстраполирует клин на 2025–LOM и увязывает его к групповому AISC замкнутой скалярной поправкой `m`. Скрипт **в ClickHouse не пишет** — он генерирует `sql/dcf_mine_plans_seed.sql` (77 строк = 5 активов × 10/13/24/15/15 лет) и `polyus_lom_reconciled_long.csv` (локальный артефакт контроля, в git не хранится). Сид применяется **вручную, отдельным шагом**: в `dagu/dcf.yaml` его нет — цепочка там остаётся двухшаговой (`dcf_mine_plans` → `dcf_engine`), потому что запланированный `make ch-sql` на staging не заработал бы (в релиз попадают только `README.md` и `dagu/*.yaml`, а `make`, Makefile и сам SQL-файл на хосте отсутствуют) и провал был бы **бесшумным**: `dcf_mine_plans` в той же цепочке пересеял бы ключи из Go-снимка, и расчёт прошёл бы на старом клине с зелёным логом. Порядок ручного применения — после `dcf_mine_plans`, до `dcf_engine`:

```bash
make dcf-aisc                                            # сгенерировать sql/dcf_mine_plans_seed.sql
make ch-sql FILE=sql/dcf_mine_plans_seed.sql             # применить (ReplacingMergeTree перезапишет 77 строк из 87)
DCF_RUN_ID=$(uuidgen | tr A-Z a-z) make import STAT=dcf_engine
```

Отличие от Go-сида — в структуре затрат, а не в горизонте: клин `(AISC − TCC)` у каждого рудника свой (доля `r̄` от ~0.49 у Куранаха до ~0.75 у Вернинского против единого «TCC + $698»), а `capex_sustaining` в этих строках равен **0**, потому что поддержание уже внутри `aisc` (Полюс определяет AISC как «TCC plus sustaining capital expenditures», а `npvLOM` вычитает и `aisc`, и `capex_sustaining` — ненулевое значение удвоило бы отток). Клин для глазной сверки виден только в CSV (`wedge_usd_per_oz`, `wedge_mn_usd`). Сухой Лог сид не трогает (проект развития, собственного AISC не раскрывает) — его 10 строк остаются от Go-сида. `VERNINSKOYE2` несёт консервацию карьера с сентября 2024: **нулевая добыча 2025–2028 при сохранённых строках**. Живая приёмка 2026-10-11: `mine_plans FINAL` — 87 строк (164 сырых = 87 + 77, затем 77 перезаписывают 77 по ключу), `dcf_engine` импортирует 36 строк (6 активов × 3 дека × 2 контура), невязка увязки 0.000000, `m` ≈ 1.992279; NAV Сухого Лога не изменился (54 887.40), Вернинского на `own_scenario`/`industrial` — 3 234.85 (было 3 435.34 под Go-сидом).

Импортёр `dcf_engine` (пакет `dcf/`) считает sum-of-parts LOM-NAV по активам: читает LOM-планы из `mine_plans` и цены из `price_decks` (на пустой таблице деков сеет её сама — спот из `gold_prices` плюс два задокументированных LT-якоря), считает NPV каждого актива по трём декам и двум контурам ставки и пишет `nav_by_asset` **одним батчем**. Агенту виден только результат — витрина `v_dcf_assumptions` (ARCHITECTURE.md §6.3); сырые `mine_plans`, `price_decks` и `nav_by_asset` не выданы. **Порядок запуска: сначала `dcf_mine_plans`, затем `dcf_engine`** — движок читает `mine_plans` как вход, и прогон ядра раньше сида посчитал бы по пустому или прошлогоднему плану. В расписании этот порядок ГАРАНТИРУЕТ `dagu/dcf.yaml`: `type: chain` исполняет шаги строго по порядку (`dcf_mine_plans` → `dcf_engine`), поэтому «сид раньше расчёта» — не пожелание, а свойство DAG-а. Запуск:

```bash
make import STAT=dcf_mine_plans                                  # сид mine_plans (до движка)
DCF_RUN_ID=$(uuidgen | tr A-Z a-z) make import STAT=dcf_engine   # пишет nav_by_asset
make import STAT=dcf_engine                                      # без DCF_RUN_ID ничего не пишет
```

`run_id` — клиентский и обязателен: строки `nav_by_asset` ссылаются на `model_runs`, а туда пишет только ingest-endpoint. Без `DCF_RUN_ID` движок считает, печатает в лог готовое тело `POST /v1/model_run` (его дописывает человек и отправляет в ingest) и **не пишет ничего** — `0` строк и запись в лог. Пока `mine_plans` пуста, прогон тоже успешен: предупреждение и `Imported 0 rows`. Строк на прогон — `активы × 3 дека × 2 контура`.

Отчётность Полюса даёт два импортёра в пакете `polyus/`: `databook_polyus` (xlsx-датапак) и `polyus_financial_metrics` (PDF-отчёты, пишет в общую таблицу `company_financials`). Текст извлекается `pdftotext -tsv`; полоса шапки финансовой таблицы собирается по ролям строк (маркер единиц, подписи периодов, подписи изменений), а подписи периодов, разбитые переносом строки («4Q» на одной строке шапки, «2019» на другой), склеиваются по X; значения раскладываются по колонкам, выведенным из шапки по X-координатам. Запуск: `make import STAT=polyus_financial_metrics` или `make import STAT=databook_polyus`; витрины над `company_financials` — `make import STAT=company_views` (в расписании — шаг после импорта метрик, см. `dagu/financial.yaml`). Ограничения этих данных — в разделе «Ограничения»; из 14 отчётов списка включены 9 (2014FY, 2019FY…2024FY, 1H2026), отключены 5 — 2015FY…2018FY (другой тип документа: MD&A и консолидированная МСФО) и 2025H2.

Пакет `wgc/` даёт два импортёра: `wgc_etf_flows` — 20 рядов месячных потоков и запасов физически обеспеченных золотых ETF (`etf_<метрика>_<регион>`, 4 метрики × 5 регионов) и `wgc_cb_reserves` — 14 рядов золотых резервов семи ЦБ (`cb_gold_change_tonnes_<iso3>` — месячные дельты, `cb_gold_holdings_tonnes_<iso3>` — уровни). Оба пишут в `macro_series` с `source = 'wgc'`, читаются витринами `v_gold_etf_flows` (приоритет источников `wgc > webbridge > manual`; сессионный сбор `webbridge` и ручной ввод — фолбэки) и `v_cb_gold_dynamics` (7 стран: MoM, YTD и уровень). **Транспорт ETF-канала — JSON API `fscdn.gold.org`** (`flows-chart2` — потоки, `holdings-chart2` — запасы и AUM; без ключа, `asOfDate` источника — 2026-09-30): ручного шага в прогоне нет, поэтому публикация WGC ловится в день выхода. Локальная выгрузка `ETF_Flows_*.xlsx` в `wgc/data/` — **фолбэк**, если API недоступно (в логе видно `API WGC недоступно … — пробую локальную выгрузку`), а для выгрузок ЦБ ручное скачивание браузером остаётся единственным путём: goldhub отдаёт `/download/file/*` залогированному браузеру, боту отвечает `403`, поэтому папка `wgc/data/` лежит в `.gitignore` и в репозитории не хранится (фикстуры тестов — `wgc/testdata/`). Runbook: ETF — качать ничего не нужно, достаточно дождаться `dagu/wgc.yaml` (ежедневно 20:17 МСК) или прогнать вручную; ЦБ — скачать на gold.org/goldhub актуальные `Changes_latest_as_of_*.xlsx` и `World_official_gold_holdings_as_of_*.xlsx` → положить в `wgc/data/`:

```bash
make import STAT=wgc_etf_flows     # 20 рядов ETF: JSON API (фолбэк — свежайший по mtime ETF_Flows_*.xlsx в wgc/data)
make import STAT=wgc_cb_reserves   # 14 рядов ЦБ (Changes_*.xlsx + World_official_gold_holdings_*.xlsx)
```

Ни API и ни файла ETF, равно как и отсутствие выгрузок ЦБ, — предупреждение в логе и `0` строк (DAG остаётся зелёным); битый файл или пустой разбор — ошибка импорта, а ошибка API сама по себе уводит прогон на файл, а не гасит его. Данные месячные (недельные апдейты — только на сайте, вне контура): релиз ETF-отчёта выходит ~10-го числа следующего месяца, данные ЦБ — с лагом ~2 месяца. Тоннажные стоки (`holdings_tonnes`) API отдаёт **округлёнными до 0,01 т** (потоки и AUM — полной точности), поэтому один и тот же месяц из API и из xlsx-фолбэка расходится не более чем на 0,01 т — это округление источника, а не дефект импорта; глубина: потоки (`flow_*`) всех пяти регионов — с 2003-02-28 (нули в потоках явные), стоки и AUM `world` — с 2007-03-31 (в `holdings` Азии до этого месяца `null`). Оговорки данных (пропуски RUS, net-ряд TUR, UZB с 2013-02) — в разделе «Ограничения».

Пакет `bold/` даёт два импортёра T+1-контура bold.report (ByteTree) — публичного JSON API без ключа (у эндпоинтов обязателен суффикс `.json`, без него источник отвечает `403` — см. «Ограничения»): `bold_etf_flows` — 7 дневных рядов `etf_bold_*` в `macro_series` (`source = 'bold'`) по агрегату 73 золотых ETF и таблица пер-фондового среза `bold_fund_snapshot` (ключ `(date, ticker, name)`: тикер не уникален — `GLD` встречается трижды), а `bold_gold_price` — цена дня источника в `gold_prices` (`venue = 'bold'`; метод источником не раскрыт — «Futures Price», это не спот и не `GC=F`). Агрегат покрывает ≈83 % мировой величины WGC, поэтому его нельзя складывать и сравнивать напрямую с `etf_holdings_tonnes_world`/`etf_flow_tonnes_world` (там все фонды мира и месячная частота); окна 7/30/90 дней календарные (сумма 5 торговых дней не равна `culm7day`), а пер-фондовая история копится с первого прогона — по фондам истории источник не отдаёт. Ряды описаны в каталоге (7 записей), читаются витринами `v_gold_etf_flows_daily` (одна строка на торговый день, 8 колонок) и `v_gold_etf_funds_daily` (73 фонда в день, 10 колонок) — вторая гейтится по `bold_fund_snapshot`, поэтому `make import STAT=gold_views` запускают после первого прогона ETF-импортёра. Обычный прогон щадящий: 4 запроса у ETF и 1 у цены; полный путь (пустая таблица, сохранённый максимум не является непосредственно предыдущим торговым днём даты источника — пропущенный прогон или разрыв больше 4 календарных дней, расхождение максимумов рядов и среза) читает файлы истории — 8 запросов, результат идемпотентен по ключам `ReplacingMergeTree`. Данные T+1 (прошедший торговый день; выходные источник опускает), расписание — `dagu/bold.yaml`, ежедневно 08:07 МСК цепочкой (цена дня → ETF-контур). Запуск:

```bash
make import STAT=bold_gold_price   # цена дня: инкремент — 1 точка, полный путь — вся история (3138 точек с 2014-10-01)
make import STAT=bold_etf_flows    # 7 рядов ETF + 73 строки пер-фондового среза; инкремент — 4 запроса, полный путь — 8
make import STAT=gold_views        # витрины контура, включая v_gold_etf_flows_daily и v_gold_etf_funds_daily
```

Пакет `news/` даёт импортёр `news_watch` — событийная лента РФ-канала: пять RSS-лент (Интерфакс, РБК, две ленты ЦБ РФ — `RssNews` и `eventrss`, TASS) пишутся в `news_events` (ключ `(date, source, title)` — повторный прогон идемпотентен), а агент читает витрину `v_news_events` с широким фильтром по стемам тезисов (дизель/нефть/НПЗ, санкции, ставка/ФРС, инфляция, золото/резервы, НДПИ, Полюс) — сырая таблица ему не выдана. Окна лент конечны (Интерфакс 25 позиций, РБК 30, ЦБ и TASS по 100), поэтому расписание — каждые 30 минут (`dagu/news.yaml`, :07/:37 МСК), а режим расписания — `since`: прогон пишет **только отсутствующие** в `news_events` позиции (проверка наличия по ключу `(date, source, title)`, а не по дате-курсору), поэтому окно не переписывается целиком каждые полчаса, а опоздавшие позиции и второй фид ЦБ под тем же `source` не теряются; окно лент всё равно вычитывается целиком — у RSS нет «since»-параметра. Пропущенный прогон не теряет данные, пока следующее успешное чтение укладывается в окно ленты (простой дольше окна теряет её позиции безвозвратно — границы окон в «Ограничениях и известных проблемах»), и история копится с первого прогона (глубокой ретроспективы ленты не отдают). Дата позиции разбирается по формам `RFC1123Z`, той же форме без секунд и `RFC1123` (последняя — только для `GMT`/`UTC`: иначе аббревиатура зоны молча сдвинула бы время); позиция без распознанной даты или заголовка попадает в счётчик пропусков (ненулевое значение в логе — сигнал, что лента сменила форму), а ноль позиций в ленте или её сбой — ошибка ленты: остальные ленты при этом пишутся, итог собирается через `errors.Join`. Колонка `sentiment` пока остаётся NULL — канал записи для LLM-сессии это follow-up. Режимы переключаются переменной `NEWS_WATCH_MODE` (`all` по умолчанию):

```bash
make import STAT=news_watch                       # полный upsert окна всех лент (режим all — ручная пересборка)
NEWS_WATCH_MODE=since make import STAT=news_watch # режим расписания: только отсутствующие позиции (ключ date/source/title)
NEWS_WATCH_MODE=check make import STAT=news_watch # диагностика покрытия: окна источников и дыры, без записи
```

`check` читает ленты по HTTP и выполняет DDL `CREATE TABLE IF NOT EXISTS news_events` (импортёр обязан отработать и на чистой базе), но **строк не пишет** и витрину не публикует; покрытие он берёт из сырой `news_events`; найденная дыра — ошибка прогона (режим ручной, «зелёный» отчёт с дырой никто не перечитает). Ограничение метода: дыру видно, пока сохранённый максимум источника старше начала окна его ленты, — после следующей успешной записи окно сдвигается вперёд, и `check` её больше не покажет, хотя пропущенные позиции так и не собраны. Отказ ленты виден и в `check`: список непрочитанных лент есть в отчёте, а окно помечено неполным — по неполному окну (например, выжила одна из двух лент ЦБ) дыра намеренно не объявляется, потому что промежуток могла закрывать упавшая лента.

Пакет `craw/` даёт импортёр `gost_vehicle_safety_certificate` — реестр СБКТС Росстандарта (свидетельства о безопасности конструкции ТС), прокси объёма импорта автомобилей: каждой ввезённой машине соответствует запись реестра, и дневной поток выданных сертификатов читается как поток импорта. **Транспорт — REST API портала `gost.ru`** (`/standards/rest-api/libraries/search`, заголовок `gtn.access.appid`), а не HTTP-листинг: портальная пагинация упирается в стену (`from=20000` ≈ 3 с, `from=100000` ≈ 10.7 с, `from=150000` и глубже — стабильный таймаут 60 с), а текстовый `query=DD.MM.YYYY` отдаёт те же документы плоским дневным срезом по дате публикации любой глубины. День читается целиком одним батчем, документы дедуплицируются по `id`, день с недобором — ошибка прогона, день с `count > 10000` отвергается (окно поиска ручки; исторический максимум дня ≈ 2.3 тыс.). Режим задаёт `GOST_MODE`: `since` (по умолчанию и режим расписания — хвост `[якорь − GOST_SETTLE_DAYS, вчера МСК]`, дефолт `GOST_SETTLE_DAYS=3`; лаг «выдача → публикация» у источника ≈ 2 суток, поэтому хвост перечитывается каждый час), `full` (вся история, диапазон `GOST_FROM`/`GOST_TO`, по умолчанию `2016-01-01` … вчера МСК), `check` (read-only сверка `count` API против `uniqExact(certificate_number)` по дню публикации; расхождение — ошибка прогона). Неизвестный режим — ошибка до сети и до БД. Расписание — `dagu/craw.yaml`, раз в час (`:17` МСК), режим `since`; `full` и `check` — только вручную. Витрины `v_gost_vehicle_safety_certificate` (строки, без технического `uid`) и `v_gost_vehicle_safety_daily` (`date, mark, certificates = uniqExact(certificate_number)`) публикует сам импортёр в пишущих режимах. Запуск:

```bash
make import STAT=gost_vehicle_safety_certificate                        # since: хвост (штатный, этим ходит dagu)
GOST_MODE=full GOST_FROM=2016-01-01 GOST_TO=2026-10-11 make import STAT=gost_vehicle_safety_certificate
GOST_MODE=check GOST_FROM=2026-06-01 GOST_TO=2026-06-30 make import STAT=gost_vehicle_safety_certificate
```

`GOST_APP_ID` лежит в `~/.config/rosstat/env` (значение — токен `gtn.access.appid` из JS-конфига страницы реестра, `standarts.adminsHeaders`; в репозитории не хранится, без него импортёр падает до сети). Полный прогон — часы работы (5–10 тыс. запросов), запускается **чанками по годам** (`GOST_FROM`/`GOST_TO`) и **резюмируем**: запись идемпотентна по ключу `ReplacingMergeTree`, неудачные дни перечисляются в ошибке и повторяются следующим прогоном; пустая таблица в режиме `since` даёт предупреждение «запустите `GOST_MODE=full`» и читает последние 7 дней, а если якорь отстал больше чем на 30 дней, окно обрезается месяцем с тем же предупреждением — глубже 30 дней `since` историю не перечитывает, для неё нужен `full`. Ограничения данных: день с более чем 10 тыс. документов отвергается как нечитаемый; повторная публикация изменённого сертификата даёт вторую строку при том же номере (поэтому дневная метрика — `uniqExact`, а не `count()` строк); строки, записанные до бэкфилла, несут `publication_date = 1970-01-01` и в сверке `check` не участвуют (импортёр о них предупреждает); расхождение счётчиков портала (≈3.569 млн документов) и API (≈3.44 млн) — 3.6 % не объяснено (на проверенном окне наборы совпали).

## Структура репозитория

```text
main.go                  точка входа: подключение к ClickHouse, фильтр импортёров, код выхода
chimport/                интерфейс ImportStat и глобальный реестр Stats
util/                    HTTP-клиент (национальные TLS-сертификаты РФ, ротация User-Agent; скачивание XLSX/CSV/JSON), шаблон импортёра ClickHouseImport, батч-вставка
bank/ cbr/ craw/ customs/ fao/ minfin/ rosstat/   доменные импортёры (см. таблицу выше)
fred/                    FRED CSV → macro_series
calendar/                календарь событий-триггеров → events_calendar
forecast/                резолвер журнала прогнозов forecast_resolve: forecast_log + gold_prices → actual/error_pct (своих таблиц не создаёт)
views/                   витрины контура прогноза (v_model_inputs, v_gold_dashboard, v_forecast_accuracy, v_imf_macro, v_gold_forecasts, v_gold_etf_flows, v_gold_etf_flows_daily, v_gold_etf_funds_daily; импортёр gold_views) и витрины метрик компаний сектора (company_financials → v_company_financials, v_company_metric_sources, v_company_operating; импортёр company_views + описания рядов Полюса в series_catalog, views/series_meta.go)
ingest/                  ingest-endpoint контура прогноза: model_runs, forecast_log, macro_series (source = 'manual' | 'webbridge' | 'imf'), gold_prices (venue comex_front), gold_forecasts
cmd/ingest/              бинарник ingest (отдельный от основного импорта)
bls/                     BLS API v2 → macro_series
bea/                     BEA API (NIPA T20804) → macro_series
eia/                     EIA API v2: недельные запасы дистиллятов и загрузка НПЗ США → macro_series
gold/                    MOEX GOLDFIXME + cbr_currency_usd → gold_prices
wgc/                     WGC: месячные потоки и запасы золотых ETF (JSON API fscdn.gold.org; выгрузка ETF_Flows_*.xlsx — фолбэк) + золотые резервы семи ЦБ (выгрузки скачиваются вручную браузером) → macro_series (source = 'wgc'); wgc/data в .gitignore, фикстуры тестов — wgc/testdata
bold/                    bold.report (ByteTree), публичный JSON API без ключа (у эндпоинтов обязателен суффикс .json): два импортёра — bold_etf_flows (7 рядов etf_bold_* → macro_series, source = 'bold' + таблица пер-фондового среза bold_fund_snapshot) и bold_gold_price (цена дня → gold_prices, venue = 'bold'); витрины v_gold_etf_flows_daily и v_gold_etf_funds_daily (views/); тесты офлайн — httptest + bold/testdata
moex/                    MOEX ISS: свечи PLZL → stock_prices, RGBI + G-curve → ofz_curve
news/                    новости РФ-канала: RSS-ленты Интерфакс/РБК/ЦБ РФ/TASS → news_events (режимы all/since/check через NEWS_WATCH_MODE); витрина v_news_events (сырая news_events агенту не выдана); расписание dagu/news.yaml
polyus/                  отчётность Полюса: xlsx-датапак → databook_polyus; PDF-отчёты (KPI EN/RU, МСФО EN) → company_financials (общая таблица сектора; читается витринами v_company_*, разбор — колоночная модель: pdftotext -tsv + полоса шапки по ролям строк + колонки по X)
dcf/                     расчётное ядро DCF: sum-of-parts LOM-NAV по активам, таблицы mine_plans, price_decks, nav_by_asset (DDL — копии канонических из ARCHITECTURE.md §6.2), чистое ядро без БД (НДПИ на унцию, LOM-NPV с хвостом закрытия, эскалация по ИПЦ, две ставки контуров). Два импортёра: dcf_mine_plans (сид mine_plans из отчётности Полюса + каталог рядов dcf/series_meta.go; идёт ДО движка) и dcf_engine (расчёт; витрина views/dcf_assumptions.go). Порядок гарантирует dagu/dcf.yaml. model_runs не пишет
financial/               legacy-контур (database/sql, свой main), новый код туда не добавляется; остался databook_ugk (ЮГК)
dagu/                    расписания: один DAG-файл на домен, имя файла = имя DAG
sql/                     DDL и гранты вручную: пользователь kimi_reader и гранты на витрины, сид календаря Q4-2026, сид консенсуса gold_forecasts (gold_forecasts_q4_2026_seed.sql), каталог 20 рядов WGC ETF Flows (wgc_etf_flows_catalog.sql), бэкфилл журнала прогнозов (forecast_log_backfill.sql)
scripts/                 проверки MCP (mcp_setup_user.py, mcp_check.py), smoke дашборда Grafana (grafana_check.py), экстраполяция поактивного AISC Полюса (extrapolation_aisc.py + тест и генератор испорченной PDF-фикстуры) и тесты скриптов
grafana/                 дашборд gold-thesis как код: отдельный Go-модуль на grafana-foundation-sdk (цель make grafana-build генерирует dashboard/gold-thesis.json), тесты состава панелей и валидации; провижининг датасорса локального стенда (grafana/provisioning/datasources/clickhouse.yaml)
dashboard/               артефакты дашбордов Grafana: gold-thesis.json коммитится и генерируется из grafana/ (руками не правится); легаси-экспорты rosstat.json, chmf.json, finance-polyus.json остаются как есть — миграция на код отдельным пунктом
deploy/staging/           артефакты деплоя на staging: юниты dagu и mcp-clickhouse, образец env
docs/                    аналитические статьи и дорожная карта
.github/workflows/       CI (go.yml, dashboards.yml) и релиз по тегу v* (release.yml)
```

## Быстрый старт

Нужны Go 1.27, ClickHouse с нативным протоколом (порт 9000) и `CLICKHOUSE_URL`.

```bash
make all                                # lint, тесты, сборка
export CLICKHOUSE_URL="clickhouse://user:password@host:9000/database"
CLICKHOUSE_IMPORT_STAT=fred,gold_prices ./build/clickhouse-import-rosstat
```

`make build` собирает статический бинарник `linux/amd64` в `build/` (для сервера). Для запуска на Mac соберите нативный бинарник: `go build -o /tmp/rosstat-bin .`

Проверка результата в ClickHouse:

```sql
SELECT series, count(), max(date) FROM macro_series FINAL GROUP BY series;
SELECT venue, count(), max(date) FROM gold_prices FINAL GROUP BY venue;
```

## Конфигурация

| Переменная | Назначение |
|---|---|
| `CLICKHOUSE_URL` | DSN подключения (обязательна). Не выводится в логах |
| `CLICKHOUSE_IMPORT_STAT` | Фильтр импортёров через запятую по имени (`Name()`). Пусто — все импортёры |
| `LOG_LEVEL` | Уровень logrus: `debug`, `info`, `warn`, `error` |
| `CERT_FILES` | PEM-сертификаты через запятую (нац. УЦ РФ для gos-сайтов) |
| `BLS_API_KEY` | Регистрационный ключ BLS (необязателен; без него работает, но лимиты строже). Не выводится в логах |
| `BEA_API_KEY` | UserID BEA API (обязателен для `bea`; без него импортёр завершается ошибкой). Не выводится в логах |
| `EIA_API_KEY` | Ключ EIA API v2 (обязателен для `eia`; без него импортёр завершается ошибкой, ключ маскируется в текстах ошибок). Не выводится в логах |
| `INGEST_TOKEN` | Bearer-токен ingest-endpoint, обязателен для `cmd/ingest` (без него процесс не стартует). Не выводится в логах |
| `INGEST_ADDR` | Адрес HTTP-сервера `cmd/ingest`, по умолчанию `:8081` |
| `INGEST_RATE_PER_MIN` | Лимит запросов `cmd/ingest` в минуту (token bucket, burst 10), по умолчанию 60; превышение — `429` |
| `NEWS_WATCH_MODE` | Режим импортёра `news_watch`: пусто или `all` — полный upsert окна всех лент (ручная пересборка), `since` — режим расписания dagu: только отсутствующие в `news_events` позиции по ключу `(date, source, title)`, `check` — проверка дыр/окон без записи. Неизвестное значение — ошибка до сети и до БД |

Поведение при запуске: неизвестное имя в `CLICKHOUSE_IMPORT_STAT` даёт предупреждение; ошибка любого импортёра даёт код выхода `1`, остальные импортёры при этом выполняются.

Импортёр `financial` читает `TICKER`, `FINANCIAL_DATA_DIR`, `INVESTING_EMAIL`, `INVESTING_PASSWORD`; эти переменные устаревают вместе с legacy-контуром.

`DCF_RUN_ID` — переменная окружения **разового прогона** `dcf_engine`, а не строка конфигурации источника: UUID прогона, который уходит в `nav_by_asset.run_id` и связывает строки с записью `model_runs` (её создаёт ingest-endpoint). Передавайте её прямо в команде — `DCF_RUN_ID=$(uuidgen | tr A-Z a-z) make import STAT=dcf_engine`; в `~/.config/rosstat/env` её хранить не нужно. Без `DCF_RUN_ID` движок считает и печатает тело `POST /v1/model_run`, но не пишет ни строки (см. «Что импортируется»). Импортёр `dcf_mine_plans` переменных окружения не имеет (только `CLICKHOUSE_URL`).

Переменные стендов Grafana (`GRAFANA_TEST_URL`/`GRAFANA_TEST_TOKEN`, `GRAFANA_STAGING_URL`/`GRAFANA_STAGING_TOKEN`, `GRAFANA_ORG_ID`) к импортёрам не относятся — они нужны целям `make grafana-*` (дашборд как код, деплой и smoke). Значения живут в том же `~/.config/rosstat/env`, `make env-check` печатает только «задан/не задан»; описание — в разделе «Grafana».

## Оркестрация (Dagu)

Расписания описаны в `dagu/*.yaml`: `type: chain`, `overlap_policy: skip`, каждый шаг запускает бинарник с `CLICKHOUSE_IMPORT_STAT=<имя>`, ретраи — 3 попытки с паузой 300 с. Путь к бинарнику (`ROSSTAT_IMPORT_BIN`) и `CLICKHOUSE_URL` задаются в `base.yaml` Dagu; секреты лежат в файлах `/etc/dagu/secrets/` с правами `0600`, в репозитории их нет. Внутри домена зависимости задаются через `depends`; между DAG зависимостей нет (см. `gold.yaml`: шаг `cbr_currency_usd` стоит внутри DAG). Отдельный `dagu/dcf.yaml` использует именно это свойство DAG-а: `type: chain` исполняет `dcf_mine_plans` строго до `dcf_engine`, и «сид раньше расчёта» — гарантия расписания, а не только порядок ручных команд (Finding 3 финального ревью). **Шага уточнённого AISC-сида в `dagu/dcf.yaml` нет** — он применяется вручную (см. «Что импортируется» и «Ограничения»); DAG содержит ровно два шага, и придумывать третий не нужно.

Запуск одного DAG вручную: `dagu start dagu/gold.yaml` (dev-окружение Dagu должно видеть `ROSSTAT_IMPORT_BIN` и `CLICKHOUSE_URL` через `base.yaml`).

## Контур прогноза золота

Составные части контура (подробно — ARCHITECTURE.md §6.3 и §6.5):

- витрины `v_model_inputs`, `v_gold_dashboard`, `v_forecast_accuracy`, `v_imf_macro`, `v_eia_petroleum`, `v_gold_forecasts`, `v_gold_etf_flows`, `v_cb_gold_dynamics`, `v_gold_etf_flows_daily`, `v_gold_etf_funds_daily`, `v_news_events` и календарь `v_events_calendar` — читаются агентом через MCP (пользователь `kimi_reader`, только `v_*`); гранты — в `sql/mcp_kimi_reader.sql`;
- витрины метрик компаний сектора `v_company_financials`, `v_company_metric_sources`, `v_company_operating` (импортёр `company_views`) — читаются агентом через MCP; сейчас наполнены данными Полюса (ARCHITECTURE.md §6.3);
- витрина `v_dcf_assumptions` — NPV рудников Полюса по каждому (deck, контур ставки, актив) плюс суммарный NAV рудников; пишет её импортёр `dcf_engine` (пакет `dcf/`, `DCF_RUN_ID=<uuid> make import STAT=dcf_engine`), сырая `nav_by_asset` агенту не выдана;
- импортёр `forecast_resolve` (пакет `forecast/`, `dagu/forecast.yaml`, 20:23 МСК) — обратная связь журнала прогнозов: заполняет `actual`/`error_pct` в `forecast_log` сверкой `xau_q_avg` со средним `gold_prices` за квартал `target_date` (ARCHITECTURE.md §6.1);
- ingest-endpoint (`cmd/ingest`) — единственный путь записи прогонов модели, ручных рядов, цен COMEX и консенсус-прогнозов;
- плагин Kimi `gold-nav` — отдельный репозиторий, сценарная сессия прогноза и DCF NAV PLZL.

### Ingest-endpoint

Запуск: `make run-ingest` (нужны `CLICKHOUSE_URL` и `INGEST_TOKEN` в `~/.config/rosstat/env`; `INGEST_ADDR` по умолчанию `:8081`). При старте создаются `model_runs`, `forecast_log`, `macro_series`, `gold_prices`, `gold_forecasts`, если их ещё нет. Витрины `views/` создаются отдельно: `make import STAT=gold_views` после первого запуска ingest.

| Метод и путь | Тело | Ответ |
|---|---|---|
| `POST /v1/model_run` | JSON прогона: клиентский `run_id` (UUID), `trigger_type`, `price_deck`, `probabilities`, `gold_scenario` с `horizon`, значения NAV | `200 {"inserted": 1}`; `400` при ошибке валидации |
| `POST /v1/manual_series` | JSON-массив `{"series", "date": "YYYY-MM-DD" (диапазон Date32 [1900-01-01, 2299-12-31]), "value", "source"?}` → `macro_series`; `source` необязателен (по умолчанию `manual`), допустимы `manual`, `webbridge`, `imf` | `200 {"inserted": N}`; `400` при ошибке любой точки |
| `POST /v1/gold_prices` | JSON-массив `{"venue", "date": "YYYY-MM-DD" (диапазон Date32), "usd"}` → `gold_prices`; допустимая venue — `comex_front` (словарь расширяемый), `usd` > 0 | `200 {"inserted": N}`; `400` при ошибке любой точки |
| `POST /v1/gold_forecasts` | JSON-массив `{"author", "published": "YYYY-MM-DD" (диапазон Date32), "horizon", "scenario", "low", "high", "point", "probability"}` → `gold_forecasts`; `author`/`horizon` непусты, `scenario` ∈ {`point`,`range`,`bull`,`base`,`bear`}, `low`/`high`/`point` ≥ 0 и хотя бы одно > 0, `high ≥ low` (когда заданы оба), `probability` 0–100 (0 — «не задана») | `200 {"inserted": N}`; `400` при ошибке любой строки |

Все запросы — с заголовком `Authorization: Bearer <INGEST_TOKEN>`; без него или с неверным токеном — `401`. Сверх `INGEST_RATE_PER_MIN` (по умолчанию 60 в минуту) — `429`; тело больше 1 MiB — `413`; неизвестное поле в JSON — `400`. Повторный `POST /v1/model_run` с тем же `run_id` ничего не записывает. Формат тела — в `ingest/model_run.go` (структура `ModelRun`).

### Плагин gold-nav

Плагин — отдельный репозиторий (не входит в этот проект). Локальная копия рядом: `../gold-nav`. Спецификация: [docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md](docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md).

- Kimi Code: `/plugins install <github-url>` (ссылка на репозиторий — после публикации);
- Kimi Work: зарегистрировать каталог плагина в персональном маркете (`kimi-daimon kimi-plugin register-personal <путь к gold-nav>`) и включить в «Плагины → Персональный».

Переменные окружения пользователя плагина (только имена, значения выдаёт автор): `GOLD_NAV_MCP_TOKEN` — read-only токен MCP; `GOLD_NAV_INGEST_URL` и `GOLD_NAV_INGEST_TOKEN` — только у автора. Команды плагина: `/gold-nav:session` (сценарная сессия) и `/gold-nav:verify` (сверка прогнозов с фактом).

Живая приёмка плагина в Kimi Code и Kimi Work не выполнена; см. раздел «Результат проверки» в README плагина.

## Разработка

| Команда | Что делает |
|---|---|
| `make lint` | `gofmt -l` (падает, если есть неотформатированный код) и `golangci-lint run ./...` |
| `make test` | `go vet` и `go test -race ./...` (тесты есть у `bls`, `bea`, `eia`, `cbr`, `rosstat`, `minfin`, `gold`, `moex`, `polyus`) |
| `make build` | статический бинарник `linux/amd64` в `build/` |
| `make run` | сборка и запуск (нужен `CLICKHOUSE_URL`) |
| `make import STAT=<имя>` | локальный прогон одного импортёра по `Name()` (собирает `build/clickhouse-import-rosstat-native`). Для расчёта NAV: `DCF_RUN_ID=$(uuidgen \| tr A-Z a-z) make import STAT=dcf_engine` |
| `make build-ingest` | сборка ingest-endpoint (`cmd/ingest`) в `build/ingest` |
| `make run-ingest` | сборка и запуск ingest-endpoint (нужны `CLICKHOUSE_URL` и `INGEST_TOKEN`) |
| `make fmt` | `gofmt -w .` |
| `make deps` | `go mod download` и `go mod tidy` |
| `make clean` | удалить `build/` |
| `make ch-up` | запустить локальный ClickHouse (порты 8123/9000), если он ещё не отвечает |
| `make ch-down` | остановить сервер, поднятый через `make ch-up` (по PID-файлу) |
| `make ch-status` | проверить, отвечает ли сервер на `localhost:8123` |
| `make ch-sql FILE=...` | прогнать SQL-файл репозитория в локальный ClickHouse (`--multiquery`; пользователь `CH_USER`, по умолчанию `default` без пароля) |
| `make dcf-aisc` | сгенерировать сид поактивного AISC Полюса `sql/dcf_mine_plans_seed.sql` (77 строк) и CSV-артефакт контроля; в ClickHouse не пишет. Нужны env-переменные ClickHouse и живая БД (читает факт добычи из `databook_polyus`); Python 3, без зависимостей. Применение сида — вручную, отдельным шагом (см. «Что импортируется») |
| `make dev-check` | проверить наличие `go`, `gofmt`, `golangci-lint`, `uv`, `curl`, `clickhouse` |
| `make mcp-run` | запустить MCP-сервер `mcp-clickhouse` в stdio вручную (нужен `CLICKHOUSE_PASSWORD`) |
| `make mcp-user` | создать пользователя `kimi_reader` и выдать `GRANT SELECT` на витрины из `sql/mcp_kimi_reader.sql` (нужен `CLICKHOUSE_PASSWORD`, запущенный ClickHouse) |
| `make mcp-check` | smoke-проверка MCP: инструменты, витрины, отказ на сырые таблицы; отдельно — артефакты фаз 3 и wgc-контура: витрины `v_gold_forecasts`, `v_gold_etf_flows` и `v_cb_gold_dynamics`, каталоги `webbridge` (20 рядов) и `wgc` (34 ряда), резолв `v_forecast_accuracy` и запрет сырой `gold_forecasts` (нужен `CLICKHOUSE_PASSWORD`, запущенный ClickHouse). Адрес переопределяется: `MCP_CHECK_HOST`/`MCP_CHECK_PORT` — для проверки staging через туннель |
| `make deploy-staging` | развернуть на staging бинарник, `dagu` и `mcp-clickhouse` (см. «Деплой на staging») |
| `make deploy-staging-binary` / `-dagu` / `-mcp` | точечно: только бинарник, только dagu, только MCP |
| `make upgrade-staging` | обновить **только** dagu и mcp-clickhouse до версий из репозитория (без `apt upgrade`) |
| `make mcp-user-staging` | создать `kimi_reader` на staging (нужны `CLICKHOUSE_PASSWORD` и `CH_SETUP_PASSWORD`) |
| `make grafana-build` | сгенерировать `dashboard/gold-thesis.json` из кода модуля `grafana/` (grafana-foundation-sdk). Детерминирована: повторный прогон не меняет артефакт |
| `make grafana-validate` | `go vet` и тесты модуля `grafana/` (состав панелей и ряд негативных правил `Validate`). Серверная `gcx resources validate` сюда не входит — она требует живого инстанса и кредов (рабочая форма — в комментарии цели; на стенде даёт `✔ No errors found`) |
| `make grafana-up` / `make grafana-down` | поднять/остановить локальный стенд Grafana (brew-Grafana 13.2.3; данные, логи и плагины в `$(GF_DIR)`, провижининг — `grafana/provisioning`, порт 3000). Плагин `grafana-clickhouse-datasource` 4.3.2 ставится сам; системный `brew services` не используется |
| `make grafana-test-token` | сервис-аккаунт `dashboards-cd` (роль **Editor**) на локальном стенде и свежий токен к нему: аккаунт создаётся идемпотентно (повтор — «уже есть (id N)»), токен — всегда новый (имя `deploy-<UTC-метка>-<pid>`: Grafana не принимает два токена с одним именем) и проверяется `GET /api/user` **до** печати. Печатает готовую строку `GRAFANA_TEST_TOKEN=glsa_…` для `~/.config/rosstat/env` (секрет в репозиторий/логи не попадает). Нужен запущенный стенд (иначе «стенд не отвечает: make grafana-up») и админ-креды `GRAFANA_ADMIN_USER`/`GRAFANA_ADMIN_PASSWORD` (дефолт `admin`/`admin` — локальный одноразовый стенд) |
| `make grafana-check` | smoke задеплоенного дашборда: `GET /api/dashboards/uid/gold-thesis` = 200 и первые панели отдают точки (SQL берётся из самого дашборда, снятого со стенда). Нужны `GRAFANA_TEST_URL` и `GRAFANA_TEST_TOKEN` |
| `make grafana-deploy-test` | выгрузить дашборд на тест-стенд (`gcx resources push dashboards --path dashboard/gold-thesis.json`) и прогнать `grafana-check`. Идемпотентна: повторный push не создаёт дубль, растёт только `version` |
| `make grafana-provision-staging` | идемпотентно разложить провижининг датасорса на staging: шаблон `deploy/staging/grafana-datasource.yaml`, пароль подставляется на хосте из env-файла, результат — `root:grafana 640`, переустановка только при отличии sha256 |
| `make grafana-deploy-staging` | staging-деплой через туннель `:3001`: проверка токена → доступность → `grafana-provision-staging` → push дашборда → smoke. Нужен `GRAFANA_STAGING_TOKEN` |

### Деплой на staging

Разворачивает три компонента на хост `palmshell`: бинарник `clickhouse-import-rosstat`, планировщик `dagu` v2.18.2 и MCP-сервер `mcp-clickhouse` 0.7.0 (HTTP, read-only). ClickHouse и Grafana на хосте не трогаются. Артефакты — `deploy/staging/`, порядок и отладка — `deploy/staging/README.md`.

```bash
# 1. Файл секретов на хосте (один раз; значения задаёт владелец). Токен: openssl rand -hex 32
ssh palmshell 'install -m 600 /dev/null /etc/clickhouse-import-rosstat.env'
#    вписать по образцу deploy/staging/clickhouse-import-rosstat.env.example

# 2. Развернуть и создать пользователя MCP
make deploy-staging
CH_SETUP_PASSWORD=<пароль default> make mcp-user-staging

# 3. Проверить end-to-end через туннель
ssh -N -L 8123:127.0.0.1:8123 palmshell &
CLICKHOUSE_PASSWORD=<пароль kimi_reader> make mcp-check MCP_CHECK_HOST=127.0.0.1 MCP_CHECK_PORT=8123
```

Переменные: `STAGING_HOST` (по умолчанию `palmshell`), `MCP_VERSION`, `DAGU_VERSION`. `deploy-staging` **падает**, если файла секретов нет, и никогда его не создаёт.

**Внимание:** после `make deploy-staging` `dagu` включается и **начинает выполнять DAG-и по расписанию** — это реальные запросы к внешним источникам. Порядок внутри цели защищает от запуска DAG-ов на неисправном бинарнике: сначала проверяются бинарь и DAG-файлы, потом сервисы включаются, потом идёт smoke.

Сервисы слушают только `127.0.0.1` (`dagu` :8080, MCP :8000): наружу ничего не публикуется, TLS/Caddy и публичный MCP-эндпоинт — следующая фаза.

### Зависимости для разработки

- Go — версия из `go.mod` (сейчас `1.27`); в CI используется Go `1.27.x`;
- `golangci-lint` v2 (`brew install golangci-lint`), конфиг — `.golangci.yml`;
- `uv` (`brew install uv`): запускает `mcp-clickhouse` без установки в систему, Python `3.12` подтягивается `uv`;
- ClickHouse локально: бинарник из `PATH` или `~/.clickhouse/versions/*/clickhouse` (или `CH_BIN=...`);
- `curl` — для проверки статуса сервера;
- GoLand 2025.2+ (опционально) — MCP-сервер IDE для Kimi Code, см. «MCP GoLand (опционально)» ниже.

Порядок проверки окружения и работы:

```bash
make dev-check                       # что установлено
make all                             # lint, test, build
make env-check                       # ключи из ~/.config/rosstat/env на месте
make ch-up                           # локальный ClickHouse на 8123/9000
make import STAT=bea                 # импорт bea (ключ BEA_API_KEY из файла окружения)
make import STAT=eia                 # импорт eia (ключ EIA_API_KEY из файла окружения)
make import STAT=wgc_etf_flows       # WGC ETF Flows: JSON API fscdn.gold.org (фолбэк — ETF_Flows_*.xlsx в wgc/data)
make import STAT=wgc_cb_reserves     # WGC золотые резервы ЦБ (Changes_*.xlsx + World_official_gold_holdings_*.xlsx в wgc/data)
make import STAT=bold_gold_price     # bold.report: цена дня (JSON API без ключа; полный путь — вся история)
make import STAT=bold_etf_flows      # bold.report: 7 дневных рядов ETF-агрегата 73 фондов + пер-фондовый срез
make import STAT=gold_views          # витрины контура, включая v_gold_etf_flows_daily и v_gold_etf_funds_daily (после первого прогона bold_etf_flows)
make import STAT=dcf_mine_plans      # сид mine_plans из отчётности Полюса (идёт до движка)
make dcf-aisc                        # (опционально) уточнённый поактивный AISC → sql/dcf_mine_plans_seed.sql
make ch-sql FILE=sql/dcf_mine_plans_seed.sql   # (опционально) применить сид AISC поверх Go-сида
DCF_RUN_ID=$(uuidgen | tr A-Z a-z) make import STAT=dcf_engine   # расчёт NAV по засеянным планам (в шелле без uuidgen — любой UUID)
make import STAT=forecast_resolve    # сверка прогнозов журнала с фактом (нужны строки forecast_log: ingest-прогон или бэкфилл sql/forecast_log_backfill.sql)
make import STAT=news_watch          # ленты РФ-канала: события RSS пяти лент → news_events (all по умолчанию — полный upsert окна)
NEWS_WATCH_MODE=since make import STAT=news_watch   # штатный режим расписания: только отсутствующие позиции (ключ date/source/title)
NEWS_WATCH_MODE=check make import STAT=news_watch   # диагностика покрытия: окна источников и дыры, без записи
make ch-sql FILE=sql/gold_forecasts_q4_2026_seed.sql   # (опционально) сид консенсуса банков → v_gold_forecasts (до make import STAT=gold_views)
make ch-sql FILE=sql/wgc_etf_flows_catalog.sql         # (опционально) каталог 20 рядов WGC ETF Flows (source='webbridge')
make mcp-check                       # MCP end-to-end (CLICKHOUSE_PASSWORD из файла окружения)
make ch-down                         # остановить сервер
```

Пароль `CLICKHOUSE_PASSWORD` задаётся только в окружении текущей сессии, в файлы репозитория не записывается.

Проверка MCP в Kimi Code: откройте новую сессию в этом репозитории, командой `/mcp-config` убедитесь, что сервер `clickhouse` подключён (в версии Kimi Code, где команды `/mcp` нет), и отправьте агенту промпт:

> Проверь MCP-сервер clickhouse. 1) Через list_databases покажи базы. 2) Через list_tables для базы default покажи таблицы и комментарии колонок v_bea_pce. 3) run_query: SELECT series, unit, title FROM v_series_catalog WHERE source = 'bea' ORDER BY series — ожидаю 2 ряда (PCE_PI, PCE_PI_CORE). 4) run_query: SELECT date, value FROM v_bea_pce WHERE series = 'PCE_PI' ORDER BY date DESC LIMIT 3 — ожидаю 3 строки. 5) run_query: SELECT count() FROM macro_series — ожидаю отказ ACCESS_DENIED (доступа к сырым таблицам у агента нет). Ответь таблицей: шаг, результат, ожидание, совпало ли.

Локальный ClickHouse для проверок:

- бинарник ищется в `PATH` или в `~/.clickhouse/versions/*/clickhouse`; иначе укажите `CH_BIN=/путь/к/clickhouse`;
- данные, PID и лог лежат в `~/clickhouse-dev/.clickhouse/servers/dev/` (переменная `CH_DIR`), вне репозитория;
- запуск: `make ch-up`, затем `export CLICKHOUSE_URL="clickhouse://localhost:9000/default"`;
- импорт и проверка: `CLICKHOUSE_IMPORT_STAT=bls ./build/clickhouse-import-rosstat` (или нативный бинарник, см. «Быстрый старт»);
- остановка: `make ch-down`.

### Ключи и окружение

Ключи и пароли лежат в одном файле вне репозитория: `~/.config/rosstat/env` (права `600`, каталог `700`). Формат:

```bash
export BEA_API_KEY=<your-key>
export EIA_API_KEY=<your-key>
export CLICKHOUSE_URL=clickhouse://localhost:9000/default
export CLICKHOUSE_PASSWORD=<kimi_reader-password>
# export CH_IMPORT_PASSWORD=<пароль пользователя, читающего сырые таблицы>
# export BLS_API_KEY=<your-key>
# export INGEST_TOKEN=<ingest-token>
# export INGEST_RATE_PER_MIN=60
# export GRAFANA_TEST_TOKEN=<токен сервис-аккаунта Editor локального стенда :3000>
# export GRAFANA_STAGING_TOKEN=<токен сервис-аккаунта Editor на staging, доступ через туннель :3001>
# export GRAFANA_ORG_ID=1
```

- `CLICKHOUSE_URL` — **основной** способ задать адрес: это DSN, из которого Go-код (`clickhouse.ParseDSN`) и `scripts/extrapolation_aisc.py` берут хост, порт, пользователя и базу. Дискретные `CLICKHOUSE_HOST`/`CLICKHOUSE_PORT`/`CLICKHOUSE_USER`/`CLICKHOUSE_DATABASE` в файле обычно **не задаются**; скрипт читает их как переопределение поверх DSN;
- `CLICKHOUSE_PASSWORD` — пароль **`kimi_reader`**, read-only пользователя витрин (`make mcp-check`). Сырые таблицы ему закрыты: чтение `databook_polyus` и подобных идёт под другим пользователем, поэтому у скриптов есть **`CH_IMPORT_PASSWORD`**. Если пользователь из DSN пароля не имеет (локальный `default`), переменную задавать не нужно — подставлять вместо неё `CLICKHOUSE_PASSWORD` нельзя, это чужой пароль и он даёт 401.

- Makefile подключает файл сам (`-include`), поэтому любая `make`-цель видит ключи без `export` в shell; путь можно переопределить: `make ROSSTAT_ENV=/путь/к/файлу ...`;
- в терминале: `set -a; . ~/.config/rosstat/env; set +a`;
- проверка без вывода значений: `make env-check`;
- локальный импорт одного источника: `make import STAT=bea` (собирает бинарник под macOS в `build/clickhouse-import-rosstat-native`);
- Kimi Code: ключи для MCP задаются в `~/.kimi-code/mcp.json` (блок `env`), для остальных команд Kimi вызывает `make`, поэтому файл окружения подхватывается автоматически;
- Dagu (расписания): секреты описаны в `~/dagu-dev/base.yaml` через provider `file`, каждый ключ — отдельный файл `~/dagu-dev/secrets/<name>` (`600`). Для BEA: `bea_api_key`; для EIA: `eia_api_key`; при добавлении ключа нового импортёра — новый файл и строка в `secrets:`;
- файлы `*.env`, `secrets/` и ключи в коде не коммитить; `CLICKHOUSE_PASSWORD` только в файле окружения (`~/.config/rosstat/env`), в `mcp.json` его нет.

MCP для Kimi (агент читает витрины `v_*`, без записи):

- установить `uv`: `brew install uv`;
- создать пользователя и выдать права: `make mcp-user` (берёт `CLICKHOUSE_PASSWORD` из файла окружения, выполняет `sql/mcp_kimi_reader.sql`; пароль в выводе не печатается; повторный запуск не меняет уже созданного пользователя);
- записать `~/.kimi-code/mcp.json` (права `600`); пароль в файл не пишется, команда сама подгружает `~/.config/rosstat/env`:

```json
{
  "mcpServers": {
    "clickhouse": {
      "command": "bash",
      "args": ["-c", "set -a; . \"$HOME/.config/rosstat/env\"; set +a; exec uv run --with mcp-clickhouse --python 3.12 mcp-clickhouse"],
      "env": {
        "CLICKHOUSE_HOST": "localhost",
        "CLICKHOUSE_PORT": "8123",
        "CLICKHOUSE_SECURE": "false",
        "CLICKHOUSE_VERIFY": "false",
        "CLICKHOUSE_USER": "kimi_reader",
        "CLICKHOUSE_DATABASE": "default",
        "CLICKHOUSE_MCP_SERVER_TRANSPORT": "stdio"
      }
    }
  }
}
```

- проверить в Kimi командой `/mcp-config` в новой сессии: сервер `clickhouse` должен быть подключён;
- новые ряды и витрины: см. AGENTS.md, правило 11. Legacy-таблицы `cbr_*`, `rosstat`, `minfin`, `gold_prices` агенту доступны через групповые витрины `v_cbr_macro`, `v_rosstat_macro`, `v_minfin_budget`, `v_gold_prices` (создаются, когда все таблицы группы импортированы), описания рядов — в `v_series_catalog`. Витрины фазы 3 (`v_gold_forecasts`, `v_gold_etf_flows`), wgc-контура (`v_cb_gold_dynamics`) и контура новостей (`v_news_events`; сырая `news_events` агенту не выдана) выданы тем же грантом в `sql/mcp_kimi_reader.sql`; их чтение, каталоги `webbridge` (20 рядов) и `wgc` (34 ряда) и запрет сырых `gold_forecasts` и `news_events` проверяет `make mcp-check`.

MCP GoLand (опционально, ускоряет работу Kimi Code с кодом):

- зачем: агент получает инструменты IDE — ошибки и инспекции файла по индексу GoLand (`get_file_problems`), сигнатуры символов (`get_symbol_info`), семантический поиск и иерархию вызовов (`search_symbol`, `analyze_calls`), безопасный rename (`rename_refactoring`); как и когда ими пользоваться — в AGENTS.md, раздел «Инструменты GoLand (MCP `jetbrains`)». Без них агент работает обычными `Grep`/`Read`/`Edit`, настройка не обязательна;
- требование: GoLand 2025.2 или новее — MCP-сервер встроен (плагин MCP Server включён по умолчанию); npm-пакет `@jetbrains/mcp-proxy` deprecated и не нужен;
- настройка:
  1. открыть этот репозиторий в GoLand;
  2. Settings | Tools | MCP Server → включить **Enable MCP Server**;
  3. там же: **Copy HTTP Stream Config** — в буфере окажется URL вида `http://127.0.0.1:<port>/stream`;
  4. добавить запись в `~/.kimi-code/mcp.json` (URL из шага 3, порт подставить свой):

  ```json
  {
    "mcpServers": {
      "jetbrains": {
        "url": "http://127.0.0.1:<port>/stream"
      }
    }
  }
  ```

  5. начать новую сессию Kimi Code (`/new`) и проверить `/mcp-config`: сервер `jetbrains` подключён;
- порт выдаётся динамически и может смениться после перезапуска GoLand: если инструменты `jetbrains` пропали, повторите шаги 3–4;
- инструменты видны только пока GoLand запущен с открытым проектом.

Правила кода (подробно — в [AGENTS.md](AGENTS.md) и [ARCHITECTURE.md](ARCHITECTURE.md)):

- вставка в ClickHouse только батчами: `PrepareBatch` → `Append` → `Send`; построчный `Exec` в цикле запрещён;
- секреты только через `os.Getenv`; сетевых вызовов в `init()` не делать;
- HTTP только через `util.HttpClient` (и `util.GetXlsx`, `util.GetCSV`, `util.GetJSON` — последний проверяет HTTP-статус);
- DDL — `CREATE TABLE IF NOT EXISTS`, движок `ReplacingMergeTree`, значения новых таблиц — `Float64`;
- ошибки не проглатывать: парсинг чисел и `batch.Append` проверяются.

### CI и релизы

`.github/workflows/go.yml` на каждый push в `main` и на pull request:

- `test`: `gofmt`, `go vet`, `go test -race`;
- `lint`: `golangci-lint` v2.14.0 с конфигом `.golangci.yml`. Пакет `financial/` исключён из линта как legacy;
- `vulncheck`: `govulncheck ./...` (падает при уязвимостях, которые затрагивают код);
- `build`: `make build`, артефакт `build/`.

Все задания используют Go `1.27.x`: `govulncheck` проверяет стандартную библиотеку тулчейном, которым запущен, и на go1.27.1 находил уязвимости `net/http` и `crypto/tls`, исправленные в go1.27.2.

`.github/workflows/dashboards.yml` (`dashboards`) на каждый push в `main` и на pull request проверяет дашборд `gold-thesis` как код: `go vet` и `go test` модуля `grafana/`, `make grafana-build`, `git diff --exit-code -- dashboard/gold-thesis.json` (артефакт совпадает со свежей сборкой — заодно ловит недетерминированную генерацию), установка пинуемой `gcx` v1.5.0 и офлайновый `gcx dev lint run` по файлу артефакта (два idiomatic-предупреждения генератора — `editable`-дашборд и отсутствие описаний панелей — отключены по именам, иначе был бы exit 4), затем upload артефакта. Секретов и деплоя в workflow нет: на стенд дашборд уезжает только вручную (`make grafana-deploy-test` / `make grafana-deploy-staging`), серверная `gcx resources validate` остаётся ручной проверкой (нужны сервер, org-id и креды).

`.github/workflows/release.yml` на тег `v*` запускает тесты и `goreleaser` (архив `linux/amd64`, `SHA256SUMS`, файлы `README.md` и `dagu/*.yaml`). Конфиг — `.goreleaser.yaml`; проверка: `goreleaser check`. В релиз входит только основной бинарник импорта; `cmd/ingest` собирается локально через `make build-ingest`.

## Ограничения и известные проблемы

- **У эндпоинтов bold.report обязателен суффикс `.json`.** Источник — статика на S3 за CloudFront: объекта без расширения там нет, и CDN отвечает `403 AccessDenied` (не `404`). На этом упала первая живая приёмка 2026-10-12: в девяти константах путей `bold/parse.go` суффикса не было, оба импортёра писали `0` строк. Дефект жил с первого коммита контура (`5b2341a`) и был невидим офлайн-тестам: `httptest` отвечает на любой путь. Исправлено коммитом `bold: require .json suffix on api paths`; суффикс закреплён тестом `TestBoldEndpointsPinPaths` — путь без `.json` валит тест. Приёмка после фикса пройдена (`make mcp-check` — ПРОВЕРКА ПРОЙДЕНА): 3138 точек цены, 21054 точки + 73 строки пер-фондового среза; подробности — [docs/GOLD_THESIS_DATA_ROADMAP.md](docs/GOLD_THESIS_DATA_ROADMAP.md) (фаза 3, follow-up 4).
- **LBMA AM/PM не подключён.** Официальный LBMA JSON, Nasdaq Data Link `LBMA/GOLD` и stooq недоступны из этой сети (403 WAF, JS-проверка), FRED серии LBMA удалил. Временно используется `gold` (MOEX GOLDFIXME ÷ курс ЦБ).
- **Курс ЦБ без понедельников и новогодних праздников.** В `cbr_currency_usd` этих дат нет; `gold` берёт последний курс не позже даты и пропускает дни без курса старше 10 дней. Это дыра в источнике ЦБ.
- **Импортёры ЦБ/Росстата/ВТБ берут ссылку со страницы источника.** `cbr_infl_exp`, `cbr_credit_m2x`, `vvp_kvartal`, `salaries_mes`, `rus_vtb_group_ifrs` не хардкодят URL XLSX, а скрейпят актуальную ссылку (colly). Остальные источники ЦБ используют стабильные пути `vfs/statistics/...`.
- **`domrf_mortgage`, `sber_finansovie_rezultaty`, `tbank_group_ifrs` — ссылка задана статически.** Страницы источников закрыты JS-challenge (ServicePipe у ДОМ.РФ, TSPD у Сбера) либо не имеют листинга (CDN с UUID у Т-Банка), colly их не парсит: ссылку обновляют вручную при каждом релизе. У `tbank_group_ifrs` текущая ссылка ведёт на PDF, а не XLSX, — импорт падает.
- **Росстат может отдавать HTTP 426.** Домен `rosstat.gov.ru` периодически отклоняет запросы из-за технических работ (`Upgrade Required`), включая уже работавшие `ipc_mes`/`ipc_weeks`; это ограничение источника/сети, а не кода. Импортёры падают с диагностикой «не найдена ссылка на …».
- **`financial/`.** Legacy-контур (database/sql, свой main). Новые импортёры туда не добавляются; отчётность Полюса вынесена в `polyus/`, в `financial/` остался `databook_ugk` (ЮГК) и мёртвый код металлургов.
- **Поактивный AISC Полюса применяется вручную, а не по расписанию.** Уточнённый сид `sql/dcf_mine_plans_seed.sql` (`make dcf-aisc`) намеренно **не входит в `dagu/dcf.yaml`**: запланированный шаг `make ch-sql` на staging не заработал бы — в релиз попадают только `README.md` и `dagu/*.yaml` (`.goreleaser.yaml`), а `make`, Makefile и сам SQL-файл на хосте отсутствуют. Хуже то, что провал был бы **бесшумным**: `dcf_mine_plans` в той же цепочке пересеял бы ключи из Go-снимка, и `dcf_engine` посчитал бы на старом плоском клине «TCC + $698» с зелёным логом. Поэтому цепочка в `dagu/dcf.yaml` остаётся двухшаговой (`dcf_mine_plans` → `dcf_engine`), а сид AISC применяется человеком: `make ch-sql FILE=sql/dcf_mine_plans_seed.sql` до `DCF_RUN_ID=<uuid> make import STAT=dcf_engine`. Автоматизация (доставка SQL как релизного артефакта + способ его применить) — отдельный пункт роадмапа, вне текущего объёма.
- **Сторож устаревания сида и Python-тесты не покрыты CI.** `TestCommittedSeedIsFresh` (`scripts/extrapolation_aisc_test.py`) сверяет тело коммитируемого `sql/dcf_mine_plans_seed.sql` с тем, что порождает генератор, но для точного воспроизведения ему нужна живая ClickHouse (факт добычи приходит из `databook_polyus`); при недоступной БД тест **пропускается** с явной причиной. Сам Python-набор не запускается в CI: `.github/workflows/go.yml` выполняет только Go-проверки (`gofmt`, `go vet`, `go test -race`, `golangci-lint`, сборку), поэтому правленый вручную сид ловится только локальным прогоном `python3 scripts/extrapolation_aisc_test.py`. Связка Python-тестов с CI — отдельный пункт.
- **AISC Полюса за 2025+ остаётся выведенной величиной.** Per-asset AISC и TCC публикуются только в пресс-релизах 4Q/FY **2018–2021**; начиная с 2022 отчётность перешла на полугодовой формат и даёт поактивный TCC, но AISC — только групповой. Поэтому калибровка клина-доли опирается на 2019–2021, а годы плана 2025–LOM **экстраполируются** с них: сид улучшает структуру затрат (взвешенную по активам вместо единого клина) и переносит в модель наблюдённые веса, но не создаёт поздних измерений. Ровно поэтому `aisc` в каталоге рядов и в [docs/DCF_DATA_COVERAGE.md](docs/DCF_DATA_COVERAGE.md) §2 остаётся `derived`, а увязка к групповому AISC «точна» только на групповом уровне (по построению, независимо от качества входов).
- **Отчётность Полюса: `unitsMarker` привязан к строке, а не к смыслу.** `polyus/columns.go` распознаёт шапку финансовой таблицы только по склеенным меткам `$млн`, `$mln`, `$million`. Отчёт, печатающий `US$ million`, оставит набор колонок пустым, и парсер вернёт **0 записей с `err == nil`** — молчаливо пустую страницу, не отличимую от «в отчёте нет метрик». (Форма `US$ million` — находка контролёра по другим релизам; у включённых релизов 2019–2024 фикстуры несут `$ million`, и оно совпадает, поэтому расширение маркера этой работой не потребовалось.) Это самое важное ограничение парсера Полюса; подробности и остальные — в [ARCHITECTURE.md](ARCHITECTURE.md) §6.6.
- **Отчётность Полюса: остальные ограничения колоночной модели.** `parsePeriod` не принимает форму `FY 2014` (год с пробелом); на МСФО-страницах допущение «первое число = отчётный период» выполняется на обеих текущих страницах, но во время прогона не проверяется, и отчёт со сравнительным столбцом первым молча поменяет периоды местами; счётчик значений вне периодных колонок (178 на приёмке релизов 2019–2024) подтверждён только живым прогоном — сквозного теста на итоговую строку нет; `pageOfLine` возвращает первую ненулевую страницу слова (корректно, т.к. строка всегда принадлежит одной странице, но тест-инварианта нет); словарь `metrics.go` содержит метку `"Earnings per share – basic (US\nDollar)"` со встроенным переводом строки, который не совпадает со склеенным текстом, — на фикстуре FY2024 записей `eps_*` нет. Перечень — ARCHITECTURE.md §6.6.
- **Отчётность Полюса: полоса шапки по ролям строк может расти вниз, а подъём вверх ограничен 8pt.** Вниз полоса продолжается, пока строки играют роль строк шапки, а роль «подпись изменения» опознаётся по словарю меток (`Y-o-Y`, `H-o-H`, `Q-o-Q`, `change`): строка с такой подписью в области данных попадёт в полосу, и её метрики не прочитаются — ни записей, ни нераспределённых значений, тот же тихий ноль, что и у ограничения с `unitsMarker`. Вверх полоса поднимается на строку кварталов («4Q 3Q 4Q» над якорем), но только если разрыв не больше `maxFragmentLiftGap = 8pt` (`polyus/columns.go`): настоящий обрывок дальше 8pt не поднимется, и «4Q 2020» с «3Q 2020» молча схлопнутся в два годовых `2020FY` — значения останутся на месте, сменится только ключ периода, и счётчик нераспределённых этого не покажет. Ловит это `TestKPIPeriodsAreUniquePerMetric`: дубликат ключа внутри страницы валит тест громко. На девяти включённых отчётах оба порога недостижимы (замерено: полосы обрываются на строке данных, настоящие подъёмы — ровно 5.40pt), но 2015–2018, включаемые следующим пунктом, несут подписи изменений в области данных. Остальные ограничения того же списка — отбор строки метрики по первому `Top` и прочее — в ARCHITECTURE.md §6.6.
- **Отчётность Полюса: 5 отчётов 2015–2018 и 2025H2 отключены.** Включены 9 отчётов (2014FY EN KPI, 2019FY…2024FY EN 4Q/FY, RU 1H2026, EN МСФО 1H2026). Причина отключения оставшихся — не выверка страниц, а тип документа: 2015–2017 — MD&A с консолидированной отчётностью, 2018FY — консолидированная МСФО-форма (у них нет шапки «Comparative financial results»), 2025H2 — англоязычный 2H25-релиз с двухшапочной таблицей. Включение 2015–2018 — следующий пункт работ. Снятые ограничения (потеря документа на ключе без измерения источника, недоступность данных агенту) — ниже.
- **Отчётность Полюса: ключ таблицы больше не теряет документ — дефект закрыт 2026-10-10.** Прежний ключ `(company, metric, period)` (таблица `polyus_financial_metrics`) не содержал измерения источника, поэтому два отчёта, печатающие один показатель за один период, конкурировали за одну строку. Среди ключей шести включённых релизов 4Q/FY за 2019–2024 таких **79, и 17 из них несли разные значения** (62 — одинаковые); побеждал первый отчёт в порядке `polyus/pages.go`, потому что импортёр отбрасывал повтор ключа внутри батча (`dedup.add`, 82 отброшенных повтора на приёмке релизов). Показательный случай: `gold_output` за `2023FY` хранился как **2 902** (релиз FY2023), а релиз FY2024 печатал ту же строку как **2 799**. Схема заменена на общую таблицу `company_financials` с ключом `(company, metric, period, source_kind, source_url)` — вид документа И сам документ; смена ключа потребовала новой таблицы, потому что `ORDER BY` — часть идентичности: `CREATE TABLE IF NOT EXISTS` уже созданную таблицу не меняет. **Проверено живым прогоном 2026-10-10:** импорт даёт 347 строк при **265 уникальных** `(company, metric, period)` и `0 duplicate keys skipped`; оба значения `gold_output 2023FY` (2 902 из релиза FY2023, 2 799 из релиза FY2024) видны в `v_company_metric_sources` с разными `source_url`, а `v_company_financials` отдаёт одно разрешённое значение (приоритет `ifrs` > `kpi`, затем `loaded_at`, затем `source_url` — детальный тай-брейк). Обе цифры всегда были прочитаны верно — спорят документы, а не парсер. Схема заложена под металлургов (`CHMF`/`MAGN`/`NLMK`), но наполняет её пока только Полюс: см. [docs/ROADMAP_DCF_METALS.md](docs/ROADMAP_DCF_METALS.md). Legacy-таблица `polyus_financial_metrics` не удалена и **больше не наполняется**: её читает дашборд `dashboard/finance-polyus.json`, перевод дашборда на `v_company_*` — отдельный шаг (в дорожной карте металлургов)
- **Отчётность Полюса: legacy-таблица `polyus_financial_metrics` заморожена.** Импортёр `polyus_financial_metrics` пишет в `company_financials` (двойной записи нет), а одноимённая таблица остаётся как есть — её нельзя переименовать или пересоздать, не сломав дашборд `dashboard/finance-polyus.json`; новых строк в ней не появляется. Поэтому цифры дашборда со временем отстают от витрин. Агент MCP эту таблицу не видит (сырая таблица, гранта нет) — и это правильно.
- **Отчётность Полюса доступна агенту MCP — ограничение снято 2026-10-10.** Витрины `v_company_financials`, `v_company_metric_sources`, `v_company_operating` созданы с комментариями и грантом `kimi_reader` (`sql/mcp_kimi_reader.sql`), ряды Полюса описаны в `series_catalog` (source `polyus`, 18 рядов; `polyus_datapack`, 19 рядов), поэтому агент читает производство, TCC, AISC, capex, выручку и операционку по активам через MCP. Сырые `databook_polyus`, `company_financials` и legacy `polyus_financial_metrics` агенту по-прежнему не выданы. Проверено живым `make mcp-check` 2026-10-10 (ПРОВЕРКА ПРОЙДЕНА). Витрины заведены общими для сектора, а не набором `v_polyus_*`.
- **Ingest-endpoint: TLS — снаружи.** Лимит частоты (`INGEST_RATE_PER_MIN`, `429`) и предел тела 1 MiB (`413`) реализованы в процессе; TLS предполагается на reverse proxy, в репозитории он не описан. Повторный `POST /v1/model_run` с тем же `run_id` отвечает `200 {"inserted": 1}`, хотя строк не пишет: ответ не отличает дубль от новой записи.
- **HTTP-режим mcp-clickhouse развёрнут на staging (2026-10-10).** Сервис `deploy/staging/mcp-clickhouse.service` слушает `127.0.0.1:8000`, требует Bearer-токен (`CLICKHOUSE_MCP_AUTH_TOKEN`, обязателен), читает витрины от пользователя `kimi_reader`. Локальная конфигурация Kimi остаётся stdio (блок «MCP для Kimi»). Наружу эндпоинт не опубликован и TLS не настроен — доступ через SSH-туннель; публикация для плагина `gold-nav` требует reverse proxy и отдельного шага.
- **Индикаторы `v_gold_dashboard`: `distillate_stocks` автоматизирован, остальные — вручную.** `distillate_stocks` пополняет импортёр `eia` (EIA API v2, расписание — среда 19:43 МСК); `crack_ulsd_proxy`, `fedwatch_dec_hike`, `etf_flows_month`, `dxy` импортёров не имеют — только `manual_series`, без ввода показываются как `stale`. Витрина берёт значение индикатора по приоритету источников `eia > webbridge > manual` (автоматический источник канонический, ручной ввод — фолбэк). Цена COMEX (`gold_prices.venue = 'comex_front'`, `POST /v1/gold_prices`) и резервы ЦБ (`source = 'imf'`, витрина `v_imf_macro`) — тоже сессионные ряды без импортёра: без сессии они не пополняются (исторический хвост — разовый backfill). Структурные ряды WGC ETF Flows (`etf_<метрика>_<регион>`, 20 рядов) — отдельный набор: читается витриной `v_gold_etf_flows`, а не `v_gold_dashboard`; канонический источник — импортёр `wgc_etf_flows` (`source = 'wgc'`), а сессионный ввод `POST /v1/manual_series` (`source = 'webbridge'`) и ручной (`manual`) остаются фолбэками (приоритет `wgc > webbridge > manual`); мировой `etf_flow_usd_mln_world` (млн USD) — не путать и не складывать с ручным `etf_flows_month` (млрд USD).
- **Выгрузки WGC: ETF — из API, ЦБ — в `wgc/data/` вручную.** ETF-канал читает JSON API `fscdn.gold.org`, а локальная выгрузка `ETF_Flows_*.xlsx` в `wgc/data/` — только фолбэк: при недоступности API в лог идёт причина перехода, и лишь затем читается файл. Выгрузки ЦБ скачать можно только браузером (goldhub отдаёт `/download/file/*` залогированному браузеру, боту — `403`), поэтому `wgc_cb_reserves` читает локальную папку, а не сеть; на staging файлы кладутся на хост вручную (как сиды). Excel-выгрузки — только месячные (недельные апдейты существуют только на сайте — вне объёма), данные ЦБ публикуются с лагом ~2 мес. (выгрузка «Updated October 2026» несёт данные до 31.08.2026). Ни API и ни файла ETF, равно как и отсутствие выгрузок ЦБ, — `log.Warn` и `0` строк; битый файл или пустой разбор — ошибка. Тоннажные стоки ETF (`holdings_tonnes`) API отдаёт округлёнными до 0,01 т (потоки и AUM — полной точности), поэтому расхождение с xlsx-фолбэком в этих пределах — округление источника, а не дефект импорта. Оговорки данных: у RUS в файле пропущены месяцы 2025-12…2026-03 (сумма файла −34,2 т против блоговой оценки WGC −56 т — значения пишутся как в файле), TUR — net-ряд `Turkey` (gross `Turkey*` не импортируется), UZB — точки с 2013-02, хотя комментарий файла говорит «Data first available in January 2018» (данным не соответствует).
- **Сверка прогнозов автоматизирована, кроме `nav`.** `actual` и `error_pct` в `forecast_log` заполняет импортёр `forecast_resolve` (пакет `forecast/`, `dagu/forecast.yaml`, ежедневно 20:23 МСК): окно сверки — квартал, содержащий `target_date`, факт — среднее `gold_prices` по venue `comex_front` (фолбэк `moex_fix_usd`). Строка остаётся `is_resolved = 0`, пока `target_date` в будущем или в окне нет точек цены (так Q4-прогноз с `target_date = 2026-12-31` резолвится только после этой даты), а `metric = 'nav'` не резолвится вовсе — факта (NAV на дату) нет, `actual` остаётся NULL.
- **ИПЦ в `v_model_inputs` может быть пустым.** Витрина требует, чтобы существовали таблицы `ipc_mes` и `ipc_weeks`, но не проверяет их заполненность. Таблицы создаются до загрузки Росстата, поэтому при его ошибке (см. выше) витрина создаётся, а `ipc_mes_last` и `ipc_week_ytd` остаются NULL или устаревают; ориентироваться по `*_date`.
- **`nav_beta_gold` не заполняется** (поля нет во входе `ModelRun`), в таблицу пишется NULL.
- **Имена импортёров.** `Name()` у `fred` — `fred` (несколько серий, одна таблица `macro_series`), у `cbr` часть импортёров делит таблицу (`cbr_currency_usd`, `households_b_mes`).
- **Таблица `gold_prices` использует `Date32`.** Так как LBMA-ряд начинается в 1968 году.
- **Доходности G-curve (`ofz_curve`, теноры 1y/3y/5y/10y) — без ретроспективы.** Эндпоинт MOEX ISS `/iss/engines/stock/zcyc.json` отдаёт только снимок текущего дня; история накапливается с даты первого запуска импортёра. Индекс RGBI (тот же zcyc + свечи) имеет полную историю с 2010 года.
- **Дашборд `gold-thesis`: провижининг `datasources` применяется только при старте Grafana.** Проверено на staging (Grafana 13.1.1): каталог `provisioning/datasources` читается один раз при старте сервера (поллинг есть только у дашбордов), поэтому после `make grafana-provision-staging` новый датасорс `ch-rosstat` не появляется, пока владелец не сделает admin-reload (`POST /api/admin/provisioning/datasources/reload`) или не перезапустит `grafana-server`; до этого smoke упадёт с «datasource not found». Цель сама сервис не рестартует и печатает готовую команду reload. Токены стендов (`GRAFANA_TEST_TOKEN`/`GRAFANA_STAGING_TOKEN`) и полный staging-прогон — owner-assisted: сервис-аккаунты Editor и токены создаются вручную (см. «Grafana»).
- **`news_watch`: `sentiment` в `news_events` пока всегда NULL.** Тональность событий будет заполнять LLM-сессия, но канал записи для неё в ingest-endpoint не реализован (план — `POST /v1/news_events`, follow-up), поэтому витрина `v_news_events` отдаёт `sentiment` пустым: NULL означает «не размечено», а не «нейтрально».
- **`news_watch`: история событий копится с первого прогона, и простой дольше окна ленты теряет её позиции безвозвратно.** RSS отдаёт только текущее окно ленты, а окна у лент разные: Интерфакс 25 позиций, РБК 30 и TASS 100 покрывают ~1–2 часа публикаций, а фиды ЦБ (по 100 позиций) — дни (`cbr_news` ≈12 дней, `cbr_press` ≈83 дня публикаций). Глубокой ретроспективы у источников нет, поэтому позиции, выпавшие из окна за время простоя, не восстанавливаются — ни задним числом, ни повторным прогоном. `NEWS_WATCH_MODE=check` покажет дыру лишь пока следующая успешная запись её не «залечила», и только для тех лент, окно которых уже прошло мимо сохранённого максимума.
- **`news_watch`, режим `check`: дыра покрытия видна только до «залечивания» следующей успешной записью.** Пока сохранённый максимум источника старше начала окна его ленты, `check` показывает дыру (и это ошибка прогона); после следующей успешной записи окно сдвигается вперёд, и дыру `check` больше не покажет, хотя пропущенные позиции так и не собраны. Диагностика ручная: автопрогона `check` в `dagu/news.yaml` нет (там режим `since`), запускать нужно самому — `NEWS_WATCH_MODE=check make import STAT=news_watch`.
- **Витрина `v_news_events` фильтрует по широкому списку стемов тезисов.** Фильтр (`newsFilter`, RE2, `(?i)`) намеренно широкий: ложное срабатывание дешевле ложного пропуска, но полностью исключить пропуск события тезиса он не может — заголовок без слова из списка в витрину не попадёт. Лишнее снимает запрос сессии, а сам список уточняется пересозданием витрины: полный поток лежит в сырой `news_events`, поэтому сужение фильтра историю не теряет.

## Grafana

Установка Grafana 11.2.0 (Debian/Ubuntu, arm64):

```bash
sudo apt-get install -y adduser libfontconfig1 musl
wget https://dl.grafana.com/oss/release/grafana_11.2.0_arm64.deb
sudo dpkg -i grafana_11.2.0_arm64.deb
```

Дашборд статьи **`gold-thesis`** живёт как код, а не как экспорт из UI. Конвейер:

```text
grafana/ (Go, grafana-foundation-sdk) --make grafana-build--> dashboard/gold-thesis.json (коммитится)
        |                                                              |
   make grafana-validate                              make grafana-deploy-test / -staging (gcx resources push)
   (go vet + тесты)                                                |
   CI dashboards.yml: тесты, сборка,                           make grafana-check
   diff-проверка артефакта, gcx dev lint                  (smoke: uid + точки панелей)
```

Легаси-экспорты `dashboard/rosstat.json`, `dashboard/chmf.json`, `dashboard/finance-polyus.json` остаются как есть — их миграция на код, алерты Grafana и PNG-рендер панелей для статьи вне объёма этой работы.

### Дашборд как код

- Источник истины — `grafana/` (отдельный Go-модуль): 9 панелей (id 1–9) в 5 рядах, все запросы — только по витринам `v_*`, датасорс каждой панели — константа uid `ch-rosstat`. Артефакт `dashboard/gold-thesis.json` (k8s-ресурс `dashboard.grafana.app/v1beta1`, uid `gold-thesis`) руками не правится: `make grafana-build` перезаписывает его детерминированно, а CI падает, если артефакт разошёлся с кодом.
- `make grafana-validate` — `go vet` + тесты модуля (состав панелей и негативные правила `Validate`). Серверный `gcx resources validate` намеренно не входит в цель: он проверяет ресурс на живом инстансе (server-side dry-run) и требует кредов — рабочая форма записана в комментарии цели.
- Локальный стенд: `make grafana-up` (brew-Grafana; данные, логи и плагины в `$(GF_DIR)`, провижининг датасорса — `grafana/provisioning/datasources/clickhouse.yaml`, порт 3000) и `make grafana-down`. При первом входе в UI смените пароль `admin/admin`.

### Переменные окружения стендов

| Переменная | Назначение |
|---|---|
| `GRAFANA_TEST_URL` | адрес тест-стенда, по умолчанию `http://localhost:3000` |
| `GRAFANA_TEST_TOKEN` | токен сервис-аккаунта **Editor** на тест-стенде: `make grafana-deploy-test`, `make grafana-check` (в `gcx` уходит как `GRAFANA_TOKEN` — make-цели подставляют его сами; при ручных вызовах `gcx` подставляйте явно, см. «Цикл разработки и деплой») |
| `GRAFANA_STAGING_URL` | адрес staging, по умолчанию `http://localhost:3001` — **только через ssh-туннель** |
| `GRAFANA_STAGING_TOKEN` | токен сервис-аккаунта **Editor** на staging: `make grafana-deploy-staging` |
| `GRAFANA_ORG_ID` | org-id инстанса для `gcx` (on-prem без него отвергается), по умолчанию `1` |

Значения — только в `~/.config/rosstat/env`; `make env-check` печатает «задан/не задан» без значений. Версии: `gcx` 1.5.0 (`brew install gcx`), локальный стенд — Grafana 13.2.3 + плагин `grafana-clickhouse-datasource` 4.3.2 (ставится `make grafana-up`), staging — Grafana 13.1.1.

### Одноразовая настройка (владелец)

1. `brew install gcx`.
2. Локальный стенд: `make grafana-up` → войти `admin/admin` (первый запуск) → **Administration → Users and access → Service accounts** → аккаунт с ролью **Editor** → **Add service account token** → значение в `~/.config/rosstat/env` как `GRAFANA_TEST_TOKEN`; то же одной командой — `make grafana-test-token` (создаёт аккаунт `dashboards-cd` роли Editor и печатает строку для env-файла).
3. Staging: то же в UI staging (через туннель) → токен как `GRAFANA_STAGING_TOKEN`.

Токены не попадают в репозиторий, CI и логи (`gcx` получает их из окружения, а не аргументом командной строки).

### Цикл разработки и деплой

```bash
make ch-up && make grafana-up           # ClickHouse + локальный стенд с провижинингом датасорса
make grafana-build && make grafana-validate
make grafana-deploy-test                # gcx resources push на $(GRAFANA_TEST_URL) + smoke
make grafana-check                      # только smoke
```

Push идемпотентен: повторный прогон обновляет тот же ресурс (`metadata.name=gold-thesis`), дубль не создаётся. Перед публикацией — серверная проверка ресурса на живом инстансе (шаг ручной, нужны креды):

```bash
set -a; . ~/.config/rosstat/env; set +a             # источник значений; ничего не печатает
# если в env-файле нет адреса/токена — подставьте их: GRAFANA_TEST_URL=http://localhost:3000, GRAFANA_TEST_TOKEN=<токен Editor>
GRAFANA_SERVER="$GRAFANA_TEST_URL" GRAFANA_TOKEN="$GRAFANA_TEST_TOKEN" GRAFANA_ORG_ID=1 \
  gcx resources validate dashboards --path dashboard/gold-thesis.json   # пример вывода: ✔ No errors found
```

**Ручные вызовы `gcx`.** `gcx` читает креды только из `GRAFANA_SERVER`/`GRAFANA_TOKEN`/`GRAFANA_ORG_ID`; переменную `GRAFANA_TEST_TOKEN` он не знает — make-цели подставляют её в `GRAFANA_TOKEN` сами. Запуская `gcx` руками, подставляйте `GRAFANA_TOKEN` явно (как в примере выше), иначе push падает с `401 Unauthorized: the server has asked for the client to provide credentials` — это не признак недействительного токена.

Staging — через туннель `:3001` (порт не конфликтует с локальным стендом `:3000`):

```bash
ssh -N -L 3001:127.0.0.1:3000 palmshell &
make grafana-deploy-staging             # провижининг датасорса → push дашборда → smoke
```

**Ловушка staging:** Grafana (проверено на 13.1.1) применяет `provisioning/datasources` **только при старте** — сразу после первого `make grafana-provision-staging` датасорса `ch-rosstat` в инстансе ещё нет, и smoke упадёт. Активировать без рестарта сервиса:

```bash
curl -u admin:<пароль> -X POST http://localhost:3001/api/admin/provisioning/datasources/reload
```

(либо один `systemctl restart grafana-server`). Цель Grafana не перезапускает и сама печатает эту команду; подробности — `deploy/staging/README.md`.

### Проверки и приёмка (2026-10-12)

**Проверено на стенде/локально (пункты с пометкой «по отчёту исполнителя» приводятся со слов исполнителя задачи и при финальном ревью повторно не проверялись):**

- **Grafana-smoke на локальном стенде (13.2.3).** Дашборд виден по uid, панель 1 «Цена золота и пол $4 000» вернула **250 точек** — на тот момент токена сервис-аккаунта у владельца ещё не было, и запрос шёл через Grafana (`/api/ds/query`) временной обёрткой с basic-auth стенда.
- **SQL-проверка через MCP — отдельная проверка, не то же самое.** То же окно дашборда (`now-1y`) в `v_gold_prices` под `kimi_reader` через MCP даёт `SELECT count()` = **250** (250 — скользящее окно; за календарный 2025 — 252) — Grafana и агент читают одни данные, но это два разных канала (Grafana-запрос и MCP-запрос).
- `make grafana-build` — детерминирован (повторный прогон — пустой `git diff`); `make grafana-validate`, `make all`, `make mcp-check` — зелёные **по отчёту исполнителя задачи; повторный прогон при финальном ревью ветки не выполнялся**.
- `gcx resources push` идемпотентен (повторный push не создаёт дубль, растёт только `version`); серверная `gcx resources validate` на стенде отдала `✔ No errors found` — **по отчёту исполнителя задачи**.
- Провижининг датасорса применён на staging и идемпотентен: повторный прогон печатает «актуален», sha256/mtime/inode не меняются; в репозитории лежит только шаблон с плейсхолдером, пароль подставляется на хосте — **по отчёту исполнителя; полный live-прогон с токеном — owner-assisted**.
- **Живой прогон цели с Bearer-токеном (после того, как владелец задал `GRAFANA_TEST_TOKEN`).** `make grafana-deploy-test` — `✔ 1 resources pushed, 0 errors`; smoke `make grafana-check` — «ПРОВЕРКА ПРОЙДЕНА»: дашборд виден (версия 8), 9 панелей с таргетами, панель 1 «Цена золота и пол $4 000» — 250+6 точек, панель 2 «Золото vs DXY» — 250+1.
- **CI на PR #41 — зелёный:** workflow `dashboards` (run 38229317402) и `Go` (run 38229317450); офлайн `gcx dev lint run` и diff-шаг артефакта в CI прошли.

**Ещё не подтверждено (ожидает владельца):**

- Полный staging-прогон `make grafana-deploy-staging` (туннель `:3001` → провижининг датасорса → push → smoke): `GRAFANA_STAGING_TOKEN` задан, туннель ещё не поднимался; после первого провижининга нужен один admin-reload датасорса (см. «Ловушка staging»).
- Визуальный просмотр дашборда в браузере (`http://localhost:3000`).
