# Спека: группа источников контура ключевой ставки и runbook публикационного прогона

**Дата:** 21 октября 2026 г.
**Пункт:** `infra/5.6` ([issue #332](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/332))
**Роль:** go (Makefile; Go-код и схема не меняются)

## 1. Задача

Перед публикацией по ключевой ставке нужно импортировать все источники контура: сегодня это либо длинный список `CLICKHOUSE_IMPORT_STAT=a,b,c,…`, либо десятки отдельных `make import STAT=<имя>`. Ошибка в одном имени видна только по строке `no importer named` в логе, а состав «что относится к ставке» держится в голове и в трёх документах (`KEY_RATE_PUBLICATIONS.md` §6, `KEY_RATE_CHANNELS.md` §8/§8.1, `dagu/*.yaml`). Кроме того, порядок запуска моделей, журнала и витрины нигде не собран командами: он описан прозой в §7 контракта публикации.

Цель пункта — одна команда импорта всех источников контура и runbook с исполняемыми командами остальных шагов.

## 2. Решение

### 2.1. Группа Makefile

`main.go` уже принимает **список** имён через запятую (`parseImportFilter` делит по запятой и срезает пробелы), поэтому группе не нужен Go-код: достаточно переменной `STATS_<имя>` и раскрытия в цели `import`.

```make
STATS_key_rate_sources := <50 имён через запятую>
unexport STATS_key_rate_sources
import:
	CLICKHOUSE_IMPORT_STAT="$(or $(STATS_$(STAT)),$(STAT))" ./$(BUILD_DIR)/$(BINARY)-native
```

- Обычное имя импортёра идёт как раньше (регрессия `make import STAT=bea`).
- `unexport` обязателен: в Makefile стоит голый `export`, иначе имя группы с запятыми уехало бы в окружение дочернего процесса.
- `make archive-restore` не меняется: восстановление обязано оставаться на один импортёр.
- Новая группа в будущем — новая переменная `STATS_<имя>` без правок рецепта.
- Порядок исполнения — регистрация в `chimport.Stats`, а не порядок списка; прогон идёт одним процессом и одним соединением.

**Имя.** `key_rate_sources`, а не `key_chanels`: «каналы» в контуре — это 11 каналов ЦБ (`KEY_RATE_CHANNELS.md`), другое понятие; `key_rate_views` — уже существующий импортёр, поэтому имя группы не должно выглядеть как имя импортёра.

### 2.2. Состав группы (50 имён)

Выведен из контракта публикации (§6) и карты каналов (§8/§8.1), сверен с `dagu/*.yaml`:

| Домен | Число | Импортёры |
|---|---|---|
| `cbr` | 26 | `cbr_key_rate`, `cbr_currency_usd`, `cbr_m2`, `cbr_credit_m2x`, `cbr_loans_to_corporations`, `cbr_loans_to_individuals`, `cbr_households_b_mes`, `cbr_infl_exp`, `cbr_bank_int_rate`, `cbr_int_rate_loans`, `cbr_ruania`, `cbr_indicators_cpd`, `cbr_zcyc`, `cbr_mp_survey`, `cbr_mo_br`, `cbr_mp_summary`, `cbr_bop_est`, `cbr_bop_standard`, `cbr_dip_bulletin`, `cbr_mt_forecast`, `cbr_form101`, `cbr_ccyb_indicators`, `cbr_risk_weight_addons`, `cbr_mpl`, `cbr_mp_bsu_map`, `cbr_publication_calendar` |
| `rosstat` | 7 | `rosstat_ipc_mes`, `rosstat_ipc_weeks`, `rosstat_bi_cpi_weekly`, `rosstat_ipc_weeks_bulletins`, `rosstat_labour_force_mes`, `rosstat_salaries_mes`, `rosstat_vvp_kvartal` |
| `minfin` | 6 | `minfin_fed_bud_mes`, `minfin_fed_bud_mesyats`, `minfin_oil_gas_mes`, `minfin_debt_stock`, `minfin_debt_service`, `minfin_ofz_auction` |
| `moex` | 3 | `moex_market`, `keyrate_futures`, `ofz_curve` |
| `bank` | 4 | `sber_consumper_spending_index`, `sber_izmenenie_trat`, `bank_company_metrics`, `domrf_mortgage` |
| `fedstat` | 2 | `fedstat_weekly_prices`, `fedstat_ipc` |
| `news`/`calendar` | 2 | `news_watch`, `events_calendar` |

**Вне группы осознанно:** золото и DCF (`cbr_gold`, `gold_*`, `dcf_*`), ИЦП и прочие блоки ЕМИСС (`rosstat_ppi_*`, `ppi_sa`, `fedstat_ipp`, `fedstat_retail`, `fedstat_agri`, `fedstat_investment` — «вне модели», §8.1), `stock_prices`, `customs_vneshn_torg`, `edisclosure_documents`, `company_views`, `key_rate_views` (читает `forecast_log` — её место после записи в журнал), `forecast_resolve` (пост-заседание), `ml_registry` (schema-only).

### 2.3. Runbook `docs/publications/README.md`

Рабочий сценарий, только `make`-команды (ключи подключает Makefile из `~/.config/rosstat/env`):

1. проверки окружения (`env-check`, `ch-status`, `mlflow-status`, `run-ingest`, LLM-ключи, `pdftotext`);
2. `make import STAT=key_rate_sources` — состав, поведение, признаки ошибок;
3. модели ИПЦ (`cpi-nowcast`, `cpi-monthly`, `cpi-live3-horizons`) и артефакты;
4. модели ставки (`key-rate-model`, `key-rate-scenario-fan-v3`, `key-rate-llm-forecast`);
5. журнал (`key-rate-forecast-log`, `ml-live-check`);
6. витрина и графики (`key_rate_views` → `ml-live-check`, `grafana-build`, `grafana-validate`, `grafana-check`);
7. сборка текста по шаблону;
8. copy-paste блок, экспериментальные треки, таблица частых сбоев.

Порядок «журнал → витрина → `ml-live-check`» обязателен: `v_ml_live_accuracy` создаёт `key_rate_views`, поэтому проверка журнала идёт после неё.

## 3. Синхронизация документации

- `README.md`: строка в таблице документации и уточнение команды `make import STAT=<имя|группа>`.
- `docs/site-pages.json`: страница `publications-runbook.md` в секции «Контур ключевой ставки».
- `docs/KEY_RATE_PUBLICATIONS.md` не правится (в рабочем дереве параллельная работа); ссылка на runbook — из README и docs-портала.
- `ARCHITECTURE.md` не меняется: контракт `CLICKHOUSE_IMPORT_STAT` (список через запятую) уже описан; новых импортёров, таблиц, DDL, DAG и env-переменных нет.

## 4. Критерий приёмки

1. `make -n import STAT=key_rate_sources` — в рецепте развёрнуты 50 имён.
2. `make -n import STAT=bea` — группа не подменяет обычное имя.
3. `make import STAT=key_rate_sources` на dev-ClickHouse — все 50 имён отработали, строк `no importer named` — 0; сбои источников называются в протоколе и не выдаются за успех.
4. `make docs-check`, `make all` — зелёные.
5. `make import STAT=key_rate_views` после ingest — витрины созданы; `make ml-live-check` — проходит.

## 5. Риски

- **Долгий прогон при мёртвом источнике.** HTTP-слой повторяет запрос с backoff 30 с → 1 → 2 → 5 мин; runbook называет это явно, прогон не останавливается.
- **Красный итог при сбое одного источника.** `main.go` продолжает цикл и возвращает 1; это штатное поведение группы и описано в runbook.
- **Устаревание состава.** Группа — список в Makefile; переименование импортёра даёт `no importer named`, что и является сигналом обновить список (критерий приёмки).
- **Ограничение контракта.** `CLICKHOUSE_IMPORT_STAT=key_rate_sources` без `make` не работает — так и написано в runbook.
