# ML-issue в clickhouse-import-rosstat: ревью, сверка с практиками и рекомендации (2026-10-19)

> **Объект:** все issue, найденные поиском `ML` в GitHub-репозитории `kmlebedev/clickhouse-import-rosstat`: 14 штук, из них 7 закрытых и 7 открытых.
> **Задача:** обобщить, что сделано по закрытым ML-issue; сверить результат с лучшими практиками, применимыми к этому репозиторию; дать рекомендации ML-инженерам и агентам по открытым задачам; обосновать, где хранить процесс — в `AGENTS.md` или в отдельном скиле.
> **Метод:** чтение тел и комментариев issue, роадмапов `key-rate`/`infra`/`gold-thesis`, `docs/guides/stat-ml-models.md`, `docs/research/2026-10-18-ml-experiment-management.md`, `docs/KEY_RATE_MODEL.md`, спек/планов и протоколов приёмки. Внешние практики — по устойчивой литературе и официальной документации инструментов (ссылки в §9).
> **Границы:** это обзор и рекомендации, не реализация. Числа взяты из issue/роадмапов на 2026-10-19; staging MLflow и часть внешних источников в сессии не проверялись.

---

## 1. Короткий вывод

ML-контур репозитория методологически зрелый: эксперименты предрегистрируются, идут walk-forward с purge/embargo, сравниваются DM-HLN и блочным бутстрапом, артефакты детерминированы, а прогоны имеют паспорт и реестр. Закрытые issue подтверждают это на CPI-nowcast, паритете с CB_COMP, MLflow и LLM-треке.

Главный разрыв — **эксплуатационный**: промоушен и live-мониторинг (`infra/4.3`, #184) не начаты. Пока нет gate, shadow/canary, live-ошибки в витрине и алертов, `champion` в MLflow — это alias, а не deployment. Второй разрыв — **ролевой**: процесс ML-инженера не формализован, поэтому знания живут в отдельных документах и легко теряются при передаче задачи другому агенту.

**Рекомендация:** отдельный скил `.kimi-code/skills/ml-engineer/SKILL.md` + короткий маршрутизирующий блок в `AGENTS.md`. Методологию в `AGENTS.md` не переносить: файл читается всеми сессиями, включая Go-задачи.

## 2. Закрытые issue: что сделано

| Issue | Пункт | Результат | Урок |
|---|---|---|---|
| [#123](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/123) | `key-rate/phase-4-ml-v2` | `scripts/cpi_monthly.py`: месячный nowcast ИПЦ с признаками мониторинга предприятий. Short — 155 мес. (2013-10…2026-08), long — 292 мес. (2002-05…2026-08); 4 CatBoost-варианта (direct/bottom-up × с МП/без МП) + B1–B3; DM-HLN 9 primary-пар. Знак в пользу МП везде, значимости на 5 % нет: лучшая `catboost_bottomup_mp` −0.020 п.п., p=0.083 (main), −0.023, p=0.065 (embargo 1). Long: bottom-up p=0.0183 только в robustness-окне embargo, в main p=0.1402. Вывод — «направление без устойчивой значимости», в ансамбль трек не входит | Незначимый результат — допустимый и публикуемый. Нельзя выбирать удобное окно и нельзя выдавать одно p<0.05 из robustness за основной вывод |
| [#190](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/190) | `key-rate/phase-4-ml-cb-comp-parity` | Строгое воспроизведение CB_COMP: target `TARGET_FULL`, train 2010-03…2018-12, test 2019-01…2024-07, origin `M−(h−1)`, исключения как в статье. Direct значимо хуже на h1–h3 (p=0.0048/0.0134/0.0055), h6 — p=0.0643; bottom-up не значимо отличается на h1–h3 (p=0.185/0.117/0.101), h6 значимо хуже (p=0.0458); TOST при δ=10 % RMSE CB_COMP не пройден. Вердикт — **«не близок»** | Выравнивание forecast origin меняет числа радикально: нестрогий `M−1` в первом прогоне давал ложное преимущество. Паритет без DM/TOST не заявляется; «не близко» — допустимый результат |
| [#178](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/178) | `infra/4.1` | `ml_runs`/`ml_predictions`, Go-импортёр `ml_registry`, витрины `v_ml_runs`, `v_ml_leaderboard`, `v_ml_error_by_fold`; Python-хелпер `scripts/ml_experiment.py`; `ML_LOG=1` пишет git SHA, config/sample hash, метрики, fold-предсказания и артефакты. Сырые таблицы закрыты от `kimi_reader`; повтор с тем же конфигом даёт те же метрики и sha256, но новый `run_id` | Паспорт run и fold-предсказания — обязательная часть эксперимента, а не отчётность. История неудачных прогонов тоже сохраняется |
| [#183](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/183) | `infra/4.2` | Self-hosted MLflow: SQLite backend + локальные артефакты; экспорт в ClickHouse идемпотентен; model registry с aliases `champion`/`challenger`; `make mlflow-up/down/export/register/promote/models`. ClickHouse как backend MLflow не поддерживается | Control plane (MLflow) и analytics plane (ClickHouse) — разные роли. Staging SSH был недоступен, поэтому staging-приёмка осталась владельцу; это надо честно писать, а не считать проверенным |
| [#189](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/189) | `key-rate/phase-4-llm-track` | `scripts/key_rate_llm_forecast.py`: as-of снимок витрин, фиксированный `prompt_version`, `temperature=0`, 3 повтора, медиана и разброс; JSON `{probabilities, expected_step_bp, rationale, risks}`. Живой прогон: cut/hold/hike 12/83/5 %, expected_step −5 б.п., 3/3 валидных повтора. В ClickHouse не пишет; `--log` готовит payload для #127 | LLM — экспериментальный cross-check, а не доказательство превосходства. Промпт не тюнится по результату заседания; сравнение с статистикой/ML — только через DM/encompassing в публикации |
| [#127](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/127) | `key-rate/phase-4-forecast-log` | `forecast_log`: `error_bp`, `probabilities`, `model`, `data_version`, `prompt_version`; endpoint `POST /v1/key_rate_forecast`; треки различаются суффиксом `metric` (`cbr_rate:taylor`, `:ordered_logit`, `:ml`, `:llm`); резолвер `cbr_rate` пишет факт после заседания и `error_bp=(predicted−actual)×100`, `error_pct=NULL` | Live-прогноз пишется до публикации. Ошибка ставки — в базисных пунктах, не в процентах: относительная ошибка несопоставима между режимами ставки |
| [#197](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/197) | `infra/4.4` | `--quick` (только main-окно) и `--jobs N` (`ProcessPoolExecutor`, `thread_count=1`). Long quick 20:34 против 89:29 полного (4.35×), short quick 12:33; повтор даёт те же sha256; `--jobs 1` = `--jobs N`; полный прогон без флагов не изменился (43:58, артефакты побайтно прежние) | Быстрый режим — только для итераций. Quick-числа неофициальные и должны быть помечены в паспорте (`config.quick=true`), иначе их начнут сравнивать с полными |

## 3. Что подтверждено как лучшая практика

| Практика | Как проявляется в репозитории |
|---|---|
| Предрегистрация | Спека и план до кода; признаки, бенчмарки, фолды и метрики фиксируются до результата (#123, #190) |
| Лестница бенчмарков | B1 last value, B2 AR(1)+дамми, B3 ElasticNetCV; ML входит в ансамбль только при значимом выигрыше над ними |
| Walk-forward + purge/embargo | Окна 24/48, embargo 1, режимные блоки; случайный split запрещён |
| Статистическая значимость | DM-HLN, блочный бутстрап, MZ/PT, калибровка вероятностей; сравнение на общих фолдах |
| Детерминизм | Фиксированный seed, `config_hash`/`sample_hash`, sha256 артефактов, повторные прогоны байт-в-байт |
| Паспорт run | `run_id`, git SHA, конфиг, данные, окружение, метрики, артефакты, решение |
| Реестр моделей | MLflow + ClickHouse-реплика; aliases `champion`/`challenger` |
| Публикация as-is | Незначимые и отрицательные результаты не прячутся (#123, #190) |
| Разделение слоёв | `forecast_log` — live-журнал; `ml_predictions` — fold-бэктест; не смешивать |
| Явные ограничения | В отчётах есть «что не проверено»: PIT, винтажи, staging, покрытие весов |

## 4. Разрывы с лучшими практиками

| Практика | Состояние | Разрыв / что делать |
|---|---|---|
| Промоушен и deployment | `champion` alias в MLflow есть, gate нет | #184: gate по DM/MCS, shadow/canary, rollback; alias ≠ deployment |
| Live-мониторинг | `forecast_log` и резолвер есть; live-ошибка в витрине и алерты — нет | #184: `v_ml_live_accuracy`, rolling error, drift, freshness, Grafana alerts |
| PIT и винтажи | Для `cbr_dip_bulletin` есть `v_point_in_time*` (#126); для CPI/ML-признаков as-of — приближение | Отдельный пункт PIT для CPI/МП; в отчётах явно называть приближение |
| Multiple comparisons | DM-HLN есть; SPA/MCS не применяются | При росте числа спецификаций (>5–7) — `arch` SPA/MCS, иначе data snooping |
| Model card / decision record | Паспорт run есть, отдельного стандарта model card нет | Добавить в паспорт гипотезу, ожидаемый эффект, решение и причину; не хранить только в PR |
| Stop rule / бюджет | Формально нет | Бюджет экспериментов на квартал, MCS-остановка, плато; повторный reject — только с новой гипотезой |
| Окружение | `uv run --with` с плавающими версиями | Lock/freeze для официальных прогонов; фиксировать версии в паспорте |
| Границы ML/Go | Не формализованы | Этот скил: ML-агент не делает ad-hoc DDL; DDL/витрины/гранты — Go-импортёр по `IMPORTER_PACKAGE_STANDARD.md` |
| Gold ML | C1–C4 (#154–#157) не начаты, статусов в роадмапе нет | C1 первым, с предрегистрацией и переиспользованием CPI-протокола |

## 5. Открытые issue и рекомендуемая очередь

| Приоритет | Issue | Пункт | Состояние / следующий шаг |
|---|---|---|---|
| 1 | [#184](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/184) | `infra/4.3` | Не начато; зависимости #178/#127 закрыты. Спека: gate, shadow/canary, `v_ml_live_accuracy`, алерты. Это критический путь к эксплуатации |
| 2 | [#191](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/191) | `key-rate/phase-4-first-publication` | Статья влита (PR #199); осталось: production-строки `forecast_log`, свежий снимок недельного ИПЦ/Сбера, публикация поста, ссылка в issue, пост-апдейт после 23.10 |
| 3 | [#154](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/154) | `gold-thesis/phase-6-catboost-c1` | Обучающая матрица: выгрузка признаков из витрин + таргет `gold_prices`, walk-forward без look-ahead, дата-срез в метаданных. Первый шаг gold ML |
| 4 | [#155](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/155) | `gold-thesis/phase-6-catboost-c2` | In-DB базлайн `stochasticLinearRegression`; зависит от C1 |
| 5 | [#156](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/156) | `gold-thesis/phase-6-catboost-c3` | CatBoost-трек: Python-обучение, прогнозы в БД по dagu, версионирование модели; зависит от C1 |
| 6 | [#157](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/157) | `gold-thesis/phase-6-catboost-c4` | DM-тест CatBoost vs эксперт vs in-DB; решение о роли ML-трека; зависит от C2/C3 |
| 7 | [#147](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/147) | `gold-thesis/phase-4-followup-5` | Исторический новостной слой для GRAM/sentiment/ML-признаков; нужен PIT/vintage и дедупликация |

**Смежные ML-пункты** (не попали в текстовый поиск «ML», но методологически близки): #124 (рыночные вероятности KEYRATE и ансамбль), #125 (байесовские priors), #145 (HMM regime detection), #153 (`v_forecast_accuracy_live`). Их брать после #184/#191 и только как challenger'ов с предрегистрацией.

## 6. Где хранить процесс: SKILL или AGENTS.md

**Решение: SKILL + короткий указатель в AGENTS.md.**

Почему не весь процесс в `AGENTS.md`:

- `AGENTS.md` читается каждой сессией, включая Go-задачи; 150–200 строк ML-методологии раздувают общий контекст и размывают правила 1–12.
- Ролевые скилы в репозитории уже есть: `github-tracker`, `dashboard-migration`, `news-source-onboarding`, `sync-readme-architecture`.
- Скил загружается по триггеру (ML-эксперимент, nowcast, CatBoost, MLflow, `forecast_log`), а Go-разработчик его не читает.
- Скил можно обновлять, не меняя общий контракт всех агентов.

Что остаётся в `AGENTS.md` (5–7 строк):

1. ML-задача → прочитай `.kimi-code/skills/ml-engineer/SKILL.md`.
2. Скил не отменяет правила 1–12; спека/план, ветка, issue и приёмка обязательны.
3. DDL, импортёры, витрины и гранты — зона Go-стандарта `docs/IMPORTER_PACKAGE_STANDARD.md`; ML-агент не делает ad-hoc DDL.
4. Go-разработчику ML-скил не нужен.

Отдельный тип субагента `ml-engineer` сейчас не заводим: скил работает и inline, и при диспатче coder-субагента; новый тип агента — непроверенная сущность.

## 7. Рекомендации ML-инженерам и агентам

### 7.1 Универсальный цикл

1. **Пункт роадмапа.** Назови `<slug>/<item-id>`, прочитай владеющий роадмап и issue; не делай работу вне пункта.
2. **Спека и план.** Зафиксируй гипотезу, ожидаемый эффект, primary metric, бенчмарки, фолды/окна, robustness, MDE и бюджет экспериментов — до первого прогона.
3. **Аудит данных.** Семантика и единицы, календарная полнота, дубликаты, as-of/PIT, винтажи, NaN-политика, выбросы. Чтение ClickHouse — только через MCP `clickhouse` (`kimi_reader`).
4. **Лестница бенчмарков.** B1/B2/B3 и внешние бенчмарки до ML; без значимого выигрыша модель не входит в ансамбль.
5. **Эксперимент.** Walk-forward с purge/embargo, без random split и без тюнинга по тесту; на коротких рядах — дефолты, не перебор.
6. **Оценка.** RMSE/MAE + DM-HLN + блочный бутстрап; для вероятностей — Brier/log loss/калибровка; для многих спецификаций — SPA/MCS.
7. **Паспорт run.** `ML_LOG=1`; MLflow включён по умолчанию (`MLFLOW=0` — осознанно), preflight проверяет API и спрашивает при недоступности; проверь `run_id`, config/sample hash, артефакты sha256, повтор детерминирован.
8. **Решение.** promote/reject/iterate с причиной; незначимый результат — тоже результат; не выбирай удобное окно.
9. **Промоушен.** До #184 — только `challenger`; не называть alias deployment. Live-прогноз — в `forecast_log` до публикации; для ставки — `error_bp`.
10. **Отчёт и приёмка.** Числа с командой и результатом, раздел «что не проверено», `make all`/`make docs-check`/`make mcp-check`, протокол комментарием в PR, статус роадмапа.

### 7.2 По открытым пунктам

- **#184:** начать со спеки gate: какие метрики, пороги, на каких фолдах, что считается деградацией; затем `v_ml_live_accuracy` поверх `forecast_log` + `cbr_key_rate`, резолвер, Grafana-алерты. Не подменять gate alias'ом MLflow.
- **#191:** не перезапускать исследование; добить production-строки `forecast_log`, свежий as-of снимок, опубликовать пост, добавить ссылку в issue, после заседания — пост-апдейт и сверка через `forecast_resolve`. Паритет CB_COMP не заявлять.
- **C1:** выгрузка признаков и таргета, walk-forward без look-ahead, дата-срез в метаданных; сразу определить B1/B2/B3 и DM-протокол, иначе C4 будет не с чем сравнивать.
- **C2:** in-DB `stochasticLinearRegression` на тех же признаках; это baseline, а не замена CatBoost.
- **C3:** Python-обучение, прогнозы в БД по dagu, версия модели + дата + набор признаков; прогноз публикуется тем же конвейером, что экспертный.
- **C4:** DM CatBoost vs эксперт vs in-DB на истории; решение «советник/замена» фиксируется в роадмапе, а не в PR-комментарии.
- **#147:** бэкфилл новостей с PIT/vintage, дедупликацией и явной датой публикации; без этого sentiment/GRAM-признаки дадут look-ahead.

## 8. Чего не делать

- Не переносить всю ML-методологию в `AGENTS.md`.
- Не делать ad-hoc `CREATE TABLE`/`INSERT` из ML-скрипта; схема и гранты — через Go-импортёр.
- Не сравнивать quick- и full-прогоны; quick-числа неофициальные.
- Не заявлять паритет без DM/TOST, превосходство LLM без DM/encompassing, а deployment — по alias в registry.
- Не выбирать окно/спецификацию по тесту и не прятать незначимые результаты.
- Не считать staging проверенным, если SSH/сервер были недоступны.
- Не заводить шестой роадмап и новый тип агента без решения владельца.

## 9. Источники

- Issue и PR: #123/#177, #127, #178/#181, #183, #189/#194, #190/#198, #195, #197/#202, #184, #191/#199, #154–#157, #147.
- Роадмапы: `docs/ROADMAP_KEY_RATE.md` §6, `docs/ROADMAP_INFRA.md` §5, `docs/GOLD_THESIS_DATA_ROADMAP.md` §5, `docs/ROADMAP_INDEX.md`.
- Методология: `docs/guides/stat-ml-models.md` §4–§7; `docs/KEY_RATE_MODEL.md` §4/§6; `docs/research/2026-10-18-ml-experiment-management.md` §4/§7; `docs/research/2026-10-16-cb-comp-vs-cpi-nowcast.md`.
- Внешние практики: Sculley et al., «Hidden Technical Debt in Machine Learning Systems» (NeurIPS 2015); Breck et al., «The ML Test Score» (2017); López de Prado (2018), гл. 7 (purging/embargo); Diebold & Mariano (1995); Harvey, Leybourne & Newbold (1997); Hansen (2005); Hansen, Lunde & Nason (2011); Hyndman & Athanasopoulos, «Forecasting: Principles and Practice»; MLflow Tracking/Model Registry docs.
