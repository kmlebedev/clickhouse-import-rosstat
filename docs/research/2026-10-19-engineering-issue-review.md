# Инженерные issue clickhouse-import-rosstat: ревью, сверка с практиками и рекомендации (2026-10-19)

> **Объект:** 36 закрытых и 64 открытых issue GitHub-репозитория `kmlebedev/clickhouse-import-rosstat` на 2026-10-19. ML-часть детально разобрана в [2026-10-19-ml-issue-review.md](2026-10-19-ml-issue-review.md); здесь — сводка по всем группам и процесс для Go/Python/Research-агентов.
> **Задача:** обобщить, что сделано по закрытым issue; сверить результат с лучшими практиками Go/GitHub, применимыми к этому репозиторию; дать рекомендации по открытым задачам; обосновать, где хранить процесс — в `AGENTS.md` или в ролевых скилах.
> **Метод:** чтение issue/PR, роадмапов (`docs/ROADMAP_*.md`), `ARCHITECTURE.md`, `docs/IMPORTER_PACKAGE_STANDARD.md`, `docs/DEVELOPMENT.md`, спе/планов и протоколов приёмки; GitHub-аудит через MCP (`list_issues`, `pull_request_read`, `projects_list`) и `gh api` (repo settings, branch protection). Внешние практики — по устойчивой литературе и официальной документации инструментов (ссылки в §9).
> **Границы:** это обзор и рекомендации, не реализация. Числа взяты из issue/роадмапов на 2026-10-19; staging, часть внешних источников и branch protection в живой сессии не проверялись (branch protection проверена через API: `main` не защищён).

---

## 1. Короткий вывод

Репозиторий зрелый по инженерной дисциплине: каждый пункт идёт по роадмапу, спека и план фиксируются первыми коммитами, PR один и в конце, приёмка — с командой и результатом, трекер синхронизирован с Project V2. Закрытые issue подтверждают это на импортёрах, винтажном архиве, форме 101, e-disclosure, ML-реестре и MLflow.

Главный разрыв — **ролевой и эксплуатационный**:

1. `AGENTS.md` описывает только Go-разработчика, хотя фактически работают четыре роли (Go, Python, ML, Research).
2. Python-тесты и gitleaks не входят в CI, а `main` не защищён.
3. Нет протокола выбора/«захвата» issue и WIP=1, поэтому параллельные агенты могут брать один пункт или нарушать зависимости.
4. Регламент параллельных сессий существует только в незамёрженной ветке `chore/parallel-sessions`, хотя в работе 32 worktree.

**Рекомендация:** тонкий `AGENTS.md`-роутер + ролевые скилы (`go-engineer`, `python-engineer`, `research-engineer`, `ml-engineer` уже есть) + `subagent-dispatch`; CI-принуждение и parallel-sessions — отдельными пунктами `infra/5.2`/`5.3`.

## 2. Закрытые issue: что сделано

### 2.1 Данные и импортёры (14)

| Issue | Пункт | Результат | Урок |
|---|---|---|---|
| [#42](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/42) | `gold-thesis/phase-3` | bold.report: T+1 цена дня, 7 дневных ETF-рядов 73 фондов; `gold_prices`, `bold_fund_snapshot` | Внешний JSON-API без ключа — дешёвый и устойчивый источник; фикстуры обязательны |
| [#46](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/46) | `gold-thesis/phase-4` | 16 новостных лент (8 RSS + 8 Telegram), `news_watch`; скил `news-source-onboarding` | Новый поток = онбординг по скилу + bootstrap LLM-разметки |
| [#57](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/57) | `infra/1.1` | Винтажный архив версий файлов источников (`archive/` + `registry.jsonl`, Яндекс.Диск) | Архив — часть шаблона импортёра, не отдельная логика |
| [#74](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/74) | `key-rate`/`bank` | Рефакторинг `bank/` по стандарту: реестр, транспорты, каталог, витрина, банки в `company_financials` | Legacy-пакет переводится на `util.ClickHouseImport`; новый код в `financial/` не добавляется |
| [#75](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/75) | `key-rate` | Восстановление рядов Росстата 2021 из живого буфера; недельные ИПЦ | Живой буфер — источник восстановления; повтор no-op |
| [#86](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/86) | `key-rate` | Календарь раскрытия топ-5 банков и форвардная витрина кредитного портфеля | Календарь — отдельный импортёр, витрина — отдельный пункт |
| [#93](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/93), [#95](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/95), [#98](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/98), [#103](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/103) | `key-rate/phase-2` | Форма 101 ЦБ: SBER, 194 строки, RAR→DBF, маппинг 809-П, `v_cbr_form101_loans`, `company_financials` | Сложный формат требует research-документа и фикстур; маппинг версионируется |
| [#116](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/116) | `key-rate/phase-2` | Минфин: федеральный дефицит отдельным рядом, `minfin_oil_gas_mes` (нефтегаз, НДПИ), `v_minfin_budget` | Сайт требует браузерного UA; имя файла вычисляется по дате |
| [#117](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/117) | `key-rate/phase-2` | Общий разбор HTML-таблиц ЦБ: `util.ParseHTMLTable`, `UniDbQueryURL`, КБД, MIACR, ликвидность | Инфраструктурный helper переиспользуется несколькими импортёрами |
| [#169](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/169) | `key-rate/phase-2` | `cbr_zcyc`: 12 теноров, 2003-01-04…2026-10-16, 72 468 точек; `v_ofz_curve_compare`, `v_ofz_signal` | Лимит ЦБ ≈5 лет на запрос → окна по 4 года; инкремент по `max(date)` |
| [#182](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/182) | `dcf-polyus/phase-3` | e-disclosure: 290 документов, 20 версий, 54 701 575 байт; `v_edisclosure_documents`, архив | Полуавтоматический коллектор + детерминированный импорт; `archive_key` обязателен |

### 2.2 ML (13)

| Issue | Пункт | Результат | Урок |
|---|---|---|---|
| [#122](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/122) | `key-rate/phase-2` | LLM-разметка RSS батчами (GLM-5.3-Flash), `news_annotation` | Без env/при отказе провайдера — warn и зелёный импорт |
| [#123](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/123) | `key-rate/phase-4-ml-v2` | `cpi_monthly.py`: 155/292 месяца, 4 CatBoost + B1–B3, DM-HLN; значимости на 5 % нет | Незначимый результат публикуется; нельзя выбирать удобное окно |
| [#126](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/126) | `key-rate/phase-4-point-in-time` | PIT-слой `v_point_in_time*`, `data_version` | Ревизии и винтажи видны разностью версий |
| [#127](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/127) | `key-rate/phase-4-forecast-log` | `forecast_log`, `error_bp`, ingest endpoint, резолвер `cbr_rate` | Live-прогноз пишется до публикации; ошибка ставки — в б.п. |
| [#128](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/128) | `key-rate/phase-4-dashboard` | `v_key_rate_dashboard` | Витрина — отдельный пункт после источников |
| [#129](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/129) | `key-rate/phase-4-consensus-benchmark` | Консенсус-бенчмарк №4 (43 волны, 41 пара) и B4-проекция | Бенчмарк фиксируется до сравнения |
| [#178](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/178) | `infra/4.1` | `ml_runs`/`ml_predictions`, `ml_registry`, `v_ml_*`, `ML_LOG=1` | Паспорт run и fold-предсказания — обязательная часть эксперимента |
| [#183](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/183) | `infra/4.2` | Self-hosted MLflow, aliases `champion`/`challenger`, экспорт в ClickHouse | Control plane (MLflow) ≠ analytics plane (ClickHouse) |
| [#189](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/189) | `key-rate/phase-4-llm-track` | LLM-прогноз: as-of, 3 повтора, медиана; живые 12/83/5 % | LLM — cross-check, не доказательство превосходства |
| [#190](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/190) | `key-rate/phase-4-ml-cb-comp-parity` | Паритет CB_COMP: direct/bottom-up, DM/TOST; вердикт «не близок» | Origin `M−(h−1)` меняет числа радикально |
| [#197](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/197) | `infra/4.4` | `--quick`, `--jobs N`; long 20:34 против 89:29 (4.35×) | Quick-числа неофициальны; полный прогон не меняется |
| [#200](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/200) | `key-rate/phase-4-shock-classifier` | Классификатор шоков, as-of веса, описательный логистический слой | В ClickHouse напрямую не пишет; журнал через `ml_experiment` |
| [#209](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/209) | `infra/4.5` | ML-скил + review 14 ML-issue | Ролевой процесс — в скиле, не в AGENTS.md |

### 2.3 Инфраструктура, трекер, docs (9)

| Issue | Пункт | Результат | Урок |
|---|---|---|---|
| [#23](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/23) | `gold-thesis` | Дашборды как код, `dashboards.yml`, три артефакта | Артефакт и код в одном PR; CI ловит расхождение |
| [#52](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/52) | `infra` | Выбор моделей субагентов (GLM/MiMo/deepseek/haiku) | Политика dispatch меняется часто — выносится в отдельный скил |
| [#90](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/90) | `infra` | Superpowers интегрированы в GitHub-трекер | Процесс и трекер должны ссылаться друг на друга |
| [#105](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/105) | `infra/3.3` | Docs-портал на GitHub Pages, `site-pages.json`, `pages.yml` | Сайт — производный артефакт; источник истины в `docs/` |
| [#106](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/106) | `infra/3.1` | Модель трекера: `ROADMAP_INDEX`, правило 12, issue/PR-шаблоны | Составной ID `<slug>/<item-id>` снимает коллизии |
| [#112](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/112) | `infra/3.2` | Project V2 #11, 75 items, поля и виды | Project — вид; источник истины — роадмап |
| [#167](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/167) | `infra/3.4` | README 179 строк, 7 docs-файлов | README — landing page, спецификации — в `docs/` |
| [#179](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/179) | `infra/2.3` | Скил `news-source-onboarding` | Повторяемый онбординг источника — скил, не разовая инструкция |
| [#185](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/185) | `infra/3.5` | Синхронизация роадмап → issue после мержа | Правило идемпотентно; issue не закрываются автоматически |

## 3. Что подтверждено как лучшая практика

| Практика | Как проявляется в репозитории |
|---|---|
| Roadmap-driven | Один пункт за раз, внеплановое — отдельный пункт, а не «попутно» |
| One item = one branch = one PR | Спека/план первыми коммитами, PR один и в конце |
| Предрегистрация | Спека и план до кода; критерий приёмки с командой и результатом |
| Живая приёмка | Число строк, крайние даты, повтор no-op, `make mcp-check` |
| Протокол в PR | Комментарий с командами и результатом, а не только тело PR |
| Трекер синхронизирован | Issue + Project V2 + статус в роадмапе тем же PR |
| Каталог и гранты | `series_catalog`, комментарии таблицы/колонок, `GRANT SELECT` только на витрины |
| Идемпотентность | `ReplacingMergeTree`, повторный импорт — no-op |
| Винтажный архив | Версия файла + `registry.jsonl`; восстановление из архива |
| CI-гейты Go | `gofmt`, `golangci-lint`, `go vet`, `go test -race`, `govulncheck`, сборка |
| Ролевые скилы | ML-скил и `github-tracker` уже доказали формат |

## 4. Разрывы с лучшими практиками

| Практика | Состояние | Разрыв / что делать |
|---|---|---|
| Ролевая маршрутизация | `AGENTS.md` — одна роль | Тонкий роутер + `go/python/research`-скилы (`infra/5.1`) |
| Python CI | Тесты только вручную (`docs/DEVELOPMENT.md`) | `python.yml`: fast stdlib-only + тяжёлые ML-наборы (`infra/5.2`) |
| Секреты в CI | gitleaks нет, хотя была утечка | `gitleaks.yml` + allowlist (`infra/5.2`) |
| Branch protection | `main` не защищён | Required checks + запрет force push (`infra/5.2`) |
| Проверка скилов | `docs-check` не видит `.kimi-code/**` и `AGENTS.md` | `skills_check.py` + `make skills-check` (`infra/5.2`) |
| WIP/claim | Нет протокола | Claim-комментарий + `In Progress` + `Branch`; WIP=1 (`infra/5.1`) |
| Параллельные сессии | Регламент в ветке `chore/parallel-sessions`, в `main` нет | Влить ветку (`infra/5.3`); пока — краткий блок в AGENTS.md |
| Inbox-триаж | 8 issue без роадмапа | Назначить владельца или закрыть |
| DoD по ролям | Только Go-чек-лист | Role-specific DoD в PR-шаблоне |
| Метрики процесса | Не ведутся | Cycle time, review-раунды, размер PR, CI-время, stale |

**Ключевой вывод:** `make all` не включает Python-тесты; для Python/ML-ролей «зелёный `make all`» недостаточен. Это должно быть явно в их Definition of Done.

## 5. Открытые issue: портфель и очередь

Открыто **64 issue**: 56 с label `roadmap:<slug>`, 8 в Inbox (#88, #89, #81, #77, #76, #58, #51, #48).

| Роль | Открытые пункты | Зависимости/комментарий |
|---|---|---|
| Go / данные | `infra/1.2` #109, `1.3` #110, `1.4` #111; `key-rate/phase-2` #113–#121, #142; `dcf-metals` #158–#165; `dcf-polyus` #131–#141; `gold-thesis` #143–#153, #173; `key-rate` #130 | Metals — последовательно step 1→8; polyus — проверить каждый `Depends on` |
| ML | #154–#157 (gold C1–C4), #145 (HMM), #124/#125 (вероятности/priors), #153 (`v_forecast_accuracy_live`), #184 (промоушен/live), #191 (первая публикация) | C2–C4 зависят от C1; #184 — критический путь, Go-часть после ML-спеки |
| Research | #77, #81, #85, #51, #58, #48, #76 | Сначала research-документ и критерий приёмки, потом реализация |
| Mixed | #184 (ML + Go), #131 (тестовая сессия DCF) | В спеке делить задачи по ролям |

**Очередь для Go-агента:**

1. `infra/1.2` #109 → `1.3` #110 → `1.4` #111 — надёжность архива.
2. `dcf-metals/step-1..8` #158–#165 — последовательно.
3. `key-rate/phase-2` #113/#114/#115/#118/#119/#120/#121/#142 — независимые импортёры.
4. `gold-thesis` #173, #143–#153 — витрины/новостной слой.
5. `dcf-polyus` #131–#141 — после проверки зависимостей.
6. `infra/4.3` #184 — Go-часть после ML-спеки.
7. Research-first #77/#81/#85/#51/#58/#48/#76.
8. Inbox #88/#89 — владелец или закрытие.

**Правило выбора:** `Status = Todo`, все `Depends on` закрыты, нет claim-комментария, один агент — один пункт.

## 6. Рекомендации Go-агентам

### 6.1 Цикл 12 шагов

1. **Выбрать ready-пункт** по §5.
2. **Захватить issue:** комментарий «Taking `<slug>/<item-id>`; branch …; worktree …», `In Progress`, `Branch`, `Spec / Plan`.
3. **Изолироваться:** worktree + ветка от `main`; в корне не работать.
4. **Прочитать минимум:** `AGENTS.md`, `go-engineer`, строку роадмапа, issue, спеку/план, `ARCHITECTURE.md` §2–§4 + нужный §6.x, стандарт §14/§15.
5. **Спека/план** — первыми коммитами, если их нет.
6. **Реализовать по стандарту:** один импортёр/таблица на PR; батчи; DDL; каталог; витрина; комментарии; `GRANT`.
7. **Проверить живьём:** `make import STAT=...`, число строк, крайние даты, повтор no-op, `make mcp-check`.
8. **Прогнать CI-набор:** `make all`, `make docs-check`, `dagu validate`.
9. **Ревью:** task-reviewer на другой модели; fix-loop; финальное ревью на сильнейшей модели.
10. **Один PR в конце:** item, `Closes #N`, схема, запуск, проверка; протокол — комментарием.
11. **После мержа:** `Done` только после протокола; синхронизация роадмапа (правило 12); удалить worktree.
12. **Стоп-правила:** приёмка не проверена → «не проверено»; объём вырос → новый пункт; противоречие issue/роадмапа → Q&A и стоп.

### 6.2 По открытым пунктам

- **#109/#110/#111:** начать с `archive-get` и листинга Яндекс.Диска; затем S3-бэкенд; затем детектор урезанного покрытия. Не менять формат `registry.jsonl` без спеки.
- **#158–#165:** step 1 — аудит метрик; step 2 — решение по MAGN; step 3 — пакет `metals/`; step 4 — CHMF+NLMK; step 5 — MAGN; step 6 — дашборд; step 7 — SVST; step 8 — документация. Не начинать step N+1, пока не закрыт step N.
- **#113–#121, #142:** каждый импортёр — отдельный пакет/файл, каталог, витрина, грант; для национальных ЦБ — отдельный research по доступности.
- **#173:** госдолг США и обслуживание — источник FRED/Treasury; проверить единицы и частоту.
- **#143–#153:** новостной слой и витрины; PIT/vintage обязательны, иначе look-ahead.
- **#184:** Go-часть — витрина `v_ml_live_accuracy`, комментарии, `GRANT`, `make mcp-check`, Grafana alerts, dagu; gate-скрипт и пороги — ML-инженер.
- **Research-first:** #77 (чувствительность ИЦП), #81 (бенчмарки доходности), #85 (статья об ошибках ЦБ), #51 (притоки граждан), #58 (политический канал), #48 (винтажная БД), #76 (ML по компонентам ИПЦ) — сначала research-документ с источником, покрытием и критерием, потом issue на реализацию.

### 6.3 Метрики эффективности

Cycle time issue → PR → merge, число review-раундов, размер PR (файлы/строки), время CI, доля fix-раундов 4–5, возраст открытых issue, нарушения WIP=1. Вести в ежемесячном аудите `github-tracker` §7.

## 7. Где хранить процесс: SKILL или AGENTS.md

**Решение: тонкий `AGENTS.md`-роутер + ролевые скилы.**

Почему не весь процесс в `AGENTS.md`:

- файл читается каждой сессией, включая задачи других ролей; Go-детали, Python-команды и Research-протокол раздувают общий контекст;
- ролевые скилы в репозитории уже есть (`ml-engineer`, `github-tracker`, `dashboard-migration`, `news-source-onboarding`, `sync-readme-architecture`);
- скил загружается по триггеру; Go-разработчик не читает Python/ML, Python — Go;
- скил обновляется, не меняя общий контракт.

Что остаётся в `AGENTS.md`: роутер ролей, порядок начала сессии, WIP/claim, краткий блок параллельных сессий, правила 1–12 (нумерация сохраняется), окружение, трекер, стиль PR, приоритеты.

## 8. Чего не делать

- Не переносить всю ролевую глубину в `AGENTS.md`.
- Не делать ad-hoc DDL/INSERT из Python/ML; схема и гранты — через Go-импортёр.
- Не брать пункт с открытым `Depends on` и не работать в общем корне при параллельных сессиях.
- Не считать `make all` полной проверкой Python/ML.
- Не объявлять `Done` до мержа и протокола; не закрывать issue автоматически при расхождении с роадмапом.
- Не заводить новые типы субагентов и шестой роадмап без решения владельца.
- Не добавлять CI-workflows и parallel-sessions в этот PR — это `infra/5.2`/`5.3`.

## 9. Источники

- Issue/PR: #23, #42, #46, #57, #74, #75, #86, #90, #93, #95, #98, #103, #105, #106, #112, #116, #117, #122, #123, #126–#129, #167, #169, #178, #179, #182, #183, #185, #189, #190, #197, #200, #209; PR #210 (ML-скил).
- Роадмапы: `docs/ROADMAP_INDEX.md`, `docs/ROADMAP_INFRA.md` §6, `docs/ROADMAP_KEY_RATE.md`, `docs/ROADMAP_DCF_POLYUS.md`, `docs/ROADMAP_DCF_METALS.md`, `docs/GOLD_THESIS_DATA_ROADMAP.md`.
- Нормативные документы: `ARCHITECTURE.md`, `docs/IMPORTER_PACKAGE_STANDARD.md`, `docs/DEVELOPMENT.md`, `docs/CONFIGURATION.md`, `docs/KEY_RATE_MODEL.md`, `docs/guides/stat-ml-models.md`.
- ML-ревью: `docs/research/2026-10-19-ml-issue-review.md`.
- Внешние практики: trunk-based development и short-lived branches (Fowler, «Patterns for Managing Source Code Branches»); GitHub Docs — protected branches, required status checks, issue linking (`Closes #N`), Project V2; gitleaks docs; Go Code Review Comments; Google Engineering Practices — code review; DORA — cycle time и change failure rate.
