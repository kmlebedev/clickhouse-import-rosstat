# План: группа `key_rate_sources` и runbook публикационного прогона

**Пункт:** `infra/5.6` ([issue #332](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/332))
**Спека:** [2026-10-21-key-rate-sources-runbook-design.md](../specs/2026-10-21-key-rate-sources-runbook-design.md)

## Шаг 1. Makefile: группа импортёров

Файл: `Makefile` (рядом с целью `import`).

- [ ] Переменная `STATS_key_rate_sources` — 50 имён через запятую, сгруппированных по доменам; переносы строк с запятой в конце.
- [ ] `unexport STATS_key_rate_sources` — в Makefile есть голый `export`.
- [ ] В цели `import`: эхо `группа <имя>: <список>` при непустом `$(STATS_$(STAT))`; запуск с `CLICKHOUSE_IMPORT_STAT="$(or $(STATS_$(STAT)),$(STAT))"`.
- [ ] `make archive-restore` не меняется.
- [ ] Комментарий: группа — не импортёр; порядок — регистрация в `chimport.Stats`; новая группа — новая переменная `STATS_<имя>`; состав выведен из контракта публикации и карты каналов.

**Проверка:** `make -n import STAT=key_rate_sources` — 50 имён; `make -n import STAT=bea` — `CLICKHOUSE_IMPORT_STAT="bea"`.

## Шаг 2. Runbook `docs/publications/README.md`

- [ ] Разделы: что нужно заранее; шаг 1 — источники; шаг 2 — ИПЦ; шаг 3 — ставка; шаг 4 — журнал; шаг 5 — витрина, `ml-live-check`, графики; шаг 6 — текст; copy-paste блок; экспериментальные треки; частые сбои; ссылки.
- [ ] Только `make`-команды; ключи не передаются в командной строке; артефакты названы по именам файлов в корне репозитория.
- [ ] Порядок: `key-rate-forecast-log` → `key_rate_views` → `ml-live-check` (витрина `v_ml_live_accuracy` создаётся `key_rate_views`).
- [ ] Ограничение: `CLICKHOUSE_IMPORT_STAT=key_rate_sources` без `make` не работает.
- [ ] Поведение группы: один процесс, ошибка одного не останавливает остальные, итоговый код 1, backoff при мёртвом источнике, критерий `no importer named`.

## Шаг 3. Синхронизация документации

- [ ] `README.md`: строка `docs/publications/README.md` в таблице документации; `make import STAT=<имя|группа>` в таблице команд.
- [ ] `docs/site-pages.json`: `docs/publications/README.md` → `publications-runbook.md`, секция «Контур ключевой ставки».
- [ ] `docs/ROADMAP_INFRA.md`: пункт 5.6 со ссылкой на issue #332, спекой и планом; статус `🔶 In Review` (номер PR добавляется после открытия PR).
- [ ] `ARCHITECTURE.md` и `docs/KEY_RATE_PUBLICATIONS.md` не правятся.

## Шаг 4. Проверка

- [ ] `make -n import STAT=key_rate_sources` — 50 имён; `make -n import STAT=bea` — без подмены.
- [ ] `make import STAT=key_rate_sources` на dev-ClickHouse — все 50 имён отработали, `no importer named` — 0; сбои источников зафиксированы в протоколе.
- [ ] `make import STAT=key_rate_views` — `v_key_rate_dashboard` и `v_ml_live_accuracy` созданы; `make ml-live-check` — проходит.
- [ ] `make docs-check` — зелёный (манифест и ссылки).
- [ ] `make all` — зелёный.

## Шаг 5. PR и трекер

- [ ] Ветка `feat/key-rate-sources-runbook`, worktree `.worktrees/key-rate-sources-runbook`.
- [ ] Коммиты: спека+план → реализация → отметка `In Review` с номером PR.
- [ ] PR по шаблону: пункт `infra/5.6`, `Closes #332`, роль `go`, проверки, протокол живой приёмки комментарием.
- [ ] Project V2: пункт в проекте, `Status = In Review`, поля `Branch`, `PR`, `Spec / Plan`, `Acceptance`.

## Вне объёма

Нет нового импортёра, таблицы, DDL, витрины, гранта, DAG и env-переменной. Починка сломавшихся источников, обнаруженных живым прогоном (`domrf_mortgage`, `rosstat_labour_force_mes`, `rosstat_bi_cpi_weekly`), — отдельные пункты роадмапов, в этот PR не входит.
