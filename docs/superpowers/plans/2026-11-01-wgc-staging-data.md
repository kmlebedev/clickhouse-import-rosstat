# План: `gold-thesis/phase-3-followup-8` — WGC staging

> Спека — [2026-11-01-wgc-staging-data-design.md](../specs/2026-11-01-wgc-staging-data-design.md). Issue [#507](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/507), роль `go`. Ветка `fix/wgc-staging-data`, worktree `.worktrees/wgc-staging-data`.

## Задачи

1. **DAG.** `dagu/wgc.yaml`: `working_dir: /root/dagu`; шаг `wgc_cb_reserves` — env `WGC_REQUIRE_DATA=1`; обновить комментарий (отсутствие файлов → красный DAG, ручной прогон без флага — warn+0).
2. **Fail-loud.** `wgc/cb.go`: константа `WGC_REQUIRE_DATA`, проверка обоих файлов при флаге; ошибка с каталогом и масками отсутствующих. Без флага — прежнее поведение (warn+0 / частичный набор).
3. **Тесты.** `wgc/cb_test.go`: без флага пустая папка = 0/nil; с флагом пустая папка = ошибка; с флагом один файл = ошибка с маской; с флагом оба файла — разбор (существующие фикстуры).
4. **Доставка.** `Makefile` `deploy-staging-data`: `install -d /root/dagu/wgc/data`, `scp wgc/data/*.xlsx` при наличии, warn при отсутствии.
5. **Документы.** `ARCHITECTURE.md` §6.1 (`wgc_cb_reserves`: `working_dir`, deploy, fail-loud), `docs/IMPORTERS.md` (runbook/деплой), `docs/GOLD_THESIS_DATA_ROADMAP.md` — пункт `phase-3-followup-8`.
6. **Приёмка.** `make all`, `make docs-check`, `dagu validate dagu/wgc.yaml`, `go test ./wgc`; staging: deploy binary/dagu/data, `dagu start wgc`, `Imported 1942 rows` (актуальное), `v_cb_gold_dynamics` 7 стран без NULL, повтор идемпотентен; публичный MCP.

## Порядок

- Спека и план — первые коммиты; затем DAG/код/тесты/доки одним PR.
- Task-review после реализации, финальное ревью ветки; staging-приёмка; PR с протоколом.

## Модели диспатча

- Имплементер — `deepseek/deepseek-flash` (несколько файлов: DAG, Makefile, Go, docs).
- Task-reviewer — `anthropic/haiku`; при недоступности — GLM/controller, как в #484.
- Fix-раунды — `joingonka/zai-org/GLM-5.3-Flash` для одного файла, иначе deepseek.
- Финальное ревью — `deepseek/deepseek-flash`.
