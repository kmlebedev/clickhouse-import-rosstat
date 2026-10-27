# Staging: паритет MCP-витрин и уведомления dagu — план

Спека: [2026-10-25-staging-parity-alerting-design.md](../specs/2026-10-25-staging-parity-alerting-design.md). Issue: [#384](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/384).

## Task 1. Base DAG и deploy-артефакт

- [ ] `deploy/staging/dagu-base.yaml`: дефолты dagu + `env` + `smtp`/`error_mail`/`mail_on`.
- [ ] `deploy/staging/clickhouse-import-rosstat.env.example`: `SMTP_HOST/PORT/USER/PASSWORD`, `MAIL_FROM`, `MAIL_TO`.
- [ ] `Makefile`: `STAGING_DAGU_BASE`, цель `deploy-staging-dagu-base` (sha256, `.bak`, restart только активного dagu), вызов из `deploy-staging-dagu`.
- [ ] `Makefile`: `deploy-staging-preflight` — SMTP-переменные непусты, base.yaml содержит `ROSSTAT_IMPORT_BIN`.
- [ ] `Makefile`: `deploy-staging-smoke` — временный env-smoke DAG; `deploy-staging-notify-test` — падающий DAG для проверки письма.
- [ ] `deploy/staging/dagu-env-smoke.yaml`, `deploy/staging/dagu-notify-test.yaml`.

Проверка: `make -n deploy-staging-dagu-base`; на staging — установка, `ENV_OK`, идемпотентный повтор.

## Task 2. Контракт MCP

- [ ] `sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_edisclosure_documents TO kimi_reader`.
- [ ] Проверка: `make mcp-user` на dev не требуется; на staging — `make mcp-user-staging` после импортов.

## Task 3. Живой staging

- [ ] `base.yaml` установлен, `dagu` перезапущен, env-smoke `ENV_OK`.
- [ ] `R`/`Rscript`, `seasonal`, `sqlite3` установлены; `pdftotext`/`uv` проверены.
- [ ] `make deploy-staging-data` копирует датапаки Polyus/ЮГК в `/root/...` и `/root/dagu/...`.
- [ ] Полный прогон DAG: calendar, cbr, rosstat, minfin, ofz, moex, fedstat, bank, domrf, gold, bold, wgc, eia, bea, bls, fred, fao, customs, financial, dcf, news, craw, ml, forecast, edisclosure; ppi — после R.
- [ ] Известный блокер `rosstat_labour_force_mes` (#337, смена ссылки на `trud_1_15-72.xlsx`): шаг падает, остальные шаги rosstat (bulletins, okved2, ppi_ved, bi_cpi_weekly, ppi_product, ppi_weights) добираются вручную; ряд в `v_rosstat_macro` отсутствует до закрытия #337.
- [ ] Известный блокер `fedstat_weekly_prices` (#387, 60 568 точек против 56 133 в гейте): остальные блоки ЕМИСС (ipp, retail, agri, investment, ipc) добираются вручную; `v_fedstat_weekly_prices` до закрытия может быть пустой.
- [ ] `forecast_log` DDL из `ingest/schema.go`; `gold_views`, `key_rate_views`; seeds `gold_forecasts_q4_2026_seed.sql` (и `dcf_mine_plans_seed.sql` при необходимости).
- [ ] `mcp-user-staging` после появления всех витрин.

Проверка: `/root/parity/sweep.status` без неразобранных падений; в логах `Imported`.

## Task 4. Аудит паритета

- [ ] Временный `/tmp/mcp_parity.py`: контракт vs dev vs staging — наличие, `DESCRIBE`, `SHOW CREATE VIEW`, `count()`, `max(date)`, гранты.
- [ ] Отчёт `/root/parity/after.json` + Markdown; расхождения — списком.
- [ ] MCP HTTP smoke на staging; `make mcp-check` через туннель (если доступен пароль staging-`kimi_reader`).

Проверка: 83/83 витрины, schema-match, гранты, MCP.

## Task 5. Уведомления

- [ ] Владелец заполняет SMTP-переменные в `/etc/clickhouse-import-rosstat.env`.
- [ ] `make deploy-staging-notify-test` → письмо; владелец подтверждает SMS.
- [ ] Убрать временный DAG и run history.

## Task 6. Документация и PR

- [ ] `deploy/staging/README.md`: base.yaml, SMTP, тест, rollback.
- [ ] `docs/OPERATIONS.md`: «Уведомления dagu», корректное описание base.yaml.
- [ ] `README.md`: новые цели.
- [ ] `docs/ROADMAP_INFRA.md`: строка 1.7.
- [ ] `make all`, `make docs-check`, `dagu validate` на staging.
- [ ] PR с `Closes #384`, протокол приёмки комментарием.
