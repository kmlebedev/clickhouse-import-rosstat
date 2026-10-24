# План: `ofz-market/1.1` — аналитика выпусков ОФЗ

**Дата:** 2026-10-24
**Пункт:** `ofz-market/1.1` (issue #376)
**Спека:** [2026-10-24-ofz-issue-analytics-design.md](../specs/2026-10-24-ofz-issue-analytics-design.md)
**Ветка:** `feat/ofz-market-1.1`
**Worktree:** `.worktrees/ofz-market-1.1`

## Task 1. Спека и план

- [x] Спека и план созданы.
- Коммит: `docs: add ofz issue analytics spec and plan`.

## Task 2. Пакет `ofz/`: чистый расчёт

Файлы:

- `ofz/calc.go` — `couponSchedule`, `ytm`, `macaulayDuration`, `interpolateCurve`,
  `classifyOFZ`.
- `ofz/calc_test.go` — эталонный вектор YTM/дюрации, календарь купонов,
  интерполяция, классификация.
- `ofz/importers.go` — реестр `ofzImporters` + `init()`.
- `ofz/importers_test.go` — энфорсер `Name()`/префикса таблицы.

Проверка:

```bash
go test -race ./ofz/
```

Ожидаемо: PASS.

## Task 3. Импортёр и схема

Файлы:

- `ofz/issue_analytics.go` — DDL, SQL, батч, `AfterImport`.
- `ofz/views.go` — `v_ofz_issue_analytics` и комментарии.
- `ofz/series_meta.go` — каталог метрик.

Проверка:

```bash
go test -race ./ofz/
go vet ./ofz/
```

Ожидаемо: PASS.

## Task 4. Регистрация, DAG, грант, MCP-check

- `main.go`: blank-import `_ "github.com/kmlebedev/clickhouse-import-rosstat/ofz"`.
- `dagu/ofz_analytics.yaml`: ежедневный шаг `ofz_issue_analytics`.
- `sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_ofz_issue_analytics`.
- `scripts/mcp_check.py`: проверка витрины (строки, последняя дата).

Проверка:

```bash
dagu validate dagu/ofz_analytics.yaml
python3 -m py_compile scripts/mcp_check.py
```

## Task 5. Документация и статус

- `ARCHITECTURE.md` §6.1/§6.2/§6.3: таблица, витрина, каталог, грант.
- `docs/IMPORTERS.md`: имя импортёра и таблица.
- `README.md`: строка OFZ-контура, если есть список.
- `docs/ROADMAP_OFZ_MARKET.md`: `1.1` — `🔶`/`In Progress` с пометкой о live-приёмке.
- `docs/ROADMAP_INDEX.md`: если требует статус/ссылка.

## Task 6. Проверки и приёмка

```bash
gofmt -l .
go vet ./...
go test -race ./...
make all
dagu validate dagu/ofz_analytics.yaml
```

Живой прогон (если сырые `transaq_*` доступны):

```bash
make import STAT=ofz_issue_analytics
```

Ожидаемо: `Imported N rows of ofz_issue_analytics`, N > 0; повтор — идемпотентный
результат; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.

Результат 2026-10-24 (dev-ClickHouse): `make import STAT=ofz_issue_analytics` — 61 строка,
повтор — 61 строка; `ytm`/`spread_to_curve` — 46 из 61, `duration` — 33 (только fixed/ok); каталог — 12 метрик;
сырая таблица закрыта (497); блок OFZ в `make mcp-check` — OK. Тестовый Transaq-аккаунт
не отдаёт `securities_info`, поэтому использован MOEX ISS fallback (`source='moex'`).
Общий `make mcp-check` — 2 чужих падения (EIA distillate freshness, news LLM backlog),
к этому пункту не относятся. `make docs-check` падает на уже существующем отсутствующем
изображении weekly-sa-cpi, не из-за этого PR.

## Task 7. PR

- Один PR в конце: `ofz-market/1.1`, `Closes #376`, источник, схема, запуск, пример
  проверки.
- Протокол живой приёмки — комментарием в PR.
- Статус роадмапа — тем же PR; `Done` только после мержа.
