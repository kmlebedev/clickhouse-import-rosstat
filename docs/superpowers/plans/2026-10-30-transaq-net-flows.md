# План: `ofz-market/1.5` — нетто-объёмы Transaq + деплой экспортёра на stage

**Дата:** 2026-10-30
**Пункт:** `ofz-market/1.5` (issue [#414](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/414))
**Спека:** [2026-10-30-transaq-net-flows-design.md](../specs/2026-10-30-transaq-net-flows-design.md)
**Research:** [2026-10-30-transaq-net-flows-source.md](../../research/2026-10-30-transaq-net-flows-source.md)
**Ветка:** `feat/ofz-market-1.5` (worktree `.worktrees/ofz-market-1.5`, от `main`)
**Объём:** два PR — деплой внешнего экспортёра (другой репозиторий) и агрегатор с витринами (этот репозиторий).

---

## 1. Цель

Закрыть блокер пункта 1.5 — отсутствие рыночной подписки: развернуть `transaq-clickhouse-exporter` на staging (`palmshell`) со сбором сделок по всем акциям IMOEX и всем ОФЗ, затем построить минутные и дневные нетто-объёмы из ленты `transaq_trades`.

Исходное состояние на 2026-10-30 (read-only проверка):

| Что | Состояние |
|---|---|
| `txmlconnector-server` (Wine, gRPC `:50051`) | active, слушает |
| `transaq-clickhouse-exporter` | не развёрнут |
| `transaq_*` в ClickHouse | таблицы есть, все 0 строк |
| креды Transaq на хосте | отсутствуют (вносит владелец) |
| ClickHouse | 26.2.4, писатель `default`, `kimi_reader` есть |

## 2. Два расхождения со спекой (правятся тем же PR)

1. **Пакет.** Спека называла `ofz/`, но стандарт §7.1 требует префикс пакета в `Name()`, а `ofz/importers_test.go` уже энфорсит `ofz_` и ровно один импортёр. Имя таблицы `transaq_net_flows` задано критерием приёмки issue → новый пакет `transaq/`.
2. **Граница scope.** Спека объявляла изменения в репозитории экспортёра вне scope; артефакты деплоя по решению владельца живут там.

## 3. Порядок работ

### Шаг 0. Ветка и документы

- Worktree `.worktrees/ofz-market-1.5`, ветка `feat/ofz-market-1.5` от `main`.
- Коммит 1 — research, спека, секция 1.5 роадмапа, абзац LIMITATIONS, запись site-pages.
- Коммит 2 — этот план и правки спеки (пакет, вселенная, граница scope).

### Шаг 1. PR‑B: репозиторий `transaq-clickhouse-exporter`

Отдельный PR (другой репозиторий, новая ветка от `main`).

1. `Makefile`: `test`, `vet`, `build` (`GOWORK=off GOOS=linux GOARCH=amd64 CGO_ENABLED=0`), `universe-refresh`, `universe-diff`, `deploy-staging*`, `rollback-staging`.
2. `deploy/staging/transaq-clickhouse-exporter.service`: `EnvironmentFile` секретов (владелец) + несекретных фильтров (деплой), `Restart=always`, зависимость от `txmlconnector-server.service`.
3. `deploy/staging/universe-imoex-ofz.txt` — 44 тикера IMOEX + 61 ОФЗ TQOB; генерируется из MOEX ISS, обновляется вручную.
4. `deploy/staging/exporter.export.env.in` — `EXPORT_SEC_BOARDS=TQBR,TQOB`, `EXPORT_ALL_TRADES=<вселенная>`, `EXPORT_SEC_INFO_NAMES=ALL`, пустой `EXPORT_SEC_CODES`.
5. `deploy/staging/README.md` — пути, одноразовый секрет-файл, квартальное обновление вселенной, откат, диагностика пустой ленты.
6. Деплой на `palmshell`, идемпотентный повтор, smoke, живая проверка строк в сессию.

### Шаг 2. PR‑A: пакет `transaq/` (этот репозиторий)

1. `transaq/net_flows.go` — импортёр `transaq_net_flows`: DDL, окно `[watermark − 15 мин, max(time)]`, no-op гейт, почасовые чанки, агрегация SQL (дедуп по полному ключу сделки, `lotsize` из `transaq_securities FINAL`, `gap_suspected` через `lagInFrame`), батчевая запись.
2. `transaq/views.go` — `v_transaq_net_flows`, `v_transaq_net_flows_daily` с комментариями таблиц и колонок.
3. `transaq/importers.go` + blank-import в `main.go`.
4. Тесты: `planWindows`, пин DDL, контроль-flow на фейковом `driver.Conn`, энфорсер реестра §8.3.
5. `dagu/transaq.yaml` — `*/15 9-18 * * 1-5` + `20 19 * * 1-5`, retry 3×300.
6. `sql/mcp_kimi_reader.sql`, `scripts/mcp_check.py`.
7. `ARCHITECTURE.md` §2/§6.2/§6.3/§7, `README.md`, `docs/IMPORTERS.md`, статус роадмапа.

## 4. Проверки

- PR‑B: `go test ./...`, `go vet ./...`, `make deploy-staging`, `make deploy-staging-status`, повторный деплой (идемпотентность), `make rollback-staging`; живой прогон — строки `transaq_trades` в сессию, покрытие 105 кодов в `transaq_securities`.
- PR‑A: `make all`, `make docs-check`, `dagu validate dagu/transaq.yaml`, `make import STAT=transaq_net_flows`, повторный прогон (идемпотентность), сверка дневной витрины с контрольной суммой по лентам, `make mcp-check`.
- Протокол живой приёмки — комментарием в PR; статус роадмапа — тем же PR после прогона.

## 5. Риски

- Окно агрегации не отсекает гранулы (`ORDER BY` ленты начинается с `secid`): при росте таблицы >~100 млн строк нужен отдельный пункт (проекция по `time` или TTL).
- Строгий список вселенной: опечатка или пропущенный ребаланс молча не подписывает инструмент — лечится сверкой покрытия в smoke и `make universe-diff`.
- Новые ОФЗ на ISS идут с ISIN в `SECID`; несовпадение с Transaq `seccode` ловится той же сверкой.
- Пустая лента при `success=true` — известный режим отказа аккаунта; критерий приёмки — строки и свежесть.
