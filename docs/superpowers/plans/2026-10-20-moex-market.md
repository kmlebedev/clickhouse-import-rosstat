# План: #113 Brent → `moex_market`

**Дата:** 2026-10-20
**Спека:** [2026-10-20-moex-market-design.md](../specs/2026-10-20-moex-market-design.md)
**Ветка:** `feat/key-rate-phase-2-brent` (worktree `.worktrees/key-rate-phase-2-brent`, от `main`)
**Issue:** [#113](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/113)

---

## 1. Цель

Закрыть `key-rate/phase-2-brent`: импортёр `moex_market` с рядом `brent_front` — непрерывный фронт-месяц фьючерса MOEX `BR`, дневной `SETTLEPRICE`, USD/барр., с 2009-10. В этом PR — только Brent; RVI и медь — следующий пункт `key-rate/phase-2-moex-market`.

## 2. Порядок работ

### Шаг 0. Ветка и первые коммиты

- Ветка `feat/key-rate-phase-2-brent` создана от `main` в `.worktrees/key-rate-phase-2-brent`.
- Первый коммит: спека, план и research-аудит (`docs: add moex market spec, plan and source audit`).

### Шаг 1. Реестр `moex/`

1. `moex/importers.go` — `init()` с `stockPrices`, `ofzCurve`, `keyrateFutures`, `moexMarket`.
2. Убрать `init()` из `moex/ofz_curve.go` и `moex/keyrate.go`.
3. `moex/importers_test.go` — литеральный список имён, `Name()` каждого совпадает с ожидаемым; тест не даёт забыть регистрацию.

### Шаг 2. Разбор (`moex/market_parse.go`)

1. `contractMonth` и `parseContractMonth(shortname, asset)` — `BR-10.09` → `{2009, 10}`, `COPPER-12.24` → `{2024, 12}`; мусор — ошибка.
2. `futuresBar{secid, month, year, date, settle, close, volume, openPosition}`.
3. `parseFuturesHistory(block)` — проверка колонок `TRADEDATE`, `SECID`, `CLOSE`, `SETTLEPRICE`, `VOLUME`, `OPENPOSITION`, `SHORTNAME`, `ASSETCODE`; строки с пустым/`null` `ASSETCODE` пропускаются; `null` в `CLOSE`/`SETTLEPRICE` — 0; `VOLUME`/`OPENPOSITION` — `uint64` с проверкой.
4. `parseFuturesSecids(block, asset)` — `ASSETCODE == asset`, `SHORTNAME` разбирается как контракт (опционы и спреды отбрасываются), `nil` ASSETCODE = «не наш актив», дедуп.
5. `frontMonthPoints(bars)` — на каждую дату минимальный `(year, month)` с `value = settle > 0 ? settle : close`; `<= 0` — пропуск.

### Шаг 3. Транспорт (`moex/market.go`)

1. Константы:
   - `moexMarketTable = "moex_market"`, DDL;
   - `marketInsert`;
   - `brentAsset = "BR"`, `brentSeries = "brent_front"`, `brentStart = "2009-10-01"`;
   - `futuresSecuritiesPath`, `futuresByDatePath`, `futuresHistoryPath`.
2. `fetchFuturesSecidsOnDate(ctx, date, asset)` — `date=`, `assetcode=`, блок `history`; при пустом ответе — `nil, nil`.
3. `discoverFuturesSecids(ctx, asset, from)`:
   - месяцы от `from` до сегодня; для каждого — 15-е, при пустом ответе 14…10-е;
   - плюс текущий список `securities.json`;
   - дедуп, отбор по `ASSETCODE`.
4. `fetchFuturesHistory(ctx, secid, from)` — пагинация `start` по 100, блоки `history` + `history.cursor`.
5. `fetchMarketFutures(ctx, spec, from)` — discovery + истории + `frontMonthPoints`.
6. `moexMarket.Import`:
   - DDL до сети;
   - `from` = `max(date) − 400 дней` или `spec.start`;
   - один `PrepareBatch`; `Append(marketSeries, date, value)` с проверкой; `Send`;
   - `log.Infof` с числом контрактов/точек;
   - `UpsertSeriesCatalog(moexSeriesMeta)` + `CreateView(v_moex_market)`;
   - ошибки контрактов — `errors.Join`; ноль контрактов/точек — ошибка импорта.
7. RVI/медь в этом PR **не включаются**; структура `futuresSpec` готова к следующему пункту.

### Шаг 4. Каталог и витрина

1. `moex/series_meta.go` — запись `brent_front` (полная: title/unit/frequency/origin/description).
2. `moex/market.go` — `v_moex_market` через `util.View` с комментариями таблицы и колонок.
3. `sql/mcp_kimi_reader.sql` — `GRANT SELECT ON default.v_moex_market TO kimi_reader;`.
4. `scripts/mcp_check.py` — проверка витрины: читается, `series` содержит `brent_front`, `count() > 0`, `min/max(date)` непустые.

### Шаг 5. Расписание и документация

1. `dagu/moex.yaml` — шаг `moex_market` после `keyrate_futures`, retry 3 × 300 с.
2. `ARCHITECTURE.md`:
   - §2 — состав `moex/`;
   - §6.1 — импортёр `moex_market`;
   - §6.2 — DDL `moex_market`;
   - §6.3 — `v_moex_market`;
   - §7 — шаг dagu;
   - §8 — реестр и `Name()`;
   - строка «Последнее обновление».
3. `README.md` — строка `moex` в таблице источников: `moex_market`, `v_moex_market`.
4. `docs/IMPORTERS.md` — раздел `moex_market`.
5. `docs/IMPORTER_PACKAGE_STANDARD.md` §18 — `moex`: реестр появился; точка контроля не объявляется.
6. `docs/ROADMAP_KEY_RATE.md`:
   - §4.2 №13 — источник MOEX ISS `BR`, ссылка на #113 и спеку; решение владельца 2026-10-20;
   - §6 фаза 2 — статус `⏳`/`In Progress` для #113; новый пункт `phase-2-moex-market` (RVI + медь) — `Todo`.
   - №14 Urals — комментарий с проверкой MOEX, `Blocked` (без кода в этом PR; отдельный docs-only шаг).

### Шаг 6. Тесты

1. `moex/market_parse_test.go` — 5 тестов из спеки.
2. `moex/market_test.go` — `httptest`:
   - `TestFetchFuturesHistoryPaginates` — 100 строк на первой странице, пустая вторая; `from`, `start`, `limit`;
   - `TestDiscoverFuturesSecids` — `date`-ответ + `securities`-ответ; спреды отброшены;
   - `TestFetchFuturesSecidsOnDateEmpty` — пустая страница → `nil, nil`.
3. `moex/importers_test.go` — реестр.
4. `moex/moex_test.go` — каталог.
5. `go test -race -count=1 ./moex/`.

### Шаг 7. Приёмка

1. `make env-check`, `make ch-status`.
2. `make all`.
3. `make import STAT=moex_market` — первый прогон: Brent 3 916 точек, `2009-10-01 … 2026-10-19`; записать `Imported N`, `min(date)`, `max(date)`.
4. Повтор — `count() FINAL` не изменился, тот же `max(date)`.
5. `make mcp-user` (если гранты менялись), `make mcp-check` — новый блок `v_moex_market` OK.
6. `dagu validate dagu/moex.yaml`, `make docs-check`.
7. MCP: `SELECT series, count(), min(date), max(date) FROM v_moex_market GROUP BY series`.
8. Протокол — в тело приёмочного коммита, в этот план и комментарием в PR.

### Шаг 8. PR и трекер

- Один PR в `main`; описание — источник, схема, запуск, пример проверки, `key-rate/phase-2-brent`, `Closes #113` (вне backticks).
- Протокол приёмки — комментарием в PR (стандарт §16.1 п. 8).
- При открытии PR: Project V2 `Status=In Review`, поле `PR`.
- После мержа: `Done`, `Branch`/`PR`/`Spec / Plan`/`Acceptance` заполнены.

## 3. Критерий приёмки

- `make all` зелёный; `go test -race -count=1 ./moex/` зелёный.
- `make import STAT=moex_market`: ≥3 900 точек Brent, `2009-10-01 … 2026-10-19`; повтор идемпотентен (`count() FINAL` не меняется).
- `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА, включая `v_moex_market`.
- `dagu validate dagu/moex.yaml` — exit 0; `make docs-check` — зелёный.
- `ARCHITECTURE.md`, `README.md`, `IMPORTERS.md`, роадмап обновлены тем же PR.
- Urals/VIX/CRB не подменены; разрыв зафиксирован в аудите.

## 4. Вне объёма

- RVI и медь (следующий пункт `phase-2-moex-market`).
- FRED, архив авторов, VIX/CRB.
- Изменение #114 (только комментарий и статус `Blocked` отдельным docs-only шагом).

## 5. Протокол живой приёмки (2026-10-20)

- `make all` — зелёный (golangci-lint `0 issues`, `go vet` чисто, `go test -race` все `ok`, обе сборки).
- `make import STAT=moex_market` — `moex_market: BR: 179 contracts, 33074 bars, 3916 points`; `Imported 3916 rows`; витрина `v_moex_market` — **3 916** строк, **3 916** уникальных дат, `2009-10-01 … 2026-10-19`.
- Повтор — `moex_market: BR: 20 contracts, 2044 bars, 279 points`, `Imported 279 rows` (окно 400 дней); `count() FINAL` — те же **3 916**, `max(date)` 2026-10-19 (идемпотентно).
- MCP: `brent_front` 3 916 (2009-10-01…2026-10-19); спот `2009-10-05 = 66.45`, `2026-10-19 = 108.93`; `v_series_catalog` — `brent_front`, `USD за баррель`, `D`.
- `make mcp-check` — **ПРОВЕРКА ПРОЙДЕНА**; новые проверки: `v_moex_market` непуста, `brent_front` есть, сырая `moex_market` → отказ 497.
- `dagu validate dagu/moex.yaml` — exit 0; `make docs-check` — 38 страниц, 33 теста OK.
- Живой прогон вскрыл `ASSETCODE = null` у спредов/служебных инструментов: разбор переведён на `rowStringOrEmpty`, `nil` = «не наш актив»; добавлен red-check-тест.

**Статус:** `✅ сделан этим PR` (In Review), не `Done`: мерж за владельцем.
