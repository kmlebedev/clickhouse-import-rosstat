# План: импортёр `fedstat_weekly_prices` (ЕМИСС 37426)

**Дата:** 20 октября 2026 г.
**Пункт:** `key-rate/phase-4-emiss-weekly-prices` (#263), Task 1 #264, Task 2 #265.
**Спека:** [2026-10-20-fedstat-37426-weekly-prices-design.md](../specs/2026-10-20-fedstat-37426-weekly-prices-design.md).
**Research:** [2026-10-20-fedstat-37426-weekly-prices.md](../../research/2026-10-20-fedstat-37426-weekly-prices.md).

## 0. Ветка и границы

- Worktree `.worktrees/fedstat-weekly-prices`, ветка `feat/fedstat-weekly-prices` от `origin/main`.
- Один PR: Task 1 (research/spec/plan) + Task 2 (Go). ID пункта в issue остаётся `phase-4-emiss-weekly-prices`; пакет/импортёр — по источнику `fedstat`.
- Не трогать корневой worktree, `rosstat/`, `financial/`, чужие таблицы и витрины.

## 1. Артефакты Task 1 (первые коммиты)

- [x] `docs/research/2026-10-20-fedstat-37426-weekly-prices.md` — протокол, покрытие, дата-правило, 2023–2025, terms, риски.
- [x] `docs/research/2026-10-20-fedstat-37426-products.csv` — 158 товаров: `filter_id`, `sdmx_code`, имя, флаги 2008–2022/2026, точки, крайние даты.
- [x] `docs/superpowers/specs/2026-10-20-fedstat-37426-weekly-prices-design.md`.
- [x] `docs/superpowers/plans/2026-10-20-fedstat-37426-weekly-prices.md`.
- [ ] Фикстуры в `fedstat/testdata/` (после создания пакета).

## 2. Пакет `fedstat/`

| Шаг | Файл | Содержание |
|---|---|---|
| 2.1 | `fedstat/importers.go` | реестр `fedstatImporters`, одна запись, единственный `init()`, литерал таблицы, `countPoints=""` |
| 2.2 | `fedstat/fedstat_weekly_prices.go` | константы (indicator 37426, регионы 1688487/1849012, unit 950351, годы, базовый URL), DDL, `fedstatDownload` (GET+POST, retry), XML-структуры, `fedstatParse`, `fedstatDate`, карта 158 товаров, гейт, `Import()` |
| 2.3 | `fedstat/series_meta.go` | `fedstatSeriesMeta()` — 158 `util.SeriesMeta`; `afterFedstatMacro` |
| 2.4 | `fedstat/views.go` | `v_fedstat_weekly_prices` + комментарии |
| 2.5 | `fedstat/testdata/` | реальные урезанные SDMX-срезы 2008/2016/2018/2021/2026 + HTML-фрагмент токена |
| 2.6 | `main.go` | blank-import `_ ".../fedstat"` |

Требования: один `PrepareBatch` на прогон; `Abort` на ошибке; все `Parse*`/`Append`/`Scan` с проверкой; `util.HttpClient`; без сети в `init()`; logrus.

## 3. Тесты

- [ ] `fedstat/fedstat_weekly_prices_test.go`:
  - `fedstatDate`: 2008, 2016, 2018 (`17сентября`), 2021 (`1 неделя 2021`), 2022 (`51 неделя 2022`), 2026 (`39 неделя 2026`), год метки против `Time`, невалидная дата.
  - Парсер: точки/spot-значения по фикстурам, десятичная запятая, пустое значение, дубли (same/diff), неизвестный `s_grtov`, чужой `s_OKATO`, HTML вместо XML, отсутствие токена.
- [ ] `fedstat/transport_test.go`: `httptest` — форма POST (158 товаров, 16 лет, 2 региона, unit, оси, токен, Referer/Origin), 503→retry.
- [ ] `fedstat/importers_test.go`: энфорсер реестра + `dagu/fedstat.yaml`.
- [ ] `fedstat/series_meta_test.go`: 158 записей, уникальность, `unit/frequency/source`, 143 исторических, 15 2026-only помечены.
- [ ] DDL-тест: `Float64`, `ReplacingMergeTree`, `LowCardinality`, ключ.

## 4. Расписание и MCP

- [ ] `dagu/fedstat.yaml`: weekly, один шаг, retry 3×300 с.
- [ ] `sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_fedstat_weekly_prices`.
- [ ] `scripts/mcp_check.py`: витрина непуста (56 133), 158 рядов, 2 региона; каталог 158; сырая таблица → 497.

## 5. Документация

- [ ] `docs/IMPORTERS.md` — строка `fedstat`.
- [ ] `ARCHITECTURE.md` §2/§6.1/§6.2/§6.3/§7/§8.
- [ ] `README.md` — таблица пакетов/импортёров (если меняется контракт).
- [ ] `docs/LIMITATIONS.md` — CSRF, 2023–2025, два региона, нет PIT/архива, rate limit.
- [ ] `docs/ROADMAP_KEY_RATE.md` — статус после приёмки.

## 6. Проверки

```bash
make all
make import STAT=fedstat_weekly_prices
# повтор
make import STAT=fedstat_weekly_prices
make mcp-check
make docs-check
dagu validate dagu/fedstat.yaml
```

Ожидание: первый прогон — 56 133 строки, 158 товаров, 2 региона, крайние даты 2008-01-09 … 2022-12-26 и 2026-10-05 … 2026-10-12; повтор — 0 строк без сети.

## 7. Протокол живой приёмки (2026-10-20)

- Команда: `make import STAT=fedstat_weekly_prices`.
- Строк: **56 133** (`Imported 56133 rows of fedstat_weekly_prices`), 16 годовых POST с паузой 30 с, ~8 мин 42 с.
- Товаров / регионов: **158 / 2** (РФ — 143 ряда / 55 905 точек; РФ без новых субъектов — 114 / 228).
- Крайние даты: `2008-01-09 … 2022-12-26` (РФ) и `2026-10-05 … 2026-10-12` (РФ без новых субъектов).
- Повтор: «все 158 товаров, 2 региона и 16 лет на месте», `Imported 0 rows` без сети.
- Spot-сверка (Говядина / Бензин АИ-92 / Яйца): Говядина 140,86 @2008-01-09, 311,8 @2016-06-13, 758,54 @2026-10-05, 760 @2026-10-12; АИ-92 20,4 / 34,56 / 74,72 / 74,71; Яйца 35,94 / 51,57 / 103,41 / 105,63 — совпало с XML источника.
- `make mcp-check`: три проверки fedstat — **OK** (витрина 56 133 / 158 / 2; каталог 158; сырая таблица 497). Общий прогон — 1 провал на **чужом** rosstat-ожидании: каталог 169 vs 165 (live-БД впереди check-константы, вне пункта); агрегат бюллетеней после merge main проходит (760 >= 760).
- `make all`: зелёный (golangci-lint 0 issues, `go vet`, `go test -race`, обе сборки).
- `make docs-check`: зелёный (33 теста).
- `dagu validate dagu/fedstat.yaml`: exit 0 (штатный warn dagu про `auth.mode`).

## 8. Риски

| Риск | Лечение |
|---|---|
| 503 при частых POST | один POST на год с паузой 30 с, backoff 30/60/120 с, retry DAG |
| HTML вместо XML | проверка `Content-Type`/корня; ошибка, не «0 строк» |
| Токен/сессия | GET+POST одним `util.NewDefaultTLSClient()` на прогон; токен на один год |
| Годовой ответ в `DataSet` | парсер читает и `GenericData > DataSet > Series`, и прямые `Series` |
| 2023–2025 | зафиксировано как разрыв; не расширять молча |
| 2026 — другой регион | отдельный `region`, не смешивать |
| Ревизии | нет PIT/архива; ограничение в LIMITATIONS |
| Долгий прогон | 41 с POST + батч; лог по этапам |
