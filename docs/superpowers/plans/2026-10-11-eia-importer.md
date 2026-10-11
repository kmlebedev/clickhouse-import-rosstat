# План: импортёр EIA (фаза 2 роадмапа золота)

Спека: [docs/superpowers/specs/2026-10-11-eia-importer-design.md](../specs/2026-10-11-eia-importer-design.md).
Пункт роадмапа: `docs/GOLD_THESIS_DATA_ROADMAP.md` §2, «Фаза 2 — автоматизация EIA».
Приёмка: `distillate_stocks` в `v_gold_dashboard` обновляется без ручного ввода, `stale=0`
после каждой среды; `make mcp-check` зелёный.

Исполнение — через `superpowers:subagent-driven-development` (диспатч coder-субагентов),
затем `superpowers:requesting-code-review` и `superpowers:verification-before-completion`.
Один PR в конце.

## Предпосылки

1. dev-ClickHouse поднят (`make ch-status` → отвечает).
2. Ветка `feat/eia-importer` создана; спека и этот план — первыми коммитами.
3. `EIA_API_KEY` задан в `~/.config/rosstat/env` (подтверждено 11.10.2026, значение не выводим).

## Шаг 1. Пин-коды серий по фасетам API (spike)

С ключом из env (`make` подключает файл, секрет не печатать):
- `/v2/petroleum/stoc/wstk/data/` — подтвердить WDISTUS1 (запасы дистиллятов, thousand barrels);
- `/v2/petroleum/pnp/wiup/data/` — подтвердить код загрузки НПЗ (ожидается WUPRUUS2, percent).

Фактические коды и единицы фиксируются в спеке; при расхождении — берётся код из фасета.

## Шаг 2. Пакет `eia/` (coder-субагент)

`eia/eia.go` по шаблону `bls/bls.go`:
- константы `eiaSource = "eia"`, `eiaKeyEnv = "EIA_API_KEY"`, таблица `macro_series`, таймаут 60 с,
  `eiaStart = "2019-01-01"`;
- два маршрута (запасы, загрузка НПЗ), параметры запроса и пагинация по `response.total`
  (`length=5000`, `offset`), сортировка по `period` возрастанию;
- имена рядов: `distillate_stocks`, `refinery_utilization`;
- пустой ключ → ошибка до сети; маскировка ключа в ошибках (`strings.ReplaceAll`);
- парсинг `period`/`value` с проверкой `err`, пропуск записей без значения;
- один `PrepareBatch` на обе серии → `Append` (err checked) → `Send`; затем
  `util.UpsertSeriesCatalog` (2 ряда, source `eia`, frequency `W`);
- витрина `v_eia_petroleum` + `ALTER TABLE ... MODIFY COMMENT` и `COMMENT COLUMN`;
- `main.go` — blind-import пакета `eia`;
- тесты `eia/eia_test.go` офлайн: разбор canned JSON, пропуск пустых значений, ошибки
  даты/значения, маскировка ключа, покрытие мета-рядов (`TestSeriesMetaCoversAllLines`-подобный).

В диспатч-промпт coder обязателен блок про `mcp__jetbrains__*` / `mcp__clickhouse__*` и
инструкции AGENTS.md (см. AGENTS.md, раздел про диспатч).

## Шаг 3. `v_gold_dashboard` (coder-субагент)

`views/gold_dashboard.go`: источники `('eia','webbridge','manual')`, приоритет `eia > webbridge
> manual` через агрегат `GROUP BY series, source` + `coalesce(maxIf(...))`; пороги и `stale`
не меняются; обновить комментарии витрины и строку в `views/gold_views.go` при необходимости.

## Шаг 4. Инфраструктура

- `dagu/eia.yaml` — `type: chain`, `schedule: "43 19 * * 3"`, `overlap_policy: skip`, retry 3×300 с,
  `CLICKHOUSE_IMPORT_STAT=eia`.
- `sql/mcp_kimi_reader.sql` — `GRANT SELECT ON default.v_eia_petroleum TO kimi_reader;`.
- `Makefile` (`env-check`) — добавить `EIA_API_KEY` в список проверок.
- `scripts/mcp_check.py` — проверки: витрина `v_eia_petroleum` видна; каталог source `eia`
  содержит оба ряда; `v_gold_dashboard.distillate_stocks` читается и `stale=0`.

## Шаг 5. Документация (skill `sync-readme-architecture`)

`ARCHITECTURE.md` (§5 env, §6.1 строка `eia` → реализован, §6.2 список source, §6.3 витрина и
описание `v_gold_dashboard`), `README.md` (импортёр, ключ, команда), `docs/GOLD_THESIS_DATA_ROADMAP.md`
(фаза 2 → ✅ с отметкой проверенного).

## Шаг 6. Верификация

1. `make all`; 2. `make import STAT=eia`; 3. MCP-запросы к `v_eia_petroleum` и `v_gold_dashboard`
(`stale=0`); 4. `make mcp-check`; 5. повторный импорт идемпотентен.

## Шаг 7. Ревью и PR

`superpowers:requesting-code-review` → правки → повтор шага 6 → один PR (спека, план, код, DAG,
SQL, docs).
