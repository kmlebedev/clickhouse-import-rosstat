# План: T5 #280 — витрина `v_rosstat_ppi`, каталог и грант MCP

**Пункт:** `key-rate/phase-4-rosstat-ppi-sa` T5, issue [#280](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/280), parent [#266](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/266).
**Спека:** [2026-10-21-rosstat-ppi-view-design.md](../specs/2026-10-21-rosstat-ppi-view-design.md).
**Ветка / worktree:** `feat/rosstat-ppi-view`, `.worktrees/rosstat-ppi-view`, база `origin/main` после мержа PR #308.

## 1. Решения

| Вопрос | Решение |
|---|---|
| Владелец витрины | `ppi_sa` importer (`publishPPIDerived`), не отдельный `views`-импортёр: он последний писатель, все четыре таблицы уже есть. |
| Колонки | `source, series, date, value, unit, frequency`; `value Nullable(Float64)`. |
| `source` | `rosstat` для сырых, `ppi_sa` для derived — совпадает с `v_series_catalog.source`. |
| Derived-ряды | 20: `ppi_sa:<ряд>:<sa\|mm_sa\|saar\|saar3>`; 5 базовых записей каталога остаются. |
| Веса | `date = makeDate(year,1,1)`, `frequency='A'`, unit `% (доля в структуре весов ИЦП)`. |
| Грант | `GRANT SELECT ON default.v_rosstat_ppi TO kimi_reader` в `sql/mcp_kimi_reader.sql`; сырые четыре таблицы закрыты. |
| Проверки | `scripts/mcp_check.py`: 339 585 строк / 5 408 рядов / 20 NULL, каталог `ppi_sa` 25, spot `ppi_sa:core:saar3`, 497 на четырёх сырых. |

## 2. Файлы

- `ppi/views.go` — `util.View` `v_rosstat_ppi` (SQL, комментарий, 6 колонок).
- `ppi/series_meta.go` — `ppiSAViewSeriesMeta()` (20 записей).
- `ppi/importer.go` — `publishPPIDerived` вместо двух `UpsertSeriesCatalog(ppiSASeriesMeta)`; вызов на полном и no-op путях.
- `ppi/views_test.go` / `ppi/importer_test.go` — тесты витрины и каталога.
- `sql/mcp_kimi_reader.sql` — грант.
- `scripts/mcp_check.py` — блок PPI.
- `ARCHITECTURE.md` §6.3 (описание витрины), §6.1/§6.3 (снять «T5, не реализована»), `README.md`, `docs/IMPORTERS.md`, `docs/LIMITATIONS.md`, `docs/ROADMAP_KEY_RATE.md`.
- Спека/план — первые коммиты.

## 3. Шаги

### Шаг 1. Спека и план — первые коммиты
- `2026-10-21-rosstat-ppi-view-design.md`, `2026-10-21-rosstat-ppi-view.md`.

### Шаг 2. Витрина и каталог
- `ppi/views.go`: SQL по прототипу (проверен live: 339 585 / 5 408 / 20); `Tables` — четыре таблицы; `Comment`/`Columns` — 6 колонок.
- `ppiSAViewSeriesMeta`: 20 записей; unit/frequency/title/description; `origin` базового ряда.
- `publishPPIDerived`: upsert 5+20, `CreateView`; заменить вызовы в `Import`.
- Тесты: имя/таблицы/колонки/SQL; 20 имён и единиц; базовые 5 не изменились.

### Шаг 3. Грант и MCP-проверки
- `sql/mcp_kimi_reader.sql`: грант `v_rosstat_ppi`.
- `scripts/mcp_check.py`: блок PPI (витрина, counts, spot, каталог 25, 497 на сырых).
- `make mcp-user` (применить грант), `make mcp-check`.

### Шаг 4. Живая приёмка
- `make import STAT=ppi_sa` — no-op, но публикует витрину и 20 записей каталога (fingerprint не менялся).
- Проверить `v_rosstat_ppi`: 339 585 / 5 408 / 20; `source` 337 841/1 744; spot `ppi_sa:core:saar3` 2026-08-31 = 6,0788; каталог `ppi_sa` 25; сырые 497.
- Пример запроса из спеки §7.

### Шаг 5. Документация
- `ARCHITECTURE.md` §6.3 — реализованная витрина: колонки, источники, counts, грант; §6.1/§6.2 — снять T5-заглушки.
- `README.md` — строка `ppi`/`rosstat`: витрина; `docs/IMPORTERS.md` — раздел; `docs/LIMITATIONS.md` — оговорки (nullable, generic unit, 336k product scan).
- `docs/ROADMAP_KEY_RATE.md` — T5 `In Review` с числами.
- `make docs-check`.

### Шаг 6. Проверки и PR
- `make all`, `make docs-check`, `dagu validate dagu/ppi.yaml`, `make mcp-check`.
- PR: `Part of #266`, `Closes #280` (вне backticks), пример запроса и counts; протокол — комментарием.

## 4. Критерии готовности

- [ ] `v_rosstat_ppi` видна `kimi_reader` и непуста; сырые четыре таблицы — 497.
- [ ] 339 585 строк / 5 408 рядов / 20 NULL; каталог `ppi_sa` 25.
- [ ] Комментарии таблицы и 6 колонок; 20 записей каталога.
- [ ] `make all`, `make docs-check`, `make mcp-check` — зелёные.
- [ ] Пример запроса и counts в PR; роадмап/`ARCHITECTURE`/`README` синхронны.

## 5. Риски

| Риск | Митигация |
|---|---|
| Тяжёлый scan product 336k | long-форма + фильтр по `series`; оговорка в комментарии |
| NULL в `value` | Nullable + 20 NULL в проверке |
| Грант не применён | `make mcp-user`, `make mcp-check` |
| Витрина публикуется на no-op | `CreateView` идемпотентен; ошибка не пишет версию |
