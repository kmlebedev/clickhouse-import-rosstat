# План: HTML-таблицы ЦБ (#117) → `cbr_zcyc` (#169)

**Дата:** 2026-10-19
**Спека:** [2026-10-19-html-cb-tables-zcyc-design.md](../specs/2026-10-19-html-cb-tables-zcyc-design.md)
**Ветка:** `key-rate/phase-2-cbr-zcyc` (worktree `.worktrees/key-rate-cbr-zcyc`, от `origin/main`)
**Issues:** [#117](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/117), [#169](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/169)

---

## 1. Цель

Закрыть зависимость #169 от общего разбора HTML-таблиц ЦБ (#117), затем дать истории КБД ЦБ: импортёр `cbr_zcyc` (12 теноров, с 04.01.2003), каталог рядов, витрины `v_ofz_curve_compare` и `v_ofz_signal`, гранты и MCP-проверки.

## 2. Порядок работ

### Шаг 0. Ветка и первые коммиты

- Ветка `key-rate/phase-2-cbr-zcyc` уже создана от `origin/main` в `.worktrees/key-rate-cbr-zcyc`.
- Не трогать ветку `point-in-time` и её незакоммиченный `docs/superpowers/specs/2026-10-19-point-in-time-design.md` в основном checkout.
- Первый коммит: эта спека и план (`docs: add cbr html tables and zcyc spec and plan`).

### Шаг 1. `util`: разбор HTML-таблиц и форма `UniDbQuery` (#117)

1. `util/html.go`: `HTMLTable{Headers, Rows}`, `ErrHTMLTableNotFound`, `ParseHTMLTable(body, selector)`; goquery, без сети, `TrimSpace`, `colspan/rowspan` не разворачиваются.
2. `util/unidbquery.go`: `UniDbQueryURL(base, from, to)` (формат `02.01.2006`, сохранение query базы), `IsUniDbQueryPage(body)` (маркер `UniDbQuery.Posted`).
3. Фикстура `util/testdata/unidbquery.html` — реальная урезанная страница ЦБ (форма + таблица, строки данных урезаны).
4. Тесты `util/html_test.go`, `util/unidbquery_test.go`: заголовки/строки/spot-ячейка, `ErrHTMLTableNotFound`, true/false маркера, точные параметры URL.
5. `go test -race -count=1 ./util/`.

### Шаг 2. `cbr_zcyc`: разбор (#169)

1. `cbr/zcyc_parse.go`: `zcycPoint{tenor, date, value}`, `zcycRows(body)`.
   - `util.ParseHTMLTable`; `ErrHTMLTableNotFound` + `IsUniDbQueryPage` → `nil, nil` (пустая выборка); иначе ошибка.
   - последняя строка заголовков → карта `col → tenor`; нормализация `0,25 → 0.25`, `1,00 → 1`; неизвестный тенор — warn и пропуск; ни одного известного — ошибка.
   - дата `02.01.2006`; пустая ячейка — пропуск; непустая непарсящаяся — ошибка; таблица со строками, но 0 точек — ошибка.
2. Фикстуры `cbr/testdata/zcyc_2003.html` (реальная страница 2003, ~12 строк, сохранить 30.12.2003 и 04.01.2003), `zcyc_2026.html` (2–3 строки, включая 16.10.2026), `zcyc_empty.html` (реальный ответ пустого диапазона).
3. Тесты `cbr/zcyc_test.go`: точные числа строк, spot-значения, пустая выборка, заглушка, битое значение, `checkCbrSeriesMeta`, чанкер.

### Шаг 3. `cbr_zcyc`: импортёр и расписание

1. `cbr/zcyc.go`: DDL
   ```sql
   CREATE TABLE IF NOT EXISTS %s (
       tenor LowCardinality(String),
       date Date,
       yield Float64
   ) ENGINE = ReplacingMergeTree ORDER BY (date, tenor);
   ```
   константы `cbr_zcyc`, `https://www.cbr.ru/hd_base/zcyc_params/`, `2003-01-04`, 4 года, селектор `.table-wrapper table.data`, пауза 500 мс.
2. `cbrZcycCraw`: `max(date)` → `from`; `to = сегодня`; окна по 4 года; `UniDbQueryURL` → `GetPage` → `zcycRows`; пустое историческое окно — ошибка, пустое последнее — no-op; точки в память → один `PrepareBatch`/`Append`/`Send`; первый прогон с нулём — ошибка; повтор при `max(date)=сегодня` — без сети.
3. `cbr/importers.go`: `{cbrZcycImporter, "cbr_zcyc", ""}` в конец; комментарий 21 → 22.
4. `dagu/cbr.yaml`: шаг `cbr_zcyc` 22-м, последним, retry 3 × 300 с.
5. `cbr/importers_test.go`: `cbr_zcyc` в `keyScheduled`; префлайт не объявлять.

### Шаг 4. Каталог и витрины

1. `cbr/series_meta.go` (или `cbr/zcyc_meta.go`): 12 `util.SeriesMeta`, `source='cbr'`, `series` = тенор, unit `% годовых`, frequency `D`, origin — таблица/колонка, description — уровень, история, пропуски, ссылки на витрины.
2. `cbr/views.go`:
   - `v_ofz_curve_compare`: union `cbr_zcyc` + `ofz_curve` (MOEX `1y/3y/5y/10y` → `1/3/5/10`), `maxIf` → `cbr_yield`, `moex_yield`, `spread_pp`; комментарии таблицы и колонок.
   - `v_ofz_signal`: `ofz_1y/2y/10y`, `key_rate` (ASOF LEFT JOIN с `1 AS _one`), спреды, наклон, `inflation_yoy` (произведение 12 индексов `rosstat_ipc_mes`), `real_yield_10y_pp`; комментарии и оговорки (NULL, лаг ИПЦ, ставка с 2013-09-17).
   - `afterCbrZcyc(meta)`: каталог → compare → signal.
3. `sql/mcp_kimi_reader.sql`: гранты на обе витрины.
4. `scripts/mcp_check.py`: витрины непусты, каталог 12, сырая `cbr_zcyc` → 497.

### Шаг 5. Документация

- `ARCHITECTURE.md`: §2, §3 (22 записи), §6.1 (импортёр), §6.2 (DDL), §6.3 (витрины), §7 (22 шага, хвост), §8 п. 13 (без префлайта), хелперы `util`.
- `README.md`: строка `cbr` — КБД, `cbr_zcyc`, витрины.
- `docs/IMPORTERS.md`: строка `cbr` + отдельный абзац о `cbr_zcyc` (источник, 12 теноров, лимит 5 лет → окна 4 года, инкремент, без префлайта, формулы, оговорка об ИПЦ).
- `docs/IMPORTER_PACKAGE_STANDARD.md`: §4.1 (HTML-таблицы), §17 (эталон `cbr/zcyc.go`).
- `docs/ROADMAP_KEY_RATE.md`: §4.2 №9 ✅; §4.3 №9 — исправить «лимита выборки нет» на «≈5 лет на запрос, 4-летние окна»; §6 фаза 2 — ✅ для #117/#169 с критериями; абзацы о новых пунктах и первой публикации.
- `docs/ROADMAP_INDEX.md` — не меняется.

### Шаг 6. Приёмка

1. `make env-check`, `make ch-status`; если ClickHouse не отвечает — попросить пользователя `make ch-up`.
2. `make all`; `go test -race -count=1 ./util/ ./cbr/`.
3. `dagu validate dagu/cbr.yaml`.
4. `make import STAT=cbr_zcyc` — первый прогон: 6 окон, 2003-01-04 … сегодня, 12 теноров, ≈70 тыс. строк; записать `Imported N`, `min(date)`, `max(date)`, `uniqExact(tenor)`.
5. Повтор — идемпотентный no-op (при `max(date)=сегодня` без сетевых запросов), `count()` не меняется.
6. `make mcp-user`, `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.
7. MCP: `v_ofz_curve_compare` на 2026-10-09/10 — КБД 16,85 vs MOEX 16,8486; `v_ofz_signal` на 2026-10-16 — 16,51 − 14,00 = 2,51 п.п., наклон 2,74, реальная ≈10,16; `cbr_zcyc` → 497.
8. Протокол — в тело приёмочного коммита, в этот план и комментарием в PR.

### Шаг 7. PR и трекер

- Один PR в `main`; описание — источник, схема, запуск, пример проверки, roadmap-ID, `Closes #117`, `Closes #169` (вне backticks).
- Протокол приёмки — комментарием в PR (стандарт §16.1 п. 8).
- После мержа — Project V2: `#117`/`#169` → `Done`, поля `Branch`/`PR`/`Spec / Plan`/`Acceptance`.

## 3. Критерий приёмки

- `make all` зелёный; `go test -race -count=1 ./util/ ./cbr/` зелёный.
- Первый прогон: ≥20 лет (2003-01-04 … сегодня), 12 теноров, ≈70 тыс. строк; повтор — no-op.
- `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; `dagu validate` — exit 0.
- Обе витрины читаются через MCP, значения сходятся со спотами (КБД vs MOEX; ofz − key_rate).
- `ARCHITECTURE.md`, `README.md`, `IMPORTERS.md`, роадмап обновлены тем же PR; статусы #117/#169 — ✅.

## 4. Вне объёма

- Импортёры MIACR/MIACR-IG и ликвидности (#10/#11) — #117 даёт только хелпер.
- `phase-2-ofz-debt-stress` (#170), Grafana, публикационные артефакты.
- Изменение `v_ofz_curve` (MOEX) и `cbr_key_rate`.
- Ветка `point-in-time` и её файлы.
