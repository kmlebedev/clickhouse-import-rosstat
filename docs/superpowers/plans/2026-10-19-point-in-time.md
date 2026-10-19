# Point-in-time слой (issue #126) — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Добавить к `cbr_dip_bulletin` три витрины point-in-time слоя: нормализованную `v_point_in_time`, параметризованную `v_point_in_time_asof(as_of=…)` и `v_point_in_time_version` для записи версии данных рядом с прогнозом.

**Architecture:** Слой — не физическая таблица, а три `util.View` над уже винтажной `cbr_dip_bulletin` (18 000 точек, 49 выпусков). Определения живут в `cbr/pit_views.go`; `cbrDipStat.Import` вызывает `ensurePitViews` после импорта выпусков и на no-op-пути, поэтому витрины самолечатся без отдельного импортёра и шага dagu. Срез на дату — параметризованная витрина ClickHouse `{as_of:Date}` со строгой границей `published_at < as_of`.

**Tech Stack:** Go, `util.View`/`util.CreateView` (`util/views.go`), ClickHouse 26.10 (parameterized views), Python (`scripts/mcp_check.py`), MCP `kimi_reader`.

**Spec:** `docs/superpowers/specs/2026-10-19-point-in-time-design.md`

## Global Constraints

- Секреты — только через `os.Getenv`; в коде, комментариях и командах нет паролей и токенов (правило 1 AGENTS.md).
- Новых сетевых вызовов нет: слой читает только ClickHouse; `init()` без сети (правило 3).
- Новых таблиц и DDL нет: слой — витрины над существующей `cbr_dip_bulletin`.
- Ошибки не проглатывать: каждый `CreateView`/`Exec` проверяется (правило 6).
- Витрины создаются `util.CreateView`; комментарии таблицы и колонок — `Comment`/`Columns` (стандарт §10.2–10.3).
- Сырая `cbr_dip_bulletin` агенту не выдаётся; гранты — только на новые витрины (стандарт §10.4).
- `make all` должен быть зелёным; `make docs-check` — зелёный; `dagu validate dagu/cbr.yaml` — exit 0.
- Коммиты — кратко, по-английски, в духе истории (`add point-in-time views`).

## Review Focus

Пять классов входа/отказа, которые не покрываются разбором фикстур; их поведение должно быть явно проверено:

1. **`as_of` ровно в дату публикации** — значение этой даты не входит в срез (`<`, не `<=`); проверяется тестом `TestCbrPitAsOfViewContract` (SQL-фрагмент) и живой приёмкой на 2024-02-06/07.
2. **Два выпуска в одну дату публикации** — `argMax` по кортежу `(published_at, vintage)`, а не по одной дате; проверяется `TestCbrPitAsOfViewContract`.
3. **Ряд исчез из нового выпуска** — последняя известная версия остаётся видимой; это свойство `argMax`-группировки; проверяется `TestCbrPitAsOfViewContract` (нет `WHERE period_date` / нет фильтра «только последний выпуск»).
4. **Параметризованная витрина без аргументов** — у неё нет схемы и колонковых комментариев; `Columns` пуст, табличный комментарий содержит форму вызова; проверяется `TestCbrPitViewsContract`.
5. **Стабильность `data_version`** — повтор даёт тот же хэш, ревизия меняет; проверяется `TestCbrPitViewsContract` (наличие `groupBitXor(cityHash64(` в `v_point_in_time_version`) и живым повтором.

---

## File Structure

| Файл | Ответственность |
|---|---|
| `cbr/pit_views.go` (create) | три определения `util.View`, список `cbrPitViews`, `ensurePitViews` |
| `cbr/pit_views_test.go` (create) | офлайн-контракт определений: имена, `Tables`, SQL-фрагменты, комментарии |
| `cbr/dip_bulletin.go` (modify) | вызов `ensurePitViews` после цикла и на no-op-пути |
| `cbr/dip_bulletin_test.go` (modify) | `Exec` у `dipFakeConn`, `TestEnsurePitViewsCreatesAll` |
| `sql/mcp_kimi_reader.sql` (modify) | гранты на три витрины |
| `scripts/mcp_check.py` (modify) | проверки слоя и известной ревизии |
| `ARCHITECTURE.md`, `README.md` (modify) | схема/витрины/гранты |
| `docs/IMPORTERS.md`, `docs/LIMITATIONS.md`, `docs/KEY_RATE_MODEL.md`, `docs/ROADMAP_KEY_RATE.md` (modify) | эксплуатация, ограничения, статус |

---

### Task 1: Определения трёх витрин

**Files:**
- Create: `cbr/pit_views.go`
- Create: `cbr/pit_views_test.go`

**Interfaces:**
- Produces: `cbrPitView`, `cbrPitAsOfView`, `cbrPitVersionView` типа `util.View`; `cbrPitViews []util.View` в порядке `[v_point_in_time, v_point_in_time_asof, v_point_in_time_version]`.

- [ ] **Step 1: Написать падающий тест `TestCbrPitViewsContract`**

В `cbr/pit_views_test.go`:

```go
package cbr

import (
	"strings"
	"testing"
)

func TestCbrPitViewsContract(t *testing.T) {
	names := make([]string, 0, len(cbrPitViews))
	for _, v := range cbrPitViews {
		names = append(names, v.Name)
	}
	want := []string{"v_point_in_time", "v_point_in_time_asof", "v_point_in_time_version"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("витрины слоя = %v, want %v", names, want)
	}
	for _, v := range cbrPitViews {
		if len(v.Tables) != 1 || v.Tables[0] != "cbr_dip_bulletin" {
			t.Errorf("%s: Tables = %v, want [cbr_dip_bulletin]", v.Name, v.Tables)
		}
		if strings.TrimSpace(v.Comment) == "" {
			t.Errorf("%s: пустой комментарий таблицы", v.Name)
		}
	}
	if !strings.Contains(cbrPitView.Select, "'cbr_dip_bulletin' AS source") ||
		!strings.Contains(cbrPitView.Select, "date AS period_date") ||
		!strings.Contains(cbrPitView.Select, "publication_date AS published_at") ||
		!strings.Contains(cbrPitView.Select, "release_id AS vintage") ||
		!strings.Contains(cbrPitView.Select, "release AS vintage_label") {
		t.Errorf("v_point_in_time: не та нормализация колонок: %s", cbrPitView.Select)
	}
	if len(cbrPitView.Columns) != 7 {
		t.Errorf("v_point_in_time: колонковых комментариев %d, want 7", len(cbrPitView.Columns))
	}
	if !strings.Contains(cbrPitVersionView.Select, "groupBitXor(cityHash64(") ||
		!strings.Contains(cbrPitVersionView.Select, "hex(") ||
		!strings.Contains(cbrPitVersionView.Select, "GROUP BY source") {
		t.Errorf("v_point_in_time_version: не та версия набора: %s", cbrPitVersionView.Select)
	}
	if len(cbrPitVersionView.Columns) != 6 {
		t.Errorf("v_point_in_time_version: колонковых комментариев %d, want 6", len(cbrPitVersionView.Columns))
	}
}

// TestCbrPitAsOfViewContract — семантика среза: строгая граница по дате публикации, тай-брейк
// версий во всех argMax, группировка по (source, series, period_date) и отсутствие фильтров
// «только последний выпуск» — HAVING или условия по period_date скрыли бы последнюю известную
// версию ряда, исчезнувшего из нового выпуска (спека §2.3).
func TestCbrPitAsOfViewContract(t *testing.T) {
	sel := cbrPitAsOfView.Select
	if !strings.Contains(sel, "publication_date < {as_of:Date}") {
		t.Errorf("as-of: нет строгой границы: %s", sel)
	}
	if !strings.Contains(sel, "argMax(p.value, (p.published_at, p.vintage))") ||
		!strings.Contains(sel, "argMax(p.vintage, (p.published_at, p.vintage))") ||
		!strings.Contains(sel, "argMax(p.vintage_label, (p.published_at, p.vintage))") {
		t.Errorf("as-of: нет тай-брейка (published_at, vintage) во всех argMax: %s", sel)
	}
	if !strings.Contains(sel, "GROUP BY p.source, p.series, p.period_date") {
		t.Errorf("as-of: группировка не по (source, series, period_date): %s", sel)
	}
	if strings.Contains(sel, "HAVING") {
		t.Errorf("as-of: HAVING ограничил бы срез последним выпуском: %s", sel)
	}
	if strings.Count(sel, "WHERE") != 1 || strings.Contains(sel, "period_date <") || strings.Contains(sel, "period_date >") {
		t.Errorf("as-of: единственный WHERE должен быть publication_date < as_of, без фильтра period_date: %s", sel)
	}
	if len(cbrPitAsOfView.Columns) != 0 {
		t.Errorf("as-of: у параметризованной витрины не может быть колонковых комментариев, got %d", len(cbrPitAsOfView.Columns))
	}
}
```

- [ ] **Step 2: Убедиться, что тест падает**

Run: `go test ./cbr -run TestCbrPitViewsContract -count=1`
Expected: FAIL — `undefined: cbrPitViews` (и `undefined: cbrPitView`).

- [ ] **Step 3: Реализовать `cbr/pit_views.go`**

Три `util.View` с `Tables: []string{cbrDipTable}`. SQL — дословно из спеки §2.2. Комментарии:
- `cbrPitView`: что слой нормализует, что `period_date` — не дата публикации, что для среза использовать `v_point_in_time_asof(as_of='…')`, что сейчас источник один (`cbr_dip_bulletin`), а `vintage_label` — номер выпуска. `Columns` — 7 записей (`source`, `series`, `period_date`, `value`, `published_at`, `vintage`, `vintage_label`).
- `cbrPitAsOfView`: форма вызова, строгая граница, тай-брейк `(published_at, vintage)`, пример с `unemployment_sa` 2.885274 / 2.916127, предупреждение, что у параметризованной витрины нет колонковых комментариев (смысл — в `v_point_in_time`). `Columns` — пустая map (или `nil`).
- `cbrPitVersionView`: `data_version` — `hex(groupBitXor(cityHash64(...)))`, рекомендация `pit:cbr_dip_bulletin:<data_version>` для `forecast_log.data_version`; `Columns` — 6 записей (`source`, `points`, `vintages`, `first_publication`, `last_publication`, `data_version`).

`cbrPitViews = []util.View{cbrPitView, cbrPitAsOfView, cbrPitVersionView}`.

- [ ] **Step 4: Убедиться, что тест проходит**

Run: `go test ./cbr -run 'TestCbrPitViewsContract|TestCbrPitAsOfViewContract' -count=1`
Expected: PASS.

- [ ] **Step 5: Коммит**

```bash
git add cbr/pit_views.go cbr/pit_views_test.go
git commit -m "add point-in-time views over cbr_dip_bulletin"
```

---

### Task 2: Создание витрин в прогоне `cbr_dip_bulletin`

**Files:**
- Modify: `cbr/pit_views.go`
- Modify: `cbr/dip_bulletin.go:108-153`
- Modify: `cbr/dip_bulletin_test.go:209-238` (фейковое соединение)

**Interfaces:**
- Consumes: `cbrPitViews` из Task 1.
- Produces: `ensurePitViews(ctx context.Context, conn driver.Conn) error`.

- [ ] **Step 1: Написать падающий тест `TestEnsurePitViewsCreatesAll`**

В `cbr/dip_bulletin_test.go` добавить `execs []string` и метод `Exec` у `dipFakeConn`:

```go
func (c *dipFakeConn) Exec(_ context.Context, query string, _ ...any) error {
	c.execs = append(c.execs, query)
	return nil
}
```

Затем тест:

```go
func TestEnsurePitViewsCreatesAll(t *testing.T) {
	conn := &dipFakeConn{row: &dipFakeRow{written: 1}}
	if err := ensurePitViews(context.Background(), conn); err != nil {
		t.Fatalf("ensurePitViews: %v", err)
	}
	joined := strings.Join(conn.execs, "\n")
	for _, name := range []string{"v_point_in_time", "v_point_in_time_asof", "v_point_in_time_version"} {
		if !strings.Contains(joined, "CREATE OR REPLACE VIEW "+name+" ") {
			t.Errorf("нет CREATE OR REPLACE VIEW для %s", name)
		}
	}
	if !strings.Contains(joined, "{as_of:Date}") {
		t.Error("в определении as-of витрины нет параметра {as_of:Date}")
	}
	if strings.Count(joined, "CREATE OR REPLACE VIEW ") != 3 {
		t.Errorf("CREATE OR REPLACE VIEW: %d, want 3", strings.Count(joined, "CREATE OR REPLACE VIEW "))
	}
	// У параметризованной витрины колонковых комментариев быть не может: 7 (base) + 6 (version).
	if got := strings.Count(joined, "COMMENT COLUMN"); got != 13 {
		t.Errorf("COMMENT COLUMN: %d, want 13 (7 base + 6 version)", got)
	}
}
```

- [ ] **Step 2: Убедиться, что тест падает**

Run: `go test ./cbr -run TestEnsurePitViewsCreatesAll -count=1`
Expected: FAIL — `undefined: ensurePitViews`.

- [ ] **Step 3: Реализовать `ensurePitViews` и вызовы**

В `cbr/pit_views.go`:

```go
func ensurePitViews(ctx context.Context, conn driver.Conn) error {
	for _, v := range cbrPitViews {
		created, err := util.CreateView(ctx, conn, v)
		if err != nil {
			return fmt.Errorf("%s: %w", v.Name, err)
		}
		log.Infof("%s created: %t", v.Name, created)
	}
	return nil
}
```

В `cbr/dip_bulletin.go`:
- на no-op-пути (`if len(fresh) == 0`): вызвать `ensurePitViews` до `return 0, nil`; ошибку вернуть;
- после цикла выпусков: `if err = ensurePitViews(ctx, conn); err != nil { errs = append(errs, err) }`;
- вернуть `return count, errors.Join(errs...)`.

Обновить комментарий `Import`: слой создаётся и на no-op-пути (самолечение), сбой витрин не откатывает записанные выпуски.

- [ ] **Step 4: Убедиться, что тесты проходят**

Run: `go test ./cbr -run 'TestEnsurePitViewsCreatesAll|TestCbrPitViewsContract|TestDip' -count=1`
Expected: PASS.

- [ ] **Step 5: Коммит**

```bash
git add cbr/pit_views.go cbr/dip_bulletin.go cbr/dip_bulletin_test.go
git commit -m "create point-in-time views on dip bulletin run"
```

---

### Task 3: Гранты и проверки MCP

**Files:**
- Modify: `sql/mcp_kimi_reader.sql` (после `GRANT SELECT ON default.v_cbr_dip_bulletin`)
- Modify: `scripts/mcp_check.py:1361` (после проверки винтажности, до проверки сырой таблицы)

**Interfaces:**
- Consumes: витрины из Task 1–2.

- [ ] **Step 1: Добавить гранты**

```sql
GRANT SELECT ON default.v_point_in_time TO kimi_reader;
GRANT SELECT ON default.v_point_in_time_asof TO kimi_reader;
GRANT SELECT ON default.v_point_in_time_version TO kimi_reader;
```

- [ ] **Step 2: Добавить проверки в `scripts/mcp_check.py`**

После блока `v_cbr_dip_bulletin винтажна` и до проверки сырой таблицы добавить проверки (использовать существующие `text`, `result_rows`, `expect`):

1. `SELECT count() AS points, uniqExact(vintage) AS vintages, uniqExact(series) AS series FROM v_point_in_time` — `points > 0`, `vintages >= 49`, `series == 8`; сообщение «слой v_point_in_time непуст».
2. `SELECT count() AS n FROM v_point_in_time WHERE period_date > published_at` — `n == 0`; «инвариант периода».
3. `SELECT count() AS n FROM v_point_in_time_asof(as_of='2024-02-06') WHERE source='cbr_dip_bulletin' AND series='unemployment_sa' AND period_date='2023-11-30'` — `n == 0`; «строгая граница среза».
4. `SELECT value, vintage, vintage_label FROM v_point_in_time_asof(as_of='2024-02-07') WHERE source='cbr_dip_bulletin' AND series='unemployment_sa' AND period_date='2023-11-30'` — `value == 2.885274`, `vintage == '47847'`, `vintage_label == '24-01'`.
5. То же для `as_of='2026-09-02'` — `value == 2.916127`, `vintage == '62343'`, `vintage_label == '26-06'`.
6. `SELECT vintages, data_version FROM v_point_in_time_version WHERE source='cbr_dip_bulletin'` — одна строка, `vintages >= 49`, `data_version != ''`.
7. Сырая `cbr_dip_bulletin` уже проверяется ниже — не дублировать; оставить существующую проверку.

- [ ] **Step 3: Проверить синтаксис Python**

Run: `python3 -m py_compile scripts/mcp_check.py`
Expected: exit 0, без вывода.

- [ ] **Step 4: Живой прогон проверок**

Run: `make import STAT=cbr_dip_bulletin && make mcp-user && make mcp-check`
Expected: витрины созданы, `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА (новые проверки OK).

- [ ] **Step 5: Коммит**

```bash
git add sql/mcp_kimi_reader.sql scripts/mcp_check.py
git commit -m "grant and check point-in-time views for mcp"
```

---

### Task 4: Документация и статус роадмапа

**Files:**
- Modify: `ARCHITECTURE.md` (§6.1 `cbr_dip_bulletin`, §6.3 витрины, §6.5 MCP)
- Modify: `README.md` (строка `cbr`)
- Modify: `docs/IMPORTERS.md` (раздел `cbr`/ДИП)
- Modify: `docs/LIMITATIONS.md`
- Modify: `docs/KEY_RATE_MODEL.md` §4
- Modify: `docs/ROADMAP_KEY_RATE.md` (§5 строка `point_in_time`, §6 фаза 4, статус пункта)

- [ ] **Step 1: Обновить `ARCHITECTURE.md`**

- §6.1 в строке `cbr_dip_bulletin`: после описания витрины `v_cbr_dip_bulletin` добавить, что прогон создаёт слой `v_point_in_time*` (три витрины, `ensurePitViews` на no-op-пути).
- §6.3: добавить три абзаца/строки с контрактом, вызовом as-of, строгой границей и версией набора.
- §6.5: перечислить гранты `kimi_reader` на три витрины.

- [ ] **Step 2: Обновить `README.md`**

В строке `cbr` (таблица импортёров) перечислить `v_point_in_time`, `v_point_in_time_asof`, `v_point_in_time_version`; при необходимости — в разделе витрин.

- [ ] **Step 3: Обновить `docs/IMPORTERS.md`**

В разделе `cbr`/ДИП: слой, форма вызова `v_point_in_time_asof(as_of='…')`, строгая граница, пример ревизии, ограничения.

- [ ] **Step 4: Обновить `docs/LIMITATIONS.md`**

Добавить пункт: слой покрывает только ДИП; строгий `<`; параметризованная витрина без колонковых комментариев/`DESCRIBE` без аргумента; «выпуск не показал ряд» — последняя версия остаётся; замена файла/даты под тем же `id` не отслеживается; `data_version` — вероятностная свёртка.

- [ ] **Step 5: Обновить `docs/KEY_RATE_MODEL.md` §4**

Заменить утверждение «отдельная таблица…» на: для рядов ДИП PIT-слой — `v_point_in_time*`; остальные ряды — консервативные лаги; физическая таблица/заморозка — отдельный пункт.

- [ ] **Step 6: Обновить `docs/ROADMAP_KEY_RATE.md`**

- §5 строка `point_in_time`: статус ✅ с датой и числами приёмки, ссылка на issue #126.
- §6 фаза 4: пункт «point-in-time слой» — ✅ с датой/числами, спека и план.
- Если статус ✅ ставится до мержа, пометить «✅ сделан этим PR» (по правилам трекера — In Review до мержа).

- [ ] **Step 7: Проверить docs**

Run: `make docs-check`
Expected: зелёный.

- [ ] **Step 8: Коммит**

```bash
git add ARCHITECTURE.md README.md docs/IMPORTERS.md docs/LIMITATIONS.md docs/KEY_RATE_MODEL.md docs/ROADMAP_KEY_RATE.md
git commit -m "document point-in-time layer"
```

---

### Task 5: Живая приёмка и протокол

**Files:**
- Modify: `docs/ROADMAP_KEY_RATE.md` (если протокол требует правки статуса)
- PR comment (не файл)

- [ ] **Step 1: Полная проверка Go**

Run: `make all`
Expected: gofmt, golangci-lint `0 issues`, `go vet` чисто, `go test -race ./...` — все `ok`, обе сборки.

- [ ] **Step 2: Создать/пересоздать витрины**

Run: `make import STAT=cbr_dip_bulletin`
Expected: в логе три `created: true` — `util.CreateView` пересоздаёт витрину на каждом прогоне и возвращает true, когда исходная таблица есть; `Imported 0 rows` без скачиваний; повтор — то же.

- [ ] **Step 3: Гранты и MCP**

Run: `make mcp-user && make mcp-check`
Expected: `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.

- [ ] **Step 4: SQL-приёмка через MCP (`kimi_reader`)**

```sql
SELECT count() AS points, uniqExact(vintage) AS vintages, uniqExact(series) AS series,
       min(published_at) AS first_pub, max(published_at) AS last_pub
FROM v_point_in_time;
-- 18000, 49, 8, 2020-10-14, 2026-10-14

SELECT count() AS n FROM v_point_in_time_asof(as_of='2024-02-06')
WHERE source='cbr_dip_bulletin' AND series='unemployment_sa' AND period_date='2023-11-30';
-- 0

SELECT value, vintage, vintage_label FROM v_point_in_time_asof(as_of='2024-02-07')
WHERE source='cbr_dip_bulletin' AND series='unemployment_sa' AND period_date='2023-11-30';
-- 2.885274, 47847, 24-01

SELECT value, vintage, vintage_label FROM v_point_in_time_asof(as_of='2026-09-02')
WHERE source='cbr_dip_bulletin' AND series='unemployment_sa' AND period_date='2023-11-30';
-- 2.916127, 62343, 26-06

SELECT count() AS n FROM v_point_in_time WHERE period_date > published_at;
-- 0

SELECT vintages, data_version FROM v_point_in_time_version WHERE source='cbr_dip_bulletin';
-- 49, <16 hex>

SELECT count() FROM cbr_dip_bulletin;
-- ACCESS_DENIED (497)
```

- [ ] **Step 5: Расписание и docs**

Run: `dagu validate dagu/cbr.yaml && make docs-check`
Expected: exit 0; docs-check зелёный.

- [ ] **Step 6: Протокол**

Комментарием в PR записать: команды, числа, крайние даты, результаты as-of, `make all`/`make mcp-check`/`dagu validate`/`make docs-check`, найденные расхождения и их лечение. Обновить статус пункта в роадмапе (✅ с датой) в том же PR.

---

## Self-Review

**Spec coverage:**
- §2.1 (витрины, не таблица) → Task 1.
- §2.2 (три витрины, колонки, SQL) → Task 1.
- §2.3 (строгая граница, тай-брейк, отсутствие ряда) → Task 1 (SQL-контракт) + Task 5 (живая проверка).
- §2.4 (версия) → Task 1 + Task 5.
- §2.5 (создание в прогоне) → Task 2.
- §2.6 (MCP) → Task 3.
- §4 (критерий приёмки) → Task 5.
- §5 (ограничения) → Task 4.
- §6 (вне объёма) → не реализуется, отражено в Task 4.

**Step scan:** каждый шаг — одно действие с проверяемым результатом; тесты написаны до кода в Task 1–2; SQL-приёмка — точные значения.

**Type consistency:** `cbrPitViews`, `cbrPitView`, `cbrPitAsOfView`, `cbrPitVersionView`, `ensurePitViews(ctx, conn)` используются одинаково в Task 1–2; `Columns` у as-of — пустая map, у base/version — 7/6 записей.
