# План: свежесть `minfin_fed_bud_mesyats`

> **Для исполнителя:** пункт `key-rate/phase-2-minfin-consolidated-freshness` (issue #229), ветка `fix/minfin-mesyats-freshness`, worktree `.worktrees/minfin-mesyats-freshness`. Спека — [2026-10-19-minfin-consolidated-freshness-design.md](../specs/2026-10-19-minfin-consolidated-freshness-design.md). WIP=1: не трогать `fedbud_mes.go` и другие импортёры.

**Цель:** вернуть в БД ряд `Акцизы` консолидированного бюджета и не терять его при смене подписи источника; убрать debug-шум и colly; добавить freshness-сигнал в MCP-check.

**Корень:** в XLSX 2026/10 строка называется `Акцизы ****`; парсер ищет точное `"Акцизы"` и молча пропускает ряд.

---

## Task 1. Спека и план

**Files:**
- Create: `docs/superpowers/specs/2026-10-19-minfin-consolidated-freshness-design.md`
- Create: `docs/superpowers/plans/2026-10-19-minfin-consolidated-freshness.md`

- [x] Созданы; коммит — первым.

---

## Task 2. Парсер и транспорт

**Files:**
- Modify: `minfin/fedbud_mesyats.go`

**Step 1.** Добавить `fedbudMesyatsLinkSelector = `a[href*="_mesyats.xlsx"]``.
**Step 2.** Заменить colly на `util.GetPage` + `util.FindLink`; `getFedbudMesyatDataUrl() (string, error)`; `Import()` возвращает ошибку.
**Step 3.** Нормализовать подпись: `label := strings.TrimSpace(row[1]); label = strings.TrimRight(label, "* ")`; сравнивать `label` с `fedbudMesyatsFields`.
**Step 4.** Удалить `fmt.Printf` (активный и закомментированный); убрать `colly`, `log` при ненадобности.
**Step 5.** `gofmt`, `go test ./minfin/`.

---

## Task 3. Фикстуры и тесты

**Files:**
- Create: `minfin/testdata/conbud_execute.html`
- Create: `minfin/testdata/Prilozhenie_8_dannye_115-117_mesyats_202610.xlsx`
- Create: `minfin/fedbud_mesyats_test.go`

**Step 1.** Сохранить реальную страницу `conbud/execute` (2026-10-19) и реальный XLSX 2026/10.
**Step 2.** Тест ссылки: `util.FindLink` на HTML-фикстуре, href содержит `_mesyats.xlsx`.
**Step 3.** Тест парсера на XLSX: 5 рядов, 884 точки (188×4 + 132), `Акцизы` 188, max 2026-08, spot-значения; `Акцизы ****` нормализуется.
**Step 4.** Локальный `appendRecorder`; без сети/БД.
**Step 5.** `go test -race -count=1 ./minfin/`.

---

## Task 4. Freshness-проверка MCP

**Files:**
- Modify: `scripts/mcp_check.py`

**Step 1.** Проверка лага консолидированного от федерального: `dateDiff('month', max(minfin_mesyats), max(minfin)) <= 2`.
**Step 2.** Проверка лага `Акцизы` от общего max `minfin_mesyats` <= 1 месяца.
**Step 3.** `python3 -m py_compile scripts/mcp_check.py`.

---

## Task 5. Каталог и роадмап

**Files:**
- Modify: `minfin/series_meta.go`
- Modify: `docs/ROADMAP_KEY_RATE.md`

**Step 1.** В `origin`/`description` ряда `Акцизы` указать `Акцизы ****` и нормализацию.
**Step 2.** Обновить покрытие `minfin_fed_bud_mesyats` — 884 точки, 2026-08; статус пункта.
**Step 3.** `make docs-check`.

---

## Task 6. Проверка и живая приёмка

```bash
make import STAT=minfin_fed_bud_mesyats
make all
make mcp-check
make docs-check
dagu validate dagu/minfin.yaml
```

**Ожидаемо:**
- `Imported 884 rows`; `minfin_mesyats` 884 / 5 рядов / max 2026-08;
- `Акцизы` 188 / max 2026-08;
- повтор — `Imported 884 rows`, `FINAL` 884;
- `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА, freshness-проверки OK.

**Протокол** — комментарием в PR: команды, числа, даты, повтор, mcp-check, dagu.

---

## Out of scope

- `fedbud_mes.go` (федеральный) и его `fmt.Printf`/colly;
- `Float32`→`Float64`, общий рефакторинг `minfin/`, схема/витрина/грант;
- ML-эксперимент по бюджетному каналу — отдельный пункт.
