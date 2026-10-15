### Task 5: Документация

**Files:**
- Modify: `ARCHITECTURE.md`, `README.md`, `docs/KEY_RATE_PUBLICATIONS.md`, `docs/KEY_RATE_CHANNELS.md` (если есть строка), `docs/ROADMAP_KEY_RATE.md` (§4.1/§4.2/§5), `ingest/schema.go` (комментарий списка source)

**Interfaces:**
- Consumes: финальные имена/расписание после T1–T4.

- [ ] **Step 1: `ARCHITECTURE.md`**

Чейнджлог (новая запись 2026-10-13: пункт 1.5 — импортёр `cbr_mo_br`, 17 показателей × 6 статистик, общая `source_publications` + HEAD-префлайт, витрина `v_cbr_mo_br`); §2 — строка `cbr/` (+`cbr_mo_br`, 14 импортёров); §3 — `util`: `HeadFingerprint`/`PublicationKnown`/`SavePublication`; §6.1 — строка `cbr_mo_br` (пакет `cbr/`, `macro_series`, `source='cbr_mo_br'`, расписание `dagu/cbr.yaml`); §6.2 — канонический DDL `source_publications` **и** синк комментария списка source в `macro_series` (здесь и в `ingest/schema.go`); §6.3 — витрина `v_cbr_mo_br`; §6.5 — контур; §7 — шаг `cbr_mo_br` (13→14 шагов); §8-конвенция: «источники с обновлением реже прогонов используют HEAD-префлайт и `source_publications`» — с указанием, что первый потребитель `cbr_mo_br`.

- [ ] **Step 2: `README.md`** — таблица импортёров `cbr` (+`cbr_mo_br`, таблицы/витрина); прозаический абзац (консенсус, 8 раз в год, no-op префлайт); дерево/тестовые пакеты; расписание (14 шагов).

- [ ] **Step 3: `docs/KEY_RATE_PUBLICATIONS.md`** — §6: строка «Консенсус аналитиков … ❌ фаза 1.5» → ✅ (с витриной `v_cbr_mo_br`); проверить упоминания в шаблонах публикаций. `docs/KEY_RATE_CHANNELS.md` — если есть строка консенсуса, дополнить фактической витриной.

- [ ] **Step 4: `docs/ROADMAP_KEY_RATE.md`** — §4.1: `salaries_mes`/`vvp_kvartal`/Сбер не трогаем, но в §4.2 убрать из пробелов пункт 5 «Консенсус аналитиков» (✅, витрина `v_cbr_mo_br`); §5 — исправить строку «`macro_series` (`source='cbr_survey'`)» на «`macro_series` (`source='cbr_mo_br'`)» и текст «медианы и интервалы прогнозов»; `ingest/schema.go` — список source в комментарии DDL.

- [ ] **Step 5: Проверки и коммит**

Run: `grep -rn "cbr_mo_br" ARCHITECTURE.md README.md | wc -l` → ≥ 4; `grep -n "source='cbr_survey'" docs/ROADMAP_KEY_RATE.md` → **ровно одна строка** (легитимная: мониторинг предприятий, фаза 1.3; строка макроопроса 1.5 обязана стать `source='cbr_mo_br'`); `go build ./...` → ok.

```bash
git add ARCHITECTURE.md README.md docs/KEY_RATE_PUBLICATIONS.md docs/KEY_RATE_CHANNELS.md docs/ROADMAP_KEY_RATE.md ingest/schema.go
git commit -m "docs: sync ARCHITECTURE/README/publications for cbr_mo_br (key-rate phase 1.5)"
```

---

