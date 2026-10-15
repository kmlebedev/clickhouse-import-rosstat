### Task 4: Расписание, README, ARCHITECTURE, роадмапы

**Files:**
- Modify: `dagu/craw.yaml`, `README.md`, `ARCHITECTURE.md`, `docs/GOLD_THESIS_DATA_ROADMAP.md`, `docs/ROADMAP_DCF_POLYUS.md`

**Interfaces:**
- Consumes: env-контракт Task 2 (`GOST_MODE`/`GOST_FROM`/`GOST_TO`/`GOST_SETTLE_DAYS`/`GOST_APP_ID`), витрины Task 3.
- Produces: документация и расписание (контракт для эксплуатации).

- [ ] **Step 1: `dagu/craw.yaml`** — один шаг, `schedule: "17 * * * *"`, `overlap_policy: skip`, `run: "$ROSSTAT_IMPORT_BIN"`, `env: CLICKHOUSE_IMPORT_STAT=gost_vehicle_safety_certificate` и `GOST_MODE=since`, retry 3 × 300; комментарий-обоснование: стена портальной пагинации (почему API-транспорт), лаг публикации ~2 суток (почему хвост 3 дня), щадящая нагрузка 4–12 запросов в час, `full`/`check` — ручные (без расписания).

- [ ] **Step 2: README** — строка `craw` в таблице импортёров (источник — реестр СБКТС, три режима, витрины, расписание «раз в час», ручной `full` для истории); абзац-runbook с командами:

```bash
make import STAT=gost_vehicle_safety_certificate                        # since: хвост (штатный, этим ходит dagu)
GOST_MODE=full GOST_FROM=2016-01-01 GOST_TO=2026-10-11 make import STAT=gost_vehicle_safety_certificate
GOST_MODE=check GOST_FROM=2026-06-01 GOST_TO=2026-06-30 make import STAT=gost_vehicle_safety_certificate
```

и оговорками: полный прогон — часы, чанками по годам, резюмируемый; `GOST_APP_ID` — в `~/.config/rosstat/env` (значение из JS-конфига страницы реестра; в репозитории не хранится).

- [ ] **Step 3: ARCHITECTURE.md** — §2 (строка `craw/` — новый транспорт и режимы), §3 (заменить «Эталон краулера с двумя DDL — `craw/gost.go`» на актуальный пример `CrawFunc` — `customs/vneshn_torg_countries.go`), §5 (env-таблица + 5 строк `GOST_*`), §6.1 (строка/абзац про импортёр), §6.2 (канонический DDL — новая схема + примечание об идемпотентных ALTER-ах для существующей таблицы), §6.3 (две витрины с описанием и оговорками), §6.5 (гранты/проверки), §7 (строка календаря «раз в час»); в шапку — запись «Последнее обновление: 2026-10-12 — …».

- [ ] **Step 4: Роадмапы** — новый пункт в `docs/GOLD_THESIS_DATA_ROADMAP.md` после блока «Фаза 4 — follow-up 5»: «Фаза 4 — follow-up 6: СБКТС Росстандарта — прокси импорта авто (торговый баланс → курс рубля)», с пробелом, методом (API-срезы, режимы, витрины), ссылками на спеку/план; канал №7 в `docs/ROADMAP_DCF_POLYUS.md` §10.1 (таблица каналов) со ссылкой на пункт. Статусы ✅ — в Task 5 после живой приёмки.

- [ ] **Step 5: Проверка документов** — `grep -n "gost" README.md ARCHITECTURE.md dagu/craw.yaml` (нет упоминаний старого месячного расписания и colly-обхода), `python3 -c "import yaml,sys; yaml.safe_load(open('dagu/craw.yaml'))"`.

- [ ] **Step 6: Commit** — `git add dagu/craw.yaml README.md ARCHITECTURE.md docs/GOLD_THESIS_DATA_ROADMAP.md docs/ROADMAP_DCF_POLYUS.md && git commit -m "docs: gost hourly dags and api importer runbook"`

---

