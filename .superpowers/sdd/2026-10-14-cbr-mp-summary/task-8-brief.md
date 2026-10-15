### Task 8: Документация

**Files:**
- Modify: `ARCHITECTURE.md`, `README.md`, `docs/KEY_RATE_PUBLICATIONS.md`, `docs/KEY_RATE_CHANNELS.md`, `docs/ROADMAP_KEY_RATE.md` (§4.2/§5; §6 — Task 9)

**Interfaces:**
- Consumes: фактические имена/расписание после Task 1–7.

- [ ] **Step 1: `ARCHITECTURE.md`**: чейнджлог (запись 2026-10-14: пункт 1.2 — `cbr_mp_summary`, двухслойная разметка каналов, `dagu/calendar.yaml`); §2 — строки `cbr/` и `calendar/`; §3 — `util.PostJSON`; §6.1 — строка `cbr_mp_summary` (15-я запись реестра; кастомный Import: дельта по индексу); §6.2 — канонические DDL `cbr_mp_summary`/`_block`/`_channel` (копии в коде обязаны совпадать); §6.3 — три витрины; §6.5 — контур; §7 — расписание: 15-й шаг `cbr.yaml`, новый `dagu/calendar.yaml` (10:07 МСК), различи «каноническая строка заседания / ручной триггер» в `events_calendar`, заменить предупреждение «заседания попадают вручную» на фактическое; §8 — оговорка: `cbr_mp_summary` сознательно без HEAD-префлайта (мультидокументная дельта по индексу) + LLM-слой и переменные окружения.
- [ ] **Step 2: `README.md`** — таблица импортёров `cbr` (+`cbr_mp_summary`: три таблицы, три витрины, LLM-слой), расписание (15 шагов + calendar.yaml), дерево/тестовые пакеты.
- [ ] **Step 3: `docs/KEY_RATE_PUBLICATIONS.md`** §6 — «Резюме обсуждения (каналы и веса)» ❌→✅ с `v_cbr_mp_summary`/`_channels`; `docs/KEY_RATE_CHANNELS.md` — §2 «Практический вывод для ETL» → «реализовано в `cbr_mp_summary`»; §8 «Текстовый сигнал ЦБ» → ✅.
- [ ] **Step 4: `docs/ROADMAP_KEY_RATE.md`** — §4.2 пункт 2 «Текстовый сигнал ЦБ» → ✅; §5 — строки таблиц/витрин; §6 не трогать (Task 9).
- [ ] **Step 5: Проверки и коммит**: `grep -rn "cbr_mp_summary" ARCHITECTURE.md README.md | wc -l` ≥ 4; `go build ./...`; коммит `docs: sync ARCHITECTURE/README/key-rate docs for cbr_mp_summary (key-rate phase 1.2)`.

---

