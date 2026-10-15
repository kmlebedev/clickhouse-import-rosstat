### Task 9: Живая приёмка и статус 1.2

**Files:**
- Modify: `docs/ROADMAP_KEY_RATE.md`

**Interfaces:**
- Consumes: всё выше; спека §4.

- [ ] **Step 1: `make all`** — зелёный.
- [ ] **Step 2: Полный прогон** (предусловие — владелец добавил `LLM_BASE_URL`/`LLM_API_KEY`/`MP_SUMMARY_LLM_MODEL` в `~/.config/rosstat/env`): `make import STAT=cbr_mp_summary` → «резюме: 22 …» (числа зафиксировать); повтор → `no-op, все 22 резюме на месте`, счётчики витрин не изменились.
- [ ] **Step 3: Сверка 22/22** с реестром `KEY_RATE_CHANNELS.md` §1 (meeting_date/published/решение) — SQL по `v_cbr_mp_summary` + сверка; зафиксировать 9 сохранений / 13 изменений.
- [ ] **Step 4: Калибровка каналов** — отчёт «канал → матчи regex / цитаты LLM» по всем 22 (scratch-скрипт, не коммитится); цифры в блок приёмки.
- [ ] **Step 5: Календарь**: `make import STAT=events_calendar` → канонические строки (23.10/18.12.2026 + 8×2027), ручные строки на месте, повтор идемпотентен; `dagu validate` обоих DAG.
- [ ] **Step 6: MCP**: `make mcp-user` → `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА (включая три новые строки и закрытость сырых); споты витрин через `mcp__clickhouse__run_query`.
- [ ] **Step 7: Статус 1.2** в `docs/ROADMAP_KEY_RATE.md`: `✅ **сделан этим PR** — приёмка ниже` + блок «Живая приёмка 2026-10-14 — шаг 1.2 (резюме обсуждения) — ✅ выполнен» с фактами шагов 1–6; коммит `roadmap key rate: mark 1.2 done (mp summary and calendar acceptance)`.

---

## После задач (вне нумерации)

Финальное ревью всей ветки (сильная модель) + `verification-before-completion`; один PR `feat/cbr-mp-summary` → `main` (как фаза 3/1.5). Спека и план — первыми коммитами ветки.
