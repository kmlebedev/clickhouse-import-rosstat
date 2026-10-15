# Task 1 report: Пересмотр ROADMAP_DCF_POLYUS.md

**Status:** DONE
**Commit:** `619ca2e` — `revise roadmap for datasource-first contour`
**Branch:** `gold-nav-contour`
**Files changed:** `docs/ROADMAP_DCF_POLYUS.md` only (1 file, +34/−19).

---

## What I changed (per brief step)

### Step 1 — point edits

**§2.2 «карта пробелов» (lines 32–48)** — rewrote the «Источник» column and added a lead-in sentence explaining the column now shows *delivery mode* as of 2026-10-10 (datasource-first, ref §11.5):
- Цена золота (USD): `datasource GC=F`; historical baseline = existing `gold` importer; **Nasdaq Data Link снят с критического пути**.
- Ставка ФРС / Реальные доходности: `datasource fred`; importer `fred` — historical baseline.
- Доллар: `datasource DX-Y.NYB` (yahoo) / `fred` (DTWEXBGS).
- Инфляция США: importers `bls`/`bea` — historical baseline; `fred` datasource — primary for a session.
- Дизельный канал: crack — `datasource HO=F`; distillate stocks — **ручной ввод** in `macro_series` (`source='manual'`) until phase-5 WebBridge.
- ETF-потоки (WGC): **ручной ввод / WebBridge** (фаза 5); Go-импортёр `wgc` не планируется.
- FedWatch: **ручной ввод / WebBridge** (фаза 5); Go-импортёр `fedwatch` не планируется.
- Котировки PLZL: `` `moex_iss` ✅ `` (PLZL.ME empty on datasource).

**§7 фаза 1 (line 161)** — reframed the row as «Исторический базлайн»; **removed `eia`, native `lbma_gold`, `wgc`, `fedwatch`** from the importer plan and stated explicitly that they are «вычеркнуты из плана». Changed the status of `fred`/`bls`/`bea`/`gold` to «исторический базлайн + Grafana + верификация; первичный источник мировых данных для прогнозной сессии — datasource» (naming `kimi-datasource` for Kimi Code and Global Finance Data/IMF for Kimi Work). Kept the existing facts for `bls`/`bea`/`moex_iss`/`ofz_curve`/`gold`.

**§7 фаза 2 (line 162)** — expanded to include `v_model_inputs`, `v_gold_dashboard`, `v_events_calendar`, плагин `gold-nav`, mcp-clickhouse in HTTP mode with token, ingest-endpoint (Bearer) for writing `model_runs`.

**§7 Quick wins (lines 167–171)** — replaced the old list (FRED importer / events_calendar / mcp docker) with: (1) `events_calendar` Q4-2026, (2) `v_model_inputs` + `v_gold_dashboard`, (3) плагин `gold-nav` (skill «сценарная сессия» + `/gold-nav:session`).

**§11 (new §11.5, lines 257–270)** — added spike results of 2026-10-10, Kimi Work v3.2.15: plugin marketplace, Plugin Builder («+ Создать плагин»), personal market, remote MCP by URL, same `kimi.plugin.json` format, hot-plug; datasource equivalents available (`yahoo_finance` = Global Finance Data, IMF, World Bank; no FRED plugin — covered by Yahoo `^TNX` + IMF); the assumption «в Kimi Work нет своего MCP и datasource» is refuted. Added the **sharing model «хостинг у автора»**: recipient installs `gold-nav` in one command, sets `GOLD_NAV_MCP_TOKEN`; mcp-clickhouse HTTP on the author's side, read-only `kimi_reader` (only `v_*`), **ingest token stays with the author only**, no tokens in the plugin repo (`${ENV}` placeholders). Added a link to the design spec (path verified to resolve) and noted the unverified live end-to-end query as acceptance work.

### Additional consistency fixes (Step 2 fallout, same document)

These removed leftover implications of planned Go importers / removed-source references:
- §3 (line 57): `macro_series` legend now notes manual rows (`source='manual'`).
- §4.2 (line 90): diesel-crack source changed from `macro_series` (EIA) to `datasource HO=F`; stocks — `macro_series` manual.
- §9 risks (line 182): mitigation for investing.com input changed from «stooq/LBMA для золота» to «золото — на datasource `GC=F`».
- §11.1 cron row (line 221): «чт — EIA/ИПЦ, 10-е — WGC» → neutral wording (stocks/ИПЦ, ETF-потоки), removing EIA/WGC as if they were automated feeds.
- §11.3 phase-5 row (line 248): clarified WebBridge-сбор `fedwatch`/`wgc_etf` is «ручной ввод до внедрения, не блокер и не Go-импортёр».

## Step 2 — grep self-check results

`Grep -i -n "eia|lbma|wgc|fedwatch"` on the file. Every remaining hit is in an allowed context:

| Line | Mention | Context | Verdict |
|---|---|---|---|
| 15 | «запасы дистиллятов EIA», «FedWatch >80%» | reference to the Q3-2026 article's verification dashboard | OK — article fact, not a plan |
| 38 | «LBMA AM/PM fix» | name of the needed series (its row's source = `datasource GC=F`) | OK |
| 44 | Go-импортёр `wgc` не планируется | explicit exclusion, datasource/manual context | OK |
| 45 | Go-импортёр `fedwatch` не планируется | explicit exclusion, datasource/manual context | OK |
| 57 | `macro_series` (FRED/EIA/WGC/CME) | origins of data that may live in the table | OK |
| 139, 142 | EIA (each Wed), WGC GDT | calendar fill-in / weekly scheduler | OK — event dates |
| 148 | `news_watch` parses WGC, EIA feeds | phase 5 | OK |
| 161 | `eia`, `lbma_gold`, `wgc`, `fedwatch` — вычеркнуты из плана | phase 1, exclusion | OK |
| 165 | FedWatch/WGC parsing | phase 5 | OK |
| 179 | FedWatch/WGC no public API | risk row → manual start, phase 5 | OK |
| 223, 234 | WebBridge collects FedWatch, WGC flows | phase 5 / architecture sketch | OK |
| 248 | WebBridge-сбор `fedwatch`/`wgc_etf` | phase 5, explicitly not a Go importer | OK |
| 309, 327, 329, 330 | WGC (Qaurum, GRAM, Gold Outlook) | external methodology references | OK |

No remaining mention presents `eia`/`lbma`/`wgc`/`fedwatch` as planned Go code outside phase 5 / datasource / manual-entry context.

## Self-review findings

- Reviewed `git show 619ca2e`: single file, correct commit message, no stray edits.
- Heading structure verified after edits: `§11.5` sits between `§11.4` and the `---` before `§12`; no other heading moved.
- Relative link `superpowers/specs/2026-10-10-gold-nav-plugin-design.md` verified to resolve on disk.
- `## 8` is missing from the document — confirmed **pre-existing** (`git show HEAD~1` also lacks it); not introduced here and out of scope for this task.
- Working tree clean except the pre-existing untracked `.kimi-code/mcp.json` (present before the task; not touched).
- All edits in Russian, matching the document style.

## Issues / concerns

1. **Section numbering gap `## 8`** is pre-existing. The brief did not ask to fix it; left untouched. Worth a separate cleanup.
2. §2.2 dense table cells got longer (delivery-mode descriptions). Readability is acceptable but the table is now text-heavy; if the controller prefers, the «Источник» column could be split into «источник» + «доставка».
3. I widened scope slightly beyond the literal bullet list (5 extra consistency fixes in §3/§4.2/§9/§11.1/§11.3) to satisfy Step 2's "no leftovers" expectation. All are one-line source/cron wording changes, no structural edits.
4. The reporting requirement said "Commit exactly as the brief Step 3 specifies" — done verbatim (`git add docs/ROADMAP_DCF_POLYUS.md`; message `revise roadmap for datasource-first contour`), without `-A`, so the untracked `.kimi-code/mcp.json` was not swept in.
