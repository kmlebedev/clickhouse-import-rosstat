# План: починка `domrf_mortgage` — повтор страницы при антибот-заглушке

**Пункт:** `key-rate/phase-2-source-fixes` (фаза 2).
**Issue:** [#336](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/336).
**Спека:** [2026-10-23-domrf-mortgage-source-fix-design.md](../specs/2026-10-23-domrf-mortgage-source-fix-design.md).
**Ветка:** `fix/domrf-mortgage-source`, worktree `.worktrees/domrf-mortgage-source`.

---

## 1. Задачи

### Task 1 — признак заглушки и цикл повтора

Файл: `bank/domrf_mortgage.go`.

1. Переписать блок комментария констант (`:21-28`): страница за ServicePipe JS-challenge (HTTP 200 + ~1,8 КБ JS-оболочка, вероятностно); повтор тем же клиентом; cookie успешной страницы авторизуют скачивание (без них WAF 403); файл отдаётся 307 → 200.
2. Добавить `domrfPageAttempts = 8`, `domrfPageRetryDelay = 300 * time.Millisecond`.
3. Добавить `domrfChallengePage(page []byte) bool` (маркеры `js-challenge-loader`, `servicepipe.tech`).
4. Добавить `domrfPageSrc(fetch func(string) ([]byte, error), attempts int, delay time.Duration) (string, error)`:
   - ошибка `fetch` — сразу;
   - успешный разбор — `src`;
   - ошибка разбора на странице без маркеров заглушки — сразу (fail fast);
   - заглушка — `log.Warnf` + пауза + следующая попытка;
   - исчерпание — ошибка «антибот-заглушка ServicePipe вместо HTML после N попыток».
5. `Import()` — вызывать `domrfPageSrc(util.GetPage, domrfPageAttempts, domrfPageRetryDelay)` вместо прямого `util.GetPage`; остальной путь (локальная копия шаблона, `DataUrl`, префлайт, архив) не трогать.
6. Обновить doc-комментарии `domrfXlsSrcFromPage`, `DomrfMortgageStat`.

### Task 2 — фикстуры

1. `bank/testdata/domrf_mortgage_page.html` — сохранить живую страницу 2026-10-23 (полный HTML, как отдаёт источник).
2. `bank/testdata/domrf_mortgage_challenge.html` — сохранить живую заглушку ServicePipe (~1,8 КБ).
3. Сверить `domrfFixtureSrc`/`domrfFixtureUrl` с `props.file2.src` новой фикстуры; если источник выпустил новый отчёт — обновить константы и spot-значения книги.

### Task 3 — тесты

Файл: `bank/domrf_mortgage_test.go`.

1. `TestDomrfPageSrcRetriesChallenge` — заглушка ×2, затем страница: `src` верный, вызовов ровно 3.
2. `TestDomrfPageSrcChallengeExhausted` — всегда заглушка, `attempts=3`: ошибка содержит `ServicePipe` и `3`, не содержит имени таблицы.
3. `TestDomrfPageSrcRealPageNoBlock` — реальная страница без блока: вызов ровно 1, ошибка называет блок.
4. `TestDomrfPageSrcFetchError` — ошибка `fetch` пробрасывается, вызов 1.
5. Маркеры фикстур: `domrfChallengePage(challenge)=true`, `domrfChallengePage(page)=false`.
6. Существующие тесты (`TestDomrfXlsSrcFromPage`, `TestParseDomrfSheet`, fail-fast, DDL) — без изменений.

### Task 4 — документация

1. `ARCHITECTURE.md:4` — строка «Последнее обновление»: пункт `key-rate/phase-2-source-fixes` (#336).
2. `ARCHITECTURE.md:299` — в «Ограничении» шаблона А: DOM.RF за ServicePipe, но блок в HTML; импорт повторяет страницу; cookie страницы нужны для скачивания.
3. `ARCHITECTURE.md:450` — строка `domrf_mortgage`: та же поправка вместо «разведкой не подтвердилась».
4. `ARCHITECTURE.md:2089` — строка расписания банков: повтор страницы при антибот-заглушке.
5. `docs/IMPORTERS.md:23` — описание пакета `bank/`.
6. `docs/ROADMAP_KEY_RATE.md` — абзац `phase-2-source-fixes` в фазе 2 (после блока рыночных контролей MOEX): issue #336, причина, решение, статус `✅ сделан этим PR`.

### Task 5 — приёмка

Из worktree, dev-ClickHouse поднят:

```bash
make all
make import STAT=domrf_mortgage
ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=$(mktemp -d) make import STAT=domrf_mortgage
make mcp-check
dagu validate dagu/bank.yaml
make docs-check
```

MCP: `v_bank_macro` по `source='domrf_mortgage'` — 15 точек / 15 рядов / `max(date)=2026-08-31`; `v_series_catalog` — 15 строк.

### Task 6 — ревью и PR

1. `task-reviewer` на `anthropic/haiku`, fix-loop.
2. Финальное ревью ветки — `deepseek/deepseek-flash`.
3. Один PR: `[key-rate/phase-2-source-fixes] …`, `Closes #336` вне backticks; протокол приёмки — комментарием в PR.
4. Project V2: `Status = In Review`, `PR`, `Spec / Plan`; `Done` — только после мержа владельцем.

## 2. Протокол живой приёмки (шаблон для PR)

```text
Пункт: key-rate/phase-2-source-fixes (#336)
Ветка: fix/domrf-mortgage-source

- make all — <результат>
- make import STAT=domrf_mortgage — <no-op по версии / Imported N rows>
- полный путь со свежим архивом — <Imported N rows, дата, число банков>
- повтор с тем же архивом — <no-op>
- v_bank_macro / v_series_catalog — <числа>
- make mcp-check — <результат>
- dagu validate dagu/bank.yaml — <exit>
- make docs-check — <результат>
- не проверено: <если есть>
```
