# План: унификация слагов канала ключевой ставки

**Пункт:** `key-rate/phase-4-channel-slug-alignment`, issue [#329](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/329).
**Спека:** [2026-10-21-channel-slug-alignment-design.md](../specs/2026-10-21-channel-slug-alignment-design.md).
**Дата:** 21 октября 2026 г.
**Роль:** `role:go`.
**Ветка / worktree:** `feat/channel-slug-alignment`, `.worktrees/channel-slug-alignment`.

---

## 1. Реестр тегов

1. `news/tags.go`: `newsPromptVersion = 3`.
2. Заменить 4 записи `newsTags`:
   - `credit_transmission` → `lending_transmission`;
   - `external_conditions` → `external_fx`;
   - `savings` → `savings_behavior`;
   - `macroprudential` → `autonomous_factors`, Title «Автономные (немонетарные) факторы».
3. Добавить `newsTagAlias`/`newsTagAliases` (4 пары legacy → канон) и `newsTagCanonicalSQL(expr string) string`, который генерирует `multiIf(t.dimension = 'cb_channel' AND <expr> = '<legacy>', '<canon>', …, <expr>)` через `newsSQLString`.

## 2. Витрины

1. `news/schema.go`, `v_news_events`: в `fmt.Sprintf` добавить `%s` для агрегата тегов; порядок аргументов — выражение тегов, затем два `newsFilter`.
2. `news/schema.go`, `v_news_flash`: тот же агрегат через конкатенацию.
3. Комментарии колонок `tags` обеих витрин: слаги канонические, legacy редакций ≤2 нормализуются на чтении.

## 3. Тесты

1. `news/tags_test.go`: пины `lending_transmission`/`autonomous_factors`; негативные пины на 4 legacy-слага; `TestNewsTagAliases`; `TestNewsCBChannelSlugsCanonical` (точный список 11).
2. `news/schema_test.go`: `wants` для `groupArrayIf` — проверка `multiIf` в обеих витринах; `v_news_tags` не содержит legacy.
3. `news/annotation_test.go`: `savings` → `savings_behavior`.
4. `news/annotation_llm_test.go`: «Редакция инструкции: 2.» → 3.

## 4. Документация

1. `docs/KEY_RATE_CHANNELS.md`: §3.1, §8, §9 — канон; legacy-слаги только в исторических строках; оговорка про веер.
2. `docs/IMPORTERS.md`, `docs/CONFIGURATION.md`, `ARCHITECTURE.md`: версия промпта → 3; описание нормализации витрин.
3. `docs/ROADMAP_KEY_RATE.md`: пункт → `🔶 In Review` с PR.

## 5. Проверка

```bash
make all
make docs-check
```

Живая приёмка:

```bash
make import STAT=news_watch
```

MCP-запросы:

- `SELECT count(), countIf(dimension = 'cb_channel') FROM v_news_tags` → `46`, `11`;
- `SELECT t.2 AS tag, count() FROM v_news_flash ARRAY JOIN tags AS t WHERE t.1 = 'cb_channel' GROUP BY tag` → только канон;
- `SELECT prompt_version, count() FROM v_news_flash GROUP BY prompt_version` → обе редакции;
- `SELECT channel, method, prompt_version, count() FROM v_cbr_mp_summary_channels GROUP BY …` → как до PR (11 каналов, `llm`/1 = 2 892, `regex`/0 = 3 488);
- `make mcp-check` → «ПРОВЕРКА ПРОЙДЕНА».

## 6. Протокол

PR: пункт, `Closes #329` вне backticks, команды и числа приёмки, протокол — комментарием. После мержа — `Done` в Project V2, `✅` в роадмапе, worktree удалить.
