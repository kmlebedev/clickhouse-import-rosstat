# План #603: свежесть макрорядов

[Спека](../specs/2026-11-15-key-rate-macro-freshness-design.md).

1. Merge #602, sync #601 Done; claim #603, worktree и первая спека.
2. Независимый read-only inventory импортёров/побочных эффектов (gpt-6.1-sol).
3. Сохранить MCP baseline и проверить loopback writer.
4. Выполнить только 18 STAT из спеки, weekly_cpi_sa после weekly BI.
5. Снять after, повторить успешные импорты, снять repeat; не ремонтировать
   источники попутно, оформить обнаруженные блокеры отдельными пунктами.
6. Отчёт с остаточными gaps и границами свежести; docs-check и MCP smoke.
7. Независимое review gpt-6.1-terra; PR Closes #603, протокол комментарием.
