# План #601: ограниченный резолв ставки

[Спека](../specs/2026-11-15-rate-resolver-catchup-design.md).

1. Merge/dependency/claim, отдельная ветка, baseline; спецификация первым коммитом.
2. Минимальный scope в существующем resolver, тесты изоляции/default/errors/no-op.
3. README/ARCHITECTURE env-контракт, без изменения DAG и схемы.
4. make all, uncached forecast race, docs-check, независимое code-review.
5. Локальный dev: preflight → cbr_rate-only import → MCP сравнение → repeat0.
6. Протокол, final-review, roadmap In Review, PR Closes #601.

Обновление прочих макрорядов остаётся следующим самостоятельным пунктом P0.
