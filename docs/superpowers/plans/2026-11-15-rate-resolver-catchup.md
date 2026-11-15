# План #601: ограниченный резолв ставки

[Спека](../specs/2026-11-15-rate-resolver-catchup-design.md).

1. Merge/dependency/claim, отдельная ветка, baseline; спецификация первым коммитом.
2. Минимальный scope в существующем resolver, тесты изоляции/default/errors/no-op.
3. README/ARCHITECTURE env-контракт, без изменения DAG и схемы.
4. make all, uncached forecast race, docs-check, независимое code-review.
5. Локальный dev: preflight → cbr_rate-only import → MCP сравнение → repeat0.
6. Протокол, final-review, roadmap In Review, PR Closes #601.

Обновление прочих макрорядов остаётся следующим самостоятельным пунктом P0.

## Приёмка 2026-11-15

✅ Живая приёмка пройдена; пункт In Review, Done — после merge.
Код 4adee0f, протокол и артефакты — [отчёт](../../research/2026-11-15-rate-resolver-catchup.md).
`FORECAST_RESOLVE_SCOPE=cbr_rate make import STAT=forecast_resolve`: 8 записей,
повтор 0. Цель всех восьми — 2026-10-23, факт 14% на 2026-10-26;
исходные прогнозы датированы 2026-10-18…2026-10-20.
4 ранее resolved и 3 non-rate строки неизменны; stale 19 → 16.
make all, uncached forecast race, docs-check, dagu validate — PASS.
make mcp-check: целевые проверки PASS, 3 посторонних сбоя (news bootstrap,
каталог Fedstat, числа v_fedstat_ipc). Финальное review gpt-6.1-terra PASS.
