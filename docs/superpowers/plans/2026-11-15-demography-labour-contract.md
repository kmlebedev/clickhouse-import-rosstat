# План #612

[Спецификация](../specs/2026-11-15-demography-labour-contract-design.md).

1. Зарегистрировать research-пункт, spec/plan первым коммитом, #608 Done после merge611.
2. MCP: наличие демографических рядов/трудовых метаданных и baseline.
3. Параллельный read-only dispatch: население+возраст/перепись; участие+охват.
   Интегратор отдельно проверяет часы и совместимость компонентов.
4. Численные проверки, inventory и precision/vintage/reference-date контракт;
   GO/ITERATE/NO-GO, handoff в существующие #489/#490/#492 без дублирующих импортёров.
5. MCP after, docs-check, независимое review, единый итоговый PR с Closes #612,
   протокол комментарием и Project In Review. Merge владельцем.

Исполнение: шаги 1–4 выполнены; отдельный Go-handoff часов — #613 (Todo, Depends on #612). Шаг 5: MCP before/after идентичен (696 точек / 2 строки каталога), offline 21 проверка PASS, docs-check 66 страниц / 33 теста PASS; независимое финальное review gpt-6.1-terra PASS. Итоговый PR и протокол оформляются; статус In Review, merge владельцем.
