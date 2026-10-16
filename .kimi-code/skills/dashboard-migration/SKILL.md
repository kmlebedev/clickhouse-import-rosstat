---
name: dashboard-migration
description: Reproducible process for migrating a legacy Grafana UI export to dashboards-as-code, adding a dashboard for a new track, and revising an existing dashboard to the standard in clickhouse-import-rosstat
type: prompt
whenToUse: When a Grafana dashboard is migrated from a legacy UI export to grafana/ code, added for a new track (banks, metals, …), or revised to the standard
---

Дашборды Grafana в этом репозитории — код, а не экспорт из UI: источник истины — Go-модуль `grafana/` (grafana-foundation-sdk), артефакт `dashboard/<uid>.json` коммитится и собирается детерминированно целью `make grafana-build`; нормативное устройство механизма — [ARCHITECTURE.md](../../../ARCHITECTURE.md) §6.7. Скил описывает **процесс** для трёх режимов:

- **A — миграция легаси-экспорта**: UI-дашборд старой формы (`dashboard/*.json`) переводится в код панель за панелью — строго на существующих витринах `v_*`.
- **B — новый дашборд трека** (банки, металлургия, …): перечень графиков прогноза и прогнозных точек даёт владелец.
- **C — ревизия существующего дашборда** (в том числе уже собранного кодом) до стандарта — uid не меняется.

**Что прочитать перед стартом.** Спека и план текущего дашборда, если пункт уже оформлен (`docs/superpowers/{specs,plans}`); [sql/mcp_kimi_reader.sql](../../../sql/mcp_kimi_reader.sql) — гранты витрин агенту; `views/*.go` — определения и комментарии витрин, которые читают панели. Образцы кода — `grafana/gold_thesis.go`, `grafana/key_rate_publications.go` и их тесты; точки интеграции — упорядоченный слайс в `grafana/main.go`, `DASHBOARDS` в `Makefile`, `.github/workflows/dashboards.yml`. Синк доков — скил [sync-readme-architecture](../sync-readme-architecture/SKILL.md).

**Чек-лист (9 шагов).**

1. **Пункт роадмапа.** Назвать или завести пункт (инфраструктурный — `ROADMAP_INFRA`, трековый — роадмап трека) и объявить его с критерием приёмки до старта; без пункта работа не начинается.
2. **Инвентаризация источника.** Легаси: панели, таргеты, источники данных, юниты, анти-паттерны — по экспорту. Трек: перечень графиков прогноза и прогнозных точек от владельца.
3. **Гэп-анализ «источник → витрина».** Каждая панель → существующая `v_*` + `series`/ключи. Недостающие слои не выдумывать: либо минимальный набор v1 на готовом, либо отдельный пункт-следствие.
4. **Гранты.** Сверить `sql/mcp_kimi_reader.sql`: панель на невыданной витрине валит staging-smoke; при отсутствии — добавить `GRANT SELECT` в `sql/mcp_kimi_reader.sql` и выполнить `make mcp-user` (AGENTS.md, правило 11в).
5. **Маппинг «раздел статьи/трек → панели».** Таблица в спеке; для статьи со временем — её §«Иллюстрации» (образец — [docs/KEY_RATE_PUBLICATIONS.md](../../../docs/KEY_RATE_PUBLICATIONS.md) §11).
6. **Спека + план** (`docs/superpowers/{specs,plans}`, первыми коммитами ветки): SQL — дословный контракт, серии сверены живыми запросами до фиксации.
7. **Реализация.** Builder в `grafana/`, тесты (состав панелей и рядов, `FROM v_` в каждом таргете, юниты, точки и пороги, детерминизм), конвенции — по ссылкам ниже.
8. **Регистрация.** Упорядоченный слайс в `grafana/main.go`, `DASHBOARDS` в `Makefile`, списки lint/upload в CI; артефакт `dashboard/<uid>.json` коммитится (при миграции заменяет легаси-экспорт, история — в git).
9. **Приёмка и доки.** `make all`; живой SQL всех таргетов через MCP под `kimi_reader`; `make grafana-deploy-test` + `make grafana-check` (staging — owner-assisted); CI зелёный; протокол живой приёмки — комментарием в PR ([docs/IMPORTER_PACKAGE_STANDARD.md](../../../docs/IMPORTER_PACKAGE_STANDARD.md) §16.1 п. 8); синк доков скилом `sync-readme-architecture`; регистрация следствий и обновление очереди.

**Конвенции** (нормативные формулировки не дублируются — только ссылки):

- [ARCHITECTURE.md](../../../ARCHITECTURE.md) §6.7 — дашборд как код: модуль `grafana/`, детерминизм, `Validate`, провижининг датасорса, CI и деплой.
- [Спека gold-thesis](../../../docs/superpowers/specs/2026-10-12-grafana-gold-thesis-design.md) §3 (архитектура) и §6 (runbook); [спека key-rate-publications](../../../docs/superpowers/specs/2026-10-16-grafana-key-rate-design.md) §6 (конвенции из UI-экспорта: что берём и что осознанно нет).

Ссылки покрывают: запросы только к `v_*`; `$__timeFilter(date)`; YoY окном по полному ряду; юниты `percent`/`percentunit`/`currencyRUB`/`currencyUSD`/`short`; точки для разрежённых серий; пороги со `ThresholdsStyle`; русские алиасы; детерминизм; описания панелей.

**Очередь миграции.** Живой список — в пункте `ROADMAP_INFRA` (Фаза 2, «Шаг 0: механизм миграции и добавления дашбордов»); состав на 2026-10-16:

1. `finance-polyus` — дашборд статьи PLZL, мигрируется первым в текущем PR;
2. `chmf.json` — шаг 6 [docs/ROADMAP_DCF_METALS.md](../../../docs/ROADMAP_DCF_METALS.md);
3. `rosstat.json` и корневой `grafana_dashboard_rosstat`;
4. дашборды новых треков (банки, металлургия и др.) — по этому скилу.

Каждый следующий дашборд обновляет очередь тем же PR, что и сам дашборд (шаг 9).
