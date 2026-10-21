# План: ролевой скил главного редактора публикаций

**Дата:** 21 октября 2026 г.
**Пункт:** `infra/5.5`; issue [#325](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/325).
**Спека:** [2026-10-21-editor-in-chief-skill-design.md](../specs/2026-10-21-editor-in-chief-skill-design.md).
**Ветка:** `feat/editor-in-chief-skill`.

## Шаги

1. **Пункт и issue.** Issue #325 создан, label `roadmap:infra` + `role:shared`; строка `5.5` в `docs/ROADMAP_INFRA.md` — `In Progress`.
2. **Research.** `docs/research/2026-10-21-editor-in-chief-skill.md`: кейс #199, 12 дефектов с SQL-доказательствами, BBC/NYT/Reuters → шаги, дизайн, 10 рекомендаций к шаблону, go/no-go.
3. **Спека и план.** Первые коммиты ветки — до скила.
4. **SKILL.md.** `.kimi-code/skills/editor-in-chief/SKILL.md`: frontmatter, 9 разделов, цикл 12 шагов, матрица проверок, формат отчёта, DoD, частые ошибки. Объём 120–170 строк.
5. **Регистрация.**
   - `AGENTS.md` — строка в таблице маршрутизации: «Публикация, пост-апдейт, ревью/факт-чек статьи, шаблон публикации | Главный редактор | `editor-in-chief` | Не пишет прод-код и DDL; не публикует без записи в журнал».
   - `docs/ROADMAP_INFRA.md` — пункт `5.5` со ссылками на research/спеку/план и критерием приёмки; `In Progress` → `In Review`.
   - `docs/ROADMAP_INDEX.md` — `5.1`–`5.5`.
   - `docs/site-pages.json` — research-страница в секции «Исследования».
6. **Тест скила.** Read-only субагент получает `SKILL.md` и статью `2026-10-18-key-rate.md`; прогоняет шаги 2–6, 8–10; в отчёте должно быть ≥ 10 из 12 классов дефектов, ложных блокеров нет. Результат — в протокол приёмки.
7. **Проверки.**
   - `make docs-check` — exit 0, число страниц +1.
   - Проверка относительных ссылок скила, спеки, плана, research.
   - `python3 -m json.tool docs/site-pages.json > /dev/null`.
   - `git diff --check`.
8. **PR.** Один PR, `Closes #325` вне backticks; протокол приёмки комментарием: команды, результаты, тест субагента, что не проверено.

## Что не входит

- Правки `docs/KEY_RATE_PUBLICATIONS.md` — рекомендации в research §5, применение отдельным пунктом.
- Прод-код, DDL, витрины, гранты, ML/LLM-скрипты.
- Публикация поста и запись в `forecast_log` — за владельцем.

## Риски

- `make all` не запускается: Go/Python не меняются; в PR это фиксируется явно.
- `forecast_log` недоступен `kimi_reader` — в скиле шаг «журнал: `make ml-live-check`, иначе „не проверено“».
