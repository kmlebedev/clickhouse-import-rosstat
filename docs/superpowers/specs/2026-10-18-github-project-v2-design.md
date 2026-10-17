# Живой GitHub Project V2 и бэкфилл роадмапов

**Дата:** 2026-10-18
**Статус:** спека (реализация — пункт 3.2 [ROADMAP_INFRA.md](../../ROADMAP_INFRA.md))
**Основание:** [ROADMAP_INFRA.md](../../ROADMAP_INFRA.md) пункт 3.2; модель трекера — [2026-10-17-github-tracker-design.md](2026-10-17-github-tracker-design.md).
**Связанные документы:** [ROADMAP_INDEX.md](../../ROADMAP_INDEX.md), [AGENTS.md](../../../AGENTS.md) правило 12, скил [`.kimi-code/skills/github-tracker/SKILL.md`](../../../.kimi-code/skills/github-tracker/SKILL.md).

---

## 1. Зачем

Пункт 3.1 зафиксировал модель трекера в репозитории: реестр роадмапов, составной ID, правило владения, шаблоны issue/PR, скил и правило 12 `AGENTS.md`. Живого GitHub Project V2, полей, видов, бэкфилла открытого фронта и триажа существующих issue ещё нет.

Цель 3.2 — превратить модель в работающий инструмент:

- создать Project V2, привязать к репозиторию;
- завести поля и виды из спеки 3.1;
- завести issue на открытый фронт пяти роадмапов;
- разложить существующие issue по `Roadmap` или `Inbox`;
- проверить всё через MCP (`projects_list`, `projects_get`, `run_query` не нужен — project tools) и `gh project item-list`.

## 2. Project V2

- **Владелец:** `@me` (kmlebedev).
- **Название:** `clickhouse-import-rosstat — Roadmap`.
- **Репозиторий:** `kmlebedev/clickhouse-import-rosstat`, проект привязан к репозиторию.
- **Ссылка:** [https://github.com/users/kmlebedev/projects/11](https://github.com/users/kmlebedev/projects/11) (number 11, id `PVT_kwHOAJDr984BnhTT`).

## 3. Поля

| Поле | Тип | Значения / смысл |
|---|---|---|
| `Roadmap` | single select | `dcf-polyus`, `gold-thesis`, `dcf-metals`, `key-rate`, `infra` |
| `Phase` | text | `3`, `follow-up 2`, `step 4`, `analytics` |
| `Item` | text | `7.3`, `1.9`, `step-4`, `phase-3-followup-2` |
| `Status` | single select | `Todo`, `In Progress`, `In Review`, `Done`, `Blocked` |
| `Branch` | text | `feat/...` |
| `PR` | text | номер или URL PR |
| `Spec / Plan` | text | пути к файлам |
| `Acceptance` | text | короткий критерий из роадмапа |
| `Depends on` | text | номера issue или составные ID |
| `Priority` | single select | `P0`, `P1`, `P2` (опционально) |
| `Target date` | date | опционально |

Дефолтное поле `Status` нового проекта (Todo / In Progress / Done) заменяется полем с пятью значениями: новый проект пуст, данные не теряются.

## 4. Виды

| Вид | Layout | Фильтр | Назначение |
|---|---|---|---|
| `Все пункты` | table | — | полный список |
| `dcf-polyus` | table | `roadmap:dcf-polyus` | контур золота → DCF |
| `gold-thesis` | table | `roadmap:gold-thesis` | данные тезисов по золоту |
| `dcf-metals` | table | `roadmap:dcf-metals` | металлурги |
| `key-rate` | table | `roadmap:key-rate` | контур ключевой ставки |
| `infra` | table | `roadmap:infra` | инфраструктура |
| `Gold` | table | `roadmap:dcf-polyus,gold-thesis` | оба золотых роадмапа |
| `В работе` | table | `status:"In Progress","In Review"` | активный фронт |
| `Ждут ответа` | table | `label:needs-answer` | открытые Q&A |
| `Inbox` | table | `no:roadmap` | issue без роадмапа |
| `Roadmap` | roadmap | — | обзор по `Target date` (если API принимает) |

Синтаксис фильтров проверяется через `gh project item-list --query` до создания вида; если фильтр не принимается — вид не создаётся, отклонение фиксируется в протоколе.

## 5. Бэкфилл открытого фронта

Источник — пять роадмапов на `origin/main`; список из 59 строк собран по правилам:

- `❌`, `⏳`, `🔶`, «не начато», «план, не начато»;
- строки шагов `ROADMAP_DCF_METALS.md` без маркера;
- явные «остаток пункта/фазы» и `❌`-списки;
- частичный `✅` включается только остатком (`infra/1.2`, `dcf-polyus/7.3`);
- исторические `✅` без остатка не включаются.

| Роадмап | Строк | Уже покрыто issue |
|---|---:|---|
| `infra` | 5 | #105 (`infra/3.3`) |
| `key-rate` | 18 | — |
| `dcf-polyus` | 11 | — |
| `gold-thesis` | 17 | #47 (`phase-5`) |
| `dcf-metals` | 8 | — |
| **Всего** | **59** | **2** |

Новых issue — 57; #105 и #47 не дублируются, а добавляются в Project и получают поля.

Правила issue:

- заголовок `[<slug>/<item-id>] <короткое имя>`;
- шапка тела: `Roadmap`, `File`, `Item`, `Phase`, `Acceptance`, `Spec`, `Plan`, `Depends on`;
- тело: цель/контекст, критерий приёмки, источник (`file:line`), ссылка на `ROADMAP_INDEX.md`;
- label `roadmap:<slug>`;
- статус: `Todo` для нового открытого фронта; `In Progress` для частичных `infra/1.2`, `dcf-polyus/7.3`, `dcf-polyus/phase-3-remaining-output-model`; `In Progress` для #105; `Todo` для #47.

Бэкфилл идемпотентен: перед созданием issue проверяется точный заголовок; повторный прогон не создаёт дубль.

## 6. Триаж существующих issue

Правило: issue либо получает `Roadmap` и поля, либо остаётся в `Inbox` (`Roadmap` пуст), либо закрывается, если работа уже принята в роадмапе.

| Issue | Решение |
|---|---|
| #105 | `infra/3.3`, `In Progress` (ветка `infra-github-pages`, до мержа) |
| #90 | `Inbox`, `Todo` — основание трекера; работа 3.2 ведётся в #112 |
| #86 | `key-rate`, `Done`; закрыть с ссылкой на приёмку пункта |
| #75 | `key-rate`, `Done`; закрыть с ссылкой на `rosstat_ipc_weeks_bulletins` |
| #74 | `key-rate/3.1`, `Done`; закрыть с ссылкой на PR #87 |
| #42 | `gold-thesis/phase-3-followup-4`, `Done`; закрыть с ссылкой на `bold_gold_price` |
| #47 | `gold-thesis/phase-5`, `Todo` |
| #46 | `gold-thesis/phase-4-followup-4`, `In Progress` |
| #89, #88, #85, #81, #77, #76, #58, #52, #51, #48 | `Inbox`, `Todo` |

Закрытие — только при явной приёмке в роадмапе; комментарий закрытия содержит ссылку на пункт/PR. Остальные issue не закрываются.

## 7. Критерий приёмки

1. `mcp__github__projects_list` видит проект; `projects_get` отдаёт поля и views.
2. Поля совпадают с §3, включая `Status` с пятью значениями; опции встроенного `Status` расширены до пяти (удалить встроенное поле GitHub не даёт).
3. Views из §4 созданы (кроме тех, чей фильтр отклонён API; отклонения названы).
4. В проекте 75 items: 57 новых issue + 18 существующих.
5. Открытый фронт заведён: по каждому роадмапу есть issue с составным ID; #105/#47 не дублируются.
6. `gh project item-list` показывает все items; у каждого непустые `Roadmap`/`Item`/`Status`, кроме `Inbox`.
7. `make all` зелёный; ссылки в `ROADMAP_INDEX.md`/`ROADMAP_INFRA.md` синхронны.
8. Протокол живой приёмки — комментарием в PR.

## 8. Риски и ограничения

| Риск | Митигация |
|---|---|
| Ошибка в фильтре вида | Проверка `gh project item-list --query` до создания; отклонение фиксируется |
| 57 новых issue — шум | Только открытый фронт, без исторических `✅`; заголовки с составным ID |
| Дубли с #105/#47 | Точная проверка заголовков перед созданием |
| Закрытие не тех issue | Закрываются только #86/#75/#74/#42 с явной ссылкой на приёмку |
| Лимит GitHub API | Скрипт идемпотентен, повторный прогон продолжает с места сбоя |
| Параллельная сессия `infra-github-pages` | Не трогаем её ветку/файлы; #105 только добавляем в Project |

## 9. Что не делать

- Не создавать второй источник истины: список issue живёт в GitHub, не в репозитории.
- Не заводить issue на исторические `✅`.
- Не дублировать #105/#47.
- Не закрывать issue без явной приёмки в роадмапе.
- Не переносить спеки/планы в тело issue.
- Не менять Go-код, DDL, Makefile, CI.
