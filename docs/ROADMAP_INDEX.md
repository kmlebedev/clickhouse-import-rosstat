# Реестр роадмапов и правила GitHub-трекера

**Дата:** 17 октября 2026 г.
**Роль документа:** единый реестр всех роадмапов репозитория и правил трекера: слаги, границы владения, составной ID пункта, карта статусов и порядок заведения issue. Источник истины о работах — файлы роадмапов; GitHub (issue и Project V2) — журнал и вид.
**Сопутствующие документы:** спека [2026-10-17-github-tracker-design.md](superpowers/specs/2026-10-17-github-tracker-design.md), план [2026-10-17-github-tracker.md](superpowers/plans/2026-10-17-github-tracker.md), правило 12 [AGENTS.md](../AGENTS.md), скил [`.kimi-code/skills/github-tracker/SKILL.md`](../.kimi-code/skills/github-tracker/SKILL.md).
**Пункт роадмапа:** `infra/3.1` в [ROADMAP_INFRA.md](ROADMAP_INFRA.md).

---

## 1. Реестр роадмапов

| Слаг | Файл | Роль / граница владения | Схема item-id |
|---|---|---|---|
| `dcf-polyus` | [ROADMAP_DCF_POLYUS.md](ROADMAP_DCF_POLYUS.md) | золото → DCF → NAV PLZL; DCF-модель, парсеры Полюса, остаток фазы 3 | `0`–`5`, `3.1`–`7.3`, `phase-3-remaining-<тема>` |
| `gold-thesis` | [GOLD_THESIS_DATA_ROADMAP.md](GOLD_THESIS_DATA_ROADMAP.md) | данные и витрины тезисов по золоту; WGC/EIA/bold/news/фрахт/аналитические витрины | `phase-N`, `phase-N-followup-M`, `analytics-<витрина>` |
| `dcf-metals` | [ROADMAP_DCF_METALS.md](ROADMAP_DCF_METALS.md) | CHMF/MAGN/NLMK в `company_financials`; металлурги и `dashboard/chmf.json` | `step-1`–`step-8` |
| `key-rate` | [ROADMAP_KEY_RATE.md](ROADMAP_KEY_RATE.md) | контур прогноза ключевой ставки; ЦБ-источники, модель ставки, news-ленты контура | `1.1`–`1.11`, `phase-2-<тема>`, `phase-4-<тема>` |
| `infra` | [ROADMAP_INFRA.md](ROADMAP_INFRA.md) | общая инфраструктура, не принадлежит ни одному треку | `1.1`–`1.5`, `2.1`–`2.2`, `3.1`–`3.3` |

Слаг выведен из имени файла (у `GOLD_THESIS_DATA_ROADMAP.md` — `gold-thesis`). Шестой роадмап не заводится без решения владельца.

**Правило владения:** пункт живёт ровно в одном роадмапе — том, чья шапка или §1 объявляет его своим. В других файлах — только ссылка. Если работа упомянута в двух файлах, канонический владелец определяется по этому правилу, а второй файл получает ссылку на issue владельца.

Примеры границ:

- `fred`/`bls`/`bea`/`wgc`/`eia`/`bold` — владелец `gold-thesis`; `dcf-polyus` фаза 1 ссылается, своих issue не заводит.
- Прогноз ключевой ставки — владелец `key-rate`; `dcf-polyus` §13.1 потребляет его как вход.
- Металлурги — владелец `dcf-metals`; `dcf-polyus` §12 (peers) ссылается.
- Архив, приёмка, дашборды, деплой — владелец `infra`; трековые роадмапы ссылаются.

## 2. Составной ID и заголовок issue

Канонический ключ пункта: `<slug>/<item-id>`.

Примеры формата:

- `infra/1.1`, `key-rate/1.9`, `dcf-polyus/7.3`, `dcf-metals/step-4`;
- `gold-thesis/phase-3-followup-2`;
- `gold-thesis/analytics-v-gold-floor`;
- `dcf-polyus/7.1`.

Labels: `roadmap:<slug>` — группировка до появления Project V2; `needs-answer` — пока в issue есть открытый вопрос Q&A. Обязательных labels у шаблона нет: до шага `infra/3.2` форма работает без предварительной настройки репозитория.

Заголовок issue: `[<slug>/<item-id>] <короткое имя пункта>`.

- `[key-rate/1.9] Telegram-молния Интерфакса и винтажные версии`
- `[infra/1.1] Винтажная база: архив версий файлов источников`
- `[dcf-metals/step-4] Парсер CHMF + NLMK`

Слаг в заголовке обязателен: он снимает коллизию одинаковых номеров (`1.1` есть и в `infra`, и в `key-rate`) и делает issue читаемой без Project.

## 3. Краткая карта статусов

| В роадмапе | В Project V2 |
|---|---|
| `❌`, «не начато», «план, не начато», нет маркера | `Todo` |
| `⏳`, `🔶`, «срез сделан», «остаток пункта», «в работе» | `In Progress` |
| PR открыт, «статус финализируется после мержа PR» | `In Review` |
| `✅` + протокол приёмки в PR + PR смержен | `Done` |
| внешний блокер | `Blocked` |

`✅ сделан этим PR` до мержа — это `In Review`, не `Done`. `✅` с оговоркой «статус финализируется после мержа PR» до мержа читается как `In Review`; `Done` — только после мержа и протокола. Статус в роадмапе обновляется тем же PR, что и реализация.

## 4. Порядок добавления нового пункта

1. **Сначала правка владеющего роадмапа**: пункт, его критерий приёмки и место в фазах появляются в файле.
2. **Затем issue** с составным ID и заголовком `[<slug>/<item-id>] <короткое имя>` (шаблон — [`.github/ISSUE_TEMPLATE/roadmap-item.yml`](../.github/ISSUE_TEMPLATE/roadmap-item.yml)).
3. **Новый роадмап** — только решением владельца: сначала он попадает в этот реестр строкой (слаг, файл, граница владения, схема ID), потом в нём появляются пункты.

Незапланированная работа не делается «попутно»: она становится пунктом владеющего роадмапа, а уже потом — issue.

## 5. Где лежат правила и операции

- Модель трекера, поля и виды Project V2 — спека [2026-10-17-github-tracker-design.md](superpowers/specs/2026-10-17-github-tracker-design.md) (§6–§7 — поля и виды).
- Пошаговые операции: issue, Project V2, sub-issues, Q&A, PR, аудит — скил [`.kimi-code/skills/github-tracker/SKILL.md`](../.kimi-code/skills/github-tracker/SKILL.md).
- Обязательная норма для ассистента — правило 12 [AGENTS.md](../AGENTS.md); шаблон PR — [`.github/pull_request_template.md`](../.github/pull_request_template.md).
- Приёмка пункта и протокол живой приёмки — [IMPORTER_PACKAGE_STANDARD.md](IMPORTER_PACKAGE_STANDARD.md) §16.1 п. 8 и [ROADMAP_INFRA.md](ROADMAP_INFRA.md) §5.

Живой GitHub Project V2 и бэкфилл открытого фронта — отдельный шаг `infra/3.2`. До него issue и labels работают без Project и не блокируют работу по пунктам.
