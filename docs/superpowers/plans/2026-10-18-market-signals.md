# План: market signals — пункты роадмапов

**Дата:** 2026-10-18
**Спека:** [2026-10-18-market-signals-design.md](../specs/2026-10-18-market-signals-design.md)
**Ветка:** `roadmap/market-signals` (от `origin/main`)
**Объём:** docs-only. Код импортёров, DDL, витрины и дашборды не входят.

---

## 1. Цель

Добавить в владеющие роадмапы 5 пунктов, вытекающих из видео «Стейк 28» (Алён Капитал, 42:51): долговой контур ОФЗ, аукционы/госдолг, веса IMOEX, сигнал «акции vs ОФЗ», госдолг США → золото. Каждый пункт — отдельный issue; реализация — отдельными ветками.

## 2. Порядок работ

### Шаг 0. Ветка

- `git checkout -b roadmap/market-signals origin/main` — сделано.
- Не смешивать с веткой `infra-readme-structure` и её README-документами.

### Шаг 1. Спека и план — первыми коммитами

- `docs/superpowers/specs/2026-10-18-market-signals-design.md` — спека (состав 5 пунктов, приёмка, риски).
- `docs/superpowers/plans/2026-10-18-market-signals.md` — этот план.
- Коммит: `docs: add market signals spec and plan`.

### Шаг 2. `docs/ROADMAP_KEY_RATE.md`

1. §4.2, строка №9 «Кривая бескупонной доходности ЦБ» — переформулировать под пункт `key-rate/phase-2-cbr-zcyc`: теноры 0.25…30, история с 2003, `v_ofz_curve_compare`, `v_ofz_signal`, P0; отметить, что сейчас `v_ofz_curve` — по одной точке на тенор (2026-10-09), RGBI — с 2010.
2. §4.2 — добавить строку №25 «Долговой контур ОФЗ»: аукционы, запас госдолга, обслуживание, `v_ofz_stress`; источник — теги Минфина `Госдолг`/`Аукцион`/`ОФЗ`; пункт `key-rate/phase-2-ofz-debt-stress`, P0.
3. §6 «Фаза 2 — расширение каналов» — добавить абзац с двумя новыми шагами `phase-2-cbr-zcyc` и `phase-2-ofz-debt-stress` и их критериями приёмки. Не переписывать существующий длинный абзац приоритетов — только дописать новый абзац после него.

### Шаг 3. `docs/ROADMAP_DCF_POLYUS.md`

1. §7, таблица фаз, после строки 3 — добавить абзац «Остаток фазы 3 — market valuation & flows»: пункты `analytics-index-weights` и `analytics-rotation-signal`, P1, первый срез считает покрытый basket и публикует coverage ratio.
2. §10.1, канал 2 «Индексные потоки и ребалансировки» — дописать ссылку на пункт `dcf-polyus/analytics-index-weights`.
3. §10.1, канал 3 «Дивидендный канал и арбитраж против безрисковой ставки» — дописать ссылку на пункт `dcf-polyus/analytics-rotation-signal` и оговорку: сравниваем earnings yield покрытого basket с ОФЗ10, а не дивдоходность «в лоб».
4. §10.2, п. 3 «Новый выход модели — рыночный мост» — добавить предложение: первый срез `v_rotation_signal` даёт режимный флаг, а не готовую корректировку NAV; расширение — после coverage ratio.

### Шаг 4. `docs/GOLD_THESIS_DATA_ROADMAP.md`

1. §2, после Follow-up 5 — добавить `### Фаза 3 — follow-up 7: госдолг США и обслуживание — ⏳ не начат`.
2. Состав: `fred` + `GFDEBTN`, `A091RC1Q027SBEA`; витрина `v_debt_gold`; зачем — тезис «госдолг США → золото»; приёмка — 2 ряда, витрина, каталог, грант, `make all`, `make mcp-check`.

### Шаг 5. GitHub issues

Создать 5 issue в `kmlebedev/clickhouse-import-rosstat`:

| # | Заголовок | Label |
|---|---|---|
| 1 | `[key-rate/phase-2-cbr-zcyc] История кривой ОФЗ из КБД ЦБ` | `roadmap:key-rate` |
| 2 | `[key-rate/phase-2-ofz-debt-stress] Аукционы, госдолг и обслуживание` | `roadmap:key-rate` |
| 3 | `[dcf-polyus/analytics-index-weights] Веса IMOEX и пассивные потоки` | `roadmap:dcf-polyus` |
| 4 | `[dcf-polyus/analytics-rotation-signal] Сигнал «акции vs ОФЗ»` | `roadmap:dcf-polyus` |
| 5 | `[gold-thesis/phase-3-followup-7] Госдолг США и обслуживание` | `roadmap:gold-thesis` |

Тело issue: источник/спека, состав, критерий приёмки, ссылка на роадмап. Если label отсутствует — создать его или использовать `roadmap` (уточнить в скиле `github-tracker`).

### Шаг 6. Ссылки на issue в роадмапах

- После создания issue дописать в каждый новый пункт `issue #N`.
- Коммит: `docs: add market signal roadmap items`.

### Шаг 7. Проверка

- `git diff --check` — без whitespace-ошибок.
- `git status --short` — только спека, план и три роадмапа.
- Сверка по `docs/ROADMAP_INDEX.md`: каждый пункт ровно в одном владеющем роадмапе.
- Сверка ID: заголовок issue = `[<slug>/<item-id>] …`, ID совпадает с роадмапом.
- `make all` не требуется (docs-only).

### Шаг 8. PR

- `git push -u origin roadmap/market-signals`.
- PR в `main`, docs-only; в описании — спека, 5 пунктов, `Refs #...` на issue (не `Closes`: issue закрываются PR реализации).
- Протокол приёмки — комментарием в PR: список issue, `git diff --check`, сверка ownership.

## 3. Критерий приёмки

- 5 пунктов в трёх роадмапах, каждый — ровно в одном владеющем файле.
- 5 issue с составными ID и labels.
- Спека и план — первыми коммитами ветки.
- `git diff --check` чистый; README-документы `infra-readme-structure` не тронуты.

## 4. Вне объёма

- Код импортёров, DDL, витрины, Grafana, dagu.
- P2-пункты `ofz-holders` и `analytics-fund-flows` — остаются бэклогом в спеке, в роадмапы не вносятся.
- Новый шестой роадмап.
