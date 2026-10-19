# Спека: ролевая маршрутизация агентов и эффективный цикл issue

**Дата:** 19 октября 2026 г.
**Пункт роадмапа:** `infra/5.1` — [ROADMAP_INFRA.md](../../ROADMAP_INFRA.md) §6, фаза 5; issue [#211](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/211).
**Статус:** спека дизайна.

---

## 1. Зачем

`AGENTS.md` объявляет одну роль — «Go-разработчик», хотя в репозитории уже работают четыре инженерные роли: Go, Python, ML и Research. Из-за этого:

- Python/ML/Research-агенты читают Go-правила (батчи, DDL, `series_catalog`, гранты), которые к их задачам не относятся, а своих границ и Definition of Done не получают;
- Go-агент не видит протокола выбора и «захвата» issue, WIP-лимита и работы с зависимостями;
- dispatch-политика субагентов (модели, GLM/xiaomimimo, статистика 754 прогонов) занимает ~20% общего файла и читается каждой сессией;
- правила параллельных сессий отсутствуют, хотя фактически работают 32 worktree;
- «прочитай `ARCHITECTURE.md`» — это 1511 строк, хотя для задачи нужны 2–4 раздела.

ML-инженер уже решил ту же проблему для ML: скил `ml-engineer` + короткий указатель в `AGENTS.md` ([PR #210](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/210)). Этот пункт распространяет тот же подход на остальные роли.

## 2. Решение

**`AGENTS.md` — тонкий общий контракт и роутер ролей; глубина — в ролевых скилах.**

| Артефакт | Роль |
|---|---|
| `AGENTS.md` (≤ 120 строк) | общий контракт, роутер «триггер → роль → скил», порядок начала сессии, правила 1–12, окружение, трекер, стиль PR |
| `.kimi-code/skills/go-engineer/SKILL.md` | Go-цикл, импортёры/DDL/витрины/гранты, GoLand MCP, очередь Go-issue, DoD |
| `.kimi-code/skills/python-engineer/SKILL.md` | не-ML Python, `uv`-пины, офлайн-тесты, ingest-payload, границы записи |
| `.kimi-code/skills/research-engineer/SKILL.md` | разведка источников, аудит покрытия/винтажей, research-документ, handoff |
| `.kimi-code/skills/ml-engineer/SKILL.md` | ML-методология (существует; только cross-links) |
| `.kimi-code/skills/subagent-dispatch/SKILL.md` | выбор моделей, GLM/xiaomimimo, промпт coder-субагента, review-политика |
| `docs/research/2026-10-19-engineering-issue-review.md` | доказательства: 36 закрытых и 64 открытых issue, scorecard, разрывы, очередь |

**Нумерация правил 1–12 сохраняется:** на неё ссылаются `docs/`, роадмапы и скилы.

## 3. Границы ролей

| Роль | Делает | Не делает |
|---|---|---|
| Go | Go-пакеты, импортёры, DDL, views, grants, `cmd/ingest`, Makefile/CI, deploy/dagu | ML-эксперименты, ad-hoc Python, схема без спеки |
| Python | `scripts/*.py` вне ML, коллекторы, harvest, docs-генератор, MCP-check, forecast-log sender, ingest-payload | прямая запись в ClickHouse, DDL/GRANT, ML-модели |
| ML | nowcast, CatBoost, бэктест, MLflow, семантика `forecast_log`, DM/CI | DDL/витрины/гранты, Go-импортёры |
| Research | разведка источников, аудит покрытия/винтажей, методология, research-документ, спека | прод-код, DDL, запись в БД |

Смешанный пункт (например, #184) делится по ролям в спеке/плане; Go-часть ревьюит Go-разработчик, ML-часть — ML-инженер.

## 4. Эффективный цикл issue

1. Выбрать ready-пункт: `Todo`, все `Depends on` закрыты, нет claim, приоритет фазы.
2. Захватить issue: комментарий, `In Progress`, `Branch`, `Spec / Plan`.
3. Изолироваться: worktree + ветка от `main`; в корне не работать.
4. Прочитать минимум: `AGENTS.md`, ролевой скил, строку роадмапа, issue, спеку/план, нужные §ARCHITECTURE, стандарт §14/§15.
5. Спека/план — первыми коммитами, если их нет.
6. Реализовать по стандарту; один импортёр/таблица на PR.
7. Проверить живьём: импорт, строки, даты, повтор no-op; MCP.
8. Прогнать `make all`, `make docs-check`, `dagu validate`.
9. Ревью на другой модели; fix-loop; финальное ревью на сильнейшей модели.
10. Один PR в конце; протокол приёмки — комментарием.
11. После мержа: `Done` только после протокола; синхронизация роадмапа.
12. Стоп-правила: «не проверено» — явно; объём вырос — новый пункт; противоречие — Q&A и стоп.

## 5. Интеграция

- `AGENTS.md`: роутер, 5 шагов начала сессии, WIP=1/claim, краткий блок параллельных сессий, сжатые правила 1–12.
- Issue-шаблон: dropdown `Role` (`go`, `python`, `ml`, `research`, `mixed`, `shared`).
- PR-шаблон: role-specific DoD.
- `github-tracker`: `Role` в шапке issue и claim protocol.
- `README.md`, `docs/DEVELOPMENT.md`, `ARCHITECTURE.md` §8: указатели на роли и скилы.
- `docs/site-pages.json`: review-документ.
- `docs/ROADMAP_INFRA.md` §6: `5.1`–`5.3`; `docs/ROADMAP_INDEX.md`: схема `infra`.

## 6. Вне объёма

- CI-workflows, gitleaks, skills-check, branch protection — `infra/5.2` (#212).
- Влитие `chore/parallel-sessions` — `infra/5.3` (#213).
- Изменения Go/Python/ML-кода, схемы ClickHouse, витрин и грантов.
- Новые типы субагентов: ролевые скилы работают с `coder`/`explore`/`plan`.

## 7. Приёмка

1. `AGENTS.md` ≤ 120 строк, роутер, нумерация 1–12 сохранена.
2. Три новых ролевых скила и `subagent-dispatch` валидны, ссылки разрешаются.
3. `ml-engineer` не дублирует Python/Research-правила.
4. Review покрывает 36 закрытых и 64 открытых issue, содержит scorecard, разрывы и очередь.
5. Шаблоны содержат `Role` и role-specific DoD; `github-tracker` содержит claim protocol.
6. Тесты скилов read-only субагентами (Go/Python/Research) проходят.
7. `make docs-check` и `make all` зелёные; код/схема/гранты не менялись.
8. `infra/5.1` обновлён тем же PR; `5.2`/`5.3` заведены как `Todo`.

## 8. Риски

- **Ссылки «правила 1–12»** — не менять нумерацию; grep перед PR.
- **Дублирование ролей** — таблица границ; скилы ссылаются на канонические docs.
- **Раздувание скилов** — лимиты строк; «ссылайся, не копируй».
- **Краткий parallel-sessions блок разойдётся с веткой** — follow-up 5.3; в этом PR нет ссылки на невлитый документ.
- **Битые ссылки в скилах не ловит docs-check** — ручная проверка + `skills-check` в 5.2.
- **Работа в общем корне** — отдельный worktree `.worktrees/role-skills`.
