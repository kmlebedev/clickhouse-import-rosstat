# Параллельные сессии Kimi Code — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Две и больше сессий работают над репозиторием одновременно, не мешая друг другу: каждая в своём worktree и своей ветке, общие ресурсы (ClickHouse, стенд, staging) берутся по очереди через lock-хелпер, ветка защищена git-хуком, правила зафиксированы в `AGENTS.md` и `docs/PARALLEL_SESSIONS.md`.

**Architecture:** Разделение по рабочему дереву (`git worktree`, хелпер `scripts/worktree.sh`), сериализация общих ресурсов (`scripts/lock.sh` на атомарном `mkdir`, каталог локов вне репозитория), запрет коммита в защищённую ветку (`.githooks/pre-commit`, ставится `make hooks-install`), опциональная личная страховка (`.kimi-code`-независимый `scripts/agent-guard.sh` + `[[hooks]]` у владельца).

**Tech Stack:** POSIX sh/bash, GNU make, git worktree, macOS (без `flock`), Python не требуется.

**Spec:** `docs/superpowers/specs/2026-10-14-parallel-sessions-design.md`

## Global Constraints

- Работа — в рабочем дереве `.worktrees/parallel-sessions` на ветке `chore/parallel-sessions` (создана от `main`). Первые коммиты ветки — спека и план.
- Правки только в файлах этого пункта: `scripts/`, `.githooks/`, `Makefile`, `AGENTS.md`, `docs/PARALLEL_SESSIONS.md`, `docs/ROADMAP_DCF_POLYUS.md`, `README.md`, `ARCHITECTURE.md`.
- Импортёры, DDL таблиц, витрины, `sql/`, CI (`.github/`), dagu — не трогать.
- Скрипты — POSIX-sh-совместимый `sh`/`bash` без внешних зависимостей (`flock` на macOS нет; использовать `mkdir`). Все скрипты запускаются и на macOS, и на Linux CI.
- Секреты не читать и не печатать; `~/.config/rosstat/env` не трогать.
- Лок-каталог по умолчанию `$HOME/.rosstat-locks` (вне репозитория), переопределяется `ROSSTAT_LOCK_DIR` — нужно и для самотестов.
- Единственный fail-closed механизм — `pre-commit` (коммит в защищённую ветку). Локи и `agent-guard.sh` деградируют безопасно, но не блокируют работу при своей поломке.
- `make all`, `make lint`, `make test` обязаны остаться зелёными; README.md и ARCHITECTURE.md синхронизируются (скил `sync-readme-architecture`).
- Коммиты по-английски, кратко, в стиле истории репозитория.

## Review Focus

1. **Лок реально взаимоисключающий и переживает падение владельца** (Task 1, проверяется `make test-parallel`): второй захват ждёт и называет владельца; лок мёртвого pid'а перехватывается; вложенный `make` внутри своего лока не дедлочится.
2. **Каталог локов вне репозитория** — иначе каждое worktree получает свой лок и защита исчезает (Task 1).
3. **Локи навешены на те цели, которые пишут в общие ресурсы**, и не навешены на read-only (`ch-status`, `lint`, `test`, сборки) — иначе параллельная работа превращается в очередь (Task 2).
4. **`pre-commit` не блокирует нормальную работу**: коммит в рабочей ветке проходит, `--no-verify` остаётся доступен, хук не зависит от стейджинга конкретных файлов (Task 1).
5. **`wt-rm` не теряет незакоммиченную работу** (Task 1), `wt-new` не создаёт дерево на существующей ветке (Task 1).
6. **Правила в `AGENTS.md` совпадают с кодом**: имена целей, ключи локов и пути скриптов в тексте — ровно те, что реализованы (Task 3).

## File Structure

- Create: `scripts/lock.sh` — захват/ожидание/перехват stale-лока, запуск команды под локом
- Create: `scripts/worktree.sh` — `new`/`list`/`rm`/`info` для рабочих деревьев
- Create: `scripts/agent-guard.sh` — блокировка опасных команд для хука Kimi (PreToolUse)
- Create: `scripts/parallel_sessions_test.sh` — самотесты lock/worktree в одноразовом каталоге
- Create: `.githooks/pre-commit` — запрет коммита в защищённую ветку / detached HEAD
- Create: `docs/PARALLEL_SESSIONS.md` — регламент, схема локов, порядок работы
- Modify: `Makefile` — цели `wt-new`/`wt-list`/`wt-rm`/`session-info`/`hooks-install`/`test-parallel`, обёртка пишущих целей локом
- Modify: `AGENTS.md` — раздел «Параллельные сессии» + ссылка на регламент
- Modify: `README.md`, `ARCHITECTURE.md` — новые цели/файлы
- Modify: `docs/ROADMAP_DCF_POLYUS.md` — новый пункт статуса

---

### Task 1: Скрипты (`lock.sh`, `worktree.sh`, `agent-guard.sh`) и git-хук

**Files:**
- Create: `scripts/lock.sh`, `scripts/worktree.sh`, `scripts/agent-guard.sh`, `scripts/parallel_sessions_test.sh`
- Create: `.githooks/pre-commit`

**Interfaces:**
- Consumes: ничего.
- Produces:
  - `scripts/lock.sh <key> -- <command...>`; ключи `db`, `stand`, `staging` (произвольная строка допустима); env `ROSSTAT_LOCK_DIR` (default `$HOME/.rosstat-locks`), `WAIT` (сек, default 900, `0` = не ждать), `STALE` (сек, default 3600); выход `0` — команда выполнена, `3` — лок занят и не дождались, иначе код команды; `ROSSTAT_LOCK_HELD_<key>=1` в окружении команды (reentrancy для вложенного `make`); `scripts/lock.sh status` — печатает активные локи с владельцами.
  - `scripts/worktree.sh new NAME=<slug> [TYPE=feat] [BASE=main]`, `list`, `rm NAME=<slug> [FORCE=1]`, `info`; деревья в `<root>/.worktrees/<slug>`, ветка `<TYPE>/<slug>`.
  - `.githooks/pre-commit`: `exit 0` — можно, `exit 1` — защищённая ветка или detached HEAD (сообщение в stderr с подсказкой `make wt-new`); обход — `ALLOW_PROTECTED_COMMIT=1`.

- [ ] **Step 1: `scripts/lock.sh`**

Атомарный `mkdir` как примитив. Псевдокод обязательного поведения:

```sh
LOCK_DIR=${ROSSTAT_LOCK_DIR:-$HOME/.rosstat-locks}; WAIT=${WAIT:-900}; STALE=${STALE:-3600}
key=$1; shift; [ "$1" = "--" ] && shift
eval "held=\${ROSSTAT_LOCK_HELD_${key}:-}"        # reentrancy
[ "$held" = 1 ] && exec "$@"
lock="$LOCK_DIR/$key.lock"
mkdir -p "$LOCK_DIR"
if mkdir "$lock" 2>/dev/null; then : else
  # owner читается из $lock/owner (key=value: tool, session, pid, host, since, cwd)
  # stale: pid мёртв (kill -0) ИЛИ возраст > STALE → снять и предупредить, иначе ждать WAIT секунд с шагом 1
fi
printf 'tool=%s\nsession=%s\npid=%s\nhost=%s\nsince=%s\ncwd=%s\n' ... > "$lock/owner"
export "ROSSTAT_LOCK_HELD_${key}=1"
trap 'rm -rf "$lock"' EXIT INT TERM
"$@"
```

`session` берётся из env `ROSSTAT_SESSION` (её выставляет обёртка в `Makefile`/`session-info`), при отсутствии — basename рабочего дерева (`git rev-parse --show-toplevel`). Занятый лок печатает в stderr владельца (`key=value` из `owner`) и «жду до N с».

- [ ] **Step 2: `scripts/worktree.sh`**

```sh
root=$(git rev-parse --path-format=absolute --git-common-dir)/..   # общий .git → корень репозитория
wt_dir=$root/.worktrees/$name
```

`new`: проверка slug `^[a-z0-9][a-z0-9._-]*$`; отказ, если есть `$wt_dir` или ветка; `git fetch origin "$base"` (best effort, ошибка — предупреждение, не отказ); `git worktree add "$wt_dir" -b "$type/$name" "$base"`; вывод пути и подсказок (`make hooks-install`, открыть каталог отдельным проектом в GoLand и отдельным workspace в Desktop, `make session-info`). `list`: `git worktree list` плюс пометка «dirty» по `git -C <path> status --porcelain`. `rm`: отказ при непустом `status --porcelain` без `FORCE=1`; `git worktree remove [--force]`; ветка сохраняется. `info`: путь дерева, ветка, «main tree?», dirty, затем `scripts/lock.sh status`.

- [ ] **Step 3: `scripts/agent-guard.sh`**

Читает JSON из stdin (payload Kimi `PreToolUse`: `tool_input.command`, `cwd`), блокирует `exit 2` с причиной в stderr:

- `make ch-down`, `make grafana-down`, `make ch-up` без `FORCE_STAND=1` — «стендом владеет сессия, поднявшая его»;
- `git worktree remove`, `git worktree prune`, `git branch -D` — «дерево/ветка может принадлежать другой сессии»;
- `git checkout main|master|main_worktree`, `git switch main|master|main_worktree`, `git push --force`, `git reset --hard` — «защищённая ветка»;
- `make deploy-staging*` без `FORCE_STAGING=1`;
- `rm -rf` с `.worktrees` в аргументах.

Скрипт активен только внутри этого репозитория: если рядом с корнем нет `.githooks/pre-commit`, печатает ничего и выходит `0`. Любая ошибка разбора JSON — `exit 0` (fail-open, по докам хуков так и задумано).

- [ ] **Step 4: `.githooks/pre-commit`**

```sh
#!/bin/sh
branch=$(git symbolic-ref --quiet --short HEAD) || { echo "detached HEAD: ..." >&2; exit 1; }
case "$branch" in main|master|main_worktree) ... esac
```

Проверка `ALLOW_PROTECTED_COMMIT=1` — разрешить; `--no-verify` работает штатно (git сам обходит хук).

- [ ] **Step 5: `scripts/parallel_sessions_test.sh` — самотесты**

Герметичный прогон в `mktemp -d` (обязательный `trap` на уборку), без сети и без обращения к настоящему `$HOME/.rosstat-locks`:

1. `lock`: первый захват занимает лок, фоновая команда ждёт, после освобождения проходит; `WAIT=0` при занятом локе даёт код `3`; лок с мёртвым pid перехватывается; вложенный запуск с `ROSSTAT_LOCK_HELD_<key>=1` не блокируется; код возврата команды пробрасывается (`false` → не 0).
2. `worktree`: `git clone --local <repo> <tmp>` → `worktree.sh new/list/info/rm`; `rm` отказывает при незакоммиченном файле и проходит с `FORCE=1`; повторный `new` тем же именем отказывает.
3. `pre-commit`: в клоне с `core.hooksPath=.githooks` коммит на `main` отказывает, коммит в `feat/x` проходит.

Скрипт печатает `ok`/`FAIL` по каждому пункту и возвращает непустой код при первой ошибке. Он должен проходить без правок `Makefile` — запускается напрямую (`sh scripts/parallel_sessions_test.sh`), цель `make test-parallel` добавляет Task 2.

---

### Task 2: `Makefile` — цели и лок-обёртка

**Files:**
- Modify: `Makefile`

**Interfaces:**
- Consumes: `scripts/worktree.sh`, `scripts/lock.sh`, `scripts/parallel_sessions_test.sh` (Task 1).
- Produces цели: `wt-new`, `wt-list`, `wt-rm`, `session-info`, `hooks-install`, `test-parallel`; лок-обёртки на пишущих целях.

- [ ] **Step 1: Переменные и `.PHONY`**

```make
WT   ?= scripts/worktree.sh
LOCK ?= scripts/lock.sh
```

В `.PHONY` (строка `Makefile:66`) добавить `wt-new wt-list wt-rm session-info hooks-install test-parallel`.

- [ ] **Step 2: Новые цели**

- `wt-new: @$(WT) new NAME=$(NAME) TYPE=$(strip $(or $(TYPE),feat)) BASE=$(strip $(or $(BASE),main))` + проверка `NAME`;
- `wt-list: @$(WT) list`;
- `wt-rm: @$(WT) rm NAME=$(NAME) $(if $(FORCE),FORCE=1,)`;
- `session-info: @$(WT) info`;
- `hooks-install: git config core.hooksPath .githooks; chmod +x .githooks/pre-commit; echo ...` (идемпотентно);
- `test-parallel: sh scripts/parallel_sessions_test.sh`.

- [ ] **Step 3: Лок-обёртки**

- ключ `stand`: `ch-up`, `ch-down`, `grafana-up`, `grafana-down`, `run-ingest`;
- ключ `db`: `import`, `archive-registry`, `mcp-user`, `ch-sql`;
- ключ `staging`: `deploy-staging*`, `upgrade-staging`.

Обёртка ставится на саму команду, а не на весь рецепт, где это возможно (сборка внутри лока не нужна): например `import` → `CLICKHOUSE_IMPORT_STAT=$(STAT) $(LOCK) db -- ./$(BUILD_DIR)/$(BINARY)-native`. Read-only цели (`ch-status`, `lint`, `fmt`, `vet`, `test`, `build`, `all`, `mcp-check`, `env-check`, `dev-check`, `grafana-check`) лок **не** берут. Если цель-агрегат вызывает подцели через `$(MAKE)`, reentrancy из Task 1 обязана сохранить работоспособность (проверить `deploy-staging`).

`make all` остаётся `lint test build build-ingest` — в набор добавляется только `test-parallel` (см. Step 4).

- [ ] **Step 4: `test-parallel` в `all`**

`all: lint test test-parallel build build-ingest`. Если прогон самотестов окажется медленнее 10 с, вернуть `all` прежний вид и оставить `test-parallel` отдельной целью (и сказать об этом в отчёте).

---

### Task 3: Документация — `AGENTS.md`, регламент, README/ARCHITECTURE, роадмап

**Files:**
- Create: `docs/PARALLEL_SESSIONS.md`
- Modify: `AGENTS.md`, `README.md`, `ARCHITECTURE.md`, `docs/ROADMAP_DCF_POLYUS.md`

**Interfaces:**
- Consumes: имена целей и ключей локов из Task 2 (текст обязан совпадать с кодом).
- Produces: правило протокола в `AGENTS.md`, ссылки на него.

- [ ] **Step 1: Раздел в `AGENTS.md`**

Шесть правил (полный текст — в спеке и в плане ветки): одна сессия → одно дерево → одна ветка → один пункт роадмапа; ветка неприкосновенна (`make wt-new`, `make hooks-install`); общие ресурсы под локом (`db`/`stand`/`staging`), `ch-down`/`grafana-down`/`git worktree remove` — только владелец и по явной просьбе; JetBrains MCP всегда с `projectPath` своего дерева; один писатель на артефакт (`ARCHITECTURE.md`, `sql/*`, `dashboard/*.json`, `docs/archive/registry.jsonl`, `.superpowers/sdd/<тема>/`); сборка и тесты — в своём дереве. Раздел ставится перед «Календарь и приоритеты», ссылается на `docs/PARALLEL_SESSIONS.md`.

- [ ] **Step 2: `docs/PARALLEL_SESSIONS.md`**

Регламент: схема (дерево/ветка/локи/IDE), таблица «ресурс → общий? → чем закрыт», порядок работы сессии (создание дерева → открытие в GoLand/Desktop → работа → приёмка → PR), таблица локов с ключами и целями, поведение при занятом локе и при конфликте, инструкция подключения личных `[[hooks]]` (фрагмент `config.toml` + что блокирует `agent-guard.sh`), раздел «чего правило не гарантирует» (fail-open хуки, один инстанс IDE, общий ClickHouse как точка сериализации).

- [ ] **Step 3: README.md и ARCHITECTURE.md**

README: столбец/пункт про параллельные сессии в разделе разработки (цели `make wt-new`/`wt-list`/`wt-rm`/`hooks-install`/`session-info`/`test-parallel`). ARCHITECTURE.md: локальные стенды и их общие ресурсы, где живут локи (`$HOME/.rosstat-locks`), правило «одно дерево — одна сессия». Пройтись по скилу `sync-readme-architecture`.

- [ ] **Step 4: `docs/ROADMAP_DCF_POLYUS.md`**

В §7 новый подпункт (`### 7.3 Инфраструктура разработки: параллельные сессии — ✅ выполнен 2026-10-14`) со ссылками на спеку/план, перечнем артефактов и блоком «приёмка» (команды и результаты — заполнить фактическими из Task 4).

---

### Task 4: Приёмка — живой прогон на двух деревьях

**Files:**
- Modify (по факту находок): файлы Task 1–3.

**Interfaces:**
- Consumes: всё предыдущее.
- Produces: заполненный блок приёмки в `docs/ROADMAP_DCF_POLYUS.md` и отчёт.

- [ ] **Step 1:** `make hooks-install`; в рабочем дереве `make wt-new NAME=probe-a` и `NAME=probe-b`; `make wt-list` показывает оба; попытка `git -C .worktrees/probe-b checkout feat/probe-a` падает (git не даёт две ветки в одном дереве).
- [ ] **Step 2:** в каждом пробном дереве `git commit` в своей ветке проходит; в корневом дереве на `main` — отказ `pre-commit`.
- [ ] **Step 3:** два одновременных `make import STAT=<два разных стата>`: второй лог содержит владельца лока и ожидание; оба завершаются; `SELECT count() FROM <таблица> FINAL` не ломается (прогон — при поднятом ClickHouse; если он не отвечает, зафиксировать это в отчёте как непроверенное и не выдумывать результат).
- [ ] **Step 4:** `make all` в рабочем дереве зелёный; вывод и версии — в отчёт и в роадмап.
- [ ] **Step 5:** убрать пробные деревья (`make wt-rm NAME=probe-a`, `NAME=probe-b`), проверить `git worktree list`.

---

## Открытые вопросы (решить при исполнении)

- Нужен ли `make wt-rm` ветке `--force` при удалении незапушенной ветки: по умолчанию ветка сохраняется, удаление — отдельным шагом владельца.
- Подключать ли `test-parallel` в `make all` (Step 4 Task 2) — зависит от времени прогона.
- Нужна ли сессии отдельная переменная `ROSSTAT_SESSION` для `owner`-файла: имя берётся из имени дерева, отдельной настройки не требуется, если имя дерева читаемо.
