#!/bin/sh
# scripts/agent-guard.sh — личная страховка владельца от команд, гасящих общие
# ресурсы или теряющих чужую работу. Подключается user-level хуком Kimi Code
# ([[hooks]] в ~/.kimi-code/config.toml — файл личный, в репозиторий не коммитится;
# фрагмент конфига лежит в docs/PARALLEL_SESSIONS.md). Событие — PreToolUse.
#
# Скрипт читает JSON события из stdin, берёт tool_input.command (и cwd), ищет
# опасные вызовы и блокирует их: exit 2, причина — в stderr (так устроены хуки).
# Любая ошибка разбора JSON или отсутствие полей → exit 0: хуки Kimi fail-open,
# это страховка от случайности, а не барьер. Барьеры — .githooks/pre-commit
# (fail-closed на коммит в защищённую ветку) и scripts/lock.sh.
#
# Скрипт активен только внутри ЭТОГО репозитория, и признак «тот самый» не зависит от
# содержимого чекаута: вверх по cwd ищется либо .githooks/pre-commit (дерево от ветки,
# где хук уже влит), либо корень git-репозитория, у которого путь origin — ровно
# …/clickhouse-import-rosstat или …/clickhouse-import-rosstat.git (корневое дерево и
# деревья, созданные ДО этой ветки, где .githooks в чекауте ещё нет). Сравнение именно по
# пути, а не по вхождению подстроки: чужой форк (…/clickhouse-import-rosstat-fork.git)
# блокировок не получает. Вне этого репозитория — молча exit 0.
#
# Ручная проверка:
#   echo '{"cwd":"'"$PWD"'","tool_input":{"command":"make ch-down"}}' | sh scripts/agent-guard.sh; echo $?

set -u

payload=$(cat) || exit 0
[ -n "$payload" ] || exit 0

# Мини-разбор JSON на awk: jq в зависимостях нет, а awk есть и на macOS, и в CI.
# Экранированные кавычки в строковых значениях учитываются — иначе значение
# "cwd" внутри чужого описания принималось бы за настоящее поле.
parsed=$(
	printf '%s' "$payload" | awk '
function skip_ws(s, i,    c) {
	while (i <= length(s)) {
		c = substr(s, i, 1)
		if (c == " " || c == "\t" || c == "\n" || c == "\r") i++
		else break
	}
	return i
}

function hex2num(h,    i, c, d, v) {
	v = 0
	if (length(h) == 0) return -1
	for (i = 1; i <= length(h); i++) {
		c = tolower(substr(h, i, 1))
		d = index("0123456789abcdef", c)
		if (d == 0) return -1
		v = v * 16 + (d - 1)
	}
	return v
}

# read_string(s, i): i — позиция открывающей кавычки; значение попадает
# в глобальную str_val, возврат — позиция за закрывающей кавычкой (0 — не закрыта).
function read_string(s, i,    j, c, esc, u, code) {
	str_val = ""
	j = i + 1
	while (j <= length(s)) {
		c = substr(s, j, 1)
		if (c == "\\") {
			esc = substr(s, j + 1, 1)
			if (esc == "u") {
				code = hex2num(substr(s, j + 2, 4))
				if (code >= 32 && code < 127) str_val = str_val sprintf("%c", code)
				else str_val = str_val " "
				j += 6
			} else {
				if (esc == "n" || esc == "t" || esc == "r") str_val = str_val " "
				else str_val = str_val esc
				j += 2
			}
		} else if (c == "\"") {
			return j + 1
		} else {
			str_val = str_val c
			j++
		}
	}
	return 0
}

# find_key(s, key, from): позиция кавычки ключа, пропуская экранированные кавычки.
function find_key(s, key, from,    pat, pos, abs) {
	pat = "\"" key "\""
	pos = index(substr(s, from), pat)
	while (pos > 0) {
		abs = from + pos - 1
		if (abs == 1 || substr(s, abs - 1, 1) != "\\") return abs
		pos = index(substr(s, from + pos), pat)
	}
	return 0
}

# value_at(s, key, from): позиция открывающей кавычки значения "key": "...".
function value_at(s, key, from,    p, i) {
	p = find_key(s, key, from)
	if (p == 0) return 0
	i = skip_ws(s, p + length(key) + 2)
	if (substr(s, i, 1) != ":") return 0
	i = skip_ws(s, i + 1)
	if (substr(s, i, 1) != "\"") return 0
	return i
}

{ if (NR > 1) s = s "\n"; s = s $0 }

END {
	cwd = ""
	p = value_at(s, "cwd", 1)
	if (p > 0) { read_string(s, p); cwd = str_val }

	cmd = ""
	t = find_key(s, "tool_input", 1)
	p = value_at(s, "command", (t > 0) ? t : 1)
	if (p == 0 && t > 0) p = value_at(s, "command", 1)
	if (p > 0) { read_string(s, p); cmd = str_val }

	print cwd
	print cmd
}
' 2>/dev/null
) || exit 0

cwd=$(printf '%s\n' "$parsed" | sed -n '1p')
cmd=$(printf '%s\n' "$parsed" | sed -n '2p')
[ -n "$cwd" ] || exit 0
[ -n "$cmd" ] || exit 0

# «origin — это тот самый репозиторий»: путь сравнивается ЦЕЛИКОМ, а не по вхождению
# подстроки, иначе чужой форк (…/clickhouse-import-rosstat-fork.git) получал бы блокировки
# гарда. Хвост .git и завершающий слеш нормализуются; ssh-форма
# (git@github.com:kmlebedev/clickhouse-import-rosstat.git) разбирается тем же сравнением —
# имя репозитория стоит в конце пути.
origin_is_this_repo() {
	l_url=$1
	case "$l_url" in
	'') return 1 ;;
	esac
	case "$l_url" in
	*/) l_url=${l_url%/} ;;
	esac
	case "$l_url" in
	*.git) l_url=${l_url%.git} ;;
	esac
	case "$l_url" in
	*/clickhouse-import-rosstat|*:clickhouse-import-rosstat) return 0 ;;
	esac
	return 1
}

# Активен только внутри ЭТОГО репозитория. Признак не зависит от содержимого чекаута:
# поднимаемся от cwd вверх и активируемся, если нашли .githooks/pre-commit (дерево от
# ветки, где хук уже влит; альтернатива на случай переименования origin) или корень git,
# у которого путь origin — ровно …/clickhouse-import-rosstat(.git) (корневое дерево и
# деревья, созданные ДО этой ветки, где .githooks в чекауте ещё нет). Вне репозитория — 0.
in_repo=0
dir=$cwd
while [ -n "$dir" ]; do
	if [ -f "$dir/.githooks/pre-commit" ]; then
		in_repo=1
		break
	fi
	if [ -e "$dir/.git" ]; then
		if origin_is_this_repo "$(git -C "$dir" remote get-url origin 2>/dev/null)"; then
			in_repo=1
			break
		fi
		# Чужой .git (вложенный репозиторий или форк) — подъём продолжаем: свой
		# корень может быть выше.
	fi
	parent=$(dirname -- "$dir")
	[ "$parent" = "$dir" ] && break
	dir=$parent
done
if [ "$in_repo" != 1 ]; then
	exit 0
fi

# Нормализация для сопоставления: одна строка, одиночные пробелы, «git -C <путь>»
# сведён к «git» (иначе варианты запуска из другого дерева не ловятся).
norm=$(printf '%s' "$cmd" | tr '\n\t' '  ' | tr -s ' ')
norm=$(printf '%s' "$norm" | sed 's/ -C [^ ]* / /g')

block() {
	printf 'agent-guard: %s\n' "$1" >&2
	printf 'agent-guard: команда заблокирована хуком Kimi (scripts/agent-guard.sh); при необходимости попросите владельца снять запрет.\n' >&2
	exit 2
}

# Цели make: глобальные флаги перед целью (-C <каталог>, -s, -j, -jN, -k,
# --no-print-directory, -f <файл>) и присваивания VAR=value не мешают распознать
# цель. Печатает первую цель каждого вызова make в команде — по одной на строку.
make_targets() {
	set -f
	l_make=0
	l_skip=0
	for l_tok in $1; do
		if [ "$l_make" = 0 ]; then
			case "$l_tok" in
			make|*/make) l_make=1 ;;
			esac
			continue
		fi
		if [ "$l_skip" -gt 0 ]; then
			l_skip=$((l_skip - 1))
			continue
		fi
		case "$l_tok" in
		-C|-f|-o|-W|-I|--directory|--file|--makefile|--eval|--include-dir)
			l_skip=1
			;;
		-*) : ;;
		*=*) : ;;
		*)
			printf '%s\n' "$l_tok"
			l_make=0
			;;
		esac
	done
	set +f
	return 0
}

# Стенд общий на все сессии: погасить чужой или поднять поверх чужого — сломать
# соседям. preflight/smoke лок staging не берут и ничего не мутируют — не блокируем.
guard_targets=$(make_targets "$norm")
set -f
for guard_target in $guard_targets; do
	case "$guard_target" in
	ch-down|grafana-down)
		block 'make ch-down / make grafana-down: стендом владеет сессия, которая его подняла; гашение — только владелец и по явной просьбе пользователя'
		;;
	ch-up)
		case "$norm" in
		*FORCE_STAND=1*) ;;
		*) block 'make ch-up: стенд уже может быть поднят другой сессией; осознанный повторный подъём — FORCE_STAND=1' ;;
		esac
		;;
	deploy-staging-preflight|deploy-staging-smoke)
		: # лок не берут и ничего не мутируют
		;;
	deploy-staging*|upgrade-staging)
		case "$norm" in
		*FORCE_STAGING=1*) ;;
		*) block 'make deploy-staging... / make upgrade-staging: staging-хост один на все сессии (лок staging, рестарт systemd); осознанный прогон — FORCE_STAGING=1' ;;
		esac
		;;
	esac
done
set +f

# Дерево и ветка принадлежат сессии: снос уносит её незакоммиченную работу.
case "$norm" in
*"git worktree remove"*|*"git worktree prune"*)
	block 'git worktree remove/prune: дерево может принадлежать другой сессии; удаление — через scripts/worktree.sh rm NAME=<slug> и только своё'
	;;
*"git branch -D"*)
	block 'git branch -D: ветка может содержать незапушенную работу другой сессии (безопасная форма — git branch -d)'
	;;
esac

# Границы слова: имя защищённой ветки в "git switch main_worktree" не должно
# срабатывать как "git switch main" — поэтому обе стороны окружены пробелами.
padded=" $norm "

for protected in main master main_worktree; do
	case "$padded" in
	*"git checkout $protected "*|*"git switch $protected "*|*"git checkout -B $protected "*|*"git switch -C $protected "*)
		block "git checkout/switch $protected: защищённая ветка принадлежит другой сессии — работайте в своём дереве и ветке (make wt-new NAME=<slug>)"
		;;
	esac
done

case "$norm" in
*git*push*)
	case "$norm" in
	*--force-with-lease*) ;; # безопасная форма форс-пуша
	*--force*|*" -f"*)
		block 'git push --force: перезаписывает историю ветки на origin; для своей ветки используйте --force-with-lease'
		;;
	esac
	;;
esac

case "$norm" in
*git*reset*--hard*)
	block 'git reset --hard: молча теряет незакоммиченные правки (в общем дереве — чужие)'
	;;
esac

# Разбор команды по токенам: метасимволы оболочки отделяются пробелами, чтобы
# `rm -rf build && ls .worktrees` не читалось как один вызов rm с чужими аргументами.
tokenize() {
	printf '%s' "$1" | sed 's/[;&|()<>]/ & /g' | tr -s ' '
}

# Где идёт работа: в КОРНЕВОМ дереве репозитория или в linked worktree? У корневого
# дерева .git — каталог, у linked worktree — файл-указатель; ближайший вверх каталог
# с .git и решает. Подкаталог cwd поднимается до своего корня сам (цикл).
# Вернуть 0 (опасно, корневое дерево) или 1 (своё linked worktree).
# Если .git не нашёлся вовсе (странный/недоступный путь), возвращаем 0: fail-closed
# здесь безопаснее ложного пропуска — лишняя блокировка объясняется в stderr.
in_root_tree() {
	l_dir=$1
	while [ -n "$l_dir" ]; do
		if [ -d "$l_dir/.git" ]; then
			return 0
		fi
		if [ -e "$l_dir/.git" ]; then
			return 1
		fi
		l_parent=$(dirname -- "$l_dir")
		[ "$l_parent" = "$l_dir" ] && break
		l_dir=$l_parent
	done
	return 0
}

# git clean, git stash drop и find .worktrees -delete опасны в ОБЩЕМ корневом дереве
# (там работают все сессии); в своём linked worktree это своё дело — пропускаем.
guard_root_tree=1
in_root_tree "$cwd" || guard_root_tree=0

# Путь лежит в каталоге .worktrees — по компонентам пути, а не по подстроке:
# файл с «.worktrees» в имени (или `ls .worktrees`) локом не считается.
path_in_worktrees() {
	case "$1" in
	.worktrees|.worktrees/*|./.worktrees|./.worktrees/*) return 0 ;;
	*/.worktrees|*/.worktrees/*) return 0 ;;
	esac
	return 1
}

# git clean без -n/--dry-run уносит неотслеживаемые файлы текущего дерева.
git_clean_dangerous() {
	set -f
	l_state=0
	l_dry=0
	l_danger=0
	for l_tok in $(tokenize "$1"); do
		case "$l_tok" in
		'&&'|'||'|';'|'|'|'&'|'('|')'|'<'|'>')
			[ "$l_state" = 2 ] && [ "$l_dry" = 0 ] && l_danger=1
			l_state=0
			continue
			;;
		git|*/git)
			[ "$l_state" = 2 ] && [ "$l_dry" = 0 ] && l_danger=1
			l_state=1
			continue
			;;
		esac
		case "$l_state" in
		1)
			case "$l_tok" in
			-*|*=*) ;;
			clean) l_state=2; l_dry=0 ;;
			*) l_state=0 ;;
			esac
			;;
		2)
			case "$l_tok" in
			-n|--dry-run) l_dry=1 ;;
			-*) ;;
			*) l_state=0 ;;
			esac
			;;
		esac
	done
	[ "$l_state" = 2 ] && [ "$l_dry" = 0 ] && l_danger=1
	set +f
	[ "$l_danger" = 1 ] && return 0
	return 1
}

# git stash drop выбрасывает чужую отложенную работу (набор stash общий на дерево).
git_stash_drop() {
	set -f
	l_state=0
	for l_tok in $(tokenize "$1"); do
		case "$l_tok" in
		'&&'|'||'|';'|'|'|'&'|'('|')'|'<'|'>')
			l_state=0
			;;
		git|*/git)
			l_state=1
			;;
		*)
			case "$l_state" in
			1)
				case "$l_tok" in
				-*|*=*) ;;
				stash) l_state=2 ;;
				*) l_state=0 ;;
				esac
				;;
			2)
				case "$l_tok" in
				drop) set +f; return 0 ;;
				-*|*=*) ;;
				*) l_state=0 ;;
				esac
				;;
			esac
			;;
		esac
	done
	set +f
	return 1
}

# find <путь в .worktrees> ... -delete сносит деревья всех сессий.
find_deletes_worktrees() {
	set -f
	l_state=0
	l_wt=0
	for l_tok in $(tokenize "$1"); do
		case "$l_tok" in
		'&&'|'||'|';'|'|'|'&'|'('|')'|'<'|'>')
			l_state=0
			l_wt=0
			;;
		find|*/find)
			l_state=1
			l_wt=0
			;;
		*)
			if [ "$l_state" = 1 ]; then
				case "$l_tok" in
				-delete|--delete)
					[ "$l_wt" = 1 ] && { set +f; return 0; }
					;;
				-*) ;;
				*)
					path_in_worktrees "$l_tok" && l_wt=1
					;;
				esac
			fi
			;;
		esac
	done
	set +f
	return 1
}

# Порядок важен: git clean досматривается по границам метасимволов, поэтому
# `git clean -fd` в одном дереве блокируется, а `git clean -n` — нет.
# Все три проверки — только для корневого дерева ($guard_root_tree=1): в своём
# linked worktree сессия вправе чистить свой build/ и свой stash, а чужих деревьев
# там рядом нет. Не смогли определить дерево — считаем корневым и блокируем.
if [ "$guard_root_tree" = 1 ] && git_clean_dangerous "$norm"; then
	block 'git clean -f/-fd/-fdx в корневом дереве: уносит неотслеживаемые файлы, которые могут быть чужими (безопасная форма — git clean -n); в своём worktree чистка разрешена'
fi
if [ "$guard_root_tree" = 1 ] && git_stash_drop "$norm"; then
	block 'git stash drop в корневом дереве: выбрасывает отложенную работу, которая может быть чужой (безопасная форма — git stash list и точечное применение); в своём worktree — своё дело'
fi
if [ "$guard_root_tree" = 1 ] && find_deletes_worktrees "$norm"; then
	block 'find .worktrees … -delete в корневом дереве: сносит деревья всех сессий вместе с незакоммиченной работой; своё дерево удаляйте через scripts/worktree.sh rm NAME=<slug>'
fi

# rm с аргументом-путём внутри .worktrees сносит деревья сессий. Проверяем именно
# пути-аргументы rm, а не подстроку «.worktrees» в команде.
rm_touches_worktrees() {
	set -f
	l_inrm=0
	for l_tok in $(tokenize "$1"); do
		case "$l_tok" in
		'&&'|'||'|';'|'|'|'&'|'('|')'|'<'|'>')
			l_inrm=0
			;;
		rm|*/rm)
			l_inrm=1
			;;
		*)
			if [ "$l_inrm" = 1 ]; then
				case "$l_tok" in
				-*) ;;
				*)
					path_in_worktrees "$l_tok" && { set +f; return 0; }
					;;
				esac
			fi
			;;
		esac
	done
	set +f
	return 1
}

if rm_touches_worktrees "$norm"; then
	block 'rm по .worktrees: сносит деревья всех сессий вместе с незакоммиченной работой; своё дерево удаляйте через scripts/worktree.sh rm NAME=<slug>'
fi

exit 0
