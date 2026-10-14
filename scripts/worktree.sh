#!/bin/sh
# scripts/worktree.sh — рабочие деревья для параллельных сессий: одна сессия — одно
# дерево, одна ветка. Корень репозитория вычисляется от ОБЩЕГО .git
# (git rev-parse --git-common-dir), поэтому команда работает одинаково и из корня,
# и из любого дерева; деревья живут в <root>/.worktrees/<slug> (каталог в .gitignore).
#
# Использование:
#   scripts/worktree.sh new NAME=<slug> [TYPE=feat] [BASE=main]
#   scripts/worktree.sh list
#   scripts/worktree.sh rm NAME=<slug> [FORCE=1]
#   scripts/worktree.sh info
#
# BASE и TYPE — из env или из аргументов KEY=VALUE. Сеть нужна только в new
# (git fetch origin BASE, best effort: неудачный fetch — предупреждение, не отказ).
# База дерева — origin/$BASE (его обновляет fetch), локальный refs/heads/$BASE —
# только если origin/$BASE нет. NAME проверяется одним valid_slug в new и в rm,
# TYPE — как имя ветки (git check-ref-format --branch) в new.

set -u

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)

usage() {
	cat >&2 <<'EOF'
Использование:
  scripts/worktree.sh new NAME=<slug> [TYPE=feat] [BASE=main]   создать дерево и ветку TYPE/NAME
  scripts/worktree.sh list                                      деревья с ветками и пометкой dirty
  scripts/worktree.sh rm NAME=<slug> [FORCE=1]                  удалить дерево (ветка остаётся)
  scripts/worktree.sh info                                      дерево, ветка, состояние и активные локи
EOF
}

die() {
	printf 'worktree.sh: %s\n' "$1" >&2
	exit 1
}

valid_slug() {
	case "$1" in
	''|*[!a-z0-9._-]*) return 1 ;;
	[!a-z0-9]*) return 1 ;;
	esac
	return 0
}

dirty_state() {
	# dirty_state <путь дерева>
	if [ -n "$(git -C "$1" status --porcelain 2>/dev/null)" ]; then
		printf 'dirty'
	else
		printf 'clean'
	fi
}

new_worktree() {
	[ -n "$NAME" ] || die 'new: не задан NAME=<slug>'
	valid_slug "$NAME" || die "new: недопустимый NAME='$NAME' (ожидается [a-z0-9][a-z0-9._-]*)"
	[ -n "$TYPE" ] || die 'new: пустой TYPE'
	case "$TYPE" in
	*[!A-Za-z0-9._/-]*) die "new: недопустимый TYPE='$TYPE'" ;;
	esac
	# TYPE — префикс имени ветки, поэтому проверяется как имя ветки: произвольная строка
	# («..», «@{-1}», «-x») не должна превратиться в ветку через git worktree add -b.
	git check-ref-format --branch "$TYPE" >/dev/null 2>&1 ||
		die "new: недопустимый TYPE='$TYPE' (это не имя ветки: git check-ref-format --branch)"

	wt="$root/.worktrees/$NAME"
	branch="$TYPE/$NAME"
	git check-ref-format --branch "$branch" >/dev/null 2>&1 ||
		die "new: '$branch' — не имя ветки (TYPE=$TYPE, NAME=$NAME)"

	[ -e "$wt" ] && die "new: каталог уже существует: $wt (возьмите другое имя или удалите его дерево)"
	if git show-ref --verify --quiet "refs/heads/$branch"; then
		die "new: ветка $branch уже существует — дерево на существующей ветке не создаём"
	fi

	mkdir -p "$root/.worktrees" || die "new: не удалось создать $root/.worktrees"
	# Свежий BASE: обновить его может только fetch, локальный ref отстаёт. Неудачный
	# fetch origin/$BASE (нет сети, нет такого ref на remote) — не повод отказывать:
	# тогда работаем на локальном ref, но говорим об этом прямо.
	git fetch --quiet origin "$BASE" 2>/dev/null || true
	if git rev-parse --verify --quiet "refs/remotes/origin/$BASE^{commit}" >/dev/null; then
		base_ref="origin/$BASE"
	else
		printf 'worktree.sh: предупреждение: fetch origin/%s не удался, база — локальный ref refs/heads/%s\n' "$BASE" "$BASE" >&2
		base_ref="refs/heads/$BASE"
	fi
	# Базу приводим к коммиту: git worktree add -b ВЕТКА main при отсутствии локальной
	# main заводит её сам (DWIM до origin/main) и оставляет новое дерево на защищённой
	# ветке. С явным sha ветка дерева всегда та, что просили.
	base_sha=$(git rev-parse --verify --quiet "$base_ref^{commit}") || base_sha=''
	[ -n "$base_sha" ] || die "new: BASE=$BASE не найден ни как origin/$BASE, ни как локальный ref refs/heads/$BASE"
	git worktree add "$wt" -b "$branch" "$base_sha" || die "new: git worktree add не удался (BASE=$BASE)"

	base_subject=$(git log -1 --pretty=%h "$base_sha" 2>/dev/null)

	cat <<EOF

Дерево: $wt
Ветка:  $branch (от $base_ref $base_subject)

Дальше:
  1. Откройте (приаттачьте) $wt отдельным проектом в GoLand и отдельным workspace
     в Kimi Code Desktop: у каждого дерева свой HEAD, индекс и build/.
     Без этого файлов дерева нет в индексе IDE, и всё, что вернут поиск, символы
     и git-статус, относится к чужому дереву; после открытия projectPath выдачу
     НЕ сужает — свои строки отбирайте по префиксу пути своего дерева,
     а git-статус — по записи с путём своего дерева.
  2. Если хуки ещё не ставились: make hooks-install (один раз на репозиторий,
     действует сразу во всех деревьях).
  3. Контекст сессии: make session-info.
EOF
	return 0
}

list_worktrees() {
	# Формат git worktree list (путь, sha, ветка) плюс пометка dirty/clean.
	# Разбор через porcelain: пути могут содержать пробелы.
	porcelain=$(git worktree list --porcelain)
	printf '%s\n\n' "$porcelain" | while IFS= read -r line; do
		case "$line" in
		'')
			if [ -n "$wt_path" ]; then
				# Каталога дерева может уже не быть (снесли руками): запись о таком
				# дереве снимает git worktree prune — так и помечаем, а не «clean».
				if [ -d "$wt_path" ]; then
					wt_state=$(dirty_state "$wt_path")
				else
					wt_state='missing (git worktree prune)'
				fi
				printf '%s %s%s  %s\n' "$wt_path" "$wt_head" "$wt_ref" "$wt_state"
			fi
			wt_path=''
			wt_head=''
			wt_ref=''
			;;
		'worktree '*) wt_path=${line#worktree } ;;
		'HEAD '*) wt_head=$(printf '%s' "${line#HEAD }" | cut -c1-7) ;;
		'branch '*)
			wt_ref=${line#branch refs/heads/}
			[ "$wt_ref" = "$line" ] && wt_ref=${line#branch }
			wt_ref=" [$wt_ref]"
			;;
		'detached') wt_ref=' (detached HEAD)' ;;
		'bare') wt_ref=' (bare)' ;;
		esac
	done
	return 0
}

rm_worktree() {
	[ -n "$NAME" ] || die 'rm: не задан NAME=<slug>'
	valid_slug "$NAME" || die "rm: недопустимый NAME='$NAME' (ожидается [a-z0-9][a-z0-9._-]*)"
	wt="$root/.worktrees/$NAME"
	if [ ! -d "$wt" ]; then
		# Каталога нет: дерево могли снести руками — тогда запись о нём остаётся в git.
		printf 'rm: каталога дерева нет: %s\n' "$wt" >&2
		printf 'если дерево сносили руками (rm -rf .worktrees/<slug>), запись о нём снимает: git worktree prune\n' >&2
		die "rm: дерева нет: $wt"
	fi

	branch=$(git -C "$wt" rev-parse --abbrev-ref HEAD 2>/dev/null)
	dirty=$(git -C "$wt" status --porcelain 2>/dev/null)
	if [ -n "$dirty" ] && [ "$FORCE" != 1 ]; then
		printf '%s\n' "$dirty" >&2
		die "rm: в дереве $wt есть незакоммиченные изменения — сначала закоммитьте их; осознанный обход: FORCE=1 (изменения будут потеряны)"
	fi

	if [ "$FORCE" = 1 ]; then
		git worktree remove --force "$wt" || die "rm: git worktree remove --force не удался"
	else
		git worktree remove "$wt" || die "rm: git worktree remove не удался"
	fi

	printf 'дерево удалено: %s\n' "$wt"
	if [ -n "$branch" ] && [ "$branch" != 'HEAD' ]; then
		printf 'ветка сохранена: %s (удаление ветки — отдельным шагом владельца: git branch -D %s)\n' "$branch" "$branch"
	fi
	printf 'если каталог дерева сносили руками (rm -rf .worktrees/<slug>), запись о нём снимает: git worktree prune\n'
	return 0
}

info() {
	tree=$(git rev-parse --show-toplevel 2>/dev/null) || die 'info: это не git-репозиторий'
	branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null)
	[ "$branch" = 'HEAD' ] && branch='(detached)'
	kind='отдельное дерево (worktree)'
	[ "$tree" = "$root" ] && kind='корневое дерево репозитория'

	printf 'дерево:   %s (%s)\n' "$tree" "$kind"
	printf 'ветка:    %s\n' "$branch"
	printf 'сессия:   %s (имя для owner-файлов локов)\n' "${ROSSTAT_SESSION:-${tree##*/}}"
	printf 'состояние: %s\n' "$(dirty_state "$tree")"
	printf 'каталог локов: %s\n' "${ROSSTAT_LOCK_DIR:-$HOME/.rosstat-locks}"
	printf '\nактивные локи:\n'
	if [ -f "$script_dir/lock.sh" ]; then
		sh "$script_dir/lock.sh" status
	else
		printf '  scripts/lock.sh не найден рядом с worktree.sh\n'
	fi
	return 0
}

cmd=${1:-}
[ $# -gt 0 ] && shift

case "$cmd" in
''|help|-h|--help)
	usage
	[ -z "$cmd" ] && exit 2
	exit 0
	;;
new|list|rm|info) ;;
*)
	printf 'worktree.sh: неизвестная команда: %s\n' "$cmd" >&2
	usage
	exit 2
	;;
esac

NAME=${NAME:-}
TYPE=${TYPE:-feat}
BASE=${BASE:-main}
FORCE=${FORCE:-}

for arg in "$@"; do
	case "$arg" in
	NAME=*) NAME=${arg#NAME=} ;;
	TYPE=*) TYPE=${arg#TYPE=} ;;
	BASE=*) BASE=${arg#BASE=} ;;
	FORCE=*) FORCE=${arg#FORCE=} ;;
	*)
		printf 'worktree.sh: неизвестный аргумент: %s (ожидается KEY=VALUE)\n' "$arg" >&2
		usage
		exit 2
		;;
	esac
done

# Корень — от общего .git, а не от текущего дерева: в worktree каталог .git
# указывает на <root>/.git/worktrees/<name>, и --git-common-dir возвращает <root>/.git.
common=$(git rev-parse --path-format=absolute --git-common-dir 2>/dev/null) ||
	common=$(git rev-parse --git-common-dir 2>/dev/null) ||
	common=''
[ -n "$common" ] || die 'это не git-репозиторий (git rev-parse не нашёл .git)'
root=$(CDPATH= cd -- "$(dirname -- "$common")" && pwd -P) || die "не удалось определить корень репозитория по $common"

case "$cmd" in
new) new_worktree ;;
list) list_worktrees ;;
rm) rm_worktree ;;
info) info ;;
esac
