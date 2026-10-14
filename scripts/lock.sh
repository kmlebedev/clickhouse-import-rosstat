#!/bin/sh
# scripts/lock.sh — сериализация доступа к общим ресурсам параллельных сессий
# (локальный ClickHouse, стенд, staging). Примитив — атомарный mkdir: flock
# на macOS нет. Каталог локов лежит ВНЕ репозитория (по умолчанию
# $HOME/.rosstat-locks): иначе каждое рабочее дерево получило бы свой лок,
# и защита исчезла бы.
#
# Использование:
#   scripts/lock.sh <key> -- <команда...>   выполнить команду под локом <key>
#   scripts/lock.sh status                  показать активные локи
#
# Env: ROSSTAT_LOCK_DIR (default $HOME/.rosstat-locks), WAIT (сек, default 900,
# 0 — не ждать вовсе), GRACE (сек, default 5 — окно между mkdir и записью
# owner-файла), ROSSTAT_SESSION (имя сессии в owner-файле, default — basename
# рабочего дерева), ROSSTAT_LOCK_TOOL (поле tool в owner-файле; default — сама
# команда).
#
# Перехват брошенного лока — ТОЛЬКО если pid владельца не отвечает (kill -0 не
# проходит) ИЛИ owner-файла нет и лок простоял >= GRACE секунд. Возраст лока сам
# по себе не перехватывает ничего: живой владелец держит лок сколько нужно.
# Перехват идёт под мьютексом "$lock.takeover" (у него свой stale-таймаут, GRACE):
# под мьютексом владелец перечитывается, и только потом лок сносится — иначе два
# ожидающих могли снести свежий лок друг друга.
#
# Коды возврата: код команды; 3 — лок занят и не дождались; 2 — ошибка вызова.

set -u

LOCK_DIR=${ROSSTAT_LOCK_DIR:-$HOME/.rosstat-locks}
WAIT=${WAIT:-900}
GRACE=${GRACE:-5}

usage() {
	cat >&2 <<'EOF'
Использование:
  scripts/lock.sh <key> -- <команда...>   выполнить команду под локом <key>
  scripts/lock.sh status                  показать активные локи
Env: ROSSTAT_LOCK_DIR, WAIT (сек, 0 — не ждать), GRACE (сек — окно без
owner-файла, после которого лок считается брошенным), ROSSTAT_SESSION,
ROSSTAT_LOCK_TOOL.
EOF
}

now() {
	date +%s
}

# Возраст каталога в секундах. GNU и BSD stat несовместимы по ключам, поэтому
# выбор по uname, а не по принципу «попробуем оба»: у GNU stat -f %m завершается
# успешно и печатает точку монтирования, то есть fallback не сработал бы.
lock_age() {
	case "$(uname -s)" in
	Darwin) stat -f %m "$1" 2>/dev/null ;;
	*) stat -c %Y "$1" 2>/dev/null ;;
	esac
}

alive() {
	case "$1" in
	''|*[!0-9]*) return 1 ;;
	esac
	kill -0 "$1" 2>/dev/null
}

owner_field() {
	# owner_field <owner-файл> <поле>
	[ -f "$1" ] || return 0
	sed -n "s/^$2=//p" "$1" | head -n 1
}

owner_line() {
	# Владелец одной строкой — для сообщений в stderr.
	[ -f "$1" ] || return 0
	tr '\n' ' ' <"$1"
}

# stale_reason_for <каталог лока>: печатает причину «лок брошен» или пустоту.
# Брошен = pid владельца не отвечает ИЛИ owner-файла нет и лок простоял >= GRACE
# (окно между mkdir и записью owner). Возраст лока у живого владельца — не повод.
stale_reason_for() {
	l_owner_pid=$(owner_field "$1/owner" pid)
	l_mtime=$(lock_age "$1")
	[ -n "$l_mtime" ] || l_mtime=$(now)
	l_age=$(( $(now) - l_mtime ))
	if [ -f "$1/owner" ]; then
		if [ -z "$l_owner_pid" ] || ! alive "$l_owner_pid"; then
			printf 'pid владельца %s не отвечает' "${l_owner_pid:-<пусто>}"
		fi
	elif [ "$l_age" -ge "$GRACE" ]; then
		printf 'owner-файла нет %sс (>= GRACE=%sс)' "$l_age" "$GRACE"
	fi
	return 0
}

print_status() {
	found=0
	for l_lock in "$LOCK_DIR"/*.lock; do
		[ -d "$l_lock" ] || continue
		found=1
		l_key=${l_lock##*/}
		l_key=${l_key%.lock}
		l_mtime=$(lock_age "$l_lock")
		[ -n "$l_mtime" ] || l_mtime=$(now)
		l_age=$(( $(now) - l_mtime ))
		l_note=''
		l_pid=$(owner_field "$l_lock/owner" pid)
		if [ ! -f "$l_lock/owner" ]; then
			l_note=' (брошен: owner-файла нет)'
		elif [ -z "$l_pid" ] || ! alive "$l_pid"; then
			l_note=' (брошен: pid владельца не отвечает)'
		fi
		printf '%s: возраст %sс%s\n' "$l_key" "$l_age" "$l_note"
		if [ -f "$l_lock/owner" ]; then
			sed 's/^/    /' "$l_lock/owner"
		else
			printf '    (owner-файла нет)\n'
		fi
	done
	if [ "$found" -eq 0 ]; then
		printf 'активных локов нет (%s)\n' "$LOCK_DIR"
	fi
	return 0
}

case "${1:-}" in
'')
	usage
	exit 2
	;;
status)
	print_status
	exit 0
	;;
-h|--help)
	usage
	exit 0
	;;
esac

key=$1
shift
[ "${1:-}" = '--' ] && shift

if [ $# -eq 0 ]; then
	printf "lock.sh: для лока '%s' не задана команда (нужно: lock.sh %s -- <команда>)\n" "$key" "$key" >&2
	usage
	exit 2
fi

# Ключ попадает и в имя каталога, и в имя переменной reentrancy, поэтому
# произвольная строка недопустима: только A-Za-z0-9._- (db, stand, staging).
case "$key" in
''|*/*|.*|*[!A-Za-z0-9._-]*)
	printf "lock.sh: недопустимый ключ '%s' (разрешены A-Za-z0-9._-)\n" "$key" >&2
	exit 2
	;;
esac

# WAIT и GRACE участвуют в арифметике; нечисловое значение ловится здесь,
# а не сообщением «Illegal number» из середины ожидания.
case "$WAIT$GRACE" in
*[!0-9]*)
	printf 'lock.sh: WAIT и GRACE — целые секунды (получено WAIT=%s, GRACE=%s)\n' "$WAIT" "$GRACE" >&2
	exit 2
	;;
esac

key_var=ROSSTAT_LOCK_HELD_$(printf '%s' "$key" | tr -c 'A-Za-z0-9_' '_')
key_var_upper=ROSSTAT_LOCK_HELD_$(printf '%s' "$key" | tr -c 'A-Za-z0-9_' '_' | tr 'a-z' 'A-Z')

# Reentrancy: лок уже взят этой же сессией (вложенный make вызывает подцель,
# которая берёт тот же ключ) — команда запускается сразу, без повторного захвата.
eval "held=\${$key_var:-}"
eval "held_upper=\${$key_var_upper:-}"
if [ "$held" = 1 ] || [ "$held_upper" = 1 ]; then
	printf "lock.sh: лок '%s' уже взят этой сессией (%s=1) — выполняю команду без повторного захвата\n" "$key" "$key_var" >&2
	exec "$@"
fi

lock="$LOCK_DIR/$key.lock"
takeover_lock="$lock.takeover"
mkdir -p "$LOCK_DIR" || {
	printf 'lock.sh: не удалось создать каталог локов %s\n' "$LOCK_DIR" >&2
	exit 2
}

# Мьютекс перехвата: снести брошенный лок имеет право только один ожидающий.
# У мьютекса свой stale-таймаут (не меньше секунды): сдохший перехватчик не
# блокирует остальных навсегда.
takeover_stale=$GRACE
[ "$takeover_stale" -ge 1 ] || takeover_stale=1
acquire_takeover() {
	l_tries=0
	while :; do
		if mkdir "$takeover_lock" 2>/dev/null; then
			return 0
		fi
		l_mtime=$(lock_age "$takeover_lock")
		[ -n "$l_mtime" ] || l_mtime=$(now)
		if [ $(( $(now) - l_mtime )) -ge "$takeover_stale" ]; then
			printf "lock.sh: мьютекс перехвата %s простоял >= %sс — снимаю\n" "$takeover_lock" "$takeover_stale" >&2
			rm -rf "$takeover_lock"
			continue
		fi
		l_tries=$((l_tries + 1))
		if [ "$l_tries" -gt 20 ]; then
			return 1
		fi
		sleep 0.25
	done
}

waited=0
takeovers=0
while :; do
	if mkdir "$lock" 2>/dev/null; then
		break
	fi

	# Лок занят. Сначала выясняем, не брошен ли он, и только потом ждём.
	busy_line=$(owner_line "$lock/owner")

	# pid владельца осмыслен только на своей машине: каталог локов локальный
	# ($HOME/.rosstat-locks), поэтому чужого pid по сети здесь не бывает.
	stale_reason=$(stale_reason_for "$lock")

	if [ -n "$stale_reason" ]; then
		takeovers=$((takeovers + 1))
		if [ "$takeovers" -gt 3 ]; then
			printf "lock.sh: лок '%s' выглядит брошенным (%s), но перехватить его не удалось за 3 попытки\n" "$key" "$stale_reason" >&2
			exit 3
		fi
		if ! acquire_takeover; then
			sleep 1
			continue
		fi
		# Под мьютексом владелец перечитывается: между проверкой и rm лок мог
		# быть снят и взят заново живой сессией — свежий лок не сносим.
		stale_again=$(stale_reason_for "$lock")
		if [ -n "$stale_again" ]; then
			printf "lock.sh: перехватываю брошенный лок '%s': %s; прежний владелец: %s\n" "$key" "$stale_again" "$busy_line" >&2
			rm -rf "$lock"
		else
			printf "lock.sh: лок '%s' взят другой сессией, пока я ждал мьютекс, — свежий лок не сношу\n" "$key" >&2
		fi
		rm -rf "$takeover_lock"
		continue
	fi

	if [ "$waited" -eq 0 ]; then
		if [ "$WAIT" -gt 0 ]; then
			printf "lock.sh: лок '%s' занят, жду до %sс; владелец: %s\n" "$key" "$WAIT" "$busy_line" >&2
		else
			printf "lock.sh: лок '%s' занят (WAIT=0 — не жду); владелец: %s\n" "$key" "$busy_line" >&2
		fi
	fi
	if [ "$waited" -ge "$WAIT" ]; then
		if [ "$WAIT" -gt 0 ]; then
			printf "lock.sh: лок '%s' занят и не освободился за %sс; владелец: %s\n" "$key" "$WAIT" "$busy_line" >&2
		fi
		printf "lock.sh: лок снимет владелец; если он точно не работает — снять руками: rm -rf %s\n" "$lock" >&2
		exit 3
	fi
	sleep 1
	waited=$((waited + 1))
done

session=${ROSSTAT_SESSION:-}
if [ -z "$session" ]; then
	tree=$(git rev-parse --show-toplevel 2>/dev/null) || tree=$PWD
	session=${tree##*/}
fi
if [ -n "${ROSSTAT_LOCK_TOOL:-}" ]; then
	tool=$ROSSTAT_LOCK_TOOL
else
	tool=$*
fi

# owner пишется через временный файл + mv: второй агент читает его целиком,
# а не наполовину записанным.
{
	printf 'key=%s\n' "$key"
	printf 'tool=%s\n' "$(printf '%s' "$tool" | tr '\n\t' '  ')"
	printf 'session=%s\n' "$(printf '%s' "$session" | tr '\n\t' '  ')"
	printf 'pid=%s\n' "$$"
	printf 'host=%s\n' "$(hostname 2>/dev/null || printf 'unknown')"
	printf 'since=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
	printf 'cwd=%s\n' "$PWD"
} >"$lock/owner.tmp.$$"
mv "$lock/owner.tmp.$$" "$lock/owner"

# Команда и все её потомки видят, что лок уже взят (нужно вложенным make).
export "$key_var=1"
[ "$key_var_upper" = "$key_var" ] || export "$key_var_upper=1"

released=0
release_lock() {
	[ "$released" = 1 ] && return 0
	released=1
	# Лок мог быть перехвачен как брошенный, пока команда ещё работала: чужой
	# не снимаем, иначе следующая сессия получит два параллельных владельца.
	owner_now=$(owner_field "$lock/owner" pid)
	if [ -n "$owner_now" ] && [ "$owner_now" != "$$" ]; then
		printf "lock.sh: лок '%s' перехвачен другой сессией (pid %s) — свой не снимаю\n" "$key" "$owner_now" >&2
		return 0
	fi
	rm -rf "$lock"
}
trap 'release_lock' EXIT
trap 'release_lock; exit 130' INT
trap 'release_lock; exit 143' TERM

"$@"
rc=$?
exit "$rc"
