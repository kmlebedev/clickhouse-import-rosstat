#!/bin/sh
# scripts/parallel_sessions_test.sh — самотесты протокола параллельных сессий.
#
# Пункты: lock (взаимное исключение, ожидание, WAIT=0, перехват брошенного лока —
# мёртвый pid или отсутствие owner дольше GRACE, reentrancy, проброс кода возврата),
# worktree (new/list/info/rm в одноразовом клоне, база из origin/$BASE, отказ на
# недопустимом NAME), .githooks/pre-commit (защищённая ветка, detached HEAD, обход,
# linked worktree) и scripts/agent-guard.sh (staging-цели, различие «корневое дерево
# vs linked worktree» для git clean / git stash drop / find .worktrees -delete).
#
# Прогон герметичен: всё живёт в mktemp -d, сеть не нужна, настоящий
# $HOME/.rosstat-locks не трогается — ROSSTAT_LOCK_DIR указывает внутрь tmp.
# Печатает ok/FAIL по пунктам и выходит с ненулевым кодом при первой ошибке.
#
# Запуск: sh scripts/parallel_sessions_test.sh   (или make test-parallel)

set -u

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
lock_sh="$script_dir/lock.sh"
wt_sh="$script_dir/worktree.sh"
guard_sh="$script_dir/agent-guard.sh"
hook="$script_dir/../.githooks/pre-commit"

for required in "$lock_sh" "$wt_sh" "$guard_sh" "$hook"; do
	[ -f "$required" ] || {
		printf 'parallel_sessions_test: нет файла %s\n' "$required" >&2
		exit 1
	}
done

tmp_root=${TMPDIR:-/tmp}
tmp_root=${tmp_root%/}
tmp=$(mktemp -d "$tmp_root/rosstat-parallel.XXXXXX") || {
	printf 'parallel_sessions_test: mktemp не создал временный каталог\n' >&2
	exit 1
}
trap 'rm -rf "$tmp"' EXIT INT TERM

# Свой каталог локов и своё имя сессии: чужое состояние на результат не влияет.
ROSSTAT_LOCK_DIR="$tmp/locks"
ROSSTAT_SESSION=selftest
export ROSSTAT_LOCK_DIR ROSSTAT_SESSION
# Флаги reentrancy из окружения (могли остаться от вложенного make) снимаем:
# каждый пункт проверяет тот сценарий, который объявлен.
for stale_var in $(env | sed -n 's/^\(ROSSTAT_LOCK_HELD_[A-Za-z0-9_]*\)=.*/\1/p'); do
	unset "$stale_var"
done

checks=0
ok() {
	checks=$((checks + 1))
	printf 'ok   %s\n' "$1"
}

fail() {
	printf 'FAIL %s\n' "$1" >&2
	[ -n "${2:-}" ] && printf '%s\n' "$2" | sed 's/^/     /' >&2
	exit 1
}

wait_for() {
	# wait_for <файл> <описание> [pid-держателя] [stderr-держателя]
	i=0
	while [ ! -e "$1" ]; do
		if [ -n "${3:-}" ] && ! kill -0 "$3" 2>/dev/null; then
			cat "${4:-/dev/null}" >&2
			fail "$2" "процесс-держатель ($3) завершился, не дождавшись"
		fi
		i=$((i + 1))
		if [ "$i" -gt 50 ]; then
			cat "${4:-/dev/null}" >&2
			fail "$2" "не дождались $1 за 5 с"
		fi
		sleep 0.1
	done
	return 0
}

wait_for_text() {
	# wait_for_text <файл> <шаблон> <описание> [pid-писателя]
	i=0
	while ! grep -q "$2" "$1" 2>/dev/null; do
		if [ -n "${4:-}" ] && ! kill -0 "$4" 2>/dev/null; then
			cat "$1" >&2
			fail "$3" "процесс ($4) завершился, ничего не написав в $1"
		fi
		i=$((i + 1))
		if [ "$i" -gt 100 ]; then
			cat "$1" >&2
			fail "$3" "не дождались '$2' в $1 за 5 с"
		fi
		sleep 0.05
	done
	return 0
}

holder_pid=''
start_holder() {
	# start_holder <key> <marker> <release> <stderr>: берёт лок и держит его,
	# пока не появится файл <release>. pid — в holder_pid.
	HOLD_FILE="$2" HOLD_GO="$3" sh "$lock_sh" "$1" -- \
		sh -c 'touch "$HOLD_FILE"; while [ ! -e "$HOLD_GO" ]; do sleep 0.1; done' 2>"$4" &
	holder_pid=$!
	wait_for "$2" "lock: держатель лока '$1' не взял лок" "$holder_pid" "$4"
}

# --- 1. lock ----------------------------------------------------------------------

marker1="$tmp/marker1"
start_holder db "$marker1" "$tmp/release1" "$tmp/holder1.err"

owner_file="$ROSSTAT_LOCK_DIR/db.lock/owner"
[ -f "$owner_file" ] || fail 'lock: owner-файл не создан' "$(cat "$tmp/holder1.err")"
for field in key tool session pid host since cwd; do
	grep -q "^$field=" "$owner_file" || fail "lock: в owner-файле нет поля $field" "$(cat "$owner_file")"
done
grep -q '^key=db$' "$owner_file" || fail 'lock: owner-файл не про ключ db' "$(cat "$owner_file")"
grep -q '^session=selftest$' "$owner_file" || fail 'lock: owner-файл без ROSSTAT_SESSION' "$(cat "$owner_file")"

status_out=$(sh "$lock_sh" status)
printf '%s' "$status_out" | grep -q '^db: возраст' || fail 'lock status: не показал активный лок db' "$status_out"
printf '%s' "$status_out" | grep -q 'session=selftest' || fail 'lock status: не показал владельца' "$status_out"

second_marker="$tmp/second_marker"
WAIT=30 sh "$lock_sh" db -- touch "$second_marker" 2>"$tmp/second.err" &
second_pid=$!
wait_for_text "$tmp/second.err" 'занят' 'lock: вторая команда не сообщила о занятом локе' "$second_pid"
if [ -e "$second_marker" ]; then
	kill "$second_pid" 2>/dev/null
	fail 'lock: вторая команда выполнилась при занятом локе'
fi
grep -q 'session=selftest' "$tmp/second.err" || fail 'lock: в сообщении не назван владелец лока' "$(cat "$tmp/second.err")"

touch "$tmp/release1"
wait "$second_pid" || fail 'lock: вторая команда не завершилась успешно после освобождения'
[ -e "$second_marker" ] || fail 'lock: вторая команда не выполнилась после освобождения лока'
wait "$holder_pid" || fail 'lock: первый держатель завершился с ошибкой'
[ -d "$ROSSTAT_LOCK_DIR/db.lock" ] && fail 'lock: лок не снят после завершения команды'
# Снятый лок виден и в status: активных локов больше нет.
status_out=$(sh "$lock_sh" status) || fail 'lock status: вышел с ошибкой при отсутствии локов'
printf '%s' "$status_out" | grep -q 'активных локов нет' || fail 'lock status: без локов не сообщил об этом' "$status_out"
ok 'lock: первый захват держит лок, второй ждёт, называет владельца и проходит после освобождения'

marker2="$tmp/marker2"
start_holder db "$marker2" "$tmp/release2" "$tmp/holder2.err"
WAIT=0 sh "$lock_sh" db -- true 2>"$tmp/wait0.err"
rc=$?
[ "$rc" = 3 ] || fail "lock: WAIT=0 при занятом локе дал код $rc вместо 3"
grep -q 'занят' "$tmp/wait0.err" || fail 'lock: WAIT=0 не назвал причину отказа' "$(cat "$tmp/wait0.err")"
touch "$tmp/release2"
wait "$holder_pid" || fail 'lock: второй держатель завершился с ошибкой'
ok 'lock: WAIT=0 при занятом локе даёт код 3'

# Заведомо мёртвый pid: kill -0 проверяем, чтобы тест не зависел от pid_max.
dead_pid=''
for candidate in 999999 999998 999997 999996 999995 999994; do
	if ! kill -0 "$candidate" 2>/dev/null; then
		dead_pid=$candidate
		break
	fi
done
[ -n "$dead_pid" ] || fail 'lock: не нашли мёртвый pid для проверки перехвата'

mkdir -p "$ROSSTAT_LOCK_DIR/stale.lock"
printf 'key=stale\ntool=selftest\nsession=ghost\npid=%s\nhost=nowhere\nsince=2000-01-01T00:00:00Z\ncwd=/tmp\n' \
	"$dead_pid" >"$ROSSTAT_LOCK_DIR/stale.lock/owner"
WAIT=0 sh "$lock_sh" stale -- true 2>"$tmp/stale.err" ||
	fail 'lock: брошенный лок (мёртвый pid) не перехвачен' "$(cat "$tmp/stale.err")"
grep -q 'перехват' "$tmp/stale.err" || fail 'lock: перехват брошенного лока не предупредил в stderr' "$(cat "$tmp/stale.err")"
[ -d "$ROSSTAT_LOCK_DIR/stale.lock" ] && fail 'lock: перехваченный лок не снят после команды'
ok 'lock: лок с мёртвым pid перехватывается с предупреждением'

# Мьютекс перехвата со своим stale-таймаутом: «зависший» (сдохший перехватчик)
# не блокирует перехват брошенного лока навсегда.
mkdir -p "$ROSSTAT_LOCK_DIR/mtx.lock" "$ROSSTAT_LOCK_DIR/mtx.lock.takeover"
printf 'key=mtx\ntool=selftest\nsession=ghost\npid=%s\nhost=nowhere\nsince=2000-01-01T00:00:00Z\ncwd=/tmp\n' \
	"$dead_pid" >"$ROSSTAT_LOCK_DIR/mtx.lock/owner"
touch -t 200001010000 "$ROSSTAT_LOCK_DIR/mtx.lock" "$ROSSTAT_LOCK_DIR/mtx.lock.takeover"
WAIT=0 sh "$lock_sh" mtx -- true 2>"$tmp/mtx.err" ||
	fail 'lock: зависший мьютекс перехвата заблокировал перехват брошенного лока' "$(cat "$tmp/mtx.err")"
grep -q 'мьютекс перехвата' "$tmp/mtx.err" || fail 'lock: снятие зависшего мьютекса не сообщено в stderr' "$(cat "$tmp/mtx.err")"
[ -d "$ROSSTAT_LOCK_DIR/mtx.lock.takeover" ] && fail 'lock: мьютекс перехвата не снят после перехвата'
ok 'lock: зависший мьютекс перехвата снимается и не мешает перехвату'

# Возраст лока у ЖИВОГО владельца ничего не перехватывает: иначе два процесса
# одновременно считали бы себя владельцем одного ресурса.
mkdir -p "$ROSSTAT_LOCK_DIR/age.lock"
printf 'key=age\ntool=selftest\nsession=old\npid=%s\nhost=localhost\nsince=2000-01-01T00:00:00Z\ncwd=/tmp\n' \
	"$$" >"$ROSSTAT_LOCK_DIR/age.lock/owner"
touch -t 200001010000 "$ROSSTAT_LOCK_DIR/age.lock"
WAIT=0 sh "$lock_sh" age -- true 2>"$tmp/age.err"
rc=$?
[ "$rc" = 3 ] || fail "lock: старый лок с живым pid перехвачен (код $rc вместо 3)" "$(cat "$tmp/age.err")"
[ -d "$ROSSTAT_LOCK_DIR/age.lock" ] || fail 'lock: старый лок с живым pid снесён'
grep -q 'занят' "$tmp/age.err" || fail 'lock: отказ по старому локу не объяснён' "$(cat "$tmp/age.err")"
rm -rf "$ROSSTAT_LOCK_DIR/age.lock"
ok 'lock: лок с живым pid не перехватывается, даже если он старый'

# Окно между mkdir и записью owner-файла: пока оно не истекло (GRACE), лок не
# перехватывается; после — перехватывается как брошенный.
mkdir -p "$ROSSTAT_LOCK_DIR/grace.lock"
WAIT=0 sh "$lock_sh" grace -- true 2>"$tmp/grace.err"
rc=$?
[ "$rc" = 3 ] || fail "lock: свежий лок без owner-файла перехвачен (код $rc вместо 3)" "$(cat "$tmp/grace.err")"
rm -rf "$ROSSTAT_LOCK_DIR/grace.lock"
mkdir -p "$ROSSTAT_LOCK_DIR/noowner.lock"
touch -t 200001010000 "$ROSSTAT_LOCK_DIR/noowner.lock"
WAIT=0 sh "$lock_sh" noowner -- true 2>"$tmp/noowner.err" ||
	fail 'lock: лок без owner-файла дольше GRACE не перехвачен' "$(cat "$tmp/noowner.err")"
grep -q 'перехват' "$tmp/noowner.err" || fail 'lock: перехват лока без owner не предупредил в stderr' "$(cat "$tmp/noowner.err")"
[ -d "$ROSSTAT_LOCK_DIR/noowner.lock" ] && fail 'lock: лок без owner не снят после перехвата'
ok 'lock: лок без owner-файла старше GRACE перехватывается, свежий — нет'

marker3="$tmp/marker3"
start_holder db "$marker3" "$tmp/release3" "$tmp/holder3.err"
ROSSTAT_LOCK_HELD_db=1 WAIT=0 sh "$lock_sh" db -- true 2>"$tmp/reentrant.err" ||
	fail 'lock: ROSSTAT_LOCK_HELD_db=1 при занятом локе всё равно блокирует' "$(cat "$tmp/reentrant.err")"
grep -q 'уже взят' "$tmp/reentrant.err" || fail 'lock: повторный захват не отмечен в stderr' "$(cat "$tmp/reentrant.err")"
touch "$tmp/release3"
wait "$holder_pid" || fail 'lock: третий держатель завершился с ошибкой'
ok 'lock: ROSSTAT_LOCK_HELD_db=1 не берёт лок повторно (reentrancy по env)'

WAIT=5 sh "$lock_sh" db -- sh -c "sh '$lock_sh' db -- true" ||
	fail 'lock: вложенный вызов того же ключа задедлочился (нет reentrancy)'
ok 'lock: вложенный вызов под своим же локом не блокируется'

sh "$lock_sh" db -- false
rc=$?
[ "$rc" = 1 ] || fail "lock: код возврата команды не проброшен (false → $rc)"
[ -d "$ROSSTAT_LOCK_DIR/db.lock" ] && fail 'lock: лок не снят после падения команды'
ok 'lock: код возврата команды пробрасывается (false → 1) и лок снимается'

# --- 2. worktree ------------------------------------------------------------------

repo_root=$(git rev-parse --show-toplevel 2>/dev/null) || fail 'worktree: тест запущен вне git-репозитория'
clone="$tmp/clone"
# --shared: объекты берутся из источника по alternates — одноразовый клон создаётся
# за десятые доли секунды вместо копирования всего .git; источник только читается.
git clone --quiet --local --shared "$repo_root" "$clone" || fail 'worktree: git clone --local не удался'
git -C "$clone" config user.email selftest@example.com
git -C "$clone" config user.name selftest
git -C "$clone" config commit.gpgsign false
# Своя main в клоне: тест не зависит от того, какая ветка checked out в источнике.
git -C "$clone" branch -f main HEAD
git -C "$clone" checkout --quiet main

# Файлы Task 1 могут быть ещё не закоммичены — переносим их в клон руками,
# иначе тест проверял бы дерево без самих проверяемых скриптов и хука.
mkdir -p "$clone/scripts" "$clone/.githooks"
cp "$lock_sh" "$wt_sh" "$guard_sh" "$clone/scripts/"
cp "$hook" "$clone/.githooks/pre-commit"
chmod +x "$clone/scripts/lock.sh" "$clone/scripts/worktree.sh" "$clone/.githooks/pre-commit"
git -C "$clone" add --all scripts .githooks
git -C "$clone" commit --quiet -m 'selftest: scripts and hook under test' ||
	fail 'worktree: не удалось зафиксировать копии скриптов в клоне'

c_wt="$clone/scripts/worktree.sh"
probe="$clone/.worktrees/probe"

if ! (cd "$clone" && sh "$c_wt" new NAME=probe >"$tmp/new.out" 2>&1); then
	fail 'worktree: new NAME=probe не удался' "$(cat "$tmp/new.out")"
fi
[ -d "$probe" ] || fail 'worktree: каталог .worktrees/probe не создан'
[ "$(git -C "$probe" rev-parse --abbrev-ref HEAD)" = 'feat/probe' ] ||
	fail 'worktree: дерево не на ветке feat/probe'
grep -q 'hooks-install' "$tmp/new.out" || fail 'worktree: new не подсказал make hooks-install' "$(cat "$tmp/new.out")"
grep -q 'Kimi Code Desktop' "$tmp/new.out" || fail 'worktree: new не подсказал отдельный workspace Desktop' "$(cat "$tmp/new.out")"
ok 'worktree: new создаёт .worktrees/<slug> и ветку TYPE/<slug>'

if (cd "$clone" && sh "$c_wt" new NAME=probe >"$tmp/dup.out" 2>&1); then
	fail 'worktree: повторный new тем же именем прошёл'
fi
grep -q 'уже существует' "$tmp/dup.out" || fail 'worktree: отказ повторного new без объяснения' "$(cat "$tmp/dup.out")"

git -C "$clone" branch feat/other main
if (cd "$clone" && sh "$c_wt" new NAME=other >"$tmp/dup-branch.out" 2>&1); then
	fail 'worktree: new создал дерево на уже существующей ветке'
fi
ok 'worktree: повторный new и new на существующей ветке отказывают'

out=$(cd "$clone" && sh "$c_wt" list)
printf '%s' "$out" | grep -q '.worktrees/probe' || fail 'worktree: list не показал дерево probe' "$out"
printf '%s' "$out" | grep -q 'feat/probe' || fail 'worktree: list не показал ветку дерева' "$out"
probe_line=$(printf '%s\n' "$out" | grep 'worktrees/probe' | head -n 1)
case "$probe_line" in
*dirty*) fail 'worktree: list пометил чистое дерево грязным' "$probe_line" ;;
esac
printf '%s' "$probe_line" | grep -q 'clean' || fail 'worktree: list не пометил чистое дерево' "$probe_line"
touch "$probe/dirty.txt"
out=$(cd "$clone" && sh "$c_wt" list)
printf '%s' "$out" | grep 'worktrees/probe' | head -n 1 | grep -q 'dirty' ||
	fail 'worktree: list не пометил грязное дерево' "$out"
ok 'worktree: list показывает деревья с ветками и пометкой dirty'

out=$(cd "$probe" && sh "$c_wt" info)
printf '%s' "$out" | grep -q "$probe" || fail 'worktree: info не показал путь дерева' "$out"
printf '%s' "$out" | grep -q 'feat/probe' || fail 'worktree: info не показал ветку' "$out"
printf '%s' "$out" | grep -q 'отдельное дерево' || fail 'worktree: info не отличил worktree от корневого дерева' "$out"
printf '%s' "$out" | grep -q 'активных локов' || fail 'worktree: info не показал статус локов' "$out"
out_root=$(cd "$clone" && sh "$c_wt" info)
printf '%s' "$out_root" | grep -q 'корневое дерево' || fail 'worktree: info не распознал корневое дерево' "$out_root"
ok 'worktree: info показывает путь, ветку, тип дерева и активные локи'

if (cd "$clone" && sh "$c_wt" rm NAME=probe >"$tmp/rm1.out" 2>&1); then
	fail 'worktree: rm удалил дерево с незакоммиченным файлом без FORCE=1'
fi
[ -d "$probe" ] || fail 'worktree: rm снёс дерево без FORCE=1'
grep -q 'FORCE=1' "$tmp/rm1.out" || fail 'worktree: отказ rm без подсказки FORCE=1' "$(cat "$tmp/rm1.out")"
if ! (cd "$clone" && sh "$c_wt" rm NAME=probe FORCE=1 >"$tmp/rm2.out" 2>&1); then
	fail 'worktree: rm FORCE=1 не удалил дерево' "$(cat "$tmp/rm2.out")"
fi
[ -d "$probe" ] && fail 'worktree: каталог дерева остался после rm FORCE=1'
git -C "$clone" show-ref --verify --quiet refs/heads/feat/probe ||
	fail 'worktree: rm удалил ветку дерева (ветку не трогаем)'
if git -C "$clone" worktree list --porcelain | grep -q "worktree $probe"; then
	fail 'worktree: дерево осталось в git worktree list'
fi
grep -q 'ветка сохранена' "$tmp/rm2.out" || fail 'worktree: rm не напомнил, что ветка сохранена' "$(cat "$tmp/rm2.out")"
grep -q 'git worktree prune' "$tmp/rm2.out" || fail 'worktree: rm не подсказал git worktree prune' "$(cat "$tmp/rm2.out")"
ok 'worktree: rm отказывает при незакоммиченных изменениях, FORCE=1 удаляет, ветка остаётся'

# NAME вне .worktrees (с «..» или «/») недопустим: rm не должен уходить за пределы
# .worktrees и удалять каталоги вне него.
mkdir -p "$clone/outside"
if (cd "$clone" && sh "$c_wt" rm NAME=../outside FORCE=1 >"$tmp/rm-outside.out" 2>&1); then
	fail 'worktree: rm ушёл по NAME с «..» за пределы .worktrees'
fi
[ -d "$clone/outside" ] || fail 'worktree: rm с NAME=../outside снёс каталог вне .worktrees'
grep -q 'недопустимый NAME' "$tmp/rm-outside.out" ||
	fail 'worktree: отказ rm по NAME без объяснения' "$(cat "$tmp/rm-outside.out")"
ok 'worktree: rm отказывает на NAME с «..» и не трогает каталоги вне .worktrees'

# --- 3. pre-commit ----------------------------------------------------------------

git -C "$clone" config core.hooksPath .githooks
if (cd "$clone" && git commit --quiet --allow-empty -m 'must be refused' >"$tmp/hook-main.out" 2>&1); then
	fail 'pre-commit: коммит на main прошёл'
fi
grep -q 'make wt-new' "$tmp/hook-main.out" || fail 'pre-commit: отказ без подсказки make wt-new' "$(cat "$tmp/hook-main.out")"

git -C "$clone" checkout --quiet -b feat/hook-test
if ! (cd "$clone" && git commit --quiet --allow-empty -m 'allowed in own branch' >"$tmp/hook-feat.out" 2>&1); then
	fail 'pre-commit: коммит в своей ветке не прошёл' "$(cat "$tmp/hook-feat.out")"
fi
ok 'pre-commit: коммит на main отказывает с подсказкой, в feat/x проходит'

git -C "$clone" checkout --quiet --detach HEAD
if (cd "$clone" && git commit --quiet --allow-empty -m 'detached' >"$tmp/hook-detached.out" 2>&1); then
	fail 'pre-commit: коммит при detached HEAD прошёл'
fi
grep -q 'detached' "$tmp/hook-detached.out" || fail 'pre-commit: отказ при detached HEAD без объяснения' "$(cat "$tmp/hook-detached.out")"
git -C "$clone" checkout --quiet main
if ! (cd "$clone" && ALLOW_PROTECTED_COMMIT=1 git commit --quiet --allow-empty -m 'bypass' >"$tmp/hook-allow.out" 2>&1); then
	fail 'pre-commit: ALLOW_PROTECTED_COMMIT=1 не разрешил коммит' "$(cat "$tmp/hook-allow.out")"
fi
ok 'pre-commit: detached HEAD отказывает, ALLOW_PROTECTED_COMMIT=1 пропускает коммит'

# Хук в linked worktree: git разрешает ОТНОСИТЕЛЬНЫЙ core.hooksPath от корня того
# дерева, где идёт коммит, поэтому в дереве с каталогом .githooks хук срабатывает
# (проверено и живьём: детач-коммит в linked worktree отбивается, коммит в своей
# ветке — проходит). Если git когда-нибудь сменит правило, тест это покажет.
git -C "$clone" worktree add --quiet --detach "$clone/.worktrees/hooktree" main
if (cd "$clone/.worktrees/hooktree" && git commit --quiet --allow-empty -m 'detached in worktree' >"$tmp/hook-worktree.out" 2>&1); then
	fail 'pre-commit: в linked worktree хук не сработал (detached-коммит прошёл)'
fi
grep -q 'detached' "$tmp/hook-worktree.out" ||
	fail 'pre-commit: в linked worktree отказ без объяснения' "$(cat "$tmp/hook-worktree.out")"
git -C "$clone/.worktrees/hooktree" checkout --quiet -b feat/hooktree
if ! (cd "$clone/.worktrees/hooktree" && git commit --quiet --allow-empty -m 'own branch in worktree' >"$tmp/hook-worktree2.out" 2>&1); then
	fail 'pre-commit: в linked worktree коммит в своей ветке не прошёл' "$(cat "$tmp/hook-worktree2.out")"
fi
ok 'pre-commit: хук срабатывает и в linked worktree (относительный core.hooksPath)'

# --- 4. agent-guard ---------------------------------------------------------------
# Страховка владельца: JSON события → rc=2 (блокировка) / rc=0 (пропуск). Гард
# разбирает cwd из самого payload, поэтому запускать можно из любого каталога.
# Проверяем и распознавание staging-целей, и различие «корневое дерево vs linked
# worktree» (у корня .git — каталог, у linked worktree — файл).
c_guard="$clone/scripts/agent-guard.sh"
mkdir -p "$clone/sub/dir"
guard_rc() {
	# guard_rc <cwd> <команда> → код возврата гарда (stderr — в $tmp/guard.err)
	g_cwd=$(printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g')
	g_cmd=$(printf '%s' "$2" | sed 's/\\/\\\\/g; s/"/\\"/g')
	printf '{"cwd":"%s","tool_input":{"command":"%s"}}\n' "$g_cwd" "$g_cmd" |
		sh "$c_guard" 2>"$tmp/guard.err"
}

guard_expect() {
	# guard_expect <cwd> <команда> <ожидаемый rc> <описание>
	g_rc=0
	guard_rc "$1" "$2" || g_rc=$?
	if [ "$g_rc" != "$3" ]; then
		cat "$tmp/guard.err" >&2
		fail "$4" "код $g_rc вместо $3: $2 (cwd=$1)"
	fi
}

guard_expect "$clone" 'make upgrade-staging' 2 'guard: make upgrade-staging не заблокирован'
guard_expect "$clone" 'make -C /tmp -j4 -s upgrade-staging' 2 'guard: make upgrade-staging с глобальными флагами не заблокирован'
guard_expect "$clone" 'VAR=1 make upgrade-staging' 2 'guard: VAR=1 make upgrade-staging не заблокирован'
guard_expect "$clone" 'make deploy-staging-preflight' 0 'guard: deploy-staging-preflight заблокирован'
guard_expect "$clone" 'make deploy-staging-smoke' 0 'guard: deploy-staging-smoke заблокирован'
ok 'guard: upgrade-staging блокируется (в т.ч. с флагами), preflight/smoke — нет'

guard_expect "$clone" 'git clean -fdx' 2 'guard: git clean -fdx в корневом дереве не заблокирован'
guard_expect "$clone/sub/dir" 'git clean -fdx' 2 'guard: git clean -fdx в подкаталоге корневого дерева не заблокирован'
guard_expect "$clone/.worktrees/hooktree" 'git clean -fdx' 0 'guard: git clean -fdx в linked worktree заблокирован'
guard_expect "$clone" 'git stash drop' 2 'guard: git stash drop в корневом дереве не заблокирован'
guard_expect "$clone/.worktrees/hooktree" 'git stash drop' 0 'guard: git stash drop в linked worktree заблокирован'
guard_expect "$clone" 'find .worktrees -delete' 2 'guard: find .worktrees -delete в корневом дереве не заблокирован'
guard_expect "$clone/.worktrees/hooktree" 'find .worktrees -delete' 0 'guard: find .worktrees -delete в linked worktree заблокирован'
ok 'guard: git clean/git stash drop/find -delete блокируются только в корневом дереве'


# BASE, существующий только как origin/<BASE>: дерево обязано встать на feat/<slug>.
# Без явного sha `git worktree add -b feat/x main` сам заводит локальную main
# (DWIM до origin/main) и оставляет новое дерево на защищённой ветке.
git -C "$clone" checkout --quiet feat/hook-test
if ! git -C "$clone" branch -D main >/dev/null 2>&1; then
	fail 'worktree: не удалось удалить локальную main для проверки BASE только в origin'
fi
if ! (cd "$clone" && sh "$c_wt" new NAME=remotebase >"$tmp/remotebase.out" 2>&1); then
	fail 'worktree: new с BASE, существующим только в origin, не удался' "$(cat "$tmp/remotebase.out")"
fi
[ "$(git -C "$clone/.worktrees/remotebase" rev-parse --abbrev-ref HEAD)" = 'feat/remotebase' ] ||
	fail 'worktree: BASE только в origin — дерево встало не на feat/remotebase' "$(cat "$tmp/remotebase.out")"
if git -C "$clone" show-ref --verify --quiet refs/heads/main; then
	fail 'worktree: new завёл локальную main вместо ветки дерева'
fi
ok 'worktree: BASE только как origin/<BASE> не подменяет ветку дерева на защищённую'

# new берёт базу из origin/<BASE>: локальный refs/heads/<BASE> мог отстать, и дерево
# встало бы на старый коммит. Проверяем на разведённых локальном main и origin/main.
origin_main=$(git -C "$clone" rev-parse refs/remotes/origin/main)
git -C "$clone" branch -f main HEAD
local_main=$(git -C "$clone" rev-parse refs/heads/main)
[ "$local_main" != "$origin_main" ] || fail 'worktree: не удалось развести локальный main и origin/main'
if ! (cd "$clone" && sh "$c_wt" new NAME=originbase >"$tmp/originbase.out" 2>&1); then
	fail 'worktree: new не учёл origin/$BASE' "$(cat "$tmp/originbase.out")"
fi
[ "$(git -C "$clone/.worktrees/originbase" rev-parse HEAD)" = "$origin_main" ] ||
	fail 'worktree: new встал на локальный main вместо origin/main' "$(cat "$tmp/originbase.out")"
ok 'worktree: new берёт базу из origin/$BASE, а не из отставшего локального ref'

printf '\nвсе самотесты пройдены: %s проверок\n' "$checks"
exit 0
