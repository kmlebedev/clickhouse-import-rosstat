# SDD ledger — plan: docs/superpowers/plans/2026-10-11-polyus-output-slice2.md

Spec: docs/superpowers/specs/2026-10-11-polyus-output-slice2-design.md
Branch: main (владелец явно выбрал native-исполнение с коммитом в main)
Executor: inline (native)
BASE: b36f03c (плюс a3c99a3 — коммит самого плана)

## Pre-flight scan

Shared interfaces between tasks:

- Task 1 → Task 4: изменяет `actualProductionMetric` (Go-сид читает факт иначе). Task 4 применяет сид и проверяет сходимость. Found: match — ожидание приёмки «сумма пяти рудников = 2790.6» выполнимо только после переключения на `refined`.
- Task 2 → Task 3: сдвигает `ProfileStartYear` в `dcf/mine_plans_seed.go`; Python-модуль выводит `GO_SEED_SUKHOI_LOG_YEARS` из этого файла. Found: match ПОСЛЕ переноса обновлений тестов горизонта в Task 2 (само-ревью плана это поймало). Task 3 лишь следует за исходником.
- Task 2 → Task 4: профиль 2030–2039; приёмка проверяет состав таблицы. Found: match — 92 = 77 + 10 + 5.
- Task 3 → Task 4: окно ОПР 2025–2029. Found: match — те же 92 и отсутствие пересечения с профилем.

Self-consistency rows:

- Task 4 п.2: `count()` = 92. Проверено арифметикой: было 90 = 77 (AISC-сид) + 10 (профиль 2028–2037) + 3 (ОПР 2025–2027). Станет 77 + 10 (профиль 2030–2039) + 5 (ОПР 2025–2029) = 92.
- Task 4 п.4: `dcf_engine` = 36 строк (активов 6 — Сухой Лог уже в таблице, не новый).
- Task 1 Step 1: тест пинует литерал `Total refined gold output` и отсутствие `Total Dore gold output` — это закрывает Review Focus №2, найденный ревью среза 1 (тавтология на константе).

Pre-flight: 4 shared-interface rows, 3 self-consistency rows, no conflicts to rule on.

Task 1: complete (commits a3c99a3..710e42a, tests: go test ./dcf/ -> ok)

Task 2: Ruling: тест в плане назван `TestAssetPlanSeed`, фактическое имя — `TestPolyusAssetPlansValues` (проверено `grep '^func Test' dcf/mine_plans_seed_test.go`). Использовано фактическое. Цена ошибки при обратном выборе: `go test -run TestAssetPlanSeed` даёт «no tests to run», и можно решить, что случай не покрыт.
Task 2: notable: расширение Go-горизонта на 2030-2039 подхватилось Python-модулем АВТОМАТИЧЕСКИ (он парсит dcf/mine_plans_seed.go) — оба теста горизонта стали зелёными без правок кода, только ожиданий. Это окупает F2 из ревью среза 1.

Task 2: complete (commits 710e42a..26b0acb, tests: go test ./dcf/ -> ok; python3 scripts/polyus_output_model_test.py -> 32/32 pass)
