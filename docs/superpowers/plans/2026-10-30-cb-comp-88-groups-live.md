# План: live 88-групповой bottom-up — Task 1 (live-панель 88 групп)

**Пункт:** `key-rate/phase-4-cb-comp-88-groups-live`, зонтик [#419](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/419), Task 1 [#421](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/421). **Спека:** [2026-10-30-cb-comp-88-groups-live-design.md](../specs/2026-10-30-cb-comp-88-groups-live-design.md). **Ветка:** `research/cb-comp-88-groups-panel`. План Task 2–7 дописывается сюда же перед стартом шага.

Порядок фиксирован: спека и план → код с тестами (до результатов) → прогон → отчёт. Пороги — спека §7; после первого прогона не меняются.

## Задачи Task 1

- [x] **1.1 Спека и план** — этот коммит; роадмап: пункт ❌ → 🔶.
- [x] **1.2 Q1/Q6 — маппинг и швы (research + код).** `cb_comp_groups_panel.py`: загрузка `model.xlsx`/`dictionary_cpi.xlsx`/`cpi_weights_tovar.xlsx` через `cb_comp_bottomup` (`load_specs`, `load_weights`, `normalize_code`); чтение `v_fedstat_ipc` (`lib.clickhouse.ch_query`); нормализация названий (как Task 3 #236); метод группы `direct | aggregated | missing`, покрытие весом; список ручных соответствий с причиной; швы 2023+ и смены состава. Тесты: нормализация, выбор метода, покрытие. **Коммит кода до прогона.**
- [x] **1.3 Q2 — NSA групп.** Агрегация позиций весами СПР (лаг год) с перенормировкой на покрытую долю; сверка с архивным `mom` 2010-01…2024-09 по группам и взвешенно. Тесты: перенормировка при пропуске позиции, `direct` не агрегируется.
- [x] **1.4 Q3 — X-13 по группам.** Переиспользовать `x13_series`/`make_r_runner` (#399); режимы «вся выборка» и as-of; `sa_failed` → NaN. Сверка с `mom_sa` 2016+ и `cpi_sa_target_history.xlsx`. Тесты: отказ фита → NaN, as-of не видит будущих точек (подмена будущего не меняет значение).
- [x] **1.5 Q4 — сборка и главный гейт.** Сборка без ЖКУ `9470`, лаг весов год, накопление h1/h2/h3/h6; против `TARGET_FULL` на 2019-01…2024-07 и normal-54; регрессионный тест: сборка **авторских** SA-компонент воспроизводит #190 (RMSE h1 0,0786, покрытие 0,8974).
- [x] **1.6 Q5 — веса 2025–2026 (research).** Поиск публичного источника весов ИПЦ (Росстат/ЕМИСС/методология), проба формата, сверка 2016–2024 с архивом; go/no-go. При `go` — новый пункт роадмапа и issue для Go-импортёра (отдельный PR), здесь только handoff.
- [x] **1.7 Прогон и отчёт.** `ML_LOG` не нужен (не модель); детерминизм — повтор прогона даёт тот же JSON (sha256). Отчёт `docs/research/2026-10-30-cb-comp-88-groups-panel.{md,json}`, решения §7, «что не проверено»; `docs/site-pages.json`.
- [ ] **1.8 Приёмка.** `make cb-comp-groups-panel-test`, `make all`, `make docs-check`; README/ARCHITECTURE — строка о новых make-целях; роадмап — статус и результат; PR с `Closes #421`, протокол приёмки комментарием в PR; Project: `In Review`.

## Make-цели (1.2)

- `make cb-comp-groups-panel` — прогон (нужен ClickHouse env, R `seasonal`, локальный архив CB_COMP); `ARGS="--archive PATH --out build/cb-comp-groups"`.
- `make cb-comp-groups-panel-test` — офлайн-тесты без ClickHouse, сети и архива (фикстуры в `scripts/models/cb_comp/testdata/`).

## Риски

- 31074 может не содержать части групп услуг (W3b: услуги 0,274) — тогда `missing` и потеря покрытия; гейт покрытия это покажет.
- Названия позиций меняются между годами словаря — ручные соответствия растут; если их больше ~10 % позиций, это пишется в отчёт как риск поддержки.
- Шов 2023+ (РФ без новых субъектов) может дать сдвиг уровня — сверка 2023-01…2024-09 с архивом покажет.
- Веса live могут оказаться недоступны — тогда Task 2+ работают на весах 2024, ограничение явно в формулировке.

## Итог Task 1 и переход к Task 2

- Главный гейт не пройден: 0,106, с поправкой §11 — 0,1048. Решение владельца (спека §12): цель Task 2 — `T_LIVE`.
- Task 2 (#422) начинается с протокола в спеке (признаки, МП per-sheet, frozen-сплит #190, origin, пороги сравнения по §12) до результатов. Нужен as-of X-13 групп: `make cb-comp-groups-panel ARGS="--asof --jobs 8"`.
- Отдельный Go-пункт — импортёр весов СПР Росстата (Q5 = `go`).

## Задачи Task 2 (#422)

Ветка `exp/cb-comp-88-groups-task2`. Протокол — спека §13.

- [x] **2.1 Протокол** — спека §13 и этот раздел; роадмап.
- [x] **2.2 Код до прогона** — `scripts/models/cb_comp/cb_comp_live_bottomup.py` (+ `_test.py`): загрузка панели групп, `T_LIVE`, признаки и модели через `cb_comp_bottomup`, R0 через `run_bottomup(per_sheet)`, G2/E2/E3/E4 через `cpi_two_stage_cb_comp.compare_to_cb`; `make cb-comp-live-bottomup(-test)`. Тесты: `T_LIVE` (нормировка, лаг весов, накопление), выбор лагов full/as-of, origin и отсутствие утечки, порог G2, сборка паспорта.
- [x] **2.3 Данные** — `make cb-comp-groups-panel ARGS="--asof --jobs 8"`, `make cb-comp-public-export`.
- [x] **2.4 Прогон и повтор** — `make cb-comp-live-bottomup`, повтор для детерминизма.
- [x] **2.4a Поправка §13.1 после #431** — merge `main`; вход — C0-панель `panel.csv` вместо `panel_amended.csv` (до перепрогона).
- [ ] **2.4b Код и данные** — `PANEL_CSV = panel.csv`, ключ паспорта `panel_csv`; `make cb-comp-groups-panel ARGS="--asof --jobs 8"` на коде `main`.
- [ ] **2.4c Перепрогон ×2** — `make cb-comp-live-bottomup`, sha256 `result.json` и `predictions.csv` совпадают.
- [ ] **2.5 Отчёт** — `docs/research/2026-10-30-cb-comp-88-groups-task2.{md,json}`; роадмап, README, `docs/site-pages.json`; `make all`, `make docs-check`; PR с `Closes #422`.

## Задачи Task 1b (#430)

Ветка `research/cb-comp-88-groups-panel-fix`, worktree `.worktrees/cb-comp-88-groups-fix`. Протокол — спека §14; пороги §7 не меняются.

- [ ] **1b.1 Предрегистрация** — спека §14 и этот раздел; роадмап, issue #430. **До кода и до чисел.**
- [x] **1b.2 Код до прогона.** `cb_comp_groups_panel.py`: `REFERENCE_WINDOWS = {csv: 2010-01…2024-09, fst: 2009-01…2024-08}`, `--fit-window {csv,fst,live}`; `load_products(start)` вместо хардкода; `group_sa` получает ряд, обрезанный по окну; блок `sa_fst` в JSON рядом с `sa_csv`; `load_fst_reference` через `cb_comp_bottomup.ensure_panel_csv` + `load_cpi_diff` (лог→п.п.); поправка 9550 по AICc на 2009–2018 в дополнение к §11. Фикс `cb_comp_bottomup.py:329`: путь экспортёра `scripts/models/cb_comp/cb_comp_panel_export.R`.
- [x] **1b.3 Тесты до прогона.** Обрезка по окну (фит не видит точек вне окна), окна в константах, лог→п.п. для `fst`, выбор по AICc (NaN не участвует, равенство — меньший id), `sa_failed` при несошедшемся кандидате. Существующие 31 тест остаются зелёными.
- [x] **1b.4 Прогон ×2** — `make cb-comp-groups-panel ARGS="--fit-window csv"` и `--fit-window fst`; повтор для детерминизма (sha256 JSON и `panel.csv`).
- [x] **1b.5 Отчёт и документы.** §1/§4/§6a/§7 переписать, новый §10 «Исправление после ревью» с таблицей «было → стало» по группам; JSON; спека §13 (Task 2: 9950 и 9550 — по результату §11 и AICc); роадмап.
- [x] **1b.7 Ревью PR #431 (8 замечаний).** Окно выбора 9550 не зависело от `fit_window`; `fst` грузился и в live; `load_products(2009)` менял входы маппинга; возможный KeyError в `amendment.sa.fst.groups`; резерв по листьям до 2010 подмешивал веса 2016; вывод по AICc опирался на ничью; список точных нулей не сохранялся; не было тестов маршрутизации окна.
- [ ] **1b.6 Приёмка.** `make cb-comp-groups-panel-test`, `make all`, `make docs-check`; протокол — комментарием в PR; PR `Closes #430`.

## Риски Task 1b

- AICc для 9550 может выбрать спецификацию, которая не воспроизводит `fst` (AICc — критерий модели, не близости к автору). Тогда Q3-B остаётся > 0,15, и это результат, а не повод подбирать дальше.
- `ensure_panel_csv` до фикса падает на пустом кэше: путь экспортёра указывает на `scripts/cb_comp_panel_export.R`, которого нет.
- Живой прогон требует R `seasonal` и `fst`; в CI не идёт — тесты офлайн.
