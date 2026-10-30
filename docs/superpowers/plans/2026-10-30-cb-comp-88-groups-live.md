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
- [ ] **2.5 Отчёт** — `docs/research/2026-10-30-cb-comp-88-groups-task2.{md,json}`; роадмап, README, `docs/site-pages.json`; `make all`, `make docs-check`; PR с `Closes #422`.

## Задачи Task 3 (#423)

Ветка `exp/cb-comp-88-groups-task3` (от ветки Task 2). Протокол — спека §14.

- [x] **3.1 Протокол** — спека §14 и этот раздел; роадмап.
- [x] **3.2 Код до прогона** — `scripts/models/cb_comp/cb_comp_live_en.py` (+ `_test.py`): EN группы (конфигурация B3, импутация медианой train, без `month`), комбинация CB+EN, классификация, MCS, попарные сравнения, правило выбора и G3; `make cb-comp-live-en(-test)`. Переиспользуются загрузка данных, признаки и CatBoost из `cb_comp_live_bottomup`.
- [x] **3.3 Прогон и повтор** — `make cb-comp-live-en`; входы — те же артефакты Task 2.
- [ ] **3.4 Отчёт** — `docs/research/2026-10-30-cb-comp-88-groups-task3.{md,json}`; роадмап, README, `docs/site-pages.json`; `make all`, `make docs-check`.
