# Срез 2 модели выпуска Полюса: метрика refined и окно ОПР до 2029 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Перевести пять действующих рудников на метрику `refined` (групповая сумма сходится с публикацией) и расширить окно ОПР Сухого Лога до 2029 под базовый график запуска ЗИФ (консенсус 2030), сдвинув проектный профиль на 2030–2039.

**Architecture:** Три независимые правки в двух контурах. Go-сид меняет метрику добычи (одна константа) и `ProfileStartYear` Сухого Лога; Python-сид меняет окно ОПР. Инвариант «множества ключей не пересекаются» сохраняется: окно ОПР 2025–2029 и профиль 2030–2039 не пересекаются, а `assert_no_overlap` читает горизонт профиля прямо из исходника Go-сида, поэтому рассинхрон ловится тестом, а не глазами.

**Tech Stack:** Go (пакет `dcf/`, `go test`), Python 3 stdlib + `unittest` (без pytest), ClickHouse по HTTP, `make` цели.

**Spec:** `docs/superpowers/specs/2026-10-11-polyus-output-slice2-design.md`

## Global Constraints

- Сиды применяются **вручную**: `make ch-sql FILE=...`. Ни Go-импортёр, ни Python-скрипт не пишут в ClickHouse напрямую по произвольному адресу — Go-сид идёт штатным `make import STAT=dcf_mine_plans`, Python печатает файл.
- Секреты только через `os.Getenv` / существующий `ch_config()`; содержимое `~/.config/rosstat/env` не читать и не печатать. Текст ошибок в Python проходит через `mask()`.
- Вставка в ClickHouse — только батчами (`PrepareBatch` → `Append` с проверкой `err` → `Send`). Go-код этим планом новых вставок не заводит, только правит существующие константы.
- Комментарии и докстроки — по-русски, объясняют **почему**, а не что. Тесты — тоже.
- Тесты Python — офлайн (транспорт подменяется `unittest.mock`); Go — без БД и сети.
- `make all` (gofmt, golangci-lint, `go vet`, `go test -race`, сборка) обязан быть зелёным.
- Коммиты по-английски, в духе истории.
- **Известное, что не чинится:** `python3 scripts/extrapolation_aisc_test.py` даёт 1 красный тест (`test_committed_seed_body_matches_a_fresh_generation`, дрейф последнего знака float). Подтверждено на `main` до этих работ — не регрессия. В отчётах называть явно, а не выдавать зелёный Go-набор за зелёный Python.

## Review Focus

Пять входов/условий, которые ломаются тихо и потому получают тест в задаче-владельце:

1. **`ProfileStartYear` сдвинут в Go-сиде, а окно ОПР — нет** (или наоборот) → ключи пересекутся, `ReplacingMergeTree` перезапишет проектные строки целиком. Ожидание: рассинхрон ловится тестом, читающим исходник.
2. **Метрика переключена, но где-то осталась строка `Total Dore gold output`** (тест, комментарий-ссылка, второй вызывающий) → разные контуры читают разное. Ожидание: тест пинует литерал `refined` и отсутствие `Dore` в запросе.
3. **`MineLifeYears` истолкован как «число строк в таблице»** → горизонт Сухого Лога 2025–2039 читался бы как 15 лет проекта. Ожидание: тест на число ПРОЕКТНЫХ лет и отдельный тест на полный горизонт таблицы.
4. **Оценка 2027–2029 разъезжается на три разных правила** → три разных числа. Ожидание: одно правило на все оценённые годы, тест сравнивает их между собой.
5. **Факта за 2025/2026 нет** (пустой ответ датапака) → подстановка нуля или молчаливый пропуск. Ожидание: отказ с именем года (поведение среза 1 сохраняется).

---

### Task 1: Метрика `refined` в Go-сиде

**Files:**
- Modify: `dcf/mine_plans_import.go:29-35` (константа `actualProductionMetric`)
- Test: `dcf/mine_plans_import_test.go:20-40` (`TestActualProductionQueryShape`)

**Interfaces:**
- Consumes: ничего из предыдущих задач.
- Produces: константа `actualProductionMetric` со значением `Total refined gold output` (Go-строка, дословно).

- [ ] **Step 1: Обновить тест под новую метрику**

В `TestActualProductionQueryShape` заменить ожидаемое значение и добавить проверку, что прежняя метрика **не** встречается:

```go
for _, want := range []string{"databook_polyus", "Total refined gold output", "ANNUAL"} {
    if !strings.Contains(q, want) {
        t.Errorf("запрос не содержит %q: %q", want, q)
    }
}
// Прежняя метрика `Total Dore gold output` читалась бы тем же успехом, но на
// групповом уровне НЕ сходится с публикацией (2025: 2779.2 против 2790.6),
// а разницу создаёт ОПР-выпуск Вернинской ЗИФ. Остаться на ней — значит
// сохранить расхождение 5.61 % в сумме плана.
if strings.Contains(q, "Total Dore gold output") {
    t.Errorf("запрос читает прежнюю метрику dore: %q", q)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./dcf/ -run TestActualProductionQueryShape -v`
Expected: FAIL — «запрос не содержит "Total refined gold output"»

- [ ] **Step 3: Поменять константу и её докстроку**

В `dcf/mine_plans_import.go` заменить значение и переписать докстроку: она обязана объяснить, почему `refined`, а не `dore` (групповая сходимость: `refined` по пяти рудникам даёт 2790.6 = `CONSOLIDATED`, `dore` — 2779.2; разница 11.4 koz — ОПР-выпуск Вернинской ЗИФ), и сохранить предупреждение о `company_financials.gold_output` (сводка по Группе, не по активу).

- [ ] **Step 4: Run тесты пакета**

Run: `go test ./dcf/ -v`
Expected: PASS, включая `TestActualProductionQueryShape`

- [ ] **Step 5: Commit**

```bash
git add dcf/mine_plans_import.go dcf/mine_plans_import_test.go
git commit -m "dcf: read refined gold output instead of dore for mine plans"
```

---

### Task 2: Сдвиг `ProfileStartYear` Сухого Лога на 2030

**Files:**
- Modify: `dcf/mine_plans_seed.go:238-247` (блок `Sukhoi Log`)
- Test: `dcf/mine_plans_seed_test.go:35-42` (таблица `wantAssetPlanSeed`), `scripts/polyus_output_model_test.py` (`test_default_horizon_matches_go_seed`, `test_horizon_is_derived_from_the_go_seed_source`)

**Interfaces:**
- Consumes: ничего из Task 1 (правки независимы, но обе в `dcf/`).
- Produces: `ProfileStartYear = 2030` для актива `Sukhoi Log`; `MineLifeYears` остаётся `10`; `ProductionProfile` без изменений. `GO_SEED_SUKHOI_LOG_YEARS` в Python-модуле СТАНЕТ `2030…2039` автоматически — он читается из исходника Go-сида.

- [ ] **Step 1: Обновить ожидания в тестах обоих контуров**

В `dcf/mine_plans_seed_test.go`, в `wantAssetPlanSeed` для `"Sukhoi Log"`: `profileStartYear: 2030`.

В `scripts/polyus_output_model_test.py`:
- `test_default_horizon_matches_go_seed` → `frozenset(range(2030, 2040))`;
- `test_horizon_is_derived_from_the_go_seed_source` → `self.assertEqual(start, 2030)`.

Второй тест обязан упасть первым, если Go-сид отстал: он читает `dcf/mine_plans_seed.go` и сверяет горизонт с исходником. Это и есть проверка Review Focus №1 — рассинхрон «профиль сдвинут, окно нет» ловится тестом, а не глазами.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./dcf/ -run TestAssetPlanSeed -v`
Expected: FAIL — «Sukhoi Log: ProfileStartYear = 2028, want 2030»

Run: `python3 scripts/polyus_output_model_test.py -k horizon`
Expected: FAIL — горизонт всё ещё `2028…2037`

- [ ] **Step 3: Поменять `ProfileStartYear` и объяснить сдвиг в комментарии**

В `dcf/mine_plans_seed.go` для блока `Sukhoi Log`: `ProfileStartYear: 2030`. Комментарий обязан назвать причину: окно ОПР 2025–2029 (первая линия ЗИФ по консенсусу аналитиков — 2030) пересеклось бы по ключу `(company, asset, year)` с проектными строками 2028–2029, а `ReplacingMergeTree` сворачивает строку ЦЕЛИКОМ — чей сид применён последним, тот и определил бы выпуск, capex и AISC этого года. `MineLifeYears: 10` НЕ меняется (проект по-прежнему 10 лет производства, сдвинулось начало), и это тоже надо сказать: горизонт ТАБЛИЦЫ теперь 2025–2039, но «10 лет» описывает проектные годы.

- [ ] **Step 4: Run тесты пакета**

Run: `go test ./dcf/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add dcf/mine_plans_seed.go dcf/mine_plans_seed_test.go
git commit -m "dcf: start Sukhoi Log project profile in 2030, after the OPR window"
```

---

### Task 3: Окно ОПР 2025–2029 в Python-сиде

**Files:**
- Modify: `scripts/polyus_output_model.py:60-70` (`OPR_YEARS`), `:255-262` (`ESTIMATED_YEARS`)
- Test: `scripts/polyus_output_model_test.py` (класс `TestReadRefined`, `TestBuildRows`)

**Interfaces:**
- Consumes: `GO_SEED_SUKHOI_LOG_YEARS` (Task 3 среза 1 — горизонт, читаемый из `dcf/mine_plans_seed.go`), который после Task 2 станет `2030…2039`.
- Produces: `OPR_YEARS = (2025, 2026, 2027, 2028, 2029)`, `ESTIMATED_YEARS = (2027, 2028, 2029)`.

- [ ] **Step 1: Обновить тесты под новое окно**

```python
def test_opr_years_cover_the_window_until_the_mill_starts(self):
    # Первая линия ЗИФ Сухого Лога — 2030 (базовый график: консенсус
    # аналитиков), поэтому ОПР идёт 2025–2029 и кончается перед профилем.
    self.assertEqual(om.OPR_YEARS, (2025, 2026, 2027, 2028, 2029))

def test_all_estimated_years_share_one_rule(self):
    # Факт есть только за 2025–2026; 2027–2029 оценены ОДНИМ правилом.
    rows = {r["year"]: r["production_koz"] for r in om.build_rows({2025: 242.2, 2026: 271.7})}
    self.assertEqual(len(set(rows[y] for y in (2027, 2028, 2029))), 1)
    self.assertAlmostEqual(rows[2027], 257.0, places=1)

def test_rows_are_five_and_before_the_profile(self):
    rows = om.build_rows({2025: 242.2, 2026: 271.7})
    self.assertEqual([r["year"] for r in rows], [2025, 2026, 2027, 2028, 2029])
    self.assertFalse(set(r["year"] for r in rows) & om.GO_SEED_SUKHOI_LOG_YEARS)
```

Существующий тест `test_opr_years_are_exactly_the_first_three` заменяется первым из трёх выше (имя перестаёт быть верным). Тесты горизонта (`test_default_horizon_matches_go_seed`, `test_horizon_is_derived_from_the_go_seed_source`) уже обновлены в Task 2 — здесь их трогать не нужно: они читают исходник Go-сида и следуют за ним.

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 scripts/polyus_output_model_test.py`
Expected: FAIL — несовпадение `OPR_YEARS` и числа строк

- [ ] **Step 3: Расширить окно и правило оценки**

В `scripts/polyus_output_model.py`: `OPR_YEARS = (2025, 2026, 2027, 2028, 2029)`; `ESTIMATED_YEARS = (2027, 2028, 2029)`. Докстроки обеих констант обновить: окно привязано к дате запуска ЗИФ (2030 по базовому графику), а не к официальному плану 2028; оценка одним правилом, потому что факта нет ни за один из трёх годов и тренд продлевать нечем. `GO_SEED_SUKHOI_LOG_YEARS_FALLBACK` обновить на `frozenset(range(2030, 2040))` — запасное значение обязано совпадать с новым горизонтом Go-сида, иначе при недоступном исходнике защита от пересечения сработает по неверному множеству.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 scripts/polyus_output_model_test.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/polyus_output_model.py scripts/polyus_output_model_test.py
git commit -m "polyus output: extend the OPR window to 2029, before the mill starts"
```

---

### Task 4: Живая приёмка обоих контуров

**Files:**
- Modify: `sql/polyus_output_seed.sql` (перегенерируется), `docs/ROADMAP_DCF_POLYUS.md` (§7.3 — новый подраздел приёмки), `docs/DCF_DATA_COVERAGE.md` (метрика и окно)

**Interfaces:**
- Consumes: всё из Tasks 1–3.
- Produces: применённые сиды и записанная приёмка.

- [ ] **Step 1: Прогнать генератор и применить оба сида**

```bash
make polyus-output
make import STAT=dcf_mine_plans          # Go-сид: новая метрика + профиль 2030-2039
make ch-sql FILE=sql/polyus_output_seed.sql   # Python-сид: окно ОПР 2025-2029
```

- [ ] **Step 2: Проверить состав таблицы**

Run (HTTP, пользователь `default`):
```
SELECT count() FROM mine_plans FINAL;                          -- 92
SELECT year, production_koz FROM mine_plans FINAL
  WHERE asset='Sukhoi Log' ORDER BY year;                      -- 2025-2029 ОПР, 2030-2039 профиль
```

Ожидание: 92 = 77 (AISC-сид) + 10 (профиль 2030–2039) + 5 (ОПР 2025–2029). Ни одного года не потеряно и не удвоено.

- [ ] **Step 3: Проверить групповую сходимость**

Run: сумма `production_koz` по пяти рудникам за 2025 против `CONSOLIDATED OPERATING RESULTS` (`refined`) = **2790.6**.
Ожидание: расхождение ≤1 % (цель перевода на `refined` — сойтись).

- [ ] **Step 4: Пересчитать NAV и объяснить расхождения построчно**

```bash
DCF_RUN_ID=<uuid> make import STAT=dcf_engine    # ожидание: 36 строк
```

Ожидание и что записать: у пяти рудников NAV изменится (сменился базовый год выпуска), у Сухого Лога снизится (проектный поток сдвинулся на 2 позиции дисконта из-за двух добавленных ОПР-лет — это выбранное поведение, спека §5). **Каждое расхождение объясняется через изменившийся вход, а не констатируется.**

- [ ] **Step 5: Обновить документы и закоммитить**

`docs/ROADMAP_DCF_POLYUS.md` — подраздел §7.3 с числами приёмки; `docs/DCF_DATA_COVERAGE.md` — сноска `[^prod]` (метрика стала `refined`, расхождение 5.61 % закрыто) и пробел №9 (окно расширено до 2029).

```bash
git add -A
git commit -m "polyus output: slice 2 acceptance — refined metric and OPR window to 2029"
```

- [ ] **Step 6: Полный прогон**

Run: `make all` → зелёный; `python3 scripts/polyus_output_model_test.py` → PASS.
Отдельно назвать в отчёте: `python3 scripts/extrapolation_aisc_test.py` → 1 красный тест, известный до этих работ (см. Global Constraints).
