# Модель выпуска Полюса: атрибуция ОПР-выпуска — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Приписать ОПР-выпуск 2025–2027 Сухому Логу тремя новыми строками в `mine_plans`, чтобы эти годы перестали выпадать из NAV.

**Architecture:** Новый Python-скрипт `scripts/polyus_output_model.py` читает факт из `databook_polyus` по HTTP (тот же транспорт, что у `extrapolation_aisc.py`), считает три строки и печатает SQL-сид `sql/polyus_output_seed.sql` плюс CSV для глазной сверки. В ClickHouse скрипт не пишет — сид применяется вручную через `make ch-sql`. Ключевое: сид пишет **только строки, которых нет** у Go-сида (Сухой Лог 2025–2027), поэтому `ReplacingMergeTree` не может перезаписать калиброванный AISC пяти рудников.

**Tech Stack:** Python 3 (только стандартная библиотека, как `extrapolation_aisc.py`), `unittest` (без pytest), ClickHouse по HTTP, `make` цели.

**Spec:** `docs/superpowers/specs/2026-10-11-polyus-output-attribution-design.md`

## Global Constraints

- Скрипт **не пишет в ClickHouse**: только читает и печатает файлы. Вставка — отдельный ручной шаг `make ch-sql FILE=sql/polyus_output_seed.sql`. Причина: Python-скрипту не нужен пароль, read-only контур `kimi_reader` не нарушается (AGENTS.md, правило 11в).
- Секреты только из env через существующий `ch_config()`; содержимое `~/.config/rosstat/env` не читать и не печатать. Любой текст ошибки проходит через `mask()`.
- Сетевых вызовов в `init()` нет; URL и соединение вычисляются внутри функции.
- HTTP — только через существующий `ch_query()` из `extrapolation_aisc.py` (там `mask()`, правильный HTTP-порт 8123 и обработка `RemoteDisconnected`).
- Комментарии и докстроки — по-русски, как во всём репозитории; в них объясняется **почему**, а не что.
- Тесты — `unittest`, запуск `python3 scripts/polyus_output_model_test.py -v`. **Тесты офлайн:** обращения к ClickHouse подменяются `unittest.mock.patch.object`, поэтому ни БД, ни сети для прогона набора не нужно (в отличие от `extrapolation_aisc_test.py`, чей `TestConfig` читает живой сервер). Живая проверка — только в Task 5 Step 4, руками.
- `make all` (gofmt, golangci-lint, `go vet`, `go test -race`, сборка) должен оставаться зелёным — Go-код этим планом не меняется, но правило 9 обязательное.
- Коммиты по-английски, в духе истории (`add polyus output model seed`).

## Review Focus

Пять входов/условий, которые спека подразумевает, но которые ломаются тихо и потому получают тест в задаче-владельце:

1. **Год 2028+ попадает в новый сид** — пересекает горизонт Go-сида, `ReplacingMergeTree` молча перезапишет проектный профиль и калиброванный AISC. Ожидание: скрипт отказывает, а не печатает.
2. **Строка ОПР дублирует существующий ключ** `(PLZL, Sukhoi Log, год)` — тот же эффект, но через данные, а не через код. Ожидание: проверка пересечения по множеству годов сида.
3. **`aisc < tcc` в строке** — клин поддержания отрицательный, экономически бессмысленно. Ожидание: инвариант `aisc >= tcc` для каждой печатаемой строки.
4. **Отсутствие факта за 2025/2026 в датапаке** (пустой ответ) — расчёт подставил бы `0.0` и записал нулевой выпуск как измеренный. Ожидание: отказ с именем года.
5. **Среднее 2025–2026 считается не по тем числам** (например, по `dore` вместо `refined`) — 2027 получит 232.35 вместо 257.0, и расхождение незаметно. Ожидание: тест пинует имя метрики и значение 257.0.

---

### Task 1: Константы и чтение факта выпуска

**Files:**
- Create: `scripts/polyus_output_model.py`
- Test: `scripts/polyus_output_model_test.py`

**Interfaces:**
- Consumes: `ch_query(sql) -> list` и `ch_config() -> dict` из `extrapolation_aisc.py` (импорт модуля; тесты уже используют этот приём через `sys.path.insert`).
- Produces:
  - `ASSET = "Sukhoi Log"` — каноническое имя, дословно как в `databook_polyus.table` и `dcf/mine_plans_seed.go`.
  - `OPR_YEARS = (2025, 2026, 2027)`
  - `REFINED_METRIC = "Total refined gold output"`
  - `TCC_PROXY_USD_PER_OZ = 735.0`
  - `read_refined_output(asset: str, year: int) -> float` — выпуск года, koz; отсутствие строки → `OutputDataError`.

- [ ] **Step 1: Write the failing test**

```python
import sys
import unittest
import unittest.mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import polyus_output_model as om


class TestReadRefined(unittest.TestCase):
    def test_opr_years_are_exactly_the_first_three(self):
        # Окно ОПР в этом срезе — 2025–2027 (спека §3): 2028+ принадлежит Go-сиду.
        self.assertEqual(om.OPR_YEARS, (2025, 2026, 2027))

    def test_metric_is_refined_not_dore(self):
        # 2025: refined 242.2 против dore 230.8. Метрика обязана быть refined.
        with unittest.mock.patch.object(om, "ch_query", return_value=[{"v": 242.2}]):
            self.assertEqual(om.read_refined_output("VERNINSKOYE2", 2025), 242.2)
        self.assertIn(om.REFINED_METRIC, om.output_sql("VERNINSKOYE2", 2025))

    def test_absent_year_raises_instead_of_zero(self):
        # Пустой ответ: ноль на месте факта занизил бы NAV.
        with unittest.mock.patch.object(om, "ch_query", return_value=[]):
            with self.assertRaises(om.OutputDataError):
                om.read_refined_output("VERNINSKOYE2", 2100)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 scripts/polyus_output_model_test.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'polyus_output_model'`

- [ ] **Step 3: Implement `read_refined_output` in `scripts/polyus_output_model.py`**

Транспорт и валидацию имени актива переиспользовать из `extrapolation_aisc` (`ch_query`, `ASSET_LITERAL`), не копировать. SQL — тот же предикат импортёра, но с метрикой `REFINED_METRIC`:

```python
def output_sql(asset: str, year: int) -> str:
    """SELECT argMax(value, date) AS v FROM databook_polyus FINAL
       WHERE table='<ASSET>' AND name='Total refined gold output'
         AND data='ANNUAL' AND toYear(date)=<YEAR> FORMAT JSON"""
```

`read_refined_output` поднимает `OutputDataError` (новый класс), если список пуст или значение `None`. Отличие от `extrapolation_aisc.read_actual_production` (там ноль — нормальный результат) обязано быть в докстроке: здесь ноль означает «факта нет», а для строки плана это отказ, а не значение.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 scripts/polyus_output_model_test.py -v`
Expected: PASS (3 теста). БД не нужна: `ch_query` подменён в тестах.

- [ ] **Step 5: Commit**

```bash
git add scripts/polyus_output_model.py scripts/polyus_output_model_test.py
git commit -m "add polyus output model: refined output reader"
```

---

### Task 2: Среднее 2025–2026 для 2027 и сборка строк

**Files:**
- Modify: `scripts/polyus_output_model.py`
- Test: `scripts/polyus_output_model_test.py`

**Interfaces:**
- Consumes: `read_refined_output`, `OPR_YEARS`, `TCC_PROXY_USD_PER_OZ`, `ASSET` из Task 1.
- Produces:
  - `average_output(values: list) -> float` — среднее; пустой список → `OutputDataError`.
  - `build_rows(measured: dict) -> list[dict]` — три строки плана. Ключи строки дословно как в `extrapolation_aisc.MINE_PLANS_COLUMNS`: `asset`, `year`, `production_koz`, `tcc`, `aisc`, `capex_sustaining`, `capex_project`, `closure_costs`, `grade_gpt`.

- [ ] **Step 1: Write the failing test**

```python
class TestBuildRows(unittest.TestCase):
    def test_2027_is_mean_of_2025_and_2026(self):
        rows = om.build_rows({2025: 242.2, 2026: 271.7})
        by_year = {r["year"]: r for r in rows}
        self.assertAlmostEqual(by_year[2027]["production_koz"], 257.0, places=1)

    def test_measured_years_are_not_averaged(self):
        rows = om.build_rows({2025: 242.2, 2026: 271.7})
        by_year = {r["year"]: r for r in rows}
        self.assertAlmostEqual(by_year[2025]["production_koz"], 242.2, places=1)
        self.assertAlmostEqual(by_year[2026]["production_koz"], 271.7, places=1)

    def test_grade_is_none_and_capex_zero(self):
        rows = om.build_rows({2025: 242.2, 2026: 271.7})
        for row in rows:
            self.assertIsNone(row["grade_gpt"])
            self.assertEqual(row["capex_project"], 0)
            self.assertEqual(row["capex_sustaining"], 0.0)
            self.assertEqual(row["closure_costs"], 0)

    def test_aisc_never_below_tcc(self):
        for row in om.build_rows({2025: 242.2, 2026: 271.7}):
            self.assertGreaterEqual(row["aisc"], row["tcc"])

    def test_missing_measured_year_raises(self):
        with self.assertRaises(om.OutputDataError):
            om.build_rows({2025: 242.2})  # 2026 не прочитан
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 scripts/polyus_output_model_test.py -v`
Expected: FAIL — `AttributeError: module 'polyus_output_model' has no attribute 'build_rows'`

- [ ] **Step 3: Implement `average_output` and `build_rows`**

`average_output` — `sum(values) / len(values)`, пустой список → `OutputDataError` (по одной точке среднее не считается, как `CalibrationError` в AISC-скрипте). `build_rows` для каждого года из `OPR_YEARS`: `production_koz` — измеренное значение, если год в `measured`, иначе `average_output` по измеренным; `tcc = aisc = TCC_PROXY_USD_PER_OZ`; остальные колонки по таблице §3 спеки. Обязательная проверка при сборке: каждый год `> 0` (Review Focus №4) и `aisc >= tcc` (№3), при нарушении — `OutputDataError` с номером года.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 scripts/polyus_output_model_test.py -v`
Expected: PASS (9 тестов с Task 1)

- [ ] **Step 5: Commit**

```bash
git add scripts/polyus_output_model.py scripts/polyus_output_model_test.py
git commit -m "add polyus output model: OPR rows builder"
```

---

### Task 3: Запрет пересечения строк с горизонтом Go-сида

**Files:**
- Modify: `scripts/polyus_output_model.py`
- Test: `scripts/polyus_output_model_test.py`

**Interfaces:**
- Consumes: `OPR_YEARS`, `ASSET` из Task 1; `build_rows` из Task 2.
- Produces: `assert_no_overlap(rows: list, go_seed_years: set) -> None` — поднимает `OverlapError`, если год строки входит в `go_seed_years`.

- [ ] **Step 1: Write the failing test**

```python
class TestOverlap(unittest.TestCase):
    def test_rejects_year_owned_by_go_seed(self):
        rows = om.build_rows({2025: 242.2, 2026: 271.7})
        with self.assertRaises(om.OverlapError):
            om.assert_no_overlap(rows, go_seed_years={2028, 2029, 2030})

    def test_accepts_years_before_go_seed_horizon(self):
        rows = om.build_rows({2025: 242.2, 2026: 271.7})
        om.assert_no_overlap(rows, go_seed_years={2028, 2029})
        self.assertEqual([r["year"] for r in rows], [2025, 2026, 2027])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 scripts/polyus_output_model_test.py -v`
Expected: FAIL — `AttributeError: module 'polyus_output_model' has no attribute 'assert_no_overlap'`

- [ ] **Step 3: Implement `assert_no_overlap`**

Множество годов Go-сида задать константой `GO_SEED_SUKHOI_LOG_YEARS = frozenset(range(2028, 2038))` (10 лет: `MineLifeYears: 10`, `ProfileStartYear: 2028` — дословно из `dcf/mine_plans_seed.go`) и вызывать проверку в `main()`. Комментарий обязан назвать причину: `ReplacingMergeTree` сворачивает строку по `(company, asset, year)` целиком, поэтому пересечение = перезапись проектного профиля и калиброванного AISC (Review Focus №1, №2).

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 scripts/polyus_output_model_test.py -v`
Expected: PASS (11 тестов)

- [ ] **Step 5: Commit**

```bash
git add scripts/polyus_output_model.py scripts/polyus_output_model_test.py
git commit -m "add polyus output model: overlap guard"
```

---

### Task 4: Эмиссия SQL-сида и CSV

**Files:**
- Modify: `scripts/polyus_output_model.py`
- Test: `scripts/polyus_output_model_test.py`

**Interfaces:**
- Consumes: `build_rows` (Task 2), `MINE_PLANS_COLUMNS` и `sql_batches` из `extrapolation_aisc.py`.
- Produces:
  - `OUTPUT_SQL = ROOT / "sql/polyus_output_seed.sql"`, `OUTPUT_CSV = ROOT / "polyus_output_attribution_long.csv"`
  - `write_sql(path: Path, rows: list) -> None` — шапка с датой генерации, командой применения и объяснением атрибуции, затем один `INSERT` (3 строки, один батч).
  - `write_csv(path: Path, rows: list) -> None` — те же 10 колонок плюс справка `source_metric` (значение `refined` / `mean_2025_2026`).

- [ ] **Step 1: Write the failing test**

```python
class TestEmit(unittest.TestCase):
    def setUp(self):
        self.rows = om.build_rows({2025: 242.2, 2026: 271.7})

    def test_sql_has_single_insert_with_three_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "seed.sql"
            om.write_sql(path, self.rows)
            text = path.read_text(encoding="utf-8")
        self.assertEqual(text.count("INSERT INTO mine_plans"), 1)
        self.assertEqual(text.count("('PLZL','Sukhoi Log',"), 3)

    def test_sql_header_names_manual_apply(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "seed.sql"
            om.write_sql(path, self.rows)
            text = path.read_text(encoding="utf-8")
        self.assertIn("make ch-sql FILE=sql/polyus_output_seed.sql", text)
        self.assertIn("ПРАВИТЬ РУКАМИ НЕЛЬЗЯ", text)

    def test_csv_column_count_matches_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            om.write_csv(path, self.rows)
            with path.open(encoding="utf-8") as handle:
                table = list(csv.reader(handle))
        self.assertEqual(len(table), 4)
        self.assertTrue(all(len(row) == len(table[0]) for row in table))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 scripts/polyus_output_model_test.py -v`
Expected: FAIL — `AttributeError: module 'polyus_output_model' has no attribute 'write_sql'`

- [ ] **Step 3: Implement `write_sql` and `write_csv`**

`sql_batches(rows)` уже печатает `INSERT INTO mine_plans (company, asset, year, …) VALUES (…)` с литералом `'Sukhoi Log'`, поэтому счёт теста `('PLZL','Sukhoi Log',` совпадёт без правок. Шапку писать по образцу `SQL_HEADER_TEMPLATE` из AISC-скрипта, но короче: команда генерации, дата, объяснение атрибуции (сноска 13 релиза 1П2026) и предупреждение «ПРАВИТЬ РУКАМИ НЕЛЬЗЯ». CSV — длинный формат, числа через `repr`, пустая ячейка для `grade_gpt` (`None`): `NULL` выражается отсутствием значения, а не строкой.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 scripts/polyus_output_model_test.py -v`
Expected: PASS (14 тестов)

- [ ] **Step 5: Commit**

```bash
git add scripts/polyus_output_model.py scripts/polyus_output_model_test.py
git commit -m "add polyus output model: seed and csv emitters"
```

---

### Task 5: `main()`, цель Makefile и живая приёмка

**Files:**
- Modify: `scripts/polyus_output_model.py` (функция `main`)
- Modify: `Makefile` (новая цель рядом с `dcf-aisc`)
- Modify: `docs/ROADMAP_DCF_POLYUS.md` (§7.1 — статус пункта)

**Interfaces:**
- Consumes: всё из Tasks 1–4.
- Produces: `main() -> int` — возвращает 0 только при успехе; печатает сводку (три строки, путь к сиду, путь к CSV). Цель `make polyus-output`.

- [ ] **Step 1: Write the failing test**

```python
class TestMain(unittest.TestCase):
    @unittest.mock.patch.object(om, "read_refined_output")
    def test_main_writes_both_artifacts(self, read):
        read.side_effect = lambda asset, year: {2025: 242.2, 2026: 271.7}[year]
        with tempfile.TemporaryDirectory() as tmp:
            sql_path, csv_path = Path(tmp) / "s.sql", Path(tmp) / "s.csv"
            with unittest.mock.patch.object(om, "OUTPUT_SQL", sql_path), \
                 unittest.mock.patch.object(om, "OUTPUT_CSV", csv_path):
                code = om.main()
            self.assertEqual(code, 0)
            self.assertTrue(sql_path.exists())
            self.assertTrue(csv_path.exists())

    @unittest.mock.patch.object(om, "read_refined_output")
    def test_main_returns_nonzero_when_fact_missing(self, read):
        read.side_effect = om.OutputDataError("нет факта за 2026")
        with tempfile.TemporaryDirectory() as tmp:
            with unittest.mock.patch.object(om, "OUTPUT_SQL", Path(tmp) / "s.sql"), \
                 unittest.mock.patch.object(om, "OUTPUT_CSV", Path(tmp) / "s.csv"):
                self.assertNotEqual(om.main(), 0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 scripts/polyus_output_model_test.py -v`
Expected: FAIL — `AttributeError: module 'polyus_output_model' has no attribute 'main'`

- [ ] **Step 3: Implement `main()` and the Makefile target**

`main()`: читает 2025 и 2026 через `read_refined_output`, собирает строки, проверяет пересечение, пишет SQL и CSV, печатает сводку, возвращает 0. Любая `OutputDataError`/`OverlapError` логируется и возвращает 1 (не трейсбек: сообщение должно быть читаемым в логе прогона). Цель Makefile по образцу `dcf-aisc`:

```make
polyus-output:
	python3 scripts/polyus_output_model.py
```

- [ ] **Step 4: Run tests, then the live acceptance**

Run: `python3 scripts/polyus_output_model_test.py -v` → PASS (16 тестов)
Run: `make polyus-output` → печатает три строки и пути к артефактам
Run: `make ch-sql FILE=sql/polyus_output_seed.sql`
Run: `make all` → зелёный

Проверки в БД (HTTP, пользователь `default` без пароля):
- `SELECT count() FROM mine_plans FINAL` → **90**
- `SELECT year, production_koz FROM mine_plans FINAL WHERE asset='Sukhoi Log' ORDER BY year` → 2025: 242.2, 2026: 271.7, 2027: 257.0, далее 2028–2037 без изменений
- построчная сверка: все 87 прежних строк не изменились по `production_koz`/`tcc`/`aisc`/`capex_sustaining`/`capex_project`
- Σ `production_koz` 2025 по активам против `CONSOLIDATED OPERATING RESULTS` (2790.6) → расхождение ≤1 %, остаток объясняется переходом на `dore` у пяти рудников (Review Focus: сходимость не подгоняется)
- `DCF_RUN_ID=<uuid> make import STAT=dcf_engine` → **42 строки**

- [ ] **Step 5: Commit**

```bash
git add scripts/polyus_output_model.py scripts/polyus_output_model_test.py Makefile docs/ROADMAP_DCF_POLYUS.md
git commit -m "add polyus output model: main, make target and live acceptance"
```
