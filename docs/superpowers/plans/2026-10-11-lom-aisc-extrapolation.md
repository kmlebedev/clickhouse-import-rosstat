# AISC Extrapolation Script Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single group-level sustaining wedge (`tcc + 698`) in Polyus `mine_plans` with a per-asset cost structure calibrated on published per-asset AISC/TCC for 2019–2021, reconciled to group AISC.

**Architecture:** A standalone Python script (`scripts/extrapolation_aisc.py`) parses Polyus 4Q/FY press-release PDFs for per-asset AISC and TCC, calibrates a per-asset sustaining wedge as a *share of TCC*, extrapolates it to 2025–LOM, reconciles to group AISC by a single closed-form scalar, and emits a SQL seed plus CSV. It reads ClickHouse but never writes to it; insertion happens through `make ch-sql`.

**Tech Stack:** Python 3 (stdlib only — `re`, `json`, `csv`, `subprocess`, `urllib`, `unittest`), `pdftotext` (poppler, already used by `polyus/`), ClickHouse HTTP interface, Make.

**Spec:** [docs/superpowers/specs/2026-10-11-lom-aisc-extrapolation-design.md](../../superpowers/specs/2026-10-11-lom-aisc-extrapolation-design.md)

## Global Constraints

- **Canonical asset names**, matching `databook_polyus.table` and `dcf/mine_plans_seed.go` verbatim: `OLIMPIADA`, `BLAGODATNOYE`, `NATALKA`, `VERNINSKOYE2`, `KURANAKH`.
- **Units**: `production_koz` in thousands of troy ounces; `tcc`/`aisc` in USD/oz; `capex_sustaining`, `capex_project`, `closure_costs` in **million USD**.
- **LOM horizons** per asset, identical to `dcf/mine_plans_seed.go`: OLIMPIADA 10, BLAGODATNOYE 13, NATALKA 24, VERNINSKOYE2 15, KURANAKH 15. `PLAN_BASE_YEAR = 2025`. Total plan rows: 77.
- **Calibration years: 2019–2021 only.** 2018 is excluded (FY2018 does not disclose an annual per-asset AISC for Verninskoye).
- **Wedge model**: `r_i,t = (AISC_i,t − TCC_i,t) / TCC_i,t`; `r̄_i = mean` over non-outlier years; `AISC = TCC × (1 + r̄_i · m)`.
- **Reconciliation**: `m = 1 + (AISC_group − Σ_j tcc_j·(1+r̄_j)·w_j) / (Σ_j tcc_j·r̄_j·w_j)`; applies to the wedge only, never to TCC. `m` is clamped at 0 — a negative multiplier would put AISC below TCC.
- **Outlier rule**: discard a year for an asset when `|r − median| > 3 · 1.4826 · MAD` (median absolute deviation); report every discard. A mean/σ rule is **not** robust at n = 3 and must not be used.
- **`grade_gpt` is always `NULL`**; `capex_project = 0`; `closure_costs = 0` (0 means "not specified", never "free").
- **No secrets**: connection config from env only (`CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`). Never print the password. The script does not read `~/.config/rosstat/env` itself — the Makefile exports it.
- **Style**: `ROOT = Path(__file__).resolve().parent.parent`, module-level constants, `def main() -> int`, `if __name__ == "__main__": sys.exit(main())`, no `argparse`, no `logging`, Russian prose in messages.
- **No new dependencies.** `scipy` is not used; no `pandas`.
- **`make all` must stay green** (gofmt, golangci-lint, `go vet`, `go test -race`, build).

## Review Focus

Inputs and conditions the spec implies but whose tests do not exist yet — each is pinned by a test in the task that owns the code:

1. **A year where an asset's AISC sentence is absent** (FY2018 Verninskoye). Expect: the year is excluded for that asset with a printed note, not a crash and not a silent zero.
2. **A corrupted PDF fixture** (a number altered). Expect: cross-year verification fails with a non-zero exit, naming the asset and year.
3. **Text where a page number splits a sentence** (`AISC at \n 6 \n Blagodatnoye increased to $658`). Expect: parsed correctly, not as two fragments or a wrong value.
4. **A group AISC of zero / all wedges zero**, making `Σ tcc·r̄·w = 0`. Expect: `m = 1` and a warning, not a division by zero.
4a. **A group AISC below the weighted TCC floor**, so the unclamped `m` would be negative and push AISC below TCC. Expect: `m` clamped at 0, and a non-zero residual reported rather than a pretend fit.
5. **An asset present in `mine_plans` but absent from the calibration set**, or a name mismatch (`Verninskoye` vs `VERNINSKOYE2`). Expect: an explicit error naming the asset, never a silently skipped row.

---

### Task 1: Package skeleton, config, and fixtures

**Files:**
- Create: `scripts/extrapolation_aisc.py`
- Create: `scripts/extrapolation_aisc_test.py`
- Create: `scripts/testdata/polyus_pr_4q_fy2018.pdf`, `_fy2019.pdf`, `_fy2020.pdf`, `_fy2021.pdf`
- Create: `scripts/testdata/README.md`

**Interfaces:**
- Consumes: nothing.
- Produces: `ROOT: Path`; module constants `ASSETS: list[str]`, `CALIBRATION_YEARS = (2019, 2020, 2021)`, `PLAN_BASE_YEAR = 2025`, `MINE_LIFE_YEARS: dict[str, int]`, `GROUP_AISC_2025_USD_PER_OZ = 1437.0`, `GROUP_TCC_2025_USD_PER_OZ = 739.0`, `FIXTURE_DIR: Path`, `OUTPUT_SQL: Path`, `OUTPUT_CSV: Path`, `ch_config() -> dict[str, str]`, `mask(secret: str, text: str) -> str`.

- [ ] **Step 1: Download the four fixtures**

They are pinned by URL; commit them so tests run offline.

```bash
mkdir -p scripts/testdata
curl -sL -o scripts/testdata/polyus_pr_4q_fy2018.pdf "https://polyus.com/upload/iblock/6e2/2019_02_11_press_release_4q_fy2018_final-_1_.pdf"
curl -sL -o scripts/testdata/polyus_pr_4q_fy2019.pdf "https://polyus.com/upload/iblock/d91/press_release_4q-fy2019_final-_1_.pdf"
curl -sL -o scripts/testdata/polyus_pr_4q_fy2020.pdf "https://polyus.com/upload/iblock/f1f/press_release_4q_fy2020.pdf"
curl -sL -o scripts/testdata/polyus_pr_4q_fy2021.pdf "https://polyus.com/upload/iblock/f4c/2022_03_01_press_release_4qfy2021-eng.pdf"
```

Expected sizes: 383166, 709567, 898626, 869824 bytes. Verify with `ls -l scripts/testdata/`; a redirect or HTML error page will be obvious from a wrong size.

- [ ] **Step 2: Prove `pdftotext` is available and the files parse**

```bash
pdftotext -layout scripts/testdata/polyus_pr_4q_fy2021.pdf - | grep -c "AISC at Olimpiada"
```

Expected: a non-zero count. If `pdftotext` is missing, stop — it is a prerequisite, not something to work around.

- [ ] **Step 3: Write `scripts/testdata/README.md`**

Record for each file: the source URL, the retrieval date, why the fixture is committed (cross-year verification must run offline), and the note that these are press releases republished by Polyus, not redistributed data.

- [ ] **Step 4: Write the failing test for config and constants**

```python
import unittest
from pathlib import Path

import extrapolation_aisc as ex


class TestConfig(unittest.TestCase):
    def test_assets_match_mine_plans_names(self):
        self.assertEqual(
            ex.ASSETS,
            ["OLIMPIADA", "BLAGODATNOYE", "NATALKA", "VERNINSKOYE2", "KURANAKH"],
        )

    def test_mine_life_years_match_go_seed(self):
        self.assertEqual(ex.MINE_LIFE_YEARS, {
            "OLIMPIADA": 10, "BLAGODATNOYE": 13, "NATALKA": 24,
            "VERNINSKOYE2": 15, "KURANAKH": 15,
        })

    def test_plan_row_count_is_77(self):
        self.assertEqual(sum(ex.MINE_LIFE_YEARS.values()), 77)

    def test_fixtures_exist(self):
        for year in (2018, 2019, 2020, 2021):
            path = ex.FIXTURE_DIR / f"polyus_pr_4q_fy{year}.pdf"
            self.assertTrue(path.exists(), path)

    def test_ch_config_reads_env(self):
        import os
        os.environ["CLICKHOUSE_HOST"] = "example.invalid"
        os.environ["CLICKHOUSE_USER"] = "u"
        os.environ["CLICKHOUSE_PASSWORD"] = "secret"
        self.assertEqual(ex.ch_config()["host"], "example.invalid")

    def test_mask_hides_secret(self):
        self.assertEqual(ex.mask("secret", "pw=secret"), "pw=***")
```

- [ ] **Step 5: Run tests to verify they fail**

Run: `python3 scripts/extrapolation_aisc_test.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'extrapolation_aisc'`.

- [ ] **Step 6: Implement the skeleton in `scripts/extrapolation_aisc.py`**

Module docstring (Russian) stating: what the script computes, that it never writes to ClickHouse, that it is invoked through `make`, and that the calibration years are 2019–2021 with 2018 excluded because FY2018 lacks a Verninskoye annual AISC. Then `ROOT`, the constants listed in Interfaces, `ch_config()` reading the five env vars with `CH_HTTP_PORT` defaulting to `8123`, and `mask()`. `main()` prints a short Russian summary and returns 0.

- [ ] **Step 7: Run tests to verify they pass**

Run: `python3 scripts/extrapolation_aisc_test.py -v`
Expected: PASS (6 tests).

- [ ] **Step 8: Commit**

```bash
git add scripts/extrapolation_aisc.py scripts/extrapolation_aisc_test.py scripts/testdata/
git commit -m "dcf: add AISC extrapolation script skeleton and PDF fixtures"
```

---

### Task 2: PDF text normalization

**Files:**
- Modify: `scripts/extrapolation_aisc.py`
- Modify: `scripts/extrapolation_aisc_test.py`

**Interfaces:**
- Consumes: `FIXTURE_DIR` from Task 1.
- Produces: `pdf_to_text(path: Path) -> str`; `normalize(text: str) -> str`; `parse_amount(raw: str) -> float`.

- [ ] **Step 1: Write the failing tests**

```python
class TestNormalize(unittest.TestCase):
    def test_joins_wrapped_sentence(self):
        raw = "AISC at Natalka increased\n to $626 per ounce"
        self.assertIn("increased to $626 per ounce", ex.normalize(raw))

    def test_strips_page_number_inside_sentence(self):
        raw = "AISC at\n\n6\n\nBlagodatnoye increased to $658 per ounce"
        self.assertIn("AISC at Blagodatnoye increased", ex.normalize(raw))

    def test_collapses_runs_of_spaces(self):
        self.assertEqual(ex.normalize("a   b \n c"), "a b c")

    def test_amount_handles_comma_and_space(self):
        self.assertEqual(ex.parse_amount("1,277"), 1277.0)
        self.assertEqual(ex.parse_amount("1 277"), 1277.0)
        self.assertEqual(ex.parse_amount("618"), 618.0)

    def test_fy2021_fixture_yields_known_sentence(self):
        text = ex.normalize(ex.pdf_to_text(ex.FIXTURE_DIR / "polyus_pr_4q_fy2021.pdf"))
        self.assertIn("AISC at Natalka increased to $626 per ounce", text)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 scripts/extrapolation_aisc_test.py TestNormalize -v`
Expected: FAIL — `AttributeError: module 'extrapolation_aisc' has no attribute 'normalize'`.

- [ ] **Step 3: Implement `pdf_to_text`, `normalize`, `parse_amount`**

`pdf_to_text` runs `subprocess.run(["pdftotext", "-layout", str(path), "-"], capture_output=True, text=True, check=True)` and returns stdout; on `FileNotFoundError` raise a `RuntimeError` naming `pdftotext` as the missing prerequisite. `normalize` deletes lines consisting solely of whitespace and digits (page numbers), joins lines with a space, then collapses whitespace runs with a single regex. `parse_amount` removes both commas and spaces, then `float()`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 scripts/extrapolation_aisc_test.py TestNormalize -v`
Expected: PASS (5 tests). The last one is the real check: it asserts against the actual FY2021 PDF.

- [ ] **Step 5: Commit**

```bash
git add scripts/extrapolation_aisc.py scripts/extrapolation_aisc_test.py
git commit -m "dcf: normalize polyus press-release text for AISC/TCC extraction"
```

---

### Task 3: Extract per-asset AISC and TCC from prose

**Files:**
- Modify: `scripts/extrapolation_aisc.py`
- Modify: `scripts/extrapolation_aisc_test.py`

**Interfaces:**
- Consumes: `normalize`, `parse_amount`, `pdf_to_text`, `FIXTURE_DIR`.
- Produces: `extract_year(text: str, year: int) -> dict[str, dict[str, float]]` returning `{ASSET_NAME: {"aisc": float, "tcc": float}}` keyed by canonical asset name; `AssetNameError(Exception)`.

- [ ] **Step 1: Write the failing tests**

The expected values are the verified ground truth; they are the specification here.

```python
class TestExtract(unittest.TestCase):
    def setUp(self):
        self.texts = {
            y: ex.normalize(ex.pdf_to_text(ex.FIXTURE_DIR / f"polyus_pr_4q_fy{y}.pdf"))
            for y in (2018, 2019, 2020, 2021)
        }

    def test_2021_aisc(self):
        got = ex.extract_year(self.texts[2021], 2021)
        self.assertEqual({a: got[a]["aisc"] for a in ex.ASSETS},
                         {"OLIMPIADA": 618.0, "BLAGODATNOYE": 658.0, "NATALKA": 626.0,
                          "VERNINSKOYE2": 644.0, "KURANAKH": 865.0})

    def test_2021_tcc(self):
        got = ex.extract_year(self.texts[2021], 2021)
        self.assertEqual({a: got[a]["tcc"] for a in ex.ASSETS},
                         {"OLIMPIADA": 369.0, "BLAGODATNOYE": 367.0, "NATALKA": 368.0,
                          "VERNINSKOYE2": 358.0, "KURANAKH": 569.0})

    def test_2020_aisc(self):
        got = ex.extract_year(self.texts[2020], 2020)
        self.assertEqual({a: got[a]["aisc"] for a in ex.ASSETS},
                         {"OLIMPIADA": 515.0, "BLAGODATNOYE": 478.0, "NATALKA": 529.0,
                          "VERNINSKOYE2": 560.0, "KURANAKH": 756.0})

    def test_2019_aisc(self):
        got = ex.extract_year(self.texts[2019], 2019)
        self.assertEqual({a: got[a]["aisc"] for a in ex.ASSETS},
                         {"OLIMPIADA": 455.0, "BLAGODATNOYE": 566.0, "NATALKA": 605.0,
                          "VERNINSKOYE2": 636.0, "KURANAKH": 777.0})

    def test_2018_is_incomplete_for_verninskoye(self):
        got = ex.extract_year(self.texts[2018], 2018)
        self.assertNotIn("VERNINSKOYE2", got)
        self.assertEqual(got["OLIMPIADA"]["aisc"], 468.0)

    def test_shoulder_variant_at_kuranakh_2021(self):
        # "At Kuranakh, AISC increased to $865 per ounce"
        got = ex.extract_year(self.texts[2021], 2021)
        self.assertEqual(got["KURANAKH"]["aisc"], 865.0)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 scripts/extrapolation_aisc_test.py TestExtract -v`
Expected: FAIL — no attribute `extract_year`.

- [ ] **Step 3: Implement `extract_year`**

Two passes over the normalized text, restricted to sentences belonging to the requested year.

Locate the annual paragraph start with `In {year}, AISC at` and `In {year}, TCC at`. From each start, scan sentences (split on `.`) while they keep matching a cost pattern; stop at the first sentence that matches neither the cost pattern nor a bare-continuation pattern. A sentence inherits the paragraph year when it lacks its own `In YYYY,` prefix.

Recognized sentence shapes, all verified present in the fixtures:

- `AISC at Olimpiada decreased to $468 per ounce`
- `At Kuranakh, AISC increased to $865 per ounce`
- `AISC at Natalka were $1,253 per ounce`
- `TCC at Olimpiada amounted to $369 per ounce`
- `TCC at Blagodatnoye were $367 per ounce`
- `In 2021, TCC at Olimpiada amounted to $369 per ounce`

Map source names to canonical ones: `Olimpiada→OLIMPIADA`, `Blagodatnoye→BLAGODATNOYE`, `Natalka→NATALKA`, `Verninskoye`/`Verniskoye→VERNINSKOYE2`, `Kuranakh→KURANAKH`. `Alluvial`/`Alluvials` and `Irkutsk business unit` are **skipped deliberately** — they are not among the five producing mines; do not raise. Any other name raises `AssetNameError` naming the name and year.

**Guard against the Q4 trap:** the quarterly sentence `In the fourth quarter, AISC at Olimpiada increased to $389 per ounce` (FY2018) must never satisfy the annual pattern. Because the parser is anchored on the literal `In {year},` prefix and inherits only within that paragraph, the quarterly sentence is out of scope by construction. The 2018 test above is the regression check.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 scripts/extrapolation_aisc_test.py TestExtract -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/extrapolation_aisc.py scripts/extrapolation_aisc_test.py
git commit -m "dcf: extract per-asset AISC and TCC from polyus prose"
```

---

### Task 4: Cross-year verification

**Files:**
- Modify: `scripts/extrapolation_aisc.py`
- Modify: `scripts/extrapolation_aisc_test.py`
- Create: `scripts/testdata/polyus_pr_4q_fy2021_corrupt.pdf`

**Interfaces:**
- Consumes: `extract_year`.
- Produces: `cross_check_overlap(year: int, a: dict, b: dict, src_a: Path, src_b: Path) -> None` raising `CrossCheckError`; `build_series(fixtures: dict[int, Path]) -> dict[str, dict[int, dict[str, float]]]`.

- [ ] **Step 1: Write the failing tests**

```python
class TestCrossCheck(unittest.TestCase):
    def test_reporting_year_and_comparative_year_agree(self):
        # FY2020 states 2019 as the comparative year; FY2019 states it as the
        # reporting year. Same numbers, two sources -- that is the check.
        series = ex.build_series({
            2019: ex.FIXTURE_DIR / "polyus_pr_4q_fy2019.pdf",
            2020: ex.FIXTURE_DIR / "polyus_pr_4q_fy2020.pdf",
        })
        self.assertEqual(series["OLIMPIADA"][2019]["aisc"], 455.0)
        self.assertEqual(series["NATALKA"][2019]["aisc"], 605.0)

    def test_2020_verified_across_two_documents(self):
        series = ex.build_series({
            2020: ex.FIXTURE_DIR / "polyus_pr_4q_fy2020.pdf",
            2021: ex.FIXTURE_DIR / "polyus_pr_4q_fy2021.pdf",
        })
        self.assertEqual(series["OLIMPIADA"][2020]["aisc"], 515.0)
        self.assertEqual(series["KURANAKH"][2020]["aisc"], 756.0)

    def test_corrupt_fixture_is_detected(self):
        with self.assertRaises(ex.CrossCheckError) as ctx:
            ex.build_series({
                2020: ex.FIXTURE_DIR / "polyus_pr_4q_fy2020.pdf",
                2021: ex.FIXTURE_DIR / "polyus_pr_4q_fy2021_corrupt.pdf",
            })
        self.assertIn("OLIMPIADA", str(ctx.exception))

    def test_2018_is_excluded_from_calibration(self):
        self.assertNotIn(2018, ex.CALIBRATION_YEARS)
```

Note: the first two tests pass only because `build_series` verifies the overlap — if it did not compare, a transcription error between the two documents would go unnoticed and the plans' tests would still be green.

- [ ] **Step 2: Create the corrupted fixture**

Copy `polyus_pr_4q_fy2021.pdf` to `polyus_pr_4q_fy2021_corrupt.pdf` and alter exactly one character in the comparative 2020 AISC for Olimpiada in the extracted text stream. Use a small script that decompresses the PDF content stream, replaces the first occurrence of `515` in the relevant text operator with `516`, and rewrites the file. Add a `README.md` note recording that this fixture is deliberately corrupted and must never be used for calibration.

- [ ] **Step 3: Run tests to verify they fail**

Run: `python3 scripts/extrapolation_aisc_test.py TestCrossCheck -v`
Expected: FAIL — no attribute `build_series`.

- [ ] **Step 4: Implement `cross_check_overlap` and `build_series`**

`build_series` reads each fixture, extracts the reporting year plus the comparative year present in the same document, and merges into `{asset: {year: {"aisc":…, "tcc":…}}}`. For every year present in two documents, compare AISC and TCC per asset; on any mismatch raise `CrossCheckError` naming the asset, year, both values, and both source files. `build_series` must ignore `*_corrupt.pdf` unless it is passed explicitly.

**Important:** comparison is exact for TCC and within `0.5` USD/oz for AISC — the comparative column is occasionally restated for a reclassification, and the tolerance is what distinguishes a restatement from a transcription error. If a real restatement appears during execution, record it in the plan's Notes rather than widening the tolerance silently.

- [ ] **Step 5: Run tests to verify they pass**

Run: `python3 scripts/extrapolation_aisc_test.py TestCrossCheck -v`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add scripts/extrapolation_aisc.py scripts/extrapolation_aisc_test.py scripts/testdata/
git commit -m "dcf: verify AISC series across overlapping polyus reports"
```

---

### Task 5: Calibration with outlier filtering

**Files:**
- Modify: `scripts/extrapolation_aisc.py`
- Modify: `scripts/extrapolation_aisc_test.py`

**Interfaces:**
- Consumes: `build_series`, `ASSETS`, `CALIBRATION_YEARS`.
- Produces: `wedge_share(aisc: float, tcc: float) -> float`; `calibrate(series: dict) -> dict[str, dict]` returning `{asset: {"r_bar": float, "years": list[int], "discarded": list[tuple[int, float]]}}`; `OutlierNote` dataclass or plain dict entries for reporting.

- [ ] **Step 1: Write the failing tests**

```python
class TestCalibrate(unittest.TestCase):
    def test_wedge_share(self):
        self.assertAlmostEqual(ex.wedge_share(618.0, 369.0), 0.6748, places=3)
        self.assertAlmostEqual(ex.wedge_share(865.0, 569.0), 0.5202, places=3)

    def test_calibration_uses_2019_to_2021(self):
        cal = ex.calibrate(ex.real_series())
        self.assertEqual(sorted(cal["OLIMPIADA"]["years"]), [2019, 2020, 2021])

    def test_known_r_bar_values(self):
        cal = ex.calibrate(ex.real_series())
        self.assertAlmostEqual(cal["OLIMPIADA"]["r_bar"], 0.626, places=3)
        self.assertAlmostEqual(cal["NATALKA"]["r_bar"], 0.573, places=3)
        self.assertAlmostEqual(cal["KURANAKH"]["r_bar"], 0.488, places=3)

    def test_no_outliers_in_clean_data(self):
        cal = ex.calibrate(ex.real_series())
        for asset in ex.ASSETS:
            self.assertEqual(cal[asset]["discarded"], [])

    def test_outlier_is_discarded_and_reported(self):
        series = ex.real_series()
        series["KURANAKH"][2020] = {"aisc": 5000.0, "tcc": 518.0}
        cal = ex.calibrate(series)
        self.assertIn(2020, [y for y, _ in cal["KURANAKH"]["discarded"]])
        self.assertAlmostEqual(cal["KURANAKH"]["r_bar"], 0.503, places=2)
```

`real_series()` is a test helper assembling the verified 2019–2021 values in memory, so calibration tests need no PDF parsing. Reference values: Olimpiada (455,293) (515,312) (618,369); Blagodatnoye (566,398) (478,347) (658,367); Natalka (605,396) (529,355) (626,368); Verninskoye (636,363) (560,327) (644,358); Kuranakh (777,523) (756,518) (865,569).

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 scripts/extrapolation_aisc_test.py TestCalibrate -v`
Expected: FAIL — no attribute `wedge_share`.

- [ ] **Step 3: Implement `wedge_share` and `calibrate`**

`wedge_share` returns `(aisc - tcc) / tcc`, raising `ValueError` when `tcc <= 0`.

`calibrate` builds each asset's `r` series over `CALIBRATION_YEARS`, then filters outliers with a **median-based (MAD) rule**, not mean/σ. Use the median and the median absolute deviation: `med = median(values)`, `mad = median(|x − med|)`, `scaled = 1.4826 · mad`. Discard any value with `|x − med| > 3 · scaled`; if `scaled == 0`, discard nothing. Recompute `r_bar` as the mean of survivors and record each discard with its year and value.

**Why MAD and not mean ± 2σ:** with only three observations a single gross outlier inflates σ enough to survive its own band. On the real Kuranakh series (0.486, 8.653, 0.520 after perturbing one year) the mean/σ rule keeps all three and yields `r̄ = 3.22`, while the MAD rule keeps 0.486 and 0.520 and yields `r̄ = 0.503`. The rule must be robust at n = 3; that is the whole point of the guard.

When fewer than two values survive, raise `CalibrationError` naming the asset — a single point cannot support an outlier test.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 scripts/extrapolation_aisc_test.py TestCalibrate -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/extrapolation_aisc.py scripts/extrapolation_aisc_test.py
git commit -m "dcf: calibrate per-asset sustaining wedge with outlier filtering"
```

---

### Task 6: Reconciliation (closed form)

**Files:**
- Modify: `scripts/extrapolation_aisc.py`
- Modify: `scripts/extrapolation_aisc_test.py`

**Interfaces:**
- Consumes: `calibrate`.
- Produces: `reconcile(tcc: dict[str, float], r_bar: dict[str, float], production: dict[str, float], group_aisc: float) -> tuple[dict[str, float], float, float]` returning `(aisc, m, residual_usd_per_oz)`.

- [ ] **Step 1: Write the failing tests**

```python
class TestReconcile(unittest.TestCase):
    def test_reconciles_exactly_to_group(self):
        tcc = {"A": 400.0, "B": 700.0}
        rb = {"A": 0.60, "B": 0.50}
        prod = {"A": 1000.0, "B": 500.0}
        aisc, m, resid = ex.reconcile(tcc, rb, prod, group_aisc=1200.0)
        w = {"A": 1000 / 1500, "B": 500 / 1500}
        got = aisc["A"] * w["A"] + aisc["B"] * w["B"]
        self.assertAlmostEqual(got, 1200.0, places=6)
        self.assertAlmostEqual(resid, 0.0, places=6)

    def test_m_applies_to_wedge_not_to_tcc(self):
        tcc = {"A": 400.0}
        rb = {"A": 0.50}
        aisc, m, _ = ex.reconcile(tcc, rb, {"A": 1.0}, group_aisc=800.0)
        # r_bar*m = 1.0 -> AISC = 400*(1+1.0) = 800
        self.assertAlmostEqual(aisc["A"], 800.0, places=6)

    def test_zero_wedge_falls_back_to_m_one(self):
        aisc, m, resid = ex.reconcile({"A": 400.0}, {"A": 0.0}, {"A": 1.0}, 500.0)
        self.assertEqual(m, 1.0)
        self.assertAlmostEqual(aisc["A"], 400.0, places=6)

    def test_residual_is_zero_when_group_is_reachable(self):
        # A positive m makes any achievable group AISC reachable, so the
        # post-hoc residual must be ~0 -- not merely "some sign".
        _, _, resid = ex.reconcile({"A": 400.0}, {"A": 0.5}, {"A": 1.0}, 900.0)
        self.assertAlmostEqual(resid, 0.0, places=6)

    def test_residual_is_nonzero_when_group_is_unreachable(self):
        # m cannot go below 0: with r_bar = 0.5 and tcc = 400 the floor is
        # AISC = 400, so a group AISC of 100 is unreachable and the residual
        # must report the shortfall rather than a pretend fit.
        aisc, m, resid = ex.reconcile({"A": 400.0}, {"A": 0.5}, {"A": 1.0}, 100.0)
        self.assertLess(resid, 0.0)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 scripts/extrapolation_aisc_test.py TestReconcile -v`
Expected: FAIL — no attribute `reconcile`.

- [ ] **Step 3: Implement `reconcile`**

Compute `w_i = production_i / Σ production`. Compute `den = Σ tcc_j·r̄_j·w_j`. If `den <= 0` (or below a tiny epsilon), return `m = 1.0`, `aisc_i = tcc_i·(1+r̄_i)`, and the signed residual against `group_aisc`.

Otherwise `m = 1 + (group_aisc − Σ tcc_j·(1+r̄_j)·w_j) / den`.

**Clamp `m` at 0.** A negative `m` would drive AISC below TCC — negative sustaining cost, which is meaningless. The clamps binds only when the target group AISC is below the weighted TCC floor, i.e. genuinely unreachable. When clamped, the residual will be non-zero; that is the honest report of an unreachable target, not a failure to fit.

Then `aisc_i = tcc_i·(1 + r̄_i·m)`.

Return the post-hoc weighted residual in USD/oz, computed from the emitted AISC values (`Σ aisc_i·w_i − group_aisc`), so the caller reports what the rows actually contain rather than what the formula promised. When the target is reachable the residual is ~0; when clamped it is negative and the validator's 1 % threshold will flag it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 scripts/extrapolation_aisc_test.py TestReconcile -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/extrapolation_aisc.py scripts/extrapolation_aisc_test.py
git commit -m "dcf: add closed-form group AISC reconciliation"
```

---

### Task 7: Actual production from ClickHouse

**Files:**
- Modify: `scripts/extrapolation_aisc.py`
- Modify: `scripts/extrapolation_aisc_test.py`

**Interfaces:**
- Consumes: `ch_config`, `mask`, `ASSETS`.
- Produces: `production_sql(asset: str, year: int) -> str`; `parse_ch_json(payload: str) -> list[dict]`; `ch_query(sql: str) -> list[dict]`; `read_actual_production(assets: list[str], years: tuple[int, ...]) -> dict[str, dict[int, float]]`.

- [ ] **Step 1: Write the failing tests**

Do not hit the network in tests. Test the SQL builder and the response parser as pure functions.

```python
class TestProductionQuery(unittest.TestCase):
    def test_sql_uses_datapack_metric(self):
        sql = ex.production_sql("OLIMPIADA", 2021)
        self.assertIn("databook_polyus", sql)
        self.assertIn("Total Dore gold output", sql)
        self.assertIn("ANNUAL", sql)

    def test_parses_clickhouse_json_rows(self):
        payload = '{"data":[{"asset":"OLIMPIADA","y":2021,"v":1112.8}]}'
        rows = ex.parse_ch_json(payload)
        self.assertEqual(rows[0]["v"], 1112.8)

    def test_zero_production_is_not_an_error(self):
        # TITIMUKHTA/ZAPADNOYE are legitimately zero in the datapack
        self.assertEqual(ex.parse_ch_json('{"data":[{"v":0}]}')[0]["v"], 0.0)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 scripts/extrapolation_aisc_test.py TestProductionQuery -v`
Expected: FAIL — no attribute `production_sql`.

- [ ] **Step 3: Implement `production_sql`, `parse_ch_json`, `ch_query`, `read_actual_production`**

`production_sql(asset, year)` returns the same predicate used by `dcf/mine_plans_import.go`, reading the annual datapack value for `name = 'Total Dore gold output'` and `data = 'ANNUAL'` limited to the given asset and year. `ch_query` POSTs the SQL over HTTP to `{host}:{port}` with basic auth from `ch_config()`, using `urllib.request`; any non-200 raises `ClickHouseError` with the body, passed through `mask()` so the password cannot leak into an error message. `read_actual_production` loops assets and years, returning `0.0` for a missing row — absence is a fact, not a failure.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 scripts/extrapolation_aisc_test.py TestProductionQuery -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/extrapolation_aisc.py scripts/extrapolation_aisc_test.py
git commit -m "dcf: read actual production from databook_polyus"
```

---

### Task 8: Extrapolate to 2025–LOM and allocate capex

**Files:**
- Modify: `scripts/extrapolation_aisc.py`
- Modify: `scripts/extrapolation_aisc_test.py`

**Interfaces:**
- Consumes: `reconcile`, `MINE_LIFE_YEARS`, `PLAN_BASE_YEAR`, `ASSETS`.
- Produces: `escalation_factor(base_year: int, target_year: int) -> float`; `sustaining_capex_mn(aisc: float, tcc: float, production_koz: float) -> float`; `build_plan_rows(...) -> list[dict]` with keys `asset`, `year`, `production_koz`, `tcc`, `aisc`, `capex_sustaining`.

- [ ] **Step 1: Write the failing tests**

```python
class TestPlanRows(unittest.TestCase):
    def test_capex_units_are_million_usd(self):
        # (aisc - tcc) * koz / 1000 = USD million
        got = ex.sustaining_capex_mn(aisc=950.0, tcc=450.0, production_koz=1400.0)
        self.assertAlmostEqual(got, 700.0, places=6)

    def test_row_count_is_77(self):
        rows = ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production={a: {y: 100.0 for y in range(2021, 2031)} for a in ex.ASSETS},
            group_aisc=1400.0,
        )
        self.assertEqual(len(rows), 77)

    def test_years_start_at_plan_base(self):
        rows = ex.build_plan_rows(
            tcc_2025={"OLIMPIADA": 773.0}, r_bar={"OLIMPIADA": 0.6},
            production={"OLIMPIADA": {2025: 900.0}}, group_aisc=1437.0,
        )
        self.assertEqual(rows[0]["year"], 2025)
        self.assertEqual(rows[-1]["year"], 2034)

    def test_escalation_compounds(self):
        self.assertAlmostEqual(ex.escalation_factor(2025, 2025), 1.0, places=9)
        self.assertGreater(ex.escalation_factor(2025, 2030), 1.0)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 scripts/extrapolation_aisc_test.py TestPlanRows -v`
Expected: FAIL — no attribute `sustaining_capex_mn`.

- [ ] **Step 3: Implement the three functions**

`escalation_factor` compounds the `ESCALATION` constants from `base_year` to `target_year`. `sustaining_capex_mn` returns `(aisc - tcc) * production_koz / 1000`. `build_plan_rows` resolves each asset's TCC for each plan year (observed where available, else the 2025 anchor escalated), computes AISC through `reconcile` per year with that year's production weights, allocates capex, and emits one dict per asset-year. It must not emit a row for an asset whose production is entirely absent in the input, and it prints a Russian note naming every asset it skipped and why.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 scripts/extrapolation_aisc_test.py TestPlanRows -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/extrapolation_aisc.py scripts/extrapolation_aisc_test.py
git commit -m "dcf: extrapolate AISC to LOM and allocate sustaining capex"
```

---

### Task 9: Emit SQL seed and CSV

**Files:**
- Modify: `scripts/extrapolation_aisc.py`
- Modify: `scripts/extrapolation_aisc_test.py`

**Interfaces:**
- Consumes: `build_plan_rows`.
- Produces: `sql_batches(rows: list[dict], batch_size: int = 500) -> list[str]`; `sql_literal(value) -> str`; `write_sql(path: Path, rows: list[dict], meta: dict) -> None`; `write_csv(path: Path, rows: list[dict]) -> None`.

- [ ] **Step 1: Write the failing tests**

```python
class TestEmit(unittest.TestCase):
    def test_null_grade_and_zero_project_capex(self):
        rows = [{"asset": "OLIMPIADA", "year": 2025, "production_koz": 900.0,
                 "tcc": 773.0, "aisc": 1300.0, "capex_sustaining": 474.0}]
        sql = "\n".join(ex.sql_batches(rows))
        self.assertIn("NULL", sql)
        self.assertIn("'OLIMPIADA'", sql)
        self.assertIn(",0,0)", sql)  # capex_project, closure_costs

    def test_all_ten_columns_present(self):
        sql = "\n".join(ex.sql_batches([{
            "asset": "OLIMPIADA", "year": 2025, "production_koz": 900.0,
            "tcc": 773.0, "aisc": 1300.0, "capex_sustaining": 474.0}]))
        for col in ("company", "asset", "year", "production_koz", "grade_gpt",
                    "tcc", "aisc", "capex_sustaining", "capex_project", "closure_costs"):
            self.assertIn(col, sql)

    def test_batching_respects_size(self):
        rows = [{"asset": "A", "year": 2025, "production_koz": 1.0,
                 "tcc": 1.0, "aisc": 2.0, "capex_sustaining": 0.0}] * 1200
        self.assertEqual(len(ex.sql_batches(rows, batch_size=500)), 3)

    def test_nan_is_rejected(self):
        with self.assertRaises(ValueError):
            ex.sql_literal(float("nan"))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 scripts/extrapolation_aisc_test.py TestEmit -v`
Expected: FAIL — no attribute `sql_batches`.

- [ ] **Step 3: Implement the emitters**

`sql_literal` formats floats with `repr`-level precision, raises `ValueError` on `NaN`/`inf`, quotes strings, and emits `NULL` for `None`. `sql_batches` emits `INSERT INTO mine_plans (company, asset, year, production_koz, grade_gpt, tcc, aisc, capex_sustaining, capex_project, closure_costs) VALUES ...;` grouped into batches, always writing `'PLZL'` as company, `NULL` for grade, and `0` for `capex_project`/`closure_costs`. `write_sql` prepends a header comment: the generating command, the date, the calibration years, the fitted `m`, and the residual. `write_csv` writes the same columns plus `r_bar` and `escalation_factor` for inspection.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 scripts/extrapolation_aisc_test.py TestEmit -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/extrapolation_aisc.py scripts/extrapolation_aisc_test.py
git commit -m "dcf: emit mine_plans SQL seed and CSV"
```

---

### Task 10: Validation guards and `main()`

**Files:**
- Modify: `scripts/extrapolation_aisc.py`
- Modify: `scripts/extrapolation_aisc_test.py`
- Modify: `Makefile`

**Interfaces:**
- Consumes: everything above.
- Produces: `validate(rows, cal, meta) -> list[str]`; `main() -> int`; Make target `dcf-aisc`.

- [ ] **Step 1: Write the failing tests**

```python
class TestValidate(unittest.TestCase):
    def test_accepts_consistent_rows(self):
        rows = [{"asset": "OLIMPIADA", "year": 2025, "production_koz": 900.0,
                 "tcc": 773.0, "aisc": 1300.0, "capex_sustaining": 474.0}]
        problems = ex.validate(rows, {"OLIMPIADA": {"r_bar": 0.68}}, {"residual": 0.0})
        self.assertEqual(problems, [])

    def test_flags_aisc_below_tcc(self):
        rows = [{"asset": "OLIMPIADA", "year": 2025, "production_koz": 900.0,
                 "tcc": 773.0, "aisc": 700.0, "capex_sustaining": 0.0}]
        problems = ex.validate(rows, {"OLIMPIADA": {"r_bar": 0.68}}, {"residual": 0.0})
        self.assertTrue(any("aisc" in p for p in problems))

    def test_flags_year_outside_window(self):
        rows = [{"asset": "OLIMPIADA", "year": 2100, "production_koz": 1.0,
                 "tcc": 1.0, "aisc": 2.0, "capex_sustaining": 0.0}]
        problems = ex.validate(rows, {"OLIMPIADA": {"r_bar": 0.68}}, {"residual": 0.0})
        self.assertTrue(any("2100" in p for p in problems))

    def test_flags_missing_asset(self):
        problems = ex.validate([], {a: {"r_bar": 0.6} for a in ex.ASSETS}, {"residual": 0.0})
        self.assertTrue(any("OLIMPIADA" in p for p in problems))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 scripts/extrapolation_aisc_test.py TestValidate -v`
Expected: FAIL — no attribute `validate`.

- [ ] **Step 3: Implement `validate` and `main`**

`validate` returns a list of Russian problem strings covering: `aisc < tcc` for any row; production below zero; any year outside `[2000, 2100)`; a missing canonical asset; `r_bar` outside `0.3–1.0`; `|residual|` at or above 1 % of group AISC. An empty list means pass.

`main()` orchestrates: build series from fixtures (2019–2021), calibrate, read actual production, derive TCC anchors, reconcile, build rows, validate, write SQL and CSV, print a Russian summary (per-asset `r̄`, discarded outliers, fitted `m`, residual, row count), and return non-zero when `validate` reports problems.

- [ ] **Step 4: Add the Make target**

```make
dcf-aisc:
	python3 scripts/extrapolation_aisc.py
```

Place it with the other script targets, using the same tab-indented recipe style as `mcp-user`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `python3 scripts/extrapolation_aisc_test.py -v`
Expected: PASS (all tests from Tasks 1–10).

- [ ] **Step 6: Commit**

```bash
git add scripts/extrapolation_aisc.py scripts/extrapolation_aisc_test.py Makefile
git commit -m "dcf: validate AISC rows and wire the make target"
```

---

### Task 11: Live acceptance against ClickHouse

**Files:**
- Create: `sql/dcf_mine_plans_seed.sql` (generated)
- Modify: `dagu/dcf.yaml`
- Modify: `dcf/series_meta.go`
- Modify: `dcf/series_meta_test.go`

**Interfaces:**
- Consumes: the generated SQL seed.
- Produces: the committed seed artifact and an updated DAG chain.

- [ ] **Step 1: Generate the seed**

Run: `make dcf-aisc`
Expected: a Russian summary with per-asset `r̄`, no invented values, residual below 1 %, and 77 rows. Commit `sql/dcf_mine_plans_seed.sql`.

- [ ] **Step 2: Apply it and check the row count**

Run:
```bash
make ch-sql FILE=sql/dcf_mine_plans_seed.sql
```
Then verify: `SELECT count() FROM mine_plans FINAL` → **77**.

- [ ] **Step 3: Run the engine and compare against the old single-wedge NAV**

Run: `DCF_RUN_ID=$(uuidgen | tr A-Z a-z) make import STAT=dcf_engine`
Expected: `Imported 30 rows` (5 assets × 3 decks × 2 contours). NAV on `own_scenario`/`industrial` must **differ** from the previous run, and the two contours must remain distinguishable. Record both NAV sets in the plan's Notes.

- [ ] **Step 4: Verify the agent still sees the view**

Run: `make mcp-check`
Expected: ПРОВЕРКА ПРОЙДЕНА.

- [ ] **Step 5: Update the series catalog description**

`dcf/series_meta.go` currently states that `aisc` is "DERIVED как tcc + 698". That is no longer how the seed computes it. Update the description to name the calibration years (2019–2021), the wedge-share model, and the group reconciliation, then update the substring assertions in `dcf/series_meta_test.go` to match.

Run: `go test ./dcf/` → PASS.

- [ ] **Step 6: Chain the seed step in the DAG**

Add the seed step between `dcf_mine_plans` and `dcf_engine` in `dagu/dcf.yaml`, keeping `type: chain` so ordering is guaranteed. The seed is applied with `make ch-sql FILE=sql/dcf_mine_plans_seed.sql`.

Run: `dagu validate dagu/dcf.yaml` → OK.

- [ ] **Step 7: Run the full gate**

Run: `make all`
Expected: gofmt clean, `0 issues`, `go vet` clean, `go test -race` all `ok`, both builds succeed. Then `python3 scripts/extrapolation_aisc_test.py` → PASS.

- [ ] **Step 8: Commit**

```bash
git add sql/dcf_mine_plans_seed.sql dagu/dcf.yaml dcf/series_meta.go dcf/series_meta_test.go
git commit -m "dcf: seed mine_plans with calibrated per-asset AISC"
```

---

### Task 12: Documentation

**Files:**
- Modify: `docs/DCF_DATA_COVERAGE.md`
- Modify: `ARCHITECTURE.md`
- Modify: `README.md`
- Modify: `docs/ROADMAP_DCF_POLYUS.md`
- Modify: `docs/plans/LOM_AISC_EXTRAPOLATION_PLAN.md`

- [ ] **Step 1: Correct the gap framing**

In `docs/DCF_DATA_COVERAGE.md` §3, gap 2 currently reads "AISC по активам не публикуется". Correct it: per-asset AISC *is* published, for 2018–2021 only; from 2022 reports give per-asset TCC but group-only AISC. Update the §2 column statuses to record that `aisc` remains `derived` for the plan horizon but now rests on observed 2019–2021 shares.

- [ ] **Step 2: Update architecture and README**

Add the seed step to the documented order (Go seed → SQL seed → engine) in `ARCHITECTURE.md` §6.1/§6.4 and `README.md`. Follow the `sync-readme-architecture` skill's checklist.

- [ ] **Step 3: Record the acceptance in the roadmap**

Add the acceptance numbers to `docs/ROADMAP_DCF_POLYUS.md` §7.1: the row counts, the fitted `m`, the residual, and the NAV comparison against the previous single-wedge run. Mark gap 2 addressed by this step.

- [ ] **Step 4: Update the plan doc status**

In `docs/plans/LOM_AISC_EXTRAPOLATION_PLAN.md`, record the execution outcome, including the 2018 exclusion and the outlier rule as implemented, so the plan and the code agree.

- [ ] **Step 5: Commit**

```bash
git add docs/DCF_DATA_COVERAGE.md ARCHITECTURE.md README.md docs/ROADMAP_DCF_POLYUS.md docs/plans/LOM_AISC_EXTRAPOLATION_PLAN.md
git commit -m "docs: record calibrated AISC seed in coverage and roadmap"
```
