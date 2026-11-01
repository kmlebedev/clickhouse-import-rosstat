#!/usr/bin/env python3
"""Офлайн-тесты H2O-челленджера CPI v4 (#502).

Проверяется чистый слой: отсечка селектора от primary-окна, временные блоки
`fold_column`, кадр с NaN (не 0) и `month` как enum, заморозка/валидация
конфига, whitelist гиперпараметров, разбор версии Java и решение gate.
H2O, Java, ClickHouse и сеть не нужны; смоук реального H2O — только при
`H2O_SMOKE=1`.

Запуск — `make cpi-v4-h2o-test`.
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from models.cpi import cpi_v4_h2o as h2o_mod
from models.cpi import cpi_v4_leaderboard as lb


def month_range(first: date, count: int) -> list:
    return [lb.shift_month(first, index) for index in range(count)]


def cell(rmse_h2o: float, rmse_v3: float, delta: float, dm_p: float,
         ci: list, status: str = "ok", n_common: int = 100) -> dict:
    return {
        "status": status,
        "n_common": n_common,
        "models": {
            "h2o_bottomup": {"rmse": rmse_h2o, "mae": rmse_h2o * 0.8},
            "h2o_direct": {"rmse": rmse_h2o * 1.1, "mae": rmse_h2o},
            "v3_bottomup": {"rmse": rmse_v3, "mae": rmse_v3 * 0.8},
            "v2_bottomup": {"rmse": rmse_v3 * 1.2, "mae": rmse_v3},
            "B3": {"rmse": rmse_v3 * 1.4, "mae": rmse_v3},
        },
        "pairs": {
            "h2o_bottomup vs v3_bottomup": {
                "delta": delta,
                "dm_p": dm_p,
                "delta_ci": ci,
                "mde_ratio": 0.1,
                "n": n_common,
            }
        },
        "mcs": {"included": ["h2o_bottomup", "v3_bottomup"]},
    }


def write_cells(root: Path, cells: dict) -> None:
    for key, payload in cells.items():
        sample, horizon = key.split(":h")
        path = root / f"{sample}_full" / f"cell_T_NSA_h{horizon}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


class SelectorWindowTest(unittest.TestCase):
    def test_h1_cutoff_is_2015_11(self):
        origins = month_range(date(2015, 1, 1), 12)
        selected = h2o_mod.selector_origins(origins, 1)
        self.assertEqual(selected[-1], date(2015, 11, 1))
        self.assertNotIn(date(2015, 12, 1), selected)

    def test_h3_cutoff_moves_back(self):
        origins = month_range(date(2015, 1, 1), 12)
        selected = h2o_mod.selector_origins(origins, 3)
        self.assertEqual(selected[-1], date(2015, 9, 1))

    def test_selector_never_sees_primary_window(self):
        origins = month_range(date(2014, 1, 1), 30)
        for horizon in (1, 3, 6):
            for origin in h2o_mod.selector_origins(origins, horizon):
                self.assertLessEqual(lb.shift_month(origin, horizon), h2o_mod.SELECT_CUTOFF)


class FoldAssignmentTest(unittest.TestCase):
    def test_blocks_are_contiguous_and_complete(self):
        origins = month_range(date(2010, 1, 1), 30)
        assignment = h2o_mod.fold_assignment(origins)
        self.assertEqual(set(assignment), set(origins))
        ordered = sorted(origins)
        folds = [assignment[origin] for origin in ordered]
        self.assertEqual(folds, sorted(folds))
        self.assertEqual(set(folds), {0, 1, 2})
        self.assertEqual([folds.count(index) for index in range(3)], [10, 10, 10])

    def test_small_frame_raises(self):
        origins = month_range(date(2010, 1, 1), 20)
        with self.assertRaises(h2o_mod.SelectorFrameError):
            h2o_mod.fold_assignment(origins)


class FrameTest(unittest.TestCase):
    def make_row(self, month: date, value=None):
        from models.cpi import cpi_monthly as cm

        return cm.Row(
            month=month,
            y={"cpi_all": 0.4, "cpi_food": 0.5, "cpi_nonfood": 0.2, "cpi_services": 0.6},
            features={"y_lag1": value, "y_lag2": 9.5, "usd_mm": 1.0, "key_rate": 8.0,
                      "fao_mm": 0.1, "month": month.month},
        )

    def test_nan_is_preserved_not_zero(self):
        records = h2o_mod.frame_records([self.make_row(date(2015, 3, 1), None)], ["y_lag1", "month"])
        self.assertIsNone(records[0]["y_lag1"])
        frame = h2o_mod.to_dataframe(records, ["y_lag1", "month"], ("cpi_all",))
        self.assertTrue(frame["y_lag1"].isna().all())

    def test_month_becomes_categorical_string(self):
        records = h2o_mod.frame_records([self.make_row(date(2015, 3, 1), 1.0)], ["y_lag1", "month"])
        frame = h2o_mod.to_dataframe(records, ["y_lag1", "month"], ("cpi_all",))
        self.assertEqual(frame["month"].tolist(), ["3"])

    def test_digest_is_stable_and_sensitive(self):
        records = h2o_mod.frame_records([self.make_row(date(2015, 3, 1), 1.0), self.make_row(date(2015, 4, 1), 1.1)],
                                        ["y_lag1", "month"])
        first = h2o_mod.frame_digest(records, ["y_lag1", "month"], ("cpi_all",))
        second = h2o_mod.frame_digest(records, ["y_lag1", "month"], ("cpi_all",))
        self.assertEqual(first, second)
        records[0]["y_lag1"] = 2.0
        self.assertNotEqual(first, h2o_mod.frame_digest(records, ["y_lag1", "month"], ("cpi_all",)))


class FrozenConfigTest(unittest.TestCase):
    def test_family_mapping(self):
        self.assertEqual(h2o_mod.family_of("XGBoost_grid_1_AutoML_20261101_1_model_3"), "XGBoost")
        self.assertEqual(h2o_mod.family_of("XRT_1_AutoML_20261101_1"), "DRF")
        self.assertEqual(h2o_mod.family_of("StackedEnsemble_BestOfFamily_AutoML_1"), "StackedEnsemble")
        self.assertEqual(h2o_mod.family_of("GBM_3_AutoML_1"), "GBM")

    def test_whitelist_drops_service_keys(self):
        actual = {
            "ntrees": 50,
            "max_depth": 6,
            "training_frame": "train",
            "model_id": "GBM_1",
            "fold_column": "sel_fold",
            "nfolds": 3,
            "response_column": "cpi_all",
            "ignored_columns": [],
        }
        params, dropped = h2o_mod.frozen_params("GBM", actual)
        self.assertEqual(params, {"ntrees": 50, "max_depth": 6})
        for key in ("training_frame", "model_id", "fold_column", "nfolds", "response_column", "ignored_columns"):
            self.assertIn(key, dropped)

    def test_freeze_and_validate_roundtrip(self):
        frozen = h2o_mod.freeze_config(
            "GBM_1_AutoML_1", {"ntrees": 40, "max_depth": 5, "training_frame": "train"},
            [{"model_id": "GBM_1_AutoML_1", "family": "GBM", "rmse": 0.5}], 20261018,
            "a" * 64, [date(2010, 5, 1), date(2015, 11, 1)], 76, "3.46.0.12", "openjdk 17",
        )
        self.assertEqual(frozen["family"], "GBM")
        self.assertEqual(frozen["params"], {"ntrees": 40, "max_depth": 5})
        self.assertEqual(frozen["selector_rows"], 76)
        self.assertIs(h2o_mod.validate_frozen(frozen), frozen)

    def test_validate_rejects_unknown_family(self):
        with self.assertRaises(SystemExit):
            h2o_mod.validate_frozen({"family": "CatBoost", "params": {}})

    def test_java_major(self):
        self.assertEqual(h2o_mod.java_major('openjdk version "17.0.21.0.3" 2026-10-24'), 17)
        self.assertEqual(h2o_mod.java_major('java version "1.8.0_392"'), 8)
        self.assertEqual(h2o_mod.java_major('openjdk version "11.0.20" 2023-07-18'), 11)
        self.assertIsNone(h2o_mod.java_major(""))

    def test_ensure_supported_java(self):
        h2o_mod.ensure_supported_java('openjdk version "17.0.21" 2026-10-24')
        with self.assertRaises(h2o_mod.H2OUnavailable):
            h2o_mod.ensure_supported_java('openjdk version "21.0.1" 2023-10-17')
        with self.assertRaises(h2o_mod.H2OUnavailable):
            h2o_mod.ensure_supported_java("")


class GateTest(unittest.TestCase):
    def run_gate(self, cells: dict) -> dict:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_cells(root / "cells", cells)
            return h2o_mod.run_gate(root / "cells", root / "gate")

    def promote_cells(self) -> dict:
        return {
            "S_L09:h1": cell(0.60, 0.65, -0.05, 0.01, [-0.09, -0.01]),
            "S_L09:h3": cell(1.50, 1.55, -0.05, 0.20, [-0.15, 0.05]),
            "S_L09:h6": cell(2.60, 2.55, 0.05, 0.40, [-0.10, 0.20]),
            "S_L13:h1": cell(0.70, 0.72, -0.02, 0.30, [-0.06, 0.02]),
            "S_W22:h1": cell(0.80, 0.82, -0.02, 0.30, [-0.06, 0.02]),
        }

    def test_promote_when_all_conditions_hold(self):
        result = self.run_gate(self.promote_cells())
        self.assertEqual(result["decision"], "promote")
        self.assertTrue(all(item["passed"] for item in result["checks"].values()))

    def test_reject_when_h1_not_better(self):
        cells = self.promote_cells()
        cells["S_L09:h1"] = cell(0.70, 0.65, 0.05, 0.01, [0.01, 0.09])
        result = self.run_gate(cells)
        self.assertEqual(result["decision"], "reject")

    def test_iterate_when_not_significant(self):
        cells = self.promote_cells()
        cells["S_L09:h1"] = cell(0.64, 0.65, -0.01, 0.30, [-0.04, 0.02])
        result = self.run_gate(cells)
        self.assertEqual(result["decision"], "iterate")
        self.assertFalse(result["checks"]["2_h1_significant"]["passed"])

    def test_iterate_when_robustness_sign_flips(self):
        cells = self.promote_cells()
        cells["S_L13:h1"] = cell(0.75, 0.72, 0.03, 0.30, [-0.02, 0.08])
        result = self.run_gate(cells)
        self.assertEqual(result["decision"], "iterate")
        self.assertFalse(result["checks"]["4_robustness_sign"]["passed"])

    def test_incomplete_without_cells(self):
        result = self.run_gate({"S_L09:h1": cell(0.60, 0.65, -0.05, 0.01, [-0.09, -0.01])})
        self.assertEqual(result["decision"], "incomplete")


@unittest.skipUnless(os.environ.get("H2O_SMOKE") == "1", "H2O-смоук: H2O_SMOKE=1")
class H2OSmokeTest(unittest.TestCase):
    def test_cluster_starts_and_stops(self):
        with tempfile.TemporaryDirectory() as tmp:
            with h2o_mod.h2o_cluster(Path(tmp)) as h2o:
                frame = h2o.H2OFrame({"x": [1.0, 2.0, 3.0], "y": [1.0, 2.0, 3.0]})
                self.assertEqual(frame.nrows, 3)
                h2o.remove(frame)


if __name__ == "__main__":
    unittest.main(verbosity=2)
