#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_h1_p06_versions` (Task 16 #466): синтетика, без ClickHouse."""

import json
import random
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_h1_p06_versions as pv

MONTHS = [date(2023 + (k // 12), k % 12 + 1, 1) for k in range(45)]


def write_run(path: Path, truth: dict, truth_rt: dict, noise: dict, flags: dict, seed: int) -> None:
    """`predictions_robustness.csv` + `robustness.json` с заданным шумом моделей."""
    rng = random.Random(seed)
    path.mkdir(parents=True, exist_ok=True)
    lines = ["cutoff,model,month,y_pred,t_live,t_live_rt"]
    for cutoff in ("late", "publication"):
        for model in pv.MODELS:
            for month in MONTHS:
                value = truth[month] + rng.gauss(0.0, noise[model])
                lines.append(f"{cutoff},{model},{month.isoformat()},{value!r},{truth[month]!r},{truth_rt[month]!r}")
    (path / "predictions_robustness.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (path / "robustness.json").write_text(json.dumps({"flags": flags, "train_start": "2010-03-01"}), encoding="utf-8")


FLAGS = {"rt_holds": True, "final_holds": True, "ex_tariff_holds": True, "pub_holds": False, "bias_flag_years": []}


class MatrixTest(unittest.TestCase):
    def setUp(self):
        rng = random.Random(1)
        self.truth10 = {m: 0.4 + rng.gauss(0, 0.3) for m in MONTHS}
        self.truth09 = {m: v + rng.gauss(0, 0.02) for m, v in self.truth10.items()}
        self.rt10 = {m: v + rng.gauss(0, 0.05) for m, v in self.truth10.items()}
        self.rt09 = {m: v + rng.gauss(0, 0.05) for m, v in self.truth09.items()}

    def build(self, noise09: dict, flags09: dict = FLAGS) -> dict:
        noise10 = {"combo3": 0.2, "wk_mix_ex": 0.15, "wk_agg_ex": 0.15, "wk_mix_d": 0.17, "live_cb_wk": 0.19}
        with tempfile.TemporaryDirectory() as tmp:
            d10, d09 = Path(tmp) / "a", Path(tmp) / "b"
            write_run(d10, self.truth10, self.rt10, noise10, FLAGS, 2)
            write_run(d09, self.truth09, self.rt09, noise09, flags09, 3)
            return pv.build({"2010": d10, "2009": d09}, Path(tmp) / "none")

    def test_four_truths_and_matrix_shape(self):
        result = self.build({"combo3": 0.2, "wk_mix_ex": 0.15, "wk_agg_ex": 0.15, "wk_mix_d": 0.17, "live_cb_wk": 0.19})
        cell = result["matrix_late"]["wk_mix_ex"]
        self.assertEqual(set(cell), {"2010", "2009"})
        self.assertEqual(set(cell["2010"]), {"final@2010", "rt@2010", "final@2009", "rt@2009"})
        self.assertEqual(cell["2009"]["rt@2010"]["n"], 45)
        self.assertIn("final@2009 − final@2010", result["truth_gaps"])

    def test_no_switch_when_not_better(self):
        result = self.build({"combo3": 0.2, "wk_mix_ex": 0.15, "wk_agg_ex": 0.15, "wk_mix_d": 0.17, "live_cb_wk": 0.19})
        self.assertFalse(result["switch_rule"]["recommend_2009"])

    def test_switch_when_clearly_better(self):
        result = self.build({"combo3": 0.2, "wk_mix_ex": 0.03, "wk_agg_ex": 0.03, "wk_mix_d": 0.17, "live_cb_wk": 0.19})
        rule = result["switch_rule"]
        self.assertTrue(rule["c1_better_on_both_final_truths"])
        self.assertTrue(rule["c1_better_on_both_rt_truths"])
        self.assertTrue(rule["recommend_2009"])

    def test_flag_degradation_blocks_switch(self):
        flags = dict(FLAGS, rt_holds=False)
        result = self.build({"combo3": 0.2, "wk_mix_ex": 0.03, "wk_agg_ex": 0.03, "wk_mix_d": 0.17, "live_cb_wk": 0.19}, flags)
        self.assertEqual(result["switch_rule"]["degraded_flags"], ["rt_holds"])
        self.assertFalse(result["switch_rule"]["recommend_2009"])

    def test_bias_flag_blocks_switch(self):
        flags = dict(FLAGS, bias_flag_years=["2025"])
        result = self.build({"combo3": 0.2, "wk_mix_ex": 0.03, "wk_agg_ex": 0.03, "wk_mix_d": 0.17, "live_cb_wk": 0.19}, flags)
        self.assertFalse(result["switch_rule"]["c2_flags_not_degraded"])


class RuleTest(unittest.TestCase):
    def test_better_requires_negative_delta_and_p(self):
        self.assertTrue(pv.better({"delta": -0.01, "dm_p": 0.01, "delta_ci": [-0.02, 0.001]}))
        self.assertFalse(pv.better({"delta": -0.01, "dm_p": 0.01, "delta_ci": [-0.02, 0.001]}, need_ci=True))
        self.assertFalse(pv.better({"delta": 0.01, "dm_p": 0.01, "delta_ci": [0.0, 0.02]}))
        self.assertFalse(pv.better({"delta": -0.01, "dm_p": 0.2, "delta_ci": [-0.02, -0.001]}))


if __name__ == "__main__":
    unittest.main()
