#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_groups_panel` (Task 1 #421): без ClickHouse, сети, R и архива."""

import math
import sys
import unittest
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_groups_panel as g


def frame(rows: list) -> pd.DataFrame:
    """Словарь: (код, название, уровень, предки уровней 1…6)."""
    data = []
    for code, name, level, ancestors in rows:
        row = {"код_товара_услуги": code, "наименование_товара_услуги": name, "уровень": level}
        for i in range(1, 8):
            row[f"уровень_{i}"] = ancestors[i - 1] if i - 1 < len(ancestors) else "<==>"
        data.append(row)
    return pd.DataFrame(data)


DICT = g.dictionary_from_frame(frame([
    ("1", "Все товары и услуги", 1, ["1"]),
    ("9000", "Услуги", 2, ["1", "9000"]),
    ("10", "Мясопродукты", 3, ["1", "6", "10"]),
    ("111", "Говядина, кг", 7, ["1", "6", "10", "<==>", "<==>", "<==>"]),
    ("112", "Свинина, кг", 7, ["1", "6", "10", "<==>", "<==>", "<==>"]),
    ("88", "Услуги парикмахерских", 4, ["1", "9000", "<==>", "88"]),
    ("881", "Стрижка, услуга", 7, ["1", "9000", "<==>", "88", "<==>", "<==>"]),
]))


class NormalizeTest(unittest.TestCase):
    def test_variants_collapse(self):
        self.assertEqual(g.normalize_name("  Масло  «сливочное» , кг. "), "масло сливочное, кг")
        self.assertEqual(g.normalize_name("Ёлки ( искусственные )"), "елки (искусственные)")
        self.assertEqual(g.normalize_name(float("nan")), "")

    def test_product_index_collision_raises(self):
        with self.assertRaises(ValueError):
            g.product_index({"ipc31074/Сыр": {}, "ipc31074/сыр ": {}})


class DictionaryTest(unittest.TestCase):
    def test_leaves_by_ancestor(self):
        self.assertEqual(sorted(DICT.leaves_by_ancestor["10"]), ["111", "112"])
        self.assertEqual(DICT.leaves_by_ancestor["9000"], ["881"])
        self.assertEqual(DICT.levels["88"], 4)

    def test_is_within(self):
        self.assertTrue(g.is_within(DICT, "88", "9000"))
        self.assertFalse(g.is_within(DICT, "10", "9000"))


class MappingTest(unittest.TestCase):
    def setUp(self):
        self.dicts = {2023: DICT, 2024: DICT}
        self.weights = {2024: {"111": 0.03, "112": 0.01, "881": 0.02}}

    def test_direct_by_group_name(self):
        products = {"мясопродукты": "ipc31074/Мясопродукты", "говядина, кг": "ipc31074/Говядина, кг"}
        group = g.map_group("10", self.dicts, self.weights, products)
        self.assertEqual(group.method, "direct")
        self.assertEqual(group.chain, ("ipc31074/Мясопродукты",))
        self.assertEqual(group.coverage_2024, 1.0)
        self.assertEqual(list(group.leaves_by_year[2024]), ["111"])

    def test_aggregated_coverage_by_leaf_weight(self):
        products = {"говядина, кг": "ipc31074/Говядина, кг"}
        group = g.map_group("10", self.dicts, self.weights, products)
        self.assertEqual(group.method, "aggregated")
        self.assertAlmostEqual(group.coverage_2024, 0.75)

    def test_missing(self):
        group = g.map_group("10", self.dicts, self.weights, {})
        self.assertEqual(group.method, "missing")
        self.assertEqual(group.coverage_2024, 0.0)

    def test_alias_appended_after_primary(self):
        original = dict(g.MANUAL_ALIASES)
        try:
            g.MANUAL_ALIASES["10"] = (("Мясо и птица", "переименование"),)
            products = {"мясопродукты": "ipc31074/Мясопродукты", "мясо и птица": "ipc31074/Мясо и птица"}
            group = g.map_group("10", self.dicts, self.weights, products)
            self.assertEqual(group.chain, ("ipc31074/Мясопродукты", "ipc31074/Мясо и птица"))
            self.assertEqual(group.manual, ["Мясо и птица"])
        finally:
            g.MANUAL_ALIASES.clear()
            g.MANUAL_ALIASES.update(original)

    def test_manual_aliases_have_reasons(self):
        for code, aliases in g.MANUAL_ALIASES.items():
            for alias, reason in aliases:
                self.assertTrue(alias.strip() and reason.strip(), code)

    def test_composition_changes(self):
        group = g.GroupMap(code="10", name="", method="aggregated",
                           leaves_by_year={2022: {"111": "a"}, 2023: {"111": "a"}, 2024: {"111": "a", "112": "b"}})
        self.assertEqual(g.composition_changes(group), 1)


class AggregationTest(unittest.TestCase):
    def setUp(self):
        self.group = g.GroupMap(code="10", name="", method="aggregated",
                                leaves_by_year={2024: {"111": "s1", "112": "s2"}})
        self.weights = {2024: {"111": 0.75, "112": 0.25}}
        self.months = [date(2024, m, 1) for m in (1, 2, 3)]

    def test_laspeyres_from_december(self):
        mm = {"s1": {m: 1.0 for m in self.months}, "s2": {m: 4.0 for m in self.months}}
        out = g.aggregated_mm(self.group, mm, self.weights, self.months)
        self.assertAlmostEqual(out[date(2024, 1, 1)], 0.75 * 1.0 + 0.25 * 4.0)
        # февраль: Σw·L(фев) / Σw·L(янв) − 1, веса дрейфуют за ценами
        l_jan = 0.75 * 1.01 + 0.25 * 1.04
        l_feb = 0.75 * 1.01 ** 2 + 0.25 * 1.04 ** 2
        self.assertAlmostEqual(out[date(2024, 2, 1)], (l_feb / l_jan - 1) * 100)

    def test_leaf_with_gap_dropped_for_rest_of_year(self):
        mm = {"s1": {m: 1.0 for m in self.months}, "s2": {date(2024, 1, 1): 4.0, date(2024, 3, 1): 4.0}}
        out = g.aggregated_mm(self.group, mm, self.weights, self.months)
        self.assertAlmostEqual(out[date(2024, 2, 1)], 1.0)
        self.assertAlmostEqual(out[date(2024, 3, 1)], 1.0)

    def test_no_leaves_no_value(self):
        out = g.aggregated_mm(self.group, {}, self.weights, self.months)
        self.assertEqual(out, {})

    def test_chain_priority_then_leaves(self):
        group = g.GroupMap(code="10", name="", method="direct", chain=("p", "alias"),
                           leaves_by_year={2024: {"111": "s1"}})
        mm = {
            "p": {date(2024, 3, 1): 0.5},
            "alias": {date(2024, 2, 1): 0.3, date(2024, 3, 1): 9.9},
            "s1": {m: 1.0 for m in self.months},
        }
        out, sources = g.group_nsa(group, mm, {2024: {"111": 1.0}}, self.months)
        self.assertEqual(out[date(2024, 3, 1)], 0.5)
        self.assertEqual(sources[date(2024, 3, 1)], "chain:0")
        self.assertEqual(out[date(2024, 2, 1)], 0.3)
        self.assertEqual(sources[date(2024, 2, 1)], "chain:1")
        self.assertAlmostEqual(out[date(2024, 1, 1)], 1.0)
        self.assertEqual(sources[date(2024, 1, 1)], "leaves")

    def test_year_key_fallback(self):
        self.assertEqual(g.year_key({2016: 1, 2018: 2}, 2017), 2016)
        self.assertEqual(g.year_key({2016: 1}, 2012), 2016)


class SaTest(unittest.TestCase):
    def test_contiguous_tail(self):
        mm = {date(2020, 1, 1): 1.0, date(2020, 3, 1): 1.0, date(2020, 4, 1): 1.0}
        self.assertEqual(g.contiguous_tail(mm, date(2020, 4, 1)), [date(2020, 3, 1), date(2020, 4, 1)])
        self.assertEqual(g.contiguous_tail(mm, date(2020, 5, 1)), [])

    def test_short_series_fails_without_runner(self):
        mm = {m: 0.5 for m in g.month_grid(date(2024, 1, 1), date(2024, 12, 1))}

        def factory(start):
            raise AssertionError("runner не должен вызываться")

        out = g.group_sa(mm, date(2024, 12, 1), factory, asof=False)
        self.assertEqual(out["status"], "sa_failed")
        self.assertEqual(out["full"], {})

    def test_runner_gets_series_start_and_nan_dropped(self):
        grid = g.month_grid(date(2015, 3, 1), date(2019, 6, 1))
        mm = {m: 0.5 for m in grid}
        calls = {}

        def factory(start):
            calls["start"] = start

            def run(levels, first_rt):
                calls["first_rt"] = first_rt
                calls["n"] = len(levels)
                return [(float("nan"), float("nan"))] + [(0.4, 0.3)] * (len(levels) - 1)

            return run

        out = g.group_sa(mm, date(2019, 6, 1), factory, asof=True)
        self.assertEqual(calls["start"], date(2015, 3, 1))
        self.assertEqual(calls["n"], len(grid))
        self.assertEqual(calls["first_rt"], grid.index(date(2019, 1, 1)) + 1)
        self.assertNotIn(grid[0], out["full"])
        self.assertEqual(out["status"], "ok")

    def test_asof_off_runs_no_rt(self):
        grid = g.month_grid(date(2015, 1, 1), date(2019, 6, 1))
        seen = {}

        def factory(start):
            def run(levels, first_rt):
                seen["first_rt"] = first_rt
                return [(0.1, float("nan"))] * len(levels)

            return run

        out = g.group_sa({m: 0.2 for m in grid}, grid[-1], factory, asof=False)
        self.assertEqual(seen["first_rt"], len(grid) + 1)
        self.assertEqual(out["rt"], {})


class MetricsTest(unittest.TestCase):
    def test_compare_window_and_bias(self):
        months = g.month_grid(date(2020, 1, 1), date(2020, 6, 1))
        ref = {m: float(i) for i, m in enumerate(months)}
        ours = {m: v + 0.1 for m, v in ref.items()}
        out = g.compare(ours, ref, date(2020, 2, 1), date(2020, 5, 1))
        self.assertEqual(out["n"], 4)
        self.assertAlmostEqual(out["rmse"], 0.1)
        self.assertAlmostEqual(out["bias"], 0.1)
        self.assertAlmostEqual(out["corr"], 1.0)

    def test_weighted_rmse_skips_empty(self):
        rmse, weight = g.weighted_rmse(
            {"a": {"n": 3, "rmse": 0.1}, "b": {"n": 3, "rmse": 0.3}, "c": {"n": 0, "rmse": float("nan")}},
            {"a": 3.0, "b": 1.0, "c": 5.0},
        )
        self.assertAlmostEqual(rmse, math.sqrt((3 * 0.01 + 0.09) / 4))
        self.assertEqual(weight, 4.0)

    def test_decisions_thresholds(self):
        ok = g.decisions(0.85, 0.05, 0.15, 0.90, 0.10)
        self.assertTrue(all(ok.values()))
        bad = g.decisions(0.849, 0.051, 0.151, 0.95, 0.101)
        self.assertFalse(any(bad.values()))
        self.assertFalse(g.decisions(0.80, 0.0, 0.0, 1.0, 0.01)["assembly_ok"])

    def test_assembly_exact_components_give_zero_error(self):
        months = g.month_grid(g.PANEL_START, date(2024, 7, 1))
        rng = np.random.default_rng(1)
        comp = {c: {m: float(rng.normal(0.4, 0.2)) for m in months} for c in ("a", "b")}
        weights = {year: {"a": 0.6, "b": 0.3} for year in range(2016, 2025)}
        diffs = g.log_diff_frame(comp, months)
        agg = {}
        for horizon in (1, 2, 3, 6):
            cum = (np.exp(diffs.rolling(horizon).sum()) - 1) * 100
            agg[horizon] = (cum["a"] * 0.6 + cum["b"] * 0.3) / 0.9
        target = pd.DataFrame(agg)
        test_months = g.month_grid(date(2019, 1, 1), date(2024, 7, 1))
        out = g.assembly(comp, target, weights, test_months)
        for horizon in (1, 2, 3, 6):
            self.assertEqual(out["test_67"][horizon]["n"], 67)
            self.assertLess(out["test_67"][horizon]["rmse"], 1e-9)
            self.assertAlmostEqual(out["test_67"][horizon]["coverage_mean"], 0.9)
        self.assertEqual(out["normal_54"][1]["n"], 54)


if __name__ == "__main__":
    unittest.main()
