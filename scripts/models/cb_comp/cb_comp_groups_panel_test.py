#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_groups_panel` (Task 1 #421): без ClickHouse, сети, R и архива."""

import math
import sys
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

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

    def test_series_ended_early_keeps_history(self):
        grid = g.month_grid(date(2015, 1, 1), date(2024, 10, 1))
        seen = {}

        def factory(start):
            def run(levels, first_rt):
                seen["n"] = len(levels)
                return [(0.1, float("nan"))] * len(levels)

            return run

        out = g.group_sa({m: 0.2 for m in grid}, date(2026, 9, 1), factory, asof=False)
        self.assertEqual(out["status"], "ok")
        self.assertEqual(seen["n"], len(grid))

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

    def test_fit_window_bounds_series(self):
        """Спека §14: окно подгонки задаёт старт ряда, а не только его конец."""
        grid = g.month_grid(date(2009, 1, 1), date(2024, 8, 1))
        seen = {}

        def factory(start):
            def run(levels, first_rt):
                seen["start"], seen["n"] = start, len(levels)
                return [(0.1, float("nan"))] * len(levels)

            return run

        mm = {m: 0.2 for m in grid}
        g.group_sa(mm, date(2024, 8, 1), factory, asof=False, first=date(2009, 1, 1))
        self.assertEqual(seen["start"], date(2009, 1, 1))
        self.assertEqual(seen["n"], len(grid))
        g.group_sa(mm, date(2024, 8, 1), factory, asof=False, first=date(2010, 1, 1))
        self.assertEqual(seen["start"], date(2010, 1, 1))
        self.assertEqual(seen["n"], len(grid) - 12)
        g.group_sa(mm, date(2024, 8, 1), factory, asof=False)
        self.assertEqual(seen["start"], g.PANEL_START)


class ReferenceWindowTest(unittest.TestCase):
    """Спека §14: окна эталонов и загрузка панели fst (без R и архива)."""

    def test_reference_windows(self):
        self.assertEqual(g.REFERENCE_WINDOWS["csv"], (date(2010, 1, 1), date(2024, 9, 1)))
        self.assertEqual(g.REFERENCE_WINDOWS["fst"], (date(2009, 1, 1), date(2024, 8, 1)))
        self.assertEqual(g.FIT_WINDOWS, ("live", "csv", "fst"))
        self.assertEqual(g.DATA_START, date(2009, 1, 1))

    def test_fst_reference_converts_log_to_pct(self):
        cpi = pd.DataFrame({
            "date": [pd.Timestamp("2009-01-01"), pd.Timestamp("2009-02-01"), pd.Timestamp("2009-02-01")],
            "code": ["10", "10", "11"],
            "kind": ["diff", "diff", "base_index"],
            "value": [float("nan"), math.log(1.015), 100.0],
        })
        with mock.patch.object(g.bu, "ensure_panel_csv", return_value=(cpi, None, None)):
            ref = g.load_fst_reference(Path("/nonexistent"))
        self.assertEqual(set(ref["10"]), {date(2009, 2, 1)})
        self.assertAlmostEqual(ref["10"][date(2009, 2, 1)], 1.5)
        self.assertNotIn("11", ref)


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
        ok = g.decisions(0.85, 0.05, 0.15, 0.90, 0.10, 0.14)
        self.assertTrue(all(ok.values()))
        bad = g.decisions(0.849, 0.051, 0.151, 0.95, 0.101, 0.16)
        self.assertFalse(any(bad.values()))
        self.assertFalse(g.decisions(0.80, 0.0, 0.0, 1.0, 0.01)["assembly_ok"])
        # Q3-B — диагностика: без числа решение None, а не False (спека §14).
        self.assertIsNone(g.decisions(0.85, 0.05, 0.15, 0.90, 0.10)["sa_fst_ok"])
        self.assertTrue(g.decisions(0.85, 0.05, 0.15, 0.90, 0.10, 0.15)["sa_fst_ok"])

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


class AmendmentTest(unittest.TestCase):
    """Поправка §11: выбор спецификации X-13 только по данным ≤ 2018-12."""

    def fake_factory_for(self, offsets: dict, aiccs: dict, seen: list):
        """Фабрика: SA = NSA + сдвиг кандидата; AICc — из таблицы; None — не сошлась."""

        def factory_for(spec):
            def factory(start):
                class Runner:
                    aicc = float("nan")

                    def __call__(self, levels, first_rt):
                        seen.append((spec, start, len(levels)))
                        if offsets[spec] is None:
                            return [(float("nan"), float("nan"))] * len(levels)
                        self.aicc = aiccs.get(spec, float("nan"))
                        out = [(float("nan"), float("nan"))]
                        for prev, cur in zip(levels, levels[1:]):
                            out.append(((cur / prev - 1) * 100 + offsets[spec], float("nan")))
                        return out

                runner = Runner()
                factory.runners.append(runner)
                return runner

            factory.runners = []
            return factory

        return factory_for

    def test_constants_match_spec(self):
        self.assertEqual(g.AMENDED_GROUPS, ("9540", "9950", "9218", "9550"))
        self.assertEqual(g.AICC_GROUPS, frozenset({"9218", "9550"}))
        self.assertEqual(g.SPEC_CANDIDATES, ("C0", "C1", "C2", "C3", "C4"))
        self.assertEqual(g.SELECTION_END, date(2018, 12, 1))
        self.assertEqual(g.SELECTION_FIRST["9550"], date(2009, 1, 1))

    def test_choose_spec_min_tie_and_nan(self):
        self.assertEqual(g.choose_spec({"C0": 0.3, "C1": 0.2, "C2": float("nan")}), "C1")
        self.assertEqual(g.choose_spec({"C3": 0.2, "C1": 0.2 + 1e-7}), "C1")
        self.assertEqual(g.choose_spec({"C0": float("nan")}), "")

    def test_select_spec_respects_window_start(self):
        """Спека §14: 9550 выбирается по AICc на 2009-01…2018-12."""
        months = g.month_grid(date(2009, 1, 1), date(2019, 6, 1))
        mm = {m: 0.3 for m in months}
        seen = []
        factory_for = self.fake_factory_for(
            {spec: 0.2 for spec in g.SPEC_CANDIDATES},
            {spec: float(i) for i, spec in enumerate(g.SPEC_CANDIDATES)},
            seen,
        )
        out = g.select_spec("9550", mm, {}, factory_for=factory_for, first=date(2009, 1, 1))
        self.assertEqual(out["criterion"], "aicc")
        self.assertEqual(out["chosen"], "C0")
        self.assertEqual(seen[0][1], date(2009, 1, 1))
        self.assertEqual(seen[0][2], len(g.month_grid(date(2009, 1, 1), date(2018, 12, 1))))

    def test_rmse_selection_uses_only_data_to_2018(self):
        months = g.month_grid(date(2010, 1, 1), date(2024, 7, 1))
        mm = {m: 0.5 for m in months}
        ref = {m: 0.5 + 0.2 for m in g.month_grid(date(2016, 1, 1), date(2018, 12, 1))}
        seen = []
        factory_for = self.fake_factory_for({"C0": 0.0, "C1": 0.2, "C2": 0.1, "C3": None, "C4": 0.3}, {}, seen)
        out = g.select_spec("9540", mm, ref, factory_for)
        self.assertEqual(out["chosen"], "C1")
        self.assertEqual(out["criterion"], "rmse_mom_sa_2016_2018")
        self.assertEqual(out["candidates"]["C3"]["status"], "sa_failed")
        expected_len = len(g.month_grid(date(2010, 1, 1), date(2018, 12, 1)))
        self.assertTrue(all(n == expected_len for _, _, n in seen))

    def test_future_change_does_not_move_choice(self):
        months = g.month_grid(date(2010, 1, 1), date(2024, 7, 1))
        ref = {m: 0.7 for m in g.month_grid(date(2016, 1, 1), date(2018, 12, 1))}
        offsets = {"C0": 0.0, "C1": 0.2, "C2": 0.1, "C3": 0.15, "C4": 0.3}
        base = g.select_spec("9950", {m: 0.5 for m in months}, ref, self.fake_factory_for(offsets, {}, []))
        changed = {m: (9.0 if m.year >= 2019 else 0.5) for m in months}
        moved = g.select_spec("9950", changed, ref, self.fake_factory_for(offsets, {}, []))
        self.assertEqual(base["chosen"], moved["chosen"])
        self.assertEqual(repr(base["candidates"]), repr(moved["candidates"]))

    def test_aicc_selection_for_tariff(self):
        months = g.month_grid(date(2010, 1, 1), date(2024, 7, 1))
        offsets = {"C0": None, "C1": 0.0, "C2": 0.0, "C3": 0.0, "C4": 0.0}
        aiccs = {"C1": 120.0, "C2": 95.5, "C3": 101.0, "C4": 95.5}
        out = g.select_spec("9218", {m: 0.5 for m in months}, {}, self.fake_factory_for(offsets, aiccs, []))
        self.assertEqual(out["criterion"], "aicc")
        self.assertEqual(out["chosen"], "C2")

    def test_none_converged_gives_empty_choice(self):
        months = g.month_grid(date(2010, 1, 1), date(2024, 7, 1))
        offsets = {spec: None for spec in g.SPEC_CANDIDATES}
        out = g.select_spec("9218", {m: 0.5 for m in months}, {}, self.fake_factory_for(offsets, {}, []))
        self.assertEqual(out["chosen"], "")

    def test_unknown_spec_rejected(self):
        with self.assertRaises(ValueError):
            g.SpecRunner("C9", date(2010, 1, 1))


class FitRoutingTest(unittest.TestCase):
    """Маршрутизация окна → ряд и старт подгонки (ревью #431)."""

    def groups(self):
        return {
            "9550": g.GroupMap(code="9550", name="Экскурсии", method="direct", chain=("s",)),
            "10": g.GroupMap(code="10", name="Мясо", method="aggregated"),
        }

    def test_fit_window_start(self):
        self.assertEqual(g.fit_window_start("9550", date(2010, 1, 1), g.SELECTION_FIRST), date(2009, 1, 1))
        self.assertEqual(g.fit_window_start("9550", date(2009, 1, 1), g.SELECTION_FIRST), date(2009, 1, 1))
        self.assertEqual(g.fit_window_start("10", date(2010, 1, 1), g.SELECTION_FIRST), date(2010, 1, 1))

    def test_fit_series_map_history_only_for_direct(self):
        groups = self.groups()
        months = g.month_grid(date(2010, 1, 1), date(2010, 3, 1))
        nsa = {code: {m: 1.0 for m in months} for code in groups}
        head = g.month_grid(date(2009, 1, 1), date(2009, 12, 1))
        fit_mm = {"s": {**{m: 2.0 for m in head}, **{m: 1.0 for m in months}}}
        out = g.fit_series_map(list(groups), groups, nsa, fit_mm, {},
                               date(2010, 1, 1), date(2010, 3, 1), g.SELECTION_FIRST)
        self.assertIn(date(2009, 1, 1), out["9550"])
        self.assertEqual(out["9550"][date(2009, 1, 1)], 2.0)
        self.assertEqual(out["9550"][date(2010, 1, 1)], 1.0)
        self.assertNotIn(date(2009, 1, 1), out["10"])

    def test_group_nsa_no_leaves_fallback_before_panel_start(self):
        group = g.GroupMap(code="10", name="Мясо", method="aggregated",
                           leaves_by_year={2010: {"111": "leaf"}})
        months = [date(2009, 1, 1), date(2010, 1, 1)]
        out, sources = g.group_nsa(group, {"leaf": {m: 1.0 for m in months}}, {2010: {"111": 1.0}}, months)
        self.assertNotIn(date(2009, 1, 1), out)
        self.assertEqual(sources.get(date(2010, 1, 1)), "leaves")

    def test_select_spec_reports_window_and_tie(self):
        months = g.month_grid(date(2009, 1, 1), date(2019, 6, 1))
        seen = []
        factory_for = self._factory({"C0": -734.020286966, "C1": -734.020286961,
                                     "C2": -734.020286961, "C3": -734.020286966,
                                     "C4": -734.020286961}, seen)
        out = g.select_spec("9550", {m: 0.3 for m in months}, {}, factory_for=factory_for, first=date(2009, 1, 1))
        self.assertEqual(out["window"], ["2009-01-01", "2018-12-01"])
        self.assertEqual(out["chosen"], "C0")
        self.assertEqual(out["tied"], ["C0", "C1", "C2", "C3", "C4"])
        self.assertLess(out["spread"], g.SPEC_TIE)

    def _factory(self, aiccs, seen):
        def factory_for(spec):
            def factory(start):
                class Runner:
                    aicc = float("nan")

                    def __call__(self, levels, first_rt):
                        seen.append((spec, start, len(levels)))
                        self.aicc = aiccs[spec]
                        return [(0.1, float("nan"))] * len(levels)

                runner = Runner()
                factory.runners.append(runner)
                return runner

            factory.runners = []
            return factory

        return factory_for


if __name__ == "__main__":
    unittest.main()
