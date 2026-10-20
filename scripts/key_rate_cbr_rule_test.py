#!/usr/bin/env python3
"""Офлайн-тесты `scripts/key_rate_cbr_rule.py`.

Запуск из корня репозитория (pytest не нужен):

    make key-rate-cbr-rule-test

или:

    uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn \
        --with scipy --python 3.12 python3 scripts/key_rate_cbr_rule_test.py -v

Набор офлайн: ClickHouse и сеть не нужны. Живой прогон —
`ML_LOG=1 make key-rate-cbr-rule`.
"""

import csv
import json
import math
import sys
import tempfile
import unittest
import unittest.mock
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import key_rate_cbr_rule as cr
import key_rate_model as krm


def make_decision(day: date, rate_before: float = 10.0, step_bp: int = 0):
    rate_after = rate_before + step_bp / 100.0
    return krm.Decision(
        meeting_date=day,
        rate_before=rate_before,
        rate_after=rate_after,
        step_bp=step_bp,
        y3=(step_bp > 0) - (step_bp < 0),
    )


def make_row(
    day: date,
    rate_before: float = 10.0,
    step_bp: int = 0,
    e3_core=6.0,
    e3_headline=8.0,
    e3_consensus=7.0,
    gdp_gap=0.0,
    ibk_gap=0.0,
    r_star_trend=3.0,
    e3_core_m3=None,
    e3_headline_m3=None,
):
    return cr.CbrRow(
        decision=make_decision(day, rate_before, step_bp),
        pi_saar=5.0,
        gap=0.0,
        e3_core=e3_core,
        e3_headline_adaptive=e3_headline,
        e3_headline_consensus=e3_consensus,
        gdp_gap=gdp_gap,
        ibk_gap=ibk_gap,
        r_star_trend=r_star_trend,
        e3_core_m3=e3_core_m3,
        e3_headline_m3=e3_headline_m3,
    )


def make_prediction(day, method, step_bp, actual_step_bp, probs=None):
    return krm.Prediction(
        meeting_date=day,
        actual_step_bp=actual_step_bp,
        actual_y3=(actual_step_bp > 0) - (actual_step_bp < 0),
        method=method,
        window="expanding",
        step_bp=step_bp,
        probs=probs,
    )


class AnnualizationTest(unittest.TestCase):
    def test_three_months(self):
        self.assertAlmostEqual(
            cr.annualize_trailing([0.4, 0.5, 0.3]),
            4.906604463006348,
            places=12,
        )

    def test_one_month(self):
        self.assertAlmostEqual(
            cr.annualize_trailing([0.4]),
            ((1.004**12) - 1.0) * 100.0,
            places=12,
        )

    def test_empty(self):
        self.assertIsNone(cr.annualize_trailing([]))

    def test_non_finite_raises(self):
        with self.assertRaises(ValueError):
            cr.annualize_trailing([0.4, float("nan")])


class AvailabilityTest(unittest.TestCase):
    def test_gdp_q1(self):
        self.assertEqual(cr.available_from_gdp(date(2012, 4, 1)), date(2012, 6, 30))

    def test_gdp_q4(self):
        self.assertEqual(cr.available_from_gdp(date(2013, 1, 1)), date(2013, 3, 31))

    def test_asof_window_excludes_future(self):
        def available(period):
            return period + timedelta(days=10)

        points = [
            (date(2026, 1, 1), 1.0),
            (date(2026, 2, 1), 2.0),
            (date(2026, 3, 1), 3.0),
        ]
        self.assertEqual(
            cr.asof_window(points, available, date(2026, 2, 5), 3),
            [1.0],
        )

    def test_asof_window_last_n(self):
        def available(period):
            return period

        points = [
            (date(2026, 1, 1), 1.0),
            (date(2026, 2, 1), 2.0),
            (date(2026, 3, 1), 3.0),
            (date(2026, 4, 1), 4.0),
        ]
        self.assertEqual(
            cr.asof_window(points, available, date(2026, 4, 1), 2),
            [3.0, 4.0],
        )

    def test_adaptive_uses_only_available(self):
        points = [
            (date(2026, 1, 31), 0.4),
            (date(2026, 2, 28), 0.5),
            (date(2026, 3, 31), 0.3),
        ]
        # На 2026-03-15 доступен только январь (январь — конец февраля;
        # февраль — конец марта, уже позже среза).
        result = cr.e3_adaptive(points, date(2026, 3, 15))
        self.assertAlmostEqual(result, cr.annualize_trailing([0.4]), places=12)

    def test_yoy_requires_twelve(self):
        points = [(date(2026, m, 28), 0.4) for m in range(1, 13)]
        self.assertIsNotNone(cr.headline_yoy(points, date(2027, 1, 31)))
        self.assertIsNone(cr.headline_yoy(points[:-1], date(2027, 1, 31)))

    def test_adaptive_yoy_compounds_twelve(self):
        points = [(date(2026, m, 28), 0.4) for m in range(1, 13)]
        expected = ((1.004**12) - 1.0) * 100.0
        self.assertAlmostEqual(
            cr.e3_adaptive_yoy(points, date(2027, 1, 31)), expected, places=12
        )
        self.assertIsNone(cr.e3_adaptive_yoy(points[:-1], date(2027, 1, 31)))


class HpGapTest(unittest.TestCase):
    def test_min_points(self):
        points = [(date(2020, 1, 1) + timedelta(days=90 * i), 100.0 + i) for i in range(7)]
        self.assertIsNone(
            cr.hp_gap(
                points,
                lambda period: period,
                date(2022, 1, 1),
                lamb=1600.0,
                min_points=8,
                percent=True,
            )
        )

    def test_linear_series_near_zero(self):
        points = [(date(2015, 1, 1) + timedelta(days=90 * i), 100.0 + 3.0 * i) for i in range(20)]
        gap = cr.hp_gap(
            points,
            lambda period: period,
            date(2020, 1, 1),
            lamb=1600.0,
            min_points=8,
            percent=True,
        )
        self.assertIsNotNone(gap)
        self.assertAlmostEqual(gap, 0.0, places=6)

    def test_hump_is_positive(self):
        values = [100.0] * 18 + [110.0, 120.0]
        points = [(date(2015, 1, 1) + timedelta(days=90 * i), value) for i, value in enumerate(values)]
        gap = cr.hp_gap(
            points,
            lambda period: period,
            date(2020, 1, 1),
            lamb=1600.0,
            min_points=8,
            percent=True,
        )
        self.assertIsNotNone(gap)
        self.assertGreater(gap, 0.0)

    def test_units_without_percent(self):
        values = [100.0 + i for i in range(20)]
        points = [(date(2015, 1, 1) + timedelta(days=30 * i), value) for i, value in enumerate(values)]
        gap = cr.hp_gap(
            points,
            lambda period: period,
            date(2020, 1, 1),
            lamb=14400.0,
            min_points=8,
            percent=False,
        )
        self.assertIsNotNone(gap)
        self.assertLess(abs(gap), 1e-6)


class RuleComponentsTest(unittest.TestCase):
    def test_base_exact(self):
        components = cr.rule_components(make_row(date(2026, 1, 1)), cr.BASE_SPEC)
        self.assertAlmostEqual(components.e3_mp, 6.5, places=12)
        self.assertAlmostEqual(components.target_level, 10.8125, places=12)
        self.assertAlmostEqual(components.step_bp, 81.25, places=12)

    def test_hard(self):
        spec = cr.RuleSpec("cbr_rule_hard", gamma2=1.9)
        components = cr.rule_components(make_row(date(2026, 1, 1)), spec)
        self.assertAlmostEqual(components.step_bp, 106.25, places=12)

    def test_ibk_gap(self):
        spec = cr.RuleSpec("cbr_rule_ibk", gap_source="ibk")
        components = cr.rule_components(
            make_row(date(2026, 1, 1), gdp_gap=0.0, ibk_gap=2.0), spec
        )
        self.assertAlmostEqual(components.gap, 2.0, places=12)
        self.assertAlmostEqual(components.step_bp, 106.25, places=12)

    def test_consensus_headline(self):
        spec = cr.RuleSpec("cbr_rule_consensus", e3_source="consensus")
        components = cr.rule_components(
            make_row(date(2026, 1, 1), e3_headline=8.0, e3_consensus=9.0), spec
        )
        self.assertAlmostEqual(components.e3_headline, 9.0, places=12)
        self.assertAlmostEqual(components.e3_mp, 6.75, places=12)
        self.assertAlmostEqual(components.step_bp, 96.875, places=12)

    def test_m3_uses_m3_fields(self):
        spec = cr.RuleSpec("cbr_rule_m3", e3_source="m3")
        components = cr.rule_components(
            make_row(
                date(2026, 1, 1),
                e3_core=6.0,
                e3_headline=8.0,
                e3_core_m3=5.0,
                e3_headline_m3=7.0,
            ),
            spec,
        )
        self.assertAlmostEqual(components.e3_core, 5.0, places=12)
        self.assertAlmostEqual(components.e3_headline, 7.0, places=12)
        self.assertAlmostEqual(components.e3_mp, 5.5, places=12)
        self.assertAlmostEqual(components.step_bp, 18.75, places=12)

    def test_rtrend(self):
        spec = cr.RuleSpec("cbr_rule_rtrend", r_star=None)
        components = cr.rule_components(
            make_row(date(2026, 1, 1), r_star_trend=4.0), spec
        )
        self.assertAlmostEqual(components.r_star, 4.0, places=12)
        self.assertAlmostEqual(components.step_bp, 106.25, places=12)

    def test_missing_feature_raises(self):
        with self.assertRaises(krm.DegenerateWindowError):
            cr.rule_components(make_row(date(2026, 1, 1), e3_core=None), cr.BASE_SPEC)

    def test_missing_gap_raises(self):
        with self.assertRaises(krm.DegenerateWindowError):
            cr.rule_components(make_row(date(2026, 1, 1), gdp_gap=None), cr.BASE_SPEC)

    def test_residuals(self):
        rows = [make_row(date(2026, 1, 1) + timedelta(days=30 * i)) for i in range(3)]
        residuals = cr.rule_residuals(rows, cr.BASE_SPEC)
        self.assertEqual(len(residuals), 3)
        for residual in residuals:
            self.assertAlmostEqual(residual, -81.25, places=12)


class ProbabilitiesTest(unittest.TestCase):
    def test_sum_to_one(self):
        for step in (-200.0, -25.0, 0.0, 25.0, 200.0):
            probs = cr.class_probabilities(step, 50.0)
            self.assertAlmostEqual(sum(probs), 1.0, places=12)
            self.assertTrue(all(0.0 < value < 1.0 for value in probs))

    def test_monotone(self):
        low = cr.class_probabilities(-100.0, 50.0)
        high = cr.class_probabilities(100.0, 50.0)
        self.assertGreater(low[0], high[0])
        self.assertGreater(high[2], low[2])

    def test_sigma_floor(self):
        probs = cr.class_probabilities(0.0, 0.0)
        self.assertAlmostEqual(sum(probs), 1.0, places=12)

    def test_fit(self):
        train = [
            make_row(date(2015, 1, 1) + timedelta(days=30 * i), step_bp=0)
            for i in range(10)
        ]
        forecast = cr.fit_cbr_rule(train, make_row(date(2015, 12, 1)), cr.BASE_SPEC)
        self.assertAlmostEqual(forecast.step_bp, 81.25, places=12)
        self.assertAlmostEqual(sum(forecast.probs), 1.0, places=12)

    def test_fit_too_few_residuals(self):
        train = [
            make_row(date(2015, 1, 1) + timedelta(days=30 * i), step_bp=0)
            for i in range(7)
        ]
        with self.assertRaises(krm.DegenerateWindowError):
            cr.fit_cbr_rule(train, make_row(date(2015, 12, 1)), cr.BASE_SPEC)


class ConsensusTest(unittest.TestCase):
    def _rows(self):
        return [
            {"date": "2026-09-01", "series": "cpi_dec_2027_median", "value": 5.5},
            {"date": "2026-09-01", "series": "cpi_avg_2027_median", "value": 5.0},
            {"date": "2026-09-01", "series": "cpi_dec_2026_median", "value": 4.5},
        ]

    def test_parse_and_lag(self):
        waves = cr.cpi_consensus_from_rows(self._rows())
        self.assertEqual(len(waves), 3)
        self.assertEqual(waves[0].available_from, date(2026, 9, 7))

    def test_prefers_dec(self):
        waves = cr.cpi_consensus_from_rows(self._rows())
        self.assertEqual(cr.e3_consensus(waves, date(2026, 10, 20)), 5.5)

    def test_fallback_avg(self):
        rows = [{"date": "2026-09-01", "series": "cpi_avg_2027_median", "value": 5.0}]
        waves = cr.cpi_consensus_from_rows(rows)
        self.assertEqual(cr.e3_consensus(waves, date(2026, 10, 20)), 5.0)

    def test_target_year(self):
        rows = [{"date": "2026-09-01", "series": "cpi_dec_2026_median", "value": 4.5}]
        waves = cr.cpi_consensus_from_rows(rows)
        self.assertIsNone(cr.e3_consensus(waves, date(2026, 10, 20)))

    def test_not_available_before_wave(self):
        waves = cr.cpi_consensus_from_rows(self._rows())
        self.assertIsNone(cr.e3_consensus(waves, date(2026, 9, 5)))

    def test_duplicate_raises(self):
        rows = self._rows() + [
            {"date": "2026-09-01", "series": "cpi_dec_2027_median", "value": 5.6}
        ]
        with self.assertRaises(ValueError):
            cr.cpi_consensus_from_rows(rows)


class RStarTrendTest(unittest.TestCase):
    def test_minimum_and_clip(self):
        rows = [
            make_row(date(2015, 1, 1) + timedelta(days=30 * i), rate_before=10.0)
            for i in range(10)
        ]
        with unittest.mock.patch.object(cr, "headline_yoy", return_value=2.0):
            trends = cr.r_star_trend_series(rows, [])
        self.assertEqual(trends[:7], [None] * 7)
        self.assertAlmostEqual(trends[7], 6.0, places=12)
        self.assertAlmostEqual(trends[-1], 6.0, places=12)

    def test_low_rate_clip(self):
        rows = [
            make_row(date(2015, 1, 1) + timedelta(days=30 * i), rate_before=1.0)
            for i in range(8)
        ]
        with unittest.mock.patch.object(cr, "headline_yoy", return_value=5.0):
            trends = cr.r_star_trend_series(rows, [])
        self.assertAlmostEqual(trends[-1], 0.0, places=12)


class BuildRowsTest(unittest.TestCase):
    def test_wraps_base_rows(self):
        day = date(2016, 1, 1)
        base = krm.Row(make_decision(day), 5.0, 0.0)
        with unittest.mock.patch.object(krm, "build_rows", return_value=([base], 2)):
            rows, missing = cr.build_cbr_rows([], [], [], [], [], [])
        self.assertEqual(missing, 2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].decision.meeting_date, day)
        self.assertIsNone(rows[0].e3_core)
        self.assertIsNone(rows[0].gdp_gap)

    def test_coverage_counts(self):
        rows = [
            make_row(date(2016, 1, 1)),
            make_row(date(2016, 2, 1), e3_core=None, gdp_gap=None),
        ]
        cov = cr.coverage(rows)
        self.assertEqual(cov["rows"], 2)
        self.assertEqual(cov["e3_core_missing"], 1)
        self.assertEqual(cov["gdp_gap_missing"], 1)


class GroupingAndDmTest(unittest.TestCase):
    def test_group_order(self):
        predictions = [
            make_prediction(date(2016, 1, 1), "cbr_rule", 0.0, 0.0),
            make_prediction(date(2016, 1, 1), "hold", 0.0, 0.0),
            make_prediction(date(2016, 1, 1), "ordered_logit", 0.0, 0.0, (0.3, 0.4, 0.3)),
        ]
        groups = cr.group_predictions(predictions)
        self.assertEqual(
            list(groups.keys()),
            [
                ("hold", "expanding"),
                ("ordered_logit", "expanding"),
                ("cbr_rule", "expanding"),
            ],
        )

    def test_aligned_errors(self):
        left = [
            make_prediction(date(2016, 1, 1), "a", 10.0, 0.0),
            make_prediction(date(2016, 2, 1), "a", 20.0, 0.0),
        ]
        right = [
            make_prediction(date(2016, 2, 1), "b", 30.0, 0.0),
            make_prediction(date(2016, 3, 1), "b", 40.0, 0.0),
        ]
        errors_a, errors_b = cr.aligned_errors(left, right)
        self.assertEqual(errors_a, [20.0])
        self.assertEqual(errors_b, [30.0])

    def test_dm_pair(self):
        left = [
            make_prediction(date(2016, 1, 1) + timedelta(days=i), "a", 0.0, 0.0)
            for i in range(20)
        ]
        right = [
            make_prediction(date(2016, 1, 1) + timedelta(days=i), "b", 100.0, 0.0)
            for i in range(20)
        ]
        pair = cr.dm_pair(left, right)
        self.assertIsNotNone(pair)
        self.assertEqual(pair["n"], 20)
        self.assertLess(pair["pvalue"], 0.05)


class DecideTest(unittest.TestCase):
    def _groups(self, base_error, ordered_error):
        days = [date(2015, 1, 1) + timedelta(days=30 * i) for i in range(40)]
        actual = [25.0 if i % 2 else -25.0 for i in range(len(days))]
        base = [
            make_prediction(
                day,
                "cbr_rule",
                value + base_error,
                value,
                (0.9, 0.05, 0.05) if value < 0 else (0.05, 0.05, 0.9),
            )
            for day, value in zip(days, actual)
        ]
        ordered = [
            make_prediction(day, "ordered_logit", value + ordered_error, value, (0.33, 0.34, 0.33))
            for day, value in zip(days, actual)
        ]
        hold = [make_prediction(day, "hold", 0.0, value) for day, value in zip(days, actual)]
        ar1 = [make_prediction(day, "ar1", 0.0, value) for day, value in zip(days, actual)]
        return {
            ("cbr_rule", "expanding"): base,
            ("ordered_logit", "expanding"): ordered,
            ("hold", "expanding"): hold,
            ("ar1", "expanding"): ar1,
        }

    def test_promote(self):
        decision, _ = cr.decide(self._groups(0.0, 100.0))
        self.assertEqual(decision, "promote")

    def test_reject(self):
        decision, _ = cr.decide(self._groups(100.0, 0.0))
        self.assertEqual(decision, "reject")

    def test_no_folds(self):
        decision, _ = cr.decide({})
        self.assertEqual(decision, "reject")


class ReportAndArtifactsTest(unittest.TestCase):
    def _report(self):
        return cr.render_report(
            as_of=date(2026, 10, 20),
            rows=[],
            cov={
                "rows": 0,
                "e3_core_missing": 0,
                "e3_headline_adaptive_missing": 0,
                "e3_headline_consensus_missing": 0,
                "e3_core_m3_missing": 0,
                "e3_headline_m3_missing": 0,
                "gdp_gap_missing": 0,
                "ibk_gap_missing": 0,
                "r_star_trend_missing": 0,
            },
            groups={},
            fold_skips={},
            decision="iterate",
            decision_reason="тест",
            current=None,
            residual_rows=[],
            residual_summary=None,
            sensitivity_rows=[],
            sample_notes={
                "decisions": 0,
                "excluded": 0,
                "missing_rate": 0,
                "missing_features": 0,
                "folds_expanding": 0,
                "folds_rolling": 0,
            },
        )

    def test_sections(self):
        text = self._report()
        for marker in ("Вердикт: `iterate`", "forecast_log", "Остаток правила", "Чувствительность"):
            self.assertIn(marker, text)

    def test_write_predictions(self):
        day = date(2016, 1, 1)
        rows = [make_row(day)]
        predictions = [
            make_prediction(day, "cbr_rule", 81.25, 0.0, (0.2, 0.5, 0.3)),
            make_prediction(day, "hold", 0.0, 0.0),
        ]
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "predictions.csv"
            cr.write_predictions(path, predictions, rows)
            with path.open(encoding="utf-8") as handle:
                content = list(csv.reader(handle))
        self.assertEqual(len(content), 3)
        self.assertEqual(content[0][0], "meeting_date")
        self.assertEqual(content[0][-1], "r_star")
        self.assertEqual(content[1][1], "cbr_rule")
        self.assertEqual(content[2][1], "hold")

    def test_write_diagnostics(self):
        day = date(2016, 1, 1)
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "diagnostics.csv"
            cr.write_diagnostics(path, [make_row(day)], cr.BASE_SPEC)
            with path.open(encoding="utf-8") as handle:
                content = list(csv.reader(handle))
        self.assertEqual(len(content), 2)
        self.assertEqual(content[0][0], "meeting_date")
        self.assertEqual(content[1][0], day.isoformat())

    def test_write_forecast_none_removes(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "forecast.json"
            path.write_text("{}", encoding="utf-8")
            cr.write_forecast(path, None)
            self.assertFalse(path.exists())

    def test_write_forecast_json(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "forecast.json"
            payload = {"as_of": "2026-10-20", "methods": {}}
            cr.write_forecast(path, payload)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), payload)

    def test_no_forecast_log_writer(self):
        self.assertFalse(hasattr(cr, "write_forecast_log"))
        self.assertFalse(hasattr(cr, "key_rate_forecast_log"))


class IntegrationTest(unittest.TestCase):
    """Полный офлайн-прогон на синтетических рядах."""

    @staticmethod
    def _synthetic():
        meetings = [date(2014, 2, 14) + timedelta(days=45 * i) for i in range(30)]
        key_rate = []
        for index, meeting in enumerate(meetings):
            rate = 10.0 + 0.25 * (index // 3)
            key_rate.append((meeting, rate))
            key_rate.append((meeting + timedelta(days=1), rate))
        cpd = []
        month = date(2012, 1, 31)
        while month <= date(2017, 12, 31):
            cpd.append((month, 0.30 + 0.05 * (len(cpd) % 5)))
            month = cr.add_months(month, 1)
            month = cr._month_end(month)
        gdp = []
        quarter = date(2011, 4, 1)
        while quarter <= date(2017, 1, 1):
            gdp.append((quarter, 1000.0 + 10.0 * len(gdp)))
            quarter = cr.add_months(quarter, 3)
        ibk = []
        month = date(2011, 1, 1)
        while month <= date(2017, 12, 1):
            ibk.append((month, 1.0 + 0.5 * math.sin(len(ibk) / 3.0)))
            month = cr.add_months(month, 1)
        return meetings, key_rate, cpd, gdp, ibk

    def test_run_summary(self):
        meetings, key_rate, cpd, gdp, ibk = self._synthetic()
        with tempfile.TemporaryDirectory() as raw:
            with unittest.mock.patch.object(krm, "load_meetings", return_value=meetings), \
                 unittest.mock.patch.object(krm, "load_key_rate", return_value=key_rate), \
                 unittest.mock.patch.object(krm, "load_mp_summary_deltas", return_value=[]), \
                 unittest.mock.patch.object(krm, "load_ibk", return_value=ibk), \
                 unittest.mock.patch.object(cr, "load_cpd_both", return_value=(cpd, cpd)), \
                 unittest.mock.patch.object(cr, "load_gdp", return_value=gdp), \
                 unittest.mock.patch.object(cr, "load_cpi_consensus", return_value=[]), \
                 unittest.mock.patch.object(cr, "MIN_TRAIN", 10), \
                 unittest.mock.patch.object(cr, "MIN_RULE_RESIDUALS", 5):
                code = cr.run_summary(
                    as_of=date(2016, 12, 31),
                    out_dir=Path(raw),
                    logger_enabled=False,
                )
            self.assertEqual(code, 0)
            for name in (cr.REPORT_NAME, cr.PREDICTIONS_NAME, cr.DIAGNOSTICS_NAME):
                self.assertTrue((Path(raw) / name).exists(), name)
            report = (Path(raw) / cr.REPORT_NAME).read_text(encoding="utf-8")
            self.assertIn("Вердикт:", report)
            self.assertIn("forecast_log", report)


if __name__ == "__main__":
    unittest.main()
