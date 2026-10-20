#!/usr/bin/env python3
"""Офлайн-тесты прокси-пути CB_COMP (`scripts/models/cb_comp/cb_comp_proxy.py`).

Транспорт к ClickHouse подменяется: ни один тест не ходит в сеть. Синтетика
строится так, чтобы тождества были точными (SAAR ↔ м/м, агрегат ↔ компоненты),
а ошибки метрик можно было посчитать руками.
"""

import csv
import json
import math
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from models.cb_comp import cb_comp_proxy as cbp


def make_months(start: date, end: date) -> list:
    return cbp.month_range(start, end)


def wave(index: int, base: float = 0.4, amp: float = 0.2) -> float:
    return base + amp * math.sin(index * 0.7) + 0.05 * math.cos(index * 0.3)


def mm_to_saar(mm: float) -> float:
    """Обратное к `saar_to_mm`: м/м, % → SAAR, % годовых."""
    return ((1.0 + mm / 100.0) ** 12 - 1.0) * 100.0


def synthetic_data(start: date = date(2002, 1, 1), end: date = date(2026, 8, 1)):
    months = make_months(start, end)
    sa_cpi = {}
    food = {}
    nonfood = {}
    services = {}
    for index, month in enumerate(months):
        food[month] = wave(index, 0.45, 0.25)
        nonfood[month] = wave(index + 3, 0.35, 0.15)
        services[month] = wave(index + 7, 0.55, 0.35)
        sa_cpi[month] = 0.38 * food[month] + 0.36 * nonfood[month] + 0.26 * services[month]
    all_goods = dict(sa_cpi)
    saar = {
        "saar_all_goods": {month: mm_to_saar(value) for month, value in all_goods.items()},
        "saar_food": {month: mm_to_saar(value) for month, value in food.items()},
        "saar_nonfood": {month: mm_to_saar(value) for month, value in nonfood.items()},
        "saar_services": {month: mm_to_saar(value) for month, value in services.items()},
    }
    rosstat = {
        "cpi_all": dict(all_goods),
        "cpi_food": dict(food),
        "cpi_nonfood": dict(nonfood),
        "cpi_services": dict(services),
    }
    return sa_cpi, saar, rosstat


def synthetic_nowcast(start: date = date(2006, 1, 1), end: date = date(2026, 8, 1)):
    months = make_months(start, end)
    values = {}
    for model_index, model in enumerate(cbp.CATBOOST_MODELS + cbp.CONTEXT_MODELS):
        values[model] = {
            month: 0.5 + 0.1 * model_index + 0.15 * math.sin(index * 0.5)
            for index, month in enumerate(months)
        }
    return cbp.Nowcast(
        path=Path("/tmp/synthetic-nowcast.csv"),
        window="start=48",
        values=values,
        months={model: sorted(points) for model, points in values.items()},
        source_rows=sum(len(points) for points in values.values()),
    )


class CalendarTest(unittest.TestCase):
    def test_month_helpers(self):
        self.assertEqual(cbp.month_of(date(2024, 2, 29)), date(2024, 2, 1))
        self.assertEqual(cbp.month_shift(date(2023, 12, 1), 1), date(2024, 1, 1))
        self.assertEqual(cbp.month_shift(date(2024, 1, 1), -13), date(2022, 12, 1))
        self.assertEqual(cbp.month_end(date(2024, 2, 10)), date(2024, 2, 29))
        self.assertEqual(cbp.month_end(date(2023, 2, 1)), date(2023, 2, 28))

    def test_month_range(self):
        self.assertEqual(
            cbp.month_range(date(2023, 11, 15), date(2024, 2, 2)),
            [date(2023, 11, 1), date(2023, 12, 1), date(2024, 1, 1), date(2024, 2, 1)],
        )
        with self.assertRaises(cbp.CbCompProxyError):
            cbp.month_range(date(2024, 2, 1), date(2024, 1, 1))


class ConversionTest(unittest.TestCase):
    def test_saar_to_mm_identity(self):
        for mm in (0.0, 0.5, 1.0, -0.3, 2.5):
            self.assertAlmostEqual(cbp.saar_to_mm(mm_to_saar(mm)), mm, places=10)

    def test_saar_to_mm_rejects_minus_100(self):
        with self.assertRaises(cbp.CbCompProxyError):
            cbp.saar_to_mm(-100.0)

    def test_utilities_mm_compounds(self):
        self.assertAlmostEqual(cbp.utilities_mm(4.0), 0.3273739782, places=9)
        self.assertEqual(cbp.utilities_mm(0.0), 0.0)


class PointsTest(unittest.TestCase):
    def test_points_parses_and_sorts(self):
        rows = [
            {"month": "2024-02-01", "value": 1.5},
            {"month": "2024-01-01", "value": "0.5"},
        ]
        self.assertEqual(cbp._points(rows), {date(2024, 1, 1): 0.5, date(2024, 2, 1): 1.5})

    def test_points_rejects_duplicate_month(self):
        rows = [
            {"month": "2024-01-01", "value": 0.5},
            {"month": "2024-01-15", "value": 0.6},
        ]
        with self.assertRaises(cbp.CbCompProxyError):
            cbp._points(rows)

    def test_points_rejects_non_finite(self):
        with self.assertRaises(cbp.CbCompProxyError):
            cbp._points([{"month": "2024-01-01", "value": float("nan")}])
        with self.assertRaises(cbp.CbCompProxyError):
            cbp._points([{"month": "2024-01-01", "value": "abc"}])

    def test_series_names_pinned(self):
        self.assertEqual(cbp.SA_CPI_SERIES, "Все товары и услуги")
        self.assertEqual(cbp.SAAR_SERIES[0], "saar_all_goods")
        self.assertEqual(
            cbp.ROSSTAT_SERIES["cpi_services"],
            "Индексы потребительских цен на услуги по Российской Федерации",
        )


class WeightsTest(unittest.TestCase):
    def setUp(self):
        _, _, self.rosstat = synthetic_data()

    def test_nnls_recovers_weights(self):
        fit = cbp.nnls_weights(self.rosstat, date(2009, 1, 1), date(2018, 12, 1))
        self.assertAlmostEqual(fit.weights["cpi_food"], 0.38, places=6)
        self.assertAlmostEqual(fit.weights["cpi_nonfood"], 0.36, places=6)
        self.assertAlmostEqual(fit.weights["cpi_services"], 0.26, places=6)
        self.assertAlmostEqual(sum(fit.weights.values()), 1.0, places=12)
        self.assertLess(fit.rmse, 1e-12)
        self.assertEqual(fit.months, 120)

    def test_nnls_requires_months(self):
        with self.assertRaises(cbp.CbCompProxyError):
            cbp.nnls_weights(self.rosstat, date(2009, 1, 1), date(2009, 6, 1))

    def test_weights_are_non_negative(self):
        fit = cbp.nnls_weights(self.rosstat, date(2002, 1, 1), date(2026, 8, 1))
        self.assertTrue(all(weight >= 0 for weight in fit.weights.values()))

    def test_saar_weights_match(self):
        _, saar, _ = synthetic_data()
        saar_mm = {
            name: {month: cbp.saar_to_mm(value) for month, value in points.items()}
            for name, points in saar.items()
        }
        fit = cbp.saar_weights(saar_mm)
        self.assertAlmostEqual(fit.weights["cpi_food"], 0.38, places=6)
        self.assertAlmostEqual(fit.weights["cpi_nonfood"], 0.36, places=6)
        self.assertAlmostEqual(fit.weights["cpi_services"], 0.26, places=6)

    def test_weighted_components(self):
        _, saar, _ = synthetic_data()
        saar_mm = {
            name: {month: cbp.saar_to_mm(value) for month, value in points.items()}
            for name, points in saar.items()
        }
        weights = {"cpi_food": 0.38, "cpi_nonfood": 0.36, "cpi_services": 0.26}
        reconstructed = cbp.weighted_components(saar_mm, weights)
        rmse, maximum, n = cbp.aligned_rmse(saar_mm["saar_all_goods"], reconstructed)
        self.assertLess(rmse, 1e-10)
        self.assertLess(maximum, 1e-9)
        self.assertEqual(n, len(saar_mm["saar_all_goods"]))


class ProxyTargetTest(unittest.TestCase):
    def test_formula(self):
        sa_cpi = {date(2024, 1, 1): 1.0, date(2024, 2, 1): 0.5}
        proxy = cbp.proxy_target(sa_cpi, 0.10, 0.0)
        self.assertAlmostEqual(proxy[date(2024, 1, 1)], 1.0 / 0.9, places=12)
        self.assertAlmostEqual(proxy[date(2024, 2, 1)], 0.5 / 0.9, places=12)

    def test_constant_growth_is_shift(self):
        sa_cpi = {date(2024, month, 1): 0.5 for month in (1, 2, 3)}
        zero = cbp.proxy_target(sa_cpi, 0.10, 0.0)
        four = cbp.proxy_target(sa_cpi, 0.10, 4.0)
        eight = cbp.proxy_target(sa_cpi, 0.10, 8.0)
        shifts = [four[month] - zero[month] for month in sa_cpi]
        self.assertAlmostEqual(max(shifts), min(shifts), places=12)
        shifts_two = [eight[month] - four[month] for month in sa_cpi]
        self.assertAlmostEqual(max(shifts_two), min(shifts_two), places=12)
        self.assertNotAlmostEqual(shifts[0], shifts_two[0], places=6)

    def test_rejects_weight_out_of_range(self):
        with self.assertRaises(cbp.CbCompProxyError):
            cbp.proxy_target({date(2024, 1, 1): 0.5}, 1.0, 4.0)
        with self.assertRaises(cbp.CbCompProxyError):
            cbp.proxy_target({date(2024, 1, 1): 0.5}, -0.1, 4.0)

    def test_scale_factor(self):
        self.assertAlmostEqual(cbp.scale_factor(0.10), 1.1111111111111112, places=12)


class CumulativeTest(unittest.TestCase):
    def test_sums_h_months(self):
        points = {date(2024, month, 1): float(month) for month in range(1, 7)}
        self.assertEqual(cbp.cumulative(points, date(2024, 1, 1), 1), 1.0)
        self.assertEqual(cbp.cumulative(points, date(2024, 1, 1), 3), 1.0 + 2.0 + 3.0)
        self.assertEqual(cbp.cumulative(points, date(2024, 4, 1), 3), 4.0 + 5.0 + 6.0)

    def test_missing_month_returns_none(self):
        points = {date(2024, 1, 1): 1.0, date(2024, 3, 1): 3.0}
        self.assertIsNone(cbp.cumulative(points, date(2024, 1, 1), 3))

    def test_bad_horizon(self):
        with self.assertRaises(cbp.CbCompProxyError):
            cbp.cumulative({date(2024, 1, 1): 1.0}, date(2024, 1, 1), 0)


class ExclusionTest(unittest.TestCase):
    def test_article_boundaries(self):
        self.assertTrue(cbp.article_excluded(date(2014, 12, 1)))
        self.assertTrue(cbp.article_excluded(date(2015, 3, 1)))
        self.assertFalse(cbp.article_excluded(date(2015, 4, 1)))
        self.assertTrue(cbp.article_excluded(date(2020, 4, 1)))
        self.assertFalse(cbp.article_excluded(date(2020, 3, 1)))
        self.assertTrue(cbp.article_excluded(date(2022, 1, 1)))
        self.assertTrue(cbp.article_excluded(date(2022, 12, 1)))
        self.assertFalse(cbp.article_excluded(date(2023, 1, 1)))

    def test_window_excluded_if_any_month(self):
        self.assertTrue(
            cbp.window_excluded([date(2021, 12, 1), date(2022, 1, 1)])
        )
        self.assertFalse(
            cbp.window_excluded([date(2021, 11, 1), date(2021, 12, 1)])
        )


class NowcastCsvTest(unittest.TestCase):
    def _write(self, directory: Path, rows: list) -> Path:
        path = directory / "nowcast.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(("month", "window", "model", "y_true", "y_pred"))
            writer.writerows(rows)
        return path

    def test_filters_window_and_parses(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(
                Path(tmp),
                [
                    ("2024-01-01", "start=48", "catboost_direct", 0.5, 0.6),
                    ("2024-02-01", "start=48", "catboost_direct", 0.4, 0.3),
                    ("2024-01-01", "start=24", "catboost_direct", 0.5, 0.9),
                ],
            )
            nowcast = cbp.load_nowcast(path, "start=48")
            self.assertEqual(nowcast.window, "start=48")
            self.assertEqual(nowcast.source_rows, 2)
            self.assertEqual(
                nowcast.values["catboost_direct"],
                {date(2024, 1, 1): 0.6, date(2024, 2, 1): 0.3},
            )

    def test_duplicate_is_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(
                Path(tmp),
                [
                    ("2024-01-01", "start=48", "catboost_direct", 0.5, 0.6),
                    ("2024-01-01", "start=48", "catboost_direct", 0.5, 0.7),
                ],
            )
            with self.assertRaises(cbp.CbCompProxyError):
                cbp.load_nowcast(path, "start=48")

    def test_missing_window_lists_available(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(
                Path(tmp),
                [("2024-01-01", "start=24", "catboost_direct", 0.5, 0.6)],
            )
            with self.assertRaises(cbp.CbCompProxyError) as context:
                cbp.load_nowcast(path, "start=48")
            self.assertIn("start=24", str(context.exception))

    def test_missing_file(self):
        with self.assertRaises(cbp.CbCompProxyError):
            cbp.load_nowcast(Path("/nonexistent/nowcast.csv"), "start=48")

    def test_missing_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.csv"
            path.write_text("month,model\n2024-01-01,x\n", encoding="utf-8")
            with self.assertRaises(cbp.CbCompProxyError):
                cbp.load_nowcast(path, "start=48")


class DirectFitTest(unittest.TestCase):
    def test_ols_projection_is_exact(self):
        months = make_months(date(2009, 1, 1), date(2018, 12, 1))
        proxy = {
            month: 0.4 + 0.1 * math.sin(index * 0.7)
            for index, month in enumerate(months)
        }
        nowcast = {
            month: 0.2 + 0.1 * math.cos(index * 0.5)
            for index, month in enumerate(months)
        }
        h = 3
        fit = cbp.fit_direct(nowcast, proxy, h, date(2009, 1, 1), date(2018, 12, 1))
        self.assertEqual(fit.rows, 117)
        self.assertEqual(fit.h, h)
        # OLS-проекция: остатки ортогональны трём столбцам дизайна.
        residuals = []
        design_lag = []
        for month in months:
            if month < date(2009, 2, 1) or month > date(2018, 10, 1):
                continue
            truth = cbp.cumulative(proxy, month, h)
            lag = proxy[cbp.month_shift(month, -1)]
            prediction = cbp.predict_direct(fit, nowcast[month], lag)
            residuals.append(truth - prediction)
            design_lag.append(lag)
        self.assertAlmostEqual(sum(residuals), 0.0, places=8)
        self.assertAlmostEqual(
            sum(residual * lag for residual, lag in zip(residuals, design_lag)),
            0.0,
            places=8,
        )
        self.assertAlmostEqual(
            sum(
                residual * nowcast[month]
                for residual, month in zip(
                    residuals,
                    [
                        month
                        for month in months
                        if date(2009, 2, 1) <= month <= date(2018, 10, 1)
                    ],
                )
            ),
            0.0,
            places=8,
        )
        prediction = cbp.predict_direct(fit, nowcast[date(2018, 1, 1)], proxy[date(2017, 12, 1)])
        self.assertTrue(math.isfinite(prediction))

    def test_does_not_use_target_beyond_train(self):
        sa_cpi, _, _ = synthetic_data()
        nowcast = synthetic_nowcast()
        proxy = cbp.proxy_target(sa_cpi, 0.10, 4.0)
        fit_before = cbp.fit_direct(
            nowcast.values["catboost_direct"],
            proxy,
            6,
            date(2009, 1, 1),
            date(2018, 12, 1),
        )
        mutated = dict(proxy)
        for month in mutated:
            if month > date(2018, 12, 1):
                mutated[month] += 100.0
        fit_after = cbp.fit_direct(
            nowcast.values["catboost_direct"],
            mutated,
            6,
            date(2009, 1, 1),
            date(2018, 12, 1),
        )
        self.assertEqual(fit_before, fit_after)
        # Для h=6 последняя train-строка — 2018-07: её цель кончается 2018-12.
        self.assertEqual(fit_before.rows, 115)

    def test_requires_min_rows(self):
        nowcast = {date(2018, month, 1): 0.1 for month in range(1, 13)}
        proxy = {date(2018, month, 1): 0.1 for month in range(1, 13)}
        with self.assertRaises(cbp.DegenerateFitError):
            cbp.fit_direct(nowcast, proxy, 1, date(2018, 1, 1), date(2018, 12, 1))

    def test_degenerate_matrix(self):
        months = make_months(date(2009, 1, 1), date(2018, 12, 1))
        proxy = {month: 0.4 for month in months}
        nowcast = {month: 0.2 for month in months}
        with self.assertRaises(cbp.DegenerateFitError):
            cbp.fit_direct(nowcast, proxy, 1, date(2009, 1, 1), date(2018, 12, 1))


class MetricsTest(unittest.TestCase):
    def test_rmse_manual(self):
        sa_cpi, _, _ = synthetic_data()
        nowcast = synthetic_nowcast()
        proxy = cbp.proxy_target(sa_cpi, 0.10, 4.0)
        values = nowcast.values["catboost_direct"]
        fit = cbp.fit_direct(values, proxy, 1, date(2009, 1, 1), date(2018, 12, 1))
        metric = cbp.evaluate_model(
            "catboost_direct", values, proxy, 1, fit, date(2019, 1, 1), date(2024, 7, 1)
        )
        self.assertEqual(metric.n, 54)
        self.assertEqual(metric.n_all, 67)
        self.assertAlmostEqual(metric.rmse, math.sqrt(sum(e * e for e in metric.errors) / len(metric.errors)))
        self.assertLessEqual(metric.ci_low, metric.ci_high)

    def test_n_by_horizon(self):
        sa_cpi, _, _ = synthetic_data()
        nowcast = synthetic_nowcast()
        proxy = cbp.proxy_target(sa_cpi, 0.10, 4.0)
        values = nowcast.values["catboost_direct"]
        expected = {1: 54, 2: 51, 3: 48, 6: 39}
        for h, count in expected.items():
            fit = cbp.fit_direct(values, proxy, h, date(2009, 1, 1), date(2018, 12, 1))
            metric = cbp.evaluate_model(
                "catboost_direct", values, proxy, h, fit, date(2019, 1, 1), date(2024, 7, 1)
            )
            self.assertEqual(metric.n, count, f"h={h}")

    def test_2022_excluded(self):
        proxy = {month: 0.5 for month in make_months(date(2019, 1, 1), date(2024, 7, 1))}
        errors = [0.1] * 54
        metric = cbp._errors_metrics("x", 1, errors, [0.1] * 67)
        self.assertEqual(metric.n, 54)
        self.assertAlmostEqual(metric.rmse, 0.1, places=12)

    def test_bootstrap_deterministic(self):
        errors = [0.1, -0.2, 0.3, 0.05, -0.1, 0.2, 0.0, 0.15, -0.05, 0.25]
        first = cbp.bootstrap_rmse_ci(errors)
        second = cbp.bootstrap_rmse_ci(errors)
        self.assertEqual(first, second)
        self.assertLessEqual(first[0], first[1])

    def test_benchmark_b1(self):
        proxy = {
            month: 0.5
            for month in make_months(date(2017, 1, 1), date(2019, 12, 1))
        }
        metric = cbp.evaluate_benchmark(
            "proxy_b1_last_value", proxy, 2, date(2019, 2, 1), date(2019, 12, 1)
        )
        self.assertGreater(metric.n, 0)
        self.assertAlmostEqual(metric.rmse, 0.0, places=12)


class BuildResultTest(unittest.TestCase):
    def setUp(self):
        self.sa_cpi, self.saar, self.rosstat = synthetic_data()
        self.nowcast = synthetic_nowcast()

    def test_smoke_and_comparison(self):
        result = cbp.build_result(
            self.sa_cpi,
            self.saar,
            self.rosstat,
            self.nowcast,
            today=date(2026, 10, 18),
        )
        self.assertEqual(result["seed"], cbp.SEED)
        self.assertEqual(len(result["metrics"]), 6 * 4)  # 6 моделей × 4 h
        self.assertEqual(len(result["comparison"]), 4)
        self.assertIn("verdict", result)
        self.assertIn("sensitivity", result)
        self.assertGreater(len(result["sensitivity"]), 0)
        self.assertEqual(result["target"]["weights_months"], 120)
        self.assertAlmostEqual(result["target"]["scale"], 1.0 / 0.9, places=12)
        self.assertIn("train_start", result["method"])

    def test_deterministic(self):
        first = cbp.build_result(
            self.sa_cpi, self.saar, self.rosstat, self.nowcast, today=date(2026, 10, 18)
        )
        second = cbp.build_result(
            self.sa_cpi, self.saar, self.rosstat, self.nowcast, today=date(2026, 10, 18)
        )
        self.assertEqual(
            json.dumps(cbp._json_safe(first), sort_keys=True),
            json.dumps(cbp._json_safe(second), sort_keys=True),
        )

    def test_report_contains_numbers(self):
        result = cbp.build_result(
            self.sa_cpi, self.saar, self.rosstat, self.nowcast, today=date(2026, 10, 18)
        )
        report = cbp.render_report(result)
        self.assertIn("CB_COMP", report)
        self.assertIn("Прокси-таргет", report)
        self.assertIn("Не паритет" if "Не паритет" in result["verdict"] else "margin", report)
        self.assertIn("CatBoost bottom-up (без МП)", report)

    def test_artifacts(self):
        result = cbp.build_result(
            self.sa_cpi, self.saar, self.rosstat, self.nowcast, today=date(2026, 10, 18)
        )
        report = cbp.render_report(result)
        with tempfile.TemporaryDirectory() as tmp:
            paths = cbp.write_artifacts(Path(tmp), result, report)
            for path in paths.values():
                self.assertTrue(Path(path).exists())
            data = json.loads(Path(paths["json"]).read_text(encoding="utf-8"))
            self.assertEqual(data["seed"], cbp.SEED)
            self.assertIn("metrics", data)
            rows = list(csv.DictReader(Path(paths["csv"]).open(encoding="utf-8")))
            self.assertEqual(len(rows), len(result["metrics"]))

    def test_train_after_test_rejected(self):
        with self.assertRaises(cbp.CbCompProxyError):
            cbp.build_result(
                self.sa_cpi,
                self.saar,
                self.rosstat,
                self.nowcast,
                train_end=date(2019, 1, 1),
            )


class LoaderMockTest(unittest.TestCase):
    def setUp(self):
        self._original = cbp.ch_query

    def tearDown(self):
        cbp.ch_query = self._original

    def test_load_sa_cpi(self):
        cbp.ch_query = lambda sql: [
            {"month": "2024-01-01", "value": 0.5},
            {"month": "2024-02-01", "value": 0.6},
        ]
        self.assertEqual(
            cbp.load_sa_cpi(),
            {date(2024, 1, 1): 0.5, date(2024, 2, 1): 0.6},
        )

    def test_load_saar(self):
        rows = []
        for name in cbp.SAAR_SERIES:
            rows.append({"series": name, "month": "2024-01-01", "value": 5.0})
        cbp.ch_query = lambda sql: rows
        loaded = cbp.load_saar()
        self.assertEqual(set(loaded), set(cbp.SAAR_SERIES))
        self.assertEqual(loaded["saar_food"][date(2024, 1, 1)], 5.0)

    def test_load_saar_missing_series(self):
        cbp.ch_query = lambda sql: [
            {"series": "saar_food", "month": "2024-01-01", "value": 5.0}
        ]
        with self.assertRaises(cbp.CbCompProxyError):
            cbp.load_saar()

    def test_load_rosstat(self):
        rows = [
            {"series": name, "month": "2024-01-01", "value": 0.4}
            for name in cbp.ROSSTAT_SERIES.values()
        ]
        cbp.ch_query = lambda sql: rows
        loaded = cbp.load_rosstat()
        self.assertEqual(set(loaded), set(cbp.ROSSTAT_SERIES))
        self.assertEqual(loaded["cpi_all"][date(2024, 1, 1)], 0.4)

    def test_load_rosstat_missing(self):
        cbp.ch_query = lambda sql: []
        with self.assertRaises(cbp.CbCompProxyError):
            cbp.load_rosstat()


class JsonSafeTest(unittest.TestCase):
    def test_dates_and_nan(self):
        value = {
            "date": date(2024, 1, 1),
            "tuple": (1, 2),
            "nan": float("nan"),
            "inf": float("inf"),
            "ok": 1.5,
        }
        safe = cbp._json_safe(value)
        self.assertEqual(safe["date"], "2024-01-01")
        self.assertEqual(safe["tuple"], [1, 2])
        self.assertIsNone(safe["nan"])
        self.assertIsNone(safe["inf"])
        self.assertEqual(safe["ok"], 1.5)


class VerdictTest(unittest.TestCase):
    def test_no_rows(self):
        self.assertIn("Нет метрик", cbp.verdict([]))

    def test_all_within_margin(self):
        rows = [
            {
                "h": 1,
                "without_mp": {"within_margin": True, "ratio": 1.05},
                "with_mp": {"within_margin": True, "ratio": 1.04},
            }
        ]
        self.assertIn("НЕ паритет", cbp.verdict(rows))

    def test_outside_margin(self):
        rows = [
            {
                "h": 1,
                "without_mp": {"within_margin": False, "ratio": 1.5},
                "with_mp": None,
            }
        ]
        self.assertIn("Не паритет", cbp.verdict(rows))
        self.assertIn("h=1 without_mp", cbp.verdict(rows))


if __name__ == "__main__":
    unittest.main(verbosity=2)
