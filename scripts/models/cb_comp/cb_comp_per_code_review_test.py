#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_per_code_review` (разбор 2026-10-31): без книг ЦБ и архива."""

import datetime as dt
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_per_code_review as rv


def months(n, start="2010-01-01"):
    return pd.date_range(start, periods=n, freq="MS")


class MetricsTest(unittest.TestCase):
    def test_r2(self):
        y = np.array([1.0, 2.0, 3.0])
        self.assertAlmostEqual(rv.r2(y, y), 1.0)
        self.assertAlmostEqual(rv.r2(y, np.full(3, 2.0)), 0.0)

    def test_ridge_recovers_linear(self):
        rng = np.random.default_rng(1)
        x = rng.normal(size=(200, 3))
        y = 1.0 + x @ np.array([2.0, -1.0, 0.5])
        fit = rv.ridge_fit(x, y, 1e-8)
        np.testing.assert_allclose(fit(x), y, atol=1e-6)

    def test_cv_lambda_prefers_small_on_clean_signal(self):
        rng = np.random.default_rng(2)
        x = rng.normal(size=(150, 2))
        y = x @ np.array([3.0, -2.0])
        self.assertEqual(rv.ridge_cv_lambda(x, y), min(rv.RIDGE_LAMBDAS))

    def test_dm_symmetry(self):
        rng = np.random.default_rng(3)
        a, b = rng.normal(size=54), rng.normal(size=54) * 2
        self.assertAlmostEqual(rv.dm_hln_pvalue(a, b, 1), rv.dm_hln_pvalue(b, a, 1))
        self.assertLess(rv.dm_hln_pvalue(a, b * 3, 1), 0.05)


class SeriesTest(unittest.TestCase):
    def test_distinct_finds_duplicates(self):
        idx = months(120)
        rng = np.random.default_rng(4)
        base = rng.normal(size=120)
        frame = pd.DataFrame({"a": base, "b": base, "c": base + rng.normal(scale=1e-3, size=120),
                              "d": rng.normal(size=120)}, index=idx)
        res = rv.distinct_series(frame)
        self.assertEqual(res["codes"], 4)
        self.assertEqual(res["distinct"], 3)
        self.assertEqual(res["duplicate_groups"], [["a", "b"]])
        self.assertIn(("a", "c"), res["near_duplicates"])

    def test_offset_window(self):
        idx = months(180)
        sheet = pd.Series(0.0, index=idx)
        code = pd.Series(np.where(idx < pd.Timestamp("2022-01-01"), 1.0, 3.0), index=idx)
        self.assertAlmostEqual(rv.offset_forecast(code, sheet, 12).iloc[-1], 3.0)
        self.assertLess(rv.offset_forecast(code, sheet, 60).iloc[-1], 3.0)

    def test_ar_decay_converges_to_mean(self):
        idx = months(180)
        sheet = pd.Series(0.0, index=idx)
        rng = np.random.default_rng(5)
        code = pd.Series(rng.normal(size=180), index=idx)
        test = idx[idx >= pd.Timestamp("2023-01-01")]
        fc = rv.ar_decay_forecast(code, sheet, test)
        mu = code.loc["2010":"2022-12-01"].mean()
        self.assertLess(abs(fc.iloc[-1] - mu), abs(fc.iloc[0] - mu) + 1e-12)

    def test_residual_stats_white_noise(self):
        idx = months(177)
        rng = np.random.default_rng(6)
        sheet = pd.DataFrame({"x": rng.normal(size=177)}, index=idx)
        codes = sheet + rng.normal(size=(177, 1))
        res = rv.residual_stats(codes, sheet)
        self.assertLess(abs(res["ar1"]), 0.2)
        self.assertLess(res["slow_share"], 0.3)


class BookParsingTest(unittest.TestCase):
    def test_blocks_and_subheaders(self):
        rows = [
            ["Данные мониторинга", None, None, None],
            [None, None, "Исходные данные", "Сезонно скорректированные данные"],
            ["Отчетный период", "Месяц сбора", "Индикатор бизнес-климата Банка России", "Индикатор бизнес-климата Банка России"],
            [dt.datetime(2020, 1, 1), dt.datetime(2020, 2, 1), 1.5, 2.5],
            [dt.datetime(2020, 2, 1), dt.datetime(2020, 3, 1), "н/д", 3.0],
        ]
        cols = rv.parse_book_rows("mp_survey_data", "Услуги", rows)
        self.assertEqual(cols["mp_survey_data|Услуги|NSA|Индикатор бизнес-климата Банка России|"], {pd.Timestamp("2020-01-01"): 1.5})
        sa = cols["mp_survey_data|Услуги|SA|Индикатор бизнес-климата Банка России|"]
        self.assertEqual(sa[pd.Timestamp("2020-02-01")], 3.0)

    def test_size_subheader(self):
        rows = [
            [None, None, "Исходные данные", None],
            ["Отчетный период", "Месяц", "Индикатор бизнес-климата Банка России", None],
            [None, None, "крупные", "малые и микро"],
            [dt.datetime(2020, 1, 1), dt.datetime(2020, 2, 1), 1.0, 2.0],
        ]
        cols = rv.parse_book_rows("survey_data_group", "Торговля", rows)
        self.assertIn("survey_data_group|Торговля|NSA|Индикатор бизнес-климата Банка России|малые и микро", cols)

    def test_no_header(self):
        self.assertEqual(rv.parse_book_rows("x", "y", [["a"], ["b"]]), {})

    def test_book_column_question(self):
        idx = months(3)
        books = pd.DataFrame({
            "mp_survey_data|Услуги|SA|Ценовые ожидания предприятий на следующие 3 месяца, баланс ответов|": [1, 2, 3],
            "mp_survey_data|Услуги|SA|Как изменились цены на готовую продукцию? баланс ответов|": [9, 9, 9],
        }, index=idx)
        self.assertEqual(list(rv.book_column(books, "Услуги", "SA", "price_exp")), [1, 2, 3])
        with self.assertRaises(ValueError):
            rv.book_column(books, "Услуги", "NSA", "price_exp")


if __name__ == "__main__":
    unittest.main()
