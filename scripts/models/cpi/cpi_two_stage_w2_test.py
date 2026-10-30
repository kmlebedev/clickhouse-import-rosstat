#!/usr/bin/env python3
"""Офлайн-тесты выбора варианта коррекции КУ (#401 W2, протокол спеки §4 W2).

Walk-forward отбора: train — origin'ы с 2016-01 до `T−1` (purge), цель обучающих строк — из
винтажной истории тестового origin'а `T`, истина — окончательный ряд; отсутствие утечки;
правило выбора с допуском 2 % в пользу `V0`. R, ClickHouse и сеть не используются.
Запуск — `make cpi-two-stage-w2-test`.
"""

import math
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_two_stage_w2 as w2


def months_range(first: date, n: int) -> list:
    out, year, month = [], first.year, first.month
    for _ in range(n):
        out.append(date(year, month, 1))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


def prev(m: date) -> date:
    return date(m.year - (m.month == 1), 12 if m.month == 1 else m.month - 1, 1)


def synthetic(n=72):
    origins = months_range(date(2016, 1, 1), n)
    truth_series = {m: 0.5 + 0.2 * math.sin(i / 2.0) for i, m in enumerate(months_range(date(2015, 12, 1), n + 2))}
    rows = {o: {"sa4w_last": truth_series[prev(o)] + 0.01, "sa4w_partial": truth_series[o] * 0.8 + 0.1} for o in origins}
    vint = {o: dict(truth_series) for o in origins}  # винтаж каждого origin'а (в синтетике одинаков)
    final = dict(truth_series)
    return origins, rows, vint, final


class SelectVariantTest(unittest.TestCase):
    def test_v0_kept_when_improvement_below_tolerance(self):
        self.assertEqual(w2.select_variant({"V0": 1.00, "V1": 0.985, "V2": 0.99}), "V0")

    def test_variant_chosen_when_improvement_at_least_two_percent(self):
        self.assertEqual(w2.select_variant({"V0": 1.00, "V1": 0.98, "V2": 0.99}), "V1")
        self.assertEqual(w2.select_variant({"V0": 1.00, "V1": 0.99, "V2": 0.95}), "V2")

    def test_v0_when_it_is_best(self):
        self.assertEqual(w2.select_variant({"V0": 0.9, "V1": 1.0, "V2": 1.1}), "V0")

    def test_requires_v0(self):
        with self.assertRaises(ValueError):
            w2.select_variant({"V1": 1.0})


class WalkForwardTest(unittest.TestCase):
    def run_wf(self, rows, vint, final, tests):
        return w2.walk_forward(rows, vint, final, tests)

    def test_forecasts_per_test_origin_with_expected_keys(self):
        origins, rows, vint, final = synthetic()
        tests = [date(2018, 1, 1), date(2018, 2, 1)]
        out = self.run_wf(rows, vint, final, tests)
        self.assertEqual([r["origin"] for r in out], tests)
        for r in out:
            self.assertEqual(set(r), {"origin", "truth", "pred", "lag", "weekly_naive", "n_train"})
            self.assertTrue(math.isfinite(r["pred"]))
        self.assertEqual(out[0]["n_train"], 24)  # 2016-01 … 2017-12
        self.assertEqual(out[1]["n_train"], 25)

    def test_learns_signal_better_than_noise(self):
        origins, rows, vint, final = synthetic()
        out = self.run_wf(rows, vint, final, months_range(date(2018, 1, 1), 24))
        rmse_pred = math.sqrt(sum((r["pred"] - r["truth"]) ** 2 for r in out) / len(out))
        rmse_naive = math.sqrt(sum((r["weekly_naive"] - r["truth"]) ** 2 for r in out) / len(out))
        self.assertLess(rmse_pred, rmse_naive)

    def test_no_leak_later_origins_do_not_change_forecast(self):
        origins, rows, vint, final = synthetic()
        test = date(2018, 6, 1)
        clean = self.run_wf(rows, vint, final, [test])[0]["pred"]
        dirty_rows = {o: ({k: v * 7 + 3 for k, v in r.items()} if o > test else r) for o, r in rows.items()}
        dirty_vint = {o: ({m: v * 7 + 3 for m, v in h.items()} if o != test else h) for o, h in vint.items()}
        dirty_final = {m: (v * 7 + 3 if m != test else v) for m, v in final.items()}
        dirty_final[test] = final[test]
        # Истина месяца `test` — единственное место, где используется окончательный ряд; прогноз от неё не зависит.
        self.assertEqual(clean, self.run_wf(dirty_rows, dirty_vint, dirty_final, [test])[0]["pred"])

    def test_training_targets_come_from_test_origin_vintage(self):
        origins, rows, vint, final = synthetic()
        test = date(2018, 6, 1)
        clean = self.run_wf(rows, vint, final, [test])[0]["pred"]
        changed = {o: dict(h) for o, h in vint.items()}
        changed[test][date(2017, 5, 1)] += 2.0
        self.assertNotEqual(clean, self.run_wf(rows, changed, final, [test])[0]["pred"])

    def test_purge_excludes_test_origin_row_from_training(self):
        origins, rows, vint, final = synthetic()
        test = date(2018, 6, 1)
        clean = self.run_wf(rows, vint, final, [test])[0]
        bad_rows = {o: dict(r) for o, r in rows.items()}
        bad_rows[test]["sa4w_last"] += 5.0  # признак самой тестовой строки меняет прогноз, но не обучение
        shifted = self.run_wf(bad_rows, vint, final, [test])[0]
        self.assertEqual(clean["n_train"], shifted["n_train"])
        self.assertNotEqual(clean["pred"], shifted["pred"])
        # Строка `T−1` в обучении есть: n_train = число origin'ов с 2016-01 до T−1.
        self.assertEqual(clean["n_train"], 29)

    def test_origin_without_enough_train_rows_is_skipped(self):
        origins, rows, vint, final = synthetic()
        out = self.run_wf(rows, vint, final, [date(2016, 12, 1), date(2018, 1, 1)])
        self.assertEqual([r["origin"] for r in out], [date(2018, 1, 1)])

    def test_truth_is_final_series(self):
        origins, rows, vint, final = synthetic()
        test = date(2018, 6, 1)
        out = self.run_wf(rows, vint, {**final, test: 123.0}, [test])
        self.assertEqual(out[0]["truth"], 123.0)

    def test_missing_snapshot_row_skips_training_month_not_the_fold(self):
        origins, rows, vint, final = synthetic()
        del rows[date(2016, 7, 1)]
        out = self.run_wf(rows, vint, final, [date(2018, 2, 1)])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["n_train"], 24)  # 25 origin'ов минус один без среза

    def test_nan_partial_is_imputed_not_dropped(self):
        origins, rows, vint, final = synthetic()
        for o in origins[:10]:
            rows[o]["sa4w_partial"] = float("nan")
        out = self.run_wf(rows, vint, final, [date(2018, 1, 1)])
        self.assertEqual(out[0]["n_train"], 24)
        self.assertTrue(math.isfinite(out[0]["pred"]))


class SummaryTest(unittest.TestCase):
    def test_rmse_by_method(self):
        forecasts = [
            {"truth": 1.0, "pred": 1.5, "lag": 0.0, "weekly_naive": 2.0},
            {"truth": 0.0, "pred": 0.5, "lag": 1.0, "weekly_naive": 0.0},
        ]
        out = w2.rmse_by_method(forecasts)
        self.assertAlmostEqual(out["pred"], 0.5)
        self.assertAlmostEqual(out["lag"], 1.0)
        self.assertAlmostEqual(out["weekly_naive"], math.sqrt(0.5))


if __name__ == "__main__":
    unittest.main()
