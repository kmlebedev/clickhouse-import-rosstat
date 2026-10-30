#!/usr/bin/env python3
"""Офлайн-тесты as-of перефита недельного X-13 (#401 W1).

Часть тестов — порт `weeklysa/grid_test.go` (те же ожидания): порт сетки на
Python обязан совпадать с Go-реализацией. Остальное: доступность недели
(`date + 2`), отсутствие утечки в as-of срезе, parity с продовой таблицей на
подменяемом runner, сборка cutoff по конвенции `publication`, критерии gate W1.
R, ClickHouse и сеть не используются. Запуск — `make cpi-weekly-sa-asof-test`.
"""

import math
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import weekly_sa_asof as wa


def d(year, month, day):
    return date(year, month, day)


def almost(test, got, want, tol, msg=""):
    test.assertLessEqual(abs(got - want), tol, f"{msg}: {got} != {want}")


class MergeAndValidateTest(unittest.TestCase):
    def test_bi_overrides_bulletin_on_overlap_and_sorted(self):
        bulletin = [(d(2020, 1, 6), 100.1), (d(2020, 1, 13), 100.2)]
        bi = [(d(2020, 1, 13), 100.9), (d(2020, 1, 20), 100.3)]
        merged = wa.merge_sources(bulletin, bi)
        self.assertEqual(merged, [(d(2020, 1, 6), 100.1), (d(2020, 1, 13), 100.9), (d(2020, 1, 20), 100.3)])

    def test_duplicates_within_source_are_error(self):
        with self.assertRaises(ValueError):
            wa.merge_sources([(d(2020, 1, 6), 100.0), (d(2020, 1, 6), 100.1)], [])
        with self.assertRaises(ValueError):
            wa.merge_sources([], [(d(2020, 1, 6), 100.0), (d(2020, 1, 6), 100.1)])

    def test_non_positive_is_error(self):
        with self.assertRaises(ValueError):
            wa.merge_sources([(d(2020, 1, 6), 0.0)], [])
        with self.assertRaises(ValueError):
            wa.merge_sources([(d(2020, 1, 6), float("nan"))], [])

    def test_validate_series(self):
        pts = [(d(2020, 1, 6), 100.0), (d(2020, 1, 13), 100.0), (d(2020, 1, 20), 100.0)]
        wa.validate_series(pts, d(2020, 1, 6), 3)
        with self.assertRaises(ValueError):
            wa.validate_series(pts, d(2020, 1, 6), 4)
        with self.assertRaises(ValueError):
            wa.validate_series(pts, d(2020, 1, 13), 3)
        with self.assertRaises(ValueError):
            wa.validate_series([(d(2020, 1, 13), 1.0), (d(2020, 1, 6), 1.0)], d(2020, 1, 13), 2)


class GridPortTest(unittest.TestCase):
    """Порт `weeklysa/grid_test.go`."""

    def test_build_grid_interpolates_14_day_gap(self):
        grid = wa.build_grid([(d(2020, 1, 6), 100.0), (d(2020, 1, 20), 110.0)])
        self.assertEqual([g[0] for g in grid], [d(2020, 1, 6), d(2020, 1, 13), d(2020, 1, 20)])
        almost(self, grid[0][1], 0.0, 1e-12, "L0")
        almost(self, grid[1][1], math.log(1.1) / 2, 1e-12, "L1")
        almost(self, grid[2][1], math.log(1.1), 1e-12, "L2")

    def test_build_grid_needs_two_points(self):
        with self.assertRaises(ValueError):
            wa.build_grid([(d(2020, 1, 6), 100.0)])

    def test_four_week_index_28_days(self):
        grid = [(d(2020, 1, 6) + timedelta(days=7 * i), i * math.log(1.01)) for i in range(5)]
        index = wa.four_week_index(grid)
        self.assertEqual(len(index), 1)
        self.assertEqual(index[0][0], d(2020, 2, 3))
        almost(self, index[0][1], 1.01**4 * 100, 1e-9)

    def test_four_week_index_too_short(self):
        with self.assertRaises(ValueError):
            wa.four_week_index([(d(2020, 1, 6) + timedelta(days=7 * i), 0.0) for i in range(4)])

    def test_monthly_last_in_month_drops_partial_month(self):
        index = [
            (d(2020, 1, 6), 100.0), (d(2020, 1, 27), 101.0), (d(2020, 2, 3), 102.0), (d(2020, 2, 24), 103.0),
            (d(2020, 3, 2), 104.0), (d(2020, 3, 30), 105.0), (d(2020, 4, 6), 106.0),
        ]
        months = wa.monthly_last_in_month(index, d(2020, 4, 6), d(2020, 1, 1), 3)
        self.assertEqual([m.date for m in months], [d(2020, 1, 31), d(2020, 2, 29), d(2020, 3, 31)])
        self.assertEqual([m.value for m in months], [101.0, 103.0, 105.0])
        self.assertTrue(all(not m.partial for m in months))
        self.assertEqual(months[0].as_of, d(2020, 1, 27))

    def test_monthly_last_in_month_keeps_month_end(self):
        index = [
            (d(2020, 1, 6), 100.0), (d(2020, 1, 27), 101.0), (d(2020, 2, 3), 102.0), (d(2020, 2, 24), 103.0),
            (d(2020, 3, 2), 104.0), (d(2020, 3, 30), 105.0),
        ]
        self.assertEqual(len(wa.monthly_last_in_month(index, d(2020, 3, 31), d(2020, 1, 1), 3)), 3)

    def test_month_is_complete_only_when_grid_reaches_month_end(self):
        index = [
            (d(2020, 1, 6), 100.0), (d(2020, 1, 27), 101.0), (d(2020, 2, 3), 102.0), (d(2020, 2, 24), 103.0),
            (d(2020, 3, 2), 104.0), (d(2020, 3, 23), 105.0),
        ]
        # Сетка дошла до 28 марта (до конца месяца 31-го не хватает 3 дней): март неполный.
        months = wa.monthly_last_in_month(index, d(2020, 3, 28), d(2020, 1, 1), 2)
        self.assertEqual([m.date for m in months], [d(2020, 1, 31), d(2020, 2, 29)])
        months = wa.monthly_last_in_month(index, d(2020, 3, 31), d(2020, 1, 1), 2)
        self.assertEqual(months[-1].date, d(2020, 3, 31))

    def test_monthly_last_in_month_rejects_gap_wrong_first_and_few_months(self):
        with self.assertRaises(ValueError):
            wa.monthly_last_in_month([(d(2020, 1, 6), 100.0), (d(2020, 3, 2), 104.0)], d(2020, 4, 6), d(2020, 1, 1), 2)
        with self.assertRaises(ValueError):
            wa.monthly_last_in_month([(d(2020, 1, 6), 100.0), (d(2020, 2, 3), 102.0)], d(2020, 3, 6), d(2020, 2, 1), 2)
        with self.assertRaises(ValueError):
            wa.monthly_last_in_month([(d(2020, 1, 6), 100.0), (d(2020, 2, 3), 102.0)], d(2020, 3, 6), d(2020, 1, 1), 5)

    def test_partial_month(self):
        index = [(d(2020, 1, 6), 100.0), (d(2020, 1, 27), 101.0), (d(2020, 2, 3), 102.0), (d(2020, 2, 24), 103.0)]
        partial = wa.partial_month(index, d(2020, 2, 24))
        self.assertIsNotNone(partial)
        self.assertEqual((partial.date, partial.as_of, partial.partial, partial.value), (d(2020, 2, 29), d(2020, 2, 24), True, 103.0))
        self.assertIsNone(wa.partial_month(index, d(2020, 2, 29)))

    def test_compute_metrics(self):
        months = [wa.MonthPoint(d(2020, 1, 31), 100.5, d(2020, 1, 27), False), wa.MonthPoint(d(2020, 2, 29), 101.2, d(2020, 2, 24), False)]
        metrics = wa.compute_metrics(months, [(d(2020, 1, 31), 100.0), (d(2020, 2, 29), 101.0)])
        almost(self, metrics[0].sa_4w, 0.0, 1e-12)
        almost(self, metrics[0].saar, 0.0, 1e-12)
        almost(self, metrics[1].sa_index, 101.0, 1e-12)
        almost(self, metrics[1].sa_4w, 1.0, 1e-12)
        almost(self, metrics[1].saar, (1.01**13 - 1) * 100, 1e-9)
        with self.assertRaises(ValueError):
            wa.compute_metrics(months[:1], [(d(2020, 2, 29), 100.0)])
        with self.assertRaises(ValueError):
            wa.compute_metrics(months[:1], [(d(2020, 1, 31), -1.0)])


def weekly_points(start: date, n: int, base=100.2):
    return [(start + timedelta(days=7 * i), base + 0.05 * math.sin(i / 3.0) + 0.01 * (i % 4)) for i in range(n)]


def fake_runner(months):
    """Детерминированная подмена X-13: SA = уровень, сглаженный зависящей от истории константой."""
    mean = sum(m.value for m in months) / len(months)
    return [100.0 + (m.value - mean) * 0.5 for m in months]


class AvailabilityTest(unittest.TestCase):
    def test_week_available_two_days_after_date_inclusive(self):
        pts = [(d(2020, 6, 1), 100.0), (d(2020, 6, 8), 100.0)]
        self.assertEqual(wa.available_points(pts, d(2020, 6, 9)), [pts[0]])
        self.assertEqual(wa.available_points(pts, d(2020, 6, 10)), pts)
        self.assertEqual(wa.available_points(pts, d(2020, 5, 31)), [])


class CutoffTest(unittest.TestCase):
    def test_cutoff_uses_table_then_rule(self):
        table = {d(2020, 5, 1): d(2020, 6, 5)}
        self.assertEqual(wa.publication_cutoff(d(2020, 6, 1), table), d(2020, 6, 5))
        self.assertEqual(wa.publication_cutoff(d(2020, 7, 17), table), d(2020, 7, 9))
        self.assertEqual(wa.publication_cutoff(d(2014, 1, 1), None), d(2014, 1, 9))

    def test_consistent_with_cpi_monthly_convention(self):
        from models.cpi import cpi_monthly as cm

        table = {d(2020, 5, 1): d(2020, 6, 5), d(2025, 12, 1): d(2026, 1, 16)}
        for origin in (d(2014, 1, 1), d(2020, 6, 1), d(2026, 1, 1), d(2026, 10, 1)):
            with cm.asof_convention("publication", release_dates=table):
                self.assertEqual(wa.publication_cutoff(origin, table), cm.cutoff_for(origin))


class AsOfSnapshotTest(unittest.TestCase):
    START = d(2009, 4, 6)

    def pts(self, n):
        return weekly_points(self.START, n)

    def test_snapshot_structure(self):
        points = self.pts(200)
        cutoff = points[150][0] + timedelta(days=2)
        snap = wa.asof_snapshot(points, cutoff, fake_runner, first_month=d(2009, 5, 1), min_months=12)
        self.assertEqual(snap.last_grid_date, points[150][0])
        self.assertTrue(all(not m.partial for m in snap.months))
        self.assertGreaterEqual(len(snap.months), 12)
        self.assertEqual(len(snap.sa_complete), len(snap.months))
        if snap.partial is not None:
            self.assertTrue(snap.partial.partial)
            self.assertIsNotNone(snap.sa_partial)

    def test_no_leak_future_weeks_do_not_change_snapshot(self):
        points = self.pts(200)
        cutoff = points[150][0] + timedelta(days=2)
        clean = wa.asof_snapshot(points, cutoff, fake_runner, first_month=d(2009, 5, 1), min_months=12)
        dirty_points = [(day, value * 3 if day + timedelta(days=2) > cutoff else value) for day, value in points]
        dirty = wa.asof_snapshot(dirty_points, cutoff, fake_runner, first_month=d(2009, 5, 1), min_months=12)
        self.assertEqual(clean.sa_complete, dirty.sa_complete)
        self.assertEqual(clean.sa_partial, dirty.sa_partial)
        self.assertEqual(clean.last_grid_date, dirty.last_grid_date)

    def test_sensitivity_changing_available_week_changes_snapshot(self):
        points = self.pts(200)
        cutoff = points[150][0] + timedelta(days=2)
        clean = wa.asof_snapshot(points, cutoff, fake_runner, first_month=d(2009, 5, 1), min_months=12)
        bad = list(points)
        bad[100] = (bad[100][0], bad[100][1] * 1.05)
        dirty = wa.asof_snapshot(bad, cutoff, fake_runner, first_month=d(2009, 5, 1), min_months=12)
        self.assertNotEqual(clean.sa_complete, dirty.sa_complete)

    def test_too_few_months_is_error(self):
        points = self.pts(30)
        with self.assertRaises(ValueError):
            wa.asof_snapshot(points, points[-1][0] + timedelta(days=2), fake_runner, first_month=d(2009, 5, 1), min_months=36)

    def test_later_cutoff_adds_months_and_may_revise_history(self):
        points = self.pts(260)
        early = wa.asof_snapshot(points, points[200][0] + timedelta(days=2), fake_runner, first_month=d(2009, 5, 1), min_months=12)
        late = wa.asof_snapshot(points, points[259][0] + timedelta(days=2), fake_runner, first_month=d(2009, 5, 1), min_months=12)
        self.assertGreater(len(late.months), len(early.months))
        # С новыми данными история SA пересчитывается (ревизия): runner зависит от выборки.
        self.assertNotEqual(early.sa_complete[0], late.sa_complete[0])


class TrySnapshotTest(unittest.TestCase):
    def test_failure_is_reported_not_raised(self):
        points = weekly_points(d(2009, 4, 6), 200)

        def broken(months):
            raise RuntimeError("X-13 (R) завершился с кодом 1: boom")

        snap, error = wa.try_snapshot(points, points[-1][0] + timedelta(days=2), broken, min_months=12)
        self.assertIsNone(snap)
        self.assertIn("boom", error)

    def test_success_returns_snapshot(self):
        points = weekly_points(d(2009, 4, 6), 200)
        snap, error = wa.try_snapshot(points, points[-1][0] + timedelta(days=2), fake_runner, min_months=12)
        self.assertIsNone(error)
        self.assertIsNotNone(snap)

    def test_too_early_cutoff_is_reported(self):
        points = weekly_points(d(2009, 4, 6), 200)
        snap, error = wa.try_snapshot(points, d(2009, 4, 7), fake_runner)
        self.assertIsNone(snap)
        self.assertTrue(error)


class FirstReleaseTest(unittest.TestCase):
    def snaps(self):
        points = weekly_points(d(2009, 4, 6), 330)
        out = []
        for k in (200, 210, 214, 230):
            snap = wa.asof_snapshot(points, points[k][0] + timedelta(days=2), fake_runner, min_months=12)
            out.append((points[k][0], snap))
        return out

    def test_first_snapshot_registers_only_last_month(self):
        ordered = self.snaps()
        fr = wa.first_releases(ordered[:1])
        self.assertEqual(list(fr), [ordered[0][1].months[-1].date])

    def test_values_are_frozen_at_first_release_and_cover_new_months(self):
        ordered = self.snaps()
        fr = wa.first_releases(ordered)
        # Месяц, зарегистрированный в первом срезе, не перезаписывается позднее.
        first_month = ordered[0][1].months[-1].date
        expected = (ordered[0][1].sa_complete[-1] / 100 - 1) * 100
        self.assertEqual(fr[first_month], expected)
        # Месяцы, ставшие полными позже, зарегистрированы в первом срезе, где они появились.
        last_seen = ordered[-1][1].months[-1].date
        self.assertIn(last_seen, fr)
        months = sorted(fr)
        self.assertEqual(months, sorted(set(months)))
        for left, right in zip(months, months[1:]):
            self.assertLessEqual((right.year * 12 + right.month) - (left.year * 12 + left.month), 1 + 3)

    def test_revision_exists_when_history_changes(self):
        ordered = self.snaps()
        fr = wa.first_releases(ordered)
        revised = {m.date: (v / 100 - 1) * 100 for m, v in zip(ordered[-1][1].months, ordered[-1][1].sa_complete)}
        common = [m for m in fr if m in revised]
        self.assertTrue(any(abs(fr[m] - revised[m]) > 1e-9 for m in common))


class ParityTest(unittest.TestCase):
    def test_parity_zero_when_same_pipeline_and_detects_change(self):
        points = weekly_points(d(2009, 4, 6), 260)
        cutoff = points[-1][0]
        snap = wa.asof_snapshot(points, cutoff + timedelta(days=2), fake_runner, first_month=d(2009, 5, 1), min_months=12)
        final = wa.snapshot_rows(snap)
        result = wa.parity(final, snap)
        self.assertEqual(result["n"], len(final))
        self.assertLessEqual(result["max_abs_diff"], 1e-12)
        broken = [dict(row) for row in final]
        broken[5]["sa_index"] += 1e-3
        self.assertGreater(wa.parity(broken, snap)["max_abs_diff"], 9e-4)

    def test_parity_requires_same_dates(self):
        points = weekly_points(d(2009, 4, 6), 260)
        snap = wa.asof_snapshot(points, points[-1][0] + timedelta(days=2), fake_runner, first_month=d(2009, 5, 1), min_months=12)
        final = wa.snapshot_rows(snap)[1:]
        with self.assertRaises(ValueError):
            wa.parity(final, snap)


class X13CsvTest(unittest.TestCase):
    def test_encode_input_matches_go_format(self):
        months = [wa.MonthPoint(d(2020, 1, 31), 100.43832164128501, d(2020, 1, 27), False), wa.MonthPoint(d(2020, 2, 29), 101.0, d(2020, 2, 24), False)]
        self.assertEqual(wa.encode_x13_input(months), "date,level\n2020-01-31,100.43832164128501\n2020-02-29,101.0\n")

    def test_script_pin(self):
        self.assertEqual(wa.script_sha256(), wa.SCRIPT_SHA256)


class GateTest(unittest.TestCase):
    def quality(self, **over):
        base = {"rmse": 0.147, "bias": -0.024, "corr": 0.978}
        base.update(over)
        return base

    def test_pass(self):
        out = wa.decide_w1(1e-9, self.quality())
        self.assertEqual(out["decision"], "pass")

    def test_parity_failure_stops(self):
        out = wa.decide_w1(2e-6, self.quality())
        self.assertEqual(out["decision"], "stop_parity")

    def test_quality_failure_demotes(self):
        for bad in (self.quality(rmse=0.25), self.quality(bias=0.07), self.quality(corr=0.9)):
            self.assertEqual(wa.decide_w1(0.0, bad)["decision"], "demote_input")

    def test_boundaries_inclusive(self):
        self.assertEqual(wa.decide_w1(1e-6, self.quality(rmse=0.20, bias=0.05, corr=0.95))["decision"], "pass")


if __name__ == "__main__":
    unittest.main()
