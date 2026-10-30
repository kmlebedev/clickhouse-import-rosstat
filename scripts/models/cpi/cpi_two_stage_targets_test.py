#!/usr/bin/env python3
"""Офлайн-тесты `T_SAX` и вариантов коррекции КУ во входе Stage 1 (#401 W2).

Формула `(π − w·u)/(1 − w)`, окончательный и винтажный `T_SAX` (утечка: будущие
месяцы не влияют на историю origin'а `O`), календарь тарифных недель `V1`,
вычитание вклада КУ `V2`, хук `month_adjust` в as-of срезе. R, ClickHouse и сеть
не используются. Запуск — `make cpi-two-stage-targets-test`.
"""

import math
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_two_stage_targets as tt
import weekly_sa_asof as wa


def months_range(first: date, n: int) -> list:
    out, year, month = [], first.year, first.month
    for _ in range(n):
        out.append(date(year, month, 1))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


def fake_x13(levels, first_rt):
    """Подмена X-13: `full` — м/м уровня минус среднее м/м всей выборки (зависит от истории), `rt` — NaN."""
    mm = [float("nan")] + [(levels[i] / levels[i - 1] - 1) * 100 for i in range(1, len(levels))]
    mean = sum(v for v in mm if not math.isnan(v)) / (len(levels) - 1)
    return [(v - mean if not math.isnan(v) else float("nan"), float("nan")) for v in mm]


def world(n=60, start=date(2010, 1, 1)):
    months = months_range(start, n)
    nsa = {m: 0.5 + 0.05 * math.sin(i) for i, m in enumerate(months)}
    util = {m: (4.0 if m.month == 7 else 0.05) for m in months}
    return months, nsa, util


class SaxFormulaTest(unittest.TestCase):
    def test_sax_mm_matches_reconciliation_formula(self):
        months, nsa, util = world(24)
        out = tt.sax_mm(nsa, util, 0.10)
        for m in months:
            self.assertAlmostEqual(out[m], (nsa[m] - 0.10 * util[m]) / 0.90)

    def test_final_series_uses_all_months_and_drops_first(self):
        months, nsa, util = world(48)
        final = tt.sax_final(nsa, util, 0.10, fake_x13)
        self.assertEqual(sorted(final), months[1:])
        self.assertTrue(all(math.isfinite(v) for v in final.values()))

    def test_months_must_match(self):
        months, nsa, util = world(24)
        del util[months[5]]
        with self.assertRaises(ValueError):
            tt.sax_final(nsa, util, 0.10, fake_x13)


class VintageTest(unittest.TestCase):
    def test_vintage_covers_months_up_to_origin_minus_one(self):
        months, nsa, util = world(60)
        origin = months[50]
        vint = tt.sax_vintage(nsa, util, 0.10, origin, fake_x13)
        self.assertEqual(max(vint), months[49])
        self.assertEqual(min(vint), months[1])

    def test_no_leak_future_months_do_not_change_vintage(self):
        months, nsa, util = world(60)
        origin = months[40]
        clean = tt.sax_vintage(nsa, util, 0.10, origin, fake_x13)
        dirty_nsa = {m: (v * 5 if m >= origin else v) for m, v in nsa.items()}
        dirty_util = {m: (v + 3 if m >= origin else v) for m, v in util.items()}
        dirty = tt.sax_vintage(dirty_nsa, dirty_util, 0.10, origin, fake_x13)
        self.assertEqual(clean, dirty)

    def test_sensitivity_past_month_changes_vintage(self):
        months, nsa, util = world(60)
        origin = months[40]
        clean = tt.sax_vintage(nsa, util, 0.10, origin, fake_x13)
        bad = dict(nsa)
        bad[months[30]] += 1.0
        self.assertNotEqual(clean, tt.sax_vintage(bad, util, 0.10, origin, fake_x13))

    def test_history_is_revised_by_later_origins(self):
        months, nsa, util = world(60)
        early = tt.sax_vintage(nsa, util, 0.10, months[40], fake_x13)
        late = tt.sax_vintage(nsa, util, 0.10, months[55], fake_x13)
        common = [m for m in early if m in late]
        self.assertTrue(any(abs(early[m] - late[m]) > 1e-12 for m in common))

    def test_missing_util_for_last_published_month_gives_no_vintage(self):
        months, nsa, util = world(60)
        origin = months[40]
        del util[months[39]]  # допущение A1: u(O−1) недоступно
        with self.assertRaises(ValueError):
            tt.sax_vintage(nsa, util, 0.10, origin, fake_x13)

    def test_vintages_skip_unavailable_origins_and_report(self):
        months, nsa, util = world(60)
        del util[months[39]]
        vints, skipped = tt.sax_vintages(nsa, util, 0.10, [months[38], months[40], months[41]], fake_x13)
        self.assertIn(months[38], vints)
        self.assertNotIn(months[40], vints)
        self.assertEqual([o for o, _ in skipped], [months[40], months[41]])
        self.assertTrue(all(reason for _, reason in skipped))

    def test_min_history_is_enforced(self):
        months, nsa, util = world(60)
        with self.assertRaises(ValueError):
            tt.sax_vintage(nsa, util, 0.10, months[10], fake_x13)


class TariffCalendarTest(unittest.TestCase):
    def test_standard_dates_make_week_tariff(self):
        # Окно (prev, d]: июль, 1 июля внутри.
        self.assertTrue(tt.is_tariff_window(date(2018, 6, 25), date(2018, 7, 2), {}, date(2018, 12, 1)))
        self.assertFalse(tt.is_tariff_window(date(2018, 7, 2), date(2018, 7, 9), {}, date(2018, 12, 1)))
        self.assertTrue(tt.is_tariff_window(date(2018, 12, 31), date(2019, 1, 7), {}, date(2019, 6, 1)))

    def test_realized_utilities_month_adds_first_day(self):
        util = {date(2022, 12, 1): 8.54, date(2023, 7, 1): 0.0}
        # Окно с 1 декабря 2022: тарифное, пока месяц известен (≤ known_until) и u ≥ 1,5.
        self.assertTrue(tt.is_tariff_window(date(2022, 11, 28), date(2022, 12, 5), util, date(2023, 1, 1)))
        # Месяц ещё не известен к cutoff — реализованный `u` не используется.
        self.assertFalse(tt.is_tariff_window(date(2022, 11, 28), date(2022, 12, 5), util, date(2022, 11, 1)))
        # 1 июля 2023: стандартная дата — тарифное окно независимо от u (u=0 — только факт задним числом).
        self.assertTrue(tt.is_tariff_window(date(2023, 6, 26), date(2023, 7, 3), util, date(2023, 12, 1)))

    def test_small_utilities_month_is_not_tariff(self):
        util = {date(2021, 3, 1): 0.9}
        self.assertFalse(tt.is_tariff_window(date(2021, 2, 22), date(2021, 3, 1), util, date(2021, 6, 1)))


class V1Test(unittest.TestCase):
    def points(self):
        # Еженедельные понедельники 2018: базовая инфляция 100,1; неделя с 1 июля — 101,5.
        out, day = [], date(2018, 1, 1)
        while day < date(2018, 12, 31):
            value = 100.1
            if day == date(2018, 7, 2):
                value = 101.5
            out.append((day, value))
            day += timedelta(days=7)
        return out

    def test_tariff_week_replaced_by_same_year_median_of_non_tariff_weeks(self):
        points = self.points()
        out = tt.v1_adjust_points(points, date(2018, 12, 31), {}, date(2018, 12, 1))
        by_date = dict(out)
        self.assertAlmostEqual(by_date[date(2018, 7, 2)], 100.1)
        # Остальные точки (не 1 января и не 1 июля окна) не тронуты.
        self.assertEqual(by_date[date(2018, 3, 5)], 100.1)
        self.assertEqual(len(out), len(points))

    def test_median_is_taken_within_the_same_calendar_year(self):
        points, day = [], date(2017, 1, 2)
        while day < date(2018, 12, 31):
            base = 100.5 if day.year == 2017 else 100.1
            points.append((day, 101.5 if day == date(2018, 7, 2) else base))
            day += timedelta(days=7)
        out = dict(tt.v1_adjust_points(points, date(2018, 12, 31), {}, date(2018, 12, 1)))
        self.assertAlmostEqual(out[date(2018, 7, 2)], 100.1)  # медиана 2018 г., а не смесь с 2017
        self.assertAlmostEqual(out[date(2017, 7, 3)], 100.5)

    def test_as_of_median_uses_only_available_points(self):
        points = self.points()
        cutoff = date(2018, 7, 10)  # доступны точки date + 2 ≤ cutoff
        avail = wa.available_points(points, cutoff)
        out = tt.v1_adjust_points(avail, cutoff, {}, date(2018, 6, 1))
        # Будущие точки (после cutoff) не участвуют в медиане: подменяем их мусором — результат тот же.
        dirty = [(d, v * 3 if d + timedelta(days=2) > cutoff else v) for d, v in points]
        out_dirty = tt.v1_adjust_points(wa.available_points(dirty, cutoff), cutoff, {}, date(2018, 6, 1))
        self.assertEqual(out, out_dirty)

    def test_fallback_to_previous_non_tariff_points_when_year_has_none(self):
        # Первая точка года тарифная (окно с 1 января), других точек года ещё нет.
        points = [(date(2017, 12, 4) + timedelta(days=7 * i), 100.2) for i in range(30)]
        first_2018 = next(i for i, (d, _) in enumerate(points) if d.year == 2018)
        sub = points[: first_2018 + 1]
        sub[-1] = (sub[-1][0], 102.0)
        out = tt.v1_adjust_points(sub, sub[-1][0] + timedelta(days=2), {}, date(2018, 6, 1))
        self.assertAlmostEqual(out[-1][1], 100.2)

    def test_no_change_without_tariff_weeks(self):
        points = [(date(2018, 2, 5) + timedelta(days=7 * i), 100.1 + 0.01 * i) for i in range(6)]
        self.assertEqual(tt.v1_adjust_points(points, date(2018, 4, 1), {}, date(2018, 3, 1)), points)


class V2Test(unittest.TestCase):
    def months(self):
        return [wa.MonthPoint(date(2018, 6, 30), 100.5, date(2018, 6, 25), False), wa.MonthPoint(date(2018, 7, 31), 104.0, date(2018, 7, 30), False)]

    def test_subtracts_utilities_contribution(self):
        util = {date(2018, 6, 1): 0.1, date(2018, 7, 1): 4.0}
        out = tt.v2_adjust_months(self.months(), util, 0.10)
        self.assertAlmostEqual(out[0].value, 100 + (0.5 - 0.01) / 0.9)
        self.assertAlmostEqual(out[1].value, 100 + (4.0 - 0.4) / 0.9)
        self.assertEqual([m.date for m in out], [m.date for m in self.months()])

    def test_missing_utilities_for_a_month_is_error(self):
        with self.assertRaises(ValueError):
            tt.v2_adjust_months(self.months(), {date(2018, 6, 1): 0.1}, 0.10)

    def test_partial_month_is_not_adjusted_by_snapshot_hook(self):
        start = date(2009, 4, 6)
        points = [(start + timedelta(days=7 * i), 100.2 + 0.01 * (i % 5)) for i in range(220)]
        cutoff = points[200][0] + timedelta(days=2)

        def recorder(log):
            def runner(months):
                log.append([m.value for m in months])
                return [100.0 + 0.01 * i for i, _ in enumerate(months)]

            return runner

        util = {m: 0.5 for m in months_range(date(2009, 5, 1), 80)}
        adj_log, raw_log = [], []
        snap = wa.asof_snapshot(points, cutoff, recorder(adj_log), min_months=12, month_adjust=lambda ms: tt.v2_adjust_months(ms, util, 0.10))
        raw = wa.asof_snapshot(points, cutoff, recorder(raw_log), min_months=12)
        self.assertNotEqual(adj_log[0], raw_log[0])  # полные месяцы скорректированы
        self.assertIsNotNone(snap.partial)
        self.assertEqual(adj_log[1][-1], raw.partial.value)  # неполный месяц — сырой
        self.assertEqual(adj_log[1][:-1], adj_log[0])  # nowcast-фит: те же скорректированные полные месяцы
        self.assertEqual(snap.partial, raw.partial)


class VariantSnapshotTest(unittest.TestCase):
    def test_variant_names_and_unknown_variant(self):
        self.assertEqual(tt.VARIANTS, ("V0", "V1", "V2"))
        with self.assertRaises(ValueError):
            tt.asof_variant_snapshot([], date(2020, 1, 1), lambda m: [], "V9", {}, 0.1, date(2019, 12, 1))

    def test_v0_equals_plain_asof_snapshot(self):
        start = date(2009, 4, 6)
        points = [(start + timedelta(days=7 * i), 100.2 + 0.01 * (i % 5)) for i in range(220)]
        cutoff = points[200][0] + timedelta(days=2)

        def runner(months):
            return [100.0 + 0.001 * m.value for m in months]

        a = tt.asof_variant_snapshot(points, cutoff, runner, "V0", {}, 0.1, date(2019, 12, 1), min_months=12)
        b = wa.asof_snapshot(points, cutoff, runner, min_months=12)
        self.assertEqual(a, b)


if __name__ == "__main__":
    unittest.main()
