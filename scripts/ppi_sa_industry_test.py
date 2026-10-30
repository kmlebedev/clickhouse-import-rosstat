#!/usr/bin/env python3
"""Офлайн-тесты research-разбора отраслевого ИЦП."""

from __future__ import annotations

import csv
import math
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

from openpyxl import Workbook

import ppi_sa_industry as m


class WorkbookCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    @staticmethod
    def ved_sheet(wb: Workbook, sheet: str, year: int, bad: object = None) -> None:
        ws = wb.create_sheet(sheet)
        ws.append([f"Индексы цен производителей в {year} г."])
        ws.append(["", "ОКВЭД 2", "январь", "февраль"])
        ws.append(["Производство пищевых продуктов", "10"])
        ws.append(["Российская Федерация", "", "100,5", bad if bad is not None else "…"])
        ws.append(["Производство напитков", "11"])
        ws.append(["Российская Федерация", "", "101,0", "-"])

    def ved_path(self, duplicate: bool = False, bad: object = None) -> Path:
        wb = Workbook()
        wb.remove(wb.active)
        self.ved_sheet(wb, "8.1", 2026, bad)
        if duplicate:
            self.ved_sheet(wb, "7.1", 2026)
        path = self.root / "ved.xlsx"
        wb.save(path)
        return path

    def direct_ved_path(self) -> Path:
        wb = Workbook()
        wb.remove(wb.active)
        ws = wb.create_sheet("3.1")
        ws.append(["Индексы цен производителей в 2021 г."])
        ws.append(["", "ОКВЭД2", "январь", "февраль"])
        ws.append(["Производство пищевых продуктов", 10, "100,5", "101,0"])
        ws.append(["Производство напитков", "11", "99,5", "…"])
        path = self.root / "ved-direct.xlsx"
        wb.save(path)
        return path

    def weights_path(self, values: tuple[object, object] = ("60,0", "40,0")) -> Path:
        wb = Workbook()
        ws = wb.active
        ws.title = "3"
        ws.append(["Удельный вес"])
        ws.append(["", "Код", "2022", "2023", "2024", "2025", "2026"])
        ws.append(["Пищевые продукты", "10", 1, 1, 1, 1, values[0]])
        ws.append(["Напитки", "11", 1, 1, 1, 1, values[1]])
        path = self.root / "weights.xlsx"
        wb.save(path)
        return path


class VedLoadTest(WorkbookCase):
    def test_loads_code_name_date_and_skips_missing(self) -> None:
        rows = m.load_ved_mm(self.ved_path())
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], {
            "date": date(2026, 1, 1), "code": "10",
            "name": "Производство пищевых продуктов", "value": 100.5,
        })
        self.assertEqual(rows[1]["code"], "11")
        self.assertNotIn(date(2026, 2, 1), {r["date"] for r in rows})

    def test_loads_pre2025_rows_without_region_row(self) -> None:
        rows = m.load_ved_mm(self.direct_ved_path())
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0], {
            "date": date(2021, 1, 1), "code": "10",
            "name": "Производство пищевых продуктов", "value": 100.5,
        })
        self.assertIn(date(2021, 2, 1), {r["date"] for r in rows if r["code"] == "10"})

    def test_rejects_nonnumeric_value(self) -> None:
        with self.assertRaisesRegex(ValueError, "нечислов"):
            m.load_ved_mm(self.ved_path(bad="ошибка"))

    def test_rejects_duplicate_date_code(self) -> None:
        with self.assertRaisesRegex(ValueError, "дубль"):
            m.load_ved_mm(self.ved_path(duplicate=True))


class WeightsTest(WorkbookCase):
    def test_loads_and_normalizes_weights(self) -> None:
        weights = m.load_weights(self.weights_path())
        self.assertEqual(weights[(2026, "10")], 60.0)
        self.assertEqual(m.weight_map(weights, ("10", "11"), 2026), {"10": 0.6, "11": 0.4})

    def test_rejects_zero_weight_sum(self) -> None:
        weights = m.load_weights(self.weights_path((0, 0)))
        with self.assertRaisesRegex(ValueError, "сумма весов"):
            m.weight_map(weights, ("10", "11"), 2026)

    def test_rejects_negative_weight(self) -> None:
        weights = m.load_weights(self.weights_path((-1, 2)))
        with self.assertRaisesRegex(ValueError, "отрицатель"):
            m.weight_map(weights, ("10", "11"), 2026)


class AuditTest(WorkbookCase):
    def test_candidates_exclude_total_and_report_coverage(self) -> None:
        rows = m.load_ved_mm(self.ved_path())
        rows.append({"date": date(2026, 1, 1), "code": "1323500.029.31", "name": "Промышленность", "value": 100.0})
        weights = m.load_weights(self.weights_path())
        candidates = m.audit_candidates(rows, weights)
        self.assertEqual([r["code"] for r in candidates], ["10", "11"])
        self.assertEqual(candidates[0]["from"], "2026-01-01")
        self.assertEqual(candidates[0]["to"], "2026-01-01")
        self.assertEqual(candidates[0]["n"], 1)
        self.assertTrue(candidates[0]["has_weights_2026"])

    def test_candidates_report_internal_gaps(self) -> None:
        rows = [
            {"date": date(2026, month, 1), "code": "10", "name": "Производство пищевых продуктов", "value": 100.0}
            for month in (1, 3)
        ]
        candidates = m.audit_candidates(rows, {})
        self.assertEqual(candidates[0].get("missing"), 1)

    def test_preregistered_variant_membership(self) -> None:
        nonfood = tuple(str(code) for code in range(13, 34) if code != 19)
        self.assertEqual(getattr(m, "VARIANT_CODES", {}), {
            "food": ("10", "11"),
            "nonfood": nonfood,
            "core": ("10", "11") + nonfood,
            "food_no_alcohol": ("10",),
            "core_no_alcohol": ("10",) + nonfood,
        })

    def test_audit_includes_used_codes_and_food_subcodes(self) -> None:
        rows = [
            {"date": date(2026, 1, 1), "code": "20", "name": "Производство химических веществ", "value": 100.0},
            {"date": date(2026, 1, 1), "code": "10.2", "name": "Переработка и консервирование рыбы", "value": 101.0},
            {"date": date(2026, 1, 1), "code": "99", "name": "Посторонний ряд", "value": 102.0},
        ]
        candidates = {row["code"]: row for row in m.audit_candidates(rows, {})}
        self.assertEqual(set(candidates), {"10.2", "20"})
        self.assertTrue(candidates["20"]["used"])
        self.assertFalse(candidates["10.2"]["used"])


class ProvenanceTest(unittest.TestCase):
    def test_verify_sha256_rejects_changed_vintage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "vintage.xlsx"
            path.write_bytes(b"abc")
            self.assertEqual(
                m.verify_sha256(path, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            )
            with self.assertRaisesRegex(ValueError, "SHA-256"):
                m.verify_sha256(path, "0" * 64)


def add_months(day: date, months: int) -> date:
    month = day.month - 1 + months
    return date(day.year + month // 12, month % 12 + 1, 1)


def monthly_rows(codes: tuple[str, ...], values: dict[str, float], months: int = 36) -> list[dict]:
    return [
        {
            "date": add_months(date(2021, 1, 1), month), "code": code,
            "name": code, "value": values.get(code, 1.0),
        }
        for code in codes
        for month in range(months)
    ]


def anchors_path() -> Path:
    return Path(__file__).resolve().parent.parent / "docs" / "research" / "2026-10-21-ppi-sa-text-anchors-12.csv"


class AggregateTest(unittest.TestCase):
    def test_aggregate_full_panel_with_weights(self) -> None:
        rows = monthly_rows(("10", "11"), {"10": 1.0, "11": 3.0})
        weights = {(2026, "10"): 60.0, (2026, "11"): 40.0}
        result = m.aggregate_mm(rows, weights, year=2026)
        self.assertEqual(len(result), 36)
        self.assertAlmostEqual(result[date(2021, 1, 1)], 1.8)

    def test_aggregate_rejects_missing_component_month(self) -> None:
        rows = monthly_rows(("10", "11"), {"10": 1.0, "11": 3.0})
        rows = [row for row in rows if not (row["code"] == "11" and row["date"] == date(2021, 5, 1))]
        weights = {(2026, "10"): 60.0, (2026, "11"): 40.0}
        with self.assertRaisesRegex(ValueError, "полн"):
            m.aggregate_mm(rows, weights, year=2026)

    def test_levels_require_continuous_36_months_and_positive(self) -> None:
        points = {add_months(date(2021, 1, 1), month): 101.0 for month in range(36)}
        levels = m.levels_from_mm(points)
        self.assertEqual(levels[date(2021, 1, 1)], 100.0)
        self.assertAlmostEqual(levels[date(2021, 2, 1)], 101.0)

        short = dict(list(points.items())[:35])
        with self.assertRaisesRegex(ValueError, "36"):
            m.levels_from_mm(short)

        gapped = {add_months(date(2021, 1, 1), month): 1.0 for month in range(37)}
        gapped.pop(date(2021, 5, 1))
        with self.assertRaisesRegex(ValueError, "непрерыв"):
            m.levels_from_mm(gapped)

        broken = dict(points)
        broken[date(2021, 2, 1)] = -100.0
        with self.assertRaisesRegex(ValueError, "положительн"):
            m.levels_from_mm(broken)


class FormulaTest(unittest.TestCase):
    def test_mm_and_saar(self) -> None:
        levels = {
            date(2021, 1, 1): 100.0,
            date(2021, 2, 1): 110.0,
            date(2021, 3, 1): 132.0,
        }
        mm_sa = m.mm_from_levels(levels)
        self.assertEqual(mm_sa[date(2021, 1, 1)], None)
        self.assertAlmostEqual(mm_sa[date(2021, 2, 1)], 10.0)
        self.assertAlmostEqual(mm_sa[date(2021, 3, 1)], 20.0)
        saar = m.saar_from_mm(mm_sa)
        self.assertAlmostEqual(saar[date(2021, 2, 1)], ((1.1**12) - 1) * 100)

    def test_saar3_is_mean_of_available_not_cagr(self) -> None:
        levels = {
            date(2021, 1, 1): 100.0,
            date(2021, 2, 1): 100.0,
            date(2021, 3, 1): 110.0,
            date(2021, 4, 1): 132.0,
        }
        mm_sa = m.mm_from_levels(levels)
        saar = m.saar_from_mm(mm_sa)
        saar3 = m.saar3_from_mm(mm_sa)
        self.assertNotIn(date(2021, 1, 1), saar3)
        self.assertAlmostEqual(saar3[date(2021, 2, 1)], saar[date(2021, 2, 1)])
        expected = (saar[date(2021, 2, 1)] + saar[date(2021, 3, 1)]) / 2
        self.assertAlmostEqual(saar3[date(2021, 3, 1)], expected)
        self.assertNotAlmostEqual(saar3[date(2021, 3, 1)], ((1.32 ** 6) - 1) * 100)


class X13Test(unittest.TestCase):
    def test_uses_seats_log_cal_output(self) -> None:
        levels = {add_months(date(2021, 1, 1), month): 100.0 + month for month in range(36)}

        def fake_run(command, **kwargs):
            output = Path(command[-1])
            with output.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(["series", "variant", "date", "sa", "mm_sa", "saar", "saar3"])
                for day, value in levels.items():
                    writer.writerow(["industry", "seats_level_cal", day.isoformat(), value + 1000, "", "", ""])
                    writer.writerow(["industry", "seats_log_cal", day.isoformat(), value, "", "", ""])

        with mock.patch.object(m.subprocess, "run", side_effect=fake_run) as run:
            result = m.seasonal_adjust(levels)
        self.assertEqual(result, levels)
        self.assertIn("ppi_sa_x13.R", " ".join(run.call_args.args[0]))
        self.assertIn("R_LIBS_USER", run.call_args.kwargs["env"])

    def test_rejects_empty_r_output(self) -> None:
        levels = {add_months(date(2021, 1, 1), month): 100.0 for month in range(36)}

        def fake_run(command, **kwargs):
            Path(command[-1]).write_text(
                "series,variant,date,sa,mm_sa,saar,saar3\n", encoding="utf-8"
            )

        with mock.patch.object(m.subprocess, "run", side_effect=fake_run):
            with self.assertRaisesRegex(ValueError, "пуст"):
                m.seasonal_adjust(levels)


class VariantOrderTest(unittest.TestCase):
    def test_direct_and_indirect_apply_adjustment_in_different_order(self) -> None:
        rows = monthly_rows(("10", "11"), {"10": 102.0, "11": 104.0}, months=68)
        weights = {(2026, "10"): 60.0, (2026, "11"): 40.0}

        def nonlinear(levels: dict[date, float]) -> dict[date, float]:
            return {day: value * value / 100.0 for day, value in levels.items()}

        direct = m.direct_saar3(rows, weights, ("10", "11"), 2026, adjust=nonlinear)
        indirect = m.indirect_saar3(rows, weights, ("10", "11"), 2026, adjust=nonlinear)
        self.assertEqual(set(direct), set(indirect))
        self.assertNotEqual(direct[date(2026, 8, 1)], indirect[date(2026, 8, 1)])
        self.assertTrue(all(math.isfinite(value) for value in indirect.values()))

    def test_build_variants_maps_no_alcohol_sensitivity(self) -> None:
        codes = m.VARIANT_CODES["core"]
        rows = monthly_rows(codes, {"10": 102.0, "11": 104.0}, months=68)
        weights = {(2026, code): 1.0 for code in codes}
        variants = m.build_variants(rows, weights, 2026, adjust=lambda levels: levels)
        self.assertEqual(
            set(variants),
            {"direct", "indirect", "direct_no_alcohol", "indirect_no_alcohol"},
        )
        self.assertEqual(set(variants["direct"]), {"food", "nonfood", "core"})
        self.assertNotEqual(
            variants["direct"]["food"][date(2026, 8, 1)],
            variants["direct_no_alcohol"]["food"][date(2026, 8, 1)],
        )


class AnchorContractTest(unittest.TestCase):
    def test_contract_counts_and_baseline(self) -> None:
        anchors = m.load_anchors(anchors_path())
        self.assertEqual(len(anchors), 37)
        self.assertEqual(sum(row["kind"] == "exact" for row in anchors), 11)
        self.assertEqual(sum(row["kind"] not in ("exact", "not_stated") for row in anchors), 14)
        self.assertEqual(sum(row["kind"] == "not_stated" for row in anchors), 12)

        baseline = m.baseline_metrics(anchors)
        self.assertAlmostEqual(baseline["exact"]["overall"]["mae"], 1.538, places=3)
        self.assertEqual(baseline["exact"]["overall"]["n"], 11)
        self.assertEqual(baseline["qualitative"]["summary"], {"pass": 6, "fail": 8, "n/a": 0})

    def test_compare_excludes_qualitative_from_exact_and_rejects_missing_value(self) -> None:
        anchors = m.load_anchors(anchors_path())
        values: dict[str, dict[date, float]] = {}
        for row in anchors:
            values.setdefault(row["series"], {})[date.fromisoformat(row["target"])] = 0.0
        report = m.compare_anchors({"fake": values}, anchors_path())["variants"]["fake"]
        self.assertEqual(report["exact"]["overall"]["n"], 11)
        self.assertEqual(sum(report["qualitative"]["summary"].values()), 14)

        broken = {series: dict(points) for series, points in values.items()}
        broken["food"].pop(date(2026, 4, 1))
        with self.assertRaisesRegex(ValueError, "нет saar3"):
            m.compare_anchors({"broken": broken}, anchors_path())

    def test_rejects_incompatible_anchor_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "anchors.csv"
            path.write_text(
                "issue,url,target,window,series,kind,text,anchor_value,ours,diff,threshold,result,note\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "якор"):
                m.load_anchors(path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
