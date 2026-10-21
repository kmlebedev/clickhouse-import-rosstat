#!/usr/bin/env python3
"""Офлайн-тесты экспорта полной панели весов КИПЦ (T4)."""

from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ppi_sa_weights_panel as panel


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


class MappingGroupsTest(unittest.TestCase):
    def test_only_mapped_leaves(self) -> None:
        rows = [
            {"is_leaf": "1", "status": "mapped", "kipc_group": "01.2.1"},
            {"is_leaf": "1", "status": "mapped", "kipc_group": "01.2.2"},
            {"is_leaf": "0", "status": "mapped", "kipc_group": "01.2"},
            {"is_leaf": "1", "status": "excluded", "kipc_group": ""},
            {"is_leaf": "1", "status": "out_of_scope", "kipc_group": "04.4"},
            {"is_leaf": "1", "status": "mapped", "kipc_group": ""},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "mapping.csv"
            write_csv(path, ["is_leaf", "status", "kipc_group"], rows)
            self.assertEqual(panel.mapping_groups(path), {"01.2.1", "01.2.2"})

    def test_wanted_groups_adds_scope_and_subtract(self) -> None:
        rows = [{"is_leaf": "1", "status": "mapped", "kipc_group": "01.2.1"}]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "mapping.csv"
            write_csv(path, ["is_leaf", "status", "kipc_group"], rows)
            wanted = panel.wanted_groups(path)
            self.assertIn("01.2.1", wanted)
            self.assertIn("01.2", wanted)  # scope food
            self.assertIn("04.4", wanted)  # scope utilities
            self.assertIn("03.1.4", wanted)  # SUBTRACT_GROUPS


class SelectRowsTest(unittest.TestCase):
    def test_filters_and_sorts(self) -> None:
        rows = [
            {"period": "2026", "code": "01.2.2", "name": "b", "weight": "2"},
            {"period": "2022-04", "code": "01.2.1", "name": "a", "weight": "1"},
            {"period": "2026", "code": "99.9", "name": "no", "weight": "3"},
            {"period": "2026", "code": "01.2.1", "name": "a", "weight": "1"},
        ]
        got = panel.select_rows(rows, {"01.2.1", "01.2.2"})
        self.assertEqual(
            [(row["period"], row["code"]) for row in got],
            [("2022-04", "01.2.1"), ("2026", "01.2.1"), ("2026", "01.2.2")],
        )


class CheckPrimaryTest(unittest.TestCase):
    def test_missing_group_raises(self) -> None:
        rows = [{"period": "2026", "code": "01.2.1", "name": "a", "weight": "1"}]
        with self.assertRaises(ValueError) as ctx:
            panel.check_primary(rows, {"01.2.1", "09.3.2"})
        self.assertIn("09.3.2", str(ctx.exception))

    def test_other_period_does_not_count(self) -> None:
        rows = [{"period": "2025", "code": "09.3.2", "name": "a", "weight": "1"}]
        with self.assertRaises(ValueError):
            panel.check_primary(rows, {"09.3.2"})

    def test_complete(self) -> None:
        rows = [
            {"period": "2026", "code": "01.2.1", "name": "a", "weight": "1"},
            {"period": "2026", "code": "09.3.2", "name": "b", "weight": "2"},
        ]
        panel.check_primary(rows, {"01.2.1", "09.3.2"})


class WritePanelTest(unittest.TestCase):
    def test_deterministic_and_schema(self) -> None:
        rows = [
            {"period": "2026", "code": "01.2.1", "name": "a", "weight": "1"},
            {"period": "2022-04", "code": "01.2.1", "name": "a", "weight": "0.5"},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            first = Path(tmp) / "one.csv"
            second = Path(tmp) / "two.csv"
            panel.write_panel(first, rows)
            panel.write_panel(second, rows)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            with first.open(encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                self.assertEqual(reader.fieldnames, panel.FIELDS)
                self.assertEqual(len(list(reader)), 2)


if __name__ == "__main__":
    unittest.main()
