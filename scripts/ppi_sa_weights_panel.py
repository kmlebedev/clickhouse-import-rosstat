#!/usr/bin/env python3
"""T4: полная панель весов КИПЦ для production-слоя `ppi_sa`.

Handoff-панель T3 (`2026-10-20-ppi-kipc-weights-panel.csv`) фильтрована по
scope-группам и не содержит весов дочерних групп `01.2.1`…`01.2.6` и
`09.3.2`, в которые mapping v2 маппит листья. Здесь панель строится по всем
группам mapping v2: так primary-агрегация (фиксированные веса 2026) видит
каждый mapped-лист, а исторические периоды остаются для provenance и
будущей sensitivity-абляции.

Скрипт только читает публичный XLSX Росстата и пишет CSV в `docs/research/`;
в ClickHouse не пишет, DDL/GRANT не трогает.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ppi_sa_mapping as mapping

OUT = mapping.ROOT / "docs" / "research" / "2026-10-21-ppi-sa-weights-panel.csv"
PRIMARY_PERIOD = "2026"
FIELDS = ["period", "code", "name", "weight"]


def mapping_groups(mapping_file: Path) -> set[str]:
    """Группы КИПЦ, в которые mapping v2 маппит листья."""
    with mapping_file.open(encoding="utf-8", newline="") as fh:
        return {
            row["kipc_group"]
            for row in csv.DictReader(fh)
            if row["is_leaf"] == "1" and row["status"] == "mapped" and row["kipc_group"]
        }


def wanted_groups(mapping_file: Path) -> set[str]:
    """Группы mapping + scope-группы + вычитаемые группы."""
    groups = mapping_groups(mapping_file)
    groups |= {group for values in mapping.SCOPE_GROUPS.values() for group in values}
    groups |= {sub for values in mapping.SUBTRACT_GROUPS.values() for sub in values}
    return groups


def select_rows(panel: list[dict], wanted: set[str]) -> list[dict]:
    """Только нужные группы, стабильный порядок `period,code`."""
    return sorted(
        (row for row in panel if row["code"] in wanted),
        key=lambda row: (row["period"], row["code"]),
    )


def check_primary(rows: list[dict], groups: set[str], period: str = PRIMARY_PERIOD) -> None:
    """У каждой mapped-группы обязан быть вес primary-периода."""
    present = {row["code"] for row in rows if row["period"] == period}
    missing = sorted(groups - present)
    if missing:
        raise ValueError(f"{period}: нет весов для групп mapping: {', '.join(missing)}")


def write_panel(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows({field: row[field] for field in FIELDS} for row in rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--weights", type=Path, default=mapping.WEIGHTS_FILE)
    parser.add_argument("--mapping", type=Path, default=mapping.MAPPING_FILE)
    parser.add_argument("--output", type=Path, default=OUT)
    args = parser.parse_args()

    if not args.weights.exists():
        raise SystemExit(
            f"{args.weights}: файл не найден; скачайте исходник: "
            f"python3 scripts/ppi_sa_probe.py fetch"
        )

    groups = mapping_groups(args.mapping)
    wanted = wanted_groups(args.mapping)
    panel = mapping.parse_kipc_panel(args.weights)
    rows = select_rows(panel, wanted)
    check_primary(rows, groups)
    write_panel(args.output, rows)

    periods = sorted({row["period"] for row in rows})
    primary = [row for row in rows if row["period"] == PRIMARY_PERIOD]
    print(
        f"wrote {len(rows)} rows, {len({row['code'] for row in rows})} codes, "
        f"{len(periods)} periods to {args.output}"
    )
    print(f"primary {PRIMARY_PERIOD}: {len(primary)} weights for {len(groups)} mapping groups")
    return 0


if __name__ == "__main__":
    sys.exit(main())
