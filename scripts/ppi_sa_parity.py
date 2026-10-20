#!/usr/bin/env python3
"""T4: паритет `ppi_sa` с 10 текстовыми якорями «Инфляции в России».

Скрипт только читает `ppi_sa` (через `scripts/lib/clickhouse.py`) и пишет JSON
отчёта в `docs/research/`. В ClickHouse не пишет, DDL/GRANT не трогает.

Цель ±0,3 п.п. из research #275; если она не достигнута — это допустимый
результат T4: отчёт явно публикует расхождение, а не подгоняет веса.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "lib"))

import clickhouse  # noqa: E402

ANCHORS = ROOT.parent / "docs" / "research" / "2026-10-21-ppi-sa-anchors.csv"
OUTPUT = ROOT.parent / "docs" / "research" / "2026-10-21-ppi-sa-parity.json"
TOLERANCE_PP = 0.3
SERIES = ("food", "nonfood", "core")


def load_anchors(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    if len(rows) != 10:
        raise ValueError(f"{path}: {len(rows)} якорей, ожидалось 10")
    result = []
    for row in rows:
        result.append(
            {
                "issue": row["issue"].strip(),
                "target": row["target"].strip(),
                "anchor_series": row["anchor_series"].strip(),
                "series": row["series"].strip(),
                "anchor": float(row["value"]),
                "source": row["source"].strip(),
            }
        )
    return result


def query_rows() -> list[dict]:
    sql = """SELECT series,
                    target,
                    argMax(saar3, loaded_at) AS saar3
             FROM (
                 SELECT series,
                        toString(toStartOfMonth(date)) AS target,
                        saar3,
                        loaded_at
                 FROM ppi_sa FINAL
                 WHERE series IN ('food','nonfood','core') AND saar3 IS NOT NULL
             )
             GROUP BY series, target
             ORDER BY series, target
             FORMAT JSON"""
    return clickhouse.ch_query(sql)


def query_versions() -> dict:
    sql = """SELECT argMax(spec_version, loaded_at) AS spec_version,
                    argMax(taxonomy_version, loaded_at) AS taxonomy_version
             FROM ppi_sa FINAL
             FORMAT JSON"""
    rows = clickhouse.ch_query(sql)
    return rows[0] if rows else {}


def _stats(values: list[dict]) -> dict:
    if not values:
        return {"n": 0}
    diffs = [item["diff"] for item in values]
    mean_anchor = sum(item["anchor"] for item in values) / len(values)
    mean_ours = sum(item["ours"] for item in values) / len(values)
    cov = sum(
        (item["anchor"] - mean_anchor) * (item["ours"] - mean_ours) for item in values
    )
    var_anchor = sum((item["anchor"] - mean_anchor) ** 2 for item in values)
    var_ours = sum((item["ours"] - mean_ours) ** 2 for item in values)
    corr = cov / (var_anchor * var_ours) ** 0.5 if var_anchor > 0 and var_ours > 0 else None
    return {
        "n": len(values),
        "mae": sum(abs(value) for value in diffs) / len(diffs),
        "max_abs": max(abs(value) for value in diffs),
        "bias": sum(diffs) / len(diffs),
        "corr": corr,
    }


def compute(anchors: list[dict], rows: list[dict]) -> tuple[list[dict], dict, dict]:
    index = {(row["series"], row["target"]): float(row["saar3"]) for row in rows}
    result = []
    for anchor in anchors:
        key = (anchor["series"], anchor["target"])
        if key not in index:
            raise ValueError(f"нет ppi_sa для series={key[0]} target={key[1]}")
        ours = index[key]
        result.append({**anchor, "ours": ours, "diff": ours - anchor["anchor"]})
    by_series = {}
    for series in SERIES:
        subset = [item for item in result if item["series"] == series]
        if subset:
            by_series[series] = _stats(subset)
    return result, by_series, _stats(result)


def report(by_series: dict, overall: dict) -> None:
    print(f"{'series':10} {'n':>3} {'MAE':>8} {'max':>8} {'bias':>8} {'corr':>8}")
    for series, value in by_series.items():
        corr = value.get("corr")
        print(
            f"{series:10} {value['n']:3d} {value['mae']:8.3f} {value['max_abs']:8.3f} "
            f"{value['bias']:8.3f} {corr if corr is not None else float('nan'):8.3f}"
        )
    corr = overall.get("corr")
    print(
        f"{'overall':10} {overall['n']:3d} {overall['mae']:8.3f} {overall['max_abs']:8.3f} "
        f"{overall['bias']:8.3f} {corr if corr is not None else float('nan'):8.3f}"
    )
    reached = overall["mae"] <= TOLERANCE_PP
    print(
        f"tolerance ±{TOLERANCE_PP} п.п.: "
        f"{'достигнута' if reached else 'НЕ достигнута — расхождение публикуется'}"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--anchors", type=Path, default=ANCHORS)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()

    anchors = load_anchors(args.anchors)
    rows = query_rows()
    versions = query_versions()
    result, by_series, overall = compute(anchors, rows)

    payload = {
        "spec_version": versions.get("spec_version"),
        "taxonomy_version": versions.get("taxonomy_version"),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tolerance_pp": TOLERANCE_PP,
        "tolerance_reached": overall["mae"] <= TOLERANCE_PP,
        "overall": overall,
        "by_series": by_series,
        "rows": result,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    report(by_series, overall)
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
