#!/usr/bin/env python3
"""P0.4: сверка live-весов ИПЦ с архивом CB_COMP и покрытие 2025–2026.

Пункт `key-rate/phase-4-cb-comp-88-groups-live` P0.4, issue #479. Спека —
`docs/superpowers/specs/2026-11-01-cb-comp-cpi-weights-design.md`.

* **2016–2024** — `max |Δ|` весов 88 групп + ЖКУ `9470` в витрине
  `v_rosstat_cpi_weights` (source=`live`) против архива `cpi_weights_tovar.xlsx`;
  отсутствующие/лишние коды перечисляются. Гейт — 0 (допуск 1e-12).
* **2025–2026** — сумма товарного листа (100 ± 0,5), суммы корзины модели,
  изменения год-к-году по коду (отчёт).
* **Покрытие** 2025–2026 по месяцам: доля веса групп, у которых есть SA в
  панели; порог 0,85. Политика 803/930.АГ/9260/9218 — в отчёте.

Только чтение ClickHouse и локальных файлов; в БД не пишет.
Запуск — `make cb-comp-weights-check`, тесты — `make cb-comp-weights-check-test`.
"""

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_parity as cp

ARCHIVE_YEARS = tuple(range(2016, 2025))
LIVE_YEARS = bu.LIVE_WEIGHT_YEARS
SPECIAL_CODES = ("803", "930.АГ", "9260", "9218")
TOLERANCE = 1e-12
COVERAGE_MIN = 0.85
PANEL_CSV = ROOT / "build" / "cb-comp-groups" / "panel.csv"
OUT_DIR = ROOT / "build" / "cb-comp-weights"


def compare_weights(live: dict, archive: dict) -> dict:
    """Сверка по годам: пересечение кодов, `max |Δ|`, отсутствующие и лишние."""
    result = {}
    for year in sorted(set(live) & set(archive)):
        live_year, archive_year = live[year], archive[year]
        common = sorted(set(live_year) & set(archive_year))
        diffs = {code: abs(float(live_year[code]) - float(archive_year[code])) for code in common}
        result[str(year)] = {
            "common": len(common),
            "missing_live": sorted(set(archive_year) - set(live_year)),
            "extra_live": sorted(set(live_year) - set(archive_year)),
            "max_abs_diff": max(diffs.values()) if diffs else None,
            "worst_code": max(diffs, key=diffs.get) if diffs else None,
            "sum_live": sum(float(live_year[code]) for code in common),
            "sum_archive": sum(float(archive_year[code]) for code in common),
        }
    missing_years = sorted(set(archive) - set(live))
    return {"years": result, "missing_years": missing_years}


def compare_gate(comparison: dict, years=ARCHIVE_YEARS) -> tuple:
    """Гейт сверки: каждый год есть, `max |Δ| ≤ 1e-12`, нет отсутствующих кодов."""
    problems = []
    for year in years:
        block = comparison["years"].get(str(year))
        if block is None:
            problems.append(f"{year}: нет года")
            continue
        if block["missing_live"]:
            problems.append(f"{year}: в live нет кодов {block['missing_live'][:8]}")
        if block["max_abs_diff"] is None or block["max_abs_diff"] > TOLERANCE:
            problems.append(f"{year}: max |Δ| = {block['max_abs_diff']} (worst {block['worst_code']})")
    return (not problems, problems)


def live_sums(query=None) -> list:
    """Суммы товарного листа по (год, вариант): 100 ± 0,5 без итоговой строки."""
    if query is None:
        from lib.clickhouse import ch_query as query

    rows = query(
        "SELECT year, variant, sum(weight) AS total, count() AS n FROM v_rosstat_cpi_weights "
        "WHERE source_file = 'CPR_tov_RF' AND code != '1' AND year >= 2016 "
        "GROUP BY year, variant ORDER BY year, variant FORMAT JSON"
    )
    return [
        {
            "year": int(row["year"]),
            "variant": str(row["variant"]),
            "total": float(row["total"]),
            "n": int(row["n"]),
        }
        for row in rows
    ]


def coverage_report(weights: dict, sa_full: dict, codes: list, months: list) -> list:
    """Покрытие по месяцам: доля веса групп с SA и весом, число групп, корзина."""
    result = []
    for month in months:
        weight_month = bu.weight_for_month(weights, month)
        covered_codes = [code for code in codes if month in sa_full.get(code, {})]
        covered = sum(weight_month.get(code, 0.0) for code in covered_codes)
        basket = sum(weight_month.get(code, 0.0) for code in codes)
        result.append({
            "month": month.isoformat(),
            "weight": covered,
            "groups": len(covered_codes),
            "basket": basket,
            "share": covered / basket if basket else 0.0,
        })
    return result


def special_codes_report(weights: dict, sa_full: dict, months: list) -> dict:
    """Статус 803/930.АГ/9260/9218: вес по годам и наличие SA в месяцах."""
    result = {}
    for code in SPECIAL_CODES:
        years = {str(year): table.get(code) for year, table in sorted(weights.items())}
        result[code] = {
            "weight_by_year": years,
            "months_with_sa": sum(1 for month in months if month in sa_full.get(code, {})),
            "months": len(months),
        }
    return result


def build_report(archive: Path, panel_csv: Path, years=LIVE_YEARS) -> dict:
    specs = bu.load_specs(archive)
    codes = [spec.code for spec in specs]
    archive_weights = bu.load_weights(archive, ARCHIVE_YEARS, specs, source="archive")
    live_weights = bu.load_weights(archive, years, specs, source="live")
    if not live_weights:
        raise SystemExit(
            "v_rosstat_cpi_weights пуста: сначала `make import STAT=rosstat_cpi_weights` "
            "(P0.4; источник rosstat.gov.ru на 2026-11-01 недоступен)"
        )
    comparison = compare_weights(live_weights, archive_weights)
    gate_ok, problems = compare_gate(comparison)

    sums = live_sums()
    sums_ok = all(abs(item["total"] - 100.0) <= 0.5 for item in sums)
    sa_full, _ = lb.load_group_panel(panel_csv)
    last = max(month for values in sa_full.values() for month in values)
    months = cp.month_range(date(2025, 1, 1), min(last, date(2026, 12, 1)))
    coverage = coverage_report({year: table for year, table in live_weights.items() if year >= 2024},
                               sa_full, codes + [bu.UTILITIES_CODE], months)
    coverage_ok = all(item["weight"] >= COVERAGE_MIN for item in coverage)
    special = special_codes_report(live_weights, sa_full, months)
    return {
        "spec": "docs/superpowers/specs/2026-11-01-cb-comp-cpi-weights-design.md",
        "issue": 479,
        "years": {"archive": list(ARCHIVE_YEARS), "live": list(years)},
        "comparison": comparison,
        "gate": {"ok": gate_ok and sums_ok and coverage_ok, "problems": problems,
                 "weights_match": gate_ok, "sums_ok": sums_ok, "coverage_ok": coverage_ok},
        "sums": sums,
        "coverage": coverage,
        "special": special,
    }


def run(archive: Path, panel_csv: Path, out_dir: Path) -> dict:
    report = build_report(archive, panel_csv)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "check.json").write_text(
        json.dumps(lb._jsonable(report), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None)
    parser.add_argument("--panel", default=str(PANEL_CSV))
    parser.add_argument("--out", default=str(OUT_DIR))
    args = parser.parse_args(argv)
    archive = cp.resolve_archive(args.archive)
    report = run(archive, Path(args.panel), Path(args.out))
    print(json.dumps(lb._jsonable({"gate": report["gate"], "sums": report["sums"],
                                   "coverage_min": min(item["weight"] for item in report["coverage"])}),
                     ensure_ascii=False, indent=2))
    return 0 if report["gate"]["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
