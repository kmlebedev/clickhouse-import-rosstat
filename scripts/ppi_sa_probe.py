#!/usr/bin/env python3
"""Research probe for Rosstat PPI and CBR seasonally adjusted PPI.

Stages:
  fetch   download Rosstat workbooks, CBR methodology PDFs and inflation bulletins
  parse   parse workbooks into long CSVs and build aggregate monthly series
  pilot   run the R X-13 script and compare with CBR text anchors
  report  print a compact summary

The script writes only under build/ppi-sa-research/ and docs/research/*.json;
it is research tooling, not a production importer.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import date
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / "build" / "ppi-sa-research"
RAW = BUILD / "raw"
OUT = BUILD / "out"
R_LIB = BUILD / "r-lib"

ROSSTAT_PRICE = "https://rosstat.gov.ru/statistics/price"
ROSSTAT_INDUSTRY = "https://rosstat.gov.ru/enterprise_industrial"
ROSSTAT_FILES = {
    "ved": "https://rosstat.gov.ru/storage/mediabank/Proizvoditeli_Ind_VED_08-2026.xlsx",
    "tov": "https://rosstat.gov.ru/storage/mediabank/Proizvoditeli_Ind_tov_08-2026.xlsx",
    "cena": "https://rosstat.gov.ru/storage/mediabank/Proizvoditeli_Cena_08-2026.xlsx",
    "vesa": "https://rosstat.gov.ru/storage/mediabank/vesa_2006-2026.xlsx",
    # Веса ИПЦ в группировке КИПЦ (не веса ИЦП по ВЭД): нужны mapping T3 и панели T4.
    "vesa_kIPC": "https://rosstat.gov.ru/storage/mediabank/Vesa-tov-KIPC_2012-2026.xlsx",
    "sezon": "https://rosstat.gov.ru/storage/mediabank/sezon_2023_08-2026.xlsx",
    "concepts": "https://rosstat.gov.ru/storage/mediabank/Osnovnye%20ponyatiya_ICP_promtovary.pdf",
}
CBR_FILES = {
    "meth_cpi": "https://www.cbr.ru/Content/Document/File/108630/meth_cpi.pdf",
    "annex_2026": "https://www.cbr.ru/Content/Document/File/123575/Annex_2026.pdf",
    "cpd_2026_9": "https://www.cbr.ru/collection/collection/file/62446/cpd_2026-9.pdf",
}
CBR_INDEX = "https://www.cbr.ru/analytics/dkp/dinamic/"
MONTHS = {
    "январь": 1,
    "февраль": 2,
    "март": 3,
    "апрель": 4,
    "май": 5,
    "июнь": 6,
    "июль": 7,
    "август": 8,
    "сентябрь": 9,
    "октябрь": 10,
    "ноябрь": 11,
    "декабрь": 12,
}
TOTAL_CODE = "1323500.029.31"
SECTION_CODES = {"B", "C", "D", "E"}


def _float(value):
    if value is None:
        return None
    text = str(value).strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    if text in {"", "...", "…", "-", "—", "х", "X"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _month(value):
    text = str(value or "").strip().lower()
    text = re.sub(r"\d+\)", "", text).strip()
    return MONTHS.get(text)


def _year(value):
    if isinstance(value, int) and 1990 <= value <= 2100:
        return value
    text = str(value or "").strip()
    match = re.match(r"((?:19|20)\d{2})", text)
    return int(match.group(1)) if match else None


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _fetch(url: str, path: Path, evidence: list[dict], timeout: int = 180) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            "Accept": "*/*",
        },
    )
    started = time.time()
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
            status = response.status
            headers = dict(response.headers)
            body = response.read()
    except (urllib.error.URLError, TimeoutError) as exc:
        evidence.append({"url": url, "error": str(exc), "seconds": round(time.time() - started, 2)})
        raise
    path.write_bytes(body)
    evidence.append(
        {
            "url": url,
            "path": str(path.relative_to(ROOT)),
            "status": status,
            "content_type": headers.get("Content-Type"),
            "content_length": len(body),
            "etag": headers.get("ETag"),
            "last_modified": headers.get("Last-Modified"),
            "sha256": _sha256(path),
            "seconds": round(time.time() - started, 2),
            "observed_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }
    )
    return path


def fetch_all() -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)
    evidence: list[dict] = []
    for name, url in {**ROSSTAT_FILES, **CBR_FILES}.items():
        suffix = Path(url).suffix or ".bin"
        if name == "concepts":
            suffix = ".pdf"
        _fetch(url, RAW / f"{name}{suffix}", evidence)
    _fetch(ROSSTAT_PRICE, RAW / "rosstat_price.html", evidence)
    _fetch(ROSSTAT_INDUSTRY, RAW / "rosstat_industry.html", evidence)

    index = _fetch(CBR_INDEX, RAW / "cbr_infl_index.html", evidence)
    html = index.read_text("utf-8", errors="ignore")
    issues = sorted(
        set(re.findall(r"/analytics/dkp/dinamic/CPD_(\d{4})-(\d{1,2})/", html)),
        key=lambda item: (int(item[0]), int(item[1])),
    )[-12:]
    for year, month in issues:
        issue_url = f"https://www.cbr.ru/analytics/dkp/dinamic/CPD_{year}-{month}/"
        page = _fetch(issue_url, RAW / f"cpd_{year}-{month}.html", evidence)
        page_html = page.read_text("utf-8", errors="ignore")
        current = re.search(
            rf'href="([^"]*/CPD_{year}-{month}\.pdf)"', page_html, flags=re.IGNORECASE
        )
        if not current:
            continue
        _fetch(
            "https://www.cbr.ru" + current.group(1),
            RAW / f"cpd_{year}-{month}.pdf",
            evidence,
        )
    (OUT / "evidence.json").write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def parse_ved_months(path: Path, sheet: str) -> list[tuple[date, float]]:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb[sheet]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    header = None
    for idx, row in enumerate(rows):
        years = [_year(cell) for cell in row[1:]]
        if sum(year is not None for year in years) >= 5:
            header = idx
            break
    if header is None:
        raise ValueError(f"{path.name}:{sheet}: year header not found")
    years = [_year(cell) for cell in rows[header][1:]]
    points: list[tuple[date, float]] = []
    started = False
    for row in rows[header + 1 :]:
        label = str(row[0] or "").strip().lower()
        if "к предыдущему месяцу" in label:
            started = True
            continue
        if not started:
            continue
        month = _month(row[0])
        if month is None:
            if label.startswith("декабрь в %"):
                break
            continue
        for offset, year in enumerate(years):
            if year is None:
                continue
            value = _float(row[offset + 1]) if offset + 1 < len(row) else None
            if value is not None:
                points.append((date(year, month, 1), value))
    return points


def parse_ved_matrix(path: Path, sheet: str) -> dict[str, dict[int, float]]:
    """Parse a VED x months sheet (2021-2026) into code -> month -> m/m."""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb[sheet]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    header = None
    for idx, row in enumerate(rows):
        if any(str(cell or "").strip().upper().replace(" ", "") == "ОКВЭД2" for cell in row):
            header = idx
            break
    if header is None:
        raise ValueError(f"{path.name}:{sheet}: ОКВЭД2 header not found")
    month_cols: dict[int, int] = {}
    for col, cell in enumerate(rows[header]):
        month = _month(cell)
        if month is not None:
            month_cols[col] = month
    result: dict[str, dict[int, float]] = defaultdict(dict)
    current = None
    for row in rows[header + 1 :]:
        name = str(row[0] or "").strip()
        code = str(row[1] or "").strip() if len(row) > 1 else ""
        if name and code and not code.startswith("Код"):
            current = code
            continue
        if name == "Российская Федерация" and current:
            for col, month in month_cols.items():
                value = _float(row[col]) if col < len(row) else None
                if value is not None:
                    result[current][month] = value
            current = None
    return result


def parse_products(path: Path, sheet: str) -> list[dict]:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb[sheet]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    header = None
    for idx, row in enumerate(rows):
        if any("Код товара" in str(cell or "") for cell in row):
            header = idx
            break
    if header is None:
        raise ValueError(f"{path.name}:{sheet}: product header not found")
    month_cols = {}
    for col, cell in enumerate(rows[header]):
        month = _month(cell)
        if month is not None:
            month_cols[col] = month
    points: dict[tuple[str, int], dict] = {}
    current_code = None
    current_name = None

    def add(row, code, name):
        for col, month in month_cols.items():
            value = _float(row[col]) if col < len(row) else None
            if value is not None:
                points[(code, month)] = {
                    "code": code,
                    "name": name,
                    "month": month,
                    "value": value,
                }

    for row in rows[header + 1 :]:
        name = str(row[0] or "").strip()
        code = str(row[1] or "").strip() if len(row) > 1 else ""
        if name and code and "Код товара" not in code:
            current_code = code
            current_name = name
            add(row, code, name)
            continue
        if name == "Российская Федерация" and current_code:
            add(row, current_code, current_name)
            current_code = None
            current_name = None
    return list(points.values())


def parse_weights(path: Path, sheet: str) -> list[dict]:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb[sheet]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    def header_years(row):
        result = []
        for cell in row[2:]:
            year = _year(cell)
            if year is None:
                match = re.search(r"(20\d{2})", str(cell or ""))
                year = int(match.group(1)) if match else None
            result.append(year)
        return result

    header = None
    for idx, row in enumerate(rows):
        if sum(year is not None for year in header_years(row)) >= 5:
            header = idx
            break
    if header is None:
        raise ValueError(f"{path.name}:{sheet}: weight header not found")
    years = header_years(rows[header])
    result = []
    for row in rows[header + 1 :]:
        code = str(row[1] or "").strip()
        name = str(row[0] or "").strip()
        if not code or code.lower() == "код":
            continue
        for offset, year in enumerate(years):
            if year is None:
                continue
            value = _float(row[offset + 2]) if offset + 2 < len(row) else None
            if value is not None:
                result.append({"year": year, "code": code, "name": name, "weight": value})
    return result


def is_food(code: str) -> bool:
    return code.startswith("10.") or (code.startswith("11.") and not is_alcohol(code))


def is_alcohol(code: str) -> bool:
    return code.startswith(("11.01", "11.02", "11.03", "11.04", "11.05"))


def is_nonfood(code: str) -> bool:
    return code[:2] in {str(n) for n in range(13, 34)} and not code.startswith("19.20.21")


def is_nonfood_narrow(code: str) -> bool:
    """Final consumer non-food classes, used as a sensitivity check."""
    return code[:2] in {"13", "14", "15", "17", "18", "21", "31", "32"}


def build_products() -> dict[str, list[dict]]:
    sheets = {
        2021: "4",
        2022: "8",
        2023: "12",
        2024: "16",
        2025: "20",
        2026: "24",
    }
    result: dict[str, list[dict]] = defaultdict(list)
    for year, sheet in sheets.items():
        for item in parse_products(RAW / "tov.xlsx", sheet):
            result[year].append(item)
    return result


def build_series() -> dict[str, list[tuple[date, float]]]:
    ved = RAW / "ved.xlsx"
    total = parse_ved_months(ved, "2.1")
    total_2026 = parse_ved_matrix(ved, "8.1").get(TOTAL_CODE, {})
    total.extend((date(2026, month, 1), value) for month, value in total_2026.items())
    total.sort()

    sections: dict[str, list[tuple[date, float]]] = {}
    for code, sheet in {"B": "2.2", "C": "2.3", "D": "2.4", "E": "2.5"}.items():
        points = parse_ved_months(ved, sheet)
        points.extend(
            (date(2026, month, 1), value)
            for month, value in parse_ved_matrix(ved, "8.1").get(code, {}).items()
        )
        sections[code] = sorted(points)

    products = build_products()
    food = []
    nonfood = []
    nonfood_narrow = []
    for year, items in products.items():
        for item in items:
            if is_food(item["code"]):
                food.append((date(year, item["month"], 1), item["value"]))
            elif is_nonfood(item["code"]):
                nonfood.append((date(year, item["month"], 1), item["value"]))
                if is_nonfood_narrow(item["code"]):
                    nonfood_narrow.append((date(year, item["month"], 1), item["value"]))
    # Equal-weight monthly mean by date.
    def mean_by_date(points):
        buckets = defaultdict(list)
        for day, value in points:
            buckets[day].append(value)
        return sorted((day, sum(values) / len(values)) for day, values in buckets.items())

    food_series = mean_by_date(food)
    nonfood_series = mean_by_date(nonfood)
    nonfood_narrow_series = mean_by_date(nonfood_narrow)
    all_series = mean_by_date(food + nonfood)
    all_narrow_series = mean_by_date(food + nonfood_narrow)
    return {
        "total": total,
        "mining": sections["B"],
        "manufacturing": sections["C"],
        "electricity": sections["D"],
        "water": sections["E"],
        "food": food_series,
        "nonfood": nonfood_series,
        "nonfood_narrow": nonfood_narrow_series,
        "all": all_series,
        "all_narrow": all_narrow_series,
    }


def chain(points: list[tuple[date, float]], base: date | None = None) -> list[tuple[date, float]]:
    points = sorted(points)
    if not points:
        return []
    base = base or points[0]
    level = 100.0
    result = []
    previous = None
    for day, value in points:
        if previous is not None:
            level *= value / 100.0
        result.append((day, level))
        previous = day
    return result


def export_series() -> None:
    series = build_series()
    OUT.mkdir(parents=True, exist_ok=True)
    with (OUT / "series.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["series", "date", "level"])
        for name, points in series.items():
            for day, value in chain(points):
                writer.writerow([name, day.isoformat(), f"{value:.6f}"])
    with (OUT / "coverage.json").open("w", encoding="utf-8") as fh:
        json.dump(
            {
                name: {
                    "points": len(points),
                    "first": points[0][0].isoformat() if points else None,
                    "last": points[-1][0].isoformat() if points else None,
                }
                for name, points in series.items()
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )
    weights = parse_weights(RAW / "vesa.xlsx", "3")
    with (OUT / "weights.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["year", "code", "name", "weight"])
        writer.writeheader()
        writer.writerows(weights)
    print(json.dumps({"series": {k: len(v) for k, v in series.items()}, "weights": len(weights)}, ensure_ascii=False))


def _num(text: str) -> float:
    return float(text.replace("−", "-").replace(",", "."))


def _anchors_from_pdf(path: Path, issue: str) -> list[dict]:
    text_path = path.with_suffix(".txt")
    subprocess.run(["pdftotext", "-layout", str(path), str(text_path)], check=True)
    text = re.sub(r"\s+", " ", text_path.read_text("utf-8", errors="ignore"))
    year, month = (int(part) for part in issue.split("-"))
    target = date(year - 1, 12, 1) if month == 1 else date(year, month - 1, 1)
    found: dict[str, float] = {}

    def put(series: str, value: str) -> None:
        found.setdefault(series, _num(value))

    lower = text.lower()
    patterns = {
        "nonfood": [
            r"непродовольственн\w*.{0,140}?([-−]?\d+[.,]\d+)\s*%\s*\(?с\.к\.г",
            r"([-−]?\d+[.,]\d+)\s*%\s*(?:\(?с\.к\.г\.?\)?\s*)?(?:для\s*)?непродовольственн\w*",
        ],
        "food": [
            r"(?<!не)продовольственн\w*.{0,140}?([-−]?\d+[.,]\d+)\s*%\s*\(?с\.к\.г",
            r"([-−]?\d+[.,]\d+)\s*%\s*(?:\(?с\.к\.г\.?\)?\s*)?(?:для\s*)?(?<!не)продовольственн\w*",
        ],
        "all": [
            r"потребительских товаров.{0,320}?составил\s+([-−]?\d+[.,]\d+)\s*%\s*\(?с\.к\.г",
            r"в среднем за 3 месяца.{0,220}?составил\s+([-−]?\d+[.,]\d+)\s*%\s*\(?с\.к\.г",
        ],
    }
    for match in re.finditer(r"цен производителей", lower):
        window = lower[max(0, match.start() - 220) : match.start() + 950]
        if "продовольственн" not in window:
            continue
        for series, candidates in patterns.items():
            if series in found:
                continue
            for pattern in candidates:
                hit = re.search(pattern, window)
                if hit:
                    put(series, hit.group(1))
                    break
        if len(found) == 3:
            break
    return [
        {"issue": issue, "target": target.isoformat(), "series": series, "value": value}
        for series, value in sorted(found.items())
    ]


def parse_anchors() -> list[dict]:
    anchors = []
    for path in sorted(RAW.glob("cpd_*.pdf")):
        issue = path.stem.replace("cpd_", "")
        anchors.extend(_anchors_from_pdf(path, issue))
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "anchors.json").write_text(json.dumps(anchors, ensure_ascii=False, indent=2), encoding="utf-8")
    with (OUT / "anchors.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["issue", "target", "series", "value"])
        writer.writeheader()
        writer.writerows(anchors)
    print(json.dumps({"anchors": len(anchors)}, ensure_ascii=False))


def _shift_month(value: str, delta: int) -> str:
    year, month, _ = (int(part) for part in value.split("-"))
    index = year * 12 + (month - 1) + delta
    return f"{index // 12:04d}-{index % 12 + 1:02d}-01"


def compute_parity() -> None:
    rows = list(csv.DictReader((OUT / "sa.csv").open(encoding="utf-8")))
    anchors = list(csv.DictReader((OUT / "anchors.csv").open(encoding="utf-8")))
    sa: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    for row in rows:
        sa[(row["series"], row["variant"])][row["date"]] = float(row["sa"])
    variants = sorted({row["variant"] for row in rows})
    results = []
    for anchor in anchors:
        target = anchor["target"]
        series_list = [anchor["series"]]
        if anchor["series"] == "all":
            series_list.append("all_narrow")
        elif anchor["series"] == "nonfood":
            series_list.append("nonfood_narrow")
        expected = float(anchor["value"])
        for series in series_list:
            for variant in variants:
                values = sa.get((series, variant), {})
                current = values.get(target)
                previous = values.get(_shift_month(target, -3))
                if current is None:
                    continue
                saar3 = None
                for row in rows:
                    if row["series"] == series and row["variant"] == variant and row["date"] == target:
                        saar3 = float(row["saar3"]) if row["saar3"] not in {"", "NA"} else None
                        break
                saar3_level = None
                if previous:
                    saar3_level = ((current / previous) ** 4 - 1) * 100
                for formula, ours in (("mean3_saar", saar3), ("level3_annualized", saar3_level)):
                    if ours is None:
                        continue
                    results.append(
                        {
                            "issue": anchor["issue"],
                            "target": target,
                            "series": series,
                            "variant": variant,
                            "formula": formula,
                            "anchor": expected,
                            "ours": ours,
                            "diff": ours - expected,
                        }
                    )
    OUT.mkdir(parents=True, exist_ok=True)
    with (OUT / "parity.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["issue", "target", "series", "variant", "formula", "anchor", "ours", "diff"],
        )
        writer.writeheader()
        writer.writerows(results)

    def stats(values):
        if not values:
            return {"n": 0}
        diffs = [item["diff"] for item in values]
        mean_anchor = sum(item["anchor"] for item in values) / len(values)
        mean_ours = sum(item["ours"] for item in values) / len(values)
        cov = sum((item["anchor"] - mean_anchor) * (item["ours"] - mean_ours) for item in values)
        var_a = sum((item["anchor"] - mean_anchor) ** 2 for item in values)
        var_o = sum((item["ours"] - mean_ours) ** 2 for item in values)
        corr = cov / (var_a * var_o) ** 0.5 if var_a > 0 and var_o > 0 else None
        return {
            "n": len(values),
            "mae": sum(abs(value) for value in diffs) / len(diffs),
            "max_abs": max(abs(value) for value in diffs),
            "bias": sum(diffs) / len(diffs),
            "corr": corr,
        }

    summary = {}
    for variant in variants:
        for formula in ("mean3_saar", "level3_annualized"):
            subset = [item for item in results if item["variant"] == variant and item["formula"] == formula]
            summary[f"{variant}/{formula}"] = stats(subset)
            for series in ("all", "all_narrow", "food", "nonfood", "nonfood_narrow"):
                summary[f"{variant}/{formula}/{series}"] = stats(
                    [item for item in subset if item["series"] == series]
                )
    (OUT / "parity_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def run_r() -> None:
    env = dict(os.environ)
    env["R_LIBS_USER"] = str(R_LIB)
    subprocess.run(
        ["Rscript", str(ROOT / "scripts" / "ppi_sa_x13.R")],
        check=True,
        env=env,
        cwd=ROOT,
    )


def summary() -> None:
    coverage = json.loads((OUT / "coverage.json").read_text("utf-8"))
    anchors = json.loads((OUT / "anchors.json").read_text("utf-8")) if (OUT / "anchors.json").exists() else []
    print(json.dumps({"coverage": coverage, "anchors": anchors}, ensure_ascii=False, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["fetch", "parse", "pilot", "parity", "summary"])
    args = parser.parse_args()
    if args.stage == "fetch":
        fetch_all()
    elif args.stage == "parse":
        export_series()
        parse_anchors()
    elif args.stage == "pilot":
        run_r()
    elif args.stage == "parity":
        compute_parity()
    elif args.stage == "summary":
        summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
