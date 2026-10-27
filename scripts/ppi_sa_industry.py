#!/usr/bin/env python3
"""Research: отраслевой потребительский ИЦП против product-level PPI SA."""

from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from datetime import date
from pathlib import Path

import openpyxl

from ppi_sa_probe import parse_weights

TOTAL_CODE = "1323500.029.31"
MONTHS = {
    "январь": 1, "февраль": 2, "март": 3, "апрель": 4,
    "май": 5, "июнь": 6, "июль": 7, "август": 8,
    "сентябрь": 9, "октябрь": 10, "ноябрь": 11, "декабрь": 12,
}
MISSING = {"", "...", "…", "-", "—", "х", "x"}
CONSUMER_WORDS = (
    "потребительск", "пищев", "напит", "одежд", "текстиль", "кож",
    "табач", "мебел", "полиграф", "фармацевт", "бытов",
)
NONFOOD_CODES = tuple(str(code) for code in range(13, 34) if code != 19)
VARIANT_CODES = {
    "food": ("10", "11"),
    "nonfood": NONFOOD_CODES,
    "core": ("10", "11") + NONFOOD_CODES,
    "food_no_alcohol": ("10",),
    "core_no_alcohol": ("10",) + NONFOOD_CODES,
}


def _month(value: object) -> int | None:
    text = re.sub(r"\d+\)", "", str(value or "").strip().lower()).strip()
    return MONTHS.get(text)


def _number(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("\xa0", "").replace(" ", "")
    if text.lower() in MISSING:
        return None
    normalized = text.replace(",", ".")
    match = re.fullmatch(r"([+-]?\d+(?:\.\d+)?)\d+\)", normalized)
    if match:
        normalized = match.group(1)
    try:
        return float(normalized)
    except ValueError as exc:
        raise ValueError(f"нечисловое значение ИЦП: {value!r}") from exc


def _sheet_year(rows: list[tuple], sheet: str) -> int:
    for row in rows[:5]:
        for cell in row:
            match = re.search(r"\bв\s+((?:19|20)\d{2})\s*г\.", str(cell or ""))
            if match:
                return int(match.group(1))
    raise ValueError(f"{sheet}: год листа не найден")


def load_ved_mm(path: Path) -> list[dict]:
    path = Path(path)
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    result: list[dict] = []
    seen: set[tuple[date, str]] = set()
    try:
        for sheet in (name for name in wb.sheetnames if re.fullmatch(r"[3-8]\.1", name)):
            rows = list(wb[sheet].iter_rows(values_only=True))
            year = _sheet_year(rows, sheet)
            header = next((i for i, row in enumerate(rows) if any(
                str(cell or "").strip().upper().replace(" ", "") == "ОКВЭД2" for cell in row
            )), None)
            if header is None:
                raise ValueError(f"{sheet}: заголовок ОКВЭД2 не найден")
            month_cols = {i: m for i, cell in enumerate(rows[header]) if (m := _month(cell))}
            current_code = ""
            current_name = ""
            for row in rows[header + 1:]:
                name = str(row[0] or "").strip()
                code = str(row[1] or "").strip() if len(row) > 1 else ""
                direct = bool(name and code and not code.startswith("Код"))
                if direct:
                    current_code, current_name = code, name
                elif name != "Российская Федерация" or not current_code:
                    continue
                for col, month in month_cols.items():
                    value = _number(row[col]) if col < len(row) else None
                    if value is None:
                        continue
                    observed = date(year, month, 1)
                    key = (observed, current_code)
                    if key in seen:
                        raise ValueError(f"дубль наблюдения {observed} {current_code}")
                    seen.add(key)
                    result.append({
                        "date": observed, "code": current_code,
                        "name": current_name, "value": value,
                    })
                if not direct:
                    current_code, current_name = "", ""
    finally:
        wb.close()
    return sorted(result, key=lambda r: (r["code"], r["date"]))


def load_weights(path: Path) -> dict[tuple[int, str], float]:
    return {(r["year"], r["code"]): r["weight"] for r in parse_weights(Path(path), "3")}


def weight_map(weights: dict[tuple[int, str], float], codes: tuple[str, ...], year: int) -> dict[str, float]:
    selected = {code: weights.get((year, code)) for code in codes}
    missing = [code for code, value in selected.items() if value is None]
    if missing:
        raise ValueError(f"нет весов {year}: {','.join(missing)}")
    if any(value < 0 for value in selected.values()):
        raise ValueError("отрицательный вес")
    total = sum(selected.values())
    if total <= 0:
        raise ValueError("сумма весов должна быть положительной")
    return {code: value / total for code, value in selected.items()}


def audit_candidates(rows: list[dict], weights: dict[tuple[int, str], float]) -> list[dict]:
    grouped: dict[tuple[str, str], list[date]] = defaultdict(list)
    for row in rows:
        if row["code"] == TOTAL_CODE:
            continue
        if any(word in row["name"].lower() for word in CONSUMER_WORDS):
            grouped[(row["code"], row["name"])].append(row["date"])
    result = []
    for (code, name), dates in sorted(grouped.items()):
        expected = (max(dates).year - min(dates).year) * 12 + max(dates).month - min(dates).month + 1
        result.append({
            "code": code, "name": name, "from": min(dates).isoformat(),
            "to": max(dates).isoformat(), "n": len(dates), "missing": expected - len(set(dates)),
            "has_weights_2026": (2026, code) in weights,
        })
    return result


def write_candidates(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["code", "name", "from", "to", "n", "missing", "has_weights_2026"])
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ved-xlsx", type=Path, default=Path("build/ppi-industry-sensitivity/raw/ved.xlsx"))
    parser.add_argument("--weights-xlsx", type=Path, default=Path("build/ppi-industry-sensitivity/raw/vesa.xlsx"))
    parser.add_argument("--anchors", type=Path, default=Path("docs/research/2026-10-21-ppi-sa-text-anchors-12.csv"))
    parser.add_argument("--output-prefix", type=Path, default=Path("docs/research/2026-10-21-ppi-industry-sensitivity"))
    parser.add_argument("--print", action="store_true")
    args = parser.parse_args()
    rows = load_ved_mm(args.ved_xlsx)
    candidates = audit_candidates(rows, load_weights(args.weights_xlsx))
    write_candidates(args.output_prefix.with_name("2026-10-21-ppi-ved-candidates.csv"), candidates)
    if args.print:
        print(f"VED rows: {len(rows)}; candidates: {len(candidates)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
