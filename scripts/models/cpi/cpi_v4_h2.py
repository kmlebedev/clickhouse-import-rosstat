#!/usr/bin/env python3
"""Решение gate эксперимента 2 (H2): `wkagg_*` на T_NSA (#396 Task 6).

Пункт `key-rate/phase-4-cpi-v4-weekly-aggregate-core`, роль ML. Спека
`docs/superpowers/specs/2026-10-28-cpi-v4-weekly-aggregate-core-design.md`
§7 и поправка §7.2 (однозначное чтение пяти условий). Скрипт ничего не
обучает: читает ячейки `cpi_v4_grid` (`cell_T_NSA_h*.json` и `*_predictions.csv`)
из `build/cpi-v4/<выборка>_full/` для `S_L09`, `S_L13`, `S_W22` и пишет
gate-артефакт `build/ml-gate/cpi_v4_h2.json` и сводку `.md`. В ClickHouse не
обращается, `forecast_log` не пишет. `make ml-gate` для разных имён моделей
недоступен (follow-up #184), решение считается здесь по спеке.

Запуск — `make cpi-v4-h2`, тесты — `make cpi-v4-h2-test`.
"""

import argparse
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_v4_leaderboard as lb

REFERENCE = "v2_bottomup"
V3 = "v3_bottomup"
CANDIDATES = ("v4a_bottomup", "v4b_bottomup")
TARGET = "T_NSA"
ALPHA = 0.05
SAMPLES = ("S_L09", "S_L13", "S_W22")
PRIMARY_HORIZONS = (1, 3)
CONDITIONS = {
    1: "ΔRMSE < 0 против v2_bottomup на S_L09 в h1 и h3",
    2: "S_L09 h1: DM p < 0,05 и 95 % CI ΔRMSE не содержит 0",
    3: "знак ΔRMSE < 0 в h1 и h3 на S_L13 и S_W22",
    4: "кандидат в MCS-90 % S_L09 h1 и h3",
    5: "на S_L09 h6 нет значимого ухудшения (p < 0,05, ΔRMSE > 0)",
}


def required_cells() -> list:
    return [("S_L09", h) for h in (1, 3, 6)] + [(s, h) for s in ("S_L13", "S_W22") for h in PRIMARY_HORIZONS]


def _pair(cell: dict, candidate: str) -> dict | None:
    return (cell.get("pairs") or {}).get(f"{candidate} vs {REFERENCE}")


def _usable(cell) -> bool:
    return bool(cell) and cell.get("status") == "ok"


def relative_effect(cell: dict, candidate: str):
    """ΔRMSE кандидата к RMSE `v2_bottomup`; `None`, если ячейка не годится."""
    if not _usable(cell):
        return None
    pair = _pair(cell, candidate)
    reference = (cell.get("models") or {}).get(REFERENCE)
    if pair is None or reference is None:
        return None
    return pair["delta"] / reference["rmse"]


def _fmt(pair: dict) -> str:
    return f"Δ={pair['delta']:+.4f}, p={pair['dm_p']:.4g}, CI [{pair['delta_ci'][0]:+.4f}; {pair['delta_ci'][1]:+.4f}]"


def _decide_one(cells: dict, candidate: str) -> dict:
    missing = [key for key in required_cells() if not _usable(cells.get(key))]
    if missing:
        return {
            "decision": "incomplete",
            "failed": [],
            "missing": [list(key) for key in missing],
            "reasons": [f"нет пригодных ячеек: {missing}"],
        }
    failed, reasons = [], []

    def pair(sample, horizon):
        found = _pair(cells[(sample, horizon)], candidate)
        if found is None:
            raise KeyError(f"нет пары {candidate} vs {REFERENCE} в {sample} h{horizon}")
        return found

    # 1
    cond1 = {h: pair("S_L09", h) for h in PRIMARY_HORIZONS}
    bad1 = [h for h, p in cond1.items() if not p["delta"] < 0]
    if bad1:
        failed.append(1)
        reasons += [f"1: S_L09 h{h} {_fmt(cond1[h])} — RMSE не ниже v2_bottomup" for h in bad1]
    # 2
    p1 = cond1[1]
    if not (p1["dm_p"] < ALPHA and not (p1["delta_ci"][0] <= 0 <= p1["delta_ci"][1])):
        failed.append(2)
        reasons.append(f"2: S_L09 h1 {_fmt(p1)} — нет значимости по DM или CI содержит 0")
    # 3
    bad3 = [
        (s, h)
        for s in ("S_L13", "S_W22")
        for h in PRIMARY_HORIZONS
        if not pair(s, h)["delta"] < 0
    ]
    if bad3:
        failed.append(3)
        reasons += [f"3: {s} h{h} {_fmt(pair(s, h))} — знак эффекта не сохранился" for s, h in bad3]
    # 4
    bad4 = [h for h in PRIMARY_HORIZONS if candidate not in cells[("S_L09", h)]["mcs"]["included"]]
    if bad4:
        failed.append(4)
        reasons += [f"4: S_L09 h{h} — вне MCS-90 %" for h in bad4]
    # 5
    p6 = pair("S_L09", 6)
    if p6["dm_p"] < ALPHA and p6["delta"] > 0:
        failed.append(5)
        reasons.append(f"5: S_L09 h6 {_fmt(p6)} — значимое ухудшение")

    if not failed:
        decision = "promote"
    elif 1 in failed and 1 in bad1:
        decision = "reject"
    else:
        decision = "iterate"
    return {"decision": decision, "failed": failed, "missing": [], "reasons": reasons}


def decide_h2(cells: dict) -> dict:
    """`{кандидат: {decision, failed, missing, reasons}}` по условиям §7/§7.2.

    `cells` — `{(выборка, горизонт): результат evaluate_cell}` для `T_NSA`.
    `reject` — только если нарушено условие 1 в h1; при отсутствии ячеек —
    `incomplete`, а не `iterate`.
    """
    return {candidate: _decide_one(cells, candidate) for candidate in CANDIDATES}


def vs_v3_from_predictions(predictions: list, horizon: int, candidate: str) -> dict:
    """Разведочное сравнение кандидат vs `v3_bottomup` на общих фолдах (не gate)."""
    errors = {}
    for item in predictions:
        if item["model"] in (candidate, V3):
            errors.setdefault(item["model"], {})[item["origin"]] = float(item["pred"]) - float(item["actual"])
    if candidate not in errors or V3 not in errors:
        raise ValueError(f"в предсказаниях нет {candidate} или {V3}")
    common = lb.common_folds(errors)
    result = lb.compare_pair([errors[candidate][o] for o in common], [errors[V3][o] for o in common], horizon)
    result["n"] = len(common)
    return result


# ---------------------------------------------------------------------------
# Чтение ячеек и запись артефакта
# ---------------------------------------------------------------------------


def read_cell(directory: Path, horizon: int):
    path = directory / f"cell_{TARGET}_h{horizon}.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def read_predictions(directory: Path, horizon: int) -> list:
    with (directory / f"cell_{TARGET}_h{horizon}_predictions.csv").open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def load_cells(root: Path) -> dict:
    cells = {}
    for sample in SAMPLES:
        directory = root / f"{sample}_full"
        for horizon in (1, 3, 6):
            cell = read_cell(directory, horizon)
            if cell is not None:
                cells[(sample, horizon)] = cell
    return cells


def exploratory(root: Path, cells: dict) -> dict:
    """Не gate: эффект к v2 в долях RMSE и сравнение с v3 на S_L09 (и на выборках robustness)."""
    out = {}
    for candidate in CANDIDATES:
        entry = {"relative_effect": {}, "vs_v3": {}}
        for (sample, horizon), cell in sorted(cells.items()):
            value = relative_effect(cell, candidate)
            if value is not None:
                entry["relative_effect"][f"{sample}:h{horizon}"] = value
            directory = root / f"{sample}_full"
            if _usable(cell) and (directory / f"cell_{TARGET}_h{horizon}_predictions.csv").exists():
                entry["vs_v3"][f"{sample}:h{horizon}"] = vs_v3_from_predictions(
                    read_predictions(directory, horizon), horizon, candidate
                )
        out[candidate] = entry
    return out


def render_markdown(artifact: dict) -> str:
    lines = [
        "# CPI v4 — gate эксперимента 2 (H2)",
        "",
        f"Эталон `{REFERENCE}`; условия — спека §7/§7.2:",
        "",
    ]
    lines += [f"{n}. {text}" for n, text in CONDITIONS.items()]
    for candidate, entry in artifact["decisions"].items():
        lines += ["", f"## {candidate}: **{entry['decision']}**", ""]
        lines.append(f"Не выполнены условия: {entry['failed'] or '—'}")
        lines += [f"- {reason}" for reason in entry["reasons"]]
        info = artifact["exploratory"].get(candidate, {})
        if info.get("relative_effect"):
            lines += ["", "Эффект к `v2_bottomup` (ΔRMSE / RMSE v2, разведочно):", ""]
            lines += [f"- {k}: {v:+.1%}" for k, v in info["relative_effect"].items()]
        if info.get("vs_v3"):
            lines += ["", f"Сравнение с `{V3}` (разведочно, не gate):", ""]
            lines += [f"- {k}: {_fmt(v)}, n={v['n']}" for k, v in info["vs_v3"].items()]
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--cells-dir", type=Path, default=ROOT / "build" / "cpi-v4")
    parser.add_argument("--out", type=Path, default=ROOT / "build" / "ml-gate" / "cpi_v4_h2.json")
    args = parser.parse_args(argv)
    cells = load_cells(args.cells_dir)
    artifact = {
        "spec": "docs/superpowers/specs/2026-10-28-cpi-v4-weekly-aggregate-core-design.md §7/§7.2",
        "reference": REFERENCE,
        "decisions": decide_h2(cells),
        "exploratory": exploratory(args.cells_dir, cells),
        "cells": {f"{s}:h{h}": c.get("predictions_sha256") for (s, h), c in sorted(cells.items())},
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    args.out.with_suffix(".md").write_text(render_markdown(artifact), encoding="utf-8")
    print(render_markdown(artifact))
    return 0


if __name__ == "__main__":
    sys.exit(main())
