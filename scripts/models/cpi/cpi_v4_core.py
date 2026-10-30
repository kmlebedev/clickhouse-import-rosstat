#!/usr/bin/env python3
"""Эксперимент 3 (H3): `v4c_core_nnls`/`v4d_core_direct` на T_CORE (#396 Task 7).

Пункт `key-rate/phase-4-cpi-v4-weekly-aggregate-core`, роль ML. Спека
`docs/superpowers/specs/2026-10-28-cpi-v4-weekly-aggregate-core-design.md`
§5.2 (признак и модели) и §7.3 (чтение gate). Движок — `cpi_v4_grid`
(накопленная цель, purge, walk-forward); здесь: состав моделей `T_CORE`,
наивные прогнозы `B0f`/`M1f`, парные сравнения кандидатов на общих фолдах,
RMSE совмещённых оценок `core(O)` (разведочно) и решение gate.

Скрипт только читает ClickHouse и CSV-панель #364; в БД и `forecast_log` не
пишет. Ячейки — `build/cpi-v4/core_<выборка>_full/`, gate-артефакт —
`build/ml-gate/cpi_v4_h3.{json,md}`. Вывод формулируется как «hybrid/proxy»
независимо от решения (покрытие весов 39,3 % < 50 %, #76).

Запуск — `make cpi-v4-core ARGS="--sample S_L09"`, решение —
`make cpi-v4-core ARGS="--decide"`, тесты — `make cpi-v4-core-test`.
"""

import argparse
import json
import math
import sys
import time
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib import eval_utils as eu
from models.cpi import cpi_monthly as cm
from models.cpi import cpi_monthly_v3 as v3
from models.cpi import cpi_weekly_aggregate as agg
import cpi_core_nnls as nn
import cpi_v4_grid as grid
import cpi_v4_leaderboard as lb

TARGET = "T_CORE"
REFERENCE = "v2_baseline"
CANDIDATES = ("v4c_core_nnls", "v4d_core_direct")
BENCHMARKS = ("B1", "B2", "B3", "v2_baseline", "v3_direct", "v4a_direct")
B3_TOLERANCE = 0.05
ALPHA = 0.05
RUNS = {"S_L09": (1, 3, 6), "S_W22": (1,)}
CONDITIONS = {
    1: "S_L09 h1: RMSE ниже B0f, DM p < 0,05, CI ΔRMSE без 0",
    2: "S_W22 h1: то же против B0f и против M1f",
    3: "S_L09 h3/h6: RMSE ≤ 1,05·B3 и нет значимого ухудшения против B3",
}
WORDING = "hybrid/proxy: покрытие весов панели 39,3 % < 50 % (#76), «core выделен» не заявляется"


def b0_monthly_pp(annual_percent: float) -> float:
    """Годовой B0, % → эквивалент m/m в п.п.: `(1 + B0/100)^(1/12) − 1`."""
    return (math.pow(1.0 + annual_percent / 100.0, 1.0 / 12.0) - 1.0) * 100.0


def proxy_model(values: dict):
    """Наивный прогноз = значение proxy за месяц origin; нет значения — фолд пропущен."""

    def fit_predict(train, row):
        value = values.get(row.month)
        if value is None or not math.isfinite(value):
            raise cm.DegenerateWindowError(f"proxy не определён за {row.month}")
        return float(value)

    return fit_predict


def core_model_names(sample: str, horizon: int) -> tuple:
    names = [*BENCHMARKS, *CANDIDATES]
    if horizon == 1:
        names.append("B0f")
        if sample == "S_W22":
            names.append("M1f")
    return tuple(names)


def make_core_models(sample: str, series: dict, horizon: int, b0: dict, m1: dict) -> dict:
    v4a = agg.feature_names_v4(sample, with_diffusion=False)
    available = {
        "B1": grid.make_b1(series, horizon),
        "B2": cm.fit_ar1,
        "B3": cm.fit_enet_mp,
        "v2_baseline": v3.fit_v2_baseline,
        "v3_direct": v3.fit_v3_direct,
        "v4a_direct": grid._direct(v4a),
        "v4c_core_nnls": nn.make_v4c(nn.feature_names_v4c(sample)),
        "v4d_core_direct": nn.make_v4d(nn.feature_names_v4d(sample)),
        "B0f": proxy_model(b0),
        "M1f": proxy_model(m1),
    }
    return {name: available[name] for name in core_model_names(sample, horizon)}


def pairs_from_predictions(predictions: list, horizon: int, candidates, others, floor) -> dict:
    """`{"X vs Y": compare_pair}` на общих фолдах пары; `floor` — нижняя граница origin (или None)."""
    errors = {}
    for item in predictions:
        if floor is not None and item["origin"] < floor.isoformat():
            continue
        errors.setdefault(item["model"], {})[item["origin"]] = float(item["pred"]) - float(item["actual"])
    out = {}
    for left in candidates:
        for right in others:
            if left not in errors or right not in errors:
                continue
            common = lb.common_folds(errors, [left, right])
            if len(common) < grid.MIN_COMMON_FOLDS:
                continue
            result = lb.compare_pair([errors[left][o] for o in common], [errors[right][o] for o in common], horizon)
            result["n"] = len(common)
            out[f"{left} vs {right}"] = result
    return out


def nowcast_rmse(core: dict, proxies: dict) -> dict:
    """RMSE совмещённых оценок `core(O)` на месяцах, где определены ВСЕ proxy (разведочно)."""
    months = sorted(
        month
        for month in core
        if all(month in values and math.isfinite(values[month]) for values in proxies.values())
    )
    rmse = {
        name: (eu.rmse([values[m] - core[m] for m in months]) if months else None)
        for name, values in proxies.items()
    }
    return {"months": [m.isoformat() for m in months], "rmse": rmse}


# ---------------------------------------------------------------------------
# Решение gate (чистая функция, спека §7.3)
# ---------------------------------------------------------------------------


def _fmt(pair: dict) -> str:
    return f"Δ={pair['delta']:+.4f}, p={pair['dm_p']:.4g}, CI [{pair['delta_ci'][0]:+.4f}; {pair['delta_ci'][1]:+.4f}]"


def _significant(pair: dict) -> bool:
    low, high = pair["delta_ci"]
    return pair["dm_p"] < ALPHA and not (low <= 0 <= high)


def required_pairs(candidate: str) -> list:
    return [
        ("S_L09", 1, f"{candidate} vs B0f"),
        ("S_W22", 1, f"{candidate} vs B0f"),
        ("S_W22", 1, f"{candidate} vs M1f"),
        ("S_L09", 3, f"{candidate} vs B3"),
        ("S_L09", 6, f"{candidate} vs B3"),
    ]


def _decide_one(cells: dict, candidate: str) -> dict:
    missing = []
    found = {}
    for sample, horizon, key in required_pairs(candidate):
        cell = cells.get((sample, horizon))
        pair = ((cell or {}).get("core_pairs") or {}).get(key) if cell and cell.get("status") == "ok" else None
        if pair is None:
            missing.append([sample, horizon, key])
        else:
            found[(sample, horizon, key)] = pair
    base = {"missing": missing, "wording": WORDING}
    if missing:
        return {**base, "decision": "incomplete", "failed": [], "reasons": [f"нет данных: {missing}"]}
    failed, reasons, point_worse = [], [], False

    p1 = found[("S_L09", 1, f"{candidate} vs B0f")]
    if not p1["delta"] < 0:
        point_worse = True
    if not (p1["delta"] < 0 and _significant(p1)):
        failed.append(1)
        reasons.append(f"1: S_L09 h1 vs B0f {_fmt(p1)}")

    bad2 = []
    for other in ("B0f", "M1f"):
        pair = found[("S_W22", 1, f"{candidate} vs {other}")]
        if not pair["delta"] < 0:
            point_worse = True
        if not (pair["delta"] < 0 and _significant(pair)):
            bad2.append(f"2: S_W22 h1 vs {other} {_fmt(pair)}")
    if bad2:
        failed.append(2)
        reasons += bad2

    bad3 = []
    for horizon in (3, 6):
        pair = found[("S_L09", horizon, f"{candidate} vs B3")]
        ratio = pair["delta"] / pair["rmse_right"]
        if ratio > B3_TOLERANCE or (pair["dm_p"] < ALPHA and pair["delta"] > 0):
            bad3.append(f"3: S_L09 h{horizon} vs B3 {_fmt(pair)}, отношение к B3 {1 + ratio:.3f}")
    if bad3:
        failed.append(3)
        reasons += bad3

    if not failed:
        decision = "promote"
    elif point_worse:
        decision = "reject"
    else:
        decision = "iterate"
    return {**base, "decision": decision, "failed": failed, "reasons": reasons}


def decide_h3(cells: dict) -> dict:
    """`{кандидат: {decision, failed, missing, reasons, wording}}` по §7.3."""
    return {candidate: _decide_one(cells, candidate) for candidate in CANDIDATES}


# ---------------------------------------------------------------------------
# Прогон на данных (только чтение)
# ---------------------------------------------------------------------------


def load_proxies() -> tuple:
    """`(b0, m1, core_nnls_months)`: B0 и M1 как `{месяц: m/m п.п.}` (read-only из #364/#348)."""
    import weekly_core_components as wcc
    import weekly_core_proxy as wcp

    b0 = {
        date.fromisoformat(f"{key}-01"): b0_monthly_pp(value)
        for key, value in wcp.align_last(wcp.four_week_average(wcp.load_aggregate_history())).items()
    }
    panel = wcc.usable_rows(wcc.read_panel(wcc.PANEL_PATH))
    index, _ = wcc.core_subindex(panel)
    m1 = {date.fromisoformat(f"{key}-01"): value["mm"] for key, value in index.items()}
    return b0, m1, nn.bucket_series(panel)


def run_cell(rows, series, sample, horizon, b0, m1, floor):
    config = grid.SAMPLE_CONFIG[sample]
    origin_rows = grid.build_origin_rows(rows, TARGET, horizon, series)
    models = make_core_models(sample, series, horizon, b0, m1)
    excluded = agg.EXCLUDED_BASE_FEATURES.get(sample, ())
    started = time.time()
    with grid.feature_override(excluded):
        predictions = grid.run_walk_forward(origin_rows, models, horizon, config["min_train"], None)
    result = grid.evaluate_cell(predictions, horizon, REFERENCE, config["primary_from"])
    result["core_pairs"] = pairs_from_predictions(
        predictions, horizon, CANDIDATES, ("B0f", "M1f", "B3", "v3_direct", "v4a_direct"), config["primary_from"]
    )
    result.update(
        {
            "target": TARGET,
            "sample": sample,
            "n_origins": len(origin_rows),
            "min_train": config["min_train"],
            "primary_from": config["primary_from"].isoformat() if config["primary_from"] else None,
            "seconds": round(time.time() - started, 1),
        }
    )
    return result, predictions


def run(sample: str, horizons, out_dir: Path) -> None:
    rows, skipped = agg.build_sample_v4(sample, with_diffusion=True)
    b0, m1, buckets = load_proxies()
    series = lb.load_target_series()[TARGET]
    rows = nn.attach_core_nnls(rows, buckets, series)
    cell_dir = out_dir / f"core_{sample}_full"
    print(f"Выборка {sample}: {len(rows)} строк, пропущено {len(skipped)}", flush=True)
    for horizon in horizons:
        result, predictions = run_cell(rows, series, sample, horizon, b0, m1, None)
        grid._write_cell(cell_dir, TARGET, horizon, result, predictions)
        print(f"  {TARGET} h{horizon}: {result['status']}, фолдов {result['n_common']}, {result['seconds']} с", flush=True)
    if sample == "S_W22":
        core_nnls_values = {r.month: r.features[nn.FEATURE] for r in rows}
        proxies = {"core_nnls": core_nnls_values, "M1": m1, "B0": b0}
        (cell_dir / "nowcast_rmse.json").write_text(
            json.dumps(nowcast_rmse(series, proxies), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )


def load_cells(out_dir: Path) -> dict:
    cells = {}
    for sample, horizons in RUNS.items():
        for horizon in horizons:
            path = out_dir / f"core_{sample}_full" / f"cell_{TARGET}_h{horizon}.json"
            if path.exists():
                cells[(sample, horizon)] = json.loads(path.read_text(encoding="utf-8"))
    return cells


def render_markdown(artifact: dict) -> str:
    lines = ["# CPI v4 — gate эксперимента 3 (H3)", "", "Условия — спека §7.3:", ""]
    lines += [f"{n}. {text}" for n, text in CONDITIONS.items()]
    for candidate, entry in artifact["decisions"].items():
        lines += ["", f"## {candidate}: **{entry['decision']}**", "", f"Не выполнены условия: {entry['failed'] or '—'}"]
        lines += [f"- {reason}" for reason in entry["reasons"]]
        lines.append(f"\nФормулировка: {entry['wording']}")
    nowcast = artifact.get("nowcast_rmse")
    if nowcast:
        lines += ["", f"RMSE совмещённых оценок core(O), п.п., {len(nowcast['months'])} общих месяцев (разведочно):", ""]
        lines += [f"- {name}: {value if value is None else f'{value:.4f}'}" for name, value in nowcast["rmse"].items()]
    return "\n".join(lines) + "\n"


def decide(out_dir: Path, out: Path) -> dict:
    cells = load_cells(out_dir)
    nowcast_path = out_dir / "core_S_W22_full" / "nowcast_rmse.json"
    artifact = {
        "spec": "docs/superpowers/specs/2026-10-28-cpi-v4-weekly-aggregate-core-design.md §7.3",
        "decisions": decide_h3(cells),
        "nowcast_rmse": json.loads(nowcast_path.read_text(encoding="utf-8")) if nowcast_path.exists() else None,
        "cells": {f"{s}:h{h}": c.get("predictions_sha256") for (s, h), c in sorted(cells.items())},
        "pairs": {f"{s}:h{h}": c.get("core_pairs") for (s, h), c in sorted(cells.items())},
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    out.with_suffix(".md").write_text(render_markdown(artifact), encoding="utf-8")
    return artifact


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sample", choices=tuple(RUNS))
    parser.add_argument("--horizons", default=None, help="например 1,3,6; по умолчанию как в §7.3")
    parser.add_argument("--decide", action="store_true")
    parser.add_argument("--out-dir", type=Path, default=ROOT / "build" / "cpi-v4")
    parser.add_argument("--gate-out", type=Path, default=ROOT / "build" / "ml-gate" / "cpi_v4_h3.json")
    args = parser.parse_args(argv)
    if args.decide:
        print(render_markdown(decide(args.out_dir, args.gate_out)))
        return 0
    if not args.sample:
        parser.error("нужен --sample или --decide")
    horizons = [int(h) for h in args.horizons.split(",")] if args.horizons else list(RUNS[args.sample])
    run(args.sample, horizons, args.out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
