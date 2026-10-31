#!/usr/bin/env python3
"""Task 14 (#454): состав `T_LIVE@2009`/`@2010` по месяцам и не-ухудшение моделей.

Три вопроса к двум версиям панели 88 групп (`cb_comp_groups_panel --version`):

* **состав** — сколько групп с SA и какая доля веса выпадает по месяцам; 84
  группы у `@2009` только в 2009-01…2010-01, с 2010-02 состав версий одинаков;
* **цель** — `T_LIVE[h]` против `TARGET_FULL[h]` по периодам (2009 — только
  `@2009`; эпоха train 2010-02…2018-12; тест normal-54) и сдвиг агрегата SA
  `@2009 − @2010` на общих месяцах — эффект окна X-13;
* **не-ухудшение** — парные ошибки моделей Task 9 (`combo3`, `live_cb`,
  `combo3_tf`) `@2009` против `@2010` на `TARGET_FULL` normal-54: ΔMSE, 95 %
  CI блочного бутстрапа и односторонняя 95 % верхняя граница отношения RMSE.
  Порог `NI_MARGIN` выбран после результатов (по аналогии с не-вредом Task 9),
  поэтому вывод — описание; для решения о смене базы порог предрегистрируется.

Только чтение панелей, предсказаний и архива CB_COMP; в ClickHouse не пишет.
"""

import argparse
import csv
import hashlib
import json
import math
import sys
from datetime import date
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_parity as cp

VERSIONS = ("2010", "2009")
HORIZONS = (1, 2, 3, 6)
GAP_HORIZONS = (1, 3, 6)
NI_MODELS = ("combo3", "live_cb", "combo3_tf")
NI_HORIZONS = (1, 3, 6)
NI_MARGIN = 0.02
BOOT_B = 5000
BOOT_BLOCK = 6
SEED = 42
COMPOSITION_LAST = date(2012, 3, 1)
GAP_PERIODS = {
    "2009": (date(2009, 1, 1), date(2010, 1, 1), False),
    "train_era": (date(2010, 2, 1), date(2018, 12, 1), False),
    "test_normal54": (date(2019, 1, 1), date(2024, 7, 1), True),
}
SHIFT_PERIODS = {
    "2010-02..2012-12": (date(2010, 2, 1), date(2012, 12, 1)),
    "2013-01..2018-12": (date(2013, 1, 1), date(2018, 12, 1)),
    "2019-01..2024-07": (date(2019, 1, 1), date(2024, 7, 1)),
}
REF_PREDICTIONS = ROOT / "build" / "cb-comp-h1-bs-combo" / "predictions.csv"
OUT_DIR = ROOT / "build" / "cb-comp-tlive-versions"


def composition(sa: dict, codes: list, weights: dict, months: list) -> dict:
    """`{месяц: {n, missing, missing_weight_share}}` — группы без SA и их доля веса."""
    out = {}
    for month in months:
        w = bu.weight_for_month(weights, month)
        missing = sorted(c for c in codes if month not in sa.get(c, {}))
        total = sum(w.get(c, 0.0) for c in codes)
        out[month] = {"n": len(codes) - len(missing), "missing": missing,
                      "missing_weight_share": sum(w.get(c, 0.0) for c in missing) / total if total else math.nan}
    return out


def composition_segments(comp: dict) -> list:
    """Сжатие помесячного состава в отрезки с одинаковым набором пропусков."""
    segments = []
    for month in sorted(comp):
        item = comp[month]
        if segments and segments[-1]["missing"] == item["missing"]:
            segments[-1]["last"] = month
        else:
            segments.append({"first": month, "last": month, **item})
    return segments


def error_stats(errors: list) -> dict:
    if not errors:
        return None
    e = np.asarray(errors, dtype=float)
    return {"n": int(e.size), "rmse": float(np.sqrt(np.mean(e ** 2))), "bias": float(e.mean())}


def target_gap(t_live: dict, target_full, normal: set) -> dict:
    """RMSE/bias `T_LIVE[h] − TARGET_FULL[h]` по периодам; окно h целиком внутри периода."""
    out = {}
    for name, (first, last, normal_only) in GAP_PERIODS.items():
        for h in GAP_HORIZONS:
            truth = target_full[h].dropna().to_dict()
            errors = [value - truth[m] for m, value in sorted(t_live[h].items())
                      if first <= m <= last and m in truth and bu.shift_month(m, -(h - 1)) >= first
                      and (not normal_only or m in normal)]
            out.setdefault(name, {})[f"h{h}"] = error_stats(errors)
    return out


def sa_shift(mm_new: dict, mm_ref: dict) -> dict:
    """RMS и max |Δ| агрегата SA м/м (п.п.) на общих месяцах периодов."""
    out = {}
    for name, (first, last) in SHIFT_PERIODS.items():
        diff = [mm_new[m] - mm_ref[m] for m in sorted(mm_ref) if first <= m <= last and m in mm_new
                and not (math.isnan(mm_new[m]) or math.isnan(mm_ref[m]))]
        out[name] = {"n": len(diff), "rms_pp": float(np.sqrt(np.mean(np.square(diff)))) if diff else None,
                     "max_abs_pp": float(np.max(np.abs(diff))) if diff else None}
    return out


def block_bootstrap_means(values: np.ndarray, rng, b: int = BOOT_B, block: int = BOOT_BLOCK) -> np.ndarray:
    """Средние циклического блочного бутстрапа (блок `block`, `b` повторов)."""
    n = len(values)
    starts_per = math.ceil(n / block)
    means = np.empty(b)
    for i in range(b):
        starts = rng.integers(0, n, starts_per)
        idx = np.concatenate([np.arange(s, s + block) % n for s in starts])[:n]
        means[i] = values[idx].mean()
    return means


def noninferiority(err_new: np.ndarray, err_ref: np.ndarray, rng, margin: float = NI_MARGIN) -> dict:
    """Парное сравнение `new` против `ref`: ΔMSE, CI, верхняя граница отношения RMSE."""
    mse_ref = float(np.mean(err_ref ** 2))
    diff = err_new ** 2 - err_ref ** 2
    means = block_bootstrap_means(diff, rng)
    lo, hi, up = np.percentile(means, [2.5, 97.5, 95])
    rmse_new, rmse_ref = float(np.sqrt(np.mean(err_new ** 2))), math.sqrt(mse_ref)
    rel_upper = math.sqrt(max(0.0, mse_ref + up)) / rmse_ref
    return {
        "n": int(diff.size), "rmse_new": rmse_new, "rmse_ref": rmse_ref, "rel": rmse_new / rmse_ref,
        "dmse": float(diff.mean()), "dmse_ci95": [float(lo), float(hi)],
        "rel_upper95": rel_upper, "margin": margin, "noninferior": rel_upper <= 1 + margin,
    }


def load_predictions(path: Path) -> dict:
    """`{модель: {h: {месяц: прогноз}}}` из `predictions.csv` Task 9."""
    out = {}
    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            out.setdefault(row["model"], {}).setdefault(int(row["horizon"]), {})[
                date.fromisoformat(row["month"])] = float(row["y_pred"])
    return out


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(archive: Path, panels: dict, predictions: dict, out_dir: Path) -> dict:
    data = cp.check_archive(archive)
    target_full, normal = data["target"], set(data["normal"])
    specs = bu.load_specs(archive)
    codes = [spec.code for spec in specs]
    weights = bu.load_weights(archive, lb.WEIGHT_YEARS, specs)
    sa = {}
    for version, path in panels.items():
        sa[version] = lb.load_group_panel(path)[0]
        lb.check_panel_version(sa[version], version)

    comp, gaps, t_live, mm = {}, {}, {}, {}
    for version, series in sa.items():
        first = lb.first_sa_month(series)
        last = max(m for values in series.values() for m in values)
        months = cp.month_range(first, last)
        comp[version] = composition_segments(composition(series, codes, weights, cp.month_range(first, COMPOSITION_LAST)))
        t_live[version] = lb.t_live({c: series.get(c, {}) for c in codes}, weights, months)
        gaps[version] = target_gap(t_live[version], target_full, normal)
        mm[version] = lb.aggregate_mm({c: series.get(c, {}) for c in codes}, weights, months)

    preds = {version: load_predictions(path) for version, path in predictions.items()}
    rng = np.random.default_rng(SEED)
    ni = {}
    for model in NI_MODELS:
        for h in HORIZONS:
            truth = target_full[h].dropna().to_dict()
            months = sorted(m for m in preds["2010"][model][h]
                            if m in normal and m in preds["2009"][model][h] and m in truth)
            err = {v: np.array([preds[v][model][h][m] - truth[m] for m in months]) for v in VERSIONS}
            ni.setdefault(model, {})[f"h{h}"] = noninferiority(err["2009"], err["2010"], rng)
            ni[model][f"h{h}"]["primary"] = h in NI_HORIZONS
    for model, per_h in ni.items():
        per_h["noninferior_primary"] = all(per_h[f"h{h}"]["noninferior"] for h in NI_HORIZONS)

    result = {
        "spec": "docs/superpowers/specs/2026-10-31-cb-comp-tlive-2009-design.md",
        "config": {"seed": SEED, "boot_b": BOOT_B, "boot_block": BOOT_BLOCK, "ni_margin": NI_MARGIN,
                   "ni_margin_preregistered": False, "ni_horizons": list(NI_HORIZONS)},
        "inputs": {f"panel_{v}": sha256(p) for v, p in panels.items()}
                  | {f"predictions_{v}": sha256(p) for v, p in predictions.items()},
        "composition": comp,
        "t_live_vs_target_full": gaps,
        "sa_shift_2009_minus_2010": sa_shift(mm["2009"], mm["2010"]),
        "noninferiority_2009_vs_2010": ni,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "result.json").write_text(json.dumps(lb._jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                                         encoding="utf-8")
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None)
    parser.add_argument("--ref-predictions", default=str(REF_PREDICTIONS),
                        help="predictions.csv Task 9 на @2010 (make cb-comp-h1-bs-combo)")
    parser.add_argument("--predictions", default=str(lb.versioned_dir(REF_PREDICTIONS.parent, "2009") / REF_PREDICTIONS.name),
                        help="predictions.csv Task 9 на @2009 (make cb-comp-h1-bs-combo ARGS=--version 2009)")
    parser.add_argument("--out", default=str(OUT_DIR))
    args = parser.parse_args(argv)
    archive = cp.resolve_archive(args.archive)
    panels = {v: lb.default_panel(v) for v in VERSIONS}
    predictions = {"2010": Path(args.ref_predictions), "2009": Path(args.predictions)}
    result = run(archive, panels, predictions, Path(args.out))
    summary = {model: {h: (round(v["rel"], 3), round(v["rel_upper95"], 3), v["noninferior"])
                       for h, v in per_h.items() if h.startswith("h")} | {"primary": per_h["noninferior_primary"]}
               for model, per_h in result["noninferiority_2009_vs_2010"].items()}
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
