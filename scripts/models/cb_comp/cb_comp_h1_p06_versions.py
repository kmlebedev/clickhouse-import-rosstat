#!/usr/bin/env python3
"""P0.6 по версиям цели `@2010` и `@2009`: матрица 2×2 и правило смены базы — Task 16 #466.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-10-31-cb-comp-p06-tlive-2009-design.md`.

* **Матрица 2×2** — прогнозы, обученные на `T_LIVE@2010` и `T_LIVE@2009`,
  оцениваются на четырёх истинах: финальной и real-time каждой версии.
  Так качество модели отделяется от сложности цели (спека §2.1);
* **Δ версий** — `wk_mix_ex@2009 − wk_mix_ex@2010` на каждой истине:
  ΔRMSE, 95 % блочный бутстрап CI, DM-HLN p (`cpi_v4_leaderboard.compare_pair`);
* **правило §6** — условия рекомендации смены базы цели и сенситивность окна OLS.

Читает только готовые `predictions_robustness.csv` и `robustness.json`
(`make cb-comp-h1-shadow-robustness` по версиям); ClickHouse не нужен.
Запуск — `make cb-comp-h1-p06-versions`, тесты — `make cb-comp-h1-shadow-test`.
"""

import argparse
import csv
import json
import math
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_live_bottomup as lb

HORIZON = 1
MODELS = ("combo3", "wk_mix_ex", "wk_agg_ex", "wk_mix_d", "live_cb_wk")
CANDIDATE = "wk_mix_ex"
TRUTH_COLUMNS = {"final": "t_live", "rt": "t_live_rt"}
DM_ALPHA = 0.05
PUB_SLACK = 0.01
FLAG_KEYS = ("rt_holds", "final_holds", "ex_tariff_holds", "pub_holds")

SHADOW_DIR = ROOT / "build" / "cb-comp-h1-shadow"
DEFAULT_INPUTS = {
    "2010": SHADOW_DIR,
    "2009": SHADOW_DIR.with_name(f"{SHADOW_DIR.name}-2009"),
}
DEFAULT_SENSITIVITY = SHADOW_DIR.with_name(f"{SHADOW_DIR.name}-2009-train2009")
OUT_JSON = ROOT / "docs" / "research" / "2026-10-31-cb-comp-p06-tlive-2009.json"
SPEC = "docs/superpowers/specs/2026-10-31-cb-comp-p06-tlive-2009-design.md"


def load_predictions(path: Path) -> tuple:
    """`(прогнозы {cutoff: {model: {M: y}}}, истины {final|rt: {M: y}})` из `predictions_robustness.csv`."""
    preds, truths = {}, {label: {} for label in TRUTH_COLUMNS}
    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            month = date.fromisoformat(row["month"])
            preds.setdefault(row["cutoff"], {}).setdefault(row["model"], {})[month] = float(row["y_pred"])
            for label, column in TRUTH_COLUMNS.items():
                value = float(row[column])
                if not math.isnan(value):
                    truths[label][month] = value
    return preds, truths


def truth_key(version: str, label: str) -> str:
    return f"{label}@{version}"


def all_truths(runs: dict) -> dict:
    """Четыре истины: `final@2010`, `rt@2010`, `final@2009`, `rt@2009`."""
    return {truth_key(version, label): truth for version, run in runs.items() for label, truth in run["truths"].items()}


def common_months(*series) -> list:
    keys = set(series[0])
    for values in series[1:]:
        keys &= set(values)
    return sorted(keys)


def rmse_on(values: dict, truth: dict) -> dict:
    months = common_months(values, truth)
    errors = [values[m] - truth[m] for m in months]
    return {"n": len(months), "rmse": lb.rmse(errors), "bias": sum(errors) / len(errors) if errors else math.nan}


def matrix(runs: dict, truths: dict, cutoff: str = "late", models: tuple = MODELS) -> dict:
    """`{model: {train_version: {truth: {n, rmse, bias}}}}`."""
    out = {}
    for name in models:
        for version, run in runs.items():
            values = run["preds"].get(cutoff, {}).get(name)
            if not values:
                continue
            out.setdefault(name, {})[version] = {label: rmse_on(values, truth) for label, truth in truths.items()}
    return out


def compare(left: dict, right: dict, truth: dict) -> dict:
    """`left − right` на общих месяцах: ΔRMSE, CI, DM-HLN (отрицательная Δ — левая лучше)."""
    from models.cpi import cpi_v4_leaderboard as v4

    months = common_months(left, right, truth)
    pair = v4.compare_pair([left[m] - truth[m] for m in months], [right[m] - truth[m] for m in months], HORIZON)
    return {**pair, "months": [months[0].isoformat(), months[-1].isoformat()] if months else []}


def version_deltas(runs: dict, truths: dict, name: str = CANDIDATE, cutoff: str = "late",
                   left: str = "2009", right: str = "2010") -> dict:
    """`name@left − name@right` на каждой истине."""
    a = runs[left]["preds"].get(cutoff, {}).get(name, {})
    b = runs[right]["preds"].get(cutoff, {}).get(name, {})
    return {label: compare(a, b, truth) for label, truth in truths.items()}


def vs_combo(runs: dict, truths: dict, name: str = CANDIDATE, cutoff: str = "late") -> dict:
    """`name − combo3` внутри каждой версии на всех истинах (перекрёстная устойчивость В1)."""
    out = {}
    for version, run in runs.items():
        preds = run["preds"].get(cutoff, {})
        out[version] = {label: compare(preds[name], preds["combo3"], truth) for label, truth in truths.items()}
    return out


def better(pair: dict, alpha: float = DM_ALPHA, need_ci: bool = False) -> bool:
    ok = pair["delta"] < 0 and pair["dm_p"] < alpha
    return bool(ok and (pair["delta_ci"][1] < 0 if need_ci else True))


def switch_rule(deltas: dict, flags: dict, pub_deltas: dict) -> dict:
    """Правило §6 спеки (В2): все условия → рекомендация `@2009`, иначе база цели `@2010`."""
    finals = [label for label in deltas if label.startswith("final@")]
    rts = [label for label in deltas if label.startswith("rt@")]
    c1_final = all(better(deltas[label]) for label in finals)
    c1_rt = all(better(deltas[label]) for label in rts) and any(deltas[label]["delta_ci"][1] < 0 for label in rts)
    degraded = [key for key in FLAG_KEYS if flags["2010"].get(key) and not flags["2009"].get(key)]
    c2 = not degraded and not flags["2009"].get("bias_flag_years")
    pub_worse = {label: pair["rmse_left"] - pair["rmse_right"] for label, pair in pub_deltas.items()}
    c3 = bool(pub_deltas) and all(diff <= PUB_SLACK for diff in pub_worse.values())
    conditions = {
        "c1_better_on_both_final_truths": c1_final,
        "c1_better_on_both_rt_truths": c1_rt,
        "c2_flags_not_degraded": c2,
        "c3_publication_not_worse": c3,
    }
    return {
        **conditions,
        "degraded_flags": degraded,
        "publication_rmse_diff": pub_worse,
        "recommend_2009": all(conditions.values()),
    }


def sensitivity_rule(deltas: dict) -> dict:
    """В3: окно OLS 2009-03 лучше v1 на обеих real-time истинах (DM p < 0,05)."""
    rts = {label: pair for label, pair in deltas.items() if label.startswith("rt@")}
    return {"better_on_both_rt": bool(rts) and all(better(pair) for pair in rts.values()),
            "deltas": {label: {k: pair[k] for k in ("delta", "delta_ci", "dm_p", "rmse_left", "rmse_right")}
                       for label, pair in deltas.items()}}


def load_run(path: Path) -> dict:
    preds, truths = load_predictions(path / "predictions_robustness.csv")
    result_path = path / "robustness.json"
    result = json.loads(result_path.read_text(encoding="utf-8")) if result_path.exists() else {}
    return {"preds": preds, "truths": truths, "flags": result.get("flags", {}),
            "r_p0": result.get("r_p0"), "truth_gap": result.get("truth_gap"),
            "inputs": result.get("inputs"), "train_start": result.get("train_start"),
            "sha256": file_sha256(path / "predictions_robustness.csv")}


def file_sha256(path: Path) -> str:
    from models.cb_comp import cb_comp_h1_weekly_ex as ex

    return ex.sha256(path)


def truth_gaps(truths: dict) -> dict:
    """Разрывы истин: версии между собой и финальная против real-time."""
    pairs = (("final@2009", "final@2010"), ("rt@2009", "rt@2010"), ("final@2010", "rt@2010"), ("final@2009", "rt@2009"))
    out = {}
    for left, right in pairs:
        months = common_months(truths[left], truths[right])
        diffs = [truths[left][m] - truths[right][m] for m in months]
        out[f"{left} − {right}"] = {"n": len(diffs), "rmse": lb.rmse(diffs),
                                    "bias": sum(diffs) / len(diffs) if diffs else math.nan,
                                    "max_abs": max((abs(d) for d in diffs), default=math.nan)}
    return out


def build(inputs: dict, sensitivity: Path = None) -> dict:
    runs = {version: load_run(path) for version, path in inputs.items()}
    truths = all_truths(runs)
    deltas = version_deltas(runs, truths)
    pub_deltas = {}
    if all("publication" in run["preds"] for run in runs.values()):
        pub_deltas = version_deltas(runs, truths, cutoff="publication")
    flags = {version: run["flags"] for version, run in runs.items()}
    result = {
        "spec": SPEC,
        "git_sha": lb.git_sha(),
        "inputs": {version: {"dir": str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path),
                             "predictions_sha256": runs[version]["sha256"], "robustness_inputs": runs[version]["inputs"],
                             "train_start": runs[version]["train_start"]}
                   for version, path in inputs.items()},
        "r_p0": {version: run["r_p0"] for version, run in runs.items()},
        "truth_gap_own": {version: run["truth_gap"] for version, run in runs.items()},
        "truth_gaps": truth_gaps(truths),
        "matrix_late": matrix(runs, truths, "late"),
        "matrix_publication": matrix(runs, truths, "publication", ("combo3", "wk_mix_ex", "wk_agg_ex", "wk_mix_d")),
        "candidate_vs_combo3": vs_combo(runs, truths),
        "version_deltas": {"late": deltas, "publication": pub_deltas},
        "flags": flags,
        "switch_rule": switch_rule(deltas, flags, pub_deltas),
    }
    if sensitivity and (sensitivity / "predictions_robustness.csv").exists():
        sens = load_run(sensitivity)
        sens_runs = {"2009": runs["2009"], "train2009": sens}
        s_deltas = {label: compare(sens["preds"]["late"][CANDIDATE], runs["2009"]["preds"]["late"][CANDIDATE], truth)
                    for label, truth in truths.items()}
        result["sensitivity_train2009"] = {
            "train_start": sens["train_start"],
            "predictions_sha256": sens["sha256"],
            "matrix_late": matrix(sens_runs, truths, "late", ("wk_mix_ex", "wk_agg_ex", "wk_mix_d")),
            "flags": sens["flags"],
            "rule": sensitivity_rule(s_deltas),
        }
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dir-2010", default=str(DEFAULT_INPUTS["2010"]))
    parser.add_argument("--dir-2009", default=str(DEFAULT_INPUTS["2009"]))
    parser.add_argument("--sensitivity", default=str(DEFAULT_SENSITIVITY))
    parser.add_argument("--out", default=str(OUT_JSON))
    args = parser.parse_args(argv)
    result = build({"2010": Path(args.dir_2010), "2009": Path(args.dir_2009)}, Path(args.sensitivity))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(lb._jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(lb._jsonable({"switch_rule": result["switch_rule"],
                                   "sensitivity": result.get("sensitivity_train2009", {}).get("rule")}),
                     ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
