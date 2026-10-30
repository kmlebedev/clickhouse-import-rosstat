#!/usr/bin/env python3
"""EN по 88 группам с окном 2016+ и помесячным переобучением, h3 — Task 11 #439.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-10-31-cb-comp-h3-en16-combo-design.md`.

* **`live_cb`** — Task 3 без изменений (`cb_comp_live_bottomup.fit_predict`);
* **`live_en16`** — `StandardScaler → ElasticNetCV` B3 (`cb_comp_live_en.make_enet`)
  по группам, без `month`, импутация медианой train; строки обучения
  `2016-01+h ≤ t ≤ M−h`, выбросы статьи исключены, переобучение на каждом
  тестовом месяце;
* **`combo3`** — Task 9 (`live_cb` + BS_ECON16 + BS_ROZN16), **`combo4`** —
  среднее `combo3` и `live_en16`;
* **R11** — `combo3` = Task 9, границы окна без утечки;
* **гейт G11** — `combo4` лучше `combo3` на h3 (DM p < 0,10), не хуже на h1/h6,
  MCS 90 %.

Только чтение ClickHouse и локальных файлов; в БД не пишет. Запуск —
`make cb-comp-live-en16`, тесты — `make cb-comp-live-en16-test`.
"""

import argparse
import csv
import json
import math
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_h1_bs_combo as hb
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_live_en as en
from models.cb_comp import cb_comp_parity as cp
from models.cpi import cpi_monthly as cm

# ---------------------------------------------------------------------------
# Предзарегистрированные константы (спека §4–§7)
# ---------------------------------------------------------------------------

HORIZONS = bu.HORIZONS
PRIMARY_HORIZONS = (1, 3, 6)
EN_START = hb.BS_START
ARTICLE_OUTLIERS = hb.ARTICLE_OUTLIERS
MODEL_CB = en.MODEL_CB
MODEL_EN16 = "live_en16"
MODEL_COMBO3 = hb.MODEL_COMBO
MODEL_COMBO4 = "combo4"
MODEL_COMBO3_TF = "combo3_tf"
CANDIDATES = (MODEL_CB, MODEL_EN16, MODEL_COMBO3, MODEL_COMBO4)
CB_COMP = hb.CB_COMP
G11_ALPHA = 0.10
NOHARM_MARGIN = hb.NOHARM_MARGIN
R11A_COMBO3 = {1: 1.034768, 2: 0.957109, 3: 1.010198, 6: 0.894035}
R11A_LIVE_CB = hb.TASK3_LIVE_CB_RATIOS
R11A_TOL = 1e-3
MIN_TRAIN = max(bu.MIN_TRAIN_ROWS, cm.ENET_MIN_TRAIN)

OUT_DIR = ROOT / "build" / "cb-comp-live-en16"


# ---------------------------------------------------------------------------
# Окно 2016+ и строки без утечки
# ---------------------------------------------------------------------------


def candidate_rows(code: str, features_by_month: dict, target_frame: pd.DataFrame, horizon: int, last_test: date) -> list:
    """Строки-кандидаты окна 2016+: цели `2016-01+h … last_test−h`, без выбросов и NaN."""
    first = cp.shift_month(EN_START, horizon)
    last = cp.shift_month(last_test, -horizon)
    months = [m for m in cp.month_range(first, last) if m not in ARTICLE_OUTLIERS]
    rows = bu.model_rows(code, features_by_month, target_frame, horizon, months)
    return [row for row in rows if not pd.isna(row[2][horizon])]


def training_rows_upto(rows: list, month: date, horizon: int) -> list:
    """Строки обучения прогноза месяца `M`: только цели `t ≤ M − h` (purge)."""
    last = cp.shift_month(month, -horizon)
    return [row for row in rows if row[0] <= last]


def assert_training_bounds(rows: list, month: date, horizon: int) -> None:
    """Границы окна: `2016-01+h ≤ t ≤ M−h`; нарушение — AssertionError."""
    first = cp.shift_month(EN_START, horizon)
    last = cp.shift_month(month, -horizon)
    for target_month, _features, _targets in rows:
        if not first <= target_month <= last:
            raise AssertionError(f"утечка h{horizon}: строка {target_month} вне [{first}; {last}]")


def predict_month(train_rows: list, names: list, test_rows: list, horizon: int) -> tuple:
    """`(прогноз тестовой строки, не сошёлся)`; конфигурация B3 и медиана train."""
    if not test_rows:
        return None, False
    predictions, failed = en.enet_fit_predict(train_rows, test_rows, names, horizon)
    return predictions[0], failed


def fit_predict_en16(specs: list, features: dict, targets: dict, test_months: list) -> tuple:
    """`({код: {h: {M: прогноз}}}, число недосходов)` — EN, переобучение на каждом M."""
    out, non_converged = {}, 0
    for spec in specs:
        use_mp = bool(spec.bsu_codes)
        base = bu.feature_names(with_mp=True, mode="per_sheet", spec=spec) if use_mp else bu.feature_names(with_mp=False)
        names = en.en_names(base)
        features_by_month = features.get(spec.code, {})
        for horizon in HORIZONS:
            bu.assert_no_leakage_bottomup(test_months, horizon)
            rows = candidate_rows(spec.code, features_by_month, targets[horizon], horizon, test_months[-1])
            predictions = {}
            for month in test_months:
                train_rows = training_rows_upto(rows, month, horizon)
                assert_training_bounds(train_rows, month, horizon)
                if len(train_rows) < MIN_TRAIN:
                    continue
                test_rows = bu.model_rows(spec.code, features_by_month, targets[horizon], horizon, [month])
                value, failed = predict_month(train_rows, names, test_rows, horizon)
                if value is None:
                    continue
                non_converged += int(failed)
                predictions[month] = value
            out.setdefault(spec.code, {})[horizon] = predictions
    return out, non_converged


def combine_two(first: dict, second: dict) -> dict:
    """Среднее двух агрегатов с равными весами на общих месяцах."""
    return {
        horizon: {m: (first[horizon][m] + second[horizon][m]) / 2 for m in sorted(set(first[horizon]) & set(second[horizon]))}
        for horizon in HORIZONS
    }


# ---------------------------------------------------------------------------
# Проверки и гейт
# ---------------------------------------------------------------------------


def r11a_check(ratios: dict) -> dict:
    """R11a: `combo3` и `live_cb` воспроизводят Task 3/9 (допуск 1e-3)."""
    pins = {MODEL_COMBO3: R11A_COMBO3, MODEL_CB: R11A_LIVE_CB}
    models = {
        model: {h: {"ratio": ratios[model][h], "pinned": pins[model][h],
                    "ok": abs(ratios[model][h] - pins[model][h]) <= R11A_TOL} for h in HORIZONS}
        for model in pins if model in ratios
    }
    return {"models": models, "tolerance": R11A_TOL,
            "ok": len(models) == len(pins) and all(v["ok"] for per in models.values() for v in per.values())}


def g11_check(ratios: dict, pairs: dict, mcs_sets: dict) -> dict:
    """G11: h3 лучше (ΔRMSE < 0, DM p < 0,10), h1/h6 не хуже +0,02, MCS на h1/h3/h6."""
    pair = pairs[MODEL_COMBO4][3]
    better = pair["delta"] < 0 and pair["dm_p"] < G11_ALPHA
    noharm = {
        h: {"combo4": ratios[MODEL_COMBO4][h], "combo3": ratios[MODEL_COMBO3][h],
            "limit": ratios[MODEL_COMBO3][h] + NOHARM_MARGIN,
            "ok": ratios[MODEL_COMBO4][h] <= ratios[MODEL_COMBO3][h] + NOHARM_MARGIN + 1e-12}
        for h in (1, 6)
    }
    in_mcs = {h: MODEL_COMBO4 in mcs_sets[h] for h in PRIMARY_HORIZONS}
    return {
        "h3_better": better,
        "h3_weak_ci": pair["delta_ci"][0] <= 0 <= pair["delta_ci"][1],
        "dm_p": pair["dm_p"], "delta": pair["delta"], "delta_ci": list(pair["delta_ci"]),
        "noharm": {"horizons": noharm, "ok": all(v["ok"] for v in noharm.values())},
        "mcs": {"horizons": in_mcs, "ok": all(in_mcs.values())},
        "ok": better and all(v["ok"] for v in noharm.values()) and all(in_mcs.values()),
    }


def cb_errors_and_truth(data: dict) -> tuple:
    """`(истина TARGET_FULL по h, ошибки CB_COMP на normal-54)`."""
    truth = {h: data["target_full"][h].dropna().to_dict() for h in HORIZONS}
    cb_err = {h: cp._cb_errors(data["archive_data"]["sheet"], data["target_full"], h, data["normal"]) for h in HORIZONS}
    return truth, cb_err


def ratios_to_cb(aggregated: dict, data: dict) -> dict:
    """Отношения RMSE к CB_COMP на normal-54 для набора агрегатов."""
    truth, cb_err = cb_errors_and_truth(data)
    out = {}
    for model, by_h in aggregated.items():
        out[model] = {}
        for horizon in HORIZONS:
            months, err = lb.errors_on(by_h[horizon], truth[horizon], data["normal"])
            if months != data["normal"]:
                raise SystemExit(f"{model} h{horizon}: нет прогноза на части normal-месяцев")
            out[model][horizon] = lb.rmse(err) / lb.rmse(cb_err[horizon])
    return out


# ---------------------------------------------------------------------------
# Оценка
# ---------------------------------------------------------------------------


def evaluate(forecasts: dict, data: dict) -> dict:
    import cpi_two_stage_cb_comp as w5b
    import cpi_v4_leaderboard as v4

    normal = data["normal"]
    truth, cb_err = cb_errors_and_truth(data)
    all_test = cp.month_range(cp.TEST_START, cp.TEST_END)
    errors, per_model = {}, {}
    for model, by_h in forecasts.items():
        errors[model], per_model[model] = {}, {}
        for horizon in HORIZONS:
            months, err = lb.errors_on(by_h[horizon], truth[horizon], normal)
            if months != normal:
                raise SystemExit(f"{model} h{horizon}: нет прогноза на части normal-месяцев")
            errors[model][horizon] = err
            _, err_live = lb.errors_on(by_h[horizon], data["t_live"][horizon], normal)
            _, err_all = lb.errors_on(by_h[horizon], truth[horizon], all_test)
            early = [e for m, e in zip(normal, err) if m.year <= 2021]
            late = [e for m, e in zip(normal, err) if m.year >= 2023]
            per_model[model][horizon] = {
                "vs_cb_target_full": w5b.compare_to_cb(err, cb_err[horizon], horizon),
                "rmse_t_live": lb.rmse(err_live),
                "rmse_target_full_all67": lb.rmse(err_all),
                "rmse_2019_2021": lb.rmse(early),
                "rmse_2023_2024": lb.rmse(late),
            }
    pairs = {m: {h: v4.compare_pair(errors[m][h], errors[MODEL_COMBO3][h], h) for h in HORIZONS}
             for m in forecasts if m != MODEL_COMBO3}
    mcs = {}
    for horizon in PRIMARY_HORIZONS:
        losses = {m: [e * e for e in errors[m][horizon]] for m in CANDIDATES}
        losses[CB_COMP] = [e * e for e in cb_err[horizon]]
        included, pvalues = v4.mcs_included(losses)
        mcs[horizon] = {"included": sorted(included), "pvalues": pvalues}
    ratios = {m: {h: per_model[m][h]["vs_cb_target_full"]["ratio"] for h in HORIZONS} for m in forecasts}
    return {
        "per_model": per_model, "pairs_vs_combo3": pairs, "mcs": mcs, "ratios": ratios,
        "g11": g11_check(ratios, pairs, {h: set(mcs[h]["included"]) for h in PRIMARY_HORIZONS}),
        "g3_reference": en.g3_check({h: ratios[MODEL_COMBO4][h] for h in PRIMARY_HORIZONS}),
    }


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def run(archive: Path, panel_csv: Path, public_dir: Path, out_dir: Path) -> dict:
    data = en.prepare(archive, panel_csv, public_dir)
    test_months = data["test_months"]

    cb_groups = lb.fit_predict(data["specs"], data["features"], data["features"], data["targets"],
                               data["train_months"], test_months, with_mp=True)
    forecasts = {MODEL_CB: lb.aggregate_predictions(cb_groups, data["weights"], test_months)}

    regressors = hb.load_regressors(public_dir)
    target_full = {h: {m: float(v) for m, v in data["target_full"][h].dropna().items()} for h in HORIZONS}
    bs_live = {model: {h: hb.bs_predict(data["t_live"][h], regressors[model], h, test_months) for h in HORIZONS}
               for model in (hb.MODEL_ECON, hb.MODEL_ROZN)}
    forecasts.update(bs_live)
    forecasts[MODEL_COMBO3] = {
        h: hb.combine_mean([forecasts[MODEL_CB][h], bs_live[hb.MODEL_ECON][h], bs_live[hb.MODEL_ROZN][h]])
        for h in HORIZONS
    }

    # R11a — до дорогого прогона EN: combo3 и live_cb должны совпасть с Task 3/9.
    checks = {"r11a": r11a_check(ratios_to_cb({m: forecasts[m] for m in (MODEL_CB, MODEL_COMBO3)}, data))}
    if not checks["r11a"]["ok"]:
        raise SystemExit(f"r11a не пройден: {checks['r11a']}")

    en_groups, non_converged = fit_predict_en16(data["specs"], data["features"], data["targets"], test_months)
    forecasts[MODEL_EN16] = lb.aggregate_predictions(en_groups, data["weights"], test_months)
    forecasts[MODEL_COMBO4] = combine_two(forecasts[MODEL_COMBO3], forecasts[MODEL_EN16])

    bs_tf = {model: {h: hb.bs_predict(target_full[h], regressors[model], h, test_months) for h in HORIZONS}
             for model in (hb.MODEL_ECON, hb.MODEL_ROZN)}
    forecasts[MODEL_COMBO3_TF] = {
        h: hb.combine_mean([forecasts[MODEL_CB][h], bs_tf[hb.MODEL_ECON][h], bs_tf[hb.MODEL_ROZN][h]])
        for h in HORIZONS
    }

    result = evaluate(forecasts, data)
    result.update({
        "checks": checks,
        "spec": "docs/superpowers/specs/2026-10-31-cb-comp-h3-en16-combo-design.md",
        "git_sha": lb.git_sha(),
        "inputs": {"panel_csv": lb.sha256(panel_csv), "public_panel_csv": lb.sha256(public_dir / "public_panel.csv"),
                   "sheet_map_csv": lb.sha256(public_dir / "sheet_map.csv"), "target_xlsx": lb.sha256(archive / cp.TARGET_XLSX)},
        "enet": {"alphas": [float(a) for a in cm.ENET_ALPHAS], "l1_ratio": list(cm.ENET_L1_RATIOS),
                 "cv_splits": cm.ENET_CV_SPLITS, "min_train": MIN_TRAIN, "non_converged_fits": non_converged},
        "window": {"start": EN_START.isoformat(), "rule": "2016-01+h <= t <= M-h", "outliers": len(ARTICLE_OUTLIERS)},
        "groups_without_sa": data["groups_without_sa"],
        "groups_count": {m: len(groups) for m, groups in ((MODEL_EN16, en_groups), (MODEL_CB, cb_groups))},
    })
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "predictions.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["model", "horizon", "month", "y_pred"])
        for model in forecasts:
            for horizon in HORIZONS:
                for month in sorted(forecasts[model][horizon]):
                    writer.writerow([model, horizon, month.isoformat(), repr(forecasts[model][horizon][month])])
    (out_dir / "result.json").write_text(json.dumps(lb._jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None)
    parser.add_argument("--panel", default=str(lb.PANEL_CSV))
    parser.add_argument("--public-dir", default=str(lb.PUBLIC_DIR))
    parser.add_argument("--out", default=str(OUT_DIR))
    args = parser.parse_args(argv)
    archive = cp.resolve_archive(args.archive)
    result = run(archive, Path(args.panel), Path(args.public_dir), Path(args.out))
    g11 = result["g11"]
    summary = {
        "g11": {"ok": g11["ok"], "h3_better": g11["h3_better"], "h3_weak_ci": g11["h3_weak_ci"],
                "dm_p": g11["dm_p"], "delta": g11["delta"], "delta_ci": g11["delta_ci"],
                "noharm": g11["noharm"], "mcs": g11["mcs"]},
        "r11a": result["checks"]["r11a"]["ok"],
        "ratios": {m: {h: round(r, 3) for h, r in per.items()} for m, per in result["ratios"].items()},
        "classes": {m: {h: v["vs_cb_target_full"]["class"] for h, v in per.items()} for m, per in result["per_model"].items()},
        "mcs": result["mcs"],
        "enet": result["enet"]["non_converged_fits"],
    }
    print(json.dumps(lb._jsonable(summary), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
