#!/usr/bin/env python3
"""h1: комбинация `live_cb` с BS_ECON16/BS_ROZN16 статьи, истина `TARGET_FULL` — Task 9 #436.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-10-31-cb-comp-h1-bs-combo-design.md`.

* **`live_cb`** — Task 3 без изменений (`cb_comp_live_bottomup.fit_predict`);
* **BS_ECON16 / BS_ROZN16** — OLS статьи Кошкина–Крылова (2025):
  `T_LIVE[h](t) ~ X(t−h) + T_LIVE[h](t−h)`, X — SA ценовые ожидания ЦБ
  (экономика / розница), строки `2016-01+h ≤ t ≤ M−h` без выбросов статьи,
  переобучение на каждом тестовом месяце;
* **`combo3`** — среднее трёх с равными весами;
* **R9** — воспроизведение BS статьи, сверка регрессоров, `live_cb` = Task 3;
* **оценка** (normal-54) — против CB_COMP, попарно против `live_cb`, MCS 90 %,
  гейт G9 и условие не-вреда.

Только чтение ClickHouse и локальных файлов; в БД не пишет. Запуск —
`make cb-comp-h1-bs-combo`, тесты — `make cb-comp-h1-bs-combo-test`.
"""

import argparse
import csv
import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_live_en as en
from models.cb_comp import cb_comp_parity as cp
from models.cpi import cpi_monthly as cm

# ---------------------------------------------------------------------------
# Предзарегистрированные константы (спека §4–§7)
# ---------------------------------------------------------------------------

HORIZONS = bu.HORIZONS
PRIMARY_HORIZONS = (1, 3, 6)
BS_START = date(2016, 1, 1)
ARTICLE_OUTLIERS = cp.TRAIN_OUTLIER_MONTHS | cp.OUTLIER_MONTHS
MODEL_CB = "live_cb"
MODEL_ECON = "bs_econ16"
MODEL_ROZN = "bs_rozn16"
MODEL_COMBO = "combo3"
CANDIDATES = (MODEL_CB, MODEL_ECON, MODEL_ROZN, MODEL_COMBO)
REFERENCE = ("combo3_tf", "bs_econ16_tf", "bs_rozn16_tf", "bs_mean")
CB_COMP = "cb_comp"
G9_RATIO_H1 = 1.10
NOHARM_MARGIN = 0.02
R9A_EXPECTED = {"bs_econ16": 0.167225, "bs_rozn16": 0.169438}
R9A_TOL = 1e-4
R9B_MIN_CORR = 0.99
TASK3_LIVE_CB_RATIOS = {1: 1.193, 2: 1.013, 3: 1.076, 6: 1.062}
R9C_TOL = 0.001
# Task 14: пины R9c по версии цели; @2009 заполняется после прогона Task 3 @2009.
VERSIONS = lb.VERSIONS
DEFAULT_VERSION = lb.DEFAULT_VERSION
TASK3_PINS = {
    "2010": TASK3_LIVE_CB_RATIOS,
    "2009": {1: 1.2461360170004323, 2: 1.0472712693002966, 3: 1.063756111772496, 6: 1.0295619483425622},
}
X_ECON_SERIES = "price_expectations_sa"
X_ROZN_SLUG = "trade_retail"
X_ROZN_INDICATOR = "price_exp"
AUTHORS_SERIES = {"bs_econ16": "X1", "bs_rozn16": "X2"}
SERIES2016 = Path("Models") / "BS LM_2016" / "series2016.xlsx"
SERIES2009 = Path("Models") / "BS LM_2009" / "series2009.xlsx"
OUT_DIR = ROOT / "build" / "cb-comp-h1-bs-combo"


# ---------------------------------------------------------------------------
# BS-модели статьи
# ---------------------------------------------------------------------------


def row_features(target: dict, x: dict, month: date, horizon: int):
    """`[X(t−h), T(t−h)]` строки `t` или `None`, если точки нет."""
    lag = cp.shift_month(month, -horizon)
    if lag not in x or lag not in target:
        return None
    return [x[lag], target[lag]]


def training_months(target: dict, x: dict, horizon: int, origin_month: date) -> list:
    """Строки обучения прогноза месяца `M`: `2016-01+h ≤ t ≤ M−h`, без выбросов статьи.

    Таргет строки `t` — накопление по `t`, ИПЦ за `M−h` известен в месяце
    origin `M−(h−1)`; поэтому строки позже `M−h` — утечка (у авторов есть при h ≥ 2).
    """
    first = cp.shift_month(BS_START, horizon)
    last = cp.shift_month(origin_month, -horizon)
    return [
        m for m in sorted(target)
        if first <= m <= last and m not in ARTICLE_OUTLIERS and row_features(target, x, m, horizon) is not None
    ]


def bs_predict(target: dict, x: dict, horizon: int, test_months: list) -> dict:
    """Прогнозы `{M: ŷ}` BS-модели с переобучением на каждом месяце теста."""
    out = {}
    for month in test_months:
        features = row_features(target, x, month, horizon)
        rows = training_months(target, x, horizon, month)
        if features is None or len(rows) < 3:
            continue
        model = LinearRegression().fit(
            np.array([row_features(target, x, m, horizon) for m in rows]), np.array([target[m] for m in rows])
        )
        out[month] = float(model.predict(np.array([features]))[0])
    return out


def combine_mean(predictions: list) -> dict:
    """Среднее прогнозов с равными весами на общих месяцах."""
    common = set(predictions[0])
    for pred in predictions[1:]:
        common &= set(pred)
    return {m: sum(p[m] for p in predictions) / len(predictions) for m in sorted(common)}


def authors_bs_predict(series: pd.DataFrame, column: str, horizon: int) -> pd.Series:
    """Процедура ноутбука авторов (`data_lag_long`, regime `lag`) — для R9a.

    Дословно: обучение на шаге i — `train + test.iloc[:i]` (для h ≥ 2 — с
    утечкой будущего таргета; в кандидатах Task 9 не используется).
    """
    outliers = [pd.Timestamp(m) for m in ARTICLE_OUTLIERS]
    y = f"YH{horizon}"
    frame = series[[y, column]].copy()
    frame[column] = frame[column].shift(horizon)
    frame["ipc_lag"] = frame[y].shift(horizon)
    frame = frame.dropna()
    test = frame[frame.index.year >= 2019]
    train = frame[~frame.index.isin(test.index)]
    train = train[~train.index.isin(outliers)]
    out = test[y].copy()
    for i in range(test.shape[0]):
        fit = pd.concat([train, test.iloc[:i]])
        fit = fit[~fit.index.isin(outliers)]
        model = LinearRegression().fit(fit.loc[:, fit.columns != y], fit[y])
        out.iloc[i] = model.predict(test.loc[:, test.columns != y].iloc[i:i + 1])[0]
    return out


# ---------------------------------------------------------------------------
# Гейты
# ---------------------------------------------------------------------------


def g9_check(ratio_h1: float, pair_h1: dict) -> dict:
    """G9: `combo3` h1 ≤ 1,10 к CB_COMP и «лучше `live_cb`» (DM p < 0,05, CI ниже 0)."""
    better = en.better_than_cb(pair_h1)
    return {
        "ratio_h1": ratio_h1, "threshold": G9_RATIO_H1, "ratio_ok": ratio_h1 <= G9_RATIO_H1,
        "better_than_live_cb": better, "dm_p": pair_h1["dm_p"], "delta_ci": list(pair_h1["delta_ci"]),
        "ok": ratio_h1 <= G9_RATIO_H1 and better,
    }


def noharm_check(combo: dict, live_cb: dict) -> dict:
    """Не-вред: отношение `combo3` на h3/h6 не выше `live_cb` + 0,02."""
    per_h = {h: {"combo3": combo[h], "live_cb": live_cb[h], "limit": live_cb[h] + NOHARM_MARGIN,
                 "ok": combo[h] <= live_cb[h] + NOHARM_MARGIN + 1e-12} for h in (3, 6)}
    return {"horizons": per_h, "ok": all(v["ok"] for v in per_h.values())}


def r9c_check(ratios: dict, expected: dict = None) -> dict:
    expected = expected or TASK3_LIVE_CB_RATIOS
    per_h = {h: {"ratio": ratios[h], "task3": expected[h],
                 "ok": abs(ratios[h] - expected[h]) <= R9C_TOL} for h in HORIZONS}
    return {"horizons": per_h, "ok": all(v["ok"] for v in per_h.values())}


# ---------------------------------------------------------------------------
# Данные
# ---------------------------------------------------------------------------


def load_regressors(public_dir: Path) -> dict:
    """`{model: {month: X}}` — live-источники ClickHouse (спека §3)."""
    survey = cm.load_survey()
    econ = {cp.month_start(d): float(v) for d, v in survey[X_ECON_SERIES]}
    panel = bu.load_public_panel(public_dir / "public_panel.csv")
    sheet_map = bu.load_sheet_map(public_dir / "sheet_map.csv")
    codes = sorted(code for code, slug in sheet_map.items() if slug == X_ROZN_SLUG)
    if not codes:
        raise SystemExit(f"в sheet_map нет кодов листа {X_ROZN_SLUG}")
    part = panel[(panel["code"] == codes[0]) & (panel["indicator"] == X_ROZN_INDICATOR)]
    rozn = {cp.month_start(d): float(v) for d, v in zip(part["date"], part["value"])}
    if not econ or not rozn:
        raise SystemExit("пустой регрессор BS")
    return {MODEL_ECON: econ, MODEL_ROZN: rozn}


def read_authors_series(archive: Path, name: Path) -> pd.DataFrame:
    frame = pd.read_excel(archive / name, sheet_name="data")
    return frame.set_index(pd.to_datetime(frame["date"])).drop(columns="date")


def r9a(archive: Path, normal: list) -> dict:
    series = read_authors_series(archive, SERIES2016)
    norm = [pd.Timestamp(m) for m in normal]
    out = {}
    for model, column in AUTHORS_SERIES.items():
        pred = authors_bs_predict(series, column, 1)
        keep = pred.index.isin(norm)
        value = float(np.sqrt(np.mean((pred[keep] - series["YH1"].reindex(pred.index)[keep]) ** 2)))
        out[model] = {"rmse": value, "expected": R9A_EXPECTED[model], "n": int(keep.sum()),
                      "ok": abs(value - R9A_EXPECTED[model]) <= R9A_TOL}
    return {"models": out, "ok": all(v["ok"] for v in out.values())}


def r9b(archive: Path, regressors: dict) -> dict:
    series = read_authors_series(archive, SERIES2009)
    out = {}
    for model, column in AUTHORS_SERIES.items():
        ours = pd.Series({pd.Timestamp(m): v for m, v in regressors[model].items()})
        joined = pd.concat([ours, series[column]], axis=1).dropna()
        corr = float(joined.corr().iloc[0, 1])
        out[model] = {"corr": corr, "n": len(joined), "max_abs_diff": float((joined.iloc[:, 0] - joined.iloc[:, 1]).abs().max()),
                      "ok": corr >= R9B_MIN_CORR}
    return {"models": out, "ok": all(v["ok"] for v in out.values())}


# ---------------------------------------------------------------------------
# Оценка
# ---------------------------------------------------------------------------


def evaluate(forecasts: dict, data: dict, version: str = DEFAULT_VERSION) -> dict:
    import cpi_two_stage_cb_comp as w5b
    import cpi_v4_leaderboard as v4

    normal = data["normal"]
    target_full = data["target_full"]
    truth = {h: target_full[h].dropna().to_dict() for h in HORIZONS}
    all_test = cp.month_range(cp.TEST_START, cp.TEST_END)
    cb_err = {h: cp._cb_errors(data["archive_data"]["sheet"], target_full, h, normal) for h in HORIZONS}
    errors, per_model = {}, {}
    for model, by_h in forecasts.items():
        errors[model], per_model[model] = {}, {}
        for h in HORIZONS:
            months, err = lb.errors_on(by_h[h], truth[h], normal)
            if months != normal:
                raise SystemExit(f"{model} h{h}: нет прогноза на части normal-месяцев")
            errors[model][h] = err
            _, err_live = lb.errors_on(by_h[h], data["t_live"][h], normal)
            _, err_all = lb.errors_on(by_h[h], truth[h], all_test)
            early = [e for m, e in zip(normal, err) if m.year <= 2021]
            late = [e for m, e in zip(normal, err) if m.year >= 2023]
            per_model[model][h] = {
                "vs_cb_target_full": w5b.compare_to_cb(err, cb_err[h], h),
                "rmse_t_live": lb.rmse(err_live),
                "rmse_target_full_all67": lb.rmse(err_all),
                "rmse_2019_2021": lb.rmse(early),
                "rmse_2023_2024": lb.rmse(late),
            }
    pairs = {m: {h: v4.compare_pair(errors[m][h], errors[MODEL_CB][h], h) for h in HORIZONS}
             for m in forecasts if m != MODEL_CB}
    mcs = {}
    for h in PRIMARY_HORIZONS:
        losses = {m: [e * e for e in errors[m][h]] for m in CANDIDATES}
        losses[CB_COMP] = [e * e for e in cb_err[h]]
        included, pvalues = v4.mcs_included(losses)
        mcs[h] = {"included": sorted(included), "pvalues": pvalues}
    ratios = {m: {h: per_model[m][h]["vs_cb_target_full"]["ratio"] for h in HORIZONS} for m in forecasts}
    return {
        "per_model": per_model, "pairs_vs_live_cb": pairs, "mcs": mcs, "ratios": ratios,
        "r9c": r9c_check(ratios[MODEL_CB], TASK3_PINS[version]),
        "g9": g9_check(ratios[MODEL_COMBO][1], pairs[MODEL_COMBO][1]),
        "noharm": noharm_check(ratios[MODEL_COMBO], ratios[MODEL_CB]),
        "g3_reference": en.g3_check({h: ratios[MODEL_COMBO][h] for h in PRIMARY_HORIZONS}),
    }


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def run(archive: Path, panel_csv: Path, public_dir: Path, out_dir: Path, version: str = DEFAULT_VERSION) -> dict:
    data = en.prepare(archive, panel_csv, public_dir)
    normal, test_months = data["normal"], data["test_months"]
    checks = {"r9a": r9a(archive, normal)}
    regressors = load_regressors(public_dir)
    checks["r9b"] = r9b(archive, regressors)
    for name in ("r9a", "r9b"):
        if not checks[name]["ok"]:
            raise SystemExit(f"{name} не пройден: {checks[name]}")

    groups = lb.fit_predict(data["specs"], data["features"], data["features"], data["targets"],
                            data["train_months"], test_months, with_mp=True)
    forecasts = {MODEL_CB: lb.aggregate_predictions(groups, data["weights"], test_months)}
    target_full = {h: {m: float(v) for m, v in data["target_full"][h].dropna().items()} for h in HORIZONS}
    for suffix, target in (("", data["t_live"]), ("_tf", target_full)):
        for model in (MODEL_ECON, MODEL_ROZN):
            forecasts[model + suffix] = {h: bs_predict(target[h], regressors[model], h, test_months) for h in HORIZONS}
        forecasts[MODEL_COMBO + suffix] = {
            h: combine_mean([forecasts[MODEL_CB][h], forecasts[MODEL_ECON + suffix][h], forecasts[MODEL_ROZN + suffix][h]])
            for h in HORIZONS
        }
    forecasts["bs_mean"] = {h: combine_mean([forecasts[MODEL_ECON][h], forecasts[MODEL_ROZN][h]]) for h in HORIZONS}

    result = evaluate(forecasts, data, version)
    if not result["r9c"]["ok"]:
        raise SystemExit(f"r9c не пройден: {result['r9c']}")
    result.update({
        "checks": checks,
        "version": version,
        "spec": "docs/superpowers/specs/2026-10-31-cb-comp-h1-bs-combo-design.md",
        "git_sha": lb.git_sha(),
        "inputs": {"panel_csv": lb.sha256(panel_csv), "public_panel_csv": lb.sha256(public_dir / "public_panel.csv"),
                   "sheet_map_csv": lb.sha256(public_dir / "sheet_map.csv"), "target_xlsx": lb.sha256(archive / cp.TARGET_XLSX),
                   "series2016_xlsx": lb.sha256(archive / SERIES2016), "series2009_xlsx": lb.sha256(archive / SERIES2009)},
        "regressors": {m: {"first": min(v).isoformat(), "last": max(v).isoformat(), "n": len(v)} for m, v in regressors.items()},
        "groups_without_sa": data["groups_without_sa"],
    })
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "predictions.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["model", "horizon", "month", "y_pred"])
        for model in sorted(forecasts):
            for h in HORIZONS:
                for month in sorted(forecasts[model][h]):
                    writer.writerow([model, h, month.isoformat(), repr(forecasts[model][h][month])])
    (out_dir / "result.json").write_text(json.dumps(lb._jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None)
    parser.add_argument("--panel", default=None, help="по умолчанию — панель версии")
    parser.add_argument("--public-dir", default=str(lb.PUBLIC_DIR))
    parser.add_argument("--version", choices=VERSIONS, default=DEFAULT_VERSION,
                        help="версия цели: 2010 — канон, 2009 — T_LIVE с 2009-01")
    parser.add_argument("--out", default=None, help="по умолчанию build/cb-comp-h1-bs-combo[-<версия>]")
    args = parser.parse_args(argv)
    panel = Path(args.panel) if args.panel else lb.default_panel(args.version)
    lb.check_panel_version(lb.load_group_panel(panel)[0], args.version)
    out_dir = Path(args.out) if args.out else lb.versioned_dir(OUT_DIR, args.version)
    archive = cp.resolve_archive(args.archive)
    result = run(archive, panel, Path(args.public_dir), out_dir, args.version)
    summary = {
        "g9": result["g9"], "noharm": result["noharm"], "g3_reference": result["g3_reference"],
        "r9": {"r9a": result["checks"]["r9a"]["ok"], "r9b": result["checks"]["r9b"]["ok"], "r9c": result["r9c"]["ok"]},
        "ratios": {m: {h: round(r, 3) for h, r in per.items()} for m, per in result["ratios"].items()},
        "classes": {m: {h: v["vs_cb_target_full"]["class"] for h, v in per.items()} for m, per in result["per_model"].items()},
        "mcs": result["mcs"],
    }
    print(json.dumps(lb._jsonable(summary), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
