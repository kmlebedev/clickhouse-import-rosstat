#!/usr/bin/env python3
"""h1: недельный агрегат без ЖКУ и с сезонностью поверх `combo3` — Task 4b #446.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-10-31-cb-comp-h1-weekly-ex-utilities-design.md`.

* **`ЖКУ_wk`** — недельный индекс группы 9470 из 6 рядов (вода, водоотведение,
  отопление, сжиженный газ, электроэнергия); покрытие 83,1 % веса 9470;
  плата за жильё (9416) не входит в 9470 и не вычитается;
* **`agg_ex`** — официальный недельный агрегат без ЖКУ:
  `log agg_ex = (log agg − w_ЖКУ·log ЖКУ_wk)/(1 − w_ЖКУ)`;
* **`wk_agg_ex`** — OLS с 11 месячными дамми, цель `T_LIVE[1]`;
* **`wk_mix_ex`** — `0,5·combo3 + 0,5·wk_agg_ex`;
* **`combo3`/`combo3_wk`** — готовые прогнозы Task 4 (`predictions.csv`
  `0a67483f…`, R12), не пересчитываются;
* **G11** — критерий G10 на primary 2023+ (45 месяцев), Holm α = 0,025.

Только чтение ClickHouse и локальных файлов; в БД не пишет. Запуск —
`make cb-comp-h1-weekly-ex`, тесты — `make cb-comp-h1-weekly-ex-test`.
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
from sklearn.linear_model import LinearRegression

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_h1_weekly as tw
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_parity as cp
from models.cb_comp import cb_comp_weekly_groups as wg
from models.cpi import cpi_monthly as cm
from models.cpi import cpi_public_panel_audit as audit
from models.cpi import cpi_weekly_aggregate as agg

# ---------------------------------------------------------------------------
# Предзарегистрированные константы (спека §3–§7)
# ---------------------------------------------------------------------------

UTILITIES_CODE = "9470"
HORIZON = 1
NAN = float("nan")

# Ручной маппинг: ряд v_rosstat_macro → коды веса 9470 (2024; веса — по годам).
UTIL_SERIES = {
    "Водоснабжение холодное, м3": ("9457",),
    "Водоснабжение горячее, м3": ("9448",),
    "Водоотведение, м3": ("9458",),
    "Отопление, Гкал": ("9449",),
    "Газ сжиженный, м3": ("9459",),
    "Услуги по снабжению электроэнергией": ("9471", "9472", "9473", "9474"),
}
EXCLUDED_HOUSING = "Плата за жилье в домах государственного и муниципального жилищных фондов, м2 общей площади"
EXCLUDED_HOUSING_CODE = "9416"
UTIL_COV_MIN = 0.5

MODEL_COMBO = "combo3"
MODEL_COMBO_WK = "combo3_wk"
MODEL_AGG_EX = "wk_agg_ex"
MODEL_MIX_EX = "wk_mix_ex"
MODEL_AGG_EX_NOD = "wk_agg_ex_nodummies"
MODEL_MIX_EX_NOD = "wk_mix_ex_nodummies"
CO_PRIMARY = (MODEL_AGG_EX, MODEL_MIX_EX)
MODELS = (MODEL_COMBO, MODEL_COMBO_WK, MODEL_AGG_EX, MODEL_MIX_EX, MODEL_AGG_EX_NOD, MODEL_MIX_EX_NOD)
REFERENCE_MODELS = (MODEL_AGG_EX_NOD, MODEL_MIX_EX_NOD)

REF_START = date(2022, 1, 1)
PRIMARY_START = date(2023, 1, 1)
HOLM_ALPHA = 0.025
R12_TOL = 1e-6
R12_PINNED = {MODEL_COMBO: 0.9698445785532128, MODEL_COMBO_WK: 0.9630314456250352}
R12_PREDICTIONS_SHA256 = "0a67483f0e64917436e60645e5dad2d1ab8421ce3cdb3171dae8e85fce1aea5f"
R13_PINNED = {"covered_weight": 0.04942, "group_weight": 0.05944, "coverage": 0.8314}

PANEL_CSV = ROOT / "build" / "cb-comp-groups" / "panel.csv"
PUBLIC_DIR = ROOT / "build" / "cb-comp-parity"
BASE_PREDICTIONS = ROOT / "build" / "cb-comp-h1-weekly-ex" / "base_predictions.csv"
OUT_DIR = ROOT / "build" / "cb-comp-h1-weekly-ex"


# ---------------------------------------------------------------------------
# Утилиты: маппинг, ЖКУ_wk, agg_ex
# ---------------------------------------------------------------------------


def utilities_summary(weekly: dict, dictionary: "wg.gp.Dictionary", leaf_weights_2024: dict, group_weight_2024: float) -> dict:
    """Проверка маппинга 9470: 6 рядов, коды, покрытие; 9416 — вне 9470."""
    leaves = set(dictionary.leaves_by_ancestor.get(UTILITIES_CODE, []))
    rows = []
    for series, codes in UTIL_SERIES.items():
        if series not in weekly:
            raise SystemExit(f"нет недельного ряда ЖКУ {series!r}")
        for code in codes:
            if code not in leaves:
                raise SystemExit(f"код {code} не входит в 9470")
        weight = sum(float(leaf_weights_2024.get(code, 0.0)) for code in codes)
        rows.append({"series": series, "weight_codes": ",".join(codes), "weight_2024": weight, "included": True})
    covered = sum(row["weight_2024"] for row in rows)
    if EXCLUDED_HOUSING_CODE in leaves:
        raise SystemExit("9416 неожиданно входит в 9470 — маппинг пересмотреть")
    excluded = {
        "series": EXCLUDED_HOUSING, "weight_codes": EXCLUDED_HOUSING_CODE,
        "weight_2024": float(leaf_weights_2024.get(EXCLUDED_HOUSING_CODE, 0.0)),
        "included": False, "reason": "9416 не входит в 9470; не вычитается",
    }
    coverage = covered / group_weight_2024 if group_weight_2024 else 0.0
    return {
        "rows": rows, "excluded": excluded, "covered_weight": covered,
        "group_weight": group_weight_2024, "coverage": coverage,
        "n_series": len(rows), "n_codes": sum(len(codes) for codes in UTIL_SERIES.values()),
        "excluded_present": EXCLUDED_HOUSING in weekly,
    }


def utilities_features(weekly: dict, leaf_weights: dict, group_weights: dict, months: list) -> dict:
    """`{месяц: {wk_g_mtd, wk_g_cov, …}}` — недельный индекс ЖКУ (9470)."""
    missing = sorted(series for series in UTIL_SERIES if series not in weekly)
    if missing:
        raise SystemExit(f"нет недельных рядов ЖКУ: {missing}")
    periods = wg.periods_by_series({series: weekly[series] for series in UTIL_SERIES})
    by_group = {
        UTILITIES_CODE: [
            {"series": series, "weight_code": code}
            for series, codes in UTIL_SERIES.items() for code in codes
        ]
    }
    out = {}
    for month in months:
        leaf = wg.year_table(leaf_weights, wg.weight_year_for(month))
        groups = {UTILITIES_CODE: bu.weight_for_month(group_weights, month).get(UTILITIES_CODE, 0.0)}
        out[month] = wg.weekly_group_features(
            periods, by_group, leaf, groups, month, cm.cutoff_for(month), [UTILITIES_CODE]
        )[UTILITIES_CODE]
    return out


def ex_value(value_pct: float, util_pct: float, weight: float) -> float:
    """`agg_ex` в п.п.: `(log(1+agg) − w·log(1+ЖКУ))/(1−w)`; NaN при пропуске."""
    if not 0.0 <= weight < 1.0 or math.isnan(value_pct) or math.isnan(util_pct):
        return NAN
    log_ex = (math.log1p(value_pct / 100.0) - weight * math.log1p(util_pct / 100.0)) / (1.0 - weight)
    return math.expm1(log_ex) * 100.0


def agg_ex_features(weekly: dict, months: list, util_features: dict, group_weights: dict) -> dict:
    """`{месяц: {wkagg_ex_mtd, util_cov, w_zhku, agg_mtd, util_mtd, agg_cov}}`."""
    points = weekly[audit.WEEKLY_AGGREGATE]
    out = {}
    for month in months:
        base = agg.wkagg_features(points, month, cm.cutoff_for(month))
        util = util_features.get(month, {})
        weight = bu.weight_for_month(group_weights, month).get(UTILITIES_CODE, 0.0)
        util_cov = float(util.get("wk_g_cov", 0.0))
        item = {
            "wkagg_ex_mtd": NAN, "util_cov": util_cov, "w_zhku": weight,
            "agg_mtd": base.get("wkagg_mtd", NAN), "util_mtd": util.get("wk_g_mtd", NAN),
            "agg_cov": base.get("wkagg_cov", 0.0),
        }
        if util_cov >= UTIL_COV_MIN:
            item["wkagg_ex_mtd"] = ex_value(item["agg_mtd"], item["util_mtd"], weight)
        out[month] = item
    return out


def coverage_by_year(leaf_weights: dict, group_weights: dict, years=range(2016, 2025)) -> list:
    """Покрытие веса 9470 недельными рядами по годам."""
    rows = []
    for year in years:
        leaf = leaf_weights.get(year) or wg.year_table(leaf_weights, year)
        group = group_weights.get(year, {}).get(UTILITIES_CODE, 0.0)
        covered = sum(float(leaf.get(code, 0.0)) for codes in UTIL_SERIES.values() for code in codes)
        rows.append({"year": year, "covered_weight": covered, "group_weight": group,
                     "coverage": covered / group if group else 0.0})
    return rows


# ---------------------------------------------------------------------------
# Сезонная OLS
# ---------------------------------------------------------------------------


def design(month: date, features: dict, dummies: bool = True) -> list:
    """`[1, wkagg_ex_mtd, (11 месячных дамми)]`."""
    row = [1.0, float(features.get("wkagg_ex_mtd", NAN))]
    if dummies:
        row.extend(1.0 if month.month == m else 0.0 for m in range(2, 13))
    return row


def ols_ex_predict(features: dict, target: dict, train_months: list, test_months: list, dummies: bool = True) -> dict:
    """OLS с медианной импутацией `wkagg_ex_mtd`; перефит на каждом M."""
    train = [
        m for m in train_months
        if m in features and m in target and m not in tw.SHOCK_MONTHS and not math.isnan(float(target[m]))
    ]
    if len(train) < bu.MIN_TRAIN_ROWS:
        return {}
    matrix = np.asarray([design(m, features[m], dummies) for m in train], dtype=float)
    median = float(np.nanmedian(matrix[:, 1]))
    if math.isnan(median):
        median = 0.0
    matrix[:, 1] = np.where(np.isnan(matrix[:, 1]), median, matrix[:, 1])
    model = LinearRegression().fit(matrix, np.asarray([float(target[m]) for m in train], dtype=float))
    out = {}
    for month in test_months:
        if month not in features:
            continue
        row = np.asarray([design(month, features[month], dummies)], dtype=float)
        row[0, 1] = median if math.isnan(row[0, 1]) else row[0, 1]
        out[month] = float(model.predict(row)[0])
    return out


def agg_ex_wf(features: dict, target: dict, all_train: list, months: list, dummies: bool = True) -> dict:
    """Ежемесячный expanding-прогноз `wk_agg_ex`."""
    out = {}
    for month in months:
        prediction = ols_ex_predict(features, target, [t for t in all_train if t < month], [month], dummies)
        if month not in prediction:
            raise SystemExit(f"{MODEL_AGG_EX}: нет прогноза на {month}")
        out[month] = prediction[month]
    return out


# ---------------------------------------------------------------------------
# База Task 4 и гейт
# ---------------------------------------------------------------------------


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def load_base_predictions(path: Path, months: list) -> dict:
    """`{model: {месяц: прогноз}}` из `predictions.csv` Task 4; sha256 пинится."""
    if not path.exists():
        raise SystemExit(f"нет базы Task 4: {path}; сначала `make cb-comp-h1-weekly` (Task 4)")
    digest = sha256(path)
    if digest != R12_PREDICTIONS_SHA256:
        raise SystemExit(f"R12: sha256 базы {digest} != {R12_PREDICTIONS_SHA256}")
    wanted = set(months)
    out = {model: {} for model in (MODEL_COMBO, MODEL_COMBO_WK)}
    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row["arena"] != "wf2022" or row["model"] not in out:
                continue
            month = date.fromisoformat(row["month"])
            if month in wanted:
                out[row["model"]][month] = float(row["y_pred"])
    missing = {model: [m for m in months if m not in values] for model, values in out.items()}
    if any(missing.values()):
        raise SystemExit(f"R12: в базе Task 4 нет месяцев: {missing}")
    return out


def g11_check(pairs: dict, n_required: int) -> dict:
    """G11: Holm по двум co-primary; CI ΔRMSE ниже 0; победитель."""
    pvalues = {name: pairs[name]["dm_p"] for name in CO_PRIMARY if pairs.get(name)}
    adjusted = tw.holm(pvalues)
    winner = min(pvalues, key=pvalues.get) if pvalues else None
    pair = pairs.get(winner) if winner else None
    ok = bool(
        winner and pair and pair["n"] == n_required
        and adjusted[winner] < 2 * HOLM_ALPHA
        and pair["delta_ci"][1] < 0
    )
    return {
        "winner": winner, "pvalues": pvalues, "holm": adjusted,
        "alpha": HOLM_ALPHA, "n_required": n_required,
        "delta_ci": pair["delta_ci"] if pair else None, "ok": ok,
    }


def r12_check(evaluation: dict, predictions_path: Path) -> dict:
    out = {}
    for name, expected in R12_PINNED.items():
        got = evaluation["per_model"][name]["rmse"]
        out[name] = {"rmse": got, "expected": expected, "abs_diff": abs(got - expected), "ok": abs(got - expected) <= R12_TOL}
    out["predictions_sha256"] = sha256(predictions_path)
    out["ok"] = all(item["ok"] for item in out.values() if isinstance(item, dict) and "ok" in item)
    out["ok"] = out["ok"] and out["predictions_sha256"] == R12_PREDICTIONS_SHA256
    return out


def r13_check(utilities: dict) -> dict:
    covered = utilities["covered_weight"]
    group = utilities["group_weight"]
    coverage = utilities["coverage"]
    return {
        "covered_weight": covered, "group_weight": group, "coverage": coverage,
        "expected": R13_PINNED,
        "ok": abs(covered - R13_PINNED["covered_weight"]) <= 1e-6
        and abs(group - R13_PINNED["group_weight"]) <= 1e-6
        and abs(coverage - R13_PINNED["coverage"]) <= 1e-4,
    }


def subperiods(models: dict, truth: dict, months: list) -> dict:
    out = {}
    for label, test in (
        ("2023", lambda m: m.year == 2023),
        ("2024", lambda m: m.year == 2024),
        ("2025_2026", lambda m: m.year >= 2025),
    ):
        subset = [m for m in months if test(m)]
        out[label] = {
            name: lb.rmse([values[m] - truth[m] for m in subset if m in values and m in truth])
            for name, values in models.items() if values
        }
    return out


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def run(archive: Path, panel_csv: Path, public_dir: Path, base_predictions: Path, out_dir: Path) -> dict:
    data = tw.prepare(archive, panel_csv, public_dir)
    t_live = data["t_live"][HORIZON]
    months = [m for m in data["feature_months"] if m >= REF_START and m in t_live]
    primary = [m for m in months if m >= PRIMARY_START]
    all_train = [m for m in data["feature_months"] if m >= date(2010, 3, 1)]
    if len(months) != 57 or len(primary) != 45:
        raise SystemExit(f"окна не совпали: 2022+ {len(months)}, 2023+ {len(primary)}")

    base = load_base_predictions(base_predictions, months)
    weekly = audit.load_weekly()
    dictionary = wg.load_group_dictionary(archive)
    leaf_weights = wg.load_leaf_weights(archive)
    group_weights = bu.load_weights(archive, lb.WEIGHT_YEARS, data["specs"])
    leaf_2024 = wg.year_table(leaf_weights, 2024)
    group_2024 = group_weights.get(2024, {}).get(UTILITIES_CODE, 0.0)
    utilities = utilities_summary(weekly, dictionary, leaf_2024, group_2024)
    util_features = utilities_features(weekly, leaf_weights, group_weights, data["feature_months"])
    features_ex = agg_ex_features(weekly, data["feature_months"], util_features, group_weights)

    agg_ex = agg_ex_wf(features_ex, t_live, all_train, months, dummies=True)
    agg_ex_nod = agg_ex_wf(features_ex, t_live, all_train, months, dummies=False)
    models = {
        MODEL_COMBO: base[MODEL_COMBO],
        MODEL_COMBO_WK: base[MODEL_COMBO_WK],
        MODEL_AGG_EX: agg_ex,
        MODEL_MIX_EX: tw.mix(base[MODEL_COMBO], agg_ex),
        MODEL_AGG_EX_NOD: agg_ex_nod,
        MODEL_MIX_EX_NOD: tw.mix(base[MODEL_COMBO], agg_ex_nod),
    }
    ref_eval = tw.evaluate(models, t_live, months, None)
    r12 = r12_check(ref_eval, base_predictions)
    if not r12["ok"]:
        raise SystemExit(f"R12 не пройден: {r12}")
    r13 = r13_check(utilities)
    if not r13["ok"]:
        raise SystemExit(f"R13 не пройден: {r13}")

    primary_eval = tw.evaluate(models, t_live, primary, None)
    no_eval = tw.evaluate(models, t_live, [m for m in months if m != date(2022, 3, 1)], None)
    result = {
        "spec": "docs/superpowers/specs/2026-10-31-cb-comp-h1-weekly-ex-utilities-design.md",
        "git_sha": lb.git_sha(),
        "inputs": {
            "panel_csv": sha256(panel_csv), "public_panel_csv": sha256(public_dir / "public_panel.csv"),
            "sheet_map_csv": sha256(public_dir / "sheet_map.csv"), "target_xlsx": sha256(archive / cp.TARGET_XLSX),
            "base_predictions": sha256(base_predictions),
        },
        "utilities": {**utilities, "coverage_by_year": coverage_by_year(leaf_weights, group_weights)},
        "r12": r12,
        "r13": r13,
        "reference": {"n": len(months), "evaluation": ref_eval, "no_2022_03": no_eval},
        "primary": {
            "start": str(PRIMARY_START), "n": len(primary), "evaluation": primary_eval,
            "mcs": tw.mcs(primary_eval["errors"]),
            "subperiods": subperiods(models, t_live, primary),
            "g11": g11_check(primary_eval["pairs_vs_combo3"], len(primary)),
        },
        "limitations": [
            "недельные ряды — последняя редакция, винтажей нет",
            "ЖКУ_wk покрывает 83,1 % веса 9470 (нет сетевого газа и ТКО); 9416 не входит в 9470",
            "agg_ex — реконструкция, не официальный ряд",
            "primary 2023+ короче (45 мес.), MDE выше",
            "маппинг фиксирован словарём 2024; только h1; production не заявляется",
        ],
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    write_predictions(out_dir / "predictions.csv", models, t_live, months, primary)
    write_utilities(out_dir / "utilities_map.csv", utilities)
    (out_dir / "result.json").write_text(json.dumps(lb._jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def write_predictions(path: Path, models: dict, t_live: dict, months: list, primary: list) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["arena", "model", "month", "y_pred", "t_live"])
        for arena, subset in (("ref2022", months), ("primary2023", primary)):
            for model, values in models.items():
                for month in subset:
                    if month in values:
                        writer.writerow([arena, model, month.isoformat(), repr(values[month]), repr(t_live.get(month, NAN))])


def write_utilities(path: Path, utilities: dict) -> None:
    columns = ("series", "weight_codes", "weight_2024", "included", "reason")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        for row in utilities["rows"]:
            writer.writerow(row)
        writer.writerow(utilities["excluded"])


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None)
    parser.add_argument("--panel", default=str(PANEL_CSV))
    parser.add_argument("--public-dir", default=str(PUBLIC_DIR))
    parser.add_argument("--base-predictions", default=str(BASE_PREDICTIONS))
    parser.add_argument("--out", default=str(OUT_DIR))
    args = parser.parse_args(argv)
    archive = cp.resolve_archive(args.archive)
    result = run(archive, Path(args.panel), Path(args.public_dir), Path(args.base_predictions), Path(args.out))
    summary = {
        "r12": result["r12"]["ok"], "r13": result["r13"]["ok"],
        "utilities": {key: result["utilities"][key] for key in ("n_series", "n_codes", "covered_weight", "group_weight", "coverage")},
        "reference": {name: {"rmse": per["rmse"], "n": per["n"]} for name, per in result["reference"]["evaluation"]["per_model"].items()},
        "primary": {name: {"rmse": per["rmse"], "n": per["n"]} for name, per in result["primary"]["evaluation"]["per_model"].items()},
        "g11": result["primary"]["g11"], "mcs": result["primary"]["mcs"], "subperiods": result["primary"]["subperiods"],
    }
    print(json.dumps(lb._jsonable(summary), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
