#!/usr/bin/env python3
"""h1 paper shadow `wk_mix_ex@v1` и ретро-robustness — P0.6 #453.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-10-31-cb-comp-h1-shadow-p06-design.md`,
гайд `docs/guides/cb-comp-live-model-map.md` §7.8 (решение `shadow`, пороги
утверждены владельцем 2026-10-31 и здесь не меняются).

* **robustness** — 2023-01…2026-09 на входах Task 4b: R-P0 (воспроизведение
  G11), (а) cutoff `publication`, (б) real-time SA-истина `T_LIVE_rt`,
  (в) без тарифных месяцев, (г) bias по годам, (д) недельные срезы 7/14/21/28,
  (е) цепочка fallback `wk_mix_ex` → `wk_mix_d` → `combo3`. Описание, не гейт;
* **forecast** — прогноз месяца M для `combo3`, `wk_mix_ex`, `wk_mix_d`,
  `live_cb_wk` той же walk-forward процедурой Task 4/4b; JSON с git SHA,
  as-of входов и sha256 — `docs/research/cb-comp-h1-shadow/<YYYY-MM>.json`.

Код `wk_mix_ex@v1` (`cb_comp_h1_weekly_ex.py`, `264ce73`) импортируется и не
меняется. Только чтение ClickHouse и локальных файлов; в БД не пишет. Запуск —
`make cb-comp-h1-shadow-robustness`, `make cb-comp-h1-shadow-forecast`,
тесты — `make cb-comp-h1-shadow-test`.
"""

import argparse
import contextlib
import csv
import json
import math
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_h1_bs_combo as bs9
from models.cb_comp import cb_comp_h1_weekly as tw
from models.cb_comp import cb_comp_h1_weekly_ex as ex
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_parity as cp
from models.cb_comp import cb_comp_weekly_groups as wg
from models.cpi import cpi_monthly as cm
from models.cpi import cpi_public_panel_audit as audit
from models.cpi import cpi_release_dates as releases

# ---------------------------------------------------------------------------
# Предзарегистрированные константы (спека §2–§4)
# ---------------------------------------------------------------------------

VERSION = "wk_mix_ex@v1"
V1_CODE_SHA = "264ce73"
HORIZON = 1
NAN = float("nan")

MODEL_COMBO = ex.MODEL_COMBO
MODEL_MIX_EX = ex.MODEL_MIX_EX
MODEL_MIX_D = ex.MODEL_MIX_D
MODEL_AGG_EX = ex.MODEL_AGG_EX
MODEL_AGG_D = ex.MODEL_AGG_D
MODEL_LIVE_WK = ex.MODEL_LIVE_WK
MODEL_LIVE_CB = tw.MODEL_LIVE_CB
MODEL_ECON = tw.MODEL_ECON
MODEL_ROZN = tw.MODEL_ROZN
SHADOW_MODELS = (MODEL_COMBO, MODEL_MIX_EX, MODEL_MIX_D, MODEL_LIVE_WK)
COMPONENTS = (MODEL_LIVE_CB, MODEL_ECON, MODEL_ROZN, MODEL_AGG_EX, MODEL_AGG_D)
ROBUST_MODELS = (MODEL_COMBO, MODEL_MIX_EX, MODEL_AGG_EX, MODEL_MIX_D, MODEL_LIVE_WK)
WEEKLY_MODELS = (MODEL_AGG_EX, MODEL_MIX_EX, MODEL_AGG_D, MODEL_MIX_D)
# Уровни цепочки fallback: 0 — кандидат, 1 — агрегат с ЖКУ, 2 — база.
FALLBACK_CHAIN = (MODEL_MIX_EX, MODEL_MIX_D, MODEL_COMBO)

PRIMARY_START = ex.PRIMARY_START
REF_START = ex.REF_START
TRAIN_START = date(2010, 3, 1)
VINTAGE_DAYS = (7, 14, 21, 28)
BIAS_FLAG = 0.15
S0_TOL = 1e-6
R_P0_PINNED = {MODEL_COMBO: 0.2094492206438153, MODEL_MIX_EX: 0.14105296079204452}
R_P0_TOL = 1e-9
TRUTH = {
    "primary": "T_LIVE_rt[1]: SA ИПЦ без КУ из 88 групп, X-13 подогнан до M (sa_asof_mm) — SA на дату публикации M",
    "secondary": "T_LIVE[1]: финальная SA (sa_full_mm), пересчёт раз в год",
}

PANEL_CSV = ex.PANEL_CSV
PUBLIC_DIR = ex.PUBLIC_DIR
BASE_PREDICTIONS = ex.BASE_PREDICTIONS
EX_PREDICTIONS = ROOT / "build" / "cb-comp-h1-weekly-ex" / "predictions.csv"
OUT_DIR = ROOT / "build" / "cb-comp-h1-shadow"
SHADOW_DIR = ROOT / "docs" / "research" / "cb-comp-h1-shadow"
SPEC = "docs/superpowers/specs/2026-10-31-cb-comp-h1-shadow-p06-design.md"


# ---------------------------------------------------------------------------
# Истина, fallback, срезы
# ---------------------------------------------------------------------------


def t_live_rt(sa_asof: dict, codes: list, weights: dict, months: list) -> dict:
    """`T_LIVE_rt[1]` — агрегат real-time SA (`sa_asof_mm`) по правилу `T_LIVE`."""
    return lb.t_live({code: sa_asof.get(code, {}) for code in codes}, weights, months)[HORIZON]


def _finite(value) -> bool:
    return value is not None and not math.isnan(float(value))


def serve(values: dict, features: dict) -> tuple:
    """`(модель, прогноз, уровень)` по цепочке `wk_mix_ex` → `wk_mix_d` → `combo3`.

    `wk_mix_ex` — при `util_cov ≥ 0,5` и наличии `agg_mtd`; `wk_mix_d` — когда
    есть только headline-агрегат; `combo3` — когда недельного агрегата нет.
    """
    agg_ok = _finite(features.get("agg_mtd"))
    util_ok = float(features.get("util_cov", 0.0)) >= ex.UTIL_COV_MIN
    if agg_ok and util_ok and _finite(values.get(MODEL_MIX_EX)):
        return MODEL_MIX_EX, float(values[MODEL_MIX_EX]), 0
    if agg_ok and _finite(values.get(MODEL_MIX_D)):
        return MODEL_MIX_D, float(values[MODEL_MIX_D]), 1
    if _finite(values.get(MODEL_COMBO)):
        return MODEL_COMBO, float(values[MODEL_COMBO]), 2
    raise SystemExit("fallback: нет даже combo3")


def served_series(models: dict, features: dict, months: list) -> tuple:
    """`({M: прогноз}, {M: уровень})` цепочки fallback."""
    values, levels = {}, {}
    for month in months:
        point = {name: models[name].get(month) for name in FALLBACK_CHAIN}
        _model, values[month], levels[month] = serve(point, features.get(month, {}))
    return values, levels


def vintage_cutoff(day: int):
    """Cutoff среза: день `day` самого месяца M (вместо 8-го M+1)."""
    return lambda month: month.replace(day=day)


@contextlib.contextmanager
def cutoff_override(function):
    """Временно подменяет `cpi_monthly.cutoff_for` (все модули читают его через `cm.`)."""
    original = cm.cutoff_for
    cm.cutoff_for = function
    try:
        yield
    finally:
        cm.cutoff_for = original


# ---------------------------------------------------------------------------
# Недельные модели v1 (функции `cb_comp_h1_weekly_ex`, без изменений)
# ---------------------------------------------------------------------------


def weekly_ex_features(weekly: dict, leaf_weights: dict, group_weights: dict, months: list) -> dict:
    """Признаки `agg_ex` на активном `cm.cutoff_for`."""
    util = ex.utilities_features(weekly, leaf_weights, group_weights, months)
    return ex.agg_ex_features(weekly, months, util, group_weights)


def weekly_models(features: dict, target: dict, all_train: list, months: list, combo: dict) -> dict:
    agg_ex = ex.agg_ex_wf(features, target, all_train, months, dummies=True)
    agg_d = ex.feature_wf(features, target, all_train, months, ex.FEATURE_AGG, dummies=True)
    return {
        MODEL_AGG_EX: agg_ex, MODEL_MIX_EX: tw.mix(combo, agg_ex),
        MODEL_AGG_D: agg_d, MODEL_MIX_D: tw.mix(combo, agg_d),
    }


# ---------------------------------------------------------------------------
# Описательная оценка
# ---------------------------------------------------------------------------


def describe(models: dict, truth: dict, months: list) -> dict:
    """RMSE/MAE/bias и пары к `combo3` (CI, DM) без списков ошибок."""
    evaluation = tw.evaluate(models, truth, months, None)
    return {"n": len(months), "per_model": evaluation["per_model"], "pairs_vs_combo3": evaluation["pairs_vs_combo3"]}


def holds(summary: dict, name: str = MODEL_MIX_EX) -> bool:
    """Выигрыш держится: RMSE ниже `combo3` и верх CI ΔRMSE ниже 0."""
    pair = summary["pairs_vs_combo3"].get(name)
    return bool(pair and pair["rmse_left"] < pair["rmse_right"] and pair["delta_ci"][1] < 0)


def bias_flag(per_year: dict, threshold: float = BIAS_FLAG) -> list:
    """Годы с |bias| > порога (описательный аналог шага 5 §7.8)."""
    return sorted(year for year, item in per_year.items() if abs(item["bias"]) > threshold)


def truth_block(models: dict, truths: dict, months: list) -> dict:
    """Блоки (б)–(г): обе истины, срез без тарифов, bias по годам."""
    no_tariff = [m for m in months if not ex.is_tariff_month(m)]
    out = {}
    for label, truth in truths.items():
        out[label] = {
            "all": describe(models, truth, months),
            "ex_tariff": describe(models, truth, no_tariff),
            "bias_by_year": ex.bias_by_year(models, truth, months),
        }
    return out


def gap(truth_final: dict, truth_rt: dict, months: list) -> dict:
    diffs = [truth_final[m] - truth_rt[m] for m in months]
    return {"n": len(diffs), "rmse": lb.rmse(diffs), "bias": sum(diffs) / len(diffs) if diffs else NAN,
            "max_abs": max(abs(d) for d in diffs) if diffs else NAN}


def flags(blocks: dict) -> dict:
    rt = blocks["truths"]["rt"]
    out = {
        "rt_holds": holds(rt["all"]),
        "final_holds": holds(blocks["truths"]["final"]["all"]),
        "ex_tariff_holds": holds(rt["ex_tariff"]),
        "bias_flag_years": bias_flag(rt["bias_by_year"][MODEL_MIX_EX]),
    }
    if blocks.get("publication"):
        out["pub_holds"] = holds(blocks["publication"]["rt"], MODEL_MIX_EX)
    return out


# ---------------------------------------------------------------------------
# Данные
# ---------------------------------------------------------------------------


def prepare_through(archive: Path, panel_csv: Path, public_dir: Path, through: date = None) -> dict:
    """`cb_comp_h1_weekly.prepare`, но таблица признаков продлена до месяца `through`.

    Цель `T_LIVE` по-прежнему заканчивается последним месяцем панели; признаки
    origin `through` используют только лаги M−1/M−2 и МП/макро as-of cutoff.
    """
    data = tw.prepare(archive, panel_csv, public_dir)
    last = data["feature_months"][-1]
    if through is None or through <= last:
        return data
    if cp.shift_month(last, 1) != through:
        raise SystemExit(f"прогноз {through}: панель кончается {last}; нужен ровно следующий месяц")
    feature_rows, _ = cp.load_feature_rows()
    macro = {row.month: row.features for row in feature_rows}
    public_panel = bu.load_public_panel(public_dir / "public_panel.csv")
    sheet_map = bu.load_sheet_map(public_dir / "sheet_map.csv")
    sa_full, _sa_asof = lb.load_group_panel(panel_csv)
    months = cp.month_range(date(2010, 1, 1), through)
    diff_full = lb.log_diff_frame(sa_full, data["codes"], months)
    feature_months = cp.month_range(TRAIN_START, through)
    data["features"] = bu.feature_table(
        data["specs"], feature_months, diff_full, macro, bu._sheet_lookup(public_panel, sheet_map), feature_mode="per_sheet"
    )
    data["feature_months"] = feature_months
    return data


def wf_combo(data: dict, features: dict, regressors: dict, months: list, jobs: int) -> dict:
    """`live_cb`, BS и `combo3` ежемесячным walk-forward (как Task 4)."""
    all_train = [m for m in data["feature_months"] if m >= TRAIN_START]
    groups = tw.fit_predict_wf(data["specs"], features, data["targets"], all_train, months, True, (), jobs)
    live = tw.aggregate_h1(groups, data["weights"], months)
    bs = tw.bs_forecasts(data["t_live"][HORIZON], regressors, months)
    return {
        MODEL_LIVE_CB: live, MODEL_ECON: bs[MODEL_ECON], MODEL_ROZN: bs[MODEL_ROZN],
        MODEL_COMBO: tw.combine_mean([live, bs[MODEL_ECON], bs[MODEL_ROZN]]),
    }


def weekly_context(archive: Path, data: dict) -> dict:
    weekly = audit.load_weekly()
    return {
        "weekly": weekly,
        "leaf_weights": wg.load_leaf_weights(archive),
        "group_weights": bu.load_weights(archive, lb.WEIGHT_YEARS, data["specs"]),
    }


# ---------------------------------------------------------------------------
# Robustness
# ---------------------------------------------------------------------------


def robustness(archive: Path, panel_csv: Path, public_dir: Path, base_predictions: Path, out_dir: Path,
               jobs: int = 4, skip_publication: bool = False) -> dict:
    data = tw.prepare(archive, panel_csv, public_dir)
    t_live = data["t_live"][HORIZON]
    months = [m for m in data["feature_months"] if m >= REF_START and m in t_live]
    primary = [m for m in months if m >= PRIMARY_START]
    all_train = [m for m in data["feature_months"] if m >= TRAIN_START]
    if len(months) != 57 or len(primary) != 45:
        raise SystemExit(f"окна не совпали с Task 4b: 2022+ {len(months)}, 2023+ {len(primary)}")
    base = ex.load_base_predictions(base_predictions, months, (MODEL_COMBO, MODEL_LIVE_WK))
    context = weekly_context(archive, data)
    features_late = weekly_ex_features(context["weekly"], context["leaf_weights"], context["group_weights"], data["feature_months"])
    models = {MODEL_COMBO: base[MODEL_COMBO], MODEL_LIVE_WK: base[MODEL_LIVE_WK],
              **weekly_models(features_late, t_live, all_train, primary, base[MODEL_COMBO])}

    r_p0 = {}
    for name, expected in R_P0_PINNED.items():
        got = lb.rmse([models[name][m] - t_live[m] for m in primary])
        r_p0[name] = {"rmse": got, "expected": expected, "ok": abs(got - expected) <= R_P0_TOL}
    r_p0["ok"] = all(item["ok"] for item in r_p0.values())
    if not r_p0["ok"]:
        raise SystemExit(f"R-P0 не пройден: {r_p0}")

    _sa_full, sa_asof = lb.load_group_panel(panel_csv)
    truth_rt = t_live_rt(sa_asof, data["codes"], data["weights"], data["months"])
    missing = [m.isoformat() for m in primary if m not in truth_rt]
    if missing:
        raise SystemExit(f"T_LIVE_rt: нет месяцев {missing}")
    truths = {"final": t_live, "rt": truth_rt}
    robust = {name: models[name] for name in ROBUST_MODELS}

    served, levels = served_series(models, features_late, primary)
    fallback = {
        "chain": list(FALLBACK_CHAIN),
        "levels": {str(level): sum(1 for v in levels.values() if v == level) for level in range(len(FALLBACK_CHAIN))},
        "months_not_level0": [m.isoformat() for m in primary if levels[m] != 0],
        "util_cov_min": min(features_late[m]["util_cov"] for m in primary),
        "rmse": {label: lb.rmse([served[m] - truth[m] for m in primary]) for label, truth in truths.items()},
    }

    vintages = {}
    for day in VINTAGE_DAYS + ("late",):
        if day == "late":
            features = features_late
        else:
            with cutoff_override(vintage_cutoff(day)):
                features = weekly_ex_features(context["weekly"], context["leaf_weights"], context["group_weights"], data["feature_months"])
        wk = weekly_models(features, t_live, all_train, primary, base[MODEL_COMBO])
        wk_models = {MODEL_COMBO: base[MODEL_COMBO], **wk}
        vintages[str(day)] = {
            "agg_cov_mean": sum(features[m]["agg_cov"] for m in primary) / len(primary),
            "util_cov_mean": sum(features[m]["util_cov"] for m in primary) / len(primary),
            **{label: describe(wk_models, truth, primary) for label, truth in truths.items()},
        }

    publication = None
    pub_models = {}
    if not skip_publication:
        release_dates = releases.load_release_dates()
        regressors = bs9.load_regressors(public_dir)
        with cm.asof_convention(cm.ASOF_PUBLICATION, release_dates):
            data_pub = tw.prepare(archive, panel_csv, public_dir)
            combo_pub = wf_combo(data_pub, data_pub["features"], regressors, primary, jobs)
            features_pub = weekly_ex_features(context["weekly"], context["leaf_weights"], context["group_weights"], data["feature_months"])
            cutoffs = {m.isoformat(): cm.cutoff_for(m).isoformat() for m in primary}
        wk_pub = weekly_models(features_pub, t_live, all_train, primary, combo_pub[MODEL_COMBO])
        pub_models = {MODEL_COMBO: combo_pub[MODEL_COMBO], **wk_pub}
        publication = {
            "cutoffs": cutoffs,
            "agg_cov_mean": sum(features_pub[m]["agg_cov"] for m in primary) / len(primary),
            "combo3_late_vs_pub_rmse_diff": lb.rmse([combo_pub[MODEL_COMBO][m] - base[MODEL_COMBO][m] for m in primary]),
            **{label: describe(pub_models, truth, primary) for label, truth in truths.items()},
        }

    blocks = {"truths": truth_block(robust, truths, primary), "publication": publication}
    result = {
        "spec": SPEC, "version": VERSION, "v1_code_sha": V1_CODE_SHA, "git_sha": lb.git_sha(),
        "decision": "shadow",
        "inputs": {
            "panel_csv": ex.sha256(panel_csv), "public_panel_csv": ex.sha256(public_dir / "public_panel.csv"),
            "sheet_map_csv": ex.sha256(public_dir / "sheet_map.csv"), "target_xlsx": ex.sha256(archive / cp.TARGET_XLSX),
            "base_predictions": ex.sha256(base_predictions),
        },
        "window": {"start": primary[0].isoformat(), "end": primary[-1].isoformat(), "n": len(primary),
                   "n_ex_tariff": sum(1 for m in primary if not ex.is_tariff_month(m))},
        "r_p0": r_p0,
        "truth_gap": gap(t_live, truth_rt, primary),
        "truths": blocks["truths"],
        "vintages": vintages,
        "publication": publication,
        "fallback": fallback,
        "flags": flags(blocks),
        "limitations": [
            "те же 45 «сожжённых» месяцев — не подтверждение (§7.8 принцип 2)",
            "недельные ряды — последняя редакция; срезы моделируют доступность по дате, не ревизии",
            "T_LIVE_rt: real-time только по SA (X-13 до M); NSA и веса — последняя редакция",
            "срезы 7/14/21/28: combo3 фиксирован на late; эффект cutoff МП — блок publication",
            "модели обучены на финальной T_LIVE (v1); real-time истина меняет только оценку",
        ],
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    write_predictions(out_dir / "predictions_robustness.csv", models, pub_models, served, truths, primary)
    (out_dir / "robustness.json").write_text(dump(result), encoding="utf-8")
    return result


def write_predictions(path: Path, models: dict, pub_models: dict, served: dict, truths: dict, months: list) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["cutoff", "model", "month", "y_pred", "t_live", "t_live_rt"])
        rows = [("late", name, values) for name, values in models.items()]
        rows += [("publication", name, values) for name, values in pub_models.items()]
        rows.append(("late", "served", served))
        for cutoff, name, values in rows:
            for month in months:
                if month in values:
                    writer.writerow([cutoff, name, month.isoformat(), repr(values[month]),
                                     repr(truths["final"].get(month, NAN)), repr(truths["rt"].get(month, NAN))])


# ---------------------------------------------------------------------------
# Shadow-прогноз
# ---------------------------------------------------------------------------


def last_date(points) -> str:
    return max(day for day, _ in points).isoformat() if points else None


def read_reference(path: Path, arena: str, models: tuple, months: list) -> dict:
    """`{model: {M: прогноз}}` из `predictions.csv` Task 4/4b (для S0)."""
    out = {name: {} for name in models}
    if not path.exists():
        return out
    wanted = set(months)
    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            month = date.fromisoformat(row["month"])
            if row["arena"] == arena and row["model"] in out and month in wanted:
                out[row["model"]][month] = float(row["y_pred"])
    return out


def s0_check(models: dict, references: dict, months: list, tol: float = S0_TOL) -> dict:
    """Расхождение процедуры shadow с прогнозами Task 4/4b на прошлых месяцах."""
    out = {}
    for name, reference in references.items():
        diffs = {m.isoformat(): models[name][m] - reference[m] for m in months if m in reference and m in models.get(name, {})}
        out[name] = {"n": len(diffs), "max_abs_diff": max((abs(d) for d in diffs.values()), default=NAN), "diffs": diffs}
    compared = [item for item in out.values() if item["n"]]
    out["identical"] = bool(compared) and all(item["max_abs_diff"] <= tol for item in compared)
    return out


def check_shadow_timing(month: date, run_date: date, panel_last: date) -> date:
    """Shadow засчитывается, только если прогноз сделан до cutoff и до публикации ИПЦ M."""
    cutoff = cm.cutoff_for(month)
    if run_date > cutoff:
        raise SystemExit(f"прогноз {month:%Y-%m} после cutoff {cutoff} — не засчитывается")
    if panel_last >= month:
        raise SystemExit(f"ИПЦ {month:%Y-%m} уже в панели — прогноз не проспективный")
    return cutoff


def validate_shadow(payload: dict) -> None:
    """Паспорт shadow-JSON: обязательные поля и прогнозы всех моделей (спека §4)."""
    required = ("version", "month", "cutoff", "run_date", "git_sha", "models", "served", "asof", "inputs_sha256", "truth")
    missing = [key for key in required if key not in payload]
    if missing:
        raise SystemExit(f"shadow-JSON: нет полей {missing}")
    if payload["run_date"] > payload["cutoff"]:
        raise SystemExit("shadow-JSON: run_date позже cutoff")
    absent = [name for name in SHADOW_MODELS if not _finite(payload["models"].get(name))]
    if absent:
        raise SystemExit(f"shadow-JSON: нет прогноза {absent}")
    if payload["served"]["model"] not in FALLBACK_CHAIN:
        raise SystemExit("shadow-JSON: served вне цепочки fallback")
    for name, digest in payload["inputs_sha256"].items():
        if not (isinstance(digest, str) and len(digest) == 64):
            raise SystemExit(f"shadow-JSON: sha256 {name} некорректен")


def shadow_forecast(archive: Path, panel_csv: Path, public_dir: Path, month: date, out_path: Path,
                    base_predictions: Path, ex_predictions: Path, check_months: int = 3, jobs: int = 4,
                    run_date: date = None) -> dict:
    run_date = run_date or date.today()
    data = prepare_through(archive, panel_csv, public_dir, month)
    panel_last = max(data["t_live"][HORIZON])
    cutoff = check_shadow_timing(month, run_date, panel_last)
    past = [cp.shift_month(month, -k) for k in range(check_months, 0, -1)]
    predict = past + [month]
    t_live = data["t_live"][HORIZON]
    all_train = [m for m in data["feature_months"] if m >= TRAIN_START]

    wk_inputs = tw.weekly_inputs(archive, data["specs"])
    if any(wk_inputs["summary"][key] != tw.R11_EXPECTED[key] for key in tw.R11_EXPECTED):
        raise SystemExit(f"R11: маппинг недельных позиций изменился: {wk_inputs['summary']}")
    group_features = tw.monthly_weekly_features(wk_inputs, data["feature_months"], data["codes"])
    features_wk = tw.attach_weekly(data["features"], group_features)
    regressors = bs9.load_regressors(public_dir)
    combo = wf_combo(data, data["features"], regressors, predict, jobs)
    groups_wk = tw.fit_predict_wf(data["specs"], features_wk, data["targets"], all_train, predict, True, tw.WEEKLY_NAMES, jobs)
    live_wk = tw.aggregate_h1(groups_wk, data["weights"], predict)
    features_ex = weekly_ex_features(wk_inputs["weekly"], wk_inputs["leaf_weights"], wk_inputs["group_weights"], data["feature_months"])
    models = {**combo, MODEL_LIVE_WK: live_wk, **weekly_models(features_ex, t_live, all_train, predict, combo[MODEL_COMBO])}
    missing = [name for name in SHADOW_MODELS + COMPONENTS if month not in models[name]]
    if missing:
        raise SystemExit(f"нет прогноза {month:%Y-%m}: {missing}")

    point = {name: models[name][month] for name in SHADOW_MODELS + COMPONENTS}
    served_model, served_value, level = serve(point, features_ex[month])
    references = {
        **read_reference(base_predictions, "wf2022", (MODEL_COMBO, MODEL_LIVE_WK), past),
        **read_reference(ex_predictions, "primary2023", (MODEL_MIX_EX, MODEL_MIX_D), past),
    }
    weekly = wk_inputs["weekly"]
    survey = cm.load_survey()
    public_dates = sorted(str(d) for d in bu.load_public_panel(public_dir / "public_panel.csv")["date"])
    payload = {
        "protocol": "paper shadow, гайд cb-comp-live-model-map §7.8; пороги утверждены 2026-10-31",
        "spec": SPEC, "version": VERSION, "v1_code_sha": V1_CODE_SHA, "git_sha": lb.git_sha(),
        "month": month.strftime("%Y-%m"), "cutoff": cutoff.isoformat(), "run_date": run_date.isoformat(),
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "unit": "п.п., м/м SA ИПЦ без КУ (h1-nowcast)",
        "models": {name: point[name] for name in SHADOW_MODELS},
        "components": {name: point[name] for name in COMPONENTS},
        "served": {"model": served_model, "value": served_value, "fallback_level": level},
        "weekly_features": {key: features_ex[month][key] for key in sorted(features_ex[month])},
        "asof": {
            "panel_last_month": panel_last.isoformat(),
            "weekly_last_point": max(last_date(points) for points in weekly.values()),
            "weekly_aggregate_last_point": last_date(weekly.get(audit.WEEKLY_AGGREGATE, [])),
            "utilities_last_point": {series: last_date(weekly.get(series, [])) for series in ex.UTIL_SERIES},
            "survey_price_expectations_sa_last": last_date(survey.get(bs9.X_ECON_SERIES, [])),
            "public_panel_last": public_dates[-1] if public_dates else None,
            "bs_regressor_month": cp.shift_month(month, -1).isoformat(),
        },
        "inputs_sha256": {
            "panel_csv": ex.sha256(panel_csv), "public_panel_csv": ex.sha256(public_dir / "public_panel.csv"),
            "sheet_map_csv": ex.sha256(public_dir / "sheet_map.csv"), "target_xlsx": ex.sha256(archive / cp.TARGET_XLSX),
        },
        "s0": {"months": [m.isoformat() for m in past], **s0_check(models, references, past)},
        "past_predictions": {name: {m.isoformat(): models[name][m] for m in past if m in models[name]} for name in SHADOW_MODELS},
        "truth": TRUTH,
        "actual": None,
    }
    validate_shadow(payload)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(dump(payload), encoding="utf-8")
    return payload


def dump(payload: dict) -> str:
    return json.dumps(lb._jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("mode", choices=("robustness", "forecast"))
    parser.add_argument("--archive", default=None)
    parser.add_argument("--panel", default=str(PANEL_CSV))
    parser.add_argument("--public-dir", default=str(PUBLIC_DIR))
    parser.add_argument("--base-predictions", default=str(BASE_PREDICTIONS))
    parser.add_argument("--ex-predictions", default=str(EX_PREDICTIONS))
    parser.add_argument("--out", default=None)
    parser.add_argument("--month", default=None, help="месяц shadow-прогноза YYYY-MM")
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--skip-publication", action="store_true")
    args = parser.parse_args(argv)
    archive = cp.resolve_archive(args.archive)
    if args.mode == "robustness":
        result = robustness(archive, Path(args.panel), Path(args.public_dir), Path(args.base_predictions),
                            Path(args.out or OUT_DIR), args.jobs, args.skip_publication)
        summary = {"r_p0": result["r_p0"], "truth_gap": result["truth_gap"], "flags": result["flags"],
                   "fallback": result["fallback"]["levels"]}
    else:
        if not args.month:
            raise SystemExit("--month YYYY-MM обязателен для forecast")
        month = date.fromisoformat(args.month + "-01")
        out = Path(args.out) if args.out else SHADOW_DIR / f"{args.month}.json"
        payload = shadow_forecast(archive, Path(args.panel), Path(args.public_dir), month, out,
                                  Path(args.base_predictions), Path(args.ex_predictions), jobs=args.jobs)
        summary = {key: payload[key] for key in ("month", "cutoff", "models", "served", "asof")}
        summary["s0_identical"] = payload["s0"]["identical"]
    print(json.dumps(lb._jsonable(summary), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
