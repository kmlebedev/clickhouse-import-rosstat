#!/usr/bin/env python3
"""Итоговый gate линии 88 групп и paper shadow 2024-08+ — Task 7 (#427).

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-10-30-cb-comp-88-groups-live-design.md` §18.

* **gate** — воспроизводит `combo3@frozen` кодом Task 9 (`cb_comp_h1_bs_combo.run`),
  проверяет пины (отношения к CB_COMP и RMSE CB_COMP), строит таблицу классов
  §6.3 спеки #401 на normal-54 и справочно на 67 месяцах, MCS 90 % и решение;
  справочно — режимы Task 5 (`annual`, `rolling120`) из `--refit-predictions`.
* **shadow** — out-of-time прогон `combo3@frozen` на 2024-08…2026-09 с as-of
  входами: тестовые лаги `sa_asof_mm`, МП и макро as-of по `cm.cutoff_for`;
  истина primary `T_LIVE_rt`, secondary `T_LIVE`, cross-check — SA ЦБ
  (`cbr_indicators_cpd`), бенчмарк B1; без DM/CI (n = 26).

Только чтение ClickHouse и локальных файлов; в БД не пишет. Запуск —
`make cb-comp-88-live-gate`, тесты — `make cb-comp-88-live-gate-test`.
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
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_h1_bs_combo as combo
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_parity as cp
from models.cpi import cpi_monthly as cm

# ---------------------------------------------------------------------------
# Предзарегистрированные константы (спека §18)
# ---------------------------------------------------------------------------

HORIZONS = bu.HORIZONS
PRIMARY_HORIZONS = (1, 3, 6)
SHADOW_START = date(2024, 8, 1)
REQUIRED_SHADOW_MONTHS = 12
CUTOFF = "late"

MODEL_COMBO = combo.MODEL_COMBO
MODEL_CB = combo.MODEL_CB
MODEL_ECON = combo.MODEL_ECON
MODEL_ROZN = combo.MODEL_ROZN
MCS_POOL = (MODEL_COMBO, MODEL_CB, MODEL_ECON, MODEL_ROZN)
REFERENCE_MODELS = (MODEL_COMBO, MODEL_CB)
REGIME_MODELS = (combo.MODEL_COMBO,)

COMBO_RATIO_PINS = {
    1: 1.0347684348311972,
    2: 0.9571087729271289,
    3: 1.0101981596603171,
    6: 0.8940353315189836,
}
LIVE_CB_RATIO_PINS = {
    1: 1.1928505290859988,
    2: 1.0127041023069754,
    3: 1.0758201741459672,
    6: 1.0616906586271115,
}
CB_RMSE_PINS = {
    1: 0.15880518209979588,
    2: 0.2966548024351276,
    3: 0.4436229873470929,
    6: 0.9714588090072994,
}
RATIO_TOLERANCE = 1e-6
RMSE_TOLERANCE = 1e-5

CBR_SA_NAME = "Все товары и услуги"
CBR_TABLE = "cbr_indicators_cpd"

CLASS_RU = {"better": "лучше", "parity": "паритет", "worse": "хуже", "undetermined": "не определено"}

PANEL_CSV = ROOT / "build" / "cb-comp-groups" / "panel.csv"
PUBLIC_DIR = ROOT / "build" / "cb-comp-parity"
OUT_DIR = ROOT / "build" / "cb-comp-88-live-gate"
GATE_DIR = ROOT / "build" / "ml-gate"
GATE_NAME = "cb_comp_88_live"
SPEC = "docs/superpowers/specs/2026-10-30-cb-comp-88-groups-live-design.md §18"


# ---------------------------------------------------------------------------
# Чистые функции (тесты — без ClickHouse и архива)
# ---------------------------------------------------------------------------


def shadow_months(through: date, start: date = SHADOW_START) -> list:
    """Target-месяцы shadow: от `start` до последнего месяца панели включительно."""
    if through < start:
        return []
    return cp.month_range(start, through)


def describe(models: dict, truth: dict, months: list) -> dict:
    """RMSE/MAE/bias/n по каждой модели на общих с истиной месяцах."""
    out = {}
    for name in sorted(models):
        pairs = [(models[name][m], truth[m]) for m in months if m in models[name] and m in truth]
        errors = [pred - fact for pred, fact in pairs]
        out[name] = {
            "n": len(errors),
            "coverage": round(len(errors) / len(months), 6) if months else 0.0,
            "rmse": lb.rmse(errors),
            "mae": sum(abs(e) for e in errors) / len(errors) if errors else float("nan"),
            "bias": sum(errors) / len(errors) if errors else float("nan"),
        }
    return out


def by_year(models: dict, truth: dict, months: list) -> dict:
    """RMSE/bias/n по моделям и годам — описание устойчивости, не гейт."""
    years = sorted({m.year for m in months})
    out = {}
    for name in sorted(models):
        out[name] = {}
        for year in years:
            pairs = [(models[name][m], truth[m]) for m in months
                     if m.year == year and m in models[name] and m in truth]
            errors = [pred - fact for pred, fact in pairs]
            out[name][str(year)] = {
                "n": len(errors),
                "rmse": lb.rmse(errors),
                "bias": sum(errors) / len(errors) if errors else float("nan"),
            }
    return out


def persistence(truth: dict, horizon: int) -> dict:
    """B1: накопленный прирост за h месяцев, закончившихся в `M−h` — последний известный origin."""
    return {m: truth[cp.shift_month(m, -horizon)] for m in truth
            if cp.shift_month(m, -horizon) in truth}


def gap_stats(left: dict, right: dict, months: list) -> dict:
    """Статистика разрыва `left − right` на общих месяцах: RMSE, bias, corr."""
    pairs = [(left[m], right[m]) for m in months if m in left and m in right]
    diffs = [a - b for a, b in pairs]
    if len(pairs) < 2:
        return {"n": len(pairs), "rmse": float("nan"), "bias": float("nan"), "corr": float("nan")}
    xs = np.array([a for a, _ in pairs])
    ys = np.array([b for _, b in pairs])
    corr = float(np.corrcoef(xs, ys)[0, 1]) if xs.std() and ys.std() else float("nan")
    return {
        "n": len(pairs),
        "rmse": lb.rmse(diffs),
        "bias": sum(diffs) / len(diffs),
        "corr": corr,
    }


def cbr_to_mm(values: dict) -> tuple:
    """SA ЦБ → м/м, п.п.; единицы определяются по медиане, а не молча.

    В `cbr_indicators_cpd` ряд «Все товары и услуги» — уже м/м, %; но если
    источник отдаст уровни (100 = предыдущий месяц), они переводятся в п.п.
    """
    finite = sorted(abs(v) for v in values.values() if not math.isnan(v))
    if not finite:
        return {}, "empty"
    median = finite[len(finite) // 2]
    if median > 50:
        return {month: value - 100.0 for month, value in values.items()}, "levels"
    return dict(values), "mm_pct"


def by_horizon(mapping: dict, horizon: int):
    """Значение по горизонту: ключи бывают int (до JSON) и str (после)."""
    return mapping[str(horizon)] if str(horizon) in mapping else mapping[horizon]


def pins_check(result: dict) -> dict:
    """Регрессия: отношения Task 3/9 и RMSE CB_COMP обязаны совпасть."""
    ratios = result["ratios"]
    checks = {"ratio": {}, "cb_rmse": {}}
    for h in HORIZONS:
        for name, pins in ((MODEL_COMBO, COMBO_RATIO_PINS), (MODEL_CB, LIVE_CB_RATIO_PINS)):
            value = by_horizon(ratios[name], h)
            delta = abs(value - pins[h])
            checks["ratio"][f"{name}:h{h}"] = {
                "value": value, "pinned": pins[h], "abs_delta": delta, "ok": delta <= RATIO_TOLERANCE,
            }
        value = by_horizon(result["per_model"][MODEL_COMBO], h)["vs_cb_target_full"]["rmse_cb"]
        delta = abs(value - CB_RMSE_PINS[h])
        checks["cb_rmse"][f"h{h}"] = {
            "value": value, "pinned": CB_RMSE_PINS[h], "abs_delta": delta, "ok": delta <= RMSE_TOLERANCE,
        }
    checks["ok"] = all(item["ok"] for block in ("ratio", "cb_rmse") for item in checks[block].values())
    return checks


def decision_of(classes: dict) -> dict:
    """Правило §18.2: `promote` не ставится ни при каком исходе."""
    weak = [h for h in PRIMARY_HORIZONS if classes.get(h) != "better"]
    if weak:
        reasons = [f"h{h}: класс «{CLASS_RU.get(classes.get(h), classes.get(h))}»" for h in weak]
    else:
        reasons = ["все primary-классы «лучше», но один бэктест не промоушен: нужен проспективный shadow и live-слой"]
    return {
        "decision": "iterate",
        "promote": False,
        "production": False,
        "rule": "§18.2: promote не ставится ни при каком исходе; h1-трек — shadow по P0.6",
        "reasons": reasons,
    }


def verdict_76(gate_classes: dict, ratios: dict) -> str:
    """Вердикт зонтика #76 по измеренному, без заявлений о production."""
    parts = [f"h{h}: отношение {ratios[h]:.3f}, класс «{CLASS_RU.get(gate_classes[h], gate_classes[h])}»"
             for h in PRIMARY_HORIZONS]
    return (
        "Точечный уровень CB_COMP на окне статьи достигнут (" + "; ".join(parts) + "), "
        "но статистически паритет или превосходство не подтверждены: класс §6.3 — «не определено» "
        "на всех primary-горизонтах. Production-v3 не заявляется: CPI-метрики в forecast_log нет, "
        "live-слой #184 — только cbr_rate; h1-nowcast `wk_mix_ex@v1` остаётся в paper shadow (P0.6). "
        "Публичная панель #76 (покрытие весов 39,3 %) этим результатом не закрывается — линия работает "
        "на своей 88-групповой панели (покрытие весов групп 0,927), per-code МП недоступна."
    )


def config_hash(payload: dict) -> str:
    """sha256 канонического JSON конфигурации — без времени и путей запуска."""
    canon = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


def parse_refit_rows(rows: list, models: tuple, horizon: int, kind: str = MODEL_COMBO) -> dict:
    """`{model: {M: прогноз}}` из строк `model,horizon,month,y_pred`.

    Строки Task 5 различают компонент колонкой `kind` (там `live_cb` и `combo3`
    имеют одинаковые имена режимов); в предсказаниях gate колонки нет.
    """
    out = {}
    for row in rows:
        if "kind" in row and row["kind"] != kind:
            continue
        if int(row["horizon"]) != horizon or row["model"] not in models:
            continue
        out.setdefault(row["model"], {})[date.fromisoformat(row["month"])] = float(row["y_pred"])
    return out


def aligned_errors(predictions: dict, truth: dict, months: list, cb_errors: dict) -> tuple:
    """`(ошибки модели, ошибки CB_COMP)` по общим месяцам — без сдвига позиций."""
    ours, reference = [], []
    for month in months:
        if month in predictions and month in truth and month in cb_errors:
            ours.append(predictions[month] - truth[month])
            reference.append(cb_errors[month])
    return ours, reference


# ---------------------------------------------------------------------------
# IO и прогон
# ---------------------------------------------------------------------------


def sha256(path: Path) -> str:
    return lb.sha256(path)


def git_sha() -> str:
    return lb.git_sha()


def read_predictions(path: Path) -> list:
    with path.open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def live_macro_rows(through: date) -> tuple:
    """Признаки месяцев `2010-01…through` as-of `cutoff_for(month)`.

    `cp.load_feature_rows` обрывается на `TEST_END` (2024-07); для shadow тем же
    правилом продлеваем таблицу до последнего месяца панели.
    """
    targets = cm.load_targets()
    survey = cm.load_survey()
    usd = cm.load_usd()
    key_rate = cm.load_key_rate()
    fao = cm.load_fao()
    index = cm.target_index(targets)
    rows, skipped = {}, {}
    for month in cp.month_range(date(2010, 1, 1), through):
        cutoff = cm.cutoff_for(month)
        base, reason = cm.base_features(month, index, usd, key_rate, fao, cutoff)
        if base is None:
            skipped[month.isoformat()] = f"base: {reason}"
            continue
        mp_part, reason = cm.mp_features(month, survey, cutoff)
        if mp_part is None:
            skipped[month.isoformat()] = f"mp: {reason}"
            continue
        rows[month] = {**base, **mp_part}
    return rows, skipped


def prepare_live(archive: Path, panel_csv: Path, public_dir: Path, through: date) -> dict:
    """Данные shadow: as-of лаги, продлённая таблица признаков, обе цели."""
    archive_data = cp.check_archive(archive)
    sheet_map = bu.load_sheet_map(public_dir / "sheet_map.csv")
    public_panel = bu.load_public_panel(public_dir / "public_panel.csv")
    specs = bu.load_specs(archive, sheet_map)
    codes = [spec.code for spec in specs]
    weights = bu.load_weights(archive, lb.WEIGHT_YEARS, specs)
    sa_full, sa_asof = lb.load_group_panel(panel_csv)
    first = lb.first_sa_month(sa_full)
    last = max(m for values in sa_full.values() for m in values)
    through = min(through, last)
    months = cp.month_range(first, through)
    merged = lb.asof_series(sa_full, sa_asof)
    diff = lb.log_diff_frame(merged, codes, months)
    macro, skipped = live_macro_rows(through)
    features = bu.feature_table(specs, months, diff, macro,
                                bu._sheet_lookup(public_panel, sheet_map), feature_mode="per_sheet")
    train_months = [m for m in months if cp.TRAIN_START <= m <= cp.TRAIN_END and m not in cp.TRAIN_OUTLIER_MONTHS]
    return {
        "archive_data": archive_data, "specs": specs, "codes": codes, "weights": weights,
        "months": months, "through": through, "features": features, "macro_skipped": skipped,
        "sa_full": sa_full, "sa_asof": sa_asof,
        "targets": bu.component_targets(diff),
        "t_live": lb.t_live({c: sa_full.get(c, {}) for c in codes}, weights, months),
        "t_live_rt": lb.t_live({c: sa_asof.get(c, {}) for c in codes}, weights, months),
        "train_months": train_months,
        "groups_without_sa": sorted(c for c in codes if not sa_full.get(c)),
    }


def shadow_forecasts(data: dict, public_dir: Path, test_months: list) -> tuple:
    """`combo3@frozen` и компоненты на `test_months`, покрытие агрегата по месяцам."""
    groups = lb.fit_predict(data["specs"], data["features"], data["features"], data["targets"],
                            data["train_months"], test_months, with_mp=True)
    regressors = combo.load_regressors(public_dir)
    forecasts, coverage = {}, {}
    for horizon in HORIZONS:
        by_group = {code: per_h[horizon] for code, per_h in groups.items() if per_h.get(horizon)}
        values, cov = bu.aggregate(by_group, data["weights"], test_months)
        forecasts.setdefault(MODEL_CB, {})[horizon] = {m: v for m, v in values.items() if not math.isnan(v)}
        coverage[str(horizon)] = cov
    for horizon in HORIZONS:
        for model in (MODEL_ECON, MODEL_ROZN):
            forecasts.setdefault(model, {})[horizon] = combo.bs_predict(
                data["t_live"][horizon], regressors[model], horizon, test_months)
        forecasts.setdefault(MODEL_COMBO, {})[horizon] = combo.combine_mean(
            [forecasts[MODEL_CB][horizon], forecasts[MODEL_ECON][horizon], forecasts[MODEL_ROZN][horizon]])
    return forecasts, coverage


def cbr_block(data: dict, models: dict, test_months: list) -> dict:
    """Cross-check с SA ЦБ: разрыв целей и ошибка модели на h1 (м/м)."""
    import cpi_sa_reconciliation as rec

    levels = rec.load_cbr_sa()
    cbr, unit = cbr_to_mm(levels)
    our_mm = lb.aggregate_mm({c: data["sa_asof"].get(c, {}) for c in data["codes"]}, data["weights"], data["months"])
    window = [m for m in test_months if m in cbr and m in our_mm]
    block = {
        "series": CBR_SA_NAME, "table": CBR_TABLE, "unit": f"п.п. м/м ({unit})",
        "last_available": max(cbr).isoformat() if cbr else None,
        "window": [window[0].isoformat(), window[-1].isoformat()] if window else None,
        "n": len(window),
        "target_gap": gap_stats(our_mm, cbr, window),
        "model_h1": gap_stats(models[MODEL_COMBO].get(1, {}), cbr, window),
        "note": "SA ЦБ включает ЖКУ, T_LIVE — без ЖКУ: это разрыв целей, не ошибка модели",
    }
    return block


def gate_block(archive: Path, panel_csv: Path, public_dir: Path, out_dir: Path,
               refit_path: Path, jobs: int, reuse: bool = False) -> dict:
    """Итоговый gate: прогон Task 9, пины, классы, MCS, режимные строки."""
    import cpi_two_stage_cb_comp as w5b

    gate_dir = out_dir / "gate"
    result_path = gate_dir / "result.json"
    predictions_path = gate_dir / "predictions.csv"
    if reuse and result_path.exists() and predictions_path.exists():
        result = json.loads(result_path.read_text(encoding="utf-8"))
    else:
        result = combo.run(archive, panel_csv, public_dir, gate_dir)
    pins = pins_check(result)
    if not pins["ok"]:
        raise SystemExit(f"пины Task 9 не совпали: {json.dumps(pins, ensure_ascii=False)}")

    archive_data = cp.check_archive(archive)
    target_full = archive_data["target"]
    normal = archive_data["normal"]
    all67 = cp.month_range(cp.TEST_START, cp.TEST_END)
    predictions = read_predictions(gate_dir / "predictions.csv")

    classes = {MODEL_COMBO: {}, MODEL_CB: {}}
    for h in HORIZONS:
        truth = {m: float(v) for m, v in target_full[h].dropna().items()}
        cb_by_month = {
            "normal": {m: e for m, e in zip(normal, cp._cb_errors(archive_data["sheet"], target_full, h, normal))},
            "all67": {m: e for m, e in zip(all67, cp._cb_errors(archive_data["sheet"], target_full, h, all67))},
        }
        for model in REFERENCE_MODELS:
            monthly = parse_refit_rows(predictions, (model,), h).get(model, {})
            ours_n, cb_n = aligned_errors(monthly, truth, normal, cb_by_month["normal"])
            ours_a, cb_a = aligned_errors(monthly, truth, all67, cb_by_month["all67"])
            classes[model][h] = {
                "normal54": w5b.compare_to_cb(ours_n, cb_n, h),
                "months67": w5b.compare_to_cb(ours_a, cb_a, h),
            }

    regime = regime_block(refit_path, predictions, target_full, archive_data, normal)
    return {
        "pins": pins,
        "classes": {model: {str(h): value for h, value in by_h.items()} for model, by_h in classes.items()},
        "mcs": result["mcs"],
        "g9": result["g9"],
        "noharm": result["noharm"],
        "g3_reference": result["g3_reference"],
        "regime": regime,
        "predictions_sha256": sha256(gate_dir / "predictions.csv"),
        "result_sha256": sha256(gate_dir / "result.json"),
    }


def regime_block(refit_path, frozen_rows: list, target_full, archive_data, normal: list) -> dict:
    """Справочные строки режимов Task 5: RMSE, отношение к frozen, класс к CB_COMP."""
    import cpi_two_stage_cb_comp as w5b

    if refit_path is None or not Path(refit_path).exists():
        return {"provided": False, "note": "нет --refit-predictions: строки режимов не считаются"}
    path = Path(refit_path)
    rows = read_predictions(path)
    out = {"provided": True, "file": Path(path).name, "sha256": sha256(path), "models": {}}
    for h in PRIMARY_HORIZONS:
        truth = {m: float(v) for m, v in target_full[h].dropna().items()}
        cb_by_month = {m: e for m, e in zip(normal, cp._cb_errors(archive_data["sheet"], target_full, h, normal))}
        frozen = parse_refit_rows(frozen_rows, (MODEL_COMBO,), h).get(MODEL_COMBO, {})
        frozen_rmse = lb.rmse([frozen[m] - truth[m] for m in normal if m in frozen and m in truth])
        for mode in ("annual", "rolling120"):
            series = parse_refit_rows(rows, (mode,), h).get(mode, {})
            errors, cb_errors = aligned_errors(series, truth, normal, cb_by_month)
            out["models"].setdefault(mode, {})[str(h)] = {
                "n": len(errors),
                "rmse": lb.rmse(errors),
                "ratio_to_frozen": lb.rmse(errors) / frozen_rmse if frozen_rmse else float("nan"),
                "vs_cb": w5b.compare_to_cb(errors, cb_errors, h) if len(errors) >= 3 else None,
            }
    return out


def shadow_block(data: dict, public_dir: Path, out_dir: Path) -> dict:
    """Paper shadow 2024-08…2026-09: обе истины, B1, SA ЦБ, по годам."""
    test_months = shadow_months(data["through"])
    if len(test_months) < REQUIRED_SHADOW_MONTHS:
        raise SystemExit(f"shadow: {len(test_months)} месяцев < {REQUIRED_SHADOW_MONTHS}")
    forecasts, coverage = shadow_forecasts(data, public_dir, test_months)
    truths = {"rt": data["t_live_rt"], "final": data["t_live"]}
    models = {model: by_h for model, by_h in forecasts.items()}
    blocks = {}
    for horizon in HORIZONS:
        truth_rt = truths["rt"][horizon]
        truth_final = truths["final"][horizon]
        models_h = {model: models[model][horizon] for model in models if horizon in models[model]}
        models_h["b1"] = persistence(truth_rt, horizon)
        blocks[str(horizon)] = {
            "rt": describe(models_h, truth_rt, test_months),
            "final": describe(models_h, truth_final, test_months),
            "by_year_rt": by_year(models_h, truth_rt, test_months),
            "coverage": coverage.get(str(horizon)),
        }
    cbr = cbr_block(data, forecasts, test_months)
    shadow_dir = out_dir / "shadow"
    write_shadow_predictions(shadow_dir / "predictions_shadow.csv", forecasts, truths, test_months)
    return {
        "window": {"start": test_months[0].isoformat(), "end": test_months[-1].isoformat(), "n": len(test_months)},
        "cutoff": CUTOFF, "truth_primary": "T_LIVE_rt", "truth_secondary": "T_LIVE",
        "horizons": blocks, "cbr": cbr,
        "macro_skipped": data["macro_skipped"],
        "groups_without_sa": data["groups_without_sa"],
        "predictions_sha256": sha256(shadow_dir / "predictions_shadow.csv"),
    }


def write_shadow_predictions(path: Path, forecasts: dict, truths: dict, months: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["model", "horizon", "month", "y_pred", "t_live_rt", "t_live"])
        for model in sorted(forecasts):
            for horizon in HORIZONS:
                for month in months:
                    value = forecasts[model].get(horizon, {}).get(month)
                    if value is None:
                        continue
                    writer.writerow([
                        model, horizon, month.isoformat(), repr(value),
                        repr(truths["rt"][horizon].get(month, float("nan"))),
                        repr(truths["final"][horizon].get(month, float("nan"))),
                    ])


def render_md(artifact: dict) -> str:
    """Человекочитаемая таблица §6.3 + shadow + решение."""
    lines = []
    decision = artifact["decision"]
    lines += [
        "# Итоговый gate линии 88 групп и paper shadow (`combo3@frozen`), Task 7 #427",
        "",
        f"* git SHA: `{artifact['git_sha']}`; спека: {artifact['spec']}",
        f"* конфигурация: `{artifact['configuration']['primary']}`; режим переборки: {artifact['configuration']['refit_mode']}",
        f"* **решение: `{decision['decision']}`**, promote = {str(decision['promote']).lower()}, "
        f"production = {str(decision['production']).lower()}",
        "",
    ]
    gate = artifact.get("gate")
    if not gate:
        lines += ["## Gate", "", "не считан (`--skip-gate`)", ""]
    else:
        lines += [
            "## Классы против CB_COMP (истина `TARGET_FULL`, normal-54)",
            "",
            "| модель | h | класс | RMSE | RMSE CB | отношение | ΔRMSE [95 % CI] | DM p |",
            "|---|---:|---|---:|---:|---:|---|---:|",
        ]
        for model in REFERENCE_MODELS:
            for h in HORIZONS:
                cell = gate["classes"][model][str(h)]["normal54"]
                primary = "" if h in PRIMARY_HORIZONS else " (справочно)"
                lines.append(
                    f"| `{model}`{primary} | {h} | {CLASS_RU.get(cell['class'], cell['class'])} | {cell['rmse_ours']:.4f} | "
                    f"{cell['rmse_cb']:.4f} | {cell['ratio']:.3f} | "
                    f"[{cell['delta_ci95'][0]:+.4f}; {cell['delta_ci95'][1]:+.4f}] | {cell['dm_p']:.3f} |"
                )
        lines += ["", "## MCS 90 %", "", "| h | включены |", "|---:|---|"]
        for h in PRIMARY_HORIZONS:
            lines.append(f"| {h} | {', '.join(gate['mcs'][str(h)]['included'])} |")
        lines += ["", "## Режим переобучения (Task 5, справочно)", ""]
        regime = gate["regime"]
        if not regime.get("provided"):
            lines.append(f"не считано: {regime.get('note')}")
        else:
            lines += ["| режим | h | RMSE | к frozen | класс к CB_COMP |", "|---|---:|---:|---:|---|"]
            for mode, by_h in sorted(regime["models"].items()):
                for h in PRIMARY_HORIZONS:
                    cell = by_h[str(h)]
                    cls = CLASS_RU.get(cell["vs_cb"]["class"], cell["vs_cb"]["class"]) if cell.get("vs_cb") else "n/a"
                    lines.append(f"| `{mode}` | {h} | {cell['rmse']:.4f} | {cell['ratio_to_frozen']:.3f} | {cls} |")
    shadow = artifact.get("shadow")
    if not shadow:
        lines += ["", "## Paper shadow", "", "не считан (`--skip-shadow`)", ""]
    else:
        lines += [
            "", f"## Paper shadow {shadow['window']['start']}…{shadow['window']['end']} "
            f"({shadow['window']['n']} мес., cutoff `{shadow['cutoff']}`)",
            "",
            "| h | модель | RMSE к `T_LIVE_rt` | bias | RMSE к `T_LIVE` | RMSE к B1 (rt) |",
            "|---:|---|---:|---:|---:|---:|",
        ]
        for h in HORIZONS:
            block = shadow["horizons"][str(h)]
            for model in (MODEL_COMBO, MODEL_CB):
                rt = block["rt"][model]
                final = block["final"][model]
                b1 = block["rt"].get("b1", {}).get("rmse", float("nan"))
                lines.append(f"| {h} | `{model}` | {rt['rmse']:.4f} | {rt['bias']:+.4f} | "
                             f"{final['rmse']:.4f} | {b1:.4f} |")
        cbr = shadow["cbr"]
        lines += [
            "", "## Cross-check с SA ЦБ", "",
            f"* ряд: `{cbr['series']}` (`{cbr['table']}`), доступен до {cbr['last_available']}; окно shadow — "
            f"{cbr['window'][0] if cbr['window'] else '—'}…{cbr['window'][1] if cbr['window'] else '—'} "
            f"({cbr['n']} мес.)",
            f"* разрыв целей `T_LIVE_rt − SA ЦБ`: RMSE {cbr['target_gap']['rmse']:.4f}, "
            f"bias {cbr['target_gap']['bias']:+.4f}, corr {cbr['target_gap']['corr']:.3f}",
            f"* ошибка модели (`combo3@h1 − SA ЦБ`): RMSE {cbr['model_h1']['rmse']:.4f}, "
            f"bias {cbr['model_h1']['bias']:+.4f}",
            f"* {cbr['note']}",
        ]
    lines += [
        "", "## Решение", "",
        *[f"* {reason}" for reason in decision["reasons"]],
        f"* {decision['rule']}",
        "", "## Вердикт зонтика #76", "", artifact["verdict_76"], "", "## Что не проверено", "",
        *[f"* {item}" for item in artifact["not_proven"]],
    ]
    return "\n".join(lines) + "\n"


def run(archive: Path, panel_csv: Path, public_dir: Path, out_dir: Path, gate_dir: Path,
        refit_path: Path = None, skip_gate: bool = False, skip_shadow: bool = False, jobs: int = 4,
        reuse_gate: bool = False) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    gate_dir.mkdir(parents=True, exist_ok=True)
    sa_full, _ = lb.load_group_panel(panel_csv)
    through = max(m for values in sa_full.values() for m in values)
    artifact = {
        "spec": SPEC, "issue": 427, "git_sha": git_sha(),
        "configuration": {
            "primary": "combo3@frozen",
            "refit_mode": "not_adopted",
            "note": "Task 5: формально keep_frozen, исправленное чтение adopt:rolling120; смена режима — решение владельца",
        },
        "inputs": {
            "archive_dir": Path(archive).name, "panel_csv": sha256(panel_csv),
            "public_panel_csv": sha256(public_dir / "public_panel.csv"),
            "sheet_map_csv": sha256(public_dir / "sheet_map.csv"),
            "target_xlsx": sha256(archive / cp.TARGET_XLSX),
            "series2016_xlsx": sha256(archive / combo.SERIES2016),
            "series2009_xlsx": sha256(archive / combo.SERIES2009),
            "refit_predictions": sha256(refit_path) if refit_path and Path(refit_path).exists() else None,
        },
    }
    if not skip_gate:
        artifact["gate"] = gate_block(archive, panel_csv, public_dir, out_dir, refit_path, jobs, reuse_gate)
        artifact["gate"]["reused"] = bool(reuse_gate)
    if not skip_shadow:
        data = prepare_live(archive, panel_csv, public_dir, through)
        artifact["shadow"] = shadow_block(data, public_dir, out_dir)
    if artifact.get("gate"):
        classes = {h: artifact["gate"]["classes"][MODEL_COMBO][str(h)]["normal54"]["class"]
                   for h in PRIMARY_HORIZONS}
        ratios = {h: artifact["gate"]["classes"][MODEL_COMBO][str(h)]["normal54"]["ratio"]
                  for h in PRIMARY_HORIZONS}
        artifact["decision"] = decision_of(classes)
        artifact["verdict_76"] = verdict_76(classes, ratios)
    else:
        artifact["decision"] = {"decision": "incomplete", "promote": False, "production": False,
                                "rule": "gate не считан (--skip-gate)", "reasons": []}
        artifact["verdict_76"] = "не сформирован: gate не считан"
    artifact["not_proven"] = [
        "ретроспективный shadow не заменяет проспективный; месяцы 2023+ частично «сожжены» P0.6 и Task 5",
        "режим переобучения не выбран: frozen — канон до решения владельца; annual/rolling120 — справочно",
        "CB_COMP после 2024-07 не существует; сравнение с SA ЦБ — другой таргет (с ЖКУ)",
        "per-code МП и винтажи отсутствуют; MDE и мощность живого периода — #441",
    ]
    artifact["config_hash"] = config_hash({
        "spec": SPEC, "primary": "combo3@frozen", "cutoff": CUTOFF,
        "shadow_start": SHADOW_START.isoformat(), "horizons": list(HORIZONS),
        "pins": {"combo3": COMBO_RATIO_PINS, "live_cb": LIVE_CB_RATIO_PINS, "cb_rmse": CB_RMSE_PINS},
    })
    payload = lb._jsonable(artifact)
    (gate_dir / f"{GATE_NAME}.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (gate_dir / f"{GATE_NAME}.md").write_text(render_md(payload), encoding="utf-8")
    (out_dir / "result.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None)
    parser.add_argument("--panel", default=str(PANEL_CSV))
    parser.add_argument("--public-dir", default=str(PUBLIC_DIR))
    parser.add_argument("--out-dir", default=str(OUT_DIR))
    parser.add_argument("--gate-dir", default=str(GATE_DIR))
    parser.add_argument("--refit-predictions", default=None,
                        help="predictions.csv Task 5 (справочные строки annual/rolling120)")
    parser.add_argument("--skip-gate", action="store_true")
    parser.add_argument("--skip-shadow", action="store_true")
    parser.add_argument("--reuse-gate", action="store_true",
                        help="не пересчитывать gate, взять готовые result.json и predictions.csv")
    parser.add_argument("--jobs", type=int, default=4)
    args = parser.parse_args(argv)
    archive = cp.resolve_archive(args.archive)
    result = run(archive, Path(args.panel), Path(args.public_dir), Path(args.out_dir), Path(args.gate_dir),
                 Path(args.refit_predictions) if args.refit_predictions else None,
                 args.skip_gate, args.skip_shadow, args.jobs, args.reuse_gate)
    print(json.dumps({"decision": result["decision"]["decision"],
                      "gate_pins_ok": result.get("gate", {}).get("pins", {}).get("ok"),
                      "shadow_months": result.get("shadow", {}).get("window", {}).get("n")},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
