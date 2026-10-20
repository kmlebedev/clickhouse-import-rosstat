#!/usr/bin/env python3
"""Эксперимент: CPI v3 h1/h3/h6 как прокси ожидаемой инфляции (issue #296).

Сравнивает channel-core ставки (`key_rate_scenario_fan_v3`) на четырёх
вариантах признаков:

* `baseline` — без CPI v3 (41 признак Task 4);
* `h1` — + `cpi_v3_h1` + `cpi_v3_available`;
* `h1h3h6` — + все три горизонта (primary-гипотеза);
* `replace_survey` — все три горизонта вместо survey-ожиданий.

Все варианты — один и тот же walk-forward, seed, дисперсия `disp_vol` и веера.
Скрипт только читает ClickHouse; в `forecast_log` не пишет.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from models.cpi import cpi_live3_horizons as cph  # noqa: E402
from models.cpi import cpi_monthly as cm  # noqa: E402
from models.cpi import cpi_monthly_v3 as cmv3  # noqa: E402
from lib import eval_utils as eu  # noqa: E402
from models.key_rate import key_rate_cbr_rule as cr  # noqa: E402
from models.key_rate import key_rate_model as krm  # noqa: E402
from models.key_rate import key_rate_scenario_fan as fan1  # noqa: E402
from models.key_rate import key_rate_scenario_fan_v3 as v3  # noqa: E402
from lib import ml_experiment as mlx  # noqa: E402

SPEC_PATH = "docs/superpowers/specs/2026-10-20-cpi-v3-expectations-design.md"
PLAN_PATH = "docs/superpowers/plans/2026-10-20-cpi-v3-expectations.md"
REPORT_NAME = "key_rate_cpi_v3_experiment_report.md"
RESULT_NAME = "key_rate_cpi_v3_experiment_result.json"
FOLDS_NAME = "key_rate_cpi_v3_experiment_folds.csv"
CPI_PREDICTIONS_NAME = "key_rate_cpi_v3_experiment_cpi_predictions.csv"
PRIMARY_FAN = "channel_fan_vol"
PRIMARY_WINDOW = "expanding"
VARIANTS = ("baseline", "h1", "h1h3h6", "replace_survey")
SURVEY_FEATURES = (
    "infl_exp_expected",
    "infl_exp_observed",
    "price_expectations",
    "infl_exp_expected_chg",
    "infl_exp_observed_chg",
    "price_expectations_chg",
)
RANK_MODELS = ("live3_bottomup", "direct_catboost", "B1_last", "B3_enet")
BOOTSTRAP_BLOCK = 3
BOOTSTRAP_N = 2000


def load_rate_data(as_of: date) -> dict:
    meetings = krm.load_meetings()
    past = [day for day in meetings if day <= as_of]
    key_rate = krm.load_key_rate()
    deltas = krm.load_mp_summary_deltas()
    decisions, excluded, missing_rate = krm.build_decisions(past, key_rate, deltas)
    cpd_core, cpd_all = cr.load_cpd_both()
    ibk = krm.load_ibk()
    gdp = cr.load_gdp()
    consensus = cr.load_cpi_consensus()
    rows, missing_features = cr.build_cbr_rows(
        decisions, cpd_core, cpd_all, ibk, gdp, consensus
    )
    if not rows:
        raise SystemExit("выборка решений пуста")
    panel, shock_meetings, shock_key_rate, channels = fan1.load_shock_panel()
    panel_by_date = fan1.align_rows(rows, panel)
    weight_by_date, probability_by_date = fan1.shock_arrays(panel)
    budget = fan1.load_budget()
    impulse_by_date = {
        row.decision.meeting_date: fan1.budget_impulse(
            budget, row.decision.meeting_date
        )
        for row in rows
    }
    external = {
        "ofz": v3.load_ofz(),
        "brent": v3.load_brent(),
        "bop": v3.load_bop(),
        "savings": v3.load_savings(),
        "ccyb": v3.load_ccyb(),
        "text": v3.load_text(),
    }
    return {
        "as_of": as_of,
        "rows": rows,
        "decisions": decisions,
        "excluded": excluded,
        "missing_rate": missing_rate,
        "missing_features": missing_features,
        "panel": panel,
        "panel_by_date": panel_by_date,
        "weight_by_date": weight_by_date,
        "probability_by_date": probability_by_date,
        "impulse_by_date": impulse_by_date,
        "external": external,
        "key_rate": key_rate,
        "cpd_core": cpd_core,
        "cpd_all": cpd_all,
        "ibk": ibk,
        "gdp": gdp,
        "consensus": consensus,
        "channels": channels,
        "shock_meetings": shock_meetings,
        "shock_key_rate": shock_key_rate,
    }


def load_cpi_features(rows: list, as_of: date) -> tuple:
    cpi_rows, skipped = cph.load_rows(as_of)
    meetings = [row.decision.meeting_date for row in rows]
    features = cph.build_cpi_features(meetings, cpi_rows)
    coverage = {
        "meetings": len(meetings),
        "h1": sum(1 for item in features.values() if item["cpi_v3_h1"] is not None),
        "h3": sum(1 for item in features.values() if item["cpi_v3_h3"] is not None),
        "h6": sum(1 for item in features.values() if item["cpi_v3_h6"] is not None),
        "available": sum(
            1 for item in features.values() if item["cpi_v3_available"]
        ),
        "first_origin": min(
            (
                item["cpi_v3_origin"]
                for item in features.values()
                if item["cpi_v3_origin"] is not None
            ),
            default=None,
        ),
        "last_origin": max(
            (
                item["cpi_v3_origin"]
                for item in features.values()
                if item["cpi_v3_origin"] is not None
            ),
            default=None,
        ),
        "cpi_months": len(cpi_rows),
        "cpi_skipped": len(skipped),
        "feature_last_origin": max(cpi_rows) if cpi_rows else None,
    }
    return features, coverage, cpi_rows


def build_extra(
    dates: list, cpi_by_meeting: dict, variant: str
) -> Optional[dict]:
    if variant == "baseline":
        return None
    extra = {}
    for day in dates:
        item = cpi_by_meeting.get(day, {})
        h1 = item.get("cpi_v3_h1")
        h3 = item.get("cpi_v3_h3")
        h6 = item.get("cpi_v3_h6")
        if variant == "h1":
            extra[day] = {
                "cpi_v3_h1": h1,
                "cpi_v3_available": int(h1 is not None),
            }
            continue
        payload = {
            "cpi_v3_h1": h1,
            "cpi_v3_h3": h3,
            "cpi_v3_h6": h6,
            "cpi_v3_available": int(
                h1 is not None and h3 is not None and h6 is not None
            ),
        }
        if variant == "replace_survey":
            payload.update({name: None for name in SURVEY_FEATURES})
            payload["has_expectations"] = 0
        extra[day] = payload
    return extra


def run_variant(data: dict, variant: str, cpi_by_meeting: dict) -> dict:
    dates = [row.decision.meeting_date for row in data["rows"]]
    extra = build_extra(dates, cpi_by_meeting, variant)
    fans, reference, benchmarks, diagnostics, skips, cores = v3.run_v3_walk_forward(
        data["rows"],
        data["panel"],
        data["panel_by_date"],
        data["weight_by_date"],
        data["probability_by_date"],
        data["impulse_by_date"],
        data["external"],
        extra,
    )
    return {
        "variant": variant,
        "fans": fans,
        "reference": reference,
        "benchmarks": benchmarks,
        "diagnostics": diagnostics,
        "skips": skips,
        "cores": cores,
        "summary": fan1.summarize_fans(fans),
        "episodes": fan1.summarize_episodes(fans),
    }


def primary_group(fans: list) -> list:
    return [
        item
        for item in fans
        if item.fan == PRIMARY_FAN and item.window == PRIMARY_WINDOW
    ]


def compare_variants(results: dict) -> dict:
    if "baseline" not in results:
        return {}
    baseline = primary_group(results["baseline"]["fans"])
    comparisons = {}
    for variant in VARIANTS[1:]:
        if variant not in results:
            continue
        group = primary_group(results[variant]["fans"])
        comparisons[variant] = fan1.compare_fans(group, baseline)
    return comparisons


def forecast_direct(
    rows_by_month: dict,
    targets: dict,
    months: list,
    origin: date,
    horizon: int,
) -> Optional[float]:
    """CatBoost direct на `cpi_all`; тот же train-набор, что у live3."""
    train = []
    for month in cph.training_origins(months, origin, horizon):
        target = targets[horizon].get(cph.shift_month(month, horizon))
        if target is None:
            continue
        train.append(
            cm.Row(
                month=month,
                y={cm.TARGET_ALL: target[cm.TARGET_ALL]},
                features=rows_by_month[month].features,
            )
        )
    if len(train) < cph.MIN_TRAIN:
        return None
    return cmv3.fit_catboost(train, rows_by_month[origin], cm.TARGET_ALL, True)


def forecast_b3(
    rows_by_month: dict,
    targets: dict,
    months: list,
    origin: date,
    horizon: int,
) -> Optional[float]:
    """B3 ElasticNetCV на v3-признаках; медиана train внутри фолда."""
    train = []
    for month in cph.training_origins(months, origin, horizon):
        target = targets[horizon].get(cph.shift_month(month, horizon))
        if target is None:
            continue
        train.append(
            cm.Row(
                month=month,
                y={cm.TARGET_ALL: target[cm.TARGET_ALL]},
                features=rows_by_month[month].features,
            )
        )
    if len(train) < cph.MIN_TRAIN:
        return None
    return cmv3.fit_enet_v3(train, rows_by_month[origin], True)


def cpi_ranking(rows_by_month: dict, origins: list) -> dict:
    """RMSE/MAE и DM/CI по live3/direct/B1/B3 на общих origin'ах.

    Ranking — диагностика: primary остаётся live3, выбор модели по этому
    результату post hoc не делается.
    """
    months = sorted(rows_by_month)
    targets = cph.cumulative_targets(rows_by_month)
    horizons = {}
    predictions = []
    for horizon in cph.HORIZONS:
        errors = {model: {} for model in RANK_MODELS}
        for origin in sorted(set(origins)):
            target_month = cph.shift_month(origin, horizon)
            actual_item = targets[horizon].get(target_month)
            if actual_item is None:
                continue
            actual = float(actual_item[cm.TARGET_ALL])
            naive = targets[horizon].get(cph.shift_month(origin, -1))
            values = {
                "live3_bottomup": cph.forecast_origin(
                    rows_by_month, targets, months, origin, horizon
                ),
                "direct_catboost": forecast_direct(
                    rows_by_month, targets, months, origin, horizon
                ),
                "B1_last": (
                    float(naive[cm.TARGET_ALL]) if naive is not None else None
                ),
                "B3_enet": forecast_b3(
                    rows_by_month, targets, months, origin, horizon
                ),
            }
            for model, value in values.items():
                if value is None:
                    continue
                error = float(value) - actual
                errors[model][origin] = error
                predictions.append(
                    {
                        "horizon": horizon,
                        "model": model,
                        "origin": origin,
                        "target_month": target_month,
                        "predicted": float(value),
                        "actual": actual,
                        "error": error,
                    }
                )
        summary = {}
        for model, by_origin in errors.items():
            if not by_origin:
                continue
            values = list(by_origin.values())
            summary[model] = {
                "n": len(values),
                "rmse": eu.rmse(values),
                "mae": eu.mae(values),
            }
        pairs = {}
        for left, right in (
            ("live3_bottomup", "direct_catboost"),
            ("live3_bottomup", "B1_last"),
            ("live3_bottomup", "B3_enet"),
            ("direct_catboost", "B1_last"),
        ):
            if left not in errors or right not in errors:
                continue
            common = sorted(set(errors[left]) & set(errors[right]))
            if len(common) < 2:
                continue
            err_left = [errors[left][origin] for origin in common]
            err_right = [errors[right][origin] for origin in common]
            dm_stat, dm_p = eu.diebold_mariano(err_left, err_right, horizon)
            boot = eu.block_bootstrap_rmse_diff(
                err_left, err_right, BOOTSTRAP_BLOCK, BOOTSTRAP_N, cm.SEED
            )
            pairs[f"{left} vs {right}"] = {
                "n": len(common),
                "rmse_left": eu.rmse(err_left),
                "rmse_right": eu.rmse(err_right),
                "delta": boot.rmse_diff,
                "delta_ci": [boot.low, boot.high],
                "dm_stat": dm_stat,
                "dm_p": dm_p,
                "bootstrap_p": boot.p_value,
            }
        union = set()
        for by_origin in errors.values():
            union.update(by_origin)
        horizons[f"h{horizon}"] = {
            "n_origins": len(union),
            "models": summary,
            "pairs": pairs,
        }
    return {"horizons": horizons, "predictions": predictions}


def decide(results: dict, comparisons: dict) -> tuple:
    if "h1h3h6" not in results or "baseline" not in results:
        return "reject", "нет варианта h1h3h6 или baseline"
    primary = results["h1h3h6"]["summary"].get((PRIMARY_FAN, PRIMARY_WINDOW))
    if primary is None:
        return "reject", "нет фолдов h1h3h6"
    baseline = results["baseline"]["summary"].get((PRIMARY_FAN, PRIMARY_WINDOW))
    if baseline is None:
        return "reject", "нет фолдов baseline"
    coverage = primary["coverage"][0.80]
    coverage_ok = 0.70 <= coverage["value"] <= 0.90 and coverage["interval"][0] <= 0.80 <= coverage["interval"][1]
    pair = comparisons.get("h1h3h6")
    crps_ok = bool(pair and pair["crps_diff"] < 0.0 and pair["crps_high"] <= 0.0)
    rmse_ok = primary["rmse"] <= baseline["rmse"] * 1.05
    reason = (
        f"CRPS {primary['crps']:.2f} vs {baseline['crps']:.2f}; "
        f"coverage80 {coverage['value']:.3f} "
        f"[{coverage['interval'][0]:.3f}; {coverage['interval'][1]:.3f}]; "
        f"ΔCRPS {pair['crps_diff']:.2f} "
        f"[{pair['crps_low']:.2f}; {pair['crps_high']:.2f}]"
        if pair
        else f"CRPS {primary['crps']:.2f} vs {baseline['crps']:.2f}"
    )
    if coverage_ok and crps_ok and rmse_ok:
        return "promote", reason
    if crps_ok or (pair and pair["crps_diff"] < 0.0):
        return "iterate", reason
    return "reject", reason


def _fmt(value, places: int = 2) -> str:
    if value is None:
        return "—"
    return f"{value:.{places}f}"


def _fmt_p(value) -> str:
    if value is None:
        return "—"
    if value < 0.0001:
        return "<0.0001"
    return f"{value:.4f}"


def render_report(
    data: dict,
    coverage: dict,
    results: dict,
    comparisons: dict,
    ranking: dict,
    decision: str,
    reason: str,
) -> str:
    lines = [
        "# CPI live3 h1/h3/h6 как прокси ожидаемой инфляции",
        "",
        f"**Дата:** {data['as_of'].isoformat()}",
        "**Пункт:** `key-rate/phase-4-cbr-scenario-fan`, Task 4.1 (issue #296)",
        f"**Спека:** [{SPEC_PATH}](../superpowers/specs/2026-10-20-cpi-v3-expectations-design.md)",
        f"**План:** [{PLAN_PATH}](../superpowers/plans/2026-10-20-cpi-v3-expectations.md)",
        "",
        "## 1. Выборка и CPI-прогнозы",
        "",
        "| Показатель | Значение |",
        "|---|---:|",
        f"| решений | {len(data['rows'])} |",
        f"| CPI-месяцев (feature-вселенная) | {coverage['cpi_months']} |",
        f"| пропущено CPI-месяцев | {coverage['cpi_skipped']} |",
        f"| заседаний с h1/h3/h6 | {coverage['h1']} / {coverage['h3']} / {coverage['h6']} |",
        f"| заседаний с полным набором | {coverage['available']} |",
        f"| первый/последний origin | {coverage['first_origin']} / {coverage['last_origin']} |",
        f"| последний feature-origin | {coverage['feature_last_origin']} |",
        "",
        "## 2. Варианты признаков",
        "",
        "| вариант | CPI-признаки | survey-ожидания |",
        "|---|---|---|",
        "| `baseline` | — | да |",
        "| `h1` | `cpi_v3_h1` | да |",
        "| `h1h3h6` | `cpi_v3_h1`, `cpi_v3_h3`, `cpi_v3_h6` | да |",
        "| `replace_survey` | все три | **нет** (заменены) |",
        "",
        "## 3. Веер primary × вариант",
        "",
        "| вариант | n | CRPS | coverage 50 | coverage 80 | coverage 90 | ширина 80 | RMSE | MAE |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for variant in VARIANTS:
        item = results[variant]["summary"].get((PRIMARY_FAN, PRIMARY_WINDOW))
        if item is None:
            continue
        cov = item["coverage"]
        lines.append(
            f"| {variant} | {item['n']} | {_fmt(item['crps'], 3)} "
            f"| {_fmt(cov[0.50]['value'], 3)} | {_fmt(cov[0.80]['value'], 3)} "
            f"| {_fmt(cov[0.90]['value'], 3)} | {_fmt(item['width'][0.80], 2)} "
            f"| {_fmt(item['rmse'])} | {_fmt(item['mae'])} |"
        )
    lines.extend(
        [
            "",
            "## 4. Парное сравнение с baseline",
            "",
            "| вариант | n | ΔCRPS | 95 % CI | p (бутстр.) | DM p | RMSE варианта | RMSE baseline |",
            "|---|---:|---:|---|---:|---:|---:|---:|",
        ]
    )
    baseline = results["baseline"]["summary"].get((PRIMARY_FAN, PRIMARY_WINDOW))
    for variant in VARIANTS[1:]:
        item = comparisons.get(variant)
        if item is None:
            lines.append(f"| {variant} | — | — | — | — | — | — | — |")
            continue
        variant_summary = results[variant]["summary"].get((PRIMARY_FAN, PRIMARY_WINDOW))
        lines.append(
            f"| {variant} | {item['n']} | {_fmt(item['crps_diff'], 3)} "
            f"| [{_fmt(item['crps_low'], 3)}; {_fmt(item['crps_high'], 3)}] "
            f"| {_fmt_p(item['crps_p'])} | {_fmt_p(item['dm_p'])} "
            f"| {_fmt(variant_summary['rmse'] if variant_summary else None)} "
            f"| {_fmt(baseline['rmse'] if baseline else None)} |"
        )
    lines.extend(
        [
            "",
            "## 5. Эпизоды h1h3h6",
            "",
            "| эпизод | n | CRPS | coverage 80 | ширина 80 |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for (fan_name, window, episode), item in sorted(
        results["h1h3h6"]["episodes"].items()
    ):
        if fan_name != PRIMARY_FAN or window != PRIMARY_WINDOW:
            continue
        lines.append(
            f"| {episode} | {item['n']} | {_fmt(item['crps'], 3)} "
            f"| {_fmt(item['coverage80'], 3)} | {_fmt(item['width80'], 2)} |"
        )
    lines.extend(
        [
            "",
            "## 6. CPI-ranking (диагностика)",
            "",
            "Primary CPI-модель — `live3_bottomup`; `direct_catboost`, `B1_last` и "
            "`B3_enet` — сравнение на тех же origin'ах. Ranking не переключает primary.",
            "",
        ]
    )
    for horizon in ("h1", "h3", "h6"):
        item = ranking.get("horizons", {}).get(horizon)
        if not item:
            continue
        lines.extend(
            [
                f"### {horizon}",
                "",
                "| модель | n | RMSE | MAE |",
                "|---|---:|---:|---:|",
            ]
        )
        for model, values in item["models"].items():
            lines.append(
                f"| {model} | {values['n']} | {_fmt(values['rmse'], 4)} "
                f"| {_fmt(values['mae'], 4)} |"
            )
        lines.extend(
            [
                "",
                "| пара | n | RMSE left | RMSE right | Δ | 95 % CI | DM p | bootstrap p |",
                "|---|---:|---:|---:|---:|---|---:|---:|",
            ]
        )
        for name, values in item["pairs"].items():
            lines.append(
                f"| {name} | {values['n']} | {_fmt(values['rmse_left'], 4)} "
                f"| {_fmt(values['rmse_right'], 4)} | {values['delta']:+.4f} "
                f"| [{values['delta_ci'][0]:+.4f}; {values['delta_ci'][1]:+.4f}] "
                f"| {_fmt_p(values['dm_p'])} | {_fmt_p(values['bootstrap_p'])} |"
            )
        lines.append("")
    lines.extend(
        [
            "## 7. Вердикт",
            "",
            f"**`{decision}`** — {reason}.",
            "",
            "Primary — `h1h3h6`; `baseline` — Task 4; `h1` и `replace_survey` — ablations. "
            "Все варианты используют один walk-forward и `disp_vol`.",
            "",
            "## 8. Ограничения",
            "",
            "- PIT/винтажей ИПЦ нет: as-of — cutoff 8-е M+1; ревизии не учтены.",
            "- Целевое окно — `O+1 … O+h`; train-origin `O' <= O−h−1`; h1 — следующий месяц.",
            "- Feature-only origin (последний месяц) не имеет фактического таргета: это live-прогноз.",
            "- CPI v3 long-выборка 2013-10+; для части заседаний CPI-признаки missing.",
            "- h3/h6 — прямые CatBoost-модели на 24+ train-origin'ах; early folds могут быть нестабильны.",
            "- Сценарии и `disp_vol` не менялись; это эксперимент признаков, не новый primary.",
            "- В `forecast_log` не пишется; Go/DDL/витрины/гранты не менялись.",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def write_folds(path: Path, results: dict) -> None:
    import csv

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["variant", "fan", "window", "meeting_date", "step", "weight", "actual"])
        for variant in VARIANTS:
            for item in results[variant]["fans"]:
                for (step, weight) in item.steps:
                    writer.writerow(
                        [
                            variant,
                            item.fan,
                            item.window,
                            item.meeting_date.isoformat(),
                            f"{step:.6f}",
                            f"{weight:.6f}",
                            item.actual_step_bp,
                        ]
                    )


def write_cpi_predictions(path: Path, predictions: list) -> None:
    import csv

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "horizon",
                "model",
                "origin",
                "target_month",
                "predicted",
                "actual",
                "error",
            ]
        )
        for item in predictions:
            writer.writerow(
                [
                    f"h{item['horizon']}",
                    item["model"],
                    item["origin"].isoformat(),
                    item["target_month"].isoformat(),
                    f"{item['predicted']:.6f}",
                    f"{item['actual']:.6f}",
                    f"{item['error']:.6f}",
                ]
            )


def _log_run(
    data: dict,
    results: dict,
    comparisons: dict,
    ranking: dict,
    coverage: dict,
    decision: str,
    artifacts: list,
    enabled: Optional[bool] = None,
) -> str:
    logger = mlx.start_run(
        experiment="key_rate_cpi_v3_expectations",
        run_name="cpi_v3_expectations__h1h3h6",
        config={
            "variants": list(VARIANTS),
            "primary": "h1h3h6",
            "primary_fan": PRIMARY_FAN,
            "as_of": data["as_of"].isoformat(),
            "cpi_model": "live3 bottom-up",
            "cpi_target_window": "O+1..O+h",
            "cpi_train_rule": "O'<=O-h-1",
            "cpi_feature_last_origin": (
                coverage["feature_last_origin"].isoformat()
                if coverage["feature_last_origin"]
                else None
            ),
            "horizons": [1, 3, 6],
        },
        data_asof=data["as_of"],
        sample_meta={
            "rows": len(data["rows"]),
            "cpi_months": coverage["cpi_months"],
            "h1": coverage["h1"],
            "h3": coverage["h3"],
            "h6": coverage["h6"],
            "available": coverage["available"],
        },
        sample=[
            {
                "meeting_date": row.decision.meeting_date.isoformat(),
                "step_bp": row.decision.step_bp,
            }
            for row in data["rows"]
        ],
        spec_path=SPEC_PATH,
        enabled=enabled,
    )
    primary_fans = [
        item
        for item in results.get("h1h3h6", {}).get("fans", [])
        if item.fan == PRIMARY_FAN and item.window == PRIMARY_WINDOW
    ]
    logger.log_metrics(
        {
            "primary_metric": "crps",
            "decision": decision,
            "variants": {
                variant: {
                    "crps": item["crps"],
                    "coverage80": item["coverage"][0.80]["value"],
                    "rmse": item["rmse"],
                    "n": item["n"],
                }
                for variant, result in results.items()
                for (fan, window), item in result["summary"].items()
                if fan == PRIMARY_FAN and window == PRIMARY_WINDOW
            },
            "comparisons": comparisons,
            "cpi_ranking": ranking["horizons"],
        }
    )
    logger.log_predictions(
        [
            mlx.PredictionRow(
                model=f"{item.fan}|{label}",
                window=item.window,
                target_date=item.meeting_date,
                predicted=float(step),
                actual=float(item.actual_step_bp),
                unit="bp",
                meta={"variant": "h1h3h6", "fan": item.fan, "weight": weight},
            )
            for item in primary_fans
            for label, (step, weight) in zip(item.labels, item.steps)
        ]
        + [
            mlx.PredictionRow(
                model=f"cpi__{item['model']}",
                window=f"h{item['horizon']}",
                target_date=item["target_month"],
                predicted=float(item["predicted"]),
                actual=float(item["actual"]),
                unit="pp",
                meta={
                    "variant": "h1h3h6",
                    "origin": item["origin"].isoformat(),
                },
            )
            for item in ranking["predictions"]
        ]
    )
    logger.log_artifacts(artifacts)
    return logger.finish("ok", notes=f"decision={decision}")


def run(
    as_of: Optional[date] = None,
    out_dir: Path = Path("build/cpi-v3-expectations"),
    variants: tuple = VARIANTS,
    logger_enabled: Optional[bool] = None,
) -> int:
    as_of = as_of or date.today()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    data = load_rate_data(as_of)
    cpi_by_meeting, coverage, cpi_rows = load_cpi_features(data["rows"], as_of)
    origins = sorted(
        {
            item["cpi_v3_origin"]
            for item in cpi_by_meeting.values()
            if item["cpi_v3_origin"] is not None
        }
    )
    ranking = cpi_ranking(cpi_rows, origins)
    print(
        f"решений {len(data['rows'])}; CPI h1/h3/h6 "
        f"{coverage['h1']}/{coverage['h3']}/{coverage['h6']}; "
        f"origin {coverage['first_origin']}…{coverage['last_origin']}; "
        f"feature-origin до {coverage['feature_last_origin']}"
    )
    results = {
        variant: run_variant(data, variant, cpi_by_meeting)
        for variant in variants
    }
    comparisons = compare_variants(results)
    decision, reason = decide(results, comparisons)
    report_path = out_dir / REPORT_NAME
    result_path = out_dir / RESULT_NAME
    folds_path = out_dir / FOLDS_NAME
    cpi_predictions_path = out_dir / CPI_PREDICTIONS_NAME
    report_path.write_text(
        render_report(
            data, coverage, results, comparisons, ranking, decision, reason
        ),
        encoding="utf-8",
    )
    payload = {
        "as_of": as_of.isoformat(),
        "coverage": {
            **coverage,
            "first_origin": coverage["first_origin"].isoformat()
            if coverage["first_origin"]
            else None,
            "last_origin": coverage["last_origin"].isoformat()
            if coverage["last_origin"]
            else None,
            "feature_last_origin": coverage["feature_last_origin"].isoformat()
            if coverage["feature_last_origin"]
            else None,
        },
        "variants": {
            variant: {
                f"{fan}__{window}": item
                for (fan, window), item in result["summary"].items()
            }
            for variant, result in results.items()
        },
        "comparisons": comparisons,
        "cpi_ranking": ranking["horizons"],
        "decision": decision,
        "reason": reason,
    }
    result_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    write_folds(folds_path, results)
    write_cpi_predictions(cpi_predictions_path, ranking["predictions"])
    for variant in variants:
        item = results[variant]["summary"].get((PRIMARY_FAN, PRIMARY_WINDOW))
        if item is None:
            continue
        print(
            f"  {variant:<15} n={item['n']:>3} CRPS {item['crps']:>7.3f} "
            f"coverage80 {item['coverage'][0.80]['value']:.3f} "
            f"RMSE {item['rmse']:>7.2f}"
        )
    print(f"вердикт: {decision} — {reason}")
    print(f"отчёт: {report_path}")
    try:
        result = _log_run(
            data,
            results,
            comparisons,
            ranking,
            coverage,
            decision,
            [report_path, result_path, folds_path, cpi_predictions_path],
            enabled=logger_enabled,
        )
        print(f"журнал эксперимента: {result}")
    except Exception as error:  # noqa: BLE001
        print(f"журнал эксперимента: ошибка ({error}) — прогон продолжен", file=sys.stderr)
    return 0


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="CPI v3 h1/h3/h6 как прокси ожидаемой инфляции (issue #296)."
    )
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--out-dir", default="build/cpi-v3-expectations")
    parser.add_argument(
        "--variants",
        default=",".join(VARIANTS),
        help="через запятую: baseline,h1,h1h3h6,replace_survey",
    )
    args = parser.parse_args(argv)
    as_of = None
    if args.as_of:
        try:
            as_of = date.fromisoformat(args.as_of)
        except ValueError:
            print(f"ошибка --as-of: {args.as_of!r}", file=sys.stderr)
            return 2
    variants = tuple(item.strip() for item in args.variants.split(",") if item.strip())
    unknown = set(variants) - set(VARIANTS)
    if unknown:
        print(f"неизвестные варианты: {sorted(unknown)}", file=sys.stderr)
        return 2
    return run(as_of=as_of, out_dir=Path(args.out_dir), variants=variants)


if __name__ == "__main__":
    raise SystemExit(main())
