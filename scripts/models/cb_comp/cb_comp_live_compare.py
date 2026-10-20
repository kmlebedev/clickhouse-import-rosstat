#!/usr/bin/env python3
"""Сравнение live 3-компонентной модели и per-sheet live на общем таргете.

Зачем: production live-пайплайн (#251) меряет 24 месяца m/m, а per-sheet (#252)
и CB_COMP — 54 normal-месяца накопленного h-роста на `TARGET_FULL`. Прямая
таблица была бы некорректной. Скрипт строит **live 3-компонентную** модель на
том же таргете, окне и origin, что per-sheet/CB_COMP:

* компоненты — официальные ИПЦ food/nonfood/services;
* признаки — v3 (недельные diffusion + МП + USD/ставка/FAO + лаги компонент);
* origin — `M−(h−1)` (конструкция CB_COMP), фиксированный train
  2010-03…2018-12, test — 54 normal-месяца;
* агрегат — NNLS-веса по train;
* факт — `TARGET_FULL[h]` из архива авторов.

Per-sheet и CB_COMP числа читаются из
`docs/research/2026-10-19-cb-comp-per-sheet-live.json`, чтобы не переобучать
per-sheet повторно. Скрипт только читает ClickHouse/архив; в БД не пишет.
"""

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from models.cb_comp import cb_comp_parity as cp
from models.cpi import cpi_monthly as cm
from models.cpi import cpi_monthly_v3 as v3
from lib import eval_utils as eu

HORIZONS = (1, 2, 3, 6)
TRAIN_START = date(2010, 3, 1)
TRAIN_END = date(2018, 12, 1)
TEST_START = date(2019, 1, 1)
TEST_END = date(2024, 7, 1)
SA_HISTORY = (
    "Models/CpiComponents/StfCpi_External_data/databases/StfCpiR_db/"
    "raw_data_store/cpi_sa_target_history.xlsx"
)
SA_COMPONENT_MAP = {
    "6": "cpi_food",
    "7": "cpi_nonfood",
    "9000": "cpi_services",
    "1 - 9470": "cpi_all",
}
SA_TRAIN_START = date(2016, 1, 1)


def shift_month(day: date, months: int) -> date:
    index = day.year * 12 + day.month - 1 + months
    return date(index // 12, index % 12 + 1, 1)


def month_start(value) -> date:
    if isinstance(value, datetime):
        return date(value.year, value.month, 1)
    if isinstance(value, date):
        return date(value.year, value.month, 1)
    return date.fromisoformat(str(value)[:10]).replace(day=1)


def build_rows(first: date, last: date) -> tuple:
    """Месячные v3-строки с официальными ИПЦ-метками; пропуски — с причинами."""
    targets = cm.load_targets()
    index = cm.target_index(targets)
    survey = cm.load_survey()
    usd = cm.load_usd()
    key_rate = cm.load_key_rate()
    fao = cm.load_fao()
    panel = v3.weekly_panel(v3.load_weekly())
    rows = []
    skipped = []
    for month in cm.sample_months(targets):
        if month < first or month > last:
            continue
        cutoff = cm.cutoff_for(month)
        base, reason = cm.base_features(
            month, index, usd, key_rate, fao, cutoff
        )
        if base is None:
            skipped.append((month, reason))
            continue
        mp_part, reason = cm.mp_features(month, survey, cutoff)
        if mp_part is None:
            skipped.append((month, reason))
            continue
        weekly_part = v3.weekly_features(panel, cutoff)
        for name in v3.WEEKLY_FEATURES:
            weekly_part.setdefault(name, float("nan"))
        y = {
            code: values[cm.month_end(month)]
            for code, values in index.items()
            if cm.month_end(month) in values
        }
        if len(y) != len(cm.TARGET_SERIES):
            skipped.append((month, "неполная метка компонент"))
            continue
        rows.append(
            cm.Row(
                month=month,
                y=y,
                features={**base, **mp_part, **weekly_part},
            )
        )
    return rows, skipped


def cumulative_targets(rows: list, horizon: int) -> dict:
    """`{M: {code: накопленный h-рост, п.п.}}` по официальным ИПЦ-меткам."""
    by_month = {row.month: row for row in rows}
    result = {}
    for month in by_month:
        window = [shift_month(month, -offset) for offset in range(horizon)]
        if any(item not in by_month for item in window):
            continue
        result[month] = {}
        for code in (*cm.BOTTOMUP_COMPONENTS, cm.TARGET_ALL):
            product = 1.0
            for item in window:
                product *= 1.0 + float(by_month[item].y[code]) / 100.0
            result[month][code] = (product - 1.0) * 100.0
    return result


def parse_sa_levels(archive: Path) -> dict:
    """SA-уровни авторов (100 = пред. месяц) для 6/7/9000/1−9470."""
    frame = pd.read_excel(archive / SA_HISTORY, sheet_name="Россия")
    frame["Код"] = frame["Код"].astype(str).str.strip()
    levels = {code: {} for code in SA_COMPONENT_MAP}
    for _, row in frame.iterrows():
        sa_code = row["Код"]
        if sa_code not in SA_COMPONENT_MAP:
            continue
        for column in frame.columns[2:]:
            month = month_start(column)
            value = row[column]
            if pd.isna(value):
                continue
            levels[sa_code][month] = float(value)
    return levels


def cumulative_sa_targets(levels: dict, horizon: int) -> dict:
    """`{M: {code: накопленный h-рост}}` из SA-уровней авторов."""
    aggregate = levels["1 - 9470"]
    result = {}
    for month in aggregate:
        window = [shift_month(month, -offset) for offset in range(horizon)]
        if any(item not in aggregate for item in window):
            continue
        result[month] = {}
        for sa_code, code in SA_COMPONENT_MAP.items():
            if any(item not in levels[sa_code] for item in window):
                continue
            product = 1.0
            for item in window:
                product *= levels[sa_code][item] / 100.0
            result[month][code] = (product - 1.0) * 100.0
    return result


def fit_component(train: list, code: str) -> tuple:
    names = v3.feature_names_v3()
    matrix = np.array(
        [[row.features[name] for name in names] for row in train], dtype=object
    )
    target = np.asarray([float(row.y[code]) for row in train], dtype=float)
    model = CatBoostRegressor(
        loss_function="RMSE",
        random_seed=cm.SEED,
        verbose=False,
        allow_writing_files=False,
        cat_features=[names.index("month")],
    )
    model.fit(matrix, target)
    return model, names


def predict(model: CatBoostRegressor, names: list, features: dict) -> float:
    matrix = np.array([[features[name] for name in names]], dtype=object)
    return float(model.predict(matrix)[0])


def evaluate_h(
    rows: list,
    targets: dict,
    archive_target: pd.DataFrame,
    normal: list,
    horizon: int,
    train_start: date = TRAIN_START,
    train_end: date = TRAIN_END,
) -> dict:
    by_month = {row.month: row for row in rows}
    train_months = [
        row.month
        for row in rows
        if train_start <= row.month <= train_end
        and row.month not in cp.TRAIN_OUTLIER_MONTHS
        and row.month in targets
        and shift_month(row.month, -(horizon - 1)) in by_month
    ]
    train = [
        cm.Row(
            month=shift_month(month, -(horizon - 1)),
            y=targets[month],
            features=by_month[shift_month(month, -(horizon - 1))].features,
        )
        for month in train_months
    ]
    if len(train) < 20:
        return {"n_train": len(train), "error": "мало train-строк"}
    weights = cm.nnls_weights(train)
    models = {}
    names = None
    for code in cm.BOTTOMUP_COMPONENTS:
        model, names = fit_component(train, code)
        models[code] = model
    errors = []
    predictions = []
    for month in normal:
        if month < TEST_START or month > TEST_END:
            continue
        origin = shift_month(month, -(horizon - 1))
        if origin not in by_month or month not in archive_target.index:
            continue
        actual = float(archive_target.at[month, horizon])
        if np.isnan(actual):
            continue
        components = [
            predict(models[code], names, by_month[origin].features)
            for code in cm.BOTTOMUP_COMPONENTS
        ]
        forecast = float(
            sum(weight * value for weight, value in zip(weights, components))
        )
        errors.append(forecast - actual)
        predictions.append(
            {
                "month": month.isoformat(),
                "actual": actual,
                "forecast": forecast,
                "error": forecast - actual,
            }
        )
    return {
        "horizon": horizon,
        "n_train": len(train),
        "n": len(errors),
        "rmse": eu.rmse(errors) if errors else None,
        "mae": eu.mae(errors) if errors else None,
        "weights": list(weights),
        "predictions": predictions,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Live 3-компонентная модель vs per-sheet vs CB_COMP"
    )
    parser.add_argument("--archive", default=None)
    parser.add_argument(
        "--out-dir",
        default=str(ROOT / "build" / "cb-comp-live-compare"),
    )
    parser.add_argument(
        "--per-sheet-json",
        default=str(
            ROOT / "docs/research/2026-10-19-cb-comp-per-sheet-live.json"
        ),
    )
    arguments = parser.parse_args(argv)

    archive = cp.resolve_archive(arguments.archive)
    archive_data = cp.check_archive(archive)
    normal = archive_data["normal"]
    out_dir = Path(arguments.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Архив: {archive}", flush=True)
    print(f"normal: {len(normal)} месяцев", flush=True)

    rows, skipped = build_rows(TRAIN_START, TEST_END)
    print(
        f"v3-строки: {len(rows)} ({rows[0].month}…{rows[-1].month}), "
        f"пропущено {len(skipped)}",
        flush=True,
    )
    sa_levels = parse_sa_levels(archive)
    results = {}
    results_sa = {}
    for horizon in HORIZONS:
        targets = cumulative_targets(rows, horizon)
        result = evaluate_h(rows, targets, archive_data["target"], normal, horizon)
        results[str(horizon)] = result
        if result.get("rmse") is not None:
            print(
                f"h{horizon}: live3 official RMSE {result['rmse']:.6f}, "
                f"MAE {result['mae']:.6f}, n={result['n']}, "
                f"train={result['n_train']}",
                flush=True,
            )
        else:
            print(f"h{horizon}: live3 official {result}", flush=True)

        sa_targets = cumulative_sa_targets(sa_levels, horizon)
        sa_result = evaluate_h(
            rows,
            sa_targets,
            archive_data["target"],
            normal,
            horizon,
            train_start=SA_TRAIN_START,
        )
        results_sa[str(horizon)] = sa_result
        if sa_result.get("rmse") is not None:
            print(
                f"h{horizon}: live3 SA RMSE {sa_result['rmse']:.6f}, "
                f"MAE {sa_result['mae']:.6f}, n={sa_result['n']}, "
                f"train={sa_result['n_train']}",
                flush=True,
            )
        else:
            print(f"h{horizon}: live3 SA {sa_result}", flush=True)

    per_sheet = json.loads(Path(arguments.per_sheet_json).read_text(encoding="utf-8"))
    per_sheet_rmse = per_sheet["rmse"]
    table = []
    for horizon in HORIZONS:
        live = results[str(horizon)].get("rmse")
        live_sa = results_sa[str(horizon)].get("rmse")
        sheet = per_sheet_rmse[str(horizon)]["per_sheet_public"]
        cb = per_sheet_rmse[str(horizon)]["cb"]
        table.append(
            {
                "horizon": horizon,
                "live3_official_rmse": live,
                "live3_sa_rmse": live_sa,
                "per_sheet_rmse": sheet,
                "cb_rmse": cb,
                "live3_official_vs_per_sheet": live / sheet if live else None,
                "live3_sa_vs_per_sheet": live_sa / sheet if live_sa else None,
                "live3_official_vs_cb": live / cb if live else None,
                "live3_sa_vs_cb": live_sa / cb if live_sa else None,
                "per_sheet_vs_cb": sheet / cb,
            }
        )
    payload = {
        "run": "cb_comp_live_vs_per_sheet",
        "date": "2026-10-20",
        "status": "diagnostic",
        "target": "TARGET_FULL (архив CB_COMP)",
        "window": {
            "train_official": f"{TRAIN_START}…{TRAIN_END}",
            "train_sa": f"{SA_TRAIN_START}…{TRAIN_END}",
            "test": f"{TEST_START}…{TEST_END}",
            "normal_months": len(normal),
            "origin": "M−(h−1)",
        },
        "models": {
            "live3_official": "3 официальных компонента ИПЦ NSA + v3-признаки; NNLS-агрегат",
            "live3_sa": "3 SA-компонента авторов (6/7/9000) + v3-признаки; NNLS-агрегат",
            "per_sheet": "88 групп ИПЦ + публичные листовые МП; CPI-веса",
            "cb": "эталон CB_COMP (архив)",
        },
        "rows": len(rows),
        "skipped": len(skipped),
        "results": {"official": results, "sa": results_sa},
        "table": table,
        "caveats": [
            "Live3 — диагностическая реконструкция: фиксированный train и h-таргеты, а не production nowcast #251.",
            "live3_official: официальные ИПЦ NSA — не тот же таргет, что SA TARGET_FULL; это нижняя/смещённая оценка.",
            "live3_sa: SA-компоненты авторов (6/7/9000) и агрегат 1−9470; train 2016-01…2018-12, короче per-sheet.",
            "Per-sheet обучался на 2010-03…2018-12; live3 official — на доступных v3-строках того же окна; n_train назван.",
            "CB_COMP не переобучался; сравнение с ним — на архивных прогнозах.",
        ],
    }
    json_path = out_dir / "cb_comp_live_vs_per_sheet.json"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    md_path = out_dir / "cb_comp_live_vs_per_sheet.md"
    lines = [
        "# Live 3-компонентная модель vs per-sheet vs CB_COMP",
        "",
        f"Таргет: `{payload['target']}`; test: 54 normal-месяца "
        f"({TEST_START}…{TEST_END}); origin `M−(h−1)`.",
        "",
        "| h | live3 official RMSE | live3 SA RMSE | per-sheet RMSE | CB_COMP RMSE |",
        "|---:|---:|---:|---:|---:|",
    ]
    for item in table:
        official = item["live3_official_rmse"]
        sa = item["live3_sa_rmse"]
        official_text = "—" if official is None else f"{official:.6f}"
        sa_text = "—" if sa is None else f"{sa:.6f}"
        lines.append(
            f"| {item['horizon']} | {official_text} | {sa_text} | "
            f"{item['per_sheet_rmse']:.6f} | {item['cb_rmse']:.6f} |"
        )
    lines.extend(
        [
            "",
            "## Оговорки",
            "",
            *[f"- {item}" for item in payload["caveats"]],
            "",
            f"Live3 official train-строк: "
            + ", ".join(
                f"h{h}={results[str(h)].get('n_train')}" for h in HORIZONS
            ),
            f"Live3 SA train-строк: "
            + ", ".join(
                f"h{h}={results_sa[str(h)].get('n_train')}" for h in HORIZONS
            ),
        ]
    )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Артефакты: {json_path}, {md_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
