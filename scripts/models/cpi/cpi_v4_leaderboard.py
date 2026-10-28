#!/usr/bin/env python3
"""Каркас лидерборда CPI v4 и регрессионный гейт v2/v3 (#396, Task 4).

Пункт `key-rate/phase-4-cpi-v4-weekly-aggregate-core`, роль ML. Спека
`docs/superpowers/specs/2026-10-28-cpi-v4-weekly-aggregate-core-design.md`
§4–§7, план Task 4. Здесь — общий слой для экспериментов 1–3:

* сетка 3 таргета (`T_NSA`, `T_SA`, `T_CORE`) × горизонты 1/3/6;
* накопленные цели `O+1…O+h` и purge `O' ≤ O−h−1` (правило live3);
* общие фолды, парное сравнение (DM-HLN с поправкой на перекрытие, блочный
  бутстрап, MDE), MCS 90 %, детерминированный отпечаток предсказаний;
* регрессионный гейт: v2/v3 на `S_W22` совпадают с эталоном Task 4 (спека §7.1) до 1e-6, признаки — по sha256 — иначе
  эксперименты 2–3 не запускаются (спека §8).

Скрипт только читает ClickHouse; в БД, `forecast_log` и MLflow не пишет.
Оценка сетки по моделям — Task 5 (модели уже вызываются через `cpi_monthly`/
`cpi_monthly_v3` без копирования).

Запуск — `make cpi-v4-leaderboard ARGS="--gate"`, тесты —
`make cpi-v4-leaderboard-test`.
"""

import argparse
import hashlib
import json
import math
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

TARGETS = ("T_NSA", "T_SA", "T_CORE")
HORIZONS = (1, 3, 6)
SEED = 20261018
BOOTSTRAP_BLOCK = 3
BOOTSTRAP_N = 2000
MCS_ALPHA = 0.10
MCS_REPS = 2000

# Историческая справка: таблица Stage 1 `weekly` (2022-01…2026-08, 32 фолда,
# start=24, embargo=0), docs/research/2026-10-19-cpi-components-public-data.md §4.
# В гейт НЕ входит: на данных 2026-10-28 она не воспроизводится
# (docs/research/2026-10-28-cpi-v4-regression-gate.md), эталон — ниже.
STAGE1_WEEKLY_RMSE = {
    "v3_bottomup": 0.311487,
    "v2_bottomup": 0.362300,
    "v3_direct": 0.384900,
    "B3_weekly": 0.390178,
    "v2_baseline": 0.437790,
    "B3": 0.438010,
    "B1": 0.489295,
    "B2": 0.873540,
}

# Эталон гейта Task 4 (спека §7.1, поправка 2026-10-28): результат текущего кода
# на данных 2026-10-28 (`S_W22`, 32 фолда, start=24, embargo=0); два полных
# прогона подряд идентичны. Эталон проверяет воспроизводимость и неизменность
# входов, а не «правильность» чисел Stage 1. Переустанавливать молча нельзя.
TASK4_REFERENCE_RMSE = {
    "v3_bottomup": 0.312836438,
    "v2_bottomup": 0.367059285,
    "v3_direct": 0.369967068,
    "B3_weekly": 0.391455174,
    "v2_baseline": 0.419888975,
    "B3": 0.432999805,
    "B1": 0.489294773,
    "B2": 0.873540285,
}
# sha256 значений признаков v3 по месяцам `S_W22` (`feature_digests`): при
# расхождении гейт называет изменившиеся признаки.
TASK4_FEATURE_SHA256 = {
    "business_climate_sa": "b3756922710905365999e7588e0276f8df396911df953ddd31bb701b21a30386",
    "business_climate_sa_chg": "b5c00e3c342a2de7848e9a15f78e7693363f4fcca8ccc46d94a9897f8f0625dc",
    "capacity_utilization_sa": "26d3e4cb2ca4ffd207a3a11c737025b59ac5a63550b1d7f78ec591802f28e962",
    "credit_conditions": "31cfdb7b78e40dc21ff12acb231ec485927258e6f7ccef4bd49adea6f5d2d6ed",
    "fao_mm": "c63c18422ca500d60e85390dbf0b021f32054b9e30f49a345c05f826c6530109",
    "headcount_expectations_sa": "7d9a53985259278a11e605f28a00b1f2c124867d984b178cda44a6be85246e31",
    "key_rate": "856d0e50380cedc918431f8357014619df53735d94b9948ea1f30cb7ed2b88ed",
    "month": "c9c90dbd2797668fc7d9db401e29a2c592921484c3feac70a987054986c8217b",
    "price_expectations_sa": "7ea14d10d48308667cb6c1dc7b14f406bf26186c1e424bc157403f0c1188c1d0",
    "price_expectations_sa_chg": "60309ebbbb23fbafcf819eef5e1b84439206714a912ebe0e05b4637979b7e69a",
    "usd_mm": "7ce5d7f7c43b7961f0b2c0381c9d82dd8469535e5f10fbeb1c74969f07b6e7e3",
    "wk_gasoline4": "bd4d9cbcf6eda0691bdf6c1751551c97e53c5948bac497defd874d1eb7cf6821",
    "wk_median": "b1e600d17ecbb8dcb1d4b0d5e25ec146ff788747706a53f5da55c0d47cdc8719",
    "wk_median4": "08e3bf5c935672877e7a3d782df1945ec41318830bb77540fe29de56f93dab34",
    "wk_share_rising": "839b6c173f2f58bc35f0716903138ece94c762b85e796b8d989d69195f5f7452",
    "y_lag1": "1d4e45cd0b7ba9b2c42f3baec015df8921a0f6cb06f3f166f3540f8ed556222a",
    "y_lag2": "45262cf93ef7498dc24b98cb466d0b47aea0a1d932db910d1065d47d12cbb3d2",
}
REGRESSION_TOLERANCE = 1e-6
GATE_START = 24
GATE_EMBARGO = 0

CORE_SERIES = "Базовый ИПЦ"
CORE_SQL = f"""
SELECT toStartOfMonth(date) AS month, argMax(toFloat64(value), date) AS value
FROM cbr_indicators_cpd FINAL
WHERE name = '{CORE_SERIES}'
GROUP BY month
ORDER BY month
FORMAT JSON
"""


# ---------------------------------------------------------------------------
# Чистые функции
# ---------------------------------------------------------------------------


def shift_month(day: date, months: int) -> date:
    """Первое число месяца, сдвинутого на `months` от месяца дня `day`."""
    index = day.year * 12 + (day.month - 1) + months
    return date(index // 12, index % 12 + 1, 1)


def grid_cells() -> list:
    """Девять ячеек лидерборда `(таргет, h)` в фиксированном порядке."""
    return [(target, horizon) for target in TARGETS for horizon in HORIZONS]


def cumulative_pp(series: dict, month: date, horizon: int):
    """Накопленный рост за `month … month+h−1` (сложный процент, п.п.); None при дыре."""
    if horizon < 1:
        raise ValueError(f"горизонт должен быть ≥ 1: {horizon}")
    product = 1.0
    for offset in range(horizon):
        value = series.get(shift_month(month, offset))
        if value is None:
            return None
        product *= 1.0 + float(value) / 100.0
    return (product - 1.0) * 100.0


def cumulative_by_origin(series: dict, horizon: int) -> dict:
    """`{origin: рост за O+1…O+h}` для origin'ов, у которых окно целиком наблюдено."""
    out = {}
    for origin in sorted(series):
        value = cumulative_pp(series, shift_month(origin, 1), horizon)
        if value is not None:
            out[origin] = value
    return out


def purged_train_origins(origins: list, origin: date, horizon: int) -> list:
    """Origin'ы `O' ≤ O−h−1`: цель `O'+h` наблюдена до месяца `O` (правило live3)."""
    boundary = shift_month(origin, -horizon - 1)
    return [month for month in origins if month <= boundary]


def common_folds(errors_by_model: dict, models=None) -> list:
    """Origin'ы, на которых есть ошибка у всех выбранных моделей (по возрастанию)."""
    names = list(errors_by_model) if models is None else list(models)
    if not names:
        return []
    common = set(errors_by_model[names[0]])
    for name in names[1:]:
        common &= set(errors_by_model[name])
    return sorted(common)


def _mde_ratio(err_a: list, err_b: list) -> float:
    if len(err_a) < 2:
        return float("nan")
    import numpy as np
    from lib import eval_utils as eu

    differences = [a * a - b * b for a, b in zip(err_a, err_b)]
    sd = float(np.std(differences, ddof=1))
    rmse_sum = eu.rmse(err_a) + eu.rmse(err_b)
    mde_mse = 2.802 * sd / math.sqrt(len(differences))
    return mde_mse / rmse_sum if rmse_sum else float("nan")


def compare_pair(err_left: list, err_right: list, horizon: int) -> dict:
    """Парное сравнение на общих фолдах: ΔRMSE, 95 % бутстрап-CI, DM-HLN, MDE.

    DM-HLN получает горизонт `h` (поправка на перекрытие окон накопленной цели).
    Отрицательная `delta` — левая модель лучше правой.
    """
    from lib import eval_utils as eu

    if len(err_left) != len(err_right):
        raise ValueError(f"{len(err_left)} ошибок против {len(err_right)}")
    dm_stat, dm_p = eu.diebold_mariano(err_left, err_right, horizon)
    boot = eu.block_bootstrap_rmse_diff(err_left, err_right, BOOTSTRAP_BLOCK, BOOTSTRAP_N, SEED)
    return {
        "n": len(err_left),
        "rmse_left": eu.rmse(err_left),
        "rmse_right": eu.rmse(err_right),
        "delta": boot.rmse_diff,
        "delta_ci": [boot.low, boot.high],
        "dm_stat": dm_stat,
        "dm_p": dm_p,
        "mde_ratio": _mde_ratio(err_left, err_right),
    }


def mcs_included(losses_by_model: dict) -> tuple:
    """90 % MCS по потерям моделей на общих фолдах: `(множество включённых, p-значения)`.

    Потери — квадраты ошибок, одинаковой длины; нужны минимум три модели.
    Бутстрап стационарный, блок и seed фиксированы. Вход `arch` — DataFrame
    «наблюдения × модели», чтобы имена не зависели от порядка индексов.
    """
    if len(losses_by_model) < 3:
        raise ValueError("MCS: нужно минимум три модели")
    import pandas as pd
    from arch.bootstrap import MCS

    frame = pd.DataFrame({name: list(values) for name, values in losses_by_model.items()})
    mcs = MCS(
        frame,
        size=MCS_ALPHA,
        reps=MCS_REPS,
        block_size=BOOTSTRAP_BLOCK,
        method="R",
        bootstrap="stationary",
        seed=SEED,
    )
    mcs.compute()
    included = set(mcs.included)
    pvalues = {name: float(value) for name, value in mcs.pvalues["Pvalue"].items()}
    return included, pvalues


def predictions_digest(rows: list) -> str:
    """sha256 канонического представления предсказаний (порядок строк не важен)."""
    lines = sorted(
        json.dumps(row, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
        for row in rows
    )
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def feature_digests(rows: list) -> dict:
    """`{признак: sha256 значений по месяцам}`: по какому признаку разошлись данные.

    Нужен гейту: при расхождении с паспортом Stage 1 хеш всей выборки
    говорит только «изменилась», а поэлементный отпечаток называет признак.
    """
    names = sorted({name for row in rows for name in row.features})
    digests = {}
    for name in names:
        values = sorted(
            (row.month.isoformat(), repr(float(row.features[name])) if name in row.features else "absent")
            for row in rows
        )
        digests[name] = hashlib.sha256(json.dumps(values, separators=(",", ":")).encode("utf-8")).hexdigest()
    return digests


def check_features(digests: dict, reference=None) -> dict:
    """Сверка отпечатков признаков с эталоном Task 4: `changed` — отличающиеся или отсутствующие.

    Лишние признаки (например `wkagg_*`) не проверяются: эталон — набор v3.
    """
    reference = TASK4_FEATURE_SHA256 if reference is None else reference
    changed = sorted(name for name, expected in reference.items() if digests.get(name) != expected)
    return {"passed": not changed, "changed": changed}


def check_regression(rmse_by_model: dict, reference=None, tolerance=REGRESSION_TOLERANCE) -> dict:
    """Сверка RMSE моделей с эталоном Task 4; `passed` только если все модели в допуске."""
    reference = TASK4_REFERENCE_RMSE if reference is None else reference
    rows = []
    for model, expected in reference.items():
        got = rmse_by_model.get(model)
        ok = got is not None and not math.isnan(got) and abs(got - expected) <= tolerance
        rows.append(
            {
                "model": model,
                "expected": expected,
                "got": got,
                "delta": None if got is None else got - expected,
                "ok": bool(ok),
            }
        )
    return {"passed": all(row["ok"] for row in rows), "tolerance": tolerance, "rows": rows}


def select_reference(models) -> dict:
    """Эталон Task 4 целиком (`None`) или только выбранные модели; неизвестная — `ValueError`."""
    if models is None:
        return dict(TASK4_REFERENCE_RMSE)
    unknown = [model for model in models if model not in TASK4_REFERENCE_RMSE]
    if unknown:
        raise ValueError(f"нет в эталоне Task 4: {unknown}")
    return {model: TASK4_REFERENCE_RMSE[model] for model in models}


def core_from_rows(rows: list) -> dict:
    """`{месяц: Базовый ИПЦ SA m/m, п.п.}` из строк `{month, value}`."""
    out = {}
    for row in rows:
        month = date.fromisoformat(str(row["month"])[:10])
        value = float(row["value"])
        if not math.isfinite(value):
            raise ValueError(f"нечисловое значение core за {month}: {row['value']!r}")
        if month in out:
            raise ValueError(f"дубль месяца core: {month}")
        out[month] = value
    return out


# ---------------------------------------------------------------------------
# Загрузчики таргетов (только чтение ClickHouse)
# ---------------------------------------------------------------------------


def load_target_series() -> dict:
    """`{код таргета: {месяц: m/m, п.п.}}` для T_NSA, T_SA, T_CORE.

    `T_NSA` — официальный ИПЦ (`rosstat_ipc_mes`, `y = percent − 100`);
    `T_SA` — прокси «SA ИПЦ без КУ» с допущениями cb_comp_proxy (вес ЖКУ 0,10,
    рост тарифа 4 %/год); `T_CORE` — «Базовый ИПЦ» SA ЦБ.
    """
    from lib.clickhouse import ch_query
    from models.cb_comp import cb_comp_proxy as proxy
    from models.cpi import cpi_monthly as cm

    targets = cm.load_targets()
    nsa = {cm.month_of(day): float(value) for day, value in targets[cm.TARGET_ALL]}
    sa = proxy.proxy_target(
        proxy.load_sa_cpi(), proxy.DEFAULT_UTILITIES_WEIGHT, proxy.DEFAULT_UTILITIES_GROWTH
    )
    return {"T_NSA": nsa, "T_SA": dict(sa), "T_CORE": core_from_rows(ch_query(CORE_SQL))}


# ---------------------------------------------------------------------------
# Регрессионный гейт (S_W22, v2/v3 против эталона Task 4, спека §7.1)
# ---------------------------------------------------------------------------


def run_gate(out_dir: Path, models=None) -> dict:
    """Прогон Stage 1-моделей на `S_W22` через строки v4 и сверка с эталоном Task 4.

    `models` — подмножество только для диагностики дрейфа: гейт на подмножестве
    помечается `partial` и не заменяет полный.
    """
    from models.cpi import cpi_monthly_v3 as v3
    from models.cpi import cpi_weekly_aggregate as agg

    rows, skipped = agg.build_sample_v4("S_W22", with_diffusion=True)
    v3_rows, _ = v3.build_sample_v3("weekly")
    v3_names = v3.feature_names_v3()
    if [row.month for row in rows] != [row.month for row in v3_rows] or any(
        a.features[name] != b.features[name] and not (math.isnan(a.features[name]) and math.isnan(b.features[name]))
        for a, b in zip(rows, v3_rows)
        for name in v3_names
    ):
        raise SystemExit("гейт: строки v4 S_W22 не совпали со строками v3 weekly")
    v3.assert_no_leakage_v3(rows)
    reference = select_reference(models)
    selected = {name: v3.MODELS[name] for name in reference}
    summary, _pairs, predictions = v3.evaluate(rows, GATE_START, GATE_EMBARGO, selected)
    rmse = {model: item.get("rmse") for model, item in summary.items()}
    report = check_regression(rmse, reference)
    features = feature_digests(rows)
    feature_report = check_features(features)
    report["features"] = feature_report
    report["passed"] = bool(report["passed"] and feature_report["passed"])
    prediction_rows = [
        {
            "model": item.model,
            "origin": item.month.isoformat(),
            "horizon": 0,
            "pred": item.y_pred,
            "actual": item.y_true,
        }
        for item in predictions
    ]
    payload = {
        "sample": "S_W22",
        "partial": models is not None,
        "n_rows": len(rows),
        "n_skipped": len(skipped),
        "start": GATE_START,
        "embargo": GATE_EMBARGO,
        "n_folds": {model: item.get("n") for model, item in summary.items()},
        "rmse": rmse,
        "gate": report,
        "predictions_sha256": predictions_digest(prediction_rows),
        "feature_sha256": features,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "cpi_v4_gate_predictions.csv").open("w", encoding="utf-8") as handle:
        handle.write("model,origin,pred,actual\n")
        for row in sorted(prediction_rows, key=lambda r: (r["model"], r["origin"])):
            handle.write(f"{row['model']},{row['origin']},{row['pred']!r},{row['actual']!r}\n")
    (out_dir / "cpi_v4_gate.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# Регрессионный гейт CPI v4: v2/v3 на S_W22 против эталона Task 4",
        "",
        f"Строк {payload['n_rows']}, start={GATE_START}, embargo={GATE_EMBARGO}, допуск {REGRESSION_TOLERANCE}.",
        f"sha256 предсказаний: `{payload['predictions_sha256']}`.",
        "",
        "| модель | ожидалось | получено | Δ | статус |",
        "|---|---:|---:|---:|---|",
    ]
    for row in report["rows"]:
        got = "—" if row["got"] is None else f"{row['got']:.6f}"
        delta = "—" if row["delta"] is None else f"{row['delta']:+.9f}"
        lines.append(f"| {row['model']} | {row['expected']:.6f} | {got} | {delta} | {'ok' if row['ok'] else 'FAIL'} |")
    lines.append("")
    changed = report["features"]["changed"]
    lines.append(f"Признаки v3 против эталона: {'совпали' if not changed else 'изменились: ' + ', '.join(changed)}.")
    lines.append("")
    lines.append(f"**Гейт: {'пройден' if report['passed'] else 'НЕ пройден'}.**")
    (out_dir / "cpi_v4_gate.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return payload


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--gate", action="store_true", help="регрессионный гейт v2/v3 на S_W22")
    parser.add_argument("--out-dir", default="build/cpi-v4", type=Path)
    parser.add_argument("--models", default=None, help="диагностика: подмножество моделей Stage 1 через запятую")
    args = parser.parse_args(argv)
    if not args.gate:
        parser.error("пока доступен только --gate; сетка 9 ячеек — Task 5")
    out_dir = args.out_dir if args.out_dir.is_absolute() else ROOT / args.out_dir
    payload = run_gate(out_dir, args.models.split(",") if args.models else None)
    for row in payload["gate"]["rows"]:
        print(f"  {row['model']}: ожидалось {row['expected']:.6f}, получено {row['got']}, ok={row['ok']}", flush=True)
    print("Гейт:", "пройден" if payload["gate"]["passed"] else "НЕ пройден", flush=True)
    return 0 if payload["gate"]["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
