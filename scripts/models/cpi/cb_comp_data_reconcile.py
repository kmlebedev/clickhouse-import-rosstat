#!/usr/bin/env python3
"""Сверка данных CPI с датасетом CB_COMP (#401 W3b, протокол — спека §4 «Протокол W3b»).

Сравнивает наши ряды с данными архива Кошкина–Крылова (`TARGET_FULL`, SA-история 50 кодов,
ряд КУ): таргет «SA ИПЦ без КУ», агрегаты и core, КУ и подразумеваемый вес, калибровка веса `w`.
Архив скачивается отдельно (`docs/research/2026-10-18-cb-comp-archive.md` §2) и в репозиторий не
входит. Скрипт только читает ClickHouse и архив. Запуск — `make cb-comp-data-reconcile`, тесты —
`make cb-comp-data-reconcile-test`.
"""

import argparse
import json
import math
import statistics
import sys
from datetime import date
from pathlib import Path
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_sa_reconciliation as rec
import cpi_two_stage_folds as tf
import cpi_two_stage_targets as tt

TEST_FIRST, TEST_LAST = date(2019, 1, 1), date(2024, 7, 1)
TRAIN_FIRST, TRAIN_LAST = date(2010, 3, 1), date(2018, 12, 1)
OUTLIERS = frozenset([date(2020, 4, 1)] + [date(2022, m, 1) for m in range(1, 13)])
HISTORY_FIRST = date(2016, 1, 1)
RMSE_ALIGNED_MAX = 0.10
CORR_ALIGNED_MIN = 0.95
WEIGHT_GRID = tuple(round(0.05 + 0.01 * k, 2) for k in range(11))
WEIGHT_BASE = 0.10
WEIGHT_MIN_STEP = 0.01
WEIGHT_MIN_GAIN = 0.10  # улучшение RMSE на eval-окне не менее 10 %
IMPLIED_MIN_GAP = 1.0  # п.п.: |u − x| для устойчивого знаменателя подразумеваемого веса
KU_THRESHOLD = 0.05  # п.п.
HISTORY_CODES = {"all_ex_ku": "1 - 9470", "all": "1", "food": "6", "nonfood": "7", "services": "9000", "core": "3"}


def _months(first: date, last: date) -> list:
    out, current = [], first
    while current <= last:
        out.append(current)
        current = date(current.year + current.month // 12, current.month % 12 + 1, 1)
    return out


def window_months(name: str) -> list:
    """Окна сверки: `test` (67 мес. 2019-01…2024-07), `normal` (54, без 2020-04 и 2022), `train` (2010-03…2018-12)."""
    if name == "test":
        return _months(TEST_FIRST, TEST_LAST)
    if name == "normal":
        return [m for m in _months(TEST_FIRST, TEST_LAST) if m not in OUTLIERS]
    if name == "train":
        return _months(TRAIN_FIRST, TRAIN_LAST)
    if name == "history":
        return _months(HISTORY_FIRST, TEST_LAST)
    raise ValueError(f"неизвестное окно {name!r}")


def levels_to_mm(levels: dict) -> dict:
    """SA-уровни (100 = предыдущий месяц) → м/м, п.п."""
    return {month: value - 100.0 for month, value in levels.items()}


def align_cumulative(mm: dict, horizon: int) -> dict:
    """`TARGET_FULL[h][M]` — рост за `M−h+1 … M` (M — конец окна): значение по ряду м/м для каждого месяца `M` с полным окном."""
    out = {}
    for month in sorted(mm):
        value = tf.cumulative(mm, _shift(month, -(horizon - 1)), horizon)
        if value is not None:
            out[month] = value
    return out


def _shift(month: date, k: int) -> date:
    index = month.year * 12 + (month.month - 1) + k
    return date(index // 12, index % 12 + 1, 1)


def compare_on_windows(ours: dict, theirs: dict, names: tuple) -> dict:
    """Метрики `ours − theirs` (RMSE, MAE, bias, корр., наклон) по окнам; окно без ≥ 3 общих точек пропускается."""
    out = {}
    for name in names:
        months = window_months(name)
        common = [m for m in months if m in ours and m in theirs and math.isfinite(ours[m]) and math.isfinite(theirs[m])]
        if len(common) < 3:
            continue
        sub_o, sub_t = {m: ours[m] for m in common}, {m: theirs[m] for m in common}
        out[name] = rec.metrics(sub_o, sub_t, min(common), max(common))
    return out


def implied_weights(pi: dict, x: dict, u: dict, min_gap: float = IMPLIED_MIN_GAP) -> dict:
    """`w_t = (π_t − x_t)/(u_t − x_t)` из тождества `π = (1−w)·x + w·u`; месяцы с малым `|u − x|` отбрасываются."""
    out = {}
    for month in sorted(set(pi) & set(x) & set(u)):
        gap = u[month] - x[month]
        if abs(gap) >= min_gap:
            out[month] = (pi[month] - x[month]) / gap
    return out


def summarize_weights(weights: dict) -> dict:
    values = sorted(weights.values())
    if not values:
        return {"n": 0}
    quantile = lambda q: values[min(len(values) - 1, int(q * len(values)))]
    return {"n": len(values), "median": statistics.median(values), "min": values[0], "max": values[-1], "q25": quantile(0.25), "q75": quantile(0.75)}


def compare_utilities(ours: dict, theirs: dict, threshold: float = KU_THRESHOLD) -> dict:
    """Наш КУ против их КУ (NSA м/м): разность, корреляция, месяцы с `|diff| > threshold`."""
    common = sorted(set(ours) & set(theirs))
    if not common:
        return {"n": 0}
    diffs = [ours[m] - theirs[m] for m in common]
    a, b = [ours[m] for m in common], [theirs[m] for m in common]
    corr = float("nan")
    if len(common) > 2 and statistics.pstdev(a) > 0 and statistics.pstdev(b) > 0:
        mean_a, mean_b = sum(a) / len(a), sum(b) / len(b)
        corr = sum((p - mean_a) * (q - mean_b) for p, q in zip(a, b)) / math.sqrt(
            sum((p - mean_a) ** 2 for p in a) * sum((q - mean_b) ** 2 for q in b)
        )
    return {
        "n": len(common),
        "max_abs": max(abs(d) for d in diffs),
        "rmse": math.sqrt(sum(d * d for d in diffs) / len(diffs)),
        "corr": corr,
        "flagged": [m.isoformat() for m, d in zip(common, diffs) if abs(d) > threshold],
    }


def history_vs_target(levels: dict, target) -> dict:
    """Код `1 - 9470` истории (`level − 100`) против `TARGET_FULL[1]`: должны совпадать (внутренняя согласованность архива)."""
    diffs = []
    for month, level in levels.items():
        if month in target.index and not math.isnan(float(target.at[month, 1])):
            diffs.append(abs((level - 100.0) - float(target.at[month, 1])))
    return {"n": len(diffs), "max_abs": max(diffs) if diffs else float("nan")}


def history_from_frame(frame) -> dict:
    """`{код: {месяц: SA-уровень}}` из листа `cpi_sa_target_history.xlsx` (колонки `Код`, `Наименование`, месяцы)."""
    import pandas as pd

    month_columns = [c for c in frame.columns if isinstance(c, (pd.Timestamp, date)) or hasattr(c, "year")]
    out = {}
    for _, row in frame.iterrows():
        code = str(row["Код"]).strip()
        out[code] = {date(c.year, c.month, 1): float(row[c]) for c in month_columns if not pd.isna(row[c])}
    return out


def utilities_from_frame(frame) -> tuple:
    """`(SA м/м, NSA м/м)` КУ по месяцам «факт» из `Коммунальные услуги прогноз MoM_sa.xlsx`."""
    sa, nsa = {}, {}
    for _, row in frame.iterrows():
        if str(row["факт_прогноз"]).strip() != "факт":
            continue
        month = date(row["дата"].year, row["дата"].month, 1)
        sa[month], nsa[month] = float(row["Россия"]), float(row["Россия не SA"])
    return sa, nsa


def choose_weight(table: dict, base: float = WEIGHT_BASE) -> dict:
    """Калибровка по fit-окну; смена `w` — только при улучшении eval-RMSE ≥ 10 % и сдвиге ≥ 0,01 (спека §4 W3b п. 4)."""
    calibrated = min(sorted(table), key=lambda w: table[w]["fit"])
    gain_ok = table[calibrated]["eval"] <= (1 - WEIGHT_MIN_GAIN) * table[base]["eval"]
    far_enough = abs(calibrated - base) >= WEIGHT_MIN_STEP - 1e-9
    changed = bool(gain_ok and far_enough)
    return {"calibrated": calibrated, "chosen": calibrated if changed else base, "changed": changed, "base": base}


def decide_alignment(metrics: dict) -> dict:
    """`aligned`: RMSE `T_SAX` против `TARGET_FULL[1]` ≤ 0,10 п.п. и корреляция ≥ 0,95 на 2019-01…2024-07."""
    aligned = metrics["rmse"] <= RMSE_ALIGNED_MAX and metrics["corr"] >= CORR_ALIGNED_MIN
    return {"aligned": bool(aligned), "rmse_max": RMSE_ALIGNED_MAX, "corr_min": CORR_ALIGNED_MIN}


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def _rmse(ours: dict, theirs: dict, months: list) -> float:
    common = [m for m in months if m in ours and m in theirs]
    return math.sqrt(sum((ours[m] - theirs[m]) ** 2 for m in common) / len(common))


def run(archive: Optional[str] = None, runner: Optional[Callable] = None) -> dict:
    import pandas as pd
    from models.cb_comp import cb_comp_parity as cbp
    from models.cb_comp import cb_comp_proxy as proxy

    root = cbp.resolve_archive(archive)
    base = cbp.check_archive(root)  # регрессионная база: исключение, если архив изменился
    target = base["target"]
    runner = runner or rec.make_r_runner(tt.FIRST_MONTH)
    components = root / "Models" / "CpiComponents"
    history = history_from_frame(
        pd.read_excel(components / "StfCpi_External_data" / "databases" / "StfCpiR_db" / "raw_data_store" / "cpi_sa_target_history.xlsx")
    )
    ku_sa, ku_nsa = utilities_from_frame(pd.read_excel(components / "data" / "Коммунальные услуги прогноз MoM_sa.xlsx"))

    nsa, util = rec.load_nsa(), rec.load_utilities()
    cbr_sa, cbr_core = rec.load_cbr_sa(), rec.load_cbr_core()
    theirs_h1 = {m: float(v) for m, v in target[1].items() if not math.isnan(float(v))}

    # 2. Таргет: T_SAX (окончательный; винтаж на 2024-08), прокси T_SA, SA ЦБ.
    sax_final = tt.sax_final(nsa, util, WEIGHT_BASE, runner)
    sax_2024 = tt.sax_vintage(nsa, util, WEIGHT_BASE, date(2024, 8, 1), runner)
    proxy_sa = dict(proxy.proxy_target(cbr_sa, WEIGHT_BASE, proxy.DEFAULT_UTILITIES_GROWTH))
    windows = ("test", "normal", "train", "history")
    result = {
        "regression_base": {
            "rmse_cb": {str(h): v for h, v in base["rmse_cb"].items()},
            "target_max_diff": base["target_max_diff"],
            "max_source_diff": base["max_source_diff"],
        },
        "target_h1": {
            "tsax_final": compare_on_windows(sax_final, theirs_h1, windows),
            "tsax_vintage_2024_08": compare_on_windows(sax_2024, theirs_h1, windows),
            "proxy_t_sa": compare_on_windows(proxy_sa, theirs_h1, windows),
            "cbr_sa_all_vs_theirs": compare_on_windows(cbr_sa, levels_to_mm(history["1"]), ("history", "test")),
        },
        "history_vs_target_full": history_vs_target(history["1 - 9470"], target),
        "target_cumulative": {},
    }
    for horizon in (3, 6):
        theirs_h = {m: float(v) for m, v in target[horizon].items() if not math.isnan(float(v))}
        result["target_cumulative"][f"h{horizon}"] = compare_on_windows(align_cumulative(sax_final, horizon), theirs_h, ("test", "normal"))

    # 3. Агрегаты и core: история CB_COMP (2016-01…2024-07) против SA ЦБ и нашего X-13 по компонентам.
    aggregates = {}
    cbr_pairs = {"all": cbr_sa, "core": cbr_core}
    for key, ref in cbr_pairs.items():
        aggregates[f"{key}_theirs_vs_cbr"] = compare_on_windows(levels_to_mm(history[HISTORY_CODES[key]]), ref, ("history",))
    for key, name in (("food", "cpi_food"), ("nonfood", "cpi_nonfood"), ("services", "cpi_services")):
        component = _component_nsa(name)
        months = [m for m in sorted(component) if m >= tt.FIRST_MONTH]
        ours = rec.x13_series(months, {m: component[m] for m in months}, rec.NO_RT if hasattr(rec, "NO_RT") else 10**6, runner)["full"]
        aggregates[f"{key}_ours_x13_vs_theirs"] = compare_on_windows({m: v for m, v in ours.items() if v == v}, levels_to_mm(history[HISTORY_CODES[key]]), ("history",))
    result["aggregates"] = aggregates

    # 4. КУ и вес.
    result["utilities"] = {
        "nsa_theirs_vs_ours": compare_utilities(util, ku_nsa, KU_THRESHOLD),
        "sa_theirs_constant_months": sum(1 for v in ku_sa.values() if abs(v - 0.327374) < 1e-5),
        "sa_theirs_n": len(ku_sa),
    }
    pi, x = levels_to_mm(history["1"]), levels_to_mm(history["1 - 9470"])
    implied = implied_weights(pi, x, ku_sa, IMPLIED_MIN_GAP)
    result["implied_weight"] = {**summarize_weights(implied), "months": [m.isoformat() for m in sorted(implied)]}
    table = {}
    for weight in WEIGHT_GRID:
        series = tt.sax_final(nsa, util, weight, runner)
        table[weight] = {"fit": _rmse(series, theirs_h1, window_months("train")), "eval": _rmse(series, theirs_h1, window_months("test"))}
    result["weight_calibration"] = {"table": {f"{w:.2f}": v for w, v in table.items()}, **choose_weight(table, WEIGHT_BASE)}

    # 5. Решение.
    result["alignment"] = decide_alignment(result["target_h1"]["tsax_final"]["test"])
    return result


def _component_nsa(name: str) -> dict:
    from models.cpi import cpi_monthly as cm

    targets = cm.load_targets()
    return {cm.month_of(d): float(v) for d, v in targets[name]}


def _jsonable(value):
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        return None if not math.isfinite(value) else round(value, 6)
    return value


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None, help="каталог BusinessSurveyCBRCpiForecastingSourceCode")
    parser.add_argument("--out", type=Path, default=ROOT / "build" / "cpi-two-stage" / "cb_comp_reconcile.json")
    args = parser.parse_args(argv)
    result = run(args.archive)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(_jsonable({"alignment": result["alignment"], "target_h1_test": result["target_h1"]["tsax_final"].get("test"), "weight": {k: v for k, v in result["weight_calibration"].items() if k != "table"}}), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
