#!/usr/bin/env python3
"""Живая цель `T_LIVE` 2024-08+ и политика пропусков — P0.3 #480.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-11-01-cb-comp-p03-tlive-target-design.md`.

* **цель** — `T_LIVE[h]` (финальная SA) и `T_LIVE_rt[h]` (as-of SA) по
  правилу `renorm_v1`: группа входит в месяц, только если есть SA и вес > 0;
  иначе выпадает из числителя и знаменателя без импутации;
* **покрытие** — `cov_group` (доля веса 88 групп) и `cov_total` (доля всего
  ИПЦ) по месяцам, отдельно `9218`, `9260`, `803`, `930.АГ`;
* **альтернатива** — `T_LIVE_direct`: X-13 C0 на взвешенном NSA-агрегате без
  ЖКУ (SA агрегата, а не агрегат SA), калибровка на `TARGET_FULL` до 2024-07;
* **NSA-проверка весов** — наш агрегат с `9470` против публикуемого
  `ipc31074/Все товары и услуги` (2022+); измеряет дрейф архивных весов 2024;
* **гейт** — нет разрывов, `cov_total ≥ 0,85`, расхождение описано, повтор
  побайтно тот же; провал — `exit 2`;
* **снимок** — `docs/research/cb-comp-tlive-vintage/<версия>/<M>.json`, append-only.

Только чтение ClickHouse и локальных файлов; в БД не пишет. Запуск —
`make cb-comp-tlive-target`, тесты — `make cb-comp-tlive-target-test`.
"""

import argparse
import csv
import hashlib
import json
import math
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_groups_panel as gp
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_parity as cp

# ---------------------------------------------------------------------------
# Предзарегистрированные константы (спека §3–§7)
# ---------------------------------------------------------------------------

RULE = "renorm_v1"
WEIGHT_YEARS = lb.WEIGHT_YEARS
REPORT_HORIZONS = (1, 2, 3, 6)
COVERAGE_MIN = bu.COVERAGE_MIN
LIVE_START = date(2024, 8, 1)
NSA_CHECK_START = date(2022, 1, 1)
DIRECT_SPEC = "C0"
DECISION_MARGIN = 0.010
SUBPERIODS = {"2019_2021": (date(2019, 1, 1), date(2021, 12, 1)),
              "2023_2024": (date(2023, 1, 1), date(2024, 7, 1))}
GROUPS_OF_INTEREST = ("9218", "9260", "803", "930.АГ")
HEADLINE_SERIES = "ipc31074/Все товары и услуги"
UTILITIES_SERIES = "ipc31074/Жилищные и коммунальные услуги (включая аренду квартир)"

OUT_DIR = ROOT / "build" / "cb-comp-tlive"
VINTAGE_DIR = ROOT / "docs" / "research" / "cb-comp-tlive-vintage"
JSON_COPY = ROOT / "docs" / "research" / "2026-11-01-cb-comp-p03-tlive-target-{version}.json"

EXIT_GATE = 2
EXIT_CONFLICT = 3


def versioned_dir(path: Path, version: str) -> Path:
    """Каталог версии: `@2010` — как есть, `@2009` — с суффиксом."""
    return path if version == lb.DEFAULT_VERSION else path.with_name(f"{path.name}-{version}")


# ---------------------------------------------------------------------------
# Панель и веса
# ---------------------------------------------------------------------------


def load_panel(path: Path) -> tuple:
    """`(sa_full, sa_asof, nsa)`: `{код: {месяц: м/м, %}}` из артефакта Task 1."""
    sa_full, sa_asof, nsa = {}, {}, {}
    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            month = date.fromisoformat(row["month"])
            for column, table in (("sa_full_mm", sa_full), ("sa_asof_mm", sa_asof), ("nsa_mm", nsa)):
                if row[column]:
                    table.setdefault(row["code"], {})[month] = float(row[column])
    return sa_full, sa_asof, nsa


def year_weights_with_total(archive: Path, year: int, codes: list) -> tuple:
    """`({код: вес}, сумма весов всех листьев)` по правилу `bu._year_weights`.

    Повторяет агрегацию листьев (уровень 7) по уровню группы; дополнительно
    возвращает `W_all` — знаменатель `cov_total`, которого нет в `_year_weights`.
    """
    import pandas as pd

    weights = pd.read_excel(bu.weights_path(archive), sheet_name=str(year))
    weights["code"] = weights["код_товара_услуги"].map(bu.normalize_code)
    weights["okato"] = weights["окато_код"].map(bu.normalize_code)
    weights = weights[weights["okato"] == "643"]
    dictionary = pd.read_excel(bu.dictionary_path(archive), sheet_name=str(year))
    dictionary["code"] = dictionary["код_товара_услуги"].map(bu.normalize_code)
    for level in range(1, 8):
        dictionary[f"уровень_{level}"] = dictionary[f"уровень_{level}"].map(bu.normalize_code)
    merged = weights.merge(
        dictionary[["code", "уровень"] + [f"уровень_{i}" for i in range(1, 8)]],
        on="code",
        how="left",
    )
    merged = merged[merged["уровень"] == 7]
    by_level = {level: merged.groupby(f"уровень_{level}")["вес_товара"].sum() for level in range(1, 7)}
    level_map = bu._level_map(dictionary)
    table = {}
    for code in list(codes) + [bu.UTILITIES_CODE]:
        level = level_map.get(code)
        if level is None:
            continue
        values = by_level.get(level)
        if values is not None and code in values.index:
            table[code] = float(values[code])
    return table, float(merged["вес_товара"].sum())


def load_weights(archive: Path, years, codes: list) -> tuple:
    """`({год: {код: вес}}, {год: W_all})`."""
    tables, totals = {}, {}
    for year in years:
        tables[year], totals[year] = year_weights_with_total(archive, year, codes)
    return tables, totals


def weight_year(weights: dict, month: date) -> tuple:
    """`(год весов, фолбэк)` — правило `M.year−1`, иначе ближайший более ранний."""
    year = month.year - 1
    if year in weights:
        return year, False
    available = sorted(weights)
    earlier = [value for value in available if value <= year]
    return (earlier[-1], True) if earlier else (available[0], True)


# ---------------------------------------------------------------------------
# Агрегат и цель
# ---------------------------------------------------------------------------


def rt_series(sa_full: dict, sa_asof: dict) -> dict:
    """As-of SA только по группам с валидной полной серией (`renorm_v1` п. 3).

    `group_sa` считает `rt` независимыми помесячными фитами, поэтому у группы
    со статусом `sa_failed` (9218) в панели есть отдельные as-of значения без
    полной серии. В цель такая группа не входит: иначе корзина `T_LIVE_rt`
    отличалась бы от `T_LIVE` по составу.
    """
    return {code: values for code, values in sa_asof.items() if code in sa_full}


def aggregate_with_coverage(sa: dict, weights: dict, totals: dict, months: list, codes: list,
                            rule: str = RULE) -> tuple:
    """`(mm, stats)` по правилу `rule`: `Σ w·sa / Σ w` по группам с SA и весом.

    `stats[M]` — `n_groups`, `w_included`, `cov_group`, `cov_total`,
    `weight_year`, `weight_fallback`. Импутации нет: пропуск группы выпадает
    из числителя и знаменателя.
    """
    if rule != RULE:
        raise ValueError(f"неизвестное правило пропусков {rule}")
    aggregated, stats = {}, {}
    for month in months:
        year, fallback = weight_year(weights, month)
        weights_month = weights[year]
        numerator = denominator = denominator_all = 0.0
        count = 0
        for code in codes:
            weight = weights_month.get(code, 0.0)
            if weight <= 0:
                continue
            denominator_all += weight
            value = sa.get(code, {}).get(month)
            if value is None:
                continue
            numerator += weight * value
            denominator += weight
            count += 1
        stats[month] = {
            "n_groups": count,
            "w_included": denominator,
            "w_groups": denominator_all,
            "cov_group": denominator / denominator_all if denominator_all else float("nan"),
            "cov_total": denominator / totals[year] if totals.get(year) else float("nan"),
            "weight_year": year,
            "weight_fallback": fallback,
        }
        if denominator:
            aggregated[month] = numerator / denominator
    return aggregated, stats


def targets(mm: dict) -> dict:
    """`{h: {M: T_LIVE[h](M)}}` — та же формула, что `cb_comp_live_bottomup.t_live`."""
    return {horizon: lb.cumulative(mm, horizon) for horizon in cp.ALL_HORIZONS}


def gaps(series: dict) -> list:
    """Внутренние разрывы: пары соседних месяцев, между которыми пропуск."""
    months = sorted(series)
    return [[first.isoformat(), second.isoformat()] for first, second in zip(months, months[1:])
            if (second.year * 12 + second.month) - (first.year * 12 + first.month) != 1]


def direct_targets(nsa: dict, weights: dict, totals: dict, months: list, codes: list,
                   runner_factory=None) -> tuple:
    """`T_LIVE_direct`: X-13 на взвешенном NSA без ЖКУ (SA агрегата, не агрегат SA)."""
    mm, stats = aggregate_with_coverage(nsa, weights, totals, months, codes)
    ordered = sorted(mm)
    if not ordered:
        return {horizon: {} for horizon in cp.ALL_HORIZONS}, {"months": 0}
    levels = [1.0]
    for month in ordered:
        levels.append(levels[-1] * (1 + mm[month] / 100))
    levels = levels[1:]
    factory = runner_factory or gp.spec_factory(DIRECT_SPEC)
    rows = factory(ordered[0])(levels, len(levels) + 1)
    sa = {month: float(row[0]) for month, row in zip(ordered, rows) if not math.isnan(float(row[0]))}
    return targets(sa), {"months": len(ordered), "first": ordered[0].isoformat(),
                         "last": ordered[-1].isoformat(), "spec": DIRECT_SPEC}


# ---------------------------------------------------------------------------
# Оценка против TARGET_FULL
# ---------------------------------------------------------------------------


def evaluate(values: dict, target_full, horizon: int, months: list) -> dict:
    common = [month for month in months
              if month in values and month in target_full.index and not math.isnan(float(target_full.at[month, horizon]))]
    diffs = [values[month] - float(target_full.at[month, horizon]) for month in common]
    if not diffs:
        return {"n": 0, "rmse": float("nan"), "bias": float("nan")}
    return {"n": len(diffs),
            "rmse": math.sqrt(sum(value * value for value in diffs) / len(diffs)),
            "bias": sum(diffs) / len(diffs)}


def compare_to_target_full(truths: dict, target_full, normal: list, months: list) -> dict:
    """RMSE/bias каждого варианта цели на 67 месяцах, normal-54 и подпериодах."""
    blocks = {"test_67": months, "normal_54": [month for month in months if month in set(normal)]}
    for name, (first, last) in SUBPERIODS.items():
        blocks[name] = [month for month in months if first <= month <= last]
    out = {}
    for label, series in truths.items():
        out[label] = {}
        for horizon in REPORT_HORIZONS:
            if horizon not in series:
                continue
            out[label][f"h{horizon}"] = {
                name: evaluate(series.get(horizon, {}), target_full, horizon, window)
                for name, window in blocks.items()
            }
    return out


def discrepancy(alternative: dict, base: dict, months: list) -> dict:
    """`T_LIVE_direct − T_LIVE` на live-окне: RMSE, bias, n — расхождение числом."""
    out = {}
    live = [month for month in months if month >= LIVE_START]
    for horizon in REPORT_HORIZONS:
        left, right = alternative.get(horizon, {}), base.get(horizon, {})
        common = [month for month in live if month in left and month in right]
        diffs = [left[month] - right[month] for month in common]
        out[f"h{horizon}"] = {
            "n": len(diffs),
            "rmse": math.sqrt(sum(value * value for value in diffs) / len(diffs)) if diffs else float("nan"),
            "bias": sum(diffs) / len(diffs) if diffs else float("nan"),
        }
    return out


def decision(comparison: dict) -> dict:
    """Предрегистрированное правило §6: эскалация, если альтернатива ближе на ≥ 0,010."""
    base = comparison["T_LIVE"]["h1"]
    alternative = comparison["T_LIVE_direct"]["h1"]
    margin = base["normal_54"]["rmse"] - alternative["normal_54"]["rmse"]
    stable = all(
        base[window]["rmse"] - alternative[window]["rmse"] > 0
        for window in SUBPERIODS
    )
    escalate = bool(margin >= DECISION_MARGIN and stable)
    return {
        "live_truth": "T_LIVE",
        "alternative": "T_LIVE_direct",
        "margin_normal_54_h1": margin,
        "margin_threshold": DECISION_MARGIN,
        "stable_subperiods": stable,
        "escalate_to_owner": escalate,
        "reason": ("альтернатива ближе к TARGET_FULL на normal-54 h1 на "
                   f"{margin:.4f} п.п. при пороге {DECISION_MARGIN}" if escalate
                   else "порог не пройден: T_LIVE остаётся live-истиной"),
    }


# ---------------------------------------------------------------------------
# Группы и покрытие
# ---------------------------------------------------------------------------


def group_report(sa_full: dict, nsa: dict, weights: dict, codes: list, last: date) -> list:
    """По каждой группе: месяцы с SA/NSA, вес по годам, входит ли в последний месяц."""
    current_year, _fallback = weight_year(weights, last)
    rows = []
    for code in codes:
        months = sorted(sa_full.get(code, {}))
        rows.append({
            "code": code,
            "n_sa": len(months),
            "first_sa": months[0].isoformat() if months else "",
            "last_sa": months[-1].isoformat() if months else "",
            "n_nsa": len(nsa.get(code, {})),
            "in_last_month": last in sa_full.get(code, {}),
            "weight_last": weights[current_year].get(code, 0.0),
            "weight_last_year": current_year,
            "weights": {str(year): weights[year].get(code, 0.0) for year in sorted(weights)},
            "of_interest": code in GROUPS_OF_INTEREST,
        })
    return rows


def coverage_yearly(stats: dict) -> dict:
    """Минимум/среднее покрытия по годам — для отчёта."""
    by_year = {}
    for month, stat in stats.items():
        by_year.setdefault(month.year, []).append(stat)
    out = {}
    for year, values in sorted(by_year.items()):
        out[str(year)] = {
            "months": len(values),
            "cov_group_min": min(value["cov_group"] for value in values),
            "cov_total_min": min(value["cov_total"] for value in values),
            "cov_group_mean": sum(value["cov_group"] for value in values) / len(values),
            "cov_total_mean": sum(value["cov_total"] for value in values) / len(values),
            "n_groups_min": min(value["n_groups"] for value in values),
            "weight_year": sorted({value["weight_year"] for value in values}),
            "weight_fallback_months": sum(1 for value in values if value["weight_fallback"]),
        }
    return out


def coverage_window(stats: dict, first: date) -> dict:
    window = [value for month, value in stats.items() if month >= first]
    if not window:
        return {}
    return {
        "months": len(window),
        "cov_group_min": min(value["cov_group"] for value in window),
        "cov_group_max": max(value["cov_group"] for value in window),
        "cov_total_min": min(value["cov_total"] for value in window),
        "cov_total_max": max(value["cov_total"] for value in window),
        "n_groups_min": min(value["n_groups"] for value in window),
        "n_groups_max": max(value["n_groups"] for value in window),
    }


# ---------------------------------------------------------------------------
# NSA-проверка весов
# ---------------------------------------------------------------------------


def load_published(series: list, first: date) -> dict:
    """`{ряд: {месяц: м/м, %}}` из `v_fedstat_ipc` (только чтение)."""
    from lib.clickhouse import ch_query

    quoted = ", ".join(f"'{name}'" for name in series)
    rows = ch_query(
        "SELECT series, toString(date) AS d, value FROM v_fedstat_ipc "
        f"WHERE series IN ({quoted}) AND date >= '{first.isoformat()}' ORDER BY series, date FORMAT JSON"
    )
    out = {}
    for row in rows:
        month = date.fromisoformat(row["d"][:10]).replace(day=1)
        out.setdefault(row["series"], {})[month] = float(row["value"]) - 100.0
    return out


def nsa_weights_check(nsa: dict, weights: dict, months: list, codes: list, published: dict) -> dict:
    """Наш NSA-агрегат с ЖКУ против публикуемого ИПЦ: дрейф весов и состава."""
    headline = published.get(HEADLINE_SERIES, {})
    utilities = published.get(UTILITIES_SERIES, {})
    by_year = {}
    for month in months:
        if month < NSA_CHECK_START or month not in headline or month not in utilities:
            continue
        year, _fallback = weight_year(weights, month)
        weights_month = weights[year]
        numerator = denominator = 0.0
        for code in codes:
            weight = weights_month.get(code, 0.0)
            value = nsa.get(code, {}).get(month)
            if weight <= 0 or value is None:
                continue
            numerator += weight * value
            denominator += weight
        utility_weight = weights_month.get(bu.UTILITIES_CODE, 0.0)
        if not denominator or not utility_weight:
            continue
        numerator += utility_weight * utilities[month]
        denominator += utility_weight
        ours = numerator / denominator
        by_year.setdefault(str(month.year), []).append(ours - headline[month])
    out = {}
    for year, diffs in sorted(by_year.items()):
        out[year] = {
            "months": len(diffs),
            "bias": sum(diffs) / len(diffs),
            "rmse": math.sqrt(sum(value * value for value in diffs) / len(diffs)),
            "max_abs": max(abs(value) for value in diffs),
        }
    all_diffs = [value for diffs in by_year.values() for value in diffs]
    out["all"] = {
        "months": len(all_diffs),
        "bias": sum(all_diffs) / len(all_diffs) if all_diffs else float("nan"),
        "rmse": math.sqrt(sum(value * value for value in all_diffs) / len(all_diffs)) if all_diffs else float("nan"),
        "max_abs": max((abs(value) for value in all_diffs), default=float("nan")),
    }
    late = [value for year, diffs in by_year.items() for value in diffs if int(year) >= 2025]
    out["late_2025_2026"] = {
        "months": len(late),
        "bias": sum(late) / len(late) if late else float("nan"),
        "rmse": math.sqrt(sum(value * value for value in late) / len(late)) if late else float("nan"),
        "max_abs": max((abs(value) for value in late), default=float("nan")),
        "verdict": ("negligible" if late and abs(sum(late) / len(late)) <= 0.02
                    and math.sqrt(sum(value * value for value in late) / len(late)) <= 0.05
                    else "weight_vintage_degraded"),
    }
    return out


# ---------------------------------------------------------------------------
# Гейт, артефакты, снимок
# ---------------------------------------------------------------------------


def gates(targets_block: dict, stats: dict, discrepancy_block: dict, regression: dict, first_expected: date) -> dict:
    """Флаги гейта §7 спеки."""
    clean = {}
    for horizon in cp.ALL_HORIZONS:
        series = targets_block.get(horizon, {})
        ordering = sorted(series)
        clean[f"h{horizon}"] = bool(ordering) and not gaps(series)
    starts = {}
    for horizon in cp.ALL_HORIZONS:
        ordering = sorted(targets_block.get(horizon, {}))
        starts[f"h{horizon}"] = ordering[0].isoformat() if ordering else ""
    coverage_min = min((value["cov_total"] for value in stats.values()), default=float("nan"))
    described = all(item["n"] > 0 for item in discrepancy_block.values())
    flags = {
        "no_gaps": all(clean.values()),
        "no_gaps_detail": clean,
        "starts": starts,
        "starts_ok": all(starts[f"h{horizon}"] == bu.shift_month(first_expected, horizon - 1).isoformat()
                         for horizon in cp.ALL_HORIZONS),
        "coverage_min": coverage_min,
        "coverage_ok": coverage_min >= COVERAGE_MIN,
        "discrepancy_documented": described,
        "regression_max_abs_diff": regression["max_abs_diff"],
        "regression_ok": regression["ok"],
    }
    flags["ok"] = bool(flags["no_gaps"] and flags["starts_ok"] and flags["coverage_ok"]
                       and flags["discrepancy_documented"] and flags["regression_ok"])
    return flags


def regression_check(base: dict, months: list, codes: list, sa_full: dict,
                     weights: dict) -> dict:
    """Сверка с текущим `t_live()`: расхождение должно быть нулевым."""
    reference = lb.t_live({code: sa_full[code] for code in codes if code in sa_full}, weights, months)
    diffs = {}
    for horizon in cp.ALL_HORIZONS:
        left, right = base.get(horizon, {}), reference.get(horizon, {})
        common = sorted(set(left) & set(right))
        if not common:
            diffs[f"h{horizon}"] = float("nan")
            continue
        diffs[f"h{horizon}"] = max(abs(left[month] - right[month]) for month in common)
    worst = max((value for value in diffs.values() if not math.isnan(value)), default=float("nan"))
    return {"max_abs_diff": worst, "detail": diffs, "ok": bool(not math.isnan(worst) and worst == 0.0)}


def write_target(path: Path, series: dict, stats: dict) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["month", "h1", "h2", "h3", "h4", "h5", "h6"])
        for month in sorted(stats):
            writer.writerow([month.isoformat()] + [repr(series[horizon].get(month, float("nan")))
                                                   for horizon in cp.ALL_HORIZONS])


def write_coverage(path: Path, stats: dict) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["month", "n_groups", "w_included", "cov_group", "cov_total",
                         "weight_year", "weight_fallback"])
        for month in sorted(stats):
            stat = stats[month]
            writer.writerow([month.isoformat(), stat["n_groups"], repr(stat["w_included"]),
                             repr(stat["cov_group"]), repr(stat["cov_total"]),
                             stat["weight_year"], int(stat["weight_fallback"])])


def write_groups(path: Path, rows: list) -> None:
    years = sorted({year for row in rows for year in row["weights"]}, key=int)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["code", "n_sa", "first_sa", "last_sa", "n_nsa", "in_last_month",
                         "weight_last", "weight_last_year"] + [f"w_{year}" for year in years])
        for row in rows:
            writer.writerow([row["code"], row["n_sa"], row["first_sa"], row["last_sa"], row["n_nsa"],
                             int(row["in_last_month"]), repr(row["weight_last"]), row["weight_last_year"]]
                            + [repr(row["weights"][year]) for year in years])


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def git_sha() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True,
                              text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def git_dirty() -> bool:
    try:
        status = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=ROOT,
                                capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return False
    return bool(status.strip())


def snapshot_payload(version: str, month: date, series: dict, rt_series: dict, stats: dict,
                     panel_sha: str, weights_sha: str) -> dict:
    stat = stats.get(month, {})
    return {
        "version": version,
        "month": f"{month.year:04d}-{month.month:02d}",
        "computed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cutoff": "late",
        "rule": RULE,
        "panel_sha256": panel_sha,
        "weights_sha256": weights_sha,
        "t_live": {f"h{horizon}": series[horizon].get(month) for horizon in REPORT_HORIZONS},
        "t_live_rt": {f"h{horizon}": rt_series[horizon].get(month) for horizon in REPORT_HORIZONS},
        "coverage": {
            "n_groups": stat.get("n_groups"),
            "cov_group": stat.get("cov_group"),
            "cov_total": stat.get("cov_total"),
            "weight_year": stat.get("weight_year"),
            "weight_fallback": stat.get("weight_fallback"),
        },
        "note": "наша пересборка цели на дату cutoff, не первая публикация Росстата",
    }


def write_snapshot(path: Path, payload: dict) -> dict:
    """Append-only: повтор с теми же значениями — `skip`, иначе отказ без записи."""
    if path.exists():
        previous = json.loads(path.read_text(encoding="utf-8"))
        same_values = all(previous.get(key) == payload[key] for key in ("t_live", "t_live_rt", "coverage", "version"))
        if same_values:
            return {"status": "skip", "path": str(path), "reason": "значения совпадают"}
        return {"status": "conflict", "path": str(path),
                "previous": {key: previous.get(key) for key in ("panel_sha256", "t_live", "coverage")},
                "current": {key: payload[key] for key in ("panel_sha256", "t_live", "coverage")}}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8")
    return {"status": "written", "path": str(path)}


def jsonable(value):
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key.isoformat() if isinstance(key, date) else key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [jsonable(item) for item in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def run(archive: Path, panel_csv: Path, out_dir: Path, version: str, snapshot: bool = False,
        month: date = None, copy_json: bool = False, nsa_check: bool = True,
        with_direct: bool = True) -> dict:
    archive_data = cp.check_archive(archive)
    target_full = archive_data["target"]
    normal = archive_data["normal"]
    test_months = cp.month_range(cp.TEST_START, cp.TEST_END)

    sa_full, sa_asof, nsa = load_panel(panel_csv)
    lb.check_panel_version(sa_full, version)
    codes = sorted({code for code in nsa} | {code for code in sa_full})
    months = sorted({month for series in sa_full.values() for month in series})
    weights, totals = load_weights(archive, WEIGHT_YEARS, codes)

    mm, stats = aggregate_with_coverage(sa_full, weights, totals, months, codes)
    asof = rt_series(sa_full, sa_asof)
    mm_rt, stats_rt = aggregate_with_coverage(asof, weights, totals, sorted(
        {month for series in asof.values() for month in series}), codes)
    live = targets(mm)
    live_rt = targets(mm_rt)

    direct_block = {"spec": DIRECT_SPEC, "months": 0}
    direct = {horizon: {} for horizon in cp.ALL_HORIZONS}
    if with_direct:
        direct, direct_block = direct_targets(nsa, weights, totals, months, codes)

    truths = {"T_LIVE": live, "T_LIVE_direct": direct, "T_LIVE_rt": live_rt}
    comparison = compare_to_target_full(truths, target_full, normal, test_months)
    gap_block = discrepancy(direct, live, months)
    decision_block = decision(comparison)

    regression = regression_check(live, months, codes, sa_full, weights)
    first_expected = min(month for series in sa_full.values() for month in series)
    gate = gates(live, stats, gap_block, regression, first_expected)

    weights_sha = sha256(bu.weights_path(archive))
    published = load_published([HEADLINE_SERIES, UTILITIES_SERIES], NSA_CHECK_START) if nsa_check else {}
    nsa_block = (nsa_weights_check(nsa, weights, months, codes, published)
                 if nsa_check else {})

    result = {
        "version": version,
        "rule": RULE,
        "git_sha": git_sha(),
        "git_dirty": git_dirty(),
        "panel": str(panel_csv),
        "panel_sha256": sha256(panel_csv),
        "weights_sha256": weights_sha,
        "weights_years": sorted(weights),
        "months": {"first": months[0], "last": months[-1], "n": len(months)},
        "first_target_month": first_expected,
        "coverage": {"by_year": coverage_yearly(stats), "live_window": coverage_window(stats, LIVE_START),
                     "live_window_rt": coverage_window(stats_rt, LIVE_START)},
        "nsa_weights_check": nsa_block,
        "comparison": comparison,
        "discrepancy_live": gap_block,
        "direct": direct_block,
        "decision": decision_block,
        "gates": gate,
        "groups_of_interest": [row for row in group_report(sa_full, nsa, weights, codes, months[-1])
                               if row["of_interest"]],
        "limits": [
            "публикуемого SA ИПЦ без ЖКУ нет: сверка только с реконструкцией",
            "веса 2025–26 — архивные 2024 (P0.4 #479), дрейф измеряется NSA-проверкой",
            "PIT/винтажей нет: as-of только у sa_asof_mm панели",
        ],
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    write_target(out_dir / "target.csv", live, stats)
    write_coverage(out_dir / "coverage.csv", stats)
    write_groups(out_dir / "groups.csv", group_report(sa_full, nsa, weights, codes, months[-1]))
    result["target_sha256"] = sha256(out_dir / "target.csv")
    if copy_json:
        path = Path(str(JSON_COPY).format(version=version))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8")
        result["json_copy"] = str(path)

    if snapshot:
        frozen = month or months[-1]
        if frozen not in stats:
            raise SystemExit(f"снимок: месяца {frozen.isoformat()} нет в панели")
        payload = snapshot_payload(version, frozen, live, live_rt, stats, result["panel_sha256"], weights_sha)
        result["snapshot"] = write_snapshot(VINTAGE_DIR / version / f"{frozen.isoformat()[:7]}.json", payload)
        if result["snapshot"]["status"] == "conflict":
            (out_dir / "result.json").write_text(
                json.dumps(jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return result

    (out_dir / "result.json").write_text(json.dumps(jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                                         encoding="utf-8")
    return result


def summary(result: dict) -> dict:
    return {
        "version": result["version"],
        "months": result["months"],
        "gates": {key: result["gates"][key] for key in
                  ("ok", "no_gaps", "coverage_ok", "coverage_min", "discrepancy_documented", "regression_ok")},
        "coverage_live": result["coverage"]["live_window"],
        "nominal": {horizon: result["comparison"]["T_LIVE"].get(horizon, {}).get("normal_54")
                    for horizon in ("h1", "h3", "h6")},
        "direct": {horizon: result["comparison"]["T_LIVE_direct"].get(horizon, {}).get("normal_54")
                   for horizon in ("h1", "h3", "h6")},
        "discrepancy_live": result["discrepancy_live"],
        "decision": result["decision"],
        "nsa_check": result["nsa_weights_check"].get("late_2025_2026", {}),
        "target_sha256": result["target_sha256"],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None)
    parser.add_argument("--panel", default=None, help="panel.csv из make cb-comp-groups-panel; по умолчанию — панель версии")
    parser.add_argument("--out", default=None, help="по умолчанию build/cb-comp-tlive[-<версия>]")
    parser.add_argument("--version", choices=lb.VERSIONS, default=lb.DEFAULT_VERSION)
    parser.add_argument("--copy-json", action="store_true", help="копия result.json в docs/research/")
    parser.add_argument("--snapshot", action="store_true", help="append-only снимок цели в docs/research/cb-comp-tlive-vintage/")
    parser.add_argument("--month", default=None, help="месяц снимка YYYY-MM (по умолчанию последний месяц панели)")
    parser.add_argument("--skip-nsa-check", action="store_true", help="без чтения ClickHouse (отладка)")
    parser.add_argument("--skip-direct", action="store_true", help="без X-13 альтернативы (отладка)")
    args = parser.parse_args(argv)

    panel = Path(args.panel) if args.panel else lb.default_panel(args.version)
    out_dir = Path(args.out) if args.out else versioned_dir(OUT_DIR, args.version)
    archive = cp.resolve_archive(args.archive)
    month = date.fromisoformat(f"{args.month}-01") if args.month else None
    result = run(archive, panel, out_dir, args.version, args.snapshot, month, args.copy_json,
                 not args.skip_nsa_check, not args.skip_direct)
    print(json.dumps(jsonable(summary(result)), ensure_ascii=False, indent=2))
    if result.get("snapshot", {}).get("status") == "conflict":
        return EXIT_CONFLICT
    return 0 if result["gates"]["ok"] else EXIT_GATE


if __name__ == "__main__":
    sys.exit(main())
