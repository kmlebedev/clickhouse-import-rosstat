#!/usr/bin/env python3
"""`core_nnls` и модели v4c/v4d для T_CORE (#396 Task 3, H3).

Пункт `key-rate/phase-4-cpi-v4-weekly-aggregate-core`, роль ML. Спека
`docs/superpowers/specs/2026-10-28-cpi-v4-weekly-aggregate-core-design.md`
§5 и поправка §5.2 (корзины, as-of правило, состав моделей).

* 29 товарных групп панели #364 (после предрегистрированных исключений
  `weekly_core_components.EXCLUSIONS`) сводятся в 14 корзин (`BUCKETS`);
  значение корзины месяца — среднее `mm_pp`, взвешенное `weight_used`.
* `core_nnls(O)` — NNLS (без свободного члена и без нормировки) по месяцам
  `m ≤ O−1` с известным `Базовый ИПЦ`, применённый к корзинам месяца `O`.
  Не зависит от фолда, поэтому считается один раз на месяц и безопасен для
  train-строк (веса строки `O'` не видят ничего позже `O'−1`).
* `v4c_core_nnls` — CatBoost на признаках v4a + `core_nnls`; `v4d_core_direct` —
  ElasticNetCV (B3) на признаках v4a без `core_nnls` и без `month`.

Панель читается read-only из CSV (`weekly_core_components`); в ClickHouse
скрипт при расчёте признака не обращается. Диагностический запуск —
`make cpi-core-nnls`, тесты — `make cpi-core-nnls-test`.
"""

import argparse
import dataclasses
import json
import math
import sys
import warnings
from collections import defaultdict
from datetime import date
from pathlib import Path

import numpy as np
from scipy.optimize import nnls
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import ElasticNetCV
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from models.cpi import cpi_monthly as cm
from models.cpi import cpi_weekly_aggregate as agg
import cpi_v4_grid as grid
import weekly_core_components as wcc

MIN_TRAIN_MONTHS = 24
MIN_BUCKETS = 8  # меньше — это не core-прокси (панель до 2021 даёт 2–4 корзины)
FEATURE = "core_nnls"

# Маппинг группа панели → корзина зафиксирован в спеке §5.2 до результатов.
BUCKETS = {
    "Мясопродукты": "meat",
    "Молоко и молочная продукция": "dairy_fats",
    "Сыр": "dairy_fats",
    "Масло и жиры": "dairy_fats",
    "Хлеб и хлебобулочные изделия": "bakery_grain",
    "Мука": "bakery_grain",
    "Макаронные и крупяные изделия": "bakery_grain",
    "Рыбопродукты": "fish",
    "Кондитерские изделия": "sweets_tea",
    "Чай, кофе, какао": "sweets_tea",
    "Консервы овощные": "canned_spices",
    "Консервы фруктово-ягодные": "canned_spices",
    "Соль, соус, специи, концентраты": "canned_spices",
    "Общественное питание": "catering",
    "Одежда и белье": "apparel",
    "Трикотажные изделия": "apparel",
    "Чулочно-носочные изделия": "apparel",
    "Обувь кожаная, текстильная и комбинированная": "footwear",
    "Моющие и чистящие средства": "household",
    "Бумажно-беловые товары": "household",
    "Медицинские товары": "health_beauty",
    "Парфюмерно-косметические товары": "health_beauty",
    "Электротовары и другие бытовые приборы": "appliances",
    "Телерадиотовары": "appliances",
    "Строительные материалы": "misc_goods",
    "Галантерея": "misc_goods",
    "Спички": "misc_goods",
    "Товары для животных": "misc_goods",
    "Бытовые услуги": "services",
}
BUCKET_NAMES = tuple(dict.fromkeys(BUCKETS.values()))


def bucket_of(group_name: str) -> str:
    """Корзина группы панели; неизвестная группа — ошибка, а не молчаливый пропуск."""
    try:
        return BUCKETS[group_name]
    except KeyError:
        raise ValueError(f"группа панели {group_name!r} не входит в маппинг корзин (спека §5.2)") from None


def _month_start(key: str) -> date:
    return date.fromisoformat(f"{key}-01")


def bucket_series(rows: list) -> dict:
    """`{месяц: {корзина: взвешенное среднее mm_pp}}` из usable-строк панели."""
    sums = defaultdict(lambda: defaultdict(lambda: [0.0, 0.0]))
    for row in rows:
        acc = sums[_month_start(row.month)][bucket_of(row.group_name)]
        acc[0] += row.weight * row.mm_pp
        acc[1] += row.weight
    return {
        month: {name: acc[0] / acc[1] for name, acc in sorted(buckets.items()) if acc[1] > 0}
        for month, buckets in sorted(sums.items())
    }


def fit_nnls(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Неотрицательные веса по МНК без свободного члена и без нормировки суммы."""
    weights, _ = nnls(np.asarray(x, dtype=float), np.asarray(y, dtype=float))
    return weights


def _prev_month(month: date) -> date:
    return date(month.year - 1, 12, 1) if month.month == 1 else date(month.year, month.month - 1, 1)


def nnls_fit_asof(buckets_by_month: dict, core_mm: dict, origin: date) -> dict | None:
    """Колонки и веса NNLS на данных `m ≤ O−1` (спека §5.2); `None`, если истории мало."""
    current = buckets_by_month.get(origin)
    if not current:
        return None
    train = [
        month
        for month in sorted(buckets_by_month)
        if month <= _prev_month(origin) and month in core_mm and math.isfinite(core_mm[month])
    ]
    columns = [
        name
        for name in BUCKET_NAMES
        if name in current and sum(name in buckets_by_month[month] for month in train) >= MIN_TRAIN_MONTHS
    ]
    if len(columns) < MIN_BUCKETS:
        return None
    complete = [month for month in train if all(name in buckets_by_month[month] for name in columns)]
    if len(complete) < MIN_TRAIN_MONTHS:
        return None
    x = np.asarray([[buckets_by_month[month][name] for name in columns] for month in complete], dtype=float)
    y = np.asarray([core_mm[month] for month in complete], dtype=float)
    return {"columns": columns, "weights": fit_nnls(x, y), "n_train": len(complete)}


def core_nnls_asof(buckets_by_month: dict, core_mm: dict, origin: date) -> float:
    """`core_nnls(O)`; NaN, если обучающей истории меньше `MIN_TRAIN_MONTHS` месяцев."""
    fit = nnls_fit_asof(buckets_by_month, core_mm, origin)
    if fit is None:
        return float("nan")
    current = buckets_by_month[origin]
    return float(sum(weight * current[name] for name, weight in zip(fit["columns"], fit["weights"])))


def attach_core_nnls(rows: list, buckets_by_month: dict, core_mm: dict) -> list:
    """Новые `Row` с признаком `core_nnls`; исходные строки не меняются."""
    return [
        dataclasses.replace(
            row, features={**row.features, FEATURE: core_nnls_asof(buckets_by_month, core_mm, row.month)}
        )
        for row in rows
    ]


def feature_names_v4c(sample: str) -> list:
    """Признаки v4a + `core_nnls` перед `month`."""
    names = agg.feature_names_v4(sample, with_diffusion=False)
    return names[:-1] + [FEATURE, names[-1]]


def feature_names_v4d(sample: str) -> list:
    """Признаки v4a (лаги core, ожидания, `wkagg_*`), без `core_nnls`."""
    return agg.feature_names_v4(sample, with_diffusion=False)


def make_v4c(names: list):
    """CatBoost на `T_CORE` с явным списком признаков (NaN в `core_nnls` допустим)."""
    return lambda train, row: grid.fit_catboost_names(train, row, cm.TARGET_ALL, names)


def make_v4d(names: list):
    """ElasticNetCV (конфигурация B3), импутация и масштаб — только по train; без `month`."""
    columns = [name for name in names if name != "month"]

    def matrix(rows: list) -> np.ndarray:
        return np.asarray([[float(row.features[name]) for name in columns] for row in rows], dtype=float)

    def fit_predict(train: list, row) -> float:
        x_train = matrix(train)
        if x_train.shape[0] < cm.ENET_MIN_TRAIN:
            raise cm.DegenerateWindowError(
                f"v4d (ElasticNetCV): окно {x_train.shape[0]} короче {cm.ENET_MIN_TRAIN}"
            )
        medians = cm.enet_medians(x_train)
        model = Pipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "enet",
                    ElasticNetCV(
                        alphas=cm.ENET_ALPHAS,
                        l1_ratio=list(cm.ENET_L1_RATIOS),
                        cv=TimeSeriesSplit(cm.ENET_CV_SPLITS),
                        random_state=cm.SEED,
                    ),
                ),
            ]
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ConvergenceWarning)
            model.fit(
                np.where(np.isnan(x_train), medians, x_train),
                np.asarray([float(item.y[cm.TARGET_ALL]) for item in train], dtype=float),
            )
        x_test = matrix([row])
        return float(model.predict(np.where(np.isnan(x_test), medians, x_test))[0])

    return fit_predict


# ---------------------------------------------------------------------------
# Диагностика на данных (только чтение CSV панели и ClickHouse)
# ---------------------------------------------------------------------------


def diagnostics(buckets_by_month: dict, core_mm: dict) -> dict:
    """Покрытие корзин по годам, первый месяц с `core_nnls`, веса на последнем origin."""
    by_year = defaultdict(lambda: defaultdict(int))
    for month, vector in buckets_by_month.items():
        for name in vector:
            by_year[month.year][name] += 1
    origins = sorted(buckets_by_month)
    defined = [m for m in origins if not math.isnan(core_nnls_asof(buckets_by_month, core_mm, m))]
    last = nnls_fit_asof(buckets_by_month, core_mm, origins[-1])
    return {
        "months": len(origins),
        "first": origins[0].isoformat(),
        "last": origins[-1].isoformat(),
        "core_months": len(core_mm),
        "first_core_nnls": defined[0].isoformat() if defined else None,
        "defined_origins": len(defined),
        "bucket_months_by_year": {str(y): dict(sorted(v.items())) for y, v in sorted(by_year.items())},
        "last_weights": (
            {
                "origin": origins[-1].isoformat(),
                "n_train": last["n_train"],
                "weights": {n: round(float(w), 6) for n, w in zip(last["columns"], last["weights"])},
                "weight_sum": round(float(last["weights"].sum()), 6),
            }
            if last
            else None
        ),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=ROOT / "build" / "cpi-v4" / "core_nnls_diagnostics.json")
    args = parser.parse_args(argv)
    import cpi_v4_leaderboard as lb

    rows = wcc.usable_rows(wcc.read_panel(wcc.PANEL_PATH))
    buckets = bucket_series(rows)
    core = lb.load_target_series()["T_CORE"]
    report = diagnostics(buckets, core)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
