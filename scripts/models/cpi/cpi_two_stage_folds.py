#!/usr/bin/env python3
"""Таргеты, доступность и фолды two-stage CPI (#401 W3).

Спека `docs/superpowers/specs/2026-10-29-cpi-two-stage-weekly-sa-design.md` §3 и §5.

Строка с месяцем `O` — месяц `M0` (cm-конвенция строк `cpi_monthly`; в сетке #396 это origin `O−1`):
признаки на cutoff `P_{O−1}` (конвенция `publication`), таргет — накопленный рост `O … O+h−1`
(`cumulative`). Доступность месяца: Росстат — день релиза (иначе 9-е `M+1`), ЦБ SA — 18-е `M+1`;
`T_SAX`/`T_NSA` — по Росстату (допущение A1: КУ с релизом ИПЦ), `T_CORE` — по ЦБ.
Purge: строка `O'` в обучении origin'а `O` только если последний месяц её окна доступен к cutoff `O`
(embargo 0). Скрипт — библиотека (без записи в БД); тесты — `make cpi-two-stage-folds-test`.
"""

import hashlib
import json
import math
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Callable, NamedTuple, Optional

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_sa_reconciliation as rec
import cpi_two_stage_targets as tt
import weekly_sa_asof as wa

TARGETS = ("T_SAX", "T_CORE", "T_NSA")
HORIZONS = (1, 3, 6)
CBR_PUBLICATION_DAY = 18  # SA/core ЦБ: 18-е число `M+1` (календарь `publication_calendar`)
SEED = 20261018

# Параметры фолдов этапов (спека §4 W4/W5).
STAGE1_FIRST_TRAIN = date(2016, 1, 1)
STAGE1_MIN_TRAIN = 24
STAGE2_MIN_TRAIN = 60


class Fold(NamedTuple):
    origin: date
    cutoff: date
    train: list


def _shift(month: date, k: int) -> date:
    index = month.year * 12 + (month.month - 1) + k
    return date(index // 12, index % 12 + 1, 1)


# ---------------------------------------------------------------------------
# Доступность
# ---------------------------------------------------------------------------


def rosstat_available(month: date, release_dates: Optional[dict]) -> date:
    """Дата публикации ИПЦ за `month`: из таблицы релизов, иначе 9-е число `M+1`."""
    first = month.replace(day=1)
    if release_dates and first in release_dates:
        return release_dates[first]
    return _shift(first, 1).replace(day=wa.PUBLICATION_DAY)


def cbr_available(month: date) -> date:
    """Дата публикации SA/core ЦБ за `month`: 18-е число `M+1` (правило; точные даты не хранятся)."""
    return _shift(month.replace(day=1), 1).replace(day=CBR_PUBLICATION_DAY)


def target_available(target: str, month: date, release_dates: Optional[dict]) -> date:
    if target in ("T_SAX", "T_NSA"):
        return rosstat_available(month, release_dates)
    if target == "T_CORE":
        return cbr_available(month)
    raise ValueError(f"неизвестный таргет {target!r}: ожидается одно из {TARGETS}")


def cutoff(origin: date, release_dates: Optional[dict]) -> date:
    """Cutoff строки `O` — день релиза ИПЦ `O−1` (конвенция `publication`, W0)."""
    return wa.publication_cutoff(origin, release_dates)


# ---------------------------------------------------------------------------
# Окна, purge, фолды
# ---------------------------------------------------------------------------


def window(origin: date, horizon: int) -> list:
    """Месяцы таргет-окна строки `O`: `O … O+h−1`."""
    if horizon < 1:
        raise ValueError(f"горизонт должен быть ≥ 1: {horizon}")
    return [_shift(origin.replace(day=1), offset) for offset in range(horizon)]


def _window_available(target: str, origin: date, horizon: int, release_dates: Optional[dict]) -> date:
    return max(target_available(target, month, release_dates) for month in window(origin, horizon))


def training_origins(target: str, horizon: int, test_origin: date, candidates: list, release_dates: Optional[dict]) -> list:
    """Origin'ы-кандидаты, чьё таргет-окно целиком доступно к cutoff тестового origin'а (purge, embargo 0)."""
    limit = cutoff(test_origin, release_dates)
    return sorted(
        origin
        for origin in candidates
        if origin < test_origin and _window_available(target, origin, horizon, release_dates) <= limit
    )


def make_folds(
    target: str,
    horizon: int,
    test_origins: list,
    candidates: list,
    first_train: date,
    min_train: int,
    release_dates: Optional[dict],
) -> list:
    """Walk-forward фолды: расширяющееся окно, один origin в месяц; origin'ы с train < `min_train` пропускаются."""
    folds = []
    for origin in sorted(test_origins):
        train = [o for o in training_origins(target, horizon, origin, candidates, release_dates) if o >= first_train]
        if len(train) >= min_train:
            folds.append(Fold(origin, cutoff(origin, release_dates), train))
    return folds


def assert_no_lookahead(target: str, horizon: int, fold: Fold, release_dates: Optional[dict]) -> None:
    """Структурная проверка фолда: обучение строго до теста, окна целиком доступны к cutoff."""
    for origin in fold.train:
        if origin >= fold.origin:
            raise AssertionError(f"обучающий origin {origin} не раньше тестового {fold.origin}")
        available = _window_available(target, origin, horizon, release_dates)
        if available > fold.cutoff:
            raise AssertionError(
                f"{target} h{horizon}: окно origin'а {origin} доступно {available}, позже cutoff {fold.cutoff} теста {fold.origin}"
            )


def common_folds(origins_by_model: dict, models=None) -> list:
    """Origin'ы, на которых есть результат у всех выбранных моделей (по возрастанию)."""
    names = list(origins_by_model) if models is None else list(models)
    if not names:
        return []
    common = set(origins_by_model[names[0]])
    for name in names[1:]:
        common &= set(origins_by_model[name])
    return sorted(common)


# ---------------------------------------------------------------------------
# Таргеты
# ---------------------------------------------------------------------------


def cumulative(series: dict, origin: date, horizon: int) -> Optional[float]:
    """Накопленный рост `O … O+h−1` (сложный процент, п.п.); `None` при дыре — как `cpi_v4_leaderboard.cumulative_pp`."""
    product = 1.0
    for month in window(origin, horizon):
        value = series.get(month)
        if value is None:
            return None
        product *= 1.0 + float(value) / 100.0
    return (product - 1.0) * 100.0


@dataclass
class TargetData:
    """`T_NSA` (м/м Росстата), `T_CORE` (SA ЦБ, последняя редакция), `T_SAX` (окончательный ряд и винтажи origin'ов)."""

    nsa: dict
    core: dict
    sax_final: dict
    sax_vintages: dict
    skipped_vintages: list = field(default_factory=list)

    def _latest(self, target: str) -> dict:
        if target == "T_NSA":
            return self.nsa
        if target == "T_CORE":
            return self.core
        if target == "T_SAX":
            return self.sax_final
        raise ValueError(f"неизвестный таргет {target!r}: ожидается одно из {TARGETS}")

    def train_value(self, target: str, test_origin: date, train_origin: date, horizon: int) -> Optional[float]:
        """Цель обучающей строки: `T_SAX` — из винтажной истории тестового origin'а, остальные — последняя редакция."""
        if target == "T_SAX":
            vintage = self.sax_vintages.get(test_origin)
            if vintage is None:
                return None
            return cumulative(vintage, train_origin, horizon)
        return cumulative(self._latest(target), train_origin, horizon)

    def truth(self, target: str, origin: date, horizon: int) -> Optional[float]:
        """Истина для оценки: окончательный `T_SAX`, последние редакции `T_CORE`/`T_NSA`."""
        return cumulative(self._latest(target), origin, horizon)


def build_target_data(nsa: dict, util: dict, core: dict, weight: float, origins: list, runner: Callable) -> TargetData:
    """Собирает `TargetData`; винтаж недоступного origin'а — в `skipped_vintages` с причиной, не заполняется."""
    final = tt.sax_final(nsa, util, weight, runner)
    vintages, skipped = tt.sax_vintages(nsa, util, weight, origins, runner)
    return TargetData(nsa=dict(nsa), core=dict(core), sax_final=final, sax_vintages=vintages, skipped_vintages=skipped)


def load_target_data(origins: list, weight: float = tt.DEFAULT_W, runner: Optional[Callable] = None) -> TargetData:
    """Чтение ClickHouse (`rosstat_ipc_mes`, `cbr_indicators_cpd`, `v_fedstat_ipc`) и сборка `TargetData`."""
    runner = runner or rec.make_r_runner(tt.FIRST_MONTH)
    return build_target_data(rec.load_nsa(), rec.load_utilities(), rec.load_cbr_core(), weight, origins, runner)


# ---------------------------------------------------------------------------
# Отпечатки (детерминизм)
# ---------------------------------------------------------------------------


def predictions_digest(rows: list) -> str:
    """sha256 канонического представления предсказаний (порядок строк не важен) — как в #396."""
    lines = sorted(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":")) for row in rows)
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def feature_digests(rows_by_origin: dict) -> dict:
    """`{признак: sha256 значений по origin'ам}`: по какому признаку разошлись данные."""
    names = sorted({name for features in rows_by_origin.values() for name in features})
    out = {}
    for name in names:
        values = sorted(
            (origin.isoformat(), repr(float(features[name])) if name in features else "absent")
            for origin, features in rows_by_origin.items()
        )
        out[name] = hashlib.sha256(json.dumps(values, separators=(",", ":")).encode("utf-8")).hexdigest()
    return out


# ---------------------------------------------------------------------------
# Описание фолдов и прогон
# ---------------------------------------------------------------------------


def fold_summary(folds: list) -> dict:
    """Сводка фолдов: число, границы origin'ов, размеры train, отпечаток структуры (origin, cutoff, train)."""
    if not folds:
        return {"n": 0}
    structure = [[f.origin.isoformat(), f.cutoff.isoformat(), [o.isoformat() for o in f.train]] for f in folds]
    return {
        "n": len(folds),
        "first_origin": folds[0].origin.isoformat(),
        "last_origin": folds[-1].origin.isoformat(),
        "train_min": min(len(f.train) for f in folds),
        "train_max": max(len(f.train) for f in folds),
        "digest": hashlib.sha256(json.dumps(structure, separators=(",", ":")).encode("utf-8")).hexdigest(),
    }


STAGES = {
    # этап: (первый origin кандидатов, first_train, min_train, первый тестовый origin отчёта)
    "stage1": (date(2016, 1, 1), STAGE1_FIRST_TRAIN, STAGE1_MIN_TRAIN),
    "stage2": (date(2002, 5, 1), date(2002, 5, 1), STAGE2_MIN_TRAIN),
}


def _origins(first: date, last: date) -> list:
    out, current = [], first
    while current <= last:
        out.append(current)
        current = _shift(current, 1)
    return out


def run(runner: Optional[Callable] = None, release_dates: Optional[dict] = None, last_origin: Optional[date] = None) -> dict:
    """Целевые данные и фолды обоих этапов по всем таргетам и горизонтам; только чтение ClickHouse."""
    import cpi_release_dates as rd

    release_dates = release_dates if release_dates is not None else rd.load_release_dates()
    nsa, core = rec.load_nsa(), rec.load_cbr_core()
    last_origin = last_origin or _shift(max(nsa), 1)
    vintage_origins = _origins(date(2007, 5, 1), last_origin)
    data = load_target_data(vintage_origins, runner=runner)
    result = {
        "data": {
            "nsa": {"n": len(nsa), "first": min(nsa).isoformat(), "last": max(nsa).isoformat()},
            "core": {"n": len(core), "first": min(core).isoformat(), "last": max(core).isoformat()},
            "sax_final": {"n": len(data.sax_final), "first": min(data.sax_final).isoformat(), "last": max(data.sax_final).isoformat()},
            "sax_vintages": {
                "built": len(data.sax_vintages),
                "skipped": [{"origin": o.isoformat(), "reason": r} for o, r in data.skipped_vintages],
            },
        },
        "folds": {},
    }
    for stage, (first_candidate, first_train, min_train) in STAGES.items():
        candidates = _origins(first_candidate, last_origin)
        for target in TARGETS:
            for horizon in HORIZONS:
                usable = [
                    o for o in candidates
                    if data.truth(target, o, horizon) is not None
                ]
                folds = make_folds(target, horizon, candidates, usable, first_train, min_train, release_dates)
                folds = [f for f in folds if data.truth(target, f.origin, horizon) is not None and (target != "T_SAX" or f.origin in data.sax_vintages)]
                for fold in folds:
                    assert_no_lookahead(target, horizon, fold, release_dates)
                result["folds"][f"{stage}/{target}/h{horizon}"] = fold_summary(folds)
    return result


def main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=ROOT / "build" / "cpi-two-stage" / "w3_folds.json")
    args = parser.parse_args(argv)
    result = run()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"data": result["data"], "folds": {k: (v["n"], v.get("first_origin"), v.get("train_min"), v.get("train_max")) for k, v in result["folds"].items()}}, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
