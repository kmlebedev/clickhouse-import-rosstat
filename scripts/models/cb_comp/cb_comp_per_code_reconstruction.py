#!/usr/bin/env python3
"""Task 8 #434: восстановление per-code МП (87 кодов) из листовых агрегатов.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-10-30-cb-comp-88-groups-live-design.md` §16.

Публичная книга ЦБ даёт 11 листов × 3 индикатора, нужны 87 кодов × 3. Лист —
агрегат своих кодов (`W`), поэтому восстановление — обратная задача: на каждый
месяц 11 уравнений, 87 неизвестных (76 степеней свободы). Скрипт оценивает
forward-оператор `W`, проверяет его OOS, затем решает обратную задачу методами
поиска (identity/offset/ridge/factor/omp/coord/elim) и сравнивает с настоящими
per-code значениями архива на holdout.

Гейты (до результатов): G-R1 — медиана forward OOS R² ≥ 0,7 и ни один
индикатор < 0,5; G-R2 — лучший метод даёт медиану R² > 0 и выше identity
минимум на 0,05 хотя бы на двух индикаторах. G-R3 (downstream) — только при
пройденном G-R2, в этом скрипте не считается.

Только чтение локальных файлов; в ClickHouse не пишет. Запуск —
`make cb-comp-per-code-reconstruction`, тесты — `...-test`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

SEED = 20261018
INDICATORS = {
    "Indikator biznes-klimata Banka Rossii (IBK)": "ibk",
    "Izmeneniye izderzhek proizvodstva (obrashcheniya): balans otvetov": "costs",
    "Ozhidaniya izmeneniya tsen na gotovuyu produktsiyu (uslugi) predpriyatiy: balans otvetov": "price_exp",
}
IND_ORDER = ("ibk", "costs", "price_exp")
TRAIN_START = "2010-01-01"
TRAIN_END = "2022-12-01"
TEST_START = "2023-01-01"
TEST_END = "2024-09-01"
LAMBDAS = (0.01, 0.1, 1.0, 10.0, 100.0)
FACTOR_KS = (5, 8, 11)
OMP_KS = (3, 5, 10)
ELIM_THRESHOLDS = (0.005, 0.01, 0.02)
COORD_LAMBDAS = (0.1, 1.0)
BOUND = 100.0
G_R1_MEDIAN = 0.7
G_R1_MIN_IND = 0.5
G_R2_MARGIN = 0.05
DEFAULT_PANEL = ROOT / "build" / "cb-comp-parity" / "panel.csv"
DEFAULT_PUBLIC_DIR = ROOT / "build" / "cb-comp-parity"
DEFAULT_OUT = ROOT / "build" / "cb-comp-per-code"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_codes(panel_csv: Path, map_csv: Path) -> dict:
    """Per-code МП из архива: {ind: DataFrame(date × code)}."""
    frame = pd.read_csv(panel_csv, usecols=["date", "group_stat", "code", "kind", "value"],
                       dtype={"code": str})
    frame = frame[frame["group_stat"] == "Мониторинг предприятий"].copy()
    frame["ind"] = frame["kind"].map(INDICATORS)
    frame = frame.dropna(subset=["ind"])
    frame["date"] = pd.to_datetime(frame["date"])
    frame["code"] = frame["code"].astype(str)
    mapping = pd.read_csv(map_csv, dtype={"code": str})
    frame = frame[frame["code"].isin(set(mapping["code"]))]
    out = {}
    for ind in IND_ORDER:
        part = frame[frame["ind"] == ind]
        out[ind] = part.pivot_table(index="date", columns="code", values="value", aggfunc="first").sort_index()
    return out


def load_sheets(public_csv: Path, map_csv: Path) -> tuple:
    """Листовые серии: ({ind: DataFrame(date × slug)}, {code: slug})."""
    frame = pd.read_csv(public_csv, dtype={"code": str})
    frame["date"] = pd.to_datetime(frame["date"])
    mapping = pd.read_csv(map_csv, dtype={"code": str})
    frame = frame.merge(mapping, on="code", how="left")
    if frame["industry_slug"].isna().any():
        raise ValueError("в публичной панели есть коды без industry_slug")
    out = {}
    for ind in IND_ORDER:
        part = frame[frame["indicator"] == ind]
        out[ind] = part.drop_duplicates(["industry_slug", "date"]).pivot_table(
            index="date", columns="industry_slug", values="value", aggfunc="first"
        ).sort_index()
    return out, dict(zip(mapping["code"], mapping["industry_slug"]))


def estimate_weights(codes: pd.DataFrame, sheets: pd.DataFrame, code_sheet: dict,
                     train: pd.Series, ridge: float = 1.0) -> tuple:
    """Блочно-диагональный W (sheets × codes), строки нормированы на 1."""
    codes_list = list(codes.columns)
    slugs = list(sheets.columns)
    index = {code: i for i, code in enumerate(codes_list)}
    weights = np.zeros((len(slugs), len(codes_list)), dtype=float)
    for row, slug in enumerate(slugs):
        cols = [index[code] for code in codes_list if code_sheet[code] == slug]
        if not cols:
            continue
        block = codes.iloc[:, cols]
        target = sheets[slug].reindex(codes.index)
        mask = target.notna() & block.notna().all(axis=1) & train
        if mask.sum() < 24:
            continue
        gram = block[mask].to_numpy().T @ block[mask].to_numpy()
        rhs = block[mask].to_numpy().T @ target[mask].to_numpy()
        coef = np.linalg.solve(gram + ridge * np.eye(len(cols)), rhs)
        coef = np.clip(coef, 0.0, None)
        total = coef.sum()
        weights[row, cols] = coef / total if total > 0 else np.full(len(cols), 1.0 / len(cols))
    return weights, slugs, codes_list


def forward_oos(weights: np.ndarray, slugs: list, codes: pd.DataFrame,
                sheets: pd.DataFrame, train: pd.Series, test: pd.Series) -> dict:
    """R² «лист на кодах» на train (in-sample) и test (OOS)."""
    result = {}
    full = codes.notna().all(axis=1)
    for row, slug in enumerate(slugs):
        target = sheets[slug].reindex(codes.index)
        mask = full & target.notna()
        pred = pd.Series(codes.to_numpy() @ weights[row], index=codes.index)
        entry = {}
        for name, part in (("train", train), ("test", test)):
            sel = mask & part
            if sel.sum() < 6:
                entry[name] = None
                continue
            y = target[sel].to_numpy()
            p = pred[sel].to_numpy()
            total = ((y - y.mean()) ** 2).sum()
            entry[name] = None if total == 0 else float(1.0 - ((y - p) ** 2).sum() / total)
        result[slug] = entry
    return result


def _base(sheets: np.ndarray, offsets: np.ndarray, sheet_of: np.ndarray) -> np.ndarray:
    return sheets[:, sheet_of] + offsets


def _solve(weights: np.ndarray, rhs: np.ndarray, lam: float, prior: np.ndarray) -> np.ndarray:
    return np.linalg.solve(weights.T @ weights + lam * np.eye(weights.shape[1]),
                           weights.T @ rhs + lam * prior)


def ar_prior(x_train: np.ndarray, x_test: np.ndarray) -> tuple:
    """AR(1) на месяц вперёд: train — по фактическому t−1, test — по предыдущему факту."""
    mu = x_train.mean(axis=0)
    if len(x_train) >= 3:
        prev, cur = x_train[:-1], x_train[1:]
        denom = ((prev - prev.mean(axis=0)) ** 2).sum(axis=0)
        with np.errstate(invalid="ignore", divide="ignore"):
            phi = ((prev - prev.mean(axis=0)) * (cur - cur.mean(axis=0))).sum(axis=0) / denom
        phi = np.clip(np.nan_to_num(phi, nan=0.0), 0.0, 0.99)
    else:
        phi = np.zeros(x_train.shape[1])
    m_train = np.empty_like(x_train)
    m_train[0] = x_train[0]
    for t in range(1, len(x_train)):
        m_train[t] = mu + phi * (x_train[t - 1] - mu)
    m_test = np.empty_like(x_test)
    m_test[0] = mu + phi * (x_train[-1] - mu)
    for t in range(1, len(x_test)):
        m_test[t] = mu + phi * (x_test[t - 1] - mu)
    return m_train, m_test


def reconstruct(weights: np.ndarray, slugs: list, code_sheet: dict, codes_list: list,
                s_train: np.ndarray, x_train: np.ndarray, s_test: np.ndarray,
                m_train: np.ndarray, m_test: np.ndarray,
                method: str, param) -> np.ndarray:
    """Восстановленные коды на тестовых месяцах; (n_test × n_codes)."""
    sheet_of = np.array([slugs.index(code_sheet[code]) for code in codes_list])
    offsets = (x_train - s_train[:, sheet_of]).mean(axis=0)
    base_test = s_test[:, sheet_of] + offsets
    if method == "identity":
        return s_test[:, sheet_of].copy()
    if method == "offset":
        return base_test
    if method == "ridge_prior":
        lam = float(param)
        return np.vstack([_solve(weights, s_test[t], lam, m_test[t]) for t in range(len(s_test))])
    if method == "factor":
        k = int(param)
        dev = x_train - m_train
        mu = dev.mean(axis=0)
        centered = dev - mu
        _, _, vt = np.linalg.svd(centered, full_matrices=False)
        load = vt[:k].T
        proj = weights @ load
        out = np.empty_like(m_test)
        for t in range(len(s_test)):
            rhs = s_test[t] - weights @ (m_test[t] + mu)
            f, *_ = np.linalg.lstsq(proj, rhs, rcond=None)
            out[t] = m_test[t] + mu + load @ f
        return out
    if method == "omp":
        k = int(param)
        out = m_test.copy()
        for t in range(len(s_test)):
            residual = s_test[t] - weights @ m_test[t]
            active: list = []
            for _ in range(k):
                scores = np.abs(weights.T @ residual)
                if active:
                    scores[active] = -np.inf
                active.append(int(np.argmax(scores)))
                sub = weights[:, active]
                coef, *_ = np.linalg.lstsq(sub, s_test[t] - weights @ m_test[t], rcond=None)
                residual = s_test[t] - weights @ m_test[t] - sub @ coef
            sub = weights[:, active]
            coef, *_ = np.linalg.lstsq(sub, s_test[t] - weights @ m_test[t], rcond=None)
            out[t, active] = m_test[t, active] + coef
        return out
    if method == "coord":
        lam = float(param)
        lipschitz = 2.0 * float(np.linalg.norm(weights, 2) ** 2) + 2.0 * lam
        out = np.empty_like(m_test)
        for t in range(len(s_test)):
            x = m_test[t].copy()
            for _ in range(300):
                grad = 2.0 * weights.T @ (weights @ x - s_test[t]) + 2.0 * lam * (x - m_test[t])
                x = np.clip(x - grad / lipschitz, -BOUND, BOUND)
            out[t] = x
        return out
    if method == "elim":
        threshold = float(param)
        keep = weights.sum(axis=0) >= threshold
        if keep.sum() < weights.shape[0]:
            keep = np.ones(weights.shape[1], dtype=bool)
        sub = weights[:, keep]
        out = m_test.copy()
        for t in range(len(s_test)):
            out[t, keep] = _solve(sub, s_test[t], 1e-6, m_test[t, keep])
        return out
    raise ValueError(f"неизвестный метод {method!r}")


def _r2_per_series(truth: np.ndarray, pred: np.ndarray) -> np.ndarray:
    with np.errstate(invalid="ignore", divide="ignore"):
        sse = ((truth - pred) ** 2).sum(axis=0)
        sst = ((truth - truth.mean(axis=0)) ** 2).sum(axis=0)
        return 1.0 - sse / sst


def evaluate(truth: np.ndarray, pred: np.ndarray) -> dict:
    r2 = _r2_per_series(truth, pred)
    rmse = np.sqrt(((truth - pred) ** 2).mean(axis=0))
    return {
        "n_months": int(truth.shape[0]),
        "r2_median": float(np.nanmedian(r2)),
        "r2_mean": float(np.nanmean(r2)),
        "share_r2_positive": float(np.nanmean(r2 > 0)),
        "share_r2_above_half": float(np.nanmean(r2 > 0.5)),
        "rmse_median": float(np.nanmedian(rmse)),
    }


def null_space(codes: pd.DataFrame, sheets: pd.DataFrame, weights: np.ndarray,
               slugs: list, code_sheet: dict, codes_list: list) -> dict:
    """Доля дисперсии отклонений (код − лист), объяснимая статикой; ранг W."""
    block = codes[codes_list]
    valid = block.notna().all(axis=1)
    x = block[valid].to_numpy()
    sheet_of = np.array([slugs.index(code_sheet[code]) for code in codes_list])
    dev = x - sheets.loc[valid, slugs].to_numpy()[:, sheet_of]
    total = float(dev.var(ddof=0))
    static = float(dev.mean(axis=0).var(ddof=0))
    rank = int(np.linalg.matrix_rank(weights))
    return {
        "deviation_variance": total,
        "static_share": static / total if total else None,
        "rank_weights": rank,
        "n_codes": int(weights.shape[1]),
        "null_dim": int(weights.shape[1] - rank),
    }


def run(panel_csv: Path, public_dir: Path, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    codes_all = load_codes(panel_csv, public_dir / "sheet_map.csv")
    sheets_all, code_sheet = load_sheets(public_dir / "public_panel.csv", public_dir / "sheet_map.csv")
    result = {
        "spec": "docs/superpowers/specs/2026-10-30-cb-comp-88-groups-live-design.md §16",
        "seed": SEED,
        "config": {"train": [TRAIN_START, TRAIN_END], "test": [TEST_START, TEST_END],
                   "lambdas": list(LAMBDAS), "factor_k": list(FACTOR_KS), "omp_k": list(OMP_KS),
                   "elim_thresholds": list(ELIM_THRESHOLDS)},
        "inputs": {"panel_csv": sha256(panel_csv), "public_panel_csv": sha256(public_dir / "public_panel.csv"),
                   "sheet_map_csv": sha256(public_dir / "sheet_map.csv")},
        "gates": {}, "indicators": {},
    }
    forward_median = {}
    for ind in IND_ORDER:
        codes = codes_all[ind]
        sheets = sheets_all[ind]
        months = codes.index.intersection(sheets.index)
        codes = codes.loc[months]
        sheets = sheets.loc[months]
        train = pd.Series((months >= TRAIN_START) & (months <= TRAIN_END), index=months)
        test = pd.Series((months >= TEST_START) & (months <= TEST_END), index=months)
        weights, slugs, codes_list = estimate_weights(codes, sheets, code_sheet, train)
        fwd = forward_oos(weights, slugs, codes, sheets, train, test)
        forward_median[ind] = float(np.nanmedian([v["test"] for v in fwd.values() if v["test"] is not None]))
        train_block = codes.loc[train[train].index]
        train_valid = train_block.notna().all(axis=1)
        x_train = train_block[train_valid].to_numpy()
        s_train = sheets.loc[train_block.index[train_valid], slugs].to_numpy()
        test_block = codes.loc[test[test].index]
        test_valid = test_block.notna().all(axis=1)
        x_test = test_block[test_valid].to_numpy()
        s_test = sheets.loc[test_block.index[test_valid], slugs].to_numpy()
        m_train, m_test = ar_prior(x_train, x_test)
        metrics = {"forward": fwd, "methods": {},
                   "null_space": null_space(codes, sheets, weights, slugs, code_sheet, codes_list)}
        for method, params in (
            ("identity", [None]), ("offset", [None]), ("ridge_prior", LAMBDAS),
            ("factor", FACTOR_KS), ("omp", OMP_KS), ("coord", COORD_LAMBDAS), ("elim", ELIM_THRESHOLDS),
        ):
            for param in params:
                pred = reconstruct(weights, slugs, code_sheet, codes_list, s_train, x_train, s_test, m_train, m_test, method, param)
                key = method if param is None else f"{method}:{param}"
                metrics["methods"][key] = evaluate(x_test, pred)
        result["indicators"][ind] = metrics
    g_r1 = bool(all(v >= G_R1_MIN_IND for v in forward_median.values()) and
                float(np.median(list(forward_median.values()))) >= G_R1_MEDIAN)
    identity = {ind: result["indicators"][ind]["methods"]["identity"]["r2_median"] for ind in IND_ORDER}
    best = {}
    for ind in IND_ORDER:
        cand = {k: v for k, v in result["indicators"][ind]["methods"].items()
                if not k.startswith("identity") and not k.startswith("offset")}
        key = max(cand, key=lambda k: cand[k]["r2_median"] if np.isfinite(cand[k]["r2_median"]) else -9e9)
        best[ind] = (key, cand[key]["r2_median"])
    wins = sum(1 for ind in IND_ORDER if np.isfinite(best[ind][1]) and best[ind][1] > identity[ind] + G_R2_MARGIN)
    positive = sum(1 for ind in IND_ORDER if np.isfinite(best[ind][1]) and best[ind][1] > 0)
    g_r2 = bool(positive == len(IND_ORDER) and wins >= 2)
    result["gates"] = {
        "G-R1": {"pass": g_r1, "forward_median": forward_median,
                 "threshold_median": G_R1_MEDIAN, "threshold_ind": G_R1_MIN_IND},
        "G-R2": {"pass": g_r2, "identity": identity,
                 "best": {ind: {"method": best[ind][0], "r2_median": best[ind][1]} for ind in IND_ORDER},
                 "margin": G_R2_MARGIN, "wins": wins, "positive": positive},
    }
    result["decision"] = "go" if g_r1 and g_r2 else "no-go"
    (out_dir / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=1, sort_keys=True))
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--panel", default=str(DEFAULT_PANEL))
    parser.add_argument("--public-dir", default=str(DEFAULT_PUBLIC_DIR))
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    args = parser.parse_args(argv)
    result = run(Path(args.panel), Path(args.public_dir), Path(args.out))
    print(json.dumps({"decision": result["decision"], "gates": result["gates"]}, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
