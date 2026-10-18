#!/usr/bin/env python3
"""Паритет ML-трека с CB_COMP (Кошкин–Крылов, 2025): строгое сравнение.

Пункт `key-rate/phase-4-ml-cb-comp-parity` (issue #190), спека
`docs/superpowers/specs/2026-10-18-cb-comp-parity-design.md`.

Что делает скрипт:

* **таргет** — `TARGET_FULL` из архива авторов: SA ИПЦ без коммунальных услуг,
  накопленный прирост за h месяцев, % (2009-01…2024-07); таргет не
  реконструируется и не заменяется прокси;
* **прогнозы CB_COMP** — per-period, из листа `StfCpiCB` (h1/h2/h3/h6) и
  `out_of_sample_forecasts/catboostVCB_IBK.xlsx` (h1…h6); retrain не
  выполняется. До обучения жёстко проверяются: RMSE статьи (0.158805 /
  0.296655 / 0.443623 / 0.971459), совпадение двух архивных источников
  прогнозов и normal-флаг `Учитывать` (54 месяца);
* **признаки** — функции `scripts/cpi_monthly.py` (ML-трек v2) как есть:
  базовые `y_lag1`, `y_lag2`, `usd_mm`, `key_rate`, `fao_mm`, `month` и 7
  МП-признаков `cbr_survey`; cutoff месяца — 8-е число M+1, собственный
  таргет месяца в признаки не попадает;
* **модели** — CatBoost direct на h-таргете, две конфигурации на горизонт:
  `nowcast_v2_mp` (с МП) и `nowcast_v2_nomp` (без МП); дефолты библиотеки,
  seed 20261018, `month` категориальным. Bottom-up не строится: таргет
  статьи — SA ИПЦ без КУ, его компоненты в публичном архиве не собираются
  без весов СПР и 92 компонентных моделей (спека §4);
* **сравнение** — на общих 54 normal-месяцах 2019-01…2024-07 (исключены
  2020-04 и 2022): RMSE, MAE, DM-HLN (h=1 — конвенция авторов; h=h —
  робастность), TOST с margin δ = 10% RMSE CB_COMP (зафиксирован в спеке до
  прогона), RMSE ratio с 90% CI, парный блочный бутстрап (блок 3, n=2000,
  seed 20261018);
* **вердикт** — «близок» только если все четыре основных горизонта (1/2/3/6)
  дают DM p > 0.05 И 90% CI разницы RMSE внутри ±δ; иначе «не близок»
  (поимённо по горизонтам). «Не близок» — законный результат issue;
* **артефакты** — `cb_comp_parity_report.md`, `cb_comp_parity_predictions.csv`,
  `cb_comp_parity_result.json` в корне worktree (gitignored).

Скрипт только читает ClickHouse (ряды v2) и архив; ничего не пишет в БД.
Запуск — через `make cb-comp-parity` (env подключает Makefile), тесты —
`make cb-comp-parity-test` (офлайн, без ClickHouse и без архива).

Границы, названные до прогона (спека §6): train — 2010-03…2018-12 (106 месяцев,
предзарегистрированное окно). Признаки `key_rate` и `usd_mm` до 2013 г. — NaN:
вендоренный `cpi_monthly.py` (long-sample политика v2, §8.2) не выбрасывает
такие месяцы, и CatBoost обрабатывает NaN нативно; short-sample v2 отсекал бы
2013-10, но parity использует long-sample, чтобы окно совпало с CB_COMP.
Retrain CB_COMP не выполняется; наш DM-HLN считается по квадратам ошибок, а
архивные p-value авторов — по четвёртым степеням (в `dm.test` подаются уже
квадраты ошибок), поэтому архивные p-value воспроизводятся отдельной
кросс-проверкой, но не заменяют наш тест.
"""

import argparse
import csv
import json
import math
import os
import random
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent

# Скрипт-сосед: признаки, as-of и календарь месяцев переиспользуются из
# ML-трека v2, а не дублируются. `cpi_monthly.py` — побайтовая копия файла из
# PR #177 (в `main` его ещё нет); см. спеку §3.2.
sys.path.insert(0, str(ROOT / "scripts"))

import cpi_monthly as cm
import eval_utils as eu

# ---------------------------------------------------------------------------
# Предзарегистрированные константы (спека §5.2; значения пинуются тестами)
# ---------------------------------------------------------------------------

SEED = 20261018

# Основные горизонты — как в таблице статьи; 4/5 есть в архиве, но в статье их
# нет: считаются как дополнительные и на вердикт не влияют.
HORIZONS = (1, 2, 3, 6)
HORIZONS_EXTRA = (4, 5)
ALL_HORIZONS = HORIZONS + HORIZONS_EXTRA

# Сплит статьи. Train 2010-03…2018-12 — предзарегистрированное окно; фактически
# месяцы до 2013-10 отсекаются доступностью признаков v2 (спека §6.1), и это
# ограничение данных, а не выбор по результату.
TRAIN_START = date(2010, 3, 1)
TRAIN_END = date(2018, 12, 1)
TEST_START = date(2019, 1, 1)
TEST_END = date(2024, 7, 1)

# Test outlier статьи: 2020-04 и весь 2022 (2014-12…2015-03 вне тестового окна).
OUTLIER_MONTHS = frozenset(
    [date(2020, 4, 1)] + [date(2022, month, 1) for month in range(1, 13)]
)

# Выбросы train по статье: декабрь 2014 — март 2015. CB_COMP обучался на 102
# месяцах (106 минус эти 4); parity повторяет фильтр, иначе сравнение было бы
# «одинаковое окно, разный состав». В тест они не входят по определению окна.
TRAIN_OUTLIER_MONTHS = frozenset(
    [date(2014, 12, 1), date(2015, 1, 1), date(2015, 2, 1), date(2015, 3, 1)]
)

# Margin фиксируется ДО прогона: δ_h = 10% RMSE CB_COMP_h.
MARGIN_FRACTION = 0.10

# Парный блочный бутстрап: блок 3 месяца, 2000 ресэмплов, сид прогона.
BOOTSTRAP_BLOCK = 3
BOOTSTRAP_N = 2000

# RMSE статьи/архива — жёсткий гейт: если архив или листы поменялись, прогон
# останавливается, а не публикует сравнение с другой моделью.
CB_RMSE_PINNED = {1: 0.158805, 2: 0.296655, 3: 0.443623, 6: 0.971459}
CB_RMSE_TOLERANCE = 1e-6
CB_SOURCE_TOLERANCE = 1e-9
# `TARGET_FULL[h]` обязан быть накопленным произведением `TARGET_FULL[1]`:
# проверка ловит сдвиг/подмену колонки таргета до обучения.
TARGET_CONSISTENCY_TOLERANCE = 1e-9

# DM-конвенция авторов: `forecast::dm.test` по умолчанию `h=1`, `power=2`, а на
# вход ему подаются уже квадраты ошибок, поэтому фактически тест идёт на
# четвёртых степенях. Эти p-value воспроизводятся только как кросс-проверка
# архива; наш тест — стандартный DM-HLN на квадратах ошибок (спека §6.4).
ARCHIVE_DM_PINNED = {1: 0.032952, 2: 0.005115, 3: 0.005355, 6: 0.132256}
ARCHIVE_DM_TOLERANCE = 1e-6

# Конфигурация CatBoost фиксируется до прогона: дефолты библиотеки, без
# валидации/early stopping/перебора. Остальные параметры — дефолты CatBoost.
# `cat_features` на numpy-матрице задаётся ИНДЕКСОМ, поэтому имя «month»
# переводится в позицию в `catboost_params`, а не передаётся строкой.
CATBOOST_CAT_FEATURES = ("month",)
CATBOOST_DEFAULTS = {
    "loss_function": "RMSE",
    "random_seed": SEED,
    "verbose": False,
    "allow_writing_files": False,
}


def catboost_params(names: list) -> dict:
    """Параметры CatBoost для набора признаков `names` (категориальные — индексы)."""
    params = dict(CATBOOST_DEFAULTS)
    params["cat_features"] = [names.index(name) for name in CATBOOST_CAT_FEATURES]
    return params

MODEL_MP = "nowcast_v2_mp"
MODEL_NOMP = "nowcast_v2_nomp"
CB_MODEL = "cb_comp"

ARTIFACT_REPORT = "cb_comp_parity_report.md"
ARTIFACT_PREDICTIONS = "cb_comp_parity_predictions.csv"
ARTIFACT_RESULT = "cb_comp_parity_result.json"

# ---------------------------------------------------------------------------
# Архив: пути и чтение листов
# ---------------------------------------------------------------------------

ARCHIVE_ROOT_NAME = "BusinessSurveyCBRCpiForecastingSourceCode"
TARGET_XLSX = "Расчет ошибок вневыборочного прогноза по моделям.xlsx"
CB_XLSX = Path("Models/CpiComponents/out_of_sample_forecasts/catboostVCB_IBK.xlsx")
DM_XLSX = "model_errors_and_dm_tests.xlsx"

# Имена листов/колонок архива — контракт с читателями. Лист `StfCpiCB` — те же
# прогнозы, что `catboostVCB_IBK.xlsx`, плюс флаг `Учитывать`.
CB_SHEET = "StfCpiCB"
CB_NOMP_SHEET = "StfCpiCB_nomp"
TARGET_SHEET = "TARGET_FULL"
DM_SHEET = "dm_tests_table"


def month_start(value) -> date:
    """Первое число месяца: канонический ключ месяца во всех расчётах."""
    if isinstance(value, datetime):
        return date(value.year, value.month, 1)
    if isinstance(value, date):
        return date(value.year, value.month, 1)
    return date.fromisoformat(str(value)[:10]).replace(day=1)


def month_range(first: date, last: date) -> list:
    """Месяцы `first`…`last` включительно, по первому числу, по возрастанию."""
    months = []
    year, month = first.year, first.month
    while date(year, month, 1) <= last:
        months.append(date(year, month, 1))
        month += 1
        if month == 13:
            year, month = year + 1, 1
    return months


def archive_candidates() -> list:
    """Известные места распакованного архива (без скачивания)."""
    return [
        ROOT.parent / "cb-comp-archive" / "build" / "cb-comp" / "extracted" / ARCHIVE_ROOT_NAME,
        ROOT / "build" / "cb-comp" / "extracted" / ARCHIVE_ROOT_NAME,
        ROOT.parent / ARCHIVE_ROOT_NAME,
    ]


def resolve_archive(explicit: str = None) -> Path:
    """Каталог `BusinessSurveyCBRCpiForecastingSourceCode` с нужными файлами.

    Порядок: `--archive` → env `CB_COMP_ARCHIVE` → известные кандидаты. Ошибка
    перечисляет проверенные пути и отсылает к source report §2 — архив
    скачивается отдельно и в репозиторий не входит.
    """
    candidates = []
    if explicit:
        candidate = Path(explicit).expanduser()
        candidates.append(candidate)
        if candidate.name != ARCHIVE_ROOT_NAME:
            candidates.append(candidate / ARCHIVE_ROOT_NAME)
    else:
        env_path = os.environ.get("CB_COMP_ARCHIVE")
        if env_path:
            candidates.append(Path(env_path).expanduser())
        candidates.extend(archive_candidates())
    checked = []
    for candidate in candidates:
        checked.append(str(candidate))
        if (candidate / TARGET_XLSX).is_file() and (candidate / CB_XLSX).is_file():
            return candidate
    raise SystemExit(
        "архив CB_COMP не найден. Передайте --archive PATH или CB_COMP_ARCHIVE; "
        "скачивание и распаковка описаны в docs/research/2026-10-18-cb-comp-archive.md §2. "
        "Проверены: " + "; ".join(checked)
    )


def read_target_full(archive: Path) -> pd.DataFrame:
    """`TARGET_FULL`: таргет h1…h6, индекс — первое число месяца.

    Колонки архива — целые 1…6. Пропуски (первые h−1 месяцев 2009 г.) остаются
    NaN: скрипт не заполняет их и не сдвигает окно.
    """
    frame = pd.read_excel(archive / TARGET_XLSX, sheet_name=TARGET_SHEET)
    if "дата" not in frame.columns:
        raise ValueError("TARGET_FULL: нет колонки 'дата'")
    index = [month_start(value) for value in frame["дата"]]
    data = {}
    for horizon in sorted(ALL_HORIZONS):
        if horizon not in frame.columns:
            raise ValueError(f"TARGET_FULL: нет колонки h{horizon}")
        data[horizon] = pd.to_numeric(frame[horizon], errors="coerce").to_numpy(dtype=float)
    target = pd.DataFrame(data, index=pd.Index(index, name="month"))
    if target.index.duplicated().any():
        raise ValueError("TARGET_FULL: повторяющаяся дата")
    return target.sort_index()


def flag_value(value) -> bool:
    """Флаг `Учитывать при расчете ошибки` из ячейки Excel — строго bool."""
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("true", "1"):
        return True
    if text in ("false", "0"):
        return False
    raise ValueError(f"флаг 'Учитывать' не распознан: {value!r}")


def read_cb_sheet(archive: Path, sheet_name: str = CB_SHEET) -> pd.DataFrame:
    """Per-period прогнозы CB_COMP из листа `StfCpiCB`/`StfCpiCB_nomp`.

    Возвращает DataFrame с индексом-месяцем, колонкой `normal` (флаг
    `Учитывать`) и колонками h1/h2/h3/h6 — ровно теми, что публикует статья.
    Заголовок ищется по строке «Учитывать при расчете ошибки», а не по
    фиксированному номеру: у листов с прогнозами и ошибками он один.
    """
    frame = pd.read_excel(archive / TARGET_XLSX, sheet_name=sheet_name, header=None)
    header = None
    for row_index in range(min(20, len(frame))):
        if str(frame.iloc[row_index, 0]).strip().startswith("Учитывать"):
            header = row_index
            break
    if header is None:
        raise ValueError(f"{sheet_name}: не найдена строка заголовка 'Учитывать'")
    data = frame.iloc[header + 1 :, [0, 1, 2, 3, 4, 5]].copy()
    data.columns = ["flag", "date", "h1", "h2", "h3", "h6"]
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data = data.dropna(subset=["date"])
    index = [month_start(value) for value in data["date"]]
    out = pd.DataFrame(
        {"normal": [flag_value(value) for value in data["flag"]]},
        index=pd.Index(index, name="month"),
    )
    for horizon, column in zip((1, 2, 3, 6), ("h1", "h2", "h3", "h6")):
        out[horizon] = pd.to_numeric(data[column], errors="coerce").to_numpy(dtype=float)
    if out.index.duplicated().any():
        raise ValueError(f"{sheet_name}: повторяющаяся дата")
    return out.sort_index()


def read_cb_xlsx(archive: Path) -> pd.DataFrame:
    """Per-period прогнозы CB_COMP из `catboostVCB_IBK.xlsx` (h1…h6)."""
    frame = pd.read_excel(archive / CB_XLSX)
    if "дата" not in frame.columns:
        raise ValueError("catboostVCB_IBK.xlsx: нет колонки 'дата'")
    index = [month_start(value) for value in frame["дата"]]
    data = {}
    for horizon in sorted(ALL_HORIZONS):
        column = str(horizon)
        if column not in frame.columns:
            raise ValueError(f"catboostVCB_IBK.xlsx: нет колонки {column}")
        data[horizon] = pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=float)
    out = pd.DataFrame(data, index=pd.Index(index, name="month"))
    if out.index.duplicated().any():
        raise ValueError("catboostVCB_IBK.xlsx: повторяющаяся дата")
    return out.sort_index()


def read_dm_xlsx(archive: Path) -> dict:
    """`dm_tests_table` архива: id → DM p-value (для кросс-проверки)."""
    frame = pd.read_excel(archive / DM_XLSX, sheet_name=DM_SHEET)
    if "DM" not in frame.columns or "id" not in frame.columns:
        raise ValueError("model_errors_and_dm_tests.xlsx: нет колонок DM/id")
    return {
        str(row_id): float(value)
        for row_id, value in zip(frame["id"], frame["DM"])
        if not pd.isna(value)
    }


# ---------------------------------------------------------------------------
# Таргет из уровней (документированная формула; используется тестами)
# ---------------------------------------------------------------------------


def cumulative_growth(levels, horizons=ALL_HORIZONS) -> pd.DataFrame:
    """Накопленный прирост за h месяцев из SA-уровней (100 = пред. месяц).

    `TARGET_h(M) = (Π_{j=0..h−1} level(M−j)/100 − 1)·100`. Первые h−1 строк
    остаются NaN: истории для окна нет, и заполнять её нельзя.
    """
    series = pd.Series(levels, dtype=float)
    if (series <= 0).any():
        raise ValueError("SA-уровень ≤ 0: логарифм не определён")
    log_levels = np.log(series.to_numpy(dtype=float) / 100.0)
    out = pd.DataFrame(index=series.index)
    for horizon in horizons:
        values = np.full(len(log_levels), np.nan)
        for position in range(horizon - 1, len(log_levels)):
            values[position] = (math.exp(float(log_levels[position - horizon + 1 : position + 1].sum())) - 1.0) * 100.0
        out[horizon] = values
    return out


def target_consistency(target: pd.DataFrame) -> float:
    """Max |TARGET_FULL[h] − накопленный прирост из TARGET_FULL[1]| по h2…h6.

    `TARGET_FULL[1]` — месячный прирост; уровень `100·(1+y/100) = 100 + y`.
    Накопленный h-прирост обязан совпасть с `cumulative_growth`, иначе колонки
    таргета сдвинуты/подменены — и сравнение шло бы не с тем таргетом.
    Первые h−1 NaN пропускаются: истории для окна нет.
    """
    if 1 not in target.columns:
        raise ValueError("TARGET_FULL: нет колонки h1 для проверки накопления")
    levels = 100.0 + target[1]
    horizons = tuple(sorted(horizon for horizon in ALL_HORIZONS if horizon != 1))
    derived = cumulative_growth(levels, horizons=horizons)
    max_diff = 0.0
    for horizon in horizons:
        for month in target.index:
            expected = target.at[month, horizon]
            if pd.isna(expected):
                continue
            actual = derived.at[month, horizon]
            if pd.isna(actual):
                raise ValueError(
                    f"TARGET_FULL h{horizon} {month}: нет истории для накопления, "
                    "но значение есть"
                )
            max_diff = max(max_diff, abs(float(actual) - float(expected)))
    return max_diff


def normal_months() -> list:
    """54 normal-месяца: test 2019-01…2024-07 минус 2020-04 и весь 2022."""
    return [month for month in month_range(TEST_START, TEST_END) if month not in OUTLIER_MONTHS]


def authors_dm_pvalue(err_a, err_b) -> float:
    """DM авторов: `forecast::dm.test` получает КВАДРАТЫ ошибок и возводит их
    в квадрат ещё раз (`power=2`), то есть тест идёт на четвёртых степенях.

    Формула: `d = ea⁴ − eb⁴`, статистика `mean(d)/sqrt(var(d)/T)`, двусторонний
    p-value по t с `T−1` степенями свободы (`h=1` — дефолт `dm.test`). Это
    воспроизведение архивных чисел, а не наш тест (спека §6.4).
    """
    if len(err_a) != len(err_b):
        raise ValueError("DM авторов: разная длина рядов ошибок")
    if not err_a:
        raise ValueError("DM авторов: пустой ряд ошибок")
    differences = [a**4 - b**4 for a, b in zip(err_a, err_b)]
    size = len(differences)
    mean = sum(differences) / size
    variance = sum((value - mean) ** 2 for value in differences) / (size - 1)
    if variance == 0.0:
        return 1.0 if mean == 0.0 else 0.0
    statistic = mean / math.sqrt(variance / size)
    return float(2.0 * stats.t.sf(abs(statistic), size - 1))


# ---------------------------------------------------------------------------
# Проверки архива до обучения
# ---------------------------------------------------------------------------


def normal_flag_months(sheet: pd.DataFrame) -> list:
    """Месяцы тестового окна, у которых флаг `Учитывать` = True."""
    return sorted(
        month
        for month, is_normal in sheet["normal"].items()
        if is_normal and TEST_START <= month <= TEST_END
    )


def assert_normal_flags(flag_months: list, normal: list) -> None:
    """Флаг `Учитывать` архива обязан совпасть с правилом статьи (§3.1)."""
    if list(flag_months) != list(normal):
        only_flag = sorted(set(flag_months) - set(normal))
        only_rule = sorted(set(normal) - set(flag_months))
        raise ValueError(
            "normal-месяцы архива не совпали с правилом статьи: "
            f"только в архиве {only_flag}, только по правилу {only_rule}"
        )


def source_max_diff(sheet: pd.DataFrame, xlsx: pd.DataFrame, normal: list) -> float:
    """Max |diff| между `StfCpiCB` и `catboostVCB_IBK.xlsx` (h1/h2/h3/h6).

    NaN в любой из ячеек — ошибка: пропуск прогноза на normal-месяце сломал бы
    сравнение, а не «немного» его сдвинул.
    """
    max_diff = 0.0
    for horizon in HORIZONS:
        for month in normal:
            if month not in xlsx.index:
                raise ValueError(f"catboostVCB_IBK.xlsx: нет месяца {month} для h{horizon}")
            sheet_value = sheet.at[month, horizon]
            xlsx_value = xlsx.at[month, horizon]
            if pd.isna(sheet_value) or pd.isna(xlsx_value):
                raise ValueError(f"прогноз CB_COMP h{horizon} {month}: NaN")
            max_diff = max(max_diff, abs(float(sheet_value) - float(xlsx_value)))
    return max_diff


def check_archive(archive: Path) -> dict:
    """Жёсткие проверки архива; расхождение — исключение, не предупреждение.

    1. `TARGET_FULL[h]` — накопленный прирост из `TARGET_FULL[1]` (h2…h6);
    2. RMSE CB_COMP на 54 normal-месяцах совпадает с константами статьи;
    3. `StfCpiCB` и `catboostVCB_IBK.xlsx` совпадают на общих normal-месяцах
       для h1/h2/h3/h6;
    4. normal-набор по правилу статьи совпадает с флагом `Учитывать`;
    5. архивные DM p-value воспроизводятся формулой авторов.
    """
    target = read_target_full(archive)
    sheet = read_cb_sheet(archive)
    no_mp_sheet = read_cb_sheet(archive, CB_NOMP_SHEET)
    xlsx = read_cb_xlsx(archive)
    normal = normal_months()

    target_max_diff = target_consistency(target)
    if target_max_diff > TARGET_CONSISTENCY_TOLERANCE:
        raise ValueError(
            f"TARGET_FULL h2…h6 не совпадают с накоплением h1: "
            f"max |diff| = {target_max_diff}"
        )

    assert_normal_flags(normal_flag_months(sheet), normal)

    rmse_cb = {}
    for horizon in HORIZONS:
        errors = _cb_errors(sheet, target, horizon, normal)
        value = eu.rmse(errors)
        pinned = CB_RMSE_PINNED[horizon]
        if abs(value - pinned) > CB_RMSE_TOLERANCE:
            raise ValueError(
                f"RMSE CB_COMP h{horizon} = {value:.6f}, ожидалось {pinned:.6f}: "
                "архив или лист изменились"
            )
        rmse_cb[horizon] = value

    max_source_diff = source_max_diff(sheet, xlsx, normal)
    if max_source_diff > CB_SOURCE_TOLERANCE:
        raise ValueError(
            f"StfCpiCB и catboostVCB_IBK.xlsx расходятся: max |diff| = {max_source_diff}"
        )

    archive_dm = read_dm_xlsx(archive)
    reproduced = {}
    for horizon in HORIZONS:
        errors_mp = _cb_errors(sheet, target, horizon, normal)
        errors_nomp = _cb_errors(no_mp_sheet, target, horizon, normal)
        value = authors_dm_pvalue(errors_mp, errors_nomp)
        expected = archive_dm.get(f"h{horizon}_StfCpiCB")
        if expected is None:
            raise ValueError(f"dm_tests_table: нет строки h{horizon}_StfCpiCB")
        if abs(value - expected) > ARCHIVE_DM_TOLERANCE:
            raise ValueError(
                f"DM авторов h{horizon} = {value:.6f}, в архиве {expected:.6f}: "
                "формула воспроизведения разошлась с архивом"
            )
        reproduced[horizon] = {"computed": value, "archive": expected}

    margins = {horizon: MARGIN_FRACTION * value for horizon, value in rmse_cb.items()}
    extra_rmse = {}
    for horizon in HORIZONS_EXTRA:
        extra_rmse[horizon] = eu.rmse(_cb_errors(xlsx, target, horizon, normal))
    for horizon, value in extra_rmse.items():
        margins[horizon] = MARGIN_FRACTION * value

    return {
        "target": target,
        "sheet": sheet,
        "no_mp_sheet": no_mp_sheet,
        "xlsx": xlsx,
        "normal": normal,
        "rmse_cb": rmse_cb,
        "extra_rmse_cb": extra_rmse,
        "margins": margins,
        "max_source_diff": max_source_diff,
        "target_max_diff": target_max_diff,
        "archive_dm": reproduced,
    }


def _cb_errors(forecasts: pd.DataFrame, target: pd.DataFrame, horizon: int, months: list) -> list:
    """Ошибки CB_COMP `прогноз − факт` на перечисленных месяцах."""
    errors = []
    for month in months:
        if month not in forecasts.index:
            raise ValueError(f"прогноз CB_COMP h{horizon}: нет месяца {month}")
        if month not in target.index:
            raise ValueError(f"TARGET_FULL h{horizon}: нет месяца {month}")
        prediction = forecasts.at[month, horizon]
        fact = target.at[month, horizon]
        if pd.isna(prediction) or pd.isna(fact):
            raise ValueError(f"CB_COMP h{horizon} {month}: NaN в прогнозе или таргете")
        errors.append(float(prediction) - float(fact))
    return errors


# ---------------------------------------------------------------------------
# Выборка признаков (v2) и строки parity
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FeatureRow:
    """Месяц и признаки v2 (объединение базовых и МП)."""

    month: date
    features: dict


@dataclass(frozen=True)
class ParityRow:
    """Строка parity: месяц, признаки и таргет статьи по горизонтам."""

    month: date
    features: dict
    targets: dict


def load_feature_rows() -> tuple:
    """Строки признаков v2 для окна train+test; пропуски — с причинами.

    `cpi_monthly.base_features`/`mp_features` применяются как есть: cutoff —
    8-е число M+1, as-of правила v2. `base_features` работает в long-sample
    режиме v2 (§8.2): отсутствие `usd_mm`/`key_rate` до 2013 г. даёт NaN, а не
    пропуск месяца, поэтому предзарегистрированный train 2010-03…2018-12
    собирается целиком. Обязательными остаются `y_lag1`, `y_lag2`, `fao_mm` и
    все МП-точки — их отсутствие по-прежнему пропускает месяц с причиной.
    """
    targets = cm.load_targets()
    survey = cm.load_survey()
    usd = cm.load_usd()
    key_rate = cm.load_key_rate()
    fao = cm.load_fao()
    target_index = cm.target_index(targets)

    rows = []
    skipped = []
    for month in cm.sample_months(targets):
        if month < TRAIN_START or month > TEST_END:
            continue
        cutoff = cm.cutoff_for(month)
        base, reason = cm.base_features(month, target_index, usd, key_rate, fao, cutoff)
        if base is None:
            skipped.append((month, reason))
            continue
        mp_part, reason = cm.mp_features(month, survey, cutoff)
        if mp_part is None:
            skipped.append((month, reason))
            continue
        rows.append(FeatureRow(month=month, features={**base, **mp_part}))
    return rows, skipped


def build_rows(feature_rows: list, target: pd.DataFrame, horizons=ALL_HORIZONS) -> tuple:
    """Приклеить к строкам признаков таргет статьи; вернуть `(rows, missing)`.

    Таргет не заполняется и не сдвигается: месяц без значения хотя бы одного
    горизонта попадает в `missing` с причиной, а не в выборку.
    """
    rows = []
    missing = []
    for feature_row in feature_rows:
        if feature_row.month not in target.index:
            missing.append((feature_row.month, "нет месяца в TARGET_FULL"))
            continue
        values = {}
        reason = ""
        for horizon in horizons:
            value = target.at[feature_row.month, horizon]
            if pd.isna(value):
                reason = f"нет TARGET_FULL h{horizon}"
                break
            values[horizon] = float(value)
        if reason:
            missing.append((feature_row.month, reason))
            continue
        rows.append(ParityRow(month=feature_row.month, features=feature_row.features, targets=values))
    return rows, missing


def assert_no_leakage(rows: list) -> None:
    """Структурная проверка отсутствия утечки (спека §3.2, §6).

    - месяц в выборке ровно один раз;
    - train и test не пересекаются, max(train) < min(test);
    - cutoff(M) строго раньше публикации метки M: собственный таргет месяца в
      признаки попасть не может;
    - точка МП самого месяца M на его cutoff ещё не опубликована;
    - в признаках нет ключа `y` (таргет — только в `targets`).
    """
    months = [row.month for row in rows]
    if len(set(months)) != len(months):
        raise AssertionError("утечка: повторяющийся месяц в выборке")
    train = [row for row in rows if TRAIN_START <= row.month <= TRAIN_END]
    test = [row for row in rows if TEST_START <= row.month <= TEST_END]
    if not train or not test:
        raise AssertionError("утечка: пустой train или test")
    if max(row.month for row in train) >= min(row.month for row in test):
        raise AssertionError("утечка: train пересекается с test")
    for row in rows:
        if cm.cutoff_for(row.month) >= cm.available_from_mes(cm.month_end(row.month)):
            raise AssertionError(
                f"утечка: cutoff {row.month} не раньше публикации его метки"
            )
        if cm.available_from_survey(row.month) <= cm.cutoff_for(row.month):
            raise AssertionError(f"утечка: МП месяца {row.month} доступен на его cutoff")
        if "y" in row.features:
            raise AssertionError("утечка: таргет попал в признаки под ключом 'y'")


def split_rows(rows: list) -> tuple:
    """`(train, test)` по предзарегистрированным границам.

    Train — 2010-03…2018-12 без четырёх выбросов статьи (2014-12…2015-03):
    у CB_COMP факт train — 102 месяца, и parity повторяет тот же фильтр.
    """
    train = [
        row
        for row in rows
        if TRAIN_START <= row.month <= TRAIN_END and row.month not in TRAIN_OUTLIER_MONTHS
    ]
    test = [row for row in rows if TEST_START <= row.month <= TEST_END]
    return train, test


# ---------------------------------------------------------------------------
# Модели
# ---------------------------------------------------------------------------


def feature_matrix(rows: list, names: list) -> np.ndarray:
    """Матрица признаков: числовые float, `month` — категориальный int.

    `dtype=object` обязателен: CatBoost не принимает `cat_features` в
    float-матрице (конвенция v2).
    """
    return np.array([[row.features[name] for name in names] for row in rows], dtype=object)


def fit_direct(train_rows: list, names: list, horizon: int, **overrides) -> CatBoostRegressor:
    """CatBoost direct на h-таргете; `overrides` — только для тестов."""
    params = {**catboost_params(names), **overrides}
    model = CatBoostRegressor(**params)
    model.fit(
        feature_matrix(train_rows, names),
        np.asarray([row.targets[horizon] for row in train_rows], dtype=float),
    )
    return model


def predict_direct(model: CatBoostRegressor, rows: list, names: list) -> list:
    """Прогноз модели по строкам, по возрастанию месяца."""
    values = model.predict(feature_matrix(rows, names))
    return [float(value) for value in values]


# ---------------------------------------------------------------------------
# Метрики: бутстрап, TOST, DM
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BootstrapResult:
    """Парный блочный бутстрап: Δ и ratio, 90% CI и хвосты относительно margin."""

    delta: float
    ratio: float
    delta_low: float
    delta_high: float
    ratio_low: float
    ratio_high: float
    tail_low: float
    tail_high: float


def _quantile(sorted_values: list, quantile: float) -> float:
    """Порядковая статистика без интерполяции (конвенция `eval_utils`)."""
    size = len(sorted_values)
    if quantile <= 0.5:
        return sorted_values[int(quantile * size)]
    return sorted_values[min(size - 1, math.ceil(quantile * size) - 1)]


def block_bootstrap_delta_ratio(
    err_ours: list,
    err_cb: list,
    margin: float,
    block: int = BOOTSTRAP_BLOCK,
    n: int = BOOTSTRAP_N,
    seed: int = SEED,
) -> BootstrapResult:
    """Парный блочный бутстрап разницы RMSE и отношения RMSE.

    Блоки длиной `block` берутся одними и теми же индексами из обоих рядов
    (парность), блоки циклические, последний обрезается до размера выборки.
    Квантили — по порядковым статистикам: 5% и 95% (90% CI), как в
    `eval_utils.block_bootstrap_rmse_diff` для 95%.
    """
    if len(err_ours) != len(err_cb):
        raise ValueError(f"бутстрап: {len(err_ours)} ошибок против {len(err_cb)}")
    if not err_ours:
        raise ValueError("бутстрап: пустой ряд ошибок")
    if block < 1:
        raise ValueError("бутстрап: блок меньше одного месяца")
    if n < 1:
        raise ValueError("бутстрап: ресэмплов меньше одного")

    size = len(err_ours)
    blocks_per_sample = -(-size // block)
    generator = random.Random(seed)
    deltas = []
    ratios = []
    for _ in range(n):
        indices = []
        for _ in range(blocks_per_sample):
            start = generator.randrange(size)
            indices.extend((start + offset) % size for offset in range(block))
        del indices[size:]
        squared_ours = sum(err_ours[index] * err_ours[index] for index in indices)
        squared_cb = sum(err_cb[index] * err_cb[index] for index in indices)
        rmse_ours = math.sqrt(squared_ours / size)
        rmse_cb = math.sqrt(squared_cb / size)
        deltas.append(rmse_ours - rmse_cb)
        ratios.append(rmse_ours / rmse_cb if rmse_cb > 0.0 else math.inf)
    deltas.sort()
    ratios.sort()
    rmse_ours = eu.rmse(err_ours)
    rmse_cb = eu.rmse(err_cb)
    return BootstrapResult(
        delta=rmse_ours - rmse_cb,
        ratio=rmse_ours / rmse_cb if rmse_cb > 0.0 else math.inf,
        delta_low=_quantile(deltas, 0.05),
        delta_high=_quantile(deltas, 0.95),
        ratio_low=_quantile(ratios, 0.05),
        ratio_high=_quantile(ratios, 0.95),
        tail_low=sum(1 for value in deltas if value <= -margin) / n,
        tail_high=sum(1 for value in deltas if value >= margin) / n,
    )


@dataclass(frozen=True)
class TostResult:
    """TOST: 90% CI разницы RMSE внутри ±δ."""

    delta: float
    margin: float
    low: float
    high: float
    tail_low: float
    tail_high: float
    equivalent: bool


def tost(bootstrap: BootstrapResult, margin: float) -> TostResult:
    """Критерий эквивалентности: `−δ < CI_low` и `CI_high < +δ`.

    Хвостовые вероятности `tail_low/tail_high` — описательные: это доля
    бутстрап-реплик за margin, а не формальный p-value (спека §5.1).
    """
    return TostResult(
        delta=bootstrap.delta,
        margin=margin,
        low=bootstrap.delta_low,
        high=bootstrap.delta_high,
        tail_low=bootstrap.tail_low,
        tail_high=bootstrap.tail_high,
        equivalent=bootstrap.delta_low > -margin and bootstrap.delta_high < margin,
    )


def horizon_verdict(dm_p: float, delta: float, tost_result: TostResult) -> str:
    """Вердикт по горизонту (спека §5.3)."""
    if dm_p <= 0.05 and delta > 0.0:
        return "ours worse"
    if dm_p <= 0.05 and delta < 0.0:
        return "ours better"
    if dm_p > 0.05 and tost_result.equivalent:
        return "equivalent"
    return "inconclusive"


def overall_verdict(results: list, model: str) -> str:
    """«близок» — все основные горизонты модели `equivalent`, иначе «не близок»."""
    primary = [item for item in results if item["model"] == model and item["horizon"] in HORIZONS]
    if len(primary) != len(HORIZONS):
        raise ValueError(f"вердикт: не все основные горизонты посчитаны для {model}")
    return "близок" if all(item["verdict"] == "equivalent" for item in primary) else "не близок"


def evaluate_horizon(
    rows: list,
    predictions: list,
    cb_errors: list,
    horizon: int,
    margin: float,
    model: str,
) -> dict:
    """Сравнение одной модели с CB_COMP на общих normal-месяцах."""
    ours_errors = [prediction - row.targets[horizon] for row, prediction in zip(rows, predictions)]
    rmse_ours = eu.rmse(ours_errors)
    rmse_cb = eu.rmse(cb_errors)
    dm_stat_h1, dm_p_h1 = eu.diebold_mariano(ours_errors, cb_errors, 1)
    dm_stat_h, dm_p_h = eu.diebold_mariano(ours_errors, cb_errors, horizon)
    bootstrap = block_bootstrap_delta_ratio(ours_errors, cb_errors, margin)
    tost_result = tost(bootstrap, margin)
    verdict = horizon_verdict(dm_p_h1, bootstrap.delta, tost_result)
    return {
        "model": model,
        "horizon": horizon,
        "n": len(rows),
        "rmse_ours": rmse_ours,
        "rmse_cb": rmse_cb,
        "mae_ours": eu.mae(ours_errors),
        "mae_cb": eu.mae(cb_errors),
        "delta": bootstrap.delta,
        "ratio": bootstrap.ratio,
        "margin": margin,
        "delta_ci90": [bootstrap.delta_low, bootstrap.delta_high],
        "ratio_ci90": [bootstrap.ratio_low, bootstrap.ratio_high],
        "dm_stat_h1": dm_stat_h1,
        "dm_p_h1": dm_p_h1,
        "dm_stat_h": dm_stat_h,
        "dm_p_h": dm_p_h,
        "tost_equivalent": tost_result.equivalent,
        "tost_tail_low": tost_result.tail_low,
        "tost_tail_high": tost_result.tail_high,
        "verdict": verdict,
    }


# ---------------------------------------------------------------------------
# Артефакты
# ---------------------------------------------------------------------------


def _format(value: float, digits: int = 4) -> str:
    return f"{value:.{digits}f}"


def write_report(path: Path, payload: dict) -> None:
    """Markdown-отчёт прогона: дизайн, архив, выборка, таблицы, вердикт."""
    design = payload["design"]
    archive = payload["archive"]
    sample = payload["sample"]
    results = payload["results"]
    lines = []
    lines.append("# Паритет nowcast v2 с CB_COMP: отчёт прогона")
    lines.append("")
    lines.append(
        "> Автоматический отчёт `scripts/cb_comp_parity.py`; пункт "
        "`key-rate/phase-4-ml-cb-comp-parity` (issue #190). Числа не редактируются "
        "руками — интерпретация в `docs/research/2026-10-18-cb-comp-parity.md`."
    )
    lines.append("")
    lines.append("## 1. Предзарегистрированный дизайн")
    lines.append("")
    lines.append(f"- Train: {design['train_start']} … {design['train_end']} (факт. {sample['train_actual']})")
    lines.append(f"- Test: {design['test_start']} … {design['test_end']}; normal-месяцев: {sample['n_normal']}")
    lines.append(f"- Исключены из теста: 2020-04, 2022-01…2022-12 (как в статье)")
    lines.append(f"- Seed: {design['seed']}; CatBoost: дефолты, `month` категориальным")
    lines.append(f"- Margin: δ_h = {design['margin_fraction']:.0%} RMSE CB_COMP_h")
    lines.append(f"- Бутстрап: парный блочный, блок {design['bootstrap_block']}, n={design['bootstrap_n']}")
    lines.append("")
    lines.append("## 2. Проверки архива (до обучения)")
    lines.append("")
    lines.append("| h | RMSE CB_COMP | пин статьи | δ = 10% |")
    lines.append("|---|---:|---:|---:|")
    for horizon in HORIZONS:
        lines.append(
            f"| {horizon} | {_format(archive['rmse_cb'][str(horizon)], 6)} | "
            f"{_format(design['cb_rmse_pinned'][str(horizon)], 6)} | "
            f"{_format(archive['margins'][str(horizon)], 6)} |"
        )
    for horizon in HORIZONS_EXTRA:
        lines.append(
            f"| {horizon} (доп.) | {_format(archive['extra_rmse_cb'][str(horizon)], 6)} | — | "
            f"{_format(archive['margins'][str(horizon)], 6)} |"
        )
    lines.append("")
    lines.append(
        f"- `TARGET_FULL[h]` vs накопление `TARGET_FULL[1]` (h2…h6): "
        f"max |diff| = {archive['target_max_diff']:.1e}"
    )
    lines.append(
        f"- `StfCpiCB` vs `catboostVCB_IBK.xlsx`: max |diff| = {archive['max_source_diff']:.1e}"
    )
    lines.append("- Normal-флаг `Учитывать` совпал с правилом статьи (54 месяца)")
    lines.append("- DM авторов воспроизведён (квадраты ошибок в `dm.test`):")
    lines.append("")
    lines.append("| h | наш пересчёт | архив |")
    lines.append("|---|---:|---:|")
    for horizon in HORIZONS:
        item = archive["archive_dm"][str(horizon)]
        lines.append(f"| {horizon} | {_format(item['computed'], 6)} | {_format(item['archive'], 6)} |")
    lines.append("")
    lines.append("## 3. Выборка")
    lines.append("")
    lines.append(
        f"- Train: {sample['n_train']} мес. ({sample['train_actual']}); "
        f"Test: {sample['n_test']} мес.; normal: {sample['n_normal']}"
    )
    lines.append(
        "- Из train исключены выбросы статьи: "
        + ", ".join(sample["train_outliers_excluded"])
        + " (у CB_COMP факт train — 102 месяца)"
    )
    lines.append(f"- Пропущено при сборке признаков: {sample['n_skipped']} мес.")
    if sample["skipped_reasons"]:
        lines.append("")
        lines.append("| Причина | Месяцев |")
        lines.append("|---|---:|")
        for reason, count in sample["skipped_reasons"]:
            lines.append(f"| {reason} | {count} |")
    lines.append("")
    for position, (model, title) in enumerate(((MODEL_MP, "с МП"), (MODEL_NOMP, "без МП")), start=1):
        primary = [item for item in results if item["model"] == model and item["horizon"] in HORIZONS]
        lines.append(f"## 4.{position}. {model} ({title}) — основные горизонты")
        lines.append("")
        lines.append(
            "| h | RMSE ours | RMSE CB | ratio | ratio 90% CI | Δ | Δ 90% CI | δ | "
            "DM p (h=1) | DM p (h=h) | TOST | вердикт |"
        )
        lines.append("|---|---:|---:|---:|---|---:|---|---:|---:|---:|---|---|")
        for item in primary:
            ratio_ci = f"[{_format(item['ratio_ci90'][0], 3)}; {_format(item['ratio_ci90'][1], 3)}]"
            delta_ci = f"[{_format(item['delta_ci90'][0], 4)}; {_format(item['delta_ci90'][1], 4)}]"
            lines.append(
                f"| {item['horizon']} | {_format(item['rmse_ours'], 6)} | "
                f"{_format(item['rmse_cb'], 6)} | {_format(item['ratio'], 3)} | {ratio_ci} | "
                f"{_format(item['delta'], 4)} | {delta_ci} | {_format(item['margin'], 4)} | "
                f"{_format(item['dm_p_h1'], 4)} | {_format(item['dm_p_h'], 4)} | "
                f"{'да' if item['tost_equivalent'] else 'нет'} | {item['verdict']} |"
            )
        lines.append("")
        lines.append(f"**Вердикт по модели: {payload['overall'][model]}**")
        lines.append("")
    extras = [item for item in results if item["horizon"] in HORIZONS_EXTRA]
    if extras:
        lines.append("## 5. Дополнительные горизонты (h4/h5)")
        lines.append("")
        lines.append("| Модель | h | RMSE ours | RMSE CB | ratio | Δ 90% CI | DM p (h=1) | вердикт |")
        lines.append("|---|---:|---:|---:|---:|---|---:|---|")
        for item in extras:
            lines.append(
                f"| {item['model']} | {item['horizon']} | {_format(item['rmse_ours'], 6)} | "
                f"{_format(item['rmse_cb'], 6)} | {_format(item['ratio'], 3)} | "
                f"[{_format(item['delta_ci90'][0], 4)}; {_format(item['delta_ci90'][1], 4)}] | "
                f"{_format(item['dm_p_h1'], 4)} | {item['verdict']} |"
            )
        lines.append("")
    lines.append("## 6. Ограничения прогона")
    lines.append("")
    lines.append(
        f"- Train — {sample['train_actual']} ({sample['n_train']} мес.), "
        "предзарегистрированное окно статьи. `key_rate` (ряд с 2013-09-17) и "
        "`usd_mm` (дневной USD с 2013-01-10) до 2013 г. — NaN: long-sample "
        "политика v2 (§8.2) не выбрасывает такие месяцы, CatBoost обрабатывает "
        "NaN нативно; окно не расширялось прокси и не сокращалось по результату."
    )
    lines.append("- CB_COMP не переобучался: использованы архивные per-period прогнозы.")
    lines.append("- Наша модель — direct, не bottom-up (таргет SA ИПЦ без КУ не собирается из публичных компонент).")
    lines.append(
        "- Наш DM-HLN — на квадратах ошибок; архивные p-value авторов — на четвёртых "
        "степенях (особенность их `dm.test`); p-value напрямую не сравниваются."
    )
    lines.append("- Винтажи не учитываются — общий ограничитель с авторами.")
    lines.append("- h4/h5 — дополнительные: в таблице статьи их нет.")
    lines.append("")
    lines.append("## 7. Что не проверено")
    lines.append("")
    lines.append("- Полный retrain CB_COMP (R/CatBoost/Windows) не выполнялся и для паритета не требуется.")
    lines.append("- Прокси-таргет (SA ИПЦ без КУ из публичных рядов) в этом прогоне не строился.")
    lines.append("- Вклад отдельных МП-рядов не декомпозировался: сравниваются два информационных множества.")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def write_predictions_csv(path: Path, rows: list) -> None:
    """Построчные прогнозы: наши модели (train+test) и CB_COMP (test)."""
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            ["month", "horizon", "model", "sample", "normal", "y_true", "y_pred", "error"]
        )
        for item in rows:
            writer.writerow(
                [
                    item["month"],
                    item["horizon"],
                    item["model"],
                    item["sample"],
                    item["normal"],
                    f"{item['y_true']:.10f}",
                    f"{item['y_pred']:.10f}",
                    f"{item['error']:.10f}",
                ]
            )


def write_result_json(path: Path, payload: dict) -> None:
    """Машинный результат: дизайн, архив, выборка, все горизонты, вердикт."""
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def run(archive: Path, out_dir: Path, train_start: date = TRAIN_START, test_end: date = TEST_END) -> dict:
    """Полный прогон: архив → margin → признаки → модели → метрики → артефакты."""
    if train_start != TRAIN_START or test_end != TEST_END:
        print(
            f"ВНИМАНИЕ: окно переопределено (train_start={train_start}, test_end={test_end}); "
            "предзарегистрированные значения — в спеке §5.2",
            flush=True,
        )

    print(f"Архив: {archive}", flush=True)
    archive_data = check_archive(archive)
    print("Проверки архива пройдены; margin зафиксирован до обучения:", flush=True)
    for horizon in sorted(archive_data["margins"]):
        print(
            f"  h{horizon}: RMSE CB_COMP = {archive_data['margins'][horizon] / MARGIN_FRACTION:.6f}, "
            f"δ = {archive_data['margins'][horizon]:.6f}",
            flush=True,
        )

    feature_rows, skipped = load_feature_rows()
    parity_rows, missing = build_rows(feature_rows, archive_data["target"])
    assert_no_leakage(parity_rows)
    train_rows, test_rows = split_rows(parity_rows)
    if not train_rows or not test_rows:
        raise SystemExit("пустой train или test: проверить признаки и окно")
    print(
        f"Выборка: train {len(train_rows)} ({train_rows[0].month}…{train_rows[-1].month}), "
        f"test {len(test_rows)} ({test_rows[0].month}…{test_rows[-1].month}), "
        f"пропущено {len(skipped)} + {len(missing)}",
        flush=True,
    )

    normal = archive_data["normal"]
    normal_set = set(normal)
    test_by_month = {row.month: row for row in test_rows}
    for month in normal:
        if month not in test_by_month:
            raise SystemExit(f"normal-месяц {month} отсутствует в тестовой выборке")

    results = []
    prediction_rows = []
    for horizon in ALL_HORIZONS:
        cb_forecasts = archive_data["sheet"] if horizon in HORIZONS else archive_data["xlsx"]
        cb_errors = _cb_errors(cb_forecasts, archive_data["target"], horizon, normal)
        margin = archive_data["margins"][horizon]

        for model_name, with_mp in ((MODEL_MP, True), (MODEL_NOMP, False)):
            names = cm.feature_names(with_mp=with_mp)
            model = fit_direct(train_rows, names, horizon)
            train_predictions = predict_direct(model, train_rows, names)
            test_predictions = predict_direct(model, test_rows, names)
            normal_rows = [test_by_month[month] for month in normal]
            normal_predictions = [
                prediction
                for row, prediction in zip(test_rows, test_predictions)
                if row.month in normal_set
            ]
            result = evaluate_horizon(
                normal_rows, normal_predictions, cb_errors, horizon, margin, model_name
            )
            results.append(result)
            print(
                f"  {model_name} h{horizon}: RMSE {result['rmse_ours']:.6f} vs CB "
                f"{result['rmse_cb']:.6f}, DM p={result['dm_p_h1']:.4f}, "
                f"TOST {'да' if result['tost_equivalent'] else 'нет'} → {result['verdict']}",
                flush=True,
            )

            for row, prediction in zip(train_rows, train_predictions):
                prediction_rows.append(
                    {
                        "month": row.month.isoformat(),
                        "horizon": horizon,
                        "model": model_name,
                        "sample": "train",
                        "normal": 0,
                        "y_true": row.targets[horizon],
                        "y_pred": prediction,
                        "error": prediction - row.targets[horizon],
                    }
                )
            for row, prediction in zip(test_rows, test_predictions):
                prediction_rows.append(
                    {
                        "month": row.month.isoformat(),
                        "horizon": horizon,
                        "model": model_name,
                        "sample": "test",
                        "normal": 1 if row.month in normal_set else 0,
                        "y_true": row.targets[horizon],
                        "y_pred": prediction,
                        "error": prediction - row.targets[horizon],
                    }
                )
        for month, error in zip(normal, cb_errors):
            row = test_by_month[month]
            prediction_rows.append(
                {
                    "month": month.isoformat(),
                    "horizon": horizon,
                    "model": CB_MODEL,
                    "sample": "test",
                    "normal": 1,
                    "y_true": row.targets[horizon],
                    "y_pred": row.targets[horizon] + error,
                    "error": error,
                }
            )

    overall = {model: overall_verdict(results, model) for model in (MODEL_MP, MODEL_NOMP)}
    skipped_reasons = {}
    for _, reason in skipped + missing:
        skipped_reasons[reason] = skipped_reasons.get(reason, 0) + 1
    skipped_reasons = sorted(skipped_reasons.items(), key=lambda item: (-item[1], item[0]))

    payload = {
        "design": {
            "train_start": TRAIN_START.isoformat(),
            "train_end": TRAIN_END.isoformat(),
            "test_start": TEST_START.isoformat(),
            "test_end": TEST_END.isoformat(),
            "seed": SEED,
            "horizons": list(HORIZONS),
            "horizons_extra": list(HORIZONS_EXTRA),
            "margin_fraction": MARGIN_FRACTION,
            "bootstrap_block": BOOTSTRAP_BLOCK,
            "bootstrap_n": BOOTSTRAP_N,
            "cb_rmse_pinned": {str(key): value for key, value in CB_RMSE_PINNED.items()},
        },
        "archive": {
            "path": str(archive),
            "rmse_cb": {str(key): value for key, value in archive_data["rmse_cb"].items()},
            "extra_rmse_cb": {
                str(key): value for key, value in archive_data["extra_rmse_cb"].items()
            },
            "margins": {str(key): value for key, value in archive_data["margins"].items()},
            "max_source_diff": archive_data["max_source_diff"],
            "target_max_diff": archive_data["target_max_diff"],
            "archive_dm": {
                str(key): value for key, value in archive_data["archive_dm"].items()
            },
        },
        "sample": {
            "n_train": len(train_rows),
            "train_actual": f"{train_rows[0].month.isoformat()}…{train_rows[-1].month.isoformat()}",
            "n_test": len(test_rows),
            "n_normal": len(normal),
            "n_skipped": len(skipped) + len(missing),
            "skipped_reasons": [[reason, count] for reason, count in skipped_reasons],
            "train_outliers_excluded": [
                month.isoformat() for month in sorted(TRAIN_OUTLIER_MONTHS)
            ],
        },
        "results": results,
        "overall": overall,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    write_report(out_dir / ARTIFACT_REPORT, payload)
    write_predictions_csv(out_dir / ARTIFACT_PREDICTIONS, prediction_rows)
    write_result_json(out_dir / ARTIFACT_RESULT, payload)
    print(
        f"Вердикт: {MODEL_MP} — {overall[MODEL_MP]}; {MODEL_NOMP} — {overall[MODEL_NOMP]}",
        flush=True,
    )
    print(f"Артефакты: {out_dir / ARTIFACT_REPORT}, {out_dir / ARTIFACT_PREDICTIONS}, {out_dir / ARTIFACT_RESULT}", flush=True)
    return payload


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Паритет ML-трека v2 с CB_COMP на общем таргете и окне (issue #190)"
    )
    parser.add_argument(
        "--archive",
        default=None,
        help="каталог BusinessSurveyCBRCpiForecastingSourceCode (иначе env CB_COMP_ARCHIVE/известные пути)",
    )
    parser.add_argument(
        "--out-dir",
        default=str(ROOT),
        help="каталог артефактов (по умолчанию корень worktree)",
    )
    parser.add_argument(
        "--train-start",
        default=TRAIN_START.isoformat(),
        help=f"переопределение train start для отладки (по умолчанию {TRAIN_START})",
    )
    parser.add_argument(
        "--test-end",
        default=TEST_END.isoformat(),
        help=f"переопределение test end для отладки (по умолчанию {TEST_END})",
    )
    arguments = parser.parse_args(argv)
    archive = resolve_archive(arguments.archive)
    run(
        archive=archive,
        out_dir=Path(arguments.out_dir),
        train_start=date.fromisoformat(arguments.train_start),
        test_end=date.fromisoformat(arguments.test_end),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
