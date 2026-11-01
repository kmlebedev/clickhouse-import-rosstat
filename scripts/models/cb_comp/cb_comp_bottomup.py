#!/usr/bin/env python3
"""Bottom-up parity с CB_COMP: компонентные CatBoost-модели и агрегат СПР.

Предзарегистрированный вариант пункта `key-rate/phase-4-ml-cb-comp-parity`
(issue #190), спека §4.1. Таргет, окно, normal-месяцы, margin и критерий —
те же, что у direct-модели; меняется только архитектура:

* **компонентный таргет** — 88 групп `model.xlsx` (`1.0.4.2`, без агрегатов
  `1/6/7/9000`), `TARGET_j,h(M) = (exp(Σ diff_j(M−i))−1)·100`, где `diff` —
  месячный лог-прирост из `data_panel_bi_sa.fst`; ЖКУ `9470` в панели нет и в
  компоненты не входит (вес публикуется как проверка);
* **признаки** — компонентные лаги `y_lag1`/`y_lag2` + v2-макро
  (`usd_mm`, `key_rate`, `fao_mm`) + `month`; режимы МП: `mean` — 6
  усреднённых признаков, `per_bsu` — 6 на каждый BSU-код, `per_sheet` — 6 на
  каждый уникальный отраслевой лист публичной книги (live-диагностика),
  `full` — плюс кредит/ставки/USD/тренд CPI; 16 групп без BSU — без МП;
* **веса** — `cpi_weights_tovar.xlsx` (Россия, `окато_код=643`) +
  `dictionary_cpi.xlsx`: листовые веса (`уровень=7`) агрегируются по уровню
  группы; для месяца M — год `M.year−1`; нормировка на группы с прогнозом и
  весом;
* **агрегат** — `Σ w_j·ŷ_j / Σ w_j`; `bottomup_mp` — MP для 72 групп с BSU и
  no-MP для 16, `bottomup_nomp` — no-MP для всех 88;
* **гейт сборки** — агрегат фактических компонентных таргетов сравнивается с
  `TARGET_FULL`; при RMSE h1 > 0.10 п.п. или покрытии < 0.85 bottom-up
  считается несобранным, и причина фиксируется (таргет не подменяется).

Модуль ничего не пишет в ClickHouse и не импортирует `cb_comp_parity`
(parity импортирует его, а не наоборот). Для живого прогона нужен `Rscript` с
пакетом `fst`: панель экспортируется `scripts/models/cb_comp/cb_comp_panel_export.R` в
`build/cb-comp-parity/` (gitignored). Тесты работают на синтетических
DataFrame и R не требуют.

Запуск — только из `make cb-comp-parity`; отдельной цели нет.
"""

import bisect
import math
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from models.cpi import cpi_monthly as cm

SEED = 20261018
HORIZONS = (1, 2, 3, 6)
AGGREGATE_CODES = frozenset({"1", "6", "7", "9000"})
UTILITIES_CODE = "9470"
MIN_TRAIN_ROWS = 20
# Гейт сборки данных (спека §4.1): ниже — bottom-up не публикуется.
RECONSTRUCTION_RMSE_H1_MAX = 0.10
COVERAGE_MIN = 0.85

MODEL_MP = "bottomup_mp"
MODEL_NOMP = "bottomup_nomp"

# Панель из FST — локальный артефакт (build/ gitignored), не вход репозитория.
PANEL_DIR = ROOT / "build" / "cb-comp-parity"

BASE_FEATURES = ("y_lag1", "y_lag2", "usd_mm", "key_rate", "fao_mm", "month")
MP_FEATURES = (
    "mp_ibk",
    "mp_ibk_chg",
    "mp_costs",
    "mp_costs_chg",
    "mp_price_exp",
    "mp_price_exp_chg",
)

MP_INDICATORS = {
    "Indikator biznes-klimata Banka Rossii (IBK)": "ibk",
    "Izmeneniye izderzhek proizvodstva (obrashcheniya): balans otvetov": "costs",
    "Ozhidaniya izmeneniya tsen na gotovuyu produktsiyu (uslugi) predpriyatiy: balans otvetov": "price_exp",
}

# Режимы признаков (спека §3): mean — baseline #190; per_bsu — primary;
# per_sheet — live-диагностика на публичной панели (11 листов × 3 индикатора);
# full — robustness с кредитом/ставками/USD/трендом CPI.
FEATURE_MODES = ("mean", "per_bsu", "per_sheet", "full")
DEFAULT_FEATURE_MODE = "mean"

FULL_FEATURES = (
    "cpi_trend",
    "cpi_trend_dev",
    "usd_level",
    "usd_chg",
    "miacr",
    "miacr_chg",
    "debt_mortgage",
)

# Полная панель `panel.csv`: (group_stat, code, kind) → as-of ряд.
# В публичной панели ставка одна (`miacr`), её уровень лежит в kind
# `interest_rate`, изменение — в `diff`; отдельного ряда `interest_rate` нет.
FULL_PANEL_KEYS = {
    "usd_level": ("Рыночные индексы", "usd_rub", "base_index"),
    "usd_chg": ("Рыночные индексы", "usd_rub", "diff"),
    "miacr": ("Кредитные ставки", "miacr", "interest_rate"),
    "miacr_chg": ("Кредитные ставки", "miacr", "diff"),
    "debt_mortgage": ("Кредит населению", "debt_households_wh_mortgage", "base_index"),
}

CATBOOST_DEFAULTS = {
    "loss_function": "RMSE",
    "random_seed": SEED,
    "verbose": False,
    "allow_writing_files": False,
}


# ---------------------------------------------------------------------------
# Коды, месяцы, as-of
# ---------------------------------------------------------------------------


def normalize_code(value) -> str:
    """Код группы/ОКВЭД строкой: 803.0 → «803», 9300->930.АГ → «930.АГ»."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if "->" in text:
        text = text.split("->")[-1]
    return text


def month_start(value) -> date:
    if isinstance(value, datetime):
        return date(value.year, value.month, 1)
    if isinstance(value, date):
        return date(value.year, value.month, 1)
    return date.fromisoformat(str(value)[:10]).replace(day=1)


def shift_month(day: date, months: int) -> date:
    index = day.year * 12 + (day.month - 1) + months
    return date(index // 12, index % 12 + 1, 1)


def month_end(day: date) -> date:
    return shift_month(day, 1) - timedelta(days=1)


def previous_month(day: date) -> date:
    return shift_month(day, -1)


def mp_available(point_date: date, publication_lag: int) -> date:
    """Публикация точки МП: конец месяца `point_date + lag`."""
    return month_end(shift_month(point_date, int(publication_lag)))


# ---------------------------------------------------------------------------
# Спецификация групп, веса, панель
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GroupSpec:
    """Группа ИПЦ: код, BSU-коды (пусто — группа без МП), уровень иерархии.

    `sheet_slugs` заполняется только для режима `per_sheet`: уникальные
    отраслевые листы публичной книги, в которые попадают BSU-коды группы.
    """

    code: str
    bsu_codes: tuple
    level: int
    sheet_slugs: tuple = ()


@dataclass
class BottomUpResult:
    """Результат bottom-up: статус, прогнозы и метаданные для отчёта."""

    status: str
    reason: str
    predictions: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)


def model_path(archive: Path) -> Path:
    return archive / "Models/CpiComponents/data/model/model.xlsx"


def dictionary_path(archive: Path) -> Path:
    return archive / "Models/CpiComponents/StfCpi_External_data/dictionaries/dictionary_cpi.xlsx"


def weights_path(archive: Path) -> Path:
    return archive / "Models/CpiComponents/StfCpi_External_data/databases/weights/cpi_weights_tovar.xlsx"


def utilities_path(archive: Path) -> Path:
    return archive / "Models/CpiComponents/data/Коммунальные услуги прогноз MoM_sa.xlsx"


def load_specs(archive: Path, sheet_map: dict = None) -> list:
    """88 компонентных групп `model.xlsx` и их BSU-коды.

    `используется=True`, агрегаты `1/6/7/9000` исключены. BSU-коды берутся из
    `мониторинг_код1..4`; `<==>`/NaN — нет привязки. Уровень — минимальный
    уровень, на котором код встречается в иерархии `dictionary_cpi` (2024):
    нужен для выбора агрегированного веса.

    `sheet_map` (`код ОКВЭД2 → industry_slug`) передаётся только для режима
    `per_sheet`; тогда `GroupSpec.sheet_slugs` — уникальные листы группы.
    Пропуск кода в карте — ошибка, а не молчаливое выпадение листа.
    """
    model = pd.read_excel(model_path(archive), sheet_name="1.0.4.2", header=4)
    used = model[model["используется"].astype(str).str.lower().isin(["true", "1"])]
    dictionary = pd.read_excel(dictionary_path(archive), sheet_name="2024")
    level_map = _level_map(dictionary)
    specs = []
    for _, row in used.iterrows():
        code = normalize_code(row["код_товара_услуги"])
        if not code or code in AGGREGATE_CODES:
            continue
        bsu = []
        for column in ("мониторинг_код1", "мониторинг_код2", "мониторинг_код3", "мониторинг_код4"):
            value = row.get(column)
            text = normalize_code(value) if not pd.isna(value) else ""
            if text and text != "<==>":
                bsu.append(text)
        level = level_map.get(code)
        if level is None:
            raise ValueError(f"группа {code}: нет уровня в dictionary_cpi")
        sheet_slugs = ()
        if sheet_map is not None:
            missing = [bsu_code for bsu_code in bsu if bsu_code not in sheet_map]
            if missing:
                raise ValueError(
                    f"группа {code}: BSU-коды без industry_slug: {missing}"
                )
            sheet_slugs = tuple(sorted({sheet_map[bsu_code] for bsu_code in bsu}))
        specs.append(
            GroupSpec(
                code=code,
                bsu_codes=tuple(sorted(set(bsu))),
                level=level,
                sheet_slugs=sheet_slugs,
            )
        )
    if not specs:
        raise ValueError("model.xlsx: не найдено ни одной компонентной группы")
    return specs


def _level_map(dictionary: pd.DataFrame) -> dict:
    """Код → минимальный уровень иерархии (1…7) в dictionary_cpi."""
    level_map = {}
    for _, row in dictionary.iterrows():
        for level in range(1, 8):
            code = normalize_code(row.get(f"уровень_{level}"))
            if not code or code == "<==>":
                continue
            level_map[code] = min(level_map.get(code, 99), level)
    return level_map


def _year_weights(archive: Path, year: int, specs: list, book=None) -> dict:
    """Веса групп за год: листья (уровень 7) агрегируются по уровню группы.

    Отсутствующий лист года — не ошибка: архив кончается 2024, а live-набор
    годов может тянуться до 2026. `book` передаётся, когда читается несколько
    лет подряд: файл открывается один раз.
    """
    if book is None:
        book = pd.ExcelFile(weights_path(archive))
    if str(year) not in book.sheet_names:
        return None
    weights = pd.read_excel(book, sheet_name=str(year))
    weights["code"] = weights["код_товара_услуги"].map(normalize_code)
    weights["okato"] = weights["окато_код"].map(normalize_code)
    weights = weights[weights["okato"] == "643"]
    dictionary = pd.read_excel(dictionary_path(archive), sheet_name=str(year))
    dictionary["code"] = dictionary["код_товара_услуги"].map(normalize_code)
    for level in range(1, 8):
        dictionary[f"уровень_{level}"] = dictionary[f"уровень_{level}"].map(normalize_code)
    merged = weights.merge(
        dictionary[["code", "уровень"] + [f"уровень_{i}" for i in range(1, 8)]],
        on="code",
        how="left",
    )
    merged = merged[merged["уровень"] == 7]
    by_level = {
        level: merged.groupby(f"уровень_{level}")["вес_товара"].sum()
        for level in range(1, 7)
    }
    level_map = _level_map(dictionary)
    result = {}
    for spec in specs:
        level = level_map.get(spec.code)
        if level is None:
            continue
        table = by_level.get(level)
        if table is not None and spec.code in table.index:
            result[spec.code] = float(table[spec.code])
    utilities_level = level_map.get(UTILITIES_CODE)
    if utilities_level and UTILITIES_CODE in by_level.get(utilities_level, {}).index:
        result[UTILITIES_CODE] = float(by_level[utilities_level][UTILITIES_CODE])
    return result


def load_archive_weights(archive: Path, years, specs: list) -> dict:
    """`{год: {код: доля}}` из архива CB_COMP; годы без листа пропускаются."""
    book = pd.ExcelFile(weights_path(archive))
    result = {}
    for year in years:
        table = _year_weights(archive, year, specs, book)
        if table is not None:
            result[int(year)] = table
    return result


LIVE_WEIGHTS_VIEW = "v_rosstat_cpi_weights"
LIVE_WEIGHTS_GROUP_FILE = "CPR_Gr-Tov-RF"
LIVE_WEIGHTS_ITEM_FILE = "CPR_tov_RF"
# Live-набор годов: архив кончается 2024, а для месяцев 2026 нужен вес 2025.
LIVE_WEIGHT_YEARS = tuple(range(2016, 2027))


def load_live_weights(years, specs: list, query=None) -> dict:
    """`{год: {код: доля}}` из витрины `v_rosstat_cpi_weights`.

    Витрина хранит проценты как в публикации, API модели — доли архива:
    поэтому `/100`. Групповой файл — приоритет (в нём опубликован вес группы),
    товарный — фолбэк; если оба дали разные значения для одного кода, это
    ошибка данных, а не молчаливый выбор. Отсутствующий код — отсутствие веса,
    а не ноль.
    """
    if query is None:
        from lib.clickhouse import ch_query as query

    years = sorted({int(year) for year in years})
    codes = sorted({spec.code for spec in specs} | {UTILITIES_CODE})
    if not years or not codes:
        return {}
    codes_sql = ", ".join("'" + code.replace("'", "''") + "'" for code in codes)
    rows = query(
        "SELECT year, code, source_file, weight FROM " + LIVE_WEIGHTS_VIEW + " "
        f"WHERE year IN ({', '.join(str(year) for year in years)}) "
        f"AND code IN ({codes_sql}) "
        "ORDER BY year, code, source_file FORMAT JSON"
    )
    by_year = {}
    for row in rows:
        year = int(row["year"])
        code = str(row["code"])
        source = str(row["source_file"])
        weight = float(row["weight"])
        by_year.setdefault(year, {}).setdefault(code, {})[source] = weight
    result = {}
    for year, table in by_year.items():
        result[year] = {}
        for code, files in table.items():
            group = files.get(LIVE_WEIGHTS_GROUP_FILE)
            item = files.get(LIVE_WEIGHTS_ITEM_FILE)
            if group is not None and item is not None and abs(group - item) > 1e-6:
                raise ValueError(
                    f"live weights: {year} {code}: группа {group} и товар {item} расходятся"
                )
            value = group if group is not None else item
            result[year][code] = value / 100.0
    return result


def load_weights(archive: Path, years, specs: list, source: str = "archive", query=None) -> dict:
    """`{год: {код: вес}}` для компонент и ЖКУ.

    `source="archive"` — прежний путь (архив CB_COMP 2016–2024), по умолчанию:
    исторические прогоны и пины воспроизводятся без ClickHouse.
    `source="live"` — витрина `v_rosstat_cpi_weights` (2016–2026). Переключение
    базовой модели на live — явным аргументом в вызывающем скрипте (Task 9,
    live bottom-up, gate); замороженные P0.6/Task 4/4b остаются на архиве.
    """
    if source == "live":
        return load_live_weights(years, specs, query=query)
    if source == "archive":
        return load_archive_weights(archive, years, specs)
    raise ValueError(f"неизвестный источник весов: {source!r} (ожидалось live|archive)")


def ensure_panel_csv(archive: Path, out_dir: Path, require_full: bool = False) -> tuple:
    """CSV панели из FST; при отсутствии — вызов Rscript-экспортёра.

    R-экспортёр — `scripts/models/cb_comp/cb_comp_panel_export.R`; он не входит в тесты и
    вызывается только живым прогоном. Если R/fst недоступны, исключение
    называется явно — панель не подменяется синтетикой.

    Возвращает `(cpi, mp, full)`; `full` — `panel.csv` (кредит, ставки,
    рыночные индексы, CPI trend/trend_deviation) или пустой DataFrame, если
    файла нет и режим не требует его.
    """
    cpi_csv = out_dir / "cpi_panel.csv"
    mp_csv = out_dir / "mp_panel.csv"
    full_csv = out_dir / "panel.csv"
    missing = not cpi_csv.exists() or not mp_csv.exists()
    if require_full and not full_csv.exists():
        missing = True
    if missing:
        exporter = ROOT / "scripts" / "models" / "cb_comp" / "cb_comp_panel_export.R"
        try:
            subprocess.run(
                ["Rscript", str(exporter), str(archive), str(out_dir)],
                check=True,
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as error:
            raise RuntimeError(
                "Rscript не найден: для bottom-up нужен R с пакетом fst; "
                "экспорт панели — scripts/models/cb_comp/cb_comp_panel_export.R"
            ) from error
        except subprocess.CalledProcessError as error:
            raise RuntimeError(
                f"экспорт панели упал: {error.stderr.strip() or error.stdout.strip()}"
            ) from error
    cpi = pd.read_csv(cpi_csv, parse_dates=["date"])
    mp = pd.read_csv(mp_csv, parse_dates=["date"])
    cpi["code"] = cpi["code"].map(normalize_code)
    mp["code"] = mp["code"].map(normalize_code)
    if full_csv.exists():
        full = pd.read_csv(full_csv, parse_dates=["date"])
        full["code"] = full["code"].map(normalize_code)
    else:
        full = pd.DataFrame(columns=["date", "group_stat", "code", "kind", "value", "lag"])
    return cpi, mp, full


def load_cpi_diff(cpi: pd.DataFrame) -> pd.DataFrame:
    """Пивот `diff` (месячный лог-прирост) по месяцам × группам."""
    diff = cpi[cpi["kind"] == "diff"]
    pivot = diff.pivot_table(index="date", columns="code", values="value", aggfunc="first")
    pivot.index = [month_start(value) for value in pivot.index]
    return pivot.sort_index()


def load_mp_panel(mp: pd.DataFrame) -> pd.DataFrame:
    """Только три индикатора МП, Россия, с короткими именами."""
    frame = mp[mp["kind"].isin(MP_INDICATORS)].copy()
    frame["indicator"] = frame["kind"].map(MP_INDICATORS)
    frame["date"] = [month_start(value) for value in frame["date"]]
    frame["lag"] = pd.to_numeric(frame["lag"], errors="coerce").fillna(1).astype(int)
    return frame.sort_values(["code", "indicator", "date"])


def load_sheet_map(path: Path) -> dict:
    """`код ОКВЭД2 → industry_slug` из CSV `v_cbr_mp_bsu_map`.

    Дубликат кода с разными листами и пустые поля — ошибка: молчаливая
    потеря листа исказила бы `per_sheet`-признаки.
    """
    # dtype=str обязателен: pandas иначе выводит `01.11` как float 1.11 и
    # теряет ведущий ноль, а с ним — связь с кодом ОКВЭД2.
    frame = pd.read_csv(path, dtype={"code": str})
    required = {"code", "industry_slug"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{path}: нет колонок {sorted(missing)}")
    result = {}
    for _, row in frame.iterrows():
        code = normalize_code(row["code"])
        sheet = str(row["industry_slug"]).strip()
        if not code or not sheet:
            raise ValueError(f"{path}: пустой код или industry_slug")
        if code in result and result[code] != sheet:
            raise ValueError(
                f"{path}: код {code} встречается с разными листами "
                f"{result[code]!r} и {sheet!r}"
            )
        result[code] = sheet
    if not result:
        raise ValueError(f"{path}: пустая карта кодов")
    return result


def load_public_panel(path: Path) -> pd.DataFrame:
    """Live-панель `v_cbr_mp_public_panel`: код, индикатор, месяц, значение.

    Панель публичной книги — 87 кодов, но значения внутри листа совпадают;
    проверка согласованности листа делается в `_sheet_lookup`, а не здесь:
    лоадер отвечает только за формат и отсутствие дублей.
    """
    # dtype=str: `01.11` не должен превращаться в 1.11 (см. load_sheet_map).
    frame = pd.read_csv(path, dtype={"code": str})
    required = {"code", "indicator", "date", "value"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{path}: нет колонок {sorted(missing)}")
    frame["code"] = frame["code"].map(normalize_code)
    frame["indicator"] = frame["indicator"].astype(str).str.strip()
    unknown = sorted(set(frame["indicator"]) - set(MP_INDICATORS.values()))
    if unknown:
        raise ValueError(f"{path}: неизвестные индикаторы {unknown}")
    frame["date"] = [month_start(value) for value in frame["date"]]
    frame["value"] = pd.to_numeric(frame["value"], errors="raise").astype(float)
    if frame[["code", "indicator", "date"]].duplicated().any():
        duplicates = frame[
            frame[["code", "indicator", "date"]].duplicated(keep=False)
        ].head(5)
        raise ValueError(f"{path}: дубли (code, indicator, date):\n{duplicates}")
    if frame["value"].isna().any():
        raise ValueError(f"{path}: NaN в value")
    return frame.sort_values(["code", "indicator", "date"])


def load_utilities(archive: Path) -> pd.DataFrame:
    """ЖКУ (`9470`) из файла авторов: дата, SA MoM, факт/прогноз.

    В компонентный агрегат ЖКУ не входит (его нет в панели и в 88 группах);
    файл читается, чтобы подтвердить код и опубликовать его вес.
    """
    frame = pd.read_excel(utilities_path(archive), sheet_name="Прогноз")
    frame["date"] = [month_start(value) for value in frame["дата"]]
    frame = frame.rename(columns={"Россия": "utilities_mom_sa", "факт_прогноз": "kind"})
    return frame[["date", "utilities_mom_sa", "kind"]].sort_values("date")


# ---------------------------------------------------------------------------
# Компонентный таргет и признаки
# ---------------------------------------------------------------------------


def component_targets(cpi_diff: pd.DataFrame, horizons=HORIZONS) -> dict:
    """`{h: DataFrame}` накопленного прироста из месячных лог-приростов.

    `TARGET_h(M) = (exp(Σ_{i=0..h−1} diff(M−i)) − 1)·100`; первые h−1 строк
    остаются NaN (истории нет), NaN внутри окна не заполняется.
    """
    targets = {}
    for horizon in horizons:
        rolled = cpi_diff.rolling(horizon).sum()
        targets[horizon] = (np.exp(rolled) - 1.0) * 100.0
    return targets


def base_features(
    spec: GroupSpec,
    month: date,
    cpi_diff: pd.DataFrame,
    macro: dict,
) -> tuple:
    """Базовые признаки группы или `(None, причина)`.

    Компонентные лаги — `100·diff` за M−1/M−2 (лог-прирост в процентах);
    макро — v2 (`usd_mm`, `key_rate`, `fao_mm`), одинаковые для всех групп.
    """
    lag1_month = previous_month(month)
    lag2_month = previous_month(lag1_month)
    if lag1_month not in cpi_diff.index or lag2_month not in cpi_diff.index:
        return None, "нет месяца компонентного лага"
    lag1 = cpi_diff.at[lag1_month, spec.code]
    lag2 = cpi_diff.at[lag2_month, spec.code]
    if pd.isna(lag1) or pd.isna(lag2):
        return None, "нет компонентного лага M−1/M−2"
    macro_row = macro.get(month, {})
    return {
        "y_lag1": float(lag1) * 100.0,
        "y_lag2": float(lag2) * 100.0,
        "usd_mm": macro_row.get("usd_mm", float("nan")),
        "key_rate": macro_row.get("key_rate", float("nan")),
        "fao_mm": macro_row.get("fao_mm", float("nan")),
        "month": month.month,
    }, ""


def feature_code(code: str) -> str:
    """BSU-код в имени признака: `.`/`-`/пробел → `_` (без коллизий на практике)."""
    return str(code).replace(".", "_").replace("-", "_").replace(" ", "_")


def _mp_lookup(mp_panel: pd.DataFrame) -> dict:
    """`(код, индикатор) → (доступные даты, значения)` для bisect-as-of."""
    lookup = {}
    for (code, indicator), group in mp_panel.groupby(["code", "indicator"]):
        points = sorted(
            (mp_available(row.date, row.lag), row.date, float(row.value))
            for row in group.itertuples()
        )
        lookup[(code, indicator)] = (
            [point[0] for point in points],
            [point[2] for point in points],
        )
    return lookup


def _sheet_lookup(public_panel: pd.DataFrame, sheet_map: dict) -> dict:
    """`(industry_slug, indicator) → (доступные даты, значения)`.

    Публичная панель содержит 87 кодов, но значения внутри листа совпадают
    (11 листов × 3 индикатора = 33 ряда). Сначала проверяем это, затем
    дедуплицируем до одного ряда на лист. Точка публикуется ЦБ в следующем
    месяце, поэтому as-of — `date + 1` (как у МП в панели авторов).
    """
    frame = public_panel.copy()
    missing = sorted(set(frame["code"]) - set(sheet_map))
    if missing:
        raise ValueError(f"public panel: коды без industry_slug: {missing}")
    frame["industry_slug"] = frame["code"].map(sheet_map)
    stats = frame.groupby(["industry_slug", "indicator", "date"])["value"].agg(
        ["min", "max"]
    )
    inconsistent = stats[(stats["max"] - stats["min"]).abs() > 1e-9]
    if not inconsistent.empty:
        raise ValueError(
            "public panel: значения внутри листа расходятся:\n"
            f"{inconsistent.head(5)}"
        )
    frame = frame.drop_duplicates(["industry_slug", "indicator", "date"])
    lookup = {}
    for (sheet, indicator), group in frame.groupby(["industry_slug", "indicator"]):
        points = sorted(
            (mp_available(row.date, 1), row.date, float(row.value))
            for row in group.itertuples()
        )
        lookup[(sheet, indicator)] = (
            [point[0] for point in points],
            [point[2] for point in points],
        )
    return lookup


def _full_lookup(panel: pd.DataFrame) -> dict:
    """`(group_stat, code, kind) → (доступные даты, значения)` для режима `full`.

    CPI-тренд доступен по правилу v2 (`available_from_mes`), остальные ряды —
    по `lag` панели (кредит — 2 месяца, ставки/рынок — как в архиве).
    """
    lookup = {}
    if panel.empty:
        return lookup
    for (group_stat, code, kind), group in panel.groupby(["group_stat", "code", "kind"]):
        points = []
        for row in group.itertuples():
            lag = 0 if pd.isna(row.lag) else int(row.lag)
            if group_stat == "Индекс потребительских цен":
                available = cm.available_from_mes(month_end(row.date))
            else:
                available = mp_available(row.date, lag)
            points.append((available, row.date, float(row.value)))
        points.sort(key=lambda point: (point[0], point[1]))
        lookup[(group_stat, code, kind)] = (
            [point[0] for point in points],
            [point[2] for point in points],
        )
    return lookup


def _asof_value(lookup: dict, key: tuple, cutoff: date) -> float:
    available, values = lookup.get(key, ([], []))
    position = bisect.bisect_right(available, cutoff) - 1
    if position < 0:
        return float("nan")
    return float(values[position])


def mp_feature_names(spec: GroupSpec, mode: str = DEFAULT_FEATURE_MODE) -> list:
    """Имена МП-признаков группы для режима `mean`/`per_bsu`/`per_sheet`/`full`."""
    validate_feature_mode(mode)
    if mode == "mean":
        return list(MP_FEATURES)
    if mode == "per_sheet":
        names = []
        for sheet in spec.sheet_slugs:
            for indicator in ("ibk", "costs", "price_exp"):
                base = f"mp_sheet_{feature_code(sheet)}_{indicator}"
                names.extend([base, f"{base}_chg"])
        return names
    names = []
    for code in spec.bsu_codes:
        for indicator in ("ibk", "costs", "price_exp"):
            base = f"mp_{feature_code(code)}_{indicator}"
            names.extend([base, f"{base}_chg"])
    return names


def mp_features(
    spec: GroupSpec,
    month: date,
    lookup: dict,
    mode: str = DEFAULT_FEATURE_MODE,
) -> dict:
    """МП-признаки группы, as-of cutoff(M).

    `mean` — 6 усреднённых признаков (поведение #190); `per_bsu`/`full` —
    уровень и изменение по каждому BSU-коду и индикатору; `per_sheet` — то же
    по каждому уникальному листу публичной книги. Если у группы нет BSU/листов
    или ни один ряд не даёт точку, значения — NaN (CatBoost обрабатывает NaN
    нативно); для группы без BSU MP-модель не обучается.
    """
    validate_feature_mode(mode)
    if mode == "per_sheet":
        if not spec.sheet_slugs:
            return {}
    elif not spec.bsu_codes:
        return {}
    cutoff = cm.cutoff_for(month)
    if mode == "mean":
        result = {}
        for indicator, short in (("ibk", "ibk"), ("costs", "costs"), ("price_exp", "price_exp")):
            levels = []
            changes = []
            for code in spec.bsu_codes:
                available, values = lookup.get((code, indicator), ([], []))
                position = bisect.bisect_right(available, cutoff) - 1
                if position < 0:
                    continue
                levels.append(values[position])
                if position >= 1:
                    changes.append(values[position] - values[position - 1])
            result[f"mp_{short}"] = float(np.mean(levels)) if levels else float("nan")
            result[f"mp_{short}_chg"] = float(np.mean(changes)) if changes else float("nan")
        return result

    if mode == "per_sheet":
        result = {}
        for sheet in spec.sheet_slugs:
            for indicator in ("ibk", "costs", "price_exp"):
                base = f"mp_sheet_{feature_code(sheet)}_{indicator}"
                available, values = lookup.get((sheet, indicator), ([], []))
                position = bisect.bisect_right(available, cutoff) - 1
                if position < 0:
                    result[base] = float("nan")
                    result[f"{base}_chg"] = float("nan")
                else:
                    result[base] = float(values[position])
                    result[f"{base}_chg"] = (
                        float(values[position] - values[position - 1])
                        if position >= 1
                        else float("nan")
                    )
        return result

    result = {}
    for code in spec.bsu_codes:
        for indicator in ("ibk", "costs", "price_exp"):
            base = f"mp_{feature_code(code)}_{indicator}"
            available, values = lookup.get((code, indicator), ([], []))
            position = bisect.bisect_right(available, cutoff) - 1
            if position < 0:
                result[base] = float("nan")
                result[f"{base}_chg"] = float("nan")
            else:
                result[base] = float(values[position])
                result[f"{base}_chg"] = (
                    float(values[position] - values[position - 1])
                    if position >= 1
                    else float("nan")
                )
    return result


def full_features(spec: GroupSpec, month: date, lookup: dict) -> dict:
    """Дополнительные признаки режима `full`: CPI-тренд, кредит, ставки, USD."""
    cutoff = cm.cutoff_for(month)
    result = {
        "cpi_trend": _asof_value(
            lookup, ("Индекс потребительских цен", spec.code, "trend"), cutoff
        ),
        "cpi_trend_dev": _asof_value(
            lookup, ("Индекс потребительских цен", spec.code, "trend_deviation"), cutoff
        ),
    }
    for name, key in FULL_PANEL_KEYS.items():
        result[name] = _asof_value(lookup, key, cutoff)
    return result


def validate_feature_mode(mode: str) -> None:
    if mode not in FEATURE_MODES:
        raise ValueError(f"неизвестный feature_mode {mode!r}; ожидается {FEATURE_MODES}")


def feature_table(
    specs: list,
    months: list,
    cpi_diff: pd.DataFrame,
    macro: dict,
    mp_lookup: dict,
    feature_mode: str = DEFAULT_FEATURE_MODE,
    full_lookup: dict = None,
) -> dict:
    """`{код: {origin: признаки}}` — признаки по месяцам origin (без таргета)."""
    validate_feature_mode(feature_mode)
    table = {}
    for spec in specs:
        per_month = {}
        for month in months:
            features, reason = base_features(spec, month, cpi_diff, macro)
            if features is None:
                continue
            features.update(mp_features(spec, month, mp_lookup, mode=feature_mode))
            if feature_mode == "full":
                features.update(full_features(spec, month, full_lookup or {}))
            per_month[month] = features
        table[spec.code] = per_month
    return table


def model_rows(
    code: str,
    features_by_month: dict,
    target_frame: pd.DataFrame,
    horizon: int,
    target_months: list,
) -> list:
    """Строки цели M с признаками origin `M−(h−1)` (конструкция CB_COMP).

    `target_frame` — компонентный таргет h; для тестовых строк значение может
    быть NaN (оно не нужно для прогноза, только для обучения/оценки).
    """
    rows = []
    for target_month in target_months:
        origin = shift_month(target_month, -(horizon - 1))
        features = features_by_month.get(origin)
        if features is None:
            continue
        value = float("nan")
        if target_month in target_frame.index and code in target_frame.columns:
            value = target_frame.at[target_month, code]
        rows.append((target_month, features, {horizon: float(value) if not pd.isna(value) else float("nan")}))
    return rows


def assert_no_leakage_bottomup(target_months: list, horizon: int) -> None:
    """Origin `M−(h−1)` и его cutoff строго раньше публикации цели M."""
    for target_month in target_months:
        origin = shift_month(target_month, -(horizon - 1))
        if cm.cutoff_for(origin) >= cm.available_from_mes(cm.month_end(target_month)):
            raise AssertionError(
                f"утечка h{horizon}: cutoff origin {origin} не раньше цели {target_month}"
            )


def feature_names(
    with_mp: bool,
    mode: str = DEFAULT_FEATURE_MODE,
    spec: GroupSpec = None,
) -> list:
    """Имена признаков; `mode="mean"` сохраняет порядок #190.

    Для `per_bsu`/`full` нужен `spec` — число и имена признаков зависят от
    BSU-кодов группы. `month` остаётся последним (индекс `cat_features`).
    """
    validate_feature_mode(mode)
    if not with_mp:
        return list(BASE_FEATURES)
    if mode == "mean":
        return list(BASE_FEATURES[:-1]) + list(MP_FEATURES) + [BASE_FEATURES[-1]]
    if spec is None:
        raise ValueError(f"feature_names: для mode={mode!r} нужен spec")
    names = list(BASE_FEATURES[:-1]) + mp_feature_names(spec, mode)
    if mode == "full":
        names.extend(FULL_FEATURES)
    names.append(BASE_FEATURES[-1])
    return names


def feature_matrix(rows: list, names: list) -> np.ndarray:
    return np.array([[row[1][name] for name in names] for row in rows], dtype=object)


def catboost_params(names: list) -> dict:
    params = dict(CATBOOST_DEFAULTS)
    params["cat_features"] = [names.index("month")]
    return params


def fit_component(
    train_rows: list,
    names: list,
    horizon: int,
    **overrides,
) -> CatBoostRegressor:
    params = {**catboost_params(names), **overrides}
    model = CatBoostRegressor(**params)
    model.fit(
        feature_matrix(train_rows, names),
        np.asarray([row[2][horizon] for row in train_rows], dtype=float),
    )
    return model


def predict_component(model: CatBoostRegressor, rows: list, names: list) -> list:
    return [float(value) for value in model.predict(feature_matrix(rows, names))]


# ---------------------------------------------------------------------------
# Веса и агрегат
# ---------------------------------------------------------------------------


def weight_for_month(weights: dict, month: date) -> dict:
    """Веса года `M.year−1`; если года нет — ближайший более ранний."""
    year = month.year - 1
    if year in weights:
        return weights[year]
    available = sorted(weights)
    if not available:
        raise ValueError("нет ни одного года весов")
    earlier = [value for value in available if value <= year]
    return weights[earlier[-1]] if earlier else weights[available[0]]


def aggregate(
    predictions: dict,
    weights: dict,
    months: list,
    prefer_mp_groups=frozenset(),
) -> tuple:
    """`Σ w·ŷ / Σ w` по группам с прогнозом и весом; вернуть `(agg, coverage)`.

    `predictions` — `{код: {месяц: прогноз}}`; для `bottomup_mp` прогноз
    группы с BSU должен быть MP-прогнозом, для остальных — no-MP.
    """
    aggregated = {}
    coverage = {}
    for month in months:
        weights_month = weight_for_month(weights, month)
        numerator = 0.0
        denominator = 0.0
        for code, series in predictions.items():
            if month not in series:
                continue
            weight = weights_month.get(code)
            if not weight:
                continue
            numerator += weight * series[month]
            denominator += weight
        aggregated[month] = numerator / denominator if denominator else float("nan")
        coverage[month] = denominator
    return aggregated, coverage


def assembly_gate(reconstruction_metrics: dict, coverage_mean: float) -> tuple:
    """Гейт сборки: `(ok, reason)` по RMSE h1 и покрытию (спека §4.1)."""
    rmse_h1 = reconstruction_metrics.get(1, {}).get("rmse", float("inf"))
    if rmse_h1 > RECONSTRUCTION_RMSE_H1_MAX:
        return False, (
            f"агрегат компонент не сходится с TARGET_FULL: RMSE h1 = {rmse_h1:.4f} "
            f"(порог {RECONSTRUCTION_RMSE_H1_MAX})"
        )
    if coverage_mean < COVERAGE_MIN:
        return False, (
            f"покрытие весов {coverage_mean:.3f} ниже порога {COVERAGE_MIN}"
        )
    return True, ""


def reconstruction(
    targets: dict,
    target_full: pd.DataFrame,
    weights: dict,
    months: list,
) -> dict:
    """Агрегат фактических компонентных таргетов vs `TARGET_FULL` (гейт сборки)."""
    metrics = {}
    for horizon in sorted(targets):
        differences = []
        coverages = []
        for month in months:
            if month not in target_full.index:
                continue
            weights_month = weight_for_month(weights, month)
            numerator = 0.0
            denominator = 0.0
            for code in targets[horizon].columns:
                if month not in targets[horizon].index:
                    continue
                value = targets[horizon].at[month, code]
                weight = weights_month.get(code)
                if pd.isna(value) or not weight:
                    continue
                numerator += weight * value
                denominator += weight
            if not denominator:
                continue
            actual = float(target_full.at[month, horizon])
            if pd.isna(actual):
                continue
            differences.append(numerator / denominator - actual)
            coverages.append(denominator)
        if not differences:
            metrics[horizon] = {
                "n": 0,
                "rmse": float("inf"),
                "max_abs": float("inf"),
                "bias": float("nan"),
                "coverage_mean": 0.0,
            }
            continue
        metrics[horizon] = {
            "n": len(differences),
            "rmse": float(np.sqrt(np.mean(np.square(differences)))),
            "max_abs": float(np.max(np.abs(differences))),
            "bias": float(np.mean(differences)),
            "coverage_mean": float(np.mean(coverages)),
        }
    return metrics


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def run_bottomup(
    archive: Path,
    target_full: pd.DataFrame,
    macro: dict,
    normal_months: list,
    train_months: list,
    test_months: list,
    train_outliers=frozenset(),
    feature_mode: str = DEFAULT_FEATURE_MODE,
    public_panel: pd.DataFrame = None,
    sheet_map: dict = None,
) -> BottomUpResult:
    """Полный bottom-up: данные → гейт → модели → агрегат.

    `macro` — `{месяц: {usd_mm, key_rate, fao_mm}}` из v2; `target_full` —
    `TARGET_FULL` (агрегатный таргет статьи); `train_months`/`test_months` —
    предзарегистрированные окна; `normal_months` — 54 месяца оценки;
    `feature_mode` — `mean` (baseline #190), `per_bsu`, `per_sheet` или `full`.
    Для `per_sheet` обязательны `public_panel` (live-панель 87 кодов) и
    `sheet_map` (`код → industry_slug`): значения внутри листа совпадают, и
    режим использует 11 листов × 3 индикатора вместо 87 кодов.
    """
    validate_feature_mode(feature_mode)
    if feature_mode == "per_sheet" and (public_panel is None or sheet_map is None):
        return BottomUpResult(
            status="not_assembled",
            reason="per_sheet требует public_panel и sheet_map",
        )
    specs = load_specs(archive, sheet_map if feature_mode == "per_sheet" else None)
    years = sorted({month.year - 1 for month in test_months})
    weights = load_weights(archive, years, specs)
    cpi, mp, panel = ensure_panel_csv(
        archive, PANEL_DIR, require_full=(feature_mode == "full")
    )
    cpi_diff = load_cpi_diff(cpi)
    mp_panel = load_mp_panel(mp)
    full_lookup = _full_lookup(panel) if feature_mode == "full" else {}
    utilities = load_utilities(archive)

    missing_groups = [spec.code for spec in specs if spec.code not in cpi_diff.columns]
    if missing_groups:
        return BottomUpResult(
            status="not_assembled",
            reason=f"в панели нет групп: {missing_groups}",
            metadata={"missing_groups": missing_groups},
        )

    targets = component_targets(cpi_diff)
    reconstruction_metrics = reconstruction(targets, target_full, weights, normal_months)
    coverage_mean = reconstruction_metrics[HORIZONS[0]]["coverage_mean"]
    assembled, reason = assembly_gate(reconstruction_metrics, coverage_mean)
    if not assembled:
        return BottomUpResult(
            status="not_assembled",
            reason=reason,
            metadata={
                "reconstruction": reconstruction_metrics,
                "coverage_mean": coverage_mean,
            },
        )

    if feature_mode == "per_sheet":
        mp_lookup = _sheet_lookup(public_panel, sheet_map)
    else:
        mp_lookup = _mp_lookup(mp_panel)
    all_months = sorted(set(train_months) | set(test_months))
    features_by_group = feature_table(
        specs,
        all_months,
        cpi_diff,
        macro,
        mp_lookup,
        feature_mode=feature_mode,
        full_lookup=full_lookup,
    )
    train_set = sorted(set(train_months) - set(train_outliers))
    test_set = sorted(set(test_months))

    no_mp_predictions = {}
    mp_predictions = {}
    skipped = []
    models_count = 0
    for spec in specs:
        features = features_by_group.get(spec.code, {})
        for horizon in HORIZONS:
            assert_no_leakage_bottomup(test_set, horizon)
            train_rows = [
                row
                for row in model_rows(
                    spec.code, features, targets[horizon], horizon, train_set
                )
                if not pd.isna(row[2][horizon])
            ]
            test_rows = model_rows(
                spec.code, features, targets[horizon], horizon, test_set
            )
            if len(train_rows) < MIN_TRAIN_ROWS:
                skipped.append((spec.code, horizon, "no_mp", len(train_rows)))
                continue
            names = feature_names(with_mp=False)
            model = fit_component(train_rows, names, horizon)
            predictions = predict_component(model, test_rows, names)
            no_mp_predictions.setdefault(spec.code, {})  # type: ignore[arg-type]
            no_mp_predictions[spec.code].setdefault(horizon, {})  # type: ignore[arg-type]
            for row, prediction in zip(test_rows, predictions):
                no_mp_predictions[spec.code][horizon][row[0]] = prediction  # type: ignore[index]
            models_count += 1

            if spec.bsu_codes:
                mp_names = feature_names(
                    with_mp=True, mode=feature_mode, spec=spec
                )
                mp_model = fit_component(train_rows, mp_names, horizon)
                mp_values = predict_component(mp_model, test_rows, mp_names)
                mp_predictions.setdefault(spec.code, {})  # type: ignore[arg-type]
                mp_predictions[spec.code].setdefault(horizon, {})  # type: ignore[arg-type]
                for row, prediction in zip(test_rows, mp_values):
                    mp_predictions[spec.code][horizon][row[0]] = prediction  # type: ignore[index]
                models_count += 1

    predictions = {}
    for variant, source, prefer in (
        (MODEL_MP, mp_predictions, True),
        (MODEL_NOMP, no_mp_predictions, False),
    ):
        predictions[variant] = {}
        for horizon in HORIZONS:
            by_group = {}
            for spec in specs:
                series = source.get(spec.code, {}).get(horizon, {})
                if series:
                    by_group[spec.code] = series
            if prefer:
                for spec in specs:
                    if spec.bsu_codes:
                        series = mp_predictions.get(spec.code, {}).get(horizon, {})
                        if series:
                            by_group[spec.code] = series
                    else:
                        series = no_mp_predictions.get(spec.code, {}).get(horizon, {})
                        if series:
                            by_group[spec.code] = series
            aggregated, coverage = aggregate(by_group, weights, test_months)
            predictions[variant][horizon] = {
                "values": aggregated,
                "coverage": coverage,
                "groups": sorted(by_group),
            }

    utilities_weight = {
        year: weights[year].get(UTILITIES_CODE)
        for year in years
    }
    feature_counts = [
        len(feature_names(True, feature_mode, spec))
        if spec.bsu_codes
        else len(feature_names(False))
        for spec in specs
    ]
    metadata = {
        "feature_mode": feature_mode,
        "feature_counts": {
            "min": int(min(feature_counts)),
            "median": float(np.median(feature_counts)),
            "max": int(max(feature_counts)),
        },
        "full_panel_rows": int(len(panel)),
        "public_panel_rows": int(len(public_panel)) if public_panel is not None else 0,
        "n_sheets": len(
            {sheet for spec in specs for sheet in spec.sheet_slugs}
        ),
        "n_groups": len(specs),
        "n_with_bsu": sum(1 for spec in specs if spec.bsu_codes),
        "n_without_bsu": sum(1 for spec in specs if not spec.bsu_codes),
        "n_models": models_count,
        "skipped": skipped,
        "weights_years": years,
        "utilities_weight": utilities_weight,
        "reconstruction": reconstruction_metrics,
        "coverage_mean": coverage_mean,
        "coverage_by_month": {
            variant: {
                month.isoformat(): value
                for month, value in predictions[variant][HORIZONS[0]]["coverage"].items()
            }
            for variant in predictions
        },
        "utilities_rows": int(len(utilities)),
        "utilities_kind": sorted(utilities["kind"].astype(str).unique().tolist()),
        "cpi_rows": int(len(cpi)),
        "mp_rows": int(len(mp)),
    }
    return BottomUpResult(
        status="ok",
        reason="",
        predictions=predictions,
        metadata=metadata,
    )
