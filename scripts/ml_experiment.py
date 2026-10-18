#!/usr/bin/env python3
"""Паспорт ML-прогона и fold-предсказания в ClickHouse.

Хелпер для скриптов `cpi_nowcast.py` и `key_rate_model.py`: собирает
метаданные прогона (git, конфигурация, выборка, версии пакетов), пишет одну
строку в `ml_runs` и один batch в `ml_predictions` через HTTP-транспорт
`extrapolation_aisc.ch_exec` и падает обратно в локальные `run.json` /
`predictions.jsonl`, если ClickHouse недоступен.

Ведение журнала — ОПТ-ИН: по умолчанию (`ML_LOG` не равен `1`) `finish`
ничего не пишет и возвращает `"disabled"`, поэтому поведение вызывающих
скриптов без переменной остаётся прежним.

Зависимости — только стандартная библиотека. Запуск тестов:

    python3 scripts/ml_experiment_test.py -v
"""

import hashlib
import http.client
import importlib.metadata
import json
import logging
import math
import os
import platform
import subprocess
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

try:  # запуск из каталога scripts (`python3 scripts/...`)
    from extrapolation_aisc import ClickHouseError, ch_exec
except ImportError:  # запуск как пакет из корня репозитория
    from scripts.extrapolation_aisc import ClickHouseError, ch_exec

ROOT = Path(__file__).resolve().parent.parent

# Куда складывается fallback, если ClickHouse недоступен. Каталог можно
# переопределить `ML_LOCAL_DIR` (например, на tmpfs в тестах/CI).
DEFAULT_LOCAL_DIR = ROOT / "build" / "ml-runs"

# Пакеты, версии которых попадают в паспорт окружения. Отсутствующий пакет
# записывается как None, а не выкидывает исключение: один и тот же хелпер
# используется скриптами с разным набором зависимостей.
PACKAGES = ("catboost", "scikit-learn", "pandas", "statsmodels", "scipy", "numpy")

log = logging.getLogger(__name__)


def canonical_json(value) -> str:
    """Канонический JSON: стабильный порядок ключей и компактная запись.

    Именно эта форма хешируется в `config_hash`/`sample_hash`: `sort_keys`
    убирает зависимость от порядка вставки, `ensure_ascii=False` — от формы
    escape-последовательностей, а `allow_nan=False` не даёт записать в JSON
    невалидные `NaN`/`Infinity`. Перед сериализацией значение проходит
    `json_safe`, поэтому даты, пути и UUID не требуют ручной подготовки.
    """
    return json.dumps(
        json_safe(value),
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def sha256_text(text: str) -> str:
    """SHA-256 строки в hex; используется для config/sample/artifact hash."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def json_safe(value):
    """Рекурсивно приводит значение к JSON-совместимому виду.

    `date`/`datetime` → `isoformat()`, `Path`/`UUID` → `str`, tuple → list,
    `NaN`/`±Inf` → `None`, объекты с `.item()` (numpy-скаляры) — рекурсивно
    через `.item()`. `NaN` в метриках не должен ронять запись паспорта, но и
    невалидный JSON в колонку попадать не должен.
    """
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (Path, uuid.UUID)):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    # numpy-скаляры (`np.int64`, `np.float32`, `np.bool_`, ...) не знают
    # json.dumps, но отдают питоновский примитив через `.item()`. Проверка по
    # протоколу, а не по типу: numpy остаётся необязательной зависимостью
    # (`np.float64` — подкласс float и сюда не доходит, а `np.float32` — да).
    item = getattr(value, "item", None)
    if callable(item):
        converted = item()
        if converted is not value:  # защита от `.item() -> self`
            return json_safe(converted)
    return value


def _ch_safe(value):
    """Как `json_safe`, но datetime — в формате, который понимает ClickHouse.

    ClickHouse принимает `DateTime` и с `T`, но канонический для него вид —
    `YYYY-MM-DD HH:MM:SS` (пробел). Отдельная функция нужна и потому, что
    `json_safe` обязан оставаться ISO-8601 (`date.isoformat()`), а формат
    вставки — это контракт `JSONEachRow`, а не общего JSON.
    """
    if isinstance(value, dict):
        return {str(key): _ch_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_ch_safe(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat(" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (Path, uuid.UUID)):
        return str(value)
    # numpy-скаляры (`np.int64`, `np.float32`, ...): в JSONEachRow они обязаны
    # уйти примитивом, иначе json.dumps поднимет TypeError.
    item = getattr(value, "item", None)
    if callable(item):
        converted = item()
        if converted is not value:
            return _ch_safe(converted)
    # NaN/±Inf НЕ заменяются на None: колонки Float64 в ClickHouse не
    # Nullable, и молчаливая подмена скрыла бы дефект. `json.dumps(...,
    # allow_nan=False)` ниже поднимет ValueError — как требует контракт
    # JSONEachRow («NaN/Inf запрещены»).
    return value


def json_each_row(rows) -> str:
    """Сериализует строки в формат ClickHouse `JSONEachRow` (одна на строку).

    Нечисловые `Float64` запрещены: `allow_nan=False` поднимает `ValueError`
    на `NaN`/`±Inf`, а не пишет в таблицу невалидное значение.
    """
    return "\n".join(
        json.dumps(
            _ch_safe(row),
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        for row in rows
    )


def insert_sql(table: str, rows) -> str:
    """Готовый INSERT ... FORMAT JSONEachRow для HTTP-интерфейса.

    `table` — внутренняя константа вызывающего кода (`ml_runs` или
    `ml_predictions`), а не пользовательский ввод, поэтому имя подставляется
    как есть; значения строк сериализуются JSON и SQL-инъекцией не являются.
    """
    body = json_each_row(rows)
    header = f"INSERT INTO {table} FORMAT JSONEachRow"
    return f"{header}\n{body}" if body else header


class PredictionRow:
    """Одно fold-предсказание: факт, прогноз и их разность в unit.

    Все числа обязаны быть конечными. Проверка выполняется и в конструкторе,
    и в `error`: значение, испорченное уже после создания строки, не должно
    просочиться в ClickHouse как `NaN`.
    """

    def __init__(self, model: str, window: str, target_date: date,
                 predicted: float, actual: float, unit: str, meta=None):
        self.model = model
        self.window = window
        self.target_date = target_date
        self.predicted = predicted
        self.actual = actual
        self.unit = unit
        self.meta = {} if meta is None else meta
        self._validate()

    def _validate(self) -> None:
        for name in ("predicted", "actual"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} должно быть числом, получено {value!r}")
            if not math.isfinite(value):
                raise ValueError(f"{name} должно быть конечным, получено {value!r}")

    @property
    def error(self) -> float:
        """Ошибка прогноза: `predicted - actual` (в `unit`)."""
        self._validate()
        return self.predicted - self.actual


def _git(args) -> str:
    """Вывод git-команды или пустая строка, если git/команда недоступны."""
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return ""
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def capture_git() -> dict:
    """Снимок состояния рабочего дерева для паспорта прогона.

    `dirty=1`, если `git status --porcelain` что-то вернул: прогон на грязном
    дереве должен быть отличим от прогона на коммите.
    """
    status = _git(["status", "--porcelain"])
    return {
        "author": _git(["config", "user.name"]) or os.environ.get("GIT_AUTHOR_NAME", ""),
        "git_sha": _git(["rev-parse", "HEAD"]),
        "git_branch": _git(["branch", "--show-current"]),
        "git_dirty": 1 if status else 0,
    }


def package_versions() -> dict:
    """Версии пакетов, влияющих на результат; отсутствующие — None."""
    versions = {}
    for name in PACKAGES:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = None
    return versions


def _artifact(path: Path) -> dict:
    """Паспорт файла: имя, путь, sha256 и размер. Отсутствующий — None."""
    path = Path(path)
    if not path.is_file():
        log.warning("артефакт %s не является файлом — пропущен", path)
        return None
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)

    candidate = path if path.is_absolute() else Path.cwd() / path
    try:
        display = candidate.resolve().relative_to(ROOT)
    except ValueError:
        display = candidate
    return {
        "name": path.name,
        "path": str(display),
        "sha256": digest.hexdigest(),
        "bytes": size,
    }


class ExperimentRun:
    """Один прогон ML-эксперимента: метрики, предсказания, артефакты.

    `finish` идемпотентен: повторный вызов (в том числе из `__exit__` после
    явного `finish`) ничего не пишет и возвращает тот же результат. Контекстный
    менеджер помечает прогон `failed`, если исключение вылетело из тела, но
    исключение НЕ подавляет.
    """

    def __init__(self, experiment: str, run_name: str, config, data_asof: date,
                 sample_meta, sample, spec_path: str = "", enabled: bool = False):
        self.run_id = str(uuid.uuid4())
        self.experiment = experiment
        self.run_name = run_name
        self.config = config
        self.data_asof = data_asof
        self.sample_meta = sample_meta
        self.sample = sample
        self.spec_path = spec_path
        self.enabled = enabled
        self.started_at = datetime.now()
        self.git = capture_git()
        self.packages = package_versions()
        self.metrics = {}
        self.predictions = []
        self.artifacts = []
        self._finished = False
        self._result = ""

    def log_metrics(self, metrics: dict) -> None:
        """Добавляет/перезаписывает агрегированные метрики прогона."""
        self.metrics.update(metrics)

    def log_predictions(self, rows) -> None:
        """Добавляет fold-предсказания (объекты `PredictionRow`)."""
        self.predictions.extend(rows)

    def log_artifacts(self, paths) -> None:
        """Добавляет паспорта файлов; `None` и не-файлы пропускаются."""
        for path in paths:
            if path is None:
                continue
            entry = _artifact(path)
            if entry is not None:
                self.artifacts.append(entry)

    def _run_row(self, status: str, notes: str, finished_at: datetime) -> dict:
        config_json = canonical_json(self.config)
        duration = (finished_at - self.started_at).total_seconds()
        return {
            "run_id": self.run_id,
            "experiment": self.experiment,
            "run_name": self.run_name,
            "status": status,
            "author": self.git["author"],
            "git_sha": self.git["git_sha"],
            "git_branch": self.git["git_branch"],
            "git_dirty": self.git["git_dirty"],
            "config_hash": sha256_text(config_json),
            "config_json": config_json,
            "env_json": canonical_json({
                "python": sys.version,
                "platform": platform.platform(),
                "packages": self.packages,
            }),
            "data_asof": self.data_asof,
            "sample_hash": sha256_text(canonical_json(self.sample)),
            "sample_meta_json": canonical_json(self.sample_meta),
            "spec_path": self.spec_path,
            "metrics_json": canonical_json(self.metrics),
            "artifacts_json": canonical_json(self.artifacts),
            "started_at": self.started_at,
            "finished_at": finished_at,
            "duration_sec": max(0.0, duration),
            "notes": notes,
        }

    def _prediction_rows(self, created_at: datetime) -> list:
        rows = []
        for prediction in self.predictions:
            rows.append({
                "run_id": self.run_id,
                "experiment": self.experiment,
                "model": prediction.model,
                "window": prediction.window,
                "target_date": prediction.target_date,
                "predicted": float(prediction.predicted),
                "actual": float(prediction.actual),
                "error": prediction.error,
                "unit": prediction.unit,
                "meta_json": canonical_json(prediction.meta),
                "created_at": created_at,
            })
        return rows

    def _local_dir(self) -> Path:
        """Каталог локального fallback: `ML_LOCAL_DIR` или `build/ml-runs`."""
        return Path(os.environ.get("ML_LOCAL_DIR") or DEFAULT_LOCAL_DIR) / self.run_id

    def _write_minimal_run(self, notes: str, finished_at: datetime) -> None:
        """Пишет минимальный безопасный паспорт без пользовательских данных.

        Используется, когда полный `run_row` не удалось даже сериализовать
        (или записать): поля берутся только из самой обвязки прогона, поэтому
        сбой `json.dumps` здесь невозможен, и паспорт не теряется.
        """
        directory = self._local_dir()
        payload = {
            "run_id": self.run_id,
            "experiment": self.experiment,
            "run_name": self.run_name,
            "status": "failed",
            "started_at": self.started_at,
            "finished_at": finished_at,
            "notes": notes,
        }
        try:
            directory.mkdir(parents=True, exist_ok=True)
            (directory / "run.json").write_text(
                canonical_json(payload), encoding="utf-8"
            )
        except OSError as local_exc:
            log.warning(
                "минимальный паспорт прогона %s тоже не записался: %s",
                self.run_id, local_exc,
            )

    def _write_local(self, run_row: dict, prediction_rows: list, exc: Exception) -> None:
        """Fallback: run.json и predictions.jsonl рядом, без падения наружу."""
        directory = self._local_dir()
        log.warning(
            "запись прогона %s в ClickHouse не удалась (%s); локальная копия: %s",
            self.run_id, exc, directory,
        )
        try:
            directory.mkdir(parents=True, exist_ok=True)
            (directory / "run.json").write_text(
                canonical_json(run_row), encoding="utf-8"
            )
            (directory / "predictions.jsonl").write_text(
                json_each_row(prediction_rows), encoding="utf-8"
            )
        except (OSError, TypeError, ValueError) as local_exc:
            log.warning(
                "локальная копия прогона %s не записалась (%s); "
                "пишу минимальный паспорт",
                self.run_id, local_exc,
            )
            self._write_minimal_run(
                f"local fallback failed: {type(local_exc).__name__}: {local_exc}",
                datetime.now(),
            )

    def finish(self, status: str = "finished", notes: str = "") -> str:
        """Пишет предсказания и паспорт; возвращает `clickhouse`/`local`/`disabled`.

        Сначала одним batch `ml_predictions` (если они есть), затем `ml_runs`.
        Паспорт пишется ПОСЛЕДНИМ, поэтому сбой вставки предсказаний не может
        оставить `finished`-паспорт без фолдов: в этом случае паспорт не
        пишется вовсе. Ни ошибка ClickHouse (`ClickHouseError`, `OSError`,
        `http.client.HTTPException` — обрыв HTTP вроде `IncompleteRead` не
        является `OSError`), ни ошибка сериализации конфига или метрик не
        роняют прогон: полный (или минимальный безопасный) паспорт
        сохраняется локально, а вызывающий получает `"local"`.
        """
        if self._finished:
            return self._result
        self._finished = True

        if not self.enabled:
            self._result = "disabled"
            return self._result

        finished_at = datetime.now()
        run_row = None
        prediction_rows: list = []
        try:
            run_row = self._run_row(status, notes, finished_at)
            prediction_rows = self._prediction_rows(finished_at)
            if prediction_rows:
                ch_exec(insert_sql("ml_predictions", prediction_rows))
            ch_exec(insert_sql("ml_runs", [run_row]))
        except (TypeError, ValueError) as exc:
            # Конфиг/метрики могут содержать объект, который не умеет
            # сериализовать даже `json_safe` (например, произвольный класс).
            # Полный паспорт построить нельзя, но минимальный — можно:
            # пользовательских значений в нём нет, поэтому сбой не повторится.
            note = f"serialization failed: {type(exc).__name__}: {exc}"
            log.warning(
                "прогон %s не сериализован (%s); минимальный локальный паспорт",
                self.run_id, note,
            )
            self._write_minimal_run(note, finished_at)
            self._result = "local"
            return self._result
        except (ClickHouseError, OSError, http.client.HTTPException) as exc:
            if run_row is None:
                self._write_minimal_run(
                    f"{type(exc).__name__}: {exc}", finished_at
                )
            else:
                self._write_local(run_row, prediction_rows, exc)
            self._result = "local"
            return self._result

        self._result = "clickhouse"
        return self._result

    def __enter__(self) -> "ExperimentRun":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if not self._finished:
            if exc_type is not None:
                self.finish("failed", f"{exc_type.__name__}: {exc}")
            else:
                # Штатный выход из `with` без явного `finish` — тоже дефект
                # вызывающего кода: прогон не подтвердил результат, поэтому
                # паспорт помечается failed (спека §6.1), а не теряется молча.
                self.finish("failed", "finish не вызван")
        return False


def start_run(experiment: str, run_name: str, config, data_asof: date,
              sample_meta, sample, spec_path: str = "", enabled=None) -> ExperimentRun:
    """Создаёт прогон; `ML_LOG=1` включает запись, `ML_RUN_NAME` меняет имя.

    Явный `enabled=` переопределяет переменную окружения: тесты и вызывающий
    код могут включить/выключить журнал, не трогая процесс.
    """
    if enabled is None:
        enabled = os.environ.get("ML_LOG") == "1"
    run_name = os.environ.get("ML_RUN_NAME") or run_name
    return ExperimentRun(
        experiment=experiment,
        run_name=run_name,
        config=config,
        data_asof=data_asof,
        sample_meta=sample_meta,
        sample=sample,
        spec_path=spec_path,
        enabled=enabled,
    )


if __name__ == "__main__":
    raise SystemExit("ml_experiment — библиотека; используйте тесты или импорт")
