#!/usr/bin/env python3
"""MLflow-зеркало месячного paper shadow `wk_mix_ex@v1` — P0.6a #462.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Читает
shadow-JSON `docs/research/cb-comp-h1-shadow/<YYYY-MM>.json` (P0.6, #455) и
логирует его в MLflow experiment `cb_comp_h1_shadow`:

* run name `shadow__<YYYY-MM>__wk_mix_ex@v1`;
* теги `decision=shadow`, `stage=paper_shadow`, `champion=combo3`,
  `challenger`/`model_version=wk_mix_ex@v1`, `source=json`, `json_sha256`;
* метрики прогнозов (`pred.*`), `served.*`, `weekly_features.*`, `s0.*`, а при
  наличии `actual` — `actual.t_live_rt`, `actual.t_live`, `error_bp`,
  `abs_error_bp`;
* артефакты `shadow.json` (байты источника) и `run_summary.json`
  (`log_dict`).

**JSON — источник истины, MLflow — зеркало.** Run можно удалить и пересоздать
из JSON; обратное неверно. Идемпотентность — по `(month, json_sha256)` через
`search_runs`; `--force` создаёт дубль, старый run не меняется.

Скрипт только читает JSON. В ClickHouse не пишет, модель/пороги/`ml-gate` не
меняет, alias не двигает, mlflow импортируется лениво (зависимость ставит
Makefile-цель через `uv run --with mlflow==...`).

Запуск:

    make cb-comp-h1-shadow-mlflow ARGS="--backfill"
    make cb-comp-h1-shadow-mlflow ARGS="--month 2026-10 --dry-run"

Тесты — офлайн (`sqlite:///<tmp>/mlflow.db`, без сети и ClickHouse):

    make cb-comp-h1-shadow-mlflow-test
"""

import argparse
import hashlib
import json
import math
import os
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SHADOW_DIR = ROOT / "docs" / "research" / "cb-comp-h1-shadow"
DEFAULT_URI = "http://127.0.0.1:5000"

EXPERIMENT = "cb_comp_h1_shadow"
VERSION = "wk_mix_ex@v1"
CHAMPION = "combo3"
DECISION = "shadow"
STAGE = "paper_shadow"
SOURCE = "json"
SHADOW_MODELS = ("combo3", "wk_mix_ex", "wk_mix_d", "live_cb_wk")
COMPONENTS = ("live_cb", "bs_econ16", "bs_rozn16", "wk_agg_ex", "wk_agg_d")
FALLBACK_CHAIN = ("wk_mix_ex", "wk_mix_d", "combo3")
REQUIRED = (
    "version",
    "month",
    "cutoff",
    "run_date",
    "git_sha",
    "models",
    "served",
    "asof",
    "inputs_sha256",
    "truth",
)
MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
# MLflow допускает в именах param/metric `\w`, `.`, `-`, `/`, пробел; запятые и
# прочие символы из названий ЖКУ-рядов заменяются, чтобы run не падал.
KEY_RE = re.compile(r"[^\w.\-/ ]+")
PARAM_MAX = 5000
TAG_MAX = 5000


class ShadowJsonError(ValueError):
    """Shadow-JSON не соответствует схеме P0.6 или не читается."""


# ---------------------------------------------------------------------------
# Чтение и валидация
# ---------------------------------------------------------------------------


def _finite(value) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _valid_month(value) -> bool:
    """`YYYY-MM` с месяцем 01…12 (regex пропускает `2026-13`)."""
    if not isinstance(value, str) or not MONTH_RE.fullmatch(value):
        return False
    return 1 <= int(value[5:7]) <= 12


def _parse_date(value, field: str) -> date:
    if not isinstance(value, str):
        raise ShadowJsonError(f"{field}: ожидается дата YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ShadowJsonError(f"{field}: не дата YYYY-MM-DD: {value!r}") from exc


def _require_text(payload: dict, key: str, allow_empty: bool = False) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        raise ShadowJsonError(f"{key}: ожидается непустая строка")
    return value


def _actual_value(actual: dict, *keys: str):
    for key in keys:
        value = actual.get(key)
        if value is None:
            continue
        if not _finite(value):
            raise ShadowJsonError(f"actual.{key}: ожидается конечное число")
        return float(value)
    return None


def validate_payload(payload) -> None:
    """Минимальная схема P0.6 без импорта тяжёлых модулей модели."""
    if not isinstance(payload, dict):
        raise ShadowJsonError("shadow-JSON: ожидается объект")
    missing = [key for key in REQUIRED if key not in payload]
    if missing:
        raise ShadowJsonError(f"shadow-JSON: нет полей {missing}")

    if payload["version"] != VERSION:
        raise ShadowJsonError(
            f"version: ожидается {VERSION!r}, получено {payload['version']!r}; "
            "новая версия — новый протокол/логгер"
        )
    if not _valid_month(payload["month"]):
        raise ShadowJsonError(f"month: ожидается YYYY-MM, получено {payload['month']!r}")
    cutoff = _parse_date(payload["cutoff"], "cutoff")
    run_date = _parse_date(payload["run_date"], "run_date")
    if run_date > cutoff:
        raise ShadowJsonError("run_date позже cutoff: прогноз не засчитывается")

    _require_text(payload, "git_sha")
    models = payload["models"]
    if not isinstance(models, dict):
        raise ShadowJsonError("models: ожидается объект")
    absent = [name for name in SHADOW_MODELS if not _finite(models.get(name))]
    if absent:
        raise ShadowJsonError(f"models: нет конечного прогноза {absent}")

    components = payload.get("components")
    if components is not None and not isinstance(components, dict):
        raise ShadowJsonError("components: ожидается объект")

    served = payload["served"]
    if not isinstance(served, dict):
        raise ShadowJsonError("served: ожидается объект")
    if served.get("model") not in FALLBACK_CHAIN:
        raise ShadowJsonError(f"served.model: вне цепочки {FALLBACK_CHAIN}")
    if not _finite(served.get("value")):
        raise ShadowJsonError("served.value: ожидается конечное число")
    level = served.get("fallback_level")
    if not isinstance(level, int) or isinstance(level, bool) or not 0 <= level < len(FALLBACK_CHAIN):
        raise ShadowJsonError("served.fallback_level: ожидается 0…2")

    truth = payload["truth"]
    if not isinstance(truth, dict) or not isinstance(truth.get("primary"), str) or not truth["primary"].strip():
        raise ShadowJsonError("truth.primary: ожидается непустая строка")
    if "secondary" in truth and not isinstance(truth["secondary"], str):
        raise ShadowJsonError("truth.secondary: ожидается строка")

    hashes = payload["inputs_sha256"]
    if not isinstance(hashes, dict) or not hashes:
        raise ShadowJsonError("inputs_sha256: ожидается непустой объект")
    for name, digest in hashes.items():
        if not isinstance(digest, str) or not SHA256_RE.fullmatch(digest):
            raise ShadowJsonError(f"inputs_sha256.{name}: ожидается sha256 (64 hex)")

    if not isinstance(payload["asof"], dict):
        raise ShadowJsonError("asof: ожидается объект")

    actual = payload.get("actual")
    if actual is not None:
        if not isinstance(actual, dict):
            raise ShadowJsonError("actual: ожидается объект или null")
        if _actual_value(actual, "t_live_rt", "T_LIVE_rt") is None:
            raise ShadowJsonError("actual: нет конечного t_live_rt")
        _actual_value(actual, "t_live", "T_LIVE")


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def load_payload(path: Path) -> tuple:
    """`(payload, text, sha256)`; ошибки схемы/чтения — `ShadowJsonError`."""
    raw = Path(path).read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ShadowJsonError(f"shadow-JSON не UTF-8: {exc}") from exc
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ShadowJsonError(f"shadow-JSON не разобран: {exc.msg}") from exc
    validate_payload(payload)
    return payload, text, sha256_bytes(raw)


# ---------------------------------------------------------------------------
# Схема run'а
# ---------------------------------------------------------------------------


def safe_key(name) -> str:
    key = KEY_RE.sub("_", str(name)).strip()
    return key[:250]


def display_path(path: Path) -> str:
    """Путь относительно корня репозитория; вне корня — только basename."""
    try:
        return str(Path(path).resolve().relative_to(ROOT))
    except ValueError:
        return Path(path).name


def run_name(month: str) -> str:
    return f"shadow__{month}__{VERSION}"


def _flatten_asof(asof: dict) -> dict:
    out = {}
    for key, value in asof.items():
        name = safe_key(f"asof.{key}")
        if isinstance(value, dict):
            for sub, subvalue in value.items():
                if subvalue is None:
                    continue
                out[safe_key(f"{name}.{sub}")] = str(subvalue)
        elif value is not None:
            out[name] = str(value)
    return out


def _check_length(kind: str, key: str, value: str) -> None:
    limit = PARAM_MAX if kind == "param" else TAG_MAX
    if len(value) > limit:
        raise ShadowJsonError(f"{kind} {key}: длиннее {limit} символов")


def build_plan(payload: dict, path: Path, sha: str) -> dict:
    """Tags/params/metrics/summary run'а из shadow-JSON."""
    month = payload["month"]
    tags = {
        "decision": DECISION,
        "stage": STAGE,
        "champion": CHAMPION,
        "challenger": VERSION,
        "model_version": VERSION,
        "source": SOURCE,
        "json_sha256": sha,
        "month": month,
        "cutoff": payload["cutoff"],
        "version": payload["version"],
        "spec": str(payload.get("spec") or ""),
        "git_sha": payload["git_sha"],
        "run_date": payload["run_date"],
        "resolved": "1" if payload.get("actual") is not None else "0",
        "truth_primary": payload["truth"]["primary"],
        "truth_secondary": str(payload["truth"].get("secondary") or ""),
    }

    params = {
        "month": month,
        "cutoff": payload["cutoff"],
        "run_date": payload["run_date"],
        "version": payload["version"],
        "v1_code_sha": str(payload.get("v1_code_sha") or ""),
        "git_sha": payload["git_sha"],
        "spec": str(payload.get("spec") or ""),
        "unit": str(payload.get("unit") or ""),
        "json_sha256": sha,
        "source_path": display_path(path),
        "served.model": str(payload["served"]["model"]),
        "served.fallback_level": str(payload["served"]["fallback_level"]),
    }
    for name, digest in payload["inputs_sha256"].items():
        params[safe_key(f"input.{name}_sha256")] = str(digest)
    params.update(_flatten_asof(payload["asof"]))

    s0 = payload.get("s0")
    if isinstance(s0, dict):
        if isinstance(s0.get("identical"), bool):
            params["s0.identical"] = "1" if s0["identical"] else "0"
        months = s0.get("months")
        if isinstance(months, list) and all(isinstance(item, str) for item in months):
            params["s0.months"] = ",".join(months)

    metrics = {}
    for name in SHADOW_MODELS:
        metrics[f"pred.{name}"] = float(payload["models"][name])
    components = payload.get("components") or {}
    for name in COMPONENTS:
        value = components.get(name)
        if _finite(value):
            metrics[f"pred.{name}"] = float(value)
    metrics["served.value"] = float(payload["served"]["value"])

    weekly = payload.get("weekly_features")
    if isinstance(weekly, dict):
        for key, value in weekly.items():
            if _finite(value):
                metrics[safe_key(f"weekly_features.{key}")] = float(value)

    if isinstance(s0, dict):
        for name in SHADOW_MODELS:
            item = s0.get(name)
            if isinstance(item, dict) and _finite(item.get("max_abs_diff")):
                metrics[f"s0.max_abs_diff.{name}"] = float(item["max_abs_diff"])

    actual = payload.get("actual")
    if isinstance(actual, dict):
        rt = _actual_value(actual, "t_live_rt", "T_LIVE_rt")
        metrics["actual.t_live_rt"] = rt
        t_live = _actual_value(actual, "t_live", "T_LIVE")
        if t_live is not None:
            metrics["actual.t_live"] = t_live
        error = (float(payload["served"]["value"]) - rt) * 100.0
        metrics["error_bp"] = error
        metrics["abs_error_bp"] = abs(error)

    errors_bp = {}
    if isinstance(actual, dict):
        rt = _actual_value(actual, "t_live_rt", "T_LIVE_rt")
        for name in SHADOW_MODELS:
            errors_bp[name] = (float(payload["models"][name]) - rt) * 100.0
        errors_bp["served"] = (float(payload["served"]["value"]) - rt) * 100.0

    for key, value in params.items():
        _check_length("param", key, value)
    for key, value in tags.items():
        _check_length("tag", key, value)

    summary = {
        "experiment": EXPERIMENT,
        "run_name": run_name(month),
        "source_path": display_path(path),
        "json_sha256": sha,
        "logged_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "month": month,
        "tags": tags,
        "params": params,
        "metrics": metrics,
        "errors_bp": errors_bp,
        "actual": actual,
    }
    return {
        "run_name": run_name(month),
        "tags": tags,
        "params": params,
        "metrics": metrics,
        "summary": summary,
    }


# ---------------------------------------------------------------------------
# MLflow
# ---------------------------------------------------------------------------


def ensure_experiment(client) -> str:
    experiment = client.get_experiment_by_name(EXPERIMENT)
    if experiment is not None:
        return str(experiment.experiment_id)
    return str(client.create_experiment(EXPERIMENT))


def find_existing(client, experiment_id: str, month: str, sha: str):
    """Run id с тем же `(month, json_sha256)` или `None`."""
    runs = client.search_runs(
        [experiment_id],
        filter_string=f"tags.month = '{month}' and tags.json_sha256 = '{sha}'",
        order_by=["attributes.start_time DESC"],
        max_results=1,
    )
    return runs[0].info.run_id if runs else None


def _log_mlflow(plan: dict, raw_text: str, tracking_uri: str, force: bool) -> dict:
    from mlflow.tracking import MlflowClient

    month = plan["tags"]["month"]
    sha = plan["tags"]["json_sha256"]
    client = MlflowClient(tracking_uri=tracking_uri)
    experiment_id = ensure_experiment(client)
    existing = find_existing(client, experiment_id, month, sha)
    if existing and not force:
        return {"status": "skipped", "month": month, "run_id": existing,
                "experiment_id": experiment_id, "run_name": plan["run_name"]}

    created = client.create_run(
        experiment_id, run_name=plan["run_name"], tags=plan["tags"]
    )
    run_id = created.info.run_id
    try:
        for key, value in plan["params"].items():
            client.log_param(run_id, key, value)
        for key, value in plan["metrics"].items():
            client.log_metric(run_id, key, value)
        client.log_text(run_id, raw_text, "shadow.json")
        client.log_dict(run_id, plan["summary"], "run_summary.json")
        client.set_terminated(run_id, status="FINISHED")
    except Exception:
        client.set_terminated(run_id, status="FAILED")
        raise
    return {"status": "created", "month": month, "run_id": run_id,
            "experiment_id": experiment_id, "run_name": plan["run_name"]}


def log_path(path: Path, tracking_uri: str, expected_month: str = None,
             force: bool = False, dry_run: bool = False) -> dict:
    """Логирует один shadow-JSON; при `dry_run` MLflow не импортируется."""
    payload, text, sha = load_payload(path)
    if expected_month is not None and payload["month"] != expected_month:
        raise ShadowJsonError(
            f"month: в JSON {payload['month']!r}, запрошено {expected_month!r}"
        )
    plan = build_plan(payload, path, sha)
    if dry_run:
        return {"status": "would-create", "month": payload["month"], "plan": plan}
    return _log_mlflow(plan, text, tracking_uri, force)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def collect_paths(month: str = None, path: str = None, backfill: bool = False) -> list:
    if path:
        return [Path(path)]
    if month:
        return [SHADOW_DIR / f"{month}.json"]
    if backfill:
        paths = sorted(
            item for item in SHADOW_DIR.glob("*.json")
            if _valid_month(item.stem)
        )
        if not paths:
            raise ShadowJsonError(f"в {display_path(SHADOW_DIR)} нет shadow-JSON")
        return paths
    raise ShadowJsonError("нужен --month, --path или --backfill")


def _parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MLflow-зеркало paper shadow wk_mix_ex@v1 (P0.6a #462)"
    )
    parser.add_argument("--month", help="месяц YYYY-MM (файл <месяц>.json)")
    parser.add_argument("--path", help="явный путь к shadow-JSON")
    parser.add_argument("--backfill", action="store_true",
                        help="залогировать все месяцы из docs/research/cb-comp-h1-shadow")
    parser.add_argument("--dry-run", action="store_true",
                        help="валидировать и напечатать план, не трогая MLflow")
    parser.add_argument("--force", action="store_true",
                        help="создать новый run, даже если (month, json_sha256) уже есть")
    parser.add_argument("--tracking-uri",
                        default=os.environ.get("MLFLOW_TRACKING_URI") or DEFAULT_URI,
                        help="MLflow tracking URI (по умолчанию MLFLOW_TRACKING_URI или %(default)s)")
    args = parser.parse_args(argv)
    if args.month is not None and not _valid_month(args.month):
        parser.error(f"--month: ожидается YYYY-MM, получено {args.month!r}")
    if not (args.month or args.path or args.backfill):
        parser.error("нужен --month, --path или --backfill")
    return args


def main(argv=None) -> int:
    args = _parse_args(argv)
    try:
        paths = collect_paths(args.month, args.path, args.backfill)
    except ShadowJsonError as exc:
        print(f"{exc}", file=sys.stderr)
        return 1

    failures = 0
    for path in paths:
        try:
            payload, _text, _sha = load_payload(path)
            if args.backfill and args.path is None and path.stem != payload["month"]:
                raise ShadowJsonError(
                    f"имя файла {path.name!r} не совпадает с month {payload['month']!r}"
                )
            result = log_path(
                path, args.tracking_uri, expected_month=args.month,
                force=args.force, dry_run=args.dry_run,
            )
        except (OSError, ShadowJsonError) as exc:
            print(f"{path}: {exc}", file=sys.stderr)
            failures += 1
            continue
        if result["status"] == "would-create":
            print(json.dumps(
                {
                    "month": result["month"],
                    "run_name": result["plan"]["run_name"],
                    "tags": result["plan"]["tags"],
                    "params": result["plan"]["params"],
                    "metrics": result["plan"]["metrics"],
                    "artifacts": ["shadow.json", "run_summary.json"],
                },
                ensure_ascii=False, indent=2, sort_keys=True,
            ))
        else:
            print(f"{result['month']}: {result['status']} {result['run_id']} "
                  f"{result['run_name']} (experiment {result['experiment_id']})",
                  flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
