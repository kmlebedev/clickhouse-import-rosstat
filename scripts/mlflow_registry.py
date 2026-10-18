#!/usr/bin/env python3
"""Model registry MLflow: регистрация версии и алиасы `challenger`/`champion`.

Запуск (mlflow импортируется лениво, только в `main`):

    python3 scripts/mlflow_registry.py register \
        --run-id <mlflow-run-id> --name cpi_nowcast_catboost_direct
    python3 scripts/mlflow_registry.py promote \
        --name cpi_nowcast_catboost_direct --version 1
    python3 scripts/mlflow_registry.py list \
        --name cpi_nowcast_catboost_direct

`--tracking-uri` по умолчанию берётся из `MLFLOW_TRACKING_URI`, иначе
`http://127.0.0.1:5000`.

Промоушен в infra/4.2 ручной (гейт качества — infra/4.3): скрипт не проверяет
метрики и только двигает алиас `champion`, не снимая `challenger`.

Имя модели (`--name` у `register`/`list`) ограничено набором `[A-Za-z0-9._-]`:
парсер `filter_string` MLflow не разэкранирует `\'`/`\\`, поэтому имена с
кавычкой или обратным слэшем отвергаются с кодом 1 до вызова MLflow. При
листинге без `--name` посторонние имена из реестра не подставляются в
`filter_string`, а пропускаются с warning.

Тесты — stdlib-only, без пакета mlflow и без сети:

    python3 scripts/mlflow_registry_test.py -v
"""

import argparse
import logging
import os
import re
import sys

DEFAULT_TRACKING_URI = "http://127.0.0.1:5000"

log = logging.getLogger(__name__)

CHALLENGER_ALIAS = "challenger"
CHAMPION_ALIAS = "champion"
FALLBACK_ALIASES = (CHAMPION_ALIAS, CHALLENGER_ALIAS)

# Код MLflow для отсутствующего объекта. Только он означает «модели/алиаса
# нет»: текст «not found» может прийти от прокси/HTTP 404 и не должен
# превращаться в отсутствующий объект registry.
NOT_FOUND_ERROR_CODE = "RESOURCE_DOES_NOT_EXIST"

MODEL_COLUMNS = ("name", "version", "status", "aliases", "run_id")

# Консервативный allowlist: имя модели подставляется в `filter_string`
# MLflow, парсер которого не разэкранирует `\'` и `\\`.
MODEL_NAME_RE = re.compile(r"[A-Za-z0-9._-]+")


class UnsafeModelNameError(ValueError):
    """Имя модели содержит символы, ломающие `filter_string` MLflow."""


def _validate_model_name(name: str) -> str:
    """Проверяет имя модели до вызова MLflow и возвращает его же."""
    if not MODEL_NAME_RE.fullmatch(name):
        raise UnsafeModelNameError(
            f"invalid model name {name!r}: allowed characters are "
            "[A-Za-z0-9._-]"
        )
    return name


def _is_not_found(exc: Exception) -> bool:
    """`True` только для «объект не найден» по коду ошибки MLflow.

    `get_registered_model` и `get_model_version_by_alias` поднимают
    `MlflowException` и при отсутствии объекта, и при HTTP-ошибке. Признак
    отсутствия — только `error_code == RESOURCE_DOES_NOT_EXIST`; свободный
    текст ошибки (например, «Not Found» от прокси) отсутствием не считается.
    """
    code = getattr(exc, "error_code", None)
    if code is None:
        return False
    # MLflow может отдать строку или ErrorCode.RESOURCE_DOES_NOT_EXIST.
    text = str(code)
    return (
        text == NOT_FOUND_ERROR_CODE
        or text.endswith("." + NOT_FOUND_ERROR_CODE)
    )


def register_version(client, name: str, run_id: str):
    """Регистрирует версию из `runs:/<run_id>/model` и ставит `challenger`.

    Зарегистрированная модель создаётся, если её ещё нет. Возвращает объект
    версии MLflow (`version.version` — номер строкой). Имя модели обязано
    проходить allowlist `[A-Za-z0-9._-]` (проверяется до вызова клиента).
    """
    _validate_model_name(name)
    try:
        existing = client.get_registered_model(name)
    except Exception as exc:
        if not _is_not_found(exc):
            raise
        existing = None
    if existing is None:
        client.create_registered_model(name)

    version = client.create_model_version(
        name,
        source=f"runs:/{run_id}/model",
        run_id=run_id,
    )
    client.set_registered_model_alias(
        name, CHALLENGER_ALIAS, version.version
    )
    print(
        f"registered {name} v{version.version}; "
        f"alias {CHALLENGER_ALIAS} -> v{version.version}"
    )
    return version


def promote_version(
    client, name: str, version, alias: str = CHAMPION_ALIAS
) -> None:
    """Двигает алиас `alias` (по умолчанию `champion`) на версию `version`.

    `challenger` не снимается: promotion в 4.2 ручной и не является гейтом.
    """
    client.set_registered_model_alias(name, alias, version)
    print(f"alias {alias} -> v{version}")


def _version_sort_key(version):
    """Сортирует версии численно; нечисловые — после числовых, по строке."""
    text = str(getattr(version, "version", ""))
    try:
        return (0, int(text))
    except ValueError:
        return (1, text)


def _version_aliases(client, name: str, version) -> list:
    """Алиасы версии из `version.aliases`; fallback — только для старого API.

    В MLflow 3.x атрибут есть всегда, у версии без алиасов он равен `[]`;
    запросы `get_model_version_by_alias` в этом случае не нужны (N+1).
    """
    aliases = getattr(version, "aliases", None)
    if aliases is not None:
        return list(aliases)

    found = []
    for alias in FALLBACK_ALIASES:
        try:
            aliased = client.get_model_version_by_alias(name, alias)
        except Exception as exc:
            if not _is_not_found(exc):
                raise
            continue
        if str(getattr(aliased, "version", "")) == str(version.version):
            found.append(alias)
    return found


def format_models(rows: list) -> str:
    """Таблица `name version status aliases run_id` (tab-separated)."""
    lines = ["\t".join(MODEL_COLUMNS)]
    for row in rows:
        aliases = ",".join(row["aliases"]) if row["aliases"] else "-"
        lines.append(
            "\t".join(
                (
                    row["name"],
                    row["version"],
                    row["status"],
                    aliases,
                    row["run_id"],
                )
            )
        )
    return "\n".join(lines)


def list_models(client, name=None) -> list:
    """Печатает и возвращает версии моделей с их алиасами.

    Без `name` обходит все зарегистрированные модели; с `name` — только её
    (отсутствующая модель даёт `MlflowException`, CLI завершается кодом 1).
    Allowlist `[A-Za-z0-9._-]` проверяет только имя, заданное пользователем:
    чужие имена из реестра не подставляются в `filter_string`, а
    пропускаются с warning — одна посторонняя модель не должна ломать
    `make mlflow-models` для всех остальных.
    """
    if name is not None:
        _validate_model_name(name)
        client.get_registered_model(name)
        names = [name]
    else:
        models = client.search_registered_models()
        names = []
        for model in models:
            model_name = getattr(model, "name", str(model))
            if not MODEL_NAME_RE.fullmatch(model_name):
                log.warning(
                    "registry: модель %r пропущена — имя вне allowlist "
                    "[A-Za-z0-9._-] и не может быть подставлено в "
                    "filter_string",
                    model_name,
                )
                continue
            names.append(model_name)
        names.sort()

    rows = []
    for model_name in names:
        versions = client.search_model_versions(f"name='{model_name}'")
        for version in sorted(versions, key=_version_sort_key):
            rows.append(
                {
                    "name": model_name,
                    "version": str(version.version),
                    "status": str(getattr(version, "status", "")),
                    "aliases": _version_aliases(client, model_name, version),
                    "run_id": str(getattr(version, "run_id", "") or ""),
                }
            )

    print(format_models(rows))
    return rows


def _parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Регистрирует версии модели в MLflow и управляет алиасами "
            "challenger/champion."
        ),
    )
    parser.add_argument(
        "--tracking-uri",
        default=(
            os.environ.get("MLFLOW_TRACKING_URI") or DEFAULT_TRACKING_URI
        ),
        help=(
            "MLflow Tracking URI (по умолчанию MLFLOW_TRACKING_URI "
            "или %(default)s)"
        ),
    )

    commands = parser.add_subparsers(dest="command", required=True)

    register = commands.add_parser(
        "register", help="зарегистрировать версию из runs:/<run_id>/model"
    )
    register.add_argument("--run-id", required=True, help="MLflow run id")
    register.add_argument(
        "--name",
        required=True,
        help="имя модели (только A-Za-z0-9._-)",
    )

    promote = commands.add_parser(
        "promote", help="перевести алиас champion на версию"
    )
    promote.add_argument("--name", required=True, help="имя модели")
    promote.add_argument("--version", required=True, help="номер версии")
    promote.add_argument(
        "--alias",
        default=CHAMPION_ALIAS,
        help="алиас для перевода (по умолчанию %(default)s)",
    )

    listing = commands.add_parser(
        "list", help="показать версии и алиасы моделей"
    )
    listing.add_argument(
        "--name",
        help="только указанная модель (только A-Za-z0-9._-; по умолчанию все)",
    )

    return parser.parse_args(argv)


def main(argv=None) -> int:
    """CLI; 0 — успех, 1 — ожидаемая ошибка MLflow/сети/импорта или имени.

    Имена для `register`/`list` проверяются allowlist-ом `[A-Za-z0-9._-]`
    до ленивого импорта mlflow, потому что они подставляются в
    `filter_string`. Перехватываются только `MlflowException` и `OSError`
    (в него входят сетевые `requests`-ошибки). Ошибки программирования
    (TypeError, AttributeError) пробрасываются traceback-ом, а не
    маскируются под нормальный отказ MLflow.
    """
    args = _parse_args(argv)

    if args.command in ("register", "list") and args.name is not None:
        try:
            _validate_model_name(args.name)
        except UnsafeModelNameError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1

    # Ленивый импорт: модуль и чистые функции работают без установленного
    # mlflow, тесты подменяют пакет через sys.modules. Это граница
    # зависимости, поэтому здесь допустим широкий `except`.
    try:
        from mlflow import MlflowClient
        from mlflow.exceptions import MlflowException
    except Exception as exc:
        print(f"mlflow недоступен: {exc}", file=sys.stderr)
        return 1

    try:
        client = MlflowClient(tracking_uri=args.tracking_uri)
    except (MlflowException, OSError) as exc:
        print(f"MLflow-клиент не создан: {exc}", file=sys.stderr)
        return 1

    try:
        if args.command == "register":
            register_version(client, args.name, args.run_id)
        elif args.command == "promote":
            promote_version(client, args.name, args.version, args.alias)
        else:
            list_models(client, args.name)
    except (MlflowException, OSError, UnsafeModelNameError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
