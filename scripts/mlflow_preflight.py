#!/usr/bin/env python3
"""Preflight MLflow API перед ML-прогоном.

ML-цели Makefile вызывают этот скрипт до `uv run`. Если MLflow API отвечает,
прогон продолжается. Если нет — скрипт явно спрашивает, запускать ли прогон
без лога; в неинтерактивном режиме (CI, агент) по умолчанию запрещает запуск,
пока не выставлен `MLFLOW_ALLOW_NO_LOG=1`.

Это не MLflow-клиент: проверка — обычный HTTP GET `/health`, без пакета
`mlflow` и без секретов. Код возврата 0 — можно запускать, 1 — нельзя.
"""

import argparse
import os
import sys
import urllib.error
import urllib.request

DEFAULT_URI = "http://127.0.0.1:5000"
ALLOW_NO_LOG_ENV = "MLFLOW_ALLOW_NO_LOG"
TIMEOUT_SECONDS = 3


def health(uri: str) -> bool:
    """`True`, если MLflow API отвечает 2xx на `/health`."""
    url = uri.rstrip("/") + "/health"
    try:
        with urllib.request.urlopen(url, timeout=TIMEOUT_SECONDS) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, urllib.error.HTTPError, OSError):
        return False


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="MLflow API preflight")
    parser.add_argument(
        "--uri",
        default=os.environ.get("MLFLOW_TRACKING_URI") or DEFAULT_URI,
        help=f"MLflow tracking URI (default: {DEFAULT_URI})",
    )
    arguments = parser.parse_args(argv)
    uri = arguments.uri
    if health(uri):
        print(f"MLflow API: OK ({uri})", flush=True)
        return 0
    if os.environ.get(ALLOW_NO_LOG_ENV) == "1":
        print(
            f"MLflow API: недоступен ({uri}); продолжение разрешено "
            f"{ALLOW_NO_LOG_ENV}=1 — прогон без лога",
            flush=True,
        )
        return 0
    message = (
        f"MLflow API недоступен ({uri}). "
        "Запускать прогон без логирования в MLflow?"
    )
    if not sys.stdin.isatty():
        print(
            message + " Нет TTY: запуск без лога запрещён. "
            f"Установите {ALLOW_NO_LOG_ENV}=1, чтобы продолжить осознанно.",
            file=sys.stderr,
            flush=True,
        )
        return 1
    try:
        answer = input(message + " [y/N]: ").strip().lower()
    except EOFError:
        answer = ""
    if answer in {"y", "yes", "д", "да"}:
        print("MLflow API: прогон без лога разрешён пользователем", flush=True)
        return 0
    print("MLflow API: прогон отменён", file=sys.stderr, flush=True)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
