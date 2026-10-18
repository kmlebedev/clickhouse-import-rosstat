#!/usr/bin/env python3
"""Запись прогноза ключевой ставки в `forecast_log` через ingest-контур.

Пункт роадмапа `key-rate/phase-4-forecast-log` (issue #127), спека
`docs/superpowers/specs/2026-10-18-key-rate-forecast-log-design.md`.

Единственный путь записи — `POST /v1/key_rate_forecast` ingest-сервиса
(`cmd/ingest`). Скрипт **не пишет в ClickHouse напрямую** и не читает его: вход —
локальные артефакты модели, поэтому writer не зависит от соединения с БД и не
создаёт второй путь записи (MCP для записи закрыт намеренно).

Входы:

* `key_rate_model_forecast.json` (обязателен) — текущий прогноз
  `scripts/key_rate_model.py`. Строки треков:
  `cbr_rate:taylor` (шаг `step_bp`) и `cbr_rate:ordered_logit` (шаг
  `expected_step_bp` + вероятности классов). `predicted` — ожидаемая СТАВКА
  после заседания: `rate_before + step_bp/100`, где `rate_before` — ставка на
  дату среза из того же артефакта (поле добавлено этим пунктом).
* `key_rate_llm_forecast.json` (необязателен) — артефакт LLM-трека (issue #189,
  спека `2026-10-18-llm-rate-forecast-design.md` §7). Отсутствие файла —
  штатно: трек `llm` просто не пишется, о чём скрипт сообщает строкой.

Env (подключает Makefile; без них — понятная ошибка, не молчаливый пропуск):

* `INGEST_URL` — базовый URL ingest-сервиса, например `http://127.0.0.1:8081`;
* `INGEST_TOKEN` — Bearer-токен (тот же, что у сервера; значение не печатается).

Запуск:

    make key-rate-forecast-log
    make key-rate-forecast-log ARGS="--dry-run"
    make key-rate-forecast-log ARGS="--llm-artifact PATH"

Повтор идемпотентен на стороне ClickHouse: ключ `forecast_log` —
`(metric, forecast_date)`, поэтому повторная запись того же трека заменяет
строку, а не плодит дубли.
"""

import argparse
import json
import math
import os
import sys
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Имена артефактов по умолчанию — те же, что пишут `key_rate_model.py` и
# LLM-трек; оба gitignored.
MODEL_ARTIFACT_NAME = "key_rate_model_forecast.json"
LLM_ARTIFACT_NAME = "key_rate_llm_forecast.json"

# Путь ingest-endpoint'а (единственный путь записи прогноза ставки).
INGEST_PATH = "/v1/key_rate_forecast"

# Таймаут одного HTTP-запроса, секунды.
HTTP_TIMEOUT = 30

# Сколько байт тела ошибки сервера показывать в консоли.
ERROR_BODY_LIMIT = 512

# Env-переменные ingest-клиента. Имена серверных переменных (`INGEST_ADDR`) для
# клиента не годятся: это адрес bind, а не URL, по которому сервер доступен.
INGEST_URL_ENV = "INGEST_URL"
INGEST_TOKEN_ENV = "INGEST_TOKEN"

# Классы решения в модели v0 — вероятности в артефакте лежат долями (0..1).
MODEL_CLASS_KEYS = ("-1", "0", "+1")
MODEL_CLASS_NAMES = ("cut", "hold", "hike")

# Допуск суммы распределения (в долях для модели, в процентах для LLM).
PROBABILITY_SUM_TOLERANCE = 1e-6
PROBABILITY_PERCENT_TOLERANCE = 0.1


class ForecastLogError(Exception):
    """Ошибка входа: артефакт, env или ответ ingest. Текст — без секретов."""


# --- Разбор артефактов ---------------------------------------------------------


def _date(value, field: str) -> date:
    """Дата артефакта: ISO `YYYY-MM-DD` (ClickHouse Date в журнале)."""
    if not isinstance(value, str):
        raise ForecastLogError(f"{field}: ожидается дата YYYY-MM-DD, получено {value!r}")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise ForecastLogError(f"{field}: {value!r} — не дата YYYY-MM-DD") from error
    if parsed.isoformat() != value:
        raise ForecastLogError(f"{field}: {value!r} — не дата YYYY-MM-DD")
    return parsed


def _number(value, field: str) -> float:
    """Конечное число артефакта; bool и строки отвергаются явно."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ForecastLogError(f"{field}: ожидается число, получено {value!r}")
    number = float(value)
    if not math.isfinite(number):
        raise ForecastLogError(f"{field}: {value!r} — не конечное число")
    return number


def _non_empty(value, field: str) -> str:
    """Непустая строка артефакта."""
    if not isinstance(value, str) or not value.strip():
        raise ForecastLogError(f"{field}: ожидается непустая строка, получено {value!r}")
    return value.strip()


def _rate(value, field: str) -> float:
    """Ожидаемая ставка, % годовых: положительная и правдоподобная.

    Верхняя граница та же, что у ingest (`keyRateForecastMaxRate`): перепутанные
    единицы (шаг в б.п. вместо уровня) должны падать в writer'е с именем поля, а
    не превращаться в 14 000 % после записи.
    """
    rate = _number(value, field)
    if rate <= 0 or rate >= 1000:
        raise ForecastLogError(f"{field}: {value!r} — ставка должна быть в (0, 1000) % годовых")
    return rate


def _require(mapping: dict, field: str, what: str):
    """Поле артефакта обязательно; отсутствие — ошибка с именем поля."""
    if not isinstance(mapping, dict):
        raise ForecastLogError(f"{what}: ожидается JSON-объект, получено {type(mapping).__name__}")
    if field not in mapping:
        raise ForecastLogError(f"{what}: отсутствует обязательное поле {field!r}")
    return mapping[field]


def model_data_version(forecast_date: date, sample_size: int) -> str:
    """Версия набора данных v0: дата прогноза + размер выборки (спека §8).

    Тот же смысл, что у строки отчёта «версия набора данных v0 = дата прогона +
    размер выборки»: без неё публикация запрещена (KEY_RATE_PUBLICATIONS §7 п. 5).
    """
    return f"key-rate-model-v0:{forecast_date.isoformat()}:n={sample_size}"


def model_probabilities(probs, field: str) -> dict:
    """Вероятности модели v0: доли `-1/0/+1` → проценты `cut/hold/hike`.

    Артефакт хранит доли (сумма 1), endpoint и журнал — проценты (сумма 100):
    перевод делается здесь один раз, чтобы writer не зависел от того, какую
    шкалу выберет следующая редакция артефакта.
    """
    if not isinstance(probs, dict):
        raise ForecastLogError(f"{field}: ожидается объект классов -1/0/+1, получено {probs!r}")
    if sorted(probs) != sorted(MODEL_CLASS_KEYS):
        raise ForecastLogError(
            f"{field}: ключи должны быть {', '.join(MODEL_CLASS_KEYS)}, получено {sorted(probs)}"
        )
    values = [_number(probs[key], f"{field}[{key!r}]") for key in MODEL_CLASS_KEYS]
    if any(value < 0.0 or value > 1.0 for value in values):
        raise ForecastLogError(f"{field}: вероятности должны быть долями 0..1, получено {values}")
    total = sum(values)
    if abs(total - 1.0) > PROBABILITY_SUM_TOLERANCE:
        raise ForecastLogError(f"{field}: сумма вероятностей {total!r} ≠ 1")
    return {
        name: value * 100.0 for name, value in zip(MODEL_CLASS_NAMES, values)
    }


def model_rows(artifact: dict) -> tuple:
    """Строки журнала из артефакта `key_rate_model_forecast.json`.

    Возврат `(rows, missing)`: `rows` — taylor и/или ordered_logit, `missing` —
    имена методов, которых в артефакте нет (вырожденное окно — штатное состояние
    модели). Если не осталось ни одного метода, вызывающий считает это ошибкой:
    журнал без прогноза не пишется.
    """
    forecast_date = _date(_require(artifact, "forecast_date", "артефакт модели"), "forecast_date")
    meeting_date = _date(_require(artifact, "meeting_date", "артефакт модели"), "meeting_date")
    if meeting_date < forecast_date:
        raise ForecastLogError(
            f"артефакт модели: meeting_date {meeting_date} раньше forecast_date {forecast_date}"
        )
    if "rate_before" not in artifact:
        raise ForecastLogError(
            "артефакт модели: нет поля rate_before — перегенерируйте `make key-rate-model` "
            "(без ставки до заседания шаг в б.п. не перевести в ожидаемую ставку)"
        )
    rate_before = _rate(artifact["rate_before"], "rate_before")

    methods = _require(artifact, "methods", "артефакт модели")
    if not isinstance(methods, dict):
        raise ForecastLogError("артефакт модели: methods должен быть JSON-объектом")
    sample_size_value = _number(_require(artifact, "sample_size", "артефакт модели"), "sample_size")
    if not sample_size_value.is_integer() or sample_size_value <= 0:
        raise ForecastLogError(
            f"артефакт модели.sample_size: ожидается целое > 0, получено {sample_size_value!r}"
        )
    sample_size = int(sample_size_value)
    data_version = model_data_version(forecast_date, sample_size)

    rows = []
    missing = []

    if "taylor" in methods:
        body = methods["taylor"]
        step_bp = _number(_require(body, "step_bp", "methods.taylor"), "methods.taylor.step_bp")
        rows.append(
            {
                "forecast_date": forecast_date.isoformat(),
                "target_date": meeting_date.isoformat(),
                "metric": "cbr_rate:taylor",
                "predicted": _rate(rate_before + step_bp / 100.0, "methods.taylor.predicted"),
                "data_version": data_version,
                "model": "taylor",
                "prompt_version": 0,
            }
        )
    else:
        missing.append("taylor")

    if "ordered_logit" in methods:
        body = methods["ordered_logit"]
        step_bp = _number(
            _require(body, "expected_step_bp", "methods.ordered_logit"),
            "methods.ordered_logit.expected_step_bp",
        )
        probabilities = model_probabilities(
            _require(body, "probs", "methods.ordered_logit"),
            "methods.ordered_logit.probs",
        )
        rows.append(
            {
                "forecast_date": forecast_date.isoformat(),
                "target_date": meeting_date.isoformat(),
                "metric": "cbr_rate:ordered_logit",
                "predicted": _rate(
                    rate_before + step_bp / 100.0, "methods.ordered_logit.predicted"
                ),
                "probabilities": probabilities,
                "data_version": data_version,
                "model": "ordered_logit",
                "prompt_version": 0,
            }
        )
    else:
        missing.append("ordered_logit")

    return rows, missing


def llm_row(artifact: dict) -> dict:
    """Строка журнала из артефакта LLM-трека (issue #189, спека §7).

    `predicted` — ожидаемая ставка: `expected_rate`, а если его нет —
    `current_rate + expected_step_bp/100`. Распределение уже в процентах
    (`cut/hold/hike`); модель и `prompt_version` обязательны: без них трек
    невоспроизводим, а #189 требует их в журнале.
    """
    forecast_date = _date(_require(artifact, "forecast_date", "LLM-артефакт"), "forecast_date")
    meeting_date = _date(_require(artifact, "meeting_date", "LLM-артефакт"), "meeting_date")
    if meeting_date < forecast_date:
        raise ForecastLogError(
            f"LLM-артефакт: meeting_date {meeting_date} раньше forecast_date {forecast_date}"
        )

    if "expected_rate" in artifact:
        predicted = _rate(artifact["expected_rate"], "LLM-артефакт.expected_rate")
    else:
        current_rate = _rate(
            _require(artifact, "current_rate", "LLM-артефакт"), "LLM-артефакт.current_rate"
        )
        step_bp = _number(
            _require(artifact, "expected_step_bp", "LLM-артефакт"),
            "LLM-артефакт.expected_step_bp",
        )
        predicted = _rate(current_rate + step_bp / 100.0, "LLM-артефакт.expected_rate")

    probabilities = _require(artifact, "probabilities", "LLM-артефакт")
    if not isinstance(probabilities, dict):
        raise ForecastLogError("LLM-артефакт.probabilities: ожидается JSON-объект cut/hold/hike")
    expected_keys = ("cut", "hold", "hike")
    if sorted(probabilities) != sorted(expected_keys):
        raise ForecastLogError(
            "LLM-артефакт.probabilities: ключи должны быть cut, hold, hike, "
            f"получено {sorted(probabilities)}"
        )
    values = {
        key: _number(probabilities[key], f"LLM-артефакт.probabilities[{key!r}]")
        for key in expected_keys
    }
    if any(value < 0.0 or value > 100.0 for value in values.values()):
        raise ForecastLogError(
            f"LLM-артефакт.probabilities: вероятности должны быть в процентах 0..100, получено {values}"
        )
    total = sum(values.values())
    if abs(total - 100.0) > PROBABILITY_PERCENT_TOLERANCE:
        raise ForecastLogError(f"LLM-артефакт.probabilities: сумма {total!r} ≠ 100")

    prompt_version = _require(artifact, "prompt_version", "LLM-артефакт")
    if isinstance(prompt_version, bool) or not isinstance(prompt_version, int) or prompt_version <= 0:
        raise ForecastLogError(
            f"LLM-артефакт.prompt_version: ожидается целое > 0, получено {prompt_version!r}"
        )
    model = _non_empty(_require(artifact, "model", "LLM-артефакт"), "LLM-артефакт.model")
    data_version = _non_empty(
        _require(artifact, "data_version", "LLM-артефакт"), "LLM-артефакт.data_version"
    )

    return {
        "forecast_date": forecast_date.isoformat(),
        "target_date": meeting_date.isoformat(),
        "metric": "cbr_rate:llm",
        "predicted": predicted,
        "probabilities": values,
        "data_version": data_version,
        "model": model,
        "prompt_version": prompt_version,
    }


# --- HTTP и CLI ---------------------------------------------------------------


def _load_json(path: Path, what: str) -> dict:
    """Читает JSON-объект артефакта; ошибки — с путём и без содержимого."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ForecastLogError(f"{what}: файл не найден: {path}") from error
    except OSError as error:
        raise ForecastLogError(f"{what}: не удалось прочитать {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise ForecastLogError(f"{what}: {path} — некорректный JSON: {error}") from error
    if not isinstance(payload, dict):
        raise ForecastLogError(f"{what}: {path} — ожидается JSON-объект")
    return payload


def ingest_url(base: str) -> str:
    """URL endpoint'а записи из базового `INGEST_URL`.

    `INGEST_URL` — база сервиса (`http://host:port`), а не полный путь: writer
    сам добавляет `/v1/key_rate_forecast`, чтобы опечатка в пути не создавала
    второй endpoint.
    """
    base = base.strip().rstrip("/")
    if not base:
        raise ForecastLogError(f"{INGEST_URL_ENV}: пустое значение")
    if base.endswith(INGEST_PATH):
        return base
    return base + INGEST_PATH


def post_row(url: str, token: str, row: dict, timeout: int = HTTP_TIMEOUT) -> dict:
    """POST одной строки в ingest; non-2xx и обрыв — ForecastLogError.

    Токен уходит только заголовком `Authorization`; тело ошибки сервера
    обрезается до ERROR_BODY_LIMIT байт и в сообщение не попадает ни заголовок,
    ни значение токена.
    """
    body = json.dumps(row, ensure_ascii=False, sort_keys=True).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read()
    except urllib.error.HTTPError as error:
        snippet = error.read(ERROR_BODY_LIMIT).decode("utf-8", errors="replace")
        raise ForecastLogError(
            f"ingest {url}: HTTP {error.code}: {snippet.strip()}"
        ) from error
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        raise ForecastLogError(f"ingest {url}: запрос не выполнен: {error}") from error

    if not payload:
        return {}
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ForecastLogError(f"ingest {url}: ответ не JSON: {error}") from error


def rows_from_artifacts(model_path: Path, llm_path: Path, log=print) -> list:
    """Строки журнала из обоих артефактов; отсутствие LLM-файла — штатно."""
    artifact = _load_json(model_path, "артефакт модели")
    rows, missing = model_rows(artifact)
    if missing:
        log(f"warn: в артефакте модели нет методов: {', '.join(missing)} — трек не пишется")
    if not rows:
        raise ForecastLogError(
            f"артефакт модели {model_path}: ни одного метода — журнал без прогноза не пишется"
        )

    if llm_path.exists():
        rows.append(llm_row(_load_json(llm_path, "LLM-артефакт")))
    else:
        log(f"LLM-артефакт не найден ({llm_path}) — трек cbr_rate:llm не пишется")

    return rows


def _env_required(name: str) -> str:
    """Обязательная env-переменная; ошибка называет переменную, не значение."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise ForecastLogError(
            f"не задана переменная {name} (нужны {INGEST_URL_ENV} и {INGEST_TOKEN_ENV}; "
            "значения подключает Makefile)"
        )
    return value


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Запись прогноза ключевой ставки в forecast_log через ingest "
                    "(POST /v1/key_rate_forecast)."
    )
    parser.add_argument(
        "--model-artifact",
        default=str(ROOT / MODEL_ARTIFACT_NAME),
        help=f"артефакт key_rate_model.py (по умолчанию {MODEL_ARTIFACT_NAME})",
    )
    parser.add_argument(
        "--llm-artifact",
        default=str(ROOT / LLM_ARTIFACT_NAME),
        help=f"артефакт LLM-трека; отсутствие файла — штатно (по умолчанию {LLM_ARTIFACT_NAME})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="напечатать строки JSON и не отправлять их в ingest",
    )
    args = parser.parse_args(argv)

    try:
        rows = rows_from_artifacts(Path(args.model_artifact), Path(args.llm_artifact))
        if args.dry_run:
            for row in rows:
                print(json.dumps(row, ensure_ascii=False, sort_keys=True))
            print(f"dry-run: {len(rows)} строк(и) не отправлены")
            return 0

        base = _env_required(INGEST_URL_ENV)
        token = _env_required(INGEST_TOKEN_ENV)
        url = ingest_url(base)

        for row in rows:
            response = post_row(url, token, row)
            inserted = response.get("inserted", "?") if isinstance(response, dict) else "?"
            print(f"{row['metric']} на {row['target_date']}: predicted {row['predicted']:.4f} "
                  f"→ inserted {inserted}")
        print(f"записано строк: {len(rows)}")
        return 0
    except ForecastLogError as error:
        # Ошибка входа/сети: код 1 и текст без секретов. Молчаливый пропуск
        # запрещён: публикация без записи в журнал не выпускается.
        print(f"ошибка: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
