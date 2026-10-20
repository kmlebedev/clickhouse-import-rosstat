#!/usr/bin/env python3
"""Общий транспорт и SQL-хелперы ClickHouse для Python-скриптов.

Выделено из `extrapolation_aisc.py` при раскладке `scripts/` по семействам
моделей (`infra/5.4`): этим кодом пользуются все модельньные треки, а не
только AISC-экстраполяция. Модуль ничего не знает про модели и не пишет
данные по своей инициативе.

Секреты приходят только из окружения (`CLICKHOUSE_URL`, `CH_IMPORT_PASSWORD`,
`CH_HTTP_PORT`); Makefile подключает env-файл сам.
"""

import base64
import http.client
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request


# Порт HTTP-интерфейса ClickHouse. `ch_config()` отдаёт ДВА порта, и это не
# взаимозаменяемые значения: `port` (9000) — нативный протокол, `http_port`
# (8123) — HTTP. `urllib.request` говорит по HTTP, поэтому на нативном порту
# он бы завис или упал. Для чтения используется ТОЛЬКО `http_port`.
DEFAULT_CH_HTTP_PORT = "8123"

# Таймаут HTTP-запроса, секунды. Значение конечно намеренно: без него
# недоступный сервер подвесил бы весь расчёт.
CH_QUERY_TIMEOUT_SECONDS = 30

# Псевдоним: HTTP-ошибка нужна вызывающему коду (и тестам) для подмены
# транспорта без импорта `urllib.error`.
HTTPError = urllib.error.HTTPError


def mask(secret: str, text: str) -> str:
    """Убирает секрет из текста сообщения.

    Слепой replace портит слова, содержащие короткий пароль ('x' ломает
    'Exception'), поэтому заменяются только вхождения, не окружённые
    буквами и цифрами.
    """
    if not secret:
        return text
    return re.sub(rf"(?<![0-9A-Za-z]){re.escape(secret)}(?![0-9A-Za-z])", "***", text)


def parse_clickhouse_url(url: str) -> dict:
    """Разбирает DSN `clickhouse://[user[:password]@]host[:port]/database`.

    Это ТОТ ЖЕ формат, что разбирает `clickhouse.ParseDSN` в Go-коде проекта
    (`cmd/ingest/main.go`, `financial/db.go`), потому что `CLICKHOUSE_URL` —
    основной способ задать подключение в этом репозитории, и env-файл
    `~/.config/rosstat/env` содержит именно его, а не дискретные
    `CLICKHOUSE_HOST`/`CLICKHOUSE_USER`/`CLICKHOUSE_DATABASE`.

    Незаданные части не подставляются: возвращаются только реально
    присутствующие в DSN ключи, чтобы вызывающий мог отличить «в DSN указано»
    от «не указано вовсе» и применить свой default. Порт здесь — НАТИВНЫЙ
    (обычно 9000); HTTP-порт к нему отношения не имеет и задаётся отдельно.
    """
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("clickhouse", "http", "https"):
        raise ValueError(
            f"CLICKHOUSE_URL со схемой {parsed.scheme!r}: ожидается "
            f"clickhouse://[user[:password]@]host[:port]/database — {url!r}"
        )
    out: dict = {"scheme": parsed.scheme}
    if parsed.hostname:
        out["host"] = parsed.hostname
    if parsed.port:
        out["port"] = str(parsed.port)
    if parsed.username:
        out["user"] = urllib.parse.unquote(parsed.username)
    if parsed.password:
        out["password"] = urllib.parse.unquote(parsed.password)
    if parsed.path and parsed.path != "/":
        out["database"] = parsed.path.lstrip("/")
    return out


def ch_config() -> dict:
    """Параметры подключения к ClickHouse из переменных окружения.

    Секреты приходят только из env: скрипт не читает `~/.config/rosstat/env`
    сам, это делает Makefile.

    Источник адреса — `CLICKHOUSE_URL` (DSN), как во всём остальном проекте.
    Дискретные `CLICKHOUSE_HOST`/`CLICKHOUSE_PORT`/`CLICKHOUSE_USER`/
    `CLICKHOUSE_DATABASE` работают как ПЕРЕОПРЕДЕЛЕНИЕ поверх DSN: так можно
    прогнать скрипт против другого сервера, не собирая DSN.

    Пароль берётся из `CH_IMPORT_PASSWORD` либо из DSN — и НИКОГДА из
    `CLICKHOUSE_PASSWORD`. Это не придирка: `CLICKHOUSE_PASSWORD` в этом
    репозитории — пароль `kimi_reader`, read-only пользователя ВИТРИН,
    которого Makefile задаёт для `mcp-check` (`MCP_USER ?= kimi_reader`).
    Скрипту же нужно читать СЫРУЮ таблицу `databook_polyus`, закрытую для
    `kimi_reader` (`ACCESS_DENIED`), то есть ходить под другим пользователем.
    Прогон 2026-10-11 показал, чем обходится путаница: `CLICKHOUSE_URL`
    объявляет пользователя `default` БЕЗ пароля (у локального dev-сервера его
    нет), подстановка `CLICKHOUSE_PASSWORD` «на всякий случай» давала 401 —
    чужой пароль к чужому пользователю.
    """
    cfg = {"host": "localhost", "port": "9000", "user": "", "password": "",
           "database": "default", "http_port": "8123"}

    url = os.environ.get("CLICKHOUSE_URL", "")
    if url:
        from_url = parse_clickhouse_url(url)
        for key in ("host", "port", "user", "password", "database"):
            if key in from_url:
                cfg[key] = from_url[key]

    for key, env in (("host", "CLICKHOUSE_HOST"), ("port", "CLICKHOUSE_PORT"),
                     ("user", "CLICKHOUSE_USER"), ("database", "CLICKHOUSE_DATABASE")):
        value = os.environ.get(env)
        if value:
            cfg[key] = value

    cfg["password"] = os.environ.get("CH_IMPORT_PASSWORD") or cfg["password"]
    cfg["http_port"] = os.environ.get("CH_HTTP_PORT", DEFAULT_CH_HTTP_PORT)
    return cfg


class ClickHouseError(Exception):
    """Сбой чтения из ClickHouse (транспорт, статус ответа, разбор JSON).

    Текст ошибки ВСЕГДА проходит через `mask()`: сервер отражает текст запроса
    и заголовки в теле ответа, и без маскирования пароль из basic auth мог бы
    попасть в лог или в отчёт вместе с диагностикой.
    """


def sql_literal(value) -> str:
    """Литерал ClickHouse для одного значения.

    * `None` → `NULL` (отличает «не раскрыто» от измеренного нуля);
    * `str` → в одинарных кавычках с удвоением тика (`O'HARA` → `'O''HARA'`):
      литерал не должен разрывать текст запроса;
    * `bool` → `1`/`0`: в Python `True` — это `int`, и без явной ветви он
      попал бы в `repr(True)` = `'True'`, что ClickHouse не разберёт;
    * `int` → `repr`;
    * `float` → `repr`, то есть ПОЛНАЯ точность двоичного представления
      (`0.1 + 0.2` → `'0.30000000000000004'`). Формат вроде `%.6f` внёс бы
      ошибку округления, неотличимую потом от ошибки модели.

    Не-конечные float НЕ допускаются: `NaN` и `±inf` поднимают `ValueError`.
    ClickHouse взял бы их как Float64 и записал бы строку, а `NaN`/`inf`
    разъехались бы по всему NPV.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        # Ветвь ОБЯЗАНА стоять до `isinstance(value, int)`: bool — подкласс int.
        return "1" if value else "0"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, int):
        return repr(value)
    if isinstance(value, float):
        if value != value:  # NaN: единственное значение, не равное себе
            raise ValueError("NaN недопустим в литерале SQL: значение не записано")
        if value == float("inf") or value == float("-inf"):
            raise ValueError(f"бесконечность недопустима в литерале SQL: {value!r}")
        return repr(value)
    raise ValueError(
        f"тип {type(value).__name__} не поддержан в литерале SQL: {value!r}"
    )


def parse_ch_json(payload: str) -> list:
    """Разбирает JSON-ответ ClickHouse в список строк-словарей.

    Формат ответа — `{"data":[{...}, ...]}` (плюс служебные секции, которые
    здесь не нужны). Отсутствие секции `data` равнозначно отсутствию строк:
    так выглядит пустой агрегат и ответ на служебные запросы, и это НЕ ошибка.

    Голый `json.JSONDecodeError` не годится как диагностика: он не называет
    источник (кто именно прислал мусор) и уносит в текст сырой payload,
    который при HTML-странице логина содержит не пароль, а форму входа.
    Поэтому поднимается `ClickHouseError` с позицией сбоя и длиной ответа,
    но БЕЗ самого тела.
    """
    try:
        parsed = json.loads(payload)
    except (json.JSONDecodeError, ValueError) as exc:
        raise ClickHouseError(
            f"ответ ClickHouse не разобран как JSON: {exc.msg} "
            f"(строка {exc.lineno}, колонка {exc.colno}; длина ответа {len(payload)} байт)"
        ) from exc
    if not isinstance(parsed, dict):
        raise ClickHouseError(
            f"ответ ClickHouse — не объект JSON, а {type(parsed).__name__}"
        )
    rows = parsed.get("data", [])
    if not isinstance(rows, list):
        raise ClickHouseError(
            f'секция "data" ответа ClickHouse — не массив, а {type(rows).__name__}'
        )
    return rows


def _error_body(exc) -> str:
    """Тело HTTP-ошибки в виде текста, если оно вообще есть.

    `urllib.error.HTTPError` — и файлоподобный объект (`read()`), и исключение
    с атрибутом `fp`. Реальный ответ приходит байтами, но подменённый в тесте
    транспорт может положить готовую строку. Оба варианта разбираются, чтобы
    диагностика не терялась из-за формы носителя, а ЛЮБОЙ другой сбой чтения
    тела не заменял исходную ошибку новой.
    """
    fp = getattr(exc, "fp", None)
    try:
        raw = exc.read() if fp is not None else ""
    except Exception:  # noqa: BLE001 — тело необязательно, исходная ошибка важнее
        return ""
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return str(raw)


def ch_query(sql: str) -> list:
    """Выполняет SQL через HTTP-интерфейс ClickHouse и возвращает строки.

    POST на `http://{host}:{http_port}/` с basic auth. Берётся именно
    HTTP-порт из `ch_config()["http_port"]` (по умолчанию 8123), а НЕ
    `port` (нативный протокол, 9000): `urllib.request` говорит по HTTP, и на
    нативном порту соединение повисло бы.

    Basic auth — требование HTTP-интерфейса; пароль уходит в заголовке
    `Authorization`, а не в URL, поэтому он не попадает в текст ошибок
    `urllib` (те печатают URL, но не заголовки). Дополнительно КАЖДЫЙ текст
    ошибки проходит через `mask()` — на случай, когда сервер отражает запрос
    или заголовки в теле ответа.

    Любой не-200 статус и ЛЮБОЙ сбой транспорта поднимают `ClickHouseError`.
    Обработчиков ровно два, и порядок обязателен:

    1. `HTTPError` — ПЕРВЫМ, потому что только у него есть читаемое тело с
       кодом и текстом ошибки; широкая ветка ниже его тела не увидит;
    2. `OSError` — вторым, и им покрывается весь остальной транспорт.
       `URLError` (обёртка `urllib` вокруг сбоя соединения) — уже подкласс
       `OSError`, так что отдельная ветка для него избыточна. Важнее другое:
       НЕ всякий сбой транспорта приходит обёрнутым в `URLError`.
       `http.client.RemoteDisconnected` наследуется через
       `ConnectionResetError → ConnectionError → OSError`, а `BadStatusLine` —
       через `HTTPException`, и обработчики `HTTPError`/`URLError` пропускали
       бы их сырыми.
    """
    config = ch_config()
    password = config["password"]
    # ВАЖНО: HTTP-порт (`http_port`, 8123), а не нативный (`port`, 9000).
    # Этот инвариант пинуется тестом, который перехватывает URL запроса
    # (`test_query_targets_http_port_not_native_port`): подмена на `port`
    # обязана ломать набор тестов.
    url = f"http://{config['host']}:{config['http_port']}/"
    request = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    token = base64.b64encode(
        f"{config['user']}:{password}".encode("utf-8")
    ).decode("ascii")
    request.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(request, timeout=CH_QUERY_TIMEOUT_SECONDS) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        # Тело ответа — главный источник диагностики (ClickHouse кладёт туда
        # код и текст ошибки), но именно оно может содержать пароль: сначала
        # маскируем, потом кладём в сообщение. Ветка стоит ПЕРВОЙ: широкий
        # `OSError` ниже — её подкласс, и тела у него уже нет.
        detail = _error_body(exc).strip() or str(exc.reason or "")
        raise ClickHouseError(
            "ClickHouse ответил статусом "
            + mask(password, f"{exc.code} на запрос к {url}: {detail}")
        ) from exc
    except OSError as exc:
        # Весь остальной транспорт: URLError (обёртка urllib), обрывы
        # соединения (ConnectionError/RemoteDisconnected), таймауты
        # (TimeoutError), ошибки DNS и «сброс пира» — все они OSError.
        # Сообщение всё равно проходит через mask(): пароля в этих путях нет
        # (проверено на живом сервере — он в заголовке, не в URL и не в
        # reason), но гарантия должна пережить будущие правки.
        reason = getattr(exc, "reason", None) or str(exc)
        raise ClickHouseError(
            mask(password, f"ClickHouse недоступен по {url}: {reason}")
        ) from exc
    return parse_ch_json(mask(password, body))


def ch_exec(sql: str) -> None:
    """Выполняет SQL через HTTP-интерфейс ClickHouse, не разбирая ответ.

    Пишущий двойник `ch_query`: тот же POST на `http_port` (8123, а не
    нативный 9000), тот же basic auth и тот же таймаут, но тело УСПЕШНОГО
    ответа игнорируется. Это не небрежность: на INSERT ClickHouse отвечает
    пустой строкой или служебным `Ok.`, и разбор ответа как JSON падал бы на
    каждой успешной вставке.

    Контракт ошибок совпадает с `ch_query`: не-200 и любой сбой транспорта
    поднимают `ClickHouseError`, а каждый текст проходит через `mask()` —
    сервер может отразить запрос или заголовки в теле ошибки. Транспортных
    веток две: `OSError` (`URLError`, `RemoteDisconnected`, таймауты) и
    `http.client.HTTPException` (`IncompleteRead`, `BadStatusLine`) — вторые
    НЕ являются `OSError` и без отдельной ветки уносили бы `finish`
    `ml_experiment` без локального паспорта.
    """
    config = ch_config()
    password = config["password"]
    url = f"http://{config['host']}:{config['http_port']}/"
    request = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    token = base64.b64encode(
        f"{config['user']}:{password}".encode("utf-8")
    ).decode("ascii")
    request.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(request, timeout=CH_QUERY_TIMEOUT_SECONDS) as response:
            # Тело читается и выбрасывается: оно не нужно, но чтение
            # завершает запрос так же, как в `ch_query`.
            response.read()
    except urllib.error.HTTPError as exc:
        detail = _error_body(exc).strip() or str(exc.reason or "")
        raise ClickHouseError(
            "ClickHouse ответил статусом "
            + mask(password, f"{exc.code} на запрос к {url}: {detail}")
        ) from exc
    except (OSError, http.client.HTTPException) as exc:
        # `IncompleteRead`/`BadStatusLine` — HTTPException, а НЕ OSError:
        # без этой ветки они уходили бы из `finish` сырыми, и локальный
        # паспорт прогона не писался бы.
        reason = getattr(exc, "reason", None) or str(exc)
        raise ClickHouseError(
            mask(password, f"ClickHouse недоступен по {url}: {reason}")
        ) from exc
