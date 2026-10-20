#!/usr/bin/env python3
"""Тесты общего транспорта ClickHouse `scripts/lib/clickhouse.py`.

Запуск из корня репозитория (pytest не нужен):
    python3 scripts/lib/clickhouse_test.py -v

Все тесты офлайновые: сеть и живой ClickHouse не нужны.
"""

import http.client
import io
import os
import sys
import unittest
import unittest.mock
import urllib.error
from pathlib import Path

# Тест лежит в `scripts/lib/`, запуск прямой, поэтому каталог `scripts`
# добавляется в sys.path до импорта пакета.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib import clickhouse as ex


# Переменные, которые `ch_config()` читает. Тесты, трогающие конфигурацию,
# обязаны изолировать ВСЕ их: иначе значение утекает в соседний тест (первая
# версия `test_ch_config_reads_env` оставляла за собой `CLICKHOUSE_PASSWORD`, и
# следующий тест смотрел бы на чужой пароль).
_CONFIG_ENV_VARS = (
    "CLICKHOUSE_URL", "CLICKHOUSE_HOST", "CLICKHOUSE_PORT", "CLICKHOUSE_USER",
    "CLICKHOUSE_DATABASE", "CLICKHOUSE_PASSWORD", "CH_IMPORT_PASSWORD",
    "CH_HTTP_PORT",
)


class _isolated_env:
    """Контекст: выставляет указанные переменные и возвращает прежние.

    Отсутствие переменной восстанавливается как отсутствие (`os.environ.pop`),
    а не как пустая строка: `ch_config()` различает «не задано» и «задано
    пустым», и тест не должен стирать это различие.
    """

    def __init__(self, **values):
        self._values = values
        self._saved: dict = {}

    def __enter__(self):
        for name in _CONFIG_ENV_VARS:
            self._saved[name] = os.environ.get(name)
            os.environ.pop(name, None)
        for name, value in self._values.items():
            os.environ[name] = value
        return self

    def __exit__(self, *_exc):
        for name, previous in self._saved.items():
            if previous is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous
        return False


class TestConfig(unittest.TestCase):
    def test_ch_config_reads_env(self):
        with _isolated_env(CLICKHOUSE_HOST="example.invalid",
                           CLICKHOUSE_USER="u", CLICKHOUSE_PASSWORD="secret"):
            self.assertEqual(ex.ch_config()["host"], "example.invalid")

    # --- CLICKHOUSE_URL: основной способ задать подключение в проекте --------
    #
    # Env-файл `~/.config/rosstat/env` задаёт именно DSN (`CLICKHOUSE_URL`), а
    # дискретных `CLICKHOUSE_HOST`/`CLICKHOUSE_USER` не содержит вовсе. Пока
    # скрипт читал только их, он подключался пустым пользователем и падал на
    # 401 при `make dcf-aisc` — эти тесты держат DSN-путь рабочим.

    def test_parses_dsn_without_credentials(self):
        got = ex.parse_clickhouse_url("clickhouse://localhost:9000/default")
        self.assertEqual(got["host"], "localhost")
        self.assertEqual(got["port"], "9000")
        self.assertEqual(got["database"], "default")
        self.assertNotIn("user", got)
        self.assertNotIn("password", got)

    def test_parses_dsn_with_credentials_and_unquotes(self):
        got = ex.parse_clickhouse_url("clickhouse://u%40corp:p%40ss@h:8123/db")
        self.assertEqual(got["user"], "u@corp")
        self.assertEqual(got["password"], "p@ss")

    def test_dsn_rejects_missing_scheme(self):
        with self.assertRaises(ValueError):
            ex.parse_clickhouse_url("localhost:9000")

    def test_config_takes_user_and_host_from_url(self):
        with _isolated_env(CLICKHOUSE_URL="clickhouse://someuser@some.host:9000/mydb"):
            cfg = ex.ch_config()
        self.assertEqual(cfg["host"], "some.host")
        self.assertEqual(cfg["user"], "someuser")
        self.assertEqual(cfg["database"], "mydb")

    def test_config_never_borrows_kimi_reader_password(self):
        """`CLICKHOUSE_PASSWORD` — пароль kimi_reader, а не того, кто читает
        сырую таблицу. Подставлять его нельзя: у пользователя из DSN пароля
        может не быть вовсе, и тогда чужой пароль даёт 401 (реальный сбой
        2026-10-11 при `make dcf-aisc`)."""
        with _isolated_env(CLICKHOUSE_URL="clickhouse://localhost:9000/default",
                           CLICKHOUSE_PASSWORD="kimi-reader-secret"):
            cfg = ex.ch_config()
        self.assertEqual(cfg["password"], "")

    def test_config_uses_dedicated_import_password(self):
        with _isolated_env(CLICKHOUSE_URL="clickhouse://localhost:9000/default",
                           CLICKHOUSE_PASSWORD="kimi-reader-secret",
                           CH_IMPORT_PASSWORD="importer-secret"):
            self.assertEqual(ex.ch_config()["password"], "importer-secret")

    def test_config_explicit_env_password_beats_url_password(self):
        """Явный `CH_IMPORT_PASSWORD` — намеренное переопределение, поэтому он
        сильнее пароля из DSN: так можно уйти с указанного в URL пользователя
        на другого, не переписывая DSN."""
        with _isolated_env(CLICKHOUSE_URL="clickhouse://u:pw@h:9000/db",
                           CH_IMPORT_PASSWORD="other"):
            self.assertEqual(ex.ch_config()["password"], "other")

    def test_config_password_from_url_used_when_no_override(self):
        with _isolated_env(CLICKHOUSE_URL="clickhouse://u:pw@h:9000/db"):
            self.assertEqual(ex.ch_config()["password"], "pw")

    def test_discrete_vars_override_url(self):
        with _isolated_env(CLICKHOUSE_URL="clickhouse://from-url:9000/urldb",
                           CLICKHOUSE_HOST="from-env",
                           CLICKHOUSE_DATABASE="envdb"):
            cfg = ex.ch_config()
        self.assertEqual(cfg["host"], "from-env")
        self.assertEqual(cfg["database"], "envdb")

    def test_mask_hides_secret(self):
        self.assertEqual(ex.mask("secret", "pw=secret"), "pw=***")


class _FakeResponse:
    """Минимальный ответ HTTP: контекстный менеджер и `read()`.

    Настоящий `urlopen` возвращает объект с обоими методами, а `ch_query`
    пользуется им как `with ... as response: response.read()`. Заглушка
    повторяет ровно этот контракт и ничего больше.
    """

    def __init__(self, body: str):
        self._body = body.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def read(self):
        return self._body


class TestQueryTransport(unittest.TestCase):
    """Инварианты HTTP-вызова: порт, обёртка сбоев, маскирование.

    Все тесты офлайновые: `urlopen` подменяется, поэтому ни сети, ни
    запущенного ClickHouse не требуется.
    """

    def _capture_request(self, body='{"data":[]}'):
        """Прогоняет `ch_query` и возвращает перехваченный `Request`."""
        captured = {}

        def fake_urlopen(request, timeout=None):
            captured["request"] = request
            captured["timeout"] = timeout
            return _FakeResponse(body)

        with unittest.mock.patch(
            "lib.clickhouse.urllib.request.urlopen", fake_urlopen
        ):
            ex.ch_query("SELECT 1")
        return captured["request"]

    def test_query_targets_http_port_not_native_port(self):
        # ГЛАВНЫЙ инвариант задачи: HTTP-интерфейс живёт на `http_port` (8123),
        # а `port` (9000) — нативный протокол. Запрос обязан идти на 8123:
        # на 9000 `urllib.request` либо повис, либо упал бы, поэтому подмена
        # `http_port` на `port` НЕ должна проходить незамеченной.
        # Порт из окружения не подставляется: проверяется именно default.
        self.addCleanup(os.environ.pop, "CH_HTTP_PORT", None)
        os.environ.pop("CH_HTTP_PORT", None)

        request = self._capture_request()

        self.assertIn(":8123/", request.full_url)
        self.assertNotIn(":9000/", request.full_url)
        self.assertEqual(ex.ch_config()["http_port"], "8123")
        self.assertNotEqual(ex.ch_config()["port"], "8123")

    def test_query_consults_ch_http_port_env(self):
        # Порт НЕ захардкожен: значение из `CH_HTTP_PORT` обязано попадать в
        # URL. Отдельное «непохожее» значение (18999) отличает чтение конфига
        # от случайного совпадения с 8123.
        self.addCleanup(os.environ.pop, "CH_HTTP_PORT", None)
        os.environ["CH_HTTP_PORT"] = "18999"

        request = self._capture_request()

        self.assertIn(":18999/", request.full_url)
        self.assertNotIn(":8123/", request.full_url)
        self.assertNotIn(":9000/", request.full_url)

    def test_query_uses_host_from_config(self):
        self.addCleanup(os.environ.pop, "CLICKHOUSE_HOST", None)
        os.environ["CLICKHOUSE_HOST"] = "ch.example.invalid"

        request = self._capture_request()

        self.assertIn("ch.example.invalid", request.full_url)

    def test_successful_response_returns_parsed_rows(self):
        # Успешный путь не сломан широкой веткой `OSError`: строки доходят.
        request = self._capture_request('{"data":[{"v":1112.8}]}')
        self.assertEqual(request.method, "POST")

        def fake_urlopen(req, timeout=None):
            return _FakeResponse('{"data":[{"v":1112.8}]}')

        with unittest.mock.patch(
            "lib.clickhouse.urllib.request.urlopen", fake_urlopen
        ):
            self.assertEqual(ex.ch_query("SELECT 1"), [{"v": 1112.8}])

    def test_remote_disconnect_raises_clickhouse_error(self):
        # `http.client.RemoteDisconnected` НЕ является `URLError` (его MRO идёт
        # через ConnectionResetError → ConnectionError → OSError), поэтому
        # обработчика `URLError` ему мало: он обязан быть пойман широкой
        # веткой `OSError` и обёрнут в ClickHouseError, а не вылететь сырым.
        self.assertTrue(issubclass(http.client.RemoteDisconnected, OSError))
        self.assertFalse(issubclass(http.client.RemoteDisconnected, urllib.error.URLError))

        def fake_urlopen(request, timeout=None):
            raise http.client.RemoteDisconnected("Remote end closed connection")

        with unittest.mock.patch(
            "lib.clickhouse.urllib.request.urlopen", fake_urlopen
        ):
            with self.assertRaises(ex.ClickHouseError) as ctx:
                ex.ch_query("SELECT 1")
        self.assertIn("ClickHouse недоступен", str(ctx.exception))

    def test_bare_oserror_raises_clickhouse_error(self):
        # Любой другой транспортный сбой (таймаут, DNS, сброс) — тоже
        # ClickHouseError, а не сырое исключение.
        for boom in (TimeoutError("timed out"), ConnectionResetError("reset"),
                     OSError("boom")):
            with self.subTest(boom=type(boom).__name__):
                def fake_urlopen(request, timeout=None, _boom=boom):
                    raise _boom

                with unittest.mock.patch(
                    "lib.clickhouse.urllib.request.urlopen", fake_urlopen
                ):
                    with self.assertRaises(ex.ClickHouseError):
                        ex.ch_query("SELECT 1")

    def test_url_error_reason_is_preserved_and_masked(self):
        # `URLError` — подкласс `OSError`, поэтому его ловит широкая ветка;
        # диагностика (`reason`) при этом не теряется, а пароль маскируется.
        secret = "url-error-secret"
        self.addCleanup(os.environ.pop, "CH_IMPORT_PASSWORD", None)
        os.environ["CH_IMPORT_PASSWORD"] = secret

        def fake_urlopen(request, timeout=None):
            raise urllib.error.URLError(f"connection refused for {secret}")

        with unittest.mock.patch(
            "lib.clickhouse.urllib.request.urlopen", fake_urlopen
        ):
            with self.assertRaises(ex.ClickHouseError) as ctx:
                ex.ch_query("SELECT 1")
        message = str(ctx.exception)
        self.assertIn("connection refused", message)
        self.assertNotIn(secret, message)
        self.assertIn("***", message)


class TestExecTransport(unittest.TestCase):
    """Инварианты пишущего транспорта `ch_exec`.

    `ch_exec` — двойник `ch_query` без разбора ответа: успешное тело INSERT
    (пустая строка или `Ok.`) не является JSON и не должно парситься.
    Все тесты офлайновые, `urlopen` подменяется.
    """

    def _capture_request(self, body="Ok.\n"):
        captured = {}

        def fake_urlopen(request, timeout=None):
            captured["request"] = request
            captured["timeout"] = timeout
            return _FakeResponse(body)

        with _isolated_env():
            with unittest.mock.patch(
                "lib.clickhouse.urllib.request.urlopen", fake_urlopen
            ):
                ex.ch_exec("INSERT INTO ml_runs FORMAT JSONEachRow\n{}")
        return captured

    def test_posts_sql_to_http_port_and_ignores_body(self):
        # Не-JSON тело успешного ответа НЕ ошибка: INSERT отвечает `Ok.`.
        captured = self._capture_request("Ok.\n")
        request = captured["request"]
        self.assertEqual(request.method, "POST")
        self.assertEqual(
            request.data, b"INSERT INTO ml_runs FORMAT JSONEachRow\n{}"
        )
        self.assertEqual(captured["timeout"], ex.CH_QUERY_TIMEOUT_SECONDS)
        self.assertIn(":8123/", request.full_url)
        self.assertNotIn(":9000/", request.full_url)

    def test_http_error_is_clickhouse_error_and_masks_password(self):
        secret = "exec-http-secret"

        def fake_urlopen(request, timeout=None):
            raise urllib.error.HTTPError(
                "http://localhost:8123/", 500, "Internal Server Error", {},
                io.BytesIO(f"Code: 62. DB::Exception: {secret}".encode("utf-8")),
            )

        with _isolated_env(CH_IMPORT_PASSWORD=secret):
            with unittest.mock.patch(
                "lib.clickhouse.urllib.request.urlopen", fake_urlopen
            ):
                with self.assertRaises(ex.ClickHouseError) as ctx:
                    ex.ch_exec("INSERT INTO ml_runs FORMAT JSONEachRow\n{}")

        message = str(ctx.exception)
        self.assertIn("500", message)
        self.assertIn("DB::Exception", message)
        self.assertNotIn(secret, message)
        self.assertIn("***", message)

    def test_os_error_is_clickhouse_error_and_masks_password(self):
        secret = "exec-os-secret"
        for boom in (TimeoutError("timed out"), ConnectionResetError("reset"),
                     urllib.error.URLError(f"refused for {secret}")):
            with self.subTest(boom=type(boom).__name__):
                def fake_urlopen(request, timeout=None, _boom=boom):
                    raise _boom

                with _isolated_env(CH_IMPORT_PASSWORD=secret):
                    with unittest.mock.patch(
                        "lib.clickhouse.urllib.request.urlopen", fake_urlopen
                    ):
                        with self.assertRaises(ex.ClickHouseError) as ctx:
                            ex.ch_exec("INSERT INTO ml_runs FORMAT JSONEachRow\n{}")
                self.assertNotIn(secret, str(ctx.exception))
                self.assertIn("ClickHouse недоступен", str(ctx.exception))

    def test_http_exception_is_clickhouse_error(self):
        # `IncompleteRead`/`BadStatusLine` наследуются от `HTTPException`, а
        # НЕ от `OSError`: без отдельной ветки они уходили бы из `ch_exec`
        # сырыми и уносили бы локальный паспорт прогона.
        def fake_urlopen(request, timeout=None):
            raise http.client.IncompleteRead(b"partial")

        with _isolated_env():
            with unittest.mock.patch(
                "lib.clickhouse.urllib.request.urlopen", fake_urlopen
            ):
                with self.assertRaises(ex.ClickHouseError) as ctx:
                    ex.ch_exec("INSERT INTO ml_runs FORMAT JSONEachRow\n{}")
        self.assertIn("ClickHouse недоступен", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
