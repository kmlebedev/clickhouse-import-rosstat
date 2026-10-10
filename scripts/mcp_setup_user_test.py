"""Проверка аутентификации mcp_setup_user.py: URL из CH_SETUP_URL и Basic-auth заголовок.

Запуск: python3 scripts/mcp_setup_user_test.py
"""
import base64
import importlib.util
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = importlib.util.spec_from_file_location("mcp_setup_user", os.path.join(HERE, "mcp_setup_user.py"))


class Env:
    """Временно подменяет окружение процесса.

    Функции модуля читают os.environ в момент вызова, поэтому окружение
    подменяется на время вызова, а не только на время загрузки модуля.
    """

    def __init__(self, env):
        self.env = env
        self.old = None

    def __enter__(self):
        self.old = dict(os.environ)
        os.environ.clear()
        os.environ.update(self.env)
        return self

    def __exit__(self, *exc):
        os.environ.clear()
        os.environ.update(self.old)
        return False


def load_module():
    module = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(module)
    return module


MODULE = load_module()


class AuthHeadersTest(unittest.TestCase):
    def test_no_user_returns_empty_headers(self):
        self.assertEqual(MODULE.auth_headers("", "secret"), {})

    def test_user_returns_basic_auth_header(self):
        headers = MODULE.auth_headers("default", "pw")
        expected = base64.b64encode(b"default:pw").decode("ascii")
        self.assertEqual(headers, {"Authorization": f"Basic {expected}"})

    def test_password_absent_when_user_empty(self):
        self.assertNotIn("Authorization", MODULE.auth_headers("", "topsecret"))


class SetupURLTest(unittest.TestCase):
    def test_default_url_uses_localhost_and_port(self):
        with Env({"CH_HTTP_PORT": "9999"}):
            self.assertEqual(MODULE.setup_url(), "http://localhost:9999/")

    def test_explicit_url_wins(self):
        with Env({"CH_HTTP_PORT": "9999", "CH_SETUP_URL": "http://clickhouse.internal:8123/"}):
            self.assertEqual(MODULE.setup_url(), "http://clickhouse.internal:8123/")

    def test_default_port_when_unset(self):
        with Env({}):
            self.assertEqual(MODULE.setup_url(), "http://localhost:8123/")


class SQLPathTest(unittest.TestCase):
    def test_sql_path_default_is_repo_relative(self):
        with Env({}):
            self.assertEqual(MODULE.sql_path(), "sql/mcp_kimi_reader.sql")

    def test_sql_path_env_override_wins(self):
        with Env({"CH_SETUP_SQL": "/tmp/mcp_kimi_reader.sql"}):
            self.assertEqual(MODULE.sql_path(), "/tmp/mcp_kimi_reader.sql")


class MaskTest(unittest.TestCase):
    def test_masks_password_verbatim(self):
        self.assertEqual(MODULE.mask("hunter2", "token hunter2 here"), "token *** here")

    def test_does_not_corrupt_words_containing_short_password(self):
        # Классический дефект: слепая замена 'x' ломает слово Exception.
        self.assertEqual(MODULE.mask("x", "DB::Exception: bad"), "DB::Exception: bad")

    def test_empty_password_is_noop(self):
        self.assertEqual(MODULE.mask("", "DB::Exception: bad"), "DB::Exception: bad")


if __name__ == "__main__":
    unittest.main(verbosity=2)
