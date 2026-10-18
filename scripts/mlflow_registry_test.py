#!/usr/bin/env python3
"""Тесты scripts/mlflow_registry.py.

Запуск из корня репозитория (pytest не нужен):

    python3 scripts/mlflow_registry_test.py -v

Набор ОФЛАЙН: настоящий пакет `mlflow` не нужен. Чистые функции получают
фейковый клиент, который записывает вызовы; `main` тестируется через
`patch.dict(sys.modules, ...)` с двойником `mlflow.MlflowClient`.
"""

import io
import os
import sys
import types
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

# Модуль и тест лежат рядом, запуск прямой (`python3 scripts/...`), поэтому его
# каталог добавляется в sys.path до импорта.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import mlflow_registry as mr

# 32 hex-символа — ровно то, что MLflow кладёт в `run.info.run_id`.
RUN_ID = "0123456789abcdef0123456789abcdef"
OTHER_RUN_ID = "fedcba9876543210fedcba9876543210"
DEFAULT_TRACKING_URI = "http://127.0.0.1:5000"


class FakeMlflowException(Exception):
    """Двойник `mlflow.exceptions.MlflowException` с `error_code`."""

    def __init__(self, message, error_code=None):
        super().__init__(message)
        self.error_code = error_code


class FakeVersion:
    """Двойник `ModelVersion`; `aliases` появляется только в MLflow 3.x."""

    def __init__(self, version, status="READY", run_id="", aliases=None):
        self.version = str(version)
        self.status = status
        self.run_id = run_id
        if aliases is not None:
            self.aliases = list(aliases)


class FakeRegisteredModel:
    def __init__(self, name):
        self.name = name


class FakeClient:
    """Фейковый `MlflowClient`: хранит модели/версии/алиасы и пишет вызовы."""

    def __init__(self):
        self.calls = []
        self.models = {}
        self.versions = {}
        self.aliases = {}
        self.tracking_uri = None

    def _record(self, *call):
        self.calls.append(call)

    def get_registered_model(self, name):
        self._record("get_registered_model", name)
        if name not in self.models:
            raise FakeMlflowException(
                f"Registered model {name!r} does not exist",
                error_code="RESOURCE_DOES_NOT_EXIST",
            )
        return self.models[name]

    def create_registered_model(self, name):
        self._record("create_registered_model", name)
        model = FakeRegisteredModel(name)
        self.models[name] = model
        return model

    def create_model_version(self, name, source, run_id=None):
        self._record("create_model_version", name, source, run_id)
        version = FakeVersion(
            len(self.versions.get(name, [])) + 1, run_id=run_id or ""
        )
        self.versions.setdefault(name, []).append(version)
        return version

    def set_registered_model_alias(self, name, alias, version):
        self._record("set_registered_model_alias", name, alias, version)
        self.aliases[(name, alias)] = str(version)

    def get_model_version_by_alias(self, name, alias):
        self._record("get_model_version_by_alias", name, alias)
        target = self.aliases.get((name, alias))
        if target is None:
            raise FakeMlflowException(
                f"Alias {alias!r} for model {name!r} does not exist",
                error_code="RESOURCE_DOES_NOT_EXIST",
            )
        for version in self.versions.get(name, []):
            if str(version.version) == target:
                return version
        raise FakeMlflowException(
            f"Version {target!r} of model {name!r} does not exist",
            error_code="RESOURCE_DOES_NOT_EXIST",
        )

    def search_registered_models(self):
        self._record("search_registered_models")
        return list(self.models.values())

    def search_model_versions(self, filter_string):
        self._record("search_model_versions", filter_string)
        prefix = "name='"
        if not filter_string.startswith(prefix) or not filter_string.endswith("'"):
            raise AssertionError(f"unexpected filter_string: {filter_string!r}")
        name = filter_string[len(prefix):-1]
        return list(self.versions.get(name, []))

    def calls_named(self, name):
        return [call for call in self.calls if call[0] == name]


def _patch_mlflow(make_client):
    """`sys.modules`-двойник mlflow: клиент и `exceptions.MlflowException`."""
    module = types.ModuleType("mlflow")
    exceptions = types.ModuleType("mlflow.exceptions")
    exceptions.MlflowException = FakeMlflowException
    module.MlflowClient = make_client
    module.exceptions = exceptions
    return patch.dict(
        sys.modules, {"mlflow": module, "mlflow.exceptions": exceptions}
    )


def _fake_mlflow(client):
    """Двойник mlflow с клиентом, запоминающим tracking URI."""

    def make_client(tracking_uri=None):
        client.tracking_uri = tracking_uri
        return client

    return _patch_mlflow(make_client)


def _seed(client, name, versions):
    """Добавляет зарегистрированную модель и её версии."""
    client.models[name] = FakeRegisteredModel(name)
    client.versions[name] = versions


class RegisterVersionTest(unittest.TestCase):
    def test_creates_model_version_and_challenger(self):
        client = FakeClient()
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            version = mr.register_version(client, "cpi_nowcast", RUN_ID)

        self.assertEqual(version.version, "1")
        # Точный порядок: отсутствующая модель создаётся, затем версия, и
        # только после неё выставляется `challenger`.
        self.assertEqual(
            client.calls,
            [
                ("get_registered_model", "cpi_nowcast"),
                ("create_registered_model", "cpi_nowcast"),
                (
                    "create_model_version",
                    "cpi_nowcast",
                    f"runs:/{RUN_ID}/model",
                    RUN_ID,
                ),
                (
                    "set_registered_model_alias",
                    "cpi_nowcast",
                    "challenger",
                    "1",
                ),
            ],
        )
        self.assertEqual(
            client.aliases[("cpi_nowcast", "challenger")], version.version
        )
        self.assertEqual(
            stdout.getvalue().strip(),
            "registered cpi_nowcast v1; alias challenger -> v1",
        )

    def test_source_uri_is_built_from_run_id(self):
        client = FakeClient()
        with redirect_stdout(io.StringIO()):
            mr.register_version(client, "model", OTHER_RUN_ID)

        call = client.calls_named("create_model_version")[0]
        self.assertEqual(call[2], f"runs:/{OTHER_RUN_ID}/model")
        self.assertEqual(call[3], OTHER_RUN_ID)

    def test_existing_model_is_not_recreated(self):
        client = FakeClient()
        _seed(client, "cpi_nowcast", [FakeVersion(1, run_id=RUN_ID)])

        with redirect_stdout(io.StringIO()):
            version = mr.register_version(client, "cpi_nowcast", OTHER_RUN_ID)

        self.assertEqual(version.version, "2")
        self.assertEqual(
            client.calls,
            [
                ("get_registered_model", "cpi_nowcast"),
                (
                    "create_model_version",
                    "cpi_nowcast",
                    f"runs:/{OTHER_RUN_ID}/model",
                    OTHER_RUN_ID,
                ),
                (
                    "set_registered_model_alias",
                    "cpi_nowcast",
                    "challenger",
                    "2",
                ),
            ],
        )
        self.assertEqual(client.aliases[("cpi_nowcast", "challenger")], "2")

    def test_missing_model_detected_by_error_code(self):
        client = FakeClient()
        with redirect_stdout(io.StringIO()):
            mr.register_version(client, "model", RUN_ID)

        self.assertEqual(
            client.calls_named("create_registered_model"),
            [("create_registered_model", "model")],
        )

    def test_message_only_not_found_is_not_treated_as_missing(self):
        # «Not Found» от прокси/HTTP без `RESOURCE_DOES_NOT_EXIST` — не повод
        # создавать модель: ошибка должна проброситься, а не притвориться
        # отсутствующим объектом registry.
        class MessageOnlyClient(FakeClient):
            def get_registered_model(self, name):
                self._record("get_registered_model", name)
                raise FakeMlflowException(
                    f"Registered model {name!r} not found"
                )

        client = MessageOnlyClient()
        with redirect_stdout(io.StringIO()), \
                self.assertRaises(FakeMlflowException):
            mr.register_version(client, "model", RUN_ID)

        self.assertEqual(client.calls_named("create_registered_model"), [])

    def test_unexpected_lookup_error_is_not_treated_as_missing(self):
        class BrokenClient(FakeClient):
            def get_registered_model(self, name):
                raise RuntimeError("connection reset")

        client = BrokenClient()
        with redirect_stdout(io.StringIO()), self.assertRaises(RuntimeError):
            mr.register_version(client, "model", RUN_ID)

        self.assertEqual(client.calls_named("create_registered_model"), [])

    def test_unsafe_name_is_rejected_before_mlflow_call(self):
        for name in ("weird'name", "back\\slash"):
            with self.subTest(name=name):
                client = FakeClient()
                with self.assertRaises(mr.UnsafeModelNameError):
                    mr.register_version(client, name, RUN_ID)
                self.assertEqual(client.calls, [])


class PromoteVersionTest(unittest.TestCase):
    def test_sets_champion_and_keeps_challenger(self):
        client = FakeClient()
        _seed(client, "cpi_nowcast", [FakeVersion(1), FakeVersion(2)])
        client.aliases[("cpi_nowcast", "challenger")] = "2"
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            result = mr.promote_version(client, "cpi_nowcast", "2")

        self.assertIsNone(result)
        self.assertEqual(client.aliases[("cpi_nowcast", "champion")], "2")
        # `challenger` не снимается: promotion только двигает `champion`.
        self.assertEqual(client.aliases[("cpi_nowcast", "challenger")], "2")
        self.assertEqual(
            client.calls,
            [
                (
                    "set_registered_model_alias",
                    "cpi_nowcast",
                    "champion",
                    "2",
                )
            ],
        )
        self.assertEqual(stdout.getvalue().strip(), "alias champion -> v2")

    def test_custom_alias(self):
        client = FakeClient()
        _seed(client, "model", [FakeVersion(1)])
        client.aliases[("model", "challenger")] = "1"
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            mr.promote_version(client, "model", "1", alias="production")

        self.assertEqual(client.aliases[("model", "production")], "1")
        self.assertEqual(client.aliases[("model", "challenger")], "1")
        self.assertEqual(stdout.getvalue().strip(), "alias production -> v1")

    def test_does_not_check_metrics(self):
        # Промоушен в 4.2 ручной: клиент не должен дергать run/metrics API.
        client = FakeClient()
        _seed(client, "model", [FakeVersion(1)])

        with redirect_stdout(io.StringIO()):
            mr.promote_version(client, "model", "1")

        self.assertEqual(
            [call[0] for call in client.calls],
            ["set_registered_model_alias"],
        )


class ListModelsTest(unittest.TestCase):
    def test_uses_version_aliases_without_fallback(self):
        client = FakeClient()
        _seed(
            client,
            "cpi_nowcast",
            [
                FakeVersion(1, status="READY", run_id=RUN_ID,
                            aliases=["champion"]),
                FakeVersion(2, status="PENDING_REGISTRATION",
                            run_id=OTHER_RUN_ID, aliases=["challenger"]),
            ],
        )
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            rows = mr.list_models(client)

        self.assertEqual(
            rows,
            [
                {
                    "name": "cpi_nowcast",
                    "version": "1",
                    "status": "READY",
                    "aliases": ["champion"],
                    "run_id": RUN_ID,
                },
                {
                    "name": "cpi_nowcast",
                    "version": "2",
                    "status": "PENDING_REGISTRATION",
                    "aliases": ["challenger"],
                    "run_id": OTHER_RUN_ID,
                },
            ],
        )
        self.assertEqual(client.calls_named("get_model_version_by_alias"), [])
        out = stdout.getvalue()
        self.assertIn("name\tversion\tstatus\taliases\trun_id", out)
        self.assertIn(
            f"cpi_nowcast\t1\tREADY\tchampion\t{RUN_ID}", out
        )
        self.assertIn(
            f"cpi_nowcast\t2\tPENDING_REGISTRATION\tchallenger\t{OTHER_RUN_ID}",
            out,
        )

    def test_falls_back_only_when_aliases_attribute_is_absent(self):
        client = FakeClient()
        # Старая схема: у первой версии атрибута `aliases` нет вовсе.
        # MLflow 3.x у версии без алиасов отдаёт `[]` — fallback не нужен.
        _seed(
            client,
            "cpi_nowcast",
            [
                FakeVersion(1, run_id=RUN_ID),
                FakeVersion(2, run_id=OTHER_RUN_ID, aliases=[]),
            ],
        )
        client.aliases[("cpi_nowcast", "champion")] = "1"
        client.aliases[("cpi_nowcast", "challenger")] = "2"

        with redirect_stdout(io.StringIO()):
            rows = mr.list_models(client)

        self.assertEqual([row["aliases"] for row in rows], [["champion"], []])
        # Fallback — только для версии 1 (атрибут отсутствует), по одному
        # запросу на алиас; версия 2 с `aliases=[]` лишних запросов не делает.
        self.assertEqual(
            client.calls_named("get_model_version_by_alias"),
            [
                ("get_model_version_by_alias", "cpi_nowcast", "champion"),
                ("get_model_version_by_alias", "cpi_nowcast", "challenger"),
            ],
        )

    def test_empty_aliases_do_not_trigger_fallback(self):
        client = FakeClient()
        _seed(client, "model", [FakeVersion(1, run_id=RUN_ID, aliases=[])])
        # Даже если сервер умеет отдать версию по алиасу, пустой список
        # `aliases` — это ответ MLflow 3.x «алиасов нет», а не повод для N+1.
        client.aliases[("model", "champion")] = "1"

        with redirect_stdout(io.StringIO()):
            rows = mr.list_models(client)

        self.assertEqual(rows[0]["aliases"], [])
        self.assertEqual(client.calls_named("get_model_version_by_alias"), [])

    def test_missing_alias_in_fallback_is_ignored(self):
        client = FakeClient()
        _seed(client, "model", [FakeVersion(1, run_id=RUN_ID)])

        with redirect_stdout(io.StringIO()):
            rows = mr.list_models(client)

        self.assertEqual(rows[0]["aliases"], [])

    def test_unsafe_name_is_rejected_before_mlflow_call(self):
        for name in ("weird'name", "back\\slash"):
            with self.subTest(name=name):
                client = FakeClient()
                with self.assertRaises(mr.UnsafeModelNameError):
                    mr.list_models(client, name=name)
                self.assertEqual(client.calls, [])

    def test_unsafe_registered_name_is_skipped_when_listing_all(self):
        # M3: чужая модель из реестра не должна ломать листинг всех моделей:
        # её имя не подставляется в filter_string, а пропускается с warning.
        client = FakeClient()
        _seed(client, "weird'name", [FakeVersion(1, run_id=RUN_ID)])
        _seed(client, "good_model", [FakeVersion(2, run_id=OTHER_RUN_ID)])

        with redirect_stdout(io.StringIO()), \
                self.assertLogs("mlflow_registry", level="WARNING") as logs:
            rows = mr.list_models(client)

        self.assertEqual([row["name"] for row in rows], ["good_model"])
        self.assertIn("weird'name", "\n".join(logs.output))
        self.assertEqual(
            client.calls_named("search_model_versions"),
            [("search_model_versions", "name='good_model'")],
        )

    def test_filter_by_name_uses_get_registered_model(self):
        client = FakeClient()
        _seed(client, "alpha", [FakeVersion(1, run_id=RUN_ID)])
        _seed(client, "beta", [FakeVersion(7, run_id=OTHER_RUN_ID)])

        with redirect_stdout(io.StringIO()):
            rows = mr.list_models(client, name="beta")

        self.assertEqual([row["name"] for row in rows], ["beta"])
        self.assertEqual(
            client.calls_named("search_model_versions"),
            [("search_model_versions", "name='beta'")],
        )
        self.assertEqual(client.calls_named("search_registered_models"), [])
        self.assertEqual(
            client.calls_named("get_registered_model"),
            [("get_registered_model", "beta")],
        )

    def test_unknown_name_propagates_mlflow_error(self):
        client = FakeClient()
        with redirect_stdout(io.StringIO()), \
                self.assertRaises(FakeMlflowException):
            mr.list_models(client, name="absent")

    def test_empty_registry_prints_header_only(self):
        client = FakeClient()
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            rows = mr.list_models(client)

        self.assertEqual(rows, [])
        self.assertEqual(
            stdout.getvalue().strip(),
            "name\tversion\tstatus\taliases\trun_id",
        )


class CliParsingTest(unittest.TestCase):
    def _parse(self, argv):
        with patch.dict(os.environ, {"MLFLOW_TRACKING_URI": ""}):
            return mr._parse_args(argv)

    def test_register(self):
        args = self._parse(
            ["register", "--run-id", RUN_ID, "--name", "cpi_nowcast"]
        )
        self.assertEqual(args.command, "register")
        self.assertEqual(args.run_id, RUN_ID)
        self.assertEqual(args.name, "cpi_nowcast")
        self.assertEqual(args.tracking_uri, DEFAULT_TRACKING_URI)

    def test_promote_defaults_to_champion(self):
        args = self._parse(
            ["promote", "--name", "cpi_nowcast", "--version", "3"]
        )
        self.assertEqual(args.command, "promote")
        self.assertEqual(args.name, "cpi_nowcast")
        self.assertEqual(args.version, "3")
        self.assertEqual(args.alias, "champion")

    def test_promote_custom_alias(self):
        args = self._parse(
            [
                "promote", "--name", "cpi_nowcast", "--version", "3",
                "--alias", "production",
            ]
        )
        self.assertEqual(args.alias, "production")

    def test_list_name_is_optional(self):
        args = self._parse(["list"])
        self.assertEqual(args.command, "list")
        self.assertIsNone(args.name)

        args = self._parse(["list", "--name", "cpi_nowcast"])
        self.assertEqual(args.name, "cpi_nowcast")

    def test_tracking_uri_env_default(self):
        with patch.dict(
            os.environ, {"MLFLOW_TRACKING_URI": "http://env:5000"}
        ):
            args = mr._parse_args(["list"])
        self.assertEqual(args.tracking_uri, "http://env:5000")

    def test_tracking_uri_flag_overrides_env(self):
        with patch.dict(
            os.environ, {"MLFLOW_TRACKING_URI": "http://env:5000"}
        ):
            args = mr._parse_args(
                ["--tracking-uri", "http://flag:5000", "list"]
            )
        self.assertEqual(args.tracking_uri, "http://flag:5000")

    def test_register_requires_run_id_and_name(self):
        for argv in (
            ["register", "--name", "cpi_nowcast"],
            ["register", "--run-id", RUN_ID],
        ):
            stderr = io.StringIO()
            with redirect_stderr(stderr), \
                    self.assertRaises(SystemExit) as ctx:
                mr._parse_args(argv)
            self.assertEqual(ctx.exception.code, 2)
            self.assertIn("required", stderr.getvalue())

    def test_missing_subcommand_is_rejected(self):
        stderr = io.StringIO()
        with redirect_stderr(stderr), self.assertRaises(SystemExit) as ctx:
            mr._parse_args([])
        self.assertEqual(ctx.exception.code, 2)


class MainTest(unittest.TestCase):
    def _run(self, client, argv):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with _fake_mlflow(client), redirect_stdout(stdout), \
                redirect_stderr(stderr):
            code = mr.main(argv)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_register_command(self):
        client = FakeClient()
        code, out, err = self._run(
            client,
            [
                "--tracking-uri", "http://flag:5000",
                "register", "--run-id", RUN_ID, "--name", "cpi_nowcast",
            ],
        )

        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        self.assertEqual(client.tracking_uri, "http://flag:5000")
        self.assertEqual(client.aliases[("cpi_nowcast", "challenger")], "1")
        self.assertEqual(
            out.strip(),
            "registered cpi_nowcast v1; alias challenger -> v1",
        )

    def test_promote_command(self):
        client = FakeClient()
        _seed(client, "cpi_nowcast", [FakeVersion(1), FakeVersion(2)])
        client.aliases[("cpi_nowcast", "challenger")] = "2"

        code, out, err = self._run(
            client,
            ["promote", "--name", "cpi_nowcast", "--version", "2"],
        )

        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        self.assertEqual(client.aliases[("cpi_nowcast", "champion")], "2")
        self.assertEqual(client.aliases[("cpi_nowcast", "challenger")], "2")
        self.assertEqual(out.strip(), "alias champion -> v2")

    def test_list_command(self):
        client = FakeClient()
        _seed(
            client,
            "cpi_nowcast",
            [FakeVersion(1, run_id=RUN_ID, aliases=["challenger"])],
        )

        code, out, err = self._run(client, ["list"])

        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        self.assertIn("name\tversion\tstatus\taliases\trun_id", out)
        self.assertIn(f"cpi_nowcast\t1\tREADY\tchallenger\t{RUN_ID}", out)

    def test_default_tracking_uri_from_env(self):
        client = FakeClient()
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch.dict(
            os.environ, {"MLFLOW_TRACKING_URI": "http://env:5000"}
        ), _fake_mlflow(client), redirect_stdout(stdout), \
                redirect_stderr(stderr):
            code = mr.main(["list"])

        self.assertEqual(code, 0)
        self.assertEqual(client.tracking_uri, "http://env:5000")

    def test_unsafe_model_names_exit_one_without_mlflow_calls(self):
        for name in ("weird'name", "back\\slash"):
            for argv in (
                ["register", "--run-id", RUN_ID, "--name", name],
                ["list", "--name", name],
            ):
                with self.subTest(name=name, argv=argv):
                    client = FakeClient()
                    code, out, err = self._run(client, argv)
                    self.assertEqual(code, 1)
                    self.assertEqual(out, "")
                    self.assertIn("invalid model name", err)
                    self.assertIn("[A-Za-z0-9._-]", err)
                    self.assertEqual(client.calls, [])

    def test_mlflow_error_exits_one_with_concise_message(self):
        client = FakeClient()

        code, out, err = self._run(client, ["list", "--name", "absent"])

        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("does not exist", err)
        self.assertNotIn("Traceback", err)

    def test_register_error_exits_one_with_concise_message(self):
        class BrokenVersionClient(FakeClient):
            def create_model_version(self, name, source, run_id=None):
                self._record("create_model_version", name, source, run_id)
                raise FakeMlflowException("cannot register version")

        client = BrokenVersionClient()

        code, out, err = self._run(
            client,
            ["register", "--run-id", RUN_ID, "--name", "cpi_nowcast"],
        )

        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("cannot register version", err)
        self.assertNotIn("Traceback", err)
        self.assertEqual(client.calls_named("set_registered_model_alias"), [])

    def test_os_error_exits_one(self):
        class OfflineClient(FakeClient):
            def search_registered_models(self):
                raise ConnectionError("connection refused")

        code, out, err = self._run(OfflineClient(), ["list"])

        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("connection refused", err)

    def test_programming_error_is_not_swallowed(self):
        class BuggyClient(FakeClient):
            def search_registered_models(self):
                raise TypeError("bug")

        client = BuggyClient()
        with _fake_mlflow(client), redirect_stdout(io.StringIO()), \
                redirect_stderr(io.StringIO()), self.assertRaises(TypeError):
            mr.main(["list"])

    def test_client_creation_error_exits_one(self):
        def broken_factory(tracking_uri=None):
            raise ConnectionError("cannot connect")

        stdout = io.StringIO()
        stderr = io.StringIO()
        with _patch_mlflow(broken_factory), redirect_stdout(stdout), \
                redirect_stderr(stderr):
            code = mr.main(["list"])

        self.assertEqual(code, 1)
        self.assertIn("cannot connect", stderr.getvalue())

    def test_missing_mlflow_package_exits_one(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch.dict(sys.modules, {"mlflow": None}), \
                redirect_stdout(stdout), redirect_stderr(stderr):
            code = mr.main(["list"])

        self.assertEqual(code, 1)
        self.assertIn("mlflow", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
