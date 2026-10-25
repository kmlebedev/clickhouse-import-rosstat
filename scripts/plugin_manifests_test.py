#!/usr/bin/env python3
"""Офлайн-тесты scripts/plugin_manifests.py.

Запуск из корня репозитория::

    python3 scripts/plugin_manifests_test.py -v

Тесты используют только stdlib и временные каталоги; сеть не нужна.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import plugin_manifests as pm


LONG = "Repo-specific skills for the data platform."
SHORT = "Repo role skills for the data platform."
NAME = "clickhouse-import-rosstat"


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def base_manifests() -> dict[str, dict]:
    return {
        pm.CLAUDE_PLUGIN: {
            "name": NAME,
            "version": "0.1.0",
            "description": LONG,
        },
        pm.CLAUDE_MARKETPLACE: {
            "name": NAME,
            "version": "0.1.0",
            "description": SHORT,
            "plugins": [
                {
                    "name": NAME,
                    "version": "0.1.0",
                    "description": LONG,
                }
            ],
        },
        pm.CURSOR_PLUGIN: {
            "name": NAME,
            "version": "0.1.0",
            "description": LONG,
        },
        pm.CURSOR_MARKETPLACE: {
            "name": NAME,
            "metadata": {
                "version": "0.1.0",
                "description": SHORT,
            },
            "plugins": [{"name": NAME, "description": LONG}],
        },
        pm.CODEX_PLUGIN: {
            "name": NAME,
            "version": "0.1.0",
            "description": LONG,
            "interface": {
                "shortDescription": SHORT,
                "longDescription": LONG,
            },
        },
        pm.CODEX_MARKETPLACE: {
            "name": NAME,
            "plugins": [{"name": NAME}],
        },
        pm.QWEN: {
            "name": NAME,
            "version": "0.1.0",
            "description": LONG,
        },
        pm.KIMI: {
            "name": NAME,
            "version": "0.1.0",
            "description": LONG,
            "interface": {
                "shortDescription": SHORT,
                "longDescription": LONG,
            },
        },
    }


class PluginManifestsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        for rel, data in base_manifests().items():
            write_json(self.root / rel, data)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def load(self, rel: str) -> dict:
        return json.loads((self.root / rel).read_text(encoding="utf-8"))

    def save(self, rel: str, data: dict) -> None:
        write_json(self.root / rel, data)

    def test_check_accepts_synced_manifests(self) -> None:
        drift, changes = pm.check(self.root)
        self.assertEqual([], drift)
        self.assertEqual({}, changes)

    def test_check_detects_drift_and_write_fixes_it(self) -> None:
        kimi = self.load(pm.KIMI)
        kimi["version"] = "0.0.1"
        kimi["interface"]["shortDescription"] = "wrong"
        self.save(pm.KIMI, kimi)

        drift, changes = pm.check(self.root)
        self.assertTrue(any("kimi.plugin.json" in item and "version" in item for item in drift))
        self.assertTrue(any("shortDescription" in item for item in drift))
        self.assertIn(pm.KIMI, changes)

        pm.write(self.root)
        drift, changes = pm.check(self.root)
        self.assertEqual([], drift)
        self.assertEqual({}, changes)

    def test_write_syncs_all_clients(self) -> None:
        cursor_plugin = self.load(pm.CURSOR_PLUGIN)
        cursor_plugin.update({"name": "wrong", "version": "0.0.1", "description": "wrong"})
        self.save(pm.CURSOR_PLUGIN, cursor_plugin)

        cursor_marketplace = self.load(pm.CURSOR_MARKETPLACE)
        cursor_marketplace["name"] = "wrong"
        cursor_marketplace["metadata"] = {"version": "0.0.1", "description": "wrong"}
        cursor_marketplace["plugins"][0] = {"name": "wrong", "description": "wrong"}
        self.save(pm.CURSOR_MARKETPLACE, cursor_marketplace)

        codex = self.load(pm.CODEX_PLUGIN)
        codex.update({"name": "wrong", "version": "0.0.1", "description": "wrong"})
        codex["interface"] = {"shortDescription": "wrong", "longDescription": "wrong"}
        self.save(pm.CODEX_PLUGIN, codex)

        codex_marketplace = self.load(pm.CODEX_MARKETPLACE)
        codex_marketplace["name"] = "wrong"
        codex_marketplace["plugins"][0]["name"] = "wrong"
        self.save(pm.CODEX_MARKETPLACE, codex_marketplace)

        qwen = self.load(pm.QWEN)
        qwen.update({"name": "wrong", "version": "0.0.1", "description": "wrong"})
        self.save(pm.QWEN, qwen)

        drift, changes = pm.check(self.root)
        self.assertGreaterEqual(len(drift), 10)
        self.assertEqual(
            {pm.CURSOR_PLUGIN, pm.CURSOR_MARKETPLACE, pm.CODEX_PLUGIN, pm.CODEX_MARKETPLACE, pm.QWEN},
            set(changes),
        )

        pm.write(self.root)
        self.assertEqual([], pm.check(self.root)[0])

        self.assertEqual(
            {"name": NAME, "version": "0.1.0", "description": LONG},
            {key: self.load(pm.CURSOR_PLUGIN)[key] for key in ("name", "version", "description")},
        )
        self.assertEqual(NAME, self.load(pm.CURSOR_MARKETPLACE)["name"])
        self.assertEqual(
            {"version": "0.1.0", "description": SHORT},
            self.load(pm.CURSOR_MARKETPLACE)["metadata"],
        )
        self.assertEqual(NAME, self.load(pm.CODEX_MARKETPLACE)["name"])
        self.assertEqual(NAME, self.load(pm.CODEX_MARKETPLACE)["plugins"][0]["name"])
        self.assertEqual(
            {"shortDescription": SHORT, "longDescription": LONG},
            self.load(pm.CODEX_PLUGIN)["interface"],
        )
        self.assertEqual(
            {"name": NAME, "version": "0.1.0", "description": LONG},
            {key: self.load(pm.QWEN)[key] for key in ("name", "version", "description")},
        )

    def test_missing_canonical_field_fails(self) -> None:
        plugin = self.load(pm.CLAUDE_PLUGIN)
        plugin.pop("description")
        self.save(pm.CLAUDE_PLUGIN, plugin)

        with self.assertRaises(pm.ManifestError):
            pm.check(self.root)

    def test_missing_target_field_fails(self) -> None:
        kimi = self.load(pm.KIMI)
        kimi.pop("interface")
        self.save(pm.KIMI, kimi)

        with self.assertRaises(pm.ManifestError):
            pm.check(self.root)

    def test_write_is_idempotent(self) -> None:
        first_drift, first_changes = pm.write(self.root)
        self.assertEqual([], first_drift)
        self.assertEqual({}, first_changes)

        second_drift, second_changes = pm.write(self.root)
        self.assertEqual([], second_drift)
        self.assertEqual({}, second_changes)


if __name__ == "__main__":
    unittest.main()
