#!/usr/bin/env python3
"""Синхронизация метаданных LLM-плагина между манифестами.

Канонический источник:

- ``.claude-plugin/plugin.json``: ``name``, ``version``, ``description``
  (длинное описание);
- ``.claude-plugin/marketplace.json``: ``name`` (имя marketplace) и
  ``description`` (короткое описание).

Команды::

    python3 scripts/plugin_manifests.py --check
    python3 scripts/plugin_manifests.py --write

``--check`` возвращает 1 при дрейфе; ``--write`` приводит манифесты к канону.
Только стандартная библиотека Python 3.9+; сеть не используется.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent

CLAUDE_PLUGIN = ".claude-plugin/plugin.json"
CLAUDE_MARKETPLACE = ".claude-plugin/marketplace.json"
CURSOR_PLUGIN = ".cursor-plugin/plugin.json"
CURSOR_MARKETPLACE = ".cursor-plugin/marketplace.json"
CODEX_PLUGIN = ".codex-plugin/plugin.json"
CODEX_MARKETPLACE = ".agents/plugins/marketplace.json"
QWEN = "qwen-extension.json"
KIMI = "kimi.plugin.json"


class ManifestError(Exception):
    """Некорректный или отсутствующий манифест."""


@dataclass(frozen=True)
class Canonical:
    name: str
    version: str
    long_description: str
    marketplace_name: str
    short_description: str


def load(root: Path, rel: str) -> dict:
    path = root / rel
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ManifestError(f"{rel}: file not found") from exc
    except json.JSONDecodeError as exc:
        raise ManifestError(f"{rel}: invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ManifestError(f"{rel}: top-level JSON must be an object")
    return data


def require_string(data: dict, field: str, rel: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ManifestError(f"{rel}: {field} must be a non-empty string")
    return value


def get_path(data: object, path: str, rel: str) -> object:
    current = data
    for part in path.split("."):
        try:
            if isinstance(current, list):
                current = current[int(part)]
            else:
                current = current[part]  # type: ignore[index]
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise ManifestError(f"{rel}: missing field {path!r}") from exc
    return current


def set_path(data: object, path: str, value: object, rel: str) -> None:
    parts = path.split(".")
    current = data
    for part in parts[:-1]:
        try:
            if isinstance(current, list):
                current = current[int(part)]
            else:
                current = current[part]  # type: ignore[index]
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise ManifestError(f"{rel}: missing field {path!r}") from exc

    last = parts[-1]
    try:
        if isinstance(current, list):
            current[int(last)] = value
        else:
            current[last] = value  # type: ignore[index]
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise ManifestError(f"{rel}: missing field {path!r}") from exc


def canonical(root: Path = ROOT) -> Canonical:
    plugin = load(root, CLAUDE_PLUGIN)
    marketplace = load(root, CLAUDE_MARKETPLACE)
    return Canonical(
        name=require_string(plugin, "name", CLAUDE_PLUGIN),
        version=require_string(plugin, "version", CLAUDE_PLUGIN),
        long_description=require_string(plugin, "description", CLAUDE_PLUGIN),
        marketplace_name=require_string(marketplace, "name", CLAUDE_MARKETPLACE),
        short_description=require_string(marketplace, "description", CLAUDE_MARKETPLACE),
    )


def updates(c: Canonical) -> dict[str, list[tuple[str, str]]]:
    return {
        CLAUDE_MARKETPLACE: [
            ("version", c.version),
            ("plugins.0.name", c.name),
            ("plugins.0.version", c.version),
            ("plugins.0.description", c.long_description),
        ],
        CURSOR_PLUGIN: [
            ("name", c.name),
            ("version", c.version),
            ("description", c.long_description),
        ],
        CURSOR_MARKETPLACE: [
            ("name", c.marketplace_name),
            ("metadata.version", c.version),
            ("metadata.description", c.short_description),
            ("plugins.0.name", c.name),
            ("plugins.0.description", c.long_description),
        ],
        CODEX_PLUGIN: [
            ("name", c.name),
            ("version", c.version),
            ("description", c.long_description),
            ("interface.longDescription", c.long_description),
            ("interface.shortDescription", c.short_description),
        ],
        CODEX_MARKETPLACE: [
            ("name", c.marketplace_name),
            ("plugins.0.name", c.name),
        ],
        QWEN: [
            ("name", c.name),
            ("version", c.version),
            ("description", c.long_description),
        ],
        KIMI: [
            ("name", c.name),
            ("version", c.version),
            ("description", c.long_description),
            ("interface.longDescription", c.long_description),
            ("interface.shortDescription", c.short_description),
        ],
    }


def check(root: Path = ROOT) -> tuple[list[str], dict[str, dict]]:
    """Возвращает (drift, changes); файлы не меняет."""
    c = canonical(root)
    drift: list[str] = []
    changes: dict[str, dict] = {}

    for rel, fields in updates(c).items():
        data = load(root, rel)
        original = copy.deepcopy(data)
        for field, expected in fields:
            actual = get_path(data, field, rel)
            if actual != expected:
                drift.append(f"{rel}: {field} = {actual!r}, expected {expected!r}")
                set_path(data, field, expected, rel)
        if data != original:
            changes[rel] = data

    return drift, changes


def write(root: Path = ROOT) -> tuple[list[str], dict[str, dict]]:
    """Приводит манифесты к канону; возвращает (drift, changes)."""
    drift, changes = check(root)
    for rel, data in changes.items():
        path = root / rel
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return drift, changes


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true", help="fail if manifests drift from the canonical metadata")
    group.add_argument("--write", action="store_true", help="update manifests from the canonical metadata")
    args = parser.parse_args(argv)

    try:
        if args.check:
            drift, _ = check()
            if drift:
                print("\n".join(drift), file=sys.stderr)
                print(
                    f"manifest drift: {len(drift)} field(s); "
                    "run python3 scripts/plugin_manifests.py --write",
                    file=sys.stderr,
                )
                return 1
            print("OK: plugin manifests in sync")
            return 0

        drift, changes = write()
        if not changes:
            print("OK: plugin manifests already in sync")
            return 0
        for rel in sorted(changes):
            print(f"updated {rel}")
        print(f"OK: {len(changes)} file(s) updated ({len(drift)} drift(s))")
        return 0
    except ManifestError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
