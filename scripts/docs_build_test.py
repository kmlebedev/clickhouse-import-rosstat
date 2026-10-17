#!/usr/bin/env python3
"""Офлайн-тесты scripts/docs_build.py.

Запуск из корня репозитория:

    python3 scripts/docs_build_test.py -v

Тесты используют только stdlib и временные каталоги; сеть не нужна.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

# Модуль и тест лежат рядом, запуск прямой (`python3 scripts/...`), поэтому его
# каталог добавляется в sys.path до импорта.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import docs_build as db


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def manifest_entry(source: str, page: str, title: str, section: str) -> dict:
    return {"source": source, "page": page, "title": title, "section": section}


def make_repo(tmp: Path) -> Path:
    """Минимальный репозиторий для тестов (состав — по плану Task 1)."""
    repo = tmp / "repo"
    repo.mkdir(parents=True, exist_ok=True)
    write_text(repo / "README.md", "# Демо\n\nСм. [архитектуру](ARCHITECTURE.md).\n")
    write_text(
        repo / "ARCHITECTURE.md",
        "# Архитектура\n\nСм. [реестр](docs/ROADMAP_INDEX.md).\n",
    )
    write_text(
        repo / "docs/ROADMAP_INDEX.md",
        "# Реестр роадмапов\n\nСм. [спеку](superpowers/specs/x-design.md).\n",
    )
    write_text(repo / "docs/superpowers/specs/x-design.md", "# Дизайн\n")
    write_text(repo / "docs/archive/registry.jsonl", "{}\n")
    write_text(repo / "docs/guides/stat-ml-models.md", "# Модели\n")
    write_text(repo / "docs/research/2026-10-16-a.md", "# Исследование\n")
    manifest = [
        manifest_entry("README.md", "index.md", "Главная", "Обзор"),
        manifest_entry("ARCHITECTURE.md", "architecture.md", "Архитектура", "Архитектура"),
        manifest_entry(
            "docs/ROADMAP_INDEX.md", "roadmap-index.md", "Реестр", "Роадмапы"
        ),
        manifest_entry(
            "docs/guides/stat-ml-models.md",
            "stat-ml-models.md",
            "Модели",
            "Справочники",
        ),
        manifest_entry(
            "docs/research/2026-10-16-a.md",
            "research-2026-10-16-a.md",
            "Исследование",
            "Исследования",
        ),
    ]
    write_text(
        repo / "docs/site-pages.json",
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
    )
    return repo


class ManifestTests(unittest.TestCase):
    def test_load_manifest_returns_pages_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            repo = make_repo(Path(raw))
            pages = db.load_manifest(repo / "docs/site-pages.json")

        self.assertEqual(
            [page.source for page in pages],
            [
                "README.md",
                "ARCHITECTURE.md",
                "docs/ROADMAP_INDEX.md",
                "docs/guides/stat-ml-models.md",
                "docs/research/2026-10-16-a.md",
            ],
        )
        self.assertEqual(
            pages[0], db.Page("README.md", "index.md", "Главная", "Обзор")
        )
        self.assertEqual(pages[2].section, "Роадмапы")

    def test_validate_reports_missing_source(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            repo = make_repo(Path(raw))
            errors = db.validate_manifest(
                [db.Page("docs/missing.md", "missing.md", "Нет", "Обзор")], repo
            )

        self.assertEqual(len(errors), 1)
        self.assertIn("docs/missing.md", errors[0])
        self.assertIn("not found", errors[0])

    def test_validate_reports_duplicate_page(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            repo = make_repo(Path(raw))
            errors = db.validate_manifest(
                [
                    db.Page("README.md", "same.md", "Раз", "Обзор"),
                    db.Page("ARCHITECTURE.md", "same.md", "Два", "Архитектура"),
                ],
                repo,
            )

        self.assertTrue(any("duplicate" in error for error in errors), errors)
        self.assertTrue(any("same.md" in error for error in errors), errors)

    def test_validate_rejects_bad_slug(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            repo = make_repo(Path(raw))
            errors = db.validate_manifest(
                [db.Page("README.md", "Bad Page.md", "Плохо", "Обзор")], repo
            )

        self.assertTrue(any("slug" in error.lower() for error in errors), errors)
        self.assertTrue(any("Bad Page.md" in error for error in errors), errors)

    def test_validate_rejects_excluded_source(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            repo = make_repo(Path(raw))
            errors = db.validate_manifest(
                [
                    db.Page(
                        "docs/superpowers/specs/x-design.md",
                        "x-design.md",
                        "Спека",
                        "Обзор",
                    )
                ],
                repo,
            )

        self.assertTrue(any("excluded" in error for error in errors), errors)
        self.assertTrue(
            any("docs/superpowers/specs/x-design.md" in error for error in errors),
            errors,
        )

    def test_validate_rejects_escaping_source(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            repo = make_repo(Path(raw))
            errors = db.validate_manifest(
                [
                    db.Page("../outside.md", "outside.md", "Вне", "Обзор"),
                    db.Page("/abs.md", "abs.md", "Абс", "Обзор"),
                ],
                repo,
            )

        self.assertTrue(
            any("outside.md" in error and ".." in error for error in errors), errors
        )
        self.assertTrue(
            any("abs.md" in error and "absolute" in error for error in errors), errors
        )


class ResolveTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.repo = make_repo(Path(self._tmp.name))
        self.pages = db.load_manifest(self.repo / "docs/site-pages.json")
        self.page_map = db.build_page_map(self.pages)

    def resolve(self, source: str, target: str):
        return db.resolve_target(
            source, target, self.page_map, self.repo, db.REPO_URL
        )

    def test_resolve_mirrored_page(self) -> None:
        self.assertEqual(
            self.resolve("docs/ROADMAP_INDEX.md", "ROADMAP_INDEX.md"),
            ("roadmap-index.md", None),
        )

    def test_resolve_mirrored_page_keeps_anchor(self) -> None:
        self.assertEqual(
            self.resolve("docs/ROADMAP_INDEX.md", "ROADMAP_INDEX.md#3"),
            ("roadmap-index.md#3", None),
        )

    def test_resolve_spec_becomes_github_blob(self) -> None:
        self.assertEqual(
            self.resolve("docs/ROADMAP_INDEX.md", "superpowers/specs/x-design.md"),
            (
                db.REPO_URL + "/blob/main/docs/superpowers/specs/x-design.md",
                None,
            ),
        )

    def test_resolve_directory_becomes_github_tree(self) -> None:
        self.assertEqual(
            self.resolve("README.md", "docs/guides/"),
            (db.REPO_URL + "/tree/main/docs/guides", None),
        )

    def test_resolve_external_and_anchor_unchanged(self) -> None:
        for target in (
            "https://example.com/a?b=1#c",
            "http://example.com",
            "mailto:dev@example.com",
            "tel:+70000000000",
            "#fragment",
            "#",
        ):
            with self.subTest(target=target):
                self.assertEqual(self.resolve("README.md", target), (target, None))

    def test_resolve_missing_target_returns_error(self) -> None:
        resolved, error = self.resolve("docs/ROADMAP_INDEX.md", "nope.md")

        self.assertIsNone(resolved)
        self.assertIsNotNone(error)
        assert error is not None
        self.assertIn("docs/ROADMAP_INDEX.md", error)
        self.assertIn("link target not found: nope.md", error)


class RewriteTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.repo = make_repo(Path(self._tmp.name))
        self.pages = db.load_manifest(self.repo / "docs/site-pages.json")
        self.page_map = db.build_page_map(self.pages)

    def test_rewrite_inline_reference_and_html_links(self) -> None:
        text = "\n".join(
            [
                "# Реестр",
                "",
                "См. [гайд](guides/stat-ml-models.md#top) и "
                "[спеку](superpowers/specs/x-design.md).",
                "[ref]: guides/stat-ml-models.md",
                '<a href="superpowers/specs/x-design.md">design</a>',
                '<img src="https://example.com/x.png">',
                "[ext](https://example.com) [anchor](#x)",
                "![pic](assets/pic.png)",
                "[^note]: сноска",
                "",
            ]
        )

        rewritten, errors = db.rewrite_links(
            text,
            "docs/ROADMAP_INDEX.md",
            self.page_map,
            self.repo,
            db.REPO_URL,
        )

        self.assertEqual(errors, [])
        self.assertIn("[гайд](stat-ml-models.md#top)", rewritten)
        self.assertIn(
            "[спеку]("
            + db.REPO_URL
            + "/blob/main/docs/superpowers/specs/x-design.md)",
            rewritten,
        )
        self.assertIn("[ref]: stat-ml-models.md", rewritten)
        self.assertIn(
            '<a href="'
            + db.REPO_URL
            + '/blob/main/docs/superpowers/specs/x-design.md">',
            rewritten,
        )
        self.assertIn('<img src="https://example.com/x.png">', rewritten)
        self.assertIn("[ext](https://example.com)", rewritten)
        self.assertIn("[anchor](#x)", rewritten)
        self.assertIn("![pic](assets/pic.png)", rewritten)
        self.assertIn("[^note]: сноска", rewritten)

    def test_rewrite_reports_line_number(self) -> None:
        text = "# Реестр\n\nСм. [битую](nope.md).\n"

        rewritten, errors = db.rewrite_links(
            text,
            "docs/ROADMAP_INDEX.md",
            self.page_map,
            self.repo,
            db.REPO_URL,
        )

        self.assertEqual(rewritten, text)
        self.assertEqual(len(errors), 1)
        self.assertIn("docs/ROADMAP_INDEX.md:3:", errors[0])
        self.assertIn("link target not found: nope.md", errors[0])


if __name__ == "__main__":
    unittest.main()
