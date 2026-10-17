#!/usr/bin/env python3
"""Офлайн-тесты scripts/docs_build.py.

Запуск из корня репозитория:

    python3 scripts/docs_build_test.py -v

Тесты используют только stdlib и временные каталоги; сеть не нужна.
"""

from __future__ import annotations

import contextlib
import io
import json
import re
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


def write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


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
    write_text(
        repo / "docs/guides/stat-ml-models.md",
        "# Модели\n\n![pic](img/pic.png)\n",
    )
    write_text(
        repo / "docs/research/2026-10-16-a.md",
        "# Исследование\n\n![pic](img/pic.png)\n",
    )
    write_bytes(repo / "docs/guides/img/pic.png", b"fixture-one")
    write_bytes(repo / "docs/research/img/pic.png", b"fixture-two")
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


class BannerTests(unittest.TestCase):
    def banner(self, source: str) -> str:
        return (
            f"> **Источник:** [{source}]"
            f"({db.REPO_URL}/blob/main/{source}). "
            "Страница собрана автоматически; правки — только в репозитории."
        )

    def test_banner_inserted_after_first_h1(self) -> None:
        text = "# Заголовок\n\nТекст.\n"

        result = db.insert_banner(text, "docs/foo.md", db.REPO_URL)
        lines = result.splitlines()

        self.assertEqual(lines[0], "# Заголовок")
        self.assertEqual(lines[1], "")
        self.assertEqual(lines[2], self.banner("docs/foo.md"))
        self.assertEqual(lines[3], "")
        self.assertEqual(lines[4], "Текст.")

    def test_banner_prepended_without_h1(self) -> None:
        text = "Просто текст.\n"

        result = db.insert_banner(text, "docs/foo.md", db.REPO_URL)

        self.assertTrue(result.startswith(self.banner("docs/foo.md") + "\n\n"))
        self.assertIn("Просто текст.", result)

    def test_banner_is_idempotent(self) -> None:
        text = "# Заголовок\n\nТекст.\n"

        once = db.insert_banner(text, "docs/foo.md", db.REPO_URL)
        twice = db.insert_banner(once, "docs/foo.md", db.REPO_URL)

        self.assertEqual(once, twice)
        self.assertEqual(once.count("**Источник:**"), 1)


class ImageTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.repo = make_repo(self.root)
        self.out = self.root / "site-docs"

    def test_image_copied_and_rewritten(self) -> None:
        text = "# Модели\n\n![pic](img/pic.png)\n"

        rewritten, errors = db.copy_images(
            text, "docs/guides/stat-ml-models.md", self.out, self.repo
        )

        self.assertEqual(errors, [])
        self.assertIn("![pic](assets/pic.png)", rewritten)
        self.assertEqual(
            (self.out / "assets/pic.png").read_bytes(), b"fixture-one"
        )

    def test_image_name_collision_uses_hash_suffix(self) -> None:
        first, first_errors = db.copy_images(
            "# A\n\n![pic](img/pic.png)\n",
            "docs/guides/stat-ml-models.md",
            self.out,
            self.repo,
        )
        second, second_errors = db.copy_images(
            "# B\n\n![pic](img/pic.png)\n",
            "docs/research/2026-10-16-a.md",
            self.out,
            self.repo,
        )

        self.assertEqual(first_errors, [])
        self.assertEqual(second_errors, [])
        self.assertIn("![pic](assets/pic.png)", first)
        match = re.search(r"!\[pic\]\(assets/(pic-[0-9a-f]{8}\.png)\)", second)
        self.assertIsNotNone(match, second)
        assert match is not None
        suffix_name = match.group(1)
        self.assertEqual(
            (self.out / "assets" / suffix_name).read_bytes(), b"fixture-two"
        )
        self.assertEqual(
            sorted(path.name for path in (self.out / "assets").iterdir()),
            sorted(["pic.png", suffix_name]),
        )

        # Повторная ссылка на уже скопированный файл не плодит копии.
        third, third_errors = db.copy_images(
            "# A\n\n![pic](img/pic.png)\n",
            "docs/guides/stat-ml-models.md",
            self.out,
            self.repo,
        )
        self.assertEqual(third_errors, [])
        self.assertIn("![pic](assets/pic.png)", third)
        self.assertEqual(len(list((self.out / "assets").iterdir())), 2)

    def test_copy_images_reports_missing_file(self) -> None:
        rewritten, errors = db.copy_images(
            "# Модели\n\n![pic](img/nope.png)\n",
            "docs/guides/stat-ml-models.md",
            self.out,
            self.repo,
        )

        self.assertIn("img/nope.png", rewritten)
        self.assertEqual(len(errors), 1)
        self.assertIn("image not found", errors[0])
        self.assertIn("docs/guides/stat-ml-models.md", errors[0])


def snapshot(root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


class WriteSiteTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.repo = make_repo(self.root)
        self.pages = db.load_manifest(self.repo / "docs/site-pages.json")

    def test_write_site_creates_pages_and_excludes_nothing(self) -> None:
        out = self.root / "site-docs"

        errors = db.write_site(self.pages, out, self.repo, db.REPO_URL)

        self.assertEqual(errors, [])
        for page in self.pages:
            with self.subTest(page=page.page):
                self.assertTrue((out / page.page).is_file())

        index = (out / "index.md").read_text(encoding="utf-8")
        self.assertIn("> **Источник:** [README.md](", index)
        self.assertIn("[архитектуру](architecture.md)", index)

        written = sorted(
            str(path.relative_to(out)) for path in out.rglob("*.md")
        )
        self.assertEqual(written, sorted(page.page for page in self.pages))
        self.assertFalse((out / "superpowers").exists())
        self.assertFalse((out / "plans").exists())
        self.assertFalse((out / "archive").exists())

    def test_write_site_is_deterministic(self) -> None:
        out_one = self.root / "run-one" / "site-docs"
        out_two = self.root / "run-two" / "site-docs"

        errors_one = db.write_site(self.pages, out_one, self.repo, db.REPO_URL)
        errors_two = db.write_site(self.pages, out_two, self.repo, db.REPO_URL)

        self.assertEqual(errors_one, [])
        self.assertEqual(errors_two, [])
        self.assertEqual(snapshot(out_one), snapshot(out_two))
        # Изображения действительно скопированы (иначе сравнение пустое).
        self.assertTrue((out_one / "assets/pic.png").is_file())


class MkdocsConfigTests(unittest.TestCase):
    def test_write_mkdocs_config_has_inherit_dirs_and_nav(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            repo = make_repo(root)
            pages = db.load_manifest(repo / "docs/site-pages.json")
            out = root / "build" / "mkdocs.yml"

            db.write_mkdocs_config(pages, out)
            text = out.read_text(encoding="utf-8")

        self.assertTrue(
            text.startswith(
                "INHERIT: ../mkdocs.base.yml\n"
                "docs_dir: site-docs\n"
                "site_dir: site\n"
                "nav:\n"
            ),
            text,
        )
        self.assertIn('  - "Обзор":\n      - "Главная": index.md\n', text)
        self.assertIn('  - "Архитектура":\n', text)
        self.assertIn('      - "Архитектура": architecture.md\n', text)
        self.assertIn('      - "Модели": stat-ml-models.md\n', text)
        self.assertLess(text.index('"Обзор"'), text.index('"Архитектура"'))
        self.assertLess(text.index('"Архитектура"'), text.index('"Роадмапы"'))
        self.assertLess(text.index('"Роадмапы"'), text.index('"Справочники"'))
        self.assertLess(text.index('"Справочники"'), text.index('"Исследования"'))


class RenderPageTests(unittest.TestCase):
    def test_render_page_uses_banner_images_and_links(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            repo = make_repo(root)
            pages = db.load_manifest(repo / "docs/site-pages.json")
            page = next(item for item in pages if item.page == "index.md")
            out = root / "site-docs"

            text, errors = db.render_page(page, repo, out, db.REPO_URL)

        self.assertEqual(errors, [])
        self.assertIn("> **Источник:** [README.md](", text)
        self.assertIn("[архитектуру](architecture.md)", text)


class MainTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.repo = make_repo(self.root)

    def run_main(self, argv: list[str]) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = db.main(argv)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_main_check_ok_returns_zero(self) -> None:
        before = snapshot(self.repo)

        code, stdout, stderr = self.run_main(
            ["--check", "--repo-root", str(self.repo)]
        )

        self.assertEqual(code, 0)
        self.assertIn("checked 5 pages", stdout)
        self.assertEqual(stderr, "")
        self.assertEqual(snapshot(self.repo), before)

    def test_main_check_broken_link_returns_one(self) -> None:
        write_text(
            self.repo / "README.md",
            "# Демо\n\nСм. [битую](missing.md).\n",
        )

        code, stdout, stderr = self.run_main(
            ["--check", "--repo-root", str(self.repo)]
        )

        self.assertEqual(code, 1)
        self.assertNotIn("checked", stdout)
        self.assertIn("README.md:3:", stderr)
        self.assertIn("link target not found: missing.md", stderr)

    def test_main_out_writes_pages_and_config(self) -> None:
        out = self.root / "build" / "site-docs"
        out.mkdir(parents=True)
        (out / "stale.txt").write_text("stale", encoding="utf-8")

        code, stdout, stderr = self.run_main(
            ["--out", str(out), "--repo-root", str(self.repo)]
        )

        self.assertEqual(code, 0, stderr)
        self.assertIn("built 5 pages", stdout)
        self.assertFalse((out / "stale.txt").exists())
        for page in db.load_manifest(self.repo / "docs/site-pages.json"):
            self.assertTrue((out / page.page).is_file(), page.page)
        config = out.parent / "mkdocs.yml"
        self.assertTrue(config.is_file())
        self.assertIn(
            "INHERIT: ../mkdocs.base.yml", config.read_text(encoding="utf-8")
        )


if __name__ == "__main__":
    unittest.main()
