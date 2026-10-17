#!/usr/bin/env python3
"""Генератор страниц docs-портала для GitHub Pages.

Читает манифест ``docs/site-pages.json``, копирует страницы в ``build/site-docs``,
вставляет баннер источника, переписывает относительные ссылки (на страницы сайта
или GitHub) и генерирует ``build/mkdocs.yml``.

Запуск::

    python3 scripts/docs_build.py --check
    python3 scripts/docs_build.py --out build/site-docs

Только стандартная библиотека Python 3.9+; сеть не используется.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import posixpath
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


REPO_URL = "https://github.com/kmlebedev/clickhouse-import-rosstat"

EXCLUDED_PREFIXES = ("docs/superpowers/", "docs/plans/", "docs/archive/")

PAGE_RE = re.compile(r"^[a-z0-9][a-z0-9-]*\.md$")

MANIFEST_PATH = "docs/site-pages.json"

MANIFEST_FIELDS = ("source", "page", "title", "section")

# URL со схемой (http:, https:, mailto:, tel:, data: …) и protocol-relative //.
_EXTERNAL_RE = re.compile(r"^(?:[a-zA-Z][a-zA-Z0-9+.-]*:|//)")

_REF_DEF_RE = re.compile(r"^(\s{0,3}\[([^\]]+)\]:\s*)(\S+)(.*)$")

_HTML_LINK_RE = re.compile(
    r"(<(?:a|img)\b[^>]*?\b(?:href|src)\s*=\s*)([\"'])(.*?)\2",
    re.IGNORECASE,
)

_INLINE_LINK_RE = re.compile(
    r"(?<!!)\[([^\]]*)\]\(([^)\s]+)(\s+(?:\"[^\"]*\"|'[^']*'))?\)"
)

# Code-регионы: fenced-блоки (``` / ~~~), indented code (4 пробела или таб)
# и inline-code spans (одинарные/двойные/тройные обратные кавычки).
_FENCE_RE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")

_INLINE_CODE_RE = re.compile(r"(`+)(.+?)\1")

_MASK_PREFIX = "\x00DOCS_MASK_"

_MASK_SUFFIX = ";\x00"


@dataclass(frozen=True)
class Page:
    """Публикуемая страница: источник, слаг, заголовок, раздел навигации."""

    source: str
    page: str
    title: str
    section: str


def load_manifest(path: Path) -> list[Page]:
    """Загрузить манифест; ошибка формата — ``ValueError``."""
    manifest_path = Path(path)
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"{manifest_path}: manifest must be a JSON array")

    pages = []
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            raise ValueError(f"{manifest_path}: entry #{index + 1} must be an object")
        missing = [field for field in MANIFEST_FIELDS if field not in item]
        if missing:
            raise ValueError(
                f"{manifest_path}: entry #{index + 1} misses "
                + ", ".join(repr(field) for field in missing)
            )
        values = {}
        for field in MANIFEST_FIELDS:
            value = item[field]
            if not isinstance(value, str) or not value:
                raise ValueError(
                    f"{manifest_path}: entry #{index + 1} field {field!r} "
                    "must be a non-empty string"
                )
            values[field] = value
        pages.append(
            Page(
                source=values["source"],
                page=values["page"],
                title=values["title"],
                section=values["section"],
            )
        )
    return pages


def validate_manifest(pages: list[Page], repo_root: Path) -> list[str]:
    """Проверить манифест; вернуть список ошибок (пустой — всё хорошо)."""
    errors = []
    seen = {}
    for page in pages:
        source = page.source
        if not source:
            errors.append("empty source")
        elif Path(source).is_absolute():
            errors.append(f"{source}: source must be a relative path (absolute given)")
        elif ".." in Path(source).parts:
            errors.append(f"{source}: source must not contain '..'")
        elif source.startswith(EXCLUDED_PREFIXES):
            errors.append(f"{source}: source is in an excluded directory")
        elif not (repo_root / source).is_file():
            errors.append(f"{source}: source not found or not a file")

        if not PAGE_RE.match(page.page):
            errors.append(
                f"{page.page}: invalid page slug "
                f"(expected {PAGE_RE.pattern})"
            )
        if page.page in seen:
            errors.append(
                f"{page.page}: duplicate page slug "
                f"(also used by {seen[page.page]})"
            )
        else:
            seen[page.page] = source
    return errors


def build_page_map(pages: list[Page]) -> dict[str, str]:
    """Сопоставить путь источника и слаг страницы сайта."""
    return {page.source: page.page for page in pages}


def resolve_target(
    source: str,
    target: str,
    page_map: dict[str, str],
    repo_root: Path,
    repo_url: str,
) -> tuple[Optional[str], Optional[str]]:
    """Разрешить target относительно source.

    Возвращает ``(new_target, None)`` или ``(None, error)``. Внешние ссылки,
    ``mailto:``/``tel:`` и ``#anchor`` возвращаются без изменений.
    """
    if target.startswith("#") or _EXTERNAL_RE.match(target):
        return target, None

    path, separator, anchor = target.partition("#")
    anchor = separator + anchor
    if not path:
        return target, None

    if path.startswith("/"):
        resolved = posixpath.normpath(path).lstrip("/")
    else:
        joined = posixpath.join(posixpath.dirname(source), path)
        resolved = posixpath.normpath(joined).lstrip("/")

    if resolved in (".", ""):
        resolved = "."
    if resolved == ".." or resolved.startswith("../"):
        return None, f"{source}: link target not found: {target}"

    if resolved in page_map:
        return page_map[resolved] + anchor, None

    candidate = repo_root / resolved
    if candidate.is_file():
        return f"{repo_url}/blob/main/{resolved}{anchor}", None
    if candidate.is_dir():
        return f"{repo_url}/tree/main/{resolved}{anchor}", None
    return None, f"{source}: link target not found: {target}"


def _error_at_line(error: str, source: str, lineno: int) -> str:
    prefix = source + ": "
    if error.startswith(prefix):
        error = error[len(prefix):]
    return f"{source}:{lineno}: {error}"


def _rewrite_reference_definitions(
    line: str,
    source: str,
    page_map: dict[str, str],
    repo_root: Path,
    repo_url: str,
    lineno: int,
    errors: list[str],
) -> str:
    def replace(match: re.Match) -> str:
        prefix, identifier, target, rest = match.groups()
        # [^note]: … — это сноска (footnotes), а не reference-определение.
        if identifier.startswith("^"):
            return match.group(0)
        new_target, error = resolve_target(
            source, target, page_map, repo_root, repo_url
        )
        if error is not None:
            errors.append(_error_at_line(error, source, lineno))
            return match.group(0)
        return prefix + new_target + rest

    return _REF_DEF_RE.sub(replace, line)


def _rewrite_html_links(
    line: str,
    source: str,
    page_map: dict[str, str],
    repo_root: Path,
    repo_url: str,
    lineno: int,
    errors: list[str],
) -> str:
    def replace(match: re.Match) -> str:
        prefix, quote, target = match.groups()
        new_target, error = resolve_target(
            source, target, page_map, repo_root, repo_url
        )
        if error is not None:
            errors.append(_error_at_line(error, source, lineno))
            return match.group(0)
        return prefix + quote + new_target + quote

    return _HTML_LINK_RE.sub(replace, line)


def _rewrite_inline_links(
    line: str,
    source: str,
    page_map: dict[str, str],
    repo_root: Path,
    repo_url: str,
    lineno: int,
    errors: list[str],
) -> str:
    def replace(match: re.Match) -> str:
        label, target, title = match.groups()
        new_target, error = resolve_target(
            source, target, page_map, repo_root, repo_url
        )
        if error is not None:
            errors.append(_error_at_line(error, source, lineno))
            return match.group(0)
        return f"[{label}]({new_target}{title or ''})"

    return _INLINE_LINK_RE.sub(replace, line)


def _mask_region(fragment: str, restore: dict[str, str]) -> str:
    placeholder = f"{_MASK_PREFIX}{len(restore)}{_MASK_SUFFIX}"
    restore[placeholder] = fragment
    return placeholder


def _mask_inline_code(line: str, restore: dict[str, str]) -> str:
    def replace(match: re.Match) -> str:
        return _mask_region(match.group(0), restore)

    return _INLINE_CODE_RE.sub(replace, line)


def _mask_code_regions(text: str) -> tuple[str, dict[str, str]]:
    """Заменить code-регионы placeholder'ами, сохранив число строк.

    Маскируются fenced-блоки (````` ``` ````` и ``~~~``), строки indented code
    (4 пробела или таб) и inline-code spans. Внутри placeholder'ов нет
    Markdown/HTML-ссылок, поэтому проходы ниже их не трогают.
    """
    restore: dict[str, str] = {}
    output_lines = []
    fence = None  # (символ, длина) открытого fence

    for line in text.splitlines(keepends=True):
        content = line.rstrip("\r\n")
        newline = line[len(content):]

        if fence is not None:
            output_lines.append(_mask_region(content, restore) + newline)
            match = _FENCE_RE.match(content)
            if (
                match is not None
                and match.group(1)[0] == fence[0]
                and len(match.group(1)) >= fence[1]
                and not match.group(2).strip()
            ):
                fence = None
            continue

        match = _FENCE_RE.match(content)
        if match is not None:
            fence = (match.group(1)[0], len(match.group(1)))
            output_lines.append(_mask_region(content, restore) + newline)
            continue

        if content.startswith("    ") or content.startswith("\t"):
            output_lines.append(_mask_region(content, restore) + newline)
            continue

        output_lines.append(_mask_inline_code(content, restore) + newline)

    return "".join(output_lines), restore


def _restore_code_regions(text: str, restore: dict[str, str]) -> str:
    for placeholder, original in restore.items():
        text = text.replace(placeholder, original)
    return text


def rewrite_links(
    text: str,
    source: str,
    page_map: dict[str, str],
    repo_root: Path,
    repo_url: str,
) -> tuple[str, list[str]]:
    """Переписать относительные ссылки; вернуть текст и ошибки ``файл:строка``.

    Обрабатываются reference-определения, HTML ``href``/``src`` и inline-ссылки.
    Markdown-изображения (``![…](…)``) не трогаются: их обрабатывает
    :func:`copy_images`. Ссылки внутри code-регионов не переписываются и не
    проверяются.
    """
    errors: list[str] = []
    output_lines = []
    masked_text, restore = _mask_code_regions(text)
    for lineno, line in enumerate(masked_text.splitlines(keepends=True), start=1):
        line = _rewrite_reference_definitions(
            line, source, page_map, repo_root, repo_url, lineno, errors
        )
        line = _rewrite_html_links(
            line, source, page_map, repo_root, repo_url, lineno, errors
        )
        line = _rewrite_inline_links(
            line, source, page_map, repo_root, repo_url, lineno, errors
        )
        output_lines.append(line)
    return _restore_code_regions("".join(output_lines), restore), errors


_IMAGE_RE = re.compile(
    r"!\[([^\]]*)\]\(([^)\s]+)(\s+(?:\"[^\"]*\"|'[^']*'))?\)"
)


def insert_banner(text: str, source: str, repo_url: str) -> str:
    """Вставить баннер источника после первого H1 (идемпотентно)."""
    banner = (
        f"> **Источник:** [{source}]({repo_url}/blob/main/{source}). "
        "Страница собрана автоматически; правки — только в репозитории."
    )
    lines = text.splitlines()
    if banner in lines:
        return text

    for index, line in enumerate(lines):
        if line.startswith("# "):
            after = lines[index + 1:]
            while after and not after[0].strip():
                after.pop(0)
            lines = lines[: index + 1] + ["", banner, ""] + after
            break
    else:
        lines = [banner, ""] + lines
    return "\n".join(lines) + "\n"


def _write_text(path: Path, text: str) -> None:
    """Записать текст с LF независимо от платформы."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)


def copy_images(
    text: str,
    source: str,
    out_dir: Path,
    repo_root: Path,
) -> tuple[str, list[str]]:
    """Скопировать локальные Markdown-изображения в ``out_dir/assets``.

    Внешние изображения (URL, ``data:``) не трогаются. При совпадении имён
    файлов имя получает суффикс из sha1 пути источника.
    """
    errors: list[str] = []
    assets_dir = Path(out_dir) / "assets"
    output_lines = []

    for lineno, line in enumerate(text.splitlines(keepends=True), start=1):

        def replace(match: re.Match, lineno: int = lineno) -> str:
            alt, src, title = match.groups()
            if src.startswith("#") or _EXTERNAL_RE.match(src):
                return match.group(0)

            path, fragment_separator, fragment = src.partition("#")
            suffix = fragment_separator + fragment
            if path.startswith("/"):
                resolved = posixpath.normpath(path).lstrip("/")
            else:
                resolved = posixpath.normpath(
                    posixpath.join(posixpath.dirname(source), path)
                )

            if resolved == ".." or resolved.startswith("../"):
                errors.append(f"{source}:{lineno}: image not found: {src}")
                return match.group(0)

            image_path = Path(repo_root) / resolved
            if not image_path.is_file():
                errors.append(f"{source}:{lineno}: image not found: {src}")
                return match.group(0)

            assets_dir.mkdir(parents=True, exist_ok=True)
            name = posixpath.basename(resolved)
            destination = assets_dir / name
            if destination.exists():
                if destination.read_bytes() == image_path.read_bytes():
                    return f"![{alt}](assets/{name}{suffix}{title or ''})"
                digest = hashlib.sha1(resolved.encode("utf-8")).hexdigest()[:8]
                stem, extension = posixpath.splitext(name)
                name = f"{stem}-{digest}{extension}"
                destination = assets_dir / name

            shutil.copyfile(str(image_path), str(destination))
            return f"![{alt}](assets/{name}{suffix}{title or ''})"

        output_lines.append(_IMAGE_RE.sub(replace, line))

    return "".join(output_lines), errors


def render_page(
    page: Page,
    repo_root: Path,
    out_dir: Path,
    repo_url: str,
    page_map: Optional[dict[str, str]] = None,
) -> tuple[str, list[str]]:
    """Прочитать источник, скопировать картинки, переписать ссылки, вставить баннер.

    ``copy_images`` и ``rewrite_links`` работают до вставки баннера, чтобы
    ошибки ``файл:строка`` указывали на строки исходного документа.
    """
    repo_root = Path(repo_root)
    out_dir = Path(out_dir)
    text = (repo_root / page.source).read_text(encoding="utf-8")

    if page_map is None:
        manifest_path = repo_root / MANIFEST_PATH
        if manifest_path.is_file():
            page_map = build_page_map(load_manifest(manifest_path))
        else:
            page_map = {page.source: page.page}

    text, image_errors = copy_images(text, page.source, out_dir, repo_root)
    text, link_errors = rewrite_links(
        text, page.source, page_map, repo_root, repo_url
    )
    text = insert_banner(text, page.source, repo_url)
    return text, image_errors + link_errors


def write_mkdocs_config(pages: list[Page], out_path: Path) -> None:
    """Записать ``mkdocs.yml`` с nav, сгруппированным по разделам манифеста."""
    sections: list[str] = []
    grouped: dict[str, list[Page]] = {}
    for page in pages:
        if page.section not in grouped:
            grouped[page.section] = []
            sections.append(page.section)
        grouped[page.section].append(page)

    lines = [
        "INHERIT: ../mkdocs.base.yml",
        "docs_dir: site-docs",
        "site_dir: site",
        "nav:",
    ]
    for section in sections:
        lines.append(f"  - {json.dumps(section, ensure_ascii=False)}:")
        for page in grouped[section]:
            lines.append(
                f"      - {json.dumps(page.title, ensure_ascii=False)}: "
                f"{page.page}"
            )
    _write_text(Path(out_path), "\n".join(lines) + "\n")


def write_site(
    pages: list[Page],
    out_dir: Path,
    repo_root: Path,
    repo_url: str,
) -> list[str]:
    """Собрать страницы сайта; вернуть список ошибок."""
    out_dir = Path(out_dir)
    repo_root = Path(repo_root)
    out_resolved = out_dir.resolve()
    repo_resolved = repo_root.resolve()
    if out_resolved == repo_resolved or out_resolved in repo_resolved.parents:
        return [f"{out_dir}: refusing to clean repository root"]

    shutil.rmtree(str(out_dir), ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    page_map = build_page_map(pages)
    errors: list[str] = []
    for page in pages:
        text, page_errors = render_page(
            page, repo_root, out_dir, repo_url, page_map
        )
        errors.extend(page_errors)
        _write_text(out_dir / page.page, text)
    return errors


def main(argv: Optional[list[str]] = None) -> int:
    """CLI: ``--check`` (ничего не пишет) или ``--out <dir>``."""
    parser = argparse.ArgumentParser(
        description="Build GitHub Pages docs from docs/site-pages.json"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--check",
        action="store_true",
        help="validate manifest, links and images without writing files",
    )
    mode.add_argument(
        "--out",
        type=Path,
        metavar="DIR",
        help="write site pages to DIR and mkdocs.yml next to DIR",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="repository root (default: parent of scripts/)",
    )
    args = parser.parse_args(argv)

    repo_root = args.repo_root.resolve()
    try:
        pages = load_manifest(repo_root / MANIFEST_PATH)
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    errors = validate_manifest(pages, repo_root)
    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 1

    if args.check:
        errors = []
        page_map = build_page_map(pages)
        with tempfile.TemporaryDirectory() as raw:
            temp_dir = Path(raw)
            for page in pages:
                _, page_errors = render_page(
                    page, repo_root, temp_dir, REPO_URL, page_map
                )
                errors.extend(page_errors)
        if errors:
            for error in errors:
                print(f"error: {error}", file=sys.stderr)
            return 1
        print(f"checked {len(pages)} pages")
        return 0

    assert args.out is not None
    out_dir = args.out
    errors = write_site(pages, out_dir, repo_root, REPO_URL)
    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 1
    write_mkdocs_config(pages, out_dir.parent / "mkdocs.yml")
    print(f"built {len(pages)} pages to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
