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

import json
import posixpath
import re
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
    :func:`copy_images`.
    """
    errors: list[str] = []
    output_lines = []
    for lineno, line in enumerate(text.splitlines(keepends=True), start=1):
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
    return "".join(output_lines), errors


def main(argv: Optional[list[str]] = None) -> int:
    """CLI-заглушка Task 1; полная реализация — в Task 2."""
    raise SystemExit("docs_build: CLI is not implemented yet")


if __name__ == "__main__":
    raise SystemExit(main())
