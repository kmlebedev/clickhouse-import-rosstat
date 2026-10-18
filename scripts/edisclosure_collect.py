#!/usr/bin/env python3
"""Коллектор документов e-disclosure.ru.

Режимы:

* ``login`` — открыть headed Chrome с постоянным профилем и дать владельцу
  вручную решить капчу;
* ``collect`` — обойти карточки компаний и разделы, собрать ``manifest.json``
  и ``documents.jsonl``.

Профиль Chrome (``build/edisclosure-profile``, каталог 0700) и cookies — секреты:
значения не печатаются, профиль лежит в gitignored ``build/`` и не коммитится.
Капчу коллектор не обходит: он только ждёт решения владельцем в окне Chrome.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import time
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

BASE = "https://e-disclosure.ru"
COMPANIES: dict[str, str] = {"PLZL": "7832", "SBER": "3043", "CHMF": "30"}
SECTIONS: dict[str, int] = {"IFRS": 4, "RAS": 3, "AFFILIATES": 6, "AGM": 16}

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
PROFILE_DIR = REPO_ROOT / "build" / "edisclosure-profile"
DEFAULT_OUT = REPO_ROOT / "build" / "edisclosure"

FILES_TABLE_MARKER = "files-table"
# Маркер карточки компании; на страницах files.aspx его нет, поэтому он же
# отличает пустой раздел (редирект на company.aspx) от страницы с документами.
COMPANY_CARD_MARKER = "Полное наименование компании"
CONTENT_MARKERS = (
    COMPANY_CARD_MARKER,
    "Существенные события",
    "Карточка компании",
)
CAPTCHA_MARKERS = (
    "Разверните картинку",
    "captcha-holder",
    "Мы хотим убедиться",
    "Forbidden",
)

# Порядок полей — контракт спеки §4.2.
DOCUMENT_FIELDS = (
    "ticker",
    "company_id",
    "section",
    "doc_type",
    "period",
    "basis_date",
    "publication_date",
    "size",
    "fileid",
    "url",
    "table_num",
)


def log(message: str) -> None:
    print(message, flush=True)


def default_out() -> pathlib.Path:
    """Каталог выхода: ``EDISCLOSURE_OUT`` или ``build/edisclosure``."""
    return pathlib.Path(os.environ.get("EDISCLOSURE_OUT") or DEFAULT_OUT)


def company_url(company_id: str) -> str:
    return f"{BASE}/portal/company.aspx?id={company_id}"


def files_url(company_id: str, section: str) -> str:
    return f"{BASE}/portal/files.aspx?id={company_id}&type={SECTIONS[section]}"


def _cell_text(cell: Any) -> str:
    return cell.get_text(" ", strip=True)


def parse_documents_table(
    html: str, section: str, ticker: str
) -> list[dict[str, str]]:
    """Разобрать ``table.files-table`` страницы раздела в строки документов."""
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", class_="files-table")
    if table is None:
        raise ValueError("на странице нет table.files-table")
    ticker = ticker.strip().upper()
    documents: list[dict[str, str]] = []
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 6:
            continue
        link = cells[5].find("a", href=True)
        if link is None:
            continue
        href = urljoin(BASE, link["href"])
        query = parse_qs(urlparse(href).query)
        documents.append(
            {
                "ticker": ticker,
                "company_id": COMPANIES.get(ticker, ""),
                "section": section,
                "doc_type": _cell_text(cells[1]),
                "period": _cell_text(cells[2]),
                "basis_date": _cell_text(cells[3]),
                "publication_date": _cell_text(cells[4]),
                "size": link.get_text(" ", strip=True),
                "fileid": query.get("Fileid", [""])[0],
                "url": href,
                "table_num": _cell_text(cells[0]),
            }
        )
    return documents


def parse_company_name(html: str) -> str:
    """Название компании из ``table.company-table`` (h1 — заголовок портала)."""
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", class_="company-table")
    if table is not None:
        values: dict[str, str] = {}
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                key = _cell_text(cells[0])
                if key:
                    values[key] = _cell_text(cells[1])
        for key in ("Сокращенное наименование компании", "Полное наименование компании"):
            value = values.get(key, "").strip()
            if value:
                return value
    heading = soup.find("h1")
    if heading is not None:
        text = _cell_text(heading)
        if text and "ЦЕНТР РАСКРЫТИЯ" not in text.upper():
            return text
    return ""


def validate_document(row: dict[str, str], companies: dict[str, str]) -> None:
    """Проверить строку документа против контракта спеки §4.2."""
    ticker = row.get("ticker", "")
    if not ticker:
        raise ValueError("ticker: пустое значение")
    if ticker != ticker.upper():
        raise ValueError(f"ticker {ticker!r}: ожидается верхний регистр")
    if ticker not in companies:
        raise ValueError(f"ticker {ticker!r} отсутствует в manifest.companies")

    company_id = row.get("company_id", "")
    if company_id and company_id != companies[ticker]:
        raise ValueError(
            f"company_id {company_id!r} не совпадает с {companies[ticker]!r} "
            f"для {ticker}"
        )

    section = row.get("section", "")
    if section not in SECTIONS:
        raise ValueError(
            f"section {section!r}: допустимые {', '.join(SECTIONS)}"
        )

    if not row.get("doc_type", "").strip():
        raise ValueError("doc_type: пустое значение")

    fileid = row.get("fileid", "")
    if re.fullmatch(r"[0-9]+", fileid) is None:
        raise ValueError(f"fileid {fileid!r}: только цифры")

    url = row.get("url", "")
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(f"url {url!r}: нужен https")
    if parsed.hostname != "e-disclosure.ru":
        raise ValueError(f"url {url!r}: host должен быть e-disclosure.ru")
    if parsed.path != "/portal/FileLoad.ashx":
        raise ValueError(f"url {url!r}: path должен быть /portal/FileLoad.ashx")
    url_fileid = parse_qs(parsed.query).get("Fileid", [""])[0]
    if url_fileid != fileid:
        raise ValueError(
            f"Fileid в url {url!r} ({url_fileid!r}) не совпадает с fileid {fileid!r}"
        )


def document_key(row: dict[str, str]) -> tuple[str, str, str]:
    """Идентичность документа: ``(ticker, section, fileid)``."""
    return (
        row.get("ticker", ""),
        row.get("section", ""),
        row.get("fileid", ""),
    )


def dedupe_documents(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Схлопнуть одинаковые строки; расхождение полей при том же ключе — ошибка."""
    seen: dict[tuple[str, str, str], dict[str, str]] = {}
    result: list[dict[str, str]] = []
    for row in rows:
        key = document_key(row)
        previous = seen.get(key)
        if previous is not None:
            if previous == row:
                continue
            fields = sorted(set(previous) | set(row))
            different = [name for name in fields if previous.get(name) != row.get(name)]
            raise ValueError(
                f"конфликт документа {key}: различаются поля "
                f"{', '.join(different)}"
            )
        seen[key] = row
        result.append(row)
    return result


def build_manifest(
    companies: list[dict[str, str]],
    sections: list[str],
    documents: list[dict[str, str]],
    generated_at: str,
) -> dict[str, Any]:
    """Собрать manifest.json — маркер завершённого прогона коллектора."""
    return {
        "schema_version": 1,
        "generated_at": generated_at,
        "collector": {"name": "edisclosure_collect.py", "version": "1"},
        "sections": list(sections),
        "companies": [dict(company) for company in companies],
        "document_count": len(documents),
        "documents_file": "documents.jsonl",
    }


def write_output(
    out_dir: pathlib.Path,
    manifest: dict[str, Any],
    documents: list[dict[str, str]],
) -> None:
    """Атомарно записать documents.jsonl, затем manifest.json (манифест последним)."""
    out_dir = pathlib.Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    documents_path = out_dir / "documents.jsonl"
    manifest_path = out_dir / "manifest.json"
    documents_tmp = out_dir / "documents.jsonl.tmp"
    manifest_tmp = out_dir / "manifest.json.tmp"
    try:
        with documents_tmp.open("w", encoding="utf-8") as handle:
            for row in documents:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(documents_tmp, documents_path)

        with manifest_tmp.open("w", encoding="utf-8") as handle:
            json.dump(manifest, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(manifest_tmp, manifest_path)
    finally:
        for tmp in (documents_tmp, manifest_tmp):
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass


def ensure_profile_dir() -> None:
    """Профиль браузера — секрет: каталог только для владельца (0700)."""
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    os.chmod(PROFILE_DIR, 0o700)


def launch_context(playwright: Any) -> Any:
    ensure_profile_dir()
    return playwright.chromium.launch_persistent_context(
        user_data_dir=str(PROFILE_DIR),
        channel="chrome",
        headless=False,
        args=["--disable-blink-features=AutomationControlled"],
    )


def _first_page(context: Any) -> Any:
    return context.pages[0] if context.pages else context.new_page()


def wait_for_markers(
    page: Any, markers: tuple[str, ...], wait_seconds: int, url: str
) -> str:
    """Дождаться любого маркера контента, ожидая ручного решения капчи."""
    deadline = time.monotonic() + wait_seconds
    last_captcha_note = 0.0
    while True:
        try:
            html = page.content()
        except Exception:
            html = ""
        if any(marker in html for marker in markers):
            return html
        if any(marker in html for marker in CAPTCHA_MARKERS):
            now = time.monotonic()
            if now - last_captcha_note > 10:
                last_captcha_note = now
                log(
                    "  капча на странице: решите её вручную в окне Chrome "
                    f"(жду до {wait_seconds} с)"
                )
        if time.monotonic() >= deadline:
            raise SystemExit(
                f"не дождались контента {markers!r} на {url} за {wait_seconds} с; "
                "капча не решена — манифест не перезаписан"
            )
        time.sleep(1)


def run_login(tickers: list[str], wait_seconds: int) -> None:
    ticker = tickers[0]
    url = company_url(COMPANIES[ticker])
    log(f"открываю карточку {ticker}: {url}")
    log(
        "если показана капча — решите её в окне Chrome; "
        f"профиль сохраняется в {PROFILE_DIR}"
    )
    with sync_playwright() as playwright:
        context = launch_context(playwright)
        try:
            page = _first_page(context)
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            wait_for_markers(page, CONTENT_MARKERS, wait_seconds, url)
            log("контент доступен; профиль сохранён (cookies не печатаются)")
        finally:
            context.close()


def run_collect(
    tickers: list[str],
    sections: list[str],
    out_dir: pathlib.Path,
    wait_seconds: int,
) -> None:
    companies: list[dict[str, str]] = []
    documents: list[dict[str, str]] = []
    with sync_playwright() as playwright:
        context = launch_context(playwright)
        try:
            page = _first_page(context)
            for ticker in tickers:
                company_id = COMPANIES[ticker]
                url = company_url(company_id)
                log(f"компания {ticker} (id={company_id})")
                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                html = wait_for_markers(page, CONTENT_MARKERS, wait_seconds, url)
                name = parse_company_name(html)
                if not name:
                    log("  не удалось прочитать название компании")
                companies.append(
                    {
                        "ticker": ticker,
                        "company_id": company_id,
                        "name": name,
                        "url": url,
                    }
                )
                for section in sections:
                    section_url = files_url(company_id, section)
                    log(f"  раздел {section}: {section_url}")
                    page.goto(
                        section_url, wait_until="domcontentloaded", timeout=60_000
                    )
                    html = wait_for_markers(
                        page,
                        (FILES_TABLE_MARKER, COMPANY_CARD_MARKER),
                        wait_seconds,
                        section_url,
                    )
                    if FILES_TABLE_MARKER not in html:
                        # Пустой раздел e-disclosure редиректит на карточку компании.
                        log("    документов нет (раздел без files-table)")
                        continue
                    rows = parse_documents_table(html, section, ticker)
                    for row in rows:
                        row["company_id"] = company_id
                        validate_document(row, COMPANIES)
                    log(f"    документов: {len(rows)}")
                    documents.extend(rows)
        finally:
            context.close()

    unique = dedupe_documents(documents)
    if len(unique) != len(documents):
        log(f"схлопнуто дубликатов: {len(documents) - len(unique)}")
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    manifest = build_manifest(companies, sections, unique, generated_at)
    write_output(out_dir, manifest, unique)
    log(f"готово: документов {len(unique)} -> {out_dir}")


def parse_tickers(value: str) -> list[str]:
    tickers = list(
        dict.fromkeys(item.strip().upper() for item in value.split(",") if item.strip())
    )
    if not tickers:
        raise SystemExit("--companies: пустой список")
    unknown = [ticker for ticker in tickers if ticker not in COMPANIES]
    if unknown:
        raise SystemExit(
            f"--companies: неизвестные тикеры {', '.join(unknown)}; "
            f"допустимые: {', '.join(COMPANIES)}"
        )
    return tickers


def parse_sections(value: str) -> list[str]:
    sections = list(
        dict.fromkeys(item.strip().upper() for item in value.split(",") if item.strip())
    )
    if not sections:
        raise SystemExit("--sections: пустой список")
    unknown = [section for section in sections if section not in SECTIONS]
    if unknown:
        raise SystemExit(
            f"--sections: неизвестные разделы {', '.join(unknown)}; "
            f"допустимые: {', '.join(SECTIONS)}"
        )
    return sections


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Коллектор документов e-disclosure.ru (капчу решает владелец).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--companies",
        default=",".join(COMPANIES),
        help="тикеры через запятую (по умолчанию все: PLZL,SBER,CHMF)",
    )
    common.add_argument(
        "--sections",
        default=",".join(SECTIONS),
        help="разделы через запятую (по умолчанию все: IFRS,RAS,AFFILIATES,AGM)",
    )
    common.add_argument(
        "--out",
        type=pathlib.Path,
        default=None,
        help="каталог выхода (по умолчанию EDISCLOSURE_OUT или build/edisclosure)",
    )
    common.add_argument(
        "--wait",
        type=int,
        default=300,
        help="сколько секунд ждать ручного решения капчи (по умолчанию 300)",
    )
    subparsers.add_parser(
        "login",
        parents=[common],
        help="открыть Chrome и дать владельцу решить капчу; профиль сохраняется",
    )
    subparsers.add_parser(
        "collect",
        parents=[common],
        help="собрать manifest.json и documents.jsonl",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.wait < 0:
        raise SystemExit("--wait: ожидается неотрицательное число секунд")
    tickers = parse_tickers(args.companies)
    if args.command == "login":
        run_login(tickers, args.wait)
        return 0
    sections = parse_sections(args.sections)
    out_dir = args.out if args.out is not None else default_out()
    run_collect(tickers, sections, out_dir, args.wait)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
