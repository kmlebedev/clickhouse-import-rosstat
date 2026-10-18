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

# Поля содержания документа: по ним дубликат ключа либо схлопывается, либо
# признаётся конфликтом. `size` и `table_num` — аудит источника (человекочитаемая
# метка размера и номер строки таблицы), а не содержание, поэтому в сравнение не
# входят.
DEDUPE_FIELDS = (
    "doc_type",
    "period",
    "basis_date",
    "publication_date",
    "url",
    "company_id",
    "fileid",
)

DATE_PATTERN = re.compile(r"[0-9]{2}\.[0-9]{2}\.[0-9]{4}")
DATE_FORMAT = "%d.%m.%Y"


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


def classify_section_page(html: str) -> str:
    """Отличить страницу с документами от пустого раздела (карточка компании)."""
    if FILES_TABLE_MARKER in html:
        return "documents"
    if COMPANY_CARD_MARKER in html:
        return "empty"
    raise ValueError(
        "неизвестная страница: нет ни files-table, ни карточки компании"
    )


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

    _validate_date_field(row, "basis_date")
    _validate_date_field(row, "publication_date")

    url = row.get("url", "")
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(f"url {url!r}: нужен https")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError(f"url {url!r}: userinfo в URL запрещён")
    try:
        port = parsed.port
    except ValueError:
        raise ValueError(f"url {url!r}: некорректный порт") from None
    if port is not None:
        raise ValueError(f"url {url!r}: порт в URL запрещён")
    if ":" in parsed.netloc:
        # Пустой порт (`host:`) urlparse отдаёт как port=None, но netloc с ':';
        # userinfo и явный порт уже отвергнуты выше.
        raise ValueError(f"url {url!r}: порт в URL запрещён")
    if parsed.hostname != "e-disclosure.ru":
        raise ValueError(f"url {url!r}: host должен быть e-disclosure.ru")
    if parsed.path != "/portal/FileLoad.ashx":
        raise ValueError(f"url {url!r}: path должен быть /portal/FileLoad.ashx")
    url_fileid = parse_qs(parsed.query).get("Fileid", [""])[0]
    if url_fileid != fileid:
        raise ValueError(
            f"Fileid в url {url!r} ({url_fileid!r}) не совпадает с fileid {fileid!r}"
        )


def _validate_date_field(row: dict[str, str], field: str) -> None:
    """Непустая дата карточки — строго существующая дата ``DD.MM.YYYY``.

    Пустая строка — отсутствие даты (в таблицу уходит NULL). ``strptime`` один
    не гарантирует двузначные день и месяц, поэтому формат проверяется
    отдельным шаблоном, а существование даты (``31.02.2026``) — разбором.
    """
    value = row.get(field, "")
    if value == "":
        return
    if DATE_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field} {value!r}: ожидается формат ДД.ММ.ГГГГ")
    try:
        datetime.strptime(value, DATE_FORMAT)
    except ValueError:
        raise ValueError(f"{field} {value!r}: несуществующая дата") from None


def document_key(row: dict[str, str]) -> tuple[str, str, str]:
    """Идентичность документа: ``(ticker, section, fileid)``."""
    return (
        row.get("ticker", ""),
        row.get("section", ""),
        row.get("fileid", ""),
    )


def _document_content(row: dict[str, str]) -> tuple[str, ...]:
    """Поля содержания документа — без аудита ``size``/``table_num``."""
    return tuple(row.get(field, "") for field in DEDUPE_FIELDS)


def dedupe_documents(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Схлопнуть одинаковые строки; расхождение содержания при том же ключе — ошибка.

    ``size`` и ``table_num`` — аудит источника: одинаковые документы с разными
    метками размера или номера строки обязаны схлопываться, а не падать.
    """
    seen: dict[tuple[str, str, str], dict[str, str]] = {}
    result: list[dict[str, str]] = []
    for row in rows:
        key = document_key(row)
        previous = seen.get(key)
        if previous is not None:
            if _document_content(previous) == _document_content(row):
                continue
            different = [
                name
                for name in DEDUPE_FIELDS
                if previous.get(name, "") != row.get(name, "")
            ]
            raise ValueError(
                f"конфликт документа {key}: различаются поля "
                f"{', '.join(different)}"
            )
        seen[key] = row
        result.append(row)
    return result


def ensure_section_has_documents(
    section: str, rows: list[dict[str, str]]
) -> None:
    """Страница с ``files-table`` обязана дать хотя бы один документ.

    Пустой раздел e-disclosure — это редирект на карточку компании
    (``classify_section_page`` → ``empty``), а не таблица без строк. Ноль строк
    при найденной ``files-table`` означает сломанную разметку или неполную
    загрузку: записать такой раздел как «пустой» нельзя, иначе сломанный сбор
    выглядел бы как пустой источник.
    """
    if not rows:
        raise ValueError(
            f"раздел {section}: страница с files-table не содержит документов — "
            "сломанная разметка или неполная загрузка"
        )


def empty_sections_audit(
    companies: list[dict[str, str]],
    sections: list[str],
    empty_pages: set[tuple[str, str]],
) -> list[dict[str, Any]]:
    """Пустые разделы — только реальные редиректы на карточку компании.

    ``empty_pages`` — пары ``(ticker, section)``, для которых
    ``classify_section_page`` вернул ``empty``. Отсутствие документов в
    найденной ``files-table`` сюда не попадает: это ошибка
    ``ensure_section_has_documents``, а не пустой раздел.
    """
    result: list[dict[str, Any]] = []
    for company in companies:
        ticker = company.get("ticker", "")
        empty = [
            section for section in sections if (ticker, section) in empty_pages
        ]
        if empty:
            result.append({"ticker": ticker, "sections": empty})
    return result


def ensure_company_has_documents(counts: dict[str, int]) -> None:
    """Пустая компания — признак сломанного сбора, а не пустого источника."""
    for ticker, count in counts.items():
        if count == 0:
            raise ValueError(
                f"у компании {ticker} ноль документов во всех запрошенных разделах"
            )


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
    page: Any,
    markers: tuple[str, ...],
    wait_seconds: int,
    url: str,
    timeout_error: str | None = None,
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
                timeout_error
                or (
                    f"не дождались контента {markers!r} на {url} за "
                    f"{wait_seconds} с; капча не решена — манифест не перезаписан"
                )
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
    # empty_pages — только реальные редиректы на карточку компании (classify →
    # empty). Отсутствие строк в найденной files-table сюда не попадает: это
    # ошибка ensure_section_has_documents, а не пустой раздел.
    empty_pages: set[tuple[str, str]] = set()
    try:
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
                    company_document_count = 0
                    for section in sections:
                        section_url = files_url(company_id, section)
                        log(f"  раздел {section}: {section_url}")
                        page.goto(
                            section_url,
                            wait_until="domcontentloaded",
                            timeout=60_000,
                        )
                        html = wait_for_markers(
                            page,
                            (FILES_TABLE_MARKER, COMPANY_CARD_MARKER),
                            wait_seconds,
                            section_url,
                            timeout_error=(
                                f"неизвестная страница {section_url}: нет ни "
                                "files-table, ни карточки компании "
                                f"за {wait_seconds} с (возможно, капча не решена) — "
                                "манифест не записан"
                            ),
                        )
                        try:
                            kind = classify_section_page(html)
                        except ValueError as exc:
                            raise SystemExit(
                                f"{exc}: {section_url}; манифест не записан"
                            ) from None
                        if kind == "empty":
                            # Пустой раздел e-disclosure редиректит на карточку
                            # компании; только этот случай попадает в аудит.
                            log("    документов нет (пустой раздел)")
                            empty_pages.add((ticker, section))
                            continue
                        try:
                            rows = parse_documents_table(html, section, ticker)
                            ensure_section_has_documents(section, rows)
                        except ValueError as exc:
                            raise SystemExit(
                                f"{exc}: {section_url}; манифест не записан"
                            ) from None
                        for row in rows:
                            row["company_id"] = company_id
                            validate_document(row, COMPANIES)
                        log(f"    документов: {len(rows)}")
                        documents.extend(rows)
                        company_document_count += len(rows)
                    ensure_company_has_documents({ticker: company_document_count})
            finally:
                context.close()

        unique = dedupe_documents(documents)
        if len(unique) != len(documents):
            log(f"схлопнуто дубликатов: {len(documents) - len(unique)}")
        generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
        manifest = build_manifest(companies, sections, unique, generated_at)
        manifest["empty_sections"] = empty_sections_audit(
            companies, sections, empty_pages
        )
        write_output(out_dir, manifest, unique)
    except ValueError as exc:
        # Сломанный files-table, невалидная строка, конфликт дубликатов или
        # пустая компания — чистый выход без traceback: манифест не записан.
        raise SystemExit(f"{exc}; манифест не записан") from None
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
