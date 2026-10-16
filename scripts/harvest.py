#!/usr/bin/env python3
"""Систематическая добыча недельных ИПЦ Росстата до 2022 года из Wayback и живого буфера.

Зачем этот скрипт
-----------------
Недельная оценка ИПЦ Росстата публиковалась до 2022 года HTML-бюллетенями «Оперативной
информации» (`bgd/free/B04_03/IssWWW.exe/Stg/d02/<N>.htm`), которые нигде не собраны в один
архив: живой буфер «плавающий» (Росстат может вычистить его в любой момент), а Wayback
сохранил только часть выпусков. Ряд `rosstat_ipc_weeks_bulletins` собирался разовыми
прогонами, артефакты которых остались в /tmp (`harvest.py`, `harvest_anchors.py`,
`harvest_anchors_curl.py`, `harvest_digest.py`, `harvest_resume.py`); этот скрипт — их
обобщение: один возобновляемый инструмент, который сам находит недостающие недели и
складывает найденные документы в винтажный архив `build/archive-dev` — в той же раскладке
и в том же реестре, что пишет конвейер (`archive.Key`, `registry.jsonl`), чтобы добранные
недели потом восстанавливались штатным `make archive-restore`.

Что делает (режим weeks, по умолчанию)
--------------------------------------
1. **CDX-энумерация** (источник `wayback`): для каждого семейства URL буфера запрашивает у
   web.archive.org полный список снимков (с 2016 года — начала официальной недельной оценки;
   дедуп по digest, только `statuscode:200`) и оставляет `.htm`-документы (вложенные ассеты
   Word-экспорта `*.files/*` отбрасываются). CDX канонизирует регистр пути, поэтому вариантов
   B04_03/b04_03/Isswww/Stg перечислять не нужно — одна запись на хост.
2. **Классификация заголовков**: для несделанных снимков читаются первые 6 КБ документа
   (форма `id_` — сырые байты; если сырой плейбек Wayback ушёл в цикл редиректов — обычная
   форма реплея: заголовок в ней тот же). Вердикт: недельный выпуск («Об оценке индекса
   потребительских цен с … по …») → дата точки; месячный; прочее.
3. **Живой буфер** (источник `live`): обходит текущие слоты `d02/{1..260}.htm`, классифицирует
   так же — страховка от того, что Wayback что-то не сохранил.
4. **Дыры**: сравнивает найденные недели с каноническим списком источников импортёра
   (`rosstat/ipc_weeks_bulletins.go`); всё найденное, чего в списке нет, — кандидат (недели
   после 2022 не трогаются: там первичен XLSX-импортёр).
5. **Загрузка и раскладка в архив**: полный документ кандидата скачивается, заголовок
   проверяется повторно (файл обязан быть той же недели), sha256 считается по содержимому,
   отпечаток версии — как `util.HeadFingerprint` (md5 от «url|basename|ETag|Last-Modified|
   Content-Length» живого HEAD). Объект ложится в `<архив>/rosstat_ipc_weeks_bulletins/
   <префикс16>_<имя>`, строка — в `<архив>/registry.jsonl` (формат и ключи повторяют
   archive.Key/registry.go: префикс отпечатка, при пустом отпечатке — «sha-<16>»; upsert по
   ключу, при смене sha строка уходит в конец файла). Сами недели в список источников скрипт
   не добавляет — он печатает кандидатов (`state/candidates.tsv`), расширение списка и
   `make archive-restore` остаются шагом человека.

Режим xlsx
----------
Проверка истории файла `https://rosstat.gov.ru/storage/mediabank/nedel_Ipc.xlsx`: все снапшоты
файла (и любых `nedel*` в медиабанке старого и нового домена) скачиваются и разбираются —
нет ли версии с табличными данными 2021 года и раньше. Вердикт и таблица — `state/xlsx_*`.

Состояние и возобновление
-------------------------
Всё состояние — в каталоге `build/harvest` (gitignored): `titles.csv` (результаты
классификации; пары «снимок → вердикт»), `cdx_*.tsv`, `snapshots.tsv`, `findings.tsv`,
`candidates.tsv`, `run.log`. Повторный запуск продолжает с места остановки:
классифицированные пары не перезапрашиваются (кроме `err`/`week:??` — они повторяются),
загруженные версии перезаписываются идемпотентно. Скрипт рассчитан на фоновый запуск; темп
щадящий (по умолчанию 2 потока и пауза 0.45 с), при сбоях — длинные паузы: Wayback отдаёт
редирект-циклы и 429 волнами, окно доступности наступает позже.

Запуск
------
    python3 scripts/harvest.py                  # недели: поиск и раскладка в build/archive-dev
    python3 scripts/harvest.py --mode xlsx      # история nedel_Ipc.xlsx
    python3 scripts/harvest.py --dry-run        # только поиск, без загрузки документов

Тесты: `python3 scripts/harvest_test.py` (офлайн, без сети).
"""

import argparse
import csv
import hashlib
import json
import os
import queue
import re
import subprocess
import sys
import threading
import time
from datetime import datetime

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
WAYBACK_DOC = "https://web.archive.org/web/{ts}id_/{url}"
WAYBACK_REPLAY = "https://web.archive.org/web/{ts}/{url}"
CDX_ENDPOINT = "http://web.archive.org/cdx/search/cdx"

# Семейства URL буфера «Оперативной информации» (CDX канонизирует регистр пути и www-хост).
CDX_FAMILIES = [
    "www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/",
    "rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/",
]
CDX_FROM = "20160101"  # недельная оценка ИПЦ — с 01.01.2016 (приказ Росстата № 519 от 02.11.2015)

LIVE_URL = "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/{n}.htm"
LIVE_SLOTS = 260

SOURCE = "rosstat_ipc_weeks_bulletins"
GO_SOURCES_DEFAULT = "rosstat/ipc_weeks_bulletins.go"

# Файл недельных ИПЦ 2022+ и его поиск в медиабанке (режим xlsx).
XLSX_FILE = "https://rosstat.gov.ru/storage/mediabank/nedel_Ipc.xlsx"
XLSX_MEDIABANK_FAMILIES = [
    "rosstat.gov.ru/storage/mediabank/",
    "www.gks.ru/storage/mediabank/",
]

MONTHS = {"января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
          "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12}
TITLE_RE = re.compile(rb"<title[^>]*>(.*?)</title>", re.S | re.I)
# Окно чтения для классификации: <title> лежит в первых килобайтах, но у документов без <title>
# (Word-экспорт 2017–2019) видимый текст начинается только после блока стилей (~25–30 КБ),
# поэтому читается одно расширенное окно — заголовок или первая строка тела.
TITLE_READ_BYTES = 48000
# «с 3 по 9 августа 2021», «со 2 по 9 марта 2021», «с 26 октября по 1 ноября 2017»,
# а также опечатка Росстата «потребительских ценс 25 по 31 мая 2021» (буква «с» в «ценс»
# служит предлогом) — поэтому возможно и `с` без пробела перед днём.
RANGE_RE = re.compile(r"с[о]?\s+\d+\s+(?:[а-яё]+\s+)?по\s+(\d+)\s+([а-яё]+)\s+(\d{4})")
GO_DATE_RE = re.compile(r"Date:\s+time\.Date\((\d+),\s+time\.(\w+),\s+(\d+),")
GO_URL_RE = re.compile(r'URL:\s+"([^"]+)"')
GO_MONTHS = {"January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
             "July": 7, "August": 8, "September": 9, "October": 10, "November": 11,
             "December": 12}
MONTH_HEADER_RE = re.compile(r"на\s+(\d{1,2})\s+([а-яё]+)")

LOG_FH = None


def log(msg):
    line = "%s %s" % (datetime.now().strftime("%H:%M:%S"), msg)
    print(line, flush=True)
    if LOG_FH:
        LOG_FH.write(line + "\n")
        LOG_FH.flush()


def clean_title(title):
    """Заголовок для состояния и логов: управляющие символы (включая NUL — на такой строке падает
    csv-модуль, и файл состояния становится нечитаемым) и «;» старых сборов заменяются пробелом."""
    return re.sub(r"[\x00-\x1f;]+", " ", title)[:200]


def classify(title):
    """Вердикт по заголовку документа: week:ГГГГ-ММ-ДД | monthly | other.

    «Об оценке индекса потребительских цен в декабре 2020 года» — месячный релиз с глаголом
    «оценке» и без диапазона дат; диапазон «с … по …» — признак недельного выпуска (дата точки —
    конец недели). Заголовки «Об объеме производства нефтепродуктов и потребительских ценах…»
    намеренно не попадают в недельные: это другой релиз буфера (недельные цены на топливо).
    """
    t = " ".join(title.split())
    weekly_marker = ("Об оценке индекса потребительских цен" in t
                     or "потребительских ценс" in t)
    if weekly_marker:
        m = RANGE_RE.search(t)
        if m and MONTHS.get(m.group(2)):
            return "week:%04d-%02d-%02d" % (int(m.group(3)), MONTHS[m.group(2)], int(m.group(1)))
        if m or " по " in t:
            return "week:??"
        return "monthly"
    if "Об индексе потребительских цен" in t:
        return "monthly"
    return "other"


def decode_title(raw):
    """Декодирование заголовка: пробуем utf-8 и cp1251 и выбираем чтение, в котором узнаются
    слова русского заголовка. Иначе мохибейк (utf-8-страница, прочитанная как cp1251) увёл бы
    недельный выпуск в «other» мимо классификатора."""
    candidates = []
    for enc in ("utf-8", "cp1251"):
        try:
            candidates.append(raw.decode(enc))
        except UnicodeDecodeError:
            continue
    if not candidates:
        candidates = [raw.decode("cp1251", errors="ignore")]
    for text in candidates:
        if "потребительск" in text or "индекс" in text or "Об оценке" in text:
            return text
    return candidates[0]


def title_of(body):
    if not body:
        return ""
    m = TITLE_RE.search(body[:6000])
    if not m:
        return ""
    return decode_title(m.group(1))


def headline_of(body):
    """Заголовок документа для классификации: <title>, а если его нет — начало видимого текста.

    Часть выпусков буфера (Word-экспорт 2017–2019, напр. 57.htm, 77.htm, ind-zen24.htm) сохранена
    **без** <title> — разовые прогоны помечали их «err/no-title» и не классифицировали. У таких
    документов заголовок лежит первой строкой видимого текста, а <body> начинается после блока
    стилей (~25–30 КБ), поэтому читается расширенное окно и теги вырезаются грубо — этого
    достаточно для вердикта (недельный/месячный/чужой релиз). Пустой ответ сети (None) — пустой
    заголовок, а не исключение: провал чтения обрабатывает вызывающий (повтор или «err»)."""
    if not body:
        return ""
    title = title_of(body)
    if title:
        return title
    text = decode_title(body)
    idx = text.lower().find("<body")
    if idx >= 0:
        text = text[idx:]
    plain = " ".join(re.sub(r"<[^>]+>", " ", text).split())
    return plain[:300]


def basename(url):
    """Имя файла из URL — как Go path.Base по строке URL (сегмент после последнего «/»)."""
    return url.rsplit("/", 1)[-1]


def sanitize_name(name):
    """Имя артефакта в ключе архива — как archive.sanitizeName (key.go): управляющие символы и
    разделители пути/запрещённые в именах символы → «_», потолок 200 байт по границе руны."""
    out = []
    for ch in name:
        if ord(ch) < 32 or ord(ch) == 127 or ch in '/\\:*?"<>|':
            out.append("_")
        else:
            out.append(ch)
    clean = "".join(out)
    raw = clean.encode("utf-8")
    if len(raw) > 200:
        cut = 200
        while cut > 0 and (raw[cut] & 0xC0) == 0x80:  # utf8.RuneStart: не резать руну
            cut -= 1
        clean = raw[:cut].decode("utf-8")
    return clean or "file"


def archive_key(source, url, fingerprint, sha_hex):
    """Ключ версии — как archive.Key (key.go): «<источник>/<префикс отпечатка>_<имя>»;
    пустой отпечаток (HEAD недоступен) → префикс из sha256 содержимого."""
    if fingerprint:
        prefix = fingerprint[:16]
    else:
        prefix = "sha-" + sha_hex[:16]
    return "%s/%s_%s" % (source, prefix, sanitize_name(basename(url)))


def head_fingerprint_headers(status_headers, url):
    """md5 от «url|basename|ETag|Last-Modified|Content-Length» — композит util.HeadFingerprint.
    status_headers — список пар (имя, значение) финального ответа; None — HEAD не дал 2xx
    (или ошибка), отпечатка нет."""
    if status_headers is None:
        return ""
    etag = lm = cl = ""
    for name, value in status_headers:
        low = name.lower()
        if low == "etag":
            etag = value
        elif low == "last-modified":
            lm = value
        elif low == "content-length":
            cl = value
    composite = "%s|%s|%s|%s|%s" % (url, basename(url), etag, lm, cl)
    return hashlib.md5(composite.encode("utf-8")).hexdigest()


def known_weeks_from_go(path):
    """Даты недель из канонического списка источников импортёра (rosstat/ipc_weeks_bulletins.go):
    все `Date: time.Date(...)` и все `URL: "..."` — список источников и есть «что уже покрыто»."""
    text = open(path, encoding="utf-8").read()
    weeks = set()
    for y, mon, d in GO_DATE_RE.findall(text):
        if mon in GO_MONTHS:
            weeks.add("%04d-%02d-%02d" % (int(y), GO_MONTHS[mon], int(d)))
    urls = set(GO_URL_RE.findall(text))
    return weeks, urls


def registry_upsert(rows, entry):
    """Как archive.upsert (registry.go): нет строки с тем же ключом — дописать в конец; та же
    версия (совпал sha256) — без изменений (None); сменился sha256 — заменить и перенести в
    конец (файл упорядочен по first_seen)."""
    for i, old in enumerate(rows):
        if old.get("key") != entry["key"]:
            continue
        if old.get("sha256") == entry["sha256"]:
            return None
        return rows[:i] + rows[i + 1:] + [entry]
    return rows + [entry]


def inspect_xlsx(body):
    """Разбор книги недельного ИПЦ: имена листов, для листов-годов — самая ранняя дата шапки
    («на 10 января …»). Возвращает (sheets, earliest_dates, notes); книга читается openpyxl
    из памяти, на диск не распаковывается."""
    import io
    try:
        import openpyxl
    except ImportError:
        raise SystemExit("режим xlsx требует openpyxl (pip install openpyxl)")
    wb = openpyxl.load_workbook(io.BytesIO(body), read_only=True, data_only=True)
    sheets = list(wb.sheetnames)
    earliest = {}
    notes = []
    for name in sheets:
        year = int(name) if name.strip().isdigit() and 1990 < int(name) < 2100 else None
        ws = wb[name]
        found = None
        for row in ws.iter_rows(min_row=1, max_row=8, max_col=80, values_only=True):
            for cell in row:
                if not isinstance(cell, str):
                    continue
                m = MONTH_HEADER_RE.search(cell.replace("**", " "))
                if not m or m.group(2) not in MONTHS:
                    continue
                y = year
                ym = re.search(r"(20\d{2})", cell)
                if ym:
                    y = int(ym.group(1))
                if y is None:
                    notes.append("лист %s: дата «%s» без года" % (name, cell.strip()))
                    continue
                d = "%04d-%02d-%02d" % (y, MONTHS[m.group(2)], int(m.group(1)))
                found = d if found is None or d < found else found
        if found:
            earliest[name] = found
    wb.close()
    return sheets, earliest, notes


class Harvester:
    def __init__(self, args):
        self.state = args.state
        self.archive = args.archive
        self.threads = args.threads
        self.pace = args.pace
        self.dry_run = args.dry_run
        self.sources = [s for s in args.sources.split(",") if s]
        self.go_sources = args.go_sources
        os.makedirs(self.state, exist_ok=True)
        os.makedirs(self.archive, exist_ok=True)
        self.registry_path = os.path.join(self.archive, "registry.jsonl")
        self.lock = threading.Lock()
        self.titles = {}  # (ts, url) -> {"ts", "url", "digest", "kind", "title"}
        self.registry = self._read_registry()
        self.consec_fail = 0
        self.raw_down = False  # волна «found capture at …» на сыром плейбеке: реплей вместо него
        self.raw_fail = 0
        self.raw_probe = 0

    # --- низкий уровень: сеть ---

    def curl(self, extra, timeout=180):
        cmd = ["curl", "-4", "-s", "-m", "50"] + extra
        try:
            p = subprocess.run(cmd, capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            return None, "curl timeout"
        if p.returncode != 0:
            err = (p.stderr.decode(errors="ignore") or "curl rc=%d" % p.returncode).strip()
            return None, err[:200]
        return p.stdout, ""

    def _curl_capture(self, cmd, n):
        """curl с ранним обрывом чтения: прочитав n байт, процесс снимается (сервер отдаёт
        остальное впустую, но соединение рвётся сразу). -m 60 в cmd ограничивает зависание."""
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            data = p.stdout.read(n) if n else p.stdout.read()
        finally:
            p.kill()
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
            p.stdout.close()
            p.stderr.close()
        return data or b""

    def read(self, url, n=None, insecure=False, f=True, retries=3, what=""):
        """Первые n байт (или весь ответ) через curl: IPv4, редиректы ≤6 (Wayback иногда уходит
        в цикл «found capture at …» — с ограничением он падает быстро), дедлайн 60 с. Пустой
        результат — ошибка с паузами и повторами: Wayback отдаёт сбои волнами."""
        for attempt in range(1, retries + 1):
            cmd = ["curl", "-4", "-s", "-L", "--max-redirs", "6", "-m", "60"]
            if f:
                cmd.append("-f")
            if insecure:
                cmd.append("-k")
            cmd += ["-A", UA, url]
            data = self._curl_capture(cmd, n)
            if data:
                return data
            if attempt == retries:
                log("FAIL %s %s" % (what, url))
                return None
            time.sleep(20 * attempt if n is not None else 30 * attempt)
        return None

    def head_headers(self, url):
        """Заголовки финального ответа HEAD (следуя редиректам): список пар или None."""
        body, err = self.curl(["-I", "-L", "--max-redirs", "6", "-m", "40", url])
        if body is None:
            log("HEAD не удался %s: %s" % (url, err))
            return None
        lines = body.decode("latin-1").splitlines()
        start = None
        for i, line in enumerate(lines):
            if line.startswith("HTTP/"):
                start = i
        if start is None:
            return None
        status = int(lines[start].split()[1])
        if status < 200 or status >= 300:
            log("HEAD %s: статус %d" % (url, status))
            return None
        headers = []
        for line in lines[start + 1:]:
            if not line.strip():
                break
            if ":" in line:
                name, value = line.split(":", 1)
                headers.append((name.strip(), value.strip()))
        return headers

    def fetch_title_snapshot(self, ts, url):
        """Заголовок снимка: сначала id_ (сырые байты), иначе обычный реплей (страница с панелью
        Wayback — заголовок документа в ней тот же). Читается окно TITLE_READ_BYTES, соединение
        рвётся; заголовок берётся из <title> либо из начала видимого текста (headline_of).
        Когда сырой плейбек лежит волной (циклы «found capture at …»), каждая неудачная попытка
        стоит 15–20 с — поэтому после 10 провалов подряд id_ пропускается, и только каждая 25-я
        проба проверяет, не ожил ли он: заголовки собираются реплеем (~2 с), волна пережидается."""
        with self.lock:
            check_raw = not self.raw_down or self.raw_probe % 25 == 0
            self.raw_probe += 1
        if check_raw:
            body = self.read(WAYBACK_DOC.format(ts=ts, url=url), n=TITLE_READ_BYTES, retries=1,
                             what="title")
            title = headline_of(body)
            if title:
                with self.lock:
                    self.raw_down, self.raw_fail = False, 0
                return title
            with self.lock:
                self.raw_fail += 1
                if self.raw_fail >= 10 and not self.raw_down:
                    self.raw_down = True
                    log("сырой плейбек Wayback недоступен (10 подряд) — реплей; проба id_ каждые 25")
        body = self.read(WAYBACK_REPLAY.format(ts=ts, url=url), n=TITLE_READ_BYTES, retries=1,
                         what="title-replay")
        return headline_of(body)

    # --- CDX ---

    def cdx(self, url, prefix=False, collapse=True, status_only=True, limit=100000,
            title_filter=None, frm=CDX_FROM):
        params = ["fl=timestamp,original,statuscode,mimetype,digest", "limit=%d" % limit]
        if prefix:
            params.append("matchType=prefix")
        if collapse:
            params.append("collapse=digest")
        if status_only:
            params.append("filter=statuscode:200")
        if title_filter:
            params.append("filter=" + title_filter)
        if frm:
            params.append("from=" + frm)
        query = CDX_ENDPOINT + "?url=" + url + "&" + "&".join(params)
        for attempt in (1, 2, 3):
            body, err = self.curl([query], timeout=180)
            if body is not None:
                rows = []
                for line in body.decode("utf-8", errors="replace").splitlines():
                    parts = line.split()
                    if len(parts) >= 2:
                        rows.append(parts)
                return rows
            log("CDX не ответил (%s), попытка %d: %s" % (url, attempt, err))
            time.sleep(10 * attempt)
        return None

    def phase_cdx(self):
        """Список снимков буфера: CDX по семействам → snapshots.tsv (только .htm-документы)."""
        all_rows = {}
        for family in CDX_FAMILIES:
            rows = self.cdx(family, prefix=True)
            if rows is None:
                log("CDX семейства %s не получен — пропуск" % family)
                continue
            slug = re.sub(r"[^A-Za-z0-9]+", "_", family)
            path = os.path.join(self.state, "cdx_%s.tsv" % slug)
            with open(path, "w", encoding="utf-8") as fh:
                for parts in rows:
                    fh.write("\t".join(parts) + "\n")
            for parts in rows:
                all_rows[(parts[0], parts[1])] = parts
            log("CDX %s: %d снимков" % (family, len(rows)))
        snapshots = []
        for (ts, url), parts in sorted(all_rows.items()):
            digest = parts[4] if len(parts) > 4 else ""
            if not url.lower().endswith(".htm"):
                continue  # вложенные ассеты Word-экспорта (*.files/*) — не документы
            snapshots.append((ts, url, digest))
        with open(os.path.join(self.state, "snapshots.tsv"), "w", encoding="utf-8") as fh:
            for ts, url, digest in snapshots:
                fh.write("%s\t%s\t%s\n" % (ts, url, digest))
        log("снимков .htm после фильтра: %d" % len(snapshots))
        return snapshots

    # --- классификация заголовков ---

    def load_titles(self):
        path = os.path.join(self.state, "titles.csv")
        if not os.path.exists(path):
            return
        with open(path, encoding="utf-8") as fh:
            # NUL вычищается до разбора: csv-модуль на такой строке падает с «line contains NUL»
            # (в заголовок Word-документа мог попасть нулевой байт), и состояние осталось бы
            # нечитаемым для всех последующих прогонов.
            content = fh.read().replace("\x00", "")
        try:
            for parts in csv.reader(content.splitlines(True)):
                if len(parts) < 5 or parts[0] == "ts":
                    continue
                ts, url, digest, kind, title = parts[:5]
                self.titles[(ts, url)] = {"ts": ts, "url": url, "digest": digest,
                                          "kind": kind, "title": title}
        except csv.Error as e:
            # Состояние — кэш классификаций: разобрать часть и продолжить лучше, чем упасть на
            # одной строке (неразобранные пары просто классифицируются заново).
            log("состояние: битая строка в titles.csv (%s) — читаю до неё" % e)
        log("состояние: классифицировано ранее %d пар" % len(self.titles))

    def save_titles(self):
        path = os.path.join(self.state, "titles.csv")
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["ts", "url", "digest", "kind", "title"])
            w.writeheader()
            for row in self.titles.values():
                w.writerow(row)
        os.replace(tmp, path)

    def pending_rows(self, rows):
        """Что осталось классифицировать: новые пары плюс повторы err/week:?? (сбой сети или
        неразобранный заголовок — их повторяем, а не считаем вердиктом)."""
        done_kinds = {"other", "monthly"}
        pending = []
        for row in rows:
            old = self.titles.get((row[0], row[1]))
            if not old:
                pending.append(row)
                continue
            kind = old["kind"]
            if kind in done_kinds or (kind.startswith("week:") and kind != "week:??"):
                continue
            pending.append(row)
        return pending

    def phase_titles(self, rows):
        pending = self.pending_rows(rows)
        log("к классификации: %d (всего снимков %d)" % (len(pending), len(rows)))
        q = queue.Queue()
        for row in pending:
            q.put(row)
        stats = {"done": 0, "total": len(pending)}

        def worker():
            while True:
                try:
                    row = q.get_nowait()
                except queue.Empty:
                    return
                ts, url = row[0], row[1]
                digest = row[2] if len(row) > 2 else ""
                title = ""
                try:
                    for attempt in (1, 2):
                        title = self.fetch_title_snapshot(ts, url)
                        if title:
                            break
                        time.sleep(20 * attempt)
                except Exception as e:  # noqa: BLE001 — один снимок не должен убивать поток
                    # Поток с исключением умирает молча, и очередь снимков (сотни пар) остаётся
                    # неразобранной — прогон выглядит завершённым, ничего не сделав. Поэтому сбой
                    # одного снимка — это «err» этого снимка, а не смерть воркера.
                    log("исключение на снимке %s %s: %s" % (ts, url, str(e)[:160]))
                    title = ""
                kind = classify(title) if title else "err"
                with self.lock:
                    self.titles[(ts, url)] = {"ts": ts, "url": url, "digest": digest,
                                              "kind": kind,
                                              "title": clean_title(title)}
                    stats["done"] += 1
                    if title:
                        self.consec_fail = 0
                    else:
                        self.consec_fail += 1
                        if self.consec_fail >= 25:
                            log("Wayback недоступен: 25 пустых ответов подряд — пауза 240 с")
                            time.sleep(240)
                            self.consec_fail = 0
                    if stats["done"] % 25 == 0:
                        self.save_titles()
                        log("заголовки: %d/%d" % (stats["done"], stats["total"]))
                time.sleep(self.pace)

        threads = [threading.Thread(target=worker, daemon=True) for _ in range(self.threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.save_titles()
        log("заголовки: готово %d/%d" % (stats["done"], stats["total"]))

    def phase_live(self):
        """Живой буфер: все слоты d02/{1..260}.htm проверяются заново каждый прогон (слот мог
        ожить или отдать новый выпуск; снимков-заглушек не боимся — они классифицируются как
        other). Флагов curl, режущих тело на HTTP-ошибке, здесь нет: заглушка приходит как 502
        со страницей, и её содержимое — ответ, а не сбой."""
        log("живой буфер: слоты 1..%d" % LIVE_SLOTS)
        fails = 0
        new_rows = []
        for n in range(1, LIVE_SLOTS + 1):
            url = LIVE_URL.format(n=n)
            body = self.read(url, n=3500, insecure=True, f=False, retries=1, what="live")
            if body is None:
                fails += 1
                new_rows.append((("live", url), {"ts": "live", "url": url, "digest": "",
                                                 "kind": "err", "title": "fetch-failed"}))
                if fails >= 10:
                    log("живой буфер недоступен: 10 слотов подряд без ответа — фаза прервана")
                    break
                continue
            fails = 0
            title = title_of(body)
            new_rows.append((("live", url), {"ts": "live", "url": url, "digest": "",
                                             "kind": classify(title), "title": clean_title(title)}))
            time.sleep(self.pace)
        with self.lock:
            for key, row in new_rows:
                self.titles[key] = row
            self.save_titles()
        log("живой буфер: проверено %d слотов" % len(new_rows))

    # --- дыры и загрузка ---

    def phase_gaps(self, known):
        found = {}
        for row in self.titles.values():
            if row["kind"].startswith("week:") and row["kind"] != "week:??":
                found.setdefault(row["kind"][5:], []).append(row)
        holes = sorted(w for w in found if w not in known and w <= "2021-12-31")
        late = sorted(w for w in found if w not in known and w > "2021-12-31")
        log("недель найдено: %d (из них новых до 2022: %d; после 2022 — покрыты XLSX: %d)"
            % (len(found), len(holes), len(late)))
        if late:
            log("после 2022 (не трогаем, XLSX первичен): %s" % ", ".join(late))
        return holes, found

    def archive_upsert(self, url, body):
        """Документ в архив: sha256 содержимого → отпечаток HEAD → объект → строка реестра.
        Возвращает строку реестра или None, если запись уже есть без изменений."""
        sha = hashlib.sha256(body).hexdigest()
        headers = self.head_headers(url)
        fingerprint = head_fingerprint_headers(headers, url) if headers is not None else ""
        key = archive_key(SOURCE, url, fingerprint, sha)
        entry = {"source": SOURCE, "name": basename(url), "fingerprint": fingerprint,
                 "sha256": sha, "size": len(body), "url": url, "key": key,
                 "first_seen": datetime.now().astimezone().isoformat(timespec="microseconds")}
        for old in self.registry:
            if old.get("key") == key and old.get("sha256") == sha:
                log("версия уже в реестре без изменений: %s" % key)
                return None
        if self.dry_run:
            log("dry-run: %s было бы заархивировано (%d байт)" % (key, len(body)))
            return entry
        obj_path = os.path.join(self.archive, key.replace("/", os.sep))
        os.makedirs(os.path.dirname(obj_path), exist_ok=True)
        tmp = obj_path + ".tmp"
        with open(tmp, "wb") as fh:
            fh.write(body)
        os.chmod(tmp, 0o644)
        os.replace(tmp, obj_path)
        next_rows = registry_upsert(self.registry, entry)
        if next_rows is None:  # гонка исключена, но контракт upsert возвращает None — не запись
            return None
        self.registry = next_rows
        tmp_reg = self.registry_path + ".tmp"
        with open(tmp_reg, "w", encoding="utf-8") as fh:
            for row in self.registry:
                fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        os.replace(tmp_reg, self.registry_path)
        return entry

    def phase_fetch(self, holes, found):
        """Кандидаты по каждой дыре: живой документ (если есть) приоритетнее, дальше — снимки по
        возрастанию ts (самый ранний ближе всего к публикации). Скачивается полный документ,
        заголовок проверяется повторно."""
        candidates_path = os.path.join(self.state, "candidates.tsv")
        findings_path = os.path.join(self.state, "findings.tsv")
        added = 0
        with open(candidates_path, "a", encoding="utf-8") as cand, \
                open(findings_path, "a", encoding="utf-8") as find:
            for week in holes:
                rows = sorted(found[week], key=lambda r: (r["ts"] != "live", r["ts"]))
                for row in rows:
                    ts, url = row["ts"], row["url"]
                    doc_url = url if ts == "live" else WAYBACK_DOC.format(ts=ts, url=url)
                    body = self.read(doc_url, insecure=(ts == "live"), retries=4, what="doc")
                    if body is None:
                        continue
                    if classify(title_of(body)) != "week:" + week:
                        log("заголовок не подтвердил неделю %s: %s" % (week, doc_url))
                        continue
                    entry = self.archive_upsert(doc_url, body)
                    if entry is None:
                        log("неделя %s уже в архиве: %s" % (week, doc_url))
                        break
                    cand.write("%s\t%s\n" % (week, doc_url))
                    cand.flush()
                    find.write("%s\t%s\t%s\t%d\t%s\n" % (week, doc_url, entry["sha256"],
                                                         entry["size"], entry["key"]))
                    find.flush()
                    added += 1
                    log("новый выпуск %s: %s (%d байт) → %s"
                        % (week, basename(doc_url), len(body), entry["key"]))
                    break  # неделя закрыта первым же подтверждённым документом
        log("заархивировано новых выпусков: %d (кандидаты — %s)" % (added, candidates_path))
        return added

    def _read_registry(self):
        rows = []
        if os.path.exists(self.registry_path):
            with open(self.registry_path, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        rows.append(json.loads(line))
        return rows

    def run_weeks(self):
        known, known_urls = known_weeks_from_go(self.go_sources)
        log("канонический список источников: %d недель, %d URL" % (len(known), len(known_urls)))
        self.load_titles()
        snapshots = self.phase_cdx()
        self.phase_titles([(ts, url, digest) for ts, url, digest in snapshots])
        if "live" in self.sources:
            self.phase_live()
        holes, found = self.phase_gaps(known)
        self.phase_fetch(holes, found)
        objects = sum(len(files) for _, _, files in os.walk(os.path.join(self.archive, SOURCE)))
        log("итог: строк реестра %d, объектов в архиве %d" % (len(self.registry), objects))

    # --- режим xlsx ---

    def phase_xlsx(self):
        rows = []
        for family, prefix in ((XLSX_FILE, False),) + tuple((f, True) for f in
                                                            XLSX_MEDIABANK_FAMILIES):
            title_filter = "urlkey:.*nedel.*" if prefix else None
            got = self.cdx(family, prefix=prefix, collapse=False, status_only=False,
                           limit=5000, title_filter=title_filter, frm=None)
            if got is None:
                log("CDX не получен для %s" % family)
                continue
            for parts in got:
                ts, url, status = parts[0], parts[1], parts[2]
                rows.append((ts, url, status, parts[4] if len(parts) > 4 else ""))
        seen = {}
        for ts, url, status, digest in rows:
            seen[(ts, url)] = (ts, url, status, digest)
        report_path = os.path.join(self.state, "xlsx_report.tsv")
        earliest_any = None
        seen_digests = set()
        with open(report_path, "w", encoding="utf-8") as fh:
            fh.write("ts\turl\tstatus\tsize\tsheets\tearliest\tverdict\n")
            for ts, url, status, digest in sorted(seen.values()):
                if status == "-":
                    fh.write("%s\t%s\t-\t\t\t\twarc/revisit (дубль содержимого)\n" % (ts, url))
                    continue
                if status != "200":
                    fh.write("%s\t%s\t%s\t\t\t\tне 200 (редирект/ошибка)\n" % (ts, url, status))
                    continue
                if digest and digest in seen_digests:
                    continue  # то же содержимое уже проверено под другим снимком/регистром
                seen_digests.add(digest)
                body = self.read(WAYBACK_DOC.format(ts=ts, url=url), retries=1, what="xlsx")
                if not body or body[:2] != b"PK":
                    # Сырой плейбек лежит волной — для бинарника реплей отдаёт тот же файл
                    body = self.read(WAYBACK_REPLAY.format(ts=ts, url=url), retries=3,
                                     what="xlsx-replay") or body
                if body is None:
                    fh.write("%s\t%s\t200\t\t\t\tскачать не удалось\n" % (ts, url))
                    continue
                if body[:2] != b"PK":
                    fh.write("%s\t%s\t200\t%d\t\t\tне xlsx (первые байты: %r)\n"
                             % (ts, url, len(body), body[:16]))
                    continue
                try:
                    sheets, earliest, _ = inspect_xlsx(body)
                except Exception as e:  # noqa: BLE001 — отчёт о любом сбое, не падение прогона
                    fh.write("%s\t%s\t200\t%d\t\t\tразбор не удался: %s\n"
                             % (ts, url, len(body), str(e)[:120]))
                    continue
                early = min(earliest.values()) if earliest else ""
                verdict = ("данные 2021 и раньше ЕСТЬ" if early and early < "2022-01-01"
                           else "только 2022+")
                if early and (earliest_any is None or early < earliest_any):
                    earliest_any = early
                fh.write("%s\t%s\t200\t%d\t%s\t%s\t%s\n"
                         % (ts, url, len(body), ",".join(sheets), early, verdict))
                fh.flush()
                log("xlsx %s %s: листы %s, самые ранние данные %s — %s"
                    % (ts, url, sheets, early or "?", verdict))
                time.sleep(0.5)
        log("xlsx: отчёт %s" % report_path)
        log("xlsx: самая ранняя табличная дата во всех снапшотах: %s" % (earliest_any or "не найдено"))


def main():
    global LOG_FH
    ap = argparse.ArgumentParser(description="Сбор недельных ИПЦ Росстата до 2022 из Wayback и живого буфера")
    ap.add_argument("--mode", choices=["weeks", "xlsx"], default="weeks")
    ap.add_argument("--state", default="build/harvest", help="каталог состояния (gitignored)")
    ap.add_argument("--archive", default="build/archive-dev", help="корень локального винтажного архива")
    ap.add_argument("--sources", default="wayback,live", help="источники поиска недель: wayback,live")
    ap.add_argument("--go-sources", default=GO_SOURCES_DEFAULT,
                    help="канонический список недель импортёра (Go)")
    ap.add_argument("--threads", type=int, default=2)
    ap.add_argument("--pace", type=float, default=0.45, help="пауза потока после запроса, с")
    ap.add_argument("--dry-run", action="store_true", help="только поиск, без загрузки документов")
    args = ap.parse_args()

    os.makedirs(args.state, exist_ok=True)
    LOG_FH = open(os.path.join(args.state, "run.log"), "a", encoding="utf-8")
    log("=== harvest.py %s: state=%s archive=%s sources=%s%s"
        % (args.mode, args.state, args.archive, args.sources,
           " DRY-RUN" if args.dry_run else ""))
    h = Harvester(args)
    if args.mode == "weeks":
        h.run_weeks()
    else:
        h.phase_xlsx()
    log("=== готово")


if __name__ == "__main__":
    sys.exit(main())
