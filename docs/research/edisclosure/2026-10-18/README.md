# e-disclosure — сохранённые артефакты пилота (2026-10-18)

**Источник:** `build/edisclosure-probe/` (gitignored, локальная директория пилота).
**Назначение:** сохранить в репозитории небольшие публичные артефакты исследования, чтобы [research-отчёт](../../2026-10-18-edisclosure-research.md) был воспроизводим без доступа к `build/`.

## Что лежит здесь

| Файл | Что это | Размер |
|---|---|---:|
| `pilot-report.md` | исходный отчёт пилота: антибот, данные, окупаемость, gate | 20 КБ |
| `accuracy-report.md` | проверка точности NAV, мост shares/net debt | 10 КБ |
| `facts.csv` | официальные факты МСФО 2025 (shares, split, net debt) | 2,6 КБ |
| `baseline.json` | baseline и три дека/контура NAV | 1,6 КБ |
| `event-summary.csv` | категории событий по эмитентам | 451 Б |
| `companies.jsonl` | карточки эмитентов (тикер, id, имя, URL) | 4,3 КБ |
| `documents-full.csv` | полный инвентарь 290 документов | 72 КБ |
| `documents-curated.csv` | выборочные 20 документов с заметками | 4,6 КБ |
| `events-curated.csv` | выборочные события с `relevance`/заметками | 11 КБ |
| `antibot-run.log` | протокол антибот-проверок | 3,4 КБ |
| `EVENTS_VINTAGE.md` | схема event-версий (API/FTP/опрос вперёд) | 3,8 КБ |
| `pilot-archive-README.md` | формат и ограничения пилотного архива | 4,6 КБ |

Все файлы — публичные метаданные/отчёты, без cookies, токенов и персональных данных.

## Что сознательно не коммитится

| Что | Почему | Где остаётся |
|---|---|---|
| `archive/files/` (20 PDF/ZIP, 52 МБ) | бинарные документы; все 20 fileid входят в production-набор 290 | локальный `build/edisclosure-probe/archive/files/`; полный набор — в винтажном архиве |
| `raw/` (8,8 МБ HTML) | сырые страницы; фикстура `PLZL_IFRS.html` уже в `scripts/testdata/edisclosure/` | `build/edisclosure-probe/raw/` |
| `parsed/events.jsonl` (2,1 МБ), `event_snapshot.jsonl` (3 МБ), `parsed/events.csv` (1,6 МБ) | производные данные событий; пересобираются коллектором | `build/edisclosure-probe/parsed/` |
| `chrome-profile/`, `state.json`, `.cookies.json`, `session_meta.json` | секреты/сессия; в репозиторий нельзя | `build/edisclosure-probe/` (gitignored) |
| `scraper.py`, `parser.py`, `snapshot.py`, `add_docs.py`, `build_archive.py`, `probe.py`, `debug_captcha.py` | пилотные скрипты; production-коллектор и импортёр их заменили (или это scratch) | `build/edisclosure-probe/` |
| `verify_registry.go.txt` | scratch-проверка `archive.Entry` | `build/edisclosure-probe/` |

## Архив на Яндекс.Диске: что уже есть

Проверка `make archive-list` (2026-10-18, `ARCHIVE_BACKEND=yandex` из env-файла):

| Source | Строк в реестре | Что это |
|---|---:|---|
| `edisclosure_documents` | **18 из 593** | частичный набор из остановленного первого прогона (yandex); все 18 fileid входят в production-290 |
| `edisclosure` (пилот) | **0** | пилотные 20 версий не загружались; все 20 fileid — подмножество production-290 |

Полный локальный production-архив: **290 объектов + `registry.jsonl`, 414 МБ** (`build/edisclosure-archive/`). В Яндекс.Диске его пока нет — там только 18 объектов.

## Что можно ещё положить в архив Яндекс.Диска

1. **Полный production-архив `edisclosure_documents` (290 объектов, 414 МБ).** Это главное: локальный архив сейчас единственный полный; на Диске — 18/290.
   - Штатный путь: `ARCHIVE_BACKEND=yandex EDISCLOSURE_RECHECK=1 make import STAT=edisclosure_documents`. Прогон заново скачает 290 документов и загрузит их в Яндекс.Диск; строки таблицы не изменятся (`needsWrite=false` на recheck при том же `sha256`), реестр дополнится до 290.
   - Альтернатива без повторного скачивания: небольшим Go/Python-скриптом прочитать `build/edisclosure-archive/registry.jsonl` и объекты, вызвать `store.Put` + `Registry.Ensure` для каждого отсутствующего ключа. Штатной цели Makefile для этого нет.
2. **Пилотные 20 версий — отдельно не нужны.** Все 20 fileid входят в production-290; после полной загрузки они будут в архиве под source `edisclosure_documents`. Отдельный source `edisclosure` добавил бы дубликаты.
3. **Research-артефакты — не в `registry.jsonl`.** Отчёты, CSV и event-снимки не являются версиями файлов источника. Если нужна долговременная копия, положить один zip в Яндекс.Диск под отдельным префиксом (например, `disk:/rosstat-archive/research/edisclosure-2026-10-18.zip`), но не в архивный реестр.
4. **Сырые HTML/event-снимки — опционально.** `raw/` + `parsed/events.jsonl` + `event_snapshot.jsonl` можно заархивировать в тот же research-zip, если нужна побайтовая воспроизводимость; в git они не идут.

## Почему граница именно такая

- В репозиторий — только небольшие публичные метаданные и отчёты, которые нужны для аудита исследования и воспроизводимости выводов.
- В винтажный архив — версии файлов источника (документы), а не исследовательские выгрузки. Полный набор — 290 документов; пилотные 20 — его подмножество.
- Секреты (профиль Chrome, cookies, state) не покидают локальную машину.
