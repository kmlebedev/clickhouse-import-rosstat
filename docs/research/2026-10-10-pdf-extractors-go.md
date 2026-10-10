# Замена `pdftotext -tsv` для колоночной модели `polyus/` — исследование (2026-10-10)

Контекст: docs/superpowers/specs/2026-10-10-polyus-column-model-design.md (§3 «pdftotext остаётся»).

## Вывод
Строго лучшей замены нет. `pdftotext -tsv` — единственный источник, который отдаёт готовые слова (left/top/width/height + block/line). Для спеки решение «pdftotext остаётся» верное. Нативный Go-источник имеет смысл позже, за интерфейсом `WordSource`; главный выигрыш — CI без poppler.

## Кандидаты
| Кандидат | Лицензия | cgo/бинарь | Единица текста | Замечания |
|---|---|---|---|---|
| poppler `pdftotext -tsv` | GPL (внешний бинарь) | бинарь | слово + bbox, block/line | эталон; «1 287» → два слова |
| klippa-app/go-pdfium (WASM, Wazero) | MIT + BSD/Apache | нет | char или rect (однострочный прогон с одним шрифтом) | `GetPageTextStructured`, ModeRects/Chars. v1.21 требует Go 1.26, v1.17.2 собирается на 1.24. Init ~1.7 с, страница ~4 мс. Bbox по контуру глифов: Top/Bottom у слов одной строки различаются на ~1–2 pt, точного совпадения Top нет. Встречено дублирование «To»+«Total…». FontSize иногда 0 |
| razvandimescu/gopdf | MIT, только stdlib | нет | TextSpan (X, Y-базовая линия, EndX, FontSize) | молодой (с 2026-03, ~15★, активен). Фрагменты режут слова («Совоку»+«пные»). Авто-`Tables()` на шапке Полюса ошибается |
| hallelx2/pdftable (модуль pdfgrab) | MIT, pdfcpu | нет | `Page.Words(WordOpts)` — слова с bbox | порт pdfplumber. Требует Go 1.25+, не собран в тесте. Автор признаёт дрейф X до ~10 pt |
| unidoc/unipdf v4 | коммерческая | нет | TextMark + BBox, `PageText.Tables()` | лицензия |
| sassoftware/pdf-xtract (форк ledongthuc) | BSD-3 | нет | Text X,Y,W, без высоты | беднее |
| go-fitz (MuPDF) | AGPL | cgo | — | лицензия |
| seehuhn.de/go/pdf | GPL-3 | нет | — | лицензия |
| Docling/Camelot/MinerU | Python | — | детекция таблиц | бенчмарки на научных/линованных PDF; для известной вёрстки избыточно |

## Тест на синтетике (копия шапок 1H2026 RU и FY2014; LibreOffice=TrueType+ToUnicode, Chromium=Type0 Identity-H)
- Кириллица корректна у pdftotext, gopdf и go-pdfium в обоих вариантах.
- Числа с пробелом-разделителем: gopdf и go-pdfium(rects) отдают «1 287» одним фрагментом, pdftotext — двумя словами.
- Сноска «4» отделяется по кеглю у всех трёх.
- На реальных PDF Полюса не проверено: polyus.com недоступен из сессии.

## Рекомендуемая архитектура
`WordSource` → `[]Word` (как в спеке §4.1). Бэкенды: `pdftotext` (сейчас, эталон) и нативный (go-pdfium WASM или gopdf) со своей склейкой фрагментов в слова. Голден-сверка двух бэкендов по TSV-фикстурам. `groupByLine` для нативных бэкендов с `deltaTop > 0`.