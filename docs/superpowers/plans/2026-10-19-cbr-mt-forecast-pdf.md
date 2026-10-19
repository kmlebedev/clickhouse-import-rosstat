# PDF-выпуски прогноза ЦБ (`cbr_mt_forecast`) — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Импортировать в `cbr_mt_forecast` 9 PDF-комментариев (2024-02 … 2026-02) и 11 PDF «Среднесрочный прогноз» через `pdftotext`, не меняя схему таблицы.

**Architecture:** Расширяется существующий импортёр `cbr_mt_forecast`: индекс читает два типа карточек (комментарий и прогноз), цикл импорта выбирает транспорт (страница+xlsx или PDF), PDF-ветка использует `BodyFunc` шаблона (байты качает и архивирует шаблон), разбор — чистая функция по тексту `pdftotext -layout`.

**Tech Stack:** Go 1.27, clickhouse-go/v2, logrus, excelize (как есть), внешняя утилита `pdftotext` (Poppler) — только в прод-пути, тесты офлайн на текстовых фикстурах.

**Spec:** `docs/superpowers/specs/2026-10-19-cbr-mt-forecast-pdf-design.md`

## Global Constraints

- Схема `cbr_mt_forecast`, ключ `(indicator, year, release_date)`, витрина `v_cbr_mt_forecast`, каталог 16 рядов, реестр `cbr/importers.go`, `dagu/cbr.yaml`, гранты — **не меняются**.
- Новый импортёр/таблица/`Name()` не заводятся; `Name() == TableName == "cbr_mt_forecast"`.
- Сеть — только через шаблон `util.ClickHouseImport` (`DataUrl` + `BodyFunc`); `exec.CommandContext` — только для локального `pdftotext`.
- Вставка — один `PrepareBatch` на выпуск (через шаблон), `batch.Append` с проверкой ошибки.
- В `cbr/` не добавляются `colly` и `fmt.Print*`; логирование — `log.Infof/Warnf`.
- Тесты офлайн: `pdftotext` в тестах не вызывается; фикстуры — дословный текст из реальных PDF.
- Ошибки не проглатываются; `panic` в разборе запрещён.
- Модели субагентов: код — `deepseek/deepseek-flash`; док-правки — `joingonka/zai-org/GLM-5.3-Flash`; ревью — `deepseek/deepseek-flash` (решение владельца 2026-10-19: `anthropic/haiku` недоступен).

## Review Focus

1. **Разрезанный диапазон.** `13,5 – 14,5¹` в `-layout` — три «колонки»; после склейки строка обязана иметь ровно `1 + len(years)` значений. Неверная склейка не должна молча сдвигать годы.
2. **Сноска, приклеенная к значению.** `15,51` = 15,5 + сноска 1; `19,21` = 19,2 + 1. Существующий `mtForecastFootnoteRe` снимает последнюю цифру только у ровно двузначной дроби — spot-тесты фиксируют оба случая.
3. **Forecast-карточка без комментария.** Отсутствие комментариев — ошибка; отсутствие прогнозов — `warn`, комментарии импортируются.
4. **Дата страницы против даты карточки.** Расхождение слага `comment_<ДДММГГГГ>/` и даты карточки — ошибка (второй винтаж одного выпуска).
5. **Подмена файла.** HTML/WAF вместо PDF (`200 text/html`) — ошибка до `exec` с именем выпуска; отсутствующий `pdftotext` — ошибка с подсказкой `poppler-utils`.

---

## Структура файлов

| Файл | Ответственность |
|---|---|
| `cbr/mt_forecast_pdf.go` (создать) | извлечение `pdftotext`, проверка `%PDF-`, нормализация строки, поиск якоря/годов, разбор строк, `BodyFunc` выпуска |
| `cbr/mt_forecast_pdf_test.go` (создать) | корпус 20 фикстур, fail-fast, warn-ветки, извлечение без `pdftotext` |
| `cbr/mt_forecast.go` (править) | `Kind` у выпуска, разбор двух типов карточек, счётчики, гейт, ветка транспорта в `Import` |
| `cbr/mt_forecast_parse.go` (править) | `mtForecastNormalizeLabel`: надстрочные цифры + хвостовое двоеточие |
| `cbr/mt_forecast_test.go` (править) | ожидания индекса/гейта/счётчиков |
| `cbr/mt_forecast_parse_test.go` (править) | тест PDF-формы подписи |
| `cbr/testdata/mt_forecast_pdf/*.txt` (создать) | фикстуры: 9 комментариев + 11 прогнозов |

## Task 1: Корпус фикстур и разбор PDF «Табл. 1»

**Files:**
- Create: `cbr/testdata/mt_forecast_pdf/` (20 файлов)
- Create: `cbr/mt_forecast_pdf.go`
- Create: `cbr/mt_forecast_pdf_test.go`
- Modify: `cbr/mt_forecast_parse.go` (`mtForecastNormalizeLabel`)

**Interfaces:**
- Consumes: `mtForecastPoint{Indicator, Year, Lo, Hi}`, `mtForecastIndicators`, `mtForecastIndicatorByLabel`, `mtForecastMissing`, `mtForecastBounds`, `mtForecastNormalizeCell`, `mtForecastStripSuperscripts`, `mtForecastYear`, `mtForecastHeaderYears`, `mtForecastTable` (все уже есть в `cbr/`).
- Produces:
  - `func mtForecastParsePDF(text []byte) ([]mtForecastPoint, error)` — чистая функция.
  - `const mtForecastPDFAnchor = "ОСНОВНЫЕ ПАРАМЕТРЫ ПРОГНОЗА БАНКА РОССИИ"`, `const mtForecastPDFStop = "Источник: Банк России"`.
  - `var mtForecastPDFJoinRangesRe = regexp.MustCompile(`([\d)])\s+[–—]\s+(\(?-?\d)`)`.

- [ ] **Step 1: Снять фикстуры**

Команды (N = 2 для комментариев, 1 для прогнозов; для `27022024` — страницы 1–2):

```bash
mkdir -p cbr/testdata/mt_forecast_pdf
base=https://www.cbr.ru/Content/Document/File
curl -sS $base/159282/comment_27022024.pdf | pdftotext -f 1 -l 2 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/comment_27022024_p1p2.txt
curl -sS $base/160782/comment_13052024.pdf | pdftotext -f 2 -l 2 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/comment_13052024.txt
curl -sS $base/162516/comment_07082024.pdf | pdftotext -f 2 -l 2 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/comment_07082024.txt
curl -sS $base/166590/comment_06112024.pdf | pdftotext -f 2 -l 2 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/comment_06112024.txt
curl -sS $base/172537/comment_26022025.pdf | pdftotext -f 2 -l 2 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/comment_26022025.txt
curl -sS $base/175398/comment_12052025.pdf | pdftotext -f 2 -l 2 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/comment_12052025.txt
curl -sS $base/179983/comment_06082025.pdf | pdftotext -f 2 -l 2 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/comment_06082025.txt
curl -sS $base/184552/comment_06112025.pdf | pdftotext -f 2 -l 2 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/comment_06112025.txt
curl -sS $base/187047/comment_26022026.pdf | pdftotext -f 2 -l 2 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/comment_26022026.txt
curl -sS $base/161610/forecast_240216.pdf | pdftotext -f 1 -l 1 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/forecast_240216.txt
curl -sS $base/160781/forecast_240426.pdf | pdftotext -f 1 -l 1 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/forecast_240426.txt
curl -sS $base/162284/forecast_240726.pdf | pdftotext -f 1 -l 1 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/forecast_240726.txt
curl -sS $base/166586/forecast_241025.pdf | pdftotext -f 1 -l 1 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/forecast_241025.txt
curl -sS $base/172536/forecast_250214.pdf | pdftotext -f 1 -l 1 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/forecast_250214.txt
curl -sS $base/175397/forecast_250425.pdf | pdftotext -f 1 -l 1 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/forecast_250425.txt
curl -sS $base/179982/forecast_250725.pdf | pdftotext -f 1 -l 1 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/forecast_250725.txt
curl -sS $base/184551/forecast_251024.pdf | pdftotext -f 1 -l 1 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/forecast_251024.txt
curl -sS $base/187046/forecast_260213.pdf | pdftotext -f 1 -l 1 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/forecast_260213.txt
curl -sS $base/189801/forecast_260424.pdf | pdftotext -f 1 -l 1 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/forecast_260424.txt
curl -sS $base/194258/forecast_260724.pdf | pdftotext -f 1 -l 1 -layout -enc UTF-8 -nopgbrk - - > cbr/testdata/mt_forecast_pdf/forecast_260724.txt
```

Проверка: каждый файл содержит якорь и строку `Ключевая ставка, в среднем за год`.

- [ ] **Step 2: Написать корпусный тест (красный)**

`cbr/mt_forecast_pdf_test.go`: таблица `mtForecastPDFFixtures` с полями `File`, `Years []uint16`, `Points int`, `KeyFirst, KeyLast [2]float64` — значения ниже. Тест:

```go
func TestMtForecastParsePDFCorpus(t *testing.T) {
    total := 0
    for _, tt := range mtForecastPDFFixtures {
        text := mtForecastFixture(t, "testdata/mt_forecast_pdf/"+tt.File)
        points, err := mtForecastParsePDF(text)
        if err != nil { t.Fatalf("%s: %v", tt.File, err) }
        if len(points) != tt.Points { t.Fatalf("%s: точек %d, want %d", tt.File, len(points), tt.Points) }
        seen := map[string]bool{}
        for _, p := range points { seen[p.Indicator] = true }
        if len(seen) != 16 { t.Errorf("%s: показателей %d, want 16", tt.File, len(seen)) }
        key := map[uint16]mtForecastPoint{}
        for _, p := range points { if p.Indicator == "key_rate_avg" { key[p.Year] = p } }
        first, last := tt.Years[0], tt.Years[len(tt.Years)-1]
        if got := key[first]; got.Lo != tt.KeyFirst[0] || got.Hi != tt.KeyFirst[1] {
            t.Errorf("%s: key_rate_avg %d = %v..%v, want %v", tt.File, first, got.Lo, got.Hi, tt.KeyFirst)
        }
        if got := key[last]; got.Lo != tt.KeyLast[0] || got.Hi != tt.KeyLast[1] {
            t.Errorf("%s: key_rate_avg %d = %v..%v, want %v", tt.File, last, got.Lo, got.Hi, tt.KeyLast)
        }
        total += len(points)
    }
    if total != 1104 { t.Fatalf("точек в корпусе %d, want 1104 (496 комментариев + 608 прогнозов)", total) }
}
```

Данные таблицы (файл → годы → точек → key_rate_avg первый/последний год):

| File | Years | Points | KeyFirst | KeyLast |
|---|---|---|---|---|
| `comment_27022024_p1p2.txt` | 2024–2026 | 48 | 13.5/15.5 | 6/7 |
| `comment_13052024.txt` | 2024–2026 | 48 | 15/16 | 6/7 |
| `comment_07082024.txt` | 2024–2027 | 64 | 16.9/17.4 | 7.5/8.5 |
| `comment_06112024.txt` | 2024–2027 | 64 | 17.5/17.5 | 7.5/8.5 |
| `comment_26022025.txt` | 2025–2027 | 48 | 19/22 | 7.5/8.5 |
| `comment_12052025.txt` | 2025–2027 | 48 | 19.5/21.5 | 7.5/8.5 |
| `comment_06082025.txt` | 2025–2028 | 64 | 18.8/19.6 | 7.5/8.5 |
| `comment_06112025.txt` | 2025–2028 | 64 | 19.2/19.2 | 7.5/8.5 |
| `comment_26022026.txt` | 2026–2028 | 48 | 13.5/14.5 | 7.5/8.5 |
| `forecast_240216.txt` | 2024–2026 | 48 | 13.5/15.5 | 6/7 |
| `forecast_240426.txt` | 2024–2026 | 48 | 15/16 | 6/7 |
| `forecast_240726.txt` | 2024–2027 | 64 | 16.9/17.4 | 7.5/8.5 |
| `forecast_241025.txt` | 2024–2027 | 64 | 17.5/17.5 | 7.5/8.5 |
| `forecast_250214.txt` | 2025–2027 | 48 | 19/22 | 7.5/8.5 |
| `forecast_250425.txt` | 2025–2027 | 48 | 19.5/21.5 | 7.5/8.5 |
| `forecast_250725.txt` | 2025–2028 | 64 | 18.8/19.6 | 7.5/8.5 |
| `forecast_251024.txt` | 2025–2028 | 64 | 19.2/19.2 | 7.5/8.5 |
| `forecast_260213.txt` | 2026–2028 | 48 | 13.5/14.5 | 7.5/8.5 |
| `forecast_260424.txt` | 2026–2028 | 48 | 14/14.5 | 7.5/8.5 |
| `forecast_260724.txt` | 2026–2029 | 64 | 14.5/14.6 | 7.5/8.5 |

- [ ] **Step 3: Запустить тест — убедиться, что падает**

Run: `go test ./cbr -run TestMtForecastParsePDFCorpus -count=1`
Expected: FAIL — `mtForecastParsePDF` не определена.

- [ ] **Step 4: Реализовать `mtForecastParsePDF`**

Порядок (детали — спека §3.5):

1. `mtForecastPDFNormalizeLine(line string) string`: удалить `\u00ad`; заменить `\u00a0\u2002\u2009\u200a\u202f\t\f` пробелом; применить `mtForecastPDFJoinRangesRe.ReplaceAllString(line, "$1–$2")`.
2. Пройти строки, найти первую с `strings.Contains(strings.ToUpper(line), mtForecastPDFAnchor)`; нет — ошибка `"%s: якорь %q не найден — вёрстка PDF изменилась"`.
3. После якоря найти первую строку, где `strings.Fields`-разбиение по `\s{2,}` даёт ≥ `mtForecastHeaderYears` токенов-годов (`mtForecastYear`). Годы обязаны возрастать и не повторяться; иначе ошибка.
4. Далее до строки, начинающейся с `mtForecastPDFStop` или совпадающей с `^(?:\d+|[¹²³⁴⁵⁶⁷⁸⁹])\s`, собирать строки; разбивать по `\s{2,}` (`mtForecastPDFColumnGapRe`), пустые и однотокенные пропускать.
5. Подпись — первый токен; `mtForecastNormalizeLabel`; если пусто — пропуск; если не найдена в реестре — `log.Warnf` и продолжение.
6. Значения: `len(values) != 1+len(years)` — ошибка с файлом/подписью/числом; первый токен (факт) пропустить; остальные — `mtForecastBounds`, ошибку обернуть подписью и годом.
7. После цикла — `mtForecastMissing` (core — ошибка, rest — warn); `len(points)==0` — ошибка.

- [ ] **Step 5: Расширить `mtForecastNormalizeLabel`**

В `cbr/mt_forecast_parse.go`:

```go
func mtForecastNormalizeLabel(raw string) string {
    label := mtForecastDashReplacer.Replace(mtForecastNormalizeCell(mtForecastStripSuperscripts(raw)))
    label = strings.TrimSpace(label)
    label = strings.TrimSuffix(label, ":")
    return strings.TrimSpace(strings.TrimRight(label, "0123456789"))
}
```

- [ ] **Step 6: Запустить тесты**

Run: `go test ./cbr -run 'TestMtForecastParsePDFCorpus|TestMtForecastNormalizeLabel|TestMtForecast' -count=1`
Expected: PASS (корпус и существующие тесты xlsx).

- [ ] **Step 7: Добавить fail-fast и warn-тесты**

`cbr/mt_forecast_pdf_test.go`:

- пустой текст → ошибка с «якорь»;
- текст без строки годов (якорь + одна строка показателя) → ошибка с «год»;
- строка с лишним значением (`13,5 – 14,5 – 15,0` в одной ячейке) → ошибка с подписью строки;
- текст без `Ключевая ставка…` (удалить строку из фикстуры) → ошибка с `key_rate_avg`;
- удалить строку `ипотечные жилищные кредиты` → `warn`, тест проверяет, что разбор не упал и точек 15×годы.

Run: `go test ./cbr -run TestMtForecastParsePDF -count=1`; Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add cbr/mt_forecast_pdf.go cbr/mt_forecast_pdf_test.go cbr/testdata/mt_forecast_pdf cbr/mt_forecast_parse.go
git commit -m "cbr_mt_forecast: parse PDF tabl 1 from forecast bulletins"
```

## Task 2: Извлечение текста `pdftotext`

**Files:**
- Modify: `cbr/mt_forecast_pdf.go`
- Test: `cbr/mt_forecast_pdf_test.go`

**Interfaces:**
- Consumes: `mtForecastParsePDF`.
- Produces:
  - `var mtForecastPDFTool = "pdftotext"` (подменяется в тесте).
  - `func mtForecastPDFText(ctx context.Context, body []byte) ([]byte, error)`.

- [ ] **Step 1: Написать тесты (красные)**

1. `mtForecastPDFText(ctx, []byte("<html>..."))` → ошибка содержит `%PDF-` и не запускает `exec`.
2. Подменить `mtForecastPDFTool = "definitely-not-pdftotext"` (вернуть в `t.Cleanup`) и подать валидный заголовок `%PDF-1.7` → ошибка содержит имя инструмента и `poppler-utils`.

Run: `go test ./cbr -run TestMtForecastPDFText -count=1`; Expected: FAIL.

- [ ] **Step 2: Реализовать**

- проверка `bytes.Contains(body[:min(len(body), 1024)], []byte("%PDF-"))` → ошибка `"%s: выпуск %s: ответ не PDF (заголовок %q) — WAF/HTML вместо файла"`;
- `ctx, cancel := context.WithTimeout(ctx, mtForecastPDFTimeout)`; `exec.CommandContext(ctx, mtForecastPDFTool, "-layout", "-enc", "UTF-8", "-nopgbrk", "-", "-")`;
- `cmd.Stdin = bytes.NewReader(body)`; `output, err := cmd.Output()`; при ошибке — `fmt.Errorf("%s: %s: %w: %s (нужен poppler-utils: pdftotext)", ...)`;
- пустой `output` — ошибка.

- [ ] **Step 3: Прогнать тесты**

Run: `go test ./cbr -run 'TestMtForecastPDFText|TestMtForecastParsePDFCorpus' -count=1`; Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add cbr/mt_forecast_pdf.go cbr/mt_forecast_pdf_test.go
git commit -m "cbr_mt_forecast: extract PDF text with pdftotext"
```

## Task 3: Индекс — две карточки, дата заседания у PDF-комментариев

**Files:**
- Modify: `cbr/mt_forecast.go`
- Modify: `cbr/mt_forecast_test.go`

**Interfaces:**
- Produces:
  - `type mtForecastKind string`; `mtForecastKindComment = "comment"`, `mtForecastKindForecast = "forecast"`.
  - `mtForecastRelease.Kind mtForecastKind`.
  - `const mtForecastForecastCardTitle = "Среднесрочный прогноз Банка России"`, `const mtForecastForecastHrefMarker = "forecast_"`.
  - `type mtForecastCounts struct { Comments, CommentPages, CommentPDFs, Forecasts int }`; `func mtForecastReleaseCounts(releases []mtForecastRelease) mtForecastCounts`.
  - `mtForecastNewReleases` больше не отбрасывает `release.PDF`.

- [ ] **Step 1: Обновить тесты индекса (красные)**

В `TestMtForecastIndexReleases`:

- `len(releases) == 22`; `counts == {Comments: 11, CommentPages: 2, CommentPDFs: 9, Forecasts: 11}`;
- у 9 PDF-комментариев `Kind == mtForecastKindComment`, `PDF == true`, `MeetingDate` не нулевая и равна дате блока; URL оканчивается `.pdf`;
- у 11 прогнозов `Kind == mtForecastKindForecast`, `PDF == true`, `ReleaseDate == MeetingDate`, URL содержит `forecast_` и оканчивается `.pdf`;
- подтест «Индекс без страничных карточек» заменить на «Индекс без комментариев»: заменить в фикстуре титул `Комментарий к среднесрочному прогнозу Банка России` на `Комментарий` → ошибка содержит «комментар»;
- добавить подтест «Индекс без прогнозов»: заменить титул `Среднесрочный прогноз Банка России` на `Прогноз` → ошибки нет, `counts.Forecasts == 0`, `counts.Comments == 11`.

В `TestMtForecastNewReleases`: пустое множество известных → 22 выпуска, первый `2024-02-16` (forecast), второй `2024-02-27` (comment); известны только 2 xlsx-даты → 20; известны все 22 → 0.

Run: `go test ./cbr -run 'TestMtForecastIndexReleases|TestMtForecastNewReleases' -count=1`; Expected: FAIL.

- [ ] **Step 2: Реализовать разбор**

В `mtForecastIndexReleases` для каждой карточки:

1. найти href с `comment_` **или** `forecast_`; титул — `mtForecastCardTitle` **или** `mtForecastForecastCardTitle`; иначе пропуск;
2. дата публикации — из карточки (как сейчас; отсутствие/неразбор — ошибка);
3. дата заседания — из шапки ближайшего `div.dropdown` для обоих типов; отсутствие — ошибка;
4. для страницы: проверка слага/даты карточки (как сейчас), `Kind=comment`, `PDF=false`;
5. для `comment_*.pdf`: `Kind=comment`, `PDF=true`, `URL=cbrUrl+href`;
6. для `forecast_*.pdf`: `Kind=forecast`, `PDF=true`, `URL=cbrUrl+href`.

После цикла: нет ни одной карточки комментария → ошибка (текст содержит «комментар»); нет прогнозов → `log.Warnf` (не ошибка).

`mtForecastNewReleases`: удалить условие `release.PDF ||` из фильтра.

`mtForecastReleaseContext`: префикс вида `комментарий`/`прогноз` в сообщении.

- [ ] **Step 3: Прогнать тесты**

Run: `go test ./cbr -run 'TestMtForecast' -count=1`; Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add cbr/mt_forecast.go cbr/mt_forecast_test.go
git commit -m "cbr_mt_forecast: read forecast and PDF comment cards from index"
```

## Task 4: Цикл импорта — PDF через BodyFunc

**Files:**
- Modify: `cbr/mt_forecast.go`
- Test: `cbr/mt_forecast_test.go`

**Interfaces:**
- Consumes: `mtForecastPDFText`, `mtForecastParsePDF`, `mtForecastRelease.Kind/PDF`, `mtForecastCounts`.
- Produces: `func mtForecastPDFAppendPoints(ctx context.Context, release mtForecastRelease) func(body []byte, batch driver.Batch) error`.

- [ ] **Step 1: Написать тест `BodyFunc` (красный)**

Тест `TestMtForecastPDFAppendPoints`: читает `testdata/mt_forecast_pdf/forecast_260724.txt`, вызывает функцию-замыкание с `appendRecorder` (уже есть в пакете), проверяет 64 `Append` и первую/последнюю строку: `meeting_date == release_date == 2026-07-24`, `url == release.URL`, `indicator/year/lo/hi` для `key_rate_avg` 2026 = 14.5/14.6.

Run: `go test ./cbr -run TestMtForecastPDFAppendPoints -count=1`; Expected: FAIL.

- [ ] **Step 2: Реализовать**

```go
func mtForecastPDFAppendPoints(ctx context.Context, release mtForecastRelease) func(body []byte, batch driver.Batch) error {
    return func(body []byte, batch driver.Batch) error {
        text, err := mtForecastPDFText(ctx, body)
        if err != nil { return err }
        points, err := mtForecastParsePDF(text)
        if err != nil { return err }
        for _, p := range points {
            if err := batch.Append(release.MeetingDate, release.ReleaseDate, release.URL, p.Indicator, p.Year, p.Lo, p.Hi); err != nil {
                return fmt.Errorf("%s: %s: точка (%s, %d): %w", mtForecastTable, release.ReleaseDate.Format(mtForecastDayLayout), p.Indicator, p.Year, err)
            }
        }
        return nil
    }
}
```

- [ ] **Step 3: Переключить ветку в `Import`**

В цикле по `fresh`:

```go
local := s.ClickHouseImport
if release.PDF {
    local.DataUrl = release.URL
    local.BodyFunc = mtForecastPDFAppendPoints(ctx, release)
} else {
    releasePage, pageErr := util.GetPage(release.URL) // как сейчас
    ...
    local.DataUrl = cbrUrl + href
    local.ImportFunc = mtForecastAppendPoints(release)
}
points, importErr := local.Import(ctx, conn)
```

`AfterImport` у копии не выставляется (производные публикует шаг 2, как сейчас). Поста-чек `mtForecastReleaseWritten` — общий.

No-op-сообщение: `новых выпусков нет (комментариев %d: страниц %d, PDF %d; прогнозов %d; в таблице %d выпусков)`.

- [ ] **Step 4: Прогнать тесты пакета**

Run: `go test -race -count=1 ./cbr/`; Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cbr/mt_forecast.go cbr/mt_forecast_test.go
git commit -m "cbr_mt_forecast: import PDF releases through BodyFunc"
```

## Task 5: Документы

**Files:**
- Modify: `ARCHITECTURE.md` (§1, §2 `cbr/`, §6.1 строка `cbr_mt_forecast`, §6.3 комментарии витрины)
- Modify: `docs/IMPORTERS.md` (строка `cbr`)
- Modify: `README.md` (зависимость `poppler-utils`)
- Modify: `deploy/staging/README.md` (prerequisite `poppler-utils`)
- Modify: `docs/IMPORTER_PACKAGE_STANDARD.md` §18 (примечание к `cbr`)
- Modify: `docs/ROADMAP_KEY_RATE.md` (§4.1, §4.2 №19, §6 фаза 2)

- [ ] **Step 1: ARCHITECTURE.md**

- §1 «Ключевые библиотеки»: добавить внешнюю утилиту `pdftotext` (Poppler) для PDF-выпусков `cbr_mt_forecast` и `polyus`;
- §2 блок `cbr/`: заменить «9 выпусков — только PDF и пропускаются со счётчиком» на «9 PDF-комментариев и 11 PDF „Среднесрочный прогноз“ импортируются через `pdftotext`; xlsx — у 2 новейших»;
- §6.1 строка `cbr_mt_forecast`: источник — оба типа карточек, разбор PDF, 22 винтажа / 1216 точек;
- §6.3 `cbrMtForecastView`: `release_date` — дата публикации любого артефакта (у прогноза = дата заседания), `url` — PDF или страница; убрать «более ранние — только PDF и в таблицу не импортируются».

- [ ] **Step 2: остальные документы**

`docs/IMPORTERS.md` — обновить абзац `cbr_mt_forecast`; `README.md` — в разделе зависимостей/эксплуатации добавить `poppler-utils` (нужен `polyus_financial_metrics` и `cbr_mt_forecast`); `deploy/staging/README.md` — в «Порядок развёртывания» добавить `apt install poppler-utils`; стандарт §18 — примечание: «PDF-транспорт `cbr_mt_forecast` — `pdftotext`, названное отступление §4, спека 2026-10-19».

- [ ] **Step 3: Роадмап**

- §4.1 строка `cbr_mt_forecast`: покрытие `2024-02-16 … 2026-08-05 (1 216 = 22 винтажа × 16 показателей; 9 PDF-комментариев + 11 PDF прогноза + 2 xlsx)` — снимок после приёмки;
- §4.2 №19: `✅ сделан этим PR (приёмка 2026-10-19, issue #118)` + спека/план + числа;
- §6 фаза 2: пункт в цепочке — ✅, с ссылкой на issue #118.

- [ ] **Step 4: Проверка документов**

Run: `make docs-check`; Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add ARCHITECTURE.md README.md docs/IMPORTERS.md docs/IMPORTER_PACKAGE_STANDARD.md docs/ROADMAP_KEY_RATE.md deploy/staging/README.md
git commit -m "docs: cbr_mt_forecast PDF releases"
```

## Task 6: Живая приёмка (выполняет основной агент)

- [ ] **Step 1:** `make ch-status` — ClickHouse отвечает (если нет — попросить владельца поднять `make ch-up`).
- [ ] **Step 2:** `make import STAT=cbr_mt_forecast` → `Imported 1104 rows` (на dev-БД с уже импортированными 112). Записать число строк и крайние даты.
- [ ] **Step 3:** повторный `make import STAT=cbr_mt_forecast` → `Imported 0 rows`, сообщение `комментариев 11 (страниц 2, PDF 9), прогнозов 11, в таблице 22 выпуска`, без скачиваний.
- [ ] **Step 4:** SQL через MCP/`make ch-sql`: `count() FINAL` = 1216, `uniqExact(release_date)` = 22, spot `key_rate_avg` 2026 из 2026-02-13 и 2026-07-24.
- [ ] **Step 5:** `ARCHIVE_BACKEND=local make import STAT=cbr_mt_forecast` (при необходимости) — 20 объектов PDF и +20 строк `registry.jsonl`.
- [ ] **Step 6:** `make mcp-check` → ПРОВЕРКА ПРОЙДЕНА; `dagu validate dagu/cbr.yaml` → exit 0; `make all` → зелёный.
- [ ] **Step 7:** Протокол — в коммит статуса ✅ и комментарий в PR.

## Протокол приёмки (шаблон)

```text
Команда: make import STAT=cbr_mt_forecast
Результат: Imported <N> rows
Крайние даты: <min release_date> … <max release_date>
Винтажей: <K>; точек: <count() FINAL>
Повтор: Imported 0 rows; «новых выпусков нет (комментариев 11: страниц 2, PDF 9; прогнозов 11; в таблице 22 выпуска)»
Архив: <N> объектов, +<N> строк registry.jsonl
make mcp-check: ПРОВЕРКА ПРОЙДЕНА
dagu validate dagu/cbr.yaml: exit 0
make all: зелёный
```
