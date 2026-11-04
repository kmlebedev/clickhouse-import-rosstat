# Hotfix: cbr_indicators_cpd — починка селектора ссылки на aipd

## Context

На staging с 2026-11-04 09:32 МСК падает шаг dagu `cbr_indicators_cpd`:
лог показывает скачивание `…/File/108633/est_infl.xlsx` и ошибку `sheet Лист1 does not exist`.

**Root cause (подтверждён, Phase 1–3 systematic-debugging):**
- Импортёр ищет ссылку на xlsx позиционным селектором `.container-fluid > div > div:nth-child(6) > …` (cbr/indicators_cpd.go:32), откалиброванным под старую разметку страницы `cbr.ru/statistics/ddkp/aipd/`.
- ЦБ добавил в начало `.col-md-23` элемент `<script type='application/ld+json'>` (BreadcrumbList schema.org). Смещение проверено парсером на живой странице и фикстурой:
  - было: child 1 = breadcrumbs, 2 = h1, 3–7 = блоки документов → `div:nth-child(6)` = блок `indicators_cpd.xlsx`;
  - стало: child 1 = script, 2 = breadcrumbs, 3 = h1, 4–8 = блоки → `div:nth-child(6)` = блок **`est_infl.xlsx`**.
- Скачивается чужой файл `est_infl.xlsx`, у него единственный лист «Лист 1» (с пробелом) → `xlsx.GetRows("Лист1")` падает с `sheet Лист1 does not exist`.
- Целевой файл жив: `…/File/108632/indicators_cpd.xlsx` на месте, листы «Лист1»/«Лист2», парсится (проверено скачиванием). Имя файла стабильно (id 108632 не менялся).
- Текущий тест `TestIndicatorsCpdFindLink` зелёный на устаревшей фикстуре — фикстура не отражает живую разметку, регрессию не ловит.
- Других `nth-child`-селекторов в репозитории нет; смежных импортёров на этой странице нет.

## Approach

Заменить позиционный селектор на атрибутный по имени файла — тот же приём, что в этом пакете у `cbr_credit_m2x`: `a.referenceable[href$="credit_m2x.xlsx"]` (cbr/credit_m2x.go:21). Атрибутный селектор не зависит от позиции блока и переживёт дальнейшие правки вёрстки страницы. Одновременно обновить фикстуру `cbr/testdata/aipd_page.html` с живой страницы, чтобы тест покрывал текущую разметку (с JSON-LD).

Это точечный hotfix одного импортёра: DDL, каталог рядов, витрины, гранты не затрагиваются (правило 11 не триггерится), ARCHITECTURE/README не меняются.

## Key changes

- `cbr/indicators_cpd.go` — `indicatorsCpdLinkSelector` = `a.referenceable[href$="indicators_cpd.xlsx"]`; комментарий: почему атрибутный селектор (имя файла на aipd стабильно, id в URL меняется) и что позиционный сломался 2026-11-04 из-за JSON-LD-скрипта.
- `cbr/indicators_cpd_test.go` — обновить формулировку комментария теста (позиционный → атрибутный селектор); сама проверка (`HasSuffix … indicators_cpd.xlsx`) сохраняется.
- `cbr/testdata/aipd_page.html` — заменить на актуальную живую страницу (уже скачана в `/tmp/aipd_live.html`, 31198 байт, в ней script JSON-LD + все 5 блоков включая reg_cpd.xlsx).
- GitHub: создать hotfix-issue, ветка `fix/cbr-indicators-cpd-selector` в worktree `.worktrees/cbr-cpd-fix` от `main`, PR с `Closes #N`.

## Verification

1. `go test ./cbr/ -run TestIndicatorsCpd -v -count=1` — зелёный на обновлённой фикстуре (новая разметка).
2. `make import STAT=cbr_indicators_cpd` против поднятого dev ClickHouse (`make ch-status` — отвечает):
   - в логе `Get xlsx file …/108632/indicators_cpd.xlsx` (не est_infl);
   - строки записались, крайние даты адекватны; запрос к `cbr_indicators_cpd` — свежие точки;
   - повторный прогон — no-op по префлайту `source_publications`.
3. `make all` — gofmt/lint/vet/race/build зелёные.
4. Удалить временные файлы диагностики `/tmp/aipd_live.html`, `/tmp/est_infl.xlsx`, `/tmp/indicators_cpd_live.xlsx`.

## Assumptions

- Hotfix идёт отдельным issue/веткой/PR как починка прод-поломки; пункта роадмапа под это нет (поиск по трекеру не дал).
- Имя `indicators_cpd.xlsx` считаем стабильным; при его смене `util.FindLink` вернёт явную ошибку «не найдена ссылка на xlsx», что видно дежурному в логе.
- Деплой на staging после мержа — действие владельца, ассистент не деплоит без явной просьбы.
