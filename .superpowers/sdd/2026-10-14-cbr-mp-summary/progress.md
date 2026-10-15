# SDD ledger — plan: docs/superpowers/plans/2026-10-14-cbr-mp-summary.md

- Spec: `docs/superpowers/specs/2026-10-14-cbr-mp-summary-design.md` (коммит `996d25b`; одобрена владельцем, с правкой про LLM-разметку)
- Plan: коммит `890939d` (+ фикс T6 ниже — pre-flight после PR #55)
- Branch: `feat/cbr-mp-summary` (от main `55cd038` — PR #55 «пункт 1.7 — preflight для 14 импортёров» влит)
- BASE (branch start): `55cd038`; спека+план — коммиты `996d25b`, `890939d`
- Models: имплементеры — `deepseek/deepseek-flash`; ревьюеры задач — `haiku`; финальное ревью — `primary`
- Workspace: `.superpowers/sdd/2026-10-14-cbr-mp-summary/` (git-ignored)
- Standing caution: незакоммиченная правка владельца `deploy/staging/dagu.service` — не трогать и не индексировать.

## Pre-flight scan (после PR #55; сверено с кодом main)

| Pair | Producer → Consumer | Finding |
|---|---|---|
| T1 → T5 | `util.PostJSON(url, headers, request, out) error` → LLM-клиент | ok (в `util/` PostJSON нет — grep) |
| T2 → T3/T7 | фикстуры + фактические числа → пины тестов | ok (порядок задач обеспечивает) |
| T3 → T4/T5/T6 | `normMPText`, `mpSummaryRef/Block/Doc` | ok |
| T4 → T5/T6 | `mpChannels`, `mpChannelMatch`, `tagMPChannels` | ok |
| T5 → T6 | `mpLLMConfigFromEnv`, `tagMPChannelsLLM`, `mpPromptVersion` | ok |
| T6 self (важно) | реестр `cbrImporters` после PR #55 — запись трёхколоночная `{imp, table, countPoints}`, тест (д) сверяет литерал с `Publication()` | **фикс плана**: `{mpSummaryImporter, "cbr_mp_summary", ""}` (префлайт не объявляем — дельта по индексу, спека §3.1; тест (д) требует пустые обе стороны) + явная обратная проверка dagu-шага |
| T6 → T8 | имена/файлы/расписание → доки | ok |
| T7 self | карта русских месяцев дублируется с T3 | Ruling: локальные копии в обоих пакетах (они независимы; `cbr` → `calendar` импорт нежелателен; перенос в `util` — при третьем потребителе) |
| T8 → T9 | §4.2/§5 правятся T8, §6 — T9 | ok (разные секции одного файла) |
| T9 self | приёмка требует LLM env + ClickHouse | ok (env добавит владелец; CH поднят) |

Rulings pre-flight:
- **Ruling (T6, фикс плана):** реестр `cbr_mp_summary` — `{mpSummaryImporter, "cbr_mp_summary", ""}`; тест (д) post-#55 запрещает непустой `countPoints` вне `expectedPublicationCountPoints`; у нас префлайта нет сознательно (спека §3.1). Cost if wrong: тривиальная правка записи реестра.
- **Ruling (T7):** карта русских месяцев дублируется локально в `calendar/` (пакеты независимы). Cost if wrong: мелкий рефакторинг при третьем потребителе.

## Progress

- Task 1: dispatched (implementer `deepseek/deepseek-flash`, BASE `1e1b310`).
- Task 1: implementer DONE (commit `93da75b`); `go test ./util/` ok, `make all` зелёный.
- Task 1: reviewer (haiku): Needs fixes — 1 Important (тест UA вакуумен; мутация «убрать User-Agent» → тесты зелёные), 3 Minor; fix round 1 — запускается.
- Task 1: minor (deferred): таймаут-тест идёт 2.00 s (хендлер не читает тело, контекст не отменяется; фикс `io.Copy(io.Discard, r.Body)` предложен ревьюером).
- Task 1: minor (deferred): ветка «пустое тело» не отличается по последствиям от generic-ошибки Unmarshal (тест проверяет только err != nil; текст не пиннится).
- Task 1: minor (deferred): `io.ReadAll` без лимита размера (тот же паттерн, что `GetJSON`).
- Task 1: minor (deferred): вакуумный UA-паттерн есть и в легаси-тестах `TestGetJSONDecodesBody`/`TestGetXMLDecodesBody`/`TestGetFileReturnsBody` — системный фикс отдельной задачей по `util` (вне объёма 1.2).
- Task 1: fix round 1/5 (1 addressed, 0 open; commit `8ebf39c`; red-green: без UA-строки тест падает, с ней — PASS); scoped re-review — запускается.
- Task 1: scoped re-review (haiku): Approved — находка ADDRESSED (независимая мутация: FAIL «User-Agent = Go-http-client/1.1 — не из списка userAgents»), нового ущерба нет.
- Task 1: complete (commits 1e1b310..8ebf39c, review clean).
- Task 2: dispatched (implementer `deepseek/deepseek-flash`, BASE `8ebf39c`).
- Task 2: implementer DONE (commit `f299fb6`, 5 файлов-фикстур). Факты: индекс 88 карточек (22 резюме), у 6 карточек `document-regular_name_visible referenceable` (селектор по точному классу теряет их); «Дополнительные материалы» — h2 (23092026) vs `h2.h3.materials_title` (06112024/07052026); 07052026 — cut до 14,50%, заседание 2026-04-24, дата вступления не указана; cal_mp — карточек-заседаний 76 (= 68 с полной датой: 64 обычных 2020–2027 + 4 внеочередных + 8 без года 2019), «Внеочередное **з**аседание» строчной «з», месяц в `<span>`; в 06112024 часть `<li>` закрыта `</p>`.
- Task 2: reviewer (haiku): Needs fixes — 2 Important (в ОТЧЁТЕ: разбивка 82 вместо 88; «72 с полной датой» вместо 68/76); фикстуры и коммит корректны.
- Task 2: fix round 1/5 (2 addressed, 0 open; правка только отчёта, без коммита; пересчёт: 22/22/22/11/11=88; 76=68+8, внеочередные 2022-02-28/04-08/05-26, 2023-08-15).
- Task 2: scoped re-review (haiku): Approved — оба ADDRESSED, нового ущерба нет.
- Task 2: complete (commits 8ebf39c..f299fb6, review clean).
- Task 3: dispatched (implementer `deepseek/deepseek-flash`, BASE `f299fb6`).
- Task 3: implementer DONE (commit `1342a1f`); 7 тестов + 4 подтеста PASS, мутации ловятся; ruling имплементера: в брифе перепутаны крайние ссылки индекса (порядок страницы — от новых к старым: первая 23092026), пиннул факт; пустые DOM-узлы пропускаются. Reviewer — запускается.
- Task 3: reviewer (haiku): Approved (Minor'ы только); 15/15 счётчиков тестов сошлись с §3.1 отчёта T2; M1/M2-мутации ловятся; в отчёте имплементера ошибка про «0 U+00A0» (фактически 7+7 — код учитывает верно).
- Task 3: complete (commits f299fb6..1342a1f, review clean).
- Task 3: minor (deferred): `mp_summary_parse_test.go:74` `order[i]` без проверки длины (паника только на провальном пути).
- Task 3: minor (deferred): `mpFetchHref` возвращает `b.Text` (имя/комментарий расходятся).
- Task 3: minor (deferred): `DecisionRemoved`-тест проверяет подстроку «решение», совпадающую с именем блока 5 — ошибка может прийти не из того места; точная строка «не распознано решение» строже.
- Task 3: minor (deferred): фолбэк «доступных к/**на**» покрыт тестом только вариантом «к» (мутация M3 не падает).
- Task 3: minor (deferred): `mp_summary_parse.go:201` комментарий «только до первого h2» неточен (сейчас безвредно).
- Task 4: dispatched (implementer `deepseek/deepseek-flash`, BASE `1342a1f`).
- Task 4: implementer DONE (commit `e746afb`); 3/3 TestMPChannels PASS; числа hold: 158 матчей = 133 prose + 19 risk_pro + 6 risk_dez; сомнения: (1) 06112024 risk_dez пуст (дезинфляционная часть — абзац, не пули), (2) сплит режет сокращения («с.к.г.», «б.п.») — фрагменты дословны, но обрубки, (3) тест назван TestMPChannelsHoldFixture ради гейта `-run TestMPChannels`.
- Task 4: reviewer (haiku; первый запуск упал с provider_error, резюм завершил): Needs fixes — Important: потеря дезинфляционного абзаца 06112024 (risk_dez=0, абзац отбрасывался молча; патч проверен в копии); Minor'ы: мёртвые `\b`-узоры (RE2 ASCII), обрезки на сокращениях в сплите, пины не из §3.2 (фактически из фикстур — дословны), `mpBlockRisks` дублирует индекс, Seq-сдвиг от вводных абзацев.
- Task 4: fix round 1/5 (1 addressed, 0 open; commit `a7cf7b4`; 06112024: 137 = 119 prose + 16 pro + 2 dez — совпало с проверочным патчем ревьюера; 23092026/07052026 без изменений); scoped re-review — запускается.
- Task 4: scoped re-review (haiku): Approved — ADDRESSED (потерь 0, добавлений ровно 2, наборы остальных фикстур идентичны, гейты 4 PASS); перенесённые Minor'ы не блокируют.
- Task 4: complete (commits 1342a1f..a7cf7b4, review clean).
- Task 4: minor (deferred): мёртвые `\b`-узоры (`\bспред…`, `\bм2\b` — RE2 `\b` только ASCII); сплит режет «с.к.г.»/«б.п.» (~12 из ~600 фрагментов); пины не из §3.2 (ссылка в отчёте неверна, цитаты из фикстур); `mpBlockRisks` дублирует `mpSummaryBlocks[3]`; слабая lower-case проверка в Registry; широкие узоры тариф/курс/бюджет/нефт.
- Task 5: dispatched (implementer `deepseek/deepseek-flash`, BASE `a7cf7b4`).
- Task 5: implementer DONE (commit `02755b9`); 7 тестов + 4 подтеста PASS, все пакеты ok; сомнения: промпт ~48 КБ/док (ретрай пересылает), mpPromptVersion вшит в текст промпта, устойчивость к ```-обёртке JSON, TrimSpace env, дедуп через mpChannelMatchKey. Reviewer — запускается.
- Task 5: reviewer (haiku): Approved (только Minor'ы); 3 мутации ловятся (валидация цитат, ретрай, классификация ошибок 500).
- Task 5: complete (commits a7cf7b4..02755b9, review clean).
- Task 5: minor (deferred): гейт-паттерн брифа `MPLLM` регистрозависим (широкий `MpLLM|…|MpPromptVersion` — 9 PASS); `mpLLMJSONBody` режет `{…}` (ослабляет «строгий JSON», покрыто FencedJSON-тестом); маскировка ключа по спеке §3.5 (образец eia.maskKey) не реализована — безопасность на том, что ключ не попадает в тела ошибок; тест OK зависит от дважды встречающейся подстроки (не хрупко).
- Task 6: dispatched (implementer `deepseek/deepseek-flash`, BASE `02755b9`).
- Task 6: implementer DONE (commit `3d456ae`, 7 файлов); гейты зелёные (dagu 15 шагов, validate exit 0, py_compile ok, DDL сверены со спекой по-байтово + EXPLAIN SYNTAX); решения сверх брифа: url-колонка для LLM-догона, `mpDay` (Date→UTC день, прецедент forecast/resolve.go), fail-fast разбора новых, тесты — mutation-check вместо RED (заявлено). Reviewer — запускается.
- Task 6: reviewer (haiku): Needs fixes — Important: partial-failure (шапка `send` первой; при падении block/regex-батчей документ считается известным по url → no-op, блоки/regex потеряны; противоречит спеке §3.1 п.2). Minor'ы: вечный LLM-pending при нуле валидных матчей; дубль llmDocs (docs + mpFetchPending без фильтра); комментарий «вместо 13 прежних» устарел; дельта ранних заседаний не пересчитывается (спека не требует); 3/4 мутаций пойманы, partial-failure тестами не покрыт.
- Task 6: Ruling (вариант (а) ревьюера, расширенный): документ считается известным, только если его url есть в шапке И по его `meeting_date` есть строки в `cbr_mp_summary_block` И есть regex-строки в `cbr_mp_summary_channel` (`WHERE method='regex'` — llm-строки не маскируют); неполный документ перекачивается и перезаписывается (ключи Replacing идемпотентны). Спека §3.1 п.1 обновляется этим же фиксом. Расширение добавлено после честного сомнения имплементера (падение только regex-батча не покрывалось). Cost if wrong: лишний рефетч редкого документа с нулём regex-матчей.
- Task 6: fix round 1/5 (1 addressed, 0 open; commits `a81a165` + `450f03d`; red-green оба уровня: шапка без блоков → missing; шапка+блоки без regex → missing); scoped re-review — запускается.
- Task 6: scoped re-review (haiku): Needs fixes — код ADDRESSED, но тест не пинил условие по блокам (мутация M-A проходила); предложен кейс regexOnly.
- Task 6: fix round 2/5 (1 addressed, 0 open; commit `0a04135`, только тест; red-green M-A: без blockDates-условия новый кейс падает); re-review round 2 — запускается.
- Task 6: scoped re-review round 2 (haiku): Approved — M-A ADDRESSED (мутация ловится, assert по составу/порядку), кода не тронут.
- Task 6: complete (commits 02755b9..0a04135, review clean).
- Task 6: minor (deferred): вечный LLM-pending при нуле валидных матчей (документ переспрашивает LLM каждый прогон); дубль `llmDocs` (`docs` + `mpFetchPending` без фильтра по датам) — возможен при частичном сбое/смене URL; комментарий `cbr/importers.go:24-25` «вместо 13 прежних» устарел; дельта ранних заседаний не пересчитывается при позднем появлении более раннего (спека не требует); теоретический вечный рефетч документа с нулём regex-матчей.
- Task 7: dispatched (implementer `deepseek/deepseek-flash`, BASE `0a04135`).
- Task 7: implementer DONE (commit `5281f26`, 3 файла); parseCalMP = 68 дат (8/8/11/9/8/8/8/8), совпало с §4.1 отчёта T2; 4 мутации ловятся; решения: фильтр-тема расширен до {«ключевой ставке», «денежно-кредитной политике»}, пин 2026-11-05 закрыт как отсутствие; живого Import() не было (T9). Reviewer — запускается.
- Task 7: reviewer (haiku): Approved по коду; Important (спека/доки): (1) буквальный фильтр брифа/спеки теряет 15 заседаний (мутация M5) — фактический фильтр шире, спеку §3.7 синхронизировать; (2) задокументировать «факт заседания / аналитический триггер» (дубль рядов 23.10/18.12) в ARCHITECTURE §7. Оба маршрутизированы в T8. Minor'ы: блочный фильтр хрупче карточного; тест не покрывает «заседание» без «Совета директоров»; fetch-fail → exit 0 — осознанное решение (спека §3.7).
- Task 7: complete (commits 0a04135..5281f26, review clean; Important #1–2 → T8).
- Task 8: dispatched (implementer `deepseek/deepseek-flash`, BASE `5281f26`).
- Task 8: implementer DONE (commit `1be7cec`, 6 файлов); grep cbr_mp_summary = 33; DDL-копии §6.2 = IDENTICAL ×3; спека §3.7 синхронизирована (фактический фильтр); сомнения: «15 vs 16» заседаний в M5-формулировках (в доки пошло 15 — по перечислению ревьюера), устаревший комментарий `cbr/importers.go:47-50`, правило 4 AGENTS не упоминает PostJSON. Reviewer — запускается.
- Task 8: reviewer (haiku): Approved (Minor'ы); `make all` exit 0; DDL/витрины/расписание/гранты/mcp_check сверены с кодом; **ground truth по фильтру**: буквальный AND оставлял 8 из 68 (проверено ревьюером; моя независимая проверка: 8 из 67 по фикстуре) — число «15» в доке неверно, правим в T9.
- Task 8: complete (commits 5281f26..1be7cec, review clean; микрофиксы → T9).
- Task 8: minor (deferred → T9): спека §3.7 «терял 15» → «оставлял 8 из 68»; `cbr/importers.go:47-50` устаревший комментарий (url → url+блоки+regex); ARCHITECTURE «последним (14-м)» про cbr_mo_br устарело (теперь 14-й из 15, последний — mp_summary); ROADMAP «отдельным шагом» vs ✅; AGENTS правило 4 не упоминает `util.PostJSON`/`GetXML`/`GetFile` (не редактируем — файл владельца, в отчёт); предсуществующие битые markdown-таблицы README:262/ARCHITECTURE:256 (вне объёма).
- Task 9: dispatched (implementer `deepseek/deepseek-flash`, BASE `1be7cec`).
- Task 9: implementer DONE (commit-диапазон `1be7cec..bca512f`, 4 коммита — 2 сверх задания). **Живая приёмка**: первый `Imported 0 rows` → находка №1: форма решения «принял решение установить ставку на уровне» не парсилась (фикс `8627168`); находка №2: LLM-догон считал отстающих инвертированно (фикс `6c0d8ab`); затем `резюме 22 новых, блоков 1303, каналов regex 3488` + LLM-догон `1089` (550+244+53+242 note); сверка 22/22 с реестром (9 hold/13 изменений, дельты ±25/50/100/200 и 0); повтор — no-op; календарь 68 канонических + 2 ручных; mcp-check ПРОВЕРКА ПРОЙДЕНА; микрофиксы (a)–(d) коммит `aaa851f`; статус 1.2 + блок приёмки — `bca512f`. **Владельцу**: `LLM_BASE_URL` в env указывает на Anthropic-совместимый URL DeepSeek → 404 у всех 22; приёмка прогнана с override `https://api.deepseek.com` — поправить строку в `~/.config/rosstat/env`. Reviewer — запускается.
- Task 9: reviewer (haiku): Approved — 0 Critical/Important; хотфиксы проверены мутациями M1–M3 (убиты тестами TestParseMPSummaryLevelSet/TestParseMPFailFast, TestMPSummaryLLMPendingDates); живые счётчики перепроверены (22/1303/4577=3488+1089; календарь 68+2; 9 hold/13 изменений); 404-инцидент — env владельца, не код.
- Task 9: complete (commits 1be7cec..bca512f, review clean).
- Task 9: minor (deferred): C4 — документ может попасть в `missing` ∧ `pending` при частичной записи слоя 1; комментарий `mp_summary.go:468-469` неточен (зафиксировано в блоке приёмки как известное ограничение).
- Task 9: minor (deferred): сообщения коммитов `8627168`/`6c0d8ab` на русском (конвенция — английские; история не переписывается ради косметики).
- Task 9: minor (deferred): ARCHITECTURE «три эпохи: hold, hike, cut» устарело после добавления фикстуры `01042024`; фикстура `27022024` не покрыта; README в диапазоне хотфиксов не менялся (правки разбора, не архитектуры).
- **ВСЕ ЗАДАЧИ (T1–T9) ЗАВЕРШЕНЫ.** Переход к финальному ревью всей ветки (primary).
- Финальное ревью (primary): **With fixes** — 1 Important (витрины не самовосстанавливаются: no-op раньше CreateView; сбой CreateView → зелёный dagu при невидимом контуре) + 7 Minor (мёртвые `\b`-узоры; утечка незакрытых батчей; дубль llmDocs; спека §3.4 не синхронна; маскировка ключа (§3.5) достигнута иначе; POST-таймаут-тест 2 с; мелкие неточности доков). Приёмка воспроизведена ревьюером read-only 1:1 (22/1303/3488/1089/4577; лаг-анализ дельт SQL-ом; 0 недословных цитат через position(); календарь 70=68+2).
- Фикс-волна (единственный диспатч, агент T9): Important #1 + Minor #2/#3/#4/#5/#6/#7 (в т.ч. спеку §3.4/§3.5); `\b`-узоры — после мержа отдельной задачей. Запускается.
- Фикс-волна: commit `2edac9f` (6 файлов); 6 из 7 адресованы; живое подтверждение Important (no-op пересоздал витрины, mcp-check ПРОЙДЕНА).
- Ruling (финал): residual load-bearing Finding 3 (дубль LLM-вызова не устранён — `mpFetchPending` всё ещё добавляет justFetched-дату) закрывается ТОЧЕЧНЫМ вторым микро-раундом до PR, а не вынесением в PR как известного дефекта (односточный фикс по указанию ревьюера). Cost if wrong: один лишний цикл ~5 минут.
- Re-review фикс-волны (primary-ревьюер): Needs fixes — Finding 3 NOT ADDRESSED (проба: len(llmDocs)=2 на одну дату); новый мелкий дефект: abort вызывается и на уже отправленном батче (лишний warn ErrBatchAlreadySent); остальные 6 — ADDRESSED.
- Микро-раунд 2: `continue`/исключение уже размеченных дат из `llmDocs` (объединённое множество docs∪pending минус tagged — покрывает и смену URL) + не аборить отправленный батч. Запускается.

- Микро-раунд 2: commit `e43c85e` — `mpLLMDates`-union (docs∪pending минус tagged) исключает уже размеченные даты из LLM-догона; abort не вызывается на уже отправленном батче.
- Финальное re-review (primary-ревьюер по `e43c85e`): **Approved** — Finding 3 ADDRESSED (структурно), abort-дефект закрыт.
- Verification (лично, на `e43c85e`): `make all` exit 0; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА (v_cbr_mp_summary/v_cbr_mp_summary_blocks=1303/v_cbr_mp_summary_channels=4577; три сырые закрыты 497); `dagu validate dagu/cbr.yaml` exit 0; `dagu validate dagu/calendar.yaml` exit 0.
- PR: #56 открыт — https://github.com/kmlebedev/clickhouse-import-rosstat/pull/56 (base main, head feat/cbr-mp-summary, 22 коммита от 55cd038).
- Post-PR фидбек владельца: «для запросов в LLM не строй свой велосипед — возьми готовый SDK anthropic-sdk-go». Причина согласуется с приёмкой: env LLM_BASE_URL владельца — Anthropic-совместимый base DeepSeek (/v1/messages, x-api-key), а самописный PostJSON ходил OpenAI-путём /chat/completions → 404. Recon (родитель): единственное место HTTP — mpLLMAskOnce; util.PostJSON имеет единственного потребителя (этот слой); Makefile позволяет override CLICKHOUSE_URL; DeepSeek compat: x-api-key/system/temperature/max_tokens — full.
- Fix-wave «SDK»: implementer (deepseek): commit `6c94fd5` — anthropic-sdk-go v1.80.0 (direct), option.WithAPIKey/WithBaseURL/WithHTTPClient(util.HttpClient)/WithMaxRetries(0), mpLLMTimeout=65s (var), mpLLMMaxTokens=8192, text-блоки, классификация ошибок, тесты на Anthropic-конверт (+NoTextBlock/Timeout), util.PostJSON+postJSONTimeout+тесты удалены, доки (README/ARCHITECTURE/спека/ROADMAP). make all exit 0. Живых прогонов не было.
- Скоуп-ревью (haiku): Needs fixes — Important #1: обрыв чтения тела 200 (io.ErrUnexpectedEOF) классифицируется форматом → лишний повтор, комментарии/спека говорят иное; миноры: сырое тело провайдерской ошибки в логах (SDK несёт его целиком), пустой 200 не описан, /v1 в base, UA SDK vs randomUA, неподтверждённый «предел deepseek-chat», overclaim'ы доков, 2 опечатки. Мутации: WithMaxRetries(0)/temperature/path//v1/base/ключ-в-ошибке — ловятся.
- Ruling: Important #1 — вариант (б) (сохранить поведение, исправить комментарии/спеку, закрыть тестом), а не хрупкий матчинг текста SDK; приняты как эквивалентные непокрытые мутации (ж) WithHTTPClient и (з) block.Type. Cost if wrong: +1 вызов LLM на редкий обрыв TCP.
- Fix коммит `21bbd20`: тест TestTagMPChannelsLLMBodyReadBroken (Content-Length>тела, 2 запроса, формат, io.ErrUnexpectedEOF), util.ErrorSnippet (экспорт; GetXML — второй потребитель), компактная провайдерская ошибка (http status %d: ≤512), комментарии/спека/доки, миноры 2–7. make all exit 0.
- Re-review фикса (haiku): **Approved**; 4 текстовых минора: M1 (обрыв на неполных заголовках тоже формат — уточнить формулировки), M2 (имя errorSnippet в util-тесте), M3 (слово «ретраибелен»), M4 (ROADMAP ✅ vs открытый пункт (а) — закроет живой прогон).
- Микро-раунд `b034773`: M1 (комментарии mpIsJSONDecodeError/mpLLMAskOnce, ARCHITECTURE:4/:1041, спека §3.5 — «без повтора: ни байта ответа; обрыв заголовков/тела → формат, один повтор»), M2, M3. make all exit 0. M4 — за живым прогоном.
- Живая приёмка (родитель, идёт): scratch-БД cbr_probe_mp (создана make ch-sql, default user), прогон `make import STAT=cbr_mp_summary CLICKHOUSE_URL=clickhouse://default:@localhost:9000/cbr_probe_mp` — реальный env (LLM-переменные владельца как есть), ожидание 22/1303/3488/≈1089; далее no-op повтор, drop scratch, контрольный no-op реальной БД.
- Живая приёмка SDK-транспорта (родитель): scratch-БД cbr_probe_mp, полный прогон с **реальным env владельца** (override только CLICKHOUSE_URL) → «резюме 22 новых, блоков 1303, каналов regex 3488, LLM 1020»; 0 ошибок транспорта/формата/деградаций; 2 недословные цитаты отклонены с warn (анти-галлюцинационная сверка); счётчики: header 22, blocks 1303, regex 3488, llm 1020 (note 242 = 11×22, risk_pro 245, risk_dez 61, prose 472), model=deepseek-chat/prompt_version=1, empty_notes 0, три витрины созданы; повтор — no-op; scratch удалена (make ch-sql), реальная dev-БД не изменялась (контрольный no-op «все 22 резюме на месте»).
- Проба модели (временный тест, удалён): запрошенное `deepseek-chat` → `served=deepseek-chat` (эхо); контрольный неизвестное имя → 400 «supported API model names are deepseek-flash, deepseek-v4-pro» (и это подтвердило построение URL `https://api.deepseek.com/anthropic/v1/messages`). Оговорка для владельца: первым классом поверхность принимает deepseek-flash/deepseek-v4-pro; deepseek-chat проходит и эхо сохраняет имя.
- Число LLM-строк вариативно между прогонами (1089 на OpenAI-поверхности vs 1020 на Anthropic): сама разметка валидна в обоих (0 приведений kind, 0 предупреждений slug; 2 отклонённые цитаты). Отражено в ROADMAP §6 (а) и теле PR.
- Доки: commit `399fbde` — ROADMAP §6 (преамбула, пункт 2 и (а): «пройдена» с числами и оговоркой о провайдере) + ARCHITECTURE чейнджлог (факт с числами); make all exit 0; grep старых формулировок («должен работать…», «дописываются после прогона») — пусто.
- PR #56: тело обновлено (тело v2), ветка запушена (6 коммитов от 55cd038... уточнить: e43c85e был вершиной до SDK; новых 4: 6c94fd5, 21bbd20, b034773, 399fbde).
