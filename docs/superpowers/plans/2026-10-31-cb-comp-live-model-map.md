# План Task 13 (#450): карта live-модели `combo3` и реестр исследований

> **Пункт:** `key-rate/phase-4-cb-comp-88-groups-live`, зонтик [#419](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/419), Task 13 [#450](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/450), `role:ml`. Ветка `docs/cb-comp-live-model-map`, worktree `.worktrees/cb-comp-model-map`. База — `origin/main` (`56efb78`).

## Зачем

Знания о базовой модели `combo3` разбросаны по research-отчётам, спекам, роадмапу и PR. Нужен один живой документ, по которому следующий ML-агент за один проход поймёт: что за модель, на каких данных она работает, какие числа канонические, что уже проверено и отвергнуто, какие вопросы открыты и какие шаги делать дальше.

## Шаги

1. **Гайд** — `docs/guides/cb-comp-live-model-map.md`: model card `combo3`, реестр исследований, ledger достигнутого, открытые вопросы, векторы P0–P3, шаблон протокола, карта кода/артефактов, словарь.
2. **Ссылки** — `docs/guides/README.md`, `docs/site-pages.json`, `.kimi-code/skills/ml-engineer/SKILL.md`, `docs/KEY_RATE_MODEL.md` §4.
3. **Роадмап** — строка Task 13 в пункте #419 `docs/ROADMAP_KEY_RATE.md`.
4. **Проверки** — `make docs-check`, `make all`, `make docs-build`; ручная сверка чисел с JSON/отчётами; `git status` без временных артефактов.
5. **PR** — `Closes #450`, протокол приёмки комментарием, Project V2 → `In Review`.

## Границы

- Только документация: код, DDL, витрины, гранты и ClickHouse не меняются.
- Числа — из существующих `docs/research/*.json` и отчётов; повторные прогоны не выполняются.
- `combo3` — research-база линии, не production; live-мониторинг CPI не заявляется.
- Task 10/11 (PR #447/#443) и Task 4b (протокол) помечаются как не вошедшие в `main`/не измеренные.

## Ревизия после мержа #447 и #451 (2026-10-31)

- Ветка обновлена с `origin/main` (`07da9e9`); при разрешении конфликта в `ROADMAP_KEY_RATE.md` восстановлена запись Task 12 (G12), потерянная при мержах #447/#451; статусы Task 4b/10/12 → merged.
- Task 10: числа заменены на перепрогон после ревью #447 (1,105 / 1,044 / 1,190 / 1,160); вывод «pooling лечит h6» снят — это была утечка `weight_share`.
- Task 4b: результат G11 и абляция внесены в §0, §1.8 (отдельная h1-nowcast арена), §2.3, §3.3, §4, §5; Holm `wk_agg_ex` — по JSON (0,00405).
- Добавлены §7.8 «Смена базы» и P0.6 «Приёмка G11 Task 4b и смена h1-базы»: решение `shadow`, заморозка `wk_mix_ex@v1`, ретро-robustness (real-time SA, `publication`-cutoff), проспективный paper shadow с 2026-10, правило решения и rollback.
