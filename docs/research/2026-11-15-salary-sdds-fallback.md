# #607 — приёмка резервного источника зарплат SDDS

Пункт `key-rate/phase-4-salary-sdds-fallback`, role:go, 2026-11-15.
Основание: [research #604](2026-11-15-macro-source-recovery.md), принятый PR #609.
[Спецификация](../superpowers/specs/2026-11-15-salary-sdds-fallback-design.md),
[план](../superpowers/plans/2026-11-15-salary-sdds-fallback.md).

## Результат

При недоступности основного `tab1-zpl` импортёр штатно использовал официальный SDDS.
Все опубликованные зарплатные месяцы уже были в dev: новый источник восстановил
работоспособность загрузки, но не увеличил свежесть данных. Текущий последний
отчётный месяц — август 2026, 103014 рублей; хранится как `2026-09-01`.
Пустая сентябрьская зарплата SDDS не превращается в ноль или новую точку.

## Dev-протокол

Writer проверен через Makefile: loopback `localhost:9000`; MCP — `kimi_reader`,
база `default`, локальный dev. Никакого staging/deploy или запуска/остановки БД.

1. MCP baseline: 1118 точек — зарплата 422 + шесть трудовых рядов по 116.
2. `make import STAT=rosstat_salaries_mes` в 19:27 (+05:00): основной HEAD404,
   файл не открывается как XLSX, fallback GET SDDS, `Imported 0 rows`, exit0.
3. Повтор той же команды в 19:28: отдельный ключ `rosstat_salaries_mes_sdds`,
   fingerprint `713bf1aba91946ac0fd5283257e4c1c0`, известная версия на 140 точек;
   SDDS всё равно прочитан для проверки множества дат; `Imported 0 rows`, exit0.
4. Дополнительный прогон с `ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-salary607`:
   `Imported 0 rows`, exit0. Сохранён отдельный объект
   `rosstat_salaries_mes_sdds/713bf1aba91946ac_SDDS_labor_market.xlsx` (21986 байт),
   SHA256 `70b53e0e07399f0ef9c0b2cf536a3881dbf242f4b5729690de57ed7b69107a1c` совпал
   с исследованием #604. В репозиторий включён только manifest, не исходная книга.
5. MCP после первого, второго и архивного прогонов: все 1118 значений и дат
   идентичны baseline. Промежуточные проверки сохранены как [сводка](salary-sdds-fallback/checkpoints.json), полный after.json — итог после архивного прогона. Зарплата — 422 точки, `1991-08-01`…`2026-09-01`;
   история до 2015 и шесть трудовых рядов сохранены. Каталог видит SDDS и
   объясняет missing-only/дату следующего месяца.

[Baseline](salary-sdds-fallback/baseline.json), [итог](salary-sdds-fallback/after.json),
[каталог](salary-sdds-fallback/catalog.json),
[первый лог](salary-sdds-fallback/import1.log), [повтор](salary-sdds-fallback/import2.log),
[архивный прогон](salary-sdds-fallback/archive-import.log),
[manifest](salary-sdds-fallback/archive-manifest.jsonl).

## Проверки

- `make all`: PASS — golangci-lint (0 issues), vet, race-тесты всего репозитория,
  обе сборки. [Лог](salary-sdds-fallback/make-all.log).
- Целевые race-тесты: PASS, включая повтор после сокращения фикстуры.
  Проверены 139 месяцев пересечения с primary-фикстурой, добавление только
  последнего отсутствующего месяца, повтор0, восстановление дыры при равном count,
  отдельная publication identity, HTTP/discovery/HTML200 маршрутизация,
  запрет fallback при semantic/DB/restore ошибках, подписи/единицы/даты/числа,
  отсутствие частичного Append и сохранение ошибки Append.
- Тест `PublicationAlwaysRead` проверяет известный fingerprint и равный count:
  чтение не пропускается, регистрация версии сохраняется. Default остальных
  импортёров не меняется.
- Независимое code-review gpt-6.1-terra: PASS после исправления count-only no-op
  и замены полной исходной книги минимальной фикстурой. Финальное независимое ревью проверило MCP-снимки, логи и manifest: PASS.
- `make docs-check`: PASS (64 страницы, 33 теста).
- `make mcp-check`: целевые проверки Росстата PASS; глобально FAIL2 по прежним
  контрольным числам fedstat: каталог1734 вместо1727, v_fedstat_ipc204101/1112/2
  вместо204038/1105/2. Эти же расхождения были до #607; их пороги/данные не менялись.
  [Сводка](salary-sdds-fallback/mcp-check-summary.log).

## Ограничения

SDDS только заполняет отсутствующие месяцы, а не пересматривает историю. Ошибки
семантики первичного XLSX, БД, батча и регистрации не скрываются переключением.
Проверка известных дат предполагает одного писателя; она не является транзакцией.
При включённом архиве сохраняются прочитанные байты; отсутствие backend не
объявляется успешной архивацией. Штатный dynamic importer без primary DataUrl
отклоняет archive restore до сети; автоматического восстановления SDDS нет.

Дальше после мержа — #608, контракт точности и согласованных периодов шести
трудовых рядов. Настоящая приёмка не означает, что эти ряды обновлены до сентября.
