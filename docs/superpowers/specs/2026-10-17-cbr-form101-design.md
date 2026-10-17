# cbr_form101: помесячные остатки РСБУ по счетам (первый срез — Сбер)

**Дата:** 17 октября 2026 г.
**Пункт роадмапа:** [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §4.2 №21.
**Issue:** [#95](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/95).

## 1. Зачем

Сид `sql/sber_ras_monthly_seed_2026_09.sql` (issue #93) закрывает только сентябрь 2026 из пресс-релиза Сбера. Нужен регулярный машиночитаемый источник РСБУ по кредитованию. Форма 101 ЦБ — единственный стабильный помесячный источник остатков по счетам всех банков; она отстаёт от пресс-релиза примерно на месяц, но пригодна для систематического пополнения.

## 2. Источник

- Страница: `https://www.cbr.ru/banking_sector/otchetnost-kreditnykh-organizaciy/`.
- Ссылка: `/vfs/credit/forms/101-YYYYMMDD.rar` (дата — отчётная; последняя на 2026-10-17 — `101-20260901.rar`).
- RAR v5, ≈350 КБ; внутри `YYYYMMB1.dbf` (остатки, 25 464 записи), `NAMES.dbf` (счета), `YYYYMMN1.dbf` (банки).
- `B1.dbf`: dBase III, CP866; поля `REGN` (N,4), `PLAN` (C,1), `NUM_SC` (C,5), `A_P` (C,1), `VR/VC/VG/VM` (N,16), `VITG` (N,20,4), `DT` (C,8), `PRIZ` (N,1).
- Сбер: `REGN=1481`, `NAME_B='СБЕРБАНК РОССИИ'`; 194 строки в `B1.dbf` на 01.09.2026.
- Остатки — в тыс. руб. (проверяется на сверке; в таблицу пишутся как есть).
- Группы `45.0`/`45.2` и детальные счета 44x/45x — сырые коды; маппинг на «кредиты ЮЛ/ФЛ» — следующий шаг, не этот.

## 3. Модель данных

Таблица `cbr_form101`:

```sql
CREATE TABLE IF NOT EXISTS cbr_form101 (
    report_date Date,
    regnum UInt32,
    account LowCardinality(String),
    plan LowCardinality(String),
    active_passive LowCardinality(String),
    balance_total Float64,
    loaded_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (report_date, regnum, account, active_passive);
```

- `report_date` — дата из имени файла (`2026-09-01`), не отчётный месяц.
- `balance_total` — `VITG`, тыс. руб., как в источнике.
- Первый срез — только Сбер (`REGN=1481`), чтобы таблица была маленькой; схема готова к другим `regnum`.
- Повторный импорт той же даты — no-op по гейту `SELECT max(report_date) FROM cbr_form101 FINAL`.

Витрина `v_cbr_form101` — long-форма сырых остатков:

```sql
SELECT report_date, regnum, account, plan, active_passive, balance_total, loaded_at
FROM cbr_form101 FINAL
ORDER BY report_date DESC, regnum, account, active_passive
```

Комментарий: сырые остатки формы 101 (Сбер), `account` — код/группа счёта, `report_date` — дата отчёта, значение в тыс. руб.; это не готовая метрика кредитования — маппинг счетов отдельным шагом.

## 4. Импортёр `cbr_form101`

1. DDL таблицы, витрина (в начале, включая no-op-путь — самолечение).
2. `util.GetPage` страницы, goquery: все `a[href*="/vfs/credit/forms/101-"]`, дата из имени, максимум. Нет ссылок — ошибка.
3. Гейт: `max(report_date)`; если `latest <= max` — `log.Infof` и возврат без скачивания.
4. `util.GetFile` RAR; `rardecode/v2`; найти entry с суффиксом `B1.dbf` (регистронезависимо); прочитать.
5. DBF-парсер: заголовок (record count, header length, record length), дескрипторы полей, записи; CP866 через `golang.org/x/text/encoding/charmap`; числовые поля — right-aligned ASCII, `strconv.ParseFloat` с проверкой; удалённые записи (`*`) пропускаются.
6. Фильтр `REGN=1481`; строки с пустым `NUM_SC` пропускаются.
7. Один `PrepareBatch`/`Append`/`Send`; ошибки `Append` проверяются; при ошибке — `Abort`.
8. `util.CreateView` витрины; `AfterImport`-каталог не нужен: это сырые строки, не series_catalog (метрики появятся на следующем шаге).
9. Регистрация: `cbr/importers.go` (`cbr_form101`, countPoints `SELECT count() FROM cbr_form101 FINAL`), шаг в хвост `dagu/cbr.yaml`.
10. Грант `v_cbr_form101` в `sql/mcp_kimi_reader.sql`; MCP-проверка в `scripts/mcp_check.py` (витрина непуста, строки Сбера за 2026-09-01, сырая таблица закрыта).

## 5. Тесты

- `parseForm101DBF` — на маленькой DBF-фикстуре (2–3 записи, включая Сбера и удалённую), проверка полей/кодировки/чисел.
- `latestForm101Link` — на фикстуре HTML с несколькими `101-*.rar`, выбор максимальной даты.
- Гейт по дате — на моке `max(report_date)` (без ClickHouse; функция вынесена).
- Интеграционный живой прогон — `make import STAT=cbr_form101`.

## 6. Приёмка

1. `make all` зелёный.
2. `make import STAT=cbr_form101` — скачивает `101-20260901.rar`, пишет строки Сбера за `2026-09-01` (≈194), повтор — no-op без скачивания.
3. `SELECT count(), min(report_date), max(report_date) FROM cbr_form101 FINAL` — строки Сбера.
4. `v_cbr_form101` непуста; `make mcp-check` — новая проверка; сырая `cbr_form101` закрыта.
5. `dagu validate dagu/cbr.yaml` — exit 0.
6. `ARCHITECTURE.md` §6.1/§6.2/§6.3, README, `docs/ROADMAP_KEY_RATE.md` §4.2 №21 обновлены; статус — ✅ первого среза (сырые остатки), маппинг метрик — следующий шаг.

## 7. Вне объёма

- Маппинг счетов на `loans_corporations`/`loans_individuals` и запись в `company_financials`.
- Все банки (сейчас только Сбер).
- Разбор помесячного пресс-релиза Сбера (HTML JS / PDF-картинки).

## 8. Риски

| Риск | Митигация |
|---|---|
| RAR v5 библиотека | `github.com/nwaples/rardecode/v2` v2.4.5 проверена на живом `101-20260901.rar` (извлекла `082026B1.dbf`) |
| CP866/DBF-формат | собственный парсер + фикстура; числа проверяются `strconv` |
| Смена разметки страницы | 0 ссылок — ошибка; фикстура HTML |
| Единицы тыс. руб. | документировано в DDL/витрине; сверка на следующем шаге |
| Отставание формы 101 | пресс-релиз остаётся быстрым сидом; форма 101 — систематический слой |
