# MOEX: покрытие рыночных рядов для CB_COMP (2026-10-20)

**Статус:** research; решение владельца — только MOEX, разрывы не подменяем.
**Связанные документы:** спека [2026-10-20-moex-market-design.md](../superpowers/specs/2026-10-20-moex-market-design.md), план [2026-10-20-moex-market.md](../superpowers/plans/2026-10-20-moex-market.md), issue [#113](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/113), [#114](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/114), [#76](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/76).

---

## 1. Короткий ответ

MOEX ISS даёт **три** из шести рядов, которые нужны для полного сырого набора CB_COMP:

| Ряд | MOEX | Что именно | Глубина |
|---|---|---|---|
| Brent | да | фьючерс `BR`, непрерывный фронт-месяц | с 2009-10-01 (первый фронт-месяц; контракт `BR-10.09` — с 2009-10-05) |
| RVI | да | индекс `RVI` | с 2013-11-18 |
| Медь | да | фьючерс `COPPER`, фронт-месяц | с 2024-10-08 |
| Urals | **нет** | `RURIURL` — фондовый индекс без данных | — |
| VIX | **нет** | `VIX6`/`VIZ6` — фьючерсы на RVI, не CBOE VIX | — |
| CRB | **нет** | индекса нет | — |

Для опубликованной модели CB_COMP эти шесть рядов не обязательны: в экспортированной панели `data_panel_bi_sa.fst` из рыночных индексов только `usd_rub` (756 строк = 189 месяцев × 4 вида), а режим `full` из #221 использует кредит/ставки/USD/тренд CPI. Шесть рядов лежат в `raw_data_store/market_data/` архива авторов, но в модель не входят.

## 2. Проверка MOEX ISS

### 2.1. Индексы

`/iss/engines/stock/markets/index/securities.json?iss.meta=off` — 905 индексов. Поиск по `RVI|VIX|URALS|CRB|COPPER|BRENT`:

- `RVI` — «Индекс RVI», есть свечи `interval=24` с 2013-11-18 (3 252 торговых дня);
- `RURIURL` — «Индекс Урала» / `Moscow Exchange Urals Index`, `GROUP=stock_index`, `ISSUEDATE=2008-01-09`, `boards` пуст, свечи и история пусты. Это фондовый индекс, не цена нефти;
- VIX, CRB, Brent/Urals как индексы — отсутствуют.

### 2.2. Фьючерсы

`/iss/engines/futures/markets/forts/securities.json?iss.meta=off`:

- `BR` — Brent, `UNIT = "В USD за 1 баррель"`, контракты с 2009 (`BRZ9_2009` — `BR-12.09`);
- `BRM` — мини-Brent, та же логика; в импортёре не используется, чтобы не смешивать два ряда;
- `COPPER` — медь, `UNIT = "В USD за 1000 килограмм"`, первый контракт `CEZ4` торгуется с 2024-10-08;
- `RVI` — фьючерсы на индекс RVI (`VIX6`, `VIZ6`), не VIX;
- Urals/VIX/CRB фьючерсов нет.

Исторические контракты обнаруживаются запросом
`/iss/history/engines/futures/markets/forts/securities.json?iss.meta=off&date=YYYY-MM-DD&assetcode=BR`:
фильтр `assetcode` работает, спреды приходят с пустым `ASSETCODE`. История контракта —
`/iss/history/engines/futures/markets/forts/securities/<secid>.json` с пагинацией по 100.

### 2.3. Единицы и поля

- Brent: `SETTLEPRICE` в USD/барр.; `CLOSE` в USD/барр.; в 2009–2010 у дальних контрактов `CLOSE=null`, `SETTLEPRICE` заполнен.
- Медь: `SETTLEPRICE` в USD/т; `CLOSE` в USD/т.
- RVI: `close` свечи — пункты индекса.

## 3. CB_COMP: где эти ряды

- `docs/research/2026-10-18-cb-comp-archive.md` §6.1: панель `data_panel_bi_sa.fst` — 183 654 строки; блок «Рыночные индексы» — 756 строк = `usd_rub` × 4 вида (`base_index`/`diff`/`trend`/`trend_deviation`), только Россия.
- §6.2: `raw_data_store/market_data/` — Brent, Urals, gold, copper, CRB, RVI, VIX; это сырой архив, не модельная панель.
- `docs/research/2026-10-19-cb-comp-feature-parity.md` §2: режим `full` добавляет кредит/ставки/USD/тренд CPI, а не товарные индексы/VIX.
- Вывод: для повторения опубликованной модели шесть рядов не нужны; для расширенного набора признаков или полного сырого паритета — нужны, но MOEX закрывает только три.

## 4. Что это значит для плана

1. **#113 Brent** — делаем на MOEX `BR` (фронт-месяц, `SETTLEPRICE`), несмотря на то что роадмап ранее указывал FRED `DCOILBRENTEU`. Решение владельца 2026-10-20: только MOEX.
2. **Новый пункт `phase-2-moex-market`** — RVI и медь; медь с 2024-10, поэтому в историческом окне CB_COMP 2010–2018 её нет.
3. **#114 Urals** — остаётся `Blocked`: MOEX не даёт ряда; `RURIURL` не является ценой нефти.
4. **VIX/CRB** — отдельного issue не заводим; разрыв зафиксирован здесь. Если понадобится live-ряд, источник придётся искать отдельно (FRED `VIXCLS` для VIX; для CRB — только платные/архивные источники).
5. **Полный паритет на шести рядах не заявляется.** Максимум на MOEX — Brent + RVI + медь (история меди короткая).

## 5. Команды проверки (2026-10-20)

```bash
curl -s 'https://iss.moex.com/iss/engines/stock/markets/index/securities.json?iss.meta=off'
curl -s 'https://iss.moex.com/iss/engines/futures/markets/forts/securities.json?iss.meta=off'
curl -s 'https://iss.moex.com/iss/history/engines/futures/markets/forts/securities.json?iss.meta=off&date=2009-10-05&assetcode=BR'
curl -s 'https://iss.moex.com/iss/history/engines/futures/markets/forts/securities/BRZ9_2009.json?iss.meta=off&limit=1'
curl -s 'https://iss.moex.com/iss/history/engines/stock/markets/index/securities/RVI.json?iss.meta=off&from=2000-01-01&limit=1'
curl -s 'https://iss.moex.com/iss/history/engines/futures/markets/forts/securities/CEZ4.json?iss.meta=off&limit=1'
```

Ожидаемые факты: `BRZ9_2009` (декабрьский контракт) — первая строка 2009-10-05; фронт-месяц `BR-10.09` — с 2009-10-01; RVI — 2013-11-18; `CEZ4` — 2024-10-08; `RURIURL` — пустые `boards`/`candles`/`history`.
