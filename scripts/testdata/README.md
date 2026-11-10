# Фикстуры платформенных Python-инструментов

Модельные фикстуры (ИПЦ, CB_COMP, ставка, Polyus PDF) переехали вместе с моделями в
[`kmlebedev/bi-ml-models`](https://github.com/kmlebedev/bi-ml-models) — каталог `testdata/`.

Здесь остаётся только то, что нужно импортёрам и коллекторам платформы:

| Каталог | Что внутри |
|---|---|
| `edisclosure/` | снимок e-disclosure для тестов `edisclosure_collect.py`: компании, документы, события, baseline и лог антибот-прогона |

Офлайн-тесты платформенных скриптов запускаются напрямую, без ClickHouse и сети:

```bash
python3 scripts/edisclosure_collect_test.py -v
python3 scripts/harvest_test.py
```
