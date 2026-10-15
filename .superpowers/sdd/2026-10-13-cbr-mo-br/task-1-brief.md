### Task 1: Фикстура `cbr/testdata/mo_br_full.xlsx` (zip-хирургия)

**Files:**
- Create: `cbr/testdata/mo_br_full.xlsx`

**Interfaces:**
- Produces: файл-фикстура (листы «1», «3», «16») — вход всех тестов T3; в отчёте — фактические числа (ряды/точки/споты), которые T3 вписывает в ожидания.

- [ ] **Step 1: Скачать живой файл**

```bash
curl -sS --cacert russian_trusted_root_ca_pem.crt --max-time 120 \
  -o /tmp/mo_br_full.xlsx \
  "https://www.cbr.ru/Content/Document/File/144490/full.xlsx"
```

Ожидание: ≈301 КБ; sha256 на 2026-10-13 — `fdd3b32693ac7cae212f38c091275351807d9e5fda6ffb55e539182b517da3f7` (если ЦБ уже опубликовал октябрьскую волну — файл свежее, это нормально; зафиксируй факт).

- [ ] **Step 2: Zip-хирургия до трёх листов**

Адаптируй скрипт из `docs/superpowers/plans/2026-10-13-cbr-mp-survey.md` (Task 2, Step 1): python3 + zipfile, `keep_names={'1','3','16'}`; удалить ненужные sheet-парты с правкой `xl/workbook.xml`, `xl/_rels/workbook.xml.rels`, `[Content_Types].xml`:

```bash
python3 - <<'PY'
import zipfile, re
src='/tmp/mo_br_full.xlsx'; dst='cbr/testdata/mo_br_full.xlsx'
keep_names={'1','3','16'}
# ... (точная логика — из прецедента mp-survey, адаптируй keep_names)
PY
```

- [ ] **Step 3: Проверить фикстуру и снять фактические числа**

```bash
python3 - <<'PY'
import openpyxl, datetime
wb = openpyxl.load_workbook('cbr/testdata/mo_br_full.xlsx', data_only=True, read_only=True)
print(wb.sheetnames)
WANT = {"Медиана","Макс","90-й процентиль","10-й процентиль","Мин","Всего ответов"}
# посчитать: число рядов и точек по 6 статистикам + споты (см. скрипт-прецедент в сессии/спеке §2)
PY
```

Ожидание (для листов 1/3/16 из файла сентября 2026): **114 рядов, 2 058 точек** (900+900+258); споты сентябрьской волны: `key_rate_avg_2026_median` = 14.5, `key_rate_avg_2027_median` = 12.43, `cpi_dec_2026_median` = 6.6, `neutral_rate_median` = 8.0. **Зафиксируй фактические значения из своей фикстуры** (если волна успела обновиться — числа и споты будут другие; в тесты T3 идут фактические).

- [ ] **Step 4: Commit**

```bash
git add cbr/testdata/mo_br_full.xlsx
git commit -m "cbr: mo_br test fixture (3 sheets of full.xlsx)"
```

---

