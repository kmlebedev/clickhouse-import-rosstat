### Task 4: MCP-проверки

**Files:**
- Modify: `scripts/mcp_check.py:514-590` (блок Росстата)

**Interfaces:**
- Consumes: имя импортёра `rosstat_labour_force_mes`, сырая таблица `rosstat_labour_force_mes`, витрина `v_rosstat_macro`, каталог `source='rosstat'` (Task 3).
- Produces: проверки приёмки.

- [ ] **Step 1: Обновить проверки `scripts/mcp_check.py`**

Четыре правки в блоке Росстата:
1. строка 546-547 — в перечень «прогнать импортёры» добавить `rosstat_labour_force_mes`;
2. строка 566 — ожидаемое число рядов каталога `141` → `147`, строка 567 — метка: `"рядов rosstat: 147 (4 ИПЦ + 131 недельный + 5 ВВП + 1 зарплата + 6 рабочая сила)"`;
3. строка 569-574 — в текст расхождения `want 141` → `want 147` и в перечень импортёров добавить пятое имя;
4. строки 582-585 — в кортеж закрытых сырых таблиц добавить `("rosstat_labour_force_mes", "рабочая сила и безработица")`.

- [ ] **Step 2: Проверить синтаксис и расписание**

```bash
python3 -m py_compile scripts/mcp_check.py && echo "py ok"
dagu validate dagu/rosstat.yaml
go test -race -count=1 ./rosstat/
```

Expected: `py ok`; `dagu validate` — exit 0 (единственное сообщение — общее для всех DAG-файлов предупреждение `auth.mode`); тесты пакета зелёные.

- [ ] **Step 3: Коммит**

```bash
git add scripts/mcp_check.py
git commit -m "extend mcp checks for rosstat_labour_force_mes"
```

---

