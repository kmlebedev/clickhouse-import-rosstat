package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// financePolyusRowTitles — ряды дашборда в порядке следования. Собран ряд 31 «Золото, ставки, рынок»
// (панели 1–6): список фиксирует и заголовок, и порядок сетки. Ряды 32–34 («Операционные показатели»,
// «Финансовые показатели», «Прогноз и точность») добавляются следующими задачами.
var financePolyusRowTitles = []string{
	"Золото, ставки, рынок",
}

func builtFinancePolyus(t *testing.T) dashboard.Dashboard {
	t.Helper()
	d, err := NewFinancePolyus().Build()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestFinancePolyusPanelCount фиксирует состав каркаса: ряд 31 — шесть панелей (id 1–6), полный
// контракт спеки §5 для ряда «Золото, ставки, рынок». Счёт растёт по мере добавления рядов 32–34
// (девятнадцать панелей в конце).
func TestFinancePolyusPanelCount(t *testing.T) {
	if panels := panelsOf(builtFinancePolyus(t)); len(panels) != 6 {
		t.Fatalf("панелей %d, ожидалось 6", len(panels))
	}
}

// TestFinancePolyusRowTitles: ряды нумеруются вне диапазона панелей 1..19 (31–34) — нулевой или
// совпавший с панелью id Grafana перезапишет своим, и артефакт разойдётся с кодом.
func TestFinancePolyusRowTitles(t *testing.T) {
	var titles []string
	for _, entry := range builtFinancePolyus(t).Panels {
		if entry.RowPanel == nil {
			continue
		}
		if entry.RowPanel.Id <= 19 {
			t.Fatalf("ряд %q: id %d попадает в диапазон панелей 1..19", optionalString(entry.RowPanel.Title), entry.RowPanel.Id)
		}
		titles = append(titles, optionalString(entry.RowPanel.Title))
	}
	if len(titles) != len(financePolyusRowTitles) {
		t.Fatalf("рядов %d, ожидалось %d: %v", len(titles), len(financePolyusRowTitles), titles)
	}
	for i, got := range titles {
		if got != financePolyusRowTitles[i] {
			t.Fatalf("ряд %d: %q, ожидался %q", i+1, got, financePolyusRowTitles[i])
		}
	}
}

// TestFinancePolyusAllTargetsUseViews: у датасорса стенда нет грантов на сырые таблицы (легаси-экспорт
// читал cbr_gold, cbr_currency_usd, databook_polyus, polyus_financial_metrics), поэтому каждый запрос
// каждой панели обязан читать витрину v_*.
func TestFinancePolyusAllTargetsUseViews(t *testing.T) {
	for _, p := range panelsOf(builtFinancePolyus(t)) {
		if len(p.Targets) == 0 {
			t.Fatalf("у панели %q нет запросов", panelTitle(p))
		}
		for _, target := range p.Targets {
			query, ok := chQuery(target)
			if !ok {
				t.Fatalf("панель %q: запрос не является запросом ClickHouse", panelTitle(p))
			}
			if !strings.Contains(query.RawSql, "FROM v_") {
				t.Fatalf("панель %q: запрос %q читает не витрину: %s", panelTitle(p), query.RefId, query.RawSql)
			}
		}
	}
}

// TestFinancePolyusValidateUID проверяет параметризацию Validate: дашборд проходит проверку со своим
// uid и отвергается с чужим — иначе опечатка в uid прошла бы молча, и один дашборд затёр бы другой
// при деплое. Id панелей здесь не проверяются: их уникальность и диапазон держит
// TestFinancePolyusSpecConventions, и имя теста обязано говорить, что именно он проверяет.
func TestFinancePolyusValidateUID(t *testing.T) {
	board := builtFinancePolyus(t)
	if err := Validate(board, FinancePolyusUID); err != nil {
		t.Fatalf("дашборд finance-polyus не прошёл валидацию: %v", err)
	}
	if err := Validate(board, KeyRatePublicationsUID); err == nil {
		t.Fatal("дашборд принят с чужим uid: проверка uid не параметризована")
	}
}

// TestFinancePolyusUnits: SQL панели 1 отдаёт учётную цену ЦБ в ₽/г (currencyRUB, 2 знака — цена
// с копейками), панели 2 — доллары за тройскую унцию (currencyUSD, у таргетов паритета и рыночных
// рядов одна единица), панели 3 — курс ₽/$ (short, 4 знака), панелей 4 и 5 — проценты годовых
// (уровни ставок, не темпы), панели 6 — рубли за акцию (currencyRUB). Перепутанная единица даёт
// ошибку чтения на порядки.
func TestFinancePolyusUnits(t *testing.T) {
	board := builtFinancePolyus(t)
	if unit := panelByID(t, board, 1).FieldConfig.Defaults.Unit; unit == nil || *unit != units.Rubles {
		t.Fatalf("панель 1: unit %v, ожидался %q (учётная цена ЦБ — ₽/г)", unit, units.Rubles)
	}
	if decimals := panelByID(t, board, 1).FieldConfig.Defaults.Decimals; decimals == nil || *decimals != 2 {
		t.Fatalf("панель 1: decimals %v, ожидалось 2 (учётная цена — рубли с копейками)", decimals)
	}
	if unit := panelByID(t, board, 2).FieldConfig.Defaults.Unit; unit == nil || *unit != units.Dollars {
		t.Fatalf("панель 2: unit %v, ожидался %q (золото — $/oz)", unit, units.Dollars)
	}
	usdRub := panelByID(t, board, 3)
	if unit := usdRub.FieldConfig.Defaults.Unit; unit == nil || *unit != units.Short {
		t.Fatalf("панель 3: unit %v, ожидался %q (курс — не деньги, а число ₽ за $)", unit, units.Short)
	}
	if decimals := usdRub.FieldConfig.Defaults.Decimals; decimals == nil || *decimals != 4 {
		t.Fatalf("панель 3: decimals %v, ожидалось 4 (курс ЦБ дают с четырьмя знаками)", decimals)
	}
	for _, id := range []uint32{4, 5} {
		if unit := panelByID(t, board, id).FieldConfig.Defaults.Unit; unit == nil || *unit != units.Percent {
			t.Fatalf("панель %d: unit %v, ожидался %q (ставки и ожидания — проценты годовых)", id, unit, units.Percent)
		}
	}
	if unit := panelByID(t, board, 6).FieldConfig.Defaults.Unit; unit == nil || *unit != units.Rubles {
		t.Fatalf("панель 6: unit %v, ожидался %q (цена акции — ₽)", unit, units.Rubles)
	}
}

// TestFinancePolyusDcfParityJoin: паритет ЦБ ($/oz) считается в SQL самоджойном витрины по дате —
// учётная цена ₽/г делится на курс того же дня и умножается на граммы в тройской унции (31.1034768).
// Ряды ЦБ разной длины, и клиентское деление на две серии взяло бы курс другой даты; LEFT JOIN, в
// свою очередь, дал бы точки без курса (NaN) вместо честного выпадения дня.
func TestFinancePolyusDcfParityJoin(t *testing.T) {
	panel := panelByID(t, builtFinancePolyus(t), 2)
	if len(panel.Targets) != 4 {
		t.Fatalf("у панели %q %d запросов, ожидалось 4 (паритет и три рыночных ряда)", panelTitle(panel), len(panel.Targets))
	}
	query := targetByRefID(t, panel, "A")
	for _, want := range []string{
		"r.value / usd.value * 31.1034768",
		"FROM v_cbr_macro AS r",
		"INNER JOIN v_cbr_macro AS usd",
		"$__timeFilter(r.date)",
		`AS "ЦБ-паритет, $/oz"`,
	} {
		if !strings.Contains(query.RawSql, want) {
			t.Fatalf("панель 2: запрос A не содержит %q: %s", want, query.RawSql)
		}
	}
	// Формула паритета читается только из Description: в легенде четыре однотипных «$/oz», и без
	// объяснения (учётная цена ₽/г ÷ курс × 31.1034768) серия A выглядела бы ещё одним рыночным рядом.
	description := optionalString(panel.Description)
	for _, want := range []string{"учётная цена", "курс", "31.1034768"} {
		if !strings.Contains(description, want) {
			t.Fatalf("Description панели 2 не объясняет формулу паритета (нет %q): %s", want, description)
		}
	}
	// B/C/D — рыночные ряды v_gold_prices: алиасы дословно из спеки §5, иначе в легенде было бы
	// три одинаковых «value», а опечатка в series молча оставила бы серию пустой.
	for _, tc := range []struct{ refID, series, alias string }{
		{"B", "moex_fix_usd", "МосБиржа фикс, $/oz"},
		{"C", "comex_front", "COMEX-фронт, $/oz"},
		{"D", "bold", "bold.report, $/oz"},
	} {
		row := targetByRefID(t, panel, tc.refID)
		for _, want := range []string{
			"FROM v_gold_prices",
			"source = 'gold'",
			"series = '" + tc.series + "'",
			`AS "` + tc.alias + `"`,
			"ORDER BY date",
		} {
			if !strings.Contains(row.RawSql, want) {
				t.Fatalf("панель 2: запрос %s не содержит %q: %s", tc.refID, want, row.RawSql)
			}
		}
	}
}

// TestFinancePolyusSparseSeriesPoints: теноры кривой ОФЗ (панель 4, B..E) сейчас — снимок по одному
// наблюдению на тенор (история кривой наполняется), поэтому рисуются точками, а не линиями; цвет
// фиксирован по тенору — палитра classic красит серии по порядку таргетов, и цвет кривой менялся бы
// при добавлении ряда.
func TestFinancePolyusSparseSeriesPoints(t *testing.T) {
	panel := panelByID(t, builtFinancePolyus(t), 4)
	for _, refID := range []string{"B", "C", "D", "E"} {
		assertPointsOnly(t, panel, refID)
		color, ok := overridePropByRefID(panel, refID, "color").(dashboard.FieldColor)
		if !ok || color.Mode != dashboard.FieldColorModeIdFixed || color.FixedColor == nil {
			t.Fatalf("панель 4: цвет серии %s = %#v, ожидался fixed (палитра classic красит по порядку таргетов)", refID, color)
		}
	}
}

// fpGoldRowLayout — ожидаемая раскладка панелей ряда 31 «Золото, ставки, рынок» (id → x/y/w/h):
// верхняя пара — цена ₽/г (1) и курс (3), панель 2 (24 колонки) — своя строка, ниже пара ставок (4/5)
// и цена акции (6). Таблица живёт в тесте, а не рядом с билдерами: она фиксирует раскладку (её видно
// в артефакте и в UI), а панель без позиции в таблице — ошибка, а не «дорисует авто-layout».
var fpGoldRowLayout = map[uint32]gridPos{
	1: {x: 0, y: 1, w: 12, h: 9},
	3: {x: 12, y: 1, w: 12, h: 9},
	2: {x: 0, y: 10, w: 24, h: 9},
	4: {x: 0, y: 19, w: 12, h: 9},
	5: {x: 12, y: 19, w: 12, h: 9},
	6: {x: 0, y: 28, w: 12, h: 9},
}

// gridPos — прямоугольник элемента сетки Grafana (24 колонки): раскладка ряда задана явно, поэтому
// пересечение прямоугольников или выход за 24 колонки — ошибка кода, а не «Grafana разберётся».
type gridPos struct {
	x, y, w, h uint32
}

// overlaps: касание краями (x+w == чужой x) пересечением не считается.
func (r gridPos) overlaps(other gridPos) bool {
	return r.x < other.x+other.w && other.x < r.x+r.w &&
		r.y < other.y+other.h && other.y < r.y+r.h
}

// layoutRect — элемент дашборда в сетке с подписью для сообщения об ошибке: в проверке пересечений
// участвуют и панели, и ряды (ряд — такая же полоса сетки шириной 24 колонки).
type layoutRect struct {
	label string
	pos   gridPos
}

// TestFinancePolyusSpecConventions фиксирует конвенции каркаса одним проходом: раскладку панелей
// 1–6 по таблице fpGoldRowLayout (позиция задана явно, а не выведена авто-layout'ом SDK — тот ставил
// панель 2 шириной 24 колонки в курсор после панели 1, то есть x=12..36, за правую границу сетки),
// непустой Description у каждой панели (спека §4), id уникальные в 1..19, FINAL и LIMIT в панельных
// запросах не пишутся (FINAL уже внутри витрин; LIMIT на временном ряду молча обрезал бы историю —
// anti-pattern легаси-экспорта). Общие инварианты сетки: x + w <= 24, y >= 1 (для SDK x=0 и y=0 —
// «позиция не задана», её перезапишет авто-layout), прямоугольники не пересекаются — ни панели между
// собой, ни ряд с панелью.
func TestFinancePolyusSpecConventions(t *testing.T) {
	board := builtFinancePolyus(t)
	panels := panelsOf(board)
	if len(panels) != len(fpGoldRowLayout) {
		t.Fatalf("панелей %d, в таблице раскладки %d: панель ряда 31 добавляется вместе со своей позицией", len(panels), len(fpGoldRowLayout))
	}

	seen := make(map[uint32]bool, len(panels))
	rects := make([]layoutRect, 0, len(panels)+1)
	for _, p := range panels {
		if p.Id == nil {
			t.Fatalf("панель %q: не задан id", panelTitle(p))
		}
		id := *p.Id
		if id < 1 || id > 19 || seen[id] {
			t.Fatalf("панель %q: id %d вне 1..19 или повторяется", panelTitle(p), id)
		}
		seen[id] = true
		if p.Description == nil || strings.TrimSpace(*p.Description) == "" {
			t.Fatalf("панель %d: пустой Description (спека §4 — описание у каждой панели)", id)
		}

		want, ok := fpGoldRowLayout[id]
		if !ok {
			t.Fatalf("панель %d (%q): нет позиции в таблице раскладки ряда 31 — новая панель получает явный GridPos и попадает в таблицу", id, panelTitle(p))
		}
		if p.GridPos == nil {
			t.Fatalf("панель %d: gridPos не задан (авто-layout положит панель в курсор предыдущей)", id)
		}
		got := gridPos{x: p.GridPos.X, y: p.GridPos.Y, w: p.GridPos.W, h: p.GridPos.H}
		if got != want {
			t.Fatalf("панель %d: gridPos {x:%d y:%d w:%d h:%d}, ожидался {x:%d y:%d w:%d h:%d}",
				id, got.x, got.y, got.w, got.h, want.x, want.y, want.w, want.h)
		}
		if got.y < 1 {
			t.Fatalf("панель %d: y=%d — для SDK это часть «позиция не задана» (x=0 и y=0), и авто-layout перезапишет её", id, got.y)
		}
		if got.x+got.w > 24 {
			t.Fatalf("панель %d: x=%d + w=%d выходит за 24 колонки сетки", id, got.x, got.w)
		}
		rects = append(rects, layoutRect{panelTitle(p), got})

		for _, target := range p.Targets {
			query, ok := chQuery(target)
			if !ok {
				t.Fatalf("панель %d: запрос не является запросом ClickHouse", id)
			}
			if strings.Contains(strings.ToUpper(query.RawSql), "FINAL") {
				t.Fatalf("панель %d: FINAL в панельном запросе (FINAL — внутри витрин): %s", id, query.RawSql)
			}
			if query.QueryType == queryTypeTimeseries && strings.Contains(query.RawSql, "LIMIT") {
				t.Fatalf("панель %d: LIMIT в timeseries-запросе (молча обрезает историю): %s", id, query.RawSql)
			}
		}
	}

	for _, entry := range board.Panels {
		if entry.RowPanel == nil || entry.RowPanel.GridPos == nil {
			continue
		}
		row := gridPos{x: entry.RowPanel.GridPos.X, y: entry.RowPanel.GridPos.Y, w: entry.RowPanel.GridPos.W, h: entry.RowPanel.GridPos.H}
		if row.x+row.w > 24 {
			t.Fatalf("ряд %q: x=%d + w=%d выходит за 24 колонки сетки", optionalString(entry.RowPanel.Title), row.x, row.w)
		}
		rects = append(rects, layoutRect{"ряд " + optionalString(entry.RowPanel.Title), row})
	}

	for i := range rects {
		for j := i + 1; j < len(rects); j++ {
			a, b := rects[i], rects[j]
			if a.pos.overlaps(b.pos) {
				t.Fatalf("прямоугольники пересекаются: %s {x:%d y:%d w:%d h:%d} и %s {x:%d y:%d w:%d h:%d}",
					a.label, a.pos.x, a.pos.y, a.pos.w, a.pos.h, b.label, b.pos.x, b.pos.y, b.pos.w, b.pos.h)
			}
		}
	}
}

// TestFinancePolyusGenerateDeterministic — как TestGenerateDeterministic в gold и key-rate: на
// детерминизме генерации стоит CI-дифф артефакта, поэтому две сборки обязаны дать байт-идентичный JSON.
func TestFinancePolyusGenerateDeterministic(t *testing.T) {
	d1, err := NewFinancePolyus().Build()
	if err != nil {
		t.Fatal(err)
	}
	d2, err := NewFinancePolyus().Build()
	if err != nil {
		t.Fatal(err)
	}
	b1, err := json.MarshalIndent(Manifest(d1), "", "  ")
	if err != nil {
		t.Fatalf("сериализация первой сборки: %v", err)
	}
	b2, err := json.MarshalIndent(Manifest(d2), "", "  ")
	if err != nil {
		t.Fatalf("сериализация второй сборки: %v", err)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatal("генерация недетерминирована")
	}
}
