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

// TestFinancePolyusPanelIDsUnique проверяет и раскладку id (через Validate), и параметризацию
// Validate: тот же дашборд с чужим uid обязан быть отвергнут, иначе опечатка в uid прошла бы молча
// и один дашборд затёр бы другой при деплое.
func TestFinancePolyusPanelIDsUnique(t *testing.T) {
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

// TestFinancePolyusSpecConventions фиксирует конвенции каркаса одним проходом: id панелей 1..6 без
// пропусков и повторов, непустой Description у каждой (спека §4), ширина 24 у панели 2 и 12
// у остальных, FINAL и LIMIT в панельных запросах не пишутся (FINAL уже внутри витрин; LIMIT на
// временном ряду молча обрезал бы историю — anti-pattern легаси-экспорта).
func TestFinancePolyusSpecConventions(t *testing.T) {
	seen := make(map[uint32]bool, 6)
	for _, p := range panelsOf(builtFinancePolyus(t)) {
		if p.Id == nil {
			t.Fatalf("панель %q: не задан id", panelTitle(p))
		}
		id := *p.Id
		if id < 1 || id > 6 || seen[id] {
			t.Fatalf("панель %q: id %d вне 1..6 или повторяется", panelTitle(p), id)
		}
		seen[id] = true
		if p.Description == nil || strings.TrimSpace(*p.Description) == "" {
			t.Fatalf("панель %d: пустой Description (спека §4 — описание у каждой панели)", id)
		}
		wantSpan := uint32(12)
		if id == 2 {
			wantSpan = 24
		}
		if p.GridPos == nil || p.GridPos.W != wantSpan {
			t.Fatalf("панель %d: span %v, ожидался %d (спека §4)", id, p.GridPos, wantSpan)
		}
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
	if len(seen) != 6 {
		t.Fatalf("панелей %d, ожидалось 6 (id 1..6 без пропусков)", len(seen))
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
	b1, _ := json.MarshalIndent(Manifest(d1), "", "  ")
	b2, _ := json.MarshalIndent(Manifest(d2), "", "  ")
	if !bytes.Equal(b1, b2) {
		t.Fatal("генерация недетерминирована")
	}
}
