package main

import (
	"strings"
	"testing"

	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// transaqTradesRowTitles — ряды дашборда в порядке следования: ряд 21 «Итоги периода» (панели 1–2)
// и ряд 22 «Объёмы по интервалам» (панели 3–5).
var transaqTradesRowTitles = []string{"Итоги периода", "Объёмы по интервалам"}

func builtTransaqTrades(t *testing.T) dashboard.Dashboard {
	t.Helper()
	d, err := NewTransaqTrades().Build()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestTransaqTradesHasFivePanels фиксирует состав: две top-10 панели и три графика; из шести панелей
// источника per-trade панель и ETF-панель не переносятся (спека §6).
func TestTransaqTradesHasFivePanels(t *testing.T) {
	if panels := panelsOf(builtTransaqTrades(t)); len(panels) != 5 {
		t.Fatalf("панелей %d, ожидалось 5", len(panels))
	}
}

// TestTransaqTradesRowTitles: ряды нумеруются вне диапазона панелей 1..5 — нулевой или совпавший с
// панелью id Grafana перезапишет своим, и артефакт разойдётся с кодом.
func TestTransaqTradesRowTitles(t *testing.T) {
	var titles []string
	for _, entry := range builtTransaqTrades(t).Panels {
		if entry.RowPanel == nil {
			continue
		}
		if entry.RowPanel.Id <= 5 {
			t.Fatalf("ряд %q: id %d попадает в диапазон панелей 1..5", optionalString(entry.RowPanel.Title), entry.RowPanel.Id)
		}
		titles = append(titles, optionalString(entry.RowPanel.Title))
	}
	if len(titles) != len(transaqTradesRowTitles) {
		t.Fatalf("рядов %d, ожидалось %d: %v", len(titles), len(transaqTradesRowTitles), titles)
	}
	for i, got := range titles {
		if got != transaqTradesRowTitles[i] {
			t.Fatalf("ряд %d: %q, ожидался %q", i+1, got, transaqTradesRowTitles[i])
		}
	}
}

// ttPanelContract — ожидаемый контракт одного таргета панели: витрина, фильтр диапазона, режим
// торгов, наличие/отсутствие LIMIT и адаптивного бакета. Один таргет на панель — дубли источника
// (вторые запросы панелей 12/13) осознанно не переносятся.
type ttPanelContract struct {
	id        uint32
	view      string
	filter    string
	board     string
	limit     bool
	interval  bool
	format    int
	queryType string
}

// TestTransaqTradesSQLContract: панели 1–2 читают дневную витрину с $__dateFilter (Date-колонка),
// панели 3–5 — минутную с $__timeFilter и $__timeInterval; LIMIT есть только у top-10. Сырые
// transaq_* не читаются — у kimi_reader на них нет грантов, и Validate такое не пропустит.
func TestTransaqTradesSQLContract(t *testing.T) {
	board := builtTransaqTrades(t)
	cases := []ttPanelContract{
		{1, "v_transaq_net_flows_daily", "$__dateFilter(date)", "TQBR", true, false, queryFormatTable, queryTypeTable},
		{2, "v_transaq_net_flows_daily", "$__dateFilter(date)", "TQBR", true, false, queryFormatTable, queryTypeTable},
		{3, "v_transaq_net_flows", "$__timeFilter(minute)", "TQBR", false, true, queryFormatTimeseries, queryTypeTimeseries},
		{4, "v_transaq_net_flows", "$__timeFilter(minute)", "TQBR", false, true, queryFormatTimeseries, queryTypeTimeseries},
		{5, "v_transaq_net_flows", "$__timeFilter(minute)", "TQOB", false, true, queryFormatTimeseries, queryTypeTimeseries},
	}
	for _, tc := range cases {
		panel := panelByID(t, board, tc.id)
		if len(panel.Targets) != 1 {
			t.Fatalf("панель %d: таргетов %d, ожидался 1", tc.id, len(panel.Targets))
		}
		query, ok := chQuery(panel.Targets[0])
		if !ok {
			t.Fatalf("панель %d: таргет не является запросом ClickHouse", tc.id)
		}
		if !strings.Contains(query.RawSql, "FROM "+tc.view) {
			t.Fatalf("панель %d: запрос не читает витрину %s:\n%s", tc.id, tc.view, query.RawSql)
		}
		if got := strings.Count(query.RawSql, tc.filter); got != 1 {
			t.Fatalf("панель %d: %q встречается %d раз, ожидался 1:\n%s", tc.id, tc.filter, got, query.RawSql)
		}
		if !strings.Contains(query.RawSql, "board = '"+tc.board+"'") {
			t.Fatalf("панель %d: нет фильтра режима торгов %s:\n%s", tc.id, tc.board, query.RawSql)
		}
		if tc.limit {
			if !strings.Contains(query.RawSql, "LIMIT 10") {
				t.Fatalf("панель %d: top-10 без LIMIT 10:\n%s", tc.id, query.RawSql)
			}
		} else if strings.Contains(strings.ToUpper(query.RawSql), "LIMIT") {
			t.Fatalf("панель %d: LIMIT на графике обрезает ряд:\n%s", tc.id, query.RawSql)
		}
		if tc.interval != strings.Contains(query.RawSql, "$__timeInterval(minute)") {
			t.Fatalf("панель %d: $__timeInterval(minute) ожидался=%v:\n%s", tc.id, tc.interval, query.RawSql)
		}
		if query.Format != tc.format || query.QueryType != tc.queryType {
			t.Fatalf("панель %d: формат %d/%q, ожидался %d/%q", tc.id, query.Format, query.QueryType, tc.format, tc.queryType)
		}
	}
}

// TestTransaqTradesNoRawTablesAndNoFill: ни один таргет не читает сырые transaq_* (только витрины
// v_*), и ни один не применяет WITH FILL — пропуск в ленте означает «нет данных или разрыв», а не
// ноль, поэтому подставлять нули нельзя (спека §7, правило «пропуск ≠ ноль»).
func TestTransaqTradesNoRawTablesAndNoFill(t *testing.T) {
	for _, panel := range panelsOf(builtTransaqTrades(t)) {
		for _, target := range panel.Targets {
			query, ok := chQuery(target)
			if !ok {
				t.Fatalf("панель %q: таргет не является запросом ClickHouse", panelTitle(panel))
			}
			for _, raw := range []string{"transaq_trades", "transaq_securities", "transaq_net_flows"} {
				if strings.Contains(query.RawSql, "FROM "+raw) || strings.Contains(query.RawSql, "JOIN "+raw) {
					t.Fatalf("панель %q: запрос читает сырую таблицу %s:\n%s", panelTitle(panel), raw, query.RawSql)
				}
			}
			if strings.Contains(query.RawSql, "WITH FILL") {
				t.Fatalf("панель %q: WITH FILL подставляет нули вместо неизвестных данных:\n%s", panelTitle(panel), query.RawSql)
			}
		}
	}
}

// TestTransaqTradesUnits: все панели — рубли (currencyRUB) без копеек: объёмы сделок округляются
// до рублей, иначе ось пестрит копейками. Перепутанная единица даёт ошибку чтения на порядки.
func TestTransaqTradesUnits(t *testing.T) {
	for _, panel := range panelsOf(builtTransaqTrades(t)) {
		if unit := panel.FieldConfig.Defaults.Unit; unit == nil || *unit != units.Rubles {
			t.Fatalf("панель %q: unit %v, ожидался %q", panelTitle(panel), unit, units.Rubles)
		}
		if decimals := panel.FieldConfig.Defaults.Decimals; decimals == nil || *decimals != 0 {
			t.Fatalf("панель %q: decimals %v, ожидалось 0", panelTitle(panel), decimals)
		}
	}
}

// TestTransaqTradesNoTemplating: переменные шаблона источника ($interval, $sec_code, $sec_code_etf)
// не переносятся — адаптивный шаг даёт $__timeInterval, режим торгов задан в SQL, а неиспользуемая
// $sec_code в источнике не участвовала вовсе.
func TestTransaqTradesNoTemplating(t *testing.T) {
	if list := builtTransaqTrades(t).Templating.List; len(list) != 0 {
		t.Fatalf("в дашборде %d переменных шаблона, ожидалось 0", len(list))
	}
}

// TestTransaqTradesValidateUID проверяет параметризацию Validate: опечатка в uid иначе прошла бы
// молча, и при деплое один дашборд затёр бы другой.
func TestTransaqTradesValidateUID(t *testing.T) {
	board := builtTransaqTrades(t)
	if err := Validate(board, TransaqTradesUID); err != nil {
		t.Fatalf("дашборд transaq-trades не прошёл валидацию: %v", err)
	}
	if err := Validate(board, GoldThesisUID); err == nil {
		t.Fatal("дашборд принят с чужим uid: проверка uid не параметризована")
	}
}
