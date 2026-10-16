package main

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/grafana/grafana-foundation-sdk/go/barchart"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// financePolyusRowTitles — ряды дашборда в порядке следования. Собраны ряды 31 «Золото, ставки,
// рынок» (панели 1–6), 32 «Операционные показатели» (панели 7–11), 33 «Финансовые показатели»
// (панели 12–15) и 34 «Прогноз и точность» (16–19): список фиксирует и заголовки, и порядок сетки.
var financePolyusRowTitles = []string{
	"Золото, ставки, рынок",
	"Операционные показатели",
	"Финансовые показатели",
	"Прогноз и точность",
}

func builtFinancePolyus(t *testing.T) dashboard.Dashboard {
	t.Helper()
	d, err := NewFinancePolyus().Build()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestFinancePolyusHasNineteenPanels фиксирует состав собранных рядов: ряд 31 — шесть панелей
// (id 1–6), ряд 32 — пять (7–11), ряд 33 — четыре (12–15), ряд 34 — четыре (16–19). Итог — все
// девятнадцать панелей спеки §5.
func TestFinancePolyusHasNineteenPanels(t *testing.T) {
	if panels := panelsOf(builtFinancePolyus(t)); len(panels) != 19 {
		t.Fatalf("панелей %d, ожидалось 19", len(panels))
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
// (уровни ставок, не темпы), панели 6 — рубли за акцию (currencyRUB), панелей 7–11 — операционные
// показатели датапака (short: koz/kT/тыс. м³/м³/т/г/т/%), панелей 12/13/15 — денежные показатели
// отчётности ($ млн и $/oz, currencyUSD), панели 14 — продажи золота (short: koz), панелей 16/17 —
// цена золота ($/oz, currencyUSD), а у таблиц 18/19 панельной единицы нет: единицы их колонок
// (₽, $/oz, %) подписаны в алиасах. Перепутанная единица даёт ошибку чтения на порядки.
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
	// Панели 7–11 — операционные показатели датапака: koz, kT, тыс. м³, м³/т, г/т и % подписаны
	// в алиасах и заголовках, а единица Grafana у всех одна — short (koz/kT — не валюты, %, м³/т и
	// г/т — не доли: currencyRUB/percent дали бы «₽» и «%» на числах другой размерности).
	for _, id := range []uint32{7, 8, 9, 10, 11} {
		if unit := panelByID(t, board, id).FieldConfig.Defaults.Unit; unit == nil || *unit != units.Short {
			t.Fatalf("панель %d: unit %v, ожидался %q (единицы koz/kT/тыс. м³/м³/т/г/т — в алиасах и заголовке)", id, unit, units.Short)
		}
	}
	// Панели 12/13/15 — показатели отчётности в долларах ($ млн и $/oz): валюта, а не «число» —
	// currencyUSD печатает знак $ и разряды. Панель 14 — продажи золота в koz: единица short, как у
	// выпуска (панели 7/8): currencyUSD дал бы «$» на тысячах унций.
	for _, id := range []uint32{12, 13, 15} {
		if unit := panelByID(t, board, id).FieldConfig.Defaults.Unit; unit == nil || *unit != units.Dollars {
			t.Fatalf("панель %d: unit %v, ожидался %q (показатели отчётности — $ млн и $/oz)", id, unit, units.Dollars)
		}
	}
	if unit := panelByID(t, board, 14).FieldConfig.Defaults.Unit; unit == nil || *unit != units.Short {
		t.Fatalf("панель 14: unit %v, ожидался %q (продажи золота — koz, не валюта)", unit, units.Short)
	}
	// Панели 16 (факт и точки сценариев) и 17 (бар сценариев и консенсуса) — цена золота в долларах
	// за унцию: та же единица currencyUSD, что у панели 2. Таблицы 18/19 — сводки с колонками разных
	// единиц (₽, $/oz, %): панельной единицы у них нет, единица каждой колонки подписана в её алиасе,
	// а панельный currencyUSD подписал бы рубли долларами.
	for _, id := range []uint32{16, 17} {
		if unit := panelByID(t, board, id).FieldConfig.Defaults.Unit; unit == nil || *unit != units.Dollars {
			t.Fatalf("панель %d: unit %v, ожидался %q (цена золота — $/oz)", id, unit, units.Dollars)
		}
	}
	for _, id := range []uint32{18, 19} {
		if unit := panelByID(t, board, id).FieldConfig.Defaults.Unit; unit != nil {
			t.Fatalf("панель %d: unit %q, ожидалось отсутствие панельной единицы (единицы колонок — в алиасах)", id, *unit)
		}
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
// при добавлении ряда. Цвет сверяется ДОСЛОВНО по тенору (как цвета сценариев панели 16): оттенок
// кодирует срок (синий темнеет с тенором), и перестановка оттенков местами прошла бы проверку
// «fixed», а кривая читалась бы наоборот.
func TestFinancePolyusSparseSeriesPoints(t *testing.T) {
	panel := panelByID(t, builtFinancePolyus(t), 4)
	for _, tc := range []struct{ refID, color string }{
		{"B", "super-light-blue"},
		{"C", "light-blue"},
		{"D", "blue"},
		{"E", "dark-blue"},
	} {
		assertPointsOnly(t, panel, tc.refID)
		color, ok := overridePropByRefID(panel, tc.refID, "color").(dashboard.FieldColor)
		if !ok || color.Mode != dashboard.FieldColorModeIdFixed || color.FixedColor == nil || *color.FixedColor != tc.color {
			t.Fatalf("панель 4: цвет серии %s = %#v, ожидался fixed %q (оттенок кодирует тенор)", tc.refID, color, tc.color)
		}
	}
}

// TestFinancePolyusRightAxis: у панелей 10 («Вскрыша и коэффициент вскрыши») и 11 («Содержание и
// извлечение») серия B измеряется в других единицах, чем A (м³/т против тыс. м³; % против г/т), и
// стоит на правой оси — на общей левой она была бы нечитаемой линией у нуля. Override — пер-серийный:
// опечатка в refId молча оставила бы B на левой оси, а свойство у A — сломало бы левую шкалу.
func TestFinancePolyusRightAxis(t *testing.T) {
	board := builtFinancePolyus(t)
	for _, tc := range []struct {
		panelID uint32
		refID   string
	}{{10, "B"}, {11, "B"}} {
		panel := panelByID(t, board, tc.panelID)
		if got := overridePropByRefID(panel, tc.refID, "custom.axisPlacement"); got != "right" {
			t.Fatalf("панель %d: custom.axisPlacement серии %s = %v, ожидался \"right\" (своя размерность против левой оси)",
				tc.panelID, tc.refID, got)
		}
		if got := overridePropByRefID(panel, "A", "custom.axisPlacement"); got != nil {
			t.Fatalf("панель %d: у серии A custom.axisPlacement = %v — левая ось это отсутствие свойства", tc.panelID, got)
		}
	}
}

// TestFinancePolyusFinancialsTargets: ряд 33 читает v_company_financials по паре (metric,
// period_type) — у capex и gold_sold на одни и те же годы лежат И полугодовые, И годовые строки, и
// перепутанный period_type молча подменил бы ряд другим (у revenue/adjusted_ebitda и
// tcc_per_ounce второй частоты в датапаке нет — ошибка оставила бы серию пустой). Алиасы — дословно
// из спеки §5: в легенде рядом стоят «полугодие» и «год» одной метрики, и без подписи они
// неразличимы; число таргетов фиксирует состав серий каждой панели.
func TestFinancePolyusFinancialsTargets(t *testing.T) {
	board := builtFinancePolyus(t)
	for _, tc := range []struct {
		panelID    uint32
		targets    int
		refID      string
		metric     string
		periodType string
		alias      string
	}{
		{12, 2, "A", "capex", "H", "Капекс, полугодие, $ млн"},
		{12, 2, "B", "capex", "FY", "Капекс, год, $ млн"},
		{13, 2, "A", "revenue", "FY", "Выручка, $ млн"},
		{13, 2, "B", "adjusted_ebitda", "FY", "Скорр. EBITDA, $ млн"},
		{14, 2, "A", "gold_sold", "H", "Продажи, полугодие, koz"},
		{14, 2, "B", "gold_sold", "FY", "Продажи, год, koz"},
		{15, 1, "A", "tcc_per_ounce", "H", "TCC, $/oz"},
	} {
		panel := panelByID(t, board, tc.panelID)
		if len(panel.Targets) != tc.targets {
			t.Fatalf("у панели %d %d запросов, ожидалось %d", tc.panelID, len(panel.Targets), tc.targets)
		}
		query := targetByRefID(t, panel, tc.refID)
		for _, want := range []string{
			"FROM v_company_financials",
			"company = 'PLZL'",
			"metric = '" + tc.metric + "'",
			"period_type = '" + tc.periodType + "'",
			`AS "` + tc.alias + `"`,
			"ORDER BY time",
		} {
			if !strings.Contains(query.RawSql, want) {
				t.Fatalf("панель %d: запрос %s не содержит %q: %s", tc.panelID, tc.refID, want, query.RawSql)
			}
		}
	}
}

// TestFinancePolyusNoTimeFilterOnDerivedAxis: ось времени панелей 12–15 вычисляется из строки
// периода (`period`), а не из даты наблюдения, — полугодовые серии ветвятся multiIf по хвосту
// периода (H1 → 1 января, H2 → 1 июля), годовые собираются одной ветвью на 1 января (multiIf у них
// нет вовсе), — и $__timeFilter в этих запросах не применяется осознанно: ось здесь производная, и
// клип окна дашборда Grafana делает сам (паттерн dxy/crack золотого дашборда). Ветки полугодовой оси
// сверяются дословными фрагментами, а не только фактом «multiIf есть»: перестановка веток (H1 →
// 1 июля) или сдвиг даты второго полугодия прошли бы такую проверку, а панели 12/14/15 показали бы
// неверные даты. Оговорка про ось обязана быть в Description: без неё отсутствие фильтра читается
// как забытый предикат, а не как контракт панели.
func TestFinancePolyusNoTimeFilterOnDerivedAxis(t *testing.T) {
	board := builtFinancePolyus(t)
	for _, tc := range []struct {
		panelID    uint32
		refID      string
		periodType string
	}{
		{12, "A", "H"},
		{12, "B", "FY"},
		{13, "A", "FY"},
		{13, "B", "FY"},
		{14, "A", "H"},
		{14, "B", "FY"},
		{15, "A", "H"},
	} {
		query := targetByRefID(t, panelByID(t, board, tc.panelID), tc.refID)
		if strings.Contains(query.RawSql, "$__timeFilter") {
			t.Fatalf("панель %d: запрос %s применяет $__timeFilter, а ось вычисляется из period: %s", tc.panelID, tc.refID, query.RawSql)
		}
		if tc.periodType == "H" {
			// Ветка H1 сверяется вместе со своей датой (1 января): по отдельности «multiIf(» и
			// «-07-01» прошли бы и на ветках, переставленных местами, — панели показали бы июль
			// вместо января. Вторая ветка ловит сдвиг даты второго полугодия с 1 июля.
			for _, want := range []string{
				"multiIf(",
				"endsWith(period, 'H1'), toDate(concat(substring(period, 1, 4), '-01-01'))",
				"toDate(concat(substring(period, 1, 4), '-07-01'))",
			} {
				if !strings.Contains(query.RawSql, want) {
					t.Fatalf("панель %d: запрос %s не содержит %q — полугодовая ось ветвится по хвосту периода (H1 → 1 января, H2 → 1 июля): %s",
						tc.panelID, tc.refID, want, query.RawSql)
				}
			}
			continue
		}
		// Годовые серии (12-B, 13-A/B, 14-B): ось — одна ветвь, multiIf в них лишний.
		if strings.Contains(query.RawSql, "multiIf(") {
			t.Fatalf("панель %d: запрос %s — годовая ось собрана через multiIf, хотя period_type = 'FY': %s", tc.panelID, tc.refID, query.RawSql)
		}
		if !strings.Contains(query.RawSql, "toDate(concat(substring(period, 1, 4), '-01-01')) AS time") {
			t.Fatalf("панель %d: запрос %s без годовой оси (1 января года периода): %s", tc.panelID, tc.refID, query.RawSql)
		}
	}
	// Оговорка сверяется целыми формулировками, а не словом «period» (оно есть почти в любом
	// описании и ничего не лочит): описания обязаны называть ось производной от строки отчётного
	// периода и отсутствие оконного фильтра осознанным.
	for _, id := range []uint32{12, 13, 14, 15} {
		description := optionalString(panelByID(t, board, id).Description)
		for _, want := range []string{
			"Ось времени вычисляется из строки периода (`period`)",
			"$__timeFilter",
			"не применяется осознанно",
			"окно дашборда",
		} {
			if !strings.Contains(description, want) {
				t.Fatalf("Description панели %d не оговаривает ось из периода (нет %q): %s", id, want, description)
			}
		}
	}
}

// TestFinancePolyusTccPanel: панель 15 — TCC на унцию, единственная серия ряда (отчётные полугодия),
// поэтому нарисована точками: сплошная линия по короткому полугодовому ряду читается как непрерывный
// ряд. Description обязан оговаривать и то, что охват полугодий — в данных, а не в панели (числа
// точек в описании не фиксируются: при наполнении витрины они устаревают), и что AISC здесь не
// публикуется вовсе — без оговорки его отсутствие читается как дефект дашборда.
func TestFinancePolyusTccPanel(t *testing.T) {
	panel := panelByID(t, builtFinancePolyus(t), 15)
	if len(panel.Targets) != 1 {
		t.Fatalf("у панели %q %d запросов, ожидалась одна серия (полугодия)", panelTitle(panel), len(panel.Targets))
	}
	assertPointsOnly(t, panel, "A")
	description := optionalString(panel.Description)
	for _, want := range []string{"AISC", "полугод", "точками", "охват полугодий — в данных"} {
		if !strings.Contains(description, want) {
			t.Fatalf("Description панели 15 не оговаривает охват TCC (нет %q): %s", want, description)
		}
	}
}

// TestFinancePolyusGoldOutputFrequencies: панель 7 — один показатель (выпуск аффинированного золота
// компании) в трёх частотах датапака: год, полугодие, квартал. Перепутанная frequency молча оставила
// бы серию пустой, а оговорка про конец квартального ряда (2022-01) обязана быть в Description: без
// неё обрыв кварталов читается как дефект дашборда, а не как охват источника.
func TestFinancePolyusGoldOutputFrequencies(t *testing.T) {
	panel := panelByID(t, builtFinancePolyus(t), 7)
	if len(panel.Targets) != 3 {
		t.Fatalf("у панели %q %d запросов, ожидалось 3 (год, полугодие, квартал)", panelTitle(panel), len(panel.Targets))
	}
	for _, tc := range []struct{ refID, frequency, alias string }{
		{"A", "ANNUAL", "Год, koz"},
		{"B", "SEMI-ANNUAL", "Полугодия, koz"},
		{"C", "QUARTERLY", "Кварталы, koz"},
	} {
		query := targetByRefID(t, panel, tc.refID)
		for _, want := range []string{
			"FROM v_company_operating",
			"asset = 'CONSOLIDATED OPERATING RESULTS'",
			"metric = 'Total refined gold output'",
			"frequency = '" + tc.frequency + "'",
			`AS "` + tc.alias + `"`,
			"$__timeFilter(date)",
			"ORDER BY date",
		} {
			if !strings.Contains(query.RawSql, want) {
				t.Fatalf("панель 7: запрос %s не содержит %q: %s", tc.refID, want, query.RawSql)
			}
		}
	}
	// Оговорка обязана называть конкретику, а не «данные» вообще: какая частота обрывается, где
	// именно (2022-01), докуда идут остальные (2026-01) и что обрыв — свойство источника.
	description := optionalString(panel.Description)
	for _, want := range []string{"квартал", "2022-01", "2026-01", "данные источника"} {
		if !strings.Contains(description, want) {
			t.Fatalf("Description панели 7 не оговаривает охват частот (нет %q): %s", want, description)
		}
	}
}

// TestFinancePolyusNineAssets: панель 8 — выпуск золота годовым срезом по активам датапака: девять
// таргетов, по одному на актив, с алиасом «<актив>, koz» (иначе в легенде девять одинаковых
// «value»). Консолидированного итога среди них нет — он на панели 7: уровень компании и уровень
// актива на одной оси читались бы как двойной счёт. Метрика у восьми активов одна
// ('Total refined gold output'), а у ALLUVIALS своя ('Refined gold production'): россыпной актив
// ведётся в датапаке отдельной строкой.
func TestFinancePolyusNineAssets(t *testing.T) {
	panel := panelByID(t, builtFinancePolyus(t), 8)
	if len(panel.Targets) != 9 {
		t.Fatalf("у панели %q %d запросов, ожидалось 9 (по одному на актив)", panelTitle(panel), len(panel.Targets))
	}
	for _, tc := range []struct{ refID, asset, metric string }{
		{"A", "OLIMPIADA", "Total refined gold output"},
		{"B", "BLAGODATNOYE", "Total refined gold output"},
		{"C", "NATALKA", "Total refined gold output"},
		{"D", "KURANAKH", "Total refined gold output"},
		{"E", "VERNINSKOYE2", "Total refined gold output"},
		{"F", "ALLUVIALS", "Refined gold production"},
		{"G", "Sukhoi Log", "Total refined gold output"},
		{"H", "TITIMUKHTA", "Total refined gold output"},
		{"I", "ZAPADNOYE", "Total refined gold output"},
	} {
		query := targetByRefID(t, panel, tc.refID)
		for _, want := range []string{
			"FROM v_company_operating",
			"company = 'PLZL'",
			"asset = '" + tc.asset + "'",
			"metric = '" + tc.metric + "'",
			"frequency = 'ANNUAL'",
			`AS "` + tc.asset + `, koz"`,
			"$__timeFilter(date)",
			"ORDER BY date",
		} {
			if !strings.Contains(query.RawSql, want) {
				t.Fatalf("панель 8: запрос %s (актив %s) не содержит %q: %s", tc.refID, tc.asset, want, query.RawSql)
			}
		}
		if strings.Contains(query.RawSql, "CONSOLIDATED") {
			t.Fatalf("панель 8: запрос %s читает консолидированный итог (он на панели 7): %s", tc.refID, query.RawSql)
		}
	}
	// Две оговорки охвата — обрыв истории ZAPADNOYE на 2024-м и пустая серия Sukhoi Log (проект до
	// запуска ЗИФ) — видны только в легенде, и обязана их объяснить Description: иначе обрыв и
	// пустота читаются как дефект дашборда, а не как охват источника.
	description := optionalString(panel.Description)
	for _, want := range []string{"консолидирован", "ALLUVIALS", "Sukhoi Log", "ZAPADNOYE", "ЗИФ", "2024"} {
		if !strings.Contains(description, want) {
			t.Fatalf("Description панели 8 не объясняет охват (нет %q): %s", want, description)
		}
	}
}

// fpPanelLayout — ожидаемая раскладка панелей по рядам (id → x/y/w/h). Ряд 31 «Золото, ставки,
// рынок» (1–6): верхняя пара — цена ₽/г (1) и курс (3), панель 2 (24 колонки) — своя строка, ниже
// пара ставок (4/5) и цена акции (6). Ряд 32 «Операционные показатели» (7–11): две широкие панели
// выпуска золота (7/8, 24 колонки) — каждая своей строкой, ниже пара руды (9/10) и содержание с
// извлечением (11); ряд встаёт сразу за панелью 6 (28 + 9 = 37), поэтому первая его панель — на
// y=38. Ряд 33 «Финансовые показатели» (12–15): капекс (12) и выручка с EBITDA (13) верхней парой,
// ниже продажи золота (14) и TCC (15); ряд — сразу за панелью 11 (65 + 9 = 74), панели начинаются
// на y=75. Ряд 34 «Прогноз и точность» (16–19): веер цены золота (16) и бар сценариев (17) верхней
// парой, ниже таблицы прогона (18) и точности (19); ряд — сразу за панелью 15 (84 + 9 = 93), панели
// начинаются на y=94. Таблица живёт в тесте, а не рядом с билдерами: она фиксирует раскладку (её
// видно в артефакте и в UI), а панель без позиции в таблице — ошибка, а не «дорисует авто-layout».
var fpPanelLayout = map[uint32]gridPos{
	1:  {x: 0, y: 1, w: 12, h: 9},
	3:  {x: 12, y: 1, w: 12, h: 9},
	2:  {x: 0, y: 10, w: 24, h: 9},
	4:  {x: 0, y: 19, w: 12, h: 9},
	5:  {x: 12, y: 19, w: 12, h: 9},
	6:  {x: 0, y: 28, w: 12, h: 9},
	7:  {x: 0, y: 38, w: 24, h: 9},
	8:  {x: 0, y: 47, w: 24, h: 9},
	9:  {x: 0, y: 56, w: 12, h: 9},
	10: {x: 12, y: 56, w: 12, h: 9},
	11: {x: 0, y: 65, w: 12, h: 9},
	12: {x: 0, y: 75, w: 12, h: 9},
	13: {x: 12, y: 75, w: 12, h: 9},
	14: {x: 0, y: 84, w: 12, h: 9},
	15: {x: 12, y: 84, w: 12, h: 9},
	16: {x: 0, y: 94, w: 12, h: 9},
	17: {x: 12, y: 94, w: 12, h: 9},
	18: {x: 0, y: 103, w: 12, h: 9},
	19: {x: 12, y: 103, w: 12, h: 9},
}

// fpRowLayout — ожидаемая раскладка рядов (id → x/y/w/h): ряд — полоса сетки шириной 24 колонки и
// высотой 1. Позицию ряда задаёт fpRowGridPos явно, потому что авто-layout ставит ряд в курсор
// панелей (currentY + высота последней): у ряда 32 курсор после явно разложенных панелей стоит на
// y=28 — то есть ряд лёг бы поверх панели 6, у ряда 33 — на y=65 (поверх панели 11), у ряда 34 —
// на y=84 (поверх панели 15). У ряда 31 y=0, и SDK пересчитывает только пару «x=0 И y=0»: у первого
// ряда дашборда пересчёт даёт ровно этот же прямоугольник.
var fpRowLayout = map[uint32]gridPos{
	31: {x: 0, y: 0, w: 24, h: 1},
	32: {x: 0, y: 37, w: 24, h: 1},
	33: {x: 0, y: 74, w: 24, h: 1},
	34: {x: 0, y: 93, w: 24, h: 1},
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
// 1–19 по таблице fpPanelLayout и рядов по fpRowLayout (позиция задана явно, а не выведена
// авто-layout'ом SDK — тот ставил панель 2 шириной 24 колонки в курсор после панели 1, то есть
// x=12..36, за правую границу сетки, а ряды 32–34 — поверх последних панелей предыдущих рядов),
// непустой Description у каждой панели (спека §4), id уникальные в 1..19, FINAL и LIMIT в панельных
// запросах не пишутся (FINAL уже внутри витрин; LIMIT на временном ряду молча обрезал бы историю —
// anti-pattern легаси-экспорта). Общие инварианты сетки: x + w <= 24, y >= 1 у панелей (для SDK
// x=0 и y=0 — «позиция не задана», её перезапишет авто-layout; ряд 31 — исключение: он первый,
// и пересчёт даёт тот же прямоугольник), прямоугольники не пересекаются — панели между собой,
// панели с рядами и ряды между собой (ряд участвует в проверке только со своим GridPos: ряд без
// позиции в таблице — ошибка, а не «пропустить и не заметить»).
func TestFinancePolyusSpecConventions(t *testing.T) {
	board := builtFinancePolyus(t)
	panels := panelsOf(board)
	if len(panels) != len(fpPanelLayout) {
		t.Fatalf("панелей %d, в таблице раскладки %d: панель добавляется вместе со своей позицией", len(panels), len(fpPanelLayout))
	}
	// Симметрично панелям: потерянный ряд обязан падать здесь, а не только в TestFinancePolyusRowTitles
	// — иначе ряд без позиции в fpRowLayout молча выпал бы из проверки пересечений.
	rows := 0
	for _, entry := range board.Panels {
		if entry.RowPanel != nil {
			rows++
		}
	}
	if rows != len(fpRowLayout) {
		t.Fatalf("рядов %d, в таблице раскладки %d: ряд добавляется вместе со своей позицией", rows, len(fpRowLayout))
	}

	seen := make(map[uint32]bool, len(panels))
	rects := make([]layoutRect, 0, len(panels)+len(fpRowLayout))
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

		want, ok := fpPanelLayout[id]
		if !ok {
			t.Fatalf("панель %d (%q): нет позиции в таблице раскладки — новая панель получает явный GridPos и попадает в таблицу", id, panelTitle(p))
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
		if entry.RowPanel == nil {
			continue
		}
		title := optionalString(entry.RowPanel.Title)
		want, ok := fpRowLayout[entry.RowPanel.Id]
		if !ok {
			t.Fatalf("ряд %q (id %d): нет позиции в таблице раскладки рядов — ряд получает явный GridPos и попадает в таблицу", title, entry.RowPanel.Id)
		}
		if entry.RowPanel.GridPos == nil {
			t.Fatalf("ряд %q: gridPos не задан (авто-layout ставит ряд в курсор панелей — поверх последней)", title)
		}
		row := gridPos{x: entry.RowPanel.GridPos.X, y: entry.RowPanel.GridPos.Y, w: entry.RowPanel.GridPos.W, h: entry.RowPanel.GridPos.H}
		if row != want {
			t.Fatalf("ряд %q: gridPos {x:%d y:%d w:%d h:%d}, ожидался {x:%d y:%d w:%d h:%d}",
				title, row.x, row.y, row.w, row.h, want.x, want.y, want.w, want.h)
		}
		if row.x+row.w > 24 {
			t.Fatalf("ряд %q: x=%d + w=%d выходит за 24 колонки сетки", title, row.x, row.w)
		}
		rects = append(rects, layoutRect{"ряд " + title, row})
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

// fpForecastScenarios — own-сценарии дашборда в порядке серий панели 16: refId, код сценария в
// v_gold_forecasts, алиас серии и фикс-цвет. Цвет кодирует смысл сценария (bull — рост, bear —
// падение, base — центральный), а не порядок таргетов: палитра classic красила бы точки по
// позиции, и цвет сценария менялся бы при добавлении ряда.
var fpForecastScenarios = []struct{ refID, scenario, alias, color string }{
	{"B", "bull", "own: bull", "green"},
	{"C", "base", "own: base", "blue"},
	{"D", "bear", "own: bear", "red"},
}

// TestFinancePolyusForecastTargets: панель 16 — факт и три точки собственных сценариев. A — дневной
// ряд COMEX-фронта (та же форма, что у панели 2-C: v_gold_prices, source = 'gold',
// series = 'comex_front' и $__timeFilter), B/C/D — по одному сценарию из v_gold_forecasts с алиасом
// «own: <сценарий>». Число таргетов фиксирует состав: пропавший сценарий молча убрал бы точку с
// веера, а опечатка в scenario оставила бы серию пустой.
func TestFinancePolyusForecastTargets(t *testing.T) {
	panel := panelByID(t, builtFinancePolyus(t), 16)
	if panel.Type != "timeseries" {
		t.Fatalf("панель 16: тип %q, ожидался timeseries (факт линией и точки сценариев)", panel.Type)
	}
	if len(panel.Targets) != 4 {
		t.Fatalf("у панели %q %d запросов, ожидалось 4 (факт и три own-сценария)", panelTitle(panel), len(panel.Targets))
	}
	fact := targetByRefID(t, panel, "A")
	for _, want := range []string{
		"FROM v_gold_prices",
		"source = 'gold'",
		"series = 'comex_front'",
		`AS "COMEX-фронт, $/oz"`,
		"$__timeFilter(date)",
		"ORDER BY date",
	} {
		if !strings.Contains(fact.RawSql, want) {
			t.Fatalf("панель 16: запрос A (факт) не содержит %q: %s", want, fact.RawSql)
		}
	}
	for _, tc := range fpForecastScenarios {
		query := targetByRefID(t, panel, tc.refID)
		for _, want := range []string{
			"FROM v_gold_forecasts",
			"author = 'own'",
			"scenario = '" + tc.scenario + "'",
			`midpoint AS "` + tc.alias + `"`,
			"ORDER BY time",
		} {
			if !strings.Contains(query.RawSql, want) {
				t.Fatalf("панель 16: запрос %s (сценарий %s) не содержит %q: %s", tc.refID, tc.scenario, want, query.RawSql)
			}
		}
	}
	// Оговорка об охвате обязана быть в Description: ярлыки 12M и short в календарь не маппятся и
	// точками не рисуются, вероятности сценариев и диапазоны low/high видны только в баре 17, а
	// прогноз читается из актуальной волны (последней по published) — без этого отсутствие точек и
	// «откуда волна» читаются как дефект дашборда. Окно дашборда обязано быть продлено вперёд
	// (now+1y): точки лежат в будущем, и с правой границей «now» Grafana их отрезала бы.
	description := optionalString(panel.Description)
	for _, want := range []string{"v_gold_forecasts", "12M", "short", "31 декабря", "панель 17", "вероятност", "published", "now+1y"} {
		if !strings.Contains(description, want) {
			t.Fatalf("Description панели 16 не оговаривает охват горизонтов (нет %q): %s", want, description)
		}
	}
}

// TestFinancePolyusHorizonMapping: дата точки сценария вычисляется из САМОГО ярлыка горизонта, без
// хардкода годов: `Q#-YYYY` и `YE-YYYY` → 31 декабря года (год берётся из последних четырёх символов
// ярлыка), голый `YYYY` → 31 декабря года; немэппируемые ярлыки (`12M`, `short`) отсекаются веткой
// NULL и фильтром time IS NOT NULL. Литерал года или UNION с рукописными строками означал бы, что
// прогноз вшит в запрос, а не прочитан из витрины (анти-паттерн легаси-экспорта).
func TestFinancePolyusHorizonMapping(t *testing.T) {
	panel := panelByID(t, builtFinancePolyus(t), 16)
	for _, scenario := range fpForecastScenarios {
		query := targetByRefID(t, panel, scenario.refID)
		for _, want := range []string{
			"multiIf(",
			`match(horizon, '^(Q[1-4]|YE)-[0-9]{4}$')`,
			"toDate(concat(substring(horizon, length(horizon) - 3, 4), '-12-31'))",
			`match(horizon, '^[0-9]{4}$')`,
			"toDate(concat(horizon, '-12-31'))",
			"NULL\n        ) AS time",
			"scenario = '" + scenario.scenario + "'",
			"time IS NOT NULL",
			// Волна одна: подвыборка отдаёт published, а внешний предикат оставляет последнюю волну
			// (max(published)) — иначе вторая волна поставила бы на ту же дату вторую точку.
			"max(published)",
		} {
			if !strings.Contains(query.RawSql, want) {
				t.Fatalf("панель 16: запрос %s: нет %q — горизонт маппится по ярлыку: %s", scenario.refID, want, query.RawSql)
			}
		}
		for _, forbidden := range []string{"UNION ALL", "'20", "12M", "short"} {
			if strings.Contains(query.RawSql, forbidden) {
				t.Fatalf("панель 16: запрос %s содержит %q (хардкод года или немэппируемого горизонта): %s",
					scenario.refID, forbidden, query.RawSql)
			}
		}
	}
}

// TestFinancePolyusForecastScenarioPoints: точки B/C/D панели 16 рисуются только точками
// (lineWidth 0 + showPoints always) и с фикс-цветом сценария. Сплошная линия по одной точке на
// горизонт не читается, а палитра classic покрасила бы сценарии по порядку таргетов.
func TestFinancePolyusForecastScenarioPoints(t *testing.T) {
	panel := panelByID(t, builtFinancePolyus(t), 16)
	for _, tc := range fpForecastScenarios {
		assertPointsOnly(t, panel, tc.refID)
		color, ok := overridePropByRefID(panel, tc.refID, "color").(dashboard.FieldColor)
		if !ok || color.Mode != dashboard.FieldColorModeIdFixed || color.FixedColor == nil || *color.FixedColor != tc.color {
			t.Fatalf("панель 16: цвет серии %s (%s) = %#v, ожидался fixed %q", tc.refID, tc.scenario, color, tc.color)
		}
	}
}

// TestFinancePolyusForecastTables: панели 17–19 — бар сценариев и две таблицы, и плагину нужен
// queryType "table" с форматом 1: строковые колонки («Прогноз», «Прогон», «Разрешён») в
// timeseries-формат не укладываются. Панель 17 — barchart с XField «Прогноз» (как gold-панель 8):
// без подписи оси X бар читался бы по первой числовой колонке, а не по ярлыку прогноза. У каждой
// панели один таргет на своей витрине.
func TestFinancePolyusForecastTables(t *testing.T) {
	board := builtFinancePolyus(t)
	for _, tc := range []struct {
		panelID   uint32
		panelType string
		view      string
		wants     []string
	}{
		{17, "barchart", "v_gold_forecasts", []string{
			`concat(author, ' / ', scenario, ' [', horizon, '] (', ifNull(toString(toInt32(probability)), '—'), '%)') AS "Прогноз"`,
			`midpoint AS "Середина, $/oz"`,
			"low,\n    high",
			// Волна одна — последняя по published: без предиката бар смешал бы волны прогнозов.
			"WHERE published = (SELECT max(published) FROM v_gold_forecasts)",
			"ORDER BY midpoint",
		}},
		{18, "table", "v_model_inputs", []string{
			`toString(last_run_id) AS "Прогон"`,
			`last_run_date AS "Дата прогона"`,
			`last_nav_per_share AS "НАВ/акция, ₽"`,
			`plzl_close AS "Цена PLZL, ₽"`,
			`gold_moex_fix_usd AS "Золото (fix), $/oz"`,
			`usdrub AS "USD/RUB"`,
			`cbr_key_rate AS "Ключевая, %"`,
			`ofz_5y AS "ОФЗ 5y, %"`,
		}},
		{19, "table", "v_forecast_accuracy", []string{
			`forecast_date AS "Дата прогноза"`,
			`target_date AS "Цель"`,
			`predicted AS "Прогноз, $/oz"`,
			`actual AS "Факт, $/oz"`,
			`round(error_pct, 2) AS "Ошибка, %"`,
			`is_resolved AS "Разрешён"`,
			"metric = 'xau_q_avg'",
			"ORDER BY target_date DESC",
		}},
	} {
		panel := panelByID(t, board, tc.panelID)
		if panel.Type != tc.panelType {
			t.Fatalf("панель %d: тип %q, ожидался %q", tc.panelID, panel.Type, tc.panelType)
		}
		if len(panel.Targets) != 1 {
			t.Fatalf("у панели %q %d запросов, ожидался 1", panelTitle(panel), len(panel.Targets))
		}
		query, ok := chQuery(panel.Targets[0])
		if !ok {
			t.Fatalf("панель %q: запрос не является запросом ClickHouse", panelTitle(panel))
		}
		if query.Format != queryFormatTable || query.QueryType != queryTypeTable {
			t.Fatalf("панель %d: format=%d queryType=%q, ожидались %d/%q (строковые колонки в timeseries не укладываются)",
				tc.panelID, query.Format, query.QueryType, queryFormatTable, queryTypeTable)
		}
		if !strings.Contains(query.RawSql, "FROM "+tc.view) {
			t.Fatalf("панель %d: запрос читает не %s: %s", tc.panelID, tc.view, query.RawSql)
		}
		for _, want := range tc.wants {
			if !strings.Contains(query.RawSql, want) {
				t.Fatalf("панель %d: запрос не содержит %q: %s", tc.panelID, want, query.RawSql)
			}
		}
	}
	options, ok := panelByID(t, board, 17).Options.(*barchart.Options)
	if !ok {
		t.Fatalf("панель 17: options %T, ожидались *barchart.Options", panelByID(t, board, 17).Options)
	}
	if options.XField == nil || *options.XField != "Прогноз" {
		t.Fatalf("панель 17: xField %v, ожидался \"Прогноз\" (подпись оси — ярлык прогноза, как у gold-панели 8)", options.XField)
	}
	// Оговорки панелей 17–19 — в Description: у 17 вероятности банковских строк не публикуются
	// («—»), горизонт стоит в подписи бара (иначе повторяющиеся пары author/scenario неразличимы),
	// прогноз читается из актуальной волны (последней по published), у 18 апсайд не считается (мост
	// НАВ → цена акции — остаток фазы 3 роадмапа), у 19 вторая строка ещё не разрешена: факт,
	// ошибка и статус пусты, пока forecast_resolve не сверит цель с фактом.
	for _, tc := range []struct {
		panelID uint32
		wants   []string
	}{
		{17, []string{"v_gold_forecasts", "вероятност", "12M", "short", "панели 16", "published", "горизонт"}},
		{18, []string{"v_model_inputs", "апсайд", "НЕ считается", "фазы 3", "как записан"}},
		{19, []string{"v_forecast_accuracy", "xau_q_avg", "не разрешённую", "знаков"}},
	} {
		description := optionalString(panelByID(t, board, tc.panelID).Description)
		for _, want := range tc.wants {
			if !strings.Contains(description, want) {
				t.Fatalf("Description панели %d не оговаривает контракт (нет %q): %s", tc.panelID, want, description)
			}
		}
	}
}

// fpInlineDateRe — дата-литерал нулевых годов, вшитая в SQL запроса: `toDate('2026-12-31')`,
// `toDateTime('2026-12-30')` (с пробелами после скобки). Именно так легаси-экспорт подмешивал
// прогнозные точки в панель; проверка идёт регулярным выражением, потому что `toDate('20…')` бывает
// и «законной» формы — но ни одной такой в дашборде быть не должно.
var fpInlineDateRe = regexp.MustCompile(`to(Date|DateTime)\(\s*'20[0-9]{2}-`)

// TestFinancePolyusNoInlinePoints: анти-паттерн легаси-экспорта — прогнозные точки вшивались прямо
// в SQL панели (`UNION ALL SELECT toDateTime('2026-12-30')` → 2500). Точки прогноза обязаны
// приходить из витрины (v_gold_forecasts) и вычисляться из ярлыка горизонта: UNION с рукописными
// строками и даты-литералы `toDate('20…')`/`toDateTime('20…')` в SQL дашборда не пишутся. Литерал
// ловится регулярным выражением (см. fpInlineDateRe), а не списком строк: пробелы после скобки и
// год ловятся вместе с самой конструкцией.
func TestFinancePolyusNoInlinePoints(t *testing.T) {
	for _, p := range panelsOf(builtFinancePolyus(t)) {
		for _, target := range p.Targets {
			query, ok := chQuery(target)
			if !ok {
				t.Fatalf("панель %q: запрос не является запросом ClickHouse", panelTitle(p))
			}
			for _, forbidden := range []string{"UNION ALL", "SELECT toDateTime", "toDateTime('20"} {
				if strings.Contains(query.RawSql, forbidden) {
					t.Fatalf("панель %d: запрос %q содержит %q — точки прогноза берутся из витрины, а не вшиваются в SQL: %s",
						*p.Id, query.RefId, forbidden, query.RawSql)
				}
			}
			if literal := fpInlineDateRe.FindString(query.RawSql); literal != "" {
				t.Fatalf("панель %d: запрос %q содержит вшитую дату точки %q — прогнозные точки приходят из витрины, а не из литералов SQL: %s",
					*p.Id, query.RefId, literal, query.RawSql)
			}
		}
	}
}
