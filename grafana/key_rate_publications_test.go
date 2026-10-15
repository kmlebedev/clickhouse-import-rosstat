package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// keyRateRowTitles — ряды дашборда в порядке следования (id 21–24). В этой задаче готов
// только первый ряд; список растёт вместе с панелями (T2–T4).
var keyRateRowTitles = []string{
	"Ставка и инфляция",
}

func builtKeyRatePublications(t *testing.T) dashboard.Dashboard {
	t.Helper()
	d, err := NewKeyRatePublications().Build()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestKeyRatePublicationsPanelCount фиксирует состав дашборда: пока собран только ряд 21 —
// 5 панелей (растёт до 19 в T2–T4).
func TestKeyRatePublicationsPanelCount(t *testing.T) {
	if panels := panelsOf(builtKeyRatePublications(t)); len(panels) != 5 {
		t.Fatalf("панелей %d, ожидалось 5", len(panels))
	}
}

// TestKeyRatePublicationsRowTitles: ряды нумеруются вне диапазона панелей 1..19 — нулевой
// или совпавший id Grafana перезапишет своим, и артефакт разойдётся с кодом.
func TestKeyRatePublicationsRowTitles(t *testing.T) {
	var titles []string
	for _, entry := range builtKeyRatePublications(t).Panels {
		if entry.RowPanel == nil {
			continue
		}
		if entry.RowPanel.Id < 20 {
			t.Fatalf("ряд %q: id %d попадает в диапазон панелей 1..19", optionalString(entry.RowPanel.Title), entry.RowPanel.Id)
		}
		titles = append(titles, optionalString(entry.RowPanel.Title))
	}
	if len(titles) != len(keyRateRowTitles) {
		t.Fatalf("рядов %d, ожидалось %d: %v", len(titles), len(keyRateRowTitles), titles)
	}
	for i, got := range titles {
		if got != keyRateRowTitles[i] {
			t.Fatalf("ряд %d: %q, ожидался %q", i+1, got, keyRateRowTitles[i])
		}
	}
}

// TestKeyRateAllTargetsUseViews: у датасорса стенда нет грантов на сырые таблицы (в UI-экспорте
// запросы как раз на сырых), поэтому каждый запрос каждой панели обязан читать витрину v_*.
func TestKeyRateAllTargetsUseViews(t *testing.T) {
	for _, p := range panelsOf(builtKeyRatePublications(t)) {
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

// TestKeyRatePanelIDsUnique проверяет и раскладку id (через Validate), и параметризацию
// Validate: тот же дашборд с чужим uid обязан быть отвергнут, иначе опечатка в uid прошла
// бы молча и один дашборд затёр бы другой при деплое.
func TestKeyRatePanelIDsUnique(t *testing.T) {
	board := builtKeyRatePublications(t)
	if err := Validate(board, KeyRatePublicationsUID); err != nil {
		t.Fatalf("дашборд key-rate-publications не прошёл валидацию: %v", err)
	}
	if err := Validate(board, GoldThesisUID); err == nil {
		t.Fatal("дашборд принят с чужим uid: проверка uid не параметризована")
	}
}

// TestKeyRateWeeklyYearOverlay: ряд B панели 2 — календарное наложение прошлого года.
// Время сдвигается на +1 год (date_add), а год отбирается ровно прошлый (toYear(now()) - 1):
// поменяй одно без другого — наложение покажет не тот год, и «прошлый год» окажется текущим.
func TestKeyRateWeeklyYearOverlay(t *testing.T) {
	panel := panelByID(t, builtKeyRatePublications(t), 2)
	if len(panel.Targets) != 2 {
		t.Fatalf("у панели %q %d запросов, ожидалось 2 (текущий и прошлый год)", panelTitle(panel), len(panel.Targets))
	}
	query, ok := chQuery(panel.Targets[1])
	if !ok {
		t.Fatalf("панель %q: запрос не является запросом ClickHouse", panelTitle(panel))
	}
	for _, want := range []string{"date_add(YEAR, 1, date)", "toYear(now()) - 1"} {
		if !strings.Contains(query.RawSql, want) {
			t.Fatalf("панель %q: запрос %q не содержит %q: %s", panelTitle(panel), query.RefId, want, query.RawSql)
		}
	}
}

// TestKeyRateWeeklyJoinCatalog: недельные ряды отбираются каталогом (frequency = 'W'), а не
// списком имён в SQL. JOIN по (source, series) обязателен в обоих недельных запросах: товар
// без строки каталога молча выпал бы из avg/count, и среднее считалось бы по неполному набору.
func TestKeyRateWeeklyJoinCatalog(t *testing.T) {
	board := builtKeyRatePublications(t)
	for _, id := range []uint32{2, 3} {
		panel := panelByID(t, board, id)
		for _, target := range panel.Targets {
			query, ok := chQuery(target)
			if !ok {
				t.Fatalf("панель %q: запрос не является запросом ClickHouse", panelTitle(panel))
			}
			for _, want := range []string{"JOIN v_series_catalog", "frequency = 'W'"} {
				if !strings.Contains(query.RawSql, want) {
					t.Fatalf("панель %q: запрос %q не содержит %q: %s", panelTitle(panel), query.RefId, want, query.RawSql)
				}
			}
		}
	}
}

// TestKeyRateUnits: SQL панели 1 отдаёт проценты годовых (percent), панели 4 — долю
// (pow(...) - 1), которую Grafana как percentunit домножит на 100 и напечатает «3.2%».
// Перепутанные единицы дают десятикратную ошибку чтения.
func TestKeyRateUnits(t *testing.T) {
	board := builtKeyRatePublications(t)
	if unit := panelByID(t, board, 4).FieldConfig.Defaults.Unit; unit == nil || *unit != units.PercentUnit {
		t.Fatalf("панель 4: unit %v, ожидался %q", unit, units.PercentUnit)
	}
	if unit := panelByID(t, board, 1).FieldConfig.Defaults.Unit; unit == nil || *unit != units.Percent {
		t.Fatalf("панель 1: unit %v, ожидался %q", unit, units.Percent)
	}
}

// TestKeyRateThresholdsStyleSet: без custom.thresholdsStyle Grafana не рисует порог (по умолчанию
// off), а 4% — смысловая граница панелей 2 и 4 (цель ЦБ по инфляции): порог без стиля молча
// пропал бы с графика. Значение шага — доля (0.04 = 4%), как и значения серий этих панелей.
func TestKeyRateThresholdsStyleSet(t *testing.T) {
	board := builtKeyRatePublications(t)
	for _, id := range []uint32{2, 4} {
		panel := panelByID(t, board, id)
		if mode := thresholdStyleMode(t, panel); mode == "" || mode == "off" {
			t.Fatalf("панель %d: thresholdsStyle.mode=%q, ожидался не off", id, mode)
		}
		if !hasThresholdStep(panel, 0.04) {
			t.Fatalf("панель %d: нет шага порога 0.04 (4%% годовых)", id)
		}
	}
}

// hasThresholdStep ищет шаг порога по значению: пороги задаются панельными thresholds,
// а не override'ом — на панели один смысловой порог.
func hasThresholdStep(p *dashboard.Panel, value float64) bool {
	thresholds := p.FieldConfig.Defaults.Thresholds
	if thresholds == nil {
		return false
	}
	for _, step := range thresholds.Steps {
		if step.Value != nil && *step.Value == value {
			return true
		}
	}
	return false
}

// TestKeyRateGenerateDeterministic — как TestGenerateDeterministic в gold: на детерминизме
// генерации стоит CI-дифф артефакта, поэтому две сборки обязаны дать байт-идентичный JSON.
func TestKeyRateGenerateDeterministic(t *testing.T) {
	d1, err := NewKeyRatePublications().Build()
	if err != nil {
		t.Fatal(err)
	}
	d2, err := NewKeyRatePublications().Build()
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := json.MarshalIndent(Manifest(d1), "", "  ")
	b2, _ := json.MarshalIndent(Manifest(d2), "", "  ")
	if !bytes.Equal(b1, b2) {
		t.Fatal("генерация недетерминирована")
	}
}
