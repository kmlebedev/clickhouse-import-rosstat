package views

import (
	"strings"
	"testing"
)

// selectedMLLiveAliases — колонки выдачи v_ml_live_accuracy. Список задан
// руками: витрина простая, но контракт (стадия, трек, error_bp) важнее формы
// SQL, и точечная проверка читается лучше, чем разбор запроса.
var selectedMLLiveAliases = []string{
	"metric",
	"track",
	"stage",
	"model",
	"forecast_date",
	"target_date",
	"predicted",
	"actual",
	"error_bp",
	"abs_error_bp",
	"is_resolved",
	"days_since_target",
	"data_version",
	"prompt_version",
}

// Контракт live-витрины (infra/4.3): имя, единственный сырой источник,
// разбор трека/стадии, error_bp и комментарии. Сам расчёт исполняется только
// живым прогоном `make import STAT=key_rate_views`.
func TestMLLiveAccuracyViewContract(t *testing.T) {
	v := mlLiveAccuracyView

	if v.Name != "v_ml_live_accuracy" {
		t.Fatalf("Name = %q, want v_ml_live_accuracy", v.Name)
	}
	if len(v.Tables) != 1 || v.Tables[0] != "forecast_log" {
		t.Fatalf("Tables = %v, want [forecast_log]", v.Tables)
	}
	if len(v.Columns) != len(selectedMLLiveAliases) {
		t.Fatalf("Columns = %d, want %d", len(v.Columns), len(selectedMLLiveAliases))
	}
	for _, column := range selectedMLLiveAliases {
		if strings.TrimSpace(v.Columns[column]) == "" {
			t.Errorf("column %q has no comment", column)
		}
	}

	selectText := strings.Join(strings.Fields(v.Select), " ")
	for _, want := range []string{
		"FROM forecast_log FINAL",
		"WHERE metric LIKE 'cbr_rate:%'",
		"splitByChar(':', metric)[2] AS track",
		"endsWith(metric, ':shadow'), 'shadow'",
		"endsWith(metric, ':canary'), 'canary'",
		"abs(error_bp) AS abs_error_bp",
		"toUInt8(actual IS NOT NULL) AS is_resolved",
		"dateDiff('day', target_date, today()) AS days_since_target",
	} {
		if !strings.Contains(selectText, want) {
			t.Errorf("SELECT must contain %q: %s", want, selectText)
		}
	}
	if strings.Contains(selectText, "FROM v_") {
		t.Error("SELECT must read the raw forecast_log, not another view")
	}
	if v.Comment == "" {
		t.Fatal("v_ml_live_accuracy has no table comment")
	}
}

func TestMLLiveAccuracyViewIsBuiltByKeyRateViews(t *testing.T) {
	var found bool

	for _, v := range keyRateViewsList {
		if v.Name == mlLiveAccuracyView.Name {
			found = true
		}
	}

	if !found {
		t.Fatalf("%s must be created by the key_rate_views importer: it is missing from keyRateViewsList",
			mlLiveAccuracyView.Name)
	}
}
