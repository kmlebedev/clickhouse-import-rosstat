package mlregistry

import (
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

func TestMlRegistryName(t *testing.T) {
	if got := (&mlRegistry{}).Name(); got != "ml_registry" {
		t.Fatalf("Name() = %q, want ml_registry", got)
	}
}

func TestMlRunsDdlPinsPassport(t *testing.T) {
	for _, want := range []string{
		"run_id UUID", "experiment LowCardinality(String)",
		"config_hash String", "env_json String", "sample_hash String", "data_asof Date",
		"metrics_json String", "artifacts_json String",
		"ENGINE = ReplacingMergeTree", "ORDER BY (experiment, run_id)",
	} {
		if !strings.Contains(mlRunsDdl, want) {
			t.Errorf("ml_runs DDL must contain %q", want)
		}
	}
}

func TestMlPredictionsDdlPinsFoldKey(t *testing.T) {
	for _, want := range []string{
		"run_id UUID", "model LowCardinality(String)",
		"window LowCardinality(String)", "target_date Date",
		"predicted Float64", "actual Float64", "error Float64",
		"unit LowCardinality(String)",
		"ORDER BY (experiment, run_id, model, window, target_date)",
	} {
		if !strings.Contains(mlPredictionsDdl, want) {
			t.Errorf("ml_predictions DDL must contain %q", want)
		}
	}
}

func TestMlViewsContract(t *testing.T) {
	names := make([]string, 0, len(mlViewsList))
	for _, v := range mlViewsList {
		names = append(names, v.Name)
	}
	want := []string{"v_ml_runs", "v_ml_leaderboard", "v_ml_error_by_fold"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("views = %v, want %v", names, want)
	}
	for _, v := range mlViewsList {
		if len(v.Tables) == 0 || v.Select == "" || v.Comment == "" {
			t.Errorf("%s: Tables/Select/Comment must be set", v.Name)
		}
	}
	if !strings.Contains(mlLeaderboardView.Select, "sqrt(avg(p.error * p.error))") {
		t.Error("v_ml_leaderboard must compute RMSE from fold errors")
	}
}

func TestMlRegistryRegistered(t *testing.T) {
	for _, stat := range chimport.Stats {
		if stat.Name() == "ml_registry" {
			return
		}
	}
	t.Fatal("ml_registry is not registered in chimport.Stats")
}
