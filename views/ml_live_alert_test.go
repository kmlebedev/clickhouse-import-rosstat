package views

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// alertFile — минимальная форма Grafana alerting provisioning, которую проверяет
// контракт infra/4.3. Полную схему Grafana валидирует сама при загрузке; тест
// держит только то, от чего зависит мониторинг: UID, запрос, порог и метки.
type alertFile struct {
	APIVersion int `yaml:"apiVersion"`
	Groups     []struct {
		Name   string `yaml:"name"`
		Folder string `yaml:"folder"`
		Rules  []struct {
			UID       string `yaml:"uid"`
			Title     string `yaml:"title"`
			Condition string `yaml:"condition"`
			For       string `yaml:"for"`
			Data      []struct {
				RefID         string `yaml:"refId"`
				DatasourceUID string `yaml:"datasourceUid"`
				Model         struct {
					RawSQL     string `yaml:"rawSql"`
					Type       string `yaml:"type"`
					Expression string `yaml:"expression"`
					Conditions []struct {
						Evaluator struct {
							Type   string    `yaml:"type"`
							Params []float64 `yaml:"params"`
						} `yaml:"evaluator"`
					} `yaml:"conditions"`
				} `yaml:"model"`
			} `yaml:"data"`
			Labels map[string]string `yaml:"labels"`
		} `yaml:"rules"`
	} `yaml:"groups"`
}

func loadAlertFile(t *testing.T) alertFile {
	t.Helper()

	body, err := os.ReadFile("../grafana/provisioning/alerting/ml-live.yaml")
	if err != nil {
		t.Fatalf("read alert provisioning: %v", err)
	}
	var file alertFile
	if err := yaml.Unmarshal(body, &file); err != nil {
		t.Fatalf("parse alert provisioning: %v", err)
	}
	return file
}

func ruleByUID(t *testing.T, file alertFile, uid string) (rawSQL string, threshold float64, labels map[string]string) {
	t.Helper()

	for _, group := range file.Groups {
		for _, rule := range group.Rules {
			if rule.UID != uid {
				continue
			}
			for _, data := range rule.Data {
				if data.RefID == "A" {
					rawSQL = data.Model.RawSQL
				}
				for _, condition := range data.Model.Conditions {
					if len(condition.Evaluator.Params) > 0 {
						threshold = condition.Evaluator.Params[0]
					}
				}
			}
			return rawSQL, threshold, rule.Labels
		}
	}
	t.Fatalf("alert rule %q not found", uid)
	return "", 0, nil
}

// TestMLLiveAlertProvisioning — контракт алертов infra/4.3: degradation читает
// rolling MAE трёх production-заседаний и сравнивает с 100 б.п.; freshness
// проверяет наличие факта после последнего заседания (порог 3 дня). Оба алерта
// — warning, без contact point (маршрутизация у владельца).
func TestMLLiveAlertProvisioning(t *testing.T) {
	file := loadAlertFile(t)
	if file.APIVersion != 1 {
		t.Fatalf("apiVersion = %d, want 1", file.APIVersion)
	}
	if len(file.Groups) != 1 || file.Groups[0].Name != "ml-live" {
		t.Fatalf("groups = %+v, want one ml-live group", file.Groups)
	}
	if file.Groups[0].Folder != "ML live" {
		t.Fatalf("folder = %q, want ML live", file.Groups[0].Folder)
	}
	if len(file.Groups[0].Rules) != 2 {
		t.Fatalf("rules = %d, want 2", len(file.Groups[0].Rules))
	}

	degradationSQL, degradationThreshold, degradationLabels := ruleByUID(t, file, "ml-live-degradation")
	for _, want := range []string{
		"v_ml_live_accuracy",
		"stage = 'production'",
		"is_resolved = 1",
		"avg(abs_error_bp) OVER",
		"ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
		"argMax(rolling_mae_bp, target_date)",
		"model != 'gate-selftest'",
	} {
		if !strings.Contains(degradationSQL, want) {
			t.Errorf("degradation SQL must contain %q: %s", want, degradationSQL)
		}
	}
	if degradationThreshold != 100 {
		t.Errorf("degradation threshold = %v, want 100", degradationThreshold)
	}
	if degradationLabels["severity"] != "warning" || degradationLabels["contour"] != "ml-live" {
		t.Errorf("degradation labels = %v, want severity=warning contour=ml-live", degradationLabels)
	}

	freshnessSQL, freshnessThreshold, freshnessLabels := ruleByUID(t, file, "ml-live-freshness")
	for _, want := range []string{
		"v_ml_live_accuracy",
		"maxOrNull(target_date)",
		"target_date <= today()",
		"dateDiff('day', last_target, today()) > 3",
		"model != 'gate-selftest'",
	} {
		if !strings.Contains(freshnessSQL, want) {
			t.Errorf("freshness SQL must contain %q: %s", want, freshnessSQL)
		}
	}
	if freshnessThreshold != 3 {
		t.Errorf("freshness threshold = %v, want 3", freshnessThreshold)
	}
	if freshnessLabels["severity"] != "warning" || freshnessLabels["contour"] != "ml-live" {
		t.Errorf("freshness labels = %v, want severity=warning contour=ml-live", freshnessLabels)
	}
}
