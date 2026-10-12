package main

import (
	"encoding/json"
	"testing"
)

func TestCHQueryMarshalsPluginShape(t *testing.T) {
	q, err := NewCHQuery("A", "SELECT 1").Format(0).QueryType("timeseries").Build()
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if m["refId"] != "A" || m["rawSql"] != "SELECT 1" || m["editorType"] != "sql" ||
		m["format"] != float64(0) || m["queryType"] != "timeseries" || m["pluginVersion"] != "4.3.2" {
		t.Fatalf("неожиданный JSON запроса: %s", b)
	}
}

func TestCHQueryVariantUsesDatasourceType(t *testing.T) {
	if got := CHQueryVariant().Identifier; got != DatasourceType {
		t.Fatalf("identifier варианта: %q", got)
	}
}
