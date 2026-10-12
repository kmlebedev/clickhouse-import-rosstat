package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/grafana/grafana-foundation-sdk/go/resource"
)

func TestManifestWrapper(t *testing.T) {
	d, err := NewGoldThesis().Build()
	if err != nil {
		t.Fatal(err)
	}
	m := Manifest(d)
	if m.ApiVersion != ApiVersion || m.Kind != resource.DashboardKind || m.Metadata.Name != DashboardUID {
		t.Fatalf("обёртка: %+v", m)
	}
}

func TestPricePanelUsesDatasourceConstant(t *testing.T) {
	d, err := NewGoldThesis().Build()
	if err != nil {
		t.Fatal(err)
	}
	panels := panelsOf(d)
	if len(panels) == 0 {
		t.Fatal("панелей нет")
	}
	for _, p := range panels {
		if p.Datasource == nil || p.Datasource.Uid == nil || *p.Datasource.Uid != DatasourceUID {
			t.Fatalf("панель %q без константного UID датасорса", panelTitle(p))
		}
		if p.Datasource.Type == nil || *p.Datasource.Type != DatasourceType {
			t.Fatalf("панель %q: тип датасорса %v", panelTitle(p), p.Datasource.Type)
		}
	}
}

func TestGenerateDeterministic(t *testing.T) {
	d1, err := NewGoldThesis().Build()
	if err != nil {
		t.Fatal(err)
	}
	d2, err := NewGoldThesis().Build()
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := json.MarshalIndent(Manifest(d1), "", "  ")
	b2, _ := json.MarshalIndent(Manifest(d2), "", "  ")
	if !bytes.Equal(b1, b2) {
		t.Fatal("генерация недетерминирована")
	}
}
