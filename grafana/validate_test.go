package main

import (
	"testing"

	"github.com/grafana/grafana-foundation-sdk/go/common"
)

func TestValidateAcceptsGenerated(t *testing.T) {
	d, err := NewGoldThesis().Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(d); err != nil {
		t.Fatalf("сгенерированный дашборд не прошёл валидацию: %v", err)
	}
}

func TestValidateRejectsForeignDatasource(t *testing.T) {
	d, err := NewGoldThesis().Build()
	if err != nil {
		t.Fatal(err)
	}
	panels := panelsOf(d)
	if len(panels) == 0 {
		t.Fatal("панелей нет")
	}
	foreign := "bdx7lxrdelxq8b"
	panels[0].Datasource = &common.DataSourceRef{Type: ptr(DatasourceType), Uid: &foreign}
	if err := Validate(d); err == nil {
		t.Fatal("чужой UID датасорса не отвергнут")
	}
}
