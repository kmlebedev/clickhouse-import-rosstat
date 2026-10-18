package cbr

import (
	"strings"
	"testing"
)

func TestCbrPitViewsContract(t *testing.T) {
	names := make([]string, 0, len(cbrPitViews))
	for _, v := range cbrPitViews {
		names = append(names, v.Name)
	}
	want := []string{"v_point_in_time", "v_point_in_time_asof", "v_point_in_time_version"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("витрины слоя = %v, want %v", names, want)
	}
	for _, v := range cbrPitViews {
		if len(v.Tables) != 1 || v.Tables[0] != "cbr_dip_bulletin" {
			t.Errorf("%s: Tables = %v, want [cbr_dip_bulletin]", v.Name, v.Tables)
		}
		if strings.TrimSpace(v.Comment) == "" {
			t.Errorf("%s: пустой комментарий таблицы", v.Name)
		}
	}
	if !strings.Contains(cbrPitView.Select, "'cbr_dip_bulletin' AS source") ||
		!strings.Contains(cbrPitView.Select, "date AS period_date") ||
		!strings.Contains(cbrPitView.Select, "publication_date AS published_at") ||
		!strings.Contains(cbrPitView.Select, "release_id AS vintage") ||
		!strings.Contains(cbrPitView.Select, "release AS vintage_label") {
		t.Errorf("v_point_in_time: не та нормализация колонок: %s", cbrPitView.Select)
	}
	if len(cbrPitView.Columns) != 7 {
		t.Errorf("v_point_in_time: колонковых комментариев %d, want 7", len(cbrPitView.Columns))
	}
	if !strings.Contains(cbrPitAsOfView.Select, "publication_date < {as_of:Date}") ||
		!strings.Contains(cbrPitAsOfView.Select, "argMax(p.value, (p.published_at, p.vintage))") ||
		!strings.Contains(cbrPitAsOfView.Select, "GROUP BY p.source, p.series, p.period_date") ||
		!strings.Contains(cbrPitAsOfView.Select, ") AS p") {
		t.Errorf("v_point_in_time_asof: не та семантика среза: %s", cbrPitAsOfView.Select)
	}
	if len(cbrPitAsOfView.Columns) != 0 {
		t.Errorf("v_point_in_time_asof: у параметризованной витрины не может быть колонковых комментариев, got %d", len(cbrPitAsOfView.Columns))
	}
	if !strings.Contains(cbrPitVersionView.Select, "groupBitXor(cityHash64(") ||
		!strings.Contains(cbrPitVersionView.Select, "hex(") ||
		!strings.Contains(cbrPitVersionView.Select, "GROUP BY source") {
		t.Errorf("v_point_in_time_version: не та версия набора: %s", cbrPitVersionView.Select)
	}
}
