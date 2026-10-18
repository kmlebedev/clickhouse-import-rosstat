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
	if !strings.Contains(cbrPitVersionView.Select, "groupBitXor(cityHash64(") ||
		!strings.Contains(cbrPitVersionView.Select, "hex(") ||
		!strings.Contains(cbrPitVersionView.Select, "GROUP BY source") {
		t.Errorf("v_point_in_time_version: не та версия набора: %s", cbrPitVersionView.Select)
	}
	if len(cbrPitVersionView.Columns) != 6 {
		t.Errorf("v_point_in_time_version: колонковых комментариев %d, want 6", len(cbrPitVersionView.Columns))
	}
}

// TestCbrPitAsOfViewContract — семантика среза: строгая граница по дате публикации, тай-брейк
// версий во всех argMax, группировка по (source, series, period_date) и отсутствие фильтров
// «только последний выпуск» — HAVING или условия по period_date скрыли бы последнюю известную
// версию ряда, исчезнувшего из нового выпуска (спека §2.3).
func TestCbrPitAsOfViewContract(t *testing.T) {
	sel := cbrPitAsOfView.Select
	if !strings.Contains(sel, "publication_date < {as_of:Date}") {
		t.Errorf("as-of: нет строгой границы: %s", sel)
	}
	if !strings.Contains(sel, "argMax(p.value, (p.published_at, p.vintage))") ||
		!strings.Contains(sel, "argMax(p.vintage, (p.published_at, p.vintage))") ||
		!strings.Contains(sel, "argMax(p.vintage_label, (p.published_at, p.vintage))") {
		t.Errorf("as-of: нет тай-брейка (published_at, vintage) во всех argMax: %s", sel)
	}
	if !strings.Contains(sel, "GROUP BY p.source, p.series, p.period_date") {
		t.Errorf("as-of: группировка не по (source, series, period_date): %s", sel)
	}
	if strings.Contains(sel, "HAVING") {
		t.Errorf("as-of: HAVING ограничил бы срез последним выпуском: %s", sel)
	}
	if strings.Count(sel, "WHERE") != 1 || strings.Contains(sel, "period_date <") || strings.Contains(sel, "period_date >") {
		t.Errorf("as-of: единственный WHERE должен быть publication_date < as_of, без фильтра period_date: %s", sel)
	}
	if len(cbrPitAsOfView.Columns) != 0 {
		t.Errorf("as-of: у параметризованной витрины не может быть колонковых комментариев, got %d", len(cbrPitAsOfView.Columns))
	}
}
