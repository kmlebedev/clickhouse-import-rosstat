package views

import (
	"strings"
	"testing"
)

// Витрина обязана быть зарегистрирована в импортёре gold_views: без этого её
// никто не создаст, и грант в sql/mcp_kimi_reader.sql упадёт на несуществующей
// витрине. Проверка идёт по САМОМУ списку импортёра (goldViewsList) — см.
// комментарий у TestDcfAssumptionsViewIsBuiltByGoldViews.
func TestImfMacroViewIsBuiltByGoldViews(t *testing.T) {
	var found bool

	for _, v := range goldViewsList {
		if v.Name == imfMacroView.Name {
			found = true
		}
	}

	if !found {
		t.Fatalf("%s must be created by the gold_views importer: it is missing from goldViewsList", imfMacroView.Name)
	}

	if imfMacroView.Comment == "" {
		t.Fatalf("%s has no table comment", imfMacroView.Name)
	}

	for _, col := range []string{"date", "series", "value"} {
		if imfMacroView.Columns[col] == "" {
			t.Fatalf("%s: column %q has no comment", imfMacroView.Name, col)
		}
	}

	if !strings.Contains(imfMacroView.Select, "source = 'imf'") {
		t.Fatalf("%s select must filter source = 'imf', got %q", imfMacroView.Name, imfMacroView.Select)
	}

	if len(imfMacroView.Tables) != 1 || imfMacroView.Tables[0] != "macro_series" {
		t.Fatalf("%s tables = %v, want [macro_series]", imfMacroView.Name, imfMacroView.Tables)
	}
}
