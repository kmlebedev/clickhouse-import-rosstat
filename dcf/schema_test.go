package dcf

import (
	"strings"
	"testing"
)

func TestNavByAssetKeyKeepsDecksAndContours(t *testing.T) {
	for _, want := range []string{"deck LowCardinality(String)", "contour LowCardinality(String)",
		"ORDER BY (run_id, deck, asset, contour)"} {
		if !strings.Contains(navByAssetCreateTable, want) {
			t.Errorf("nav_by_asset DDL must contain %q: without it the second deck or contour "+
				"silently overwrites the first", want)
		}
	}
}
