package fedstat

import "testing"

// TestFedstatProductOrderComplete — порядок POST-формы обязан содержать каждый
// filterID ровно один раз: сервер ЕМИСС разбирает selectedFilterIds позиционно и
// при чужом порядке отвечает пустым GenericData (см. fedstatFormBody).
func TestFedstatProductOrderComplete(t *testing.T) {
	byFilter := make(map[string]bool, len(fedstatProducts))
	for _, product := range fedstatProducts {
		if byFilter[product.filterID] {
			t.Errorf("duplicate filterID %q in fedstatProducts", product.filterID)
		}
		byFilter[product.filterID] = true
	}
	if len(fedstatProductOrder) != len(fedstatProducts) {
		t.Fatalf("fedstatProductOrder = %d, want %d", len(fedstatProductOrder), len(fedstatProducts))
	}
	seen := make(map[string]bool, len(fedstatProductOrder))
	for _, filterID := range fedstatProductOrder {
		if !byFilter[filterID] {
			t.Errorf("filterID %q from order is absent in fedstatProducts", filterID)
		}
		if seen[filterID] {
			t.Errorf("duplicate filterID %q in fedstatProductOrder", filterID)
		}
		seen[filterID] = true
	}
}
