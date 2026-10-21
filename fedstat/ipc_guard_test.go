package fedstat

import "testing"

func TestFedstatRequireFullFetch(t *testing.T) {
	if err := fedstatRequireFullFetch("t", "s", true, 0); err == nil {
		t.Fatal("full run with 0 points: want error")
	}
	if err := fedstatRequireFullFetch("t", "s", true, 1); err != nil {
		t.Fatalf("full run with points: %v", err)
	}
	if err := fedstatRequireFullFetch("t", "s", false, 0); err != nil {
		t.Fatalf("incremental run with 0 points: %v", err)
	}
}
