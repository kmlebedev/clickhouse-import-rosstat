package cbr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

var mpPolarityTestHost = fmt.Sprintf("unit-test-%d", os.Getpid())

type mpPolarityFakeStore struct {
	rows       []mpPolarityRow
	calls      []string
	insertFail bool
	deleteFail bool
	pending    bool
}

func (s *mpPolarityFakeStore) ready(context.Context) error {
	if s.pending {
		return errors.New("pending mutation")
	}
	return nil
}
func (s *mpPolarityFakeStore) read(context.Context) ([]mpPolarityRow, error) {
	return append([]mpPolarityRow(nil), s.rows...), nil
}
func (s *mpPolarityFakeStore) insert(_ context.Context, rows []mpPolarityRow) error {
	s.calls = append(s.calls, "insert")
	for i, r := range rows {
		found := false
		for _, old := range s.rows {
			if old.mpPolarityIdentity == r.mpPolarityIdentity && old.Kind == r.Kind {
				found = true
				break
			}
		}
		if !found {
			s.rows = append(s.rows, r)
		}
		if s.insertFail && i == 2 {
			return errors.New("partial batch failure")
		}
	}
	return nil
}
func (s *mpPolarityFakeStore) remove(_ context.Context, kind string, rows []mpPolarityRow) error {
	s.calls = append(s.calls, "delete")
	if s.deleteFail {
		return errors.New("mutation failed")
	}
	var keep []mpPolarityRow
	for _, old := range s.rows {
		remove := false
		for _, r := range rows {
			if old.mpPolarityIdentity == r.mpPolarityIdentity && old.Kind == kind && old.Text == r.Text {
				remove = true
			}
		}
		if !remove {
			keep = append(keep, old)
		}
	}
	s.rows = keep
	return nil
}
func mpOriginalPolarityRows(t *testing.T) []mpPolarityRow {
	t.Helper()
	fixture := readMPPolarityFixture(t)
	var out []mpPolarityRow
	for _, pin := range mpPolarityPins {
		text := ""
		for _, c := range fixture.Cases {
			if c.MeetingDate == pin.Date {
				for _, b := range c.Blocks {
					for _, sent := range mpChannelSentences(b.Text) {
						hash := sha256.Sum256([]byte(sent))
						if hex.EncodeToString(hash[:]) == pin.Hash {
							text = sent
						}
					}
				}
			}
		}
		if text == "" {
			t.Fatalf("pin not in reviewed source fixture: %v", pin.mpPolarityIdentity)
		}
		out = append(out, mpPolarityRow{pin.mpPolarityIdentity, mpKindRiskPro, text})
	}
	return out
}

func TestMPPolarityRepairApplyRetryRollback(t *testing.T) {
	original := mpOriginalPolarityRows(t)
	other := mpPolarityRow{mpPolarityIdentity{"2024-02-16", "expectations", 1}, mpKindRiskPro, "Other unaffected quote"}
	s := &mpPolarityFakeStore{rows: append(append([]mpPolarityRow(nil), original...), other)}
	path := filepath.Join(t.TempDir(), "backup.json")
	run := func(mode string, want int64) {
		t.Helper()
		n, err := mpRunPolarityRepair(context.Background(), s, mode, path, mpPolarityTestHost)
		if err != nil || n != want {
			t.Fatalf("%s: n=%d err=%v", mode, n, err)
		}
	}
	run("plan", 0)
	if len(s.calls) != 0 {
		t.Fatal("plan wrote to store")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("plan created backup")
	}
	run("apply", 10)
	backup, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatal("backup permissions")
	}
	run("apply", 0)
	if !reflect.DeepEqual(s.calls, []string{"insert", "delete"}) {
		t.Fatal(s.calls)
	}
	run("rollback", 10)
	run("rollback", 0)
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(backup) {
		t.Fatal("backup overwritten")
	}
	if len(s.rows) != 11 {
		t.Fatal("unrelated row lost")
	}
	state, err := mpCheckPolarityRows(s.rows)
	if err != nil || state.Old != 10 || state.New != 0 {
		t.Fatalf("rollback state %+v %v", state, err)
	}
}

func TestMPPolarityRepairInterruptedRetry(t *testing.T) {
	for _, failure := range []string{"insert", "delete"} {
		t.Run(failure, func(t *testing.T) {
			s := &mpPolarityFakeStore{rows: mpOriginalPolarityRows(t), insertFail: failure == "insert", deleteFail: failure == "delete"}
			path := filepath.Join(t.TempDir(), "backup.json")
			t.Cleanup(func() { _ = os.Remove(mpPolarityMarker(mpPolarityTestHost)) })
			if _, err := mpRunPolarityRepair(context.Background(), s, "apply", path, mpPolarityTestHost); err == nil {
				t.Fatal("expected failure")
			}
			state, err := mpCheckPolarityRows(s.rows)
			if err != nil || state.Old != 10 {
				t.Fatalf("originals lost: %+v %v", state, err)
			}
			if failure == "insert" && len(s.calls) != 1 {
				t.Fatal("deletion after failed batch")
			}
			// An accepted write can outlive a client error before showing in mutations.
			// Neither retry nor rollback may proceed until explicit operator resolution.
			for _, mode := range []string{"apply", "rollback"} {
				if _, err := mpRunPolarityRepair(context.Background(), s, mode, path, mpPolarityTestHost); err == nil {
					t.Fatal("uncertain operation allowed")
				}
			}
			if err := os.Remove(mpPolarityMarker(mpPolarityTestHost)); err != nil {
				t.Fatal(err)
			} // Simulated manual verification of the synchronous fake.
			s.insertFail = false
			s.deleteFail = false
			if n, err := mpRunPolarityRepair(context.Background(), s, "apply", path, mpPolarityTestHost); err != nil || n != 10 {
				t.Fatalf("retry: %d %v", n, err)
			}
			state, err = mpCheckPolarityRows(s.rows)
			if err != nil || state.Old != 0 || state.New != 10 {
				t.Fatalf("retry state %+v %v", state, err)
			}
		})
	}
}

func TestMPPolarityRepairFailClosed(t *testing.T) {
	for _, scenario := range []string{"text", "missing", "kind", "backup", "host", "partial-no-backup", "bad-mode"} {
		t.Run(scenario, func(t *testing.T) {
			s := &mpPolarityFakeStore{rows: mpOriginalPolarityRows(t)}
			path := filepath.Join(t.TempDir(), "backup.json")
			t.Cleanup(func() { _ = os.Remove(mpPolarityMarker(mpPolarityTestHost)) })
			mode := "apply"
			switch scenario {
			case "text":
				s.rows[0].Text += "changed"
			case "missing":
				s.rows = s.rows[1:]
			case "duplicate":
				s.rows = append(s.rows, s.rows[0])
			case "kind":
				s.rows[0].Kind = mpKindProse
			case "backup":
				if err := os.WriteFile(path, []byte("broken"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "host":
				if err := mpSavePolarityBackup(path, "other-host", s.rows); err != nil {
					t.Fatal(err)
				}
			case "partial-no-backup":
				s.rows[0].Kind = mpKindRiskDez
			case "bad-mode":
				mode = "force"
			}
			if _, err := mpRunPolarityRepair(context.Background(), s, mode, path, mpPolarityTestHost); err == nil {
				t.Fatal("expected fail closed")
			}
			if len(s.calls) != 0 {
				t.Fatal("store mutated despite preflight failure")
			}
		})
	}
}

func TestMPPolarityRepairDeleteIsBounded(t *testing.T) {
	rows := mpOriginalPolarityRows(t)
	query, args := mpPolarityDelete(mpKindRiskPro, rows)
	for _, fragment := range []string{"method='regex'", "model=''", "prompt_version=0", "kind=?", "mutations_sync=2"} {
		if !strings.Contains(query, fragment) {
			t.Fatal("missing guard", fragment)
		}
	}
	if strings.Count(query, "meeting_date=toDate(?) AND channel=? AND seq=? AND text=?") != 10 || len(args) != 41 || args[0] != mpKindRiskPro {
		t.Fatal("wrong predicate scope")
	}
	for _, r := range rows {
		if strings.Contains(query, r.Text) {
			t.Fatal("text interpolated into SQL")
		}
	}
}

func TestMPPolarityRepairPendingAndConcurrent(t *testing.T) {
	s := &mpPolarityFakeStore{rows: mpOriginalPolarityRows(t), pending: true}
	path := filepath.Join(t.TempDir(), "backup.json")
	for _, mode := range []string{"apply", "rollback"} {
		if _, err := mpRunPolarityRepair(context.Background(), s, mode, path, mpPolarityTestHost); err == nil {
			t.Fatal("pending mutation allowed")
		}
	}
	if len(s.calls) != 0 {
		t.Fatal("writes during pending mutation")
	}
	unlock, err := mpLockPolarityRepair(mpPolarityTestHost)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	s.pending = false
	if _, err := mpRunPolarityRepair(context.Background(), s, "apply", path, mpPolarityTestHost); err == nil {
		t.Fatal("concurrent operation allowed")
	}
	if len(s.calls) != 0 {
		t.Fatal("writes during concurrent operation")
	}
}
func TestMPPolarityRepairPhysicalVersions(t *testing.T) {
	rows := mpOriginalPolarityRows(t)
	rows = append(rows, rows[0])
	state, err := mpCheckPolarityRows(rows)
	if err != nil || state.Old != 10 {
		t.Fatalf("identical physical versions: %+v %v", state, err)
	}
	rows[len(rows)-1].Text = "hidden conflicting physical version"
	if _, err := mpCheckPolarityRows(rows); err == nil {
		t.Fatal("hidden text conflict accepted")
	}
}
