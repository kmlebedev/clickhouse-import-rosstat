package cbr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
)

const mpPolarityModeEnv = "MP_SUMMARY_POLARITY_REPAIR"
const mpPolarityBackupEnv = "MP_SUMMARY_POLARITY_REPAIR_BACKUP"
const mpPolarityHostEnv = "MP_SUMMARY_POLARITY_REPAIR_HOST"

type mpPolarityIdentity struct {
	Date, Channel string
	Seq           uint16
}
type mpPolarityPin struct {
	mpPolarityIdentity
	Hash string
}

// Reviewed identities from #594 replay; never accept arbitrary dates or SQL.
var mpPolarityPins = []mpPolarityPin{
	{mpPolarityIdentity{"2024-02-16", "output_gap", 204}, "c74584c35b6d23736a96ac27a2bf54d85b16dab8d7c3e810b949212ee109aebe"},
	{mpPolarityIdentity{"2024-02-16", "current_inflation", 205}, "e82785828e44b77c91bd124ce5c855b070f1205b96b1de22e014f4f284e31099"},
	{mpPolarityIdentity{"2024-03-22", "output_gap", 152}, "c74584c35b6d23736a96ac27a2bf54d85b16dab8d7c3e810b949212ee109aebe"},
	{mpPolarityIdentity{"2024-03-22", "current_inflation", 153}, "e82785828e44b77c91bd124ce5c855b070f1205b96b1de22e014f4f284e31099"},
	{mpPolarityIdentity{"2024-04-26", "output_gap", 178}, "c74584c35b6d23736a96ac27a2bf54d85b16dab8d7c3e810b949212ee109aebe"},
	{mpPolarityIdentity{"2024-04-26", "current_inflation", 179}, "e82785828e44b77c91bd124ce5c855b070f1205b96b1de22e014f4f284e31099"},
	{mpPolarityIdentity{"2024-12-20", "lending_transmission", 162}, "34eebf7c59a7a9969c32343877e8fd76701655add03042bc6038a159a0b0be26"},
	{mpPolarityIdentity{"2024-12-20", "lending_transmission", 163}, "99f2e64168321fa1671f2e38486f2c3b0aa98b30d9d1f7e0ddd597663526fd5b"},
	{mpPolarityIdentity{"2024-12-20", "lending_transmission", 164}, "8abe6fe12a7a289c456f7ed2e3cf0bfb9a5f93af9322c1c9ff2a2c34eb5bcf45"},
	{mpPolarityIdentity{"2024-12-20", "output_gap", 164}, "8abe6fe12a7a289c456f7ed2e3cf0bfb9a5f93af9322c1c9ff2a2c34eb5bcf45"},
}

type mpPolarityRow struct {
	mpPolarityIdentity
	Kind, Text string
}
type mpPolarityBackup struct {
	Version        int
	Host, Database string
	Rows           []mpPolarityRow
}
type mpPolarityStore interface {
	ready(context.Context) error
	read(context.Context) ([]mpPolarityRow, error)
	insert(context.Context, []mpPolarityRow) error
	remove(context.Context, string, []mpPolarityRow) error
}

type mpPolarityState struct {
	Old, New  int
	Originals []mpPolarityRow
}

func mpCheckPolarityRows(rows []mpPolarityRow) (mpPolarityState, error) {
	var state mpPolarityState
	for _, pin := range mpPolarityPins {
		seen := make(map[string]bool)
		var original mpPolarityRow
		for _, row := range rows {
			if row.mpPolarityIdentity != pin.mpPolarityIdentity {
				continue
			}
			hash := sha256.Sum256([]byte(row.Text))
			if hex.EncodeToString(hash[:]) != pin.Hash {
				return state, fmt.Errorf("polarity repair: text mismatch for %v", pin.mpPolarityIdentity)
			}
			if row.Kind != mpKindRiskPro && row.Kind != mpKindRiskDez {
				return state, fmt.Errorf("polarity repair: unexpected kind %q", row.Kind)
			}
			if seen[row.Kind] {
				continue // Identical physical versions are safe; every text was hash-checked.
			}
			seen[row.Kind] = true
			if row.Kind == mpKindRiskPro {
				state.Old++
			} else {
				state.New++
			}
			original = row
			original.Kind = mpKindRiskPro
		}
		if len(seen) == 0 {
			return state, fmt.Errorf("polarity repair: missing identity %v", pin.mpPolarityIdentity)
		}
		state.Originals = append(state.Originals, original)
	}
	return state, nil
}

func mpReadPolarityBackup(path, host string) (mpPolarityBackup, error) {
	var backup mpPolarityBackup
	raw, err := os.ReadFile(path)
	if err != nil {
		return backup, err
	}
	if err := json.Unmarshal(raw, &backup); err != nil {
		return backup, fmt.Errorf("polarity backup: %w", err)
	}
	if backup.Version != 1 || backup.Host != host || backup.Database != "default" || len(backup.Rows) != len(mpPolarityPins) {
		return backup, errors.New("polarity backup: wrong version, destination or row count")
	}
	state, err := mpCheckPolarityRows(backup.Rows)
	if err != nil {
		return backup, err
	}
	if state.Old != len(mpPolarityPins) || state.New != 0 {
		return backup, errors.New("polarity backup: expected original risk_pro rows")
	}
	return backup, nil
}

// A backup is durable before any write to ClickHouse and is never overwritten.
func mpSavePolarityBackup(path, host string, rows []mpPolarityRow) error {
	raw, err := json.MarshalIndent(mpPolarityBackup{1, host, "default", rows}, "", "  ")
	if err != nil {
		return err
	}
	return mpWritePolarityFile(path, append(raw, '\n'))
}

func mpWritePolarityFile(path string, raw []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(raw)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr = directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func mpPolarityMarker(host string) string {
	hash := sha256.Sum256([]byte(host + "/default/cbr_mp_summary_channel"))
	return filepath.Join("/tmp", "rosstat-mp-polarity-"+hex.EncodeToString(hash[:16])+".pending")
}

func mpRunPolarityRepair(ctx context.Context, store mpPolarityStore, mode, path, host string) (int64, error) {
	if mode != "plan" && mode != "apply" && mode != "rollback" {
		return 0, fmt.Errorf("unknown polarity repair mode %q", mode)
	}
	if mode != "plan" {
		unlock, err := mpLockPolarityRepair(host)
		if err != nil {
			return 0, err
		}
		defer unlock()
		marker := mpPolarityMarker(host)
		if _, err := os.Lstat(marker); err == nil {
			return 0, fmt.Errorf("polarity repair: unresolved operation marker %s; manual server-query/mutation and data verification required", marker)
		} else if !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
		// A timed-out ALTER can still be running server-side. Never insert or change
		// direction while any earlier mutation on this table remains unfinished.
		if err := store.ready(ctx); err != nil {
			return 0, err
		}
	}
	rows, err := store.read(ctx)
	if err != nil {
		return 0, err
	}
	state, err := mpCheckPolarityRows(rows)
	if err != nil {
		return 0, err
	}
	log.Infof("cbr_mp_summary polarity %s: original=%d corrected=%d identities=%d", mode, state.Old, state.New, len(mpPolarityPins))
	if mode == "plan" {
		return 0, nil
	}
	if path == "" {
		return 0, errors.New("polarity repair requires backup path")
	}
	backup, err := mpReadPolarityBackup(path, host)
	if errors.Is(err, os.ErrNotExist) && mode == "apply" && state.Old == len(mpPolarityPins) && state.New == 0 {
		if err = mpSavePolarityBackup(path, host, state.Originals); err != nil {
			return 0, err
		}
		backup, err = mpReadPolarityBackup(path, host)
	}
	if err != nil {
		return 0, fmt.Errorf("polarity repair backup: %w", err)
	}
	desired, stale, remaining := mpKindRiskDez, mpKindRiskPro, state.Old
	if mode == "rollback" {
		desired, stale, remaining = mpKindRiskPro, mpKindRiskDez, state.New
	}
	if remaining == 0 {
		return 0, nil
	}
	for i := range backup.Rows {
		backup.Rows[i].Kind = desired
	}
	// Insert before deletion: interrupted operations may leave both versions, never
	// neither. A retry deduplicates the same replacement keys through FINAL.
	// Persist intent before the first write. On any ambiguous failure or crash,
	// even a request not registered in system.mutations yet cannot be raced by a
	// retry/rollback. Only explicit operator verification may clear a stale marker.
	marker := mpPolarityMarker(host)
	operationID := fmt.Sprintf("mp-polarity-%d", time.Now().UnixNano())
	intent := fmt.Sprintf("mode=%s\nhost=%s\nbackup=%s\noperation_id=%s\nstarted=%s\n", mode, host, path, operationID, time.Now().UTC().Format(time.RFC3339Nano))
	if err := mpWritePolarityFile(marker, []byte(intent)); err != nil {
		return 0, fmt.Errorf("polarity repair intent: %w", err)
	}
	if err = store.insert(clickhouse.Context(ctx, clickhouse.WithQueryID(operationID+"-insert")), backup.Rows); err != nil {
		return 0, err
	}
	if err = store.remove(clickhouse.Context(ctx, clickhouse.WithQueryID(operationID+"-delete")), stale, backup.Rows); err != nil {
		return 0, err
	}
	rows, err = store.read(ctx)
	if err != nil {
		return 0, err
	}
	state, err = mpCheckPolarityRows(rows)
	if err != nil {
		return 0, err
	}
	if (mode == "apply" && (state.New != len(mpPolarityPins) || state.Old != 0)) || (mode == "rollback" && (state.Old != len(mpPolarityPins) || state.New != 0)) {
		return 0, errors.New("polarity repair: postcondition failed; intent marker retained, manual resolution required")
	}
	if err := os.Remove(marker); err != nil {
		return 0, fmt.Errorf("polarity repair completed but marker cleanup failed: %w", err)
	}
	return int64(len(backup.Rows)), nil
}

// Same-host only: all processes/users share the stable /tmp lock inode. Never
// unlink it after unlocking, or a waiting process could lock an obsolete inode.
func mpLockPolarityRepair(host string) (func(), error) {
	hash := sha256.Sum256([]byte(host + "/default/cbr_mp_summary_channel"))
	path := filepath.Join("/tmp", "rosstat-mp-polarity-"+hex.EncodeToString(hash[:16])+".lock")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("polarity repair lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("polarity repair already running: %w", err)
	}
	return func() {
		if err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN); err != nil {
			log.Warnf("polarity repair unlock: %v", err)
		}
		if err := file.Close(); err != nil {
			log.Warnf("polarity repair lock close: %v", err)
		}
	}, nil
}

type mpPolarityCHStore struct{ conn driver.Conn }

func (s mpPolarityCHStore) ready(ctx context.Context) error {
	var pending uint64
	if err := s.conn.QueryRow(ctx, "SELECT count() FROM system.mutations WHERE database=currentDatabase() AND table='cbr_mp_summary_channel' AND is_done=0").Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return errors.New("polarity repair: unfinished table mutation; wait for completion before apply or rollback")
	}
	return nil
}

func (s mpPolarityCHStore) read(ctx context.Context) ([]mpPolarityRow, error) {
	rows, err := s.conn.Query(ctx, `SELECT meeting_date,channel,seq,kind,text FROM cbr_mp_summary_channel WHERE method='regex' AND model='' AND prompt_version=0 AND meeting_date IN ('2024-02-16','2024-03-22','2024-04-26','2024-12-20')`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []mpPolarityRow
	for rows.Next() {
		var date time.Time
		var r mpPolarityRow
		if err := rows.Scan(&date, &r.Channel, &r.Seq, &r.Kind, &r.Text); err != nil {
			return nil, err
		}
		r.Date = date.Format(time.DateOnly)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s mpPolarityCHStore) insert(ctx context.Context, rows []mpPolarityRow) error {
	batch := &mpBatch{ctx: ctx, conn: s.conn, table: mpSummaryChannelTable, insert: mpSummaryChannelInsert}
	defer batch.abort()
	for _, r := range rows {
		date, err := time.Parse(time.DateOnly, r.Date)
		if err != nil {
			return err
		}
		if err := batch.append(date, r.Channel, r.Kind, mpMethodRegex, "", uint16(mpPromptVersionRegex), r.Seq, r.Text); err != nil {
			return err
		}
	}
	return batch.send()
}

func mpPolarityDelete(kind string, rows []mpPolarityRow) (string, []any) {
	terms := make([]string, 0, len(rows))
	args := []any{kind}
	for _, r := range rows {
		terms = append(terms, "(meeting_date=toDate(?) AND channel=? AND seq=? AND text=?)")
		args = append(args, r.Date, r.Channel, r.Seq, r.Text)
	}
	return "ALTER TABLE " + mpSummaryChannelTable + " DELETE WHERE method='regex' AND model='' AND prompt_version=0 AND kind=? AND (" + strings.Join(terms, " OR ") + ") SETTINGS mutations_sync=2", args
}

func (s mpPolarityCHStore) remove(ctx context.Context, kind string, rows []mpPolarityRow) error {
	query, args := mpPolarityDelete(kind, rows)
	return s.conn.Exec(ctx, query, args...)
}

func mpPolarityRepairFromEnv(ctx context.Context, conn driver.Conn, mode string) (int64, error) {
	host := strings.TrimSpace(os.Getenv(mpPolarityHostEnv))
	if host == "" {
		return 0, errors.New("polarity repair requires expected hostName")
	}
	local, err := os.Hostname()
	if err != nil {
		return 0, err
	}
	dsn, err := url.Parse(os.Getenv("CLICKHOUSE_URL"))
	if err != nil {
		return 0, errors.New("polarity repair requires a local ClickHouse URL")
	}
	address := dsn.Hostname()
	if host != local || (address != "localhost" && address != "127.0.0.1" && address != "::1") {
		return 0, errors.New("polarity repair is restricted to same-host loopback ClickHouse")
	}
	var actual, database string
	if err := conn.QueryRow(ctx, "SELECT hostName(),currentDatabase()").Scan(&actual, &database); err != nil {
		return 0, err
	}
	if actual != host || database != "default" {
		return 0, errors.New("polarity repair destination mismatch")
	}
	return mpRunPolarityRepair(ctx, mpPolarityCHStore{conn}, mode, os.Getenv(mpPolarityBackupEnv), host)
}
