package cbr

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"sort"
	"testing"
)

// Offline replay consumes an explicit MCP export. It never connects to a database
// or a model. Full source texts remain outside git; the report contains hashes.
func TestMPChannelPolarityReplay(t *testing.T) {
	input := os.Getenv("MP_CHANNEL_REPLAY_INPUT")
	if input == "" {
		t.Skip("set MP_CHANNEL_REPLAY_INPUT to a read-only MCP export")
	}
	type annotation struct {
		Channel       string `json:"channel"`
		Kind          string `json:"kind"`
		Method        string `json:"method"`
		Model         string `json:"model"`
		PromptVersion int    `json:"prompt_version"`
		Seq           int    `json:"seq"`
		Text          string `json:"text"`
	}
	var snapshot struct {
		ObservedAt string `json:"observed_at"`
		Documents  []struct {
			Date        string           `json:"meeting_date"`
			Blocks      []mpSummaryBlock `json:"blocks"`
			Annotations []annotation     `json:"annotations"`
		} `json:"documents"`
	}
	b, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Documents) == 0 {
		t.Fatal("empty snapshot")
	}
	type key struct {
		Channel string
		Seq     int
		Text    string
	}
	type change struct {
		Channel string `json:"channel"`
		Seq     int    `json:"seq"`
		Hash    string `json:"text_sha256"`
		Before  string `json:"before"`
		After   string `json:"after"`
	}
	type group struct {
		Method  string `json:"method"`
		Model   string `json:"model"`
		Version int    `json:"prompt_version"`
		Rows    int    `json:"stored_rows"`
	}
	type result struct {
		Date    string   `json:"meeting_date"`
		Before  int      `json:"regex_before"`
		After   int      `json:"regex_recomputed"`
		Changes []change `json:"changes"`
		Groups  []group  `json:"stored_groups"`
	}
	var results []result
	for _, d := range snapshot.Documents {
		// MCP exports may order block names alphabetically. Restore document order
		// before computing the global fragment sequence used by stored annotations.
		order := make(map[string]int, len(mpSummaryBlocks))
		for i, name := range mpSummaryBlocks {
			order[name] = i
		}
		for _, b := range d.Blocks {
			if _, ok := order[b.Block]; !ok {
				t.Fatalf("%s: unknown block %q", d.Date, b.Block)
			}
		}
		sort.Slice(d.Blocks, func(i, j int) bool {
			if d.Blocks[i].Block != d.Blocks[j].Block {
				return order[d.Blocks[i].Block] < order[d.Blocks[j].Block]
			}
			return d.Blocks[i].Seq < d.Blocks[j].Seq
		})
		r := result{Date: d.Date, Changes: []change{}}
		before := make(map[key]string)
		groups := make(map[group]int)
		for _, a := range d.Annotations {
			groups[group{Method: a.Method, Model: a.Model, Version: a.PromptVersion}]++
			if a.Method == mpMethodRegex {
				k := key{a.Channel, a.Seq, a.Text}
				if _, exists := before[k]; exists {
					t.Fatalf("%s: duplicate regex identity", d.Date)
				}
				before[k] = a.Kind
			}
		}
		r.Before = len(before)
		for g, n := range groups {
			g.Rows = n
			r.Groups = append(r.Groups, g)
		}
		sort.Slice(r.Groups, func(i, j int) bool {
			a, b := r.Groups[i], r.Groups[j]
			if a.Method != b.Method {
				return a.Method < b.Method
			}
			if a.Model != b.Model {
				return a.Model < b.Model
			}
			return a.Version < b.Version
		})
		for _, m := range tagMPChannels(d.Blocks) {
			r.After++
			k := key{m.Channel, m.Seq, m.Text}
			old, exists := before[k]
			if !exists {
				t.Fatalf("%s: new regex identity beyond polarity: %s/%d", d.Date, m.Channel, m.Seq)
			}
			if old != m.Kind {
				hash := sha256.Sum256([]byte(m.Text))
				r.Changes = append(r.Changes, change{m.Channel, m.Seq, hex.EncodeToString(hash[:]), old, m.Kind})
			}
			delete(before, k)
		}
		if len(before) != 0 {
			t.Fatalf("%s: %d source identities lost", d.Date, len(before))
		}
		results = append(results, r)
	}
	report := struct {
		ObservedAt string   `json:"observed_at"`
		InputHash  string   `json:"input_sha256"`
		Scope      string   `json:"scope"`
		Results    []result `json:"results"`
	}{ObservedAt: snapshot.ObservedAt, Scope: "offline regex replay; stored LLM groups only, no inference or DB writes", Results: results}
	hash := sha256.Sum256(b)
	report.InputHash = hex.EncodeToString(hash[:])
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	output := os.Getenv("MP_CHANNEL_REPLAY_OUT")
	if output == "" || output == input {
		t.Fatal("MP_CHANNEL_REPLAY_OUT must be a separate output path")
	}
	if err := os.WriteFile(output, append(encoded, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("replayed %d documents; report %s", len(results), output)
}
