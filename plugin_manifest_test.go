package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const (
	pluginName    = "clickhouse-import-rosstat"
	pluginVersion = "0.1.0"
)

var (
	pluginManifestPaths = []string{
		"kimi.plugin.json",
		".claude-plugin/plugin.json",
		".codex-plugin/plugin.json",
		".cursor-plugin/plugin.json",
		"qwen-extension.json",
	}
	marketplaceManifestPaths = []string{
		".claude-plugin/marketplace.json",
		".agents/plugins/marketplace.json",
		".cursor-plugin/marketplace.json",
	}
	allManifestPaths = append(append([]string{}, pluginManifestPaths...), marketplaceManifestPaths...)
	forbiddenKey     = regexp.MustCompile(`(?i)(api[_-]?key|token|password|secret)`)
)

type pluginManifest struct {
	Name    string          `json:"name"`
	Version string          `json:"version"`
	Skills  json.RawMessage `json:"skills"`
	Agents  json.RawMessage `json:"agents"`
}

type marketplaceManifest struct {
	Name    string `json:"name"`
	Plugins []struct {
		Name   string          `json:"name"`
		Source json.RawMessage `json:"source"`
	} `json:"plugins"`
}

func TestLLMPluginManifests(t *testing.T) {
	t.Run("plugin manifests", func(t *testing.T) {
		cases := []struct {
			path       string
			wantsAgent bool
		}{
			{"kimi.plugin.json", true},
			{".claude-plugin/plugin.json", false},
			{".codex-plugin/plugin.json", false},
			{".cursor-plugin/plugin.json", false},
			{"qwen-extension.json", false},
		}

		for _, tc := range cases {
			t.Run(tc.path, func(t *testing.T) {
				var m pluginManifest
				readJSON(t, tc.path, &m)

				if m.Name != pluginName {
					t.Errorf("name = %q, want %q", m.Name, pluginName)
				}
				if m.Version != pluginVersion {
					t.Errorf("version = %q, want %q", m.Version, pluginVersion)
				}

				skills := pathList(t, tc.path+": skills", m.Skills)
				if len(skills) == 0 {
					t.Fatalf("%s: skills is empty", tc.path)
				}
				for _, path := range skills {
					assertRepoDir(t, tc.path+": skills", path)
					assertSkillDir(t, path)
				}

				agents := pathList(t, tc.path+": agents", m.Agents)
				if tc.wantsAgent {
					if len(agents) != 1 {
						t.Fatalf("%s: agents = %v, want exactly one file", tc.path, agents)
					}
					assertRepoFile(t, tc.path+": agents", agents[0])
				} else if len(agents) != 0 {
					t.Errorf("%s: agents = %v, want none", tc.path, agents)
				}
			})
		}
	})

	t.Run("marketplace manifests", func(t *testing.T) {
		cases := []struct {
			path          string
			marketplace   string
			wantSourceRaw string
		}{
			{".claude-plugin/marketplace.json", "clickhouse-import-rosstat", `"./"`},
			{".cursor-plugin/marketplace.json", "clickhouse-import-rosstat-marketplace", `"."`},
		}

		for _, tc := range cases {
			t.Run(tc.path, func(t *testing.T) {
				var m marketplaceManifest
				readJSON(t, tc.path, &m)

				if m.Name != tc.marketplace {
					t.Errorf("marketplace name = %q, want %q", m.Name, tc.marketplace)
				}
				if len(m.Plugins) != 1 {
					t.Fatalf("%s: plugins = %d, want 1", tc.path, len(m.Plugins))
				}
				if m.Plugins[0].Name != pluginName {
					t.Errorf("plugin name = %q, want %q", m.Plugins[0].Name, pluginName)
				}
				if got := compactJSON(m.Plugins[0].Source); got != tc.wantSourceRaw {
					t.Errorf("source = %s, want %s", got, tc.wantSourceRaw)
				}
			})
		}

		t.Run(".agents/plugins/marketplace.json", func(t *testing.T) {
			var m struct {
				Name    string `json:"name"`
				Plugins []struct {
					Name   string `json:"name"`
					Source struct {
						Source string `json:"source"`
						URL    string `json:"url"`
						Ref    string `json:"ref"`
					} `json:"source"`
				} `json:"plugins"`
			}
			readJSON(t, ".agents/plugins/marketplace.json", &m)

			if m.Name != "clickhouse-import-rosstat-marketplace" {
				t.Errorf("marketplace name = %q, want clickhouse-import-rosstat-marketplace", m.Name)
			}
			if len(m.Plugins) != 1 {
				t.Fatalf("plugins = %d, want 1", len(m.Plugins))
			}
			p := m.Plugins[0]
			if p.Name != pluginName {
				t.Errorf("plugin name = %q, want %q", p.Name, pluginName)
			}
			if p.Source.Source != "url" {
				t.Errorf("source.source = %q, want url", p.Source.Source)
			}
			if p.Source.URL != "https://github.com/kmlebedev/clickhouse-import-rosstat.git" {
				t.Errorf("source.url = %q", p.Source.URL)
			}
			if p.Source.Ref != "main" {
				t.Errorf("source.ref = %q, want main", p.Source.Ref)
			}
		})
	})

	t.Run("license", func(t *testing.T) {
		data, err := os.ReadFile("LICENSE")
		if err != nil {
			t.Fatalf("read LICENSE: %v", err)
		}
		if !strings.Contains(string(data), "MIT License") {
			t.Fatal("LICENSE does not contain MIT License")
		}
	})
}

func TestLLMPluginManifestsNoSecrets(t *testing.T) {
	for _, path := range allManifestPaths {
		var raw any
		readJSON(t, path, &raw)
		walkJSON(t, path, raw)
	}
}

func readJSON(t *testing.T, path string, dst any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := json.Unmarshal(data, dst); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
}

func pathList(t *testing.T, field string, raw json.RawMessage) []string {
	t.Helper()
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}

	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return []string{single}
	}

	var list []string
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("%s: want string or []string, got %s", field, raw)
	}
	return list
}

func assertRepoDir(t *testing.T, field, path string) {
	t.Helper()
	abs := assertRepoPath(t, field, path)
	info, err := os.Stat(abs)
	if err != nil {
		t.Fatalf("%s: %s: %v", field, path, err)
	}
	if !info.IsDir() {
		t.Fatalf("%s: %s is not a directory", field, path)
	}
}

func assertRepoFile(t *testing.T, field, path string) {
	t.Helper()
	abs := assertRepoPath(t, field, path)
	info, err := os.Stat(abs)
	if err != nil {
		t.Fatalf("%s: %s: %v", field, path, err)
	}
	if info.IsDir() {
		t.Fatalf("%s: %s is a directory, want a file", field, path)
	}
}

func assertRepoPath(t *testing.T, field, path string) string {
	t.Helper()
	if filepath.IsAbs(path) {
		t.Fatalf("%s: %s is absolute", field, path)
	}
	clean := filepath.Clean(path)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		t.Fatalf("%s: %s escapes the repository root", field, path)
	}

	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	abs := filepath.Join(root, clean)
	if abs != root && !strings.HasPrefix(abs, root+string(os.PathSeparator)) {
		t.Fatalf("%s: %s escapes the repository root", field, path)
	}
	return abs
}

func assertSkillDir(t *testing.T, path string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	found := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		found++
		skillPath := filepath.Join(path, entry.Name(), "SKILL.md")
		data, err := os.ReadFile(skillPath)
		if err != nil {
			t.Errorf("read %s: %v", skillPath, err)
			continue
		}
		if !strings.Contains(string(data), "name:") {
			t.Errorf("%s has no name: frontmatter field", skillPath)
		}
	}
	if found == 0 {
		t.Fatalf("%s contains no skill directories", path)
	}
}

func compactJSON(raw json.RawMessage) string {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return string(raw)
	}
	return buf.String()
}

func walkJSON(t *testing.T, path string, v any) {
	t.Helper()
	switch value := v.(type) {
	case map[string]any:
		for key, child := range value {
			if forbiddenKey.MatchString(key) {
				t.Errorf("%s: forbidden key %q", path, key)
			}
			walkJSON(t, path, child)
		}
	case []any:
		for _, child := range value {
			walkJSON(t, path, child)
		}
	}
}
