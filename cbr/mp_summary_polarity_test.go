package cbr

import (
	"encoding/json"
	"os"
	"testing"
)

type mpPolarityCase struct {
	MeetingDate string           `json:"meeting_date"`
	Channel     string           `json:"channel"`
	Expected    string           `json:"expected_kind"`
	Blocks      []mpSummaryBlock `json:"blocks"`
}

type mpPolarityFixture struct {
	Cases []mpPolarityCase `json:"cases"`
	LLM   struct {
		mpPolarityCase
		Method        string `json:"method"`
		Model         string `json:"model"`
		PromptVersion int    `json:"prompt_version"`
		Kind          string `json:"kind"`
		Text          string `json:"text"`
	} `json:"llm_case"`
}

func readMPPolarityFixture(t *testing.T) mpPolarityFixture {
	t.Helper()
	b, err := os.ReadFile("testdata/mp_summary_polarity.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture mpPolarityFixture
	if err := json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

// Source paragraphs include parenthetical attribution and a singular risk heading;
// the negative cases mention disinflation without introducing a disinflationary list.
func TestMPChannelPolaritySourceRegression(t *testing.T) {
	fixture := readMPPolarityFixture(t)
	if len(fixture.Cases) != 6 {
		t.Fatal("expected four regression and two negative cases")
	}
	for _, c := range fixture.Cases {
		t.Run(c.MeetingDate, func(t *testing.T) {
			found := false
			for _, m := range tagMPChannels(c.Blocks) {
				if m.Channel != c.Channel {
					continue
				}
				found = true
				if m.Kind != c.Expected {
					t.Errorf("%s: got %s, want %s: %s", m.Channel, m.Kind, c.Expected, m.Text)
				}
			}
			if !found {
				t.Fatalf("missing %s evidence", c.Channel)
			}
		})
	}
}

// This is a saved model error, not a new inference or evidence of model accuracy.
// Keep provenance and verify that the evaluation distinguishes wrong from right.
func TestMPLLMSavedPolarityEvaluation(t *testing.T) {
	c := readMPPolarityFixture(t).LLM
	if c.Method != "llm" || c.Model != "zai-org/GLM-5.3-Flash" || c.PromptVersion != 1 {
		t.Fatal("saved evaluation provenance changed")
	}
	if c.Kind != mpKindRiskPro || c.Expected != mpKindRiskDez {
		t.Fatal("fixture must retain the observed error and independent expected label")
	}
	found := false
	for _, m := range tagMPChannels(c.Blocks) {
		if m.Channel == c.Channel && m.Text == c.Text {
			found = true
			if m.Kind != c.Expected {
				t.Fatalf("source context: got %s, want %s", m.Kind, c.Expected)
			}
		}
	}
	if !found {
		t.Fatal("saved LLM quotation not matched in the source context")
	}
	t.Logf("saved %s prompt=%d: observed=%s expected=%s (known semantic failure, no live inference)", c.Model, c.PromptVersion, c.Kind, c.Expected)
}

func TestMPChannelPolarityStateAndProse(t *testing.T) {
	for _, intro := range []string{
		"Дезинфляционные риски, по мнению участников обсуждения, выражены слабо.",
		"Ключевой дезинфляционный риск связан с кредитованием.",
	} {
		blocks := []mpSummaryBlock{
			{Block: mpBlockRisks, Kind: mpKindParagraph, Text: intro},
			{Block: mpBlockRisks, Kind: mpKindBullet, Text: "Замедление кредитования."},
			{Block: "Денежно-кредитные условия", Kind: mpKindParagraph, Text: "Замедление кредитования."},
		}
		var risk, prose bool
		for _, m := range tagMPChannels(blocks) {
			if m.Text == "Замедление кредитования." && m.Channel == "lending_transmission" {
				risk = risk || m.Kind == mpKindRiskDez
				prose = prose || m.Kind == mpKindProse
			}
		}
		if !risk || !prose {
			t.Errorf("intro %q: risk=%v prose=%v", intro, risk, prose)
		}
	}
}
