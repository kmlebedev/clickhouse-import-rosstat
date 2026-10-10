package dcf

import (
	"math"
	"testing"
)

func TestNdpiPerOzAroundThreshold(t *testing.T) {
	p := defaultParams // threshold 1900, surcharge 0.10, base 0 (база — допущение, в тесте явно 0)
	p.NdpiBaseUSDPerOz = 0

	cases := []struct {
		name string
		gold float64
		want float64
	}{
		{"ниже порога", 1500, 0},
		{"ровно на пороге", 1900, 0},
		{"выше порога", 4000, 210}, // 0.10 × (4000 − 1900)
		{"чуть выше порога", 1900.5, 0.05},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ndpiPerOz(c.gold, p); math.Abs(got-c.want) > 1e-9 {
				t.Fatalf("ndpiPerOz(%v) = %v, want %v", c.gold, got, c.want)
			}
		})
	}
}

func TestEscalateAccumulatesIPC(t *testing.T) {
	cases := []struct {
		name      string
		ipc       []float64
		yearIndex int
		want      float64
	}{
		{"нулевой ИПЦ — без изменений", []float64{0, 0, 0}, 2, 100},
		{"год 0 — база как есть", []float64{0.10, 0.10}, 0, 100},
		{"один год роста", []float64{0.10, 0.10}, 1, 110},
		{"два года накопления", []float64{0.10, 0.10}, 2, 121},
		{"индекс за пределами ряда", []float64{0.10}, 5, 110},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := escalate(100, c.ipc, c.yearIndex); math.Abs(got-c.want) > 1e-9 {
				t.Fatalf("escalate = %v, want %v", got, c.want)
			}
		})
	}
}
