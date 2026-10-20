package ppi

import (
	"math"
	"testing"
	"time"
)

func TestComputeMetrics(t *testing.T) {
	points := []saPoint{
		{Date: month(2021, 1, 31), SA: 100},
		{Date: month(2021, 2, 28), SA: 101},
		{Date: month(2021, 3, 31), SA: 102.01},
		{Date: month(2021, 4, 30), SA: 103.0301},
	}
	metrics, err := computeMetrics(points)
	if err != nil {
		t.Fatalf("computeMetrics: %v", err)
	}
	if len(metrics) != 4 {
		t.Fatalf("точек %d, want 4", len(metrics))
	}
	if metrics[0].MMSA != nil || metrics[0].SAAR != nil || metrics[0].SAAR3 != nil {
		t.Fatalf("первая точка должна быть NULL: %+v", metrics[0])
	}
	saar1 := (math.Pow(1.01, 12) - 1) * 100
	for i := 1; i < len(metrics); i++ {
		if metrics[i].MMSA == nil || metrics[i].SAAR == nil {
			t.Fatalf("точка %d: m/m SA и SAAR должны быть заполнены", i)
		}
		almostEqual(t, *metrics[i].MMSA, 1, 1e-9, "mm_sa")
		almostEqual(t, *metrics[i].SAAR, saar1, 1e-9, "saar")
	}
	if metrics[1].SAAR3 != nil {
		t.Fatalf("вторая точка: saar3 должен быть NULL")
	}
	for i := 2; i < len(metrics); i++ {
		if metrics[i].SAAR3 == nil {
			t.Fatalf("точка %d: saar3 должен быть заполнен", i)
		}
		almostEqual(t, *metrics[i].SAAR3, saar1, 1e-9, "saar3")
	}
}

func TestComputeMetricsKnownSAAR(t *testing.T) {
	metrics, err := computeMetrics([]saPoint{
		{Date: month(2021, 1, 31), SA: 100},
		{Date: month(2021, 2, 28), SA: 110},
	})
	if err != nil {
		t.Fatalf("computeMetrics: %v", err)
	}
	if metrics[1].MMSA == nil || metrics[1].SAAR == nil {
		t.Fatal("вторая точка без метрик")
	}
	almostEqual(t, *metrics[1].MMSA, 10, 1e-9, "mm_sa")
	almostEqual(t, *metrics[1].SAAR, (math.Pow(1.10, 12)-1)*100, 1e-9, "saar")
}

func TestComputeMetricsGap(t *testing.T) {
	_, err := computeMetrics([]saPoint{
		{Date: month(2021, 1, 31), SA: 100},
		{Date: month(2021, 3, 31), SA: 101},
	})
	if err == nil {
		t.Fatal("ожидалась ошибка пропуска месяца")
	}
}

func TestComputeMetricsNonPositive(t *testing.T) {
	_, err := computeMetrics([]saPoint{
		{Date: month(2021, 1, 31), SA: 100},
		{Date: month(2021, 2, 28), SA: 0},
	})
	if err == nil {
		t.Fatal("ожидалась ошибка неположительного SA")
	}
}

func TestComputeMetricsEmpty(t *testing.T) {
	if _, err := computeMetrics(nil); err == nil {
		t.Fatal("ожидалась ошибка пустого ряда")
	}
}

func TestComputeMetricsUnsorted(t *testing.T) {
	_, err := computeMetrics([]saPoint{
		{Date: time.Date(2021, 2, 28, 0, 0, 0, 0, time.UTC), SA: 100},
		{Date: time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC), SA: 100},
	})
	if err == nil {
		t.Fatal("ожидалась ошибка неотсортированного ряда")
	}
}
