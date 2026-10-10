package dcf

import (
	"strings"
	"testing"
)

// TestMinePlansSeederName — имя шага DAG и CLICKHOUSE_IMPORT_STAT.
//
// Имя стабильно: dagu запускает шаг по нему (make import STAT=dcf_mine_plans), и
// переименование разорвало бы связку «шаг DAG ↔ импортёр» молча — шаг просто
// перестал бы находиться, а прогон выглядел бы успешным без записи строк.
func TestMinePlansSeederName(t *testing.T) {
	if got := (&minePlansSeeder{}).Name(); got != "dcf_mine_plans" {
		t.Fatalf("Name() = %q, want dcf_mine_plans", got)
	}
}

// TestActualProductionQueryShape — запрос факта адресован датапаку (per-mine), а
// не релизной метрике (Review Focus 4).
//
// Проверяется ТЕКСТ запроса, а не его результат: живой ClickHouse здесь не нужен,
// и тест ловит ровно ту ошибку, которой не видно в логе, — факт по активу,
// прочитанный из company_financials (там gold_output консолидирован по Группе).
// Такой запрос вернул бы ненулевое число, база плана каждого актива стала бы
// групповой добычей, и NAV всех восьми активов разошёлся бы без единой ошибки.
func TestActualProductionQueryShape(t *testing.T) {
	q := actualProductionSelect

	for _, want := range []string{"databook_polyus", "Total Dore gold output", "ANNUAL"} {
		if !strings.Contains(q, want) {
			t.Errorf("запрос не содержит %q: %q", want, q)
		}
	}

	// Тот же класс дефекта, что и §8.1: месяц/квартал берутся из подписи периода
	// (имени метрики), а не из datum. Год обязан читаться из колонки date.
	if !strings.Contains(q, "toYear(date)") {
		t.Errorf("год не читается из колонки date (дефект §8.1): %q", q)
	}

	if strings.Contains(q, "company_financials") || strings.Contains(q, "gold_output'") {
		t.Errorf("запрос тянет релизную метрику вместо датапаковой: %q", q)
	}
}
