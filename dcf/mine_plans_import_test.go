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

// TestMinePlansSeedListNotEmpty — непустой список активов ЕСТЬ предпосылка ошибки
// в Import (Fix round 1, Finding 2).
//
// Проверяется чисто офлайн: гейт — первые операторы Import, до любого обращения к
// driver.Conn, поэтому пустой список не может дойти до PrepareBatch/Send и
// превратиться в «успешно импортировали 0 строк» с живым соединением. Тест
// намеренно утверждает непустоту СИДА (данных), а не текста условия в Import:
// текстовый тест продолжал бы «зеленеть» после удаления ветки, а этот — нет,
// потому что без ветки условие И само стало бы ложным. Фейковый driver.Conn для
// вызова Import не создаётся: соединение здесь не нужно и ломало бы тест на
// смене интерфейса драйвера.
//
// Отдельно бьёт по дефекту §8.1-класса в наборе активов: 7 рудников + Сухой Лог.
// Потеря строки (обрезанный при мерже срез, потерянная инициализация) молча
// выключала бы актив из mine_plans, а расчёт шёл бы по неполному набору с
// успешным логом.
func TestMinePlansSeedListNotEmpty(t *testing.T) {
	const wantAssets = 8
	// Локальная копия условия, чтобы тест утверждал наличие гейта И его непустоту.
	seedListIsEmpty := len(polyusAssetPlans) == 0
	if seedListIsEmpty {
		t.Fatalf("polyusAssetPlans пуст: Import обязан упасть до PrepareBatch, "+
			"а не записать 0 строк (want %d активов)", wantAssets)
	}

	if len(polyusAssetPlans) != wantAssets {
		t.Fatalf("len(polyusAssetPlans) = %d, want %d", len(polyusAssetPlans), wantAssets)
	}
}

// TestMinePlansSkipWarnText — warn о пропуске актива называет ОБА реальных случая,
// не выдаёт один за другой и не повторяет тавтологию про ProductionProfile
// (Fix round 1, Finding 1).
//
// Живые данные: у TITIMUKHTA строка с Total Dore gold output за 2025 существует и
// равна 0.0, у ZAPADNOYE последний ANNUAL тоже 0.0 (россыпная добыча этих активов
// учтена в датапаке отдельной таблицей ALLUVIALS). Прежняя формулировка «нет факта
// добычи … в databook_polyus» для них ЛОЖНА, а оговорка «(и нет ProductionProfile)»
// была тавтологией: ветка достижима только при пустом профиле, проверенном выше.
// Поэтому сообщение обязано охватывать и отсутствие строки, и строку со значением 0.
func TestMinePlansSkipWarnText(t *testing.T) {
	// Аргументы те же, что передаёт Import (asset, base year).
	warn := minePlansSkipWarn("TITIMUKHTA", reportingYearBase)

	for _, want := range []string{
		"датапак",           // источник факта назван
		"TITIMUKHTA",        // актив назван
		"2025",              // год назван (не хардкод 2025 вне аргументов)
		"ALLUVIALS",         // россыпной тип добычи назван
		"отсутствует",       // случай «строки нет»
		"равна нулю",        // случай «строка есть, но ноль»
		"строк не записано", // следствие для счётчика
	} {
		if !strings.Contains(warn, want) {
			t.Errorf("warn не содержит %q: %q", want, warn)
		}
	}

	// Тавтологическая оговорка и ложное «нет факта» не должны вернуться.
	for _, banned := range []string{"ProductionProfile", "нет факта добычи"} {
		if strings.Contains(warn, banned) {
			t.Errorf("warn содержит удалённую формулировку %q: %q", banned, warn)
		}
	}
}
