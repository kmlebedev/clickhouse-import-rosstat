package dcf

import (
	"fmt"
	"sort"
	"strconv"

	log "github.com/sirupsen/logrus"
)

// uncoveredPlanYears — пары «актив:год» из планов, которым НЕ нашлось цены ни в
// одном деке. Имена строк (структура Map из readDecks) значения не имеют: деков
// может быть три или больше, но цена года важна сама по себе — модель берёт её из
// конкретного дека лишь на этапе начисления.
//
// Почему это отдельная проверка, а не «и так видно по логу»: npvLOM (model.go:137-142)
// при отсутствии цены на год плана печатает Warnf и ПРОПУСКАЕТ год, но индекс года
// продолжает расти (эскалация ИПЦ и дисконт идут по календарю) и navRows всё равно
// пишет строку с суммой по остальным годам. То есть частично покрытый план даёт
// тихо заниженный NPV — в пределе ровно ноль, — а строка в nav_by_asset при этом
// выглядит как посчитанная, и отличить «посчитан по нулю лет» от «актив никогда не
// моделировался» по ней нельзя. Гард называет конкретные пары, чтобы дежурный по
// DAG видел, ЧТО именно осталось без цены, а не только «год пропущен».
//
// Сортировка — договор воспроизводимости, тот же, что в checkInputs для extra-деков:
// порядок обхода map в Go случаен, а лог должен читаться одинаково между прогонами
// (иначе дежурный сравнивает шум).
func uncoveredPlanYears(plans []MinePlanRecord, decks map[string][]DeckYear) []string {
	covered := deckYearSet(decks)

	uncovered := make([]string, 0)
	for _, plan := range plans {
		for _, y := range plan.Years {
			if _, ok := covered[y.Year]; !ok {
				uncovered = append(uncovered, plan.Asset+":"+strconv.Itoa(int(y.Year)))
			}
		}
	}

	sort.Strings(uncovered)

	return uncovered
}

// checkYearCoverage — гард пересечения годов плана и дека перед расчётом.
//
// Два уровня реакции, и это не произвол, а разные состояния входных данных:
//
//   - нет НИ ОДНОГО пересечения (годы всех планов не встречаются ни в одном деке) —
//     ошибка. Такой расчёт заведомо даст нулевой или мусорный NPV при непустых
//     mine_plans, и продолжать нельзя: успешный лог на нулевом NAV — ровно то, во
//     что гард и не должен дать превратиться. Это же состояние ловится, когда сид
//     не прошёл вовсе (пустая gold_prices отвергнута assertSpotPrice) или
//     price_decks засеяна под другой календарь;
//   - частичное покрытие (какие-то годы есть, каких-то нет) — только Warnf и nil.
//     Оставшиеся годы всё ещё считаются корректно, а падение покраснило бы
//     `make import STAT=dcf_engine` и DAG на recoverable-состоянии БД: философия
//     среза — warn-and-continue для неполных входов (dcf/engine.go:96, тот же выбор
//     сделан в checkInputs для пропавших деков). Отдельно это важно для БД, где
//     price_decks заполнена ЧАСТИЧНО (например, до сида из Task 1): там Import
//     вообще не пересеивает деки (сид идёт только на пустой таблице), и warning —
//     единственный способ увидеть непокрытые пары.
//
// Пустой план — не ошибка: Import отсекает его раньше отдельной веткой, а функция
// остаётся самостоятельным контрактом (как и checkInputs).
func checkYearCoverage(plans []MinePlanRecord, decks map[string][]DeckYear) error {
	if len(plans) == 0 {
		return nil
	}

	uncovered := uncoveredPlanYears(plans, decks)
	if len(uncovered) == 0 {
		return nil
	}

	planYearsSet := planYearSet(plans)
	deckYearsSet := deckYearSet(decks)

	// Решение — по числу ПЕРЕСЕЧЁННЫХ годов, а не по числу непокрытых пар: пар
	// больше, чем годов, как только два актива делят один год, и сравнение счётчиков
	// на такой раскладке (обычной для этого репо — engine_test, seed_test) ломается в
	// обе стороны: полное отсутствие покрытия ушло бы в Warnf и дало нулевой NAV, а
	// частичное — в ошибку и покраснило бы DAG на recoverable-состоянии БД.
	coveredYears := 0
	for y := range planYearsSet {
		if _, ok := deckYearsSet[y]; ok {
			coveredYears++
		}
	}

	planYears := formatYears(sortedYearSet(planYearsSet))
	deckYears := formatYears(sortedYearSet(deckYearsSet))

	if coveredYears == 0 {
		return fmt.Errorf("price_decks не покрывает НИ ОДНОГО года плана: годы планов %s, годы деков %s — "+
			"npvLOM пропустит все годы, и nav_by_asset получит нулевой NPV при непустых mine_plans; "+
			"проверьте сид price_decks (gold_prices) и его календарь",
			planYears, deckYears)
	}

	log.Warnf("price_decks не покрывает годы плана: %v (годы планов %s, годы деков %s) — "+
		"эти годы npvLOM пропустит (model.go:137-142), а nav_by_asset запишет строку с заниженным NPV; "+
		"заполните price_decks на них и повторите расчёт",
		uncovered, planYears, deckYears)

	return nil
}

// planYearSet, deckYearSet — множества годов планов и деков: пересечение и разность
// наборов решаются по ним, а не по длинам срезов, где годы активов дублируются.
func planYearSet(plans []MinePlanRecord) map[uint16]struct{} {
	out := make(map[uint16]struct{})
	for _, plan := range plans {
		for _, y := range plan.Years {
			out[y.Year] = struct{}{}
		}
	}

	return out
}

func deckYearSet(decks map[string][]DeckYear) map[uint16]struct{} {
	out := make(map[uint16]struct{})
	for _, years := range decks {
		for _, d := range years {
			out[d.Year] = struct{}{}
		}
	}

	return out
}

// sortedYearSet — отсортированные годы для читаемого и воспроизводимого текста
// сообщения: множество map'а само порядка не даёт.
func sortedYearSet(years map[uint16]struct{}) []uint16 {
	out := make([]uint16, 0, len(years))
	for y := range years {
		out = append(out, y)
	}

	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })

	return out
}

// formatYears — компактнее и читаемее, чем срез uint16 в %v: лог читает человек,
// а не парсер.
func formatYears(years []uint16) string {
	out := ""
	for i, y := range years {
		if i > 0 {
			out += ", "
		}

		out += strconv.Itoa(int(y))
	}

	return out
}
