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
	covered := make(map[uint16]struct{})
	for _, years := range decks {
		for _, d := range years {
			covered[d.Year] = struct{}{}
		}
	}

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

	planYearsSet := make(map[uint16]struct{})
	for _, plan := range plans {
		for _, y := range plan.Years {
			planYearsSet[y.Year] = struct{}{}
		}
	}

	deckYearsSet := make(map[uint16]struct{})
	for _, years := range decks {
		for _, d := range years {
			deckYearsSet[d.Year] = struct{}{}
		}
	}

	// Непересекающиеся множества — единственный случай, когда расчёт заведомо пуст:
	// непокрытых пар ровно столько же, сколько календарных годов плана.
	if len(uncovered) == len(planYearsSet) {
		return fmt.Errorf("price_decks не покрывает НИ ОДНОГО года плана: годы планов %s, годы деков %s — "+
			"npvLOM пропустит все годы, и nav_by_asset получит нулевой NPV при непустых mine_plans; "+
			"проверьте сид price_decks (gold_prices) и его календарь",
			formatYears(sortedPlanYears(planYearsSet)), formatYears(sortedYearSet(deckYearsSet)))
	}

	log.Warnf("price_decks не покрывает годы плана: %v (годы планов %s, годы деков %s) — "+
		"эти годы npvLOM пропустит (model.go:137-142), а nav_by_asset запишет строку с заниженным NPV; "+
		"заполните price_decks на них и повторите расчёт",
		uncovered, formatYears(sortedPlanYears(planYearsSet)), formatYears(sortedYearSet(deckYearsSet)))

	return nil
}

// sortedPlanYears и sortedYearSet — отсортированные годы для читаемого и
// воспроизводимого текста сообщения: множество map'а само порядка не даёт.
func sortedPlanYears(years map[uint16]struct{}) []uint16 {
	return sortedYearSet(years)
}

func sortedYearSet(years map[uint16]struct{}) []uint16 {
	out := make([]uint16, 0, len(years))
	for y := range years {
		out = append(out, y)
	}

	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })

	return out
}

// formatYears печатает годы через запятую — компактнее и читаемее, чем срез
// uint16 в %v (лог читает человек, а не парсер).
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
