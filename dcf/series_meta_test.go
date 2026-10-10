package dcf

import (
	"strings"
	"testing"
)

// TestMinePlansSeriesMetaCoversAllAssets — каждый актив сида описан в каталоге
// рядов (правило 11a AGENTS.md), у каждой записи заполнены все поля.
//
// Итерация идёт по polyusAssetPlans, а не по захардкоженному списку из восьми имён:
// так будущий актив, добавленный в сид (или потерянный при мерже срез), роняет
// тест, а не появляется в mine_plans без описания. Тот же класс дефекта, что
// TestMinePlansSeedListNotEmpty закрывает для самого сида: строка в таблице и
// запись в каталоге — один контракт для агента, который каталог читает ПЕРЕД
// запросом к ряду (v_series_catalog), и молча пропавшее описание агент не увидит
// ни в каком запросе.
func TestMinePlansSeriesMetaCoversAllAssets(t *testing.T) {
	got := map[string]bool{}
	for _, m := range minePlansSeriesMeta {
		if m.Source != "dcf_mine_plans" {
			t.Errorf("%s: Source = %q, want dcf_mine_plans", m.Series, m.Source)
		}
		for name, v := range map[string]string{
			"Title": m.Title, "Unit": m.Unit, "Frequency": m.Frequency,
			"Origin": m.Origin, "Description": m.Description,
		} {
			if v == "" {
				t.Errorf("%s: пустое поле %s", m.Series, name)
			}
		}
		got[m.Series] = true
	}
	for _, p := range polyusAssetPlans {
		if !got[p.Asset] {
			t.Errorf("актив %s не описан в minePlansSeriesMeta", p.Asset)
		}
	}
}

// TestMinePlansSeriesMetaNoUnknownAssets — обратное направление: в каталоге нет
// записей о активах, которых в сиде нет.
//
// Без него запись-сирота (переименованный актив, копия строки при мерже) жила бы
// в series_catalog вечно: она не ломает ни один запрос, но агент прочитал бы
// описание ряда, которого в mine_plans нет и не будет, — то же «обещание данных,
// которых нет», от которого защищает порядок upsert после успешного Send.
func TestMinePlansSeriesMetaNoUnknownAssets(t *testing.T) {
	seeded := make(map[string]bool, len(polyusAssetPlans))
	for _, p := range polyusAssetPlans {
		seeded[p.Asset] = true
	}
	for _, m := range minePlansSeriesMeta {
		if !seeded[m.Series] {
			t.Errorf("каталог описывает актив %s, которого нет в polyusAssetPlans", m.Series)
		}
	}
}

// TestMinePlansSeriesMetaDescribesAssumption — описание обязано помечать
// значения как ДОПУЩЕНИЕ, а не измеренные данные, и называть источник с годом.
//
// Это главное содержание записи для агента: населённость полей (тест выше) не
// говорит, ЧИТАЕТ ли он числа правильно. Если из описания исчезнет оговорка
// «допущение», агент отнесётся к tcc/aisc/capex как к наблюдённым значениям и
// построит на них суждение без поправки на derived-природу (aisc = tcc + клин) —
// ошибка, которую в каталоге видно, а в самой таблице mine_plans нет, потому что
// значения там лежат одинаковыми Float64 без признака происхождения.
//
// Проверяются ключевые содержательные требования брифа: происхождение
// (dcf_mine_plans, Годовой обзор 2025, URL), три названные единицы, годовая
// частота «A» (конвенция каталога — M/Q/D/W/A, не слово «annual»), и три
// содержательные оговорки: AISC derived как tcc + 698, 0 в closure_costs —
// «не задано», снимок пересматривается отдельным пунктом роадмапа.
func TestMinePlansSeriesMetaDescribesAssumption(t *testing.T) {
	for _, m := range minePlansSeriesMeta {
		if m.Frequency != "A" {
			t.Errorf("%s: Frequency = %q, want A (годовой ряд)", m.Series, m.Frequency)
		}

		// Сравнение по Unit+Description+Origin+Title: каталог — один контракт, и
		// требование брифа «назвать единицы» выполнено, если единица названа хотя
		// бы в Unit (в одном поле на все колонки строки — они разнородны).
		contract := m.Title + " " + m.Unit + " " + m.Origin + " " + m.Description

		for _, want := range []string{
			"dcf_mine_plans", // источник назван
			"mine_plans",     // таблица названа
			"2025",           // год отчётности назван
			"godovoy_obzor",  // URL Годового обзора 2025
			"koz",            // production_koz — тыс. унций
			"USD/oz",         // tcc/aisc — доллары за унцию
			"млн USD",        // capex и closure — млн USD
			"ДОПУЩЕНИЕ",      // значения — допущение, не наблюдение
			"tcc + 698",      // aisc derived от группового клина
			"не задано",      // closure_costs = 0 значит «не задано»
			"роадмапа",       // снимок пересматривается новым пунктом роадмапа
			"плато",          // годы за последним фактом — плато, не измерение
		} {
			if !strings.Contains(contract, want) {
				t.Errorf("%s: каталог не содержит %q", m.Series, want)
			}
		}

		// Заголовок обязан называть актив — иначе восемь записей каталога
		// читаются как восемь одинаковых рядов.
		if !strings.Contains(m.Title, m.Series) {
			t.Errorf("Title %q не называет актив %q", m.Title, m.Series)
		}
	}
}
