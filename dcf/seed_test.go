package dcf

import (
	"math"
	"strings"
	"testing"
)

// TestAssertSpotPriceRejectsNonPositive — Finding 1 ревью: сид spot_flat берёт
// цену через `argMax(usd, date) FROM gold_prices`, а argMax по ПУСТОМУ множеству
// возвращает нулевое значение типа (0 для Float64), НЕ NULL и без исключения
// (проверено на живом ClickHouse: `SELECT argMax(number, number+1) FROM numbers(0)`
// → 0). gold_prices наполняется отдельным шагом DAG, поэтому без этой проверки
// прогон сида до импорта золота молча записал бы spot_flat с ценой $0/oz — вход
// всего расчёта, занижённый в бесконечность раз, при успешном логе сида.
//
// Проверка вынесена в чистую assertSpotPrice, потому что воспроизвести «таблица
// есть, но пуста» на живой БД в юнит-тесте нельзя, а сама граница — здесь.
func TestAssertSpotPriceRejectsNonPositive(t *testing.T) {
	// Ноль — главный случай: именно его даёт argMax по пустой gold_prices.
	if err := assertSpotPrice(0); err == nil {
		t.Fatal("spot = 0 обязан быть ошибкой: argMax по пустому множеству даёт 0, " +
			"и spot_flat записался бы с ценой $0/oz")
	}
	// Отрицательная цена — тот же класс мусора, что и ноль.
	if err := assertSpotPrice(-1); err == nil {
		t.Fatal("отрицательный spot обязан быть ошибкой")
	}

	// Нечисловой мусор: NaN не проходит сравнение `> 0` — тоже ошибка.
	if err := assertSpotPrice(math.NaN()); err == nil {
		t.Fatal("NaN обязан быть ошибкой: !(NaN > 0) истинно")
	}

	// Сообщение обязано называть вход (gold_prices / moex_fix_usd), иначе дежурный
	// по DAG не поймёт, что наполнять, и будет искать причину в сиде.
	err := assertSpotPrice(0)
	if err == nil {
		t.Fatal("ожидалась ошибка на нулевом spot")
	}
	for _, want := range []string{"gold_prices", "moex_fix_usd", "spot_flat"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("сообщение об ошибке не упоминает %q: %q", want, err.Error())
		}
	}

	// Реальная цена — не ошибка.
	for _, ok := range []float64{0.01, 4250.10, 1e6} {
		if err := assertSpotPrice(ok); err != nil {
			t.Errorf("положительный spot %v не должен быть ошибкой: %v", ok, err)
		}
	}
}
