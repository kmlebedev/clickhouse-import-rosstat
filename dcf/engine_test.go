package dcf

import (
	"testing"

	"github.com/google/uuid"
)

// TestResolveRunID — Review Focus 2 и 3: run_id приходит только из DCF_RUN_ID,
// и мусор в него попасть не должен.
//
// Пустая строка — ошибка, а не повод сгенерировать UUID: сгенерированный run_id
// не совпал бы с клиентским run_id в POST /v1/model_run, и строки nav_by_asset
// повисли бы без своей строки model_runs — то есть прогон стал бы неотличим от
// чужого. Не-UUID отсекается здесь, до расчёта: ClickHouse отвергнет такую
// строку сам, но уже в середине батча (спека §4).
func TestResolveRunID(t *testing.T) {
	want, _ := uuid.Parse("8f14e45f-ceea-467a-9e1c-2c3b1b1c2b1c")
	if got, err := resolveRunID("8f14e45f-ceea-467a-9e1c-2c3b1b1c2b1c"); err != nil || got != want {
		t.Fatalf("валидный UUID: got %v, err %v", got, err)
	}
	if _, err := resolveRunID(""); err == nil {
		t.Fatal("пустой DCF_RUN_ID обязан давать ошибку: молчаливая генерация run_id " +
			"отвязала бы nav_by_asset от строки model_runs")
	}
	if _, err := resolveRunID("not-a-uuid"); err == nil {
		t.Fatal("не-UUID обязан отсекаться до расчёта")
	}
}

// TestResolveRunIDFromEnv — DCF_RUN_ID читается только через os.Getenv, и пустая
// строка (переменная не задана) обязана быть ошибкой: t.Setenv подменяет окружение
// теста, поэтому проверка идёт по тому же пути, что и в Task 5.
func TestResolveRunIDFromEnv(t *testing.T) {
	t.Setenv(runIDEnv, "")

	if _, err := resolveRunIDString(); err == nil {
		t.Fatal("незаданный DCF_RUN_ID обязан давать ошибку: молчаливая генерация run_id " +
			"отвязала бы nav_by_asset от строки model_runs")
	}

	const raw = "8f14e45f-ceea-467a-9e1c-2c3b1b1c2b1c"
	t.Setenv(runIDEnv, raw)

	want, _ := uuid.Parse(raw)
	got, err := resolveRunIDString()
	if err != nil || got != want {
		t.Fatalf("DCF_RUN_ID %q: got %v, err %v", raw, got, err)
	}
}

// TestPlanEmptyDoesNotFail — Review Focus 1: пустой mine_plans не ошибка, иначе
// `make import STAT=dcf_engine` и DAG краснеют на dev-БД, где сида ещё нет.
//
// Второй кейс (непустой план, пустые деки) проверяет границу проверки: она решает
// только «план пуст → не ошибка» и не имеет права падать на отсутствии деков —
// деки сеются внутри Import, и падать здесь значило бы падать на порядке шагов.
func TestPlanEmptyDoesNotFail(t *testing.T) {
	if err := checkInputs(nil, nil); err != nil {
		t.Fatalf("пустой mine_plans — не ошибка: %v", err)
	}
	if err := checkInputs([]MinePlanRecord{{Company: "PLZL", Asset: "Olimpiada"}}, nil); err != nil {
		t.Fatalf("непустой план с пустыми deck'ами не должен падать на этой проверке: %v", err)
	}
}
