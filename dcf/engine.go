package dcf

import (
	"context"
	"fmt"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	log "github.com/sirupsen/logrus"
)

// dcfEngine — импортёр расчётного ядра DCF (Name() — значение CLICKHOUSE_IMPORT_STAT
// и имя шага в dagu). Владеет таблицами mine_plans, price_decks, nav_by_asset;
// model_runs не пишет — это делает ingest-endpoint.
type dcfEngine struct{}

func (s *dcfEngine) Name() string { return "dcf_engine" }

// MinePlanRecord — LOM-план одного актива: строки mine_plans, сгруппированные по активу.
// Группировка нужна потому, что NPV считается на актив, а в таблице строка — год актива.
type MinePlanRecord struct {
	Company string
	Asset   string
	Years   []MinePlanYear
}

// runIDEnv — переменная окружения с клиентским run_id прогона. Читается только
// через os.Getenv (правило 1 AGENTS.md) и не вычисляется на этапе init.
//
// Пока Import до этой ветки не доходит: расчёт и запись nav_by_asset — Task 5.
// Но разбор (resolveRunID) и обе ветки «DCF_RUN_ID пуст / не-UUID» живут здесь
// уже сейчас: они обязаны быть покрыты тестами до того, как появится вставка
// (Review Focus 2 и 3), иначе ошибка всплывёт только на живой БД в середине батча.
const runIDEnv = "DCF_RUN_ID"

// resolveRunIDString проверяет DCF_RUN_ID по значению, а не по наличию: пустая
// строка обязана быть ошибкой ещё до parse. Читает переменную Import (Task 5);
// здесь проверка нужна, чтобы правило «пустой id — ошибка» было одним местом, а
// не условием, размазанным по вызывающему коду.
func resolveRunIDString() (uuid.UUID, error) {
	return resolveRunID(os.Getenv(runIDEnv))
}

// minePlansSelect и priceDecksSelect — запросы входов. Вынесены в константы рядом
// с DDL, потому что схемы и списки колонок — один контракт: колонки перечислены
// явно (никакого SELECT *), и порядок совпадает с порядком rows.Scan в
// readMinePlans/readDecks. grade_gpt в SELECT отсутствует намеренно: в формулу NPV
// он не входит, а Nullable-значение, которое никто не читает, только тянуло бы
// nil-проверки в сканирование (брифинг Task 4, шаг 5).
//
// ORDER BY (company, asset, year) — часть контракта readMinePlans: он даёт
// группировку одним проходом и хронологический порядок лет внутри плана.
const (
	minePlansSelect = `SELECT company, asset, year, production_koz, tcc, aisc,
		capex_sustaining, capex_project, closure_costs
	FROM mine_plans FINAL ORDER BY company, asset, year`

	priceDecksSelect = `SELECT deck, year, gold_usd FROM price_decks FINAL`
)

// resolveRunID разбирает DCF_RUN_ID в UUID.
//
// Пустая строка — ошибка, а не повод сгенерировать id. run_id клиентский: ту же
// строку пишет ingest-endpoint в model_runs, и только совпадение id связывает
// nav_by_asset с журналом прогонов. Сгенерированный здесь UUID не совпал бы ни с
// одной строкой model_runs, и прогон стал бы неотличим от чужого (Review Focus 2).
// Не-UUID отсекается до расчёта: ClickHouse отвергнет такую строку сам, но уже в
// середине батча (Review Focus 3, спека §4).
func resolveRunID(raw string) (uuid.UUID, error) {
	if raw == "" {
		return uuid.Nil, fmt.Errorf("DCF_RUN_ID пуст: nav_by_asset ссылается на run_id из model_runs, " +
			"поэтому id обязан прийти снаружи, а не быть сгенерированным здесь")
	}

	runID, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, fmt.Errorf("DCF_RUN_ID %q не UUID: %w", raw, err)
	}

	return runID, nil
}

// checkInputs — чистая проверка входов перед расчётом (Review Focus 1).
//
// Пустой mine_plans — НЕ ошибка: пока сид LOM-планов не заведён (отдельный пункт
// роадмапа), движок обязан прогоняться на dev-БД, печатать предупреждение и 0 строк,
// а не краснить `make import STAT=dcf_engine` и DAG. Пустые деки при непустом плане —
// тоже не ошибка этой проверки: деки сеются внутри Import (Task 5), и падать на их
// отсутствии значило бы падать на порядке шагов, а не на данных.
//
// Оба входа принимаются, хотя сейчас не проверяются: сигнатура — контракт Task 5,
// которая вызывает эту функцию перед расчётом, и доменные проверки (например,
// отрицательная добыча) добавляются сюда, а не в Import.
func checkInputs(plans []MinePlanRecord, decks map[string][]DeckYear) error {
	_ = plans
	_ = decks

	return nil
}

// readMinePlans читает LOM-планы и группирует их по (company, asset): NPV считается
// на актив, а строка таблицы — год актива.
//
// ORDER BY company, asset, year в запросе (см. minePlansSelect) — часть контракта, а
// не украшение: он даёт два свойства, на которые опирается группировка. Первое — все
// годы одного актива идут подряд, поэтому план собирается одним проходом без map'а и
// без второй сортировки. Второе — годы внутри группы упорядочены, а npvLOM нумерует
// годы плана по порядку и берёт по номеру ИПЦ и дисконт: перепутанный порядок лет дал
// бы верный набор слагаемых и неверный NPV.
//
// Компания входит в ключ группировки намеренно: активы разных компаний в общей
// таблице могут называться одинаково, и группировка только по asset молча слила бы
// их планы в один (тихая подмена данных — тот же класс, что расхождение ключей §8.1).
func readMinePlans(ctx context.Context, conn driver.Conn) ([]MinePlanRecord, error) {
	rows, err := conn.Query(ctx, minePlansSelect)
	if err != nil {
		return nil, fmt.Errorf("прочитать mine_plans (LOM-планы ещё не засеяны?): %w", err)
	}
	defer func() { _ = rows.Close() }()

	var plans []MinePlanRecord

	for rows.Next() {
		var (
			company, asset     string
			year               uint16
			productionKoz, tcc float64
			aisc               float64
			capexSustaining    float64
			capexProject       float64
			closureCosts       float64
		)
		if err = rows.Scan(
			&company, &asset, &year, &productionKoz, &tcc, &aisc,
			&capexSustaining, &capexProject, &closureCosts,
		); err != nil {
			return nil, fmt.Errorf("прочитать строку mine_plans: %w", err)
		}

		last := len(plans) - 1
		if last < 0 || plans[last].Company != company || plans[last].Asset != asset {
			plans = append(plans, MinePlanRecord{Company: company, Asset: asset})
			last = len(plans) - 1
		}

		plans[last].Years = append(plans[last].Years, MinePlanYear{
			Year:            year,
			ProductionKoz:   productionKoz,
			TCC:             tcc,
			AISC:            aisc,
			CapexSustaining: capexSustaining,
			CapexProject:    capexProject,
			ClosureCosts:    closureCosts,
		})
	}

	// rows.Err() обязателен: Next() возвращает false и при ошибке чтения, поэтому без
	// этой проверки неполный план выглядел бы как корректный вход — а неполный LOM
	// даёт тихо заниженный NPV, то есть именно ту ошибку, которой не видно в логе.
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("прочитать mine_plans: %w", err)
	}

	return plans, nil
}

// readDecks читает ценовые деки в карту «имя дека → годы».
//
// FINAL в запросе обязателен: price_decks — ReplacingMergeTree с ключом
// (deck, year, published), то есть у одного года одного дека бывает несколько
// версий. Без FINAL один год пришёл бы несколько раз, и какая версия победит,
// зависело бы от порядка чтения частей — цена золота в nav_by_asset перестала бы
// быть воспроизводимой.
func readDecks(ctx context.Context, conn driver.Conn) (map[string][]DeckYear, error) {
	rows, err := conn.Query(ctx, priceDecksSelect)
	if err != nil {
		return nil, fmt.Errorf("прочитать price_decks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	decks := make(map[string][]DeckYear)

	for rows.Next() {
		var (
			deck    string
			year    uint16
			goldUSD float64
		)
		if err = rows.Scan(&deck, &year, &goldUSD); err != nil {
			return nil, fmt.Errorf("прочитать строку price_decks: %w", err)
		}

		decks[deck] = append(decks[deck], DeckYear{Year: year, GoldUSD: goldUSD})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("прочитать price_decks: %w", err)
	}

	return decks, nil
}

// Import — обвязка расчётного ядра. На этом шаге (Task 4) он проверяет входы и
// останавливается: расчёт и запись nav_by_asset делает Task 5. Порядок шагов
// важен — сначала mine_plans, и только потом price_decks: пустой план означает
// «считать нечего» и завершается предупреждением с нулём строк, а не ошибкой, и
// чтение деков в этом случае было бы лишним запросом в пустую таблицу.
func (s *dcfEngine) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, ddl := range []string{minePlansCreateTable, priceDecksCreateTable, navByAssetCreateTable} {
		if err = conn.Exec(ctx, ddl); err != nil {
			return 0, err
		}
	}

	plans, err := readMinePlans(ctx, conn)
	if err != nil {
		return 0, err
	}

	if len(plans) == 0 {
		log.Warn("mine_plans пуст: сид LOM-планов — отдельный пункт роадмапа " +
			"(docs/ROADMAP_DCF_POLYUS.md, фаза 3); расчёт пропущен, записано 0 строк")

		return 0, nil
	}

	decks, err := readDecks(ctx, conn)
	if err != nil {
		return 0, err
	}

	if err = checkInputs(plans, decks); err != nil {
		return 0, err
	}

	// Расчёт и запись nav_by_asset — Task 5: до появления navRows писать нечего, и
	// пустой батч в nav_by_asset создал бы видимость прогона, которого не было.
	log.Infof("dcf inputs read: %d активов, %d деков; запись nav_by_asset — следующий шаг", len(plans), len(decks))

	return 0, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &dcfEngine{})
}
