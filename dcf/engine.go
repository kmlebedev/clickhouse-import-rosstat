package dcf

import (
	"context"
	"fmt"
	"os"
	"sort"

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
// Import читает её РОВНО ОДИН РАЗ в локальную rawRunID и разбирает это же значение
// (resolveRunID), а не перечитывает окружение: два чтения одной переменной могли бы
// вернуть разные значения, и записанный в nav_by_asset id не совпал бы с проверенным
// (или проверка прошла бы на пустой строке при непустой записи). Одно чтение — одна
// проверка — одна запись (Finding 2 финального ревью).
const runIDEnv = "DCF_RUN_ID"

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

// checkInputs — проверка полноты входов перед расчётом (Review Focus 1, Finding 3
// финального ревью). Возвращает error как контракт шага (на будущие доменные
// проверки — отрицательная добыча, нулевой год и т.п.), но ни одно из текущих
// нарушений полноты ошибкой НЕ является: см. ниже, почему выбран предупреждающий
// режим, а не падение.
//
// Пустой mine_plans — не ошибка этой функции: Import уже отсекает пустой план
// отдельной веткой (печатает предупреждение и 0 строк) ДО вызова checkInputs.
//
// Три КАНОНИЧЕСКИХ деки обязаны быть в расчёте (§6.4: «assets × 3 decks ×
// 2 contours»). Сид пишет их только когда price_decks ПУСТА, поэтому частично
// заполненная таблица (один-два дека, опечатка в имени) молча превращает
// гарантию «× 3 деки» в «× 2 деки», и пропавший дек в nav_by_asset неотличим от
// «не считали». Проверка делает пропажу ВИДИМОЙ.
//
// ВЫБОР: warning, а не error. Частичный набор деков всё ещё даёт корректные
// строки для тех деков, что есть, и падение краснило бы `make import
// STAT=dcf_engine` и DAG на recoverable-состоянии БД — это противоречит философии
// среза (пустой mine_plans — предупреждение, а не ошибка) и правилу «warn-and-continue
// для неполных входов». Поэтому функция называет пропавшие деки в логе и
// возвращает nil; сигнатура сохраняет error, чтобы будущие доменные проверки
// могли вернуть настоящую ошибку без смены контракта.
//
// Пустая карта деков при непустом плане — отдельное, более сильное предупреждение:
// Import сеет деки ДО этого вызова, поэтому пустая карта здесь означает, что и сид
// ничего не записал (например, пустая gold_prices отвергнута assertSpotPrice) —
// это стоит увидеть громко, хотя по-прежнему не ошибка.
func checkInputs(plans []MinePlanRecord, decks map[string][]DeckYear) error {
	if len(plans) == 0 {
		// Import не доходит сюда с пустым планом, но функция — самостоятельный
		// контракт: пустой план не повод ругаться ни здесь, ни в вызывающем.
		return nil
	}

	canonical := []string{deckSpotFlat, deckConsensusLT, deckOwnScenario}

	if len(decks) == 0 {
		log.Warn("price_decks пуст после сида: расчёт даст 0 строк nav_by_asset " +
			"(ни один из трёх канонических деков не записан; проверьте gold_prices и сид)")

		return nil
	}

	missing := make([]string, 0, len(canonical))
	for _, name := range canonical {
		if _, ok := decks[name]; !ok {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 {
		log.Warnf("price_decks: нет канонических деков %v — гарантия «активы × 3 деки × 2 контура» "+
			"не выполняется для них; строки по остальным декам будут записаны (Finding 3 финального ревью)",
			missing)
	}

	// Деки вне канонического списка readDecks всё равно проносит в nav_by_asset:
	// это чаще всего опечатка в имени дека, и без строки в логе она выглядела бы
	// как «четвёртый сценарий», а не как опечатка. Только info и только имена —
	// аналитик может осознанно добавить свой дек, запрещать его мы не вправе.
	extra := make([]string, 0, len(decks))
	for name := range decks {
		switch name {
		case deckSpotFlat, deckConsensusLT, deckOwnScenario:
		default:
			extra = append(extra, name)
		}
	}

	if len(extra) > 0 {
		sort.Strings(extra) // порядок в логе воспроизводим — как и порядок строк в nav_by_asset
		log.Infof("price_decks: деки вне канонических трёх: %v (сценарий аналитика или опечатка)", extra)
	}

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

// Контуры ставки: два результата на каждый ценовой дек (§6.4, §13.1). Строковые
// константы, а не захардкоженные литералы в navRows: те же строки стоят в DDL
// nav_by_asset (schema.go) и в ARCHITECTURE §6.2, и разойтись они не должны.
const (
	ContourIndustrial = "industrial"
	ContourLocal      = "local"
)

// navRows — чистое ядро записи: входы (планы, деки, ставки, допущения) → строки
// nav_by_asset. Без БД, без часов, без чтения окружения: всё это в Import, чтобы
// комбинаторику «три дека × два контура» можно было проверить юнит-тестом, а не
// живым прогоном. Ошибки у функции нет по замыслу — расчёт не может не сойтись
// (проверки входов — в checkInputs), но сигнатура оставлена с error: это контракт
// шага, и добавлять сюда доменные проверки (например, отрицательную добычу) будут
// без смены сигнатуры.
//
// Итерация по декам детерминирована: имена сортируются, потому что порядок map в
// Go случаен, а порядок вставки в nav_by_asset обязан быть воспроизводимым между
// прогонами. Активы идут в порядке планов (readMinePlans уже отсортировал их по
// company, asset), контуры — промышленный, затем локальный: фиксированный
// порядок делает diff двух прогонов осмысленным.
//
// Пустой список годов дека — не паника и не ошибка: npvLOM пропустит годы плана
// без цены, и NPV выйдет нулевым. Строку при этом пишем: «актив посчитан, но
// дековской цены на его годы нет» — это данные, которые нужно увидеть в
// nav_by_asset, а не тихо отсутствующая комбинация, неотличимая от «не считали».
func navRows(runID uuid.UUID, plans []MinePlanRecord, decks map[string][]DeckYear, rates DiscountRates, p Params) ([]NavRow, error) {
	deckNames := make([]string, 0, len(decks))
	for name := range decks {
		deckNames = append(deckNames, name)
	}
	sort.Strings(deckNames)

	contours := []struct {
		name string
		rate float64
	}{
		{ContourIndustrial, rates.Industrial},
		{ContourLocal, rates.Local},
	}

	rows := make([]NavRow, 0, len(plans)*len(deckNames)*len(contours))
	for _, plan := range plans {
		for _, deck := range deckNames {
			for _, contour := range contours {
				rows = append(rows, NavRow{
					RunID:        runID,
					Deck:         deck,
					Asset:        plan.Asset,
					Contour:      contour.name,
					DiscountRate: contour.rate,
					// npvLOM возвращает млн USD — та же единица, что npv_usd_mln,
					// поэтому результат идёт как есть: пересчёт здесь был бы
					// дефектом в 1000 раз (model.go, npvLOM).
					NPVUSDmln: npvLOM(plan.Years, decks[deck], contour.rate, p.Ipc, p),
					// Стадийные haircut'ы — вне среза: строка рудника считается
					// полным NPV, а дисконт за стадию (construction/DFS/PEA)
					// придёт отдельным шагом (ARCHITECTURE §6.4, ресурсы вне LOM).
					StageHaircut: nil,
				})
			}
		}
	}

	return rows, nil
}

// Import — обвязка расчётного ядра: проверить входы, посчитать navRows и записать
// их в nav_by_asset ОДНИМ батчем. Порядок шагов важен — сначала mine_plans, и
// только потом price_decks: пустой план означает «считать нечего» и завершается
// предупреждением с нулём строк, а не ошибкой, и чтение деков в этом случае было
// бы лишним запросом в пустую таблицу.
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

	// Сид деков только на пустой таблице: непустую не трогаем, потому что там
	// лежит ВЫБРАННЫЙ аналитиком сценарий (и его published — метка версии).
	// Перезаписать его сидом значило бы молча подменить вход расчёта.
	if len(decks) == 0 {
		if err = seedPriceDecks(ctx, conn, planYears(plans)); err != nil {
			return 0, err
		}

		if decks, err = readDecks(ctx, conn); err != nil {
			return 0, err
		}
	}

	// Гард пересечения годов — ДО checkInputs и после сида: сид мог ничего не
	// записать (пустая gold_prices), а в БД с частично заполненной price_decks он
	// вообще не вызывается, поэтому полнота деков ничего не говорит о том, что цены
	// есть на ГОДЫ ПЛАНА. Без этой проверки такой вход дал бы нулевой NPV с
	// успешным логом.
	if err = checkYearCoverage(plans, decks); err != nil {
		return 0, err
	}

	if err = checkInputs(plans, decks); err != nil {
		return 0, err
	}

	// Без DCF_RUN_ID расчёт не пишется: run_id клиентский, и связка с model_runs
	// держится только на нём (resolveRunID, Review Focus 2). Вместо молчаливого
	// нуля печатаем тело POST /v1/model_run — человек дописывает результат и
	// отправляет его в ingest, получая версию модели в журнале.
	rawRunID := os.Getenv(runIDEnv)
	if rawRunID == "" {
		log.Info(modelRunJSON(plans, decks, rates, defaultParams))
		log.Warn("DCF_RUN_ID не задан: nav_by_asset не записан; " +
			"запись model_runs — только через ingest-endpoint (POST /v1/model_run)")

		return 0, nil
	}

	// Разбираем УЖЕ прочитанное значение, а не перечитываем DCF_RUN_ID: второе
	// чтение окружения могло вернуть другую строку, и записанный id разошёлся бы с
	// проверенным (Finding 2 финального ревью — двойное чтение env).
	runID, err := resolveRunID(rawRunID)
	if err != nil {
		return 0, err
	}

	rows, err := navRows(runID, plans, decks, rates, defaultParams)
	if err != nil {
		return 0, err
	}

	// Один батч на все строки: построчный Exec в цикле запрещён (правило 2
	// AGENTS.md), а частично записанный батч оставил бы в nav_by_asset половину
	// комбинаций «дека × контур», неотличимую от полного прогона.
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO nav_by_asset")
	if err != nil {
		return 0, err
	}

	for _, r := range rows {
		if err = batch.Append(r.RunID, r.Deck, r.Asset, r.Contour, r.NPVUSDmln, r.DiscountRate, r.StageHaircut); err != nil {
			// Abort — не украшение: без него соединение остаётся с открытым
			// батчем, и следующий шаг DAG-а получит «уже есть незакрытый батч».
			_ = batch.Abort()

			return 0, err
		}
	}

	if err = batch.Send(); err != nil {
		return 0, err
	}

	return int64(len(rows)), nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &dcfEngine{})
}
