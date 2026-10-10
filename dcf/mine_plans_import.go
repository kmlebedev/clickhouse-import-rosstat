package dcf

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	log "github.com/sirupsen/logrus"
)

// minePlansSeeder — импортёр таблицы mine_plans (Name() — значение
// CLICKHOUSE_IMPORT_STAT и имя шага DAG).
//
// Порядок в DAG: dcf_mine_plans идёт ДО dcf_engine. Расчётное ядро читает
// mine_plans как вход (readMinePlans, engine.go), и прогон ядра раньше сида
// означал бы расчёт по пустому (или прошлогоднему) плану с успешным логом —
// именно поэтому шаги разведены, а не слиты в один импортёр.
type minePlansSeeder struct{}

func (s *minePlansSeeder) Name() string { return "dcf_mine_plans" }

// actualProductionMetric — имя метрики добычи в датапаке Polyus. ИМЕННО это
// значение колонки name, а не релизная метрика gold_output из company_financials:
// значения databook_polyus отнесены к АКТИВУ (OLIMPIADA, NATALKA, ...), а
// company_financials.gold_output — сводка по Группе. Прочитать одно вместо
// другого дало бы ненулевую «добычу актива», равную добыче всей Группы, и не
// бросило бы ни ошибки, ни предупреждения.
const actualProductionMetric = `Total Dore gold output`

// actualProductionSelect — факт годовой добычи одного актива, тыс. унц (koz).
//
// Год берётся из КОЛОНКИ date через toYear(date), а не из подписи периода в имени
// метрики: у ANNUAL date — это 1 декабря отчётного года (polyus/datapack.go), и
// извлечение года из строки-подписи — ровно дефект §8.1 роадмапа (период взят из
// label вместо datum). Метка периода здесь вообще не участвует в вычислении года,
// а служит только фильтром data='ANNUAL'.
//
// FINAL обязателен: databook_polyus — ReplacingMergeTree по (table, name, data,
// date), и без FINAL год мог бы прийти несколько раз с разными версиями значения.
// Ограничение table сужает чтение всем партициям (в ключе сортировки table идёт
// первым) — предикат по toYear(date) индексом не покрыт.
//
// Параметры — в форме ClickHouse-параметров {name:Type} (не позиционные ?):
// объявленный тип параметра заставляет сервер сам разобрать строку из
// clickhouse.Named по типу (UInt16 для года), и значение уходит в запрос
// типизированным. Оба параметра стоят на верхнем уровне конъюнкции, каждый при
// своём столбце, поэтому формулировка читается однозначно: «этот актив, этот год».
const actualProductionSelect = `SELECT argMax(value, date)
FROM databook_polyus FINAL
WHERE table = {asset:String}
  AND name = '` + actualProductionMetric + `'
  AND data = 'ANNUAL'
  AND toYear(date) = {year:UInt16}`

// readActualProduction читает факт добычи актива за год, тыс. унц (koz), из
// databook_polyus.
//
// ОТСУТСТВИЕ СТРОКИ — НЕ ОШИБКА, а ноль: так же ведёт себя argMax по пустому
// множеству (0.0, проверено на живом ClickHouse). Условие «есть ли факт»
// применяет вызывающий (Import): именно там ноль превращается в warn и пропуск
// актива, тогда как здесь возвращать ошибку значило бы краснить импорт на
// отсутствии строки у Сухого Лога — ожидаемом состоянии, а не сбое.
//
// Ошибка возвращается поэтому ТОЛЬКО на настоящем сбое чтения (нет таблицы,
// недоступна БД). sql.ErrNoRows — отдельная ветка на случай, когда argMax всё же
// вернул пустое множество без агрегата (ошибка в WHERE): это тот же класс «строк
// нет», что и пустой argMax, а не сбой.
func readActualProduction(ctx context.Context, conn driver.Conn, asset string, year uint16) (float64, error) {
	// Значение читается в float32, а не в float64: колонка value в databook_polyus
	// объявлена Float32 (polyus/datapack.go), а драйвер СТРОГО типизирован — Scan
	// во float64 падает с «converting Float32 to *float64 is unsupported»
	// (проверено на живом сервере), то есть чтение не состоялось бы вовсе. Расширение
	// до float64 — по конвенции репозитория (gold/gold.go: пользовательские ряды
	// Float32 поднимаются до float64 в момент чтения, чтобы арифметика модели шла в
	// одном типе).
	var value float32

	// clickhouse.Named, а не позиционные ?: в тексте запроса {name:Type} — это
	// ClickHouse-параметры, и клиент требует именно NamedValue со строкой; «сырое»
	// число в этой форме отвергается. Год уходит строкой и разбирается сервером по
	// объявленному типу UInt16.
	err := conn.QueryRow(ctx, actualProductionSelect,
		clickhouse.Named("asset", asset),
		clickhouse.Named("year", fmt.Sprintf("%d", year)),
	).Scan(&value)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}

		return 0, fmt.Errorf("прочитать факт добычи %s за %d из databook_polyus: %w", asset, year, err)
	}

	return float64(value), nil
}

// Import пишет LOM-планы Полюса в mine_plans: факт последнего отчётного года из
// databook_polyus плюс константы сида (polyusAssetPlans).
func (s *minePlansSeeder) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	// Пустой список активов — ошибка, а не «успех с нулём строк»: polyusAssetPlans —
	// package-level var с восемью записями, и пустым он становится только от
	// дефекта сборки/правки (потерянная инициализация, обрезанный при мерже срез),
	// а не от состояния данных. Молчаливый Send пустого батча записал бы ноль строк
	// с успешным логом, и следующий шаг DAG-а (dcf_engine) посчитал бы по
	// несуществующему сиду. Пустой факт у ОТДЕЛЬНОГО актива — другое дело: это
	// состояние источника, и он остаётся warn+пропуском ниже.
	if len(polyusAssetPlans) == 0 {
		return 0, errors.New("сид mine_plans: список активов polyusAssetPlans пуст — " +
			"нечего импортировать (дефект сборки, а не состояние данных)")
	}

	if err = conn.Exec(ctx, minePlansCreateTable); err != nil {
		return 0, fmt.Errorf("создать mine_plans: %w", err)
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO mine_plans")
	if err != nil {
		return 0, fmt.Errorf("сид mine_plans: %w", err)
	}

	for _, seed := range polyusAssetPlans {
		// Факт читается только у активов, которых нет в профиле производства.
		//
		// Сухой Лог попадает сюда из-за ОТСУТСТВИЯ строки в датапаке: по нему
		// публикуются только горные метрики (rock moved, stripping, ore mined,
		// grade), а output нет — проверено на живом датапаке (0 строк с метрикой
		// Total Dore gold output). Поэтому фактическую добычу для него не читаем,
		// а план строит ProductionProfile. Это осознанный пропуск чтения, а не
		// «вернулся ноль»: пустой факт у Сухого Лога — характеристика источника,
		// которую нельзя путать с отсутствием данных у действующего рудника.
		var baseProductionKoz float64

		if len(seed.ProductionProfile) == 0 {
			if baseProductionKoz, err = readActualProduction(ctx, conn, seed.Asset, seed.BaseYear); err != nil {
				abortBatch(batch, "сид mine_plans")

				return 0, err
			}
		}

		years := buildPlanYears(seed, baseProductionKoz)
		// len(years) == 0, а НЕ years != nil: buildPlanYears может вернуть пустой
		// НЕнулевой срез (make(..., 0, capacity) при MineLifeYears == 0), и
		// проверка на nil пропустила бы такой актив дальше — в батч без строк, но
		// с успешным «импортировали».
		if len(years) == 0 {
			// Почему ноль на месте факта — это «факта нет», а не «год с нулевой
			// добычей»: два состояния неразличимы на входе (readActualProduction
			// возвращает 0.0 и когда строки нет вовсе, и когда строка есть со
			// значением 0.0 — argMax одинаков), но последствия разные. Строка с
			// нулевой добычей в базовом году молча занизила бы NPV всего актива;
			// пропуск же оставляет актив вне mine_plans и пишет этот warn, то есть
			// пробел виден и в логе, и в счётчике записанных строк.
			//
			// Оба случая названы прямо: строки в датапаке может не быть (Сухой Лог)
			// или она может быть нулевой. Ноль у действующего актива — это не
			// измеренный «нулевой год», а непригодный факт: 2025-й неполный, а у
			// Титимухты и Западного россыпная добыча учтена в датапаке ОТДЕЛЬНОЙ
			// таблицей ALLUVIALS, так что их Total Dore gold output и равен нулю.
			// Сегодня этот warn штатно горит именно на TITIMUKHTA и ZAPADNOYE —
			// оператору это ожидаемое предупреждение, а не сбой импорта.
			log.Warn(minePlansSkipWarn(seed.Asset, seed.BaseYear))

			continue
		}

		// База печатается только для активов, которым факт читали: у Сухого Лога
		// она нулевая структурно, и «база 0.0 koz» в логе читалась бы как
		// измеренный ноль добычи проекта, а не как «факт не читали».
		if len(seed.ProductionProfile) > 0 {
			log.Infof("mine_plans: %s — план по ProductionProfile, строк %d", seed.Asset, len(years))
		} else {
			log.Infof("mine_plans: %s — факт базы %.1f koz (%d), строк плана %d",
				seed.Asset, baseProductionKoz, seed.BaseYear, len(years))
		}

		for _, year := range years {
			// Порядок позиций — порядок колонок minePlansCreateTable (schema.go):
			// company, asset, year, production_koz, grade_gpt, tcc, aisc,
			// capex_sustaining, capex_project, closure_costs. Сверять глазами
			// обязательно: ClickHouse примет перестановку однотипных Float64 и
			// молча запишет tcc в aisc.
			//
			// grade_gpt — nil: содержание в MinePlanYear ОТСУТСТВУЕТ (dcf/model.go),
			// в формулу NPV не входит, и ноль здесь означал бы «0 г/т» как
			// измеренное значение. NULL же отделяет «не раскрыто» от нуля.
			if err = batch.Append(
				seed.Company, seed.Asset, year.Year, year.ProductionKoz, nil,
				year.TCC, year.AISC, year.CapexSustaining, year.CapexProject, year.ClosureCosts,
			); err != nil {
				abortBatch(batch, "сид mine_plans")

				return 0, fmt.Errorf("сид mine_plans %s/%d: %w", seed.Asset, year.Year, err)
			}

			// count — только ФАКТИЧЕСКИ отправленные строки: log/возврат должны
			// отражать то, что записано, а не число перебранных годов, иначе
			// счётчик расходился бы с телом батча.
			count++
		}
	}

	// Abort на ошибке Send НЕ вызываем — так же, как остальные батч-писатели
	// репозитория (polyus/import.go, ingest/clickhouse.go, util/series_catalog.go):
	// Send() завершает и закрывает батч даже при ошибке сервера, и повторный Abort
	// смысла не имеет. Асимметрия с веткой Append намеренная: там батч ещё открыт.
	if err = batch.Send(); err != nil {
		return 0, fmt.Errorf("сид mine_plans: %w", err)
	}

	// ЗДЕСЬ Task 6 подключает upsert series_catalog для рядов mine_plans: только
	// ПОСЛЕ успешного Send, потому что каталог описывает то, что реально доступно
	// MCP-агенту, и запись описаний при провалившейся вставке обещала бы ряды,
	// которых в таблице нет.

	log.Infof("Imported %d rows of dcf_mine_plans", count)

	return count, nil
}

// minePlansSkipWarn — текст warn о пропуске актива (без записи строк).
//
// Вынесен в отдельную функцию, чтобы формат сообщения проверялся без живого
// ClickHouse (см. TestMinePlansSkipWarnText): сам по себе warn — единственный
// след пропуска, и его текст обязан называть оба случая (строки нет / строка
// равна нулю), иначе оператор читает «нет факта» там, где факт есть и равен нулю
// (TITIMUKHTA, ZAPADNOYE — их россыпная добыча лежит в таблице ALLUVIALS).
func minePlansSkipWarn(asset string, year uint16) string {
	return fmt.Sprintf("mine_plans: %s — добычи за %d нет (строка в датапаке отсутствует или равна нулю), "+
		"план не построен, строк не записано; у TITIMUKHTA и ZAPADNOYE россыпная добыча учтена "+
		"в датапаке отдельной таблицей ALLUVIALS, и их ноль здесь ожидаем", asset, year)
}

// abortBatch закрывает батч на ошибке и не даёт потерять сбой закрытия.
//
// Abort — не украшение: без него соединение остаётся с открытым батчем, и
// следующий шаг DAG-а получит «уже есть незакрытый батч». Но и проглотить ошибку
// Abort нельзя (`_ =`): тогда причина, по которой батч всё же остался открытым,
// не попадёт в лог, а следующий шаг упадёт с сообщением, не называющим источник.
// Возвращаемой ошибкой остаётся ПЕРВОПРИЧИНА (Append или чтение факта) — она
// точнее, чем сбой Abort, случившийся уже после неё.
func abortBatch(batch driver.Batch, what string) {
	if aerr := batch.Abort(); aerr != nil {
		log.Warnf("%s: не удалось закрыть батч после ошибки: %v", what, aerr)
	}
}

func init() {
	chimport.Stats = append(chimport.Stats, &minePlansSeeder{})
}
