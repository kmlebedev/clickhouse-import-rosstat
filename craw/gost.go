package craw

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// Импортёр реестра СБКТС Росстандарта: схема с идемпотентной миграцией, разбор окружения,
// три режима (since/full/check) и запись дневными батчами. Транспорт и сборка дневного среза —
// в craw/gost_api.go; здесь только обвязка прогона: что читать, что писать и как отчитаться.
// Токен gtn.access.appid приходит из GOST_APP_ID: значения токена в коде и комментариях нет
// (правило 1), а текст ошибки 401/403 называет только переменную окружения.

const (
	// gostTable — таблица реестра (она же Name() импортёра: контракт CLICKHOUSE_IMPORT_STAT
	// и dagu). Канонический DDL — ARCHITECTURE.md §6.2, копия ниже обязана с ним совпадать.
	gostTable = "gost_vehicle_safety_certificate"
	// gostDateLayout — формат дат прогона: дни окна, границы из env, ключи сверки check.
	gostDateLayout = "2006-01-02"

	// gostModeSince — режим расписания: хвост от якоря таблицы; gostModeFull — вся история
	// вручную; gostModeCheck — сверка полноты без записи.
	gostModeSince = "since"
	gostModeFull  = "full"
	gostModeCheck = "check"

	// gostDefaultFrom — начало всей истории: публикации реестра старше 2016 года практически
	// отсутствуют (разведка в спеке: 15.06.2015 — 0 документов, 15.06.2017 — 316).
	gostDefaultFrom = "2016-01-01"
	// gostDefaultSettleDays — хвост, перечитываемый каждый час: лаг «выдача → публикация»
	// около двух суток, и опоздавшие документы обязаны догоняться, а не теряться.
	gostDefaultSettleDays = 3
	// gostEmptyTableDays — окно пустой таблицы в since: обход десяти лет в режиме расписания
	// недопустим, но и падать впустую dagu не должен — читаем последнюю неделю и предупреждаем.
	gostEmptyTableDays = 7
	// gostMaxSinceDays — предел окна since: на отставшей таблице якорь бывает старше на месяцы
	// (первый плановый прогон на legacy-таблице без бэкфилла), и часовой DAG ушёл бы в обход
	// тысяч дней, заняв портал и не догнав хвост за один прогон. Окно обрезается месяцем, а
	// оператору печатается призыв к GOST_MODE=full.
	gostMaxSinceDays = 30
	// gostMaxRangeDays — предохранитель диапазона full/check: вся история реестра — ~4000 дней,
	// и больший запрос означает опечатку в границах, а не диапазон.
	gostMaxRangeDays = 4000
)

// Имена переменных окружения прогона: они же — часть ошибок конфига (по тексту понятно, что
// править в ~/.config/rosstat/env).
const (
	gostModeEnv   = "GOST_MODE"
	gostFromEnv   = "GOST_FROM"
	gostToEnv     = "GOST_TO"
	gostSettleEnv = "GOST_SETTLE_DAYS"
	gostAppIDEnv  = "GOST_APP_ID"
)

// gostClock — «сейчас» прогона. Переменная, а не time.Now на месте: от неё зависят окно since
// и дефолт GOST_TO (вчера МСК), и тесты подменяют часы, чтобы окно не зависело от дня запуска.
var gostClock = time.Now

// Запросы к ClickHouse. Их текст — часть контракта (пинится тестами) и живёт рядом с таблицей,
// чтобы правка схемы не разъезжалась с чтением.
const (
	// gostAnchorQuery — якорь режима since: последняя дата выдачи и последняя дата публикации.
	// Вторая половина нормализована: maxIf на непустой таблице без настоящих публикаций вернул бы
	// 1970-01-01 (дефолт колонки у строк до бэкфилла), и этот ноль выглядел бы настоящей датой;
	// nullIf(…, toDate('1970-01-01')) делает его NULL — как у пустой таблицы. Порог 2016-01-01
	// отсекает те же строки до бэкфилла: дата среза раньше начала реестра означает отсутствие
	// даты публикации, а не публикацию.
	gostAnchorQuery = "SELECT maxOrNull(date) AS max_issue, nullIf(maxIf(publication_date, publication_date > toDate('2016-01-01')), toDate('1970-01-01')) AS max_pub FROM " + gostTable

	// gostLegacyQuery — строки до бэкфилла: publication_date осталась дефолтом колонки.
	gostLegacyQuery = "SELECT count() FROM " + gostTable + " WHERE publication_date = toDate('1970-01-01')"

	// gostCountersQuery — счётчики таблицы по дню публикации для check: одним запросом на всё
	// окно (иначе сверка 4000 дней сделала бы 4000 запросов к базе). uniqExact, а не count():
	// ключ ReplacingMergeTree — (mark, type, date, certificate_number), и изменение сертификата
	// даёт вторую строку с тем же номером — считать надо уникальные номера, иначе check видел бы
	// дыру там, где её нет. FINAL — по определению ReplacingMergeTree: без него вытесненные
	// копии строк считались бы дважды.
	gostCountersQuery = "SELECT publication_date, uniqExact(certificate_number) FROM " + gostTable + " FINAL WHERE publication_date BETWEEN ? AND ? GROUP BY publication_date"

	// gostInsert — запись дня. Порядок колонок — контракт с порядком Append в gostWriteDay
	// и с порядком колонок DDL.
	gostInsert = "INSERT INTO " + gostTable + " (mark, type, date, certificate_number, ecoclass, enginetrademark, yearofmanufacturing, nameofmanufacturer, uid, publication_date)"
)

// gostTableDdl — копия канонического DDL (ARCHITECTURE.md §6.2, спека §«Схема и миграция»);
// копии обязаны совпадать, как у macro_series и news_events. Существующая в dev-БД таблица
// создана с тем же ORDER BY, поэтому CREATE … IF NOT EXISTS её не пересоздаёт.
const gostTableDdl = `CREATE TABLE IF NOT EXISTS ` + gostTable + ` (
		  mark LowCardinality(String)                  -- марка ТС (product)
		, type LowCardinality(String)                  -- тип/модель ТС (type)
		, date Date                                    -- дата выдачи СБКТС (МСК)
		, certificate_number String                    -- номер СБКТС («ТС RU …»)
		, ecoclass LowCardinality(String)              -- экологический класс; '-' — не применимо
		, enginetrademark String                       -- двигатель: модель, топливо (дизель/бензин/электро)
		, yearofmanufacturing UInt16                   -- год выпуска; 0 — не указан / строка до бэкфилла
		, nameofmanufacturer LowCardinality(String)    -- производитель
		, uid String                                   -- uniqueidentifierofdocument (провенанс)
		, publication_date Date                        -- дата батча публикации (провенанс/резюмируемость)
	) ENGINE = ReplacingMergeTree ORDER BY (mark, type, date, certificate_number);`

// gostAlterStatements — миграция живой четырёхколоночной таблицы (схема прежнего импортёра:
// mark/type/date/certificate_number): ADD COLUMN IF NOT EXISTS идемпотентен, поэтому выполняется
// при каждом прогоне, а не «один раз»; порядок колонок — как в DDL, чтобы AFTER не переставлял их
// местами. Старые строки получают дефолты новых колонок (0 и 1970-01-01) — их видно через
// gostLegacyRows, пока они не перезаписаны полным прогоном.
var gostAlterStatements = []string{
	`ALTER TABLE ` + gostTable + ` ADD COLUMN IF NOT EXISTS ecoclass LowCardinality(String) AFTER certificate_number`,
	`ALTER TABLE ` + gostTable + ` ADD COLUMN IF NOT EXISTS enginetrademark String AFTER ecoclass`,
	`ALTER TABLE ` + gostTable + ` ADD COLUMN IF NOT EXISTS yearofmanufacturing UInt16 AFTER enginetrademark`,
	`ALTER TABLE ` + gostTable + ` ADD COLUMN IF NOT EXISTS nameofmanufacturer LowCardinality(String) AFTER yearofmanufacturing`,
	`ALTER TABLE ` + gostTable + ` ADD COLUMN IF NOT EXISTS uid String AFTER nameofmanufacturer`,
	`ALTER TABLE ` + gostTable + ` ADD COLUMN IF NOT EXISTS publication_date Date AFTER uid`,
}

// gostSchemaStatements — DDL прогона по порядку: CREATE, затем ALTER-ы. Возвращается срез, а не
// переменная: вызывающий не должен получить возможность переставить стейтменты местами.
func gostSchemaStatements() []string {
	return append([]string{gostTableDdl}, gostAlterStatements...)
}

// gostConfig — конфигурация прогона из окружения.
type gostConfig struct {
	// Mode — since (расписание), full (вся история), check (сверка без записи).
	Mode string
	// From/To — границы диапазона режимов full/check. В since не используются: окно считает
	// якорь таблицы, и дефолтов для неиспользуемых дат нет.
	From, To time.Time
	// SettleDays — хвост since в днях: сколько дней до якоря перечитывать.
	SettleDays int
	// AppID — токен gtn.access.appid из окружения; в репозитории значения нет (правило 1).
	AppID string
}

// gostConfigFromEnv разбирает окружение прогона. Ошибка здесь — до сети и до БД: опечатка
// в режиме или границе не должна стоить ни обращения к порталу, ни половины диапазона.
func gostConfigFromEnv() (gostConfig, error) {
	cfg := gostConfig{SettleDays: gostDefaultSettleDays}

	mode := strings.TrimSpace(os.Getenv(gostModeEnv))
	switch mode {
	case "":
		// Пустая переменная — since: безопасный дефолт ручного запуска (как NEWS_WATCH_MODE),
		// а расписание задаёт режим явно.
		cfg.Mode = gostModeSince
	case gostModeSince, gostModeFull, gostModeCheck:
		cfg.Mode = mode
	default:
		return gostConfig{}, fmt.Errorf("%s=%q: неизвестный режим (since|full|check)", gostModeEnv, mode)
	}

	appID := strings.TrimSpace(os.Getenv(gostAppIDEnv))
	if appID == "" {
		return gostConfig{}, fmt.Errorf("%s не задан: токен gtn.access.appid берётся из JS-конфига страницы реестра (standarts.adminsHeaders) и в репозитории не хранится — впишите его в ~/.config/rosstat/env", gostAppIDEnv)
	}
	cfg.AppID = appID

	if raw := strings.TrimSpace(os.Getenv(gostSettleEnv)); raw != "" {
		settle, err := strconv.Atoi(raw)
		if err != nil || settle < 0 {
			return gostConfig{}, fmt.Errorf("%s=%q: ожидалось целое число дней не меньше нуля", gostSettleEnv, raw)
		}
		cfg.SettleDays = settle
	}

	from, err := gostEnvDate(gostFromEnv)
	if err != nil {
		return gostConfig{}, err
	}
	to, err := gostEnvDate(gostToEnv)
	if err != nil {
		return gostConfig{}, err
	}
	if cfg.Mode != gostModeSince {
		if from.IsZero() {
			if from, err = time.Parse(gostDateLayout, gostDefaultFrom); err != nil {
				return gostConfig{}, fmt.Errorf("разбор границы по умолчанию %s: %w", gostDefaultFrom, err)
			}
		}
		if to.IsZero() {
			// Верхняя граница — вчера МСК: публикация среза отстаёт от дня прогона (~2 суток),
			// и сегодняшний день читать нечего.
			to = gostYesterdayMSK(gostClock())
		}
	}
	cfg.From, cfg.To = from, to
	if !from.IsZero() && !to.IsZero() && from.After(to) {
		return gostConfig{}, fmt.Errorf("%s=%s больше %s=%s: пустой диапазон",
			gostFromEnv, from.Format(gostDateLayout), gostToEnv, to.Format(gostDateLayout))
	}
	return cfg, nil
}

// gostEnvDate читает дату границы диапазона: пустая переменная — нулевое время («не задана»),
// иначе строгий формат 2006-01-02. Ошибка называет переменную и формат: «01.01.2016» и
// «2026/10/09» — самые частые опечатки, и обе обязаны падать до сети.
func gostEnvDate(name string) (time.Time, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return time.Time{}, nil
	}
	day, err := time.Parse(gostDateLayout, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s=%q: ожидалась дата в формате %s: %w", name, raw, gostDateLayout, err)
	}
	return day, nil
}

// gostYesterdayMSK — «вчера» по Москве: день прогона МСК минус сутки. Одна функция на дефолт
// GOST_TO и на верхнюю границу окна since: обе границы обязаны совпадать, иначе штатный прогон
// и ручной full смотрели бы на разные дни.
func gostYesterdayMSK(now time.Time) time.Time {
	return gostMSKDate(now.UnixMilli()).AddDate(0, 0, -1)
}

// gostDaysBetween — дни диапазона включительно. Границы приводятся к календарному дню в
// UTC-полуночи (в этой шкале живут все даты прогона), шаг — календарные сутки (AddDate), а не
// 24 часа: переход на летнее время не должен ни терять, ни дублировать день. Пустой диапазон —
// ошибка (перевёрнутые границы это опечатка, а не «нечего делать»), слишком длинный — тоже:
// 4000 дней по паре запросов на день это часы работы, такие прогоны запускают чанками осознанно.
func gostDaysBetween(from, to time.Time) ([]time.Time, error) {
	from, to = gostDateOnly(from), gostDateOnly(to)
	if from.After(to) {
		return nil, fmt.Errorf("пустой диапазон %s…%s: начало позже конца",
			from.Format(gostDateLayout), to.Format(gostDateLayout))
	}
	days := make([]time.Time, 0, int(to.Sub(from).Hours()/24)+1)
	for day := from; !day.After(to); day = day.AddDate(0, 0, 1) {
		if len(days) == gostMaxRangeDays {
			return nil, fmt.Errorf("диапазон %s…%s длиннее %d дней: разбейте прогон на чанки",
				from.Format(gostDateLayout), to.Format(gostDateLayout), gostMaxRangeDays)
		}
		days = append(days, day)
	}
	return days, nil
}

// gostDateOnly — календарный день даты в UTC-полуночи. Все даты прогона (дни окна, даты колонок
// из clickhouse-go, gostMSKDate) живут в этой шкале; сравнение иначе зависело бы от времени суток
// и таймзоны процесса.
func gostDateOnly(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// gostSinceDays — дни хвоста режима since: от якоря таблицы минус settle до вчера МСК.
// Якорь — максимум даты выдачи и даты публикации: первая растёт от самих документов, вторая —
// от дня среза, и «последнее, что мы видели» — максимум обеих. Пустая таблица (оба NULL после
// нормализации 1970-01-01) — не обход всей истории, а окно последних семи дней: флаг tableEmpty
// возвращается вызывающему, чтобы тот сказал оператору про GOST_MODE=full. Якорь из будущего
// (часы источника вперёд, ручная правка) прижимается к вчера — «завтра» читать нечего.
//
// Окно ограничено gostMaxSinceDays: на отставшей таблице (якорь старше месяца) часовой прогон
// иначе читал бы тысячи дней подряд, не догоняя хвост и занимая портал, — вместо этого читается
// последний месяц, а оператор получает призыв к полному прогону.
func gostSinceDays(ctx context.Context, conn driver.Conn, settle int, now time.Time) (days []time.Time, tableEmpty bool, err error) {
	var maxIssue, maxPub *time.Time
	if err = conn.QueryRow(ctx, gostAnchorQuery).Scan(&maxIssue, &maxPub); err != nil {
		return nil, false, fmt.Errorf("%s: якорь таблицы не прочитан: %w", gostTable, err)
	}

	to := gostYesterdayMSK(now)
	if maxIssue == nil && maxPub == nil {
		from := to.AddDate(0, 0, -(gostEmptyTableDays - 1))
		days, err = gostDaysBetween(from, to)
		return days, true, err
	}

	anchor := time.Time{}
	for _, candidate := range []*time.Time{maxIssue, maxPub} {
		if candidate == nil {
			continue
		}
		if day := gostDateOnly(*candidate); day.After(anchor) {
			anchor = day
		}
	}
	if anchor.After(to) {
		anchor = to
	}
	from := anchor.AddDate(0, 0, -settle)
	if limit := to.AddDate(0, 0, -(gostMaxSinceDays - 1)); from.Before(limit) {
		log.Warnf("%s: таблица отстала: якорь %s, окно since ограничено %d днями (с %s) — запустите GOST_MODE=full для истории",
			gostTable, anchor.Format(gostDateLayout), gostMaxSinceDays, limit.Format(gostDateLayout))
		from = limit
	}
	days, err = gostDaysBetween(from, to)
	return days, false, err
}

// gostLegacyRows считает строки до бэкфилла: у них publication_date — дефолт колонки
// (1970-01-01), и в сверке check они не участвуют. Ошибка запроса фатальна: молча пропущенное
// предупреждение оставило бы оператора без единственного сигнала, что исторические строки ещё
// не перезаписаны полным прогоном.
func gostLegacyRows(ctx context.Context, conn driver.Conn) (uint64, error) {
	var legacy uint64
	if err := conn.QueryRow(ctx, gostLegacyQuery).Scan(&legacy); err != nil {
		return 0, fmt.Errorf("%s: легаси-строки не посчитаны: %w", gostTable, err)
	}
	return legacy, nil
}

// gostWriteDay пишет день одним батчем: построчный Exec в цикле запрещён (правило 2), а
// половина дня хуже пропуска — её не видно. Порядок Append — порядок колонок DDL. Abort
// вызывается только в ветке ошибки Append: у соединения не должно остаться открытого батча
// (следующий день получил бы «уже есть незакрытый батч»), а сбой Abort логируется и не
// подменяет первопричину.
func gostWriteDay(ctx context.Context, conn driver.Conn, rows []gostRow) (int64, error) {
	batch, err := conn.PrepareBatch(ctx, gostInsert)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", gostTable, gostInsert, err)
	}
	for _, r := range rows {
		if err = batch.Append(
			r.Mark,
			r.Type,
			r.Date,
			r.CertificateNumber,
			r.EcoClass,
			r.EngineTrademark,
			r.YearOfManufacturing,
			r.NameOfManufacturer,
			r.UID,
			r.PublicationDate,
		); err != nil {
			if aerr := batch.Abort(); aerr != nil {
				log.Warnf("%s: не удалось закрыть батч после ошибки append: %v", gostTable, aerr)
			}
			return 0, fmt.Errorf("%s: append %s %s: %w", gostTable, r.CertificateNumber, r.PublicationDate.Format(gostDateLayout), err)
		}
	}
	if err = batch.Send(); err != nil {
		return 0, fmt.Errorf("%s: %s: %w", gostTable, gostInsert, err)
	}
	return int64(batch.Rows()), nil
}

// gostRunDays обходит дни и пишет каждый одним батчем. День с ошибкой не останавливает проход:
// импортёр резюмируемый — остальные дни всё равно попадут в таблицу, а повтор упавших
// идемпотентен (ReplacingMergeTree по ключу). Итог — агрегат ошибок с перечислением дней:
// «упало 3 дня из 4000» без имён не разобрать.
func gostRunDays(ctx context.Context, conn driver.Conn, api *gostAPI, days []time.Time) (int64, error) {
	var (
		total  int64
		failed []string
	)
	for i, day := range days {
		if i > 0 {
			// Пауза между днями: щадим портал. Штатный прогон делает 4–12 запросов в час, но
			// полный — тысячи, и без пауз это выглядело бы для источника шквалом.
			time.Sleep(gostRequestGap)
		}
		label := day.Format(gostDateLayout)

		slice, err := api.day(ctx, day)
		if err != nil {
			log.Errorf("%s: день %s не прочитан: %v", gostTable, label, err)
			failed = append(failed, fmt.Sprintf("%s: %v", label, err))
			continue
		}
		if slice.Count == 0 {
			// Пустой день — норма (публикаций не было): батч не открываем, но и это факт дня.
			log.Infof("%s: день %s: публикаций нет", gostTable, label)
			continue
		}
		if slice.Skipped > 0 {
			// Ненулевой Skipped — датчик: клиентский фильтр по дате публикации отбросил
			// документы, то есть ручка начала отдавать чужое окно. Резкий рост доли — смена
			// формата dateofpublication, и день «похудеет» молча.
			log.Warnf("%s: день %s: отброшено документов чужой даты публикации: %d из %d",
				gostTable, label, slice.Skipped, slice.Count)
		}
		if len(slice.Rows) == 0 {
			// В ручке совпадения есть, а в срез дня не попало ни одного документа: так выглядит
			// смена формата dateofpublication (ручка ищет текстом, и день среза перестал
			// совпадать с ответом). Это ошибка прогона, а не пустой день: молча пропущенный
			// день оставил бы dagu зелёным при потерянных данных.
			errText := fmt.Sprintf("день %s: все %d документов отброшены фильтром публикации (Skipped %d) — проверьте формат даты публикации/ручку",
				label, slice.Count, slice.Skipped)
			log.Errorf("%s: %s", gostTable, errText)
			failed = append(failed, errText)
			continue
		}

		written, err := gostWriteDay(ctx, conn, slice.Rows)
		if err != nil {
			log.Errorf("%s: день %s не записан: %v", gostTable, label, err)
			failed = append(failed, fmt.Sprintf("%s: запись: %v", label, err))
			continue
		}
		total += written
		log.Infof("%s: день %s: записано %d строк", gostTable, label, written)
	}
	if len(failed) > 0 {
		return total, fmt.Errorf("%s: не прочитаны дни (%d): %s", gostTable, len(failed), strings.Join(failed, "; "))
	}
	return total, nil
}

// gostCheckDays сверяет полноту окна, не записывая ничего: по каждому дню count ручки против
// uniqExact(certificate_number) таблицы по дню публикации.
//
// Совпавшие счётчики закрывают день одним запросом. Расхождение — ещё не дыра: ручка ищет
// текстом, и в count попадают документы, чья дата публикации на самом деле другая. Поэтому при
// расхождении день выкачивается целиком и сравнивается длина среза после клиентского фильтра:
// сошлась — это текстовый шум (строка отчёта), не сошлась — дыра (ошибка прогона). Так
// «день не дочитан» и «шум» перестают быть неразличимыми.
func gostCheckDays(ctx context.Context, conn driver.Conn, api *gostAPI, days []time.Time) (checked int64, err error) {
	if len(days) == 0 {
		return 0, fmt.Errorf("%s: check без дней: пустое окно", gostTable)
	}
	counters, err := gostStoredDayCounts(ctx, conn, days[0], days[len(days)-1])
	if err != nil {
		return 0, err
	}

	var (
		problems []string
		gaps     int
	)
	for i, day := range days {
		if i > 0 {
			time.Sleep(gostRequestGap)
		}
		label := day.Format(gostDateLayout)

		apiCount, cerr := api.dayCount(ctx, day)
		if cerr != nil {
			log.Errorf("%s: check: день %s: count ручки не прочитан: %v", gostTable, label, cerr)
			problems = append(problems, fmt.Sprintf("%s: count ручки не прочитан: %v", label, cerr))
			continue
		}
		if apiCount > gostMaxDayCount {
			problems = append(problems, fmt.Sprintf("%s: в ручке %d документов — больше окна поиска (%d): день нечитаем",
				label, apiCount, gostMaxDayCount))
			continue
		}

		stored := counters[label]
		checked++
		if apiCount == int(stored) {
			log.Infof("%s: check: %s: %d против %d — совпало", gostTable, label, stored, apiCount)
			continue
		}

		slice, derr := api.day(ctx, day)
		if derr != nil {
			problems = append(problems, fmt.Sprintf("%s: день не дочитан: %v", label, derr))
			continue
		}
		if len(slice.Rows) == int(stored) {
			log.Infof("%s: check: %s: шум: %d → %d (в таблице %d) — дыр нет",
				gostTable, label, apiCount, len(slice.Rows), stored)
			continue
		}
		gaps++
		problems = append(problems, fmt.Sprintf("%s: в таблице %d против %d в дне источника", label, stored, len(slice.Rows)))
	}

	log.Infof("%s: check: проверено %d дней, дыр %d", gostTable, checked, gaps)
	if len(problems) > 0 {
		return checked, fmt.Errorf("%s: check: расхождения (%d): %s", gostTable, len(problems), strings.Join(problems, "; "))
	}
	return checked, nil
}

// gostStoredDayCounts читает число уникальных номеров по дню публикации одним запросом на всё
// окно. Дни без строк в map отсутствуют (GROUP BY их не отдаёт) — для сверки это ноль. Обрыв
// обхода строк (rows.Err) обязан быть виден: молча вернувшаяся половина счётчиков объявила бы
// дырами дни, которые на самом деле есть.
func gostStoredDayCounts(ctx context.Context, conn driver.Conn, from, to time.Time) (map[string]uint64, error) {
	rows, err := conn.Query(ctx, gostCountersQuery, from, to)
	if err != nil {
		return nil, fmt.Errorf("%s: счётчики таблицы не прочитаны: %w", gostTable, err)
	}
	defer func() { _ = rows.Close() }()

	counts := make(map[string]uint64)
	for rows.Next() {
		var (
			day   time.Time
			count uint64
		)
		// Порядок Scan — порядок колонок gostCountersQuery: publication_date, uniqExact(...).
		if err = rows.Scan(&day, &count); err != nil {
			return nil, fmt.Errorf("%s: строка счётчиков не прочитана: %w", gostTable, err)
		}
		counts[day.Format(gostDateLayout)] = count
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: счётчики таблицы не дочитаны: %w", gostTable, err)
	}
	return counts, nil
}

// gostImporter — импортёр реестра СБКТС в режимах since/full/check.
type gostImporter struct{}

func (s *gostImporter) Name() string { return gostTable }

// Import — один прогон. Порядок шагов — часть контракта, а не стиль:
//  1. конфиг из окружения: опечатка в режиме, дате или хвосте падает до любого обращения к conn;
//  2. DDL и идемпотентная миграция: импортёр обязан отработать и на чистой базе, и на живой
//     четырёхколоночной таблице dev-БД;
//  3. предупреждение о строках до бэкфилла (ошибка счёта фатальна: молчание лишило бы оператора
//     единственного сигнала, что историю надо перезаписать полным прогоном);
//  4. ветка режима: check — сверка без записи, since/full — обход дней дневными батчами.
//
// Витрины v_gost_* публикуются не здесь: их добавляет следующий шаг работы (gostPublishViews
// перед обходом дней), и до него пакет обязан собираться без этой функции.
func (s *gostImporter) Import(ctx context.Context, conn driver.Conn) (int64, error) {
	cfg, err := gostConfigFromEnv()
	if err != nil {
		return 0, err
	}

	for _, stmt := range gostSchemaStatements() {
		if err = conn.Exec(ctx, stmt); err != nil {
			return 0, fmt.Errorf("%s: схема: %w", gostTable, err)
		}
	}

	legacy, err := gostLegacyRows(ctx, conn)
	if err != nil {
		return 0, err
	}
	if legacy > 0 {
		log.Warnf("%s: %d строк до бэкфилла (publication_date = 1970-01-01) — в сверке check они не участвуют; запустите GOST_MODE=full, чтобы перезаписать их по ключу",
			gostTable, legacy)
	}

	api := newGostAPI(util.HttpClient, cfg.AppID)

	var days []time.Time
	switch cfg.Mode {
	case gostModeCheck:
		if days, err = gostDaysBetween(cfg.From, cfg.To); err != nil {
			return 0, err
		}
		log.Infof("%s: режим check, дней %d (%s…%s)", gostTable, len(days),
			days[0].Format(gostDateLayout), days[len(days)-1].Format(gostDateLayout))
		return gostCheckDays(ctx, conn, api, days)
	case gostModeFull:
		if days, err = gostDaysBetween(cfg.From, cfg.To); err != nil {
			return 0, err
		}
		log.Infof("%s: режим full, дней %d (%s…%s)", gostTable, len(days),
			days[0].Format(gostDateLayout), days[len(days)-1].Format(gostDateLayout))
	default: // since
		var tableEmpty bool
		if days, tableEmpty, err = gostSinceDays(ctx, conn, cfg.SettleDays, gostClock()); err != nil {
			return 0, err
		}
		log.Infof("%s: режим since (хвост %d дней), дней %d (%s…%s)", gostTable, cfg.SettleDays, len(days),
			days[0].Format(gostDateLayout), days[len(days)-1].Format(gostDateLayout))
		if tableEmpty {
			log.Warnf("%s: таблица пуста — история не загружена, запустите GOST_MODE=full; читаю последние %d дней (%s…%s)",
				gostTable, len(days), days[0].Format(gostDateLayout), days[len(days)-1].Format(gostDateLayout))
		}
	}
	return gostRunDays(ctx, conn, api, days)
}

func init() {
	chimport.Stats = append(chimport.Stats, &gostImporter{})
}
