package weeklysa

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

const (
	// weeklySATable — таблица производного слоя и одновременно Name() импортёра,
	// значение CLICKHOUSE_IMPORT_STAT и id шага dagu/ppi.yaml.
	weeklySATable = "weekly_cpi_sa"
	// weeklySAVintageTable — append-only история винтажей: одна строка на
	// (date, series, method, spec_version, vintage, computed_at). Основная таблица
	// остаётся «последней» для существующих потребителей, винтажи не затираются.
	weeklySAVintageTable = "weekly_cpi_sa_vintage"
	// weeklySACountPoints — количественный чек версии: без FINAL count() считал бы
	// несмёрженные части и не сходился бы на повторном прогоне.
	weeklySACountPoints = "SELECT count() FROM weekly_cpi_sa FINAL"

	// weeklySAMethod и weeklySASpecVersion — версия метода и точной спецификации.
	// Любая правка weeklysa/data/weekly_cpi_sa_x13.R или формулы обязана поднять
	// spec_version: хеш скрипта пинится тестом.
	weeklySAMethod      = "x13arima-seats"
	weeklySASpecVersion = "weekly-cpi-sa-2026-v1"
	weeklySASeries      = "weekly_cpi"

	// weeklySAFeatureVersion — версия набора строк/провенанса, не формулы:
	// nowcast-строка (as_of/partial) — v1, винтажи (vintage/computed_at) — v2.
	// Входит в отпечаток входа, чтобы первый прогон после фикса не стал no-op
	// на старом отпечатке; spec_version при этом не меняется —
	// формула и R-скрипт те же.
	weeklySAFeatureVersion = "weekly-cpi-sa-vintage-v2"

	// Источники: агрегат «Индекс потребительских цен (недельная оценка)» лежит в
	// бюллетенях 2009–2021 (колонка name) и в BI 2019+ (колонка series); на
	// пересечении 2019–2021 BI приоритетен, ровно как в v_rosstat_macro.
	weeklySABulletinsSource = "rosstat_ipc_weeks_bulletins"
	weeklySABISource        = "rosstat_bi_cpi_weekly"
	weeklySAAggregateSeries = "Индекс потребительских цен (недельная оценка)"

	// weeklySADerivedURL — аудиторская метка derived-версии в source_publications.
	// Не секрет и не сетевой адрес: файла у производного ряда нет.
	weeklySADerivedURL = "derived:rosstat_ipc_weeks_bulletins+rosstat_bi_cpi_weekly"

	// Пороги входа и месячной сетки — снимок 2026-10-22. Это полы, а не точные
	// значения: каждая новая недельная публикация/закрытый месяц увеличивает ряд,
	// и импортёр не должен падать на штатном росте источника.
	weeklySAMinInputPoints = 760
	weeklySAMinMonths      = 209

	// weeklySAAggregateWeeks — окно 4-нед. индекса (ровно 28 дней на 7-дневной
	// сетке); weeklySAAnnualPeriods — число таких окон в году для SAAR.
	weeklySAAggregateWeeks = 4
	weeklySAAnnualPeriods  = 13

	// weeklyX13ScriptSHA256 — пин встроенного R-скрипта. Правка скрипта без bump
	// spec_version валит checkEmbeddedPins и тест.
	weeklyX13ScriptSHA256 = "29f87c53fe620332dd51c4a17402b2175e85db97ea1932273aab8d187d208b6a"
)

// Начала ряда: первая точка агрегата и первый полный месяц 4-нед. индекса.
var (
	weeklySAFirstDate  = time.Date(2009, time.April, 6, 0, 0, 0, 0, time.UTC)
	weeklySAFirstMonth = time.Date(2009, time.May, 1, 0, 0, 0, 0, time.UTC)
)

// weeklySADdl — DDL производной таблицы («последний винтаж» для существующих
// потребителей). sa_index/sa_4w/saar — Float64 и не Nullable: X-13 отдаёт SA для
// каждого месяца входа, включая первый. vintage/computed_at описывают, из какого
// входа посчитаны текущие строки; сама история винтажей живёт в append-only
// weeklySAVintageTable (ключ с vintage/computed_at), потому что MODIFY ORDER BY
// существующей таблицы ClickHouse для уже добавленных колонок запрещает (дефект Ф4
// бэктеста economist-oracle, #622/#630).
const weeklySADdl = `CREATE TABLE IF NOT EXISTS weekly_cpi_sa (
    date           Date
  , series         LowCardinality(String)
  , method         LowCardinality(String)
  , spec_version   LowCardinality(String)
  , vintage        Date
  , computed_at    DateTime('Europe/Moscow')
  , sa_index       Float64
  , sa_4w          Float64
  , saar           Float64
  , as_of          Date DEFAULT '1970-01-01'
  , partial        UInt8 DEFAULT 0
  , published_at   Nullable(Date)
  , loaded_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (date, series, method, spec_version);
`

// weeklySAVintageDdl — канонический DDL append-only истории винтажей. Ключ включает
// (vintage, computed_at): каждая новая версия входа даёт отдельные строки, ревизия
// источника не затирает прежний расчёт. Основная weekly_cpi_sa остаётся «последней»
// (её ORDER BY не меняется: MODIFY ORDER BY с уже существующими колонками ClickHouse
// запрещает), а история живёт здесь.
const weeklySAVintageDdl = `CREATE TABLE IF NOT EXISTS ` + weeklySAVintageTable + ` (
    date           Date
  , series         LowCardinality(String)
  , method         LowCardinality(String)
  , spec_version   LowCardinality(String)
  , vintage        Date
  , computed_at    DateTime('Europe/Moscow')
  , sa_index       Float64
  , sa_4w          Float64
  , saar           Float64
  , as_of          Date
  , partial        UInt8
  , published_at   Nullable(Date)
  , loaded_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (date, series, method, spec_version, vintage, computed_at);
`

// weeklySAVintageBackfillInsert — однократный перенос текущих строк основной таблицы
// в историю: у них уже есть vintage/computed_at (миграция ниже). Guard — пустая
// таблица винтажей; повтор не пишет.
const weeklySAVintageBackfillInsert = `INSERT INTO ` + weeklySAVintageTable + `
    (date, series, method, spec_version, vintage, computed_at, sa_index, sa_4w, saar, as_of, partial, published_at, loaded_at)
SELECT date, series, method, spec_version, vintage, computed_at, sa_index, sa_4w, saar, as_of, partial, published_at, loaded_at
FROM weekly_cpi_sa FINAL`

// weeklySAVintageCount — guard бэкфилла истории.
const weeklySAVintageCount = "SELECT count() FROM " + weeklySAVintageTable + " FINAL"

// weeklySAAlters — миграция уже существующей таблицы (первая версия шла без
// as_of/partial, затем без винтажей). Идемпотентна: ADD COLUMN IF NOT EXISTS;
// DEFAULT нужен, чтобы старые строки читались, пока бэкфилл/полный прогон не
// перезапишет их реальными значениями.
var weeklySAAlters = []string{
	`ALTER TABLE weekly_cpi_sa ADD COLUMN IF NOT EXISTS as_of Date DEFAULT '1970-01-01'`,
	`ALTER TABLE weekly_cpi_sa ADD COLUMN IF NOT EXISTS partial UInt8 DEFAULT 0`,
	`ALTER TABLE weekly_cpi_sa ADD COLUMN IF NOT EXISTS vintage Date DEFAULT '1970-01-01'`,
	`ALTER TABLE weekly_cpi_sa ADD COLUMN IF NOT EXISTS computed_at DateTime('Europe/Moscow') DEFAULT now()`,
}

// weeklySALegacyVintageCount — guard однократного бэкфилла: строки, существовавшие
// до винтажей, несут vintage='1970-01-01'. После UPDATE условие пустое.
const weeklySALegacyVintageCount = "SELECT count() FROM weekly_cpi_sa WHERE vintage = '1970-01-01'"

// weeklySAVintageBackfill — бэкфилл старых строк: отсечка данных у них — as_of
// (последняя неделя, по которой считали), момент расчёта — loaded_at. UPDATE
// материализует computed_at, иначе DEFAULT now() показывал бы время запроса.
const weeklySAVintageBackfill = `ALTER TABLE weekly_cpi_sa UPDATE vintage = as_of, computed_at = loaded_at WHERE vintage = '1970-01-01' SETTINGS mutations_sync=2`

// checkEmbeddedPins сверяет встроенный R-скрипт с пином. Это не косметика:
// правка скрипта без bump spec_version сделала бы версию в строке ложной.
// Ошибка останавливает импорт до чтения БД.
func checkEmbeddedPins() error {
	if got := sha256Hex(weeklyX13Script); got != weeklyX13ScriptSHA256 {
		return fmt.Errorf("weeklysa/data/weekly_cpi_sa_x13.R: SHA-256 %s, ожидался %s: обновите пин и spec_version", got, weeklyX13ScriptSHA256)
	}
	return nil
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
