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
	// nowcast-строка (as_of/partial) — расширение, которого не было в v1.
	// Входит в отпечаток входа, чтобы первый прогон после фикса не стал no-op
	// на старом (209-строчном) отпечатке; spec_version при этом не меняется —
	// формула и R-скрипт те же.
	weeklySAFeatureVersion = "weekly-cpi-sa-nowcast-v1"

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

// weeklySADdl — DDL производной таблицы. sa_index/sa_4w/saar — Float64 и не
// Nullable: X-13 отдаёт SA для каждого месяца входа, включая первый.
const weeklySADdl = `CREATE TABLE IF NOT EXISTS weekly_cpi_sa (
    date           Date
  , series         LowCardinality(String)
  , method         LowCardinality(String)
  , spec_version   LowCardinality(String)
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

// weeklySAAlters — миграция уже существующей таблицы (первая версия шла без
// as_of/partial). Идемпотентна: ADD COLUMN IF NOT EXISTS; DEFAULT нужен, чтобы
// старые строки читались, пока полный прогон не перезапишет их с реальным as_of.
var weeklySAAlters = []string{
	`ALTER TABLE weekly_cpi_sa ADD COLUMN IF NOT EXISTS as_of Date DEFAULT '1970-01-01'`,
	`ALTER TABLE weekly_cpi_sa ADD COLUMN IF NOT EXISTS partial UInt8 DEFAULT 0`,
}

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
