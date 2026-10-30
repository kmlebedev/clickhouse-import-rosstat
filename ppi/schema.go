package ppi

import "time"

const (
	// ppiSATable — таблица производного слоя и одновременно Name() импортёра,
	// значение CLICKHOUSE_IMPORT_STAT и id шага dagu/ppi.yaml.
	ppiSATable = "ppi_sa"
	// ppiSACountPoints — количественный чек версии: без FINAL count() считал бы
	// несмёрженные части и не сходился бы на повторном прогоне.
	ppiSACountPoints = "SELECT count() FROM ppi_sa FINAL"

	// ppiSAMethod и ppiSASpecVersion — версия метода и точной спецификации.
	// Любая правка ppi/data/ppi_sa_x13.R или формулы обязана поднять
	// ppiSASpecVersion: хеш скрипта пинится тестом.
	ppiSAMethod      = "x13arima-seats"
	ppiSASpecVersion = "ppi-sa-2026-v1"

	// ppiSATaxonomyVersion — версия mapping и панели весов; сверяется с
	// taxonomy_version из встроенного CSV.
	ppiSATaxonomyVersion = "okpd2-kipc-2026-v2"

	// ppiSAWeightPeriod — primary-период весов. Фиксированные веса 2026, как в
	// research/T3: годовые веса 2021–2022 неполны и дали бы разрыв состава
	// КИПЦ на 2023-01. Исторические периоды встроены для provenance/sensitivity.
	ppiSAWeightPeriod = "2026"

	// ppiSATotalCode — совокупный ИЦП «Промышленность» в rosstat_ppi_ved.
	ppiSATotalCode = "1323500.029.31"

	// Источники published_at в source_publications.
	ppiSAVEDSource     = "rosstat_ppi_ved"
	ppiSAProductSource = "rosstat_ppi_product"

	// ppiSADerivedURL — аудиторская метка derived-версии в source_publications.
	// Не секрет и не сетевой адрес: файла у производного ряда нет.
	ppiSADerivedURL = "derived:rosstat_ppi_ved+rosstat_ppi_product"
)

// Начала рядов: первый месяц — база 100, его m/m не применяется (как в research).
var (
	ppiSATotalStart    = time.Date(2013, time.January, 1, 0, 0, 0, 0, time.UTC)
	ppiSAConsumerStart = time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC)
)

// ppiSAOrder — фиксированный порядок рядов в CSV и батче: повторный прогон
// пишет строки детерминированно.
var ppiSAOrder = []string{"total", "food", "nonfood", "all", "core"}

// ppiSADdl — DDL производной таблицы. taxonomy_version в ключе: смена
// mapping/весов не затирает прошлую версию, строки читаются с фильтром версии.
// mm_sa/saar/saar3 — Nullable: первые месяцы ряда не имеют предыдущего значения.
const ppiSADdl = `CREATE TABLE IF NOT EXISTS ppi_sa (
    date             Date
  , series           LowCardinality(String)
  , method           LowCardinality(String)
  , spec_version     LowCardinality(String)
  , taxonomy_version LowCardinality(String)
  , sa               Float64
  , mm_sa            Nullable(Float64)
  , saar             Nullable(Float64)
  , saar3            Nullable(Float64)
  , published_at     Nullable(Date)
  , loaded_at        DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (date, series, method, spec_version, taxonomy_version);
`

// Пины встроенных данных и скрипта. Меняя файл, разработчик обязан осознанно
// поднять spec_version/taxonomy_version и обновить пин — иначе тест падает.
const (
	ppiX13ScriptSHA256 = "66d8257ee5f051967ddb860241e2b171cd654a0b7ab0847c89c5afb3e97eaae5"
	ppiMappingSHA256   = "11fea7a8f57e9666311db4c1e1b72a2e6fe071323a67ecfc0c9ca4f49bbdd1db"
	ppiWeightsSHA256   = "5e6a2757dbc7feceaa327d3b5e4d211a78297397090c6e4840cf687dba7fa110"
)
