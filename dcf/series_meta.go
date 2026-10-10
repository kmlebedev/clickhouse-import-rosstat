package dcf

import (
	"context"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Константы каталога рядов сида mine_plans.
//
// Вынесены в константы и склеиваются через strings.Join: запись — это НЕ строка
// кода, а контракт для MCP-агента, и агент читает её перед запросом к ряду
// (v_series_catalog). Собранный из кусков текст держит одну мысль в одном месте,
// а тест TestMinePlansSeriesMetaDescribesAssumption проверяет наличие каждого
// содержательного требования — при правке константы тест падает, а не текст
// молча теряет оговорку.
const (
	// minePlansSeriesSource — source в series_catalog. Совпадает с Name() импортёра
	// (minePlansSeeder.Name): каталог обязан называть тот же источник, что и шаг
	// DAG, иначе агент не свяжет описание с запуском `make import STAT=dcf_mine_plans`.
	minePlansSeriesSource = "dcf_mine_plans"

	// Годовой ряд: конвенция series_catalog — код M/Q/D/W (см. комментарии колонки
	// frequency в util/series_catalog.go и записи всех импортёров), поэтому год
	// кодируется как «A», а не словом. Отдельного кода для года в репозитории нет;
	// выдумывать новый нельзя — агент читает эту колонку машинно.
	minePlansSeriesFrequency = "A"

	// minePlansSeriesOrigin — происхождение ряда: таблица, импортёр, публикация с
	// годом и URL. Слово «снимок» здесь принципиально: это версия отчётности, а не
	// живая выгрузка.
	minePlansSeriesOrigin = "mine_plans (таблица наполняется импортёром dcf_mine_plans: " +
		"строка на актив и год), снимок публичной отчётности на Годовой обзор ПАО «Полюс» " +
		"за 2025 год (https://polyus.com/upload/iblock/d6b/godovoy_obzor_pao_polyus_za_2025_god.pdf) " +
		"и презентацию «Ключевые проекты роста» (декабрь 2024, " +
		"https://polyus.com/upload/iblock/f13/klyuchevye_proekty_rosta.pdf); per-asset факт добычи — " +
		"databook_polyus (метрика 'Total Dore gold output', data='ANNUAL'), операционный датапак Полюса"

	// minePlansSeriesUnit — единицы колонок строки mine_plans. Названы ВСЕ три, а не
	// «разные»: у одного ряда (актива) единица меняется от колонки к колонке, и
	// «разные» не даёт агенту ничего, кроме запрета на арифметику. Смешение единиц
	// в NPV — ошибка в тысячи раз (production_koz — тыс. унц, USD/oz — доллары,
	// capex/closure — МЛН USD), поэтому каждая единица привязана к своим колонкам.
	minePlansSeriesUnit = "production_koz — тыс. тройских унций (koz); tcc и aisc — USD за унцию; " +
		"capex_sustaining, capex_project и closure_costs — млн USD; grade_gpt — NULL (содержание не раскрыто)"
)

// minePlansSeriesDescriptionPrefix — общая часть описания: что измеряет ряд и как
// его читать.
//
// Общая (не per-asset) намеренно: различие между активами лежит в данных, а не в
// способе чтения, и восемь копий одного абзаца разошлись бы при первой правке —
// агент получил бы восемь разных объяснений одного контракта.
const minePlansSeriesDescriptionPrefix = "LOM-план актива: строка на год плана, " +
	"production_koz — годовая добыча в тыс. унций (koz), tcc — общие денежные затраты, " +
	"USD/oz, aisc — затраты на поддержание производства, USD/oz (derived, см. ниже), " +
	"capex_sustaining / capex_project — капитальные затраты, млн USD, closure_costs — " +
	"хвост рекультивации, млн USD, в ПОСЛЕДНЕМ году горизонта. Темп считать нельзя: это " +
	"ПЛАН, а не ряд наблюдений — год к году меняется только консервация (нулевая добыча " +
	"внутри горизонта, строка года сохраняется) и профиль проекта развития; добыча " +
	"действующего актива — плато от последнего факта на весь срок службы, а не прогноз по годам."

// minePlansSeriesDescriptionAssumption — вторая половина описания: почему числа
// НЕЛЬЗЯ читать как измеренные. Ради этого абзаца задача и существует: в самой
// таблице tcc/aisc/capex лежат обычными Float64 без признака происхождения, и
// отличить опубликованный TCC актива от выведенного AISC по данным невозможно.
const minePlansSeriesDescriptionAssumption = "ЗНАЧЕНИЯ — ДОПУЩЕНИЕ СИДА, А НЕ ИЗМЕРЕНИЕ. " +
	"production_koz за годы после последнего отчётного факта — плато последнего факта " +
	"(databook_polyus), а не опубликованный по-годичный профиль. tcc — опубликованный TCC " +
	"актива за 2025 год, кроме TITIMUKHTA и ZAPADNOYE, где взят TCC бизнес-единицы (прокси). " +
	"aisc актива Полюс не публикует вовсе: он DERIVED как tcc + 698 — групповой AISC 1 437 " +
	"минус групповой TCC 739 (Годовой обзор 2025, стр. 31), то есть один клин на все годы и " +
	"активы. capex_sustaining — вся раскрытая сумма бизнес-единицы за 2025 год (стр. 33): деления " +
	"на sustaining/project публикация не даёт. closure_costs = 0 значит «не задано / не " +
	"опубликовано по активу» и НИКОГДА — «закрытие бесплатно»: распределение группового " +
	"провижена не утверждено. Снимок пересматривается только отдельным пунктом роадмапа " +
	"(с записью в docs/DCF_DATA_COVERAGE.md), а не правкой строк — иначе история прогонов " +
	"model_runs/nav_by_asset перестанет быть воспроизводимой."

// minePlansSeriesMeta — каталог рядов сида mine_plans: одна запись на актив, то
// есть на значение колонки asset (Series = имя актива — ключ, по которому агент
// соединит запись с таблицей).
//
// Строится из polyusAssetPlans, а не литеральным списком из восьми имён: список —
// второй источник истины рядом с сидом, и актив, добавленный в сид, появился бы в
// mine_plans без описания (тест TestMinePlansSeriesMetaCoversAllAssets это ловит,
// но строить от сида дешевле, чем синхронизировать два списка руками).
//
// Таблицей каталог владеет через minePlansSeeder.Import: запись идёт ПОСЛЕ
// успешного Send (см. комментарий на месте вызова), потому что описание ряда,
// которого в таблице нет, — обещание данных, которых агент не найдёт.
var minePlansSeriesMeta = buildMinePlansSeriesMeta()

// buildMinePlansSeriesMeta собирает записи каталога из сида. Отдельная функция, а
// не литерал var: у var с вызовом внутри инициализация видна в одном месте, а
// тест может позвать сборку повторно и сверить результат, не читая глобальную
// переменную (та же причина, по которой rosstat строит мету функцией
// ipcWeeksSeriesMeta).
func buildMinePlansSeriesMeta() []util.SeriesMeta {
	description := strings.Join([]string{
		minePlansSeriesDescriptionPrefix,
		minePlansSeriesDescriptionAssumption,
	}, " ")

	// Порядок — порядок сида: строки в series_catalog и в mine_plans читаются
	// рядом, и совпадающий порядок активов делает diff двух снимков каталога
	// осмысленным (ReplacingMergeTree по (source, series) порядок не гарантирует,
	// но вставка идёт одним батчем и порядок в теле батча детерминирован).
	meta := make([]util.SeriesMeta, 0, len(polyusAssetPlans))
	for _, plan := range polyusAssetPlans {
		meta = append(meta, util.SeriesMeta{
			Source: minePlansSeriesSource,
			Series: plan.Asset,
			Title: "LOM-план актива " + plan.Asset +
				" (добыча, TCC/AISC, capex, закрытие) — допущение сида",
			Unit:        minePlansSeriesUnit,
			Frequency:   minePlansSeriesFrequency,
			Origin:      minePlansSeriesOrigin,
			Description: description,
		})
	}

	return meta
}

// upsertMinePlansSeriesMeta пишет каталог рядов сида mine_plans.
//
// Вызывается из minePlansSeeder.Import ПОСЛЕ успешного Send: каталог описывает то,
// что реально доступно MCP-агенту, и запись описаний при провалившейся вставке
// обещала бы ряды, которых в таблице нет. Ошибка возвращается наверх, а не
// логируется: Import уже записал строки, и объявить импорт успешным, потеряв
// описание, значило бы скрыть поломку ровно там, где агент читает контракт.
func upsertMinePlansSeriesMeta(ctx context.Context, conn driver.Conn) error {
	return util.UpsertSeriesCatalog(ctx, conn, minePlansSeriesMeta)
}
