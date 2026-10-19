package cbr

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	mpSurveyName  = "cbr_mp_survey"
	mpSurveyURL   = "https://www.cbr.ru/Content/Document/File/135603/mp_survey_data.xlsx"
	mpSurveyTable = "macro_series"
	// mpSurveySource = "cbr_survey" объявлена в series_meta.go — повторно в пакете нельзя.

	// mpSurveyCountPoints — количественный чек версии (спека §3.5): число точек своего
	// ряда в общей macro_series. Литерал, как и таблица: запрос — часть утверждения
	// «эти точки мои», и незамеченная смена source увела бы чек к чужим рядам
	// (fred/bls/bea). FINAL и WHERE source обязательны: без первого count() растёт с
	// каждой несмёрженной частью, без второго чек считал бы чужие ряды.
	mpSurveyCountPoints = "SELECT count() FROM macro_series FINAL WHERE source = 'cbr_survey'"
)

const mpSurveyDdl = `CREATE TABLE IF NOT EXISTS %s (
    source LowCardinality(String),   -- 'fred','bls','bea','eia','manual','webbridge','imf','wgc','bold','cbr_survey','cbr_mo_br'
    series LowCardinality(String),   -- 'DFII10','distillate_stocks',...
    date Date32,                     -- Date32, т.к. Date не покрывает даты до 1970 (CPIAUCSL с 1947, FEDFUNDS с 1954)
    value Float64
) ENGINE = ReplacingMergeTree ORDER BY (source, series, date);`

type mpSurveyStat struct {
	util.ClickHouseImport
}

func (s *mpSurveyStat) Name() string { return mpSurveyName }

// mpSurveyImporter — мониторинг предприятий: Name() = cbr_mp_survey, а таблица —
// общая macro_series (source='cbr_survey'). Своего Import() у импортёра нет: полный
// путь (DDL → префлайт → точки → производные → версия) ведёт шаблон
// util.ClickHouseImport, а каталог рядов и витрина v_cbr_survey публикуются его
// AfterImport — после Send точек и до строки версии (§8 п. 13). PublicationSource —
// имя импортёра, а не TableName: иначе ключом версии в source_publications стало бы
// macro_series, общее с cbr_mo_br, и версии двух импортёров слились бы в одну строку.
// Регистрируется реестром cbr/importers.go.
var mpSurveyImporter = &mpSurveyStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              mpSurveyTable,
	CreateTable:            []string{mpSurveyDdl},
	DataUrl:                mpSurveyURL,
	PublicationSource:      mpSurveyName,
	PublicationCountPoints: mpSurveyCountPoints,
	AfterImport: func(ctx context.Context, conn driver.Conn) error {
		if err := util.UpsertSeriesCatalog(ctx, conn, mpSurveyAllMeta); err != nil {
			return err
		}
		_, err := util.CreateView(ctx, conn, cbrSurveyView)
		return err
	},
	ImportFunc: func(xlsx *excelize.File, batch driver.Batch) error {
		points, err := parseSurvey(xlsx, mpSurveyAllSeries)
		if err != nil {
			return err
		}
		log.Infof("Собрано %d точек мониторинга предприятий (13 агрегатных + 16 отраслевых листов)", len(points))
		for _, p := range points {
			if err = batch.Append(mpSurveySource, p.Series, p.Date, p.Value); err != nil {
				return fmt.Errorf("append %s %s: %w", p.Series, p.Date.Format("2006-01-02"), err)
			}
		}
		return nil
	},
}}
