package wgc

import (
	"context"
	"fmt"
	"sort"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	// wgcSource — source рядов пакета в macro_series и series_catalog.
	wgcSource = "wgc"

	// etfFileMask — маска выгрузки ETF в папке данных: WGC публикует её как
	// ETF_Flows_<YYYY-MM-DD>_<HHMM>.xlsx.
	etfFileMask = "ETF_Flows*.xlsx"

	// macroSeriesTable — таблица длинных рядов пакета (общая с wgc_cb_reserves,
	// Task 4). Схема — как у остальных импортёров macro_series, поэтому CREATE
	// IF NOT EXISTS безвреден, когда таблица уже создана другим импортёром.
	macroSeriesTable = "macro_series"
	macroSeriesDdl   = `CREATE TABLE IF NOT EXISTS ` + macroSeriesTable + ` (
			  source LowCardinality(String)
			, series LowCardinality(String)
			, date Date32
			, value Float64
		) ENGINE = ReplacingMergeTree ORDER BY (source, series, date);
	`
	macroSeriesInsert = "INSERT INTO " + macroSeriesTable + " (source, series, date, value)"
)

// etfFlowsStat — импортёр wgc_etf_flows: свежайшая выгрузка ETF_Flows_*.xlsx →
// 20 рядов etf_<метрика>_<регион> с source='wgc'. dir — папка выгрузок; пустая
// означает defaultDataDir (прод), в тестах подставляется t.TempDir().
type etfFlowsStat struct {
	dir string
}

func (s *etfFlowsStat) Name() string {
	return "wgc_etf_flows"
}

// Import разбирает выгрузку и пишет точки одним батчем. Файла нет — warn + 0
// строк: до первого скачивания выгрузки это не поломка (урок dcf_engine), и
// соединение не трогается вовсе. Битый файл — ошибка (fail-fast, урок eia):
// тихий ноль оставил бы dagu зелёным, а ряды — без обновления.
func (s *etfFlowsStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	dir := s.dir
	if dir == "" {
		dir = defaultDataDir
	}

	path, err := findLatestFile(dir, etfFileMask)
	if err != nil {
		return 0, fmt.Errorf("%s: поиск выгрузки в %s: %w", s.Name(), dir, err)
	}

	if path == "" {
		log.Warnf("%s: в %s нет файла %s — импорт пропущен (0 строк)", s.Name(), dir, etfFileMask)
		return 0, nil
	}

	f, err := excelize.OpenFile(path)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", s.Name(), path, err)
	}
	defer func() { _ = f.Close() }()

	points, err := parseEtfWorkbook(f)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", s.Name(), path, err)
	}

	// Пустой разбор — не «нулевой месяц», а сломанная выгрузка: пустой батч ушёл
	// бы в Send, а каталог описал бы ряды, которых в таблице нет (fail-fast, урок
	// eia). Проверка стоит до PrepareBatch и до любого обращения к соединению.
	if len(points) == 0 {
		return 0, fmt.Errorf("%s: %s: выгрузка без точек — форма листов изменилась", s.Name(), path)
	}

	// Порядок батча детерминирован: сортировка (series, date) делает повторный
	// импорт воспроизводимым независимо от порядка обхода листов парсером.
	sort.Slice(points, func(i, j int) bool {
		if points[i].Series != points[j].Series {
			return points[i].Series < points[j].Series
		}
		return points[i].Date.Before(points[j].Date)
	})

	if err = conn.Exec(ctx, macroSeriesDdl); err != nil {
		return 0, fmt.Errorf("%s: %s: %w", s.Name(), macroSeriesTable, err)
	}

	batch, err := conn.PrepareBatch(ctx, macroSeriesInsert)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", s.Name(), err)
	}

	series := make(map[string]bool, len(wgcEtfMetrics)*len(wgcEtfRegions))

	for _, p := range points {
		if err = batch.Append(wgcSource, p.Series, p.Date, p.Value); err != nil {
			// Abort не даёт соединению остаться с открытым батчем (следующий шаг
			// DAG-а получил бы «уже есть незакрытый батч»); сбой Abort логируем,
			// а не глотаем, — возвращаемой ошибкой остаётся первопричина Append
			// (прецедент dcf/mine_plans_import.go).
			if aerr := batch.Abort(); aerr != nil {
				log.Warnf("%s: не удалось закрыть батч после ошибки append: %v", s.Name(), aerr)
			}

			return 0, fmt.Errorf("%s: append %s %s: %w", s.Name(), p.Series, p.Date.Format("2006-01-02"), err)
		}

		series[p.Series] = true
		count++
	}

	if err = batch.Send(); err != nil {
		return 0, fmt.Errorf("%s: %w", s.Name(), err)
	}

	// Каталог — после успешного Send: описание ряда, которого в таблице нет, —
	// обещание данных, которых агент не найдёт (прецедент dcf_mine_plans).
	if err = util.UpsertSeriesCatalog(ctx, conn, wgcEtfCatalog()); err != nil {
		return count, fmt.Errorf("%s: каталог рядов: %w", s.Name(), err)
	}

	log.Infof("Imported %d rows of %s (wgc ETF flows, %d series)", count, s.Name(), len(series))

	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &etfFlowsStat{})
}
