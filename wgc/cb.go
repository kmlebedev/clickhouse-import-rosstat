package wgc

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	// cbChangesMask, cbHoldingsMask — маски двух выгрузок ЦБ в папке данных: имена WGC
	// содержат месяц прописью (Oct2026 лексикографически больше Nov2026), поэтому файл
	// выбирается по mtime (findLatestFile), а маска отделяет эти выгрузки от ETF.
	cbChangesMask  = "Changes_latest_as_of_*.xlsx"
	cbHoldingsMask = "World_official_gold_holdings_as_of_*.xlsx"
)

// cbReservesStat — импортёр wgc_cb_reserves: свежайшие выгрузки ЦБ в dir → 14 рядов
// cb_gold_<метрика>_<iso3> с source='wgc'. dir — папка выгрузок; пустая означает
// defaultDataDir (прод), в тестах подставляется t.TempDir().
type cbReservesStat struct {
	dir string
}

func (s *cbReservesStat) Name() string {
	return "wgc_cb_reserves"
}

// selectCbFiles возвращает пути свежайших выгрузок ЦБ в dir. Оба пути опциональны:
// "" — выгрузки нет, и решение «warn + пропустить эту часть» остаётся Import: файлы
// скачиваются вручную и появляются в папке по отдельности.
func selectCbFiles(dir string) (changesPath, holdingsPath string, err error) {
	if changesPath, err = findLatestFile(dir, cbChangesMask); err != nil {
		return "", "", fmt.Errorf("поиск %s: %w", cbChangesMask, err)
	}
	if holdingsPath, err = findLatestFile(dir, cbHoldingsMask); err != nil {
		return "", "", fmt.Errorf("поиск %s: %w", cbHoldingsMask, err)
	}
	return changesPath, holdingsPath, nil
}

// Import разбирает выгрузки и пишет точки одним батчем. Ни одной выгрузки нет — warn + 0
// строк: до первого скачивания это не поломка (урок dcf_engine), и соединение не
// трогается вовсе. Частичный набор — тоже норма (warn): нет changes — только уровни, нет
// holdings — только дельты. Битый файл или ноль точек — ошибка (fail-fast, урок eia):
// тихий ноль оставил бы dagu зелёным, а ряды — без обновления.
func (s *cbReservesStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	dir := s.dir
	if dir == "" {
		dir = defaultDataDir
	}

	changesPath, holdingsPath, err := selectCbFiles(dir)
	if err != nil {
		return 0, fmt.Errorf("%s: поиск выгрузок в %s: %w", s.Name(), dir, err)
	}

	if changesPath == "" && holdingsPath == "" {
		log.Warnf("%s: в %s нет ни %s, ни %s — импорт пропущен (0 строк)", s.Name(), dir, cbChangesMask, cbHoldingsMask)
		return 0, nil
	}

	var (
		points   []seriesPoint
		parsed   []string
		changes  int
		holdings int
	)

	if changesPath == "" {
		log.Warnf("%s: в %s нет файла %s — дельты не записаны, только уровни", s.Name(), dir, cbChangesMask)
	} else {
		changesPoints, err := cbParseWorkbook(s.Name(), changesPath, parseCbChanges)
		if err != nil {
			return 0, err
		}
		changes = len(changesPoints)
		points = append(points, changesPoints...)
		parsed = append(parsed, changesPath)
	}

	if holdingsPath == "" {
		log.Warnf("%s: в %s нет файла %s — уровни не записаны, только дельты", s.Name(), dir, cbHoldingsMask)
	} else {
		holdingsPoints, err := cbParseWorkbook(s.Name(), holdingsPath, parseCbHoldings)
		if err != nil {
			return 0, err
		}
		holdings = len(holdingsPoints)
		points = append(points, holdingsPoints...)
		parsed = append(parsed, holdingsPath)
	}

	// Ноль точек при разобранных файлах — не «пустой месяц», а сломанная выгрузка:
	// пустой батч ушёл бы в Send, а каталог описал бы ряды, которых в таблице нет
	// (fail-fast, урок eia). Проверка стоит до PrepareBatch и до любого обращения к
	// соединению.
	if len(points) == 0 {
		return 0, fmt.Errorf("%s: выгрузки (%s) без точек — дельты и уровни пусты", s.Name(), strings.Join(parsed, ", "))
	}

	// Порядок батча детерминирован: сортировка (series, date) делает повторный импорт
	// воспроизводимым независимо от порядка разбора файлов.
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

	for _, p := range points {
		if err = batch.Append(wgcSource, p.Series, p.Date, p.Value); err != nil {
			// Abort не даёт соединению остаться с открытым батчем (следующий шаг DAG-а
			// получил бы «уже есть незакрытый батч»); сбой Abort логируем, а не глотаем,
			// — возвращаемой ошибкой остаётся первопричина Append (прецедент
			// dcf/mine_plans_import.go).
			if aerr := batch.Abort(); aerr != nil {
				log.Warnf("%s: не удалось закрыть батч после ошибки append: %v", s.Name(), aerr)
			}

			return 0, fmt.Errorf("%s: append %s %s: %w", s.Name(), p.Series, p.Date.Format("2006-01-02"), err)
		}
		count++
	}

	if err = batch.Send(); err != nil {
		return 0, fmt.Errorf("%s: %w", s.Name(), err)
	}

	// Каталог — после успешного Send и только по метрикам с точками: описание ряда,
	// которого в таблице нет, — обещание данных, которых агент не найдёт (прецедент
	// dcf_mine_plans). На первом запуске выгрузки могут прийти по отдельности.
	if err = util.UpsertSeriesCatalog(ctx, conn, cbCatalogForPoints(points)); err != nil {
		return count, fmt.Errorf("%s: каталог рядов: %w", s.Name(), err)
	}

	log.Infof("Imported %d rows of %s (wgc CB reserves: changes %d / holdings %d)", count, s.Name(), changes, holdings)

	return count, nil
}

// cbParseWorkbook открывает выгрузку path и разбирает её парсером parse. Файл закрывается
// сразу после разбора: точки на выходе — скаляры, файл им больше не нужен. Ошибка
// называет файл: в wgc/data лежат выгрузки разных задач, и «что-то не открылось»
// неотличимо.
func cbParseWorkbook(name, path string, parse func(*excelize.File) ([]seriesPoint, error)) ([]seriesPoint, error) {
	f, err := excelize.OpenFile(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %s: %w", name, path, err)
	}
	defer func() { _ = f.Close() }()

	points, err := parse(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %s: %w", name, path, err)
	}
	return points, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &cbReservesStat{})
}
