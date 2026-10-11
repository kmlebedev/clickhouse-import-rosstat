package wgc

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	// wgcSource — source рядов пакета в macro_series и series_catalog.
	wgcSource = "wgc"

	// etfStatName — имя импортёра: Name() (контракт расписания) и префикс логов и ошибок
	// хелперов, которые самого etfFlowsStat не видят.
	etfStatName = "wgc_etf_flows"

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

// errNoEtfFile — в каталоге нет ни одной выгрузки ETF_Flows*.xlsx. Это не ошибка импорта:
// до первого скачивания выгрузки (страницы goldhub защищены Cloudflare, файлы скачиваются
// вручную) пустой каталог нормален (урок dcf_engine), поэтому решение «warn + 0 строк»
// принимает вызывающий. Отдельный sentinel, а не пустая строка пути: findLatestFile уже
// отдаёт ("", nil), и «файла нет» не должно смешиваться с «каталог не прочитался».
var errNoEtfFile = errors.New("в каталоге нет выгрузки ETF")

// etfFlowsStat — импортёр wgc_etf_flows: канонический JSON API WGC (потоки и стоки/AUM)
// → 20 рядов etf_<метрика>_<регион> с source='wgc'; локальная выгрузка ETF_Flows_*.xlsx —
// фолбэк на случай недоступного API. dir — папка выгрузок; пустая означает defaultDataDir
// (прод), в тестах подставляется t.TempDir(). apiBase — база API с тем же смыслом: пустая
// означает боевой etfAPIBaseURL, в тестах — httptest-сервер.
type etfFlowsStat struct {
	dir     string
	apiBase string
}

func (s *etfFlowsStat) Name() string {
	return etfStatName
}

// Import пишет точки канонического канала — JSON API WGC (потоки и стоки/AUM); ошибка API
// уводит прогон на фолбэк, свежайшую выгрузку ETF_Flows_*.xlsx в dir. Ни API, ни файла —
// warn + 0 строк: до первого скачивания выгрузки это не поломка (урок dcf_engine), и
// соединение не трогается вовсе. Битый файл — ошибка (fail-fast, урок eia): тихий ноль
// оставил бы dagu зелёным, а ряды — без обновления.
func (s *etfFlowsStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	dir := s.dir
	if dir == "" {
		dir = defaultDataDir
	}

	points, fromAPI, err := etfLoadPoints(dir, s.apiBase, time.Now())
	if err != nil {
		return 0, err
	}

	// Ноль точек — это «источника нет вовсе»; причину уже назвал etfLoadPoints, а здесь
	// остаётся решение: писать нечего, и соединение не трогается. Пустой батч ушёл бы в
	// Send, а каталог описал бы ряды, которых в таблице нет (fail-fast, урок eia).
	if len(points) == 0 {
		log.Warnf("%s: 0 точек — импорт пропущен (0 строк)", etfStatName)
		return 0, nil
	}

	if fromAPI {
		// Guard устаревшего ответа CDN: после публикации кэш fscdn ещё отдаёт прошлый месяц,
		// и молчаливая перезапись свежего ряда старым осталась бы незамеченной. Это не ошибка:
		// перезапись тех же значений идемпотентна, а ложный отказ оставил бы ряды без
		// обновления. Ошибка самого диагностического запроса — тоже warn (не отказ импорта):
		// ценность задачи — батч, а вспомогательная выборка не повод терять обновление.
		// Решение — в etfCacheLooksStale: пустая таблица нормализована etfStoredMaxDate в
		// нулевую дату, поэтому отдельной ветки «а есть ли вообще строки» здесь нет — иначе
		// условие «сравнивать не с чем» дублировалось бы в двух местах.
		stored, gerr := etfStoredMaxDate(ctx, conn)
		if gerr != nil {
			log.Warnf("%s: сохранённый максимум дат не прочитан (%v) — guard устаревшего ответа CDN пропущен", etfStatName, gerr)
		} else if apiMax := etfPointsMaxDate(points); etfCacheLooksStale(apiMax, stored) {
			log.Warnf("%s: возможен устаревший ответ CDN: API %s < сохранённый %s", etfStatName,
				apiMax.Format("2006-01-02"), stored.Format("2006-01-02"))
		}
	}

	return writeEtfPoints(ctx, conn, points)
}

// etfLoadPoints выбирает ровно один источник на прогон: канонический JSON API WGC, а при его
// ошибке — свежайшую локальную выгрузку. Транспорты не смешиваются: смешение дало бы двойные
// точки на одних месяцах и разный состав рядов на других, а разбираться с этим пришлось бы по
// витрине. apiBase — шов для тестов с тем же смыслом, что dir: пустое значение означает боевой
// etfAPIBaseURL. «Ни API, ни файла» — (nil, false, nil): warn + 0 строк, а не поломка импорта.
func etfLoadPoints(dir, apiBase string, now time.Time) (points []seriesPoint, fromAPI bool, err error) {
	if apiBase == "" {
		apiBase = etfAPIBaseURL
	}

	points, err = etfPointsFromAPI(apiBase, now)
	if err == nil {
		return points, true, nil
	}
	// Причина отказа API идёт в текст: без неё деградация канала до файла (а с ней и
	// отставание рядов от публикаций WGC) осталась бы невидимой в логе.
	log.Warnf("%s: API WGC недоступно (%v) — пробую локальную выгрузку", etfStatName, err)

	points, err = etfPointsFromFile(dir)
	if errors.Is(err, errNoEtfFile) {
		log.Warnf("%s: в %s нет файла %s — импорт пропущен (0 строк)", etfStatName, dir, etfFileMask)
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return points, false, nil
}

// etfPointsFromFile разбирает свежайшую выгрузку ETF_Flows_*.xlsx каталога dir: выбор по mtime,
// а не по имени (месяц в имени прописью — Oct2026 лексикографически больше Nov2026, см.
// findLatestFile). Файла нет — errNoEtfFile: это не ошибка импорта, а сигнал вызывающему
// «warn + 0 строк». Ошибка разбора и пустой разбор называют путь: в wgc/data лежат выгрузки
// разных задач, и «что-то не открылось» неотличимо.
func etfPointsFromFile(dir string) ([]seriesPoint, error) {
	path, err := findLatestFile(dir, etfFileMask)
	if err != nil {
		return nil, fmt.Errorf("%s: поиск выгрузки в %s: %w", etfStatName, dir, err)
	}

	if path == "" {
		return nil, errNoEtfFile
	}

	f, err := excelize.OpenFile(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %s: %w", etfStatName, path, err)
	}
	defer func() { _ = f.Close() }()

	points, err := parseEtfWorkbook(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %s: %w", etfStatName, path, err)
	}

	// Пустой разбор — не «нулевой месяц», а сломанная выгрузка: пустой батч ушёл бы в Send,
	// а каталог описал бы ряды, которых в таблице нет (fail-fast, урок eia). Проверка стоит
	// до PrepareBatch и до любого обращения к соединению.
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: %s: выгрузка без точек — форма листов изменилась", etfStatName, path)
	}

	return points, nil
}

// writeEtfPoints пишет точки одним батчем. Порядок батча детерминирован: сортировка
// (series, date) делает повторный импорт воспроизводимым независимо от порядка обхода
// источника. Каталог — после успешного Send: описание ряда, которого в таблице нет, —
// обещание данных, которых агент не найдёт (прецедент dcf_mine_plans).
func writeEtfPoints(ctx context.Context, conn driver.Conn, points []seriesPoint) (count int64, err error) {
	sort.Slice(points, func(i, j int) bool {
		if points[i].Series != points[j].Series {
			return points[i].Series < points[j].Series
		}
		return points[i].Date.Before(points[j].Date)
	})

	if err = conn.Exec(ctx, macroSeriesDdl); err != nil {
		return 0, fmt.Errorf("%s: %s: %w", etfStatName, macroSeriesTable, err)
	}

	batch, err := conn.PrepareBatch(ctx, macroSeriesInsert)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", etfStatName, err)
	}

	series := make(map[string]bool, len(wgcEtfMetrics)*len(wgcEtfRegions))

	for _, p := range points {
		if err = batch.Append(wgcSource, p.Series, p.Date, p.Value); err != nil {
			// Abort не даёт соединению остаться с открытым батчем (следующий шаг
			// DAG-а получил бы «уже есть незакрытый батч»); сбой Abort логируем,
			// а не глотаем, — возвращаемой ошибкой остаётся первопричина Append
			// (прецедент dcf/mine_plans_import.go).
			if aerr := batch.Abort(); aerr != nil {
				log.Warnf("%s: не удалось закрыть батч после ошибки append: %v", etfStatName, aerr)
			}

			return 0, fmt.Errorf("%s: append %s %s: %w", etfStatName, p.Series, p.Date.Format("2006-01-02"), err)
		}

		series[p.Series] = true
		count++
	}

	if err = batch.Send(); err != nil {
		return 0, fmt.Errorf("%s: %w", etfStatName, err)
	}

	if err = util.UpsertSeriesCatalog(ctx, conn, wgcEtfCatalog()); err != nil {
		return count, fmt.Errorf("%s: каталог рядов: %w", etfStatName, err)
	}

	log.Infof("Imported %d rows of %s (wgc ETF flows, %d series)", count, etfStatName, len(series))

	return count, nil
}

// etfStoredMaxDate — максимальная дата рядов ETF в macro_series (source='wgc'): с ней
// etfCacheLooksStale сравнивает максимум ответа API. Пустой таблице соответствует нулевое
// время: max по пустой выборке отдаёт «нулевую» дату (1970-01-01), и она нормализуется здесь,
// чтобы решение «данных для сравнения нет» жило в одном месте — в etfCacheLooksStale. FINAL —
// по определению ReplacingMergeTree: без него запрос читал бы физический хвост таблицы, а
// не её состояние после дедупликации.
func etfStoredMaxDate(ctx context.Context, conn driver.Conn) (time.Time, error) {
	row := conn.QueryRow(ctx, "SELECT max(date) FROM "+macroSeriesTable+" FINAL WHERE source = '"+wgcSource+"' AND series LIKE 'etf\\_%'")
	var maxDate time.Time
	if err := row.Scan(&maxDate); err != nil {
		return time.Time{}, err
	}
	if maxDate.Year() <= 1970 {
		return time.Time{}, nil
	}
	// Дата приводится к UTC: clickhouse-go отдаёт Date с таймзоной сессии, а даты точек — UTC,
	// и сравнение с максимумом API иначе зависело бы от настроек соединения.
	return time.Date(maxDate.Year(), maxDate.Month(), maxDate.Day(), 0, 0, 0, 0, time.UTC), nil
}

// etfPointsMaxDate — максимальная дата точек: с ней guard сравнивает сохранённый максимум,
// чтобы заметить устаревший ответ CDN, отданный из кэша после публикации.
func etfPointsMaxDate(points []seriesPoint) time.Time {
	var maxDate time.Time
	for _, p := range points {
		if p.Date.After(maxDate) {
			maxDate = p.Date
		}
	}
	return maxDate
}

// etfCacheLooksStale — решение guard-а: ответ API отстаёт от того, что уже лежит в таблице.
// Пустой storedMax (ряда ещё нет — etfStoredMaxDate нормализует пустую таблицу в нулевую дату)
// и равенство дат — false: сравнивать не с чем, а повторный ответ того же месяца «устареванием»
// не является, и warn на каждом прогоне обесценил бы сигнал. Вынесено из Import чистой функцией,
// чтобы решение проверялось табличным тестом без соединения: сама выборка max(date) тестом не
// покрывается (соединения в тестах нет), а «устарел или нет» — покрывается.
func etfCacheLooksStale(apiMax, storedMax time.Time) bool {
	if storedMax.IsZero() {
		return false
	}
	return apiMax.Before(storedMax)
}

func init() {
	chimport.Stats = append(chimport.Stats, &etfFlowsStat{})
}
