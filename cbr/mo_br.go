package cbr

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	// moBrPageUrl — страница макроэкономического опроса ЦБ; имя файла волны в коде
	// не фиксируется: ссылка ищется в Import() (§3.1, ARCHITECTURE.md §8.6).
	moBrPageUrl = "https://www.cbr.ru/statistics/ddkp/mo_br/"
	// moBrLinkSel — селектор ссылки на полную книгу волны (full.xlsx).
	moBrLinkSel = `a[href$="full.xlsx"]`
	// moBrName — имя импортёра в CLICKHOUSE_IMPORT_STAT и в id шага dagu/cbr.yaml;
	// moBrTable — общая macro_series (source='cbr_mo_br', см. moBrSource), поэтому
	// имя не совпадает с таблицей (как у cbr_mp_survey).
	moBrName  = "cbr_mo_br"
	moBrTable = "macro_series"
)

// moBrCountPoints — количественный чек известной версии (спека §3.4, п. 2): число
// точек своего source в macro_series. Литерал, как и таблицы в реестре: запрос —
// часть утверждения «эти точки мои», и незамеченная смена source увела бы чек к
// чужим рядам общей таблицы. FINAL — как в витрине v_cbr_mo_br.
const moBrCountPoints = `SELECT count() FROM macro_series FINAL WHERE source = 'cbr_mo_br'`

// moBrDdl — своя копия DDL macro_series по прецеденту пакета (у cbr_mp_survey —
// mpSurveyDdl): общая константа связала бы импортёры, которые делят таблицу, но не
// код. Комментарий source дополнен 'cbr_mo_br'. Исполняется через
// fmt.Sprintf(sql, TableName) в util.ClickHouseImport.Import, поэтому %s обязателен.
const moBrDdl = `CREATE TABLE IF NOT EXISTS %s (
    source LowCardinality(String),   -- 'fred','bls','bea','eia','manual','webbridge','imf','wgc','bold','cbr_survey','cbr_mo_br'
    series LowCardinality(String),   -- 'DFII10','distillate_stocks',...
    date Date32,                     -- Date32, т.к. Date не покрывает даты до 1970 (CPIAUCSL с 1947, FEDFUNDS с 1954)
    value Float64
) ENGINE = ReplacingMergeTree ORDER BY (source, series, date);`

type moBrStat struct {
	util.ClickHouseImport
}

func (s *moBrStat) Name() string { return moBrName }

// Import идёт по спеке §3.4: ссылка на полную книгу ищется на странице (имя файла
// меняется с каждой волной), затем HEAD-префлайт отвечает, знакома ли версия. Знакома
// и число точек в macro_series совпадает с записанным — это no-op ежедневного прогона
// (волны выходят 8 раз в год). Незнакома или точек не столько — полный импорт;
// версия записывается последней — после успешного Send, каталога и витрины
// (строка версии утверждает «эти точки записаны и опубликованы»).
func (s *moBrStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	page, err := getPage(moBrPageUrl)
	if err != nil {
		return count, fmt.Errorf("%s: %s: %w", moBrName, moBrPageUrl, err)
	}
	href, err := findLink(page, moBrLinkSel)
	if err != nil {
		return count, fmt.Errorf("%s: не найдена ссылка на xlsx на %s: %w", moBrName, moBrPageUrl, err)
	}
	url := cbrUrl + href
	name := path.Base(url)

	// HEAD-префлайт. Недоступный HEAD — деградация, а не сбой: источник мог не
	// ответить на HEAD, не отдав при этом ничего нового. Тогда импорт полный, но
	// версия не записывается (fp пуст) — иначе следующий прогон счёл бы файл уже
	// импортированным и оставил бы данные устаревшими (спека §3.4, п. 3).
	fp, fpErr := util.HeadFingerprint(url)
	if fpErr != nil {
		log.Warnf("%s: HEAD %s недоступен (%v) — полный импорт без записи версии", moBrName, url, fpErr)
		fp = ""
	}

	if fp != "" {
		if err = util.EnsurePublications(ctx, conn); err != nil {
			return count, err
		}
		points, known, knownErr := util.PublicationKnown(ctx, conn, moBrName, name, fp)
		if knownErr != nil {
			// Сбой SELECT'а версии не должен ронять ежедневный прогон: падаем в
			// полный импорт (он идемпотентен на ReplacingMergeTree), а версия потом
			// перезаписывается. Новая волна важнее диагностики старой записи.
			log.Warnf("%s: проверка версии %s в %s не удалась (%v) — полный импорт", moBrName, fp, util.PublicationsTable, knownErr)
			known = false
		}
		if known {
			var dbPoints uint64
			if err = conn.QueryRow(ctx, moBrCountPoints).Scan(&dbPoints); err != nil {
				return count, fmt.Errorf("%s: count точек в %s: %w", moBrName, moBrTable, err)
			}
			if dbPoints == points {
				log.Infof("%s: no-op, версия %s уже импортирована, %d точек на месте", moBrName, fp, points)
				return int64(points), nil
			}
			// Версия в source_publications есть, а точек в таблице не столько (ручная
			// чистка, недоехавший батч) — self-heal полным импортом, а не тихий no-op.
			log.Warnf("%s: версия %s известна, но в %s %d точек вместо %d — полный импорт", moBrName, fp, moBrTable, dbPoints, points)
		}
	}

	// Полный путь — через локальную копию встроенной структуры: меты разбора
	// нужны снаружи (каталог), а мутировать общую структуру импортёра нельзя —
	// DataUrl и ImportFunc зависят от волны.
	local := s.ClickHouseImport
	local.DataUrl = url
	var meta []util.SeriesMeta
	local.ImportFunc = func(xlsx *excelize.File, batch driver.Batch) error {
		points, metas, err := parseMoBr(xlsx)
		if err != nil {
			return err
		}
		meta = metas
		log.Infof("%s: собрано %d точек", moBrName, len(points))
		for _, p := range points {
			if err = batch.Append(moBrSource, p.Series, p.Date, p.Value); err != nil {
				return fmt.Errorf("append %s %s: %w", p.Series, p.Date.Format("2006-01-02"), err)
			}
		}
		return nil
	}
	count, err = local.Import(ctx, conn) // DDL → GetXlsx → разбор+батч → count=batch.Rows() → Send
	if err != nil {
		return count, err
	}
	// Каталог и витрина — до строки версии: версия пишется последней и утверждает
	// «эти точки записаны и опубликованы». Сбой любого пост-шага оставляет её
	// незаписанной, и следующий прогон повторяет полный путь (идемпотентный), а не
	// уходит в no-op с отсутствующим у агента каталогом или витриной до следующей
	// волны (~6 недель).
	if err = util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
		return count, err
	}
	if _, err = util.CreateView(ctx, conn, vCbrMoBrView); err != nil {
		return count, err
	}
	if fp != "" {
		if err = util.SavePublication(ctx, conn, util.Publication{
			Source:      moBrName,
			Name:        name,
			Fingerprint: fp,
			Url:         url,
			FirstSeen:   time.Now(),
			Points:      uint64(count),
		}); err != nil {
			return count, err
		}
	}
	log.Infof("Imported %d rows of %s", count, moBrName)
	return count, nil
}

// vCbrMoBrView — витрина рядов макроэкономического опроса ЦБ (source='cbr_mo_br',
// импортёр cbr_mo_br); общая витрина cbrMacroView и v_cbr_survey её не покрывают.
// Комментарии — по спеке §3.5: дата — волна опроса, значение — прогноз, а не факт.
var vCbrMoBrView = util.View{
	Name:   "v_cbr_mo_br",
	Tables: []string{"macro_series"},
	Select: `SELECT source, series, date, value FROM macro_series FINAL WHERE source = 'cbr_mo_br'`,
	Comment: "Макроэкономический опрос Банка России (mo_br): прогнозы аналитиков по 17 показателям (ИПЦ, ВВП, ключевая ставка, курс USD/RUB, нефть и др.), 8 волн в год. " +
		"Дата — волна опроса (1-е число месяца волны); волна публикуется на 2–6 дней позже, точная дата доступности версии файла — во внутренней source_publications (агенту не выдана). " +
		"Значения — прогнозы аналитиков, а не факты: по каждому показателю и прогнозному году лежат медиана, квартильный интервал (p10/p90), диапазон (min/max) и число ответов (answers). " +
		"Перед использованием читать v_series_catalog по (source='cbr_mo_br', series): единицы и смысл статистик различаются между рядами",
	Columns: map[string]string{
		"source": "Всегда cbr_mo_br",
		"series": "Код ряда: <показатель>_<год прогноза>_<статистика>, у показателей без прогнозного года — <показатель>_<статистика>; статистики — median, p10, p90, min, max (прогнозы) и answers (число ответов). Описания — в v_series_catalog (source='cbr_mo_br')",
		"date":   "Волна опроса: 1-е число месяца волны (Date32). Публикация волны — через 2–6 дней после этой даты; фактические даты доступности версий файла ведёт служебная source_publications (агенту не выдана)",
		"value":  "У median/p10/p90/min/max — прогноз аналитиков в единицах показателя (медиана, границы центральной тенденции и всего диапазона ответов), у answers — число ответов участников по показателю и году в этой волне (не прогноз); см. unit и description в v_series_catalog",
	},
}

// moBrImporter — макроэкономический опрос ЦБ: Name() = cbr_mo_br, таблица — общая
// macro_series (source='cbr_mo_br'). DataUrl и ImportFunc вычисляются в Import()
// (ссылка на файл волны меняется), регистрация — реестром cbr/importers.go.
var moBrImporter = &moBrStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   moBrTable,
	CreateTable: []string{moBrDdl},
}}
