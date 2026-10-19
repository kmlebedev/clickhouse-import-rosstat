package rosstat

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// rosstatOkved2Name — имя импортёра в CLICKHOUSE_IMPORT_STAT и id шага dagu/rosstat.yaml;
	// rosstatOkved2Table — его собственная таблица: имя и таблица совпадают.
	rosstatOkved2Name  = "rosstat_okved2"
	rosstatOkved2Table = "rosstat_okved2"
	// rosstatOkved2MetaURL — список выпусков открытых данных Росстата: в meta.csv лежит
	// актуальный data-*.csv, имя которого меняется ежемесячно. Ссылка на конкретный выпуск
	// в коде не фиксируется.
	rosstatOkved2MetaURL = "https://www.rosstat.gov.ru/opendata/7708234640-okvedva/meta.csv"
	// rosstatOkved2CountPoints — количественный чек версии: число строк классификатора.
	rosstatOkved2CountPoints = "SELECT count() FROM rosstat_okved2 FINAL"
)

const rosstatOkved2Ddl = `CREATE TABLE IF NOT EXISTS %s (
    section        LowCardinality(String),  -- 'A'…'U'
    code           LowCardinality(String),  -- '01', '01.1', '01.11', …; у раздела пусто
    name           String,
    level          UInt8,                   -- 1 раздел, 2 класс, 3 подкласс, 4 группа, 5 подгруппа, 6 вид
    parent_code    LowCardinality(String),
    source_version LowCardinality(String),  -- basename data-*.csv
    loaded_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (section, code);`

type RosstatOkved2Stat struct {
	util.ClickHouseImport
}

func (s *RosstatOkved2Stat) Name() string { return rosstatOkved2Name }

// Import — шаблон плюс явный отказ от restore: DataUrl вычисляется из meta.csv, а в режиме
// восстановления BeforeImport не вызывается и URL пуст. Ошибка до сети и до DDL называет
// причину, а не оставляет пользователя с невнятным «no CrawFunc…».
func (s *RosstatOkved2Stat) Import(ctx context.Context, conn driver.Conn) (int64, error) {
	if util.ArchiveRestoreMode() {
		return 0, fmt.Errorf("%s: restore не поддержан: DataUrl вычисляется из %s", rosstatOkved2Name, rosstatOkved2MetaURL)
	}
	return s.ClickHouseImport.Import(ctx, conn)
}

// beforeImport читает meta.csv и подставляет ссылку на самый свежий data-*.csv. Сеть — только
// внутри Import(): init() не делает запросов.
func (s *RosstatOkved2Stat) beforeImport(_ context.Context, _ driver.Conn) error {
	body, err := util.GetFileBytes(rosstatOkved2MetaURL)
	if err != nil {
		return fmt.Errorf("%s: meta %s: %w", rosstatOkved2Name, rosstatOkved2MetaURL, err)
	}
	url, err := parseOkved2Meta(body)
	if err != nil {
		return fmt.Errorf("%s: meta %s: %w", rosstatOkved2Name, rosstatOkved2MetaURL, err)
	}
	s.DataUrl = url
	return nil
}

func (s *RosstatOkved2Stat) importBody(body []byte, batch driver.Batch) error {
	rows, err := parseOkved2(body)
	if err != nil {
		return fmt.Errorf("%s: %w", rosstatOkved2Name, err)
	}
	version := path.Base(s.DataUrl)
	// loaded_at — седьмая колонка DDL; PrepareBatch без списка колонок ждёт и её, хотя
	// у колонки есть DEFAULT now(). Одно значение на батч: версия ReplacingMergeTree
	// фиксирует момент записи, а не каждую строку.
	loadedAt := time.Now()
	for i, row := range rows {
		if err := batch.Append(row.Section, row.Code, row.Name, row.Level, row.ParentCode, version, loadedAt); err != nil {
			return fmt.Errorf("%s: append строка %d (%s): %w", rosstatOkved2Name, i+1, row.Code, err)
		}
	}
	log.Infof("%s: разобрано %d строк классификатора из %s", rosstatOkved2Name, len(rows), version)
	return nil
}

func (s *RosstatOkved2Stat) afterImport(ctx context.Context, conn driver.Conn) error {
	_, err := util.CreateView(ctx, conn, rosstatOkved2View)
	return err
}

func newRosstatOkved2Importer() *RosstatOkved2Stat {
	s := &RosstatOkved2Stat{}
	s.ClickHouseImport = util.ClickHouseImport{
		TableName:              rosstatOkved2Table,
		CreateTable:            []string{rosstatOkved2Ddl},
		PublicationSource:      rosstatOkved2Name,
		PublicationCountPoints: rosstatOkved2CountPoints,
		BeforeImport:           s.beforeImport,
		BodyFunc:               s.importBody,
		AfterImport:            s.afterImport,
	}
	return s
}

var rosstatOkved2Importer = newRosstatOkved2Importer()

var rosstatOkved2View = util.View{
	Name:   "v_rosstat_okved2",
	Tables: []string{rosstatOkved2Table},
	Select: `SELECT section, code, name, level, parent_code, source_version
FROM rosstat_okved2 FINAL`,
	Comment: "Публичный классификатор ОКВЭД2 Росстата (открытые данные 7708234640-okvedva): " +
		"разделы, классы, подклассы, группы, подгруппы и виды деятельности с названиями. " +
		"Используется для проверки стандартности кодов CB_COMP (87/87) и как словарь для " +
		"публичного кроссворда cbr_mp_bsu_map. Значений мониторинга предприятий в источнике нет; " +
		"обновляется ежемесячно, версия выпуска — source_version.",
	Columns: map[string]string{
		"section":        "Буква раздела ОКВЭД2 A…U; у строки раздела code пуст, level=1",
		"code":           "Код ОКВЭД2: '01', '01.1', '01.11', '01.11.1', '01.11.11'…; у строки раздела пусто",
		"name":           "Официальное название раздела/класса/вида деятельности",
		"level":          "Уровень классификатора: 1 раздел, 2 класс, 3 подкласс, 4 группа, 5 подгруппа, 6 вид",
		"parent_code":    "Код родителя по уровню: у класса пусто (родитель — раздел), у подкласса — класс, у группы — подкласс, …",
		"source_version": "basename выпуска data-*.csv, из которого загружены строки",
	},
}
