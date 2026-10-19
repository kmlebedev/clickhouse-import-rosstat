package cbr

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// cbrMpBsuMapName — имя импортёра в CLICKHOUSE_IMPORT_STAT и id шага dagu/cbr.yaml;
	// таблица и имя совпадают.
	cbrMpBsuMapName  = "cbr_mp_bsu_map"
	cbrMpBsuMapTable = "cbr_mp_bsu_map"
	// cbrMpBsuMapSource — публичный источник списка 87 кодов: архив авторов CB_COMP.
	cbrMpBsuMapSource = "figshare:10.6084/m9.figshare.27990260.v1/model.xlsx#1.0.4.2"
)

const cbrMpBsuMapDdl = `CREATE TABLE IF NOT EXISTS %s (
    code           LowCardinality(String),
    industry_sheet LowCardinality(String),
    industry_slug  LowCardinality(String),
    basis          String,
    confidence     LowCardinality(String),  -- rule|manual|fallback
    source         LowCardinality(String),
    loaded_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY code;`

// bsuIndustry — отраслевой лист публичной книги, выбранный для кода, и обоснование выбора.
type bsuIndustry struct {
	Sheet      string
	Slug       string
	Basis      string
	Confidence string // rule|manual|fallback
}

// bsuMapRow — строка таблицы кроссворда.
type bsuMapRow struct {
	Code          string
	IndustrySheet string
	IndustrySlug  string
	Basis         string
	Confidence    string
	Source        string
}

// bsuConsumerCodes — производственные коды потребительских товаров. Разбиение на
// потребительские/инвестиционные/промежуточные не выводится из ОКВЭД2 однозначно, поэтому
// наборы зафиксированы явно и помечены confidence='manual'; спорные коды названы в
// research-отчёте.
var bsuConsumerCodes = map[string]bool{
	"10.1": true, "10.20": true, "10.3": true, "10.4": true, "10.5": true,
	"10.61": true, "10.7": true, "10.81": true, "10.82": true,
	"11.0": true, "11.07": true,
	"13.20": true, "14": true, "14.1": true, "15.20": true,
	"18.1": true, "20.4": true, "21.20": true,
	"31.0": true, "32.1": true, "32.99": true,
}

var bsuInvestmentCodes = map[string]bool{
	"29.10": true,
}

var bsuIntermediateCodes = map[string]bool{
	"10.9": true, "17.2": true, "19.20": true, "23": true,
}

// publicIndustryForCode — детерминированный публичный кроссворд ОКВЭД2 → лист
// mp_survey_data.xlsx. Разделы A/B/D/E/F/H и классы 45/46/47 и 55–96 берутся по префиксу;
// обработка (10–33) — по явным наборам выше. Возвращает ошибку, если код вне кроссворда:
// молчаливый fallback скрыл бы новый код в наборе.
func publicIndustryForCode(code string) (bsuIndustry, error) {
	code = strings.TrimSpace(code)
	top, err := okved2TopClass(code)
	if err != nil {
		return bsuIndustry{}, err
	}
	switch {
	case top >= 1 && top <= 3:
		return bsuSheet("agriculture", "OKVED2 section A (01–03)", "rule")
	case top >= 5 && top <= 9:
		return bsuSheet("mining", "OKVED2 section B (05–09)", "rule")
	case top >= 10 && top <= 33:
		switch {
		case bsuConsumerCodes[code]:
			return bsuSheet("manufacturing_consumer",
				fmt.Sprintf("OKVED2 code %s; consumer goods (manual public crosswalk)", code), "manual")
		case bsuInvestmentCodes[code]:
			return bsuSheet("manufacturing_investment",
				fmt.Sprintf("OKVED2 code %s; investment goods (manual public crosswalk)", code), "manual")
		case bsuIntermediateCodes[code]:
			return bsuSheet("manufacturing_intermediate",
				fmt.Sprintf("OKVED2 code %s; intermediate goods (manual public crosswalk)", code), "manual")
		default:
			return bsuSheet("manufacturing",
				fmt.Sprintf("OKVED2 class %02d; manufacturing total fallback", top), "fallback")
		}
	case top == 35:
		return bsuSheet("electricity", "OKVED2 class 35 (electricity, gas, steam)", "rule")
	case top >= 36 && top <= 39:
		return bsuSheet("water", "OKVED2 section E (36–39)", "rule")
	case top >= 41 && top <= 43:
		return bsuSheet("construction", "OKVED2 section F (41–43)", "rule")
	case top == 45:
		return bsuSheet("trade_auto", "OKVED2 class 45 (motor vehicle trade)", "rule")
	case top == 46:
		return bsuSheet("trade_wholesale", "OKVED2 class 46 (wholesale trade)", "rule")
	case top == 47:
		return bsuSheet("trade_retail", "OKVED2 class 47 (retail trade)", "rule")
	case top >= 49 && top <= 53:
		return bsuSheet("transport", "OKVED2 section H (49–53)", "rule")
	case top >= 55 && top <= 96:
		return bsuSheet("services", "OKVED2 section I–U (55–96)", "rule")
	default:
		return bsuIndustry{}, fmt.Errorf("код %q: класс %02d вне публичного кроссворда", code, top)
	}
}

// okved2TopClass — первые две цифры кода: '47.2' → 47, '05' → 5.
func okved2TopClass(code string) (int, error) {
	if len(code) < 2 {
		return 0, fmt.Errorf("код %q короче класса ОКВЭД2", code)
	}
	top, err := strconv.Atoi(code[:2])
	if err != nil {
		return 0, fmt.Errorf("код %q: класс %q не число: %w", code, code[:2], err)
	}
	return top, nil
}

// bsuSheet ищет лист по slug в единственном реестре листов cbr_mp_survey: имя листа и slug
// не дублируются, а расходятся с книгой только через mpSurveyIndustrySheets.
func bsuSheet(slug, basis, confidence string) (bsuIndustry, error) {
	for _, sheet := range mpSurveyIndustrySheets {
		if sheet.Slug == slug {
			return bsuIndustry{Sheet: sheet.Sheet, Slug: sheet.Slug, Basis: basis, Confidence: confidence}, nil
		}
	}
	return bsuIndustry{}, fmt.Errorf("slug %q отсутствует в mpSurveyIndustrySheets", slug)
}

// bsuMapRows собирает 87 строк кроссворда и проверяет дубли. Длина списка — контракт
// публичного архива (87), но жёсткая проверка 87 здесь не нужна: тест и live-приёмка
// сверяют список с классификатором, а изменение набора — отдельное решение.
func bsuMapRows() ([]bsuMapRow, error) {
	seen := make(map[string]bool, len(cbCompBSUCodes))
	rows := make([]bsuMapRow, 0, len(cbCompBSUCodes))
	for _, code := range cbCompBSUCodes {
		if seen[code] {
			return nil, fmt.Errorf("код %q встречается в cbCompBSUCodes дважды", code)
		}
		seen[code] = true
		industry, err := publicIndustryForCode(code)
		if err != nil {
			return nil, err
		}
		rows = append(rows, bsuMapRow{
			Code:          code,
			IndustrySheet: industry.Sheet,
			IndustrySlug:  industry.Slug,
			Basis:         industry.Basis,
			Confidence:    industry.Confidence,
			Source:        cbrMpBsuMapSource,
		})
	}
	return rows, nil
}

type cbrMpBsuMapStat struct {
	util.ClickHouseImport
}

func (s *cbrMpBsuMapStat) Name() string { return cbrMpBsuMapName }

// Import — свой цикл без сети: DDL → проверка классификатора (если он уже импортирован) →
// один батч 87 строк → витрины. Префлайта нет: внешнего артефакта у маппера нет, повтор
// идемпотентен по ключу code.
func (s *cbrMpBsuMapStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, sql := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(sql, s.TableName)); err != nil {
			return 0, err
		}
	}
	rows, err := bsuMapRows()
	if err != nil {
		return 0, err
	}
	if err = validateBsuCodesInClassifier(ctx, conn, rows); err != nil {
		return 0, err
	}
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", s.TableName))
	if err != nil {
		return 0, err
	}
	// loaded_at — седьмая колонка DDL; PrepareBatch без списка колонок ждёт и её. Одно
	// значение на батч: версия ReplacingMergeTree фиксирует момент записи.
	loadedAt := time.Now()
	for i, row := range rows {
		if err = batch.Append(row.Code, row.IndustrySheet, row.IndustrySlug, row.Basis, row.Confidence, row.Source, loadedAt); err != nil {
			abortBsuBatch(batch, fmt.Sprintf("строка %d (%s)", i+1, row.Code))
			return 0, fmt.Errorf("%s: append строка %d (%s): %w", cbrMpBsuMapName, i+1, row.Code, err)
		}
	}
	count = int64(batch.Rows())
	if err = batch.Send(); err != nil {
		return count, err
	}
	if _, err = util.CreateView(ctx, conn, cbrMpBsuMapView); err != nil {
		return count, err
	}
	if _, err = util.CreateView(ctx, conn, cbrMpPublicPanelView); err != nil {
		return count, err
	}
	return count, nil
}

// validateBsuCodesInClassifier — 87/87 по публичному классификатору, если он уже импортирован.
// Пока таблицы нет, маппер всё равно сеет кроссворд: витрина с именами появится после
// rosstat_okved2, а публичная панель доступна сразу. Ошибка при существующей таблице —
// fail-fast: она означает, что классификатор неполон, и молчаливая запись оставила бы
// v_cbr_mp_bsu_map с пропусками.
func validateBsuCodesInClassifier(ctx context.Context, conn driver.Conn, rows []bsuMapRow) error {
	var exists uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'rosstat_okved2'").Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		log.Warnf("%s: rosstat_okved2 ещё не импортирован — кроссворд записан, витрина с именами появится после классификатора", cbrMpBsuMapName)
		return nil
	}
	codes := make([]string, 0, len(rows))
	for _, row := range rows {
		codes = append(codes, "'"+row.Code+"'")
	}
	query := fmt.Sprintf("SELECT count() FROM rosstat_okved2 FINAL WHERE code IN (%s)", strings.Join(codes, ", "))
	var found uint64
	if err := conn.QueryRow(ctx, query).Scan(&found); err != nil {
		return err
	}
	if found != uint64(len(rows)) {
		return fmt.Errorf("%s: в rosstat_okved2 найдено %d из %d BSU-кодов — сначала `make import STAT=rosstat_okved2`", cbrMpBsuMapName, found, len(rows))
	}
	return nil
}

func abortBsuBatch(batch driver.Batch, what string) {
	if err := batch.Abort(); err != nil {
		log.Warnf("%s: не удалось закрыть батч после ошибки (%s): %v", cbrMpBsuMapName, what, err)
	}
}

var cbrMpBsuMapImporter = &cbrMpBsuMapStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   cbrMpBsuMapTable,
	CreateTable: []string{cbrMpBsuMapDdl},
}}
