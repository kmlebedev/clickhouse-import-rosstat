package domrf

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
)

// Импортёр domrf_price_index — индекс цен на первичном рынке жилья ДОМ.РФ: месячная панель
// «регион × разрез по комнатам» с числом сделок ДДУ, 2021-01 … 2026-09.
//
// Панель забирается POST-ом на /api/v2/priceindex/living/mainchart/ одним запросом на все регионы
// (75 регионов × 4 разреза = 300 слотов, ответ ≈7 МБ). Гейт идемпотентности дешёвый: сначала
// запрос по одному региону-агрегату (37 КБ) — если его последний месяц и число точек совпали с
// таблицей, полный запрос не делается вовсе.
type domrfPriceIndex struct{}

const (
	domrfPriceIndexTable = "domrf_price_index"
	// domrfPriceIndexDdl — DDL литералом: у импортёра свой Import() (JSON API, без файла).
	domrfPriceIndexDdl = `CREATE TABLE IF NOT EXISTS ` + domrfPriceIndexTable + ` (
			  region LowCardinality(String)
			, room_count LowCardinality(String)
			, date Date
			, index_value Float64
			, index_diff_month Float64
			, deals UInt32
		) ENGINE = ReplacingMergeTree ORDER BY (region, room_count, date);
	`
	// domrfPriceIndexRegionsUrl — срез по регионам за последний месяц: даёт список регионов
	// (75) одним дешёвым запросом. Регионы не хардкодятся — источник может добавить субъект.
	domrfPriceIndexRegionsUrl = domrfSite + "/api/v2/priceindex/living/changesfull/"
	// domrfPriceIndexMaxSlots — предел источника на число слотов в одном запросе: API отвечает
	// 400 «Поле Regions не должно превышать 120». Панель 75×4 режется на чанки по этому пределу.
	domrfPriceIndexMaxSlots = 120
	// domrfPriceIndexRFQuery — чек точки контроля публикации по строке РФ: число точек и
	// последний месяц. ifNull — чтобы пустая таблица давала нулевую дату, а не NULL в Scan.
	// Разрез all обязателен в условии: у строки РФ в таблице четыре разреза (all/1/2/3), и
	// count() без него сравнивал бы 276 точек с 69 точками дешёвого запроса — гейт не срабатывал бы
	// никогда.
	domrfPriceIndexRFQuery = `SELECT count(), ifNull(max(date), toDate('1970-01-01')) FROM ` +
		domrfPriceIndexTable + ` FINAL WHERE region = ? AND room_count = ?`
)

// domrfPriceIndexRoomValues — разрезы, которые забираются в панель: агрегат и число комнат 1–3.
// «Студия», «4» и «5» источник молча сводит к агрегату (проверено 2026-10-23), поэтому отдельных
// разрезов для них не существует.
var domrfPriceIndexRoomValues = []string{"Комнат", "1", "2", "3"}

// domrfPriceIndexRegionsResponse — ответ changesfull: срез по регионам за месяц.
type domrfPriceIndexRegionsResponse struct {
	Success bool `json:"success"`
	Data    struct {
		Items []struct {
			Region string `json:"region"`
		} `json:"items"`
	} `json:"data"`
}

func (s *domrfPriceIndex) Name() string {
	return domrfPriceIndexTable
}

// domrfPriceIndexRegions отдаёт список регионов в порядке источника без дублей.
func domrfPriceIndexRegions() ([]string, error) {
	body, err := domrfGetPage(domrfPriceIndexRegionsUrl)
	if err != nil {
		return nil, err
	}
	var response domrfPriceIndexRegionsResponse
	if err = json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("разбор списка регионов: %w", err)
	}
	if !response.Success || len(response.Data.Items) == 0 {
		return nil, fmt.Errorf("список регионов пуст (success=%t, элементов %d)", response.Success, len(response.Data.Items))
	}
	regions := make([]string, 0, len(response.Data.Items))
	seen := make(map[string]bool, len(response.Data.Items))
	for _, item := range response.Data.Items {
		if item.Region == "" || seen[item.Region] {
			continue
		}
		seen[item.Region] = true
		regions = append(regions, item.Region)
	}
	return regions, nil
}

// domrfPriceIndexSlot — один слот фильтра: регион и разрез по комнатам.
type domrfPriceIndexSlot struct {
	Region string
	Rooms  string
}

// domrfPriceIndexSlots разворачивает матрицу «разрез × регион» в слоты в порядке источника.
func domrfPriceIndexSlots(regions, rooms []string) []domrfPriceIndexSlot {
	slots := make([]domrfPriceIndexSlot, 0, len(regions)*len(rooms))
	for _, room := range rooms {
		for _, region := range regions {
			slots = append(slots, domrfPriceIndexSlot{Region: region, Rooms: room})
		}
	}
	return slots
}

// domrfPriceIndexPayload собирает тело запроса из слотов. id слотов сквозной — контракт
// источника требует уникальности в пределах запроса.
func domrfPriceIndexPayload(slots []domrfPriceIndexSlot) ([]byte, error) {
	request := domrfPriceIndexRequest{Regions: make([]domrfPriceIndexRequestRegion, 0, len(slots))}
	for i, slot := range slots {
		request.Regions = append(request.Regions, domrfPriceIndexRequestRegion{
			Region: slot.Region, EstateClass: domrfPriceIndexClassAll, Rooms: slot.Rooms, ID: i + 1,
		})
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("сборка запроса: %w", err)
	}
	return payload, nil
}

// domrfPriceIndexFetchSlots забирает точки по слоту-списку одним запросом.
func domrfPriceIndexFetchSlots(slots []domrfPriceIndexSlot) ([]domrfPriceIndexPoint, error) {
	payload, err := domrfPriceIndexPayload(slots)
	if err != nil {
		return nil, err
	}
	body, err := domrfPostPriceIndex(payload)
	if err != nil {
		return nil, err
	}
	return domrfPriceIndexPoints(body)
}

// domrfPriceIndexFetchAll забирает полную панель чанками по domrfPriceIndexMaxSlots: API не
// принимает больше 120 слотов в одном запросе (400 «Поле Regions не должно превышать 120»),
// поэтому 300 слотов панели уходят тремя запросами. Чанк смешивает разрезы — это допустимо:
// сервер валидирует только число слотов, а не их состав.
func domrfPriceIndexFetchAll(regions []string) ([]domrfPriceIndexPoint, error) {
	slots := domrfPriceIndexSlots(regions, domrfPriceIndexRoomValues)
	points := make([]domrfPriceIndexPoint, 0, len(slots)*24)
	for start := 0; start < len(slots); start += domrfPriceIndexMaxSlots {
		end := min(start+domrfPriceIndexMaxSlots, len(slots))
		chunk, err := domrfPriceIndexFetchSlots(slots[start:end])
		if err != nil {
			return nil, fmt.Errorf("слоты %d…%d: %w", start+1, end, err)
		}
		points = append(points, chunk...)
	}
	return points, nil
}

// Import: DDL → список регионов → дешёвая проверка месяца по РФ → (no-op | полная панель) →
// батч → производные. Возвращаемое число на no-op-пути — число точек РФ, как у шаблона
// (PublicationPreflight возвращает points известной версии).
func (s *domrfPriceIndex) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, domrfPriceIndexDdl); err != nil {
		return count, err
	}
	regions, err := domrfPriceIndexRegions()
	if err != nil {
		return count, fmt.Errorf("%s: %w", domrfPriceIndexTable, err)
	}
	rfFound := false
	for _, region := range regions {
		if region == domrfPriceIndexRegionRF {
			rfFound = true
			break
		}
	}
	if !rfFound {
		return count, fmt.Errorf("%s: регион %q отсутствует в срезе источника", domrfPriceIndexTable, domrfPriceIndexRegionRF)
	}
	latestPoints, err := domrfPriceIndexFetchSlots(domrfPriceIndexSlots([]string{domrfPriceIndexRegionRF}, []string{"Комнат"}))
	if err != nil {
		return count, fmt.Errorf("%s: проверка месяца: %w", domrfPriceIndexTable, err)
	}
	latest := latestPoints[0].Date
	for _, point := range latestPoints[1:] {
		if point.Date.After(latest) {
			latest = point.Date
		}
	}
	storedRF, storedMax, err := domrfPriceIndexStored(ctx, conn)
	if err != nil {
		return count, err
	}
	log.Infof("%s: источник %s (%d точек РФ), в таблице %s (%d)", domrfPriceIndexTable,
		latest.Format("2006-01"), len(latestPoints), storedMax.Format("2006-01"), storedRF)
	// Сравнение по месяцу в его собственном часовом поясе: ClickHouse отдаёт Date как полночь
	// локальной зоны, а ответ API — как полночь UTC, и After() на этих значениях сдвигал бы
	// границу на день (гейт не срабатывал бы никогда).
	sameMonth := latest.Format("2006-01") == storedMax.Format("2006-01")
	if sameMonth && storedRF == uint64(len(latestPoints)) {
		log.Infof("%s: no-op, месяц %s уже импортирован (%d точек РФ на месте)", domrfPriceIndexTable, latest.Format("2006-01"), storedRF)
		return int64(storedRF), nil
	}
	points, err := domrfPriceIndexFetchAll(regions)
	if err != nil {
		return count, fmt.Errorf("%s: %w", domrfPriceIndexTable, err)
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+domrfPriceIndexTable)
	if err != nil {
		return count, err
	}
	for i, point := range points {
		if err = batch.Append(point.Region, point.RoomCount, point.Date, point.IndexValue, point.DiffMonth, point.Deals); err != nil {
			_ = batch.Abort()
			return count, fmt.Errorf("%s: append точки %d (%s, %s, %s): %w", domrfPriceIndexTable, i+1, point.Region, point.RoomCount, point.Date.Format("2006-01-02"), err)
		}
		count++
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	log.Infof("%s: %d точек, %d регионов, месяцев %d", domrfPriceIndexTable, count, len(regions), len(latestPoints))
	if err = afterDomrfMacro(ctx, conn); err != nil {
		return count, err
	}
	log.Infof("Imported %d rows of %s", count, domrfPriceIndexTable)
	return count, nil
}

// domrfPriceIndexStored — состояние строки РФ в таблице: число точек и последний месяц. Оба
// значения нужны вместе: по одному месяцу частично записанная панель выглядела бы импортированной.
func domrfPriceIndexStored(ctx context.Context, conn driver.Conn) (points uint64, maxDate time.Time, err error) {
	if err = conn.QueryRow(ctx, domrfPriceIndexRFQuery, domrfPriceIndexRegionRF, domrfPriceIndexRoomsAll).Scan(&points, &maxDate); err != nil {
		return 0, time.Time{}, fmt.Errorf("%s: чтение версии РФ: %w", domrfPriceIndexTable, err)
	}
	return points, maxDate, nil
}
