package domrf

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// domrfPriceIndexDateLayout — формат month_dt в ответе API (YYYY-MM-DD).
	domrfPriceIndexDateLayout = "2006-01-02"
	// domrfPriceIndexRoomsAll — разрез «все квартиры»: подпись источника «Комнат» в запросе и
	// «Все классы»/«Комнат» в ответе означают агрегат, а не сумму разрезов.
	domrfPriceIndexRoomsAll = "all"
	// domrfPriceIndexRegionRF — регион-агрегат: строка РФ в панели равна сумме субъектов,
	// поэтому её нельзя складывать с субъектами повторно.
	domrfPriceIndexRegionRF = "Российская Федерация"
	// domrfPriceIndexClassAll — единственный класс, который отдаёт источник: параметр estateClass
	// он игнорирует.
	domrfPriceIndexClassAll = "Все классы"
)

// domrfPriceIndexResponse — ответ mainchart: регионы лежат картой (ключ — технический
// «<region_id>_<class_id>_<rooms>_<…>»), поэтому обход идёт по значениям, а не по ключам.
type domrfPriceIndexResponse struct {
	Success bool `json:"success"`
	Data    struct {
		Regions map[string]struct {
			Items []domrfPriceIndexItem `json:"items"`
		} `json:"regions"`
	} `json:"data"`
}

// domrfPriceIndexItem — точка панели. Числовые поля источник отдаёт строками, число сделок —
// числом; разбор приводит их к float64/UInt32.
type domrfPriceIndexItem struct {
	Region         string `json:"region"`
	RegionID       int    `json:"region_id"`
	Class          string `json:"class"`
	RoomCount      string `json:"room_count"`
	IndexValue     string `json:"index_value"`
	IndexDiffMonth string `json:"index_diff_month"`
	IndexCountDeal int64  `json:"index_count_deal"`
	MonthDT        string `json:"month_dt"`
}

// domrfPriceIndexPoint — точка таблицы domrf_price_index.
type domrfPriceIndexPoint struct {
	Region     string
	RoomCount  string
	Date       time.Time
	IndexValue float64
	DiffMonth  float64
	Deals      uint32
}

// domrfPriceIndexRoom приводит подпись разреза к каноническому виду: агрегат — all, разрез по
// числу комнат — «1»/«2»/«3». Источник пишет «Комнат»/«Все классы» в агрегате и «1 комната»/
// «2 комнаты»/«3 комнаты» в разрезах, а в items — ещё и «1.0»; неизвестная подпись — ошибка,
// иначе новый разрез молча лёг бы в агрегат.
func domrfPriceIndexRoom(raw string) (string, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	switch {
	case value == "", value == "комнат", value == "все классы":
		return domrfPriceIndexRoomsAll, nil
	case strings.HasPrefix(value, "1"):
		return "1", nil
	case strings.HasPrefix(value, "2"):
		return "2", nil
	case strings.HasPrefix(value, "3"):
		return "3", nil
	}
	return "", fmt.Errorf("разрез по комнатам %q не распознан", raw)
}

// domrfPriceIndexPoints разбирает ответ API в точки. Чистая функция (§6.1).
//
// Проверки: success=false — ошибка источника; нечисловое значение или отрицательное число
// сделок — ошибка; дубль (регион, разрез, месяц) — ошибка: два значения на одну точку означали бы,
// что панель нельзя записать ключом ReplacingMergeTree без потери.
func domrfPriceIndexPoints(body []byte) ([]domrfPriceIndexPoint, error) {
	var response domrfPriceIndexResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("разбор JSON: %w", err)
	}
	if !response.Success {
		return nil, fmt.Errorf("источник вернул success=false")
	}
	if len(response.Data.Regions) == 0 {
		return nil, fmt.Errorf("в ответе нет регионов")
	}
	points := make([]domrfPriceIndexPoint, 0, len(response.Data.Regions)*24)
	seen := make(map[string]bool, len(response.Data.Regions)*24)
	for _, region := range response.Data.Regions {
		for _, item := range region.Items {
			date, err := time.Parse(domrfPriceIndexDateLayout, item.MonthDT)
			if err != nil {
				return nil, fmt.Errorf("%s: месяц %q: %w", item.Region, item.MonthDT, err)
			}
			room, err := domrfPriceIndexRoom(item.RoomCount)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", item.Region, err)
			}
			value, err := strconv.ParseFloat(strings.TrimSpace(item.IndexValue), 64)
			if err != nil {
				return nil, fmt.Errorf("%s, %s: индекс %q не число: %w", item.Region, item.MonthDT, item.IndexValue, err)
			}
			diff, err := strconv.ParseFloat(strings.TrimSpace(item.IndexDiffMonth), 64)
			if err != nil {
				return nil, fmt.Errorf("%s, %s: м/м %q не число: %w", item.Region, item.MonthDT, item.IndexDiffMonth, err)
			}
			if item.IndexCountDeal < 0 {
				return nil, fmt.Errorf("%s, %s: сделок %d", item.Region, item.MonthDT, item.IndexCountDeal)
			}
			key := item.Region + "|" + room + "|" + item.MonthDT
			if seen[key] {
				return nil, fmt.Errorf("точка %s повторяется в ответе", key)
			}
			seen[key] = true
			points = append(points, domrfPriceIndexPoint{
				Region:     item.Region,
				RoomCount:  room,
				Date:       date,
				IndexValue: value,
				DiffMonth:  diff,
				Deals:      uint32(item.IndexCountDeal),
			})
		}
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("в ответе ни одной точки")
	}
	return points, nil
}
