package domrf

import (
	"encoding/json"
	"os"
	"testing"
)

func TestDomrfPriceIndexPoints(t *testing.T) {
	body, err := os.ReadFile("testdata/price_index_mainchart_rf.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	points, err := domrfPriceIndexPoints(body)
	if err != nil {
		t.Fatalf("domrfPriceIndexPoints: %v", err)
	}
	if len(points) != 69 {
		t.Fatalf("точек %d, ожидалось 69", len(points))
	}
	first, last := points[0], points[len(points)-1]
	if first.Region != domrfPriceIndexRegionRF || first.RoomCount != domrfPriceIndexRoomsAll {
		t.Fatalf("первая точка %s/%s", first.Region, first.RoomCount)
	}
	if got := first.Date.Format("2006-01-02"); got != "2021-01-01" {
		t.Errorf("первая дата %s, ожидалось 2021-01-01", got)
	}
	if first.IndexValue != 100 {
		t.Errorf("первый индекс %v, ожидалось 100", first.IndexValue)
	}
	if got := last.Date.Format("2006-01-02"); got != "2026-09-01" {
		t.Errorf("последняя дата %s, ожидалось 2026-09-01", got)
	}
	if last.IndexValue != 201.8342 {
		t.Errorf("последний индекс %v, ожидалось 201.8342", last.IndexValue)
	}
	if last.Deals != 63779 {
		t.Errorf("сделок %d, ожидалось 63779", last.Deals)
	}
}

func TestDomrfPriceIndexRoom(t *testing.T) {
	cases := map[string]string{
		"Комнат":     "all",
		"Все классы": "all",
		"":           "all",
		"1 комната":  "1",
		"1.0":        "1",
		"2 комнаты":  "2",
		"3 комнаты":  "3",
		"3.0":        "3",
	}
	for input, want := range cases {
		got, err := domrfPriceIndexRoom(input)
		if err != nil {
			t.Errorf("%q: %v", input, err)
			continue
		}
		if got != want {
			t.Errorf("%q: %q, ожидалось %q", input, got, want)
		}
	}
	for _, input := range []string{"Студия", "4 комнаты"} {
		if _, err := domrfPriceIndexRoom(input); err == nil {
			t.Errorf("%q: неизвестный разрез не пойман", input)
		}
	}
}

func TestDomrfPriceIndexPointsRejects(t *testing.T) {
	cases := map[string]string{
		"success=false": `{"success":false,"data":{"regions":{}}}`,
		"нет регионов":  `{"success":true,"data":{"regions":{}}}`,
		"плохая дата": `{"success":true,"data":{"regions":{"k":{"items":[
			{"region":"Российская Федерация","class":"Общий","room_count":"Все классы","index_value":"100","index_diff_month":"0","index_count_deal":1,"month_dt":"2021-13-01"}]}}}}`,
		"нечисловой индекс": `{"success":true,"data":{"regions":{"k":{"items":[
			{"region":"Российская Федерация","class":"Общий","room_count":"Все классы","index_value":"x","index_diff_month":"0","index_count_deal":1,"month_dt":"2021-01-01"}]}}}}`,
		"отрицательные сделки": `{"success":true,"data":{"regions":{"k":{"items":[
			{"region":"Российская Федерация","class":"Общий","room_count":"Все классы","index_value":"100","index_diff_month":"0","index_count_deal":-1,"month_dt":"2021-01-01"}]}}}}`,
		"дубль точки": `{"success":true,"data":{"regions":{"a":{"items":[
			{"region":"Российская Федерация","class":"Общий","room_count":"Все классы","index_value":"100","index_diff_month":"0","index_count_deal":1,"month_dt":"2021-01-01"}]},
			"b":{"items":[
			{"region":"Российская Федерация","class":"Общий","room_count":"Все классы","index_value":"100","index_diff_month":"0","index_count_deal":1,"month_dt":"2021-01-01"}]}}}}`,
	}
	for name, body := range cases {
		if _, err := domrfPriceIndexPoints([]byte(body)); err == nil {
			t.Errorf("%s: ошибка не поймана", name)
		}
	}
}

func TestDomrfPriceIndexPayload(t *testing.T) {
	payload, err := domrfPriceIndexPayload(domrfPriceIndexSlots([]string{"Российская Федерация", "Город Москва"}, []string{"Комнат", "1"}))
	if err != nil {
		t.Fatalf("domrfPriceIndexPayload: %v", err)
	}
	var request domrfPriceIndexRequest
	if err = json.Unmarshal(payload, &request); err != nil {
		t.Fatalf("разбор тела: %v", err)
	}
	if len(request.Regions) != 4 {
		t.Fatalf("слотов %d, ожидалось 4", len(request.Regions))
	}
	if request.DateFrom != nil || request.DateTo != nil {
		t.Error("даты фильтра должны быть nil: API их не принимает")
	}
	if request.Regions[0].ID != 1 || request.Regions[3].ID != 4 {
		t.Errorf("id слотов %d…%d, ожидалось 1…4", request.Regions[0].ID, request.Regions[3].ID)
	}
	if request.Regions[0].EstateClass != domrfPriceIndexClassAll {
		t.Errorf("класс %q", request.Regions[0].EstateClass)
	}
}
