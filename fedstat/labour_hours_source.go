package fedstat

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

//go:embed labour_hours_dimensions.json
var labourHoursDimensions []byte

func fetchLabourHours(ctx context.Context, client *http.Client, id string) ([]byte, error) {
	return fedstatRetryPost(ctx, id+" full excel", func(ctx context.Context) ([]byte, bool, error) {
		page, err := fedstatGetPage(ctx, client, id)
		if err != nil {
			return nil, false, err
		}
		token, err := fedstatToken(page, id)
		if err != nil {
			return nil, false, err
		}
		values := url.Values{"id": {id}, "format": {"excel"}, "struts.token.name": {"token"}, "token": {token}}
		requestCtx, cancel := context.WithTimeout(ctx, fedstatExportTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, fedstatBaseURL+"/indicator/"+id+"/download", strings.NewReader(values.Encode()))
		if err != nil {
			return nil, false, err
		}
		req.Header.Set("User-Agent", util.HttpUA)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Referer", fedstatBaseURL+"/indicator/"+id)
		req.Header.Set("Origin", fedstatBaseURL)
		resp, err := client.Do(req)
		if err != nil {
			return nil, true, err
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(io.LimitReader(resp.Body, fedstatMaxBodyBytes+1))
		if err != nil {
			return nil, true, err
		}
		if len(body) > fedstatMaxBodyBytes {
			return nil, false, fmt.Errorf("%s: oversized export", id)
		}
		if resp.StatusCode != http.StatusOK {
			return nil, resp.StatusCode == 429 || resp.StatusCode >= 500, fmt.Errorf("%s: export HTTP %d", id, resp.StatusCode)
		}
		if !bytes.HasPrefix(body, []byte(fedstatBIFFMagic)) {
			return nil, false, fmt.Errorf("%s: export is not BIFF XLS", id)
		}
		return body, false, nil
	})
}

func parseLabourHours(body []byte, id string) ([]labourHoursPoint, error) {
	if !bytes.HasPrefix(body, []byte(fedstatBIFFMagic)) {
		return nil, fmt.Errorf("not BIFF XLS")
	}
	rows, err := fedstatReadSheet(body)
	if err != nil {
		return nil, err
	}
	return parseLabourHoursRows(rows, id)
}
func parseLabourHoursRows(rows [][]fedstatCell, id string) ([]labourHoursPoint, error) {
	var allowed map[string]map[string][]string
	if err := json.Unmarshal(labourHoursDimensions, &allowed); err != nil {
		return nil, err
	}
	headers := []string{"Возраст", "Классификатор объектов административно-территориального деления (ОКАТО)", "Единица измерения", "Период"}
	unit := ""
	switch id {
	case "59749":
		headers = append(headers, "Продолжительность рабочей недели", "Структура населения")
		unit = "793 тысяча человек"
	case "59751":
		headers = append(headers, "Структура населения")
		unit = "356 час"
	case "60463":
		headers[0] = "Классификатор видов экономической деятельности (ОКВЭД2)"
		headers = append(headers, "Статус деятельности")
		unit = "542 тысяча человеко-часов"
	default:
		return nil, fmt.Errorf("unknown indicator %s", id)
	}
	header := -1
	for r, row := range rows {
		if fedstatRowText(row, 0) == headers[0] {
			header = r
			break
		}
	}
	if header < 0 {
		return nil, fmt.Errorf("missing dimension header")
	}
	for c, name := range headers {
		if fedstatRowText(rows[header], c) != name {
			return nil, fmt.Errorf("unexpected dimension column %d", c)
		}
	}
	years := map[int]int{}
	seenYears := map[int]bool{}
	for c := len(headers); c < len(rows[header]); c++ {
		raw := fedstatRowText(rows[header], c)
		year, err := strconv.Atoi(raw)
		if err != nil || year < 1900 || year > 2100 || seenYears[year] {
			return nil, fmt.Errorf("invalid/duplicate year %q", raw)
		}
		years[c] = year
		seenYears[year] = true
	}
	if len(years) == 0 {
		return nil, fmt.Errorf("no year columns")
	}
	var points []labourHoursPoint
	for r := header + 1; r < len(rows); r++ {
		row := rows[r]
		for c := len(headers); c < len(row); c++ {
			cell := fedstatCellAt(row, c)
			if cell.text == "" && !cell.numeric {
				continue
			}
			year, ok := years[c]
			if !ok || !cell.numeric {
				return nil, fmt.Errorf("unexpected value at row %d column %d", r+1, c+1)
			}
			labels := make([]string, len(headers))
			for d := range labels {
				labels[d] = fedstatRowText(row, d)
				valid := false
				for _, value := range allowed[id][headers[d]] {
					if labels[d] == value {
						valid = true
						break
					}
				}
				if !valid {
					return nil, fmt.Errorf("unknown dimension %s: %q", headers[d], labels[d])
				}
				if labels[d] == "" {
					return nil, fmt.Errorf("missing dimension row %d", r+1)
				}
			}
			if labels[2] != unit || labels[3] != "1558883 значение показателя за год" {
				return nil, fmt.Errorf("unexpected unit/period row %d", r+1)
			}
			if !strings.HasPrefix(labels[1], "643 ") && !strings.HasPrefix(labels[1], "643004.АГ ") {
				return nil, fmt.Errorf("unexpected geography %q", labels[1])
			}
			p := labourHoursPoint{series: id, region: labels[1], unit: strings.SplitN(unit, " ", 2)[1], date: time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC), value: cell.number}
			if id == "60463" {
				p.activity = labels[0]
				p.status = labels[4]
			} else {
				p.age = labels[0]
				p.sex = labels[len(labels)-1]
				if p.sex != "2 Женщины" && p.sex != "3 Мужчины" {
					return nil, fmt.Errorf("unknown sex %q", p.sex)
				}
				if id == "59749" {
					p.hoursBand = labels[4]
				}
			}
			points = append(points, p)
		}
	}
	if err := validateLabourHours(points, id); err != nil {
		return nil, err
	}
	if _, err := labourHoursDelta(points, nil); err != nil {
		return nil, err
	}
	return points, nil
}
