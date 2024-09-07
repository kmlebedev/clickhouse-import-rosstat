package rosstat

import (
	"bytes"
	"github.com/xuri/excelize/v2"
	"io"
	"net/http"
	"strconv"
	"time"
)

func GetXlsx(url string) (xlsx *excelize.File, err error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	reader := bytes.NewReader(body)
	if xlsx, err = excelize.OpenReader(reader); err != nil {
		return nil, err
	}
	return xlsx, nil
}

func weekStart(year, week string) time.Time {
	y, _ := strconv.Atoi(year)
	w, _ := strconv.Atoi(week)
	// Start from the middle of the year:
	t := time.Date(y, 7, 1, 0, 0, 0, 0, time.UTC)

	// Roll back to Monday:
	if wd := t.Weekday(); wd == time.Sunday {
		t = t.AddDate(0, 0, -6)
	} else {
		t = t.AddDate(0, 0, -int(wd)+1)
	}

	// Difference in weeks:
	_, isoWeek := t.ISOWeek()
	t = t.AddDate(0, 0, (w-isoWeek)*7)

	return t
}
