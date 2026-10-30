package fedstat

import (
	"math"
	"os"
	"testing"
	"time"
)

func fedstatFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/macro/" + name)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return data
}

func fedstatRecords(t *testing.T, name string, layout fedstatLayout, year, labels int) []fedstatRecord {
	t.Helper()
	records, err := fedstatParseExcel(fedstatFixture(t, name), layout, year, labels)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return records
}

func fedstatFindRecord(t *testing.T, records []fedstatRecord, date time.Time, match func(fedstatRecord) bool) fedstatRecord {
	t.Helper()
	for _, record := range records {
		if record.date.Equal(date) && match(record) {
			return record
		}
	}
	t.Fatalf("запись на %s не найдена", date.Format("2006-01-02"))
	return fedstatRecord{}
}

func fedstatWantValue(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-6 {
		t.Errorf("value = %v, want %v", got, want)
	}
}

func TestFedstatParseIPCLongLayout(t *testing.T) {
	records := fedstatRecords(t, "ipc_31074_2002.xls", fedstatLayoutMatrix, 2002, 2)
	if len(records) != 6876 {
		t.Fatalf("records = %d, want 6876", len(records))
	}
	record := fedstatFindRecord(t, records, time.Date(2002, 1, 1, 0, 0, 0, 0, time.UTC), func(r fedstatRecord) bool {
		return r.labels[1] == "Все товары и услуги"
	})
	fedstatWantValue(t, record.value, 103.09)
	if record.labels[0] != "Российская Федерация" || record.periodType != "M" {
		t.Errorf("record = %+v", record)
	}
}

func TestFedstatParseIPPMatrix(t *testing.T) {
	records := fedstatRecords(t, "ipp_57806_2024.xls", fedstatLayoutMatrix, 2024, 2)
	if len(records) != 4608 {
		t.Fatalf("records = %d, want 4608", len(records))
	}
	record := fedstatFindRecord(t, records, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), func(r fedstatRecord) bool {
		return r.labels[0] == "ДОБЫЧА ПОЛЕЗНЫХ ИСКОПАЕМЫХ" && r.labels[1] == "Отчетный месяц к предыдущему месяцу"
	})
	fedstatWantValue(t, record.value, 96.3)
}

func TestFedstatParseIPPLong(t *testing.T) {
	records := fedstatRecords(t, "ipp_43046_2016.xls", fedstatLayoutLong, 2016, 3)
	if len(records) != 36 {
		t.Fatalf("records = %d, want 36", len(records))
	}
	record := fedstatFindRecord(t, records, time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC), func(r fedstatRecord) bool {
		return r.labels[0] == "К предыдущему месяцу"
	})
	fedstatWantValue(t, record.value, 80.3)
}

func TestFedstatParseRetailMatrix(t *testing.T) {
	records := fedstatRecords(t, "retail_31066_2000.xls", fedstatLayoutMatrix, 2000, 3)
	if len(records) != 105 {
		t.Fatalf("31066 2000 records = %d, want 105", len(records))
	}
	record := fedstatFindRecord(t, records, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), func(r fedstatRecord) bool {
		return r.labels[0] == "К предыдущему месяцу" && r.labels[2] == "Всего"
	})
	fedstatWantValue(t, record.value, 83.3)

	records = fedstatRecords(t, "retail_31260_2000.xls", fedstatLayoutMatrix, 2000, 2)
	if len(records) != 69 {
		t.Fatalf("31260 2000 records = %d, want 69", len(records))
	}
	record = fedstatFindRecord(t, records, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), func(r fedstatRecord) bool {
		return r.labels[1] == "Всего" && r.period == "январь"
	})
	fedstatWantValue(t, record.value, 170269.4)

	cumulative := 0
	for _, record := range records {
		if record.periodType == "CUM" {
			cumulative++
		}
	}
	if cumulative != 33 {
		t.Errorf("31260 2000 CUM records = %d, want 33", cumulative)
	}
}

func TestFedstatParseAgriLayouts(t *testing.T) {
	records := fedstatRecords(t, "agri_34128_2024.xls", fedstatLayoutMatrix, 2024, 1)
	if len(records) != 8 {
		t.Fatalf("34128 records = %d, want 8", len(records))
	}
	record := fedstatFindRecord(t, records, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), func(r fedstatRecord) bool {
		return r.labels[0] == "Хозяйства всех категорий"
	})
	fedstatWantValue(t, record.value, 101.5)

	records = fedstatRecords(t, "agri_59001_2024.xls", fedstatLayoutActivityColumns, 2024, 2)
	if len(records) != 12 {
		t.Fatalf("59001 records = %d, want 12", len(records))
	}
	record = fedstatFindRecord(t, records, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), func(r fedstatRecord) bool {
		return r.activity == "Сельское хозяйство" && r.labels[1] == "Хозяйства всех категорий"
	})
	fedstatWantValue(t, record.value, 96.7)

	records = fedstatRecords(t, "agri_31062_1991-2016.xls", fedstatLayoutYearColumns, 0, 3)
	if len(records) != 312 {
		t.Fatalf("31062 records = %d, want 312", len(records))
	}
	record = fedstatFindRecord(t, records, time.Date(1991, 1, 1, 0, 0, 0, 0, time.UTC), func(r fedstatRecord) bool {
		return r.labels[1] == "Растениеводство" && r.labels[0] == "Хозяйства всех категорий"
	})
	fedstatWantValue(t, record.value, 100.4)

	records = fedstatRecords(t, "agri_43337_1990-2011.xls", fedstatLayoutYearColumns, 0, 3)
	if len(records) != 85 {
		t.Fatalf("43337 records = %d, want 85", len(records))
	}
	record = fedstatFindRecord(t, records, time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC), func(r fedstatRecord) bool {
		return r.labels[0] == "Хозяйства всех категорий"
	})
	fedstatWantValue(t, record.value, 2462187)
}

func TestFedstatParseInvestmentLayouts(t *testing.T) {
	records := fedstatRecords(t, "investment_58089_2024.xls", fedstatLayoutMatrix, 2024, 1)
	if len(records) != 4 {
		t.Fatalf("58089 records = %d, want 4", len(records))
	}
	record := fedstatFindRecord(t, records, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), func(r fedstatRecord) bool {
		return r.period == "январь-июнь"
	})
	fedstatWantValue(t, record.value, 110.9)

	records = fedstatRecords(t, "investment_58090_2024.xls", fedstatLayoutMatrix, 2024, 3)
	if len(records) != 4 {
		t.Fatalf("58090 records = %d, want 4", len(records))
	}
	record = fedstatFindRecord(t, records, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), func(r fedstatRecord) bool {
		return r.period == "январь-июнь"
	})
	fedstatWantValue(t, record.value, 0)
	if record.labels[1] != "ВСЕГО" || record.labels[2] != "ВСЕГО (по всем формам собственности)" {
		t.Errorf("labels = %v", record.labels)
	}

	records = fedstatRecords(t, "investment_34129_2000-2022.xls", fedstatLayoutYearColumns, 0, 1)
	if len(records) != 23 {
		t.Fatalf("34129 records = %d, want 23", len(records))
	}
	record = fedstatFindRecord(t, records, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), func(r fedstatRecord) bool { return true })
	fedstatWantValue(t, record.value, 117.4)

	records = fedstatRecords(t, "investment_40562_2005-2022.xls", fedstatLayoutYearColumns, 0, 1)
	if len(records) != 18 {
		t.Fatalf("40562 records = %d, want 18", len(records))
	}
	record = fedstatFindRecord(t, records, time.Date(2005, 1, 1, 0, 0, 0, 0, time.UTC), func(r fedstatRecord) bool { return true })
	fedstatWantValue(t, record.value, 2893237.7)

	records = fedstatRecords(t, "investment_34475_1998-2016.xls", fedstatLayoutYearColumns, 0, 2)
	if len(records) != 19 {
		t.Fatalf("34475 records = %d, want 19", len(records))
	}
	record = fedstatFindRecord(t, records, time.Date(1998, 1, 1, 0, 0, 0, 0, time.UTC), func(r fedstatRecord) bool { return true })
	fedstatWantValue(t, record.value, 407086.3)
}

func TestFedstatParseFailFast(t *testing.T) {
	if _, err := fedstatParseExcel([]byte("not xls"), fedstatLayoutMatrix, 2024, 2); err == nil {
		t.Fatal("want error for non-XLS")
	}
	records := fedstatRecords(t, "agri_34128_2024.xls", fedstatLayoutMatrix, 2024, 1)
	if len(records) == 0 {
		t.Fatal("no records")
	}
}
