package rosstat

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// Фикстуры — реальные артефакты спасательного дампа 2026-10-16 (спека §3.10): живой выпуск
// 2021-08-09, новогодний выпуск 2021-01-11 (у него своя шапка колонок), восстановленная через
// Wayback неделя 2021-02-01 и фрагмент 2017-05-22. Тесты офлайн: точки собирает appendRecorder,
// BodyFunc-замыкание вызывается напрямую — ни базы, ни сети тесту не нужно (§11.6).
const (
	ipcWeeksBulletinsLive2021Fixture    = "ipc_weeks_bulletins_144.htm"
	ipcWeeksBulletinsNewYear2021Fixture = "ipc_weeks_bulletins_2.htm"
	ipcWeeksBulletinsWayback21Fixture   = "ipc_weeks_bulletins_19_wayback.htm"
	ipcWeeksBulletinsWayback17Fixture   = "ipc_weeks_bulletins_100_2017_wayback.htm"
	ipcWeeksBulletinsWayback09Fixture   = "ipc_weeks_bulletins_2009_04_06.htm"
	ipcWeeksBulletinsStubFixture        = "ipc_weeks_bulletins_stub.htm"

	// Фикстуры старого формата 2016–2017 (каталоги d06/d01 «Оперативной информации», разведка
	//scripts/harvest.py): выпуски без <title>, с большой буквы в якоре («К предыдущей дате
	// регистрации») и дефисными переносами в шапке и именах товаров («19 сен-тября»,
	// «госу- дарственного») — до нормализации Key (нижний регистр, снятие переносов) разбор
	// этих восьми недель падал.
	ipcWeeksBulletinsOld20160111Fixture = "ipc_weeks_bulletins_2016_01_11.htm"
	ipcWeeksBulletinsOld20160118Fixture = "ipc_weeks_bulletins_2016_01_18.htm"
	ipcWeeksBulletinsOld20160125Fixture = "ipc_weeks_bulletins_2016_01_25.htm"
	ipcWeeksBulletinsOld20160201Fixture = "ipc_weeks_bulletins_2016_02_01.htm"
	ipcWeeksBulletinsOld20160919Fixture = "ipc_weeks_bulletins_2016_09_19.htm"
	ipcWeeksBulletinsOld20160926Fixture = "ipc_weeks_bulletins_2016_09_26.htm"
	ipcWeeksBulletinsOld20170109Fixture = "ipc_weeks_bulletins_2017_01_09.htm"
	ipcWeeksBulletinsOld20170123Fixture = "ipc_weeks_bulletins_2017_01_23.htm"
)

// importIpcWeeksBulletinsFixture прогоняет фикстуру через BodyFunc-замыкание выпуска и отдаёт
// записанные точки: тот же путь, что у шаблона (байты HTML → ipcWeeksBulletinsParse → батч).
func importIpcWeeksBulletinsFixture(t *testing.T, file string, date time.Time) []appendedRow {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", file))
	if err != nil {
		t.Fatal(err)
	}
	rec := &appendRecorder{}
	source := ipcWeeksBulletinsSource{URL: "test://" + file, Date: date}
	if err = ipcWeeksBulletinsAppend(source)(body, rec); err != nil {
		t.Fatalf("ipcWeeksBulletinsAppend(%s): %v", file, err)
	}
	return rec.rows
}

// ipcWeeksBulletinsSpotValue ищет единственную точку ряда на дату выпуска и возвращает её
// значение: spot-проверки ниже читаются одной строкой.
func ipcWeeksBulletinsSpotValue(t *testing.T, rows []appendedRow, series string, date time.Time) float64 {
	t.Helper()
	found := 0
	value := 0.0
	for _, row := range rows {
		if row.series != series || !row.date.Equal(date) {
			continue
		}
		found++
		value = row.value
	}
	if found != 1 {
		t.Fatalf("ряд %q на %s: точек %d, ожидалась 1", series, date.Format("2006-01-02"), found)
	}
	return value
}

// TestIpcWeeksBulletinsReadsLive2021 — разбор живого выпуска 144.htm (2021-08-09): 103 строки
// товаров и агрегат, 104 точки, каждая — под своим рядом (имён-дублей в выпуске нет). Значения
// сверяются точками: агрегат 100,00, «Говядина…» 100,35, «Сметана» 100,48 (значения источника
// без пробелов и в десятичной запятой). Тип записанного значения — float64: колонка таблицы
// Float64 (спека §3.1; у legacy-соседа rosstat_ipc_weeks — Float32), и appendRecorder хранит
// тип, чтобы схема колонки не осталась непроверенной.
func TestIpcWeeksBulletinsReadsLive2021(t *testing.T) {
	date := time.Date(2021, time.August, 9, 0, 0, 0, 0, time.UTC)
	rows := importIpcWeeksBulletinsFixture(t, ipcWeeksBulletinsLive2021Fixture, date)
	if len(rows) != 104 {
		t.Fatalf("точек %d, ожидалось 104 (103 товара + агрегат)", len(rows))
	}
	counts := countBySeries(rows)
	if len(counts) != 104 {
		t.Fatalf("рядов %d, ожидалось 104", len(counts))
	}
	for _, row := range rows {
		if row.valueType != "float64" {
			t.Errorf("ряд %q записан как %s, ожидался float64", row.series, row.valueType)
		}
		if !row.date.Equal(date) {
			t.Errorf("ряд %q: дата %s, ожидалась %s", row.series, row.date.Format("2006-01-02"), date.Format("2006-01-02"))
		}
	}
	for series, want := range map[string]float64{
		ipcWeeksBulletinsAggregateSeries:        100.00,
		"Говядина (кроме бескостного мяса), кг": 100.35,
		"Сметана, кг":                           100.48,
	} {
		if got := ipcWeeksBulletinsSpotValue(t, rows, series, date); got != want {
			t.Errorf("ряд %q: значение %v, ожидалось %v", series, got, want)
		}
	}
}

// TestIpcWeeksBulletinsReadsNewYear2021 — разбор самого экзотического выпуска дампа: 2.htm
// (2021-01-11, неделя 1–11 января). Шапка его детальной таблицы устроена иначе: первая колонка
// данных — накопленная («11 января 2021 г. к концу декабря 2020 г.»), а недельное значение под
// групповым якорем «к предыдущей дате регистрации» стоит во второй колонке («11 января 2021 г.»);
// разбор берёт колонку по якорю, а не по индексу (ipcWeeksBulletinsValueColumn). Поэтому
// spot-значения товаров закреплены по второй колонке и отличаются от первой: «Картофель» — 103,0
// (накопленная 102,7; текст выпуска «картофель – на 2,7%» называет именно её), «Сметана» — 100,2
// (100,1), «Баранина…» — 100,3 (100,2): сдвиг колонки на первую дал бы другие числа. Агрегат —
// 100,4: в таблице «в процентах» этого выпуска колонки «к предыдущей дате регистрации» нет
// (первая колонка — «С начала января 2021 г.»), разбор берёт первое число строки, а текст
// выпуска называет эту оценку итогом периода: «За период с 1 по 11 января 2021 г. индекс
// потребительских цен … составил 100,4%».
func TestIpcWeeksBulletinsReadsNewYear2021(t *testing.T) {
	date := time.Date(2021, time.January, 11, 0, 0, 0, 0, time.UTC)
	rows := importIpcWeeksBulletinsFixture(t, ipcWeeksBulletinsNewYear2021Fixture, date)
	if len(rows) != 103 {
		t.Fatalf("точек %d, ожидалось 103 (102 товара + агрегат)", len(rows))
	}
	if counts := countBySeries(rows); len(counts) != 103 {
		t.Fatalf("рядов %d, ожидалось 103", len(counts))
	}
	for _, row := range rows {
		if row.valueType != "float64" {
			t.Errorf("ряд %q записан как %s, ожидался float64", row.series, row.valueType)
		}
		if !row.date.Equal(date) {
			t.Errorf("ряд %q: дата %s, ожидалась %s", row.series, row.date.Format("2006-01-02"), date.Format("2006-01-02"))
		}
	}
	for series, want := range map[string]float64{
		ipcWeeksBulletinsAggregateSeries: 100.4,
		"Картофель, кг":                  103.0,
		"Сметана, кг":                    100.2,
		"Баранина (кроме бескостного мяса), кг": 100.3,
	} {
		if got := ipcWeeksBulletinsSpotValue(t, rows, series, date); got != want {
			t.Errorf("ряд %q: значение %v, ожидалось %v (недельная колонка под якорем «к предыдущей дате регистрации»)", series, got, want)
		}
	}
}

// TestIpcWeeksBulletinsReadsWayback2021 — разбор восстановленной недели 19.htm (2021-02-01) из
// Wayback: 102 товара и агрегат. Спот-значения: агрегат 100,0, «Говядина…» 100,2, «Сметана» 99,9
// и extra-ряд «Консервы мясные» 100,0 — строка, которой в XLSX 2022+ нет и которую импортёр
// описывает собственной метой.
func TestIpcWeeksBulletinsReadsWayback2021(t *testing.T) {
	date := time.Date(2021, time.February, 1, 0, 0, 0, 0, time.UTC)
	rows := importIpcWeeksBulletinsFixture(t, ipcWeeksBulletinsWayback21Fixture, date)
	if len(rows) != 103 {
		t.Fatalf("точек %d, ожидалось 103 (102 товара + агрегат)", len(rows))
	}
	if counts := countBySeries(rows); len(counts) != 103 {
		t.Fatalf("рядов %d, ожидалось 103", len(counts))
	}
	for series, want := range map[string]float64{
		ipcWeeksBulletinsAggregateSeries:        100.0,
		"Говядина (кроме бескостного мяса), кг": 100.2,
		"Сметана, кг":                           99.9,
		"Консервы мясные":                       100.0,
	} {
		if got := ipcWeeksBulletinsSpotValue(t, rows, series, date); got != want {
			t.Errorf("ряд %q: значение %v, ожидалось %v", series, got, want)
		}
	}
}

// TestIpcWeeksBulletinsReadsWayback2017 — разбор фрагмента 100.htm (2017-05-22): 37 товаров и
// агрегат. Спот-значения: агрегат 100,0, «Рыба мороженая неразделанная» 100,2, «Яйца куриные»
// 97,4. Здесь же проверяется канонизация 2017 года: имена с единицами («Отопление, Гкал»)
// приходят к тем же рядам, что имена без единиц 2021 года.
func TestIpcWeeksBulletinsReadsWayback2017(t *testing.T) {
	date := time.Date(2017, time.May, 22, 0, 0, 0, 0, time.UTC)
	rows := importIpcWeeksBulletinsFixture(t, ipcWeeksBulletinsWayback17Fixture, date)
	if len(rows) != 38 {
		t.Fatalf("точек %d, ожидалось 38 (37 товаров + агрегат)", len(rows))
	}
	if counts := countBySeries(rows); len(counts) != 38 {
		t.Fatalf("рядов %d, ожидалось 38", len(counts))
	}
	for series, want := range map[string]float64{
		ipcWeeksBulletinsAggregateSeries:   100.0,
		"Рыба мороженая неразделанная, кг": 100.2,
		"Яйца куриные, 10 шт.":             97.4,
		"Отопление, Гкал":                  100.0,
	} {
		if got := ipcWeeksBulletinsSpotValue(t, rows, series, date); got != want {
			t.Errorf("ряд %q: значение %v, ожидалось %v", series, got, want)
		}
	}
}

// TestIpcWeeksBulletinsReadsOld2016To2017 — разбор восьми выпусков старого формата (2016–2017,
// найденных в каталогах d06/d01): раньше падал на якоре «К предыдущей…» (большая буква) и
// дефисных переносах шапки («19 сен-тября»); нормализация Key (нижний регистр, снятие переносов)
// делает эти же документы разбираемыми. Проверяются: разбор без ошибки, агрегат ровно одной
// точкой, спот-значения товара и канонизация имени с переносами («госу- дарственного»).
func TestIpcWeeksBulletinsReadsOld2016To2017(t *testing.T) {
	cases := []struct {
		file string
		date time.Time
	}{
		{ipcWeeksBulletinsOld20160111Fixture, time.Date(2016, time.January, 11, 0, 0, 0, 0, time.UTC)},
		{ipcWeeksBulletinsOld20160118Fixture, time.Date(2016, time.January, 18, 0, 0, 0, 0, time.UTC)},
		{ipcWeeksBulletinsOld20160125Fixture, time.Date(2016, time.January, 25, 0, 0, 0, 0, time.UTC)},
		{ipcWeeksBulletinsOld20160201Fixture, time.Date(2016, time.February, 1, 0, 0, 0, 0, time.UTC)},
		{ipcWeeksBulletinsOld20160919Fixture, time.Date(2016, time.September, 19, 0, 0, 0, 0, time.UTC)},
		{ipcWeeksBulletinsOld20160926Fixture, time.Date(2016, time.September, 26, 0, 0, 0, 0, time.UTC)},
		{ipcWeeksBulletinsOld20170109Fixture, time.Date(2017, time.January, 9, 0, 0, 0, 0, time.UTC)},
		{ipcWeeksBulletinsOld20170123Fixture, time.Date(2017, time.January, 23, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		rows := importIpcWeeksBulletinsFixture(t, tc.file, tc.date)
		if len(rows) == 0 {
			t.Errorf("%s: точек нет", tc.file)
			continue
		}
		ipcWeeksBulletinsSpotValue(t, rows, ipcWeeksBulletinsAggregateSeries, tc.date)
	}
	// Спот-значения выпуска 2016-01-11: «Яйца куриные» 101,0 — колонка «К предыдущей дате
	// регистрации» (вторая: первая колонка этого формата — «11 января 2016г. к концу декабря
	// 2015г.», накопленное с начала года, — как у новогоднего 2.htm 2021 г.), «Плата за жилье…»
	// с переносами («госу- дарственного») канонизируется в имя с единицей.
	date := time.Date(2016, time.January, 11, 0, 0, 0, 0, time.UTC)
	rows := importIpcWeeksBulletinsFixture(t, ipcWeeksBulletinsOld20160111Fixture, date)
	if got := ipcWeeksBulletinsSpotValue(t, rows, "Яйца куриные, 10 шт.", date); got != 101.0 {
		t.Errorf("«Яйца куриные, 10 шт.» 2016-01-11: %v, ожидалось 101,0", got)
	}
	ipcWeeksBulletinsSpotValue(t, rows, "Плата за жилье в домах государственного и муниципального жилищных фондов, м2 общей площади", date)
}

// TestIpcWeeksBulletinsReadsOld2009 — разбор формата 2009 г., первого года недельного ряда: в
// шапке детальной таблицы нет группового якоря «к предыдущей дате регистрации» — стоят сравнения
// дат («6 апреля к 30 марта»), и недельная колонка определяется меткой последнего дня периода.
// Проверяются: разбор без ошибки, агрегат 100,2, товарные значения 101,6 / 100,8 / 100,2 / 99,9
// и extra-ряд «Говядина тушеная» (старого имени в XLSX 2022+ нет).
func TestIpcWeeksBulletinsReadsOld2009(t *testing.T) {
	date := time.Date(2009, time.April, 6, 0, 0, 0, 0, time.UTC)
	rows := importIpcWeeksBulletinsFixture(t, ipcWeeksBulletinsWayback09Fixture, date)
	if len(rows) != 11 {
		t.Fatalf("точек %d, ожидалось 11 (агрегат + 10 товаров)", len(rows))
	}
	if got := ipcWeeksBulletinsSpotValue(t, rows, ipcWeeksBulletinsAggregateSeries, date); got != 100.2 {
		t.Errorf("агрегат 2009-04-06: %v, ожидалось 100,2", got)
	}
	for series, want := range map[string]float64{
		"Яйца куриные, 10 шт.":    101.6,
		"Чай черный байховый, кг": 100.8,
		"Говядина тушеная":        100.2,
		"Хлеб и булочные изделия из пшеничной муки различных сортов, кг": 99.9,
	} {
		if got := ipcWeeksBulletinsSpotValue(t, rows, series, date); got != want {
			t.Errorf("ряд %q 2009-04-06: %v, ожидалось %v", series, got, want)
		}
	}
}

// TestIpcWeeksBulletinsStubFails — страница-заглушка WAF (HTTP-статус у неё 502, а GetFile статус
// не проверяет, поэтому заглушка обязана валиться по контенту): у неё нет титула периода, и
// разбор обязан упасть, а не записать «0 точек». Это же — защита от подмены документа: заглушку
// «Федеральная служба государственной статистики» живые выпуски однажды уже отдавали.
func TestIpcWeeksBulletinsStubFails(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("testdata", ipcWeeksBulletinsStubFixture))
	if err != nil {
		t.Fatal(err)
	}
	_, err = ipcWeeksBulletinsParse(body)
	if err == nil {
		t.Fatal("заглушка: ожидалась ошибка, получен nil")
	}
	if !strings.Contains(err.Error(), "титул") {
		t.Errorf("в ошибке нет разбора титула заглушки: %v", err)
	}
}

// TestIpcWeeksBulletinsNoTableFails — битый мусор: титул периода есть, таблиц нет. Разбор
// обязан упасть (нет агрегата/детальной таблицы), а не вернуть ноль точек молча (§6.4).
func TestIpcWeeksBulletinsNoTableFails(t *testing.T) {
	body := []byte("<!DOCTYPE html><html><head><title>Об оценке индекса потребительских цен с 3 по 9 августа 2021 года</title></head><body><p>нет таблиц</p></body></html>")
	if _, err := ipcWeeksBulletinsParse(body); err == nil {
		t.Fatal("документ без таблиц: ожидалась ошибка, получен nil")
	}
	if _, err := ipcWeeksBulletinsParse([]byte("совсем не html")); err == nil {
		t.Fatal("мусор: ожидалась ошибка, получен nil")
	}
}

// TestIpcWeeksBulletinsDateShiftFails — сдвиг даты: подменяем у выпуска ожидаемую дату точкой из
// другого времени. Дата титула обязана совпасть с датой записи списка источников (§3.3): буфер
// пере-нумеровался бы — и точки ушли бы не на свою дату, а молча.
func TestIpcWeeksBulletinsDateShiftFails(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("testdata", ipcWeeksBulletinsLive2021Fixture))
	if err != nil {
		t.Fatal(err)
	}
	rec := &appendRecorder{}
	wrong := ipcWeeksBulletinsSource{URL: "test://144", Date: time.Date(2021, time.August, 16, 0, 0, 0, 0, time.UTC)}
	err = ipcWeeksBulletinsAppend(wrong)(body, rec)
	if err == nil {
		t.Fatal("сдвиг даты: ожидалась ошибка, получен nil")
	}
	if !strings.Contains(err.Error(), "не совпала") {
		t.Errorf("в ошибке нет причины «не совпала»: %v", err)
	}
	if len(rec.rows) != 0 {
		t.Errorf("при ошибке записано %d строк, ожидалось 0", len(rec.rows))
	}
}

// ipcWeeksBulletinsMiniAggregateRow — строка агрегата минимального выпуска (значение 100,00):
// её подставляет в документ ветка «нет строки агрегата» или не подставляет вовсе.
const ipcWeeksBulletinsMiniAggregateRow = "<tr><td>Индекс потребительских цен (оценка)</td><td>100,00</td><td>99,98</td></tr>"

// ipcWeeksBulletinsMiniDoc — минимальный выпуск в разметке Word-HTML для веток отказа, которых
// в полной фикстуре нет: титул, таблица с переданной строкой агрегата, детальная таблица с
// шапкой (rowspan+colspan) и переданными строками товаров. Кодировка — UTF-8 без meta:
// charset.NewReader распознаёт её sniffing'ом (живые выпуски приходят в cp1251, здесь важна не
// кодировка, а разметка).
func ipcWeeksBulletinsMiniDoc(title, aggregateRow, productRows string) string {
	return fmt.Sprintf(`<!DOCTYPE html><html><head><title>%s</title></head><body>
<table>%s</table>
<table><thead>
<tr><td></td><td>Изменения цен</td><td></td></tr>
<tr><td rowspan="2"></td><td>9 августа</td><td>9 августа 2021 г. к концу</td></tr>
<tr><td>к предыдущей дате регистрации</td><td>декабря 2020 г.</td></tr>
</thead><tbody>
%s
</tbody></table></body></html>`, title, aggregateRow, productRows)
}

// TestIpcWeeksBulletinsNoDataCellNotPoint — «…» (подпись источника «данных не имеется») точкой
// не становится (§6.7): строка «Сметана» пропускается, её соседи читаются как обычно. Ошибкой
// такая ячейка тоже не является — пропуск данных у одного товара не конец таблицы.
func TestIpcWeeksBulletinsNoDataCellNotPoint(t *testing.T) {
	body := []byte(ipcWeeksBulletinsMiniDoc(
		"Об оценке индекса потребительских цен с 3 по 9 августа 2021 года", ipcWeeksBulletinsMiniAggregateRow,
		"<tr><td>Говядина (кроме бескостного мяса)</td><td>100,35</td><td>106,55</td></tr>\n"+
			"<tr><td>Сметана</td><td>…</td><td>102,80</td></tr>\n"))
	rec := &appendRecorder{}
	if err := ipcWeeksBulletinsAppend(ipcWeeksBulletinsSource{URL: "test://mini", Date: time.Date(2021, time.August, 9, 0, 0, 0, 0, time.UTC)})(body, rec); err != nil {
		t.Fatalf("«…» — подпись «данных не имеется», ошибки быть не должно: %v", err)
	}
	counts := countBySeries(rec.rows)
	if len(rec.rows) != 2 || counts["Говядина (кроме бескостного мяса), кг"] != 1 {
		t.Fatalf("точки %v: ожидался агрегат и «Говядина…», «Сметана» с «…» — не точка", counts)
	}
	if counts["Сметана, кг"] != 0 {
		t.Errorf("«…» записана точкой: %v", counts)
	}
}

// TestIpcWeeksBulletinsErrorBranches — ветки отказа разбора (§6.4), которых в полной фикстуре
// нет: без них дрейф разметки дал бы «ноль точек» вместо внятной ошибки, а мусор в ячейке стал
// бы точкой ряда. Точки в батч не пишутся: разбор идёт целиком до первой записи.
func TestIpcWeeksBulletinsErrorBranches(t *testing.T) {
	const title = "Об оценке индекса потребительских цен с 3 по 9 августа 2021 года"
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "значение не число",
			body: ipcWeeksBulletinsMiniDoc(title, ipcWeeksBulletinsMiniAggregateRow,
				"<tr><td>Говядина (кроме бескостного мяса)</td><td>нет</td><td>106,55</td></tr>"),
			want: "не число",
		},
		{
			name: "нет строки агрегата",
			body: ipcWeeksBulletinsMiniDoc(title, "",
				"<tr><td>Говядина (кроме бескостного мяса)</td><td>100,35</td><td>106,55</td></tr>"),
			want: "не найдена",
		},
		{
			name: "нет строк товаров",
			body: ipcWeeksBulletinsMiniDoc(title, ipcWeeksBulletinsMiniAggregateRow, ""),
			want: "строки данных не найдены",
		},
		{
			name: "все ячейки значений без данных",
			body: ipcWeeksBulletinsMiniDoc(title, ipcWeeksBulletinsMiniAggregateRow,
				"<tr><td>Говядина (кроме бескостного мяса)</td><td>…</td><td>106,55</td></tr>"),
			want: "нет ни одной строки товара",
		},
		{
			name: "титул без периода",
			body: ipcWeeksBulletinsMiniDoc("Об оценке индекса потребительских цен", ipcWeeksBulletinsMiniAggregateRow,
				"<tr><td>Говядина (кроме бескостного мяса)</td><td>100,35</td><td>106,55</td></tr>"),
			want: "период",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &appendRecorder{}
			err := ipcWeeksBulletinsAppend(ipcWeeksBulletinsSource{URL: "test://mini", Date: time.Date(2021, time.August, 9, 0, 0, 0, 0, time.UTC)})([]byte(tt.body), rec)
			if err == nil {
				t.Fatalf("ожидалась ошибка, получено %d строк", len(rec.rows))
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("в ошибке нет %q: %v", tt.want, err)
			}
			if len(rec.rows) != 0 {
				t.Errorf("при ошибке записано %d строк, ожидалось 0", len(rec.rows))
			}
		})
	}
}

// TestIpcWeeksBulletinsUnknownProductWarns — имя вне карты канонизации и вне extra-списка —
// ожидаемое изменение источника (§6.6): предупреждение с именем товара, точка записывается под
// исходным именем. Ошибкой это быть не может: источник заморожен, но его дрейф не должен
// молча терять строки.
func TestIpcWeeksBulletinsUnknownProductWarns(t *testing.T) {
	var logged bytes.Buffer
	prevOut := log.StandardLogger().Out
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	const unknown = "Товар, которого нет в каталоге"
	body := []byte(ipcWeeksBulletinsMiniDoc(
		"Об оценке индекса потребительских цен с 3 по 9 августа 2021 года", ipcWeeksBulletinsMiniAggregateRow,
		"<tr><td>"+unknown+"</td><td>100,35</td><td>106,55</td></tr>\n"+
			"<tr><td>Сметана</td><td>100,48</td><td>102,80</td></tr>\n"))
	rec := &appendRecorder{}
	if err := ipcWeeksBulletinsAppend(ipcWeeksBulletinsSource{URL: "test://mini", Date: time.Date(2021, time.August, 9, 0, 0, 0, 0, time.UTC)})(body, rec); err != nil {
		t.Fatalf("товар вне каталога — ожидаемое изменение источника, ошибки быть не должно: %v", err)
	}
	if !strings.Contains(logged.String(), unknown) {
		t.Errorf("в предупреждении нет имени товара %q: %q", unknown, logged.String())
	}
	if counts := countBySeries(rec.rows); counts[unknown] != 1 || counts["Сметана, кг"] != 1 {
		t.Errorf("точки товаров %v: неизвестный ряд должен быть записан под исходным именем", counts)
	}
}

// TestIpcWeeksBulletinsSourcesInvariants — инварианты списка 502 выпусков (спека §2, приложения
// А–Д; расширения 2026-10-16/17 — приложения В и Д спеки и систематическая разведка
// scripts/harvest.py): 48 живых недельных выпусков (47 недель 2021 + неделя 22–28.12.2020) и
// 454 Wayback-выпуска 2009–2021. Поток «Оперативной информации» мигрирует между каталогами
// Stg/dNN по годам (d02 в 2017/2021, d03→d01 в 2018, d04 в 2019, d05 в 2020, d06 в 2015–2017),
// поэтому разведка «только d02» находила недели лишь тех лет, когда поток жил в d02, а полное
// дерево Stg/ добавило 202 недели 2015–2020, аудит пропусков (2026-10-17) — ещё одну
// (2016-11-21: в титуле выпуска опечатка «с15 по 21», классификатор её не разбирал), а проверка
// «с самого дня существования буфера» (2026-10-17) сняла фильтр CDX from=2016 и нашла 214 недель
// 2009–2015 — ряд недельной оценки начинается в апреле 2009-го. Бакеты ниже —
// по году ДОКУМЕНТА (дате точки), не снимка. URL уникальны;
// даты строго возрастают; у Wayback-адресов — форма id_ (сырые байты документа, без панели
// Wayback); недельный блок 2020-12-28…2021-12-27 покрыт всеми неделями: обычный шаг 6–8 дней
// (5 и 9 — стыки с Wayback-выпусками), единственный 14-дневный шаг — новогодний
// (2020-12-28 → 2021-01-11; неделя 1–11 января вышла одним выпуском 2.htm), а шаг больше 14 дней
// означал бы потерянный выпуск; 2021-02-01/08/15/24 — те самые удалённые из живого буфера недели,
// ради которых и снят дамп.
func TestIpcWeeksBulletinsSourcesInvariants(t *testing.T) {
	if len(ipcWeeksBulletinsSources) != 502 {
		t.Fatalf("выпусков %d, ожидалось 502", len(ipcWeeksBulletinsSources))
	}
	live := 0
	waybackByYear := map[int]int{}
	urls := make(map[string]bool, len(ipcWeeksBulletinsSources))
	prev := time.Time{}
	weekly2021 := make([]time.Time, 0, 52)
	pre2020Tail := make([]string, 0, 46)
	for _, source := range ipcWeeksBulletinsSources {
		switch {
		case strings.HasPrefix(source.URL, "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/"):
			live++
		case strings.Contains(source.URL, "web.archive.org"):
			// Бакеты — по году ДОКУМЕНТА (дате точки), а не года снимка: недели 2015–2020
			// сохранили снимки 2016–2024 гг., и по году снимка они попали бы не в свой бакет.
			waybackByYear[source.Date.Year()]++
		}
		if !strings.HasPrefix(source.URL, "http") {
			t.Errorf("URL %q без схемы", source.URL)
		}
		if urls[source.URL] {
			t.Errorf("URL %q встречается дважды", source.URL)
		}
		urls[source.URL] = true
		if !prev.IsZero() && !source.Date.After(prev) {
			t.Errorf("даты не возрастают: %s после %s", source.Date.Format("2006-01-02"), prev.Format("2006-01-02"))
		}
		prev = source.Date
		if source.Date.Year() >= 2020 {
			// Восстановленные Wayback-недели 2020 до 2020-12-28 (2020-01-09 … 2020-12-21) пинются
			// числом и границами; сплошное покрытие проверяется от 2020-12-28, самого старого
			// документа живого буфера.
			if source.Date.Before(time.Date(2020, time.December, 28, 0, 0, 0, 0, time.UTC)) {
				pre2020Tail = append(pre2020Tail, source.Date.Format("2006-01-02"))
			} else {
				weekly2021 = append(weekly2021, source.Date)
			}
		}
		if strings.Contains(source.URL, "web.archive.org") && !strings.Contains(source.URL, "id_/") {
			t.Errorf("у Wayback-адреса %q нет формы id_: панель Wayback переписала бы документ", source.URL)
		}
	}
	if want := map[int]int{2009: 30, 2010: 30, 2011: 33, 2012: 32, 2013: 27, 2014: 24, 2015: 46, 2016: 44, 2017: 45, 2018: 51, 2019: 42, 2020: 46, 2021: 4}; !reflect.DeepEqual(waybackByYear, want) {
		t.Errorf("состав Wayback по годам документа: %v, ожидалось %v", waybackByYear, want)
	}
	if live != 48 {
		t.Errorf("живых выпусков %d, ожидалось 48", live)
	}
	if len(pre2020Tail) != 46 {
		t.Fatalf("восстановленных недель 2020 до живого буфера: %d, ожидалось 46", len(pre2020Tail))
	}
	if pre2020Tail[0] != "2020-01-09" || pre2020Tail[len(pre2020Tail)-1] != "2020-12-21" {
		t.Errorf("границы восстановленных недель 2020: %s…%s, ожидалось 2020-01-09 … 2020-12-21",
			pre2020Tail[0], pre2020Tail[len(pre2020Tail)-1])
	}
	if len(weekly2021) != 52 {
		t.Fatalf("недельный блок 2021: точек %d, ожидалось 52", len(weekly2021))
	}
	if first, last := weekly2021[0].Format("2006-01-02"), weekly2021[len(weekly2021)-1].Format("2006-01-02"); first != "2020-12-28" || last != "2021-12-27" {
		t.Errorf("недельный блок покрывает %s…%s, ожидалось 2020-12-28…2021-12-27", first, last)
	}
	longGaps := 0
	for i := 1; i < len(weekly2021); i++ {
		gap := weekly2021[i].Sub(weekly2021[i-1])
		if gap > 14*24*time.Hour {
			t.Errorf("разрыв %v между %s и %s: выпуск потерян", gap, weekly2021[i-1].Format("2006-01-02"), weekly2021[i].Format("2006-01-02"))
		}
		if gap == 14*24*time.Hour {
			longGaps++
			if got := weekly2021[i-1].Format("2006-01-02") + " → " + weekly2021[i].Format("2006-01-02"); got != "2020-12-28 → 2021-01-11" {
				t.Errorf("14-дневный шаг %s: ожидался единственный новогодний 2020-12-28 → 2021-01-11 (неделя 1–11 января одним выпуском)", got)
			}
		}
	}
	if longGaps != 1 {
		t.Errorf("14-дневных шагов %d, ожидался один новогодний: неделя 1–11 января вышла одним выпуском 2.htm", longGaps)
	}
	got := make(map[string]bool, len(weekly2021))
	for _, date := range weekly2021 {
		got[date.Format("2006-01-02")] = true
	}
	for _, date := range []string{"2021-02-01", "2021-02-08", "2021-02-15", "2021-02-24"} {
		if !got[date] {
			t.Errorf("в списке нет восстановленной недели %s (живой буфер её не отдаёт)", date)
		}
	}
}

// TestIpcWeeksBulletinsNewSources — чистая функция гейта (§3.5 шаг 2): из списка и множества
// известных дат вычисляются недостающие выпуски в хронологическом порядке. Сценарии «всё
// известно» (пусто → прогон no-op) и «известно всё, кроме одной середины» пинятся здесь, а не
// живым прогоном: без базы и без сети.
func TestIpcWeeksBulletinsNewSources(t *testing.T) {
	all := ipcWeeksBulletinsNewSources(ipcWeeksBulletinsSources, map[string]bool{})
	if len(all) != len(ipcWeeksBulletinsSources) {
		t.Fatalf("недостающих выпусков %d, ожидалось %d", len(all), len(ipcWeeksBulletinsSources))
	}
	if !sort.SliceIsSorted(all, func(i, j int) bool { return all[i].Date.Before(all[j].Date) }) {
		t.Error("недостающие выпуски не в хронологическом порядке")
	}

	known := make(map[string]bool, len(ipcWeeksBulletinsSources))
	for _, source := range ipcWeeksBulletinsSources {
		known[source.Date.Format("2006-01-02")] = true
	}
	if fresh := ipcWeeksBulletinsNewSources(ipcWeeksBulletinsSources, known); len(fresh) != 0 {
		t.Fatalf("все даты известны: недостающих %d, ожидалось 0", len(fresh))
	}

	delete(known, "2021-02-15")
	fresh := ipcWeeksBulletinsNewSources(ipcWeeksBulletinsSources, known)
	if len(fresh) != 1 {
		t.Fatalf("пропала одна дата: недостающих %d, ожидалась 1", len(fresh))
	}
	if got := fresh[0].Date.Format("2006-01-02"); got != "2021-02-15" {
		t.Errorf("недостающий выпуск %s, ожидался 2021-02-15", got)
	}
}

// TestIpcWeeksBulletinsMetaMatchesFixtures — двусторонний контракт каталога на фикстурах
// (спека §3.2.2): каждый разобранный ряд описан (describedSeries проверяет и заполненность, и
// отсутствие дублей описаний), а описанное сверх разобранного — ровно extra-ряды, которых в
// четырёх фикстурах нет: их издания (245.htm и другие выпуски вне testdata) остаются за
// контрактом и пин литералами ниже. Так контракт остаётся двусторонним, а extra-ряд не может
// пропасть из каталога незамеченным.
func TestIpcWeeksBulletinsMetaMatchesFixtures(t *testing.T) {
	var parsed []string
	seen := make(map[string]bool)
	parsed = append(parsed, ipcWeeksBulletinsAggregateSeries)
	seen[ipcWeeksBulletinsAggregateSeries] = true
	for _, fixture := range []struct {
		file string
		date time.Time
	}{
		{ipcWeeksBulletinsLive2021Fixture, time.Date(2021, time.August, 9, 0, 0, 0, 0, time.UTC)},
		{ipcWeeksBulletinsWayback21Fixture, time.Date(2021, time.February, 1, 0, 0, 0, 0, time.UTC)},
		{ipcWeeksBulletinsWayback17Fixture, time.Date(2017, time.May, 22, 0, 0, 0, 0, time.UTC)},
		// Старый формат 2009 г.: шапка детальной таблицы «6 апреля к 30 марта», группового якоря
		// «к предыдущей дате регистрации» нет; даёт extra-ряд «Говядина тушеная».
		{ipcWeeksBulletinsWayback09Fixture, time.Date(2009, time.April, 6, 0, 0, 0, 0, time.UTC)},
	} {
		for _, row := range importIpcWeeksBulletinsFixture(t, fixture.file, fixture.date) {
			if !seen[row.series] {
				seen[row.series] = true
				parsed = append(parsed, row.series)
			}
		}
	}

	described := describedSeries(t, ipcWeeksBulletinsSeriesMeta())
	for _, series := range []string{"Проживание в гостинице", "Сорочка верхняя для мальчиков школьного возраста"} {
		if !described[series] {
			t.Errorf("extra-ряд %q не описан в каталоге", series)
		}
		delete(described, series)
	}
	requireParsedDescribed(t, parsed, described)
}

// TestIpcWeeksBulletinsProductMetaShared — текст меты общего товара у двух недельных импортёров
// совпадает byte-в-byte (§3.2): иначе upsert каталога зависел бы от порядка прогонов, а
// v_series_catalog показывал бы то одну, то другую редакцию описания.
func TestIpcWeeksBulletinsProductMetaShared(t *testing.T) {
	xlsx := make(map[string]util.SeriesMeta, len(ipcWeeksProducts))
	for _, meta := range ipcWeeksSeriesMeta() {
		xlsx[meta.Series] = meta
	}
	shared := 0
	for _, meta := range ipcWeeksBulletinsSeriesMeta() {
		other, ok := xlsx[meta.Series]
		if !ok {
			continue
		}
		shared++
		if other != meta {
			t.Errorf("описание товара %q разошлось между импортёрами:\nXLSX: %+v\nбюллетени: %+v", meta.Series, other, meta)
		}
	}
	if shared == 0 {
		t.Fatal("ни один товар недельных бюллетеней не описан XLSX-импортёром: сверять нечего")
	}
}
