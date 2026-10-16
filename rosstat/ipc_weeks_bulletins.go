package rosstat

// Импортёр недельных ИПЦ Росстата до 2022 — таблица rosstat_ipc_weeks_bulletins (спека
// 2026-10-16-rosstat-ipc-weeks-bulletins-design, §3): недельные оценки индекса потребительских
// цен из HTML-бюллетеней «Оперативной информации» Росстата. Ряд rosstat_ipc_weeks начинается
// 2022-01-10 (табличная выгрузка Nedel_ipc.xlsx существует только с 2022), а официальная
// недельная оценка публиковалась бюллетенями с 2016 года; физически сохранились 48 живых
// недельных выпусков (47 недель 2021 и неделя 22–28.12.2020, дата точки 2020-12-28), 4 недели
// 2021, удалённые из живого буфера (Wayback), и 19 фрагментов 2017
// (Wayback) — всего 71 документ (спасательный дамп build/ipc-pre2022-rescue, шаг 0 плана).
// Артефакт импортёра — не один файл, а набор выпусков, поэтому у импортёра свой Import()
// (прецедент — cbr_dip_bulletin): разбор — чистая ipcWeeksBulletinsParse (байты HTML, §6.1),
// файл каждого выпуска качает шаблон util.ClickHouseImport (DataUrl + BodyFunc) — иначе не
// получились бы ни винтажный архив версии документа, ни строка source_publications
// (стандарт §9.6).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/PuerkitoBio/goquery"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/html/charset"
)

const (
	// rosstatIPCWeeksBulletinsTable — имя таблицы и Name() импортёра (совпадают, как у
	// большинства пакета): CLICKHOUSE_IMPORT_STAT и id шага dagu/rosstat.yaml — та же строка.
	rosstatIPCWeeksBulletinsTable = "rosstat_ipc_weeks_bulletins"

	// ipcWeeksBulletinsDdl — DDL таблицы (спека §3.1) в %s-форме шаблона: %s — TableName.
	// Схема повторяет соседнюю rosstat_ipc_weeks, но значение — Float64 (правило 5 AGENTS.md для
	// новых ценовых значений; у legacy-таблицы Float32): витрина v_rosstat_macro оборачивает
	// обе в toFloat64, типы не смешиваются. Ключ (name, date) — ряд и дата точки; повторный
	// прогон недели перезаписывает свои точки, а не добавляет дубли.
	ipcWeeksBulletinsDdl = `CREATE TABLE IF NOT EXISTS %s (
    name LowCardinality(String),  -- имя ряда: канонический товар или объявленный кодом ряд (стандарт §6.8)
    date Date,                    -- последний день недели регистрации цен из титула выпуска (§3.3)
    percent Float64               -- индекс, % к предыдущей дате регистрации (100 = без изменения)
) ENGINE = ReplacingMergeTree ORDER BY (name, date);
`

	// ipcWeeksBulletinsCountPoints — количественный чек версии (стандарт §9.2): число точек
	// своей таблицы. FINAL обязателен — иначе count() считал бы несмёрженные части. Число растёт
	// с каждым выпуском, поэтому решением о no-op оно не служит (это делает гейт, §3.5), но
	// объявлено: без него не включатся архив версий и строки source_publications (§9.6).
	ipcWeeksBulletinsCountPoints = "SELECT count() FROM rosstat_ipc_weeks_bulletins FINAL"

	// ipcWeeksBulletinsKnownDatesQuery — гейт прогона (спека §3.5, шаг 2): даты, уже лежащие в
	// таблице. Гейт заменяет HEAD-префлайт набора из 71 артефакта: PublicationCountPoints
	// считает точки всей таблицы, и чек известной версии не сошёлся бы ни разу — каждый прогон
	// пере-импортировал бы все 71 документ. SELECT DISTINCT отвечает на тот же вопрос («эта
	// неделя уже импортирована?») по своим данным и не делает ни одного сетевого обращения на
	// повторном прогоне. Самолечение «неделя пропала из таблицы» работает: гейт смотрит на
	// данные, а не на реестр версий (спека §3.5).
	ipcWeeksBulletinsKnownDatesQuery = "SELECT DISTINCT date FROM rosstat_ipc_weeks_bulletins FINAL"

	// ipcWeeksBulletinsDateCountQuery — число точек недели: проверка, что успешный вызов шаблона
	// записал выпуск (спека §3.5, шаг 3). FINAL обязателен (стандарт §9.2). Дата уходит
	// плейсхолдером драйвера, а не литералом: она — данные списка источников, и в тексте запроса
	// ей не место (прецедент значения-параметра — cbrDipReleaseCountQuery, cbr/dip_bulletin.go).
	ipcWeeksBulletinsDateCountQuery = "SELECT count() FROM rosstat_ipc_weeks_bulletins FINAL WHERE date = ?"

	// ipcWeeksBulletinsAggregateSeries — имя агрегатного ряда (константа, спека §3.2.1): недельная
	// оценка ИПЦ из строки «Индекс потребительских цен (оценка)» шапки выпуска, колонка
	// «К предыдущей дате регистрации цен». В XLSX 2022+ агрегатной строки нет, поэтому ряд
	// существует только за 2017/2021 — оговорка в его description (series_meta.go).
	ipcWeeksBulletinsAggregateSeries = "Индекс потребительских цен (недельная оценка)"

	// ipcWeeksBulletinsAggregateAnchor — якорь строки агрегата в шапке-таблице «в процентах».
	// Разметка ищется по признаку, а не по номеру строки (§6.3): состав шапки меняется между
	// выпусками, а якорь — нет. Сравнение — по склейке без пробелов (§6.5): Word разбивает
	// заголовок на элементы (в реальном заголовке 144.htm встречается даже опечатка «ценс 25»).
	ipcWeeksBulletinsAggregateAnchor = "Индекс потребительских цен (оценка)"

	// ipcWeeksBulletinsChangesAnchor — якорь шапки детальной таблицы: её второй столбец шапки
	// подписан «Изменения цен». Таблица ищется по паре якорей — она и «к предыдущей дате
	// регистрации» ниже (спека §3.4).
	ipcWeeksBulletinsChangesAnchor = "Изменения цен"

	// ipcWeeksBulletinsPreviousAnchor — групповой заголовок первых трёх колонок данных
	// («к предыдущей дате регистрации»): первая из них — недельное значение, ради которого
	// импортёр и существует. Рядом с ним в шапке обязана стоять метка даты (день и месяц
	// последнего дня периода) — по ней колонка значений подтверждается (§3.3).
	ipcWeeksBulletinsPreviousAnchor = "к предыдущей дате регистрации"

	// ipcWeeksBulletinsNoData — подпись источника «данных не имеется». Ей (и её вариантам со
	// сноской «…2)», «…3)») помечены ячейки, значений по которым нет: пустая ячейка, «…», «-»,
	// «–» — не точка ряда (§6.7). Ячейка без данных не обрывает строку и таблицу: пропуск у
	// одного товара — не конец списка, ниже стоят строки других товаров.
	ipcWeeksBulletinsNoData = "…"

	// ipcWeeksBulletinsInfoTerminator, ipcWeeksBulletinsEstimateTerminator — начала строк-сносок,
	// замыкающих детальную таблицу в части выпусков (в трёх фикстурах они лежат отдельной
	// таблицей ниже, но разбор обязан останавливаться на них и внутри таблицы данных). Признак
	// ищется по началу строки, а не по позиции (§6.3): у выпуска может не быть ни одной сноски,
	// а может быть несколько.
	ipcWeeksBulletinsInfoTerminator     = "Информация подготовлена"
	ipcWeeksBulletinsEstimateTerminator = "Оценка индекса потребительских цен за период"
)

// ipcWeeksBulletinsTerminators — начала строк, на которых разбор детальной таблицы
// останавливается: две сноски и нумерованные примечания («1) …», «2) …», «# …»). Список — часть
// разбора, поэтому рядом с парсером, а не в теле функции (§4.5).
var ipcWeeksBulletinsTerminators = []string{
	ipcWeeksBulletinsInfoTerminator,
	ipcWeeksBulletinsEstimateTerminator,
	"1)", "2)", "#",
}

// ipcWeeksBulletinsSource — выпуск источника: URL документа и ожидаемая дата точки (последний
// день периода из титула). Дата — часть объявления списка: разбор сверяет её с титулом
// скачанного документа, и расхождение (буфер пере-нумеровался, Wayback отдал чужой снимок)
// становится ошибкой выпуска, а не тихой записью точек не на ту дату (§3.3).
type ipcWeeksBulletinsSource struct {
	URL  string
	Date time.Time
}

// ipcWeeksBulletinsSources — все 80 выпусков (спека §2, приложения А и Б; расширение 2026-10-16 —
// приложение В), отсортированные по дате точки. Литералы кода, а не разбор страницы-каталога:
// источника-каталога у этого архива нет, а спасательный дамп зафиксировал точные адреса и даты
// (тест-инварианты: 48 живых + 4 Wayback-2021 + 25 Wayback-2017 + 3 Wayback-2020, уникальные URL,
// строго возрастающие даты). Форма id_ у Wayback-адресов обязательна: обычный режим вклеивает в
// страницу свою панель и переписывает ссылки, а id_ отдаёт сырые байты документа. Первым
// семейством идёт 2017 (ранние даты), в 2021 четыре февраля вклиниваются между живыми выпусками
// января и марта — дат этих недель в живом буфере уже нет, их сохранил только Wayback (спека §2.2).
// Девять выпусков расширения (2017×6, 2020×3) найдены разведкой Wayback-корпуса буфера (приложение
// В спеки): их версии уже лежат в винтажном архиве, поэтому и живой импорт, и restore берут их
// оттуда по этим же URL.
var ipcWeeksBulletinsSources = []ipcWeeksBulletinsSource{
	{URL: "https://web.archive.org/web/20170325031428id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/56.htm", Date: time.Date(2017, time.March, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170403234555id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/61.htm", Date: time.Date(2017, time.March, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170429233711id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/82.htm", Date: time.Date(2017, time.April, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170508035201id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/87.htm", Date: time.Date(2017, time.May, 2, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170515170702id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/90.htm", Date: time.Date(2017, time.May, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170519061804id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/93.htm", Date: time.Date(2017, time.May, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170524185422id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/100.htm", Date: time.Date(2017, time.May, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170603044855id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/108.htm", Date: time.Date(2017, time.May, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170610040613id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/111.htm", Date: time.Date(2017, time.June, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170618205104id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/113.htm", Date: time.Date(2017, time.June, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170623204140id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/121.htm", Date: time.Date(2017, time.June, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170701041113id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/130.htm", Date: time.Date(2017, time.June, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170708041326id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/134.htm", Date: time.Date(2017, time.July, 3, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170715084509id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/137.htm", Date: time.Date(2017, time.July, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170722010612id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/142.htm", Date: time.Date(2017, time.July, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170728222427id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/153.htm", Date: time.Date(2017, time.July, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170804230052id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/155.htm", Date: time.Date(2017, time.July, 31, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170812041920id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/158.htm", Date: time.Date(2017, time.August, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170816212517id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/163.htm", Date: time.Date(2017, time.August, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170826103739id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/169ozen23.htm", Date: time.Date(2017, time.August, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170831031543id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/176.htm", Date: time.Date(2017, time.August, 28, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170908130745id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/179.htm", Date: time.Date(2017, time.September, 4, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170916034320id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/182.htm", Date: time.Date(2017, time.September, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170923134535id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/190.htm", Date: time.Date(2017, time.September, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170929233425id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/198.htm", Date: time.Date(2017, time.September, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20201124084711id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/213.htm", Date: time.Date(2020, time.November, 16, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20201211031227id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/228.htm", Date: time.Date(2020, time.December, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20201224012041id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/237.htm", Date: time.Date(2020, time.December, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/245.htm", Date: time.Date(2020, time.December, 28, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/2.htm", Date: time.Date(2021, time.January, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/5.htm", Date: time.Date(2021, time.January, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/11.htm", Date: time.Date(2021, time.January, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20210203175345id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/19.htm", Date: time.Date(2021, time.February, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20210210181014id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/24.htm", Date: time.Date(2021, time.February, 8, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20210217183953id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/29.htm", Date: time.Date(2021, time.February, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20210226182409id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/36.htm", Date: time.Date(2021, time.February, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/39.htm", Date: time.Date(2021, time.March, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/43.htm", Date: time.Date(2021, time.March, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/47.htm", Date: time.Date(2021, time.March, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/51.htm", Date: time.Date(2021, time.March, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/58.htm", Date: time.Date(2021, time.March, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/63.htm", Date: time.Date(2021, time.April, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/65.htm", Date: time.Date(2021, time.April, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/71.htm", Date: time.Date(2021, time.April, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/80.htm", Date: time.Date(2021, time.April, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/82.htm", Date: time.Date(2021, time.May, 4, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/85.htm", Date: time.Date(2021, time.May, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/87.htm", Date: time.Date(2021, time.May, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/97.htm", Date: time.Date(2021, time.May, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/100.htm", Date: time.Date(2021, time.May, 31, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/104.htm", Date: time.Date(2021, time.June, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/108.htm", Date: time.Date(2021, time.June, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/111.htm", Date: time.Date(2021, time.June, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/120.htm", Date: time.Date(2021, time.June, 28, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/124.htm", Date: time.Date(2021, time.July, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/126.htm", Date: time.Date(2021, time.July, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/130.htm", Date: time.Date(2021, time.July, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/138.htm", Date: time.Date(2021, time.July, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/140.htm", Date: time.Date(2021, time.August, 2, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/144.htm", Date: time.Date(2021, time.August, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/148.htm", Date: time.Date(2021, time.August, 16, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/157.htm", Date: time.Date(2021, time.August, 23, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/159.htm", Date: time.Date(2021, time.August, 30, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/163.htm", Date: time.Date(2021, time.September, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/166.htm", Date: time.Date(2021, time.September, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/171.htm", Date: time.Date(2021, time.September, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/178.htm", Date: time.Date(2021, time.September, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/182.htm", Date: time.Date(2021, time.October, 4, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/184.htm", Date: time.Date(2021, time.October, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/188.htm", Date: time.Date(2021, time.October, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/196.htm", Date: time.Date(2021, time.October, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/199.htm", Date: time.Date(2021, time.November, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/201.htm", Date: time.Date(2021, time.November, 8, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/203.htm", Date: time.Date(2021, time.November, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/212.htm", Date: time.Date(2021, time.November, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/216.htm", Date: time.Date(2021, time.November, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/220.htm", Date: time.Date(2021, time.December, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/223.htm", Date: time.Date(2021, time.December, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/232.htm", Date: time.Date(2021, time.December, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/237.htm", Date: time.Date(2021, time.December, 27, 0, 0, 0, 0, time.UTC)},
}

// ipcWeeksBulletinsPoint — точка выпуска: ряд, дата и значение недельного индекса. Значение
// остаётся float64 до записи в батч — колонка таблицы тоже Float64 (спека §3.1). Имя ряда уже
// канонизировано разбором: в таблицу попадают только имена из карты канонизации, extra-списка
// или (с предупреждением) исходные имена строк.
type ipcWeeksBulletinsPoint struct {
	series string
	date   time.Time
	value  float64
}

// ipcWeeksBulletinsSeriesBySourceName — карта канонизации имён (стандарт §6.8: имена рядов
// объявляет код, а не заголовок источника): имя строки детальной таблицы выпуска → имя ряда
// каталога. Построена по реальным фикстурам (144.htm 2021-08-09, 19.htm 2021-02-01, 100.htm
// 2017-05-22): имена бюллетеней — без единиц («Говядина (кроме бескостного мяса)»), в XLSX
// Nedel_ipc.xlsx 2022+ — с единицами («…, кг»), поэтому каноническое имя это ipcWeeksProducts,
// начинающееся с имени бюллетеня (сравнение — по склейке без пробелов, см. ниже). Часть имён
// совпадает буквально («Бензин автомобильный», «Услуги по снабжению электроэнергией»), часть
// дополняется единицей, а несколько строк источников названы иначе, чем тот же товар в XLSX
// («Творог жирный» = «Творог, кг», «Электроснабжение» = «Услуги по снабжению электроэнергией»);
// их канонизация — решение кода, зафиксированное ниже. Где строка источника совпадает сразу с
// несколькими каноническими именами (дозы лекарств, «Водка»), выбрано имя из первого XLSX-года
// (2022) — ближайшего продолжения бюллетеней; выбор отмечен комментарием. Строки, у которых
// канонического соответствия нет, объявлены extra-списком ниже: они остаются под именем
// источника (это стабильный идентификатор ряда, §3.2).
//
// Ключи карты — имена, как их отдаёт разбор (пробелы схлопнуты). Смещение пробелов внутри слов
// (Word рвёт заголовки на элементы: «Плиты древесностружечные, ориентированно- стружечные»)
// разбор не видит: поиск идёт по ключу без пробелов (ipcWeeksBulletinsSeriesKeys).
var ipcWeeksBulletinsSeriesBySourceName = map[string]string{
	"Аллохол": "Аллохол, 50 таблеток",
	"Баранина (кроме бескостного мяса)": "Баранина (кроме бескостного мяса), кг",
	// Бензин: точное каноническое имя (без единицы) — агрегат по маркам, а не АИ-92/95/98.
	"Бензин автомобильный":                                  "Бензин автомобильный",
	"Брюки для детей школьного возраста из джинсовой ткани": "Брюки для детей школьного возраста из джинсовой ткани, шт.",
	"Бумага туалетная":                                      "Бумага туалетная, рулон",
	// Валидол, водка, метамизол и нимесулид: в 2022 XLSX доза/крепость указаны точнее, чем в
	// бюллетенях; кандидатов несколько, выбран вариант 2022 года — прямое продолжение 2021.
	"Валидол":                                        "Валидол, 60 мг, 10 таблеток",
	"Вермишель":                                      "Вермишель, кг",
	"Водка":                                          "Водка крепостью 40% об.спирта и выше, л",
	"Водоотведение":                                  "Водоотведение, м3",
	"Водоотведение, м3":                              "Водоотведение, м3",
	"Водоснабжение горячее":                          "Водоснабжение горячее, м3",
	"Водоснабжение горячее, м3":                      "Водоснабжение горячее, м3",
	"Водоснабжение холодное":                         "Водоснабжение холодное, м3",
	"Водоснабжение холодное, м3":                     "Водоснабжение холодное, м3",
	"Газ сжиженный":                                  "Газ сжиженный, м3",
	"Говядина (кроме бескостного мяса)":              "Говядина (кроме бескостного мяса), кг",
	"Дизельное топливо":                              "Дизельное топливо, л",
	"Дом отдыха, пансионат":                          "Дом отдыха, пансионат, день",
	"Доска обрезная":                                 "Доска обрезная, м3",
	"Капуста белокочанная свежая":                    "Капуста белокочанная свежая, кг",
	"Картофель":                                      "Картофель, кг",
	"Колбаса вареная":                                "Колбаса вареная, кг",
	"Колбаса полукопченая и варено-копченая":         "Колбаса полукопченая и варено-копченая, кг",
	"Колготки женские эластичные":                    "Колготки женские эластичные, шт.",
	"Комбинированные анальгетики":                    "Комбинированные анальгетики, 10 таблеток",
	"Консервы мясные для детского питания":           "Консервы мясные для детского питания, кг",
	"Консервы овощные для детского питания":          "Консервы овощные для детского питания, кг",
	"Консервы фруктово-ягодные для детского питания": "Консервы фруктово-ягодные для детского питания, кг",
	"Конфеты мягкие, глазированные шоколадом":        "Конфеты мягкие, глазированные шоколадом, кг",
	"Корвалол":                                       "Корвалол, 25 мл",
	"Костюм спортивный для детей школьного возраста": "Костюм спортивный для детей школьного возраста, шт.",
	"Кроссовые туфли для взрослых с верхом из искусственной кожи": "Кроссовые туфли для взрослых, пара",
	"Кроссовые туфли для детей с верхом из искусственной кожи":    "Кроссовые туфли для детей, пара",
	"Крупа гречневая-ядрица":                                      "Крупа гречневая-ядрица, кг",
	"Куры охлажденные и мороженые":                                "Куры охлажденные и мороженые, кг",
	"Левомеколь": "Левомеколь, мазь, 40 г",
	"Легковой автомобиль иностранной марки новый": "Легковой автомобиль иностранной марки новый, шт.",
	// 213.htm (2020-11-16): в выпуске 2020 года та же позиция напечатана во множественном числе —
	// канонизация держит ряд непрерывным (вариант написания, не другая позиция).
	"Легковые автомобили иностранной марки новые": "Легковой автомобиль иностранной марки новый, шт.",
	"Легковой автомобиль отечественный новый":     "Легковой автомобиль отечественный новый, шт.",
	"Лук репчатый":                                       "Лук репчатый, кг",
	"Майка, футболка мужская бельевая":                   "Майка, футболка мужская бельевая, шт.",
	"Макаронные изделия из пшеничной муки высшего сорта": "Макаронные изделия из пшеничной муки высшего сорта, кг",
	"Маргарин":           "Маргарин, кг",
	"Масло подсолнечное": "Масло подсолнечное, л",
	"Масло сливочное":    "Масло сливочное, кг",
	"Метамизол натрия (анальгин отечественный)": "Метамизол натрия (Анальгин отечественный), 500 мг, 10 таблеток",
	// Склейки Word в отдельных выпусках (71.htm): «Мол ко» — разорванное «Молоко», «неразделанна»
	// — опечатка источника. Канонизация возвращает такие строки к ряду XLSX, а не создаёт
	// отдельный ряд-однодневку.
	"Мол ко питьевое цельное стерилизованное": "Молоко питьевое цельное стерилизованное 2,5-3,2% жирности, л",
	"Молоко питьевое цельное пастеризованное": "Молоко питьевое цельное пастеризованное 2,5-3,2% жирности, л",
	"Молоко питьевое цельное стерилизованное": "Молоко питьевое цельное стерилизованное 2,5-3,2% жирности, л",
	"Морковь":            "Морковь, кг",
	"Мука пшеничная":     "Мука пшеничная, кг",
	"Мыло туалетное":     "Мыло туалетное, 100 г",
	"Мыло хозяйственное": "Мыло хозяйственное, 200 г",
	"Нимесулид":          "Нимесулид, 100 мг, 10 таблеток",
	"Носки мужские":      "Носки мужские, пара",
	"Обед в столовой, кафе, закусочной (кроме столовой в организации)": "Обед в столовой, кафе, закусочной (кроме столовой в организации), на 1 человека",
	"Огурцы свежие":             "Огурцы свежие, кг",
	"Отопление":                 "Отопление, Гкал",
	"Отопление, Гкал":           "Отопление, Гкал",
	"Паста зубная":              "Паста зубная, 100 г (100 мл)",
	"Пеленки для новорожденных": "Пеленки для новорожденных, шт.",
	"Печенье":                   "Печенье, кг",
	"Плата за жилье в домах государственного и муниципального жилищных фондов": "Плата за жилье в домах государственного и муниципального жилищных фондов, м2 общей площади",
	"Плиты древесностружечные, ориентированно- стружечные":                     "Плиты древесностружечные, ориентированно-стружечные, м2",
	"Подгузники детские бумажные (памперсы)":                                   "Подгузники детские бумажные, 10 шт.",
	"Поездка на отдых в Турцию":                                                "Поездка на отдых в Турцию, поездка",
	"Помидоры свежие":             "Помидоры свежие, кг",
	"Порошок стиральный":          "Порошок стиральный, кг",
	"Проезд в городском автобусе": "Проезд в городском автобусе, поездка",
	// 2017: «муниципальном» — то же наблюдение, что «Проезд в городском автобусе» в 2021.
	"Проезд в городском муниципальном автобусе": "Проезд в городском автобусе, поездка",
	"Проезд в метро":                         "Проезд в метро, поездка",
	"Проезд в трамвае":                       "Проезд в трамвае, поездка",
	"Проезд в троллейбусе":                   "Проезд в троллейбусе, поездка",
	"Проживание в гостинице 1* или в мотеле": "Проживание в гостинице 1* или в мотеле, сутки с человека",
	"Проживание в гостинице 2*":              "Проживание в гостинице 2*, сутки с человека",
	"Проживание в гостинице 3*":              "Проживание в гостинице 3*, сутки с человека",
	"Проживание в гостинице 4*-5*":           "Проживание в гостинице 4*-5*, сутки с человека",
	"Проживание в хостеле":                   "Проживание в хостеле, сутки с человека",
	"Прокладки женские гигиенические":        "Прокладки женские гигиенические, 10 шт.",
	// 245.htm (2020-12-28): «ежедневные» — та же позиция наблюдения, что и без слова с апреля 2021.
	"Прокладки женские гигиенические ежедневные": "Прокладки женские гигиенические, 10 шт.",
	"Пшено":           "Пшено, кг",
	"Рис шлифованный": "Рис шлифованный, кг",
	"Рыба мороженая неразделанна":  "Рыба мороженая неразделанная, кг",
	"Рыба мороженая неразделанная": "Рыба мороженая неразделанная, кг",
	"Санаторий":   "Санаторий, день",
	"Сахар-песок": "Сахар-песок, кг",
	"Свинина (кроме бескостного мяса)": "Свинина (кроме бескостного мяса), кг",
	// «отечественные» в бюллетене — уточнение; в XLSX ряд называется короче.
	"Сигареты с фильтром отечественные": "Сигареты с фильтром, пачка",
	"Смартфон": "Смартфон, шт.",
	"Смеси сухие молочные для детского питания": "Смеси сухие молочные для детского питания, кг",
	"Сметана": "Сметана, кг",
	"Соль поваренная пищевая":                                "Соль поваренная пищевая, кг",
	"Сосиски, сардельки":                                     "Сосиски, сардельки, кг",
	"Спички":                                                 "Спички, коробок",
	"Сухие корма для домашних животных":                      "Сухие корма для домашних животных, кг",
	"Сыры сычужные твердые и мягкие":                         "Сыры сычужные твердые и мягкие, кг",
	"Творог жирный":                                          "Творог, кг",
	"Телевизоры цветного изображения":                        "Телевизор, шт.",
	"Услуги по снабжению электроэнергией":                    "Услуги по снабжению электроэнергией",
	"Футболка детская":                                       "Футболка детская, шт.",
	"Хлеб и булочные изделия из пшеничной муки 1 и 2 сортов": "Хлеб и булочные изделия из пшеничной муки различных сортов, кг",
	"Хлеб из ржаной и ржано-пшеничной муки":                  "Хлеб из ржаной муки и из смеси муки ржаной и пшеничной, кг",
	"Чай черный байховый":                                    "Чай черный байховый, кг",
	"Шампунь":                                                "Шампунь, 250 мл",
	"Щетка зубная":                                           "Щетка зубная, шт.",
	"Электропылесос напольный":                               "Электропылесос напольный, шт.",
	// 2017: «Электроснабжение» — та же услуга, что «Услуги по снабжению электроэнергией» в 2021.
	"Электроснабжение": "Услуги по снабжению электроэнергией",
	"Яблоки":           "Яблоки, кг",
	"Яйца куриные":     "Яйца куриные, 10 шт.",
}

// ipcWeeksBulletinsExtraSeries — ряды бюллетеней, которым канонического соответствия в
// ipcWeeksProducts нет: это собственные ряды импортёра, и меты для них объявляет он сам
// (ipcWeeksBulletinsExtraMeta, series_meta.go). Имена — как в источнике: в XLSX 2022+ этих
// позиций нет, и имя строки бюллетеня остаётся стабильным идентификатором ряда. Список
// заморожен кодом: появление в источнике нового имени вне карты и вне этого списка — warn и
// импорт под исходным именем (§6.6), а не молчаливая потеря строки.
var ipcWeeksBulletinsExtraSeries = []string{
	"Алмагель",              // медикаменты 2017 (100.htm и другие выпуски 2017)
	"Бромгексин",            // медикаменты 2020–2021 (245.htm…144.htm), из XLSX 2022 выпал
	"Валокордин",            // медикаменты 2017
	"Карамель",              // 245.htm…19.htm; в XLSX 2022+ отдельной строки нет
	"Консервы мясные",       // отдельная строка 245.htm…19.htm рядом с консервами для детского питания
	"Линекс",                // медикаменты 2017
	"Металлочерепица",       // 144.htm; в XLSX 2022+ отдельной строки нет
	"Носки, гольфы детские", // 245.htm…19.htm; в XLSX 2022+ только «Носки мужские, пара»
	"Офтан катахром",        // медикаменты 2017
	"Поливитамины без минералов, отечественные", // 144.htm; с XLSX 2022 не продолжается
	"Поливитамины с макро- и микроэлементами",   // 245.htm…144.htm; в XLSX 2022+ «Поливитамины, 10 шт.» — другая позиция
	// 245.htm (2020-12-28): единственный выпуск с обезличенной строкой «Проживание в гостинице»
	// — уже со следующего выпуска (2.htm, 2021-01-11) наблюдение разбито на категории 1*…4*-5*;
	// канонической позиции у обезличенной строки нет, ряд заморожен одной точкой и описан
	// собственной метой.
	"Проживание в гостинице",
	"Ренни", // медикаменты 2017
	"Сорочка верхняя для мальчиков школьного возраста", // 213.htm (2020-11-16); в XLSX 2022+ позиции нет
	"Троксерутин",           // медикаменты 2017
	"Флуоцинолона ацетонид", // медикаменты 2017
}

// ipcWeeksBulletinsSeriesKeys — карта канонизации, переложенная на ключи без пробелов:
// сравнение имён идёт по склейке (стандарт §6.5), потому что Word разбивает слова на элементы
// и между ними появляются пробелы, которых в имени нет («ориентированно- стружечные»).
// Выведена из карты при старте пакета, а не написана вторым списком: иначе источники истины
// разошлись бы при правке карты.
var ipcWeeksBulletinsSeriesKeys = func() map[string]string {
	keys := make(map[string]string, len(ipcWeeksBulletinsSeriesBySourceName))
	for source, canonical := range ipcWeeksBulletinsSeriesBySourceName {
		keys[ipcWeeksBulletinsKey(source)] = canonical
	}
	return keys
}()

// ipcWeeksBulletinsExtraKeys — то же множество без пробелов для extra-списка.
var ipcWeeksBulletinsExtraKeys = func() map[string]bool {
	keys := make(map[string]bool, len(ipcWeeksBulletinsExtraSeries))
	for _, series := range ipcWeeksBulletinsExtraSeries {
		keys[ipcWeeksBulletinsKey(series)] = true
	}
	return keys
}()

// ipcWeeksBulletinsText — нормализация текста: любые последовательности пробельных символов
// (включая переводы строк и неразрывные пробелы Word-HTML) схлопываются в один пробел, края
// обрезаются. Так текст сравнивается и пишется в ошибки, но не в идентификаторы рядов.
func ipcWeeksBulletinsText(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

// ipcWeeksBulletinsKey — текст для сравнения: без пробелов вообще (стандарт §6.5). Word разбивает
// заголовки и имена на элементы, иногда без пробелов между ними (в реальном заголовке 100.htm
// live2021 есть даже склейка «ценс 25»), поэтому якоря, имена товаров и метки дат сравниваются
// по склейке, а не по тексту как есть.
func ipcWeeksBulletinsKey(text string) string {
	return strings.Join(strings.Fields(text), "")
}

// ipcWeeksBulletinsTitleDate разбирает титул выпуска («Об оценке индекса потребительских цен с
// 3 по 9 августа 2021 года») в дату последнего дня периода и слово месяца: первое нужно точкой,
// второе — сверкой метки даты в шапке детальной таблицы. Берётся хвост после последнего « по »
// (устойчиво к опечаткам начала титула), из него — «день месяц год»; месяц — по
// util.MonthsToNum (родительный падеж), год — четыре цифры с возможным «года»/«г.». Не
// разобрался — ошибка (§6.4): угадывать дату точки нельзя.
func ipcWeeksBulletinsTitleDate(title string) (time.Time, string, error) {
	text := strings.ToLower(ipcWeeksBulletinsText(title))
	index := strings.LastIndex(text, " по ")
	if index < 0 {
		return time.Time{}, "", fmt.Errorf("период «с … по …» в титуле не найден")
	}
	fields := strings.Fields(text[index+len(" по "):])
	if len(fields) < 3 {
		return time.Time{}, "", fmt.Errorf("хвост периода %q не «день месяц год»", strings.Join(fields, " "))
	}
	day, err := strconv.Atoi(fields[0])
	if err != nil {
		return time.Time{}, "", fmt.Errorf("день %q не число: %w", fields[0], err)
	}
	month, ok := util.MonthsToNum[fields[1]]
	if !ok {
		return time.Time{}, "", fmt.Errorf("месяц %q не распознан", fields[1])
	}
	year, err := strconv.Atoi(strings.TrimRight(fields[2], "года."))
	if err != nil {
		return time.Time{}, "", fmt.Errorf("год %q не число: %w", fields[2], err)
	}
	date := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	if date.Day() != day || date.Month() != month || date.Year() != year {
		return time.Time{}, "", fmt.Errorf("дата «%d %s %d» не существует", day, fields[1], year)
	}
	return date, fields[1], nil
}

// ipcWeeksBulletinsLabel — метка последнего дня периода, какой она должна стоять в шапке
// детальной таблицы: «9 августа», «22 мая». Год в подписи колонки не пишется — он в титуле.
func ipcWeeksBulletinsLabel(date time.Time, monthWord string) string {
	return fmt.Sprintf("%d %s", date.Day(), monthWord)
}

// ipcWeeksBulletinsSupReference — является ли текст <sup> сноской-ссылкой («1)», «2)»): тогда
// элемент вырезается из дерева (ipcWeeksBulletinsParse). <sup> в бюллетенях носит и степень в
// единицах («м<sup>3</sup>», «м<sup>2</sup>»), поэтому удалять все подряд нельзя — только
// цифры со скобкой.
func ipcWeeksBulletinsSupReference(text string) bool {
	key := ipcWeeksBulletinsKey(text)
	if len(key) < 2 || !strings.HasSuffix(key, ")") {
		return false
	}
	for _, symbol := range strings.TrimSuffix(key, ")") {
		if symbol < '0' || symbol > '9' {
			return false
		}
	}
	return true
}

// ipcWeeksBulletinsLabelMatch — метка даты из шапки подтверждает ожидаемый день периода:
// сравнение по склейке без пробелов и по началу строки — после метки в той же ячейке может
// стоять год и даже подпись соседней колонки, приклеенная Word'ом (2.htm: «11 января 2021 г.
// к концу декабря 2020 г.» как заголовок колонки с недельным значением), а в ожидании — только
// «11 января». День и месяц обязаны совпасть точно: «1 января» не подтвердит «11 января».
func ipcWeeksBulletinsLabelMatch(cell, want string) bool {
	return strings.HasPrefix(ipcWeeksBulletinsKey(cell), ipcWeeksBulletinsKey(want))
}

// ipcWeeksBulletinsValue разбирает ячейку значения: десятичная запятая источника заменяется
// точкой. Ошибка не проглатывается (§4.4) и не подменяется нулём: нечисловая ячейка — ошибка
// выпуска с именем товара (§6.4). NaN и бесконечность ParseFloat принимает, а точкой ряда они
// быть не могут. Сноски-ссылки к этому моменту вырезаны из дерева (ipcWeeksBulletinsParse),
// поэтому «100,2<sup>1)</sup>» приходит сюда как «100,2», а не «100,21)».
func ipcWeeksBulletinsValue(cell string) (float64, error) {
	value, err := strconv.ParseFloat(strings.ReplaceAll(ipcWeeksBulletinsText(cell), ",", "."), 64)
	if err != nil {
		return 0, fmt.Errorf("значение ячейки %q не число: %w", cell, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("значение ячейки %q не конечное число", cell)
	}
	return value, nil
}

// ipcWeeksBulletinsNoDataCell — ячейка без данных: пустая, «…» (в том числе со сноской «…2)»),
// «-» или «–». Такая ячейка не точка (§6.7) и не ошибка: источник помечает ею пропуск
// публикации у отдельного товара.
func ipcWeeksBulletinsNoDataCell(cell string) bool {
	key := ipcWeeksBulletinsKey(cell)
	if key == "" || key == "-" || key == "–" {
		return true
	}
	return strings.HasPrefix(key, ipcWeeksBulletinsNoData)
}

// ipcWeeksBulletinsEnd — строка-терминатор детальной таблицы: пустой первый столбец, строка из
// подчёркиваний, сноски («Информация подготовлена…», «Оценка индекса…») и нумерованные
// примечания («1) …», «2) …», «# …»). Терминатор ищется по признаку, а не по позиции (§6.3):
// у выпуска может не быть ни одной сноски, а может быть несколько.
func ipcWeeksBulletinsEnd(name string) bool {
	key := ipcWeeksBulletinsKey(name)
	if key == "" || strings.Trim(key, "_") == "" {
		return true
	}
	for _, terminator := range ipcWeeksBulletinsTerminators {
		if strings.HasPrefix(key, ipcWeeksBulletinsKey(terminator)) {
			return true
		}
	}
	return false
}

// ipcWeeksBulletinsAggregate достаёт агрегатную строку шапки-таблицы «в процентах»: строка, чей
// склеенный текст начинается с «Индекс потребительских цен (оценка)», а значение — первая
// числовая ячейка (колонка «К предыдущей дате регистрации цен», спека §3.4). Поиск по якорю, а
// не по позиции: номер строки меняется между выпусками. Нет строки или в ней нет чисел — ошибка.
func ipcWeeksBulletinsAggregate(doc *goquery.Document) (float64, error) {
	anchor := ipcWeeksBulletinsKey(ipcWeeksBulletinsAggregateAnchor)
	value := 0.0
	found := false
	doc.Find("tr").EachWithBreak(func(_ int, row *goquery.Selection) bool {
		cells := row.ChildrenFiltered("td, th")
		texts := make([]string, 0, cells.Length())
		cells.Each(func(_ int, cell *goquery.Selection) {
			texts = append(texts, cell.Text())
		})
		if !strings.HasPrefix(ipcWeeksBulletinsKey(strings.Join(texts, " ")), anchor) {
			return true
		}
		// Первая числовая ячейка строки агрегата: ячейка названия не число, поэтому пропускается.
		for i, text := range texts {
			if i == 0 {
				continue
			}
			number, err := ipcWeeksBulletinsValue(text)
			if err != nil {
				continue
			}
			value, found = number, true
			return false
		}
		return true
	})
	if !found {
		return 0, fmt.Errorf("строка «%s» с числовым значением не найдена", ipcWeeksBulletinsAggregateAnchor)
	}
	return value, nil
}

// ipcWeeksBulletinsDetailTable ищет детальную таблицу «Изменения цен» по признаку (§3.4):
// ближайшая таблица ячейки с текстом «Изменения цен», в шапке которой (первые четыре строки)
// есть якорь «к предыдущей дате регистрации». Поиск по якорям, а не по номеру таблицы: в потоке
// документа таблиц три-четыре (шапка, детальная, сноски), и их состав меняется между выпусками.
func ipcWeeksBulletinsDetailTable(doc *goquery.Document) (*goquery.Selection, error) {
	var found *goquery.Selection
	doc.Find("td, th").EachWithBreak(func(_ int, cell *goquery.Selection) bool {
		if ipcWeeksBulletinsKey(cell.Text()) != ipcWeeksBulletinsKey(ipcWeeksBulletinsChangesAnchor) {
			return true
		}
		table := cell.Closest("table")
		if table.Length() == 0 {
			return true
		}
		// Шапка — первые четыре строки таблицы. Читаются по одной, а не срезом строк:
		// Slice(0, 4) на таблице короче четырёх строк даёт срез длиннее исходного и nil-узел
		// (паника в Text), а таблица-шапка из одной строки — законный вход (мини-фикстуры тестов,
		// усечённые выпуски).
		var head strings.Builder
		table.Find("tr").EachWithBreak(func(i int, row *goquery.Selection) bool {
			head.WriteString(ipcWeeksBulletinsKey(row.Text()))
			return i < 3
		})
		if !strings.Contains(head.String(), ipcWeeksBulletinsKey(ipcWeeksBulletinsPreviousAnchor)) {
			return true
		}
		found = table
		return false
	})
	if found == nil {
		return nil, fmt.Errorf("таблица «%s» с якорем «%s» не найдена", ipcWeeksBulletinsChangesAnchor, ipcWeeksBulletinsPreviousAnchor)
	}
	return found, nil
}

// ipcWeeksBulletinsSpan — целочисленный атрибут rowspan/colspan ячейки (1, если атрибута нет).
func ipcWeeksBulletinsSpan(cell *goquery.Selection, attr string) int {
	value, ok := cell.Attr(attr)
	if !ok {
		return 1
	}
	span, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || span < 1 {
		return 1
	}
	return span
}

// ipcWeeksBulletinsGrid выпрямляет строки таблицы в сетку текстов ячеек с учётом rowspan и
// colspan: у Word-HTML шапка стековая, и без выпрямления якорь «к предыдущей дате регистрации»
// (групповой заголовок над первыми тремя колонками) не связать с первой колонкой данных. Текст
// записывается во все покрытые ячейкой позиции: колонка данных «видит» и свой групповой
// заголовок, и свою метку даты. Берутся только строки самой таблицы (её и её thead/tbody), а не
// вложенных таблиц: у документа их несколько, и строки чужой таблицы попали бы в сетку.
func ipcWeeksBulletinsGrid(table *goquery.Selection) [][]string {
	tableNode := table.Get(0)
	rows := table.Find("tr").FilterFunction(func(_ int, row *goquery.Selection) bool {
		parent := row.Parent().Get(0)
		if parent == nil {
			return false
		}
		if parent == tableNode {
			return true
		}
		return parent.Parent == tableNode && (parent.Data == "thead" || parent.Data == "tbody" || parent.Data == "tfoot")
	})

	grid := make([][]string, 0, rows.Length())
	occupied := make(map[[2]int]bool)
	rows.Each(func(r int, row *goquery.Selection) {
		grid = append(grid, nil)
		col := 0
		row.ChildrenFiltered("td, th").Each(func(_ int, cell *goquery.Selection) {
			for occupied[[2]int{r, col}] {
				col++
			}
			text := ipcWeeksBulletinsText(cell.Text())
			rowspan := ipcWeeksBulletinsSpan(cell, "rowspan")
			colspan := ipcWeeksBulletinsSpan(cell, "colspan")
			for i := 0; i < rowspan; i++ {
				for len(grid) <= r+i {
					grid = append(grid, nil)
				}
				for j := 0; j < colspan; j++ {
					occupied[[2]int{r + i, col + j}] = true
					for len(grid[r+i]) <= col+j {
						grid[r+i] = append(grid[r+i], "")
					}
					grid[r+i][col+j] = text
				}
			}
			col += colspan
		})
	})
	return grid
}

// ipcWeeksBulletinsCell — ячейка строки сетки (пустая строка, если колонки в строке нет).
func ipcWeeksBulletinsCell(row []string, col int) string {
	if col >= len(row) {
		return ""
	}
	return row[col]
}

// ipcWeeksBulletinsDataStart — первая строка данных сетки: строки шапки опознаются по пустому
// первому столбцу (его держит rowspan ячейки названия) и по якорю «к предыдущей дате
// регистрации». Возврат len(grid) означает «строк данных нет» — это ошибка выпуска.
func ipcWeeksBulletinsDataStart(grid [][]string) int {
	anchor := ipcWeeksBulletinsKey(ipcWeeksBulletinsPreviousAnchor)
	for r, row := range grid {
		name := ipcWeeksBulletinsKey(ipcWeeksBulletinsCell(row, 0))
		if name == "" || strings.Contains(name, anchor) {
			continue
		}
		return r
	}
	return len(grid)
}

// ipcWeeksBulletinsValueColumn — колонка недельного значения: самая левая колонка данных под
// групповым якорем «к предыдущей дате регистрации» (в обычном выпуске это колонка 1 после
// названий, спека §3.3). Новогодний выпуск 2.htm (неделя 1–11.01.2021) устроен иначе: его
// первая колонка данных — «11 января 2021 г. к концу декабря 2020 г.» (накопленное с начала
// года), а недельное значение стоит во второй колонке, и якорь покрывает колонки со второй по
// пятую; поэтому колонка вычисляется по шапке сетки, а не берётся жёстким индексом. Возврат -1
// — якоря в шапке нет (ошибка выпуска). Колонку подтверждает метка даты
// (ipcWeeksBulletinsCheckDateLabel): сдвиг колонок стал бы ошибкой, а не тихой записью значений
// соседней недели.
func ipcWeeksBulletinsValueColumn(grid [][]string, dataStart int) int {
	anchor := ipcWeeksBulletinsKey(ipcWeeksBulletinsPreviousAnchor)
	width := 0
	for _, row := range grid {
		if len(row) > width {
			width = len(row)
		}
	}
	for col := 1; col < width; col++ {
		for r := 0; r < dataStart; r++ {
			if strings.Contains(ipcWeeksBulletinsKey(ipcWeeksBulletinsCell(grid[r], col)), anchor) {
				return col
			}
		}
	}
	return -1
}

// ipcWeeksBulletinsCheckDateLabel подтверждает колонку значений меткой даты: в её шапке должна
// стоять метка последнего дня периода из титула («9 августа», «22 мая», «11 января 2021 г.»).
// Это защита от пере-вёрстки таблицы: сдвиг колонок увёл бы значения недели в чужую колонку, и
// без сверки ошибка была бы не видна.
func ipcWeeksBulletinsCheckDateLabel(grid [][]string, dataStart, valueCol int, date time.Time, monthWord string) error {
	want := ipcWeeksBulletinsLabel(date, monthWord)
	for r := 0; r < dataStart; r++ {
		if ipcWeeksBulletinsLabelMatch(ipcWeeksBulletinsCell(grid[r], valueCol), want) {
			return nil
		}
	}
	return fmt.Errorf("метка последнего дня периода «%s» не найдена в колонке %d шапки", want, valueCol)
}

// ipcWeeksBulletinsSeriesName — имя ряда для строки источника: каноническое из карты, исходное
// для extra-ряда или исходное с предупреждением. Предупреждение выводится один раз на имя:
// у выпуска десятки строк, а незнакомых имён обычно одно-два.
func ipcWeeksBulletinsSeriesName(source string, warned map[string]bool) string {
	key := ipcWeeksBulletinsKey(source)
	if canonical, ok := ipcWeeksBulletinsSeriesKeys[key]; ok {
		return canonical
	}
	if ipcWeeksBulletinsExtraKeys[key] {
		return source
	}
	if !warned[source] {
		warned[source] = true
		log.Warnf("%s: товар %q не описан в ipcWeeksBulletinsSeriesBySourceName / ipcWeeksBulletinsExtraSeries — точка записана под исходным именем, описание добавить в каталог", rosstatIPCWeeksBulletinsTable, source)
	}
	return source
}

// ipcWeeksBulletinsParse разбирает байты одного выпуска в точки (спека §3.4): кодировка cp1251
// из meta-объявления (заглушка WAF — UTF-8, поэтому декодер обязан читать объявление, а не
// угадывать), титул → дата, агрегатная строка шапки → точка агрегата, детальная таблица → точки
// товаров. Чистая функция без сети (§6.1) — её зовёт BodyFunc шаблона, получивший байты.
// Fail-fast: нет титула, нет агрегата, нет детальной таблицы, нет строк данных или метка даты
// не подтвердила колонку — ошибка (§6.4); panic запрещён (§6.5).
func ipcWeeksBulletinsParse(body []byte) ([]ipcWeeksBulletinsPoint, error) {
	reader, err := charset.NewReader(bytes.NewReader(body), "text/html")
	if err != nil {
		return nil, fmt.Errorf("кодировка документа не определена: %w", err)
	}
	doc, err := goquery.NewDocumentFromReader(reader)
	if err != nil {
		return nil, fmt.Errorf("HTML не разобран: %w", err)
	}
	// Сноски-ссылки в <sup> («<sup>1)</sup>», «<sup>2)</sup>») — разметка, а не данные: в ячейке
	// значения они приклеивают номер сноски к числу («100,2<sup>1)</sup>» → «100,21)» в 245.htm),
	// а в метке даты — к дате («11 января 2021 г.<sup>2)</sup>»). Вырезаем только настоящие
	// сноски: <sup> носит и показатель степени в единицах («м<sup>3</sup>», «м<sup>2</sup>»),
	// который обязан остаться в имени товара. После вырезания тексты ячеек — только содержимое,
	// и сноска не может быть принята за цифру значения.
	doc.Find("sup").Each(func(_ int, sup *goquery.Selection) {
		if ipcWeeksBulletinsSupReference(sup.Text()) {
			sup.Remove()
		}
	})
	title := ipcWeeksBulletinsText(doc.Find("title").First().Text())
	if title == "" {
		return nil, fmt.Errorf("заголовок документа (title) не найден")
	}
	date, monthWord, err := ipcWeeksBulletinsTitleDate(title)
	if err != nil {
		return nil, fmt.Errorf("титул %q: %w", title, err)
	}
	aggregate, err := ipcWeeksBulletinsAggregate(doc)
	if err != nil {
		return nil, err
	}
	table, err := ipcWeeksBulletinsDetailTable(doc)
	if err != nil {
		return nil, err
	}
	grid := ipcWeeksBulletinsGrid(table)
	dataStart := ipcWeeksBulletinsDataStart(grid)
	if dataStart == len(grid) {
		return nil, fmt.Errorf("таблица «%s»: строки данных не найдены", ipcWeeksBulletinsChangesAnchor)
	}
	valueCol := ipcWeeksBulletinsValueColumn(grid, dataStart)
	if valueCol < 0 {
		return nil, fmt.Errorf("шапка детальной таблицы не подтверждает колонку «%s»", ipcWeeksBulletinsPreviousAnchor)
	}
	if err = ipcWeeksBulletinsCheckDateLabel(grid, dataStart, valueCol, date, monthWord); err != nil {
		return nil, err
	}

	points := make([]ipcWeeksBulletinsPoint, 0, len(grid)-dataStart+1)
	points = append(points, ipcWeeksBulletinsPoint{series: ipcWeeksBulletinsAggregateSeries, date: date, value: aggregate})
	warned := make(map[string]bool)
	for r := dataStart; r < len(grid); r++ {
		source := ipcWeeksBulletinsText(ipcWeeksBulletinsCell(grid[r], 0))
		if ipcWeeksBulletinsEnd(source) {
			break
		}
		cell := ipcWeeksBulletinsCell(grid[r], valueCol)
		if ipcWeeksBulletinsNoDataCell(cell) {
			continue
		}
		value, err := ipcWeeksBulletinsValue(cell)
		if err != nil {
			return nil, fmt.Errorf("товар %q: %w", source, err)
		}
		points = append(points, ipcWeeksBulletinsPoint{series: ipcWeeksBulletinsSeriesName(source, warned), date: date, value: value})
	}
	if len(points) < 2 {
		return nil, fmt.Errorf("разбор дал %d точек — в таблице «%s» нет ни одной строки товара с значением", len(points), ipcWeeksBulletinsChangesAnchor)
	}
	return points, nil
}

// ipcWeeksBulletinsKnownDates — множество дат, уже лежащих в таблице (гейт, §3.5 шаг 2).
// Множество, а не список: в цикле важен только ответ «дата известна?». Ключ — строка даты, а не
// time.Time: драйвер может отдать Date в другой локации, и сравнение структур time.Time
// разошлось бы при том же календарном дне. Ошибка чтения — ошибка прогона, а не «считаем, что
// известных нет»: пустое множество пере-импортировало бы все 71 выпуск.
func ipcWeeksBulletinsKnownDates(ctx context.Context, conn driver.Conn) (map[string]bool, error) {
	rows, err := conn.Query(ctx, ipcWeeksBulletinsKnownDatesQuery)
	if err != nil {
		return nil, fmt.Errorf("%s: даты таблицы не прочитаны: %w", rosstatIPCWeeksBulletinsTable, err)
	}
	defer func() { _ = rows.Close() }()

	known := make(map[string]bool)
	for rows.Next() {
		var date time.Time
		if err = rows.Scan(&date); err != nil {
			return nil, fmt.Errorf("%s: строка дат таблицы не прочитана: %w", rosstatIPCWeeksBulletinsTable, err)
		}
		known[date.Format("2006-01-02")] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: даты таблицы не дочитаны: %w", rosstatIPCWeeksBulletinsTable, err)
	}
	return known, nil
}

// ipcWeeksBulletinsNewSources — выпуски, дат которых нет в таблице, в хронологическом порядке
// (старые → новые): первый прогон наполняет историю детерминированным префиксом. Список
// источников уже отсортирован, но сортировка повторяется здесь, чтобы контракт функции не
// зависел от порядка литералов. Чистая функция без обращений к БД и сети: сценарии «все даты
// известны» (пусто → no-op прогона) и «все выпуски новые» пинятся тестом.
func ipcWeeksBulletinsNewSources(sources []ipcWeeksBulletinsSource, known map[string]bool) []ipcWeeksBulletinsSource {
	fresh := make([]ipcWeeksBulletinsSource, 0, len(sources))
	for _, source := range sources {
		if !known[source.Date.Format("2006-01-02")] {
			fresh = append(fresh, source)
		}
	}
	sort.SliceStable(fresh, func(i, j int) bool {
		return fresh[i].Date.Before(fresh[j].Date)
	})
	return fresh
}

// ipcWeeksBulletinsAppend — BodyFunc шаблона для одного выпуска: разбор байтов в точки, сверка
// даты титула с ожидаемой датой выпуска и запись строк батча. Замыкание на выпуск нужно потому,
// что BodyFunc шаблона знает только байты и батч (стандарт §5.1), а дата — свойство записи
// списка источников. Сверка — часть контракта (§3.3): пере-нумерованный буфер отдал бы по тому
// же URL чужой выпуск, и точки ушли бы не на свою дату; ошибка с URL делает это громким сбоем
// недели. Append с проверкой err обязателен: без неё строка терялась бы молча.
func ipcWeeksBulletinsAppend(source ipcWeeksBulletinsSource) func(body []byte, batch driver.Batch) error {
	return func(body []byte, batch driver.Batch) error {
		points, err := ipcWeeksBulletinsParse(body)
		if err != nil {
			return fmt.Errorf("%s: %w", source.URL, err)
		}
		if !points[0].date.Equal(source.Date) {
			return fmt.Errorf("%s: дата титула %s не совпала с ожидаемой датой выпуска %s — буфер пере-нумеровался",
				source.URL, points[0].date.Format("2006-01-02"), source.Date.Format("2006-01-02"))
		}
		for _, point := range points {
			if err = batch.Append(point.series, point.date, point.value); err != nil {
				return fmt.Errorf("%s: ряд %q, %s: %w", source.URL, point.series, point.date.Format("2006-01-02"), err)
			}
		}
		return nil
	}
}

// ipcWeeksBulletinsWeekWritten — проверка, что точки недели в таблице есть: число строк по дате
// после успешного вызова шаблона (спека §3.5, шаг 3). Одного успеха шаблона мало: его префлайт
// умеет вернуть no-op («версия известна и число точек совпало»), и тогда новый по гейту выпуск
// остался бы молча неимпортированным, а следующий прогон снова счёл бы его новым. Состояние
// практически недостижимо (число точек всей таблицы растёт с каждым выпуском), но тихая дыра
// обязана стать громкой ошибкой с указанием, что делать. Дата уходит плейсхолдером драйвера.
func ipcWeeksBulletinsWeekWritten(ctx context.Context, conn driver.Conn, source ipcWeeksBulletinsSource) error {
	var written uint64
	if err := conn.QueryRow(ctx, ipcWeeksBulletinsDateCountQuery, source.Date).Scan(&written); err != nil {
		return fmt.Errorf("%s: %s: число точек выпуска не прочитано: %w", rosstatIPCWeeksBulletinsTable, source.URL, err)
	}
	if written == 0 {
		return fmt.Errorf("%s: %s: шаблон вернул no-op (версия известна), но точек выпуска в таблице нет — удалите строку выпуска из source_publications и повторите прогон",
			rosstatIPCWeeksBulletinsTable, source.URL)
	}
	return nil
}

// IpcWeeksBulletinsStat — импортёр rosstat_ipc_weeks_bulletins: встроенный util.ClickHouseImport
// ведёт таблицу, DDL, объявление точки контроля публикации и производные (afterRosstatMacro),
// а Import переопределён — у импортёра не один файл, а 71 выпуск (спека §3.5). Name() не
// переопределён: имя импортёра совпадает с именем таблицы.
type IpcWeeksBulletinsStat struct {
	util.ClickHouseImport
}

// Import — полный цикл одного прогона (спека §3.5): DDL → гейт по датам → цикл по недостающим
// выпускам в хронологическом порядке: локальная копия шаблона (DataUrl + BodyFunc) на выпуск и
// проверка, что точки недели записаны (успех шаблона записи не гарантирует — он умеет ответить
// no-op). Порядок шагов — часть контракта: DDL выполняется до сети и до гейта, потому что гейт
// обращается к таблице, а шаблонный DDL внутри цикла не отработал бы при нуле новых выпусков
// (прогон на чистой базе обязан отработать целиком). Сбой одного выпуска не отменяет
// остальные: ошибки копятся errors.Join с URL в тексте, прогон завершается ошибкой — dagu
// видит сбой и не пускает цепочку дальше. Если все даты известны — 0 строк, сообщение «все
// недели на месте» и ни одного сетевого обращения; производные (каталог и витрина) при этом
// всё равно публикуются — AfterImport шаблона зовётся и на no-op-пути (идемпотентно), поэтому
// сбой публикации прошлого прогона лечится следующим. Обратная сторона гейта: сбой архивации
// или строки версии уже записанной недели следующим прогоном не доливается (шаблон для неё не
// зовётся) — лечение ручное: удалить строки даты из rosstat_ipc_weeks_bulletins и повторить
// прогон (ARCHITECTURE §8 п. 13).
func (s *IpcWeeksBulletinsStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	// 1. DDL — до сети: шаблонный DDL внутри цикла отработал бы только при непустом списке
	// недостающих выпусков, а гейт уже обращается к таблице.
	for _, ddl := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(ddl, s.TableName)); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", rosstatIPCWeeksBulletinsTable, err)
		}
	}

	// 2. Гейт: даты, уже лежащие в таблице, пропускаются — их выпуски не скачиваются.
	known, err := ipcWeeksBulletinsKnownDates(ctx, conn)
	if err != nil {
		return 0, err
	}
	fresh := ipcWeeksBulletinsNewSources(ipcWeeksBulletinsSources, known)
	if len(fresh) == 0 {
		log.Infof("%s: все недели на месте (%d выпусков, %d дат)", rosstatIPCWeeksBulletinsTable, len(ipcWeeksBulletinsSources), len(known))
		// Производные публикуются и на no-op-пути: каталог — upsert, витрина — CREATE OR REPLACE,
		// обе операции идемпотентны, поэтому сбой публикации одного прогона лечится следующим,
		// даже когда недели уже записаны. Без этого гейт-импортёр завис бы в устаревшем
		// каталоге/витрине: гейт видит точки записанными и полный путь (а с ним AfterImport
		// шаблона) больше не повторяет (§8 п. 13).
		if s.AfterImport != nil {
			if err = s.AfterImport(ctx, conn); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}

	// 3. Цикл по недостающим выпускам. Ошибки копятся — сбой одного выпуска не отменяет
	// остальные: выпуски независимы (у каждого свой URL, файл и строка версии), а пропущенный
	// выпуск подхватился бы следующим прогоном, если бы прогон молча прекращался на первом сбое.
	var errs []error
	for _, source := range fresh {
		// Локальная копия встроенного шаблона: DataUrl и BodyFunc зависят от выпуска, а
		// мутировать общую структуру импортёра нельзя (прецедент — cbrDipStat.Import). Копия
		// приносит объявленный префлайт (PublicationCountPoints): судьбу прогона он не решает
		// (no-op считает гейт), но без него не было бы ни архива версии документа, ни строки
		// source_publications (спека §3.6).
		local := s.ClickHouseImport
		local.DataUrl = source.URL
		local.BodyFunc = ipcWeeksBulletinsAppend(source)
		points, importErr := local.Import(ctx, conn)
		if importErr != nil {
			errs = append(errs, fmt.Errorf("%s: %w", source.URL, importErr))
			continue
		}
		if err = ipcWeeksBulletinsWeekWritten(ctx, conn, source); err != nil {
			errs = append(errs, err)
			continue
		}
		count += points
		log.Infof("%s: выпуск %s (%s): импортировано %d точек", rosstatIPCWeeksBulletinsTable, source.Date.Format("2006-01-02"), source.URL, points)
	}
	return count, errors.Join(errs...)
}

// ipcWeeksBulletinsImporter — недельные ИПЦ до 2022 из бюллетеней «Оперативной информации»;
// регистрируется реестром rosstat/importers.go и шагом dagu/rosstat.yaml. Объявление точки
// контроля публикации (PublicationCountPoints) включает винтажный архив: байты каждого выпуска
// качает шаблон (BodyFunc), тот же срез идёт в разбор и в save — объект в хранилище и строка
// registry.jsonl. BodyFunc здесь не задаётся: он вычисляется в Import() локальной копией на
// каждый выпуск (URL — свойство выпуска, а не импортёра). AfterImport публикует каталог
// (ipcWeeksBulletinsSeriesMeta) и витрину v_rosstat_macro. Сетевых вызовов в init() нет.
var ipcWeeksBulletinsImporter = &IpcWeeksBulletinsStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              rosstatIPCWeeksBulletinsTable,
	CreateTable:            []string{ipcWeeksBulletinsDdl},
	PublicationCountPoints: ipcWeeksBulletinsCountPoints,
	AfterImport:            afterRosstatMacro(ipcWeeksBulletinsSeriesMeta()),
}}
