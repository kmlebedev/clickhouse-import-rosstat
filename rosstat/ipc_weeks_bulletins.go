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
	"regexp"
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
	// покрывает 2009–2021 (502 недели с разрывами) — оговорка в его description (series_meta.go).
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

// ipcWeeksBulletinsSources — все 502 выпуска (спека §2, приложения А–Д), отсортированные по дате
// точки. Литералы кода, а не разбор страницы-каталога: источника-каталога у этого архива нет, а
// спасательный дамп и систематическая разведка (scripts/harvest.py) зафиксировали точные адреса и
// даты (тест-инварианты: 48 живых + 454 Wayback 2009–2021 по годам документа, уникальные URL,
// строго возрастающие даты). Форма id_ у Wayback-адресов обязательна: обычный режим вклеивает в
// страницу свою панель и переписывает ссылки, а id_ отдаёт сырые байты документа.
// Каталог потока «Оперативной информации» мигрирует между Stg/dNN по годам: d06 (2015–2017,
// файлы «NNNozenN»), d02 (2017 и 2021), d03→d01 (2018), d04 (2019), d05 (2020). Разведка «только
// d02» поэтому находила недели лишь тех лет, когда поток жил в d02; полное дерево Stg/ (разведка
// 2026-10-16/17) добавило 202 недели 2015–2020 — документы удержали снимки 2016–2024 гг., включая
// майские краулы 2021 г. на gks.ru (2020-11-02…12-14). Девять выпусков первого
// расширения (2017×6, 2020×3) и пять второго (2020×5) — оттуда же; аудит пропусков (2026-10-17)
// добавил ещё один выпуск 2016-11-21 — в титуле опечатка «с15 по 21» без пробела, и первый
// проход разведки его не разобрал. Проверка «с самого дня существования буфера» (2026-10-17)
// сняла фильтр CDX from=2016 и нашла ещё 214 недель 2009-04-06 … 2015-12-07: ряд недельной
// оценки начинается не в 2015-м, а в апреле 2009-го (файлы d02/d03/d04/d05/d06 «NNNozenN»).
// Версии всех выпусков лежат в винтажном
// архиве, поэтому и живой импорт, и restore берут их оттуда по этим же URL; в 2021 четыре февраля
// вклиниваются между живыми выпусками января и марта — дат этих недель в живом буфере уже нет,
// их сохранил только Wayback (спека §2.2).
var ipcWeeksBulletinsSources = []ipcWeeksBulletinsSource{
	{URL: "https://web.archive.org/web/20090412160540id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/58ozen8.htm", Date: time.Date(2009, time.April, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090418152523id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/61ozen15.htm", Date: time.Date(2009, time.April, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090426045233id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/68ozen22.htm", Date: time.Date(2009, time.April, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090620010926id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/81ozen14.htm", Date: time.Date(2009, time.May, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090526204051id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/89ozen20.htm", Date: time.Date(2009, time.May, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090608010205id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/99ozen3.htm", Date: time.Date(2009, time.June, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090614072752id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/102ozen10.htm", Date: time.Date(2009, time.June, 8, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090621071004id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/109ozen17.htm", Date: time.Date(2009, time.June, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090627083410id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/118ozen24.htm", Date: time.Date(2009, time.June, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090704120642id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/121ozen1.htm", Date: time.Date(2009, time.June, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090711183825id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/127ozen8.htm", Date: time.Date(2009, time.July, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090719070012id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/131ozen15.htm", Date: time.Date(2009, time.July, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090724173912id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/141ozen22.htm", Date: time.Date(2009, time.July, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090801203257id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/144ozen29.htm", Date: time.Date(2009, time.July, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090816143903id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/149ozen12.htm", Date: time.Date(2009, time.August, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090918231646id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/158ozen19.htm", Date: time.Date(2009, time.August, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090830185941id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/165ozen26.htm", Date: time.Date(2009, time.August, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090904030433id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/168ozen2.htm", Date: time.Date(2009, time.August, 31, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090913151156id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/171ozen9.htm", Date: time.Date(2009, time.September, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090922220725id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/180ozen16.htm", Date: time.Date(2009, time.September, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20090927120758id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/188ozen23.htm", Date: time.Date(2009, time.September, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20091009212102id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/196ozen7.htm", Date: time.Date(2009, time.October, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20091017092823id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/198ozen14.htm", Date: time.Date(2009, time.October, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20091022221303id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/207ozen21.htm", Date: time.Date(2009, time.October, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20091029184640id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/214ozen28.htm", Date: time.Date(2009, time.October, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20091107090710id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/217ozen5.htm", Date: time.Date(2009, time.November, 2, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20091112093055id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/219ozen11.htm", Date: time.Date(2009, time.November, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20091203105217id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/236ozen25.htm", Date: time.Date(2009, time.November, 23, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20091206104333id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/239ozen2.htm", Date: time.Date(2009, time.November, 30, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100101185636id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/242ozen9.htm", Date: time.Date(2009, time.December, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100117204551id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/2ozen13.htm", Date: time.Date(2010, time.January, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100124164421id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/8ozen20.htm", Date: time.Date(2010, time.January, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100201001155id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/13ozen27.htm", Date: time.Date(2010, time.January, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100210015028id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/20ozen3.htm", Date: time.Date(2010, time.February, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100212045357id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/24ozen10.htm", Date: time.Date(2010, time.February, 8, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100222094515id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/30ozen17.htm", Date: time.Date(2010, time.February, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100301194419id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/36ozen26.htm", Date: time.Date(2010, time.February, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100314155331id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/46ozen11.htm", Date: time.Date(2010, time.March, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100330101408id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/59ozen24.htm", Date: time.Date(2010, time.March, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100402070900id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/63ozen31.htm", Date: time.Date(2010, time.March, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100412235214id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/68ozen7.htm", Date: time.Date(2010, time.April, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100418185746id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/70ozen14.htm", Date: time.Date(2010, time.April, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100424213301id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/77ozen21.htm", Date: time.Date(2010, time.April, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100503012525id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/85ozen28.htm", Date: time.Date(2010, time.April, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100508194238id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/89ozen6.htm", Date: time.Date(2010, time.May, 4, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100515035237id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/91ozen13.htm", Date: time.Date(2010, time.May, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100528111325id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/98ozen19.htm", Date: time.Date(2010, time.May, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100611054552id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/113ozen9.htm", Date: time.Date(2010, time.June, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100619015950id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/122ozen17.htm", Date: time.Date(2010, time.June, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100716142704id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/139ozen14.htm", Date: time.Date(2010, time.July, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100724010841id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/149ozen21.htm", Date: time.Date(2010, time.July, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100822022311id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/161ozen11.htm", Date: time.Date(2010, time.August, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100821105715id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/169ozen18.htm", Date: time.Date(2010, time.August, 16, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100909005549id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/180ozen1.htm", Date: time.Date(2010, time.August, 30, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100912101052id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/184ozen8.htm", Date: time.Date(2010, time.September, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100924023312id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/189ozen15.htm", Date: time.Date(2010, time.September, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20100929063118id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/199ozen22.htm", Date: time.Date(2010, time.September, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20101216063503id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d05/256ozen8.htm", Date: time.Date(2010, time.December, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20101220003339id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d05/261ozen15.htm", Date: time.Date(2010, time.December, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110102114325id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d05/276ozen29.htm", Date: time.Date(2010, time.December, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110115000235id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/3ozen13.htm", Date: time.Date(2011, time.January, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110123020807id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/5ozen19.htm", Date: time.Date(2011, time.January, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110206075244id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/21ozen2.htm", Date: time.Date(2011, time.January, 31, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110215220059id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/25ozen9.htm", Date: time.Date(2011, time.February, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110221113550id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/30ozen16.htm", Date: time.Date(2011, time.February, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110227084331id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/39ozen24.htm", Date: time.Date(2011, time.February, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110314065358id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/47ozen10.htm", Date: time.Date(2011, time.March, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110321194358id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/51ozen16.htm", Date: time.Date(2011, time.March, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110707044809id_/http://www.gks.ru/bgd/free/b04_03/IssWWW.exe/Stg/d01/59ozen23.htm", Date: time.Date(2011, time.March, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110402232832id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/66ozen30.htm", Date: time.Date(2011, time.March, 28, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110409063835id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/70ozen6.htm", Date: time.Date(2011, time.April, 4, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110414232154id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/74ozen13.htm", Date: time.Date(2011, time.April, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110502101123id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/90ozen27.htm", Date: time.Date(2011, time.April, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110510230627id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/93ozen5.htm", Date: time.Date(2011, time.May, 3, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110518005725id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/95ozen12.htm", Date: time.Date(2011, time.May, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110530014833id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/110ozen25.htm", Date: time.Date(2011, time.May, 23, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110612091850id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/118ozen8.htm", Date: time.Date(2011, time.June, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110912165112id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/124ozen16.htm", Date: time.Date(2011, time.June, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110623154508id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/131ozen22.htm", Date: time.Date(2011, time.June, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110707045315id_/http://www.gks.ru/bgd/free/b04_03/IssWWW.exe/Stg/d01/138ozen29.htm", Date: time.Date(2011, time.June, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110714233640id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/146ozen13.htm", Date: time.Date(2011, time.July, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110912015041id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/175ozen17.htm", Date: time.Date(2011, time.August, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110912010030id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/184ozen24.htm", Date: time.Date(2011, time.August, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110927072735id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/204ozen21.htm", Date: time.Date(2011, time.September, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20110930234051id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/212ozen28.htm", Date: time.Date(2011, time.September, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20111022163431id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/216ozen5.htm", Date: time.Date(2011, time.October, 3, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20111015164619id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/219ozen12.htm", Date: time.Date(2011, time.October, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20111022163516id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/227ozen19.htm", Date: time.Date(2011, time.October, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20111031173101id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/235ozen26.htm", Date: time.Date(2011, time.October, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20111121212646id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/244ozen16.htm", Date: time.Date(2011, time.November, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20111125161910id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/257ozen23.htm", Date: time.Date(2011, time.November, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120112075412id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/268ozen14.htm", Date: time.Date(2011, time.December, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120113105844id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/283ozen28.htm", Date: time.Date(2011, time.December, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120131092821id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/14ozen25.htm", Date: time.Date(2012, time.January, 23, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120205051148id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/22ozen1.htm", Date: time.Date(2012, time.January, 30, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120214055825id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/26ozen8.htm", Date: time.Date(2012, time.February, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120304145443id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/45ozen29.htm", Date: time.Date(2012, time.February, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120313144743id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/48ozen7.htm", Date: time.Date(2012, time.March, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120323032856id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/58ozen21.htm", Date: time.Date(2012, time.March, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120330060953id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/66ozen28.htm", Date: time.Date(2012, time.March, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120529003218id_/http://www.gks.ru/bgd/free/b04_03/IssWWW.exe/Stg/d03/70ozen4.htm", Date: time.Date(2012, time.April, 2, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120419150406id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/73ozen11.htm", Date: time.Date(2012, time.April, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120421225538id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/79ozen18.htm", Date: time.Date(2012, time.April, 16, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120502190434id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/88ozen25.htm", Date: time.Date(2012, time.April, 23, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120517123512id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/98ozen16.htm", Date: time.Date(2012, time.May, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120703192306id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/138ozen27.htm", Date: time.Date(2012, time.June, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120706224042id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/143ozen4.htm", Date: time.Date(2012, time.July, 2, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120716000110id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/146ozen11.htm", Date: time.Date(2012, time.July, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120818222432id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/171ozen15.htm", Date: time.Date(2012, time.August, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20121023035536id_/http://www.gks.ru/bgd/free/b04_03/IssWWW.exe/Stg/d04/188ozen29.htm", Date: time.Date(2012, time.August, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120915020135id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/195ozen12.htm", Date: time.Date(2012, time.September, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20120923230039id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/202ozen19.htm", Date: time.Date(2012, time.September, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20121002204425id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/212ozen26.htm", Date: time.Date(2012, time.September, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20121006085722id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/215ozen3.htm", Date: time.Date(2012, time.October, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20121013210617id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/219ozen10.htm", Date: time.Date(2012, time.October, 8, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20121029061255id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/233ozen24.htm", Date: time.Date(2012, time.October, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20121104020437id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/238ozen31.htm", Date: time.Date(2012, time.October, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20121114015048id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/241ozen8.htm", Date: time.Date(2012, time.November, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20121117220748id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/243ozen14.htm", Date: time.Date(2012, time.November, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20121201064003id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/253ozen21.htm", Date: time.Date(2012, time.November, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20121205010941id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/261ozen28.htm", Date: time.Date(2012, time.November, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20121210175050id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/264ozen5.htm", Date: time.Date(2012, time.December, 3, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20121216052232id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/268ozen12.htm", Date: time.Date(2012, time.December, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20121231162502id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/275ozen19.htm", Date: time.Date(2012, time.December, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130115172418id_/http://www.gks.ru/bgd/free/b04_03/IssWWW.exe/Stg/d04/283ozen26.htm", Date: time.Date(2012, time.December, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130115214408id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/3ozen11.htm", Date: time.Date(2013, time.January, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130123205237id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/5ozen16.htm", Date: time.Date(2013, time.January, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130127002234id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/8ozen23.htm", Date: time.Date(2013, time.January, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130212095535id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/25ozen6.htm", Date: time.Date(2013, time.February, 4, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130218151715id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/27ozen13.htm", Date: time.Date(2013, time.February, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130301043740id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/40ozen27.htm", Date: time.Date(2013, time.February, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130309061620id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/46ozen6.htm", Date: time.Date(2013, time.March, 4, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130324080051id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/56ozen20.htm", Date: time.Date(2013, time.March, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130401164854id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/64ozen27.htm", Date: time.Date(2013, time.March, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130418084610id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/67ozen3.htm", Date: time.Date(2013, time.April, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130415115259id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/71ozen10.htm", Date: time.Date(2013, time.April, 8, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130510012111id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/88ozen6.htm", Date: time.Date(2013, time.April, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130724125204id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/145ozen17.htm", Date: time.Date(2013, time.July, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130806015106id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/158ozen31.htm", Date: time.Date(2013, time.July, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130811120054id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/161ozen7.htm", Date: time.Date(2013, time.August, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130819051729id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/164ozen14.htm", Date: time.Date(2013, time.August, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20130914183727id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/187ozen11.htm", Date: time.Date(2013, time.September, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20131014073331id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/194ozen18.htm", Date: time.Date(2013, time.September, 16, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20131008135042id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d02/205ozen2.htm", Date: time.Date(2013, time.September, 30, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20131014232246id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/209ozen9.htm", Date: time.Date(2013, time.October, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20131019233818id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/214ozen16.htm", Date: time.Date(2013, time.October, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20131031184728id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/222ozen23.htm", Date: time.Date(2013, time.October, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20131106180307id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/227ozen30.htm", Date: time.Date(2013, time.October, 28, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20131110011555id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/230ozen7.htm", Date: time.Date(2013, time.November, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20131203205422id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/250ozen27.htm", Date: time.Date(2013, time.November, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20131210065207id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/252ozen4.htm", Date: time.Date(2013, time.December, 2, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20131217103901id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d02/256ozen11.htm", Date: time.Date(2013, time.December, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20140413032652id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/22ozen5.htm", Date: time.Date(2014, time.February, 3, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20140315082812id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/43ozen5.htm", Date: time.Date(2014, time.March, 3, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20140319102657id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/45ozen13.htm", Date: time.Date(2014, time.March, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20140322003546id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/51ozen19.htm", Date: time.Date(2014, time.March, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20140402174021id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/59ozen26.htm", Date: time.Date(2014, time.March, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20140427164129id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/81ozen23.htm", Date: time.Date(2014, time.April, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20140510055520id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/88ozen7.htm", Date: time.Date(2014, time.May, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20140807115252id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/132ozen2.htm", Date: time.Date(2014, time.June, 30, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20140811165550id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/158ozen6.htm", Date: time.Date(2014, time.August, 4, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20140827023805id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/168ozen20.htm", Date: time.Date(2014, time.August, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20140908033614id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/178ozen3.htm", Date: time.Date(2014, time.September, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20140914045939id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/183ozen10.htm", Date: time.Date(2014, time.September, 8, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20141004024622id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/197ozen24.htm", Date: time.Date(2014, time.September, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20141006211810id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/201ozen1.htm", Date: time.Date(2014, time.September, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20141011215352id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/206ozen8.htm", Date: time.Date(2014, time.October, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20141028015700id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/217ozen22.htm", Date: time.Date(2014, time.October, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20141109171148id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/224ozen29.htm", Date: time.Date(2014, time.October, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20141122124122id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/227ozen7.htm", Date: time.Date(2014, time.November, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20141126104349id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/229ozen12.htm", Date: time.Date(2014, time.November, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20141203105858id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/232ozen19.htm", Date: time.Date(2014, time.November, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150717144017id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d04/247ozen3.htm", Date: time.Date(2014, time.December, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150718152059id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d04/250ozen10.htm", Date: time.Date(2014, time.December, 8, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150718190757id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d04/255ozen17.htm", Date: time.Date(2014, time.December, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20141229015135id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/265ozen24.htm", Date: time.Date(2014, time.December, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150722232951id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/2ozen14.htm", Date: time.Date(2015, time.January, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150722213800id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/5ozen21.htm", Date: time.Date(2015, time.January, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150724222138id_/http://www.gks.ru/bgd/free/b04_03/IssWWW.exe/Stg/d05/ozen.htm", Date: time.Date(2015, time.January, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150726004404id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/21ozen4.htm", Date: time.Date(2015, time.February, 2, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150809111553id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/24ozen11.htm", Date: time.Date(2015, time.February, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150809112457id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/29ozen18.htm", Date: time.Date(2015, time.February, 16, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150809111918id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/38ozen26.htm", Date: time.Date(2015, time.February, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150809111613id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/40ozen4.htm", Date: time.Date(2015, time.March, 2, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150809112305id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/44ozen12.htm", Date: time.Date(2015, time.March, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150809112524id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/49ozen18.htm", Date: time.Date(2015, time.March, 16, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150809112539id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/57ozen25.htm", Date: time.Date(2015, time.March, 23, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150809111958id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/62ozen1.htm", Date: time.Date(2015, time.March, 30, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150809112330id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/67ozen8.htm", Date: time.Date(2015, time.April, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150720060808id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d05/69ozen15.htm", Date: time.Date(2015, time.April, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150727034107id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/77ozen22.htm", Date: time.Date(2015, time.April, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150809112345id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/84ozen29.htm", Date: time.Date(2015, time.April, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150809112041id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/87ozen7.htm", Date: time.Date(2015, time.May, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150809112057id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/93ozen20.htm", Date: time.Date(2015, time.May, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150809111527id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/107ozen3.htm", Date: time.Date(2015, time.June, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150814025745id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/110ozen10.htm", Date: time.Date(2015, time.June, 8, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150729231534id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/116ozen17.htm", Date: time.Date(2015, time.June, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150809111821id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/125ozen24.htm", Date: time.Date(2015, time.June, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150704200032id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d05/130ozen1.htm", Date: time.Date(2015, time.June, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170723115244id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/134ozen8.htm", Date: time.Date(2015, time.July, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20171217230705id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/138ozen15.htm", Date: time.Date(2015, time.July, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20171026044006id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/146ozen22.htm", Date: time.Date(2015, time.July, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180228012753id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d06/153ozen29.htm", Date: time.Date(2015, time.July, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150810035734id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/156ozen4.htm", Date: time.Date(2015, time.August, 3, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150817015209id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/158ozen12.htm", Date: time.Date(2015, time.August, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150824161107id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/166ozen19.htm", Date: time.Date(2015, time.August, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150830023640id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/174ozen26.htm", Date: time.Date(2015, time.August, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150910082701id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d06/176ozen2.htm", Date: time.Date(2015, time.August, 31, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150913010215id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/182ozen9.htm", Date: time.Date(2015, time.September, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150921215443id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/187ozen16.htm", Date: time.Date(2015, time.September, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20150927121717id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/194ozen23.htm", Date: time.Date(2015, time.September, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20151006175358id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/199ozen30.htm", Date: time.Date(2015, time.September, 28, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160115102724id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/203ozen7.htm", Date: time.Date(2015, time.October, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20151022071516id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/206ozen14.htm", Date: time.Date(2015, time.October, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20151029144837id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d06/215ozen21.htm", Date: time.Date(2015, time.October, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20151119042719id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/222ozen28.htm", Date: time.Date(2015, time.October, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20151109114756id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/225ozen5.htm", Date: time.Date(2015, time.November, 2, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160115102916id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/227ozen11.htm", Date: time.Date(2015, time.November, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20151123233848id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d06/233ozen18.htm", Date: time.Date(2015, time.November, 16, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160113022033id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d06/245ozen2.htm", Date: time.Date(2015, time.November, 30, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20151212215931id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/248ozen9.htm", Date: time.Date(2015, time.December, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160122073052id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d06/263ozen23.htm", Date: time.Date(2015, time.December, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160117115957id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/2ozen13.htm", Date: time.Date(2016, time.January, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160125154154id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/5ozen20.htm", Date: time.Date(2016, time.January, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160204114041id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d06/12ozen27.htm", Date: time.Date(2016, time.January, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160211021049id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d06/21ozen3.htm", Date: time.Date(2016, time.February, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160215003114id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/24ozen10.htm", Date: time.Date(2016, time.February, 8, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160320231953id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d06/40ozen2.htm", Date: time.Date(2016, time.February, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160313170556id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/44ozen11.htm", Date: time.Date(2016, time.March, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160428130203id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d06/46ozen16.htm", Date: time.Date(2016, time.March, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160402200609id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/56ozen23.htm", Date: time.Date(2016, time.March, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160404081956id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/62ozen30.htm", Date: time.Date(2016, time.March, 28, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160413171215id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/66ozen6.htm", Date: time.Date(2016, time.April, 4, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160729004241id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d06/75ozen20.htm", Date: time.Date(2016, time.April, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160511175436id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/87ozen6.htm", Date: time.Date(2016, time.May, 4, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160521035753id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/89ozen12.htm", Date: time.Date(2016, time.May, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160521035758id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d06/91ozen18.htm", Date: time.Date(2016, time.May, 16, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160731184148id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d06/101ozen25.htm", Date: time.Date(2016, time.May, 23, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160606030627id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/107ozen1.htm", Date: time.Date(2016, time.May, 30, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160612182044id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d06/110ozen8.htm", Date: time.Date(2016, time.June, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160622190747id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d06/114ozen16.htm", Date: time.Date(2016, time.June, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160707182527id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/134ozen6.htm", Date: time.Date(2016, time.July, 4, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160718030904id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/137ozen13.htm", Date: time.Date(2016, time.July, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160721195349id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/144ozen20.htm", Date: time.Date(2016, time.July, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160804031319id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/153ozen27.htm", Date: time.Date(2016, time.July, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160806064238id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/155ozen3.htm", Date: time.Date(2016, time.August, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160814194703id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/158ozen10.htm", Date: time.Date(2016, time.August, 8, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160915121545id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/164ozen17.htm", Date: time.Date(2016, time.August, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160824193941id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d01/170ozen24.htm", Date: time.Date(2016, time.August, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160903155726id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/176ozen31.htm", Date: time.Date(2016, time.August, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160910164706id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/180ozen7.htm", Date: time.Date(2016, time.September, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160917011641id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/183ozen14.htm", Date: time.Date(2016, time.September, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20160924073349id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/192ozen21.htm", Date: time.Date(2016, time.September, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20161001060128id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/199ozen28.htm", Date: time.Date(2016, time.September, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20161008051816id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/203ozen5.htm", Date: time.Date(2016, time.October, 3, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20161015072310id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/206ozen12.htm", Date: time.Date(2016, time.October, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20161022121129id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/213ozen19.htm", Date: time.Date(2016, time.October, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20161029122900id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/221ozen26.htm", Date: time.Date(2016, time.October, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20161105164614id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/224ozen2.htm", Date: time.Date(2016, time.October, 31, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20161112113451id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/227ozen9.htm", Date: time.Date(2016, time.November, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20161119111200id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/229.htm", Date: time.Date(2016, time.November, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20161125141728id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/240.htm", Date: time.Date(2016, time.November, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20161202143303id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/245.htm", Date: time.Date(2016, time.November, 28, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20161209152424id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/248.htm", Date: time.Date(2016, time.December, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20161216161346id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/252.htm", Date: time.Date(2016, time.December, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20161223175739id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/259.htm", Date: time.Date(2016, time.December, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170113161721id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/2.htm", Date: time.Date(2017, time.January, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170127091043id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d01/9.htm", Date: time.Date(2017, time.January, 23, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170210075711id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d01/24.htm", Date: time.Date(2017, time.February, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170218093422id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/26.htm", Date: time.Date(2017, time.February, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170224220713id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/33.htm", Date: time.Date(2017, time.February, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170303130809id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/40.htm", Date: time.Date(2017, time.February, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20170312053714id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/45.htm", Date: time.Date(2017, time.March, 6, 0, 0, 0, 0, time.UTC)},
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
	{URL: "https://web.archive.org/web/20171006135819id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/201.htm", Date: time.Date(2017, time.October, 2, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20171013170329id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/205.htm", Date: time.Date(2017, time.October, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20171018232933id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/211.htm", Date: time.Date(2017, time.October, 16, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20171026002012id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/218.htm", Date: time.Date(2017, time.October, 23, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20171101235228id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/223.htm", Date: time.Date(2017, time.October, 30, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20171112015539id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/226.htm", Date: time.Date(2017, time.November, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20171116000945id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/228.htm", Date: time.Date(2017, time.November, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20171123004523id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/236.htm", Date: time.Date(2017, time.November, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20171202084855id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/244.htm", Date: time.Date(2017, time.November, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20171207042630id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/247.htm", Date: time.Date(2017, time.December, 4, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20171216014056id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/250.htm", Date: time.Date(2017, time.December, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20171221174855id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/257.htm", Date: time.Date(2017, time.December, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20171229062558id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/264.htm", Date: time.Date(2017, time.December, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180111210906id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/2.htm", Date: time.Date(2018, time.January, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180117192536id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/5.htm", Date: time.Date(2018, time.January, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180125161644id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/9.htm", Date: time.Date(2018, time.January, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180131212325id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/17.htm", Date: time.Date(2018, time.January, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180208153148id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/24.htm", Date: time.Date(2018, time.February, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180217175534id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/26.htm", Date: time.Date(2018, time.February, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180221214241id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/33.htm", Date: time.Date(2018, time.February, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180303082454id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d03/40.htm", Date: time.Date(2018, time.February, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180310211242id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/45.htm", Date: time.Date(2018, time.March, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180315145351id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/46.htm", Date: time.Date(2018, time.March, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180324163322id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/54.htm", Date: time.Date(2018, time.March, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180330153136id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/60.htm", Date: time.Date(2018, time.March, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180407023556id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/64.htm", Date: time.Date(2018, time.April, 2, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180414033045id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/68.htm", Date: time.Date(2018, time.April, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180423064726id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/74.htm", Date: time.Date(2018, time.April, 16, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180425194235id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/80.htm", Date: time.Date(2018, time.April, 23, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180610032501id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/85.htm", Date: time.Date(2018, time.April, 28, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180514180117id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/88.htm", Date: time.Date(2018, time.May, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180516201024id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/90.htm", Date: time.Date(2018, time.May, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180528094655id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d03/98.htm", Date: time.Date(2018, time.May, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180610024455id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/105.htm", Date: time.Date(2018, time.May, 28, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180610022851id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/109.htm", Date: time.Date(2018, time.June, 4, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180618174154id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/111.htm", Date: time.Date(2018, time.June, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180623143057id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/118.htm", Date: time.Date(2018, time.June, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180629161228id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/126.htm", Date: time.Date(2018, time.June, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180707233604id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/129.htm", Date: time.Date(2018, time.July, 2, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180714085215id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/133.htm", Date: time.Date(2018, time.July, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180722200009id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/138.htm", Date: time.Date(2018, time.July, 16, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180728234154id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d03/144.htm", Date: time.Date(2018, time.July, 23, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180803200308id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/150.htm", Date: time.Date(2018, time.July, 30, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180811102042id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/153.htm", Date: time.Date(2018, time.August, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180818104535id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/156.htm", Date: time.Date(2018, time.August, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180826213849id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/162.htm", Date: time.Date(2018, time.August, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180902022255id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/170.htm", Date: time.Date(2018, time.August, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180906192329id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/173.htm", Date: time.Date(2018, time.September, 3, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180915015205id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/176.htm", Date: time.Date(2018, time.September, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180921195009id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/183.htm", Date: time.Date(2018, time.September, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20180930125811id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/191.htm", Date: time.Date(2018, time.September, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20181007061210id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/194.htm", Date: time.Date(2018, time.October, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20181013075137id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/198.htm", Date: time.Date(2018, time.October, 8, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20181020063640id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/204.htm", Date: time.Date(2018, time.October, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20181027113256id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/209.htm", Date: time.Date(2018, time.October, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20181103063159id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/215.htm", Date: time.Date(2018, time.October, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20181111072253id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/218.htm", Date: time.Date(2018, time.November, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20181116230338id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/220.htm", Date: time.Date(2018, time.November, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20181123194955id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/227.htm", Date: time.Date(2018, time.November, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20181201071346id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/235.htm", Date: time.Date(2018, time.November, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20181209050542id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d01/237.htm", Date: time.Date(2018, time.December, 3, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20181214030905id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/242.htm", Date: time.Date(2018, time.December, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20181221181843id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/247.htm", Date: time.Date(2018, time.December, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20181227195211id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d01/254.htm", Date: time.Date(2018, time.December, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190114144059id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/2.htm", Date: time.Date(2019, time.January, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190119005100id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/4.htm", Date: time.Date(2019, time.January, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190126061654id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/8.htm", Date: time.Date(2019, time.January, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190203083114id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/16.htm", Date: time.Date(2019, time.January, 28, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190209203722id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/23.htm", Date: time.Date(2019, time.February, 4, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190217012230id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/25.htm", Date: time.Date(2019, time.February, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190224002358id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/30.htm", Date: time.Date(2019, time.February, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190302030010id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/38.htm", Date: time.Date(2019, time.February, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190308225047id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/42.htm", Date: time.Date(2019, time.March, 4, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190316145711id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/44.htm", Date: time.Date(2019, time.March, 11, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190323085637id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/49.htm", Date: time.Date(2019, time.March, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190329195036id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/58.htm", Date: time.Date(2019, time.March, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190405080229id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/62.htm", Date: time.Date(2019, time.April, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190412230709id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/66.htm", Date: time.Date(2019, time.April, 8, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190420031844id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/71.htm", Date: time.Date(2019, time.April, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190426235659id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/74.htm", Date: time.Date(2019, time.April, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190509072056id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/82.htm", Date: time.Date(2019, time.April, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190511032511id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/85.htm", Date: time.Date(2019, time.May, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190518143535id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/87.htm", Date: time.Date(2019, time.May, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190526032203id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/91.htm", Date: time.Date(2019, time.May, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190601222712id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/99.htm", Date: time.Date(2019, time.May, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190609024134id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/104.htm", Date: time.Date(2019, time.June, 3, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190616225335id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/107.htm", Date: time.Date(2019, time.June, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190622122406id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/113.htm", Date: time.Date(2019, time.June, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190923151131id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d04/119.htm", Date: time.Date(2019, time.June, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190705101407id_/http://www.gks.ru:80/bgd/free/b04_03/IssWWW.exe/Stg/d04/125.htm", Date: time.Date(2019, time.July, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190713012734id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/129.htm", Date: time.Date(2019, time.July, 8, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190718194618id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/134.htm", Date: time.Date(2019, time.July, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190727195147id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/137.htm", Date: time.Date(2019, time.July, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190804055809id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/146.htm", Date: time.Date(2019, time.July, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190809223854id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/149.htm", Date: time.Date(2019, time.August, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190816235522id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/151.htm", Date: time.Date(2019, time.August, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190824044944id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/158.htm", Date: time.Date(2019, time.August, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190904204831id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/167.htm", Date: time.Date(2019, time.August, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190912130427id_/http://www.gks.ru:80/bgd/free/B04_03/IssWWW.exe/Stg/d04/169.htm", Date: time.Date(2019, time.September, 2, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190923185743id_/https://www.gks.ru/bgd/free/b04_03/IssWWW.exe/Stg/d04/179.htm", Date: time.Date(2019, time.September, 16, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20190926174141id_/https://www.gks.ru/bgd/free/b04_03/IssWWW.exe/Stg/d04/185.htm", Date: time.Date(2019, time.September, 23, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20191117043927id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d04/189.htm", Date: time.Date(2019, time.September, 30, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20191114215717id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d04/194.htm", Date: time.Date(2019, time.October, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20191017035404id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d04/197.htm", Date: time.Date(2019, time.October, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20191215130043id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d04/211.htm", Date: time.Date(2019, time.October, 28, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200925110605id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d04/214.htm", Date: time.Date(2019, time.November, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200418070911id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/2.htm", Date: time.Date(2020, time.January, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200414143630id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/4.htm", Date: time.Date(2020, time.January, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20210118041528id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/14.htm", Date: time.Date(2020, time.January, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20201202005441id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/22.htm", Date: time.Date(2020, time.February, 3, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20201024035733id_/https://rosstat.gov.ru/bgd/free/b04_03/IssWWW.exe/Stg/d05/25.htm", Date: time.Date(2020, time.February, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20210118045641id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/37.htm", Date: time.Date(2020, time.February, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200329062422id_/https://gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/54.htm", Date: time.Date(2020, time.March, 23, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200404024955id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/60.htm", Date: time.Date(2020, time.March, 30, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200411111557id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/65.htm", Date: time.Date(2020, time.April, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200418233958id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/67.htm", Date: time.Date(2020, time.April, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200423093327id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/71.htm", Date: time.Date(2020, time.April, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200429184034id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/80.htm", Date: time.Date(2020, time.April, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200521185406id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/84.htm", Date: time.Date(2020, time.May, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200524075655id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/86.htm", Date: time.Date(2020, time.May, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200531215446id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/88.htm", Date: time.Date(2020, time.May, 18, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200821224205id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/94.htm", Date: time.Date(2020, time.May, 25, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200821224203id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/101.htm", Date: time.Date(2020, time.June, 1, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200630081811id_/https://www.gks.ru/bgd/free/b04_03/IssWWW.exe/Stg/d05/104.htm", Date: time.Date(2020, time.June, 8, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200821224200id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/107.htm", Date: time.Date(2020, time.June, 15, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200628181530id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/113.htm", Date: time.Date(2020, time.June, 22, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200702231501id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/120.htm", Date: time.Date(2020, time.June, 29, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20210523114907id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/125.htm", Date: time.Date(2020, time.July, 6, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200716163456id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/127.htm", Date: time.Date(2020, time.July, 13, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200921132931id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/133.htm", Date: time.Date(2020, time.July, 20, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200801050939id_/https://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/142.htm", Date: time.Date(2020, time.July, 27, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20210525045836id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/144.htm", Date: time.Date(2020, time.August, 3, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200812205748id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/147.htm", Date: time.Date(2020, time.August, 10, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200821211359id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/152.htm", Date: time.Date(2020, time.August, 17, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20210525085929id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/158.htm", Date: time.Date(2020, time.August, 24, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200903021523id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/163.htm", Date: time.Date(2020, time.August, 31, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200918224527id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/166.htm", Date: time.Date(2020, time.September, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200920084312id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/171.htm", Date: time.Date(2020, time.September, 14, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20200925141419id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/176.htm", Date: time.Date(2020, time.September, 21, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20201005180559id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/183.htm", Date: time.Date(2020, time.September, 28, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20201013175057id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/188.htm", Date: time.Date(2020, time.October, 5, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20201028060612id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/190.htm", Date: time.Date(2020, time.October, 12, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20210526102612id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/195.htm", Date: time.Date(2020, time.October, 19, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20201106154117id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d05/204.htm", Date: time.Date(2020, time.October, 26, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20210526163217id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/206.htm", Date: time.Date(2020, time.November, 2, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20210526163243id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/209.htm", Date: time.Date(2020, time.November, 9, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20201124084711id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/213.htm", Date: time.Date(2020, time.November, 16, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20210526183711id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/218.htm", Date: time.Date(2020, time.November, 23, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20210527000515id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/225.htm", Date: time.Date(2020, time.November, 30, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20201211031227id_/https://rosstat.gov.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/228.htm", Date: time.Date(2020, time.December, 7, 0, 0, 0, 0, time.UTC)},
	{URL: "https://web.archive.org/web/20210527000625id_/http://www.gks.ru/bgd/free/B04_03/IssWWW.exe/Stg/d02/234.htm", Date: time.Date(2020, time.December, 14, 0, 0, 0, 0, time.UTC)},
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
	// Выпуски 2016 г. пишут «Алмагель, суспензия»; канонический ряд тот же («Алмагель» — extra-ряд).
	"Алмагель, суспензия":               "Алмагель",
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
	// Вариант написания выпусков 2015–2016: «(кроме в столовой в организации)» — та же позиция.
	"Обед в столовой, кафе, закусочной (кроме в столовой в организации)": "Обед в столовой, кафе, закусочной (кроме столовой в организации), на 1 человека",
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
	"Говядина тушеная",      // консервы 2009–2013 (старый формат, отдельная строка «Говядина тушеная»)
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
	// Нижний регистр и снятие дефисных переносов Word: старые выпуски (2015–2017) писали
	// «К предыдущей дате регистрации» с большой буквы и переносили слова дефисом в шапках и
	// названиях товаров («19 сен-тября», «госу- дарственного») — из-за чего якоря и метки дат
	// не совпадали. Нормализация общая для якорей, меток и названий товаров: ключи карты
	// канонизации строятся тем же Key, поэтому имена источника и карты склеиваются одинаково
	// («Сахар-песок» и «Сахар-песок» — один ключ, регистр не значим).
	return strings.ReplaceAll(strings.ToLower(strings.Join(strings.Fields(text), "")), "-", "")
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

// ipcWeeksBulletinsTitleRangeRe — диапазон «с … по … <год>» в первой строке текста. Запасной
// путь для выпусков, у которых <title> — только «Об оценке индекса потребительских цен»:
// три снимка 2011 и 2014 гг. период в заголовке не несут, но первой строкой текста он есть.
var ipcWeeksBulletinsTitleRangeRe = regexp.MustCompile(`(?i)с\s+(\d{1,2})\s+([а-яё]+)\s+по\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})`)

// ipcWeeksBulletinsTitleSameMonthRe — тот же диапазон внутри одного месяца («с 11 по 16 мая
// 2011 года»): у выпусков 2011-05-16 и 2014-08-25 название месяца стоит только у второй даты.
var ipcWeeksBulletinsTitleSameMonthRe = regexp.MustCompile(`(?i)с\s+(\d{1,2})\s+по\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})`)

// ipcWeeksBulletinsTitleFromBody — запасной титул из начала документа: первый диапазон
// «с … по … <год>». Пустая строка — диапазона нет (тогда ошибка титула остаётся ошибкой).
func ipcWeeksBulletinsTitleFromBody(doc *goquery.Document) string {
	text := ipcWeeksBulletinsText(doc.Find("body").Text())
	if len(text) > 400 {
		text = text[:400]
	}
	if match := ipcWeeksBulletinsTitleRangeRe.FindStringSubmatch(text); match != nil {
		return fmt.Sprintf("с %s %s по %s %s %s", match[1], match[2], match[3], match[4], match[5])
	}
	if match := ipcWeeksBulletinsTitleSameMonthRe.FindStringSubmatch(text); match != nil {
		return fmt.Sprintf("с %s %s по %s %s %s", match[1], match[3], match[2], match[3], match[4])
	}
	return ""
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
		if !strings.HasPrefix(ipcWeeksBulletinsKey(strings.Join(texts, " ")), anchor) &&
			// Опечатка источника: в 166ozen19.htm (2015-08-17) строка агрегата напечатана как
			// «Индекс отребительских цен (оценка)» — без «п». Признак «требительскихцен(оценка)»
			// однозначен: других строк с «(оценка)» в выпуске нет, а без допуска выпуск терялся бы.
			!strings.Contains(ipcWeeksBulletinsKey(strings.Join(texts, " ")), "требительскихцен(оценка)") {
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
	var specific *goquery.Selection
	doc.Find("td, th").EachWithBreak(func(_ int, cell *goquery.Selection) bool {
		if ipcWeeksBulletinsKey(cell.Text()) != ipcWeeksBulletinsKey(ipcWeeksBulletinsChangesAnchor) {
			return true
		}
		table := cell.Closest("table")
		if table.Length() == 0 || !ipcWeeksBulletinsTableHasPreviousAnchor(table) {
			return true
		}
		specific = table
		return false
	})
	if specific != nil {
		return specific, nil
	}

	// Ранние выпуски 2009–2016 подписи «Изменения цен» не печатают. Их детальная таблица ищется
	// по двум признакам, и берётся большая из найденных: у выпусков 2009–2013 в шапке нет и
	// группового якоря «к предыдущей дате регистрации» — вместо него стоят сравнения дат
	// («6 апреля к 30 марта», «11 января 2010г. к концу декабря 2009г.»). Агрегатная таблица
	// того же выпуска несёт якорь (у части лет), но в ней 4 строки, а в детальной — десятки.
	var anchored, compared *goquery.Selection
	doc.Find("table").Each(func(_ int, table *goquery.Selection) {
		if ipcWeeksBulletinsTableHasPreviousAnchor(table) {
			if anchored == nil || table.Find("tr").Length() > anchored.Find("tr").Length() {
				anchored = table
			}
		}
		if ipcWeeksBulletinsHeaderHasWeekComparison(table) {
			if compared == nil || table.Find("tr").Length() > compared.Find("tr").Length() {
				compared = table
			}
		}
	})
	if compared != nil && (anchored == nil || compared.Find("tr").Length() > anchored.Find("tr").Length()) {
		return compared, nil
	}
	if anchored != nil {
		return anchored, nil
	}
	return nil, fmt.Errorf("таблица «%s» с якорем «%s» не найдена", ipcWeeksBulletinsChangesAnchor, ipcWeeksBulletinsPreviousAnchor)
}

// ipcWeeksBulletinsTableHasPreviousAnchor — есть ли в шапке таблицы (первые четыре строки)
// групповой якорь «к предыдущей дате регистрации». Шапка читается по одной строке, а не срезом:
// Slice(0, 4) на таблице короче четырёх строк даёт nil-узел (паника в Text).
func ipcWeeksBulletinsTableHasPreviousAnchor(table *goquery.Selection) bool {
	var head strings.Builder
	table.Find("tr").EachWithBreak(func(i int, row *goquery.Selection) bool {
		head.WriteString(ipcWeeksBulletinsKey(row.Text()))
		return i < 3
	})
	return strings.Contains(head.String(), ipcWeeksBulletinsKey(ipcWeeksBulletinsPreviousAnchor))
}

// ipcWeeksBulletinsHeaderHasWeekComparison — шапка старого формата (2009–2013): в ней стоит
// сравнение дат «<день> <месяц> к …» или «… к концу <месяца>». Признак — отдельное слово «к» и
// название месяца; агрегатная таблица этих выпусков «к» не содержит.
func ipcWeeksBulletinsHeaderHasWeekComparison(table *goquery.Selection) bool {
	var head strings.Builder
	table.Find("tr").EachWithBreak(func(i int, row *goquery.Selection) bool {
		head.WriteString(" ")
		head.WriteString(ipcWeeksBulletinsText(row.Text()))
		return i < 3
	})
	text := strings.ToLower(head.String())
	if !strings.Contains(text, " к ") {
		return false
	}
	for month := range util.MonthsToNum {
		if strings.Contains(text, month) {
			return true
		}
	}
	return false
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
func ipcWeeksBulletinsValueColumn(grid [][]string, dataStart int, date time.Time, monthWord string) int {
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
	// Старый формат 2009–2013 группового якоря не печатает: недельная колонка — самая левая
	// колонка данных, в шапке которой стоит метка последнего дня периода («6 апреля»,
	// «11 января»). Проверка та же, что подтверждает колонку в обычном выпуске, поэтому сдвиг
	// колонок не станет тихой записью чужой недели.
	want := ipcWeeksBulletinsLabel(date, monthWord)
	for col := 1; col < width; col++ {
		for r := 0; r < dataStart; r++ {
			if ipcWeeksBulletinsLabelMatch(ipcWeeksBulletinsCell(grid[r], col), want) {
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

// ipcWeeksBulletinsUnitSuffixes — единицы измерения, которые старый формат 2009–2015 писал в
// имени товара («Яйца куриные, 10 шт.»), а карта канонизации — в каноническом имени. Снятие
// суффикса — второй шанс поиска: без него каждый старый выпуск давал бы десятки рядов под
// исходными именами, хотя это те же товары.
var ipcWeeksBulletinsUnitSuffixes = []string{
	", м2 общей площади", ", за месяц с человека", " (в месяц с человека)",
	", за кв.метр", ", за Гкал", ", 10 шт.", ", кг", ", л", ", м3", ", шт.", ", пара",
	", поездка", ", Гкал",
}

// ipcWeeksBulletinsWithoutUnit — имя без хвостовой единицы измерения (или исходное, если
// суффикса нет).
func ipcWeeksBulletinsWithoutUnit(name string) string {
	for _, suffix := range ipcWeeksBulletinsUnitSuffixes {
		if strings.HasSuffix(name, suffix) {
			return strings.TrimSuffix(name, suffix)
		}
	}
	return name
}

// ipcWeeksBulletinsOldAliases — старые написания 2009–2015, которые не сводятся к каноническому
// имени снятием единицы: водка и коммунальные услуги меняли формулировку, хлеб — состав строки,
// рыба писалась с двойным «н». Ключи карты перекладываются на Key ниже, как и основная карта.
var ipcWeeksBulletinsOldAliases = map[string]string{
	"Баранина": "Баранина (кроме бескостного мяса), кг",
	"Водка крепостью 40% об. спирта обыкновенного качества":                   "Водка крепостью 40% об.спирта и выше, л",
	"Водка крепостью 40% об.спирта и выше":                                    "Водка крепостью 40% об.спирта и выше, л",
	"Водка крепостью 40% об.спирта и выше обыкновенного качества":             "Водка крепостью 40% об.спирта и выше, л",
	"Водка обыкновенного качества":                                            "Водка крепостью 40% об.спирта и выше, л",
	"Водоотведение с использованием счетчиков индивидуального учета":          "Водоотведение, м3",
	"Водоснабжение горячее и водоотведение":                                   "Водоснабжение горячее, м3",
	"Водоснабжение горячее с использованием счетчиков индивидуального учета":  "Водоснабжение горячее, м3",
	"Водоснабжение холодное и водоотведение":                                  "Водоснабжение холодное, м3",
	"Водоснабжение холодное с использованием счетчиков индивидуального учета": "Водоснабжение холодное, м3",
	"Колбаса вареная 1 сорта":                                                 "Колбаса вареная, кг",
	"Колбаса полукопченая":                                                    "Колбаса полукопченая и варено-копченая, кг",
	"Куры (кроме куриных окорочков)":                                          "Куры охлажденные и мороженые, кг",
	"Рыба замороженная неразделанная":                                         "Рыба мороженая неразделанная, кг",
	"Рыба мороженная неразделанная":                                           "Рыба мороженая неразделанная, кг",
	"Сыры твердые и мягкие":                                                   "Сыры сычужные твердые и мягкие, кг",
	"Творог": "Творог, кг",
	"Хлеб пшеничный из муки 1 и 2 сортов":                        "Хлеб и булочные изделия из пшеничной муки различных сортов, кг",
	"Хлеб ржаной и ржано-пшеничный":                              "Хлеб из ржаной муки и из смеси муки ржаной и пшеничной, кг",
	"Хлеб и булочные изделия из пшеничной муки различных сортов": "Хлеб и булочные изделия из пшеничной муки различных сортов, кг",
	"Хлеб из ржаной муки и из смеси муки ржаной и пшеничной":     "Хлеб из ржаной муки и из смеси муки ржаной и пшеничной, кг",
	"Молоко питьевое цельное пастеризованное 2,5-3,2% жирности":  "Молоко питьевое цельное пастеризованное 2,5-3,2% жирности, л",
	"Молоко питьевое цельное стерилизованное 2,5-3,2% жирности":  "Молоко питьевое цельное стерилизованное 2,5-3,2% жирности, л",
}

// ipcWeeksBulletinsOldAliasKeys — та же карта по ключам без пробелов и дефисов.
var ipcWeeksBulletinsOldAliasKeys = func() map[string]string {
	keys := make(map[string]string, len(ipcWeeksBulletinsOldAliases))
	for source, canonical := range ipcWeeksBulletinsOldAliases {
		keys[ipcWeeksBulletinsKey(source)] = canonical
	}
	return keys
}()

// ipcWeeksBulletinsSeriesName — имя ряда для строки источника: каноническое из карты, исходное
// для extra-ряда или исходное с предупреждением. Предупреждение выводится один раз на имя:
// у выпуска десятки строк, а незнакомых имён обычно одно-два.
func ipcWeeksBulletinsSeriesName(source string, warned map[string]bool) string {
	// Старый формат 2009–2015: единица измерения в имени и другие написания. Три попытки —
	// точное имя, имя без единицы и вариант с «е» вместо «ё» — и для каждой основная карта и
	// карта старых алиасов. Точное совпадение всегда первое, поэтому поведение 2016–2021 не
	// меняется.
	for _, candidate := range []string{source, ipcWeeksBulletinsWithoutUnit(source)} {
		for _, variant := range []string{candidate, strings.ReplaceAll(candidate, "ё", "е")} {
			key := ipcWeeksBulletinsKey(variant)
			if canonical, ok := ipcWeeksBulletinsSeriesKeys[key]; ok {
				return canonical
			}
			if canonical, ok := ipcWeeksBulletinsOldAliasKeys[key]; ok {
				return canonical
			}
		}
	}
	key := ipcWeeksBulletinsKey(source)
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
		if fallback := ipcWeeksBulletinsTitleFromBody(doc); fallback != "" {
			date, monthWord, err = ipcWeeksBulletinsTitleDate(fallback)
		}
		if err != nil {
			return nil, fmt.Errorf("титул %q: %w", title, err)
		}
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
	valueCol := ipcWeeksBulletinsValueColumn(grid, dataStart, date, monthWord)
	if valueCol < 0 {
		return nil, fmt.Errorf("колонка недельного значения не подтверждена ни якорем «%s», ни меткой «%s»", ipcWeeksBulletinsPreviousAnchor, ipcWeeksBulletinsLabel(date, monthWord))
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
