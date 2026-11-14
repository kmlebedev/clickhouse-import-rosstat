package cbr

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

var cbrMacroView = util.View{
	Name: "v_cbr_macro",
	Tables: []string{
		"cbr_key_rate", "cbr_currency_usd", "cbr_gold", "cbr_ruania", "cbr_m2", "cbr_credit_m2x",
		"cbr_bank_int_rate", "cbr_int_rate_loans", "cbr_int_rate_loans_ind", "cbr_mortgage_rate",
		"cbr_indicators_cpd", "cbr_infl_exp",
		"cbr_loans_to_individuals", "cbr_loans_to_corporations", "cbr_households_b_mes",
		"cbr_sors_funds",
	},
	Select: `SELECT 'cbr' AS source, 'cbr_key_rate' AS series, date, toFloat64(rate) AS value FROM cbr_key_rate FINAL
UNION ALL SELECT 'cbr', 'cbr_currency_usd', date, toFloat64(price) FROM cbr_currency_usd FINAL
UNION ALL SELECT 'cbr', 'cbr_gold', date, toFloat64(price) FROM cbr_gold FINAL
UNION ALL SELECT 'cbr', 'cbr_ruania_ruo', date, toFloat64(ruo) FROM cbr_ruania FINAL
UNION ALL SELECT 'cbr', 'cbr_ruania_vol', date, toFloat64(vol) FROM cbr_ruania FINAL WHERE vol IS NOT NULL
UNION ALL SELECT 'cbr', 'cbr_ruania_trans', date, toFloat64(trans) FROM cbr_ruania FINAL WHERE trans IS NOT NULL
UNION ALL SELECT 'cbr', 'cbr_ruania_min_rate', date, toFloat64(minRate) FROM cbr_ruania FINAL WHERE minRate IS NOT NULL
UNION ALL SELECT 'cbr', 'cbr_ruania_max_rate', date, toFloat64(maxRate) FROM cbr_ruania FINAL WHERE maxRate IS NOT NULL
UNION ALL SELECT 'cbr', 'cbr_ruania_p25', date, toFloat64(percentile25) FROM cbr_ruania FINAL WHERE percentile25 IS NOT NULL
UNION ALL SELECT 'cbr', 'cbr_ruania_p75', date, toFloat64(Percentile75) FROM cbr_ruania FINAL WHERE Percentile75 IS NOT NULL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(balance) FROM cbr_m2 FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(value) FROM cbr_credit_m2x FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(rate) FROM cbr_bank_int_rate FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(rate) FROM cbr_int_rate_loans FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(rate) FROM cbr_int_rate_loans_ind FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(rate) FROM cbr_mortgage_rate FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(value) FROM cbr_indicators_cpd FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(value) FROM cbr_infl_exp FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(balance) FROM cbr_loans_to_individuals FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(balance) FROM cbr_loans_to_corporations FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(balance) FROM cbr_households_b_mes FINAL
UNION ALL SELECT 'cbr', toString(name), date, toFloat64(balance) FROM cbr_sors_funds FINAL`,
	Comment: "Макроряды ЦБ РФ: ключевая ставка, курс USD, учётная цена золота, RUONIA, М2 SA, кредиты физлицам и юрлицам, ставки по вкладам, кредитам юрлицам и физлицам по срокам, ипотека, ИПЦ (темпы), инфляционные ожидания, балансы домашних хозяйств, средства клиентов банков (счета организаций, депозиты юрлиц, вклады физлиц с учётом эскроу и без, счета эскроу, средства ИП — всего и в разрезе «рубли / иностранная валюта»). Платёжного баланса здесь нет: его ряды живут в отдельной витрине v_cbr_bop (source cbr_bop_est и cbr_bop_standard, свои единицы и виды периода) — внешние условия и курс по платёжному балансу искать там. Перед расчётом читать v_series_catalog по (source, series): единицы и привязка даты различаются между рядами",
	Columns: map[string]string{
		"source": "Импортёр-источник: всегда cbr",
		"series": "Название ряда; совпадает с series в v_series_catalog. У кредитов физлицам по регионам, кредитов юрлицам по видам деятельности (ОКВЭД), ставок по вкладам, ИПЦ, инфляционных ожиданий и балансов домохозяйств название — текст исходной таблицы ЦБ (строка региона или вида деятельности, заголовок колонки срока); у ставок по кредитам юрлицам имя — комбинация двух ячеек источника, группы заёмщиков («Всего»/«СМП») и срока кредита, которую собирает код (стандарт §6.8); у ставок по кредитам физлицам — тоже комбинация ячеек источника: совокупность («Всего (кредиты физлицам)»/«Автокредиты»; префикс отличает их от одноимённых рядов кредитов юрлицам) и разрез — срок кредита либо «по всем срокам» (агрегат группы «по всем срокам», два ряда); у восьми рядов «Автокредиты: <срок>» публикация прекращена источником на 2025-12 (см. description); у ипотечной ставки РФ — одно имя, объявленное кодом («Средневзвешенная ставка по жилищным кредитам, РФ»), без разбиения на группу и срок; у средств клиентов банков — имя-код, объявленное в cbr/series_meta.go (clients_total, org_accounts, corp_deposits, households_deposits_escrow, households_deposits, escrow_accounts, ip_accounts), с суффиксом валютного разреза: без суффикса — лист «итого» (рубли + иностранная валюта и драгоценные металлы), _rub — только рубли, _fx — только иностранная валюта и драгоценные металлы; русские названия и смысл — в v_series_catalog",
		"date":   "Дата наблюдения (Date). Сдвиги даты различаются по рядам: кредиты — на месяц вперёд, инфляционные ожидания — 14-е число, ставки по вкладам, кредитам юрлицам и физлицам и ипотеке — предпоследний день месяца (единая конвенция контура); см. description в v_series_catalog",
		"value":  "Значение ряда (Float64). Единицы зависят от ряда: % годовых, руб. за USD, руб./грамм, млрд руб., млн руб., % к предыдущему месяцу — см. unit в v_series_catalog",
	},
}

func publishSeries(ctx context.Context, conn driver.Conn, meta []util.SeriesMeta) error {
	if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
		return err
	}
	_, err := util.CreateView(ctx, conn, cbrMacroView)
	return err
}

// afterCbrMacro — производные импортёров пакета: каталог рядов и общая витрина
// v_cbr_macro. Публикуются шаблоном после Send точек и до записи строки версии (§8 п. 13).
func afterCbrMacro(meta []util.SeriesMeta) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error { return publishSeries(ctx, conn, meta) }
}

// cbrPskView — витрина квартального корпуса среднерыночных значений ПСК кредитных организаций
// (source = имя таблицы, единственный источник). Отдельная витрина, а не общая v_cbr_macro:
// у ПСК иная семантика (законодательный кап, применяемый в следующем квартале, а не ставка
// выданных кредитов) и широкая форма — обе величины одной строкой (спека §3.5 C, прецеденты
// расширенных витрин — v_cbr_mt_forecast, v_cbr_dip_bulletin). Ключ таблицы
// (category_code, date): пересмотр категории новым выпуском того же периода заменяет строку,
// поэтому витрина показывает последнюю редакцию каждого (код, квартал).
var cbrPskView = util.View{
	Name:   "v_cbr_psk",
	Tables: []string{cbrPskTable},
	Select: "SELECT date, category_code, category, psk_avg, psk_max, source_file FROM cbr_psk FINAL ORDER BY (category_code, date)",
	Comment: "Среднерыночные значения полной стоимости потребительских кредитов (займов) (ПСК) " +
		"кредитных организаций (тип ko) по категориям, квартальный корпус 2014Q3 … 2026Q2: 48 " +
		"выпусков трёх эр формата (23 PDF 2014-11 … 2020-05, 13 XLS 2020-08 … 2023-08, 12 XLSX " +
		"2023-11 … 2026-08). Это не фактические ставки по выданным кредитам, а установленный " +
		"законом предел: по ч. 11 ст. 6 Федерального закона № 353-ФЗ ПСК по договору не может " +
		"превышать среднерыночное значение более чем на одну треть (psk_max = psk_avg × 4/3). " +
		"Значение применяется в следующем календарном квартале после периода расчёта; дата точки — " +
		"конец квартала расчёта (сопоставлять по toStartOfQuarter(date)), а не дата публикации " +
		"выпуска. psk_max — Nullable: в PDF-эре 2014 года предельное значение ещё не публиковалось, " +
		"и NULL значит «в выпуске его не было», а не ноль. Набор категорий зависит от винтажа: в " +
		"PDF-эре 2014–2018 таксономия 1–4 (без ипотечных категорий), с 2019 добавлена 5. Ипотека " +
		"по ПСК: укрупнённый код 6 — с 2022-03-31 (16052022_ko.xls), детализированные 6.1.1/6.1.2 — " +
		"с 2024-06-30 (16082024_ko.xlsx), 8.1–8.3 — с 2026-03-31; в XLSX 16112023, 15022024, " +
		"16052024 ещё старая укрупнённая таксономия. Код категории НЕ стабилен между эрами " +
		"(2018: 2 = лимит кредитования; 2026: 2 = электронные средства платежа): идентичность ряда — " +
		"код вместе с наименованием, а расхождение исторических имён одного кода — норма " +
		"исторического ряда. Перед использованием читать v_series_catalog по (source='cbr', series " +
		"LIKE 'ПСК %'): там названия категорий и описание привязки даты",
	Columns: map[string]string{
		"date":          "Конец квартала расчёта (Date): последний день периода «за период с … по …» из заголовка выпуска. Значение применяется в следующем календарном квартале — дата применения в ключ не входит; сопоставлять по toStartOfQuarter(date)",
		"category_code": "Код категории потребительского кредита (займа) как в выпуске (1, 1.1, 2.1.1, 6.1.1 …). Код менял смысл между редакциями таксономии — читать вместе с category и описанием ряда в v_series_catalog",
		"category":      "Наименование категории как опубликовано в выпуске; у одного кода в разных эрах/винтажах историческое имя может отличаться, это норма исторического ряда, а не ошибка разбора",
		"psk_avg":       "Среднерыночное значение ПСК, % годовых (не фактическая ставка выданных кредитов). Это база законодательного капа: договорная ПСК не может превышать его более чем на одну треть",
		"psk_max":       "Предельное значение ПСК, % годовых: среднерыночное × 4/3 — кап по ч. 11 ст. 6 Федерального закона № 353-ФЗ. NULL — в выпуске предельное значение не публиковалось (PDF-эра 2014), а не ноль; у остальных выпусков присутствует у каждого листа с psk_avg",
		"source_file":   "Имя файла выпуска (например, 14082026_ko.xlsx) — провенанс точки. Пересмотр категории новым выпуском того же периода заменяет строку по ключу (category_code, date), поэтому здесь имя победившей редакции",
	},
}

// afterCbrPsk — производные импортёра cbr_psk: сначала каталог рядов, затем витрина v_cbr_psk
// (стандарт §10.2 «каталог → витрина»): агент, прочитавший витрину, обязан найти в каталоге
// описание каждой её категории. Публикуются собственным Import() импортёра после цикла выпусков
// и на no-op-пути: набор категорий зависит от данных, поэтому каталог нельзя объявить статическим
// списком кода, а пропущенный при прошлом сбое CreateView обязан вылечиться без нового выпуска. На
// пустой таблице каталог не пишется: пустой batch нечего утверждать, а следующий прогон с данными
// его опубликует; витрина при этом создаётся.
func afterCbrPsk(ctx context.Context, conn driver.Conn) error {
	meta, err := cbrPskCatalogMeta(ctx, conn)
	if err != nil {
		return err
	}
	if len(meta) == 0 {
		log.Warnf("%s: таблица пуста — каталог рядов не публикуется", cbrPskTable)
	} else if err = util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
		return err
	}
	_, err = util.CreateView(ctx, conn, cbrPskView)
	return err
}

// cbrBopView — витрина платёжного баланса РФ (source = имя таблицы, а не литерал 'cbr'): ряды
// выгрузок ЦБ по платёжному балансу имеют те же имена, что ряды общей витрины v_cbr_macro
// («Счет текущих операций»), и в ней они бы столкнулись, поэтому у платёжного баланса отдельная
// витрина — как у v_cbr_survey и v_cbr_mo_br. Ветки две, по выгрузкам: cbr_bop_est (оценка
// ключевых агрегатов, 51 ряд, млрд долларов США) и cbr_bop_standard (стандартная презентация,
// 34 ряда, млн долларов США) — единицы у них разные, и различает ряды двух выгрузок только
// source.
var cbrBopView = util.View{
	Name:   "v_cbr_bop",
	Tables: []string{"cbr_bop_est", "cbr_bop_standard"},
	Select: `SELECT 'cbr_bop_est' AS source, toString(name) AS series, date, toFloat64(value) AS value FROM cbr_bop_est FINAL
UNION ALL SELECT 'cbr_bop_standard', toString(name), date, toFloat64(value) FROM cbr_bop_standard FINAL`,
	Comment: "Платёжный баланс РФ (ЦБ РФ) двумя выгрузками: оценка ключевых агрегатов " +
		"(cbr_bop_est, 51 ряд: 17 агрегатов × {месяц, квартал, накопленным итогом с начала года}) " +
		"и стандартная презентация (cbr_bop_standard, 34 ряда: те же 17 верхних агрегатов × " +
		"{квартал с 1994 года, год 1994–2025}). Ряды: счёт текущих операций, экспорт и импорт " +
		"товаров, услуг и доходов, товары и услуги, торговый баланс, первичные и вторичные доходы, " +
		"счёт операций с капиталом, сальдо финансового счёта, резервные активы, чистые ошибки и " +
		"пропуски. Единицы различаются по source: у оценки — млрд долларов США, у стандартной " +
		"презентации — млн долларов США (в 1000 раз мельче). Дата точки: последний день отчётного " +
		"месяца, квартала или года; у накопленного итога — последний день последнего покрытого " +
		"месяца (сопоставлять по toStartOfMonth(date), toStartOfQuarter(date) или toYear(date)). " +
		"Лист оценки публикует только текущий год и тот же период прошлого года (накопленный итог, " +
		"два последних квартала и два последних месяца), поэтому её ряды «(месяц)» и «(квартал)» " +
		"набираются выпуск за выпуском: пропуск между точками значит «этот выпуск их не показывает», " +
		"а не «у источника нет данных». Длинную историю (с I квартала 1994 года, в млн долларов США) " +
		"даёт стандартная презентация. " +
		"Ноль — настоящее значение (сальдо нулевое), а не пропуск данных. Значения оценки ЦБ " +
		"ревизует следующим выпуском. Перед использованием читать v_series_catalog по " +
		"(source, series): там названы unit, знак ряда и привязка даты — по имени ряда две " +
		"выгрузки неразличимы",
	Columns: map[string]string{
		"source": "Импортёр-источник: cbr_bop_est — оценка ключевых агрегатов платёжного баланса ЦБ РФ, cbr_bop_standard — его стандартная презентация (квартальная и годовая)",
		"series": "Имя ряда: «<агрегат> (<вид>)»; вид — месяц, квартал или накопленным итогом с начала года у cbr_bop_est (51 ряд) и квартал или год у cbr_bop_standard (34 ряда). Имена рядов двух выгрузок совпадают, различает их source; описания — в v_series_catalog по (source, series)",
		"date":   "Дата точки (Date): последний день отчётного месяца, квартала или года; у накопленного итога — последний день последнего покрытого месяца. Сопоставлять по toStartOfMonth(date), toStartOfQuarter(date) или toYear(date)",
		"value":  "Значение (Float64). Единицы зависят от source: млрд долларов США у cbr_bop_est, млн долларов США у cbr_bop_standard (в 1000 раз мельче) — сверять unit в v_series_catalog. Положительное — превышение поступлений над выплатами или чистый прирост требований, отрицательное — обратное. Ноль — настоящее значение (сальдо нулевое), а не пропуск данных",
	},
}

// afterCbrBop — производные импортёров платёжного баланса: каталог рядов (source = имя таблицы)
// и витрина v_cbr_bop. Публикуются шаблоном после Send точек и до записи строки версии
// (стандарт §10.2).
func afterCbrBop(meta []util.SeriesMeta) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error {
		if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
		_, err := util.CreateView(ctx, conn, cbrBopView)
		return err
	}
}

// cbrDipView — витрина приложений к бюллетеню ЦБ «О чём говорят тренды» (source = имя таблицы,
// единственный источник): long-форма с винтажными колонками (прецедент расширенной витрины —
// v_keyrate_expectations). Агент читает точку как «значение, каким его опубликовал выпуск
// release на дату publication_date»: один отчётный месяц в разных выпусках — разные строки, а
// не перезапись, поэтому витрина и есть point-in-time слой — по ней видны ревизии (SA-ряды ЦБ
// пересчитывает между выпусками). Набор рядов узкий (8) и вёрстка приложений дрейфует между
// выпусками: отсутствие точки в ряду значит «этот выпуск её не показывает», а не «данных нет».
var cbrDipView = util.View{
	Name:   "v_cbr_dip_bulletin",
	Tables: []string{cbrDipTable},
	Select: "SELECT 'cbr_dip_bulletin' AS source, series, date, value, release, release_id, publication_date FROM cbr_dip_bulletin FINAL",
	Comment: "Приложения к бюллетеню Банка России «О чём говорят тренды» (расчёты ДИП): 8 рядов " +
		"— сезонно сглаженный рост цен (SAAR, % м/м в годовом выражении: все товары и услуги, " +
		"продовольственные, непродовольственные, услуги) и рынок труда (уровень безработицы и " +
		"занятости, каждый парой SA/NSA). Витрина винтажная: у точки стоят номер выпуска (release, " +
		"«26-07») и дата его публикации (publication_date), и один и тот же отчётный месяц разных " +
		"выпусков — разные строки (ключ (series, date, release_id)), а не перезапись: читать как " +
		"«значение, каким его опубликовал выпуск … на дату …». SA-ряды ЦБ пересчитывает между " +
		"выпусками (ревизии видны сравнением строк одного месяца), а NSA-колонка в части выпусков " +
		"идёт без SA-пары. Дата точки — последний день отчётного месяца (в книге источника — " +
		"первое число): сопоставлять по toStartOfMonth(date). Набор рядов и листы дрейфуют между " +
		"выпусками (листа рынка труда нет в 13 выпусках из 49; в 26-07 безработицу заменил уровень " +
		"занятости), поэтому отсутствие точки значит «выпуск её не показывает». Единица — %; перед " +
		"использованием читать v_series_catalog по (source='cbr_dip_bulletin', series)",
	Columns: map[string]string{
		"source": "Импортёр-источник: всегда cbr_dip_bulletin",
		"series": "Имя ряда из 8 объявленных: saar_all_goods, saar_food, saar_nonfood, saar_services (SAAR-инфляция, % м/м в годовом выражении), unemployment_sa/unemployment_nsa, employment_sa/employment_nsa (рынок труда, %). Описания и единицы — в v_series_catalog (source='cbr_dip_bulletin')",
		"date":   "Отчётный месяц: последний день месяца (в книге источника — первое число; 2015-01-01 → 2015-01-31). Сопоставлять по toStartOfMonth(date)",
		"value": "Значение ряда (Float64), %: у saar_* — сезонно скорректированный рост цен месяц к месяцу в годовом выражении (SAAR), у рядов рынка труда — уровень безработицы/занятости. " +
			"Значение — то, каким его опубликовал выпуск release на дату publication_date; пустая ячейка источника точкой не становится (месяца в ряду нет), литеральный ноль — точка. Единицы и смысл — в v_series_catalog",
		"release":          "Номер выпуска источника («26-07»: две цифры года группы + номер из подписи «№ 7 / Октябрь»). Выпуск — точка во времени: одна и та же метрика в разных выпусках различается",
		"release_id":       "id выпуска из адреса файла приложения (/Collection/Collection/File/<id>/): ключ винтажа и часть ключа строки. Имя файла ключом не служит — stat_bulletin_23-07.xlsx встречается у двух выпусков; замена файла под тем же id гейтом не отслеживается (лечение — удалить строки выпуска и повторить прогон)",
		"publication_date": "Дата публикации выпуска на сайте ЦБ (атрибут ссылки приложения «Опубликовано …»). Атрибут выпуска, не часть ключа: дату публикации ЦБ может поправить, id — нет",
	},
}

// cbrDipQuarterlyView — квартальное среднее месячных SAAR ДИП по каждому выпуску (спека
// 2026-11-08-cpi-quarterly-metrics §4.1): та же винтажность, что у v_cbr_dip_bulletin, но точка —
// квартал, а не месяц. Это число цитирует квартальный блок постзаседания (KEY_RATE_PUBLICATIONS
// §4.7): ЦБ усредняет месячные SAAR за квартал, поэтому avg(value) по saar_all_goods и есть
// «текущий рост цен 8,0 % за 3к26» (наш расчёт — 8,02). FINAL обязателен: таблица
// ReplacingMergeTree с ключом (series, date, release_id), без него count() и avg() посчитали бы
// несмёрженные части. months < 3 — неполный квартал: среднее по одному-двум месяцам с полным
// кварталом не сравнивается (26-06 даёт 3к26 = 11,59 по одному июлю против 8,02 по трём месяцам
// в 26-07), поэтому фильтр months = 3 — на стороне читателя. quarter — метка «3к26» рядом с
// quarter_start: у квартальных рядов каталога принята привязка «1-е число квартала, следующего
// за отчётным», а здесь ключ — первый день отчётного квартала, и метка снимает риск прочитать
// 2026-07-01 как II квартал.
var cbrDipQuarterlyView = util.View{
	Name:   "v_cbr_dip_quarterly",
	Tables: []string{cbrDipTable},
	Select: `SELECT
    release,
    release_id,
    publication_date,
    toStartOfQuarter(date) AS quarter_start,
    concat(toString(toQuarter(quarter_start)), 'к', right(toString(toYear(quarter_start)), 2)) AS quarter,
    count() AS months,
    avg(value) AS saar_q
FROM cbr_dip_bulletin FINAL
WHERE series = 'saar_all_goods'
GROUP BY release, release_id, publication_date, quarter_start`,
	Comment: "Квартальные оценки текущего роста цен ЦБ (расчёты ДИП): среднее месячных SAAR " +
		"«Все товары и услуги» за квартал по каждому выпуску приложения к бюллетеню «О чём говорят " +
		"тренды». Это то число, которое ЦБ называет в квартальном блоке пресс-релиза: 3к26 — 8,0 % " +
		"(наш расчёт 8,02), 2к26 — 4,8 % (4,89), 1к26 — 8,7 % (8,70). Витрина винтажная, как и " +
		"v_cbr_dip_bulletin: у строки стоят номер выпуска (release, «26-07») и дата его публикации, и " +
		"один и тот же квартал в разных выпусках — разные строки, а не перезапись (2к26: 5,02 в " +
		"июльском выпуске, 5,36 в сентябрьском, 4,89 в октябрьском) — это ревизия SA, а не ошибка, и " +
		"сравнивать кварталы между выпусками нужно на дату публикации. months — сколько месяцев " +
		"квартала выпуск показал: months = 3 — полный квартал, months < 3 — неполный (26-06: 3к26 = " +
		"11,59 по одному июлю), и в сравнение с полным кварталом он не идёт. quarter_start — первый " +
		"день отчётного квартала (2026-07-01 = III кв. 2026) ВОПРЕКИ правилу каталога «дата " +
		"квартального ряда — 1-е число следующего квартала»; читаемая метка квартала — quarter " +
		"(«3к26»). Перед использованием читать v_series_catalog по (source='cbr_dip_bulletin', " +
		"series='saar_q')",
	Columns: map[string]string{
		"release":          "Номер выпуска источника («26-07»). Выпуск — точка во времени: одна и та же метрика в разных выпусках различается",
		"release_id":       "id выпуска из адреса файла приложения (/Collection/Collection/File/<id>/): ключ винтажа и часть ключа сырой таблицы",
		"publication_date": "Дата публикации выпуска на сайте ЦБ. Атрибут выпуска, не часть ключа",
		"quarter_start":    "Первый день отчётного квартала (Date): 2026-07-01 = III кв. 2026. Сопоставлять по quarter_start; это НЕ каноническая привязка каталога «1-е число следующего квартала»",
		"quarter":          "Метка квартала как в публикации («3к26»): читаемая подпись к quarter_start, в тексте статьи и таблицах используется она",
		"months":           "Число месяцев квартала в этом выпуске: 3 — полный квартал; меньше 3 — неполный (среднее по одному-двум месяцам, с полным кварталом не сравнивать)",
		"saar_q":           "Квартальный SAAR, % годовых: среднее месячных saar_all_goods за квартал. Значение — редакция выпуска release на дату publication_date, а не последняя версия ряда",
	},
}

// cbrCoreQuarterlyView — квартальное среднее годовых темпов базовой инфляции из SA м/м (спека
// 2026-11-08 §4.2): вторая половина квартального блока постзаседания. value — уже сезонно
// очищенный месячный темп, %, поэтому повторная сезонная корректировка не применяется, а годовой
// пересчёт — (pow(1 + value/100, 12) − 1) × 100 по каждому месяцу и затем среднее: усреднять
// годовые темпы, а не пересчитывать годовой темп из среднего месячного, — то же правило, что в
// §4.7. pow, а не ^: в ClickHouse ^ — побитовый XOR, а не степень. months < 3 — неполный квартал
// (октябрьская базовая появляется 14.11, и до неё строки 4к26 нет вовсе: «н/д», не ноль).
// Винтажей у файла нет (cbr_indicators_cpd — текущая редакция indicators_cpd.xlsx), поэтому
// строка одна на квартал и старые релизы с ней могут расходиться (2к26 в июльском релизе —
// 4,2 % против 4,57 % текущей редакции): это ревизия файла, а не ошибка формулы.
var cbrCoreQuarterlyView = util.View{
	Name:   "v_cbr_core_quarterly",
	Tables: []string{cbrIndicatorsCpdTable},
	Select: `SELECT
    toStartOfQuarter(date) AS quarter_start,
    concat(toString(toQuarter(quarter_start)), 'к', right(toString(toYear(quarter_start)), 2)) AS quarter,
    count() AS months,
    avg((pow(1 + value / 100, 12) - 1) * 100) AS saar_q,
    max(date) AS last_month
FROM cbr_indicators_cpd FINAL
WHERE name = 'Базовый ИПЦ'
GROUP BY quarter_start`,
	Comment: "Квартальный SAAR базовой инфляции ЦБ: среднее годовых темпов из сезонно " +
		"скорректированных месячных темпов «Базового ИПЦ» (indicators_cpd.xlsx, лист «Лист1»). " +
		"Годовой пересчёт — (pow(1 + SA м/м/100, 12) − 1) × 100 по каждому месяцу, затем среднее за " +
		"квартал: 3к26 — 6,21 % (ЦБ называет 6,2), 2к26 — 4,57 % (4,5), 1к26 — 6,06 %. months = 3 — " +
		"полный квартал; меньше — неполный, и в сравнение с полным кварталом он не идёт (октябрьская " +
		"базовая появляется 14.11, до неё строки 4к26 нет вовсе: «н/д», не ноль). Винтажей нет: " +
		"cbr_indicators_cpd — текущая редакция файла ЦБ, поэтому цитируемый в старом релизе квартал " +
		"может расходиться с витриной сильнее, чем на 0,2 п.п. (2к26 в июльском релизе — 4,2 % " +
		"против 4,57 %): это ревизия файла, а не ошибка формулы. quarter_start — первый день " +
		"отчётного квартала (2026-07-01 = III кв. 2026), читаемая метка — quarter («3к26»); это не " +
		"каноническая привязка каталога «1-е число следующего квартала». Перед использованием читать " +
		"v_series_catalog по (source='cbr', series='Базовый ИПЦ:SAAR_q')",
	Columns: map[string]string{
		"quarter_start": "Первый день отчётного квартала (Date): 2026-07-01 = III кв. 2026. Сопоставлять по quarter_start; это НЕ привязка «1-е число следующего квартала»",
		"quarter":       "Метка квартала как в публикации («3к26»): читаемая подпись к quarter_start",
		"months":        "Число месяцев квартала в текущей редакции файла: 3 — полный квартал; меньше 3 — неполный",
		"saar_q":        "Квартальный SAAR базовой инфляции, % годовых: среднее месячных (pow(1 + value/100, 12) − 1) × 100. Винтажей нет — это текущая редакция файла ЦБ",
		"last_month":    "Последний месяц квартала в файле: показывает, по какое число включительно посчитано среднее (для закрытых кварталов — последний день квартала)",
	},
}

// afterCbrDip — производные импортёра cbr_dip_bulletin: каталог восьми месячных рядов
// (cbr/dip_meta.go), витрина v_cbr_dip_bulletin с винтажными колонками, каталог квартального
// ряда saar_q и витрина v_cbr_dip_quarterly. Публикуются шаблоном по каждому импортированному
// выпуску (после точек и до строки версии, стандарт §10.2): на первом прогоне это 49 повторов,
// дальше — 0–1 на прогон; идемпотентно и стоит четырёх запросов к своим таблицам (отступление 5
// спеки §5). Каталог квартального ряда — отдельный срез, а не девятая запись cbrDipSeriesMeta:
// у тех рядов unit «%» и frequency «M», и квартальный ряд в общем списке сломал бы контракт
// «все ряды источника месячные».
func afterCbrDip(meta []util.SeriesMeta) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error {
		if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
		if err := util.UpsertSeriesCatalog(ctx, conn, cbrDipQuarterlySeriesMeta); err != nil {
			return err
		}
		if _, err := util.CreateView(ctx, conn, cbrDipView); err != nil {
			return err
		}
		_, err := util.CreateView(ctx, conn, cbrDipQuarterlyView)
		return err
	}
}

// afterCbrIndicatorsCpd — производные импортёра cbr_indicators_cpd: общая витрина v_cbr_macro с
// каталогом двух месячных рядов (после CbrMacro), затем каталог квартального ряда
// «Базовый ИПЦ:SAAR_q» и витрина v_cbr_core_quarterly. Без обёртки квартальная витрина
// осталась бы без владельца: в v_cbr_macro живут только месячные темпы, и пересчёт в годовые —
// отдельный контракт с собственной привязкой даты (quarter_start), которого в общей витрине нет.
func afterCbrIndicatorsCpd(ctx context.Context, conn driver.Conn) error {
	if err := publishSeries(ctx, conn, cbrIndicatorsCpdSeriesMeta); err != nil {
		return err
	}
	if err := util.UpsertSeriesCatalog(ctx, conn, cbrCoreQuarterlySeriesMeta); err != nil {
		return err
	}
	_, err := util.CreateView(ctx, conn, cbrCoreQuarterlyView)
	return err
}

// cbrMtForecastView — витрина диапазонов среднесрочного прогноза Банка России (source = имя
// таблицы, единственный источник): dedicated-колонки lo/hi, а не long-форма (source, series,
// date, value). Отступление от буквы §10.3 названо в спеке §5.2: диапазон не сводится к паре
// «дата → значение» без распада на две строки, теряющего смысл «это один диапазон», а сужение до
// одного value потеряло бы половину прогноза; прецеденты расширенных витрин —
// v_keyrate_expectations, v_cbr_dip_bulletin. Витрина винтажная: (indicator, year) в разных
// выпусках — разные строки, а не перезапись, и по ней видны ревизии прогноза ЦБ между выпусками.
var cbrMtForecastView = util.View{
	Name:   "v_cbr_mt_forecast",
	Tables: []string{mtForecastTable},
	Select: "SELECT 'cbr_mt_forecast' AS source, meeting_date, release_date, url, indicator, year, lo, hi FROM cbr_mt_forecast FINAL",
	Comment: "Диапазоны среднесрочного прогноза Банка России (базовый сценарий): 16 показателей " +
		"листа «Табл 1» «Комментария к среднесрочному прогнозу» — в том числе среднегодовая " +
		"ключевая ставка и инфляция (декабрь к декабрю и в среднем за год), — какими их " +
		"зафиксировал каждый выпуск. Импортируются оба корпуса: комментарии (страница с xlsx у " +
		"двух новейших выпусков, PDF у остальных) и PDF «Среднесрочный прогноз Банка России» с " +
		"той же «Табл. 1» — по одному на заседание. Прогнозный PDF публикуется в день заседания " +
		"и даёт самую раннюю точку PIT; значения прогноза и комментария одного заседания " +
		"совпадают (различается только типографика), но лежат отдельными строками с разными " +
		"release_date и url — это разные выпуски, а не дубль. Выпуск release_date — точка во " +
		"времени: один и тот же показатель и прогнозный год в разных выпусках — разные строки " +
		"(ключ (indicator, year, release_date)), а не перезапись, поэтому сравнивать диапазоны " +
		"разных выпусков нужно по паре (indicator, year) с указанием release_date, а сдвиг " +
		"прогноза судить разностью границ между выпусками (прецедент — винтажные колонки " +
		"v_cbr_dip_bulletin). lo = hi — точечный прогноз (источник напечатал одно число, а не " +
		"диапазон); у первого прогнозного года диапазон уже, чем у последующих (сноска 1 листа). " +
		"Перед использованием читать v_series_catalog по (source='cbr_mt_forecast', series): там " +
		"описания показателей и код согласования с рядами опроса аналитиков (cbr_mo_br)",
	Columns: map[string]string{
		"source":       "Импортёр-источник: всегда cbr_mt_forecast",
		"meeting_date": "Дата опорного заседания Совета директоров Банка России, к которому относится выпуск (атрибут выпуска, не часть ключа). PDF «Среднесрочный прогноз» публикуется в день заседания, комментарий — через 11–17 дней; выпуски — 4 раза в год (февраль, апрель, июль, октябрь)",
		"release_date": "Дата публикации артефакта выпуска — комментария (страница или PDF) либо «Среднесрочного прогноза» (у прогноза совпадает с meeting_date) — точка во времени, ключ винтажа: значения разных выпусков не затирают друг друга, и прогноз ЦБ на один и тот же год, прочитанный из разных строк, — это разные редакции прогноза. Подмена файла под тем же release_date гейтом прогона не отслеживается (лечение — удалить строки выпуска и повторить прогон)",
		"url":          "Страница выпуска-комментария или PDF-файл (комментарий либо «Среднесрочный прогноз»), с которого взяты данные (провенанс; адрес меняется от выпуска к выпуску). Скачанный артефакт покрывается винтажным архивом версий (source_publications агенту не выдаётся)",
		"indicator":    "Код показателя: cpi_dec (ИПЦ, декабрь к декабрю), cpi_avg (ИПЦ в среднем за год), key_rate_avg (ключевая ставка в среднем за год), gdp_yoy, gdp_q4_yoy, consumption_total, consumption_households, gross_capital_formation, fixed_capital_investment, exports, imports, m2, bank_claims_total, bank_claims_organizations, bank_claims_households, bank_claims_mortgage. Шесть кодов (cpi_dec, cpi_avg, key_rate_avg, gdp_yoy, exports, imports) согласованы с именами рядов макроэкономического опроса ЦБ (source='cbr_mo_br') — прогноз ЦБ сопоставляется с консенсусом аналитиков по общим именам, единицы при этом сверять по unit. Описания — в v_series_catalog",
		"year":         "Прогнозный год (UInt16), к которому относится диапазон. Горизонт меняется от выпуска к выпуску (3–4 года), состав годов берётся из шапки книги",
		"lo":           "Нижняя граница диапазона прогноза (Float64). У точечного прогноза lo = hi (источник напечатал одно число). Единицы — по indicator (см. unit в v_series_catalog: % дек. к дек., % к пред. году, % годовых, % прироста). Значение — то, каким его зафиксировал выпуск release_date: это прогноз ЦБ, а не факт, и следующий выпуск может пересмотреть границы",
		"hi":           "Верхняя граница диапазона прогноза (Float64). Смысл границ: ЦБ публикует прогноз диапазоном, а не точкой; сужение к среднему не предусмотрено источником",
	},
}

// afterCbrMtForecast — производные импортёра cbr_mt_forecast: каталог 16 рядов
// (cbr/mt_forecast_meta.go) и витрина v_cbr_mt_forecast. Публикуются собственным Import()
// импортёра до гейта — на каждом прогоне, включая no-op (самолечение устаревшей витрины и
// каталога между выпусками: выпуск выходит 4 раза в год, а сбой CreateView мог случиться в любом
// из прошлых прогонов). Отступление от буквы §10.2 названо в спеке §5.3; прецедент —
// rosstat_ipc_weeks_bulletins. Идемпотентно и стоит двух запросов к своим таблицам.
func afterCbrMtForecast(meta []util.SeriesMeta) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error {
		if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
		_, err := util.CreateView(ctx, conn, cbrMtForecastView)
		return err
	}
}

// cbrSurveyView — витрина рядов мониторинга предприятий (source='cbr_survey', импортёр
// cbr_mp_survey); общая витрина cbrMacroView её не покрывает.
var cbrSurveyView = util.View{
	Name:    "v_cbr_survey",
	Tables:  []string{"macro_series"},
	Select:  `SELECT source, series, date, value FROM macro_series FINAL WHERE source = 'cbr_survey'`,
	Comment: "Мониторинг предприятий Банка России (опросы нефинансовых компаний): 13 агрегатных рядов «Экономики всего» и трёх квартальных листов (ценовые ожидания бизнеса, ИБК и условия кредитования; загрузка мощностей, обеспеченность персоналом, ожидания численности) и 368 отраслевых рядов 16 листов (industry_*: ИБК и его факт/ожидания, выпуск, спрос, цены, ценовые ожидания, издержки, условия кредитования, ожидаемый прирост отпускных цен; у всех, кроме условий кредитования, есть сезонно сглаженная пара _sa). Дата — отчётный период: 1-е число месяца, у квартальных — 1-е число месяца, следующего за кварталом; ЦБ публикует волну в первую декаду следующего месяца и нумерует справки по месяцу волны (справка «сентябрь 2026» цитирует точку отчётного периода август 2026). Перед использованием читать v_series_catalog по (source='cbr_survey', series): единицы и знаки различаются, а отраслевые ряды названы industry_<sheet>_<indicator>. ЦБ в комментариях цитирует сезонно сглаженные ряды (…_sa)",
	Columns: map[string]string{
		"source": "Всегда cbr_survey",
		"series": "Код ряда: 13 агрегатных и 368 отраслевых (industry_<sheet>_<indicator>, у 11 из 12 показателей есть _sa) — описания в v_series_catalog (source='cbr_survey')",
		"date":   "Отчётный период: 1-е число месяца; у квартальных рядов — 1-е число месяца, следующего за отчётным кварталом (конвенция rosstat_vvp_kvartal)",
		"value":  "Значение в единицах ряда: пункты баланса ответов, % (загрузка мощностей), % годовых (ожидаемый прирост цен) — см. unit и description в v_series_catalog",
	},
}

// cbrZcycCompareView — сравнение КБД ЦБ (cbr_zcyc, 12 теноров с 04.01.2003) и
// G-curve МосБиржи (ofz_curve, 1y/3y/5y/10y и только с 2026-10-09) по (date, tenor).
// Dedicated-форма, а не long-форма §10.3: это сравнение двух источников одной
// строкой (прецеденты — v_cbr_dip_bulletin, v_cbr_mt_forecast).
var cbrZcycCompareView = util.View{
	Name:   "v_ofz_curve_compare",
	Tables: []string{cbrZcycTable, "ofz_curve"},
	Select: `SELECT date, tenor,
        maxIf(toNullable(yield), source = 'cbr')  AS cbr_yield,
        maxIf(toNullable(yield), source = 'moex') AS moex_yield,
        round(maxIf(toNullable(yield), source = 'moex') - maxIf(toNullable(yield), source = 'cbr'), 2) AS spread_pp
    FROM (
        SELECT 'cbr' AS source, date, tenor, yield FROM cbr_zcyc FINAL
        UNION ALL
        SELECT 'moex', date, replaceRegexpOne(tenor, 'y$', ''), yield
        FROM ofz_curve FINAL WHERE tenor IN ('1y','3y','5y','10y')
    )
    GROUP BY date, tenor`,
	Comment: "Сравнение кривой бескупонной доходности ЦБ РФ (cbr_zcyc: 12 теноров, история " +
		"с 04.01.2003) и G-curve МосБиржи (ofz_curve: теноры 1y/3y/5y/10y — только они и только " +
		"с 2026-10-09). Одна строка на (date, tenor): cbr_yield и moex_yield — уровни двух " +
		"источников, % годовых; spread_pp = moex_yield − cbr_yield, п.п. (положительный — G-curve " +
		"выше КБД ЦБ). spread_pp округлён до 0.01 п.п.; у близкого к нулю отрицательного спреда " +
		"возможна запись -0.0 — это ноль, а не дефект. tenor — каноническая запись КБД ЦБ: 0.25, " +
		"0.5, 0.75, 1, 2, 3, 5, 7, 10, 15, 20, 30 (лет); теноры МосБиржи 1y/3y/5y/10y приведены " +
		"к 1/3/5/10 срезом суффикса y, остальные теноры есть только у ЦБ. NULL в cbr_yield, " +
		"moex_yield или spread_pp означает отсутствие точки у источника (пропуск публикации, " +
		"выходной, ранняя дата у МосБиржи), а не ноль; строки появляются по датам, где есть хотя " +
		"бы один источник. Перед вычислением спредов читать v_series_catalog по (source, series): " +
		"единицы обоих источников — % годовых",
	Columns: map[string]string{
		"date":       "Дата наблюдения (Date): торговый день, за который опубликована КБД ЦБ и/или котировка G-curve МосБиржи",
		"tenor":      "Тенор в канонической записи КБД ЦБ: 0.25, 0.5, 0.75, 1, 2, 3, 5, 7, 10, 15, 20, 30 (лет). Теноры МосБиржи 1y/3y/5y/10y нормализованы в 1/3/5/10; остальные значения есть только у ЦБ",
		"cbr_yield":  "Доходность по КБД ЦБ, % годовых (cbr_zcyc, тенор из колонки tenor). NULL — ЦБ не публиковал точку в этот день, а не ноль",
		"moex_yield": "Доходность по G-curve МосБиржи, % годовых (ofz_curve; только теноры 1/3/5/10 и только с 2026-10-09). NULL — точки нет, а не ноль",
		"spread_pp":  "Спред moex_yield − cbr_yield, процентных пунктов: положительный — G-curve МосБиржи выше КБД ЦБ, отрицательный — ниже. Округлён до 0.01 п.п.; -0.0 у близкого к нулю отрицательного спреда — это ноль, а не ошибка. NULL, если отсутствует хотя бы одна из точек",
	},
}

// cbrZcycSignalView — сигналы долгового рынка по КБД ЦБ: одна строка на дату
// публикации кривой. Dedicated-форма, а не long-форма §10.3: производные колонки
// одной строкой (прецеденты — v_keyrate_expectations, v_cbr_mt_forecast).
//
// Правая сторона обоих ASOF LEFT JOIN обёрнута в toNullable: при настройке
// ClickHouse join_use_nulls=0 (значение по умолчанию) промах join подставляет в
// не-Nullable колонку значение по умолчанию — 0 для ставки и ИПЦ, — и обещание
// NULL в комментарии витрины было бы ложью: до 2013-09-17 key_rate стал бы нулём,
// а спред — самой доходностью ОФЗ. Совпавшая строка остаётся числом, промах —
// NULL. count() = 12 в окне ИПЦ — та же защита от частичного г/г на первых 11
// строках ряда.
var cbrZcycSignalView = util.View{
	Name:   "v_ofz_signal",
	Tables: []string{cbrZcycTable, "cbr_key_rate", "rosstat_ipc_mes"},
	Select: `SELECT c.date AS date,
        c.ofz_1y, c.ofz_2y, c.ofz_10y,
        k.rate AS key_rate,
        round(c.ofz_10y - k.rate, 2) AS ofz_10y_minus_key_rate_pp,
        round(c.ofz_2y  - k.rate, 2) AS ofz_2y_minus_key_rate_pp,
        round(c.ofz_10y - c.ofz_1y, 2) AS slope_10y_1y_pp,
        round(i.cpi_yoy, 2) AS inflation_yoy,
        round(c.ofz_10y - i.cpi_yoy, 2) AS real_yield_10y_pp
    FROM (
        SELECT date,
               maxIf(toNullable(yield), tenor = '1')  AS ofz_1y,
               maxIf(toNullable(yield), tenor = '2')  AS ofz_2y,
               maxIf(toNullable(yield), tenor = '10') AS ofz_10y,
               1 AS _one
        FROM cbr_zcyc FINAL GROUP BY date
    ) AS c
    ASOF LEFT JOIN (
        SELECT date, toNullable(rate) AS rate, 1 AS _one FROM cbr_key_rate FINAL ORDER BY date
    ) AS k
      ON c._one = k._one AND c.date >= k.date
    ASOF LEFT JOIN (
        SELECT date, toNullable(cpi_yoy) AS cpi_yoy, 1 AS _one FROM (
            SELECT date,
                   if(count() OVER (ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) = 12,
                      (exp(sum(log(toFloat64(percent) / 100)) OVER (
                          ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)) - 1) * 100,
                      NULL) AS cpi_yoy
            FROM rosstat_ipc_mes FINAL
            WHERE name = 'Индексы потребительских цен на товары и услуги по Российской Федерации'
            ORDER BY date
        ) ORDER BY date
    ) AS i ON c._one = i._one AND c.date >= i.date`,
	Comment: "Сигналы долгового рынка по КБД ЦБ: одна строка на дату публикации кривой " +
		"(cbr_zcyc, торговый день). ofz_1y/ofz_2y/ofz_10y — доходность ОФЗ соответствующего " +
		"тенора, % годовых. key_rate — последняя действующая ключевая ставка на дату " +
		"(ступенчатая функция, ASOF JOIN с cbr_key_rate); до 2013-09-17 истории ставки нет — " +
		"NULL, а не ноль. ofz_10y_minus_key_rate_pp и ofz_2y_minus_key_rate_pp — спреды " +
		"ofz − key_rate, п.п.: положительное значение значит «ОФЗ выше ключевой» (рынок требует " +
		"премию к ставке), отрицательное — «ОФЗ ниже ключевой». slope_10y_1y_pp — наклон кривой " +
		"10y − 1y, п.п. (отрицательный — инверсия). inflation_yoy — ИПЦ Росстата г/г, %: " +
		"произведение 12 месячных индексов rosstat_ipc_mes к отчётному месяцу (первые 11 строк " +
		"ряда ИПЦ — NULL, незаконченный год не считается; окно берёт 12 строк, а не 12 календарных " +
		"месяцев, поэтому пропуск месяца в источнике оконная функция не ловит); дата ИПЦ — конец " +
		"отчётного месяца, а лаг публикации ~2 недели не моделируется (для point-in-time " +
		"расчётов дату ИПЦ сдвигать отдельно). real_yield_10y_pp = ofz_10y − inflation_yoy, п.п. " +
		"(ex post, без инфляционных ожиданий). Спреды, наклон и реальная доходность округлены " +
		"до 0.01 п.п.; NULL в любой колонке означает отсутствие точки, ставки или ИПЦ на дату, " +
		"а не ноль. Перед использованием читать v_series_catalog по (source, series)",
	Columns: map[string]string{
		"date":                      "Дата КБД ЦБ (торговый день): одна строка на дату публикации кривой",
		"ofz_1y":                    "Доходность ОФЗ с погашением через 1 год по КБД ЦБ, % годовых (тенор 1). NULL — точки нет, а не ноль",
		"ofz_2y":                    "Доходность ОФЗ с погашением через 2 года по КБД ЦБ, % годовых (тенор 2). NULL — точки нет, а не ноль",
		"ofz_10y":                   "Доходность ОФЗ с погашением через 10 лет по КБД ЦБ, % годовых (тенор 10). NULL — точки нет, а не ноль",
		"key_rate":                  "Ключевая ставка ЦБ, % годовых, действующая на дату: последнее решение не позже date (ASOF). NULL до 2013-09-17 — истории ставки раньше нет, это не ноль",
		"ofz_10y_minus_key_rate_pp": "Спред ofz_10y − key_rate, п.п.: положительный — «ОФЗ выше ключевой», отрицательный — ниже. NULL, если нет ставки или точки",
		"ofz_2y_minus_key_rate_pp":  "Спред ofz_2y − key_rate, п.п.: короткий конец кривой против ключевой ставки. NULL, если нет ставки или точки",
		"slope_10y_1y_pp":           "Наклон кривой 10y − 1y, п.п.: положительный — нормальная форма, отрицательный — инверсия. NULL, если нет одной из точек",
		"inflation_yoy":             "ИПЦ Росстата г/г, %: произведение 12 месячных индексов rosstat_ipc_mes к отчётному месяцу; первые 11 строк ряда ИПЦ — NULL (незаконченный год не считается), но окно берёт 12 строк, а не 12 календарных месяцев — пропуск месяца в источнике оконная функция не ловит. Дата — конец отчётного месяца; лаг публикации ~2 недели не моделируется, для point-in-time расчётов сдвигать отдельно. NULL — нет точки ИПЦ, а не ноль",
		"real_yield_10y_pp":         "Реальная доходность ofz_10y − inflation_yoy, п.п. (ex post, без инфляционных ожиданий). NULL, если нет точки или ИПЦ",
	},
}

// afterCbrZcyc — производные импортёра cbr_zcyc: каталог 12 рядов (cbr/zcyc_meta.go),
// витрина сравнения КБД с G-curve МосБиржи и витрина сигналов. Публикуются шаблоном
// после точек и до строки версии (стандарт §10.2). CreateView пропускает витрину,
// пока нет хотя бы одной исходной таблицы (ofz_curve, cbr_key_rate,
// rosstat_ipc_mes) — это не ошибка: следующий прогон самолечит витрину.
func afterCbrZcyc(meta []util.SeriesMeta) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error {
		if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
		if _, err := util.CreateView(ctx, conn, cbrZcycCompareView); err != nil {
			return err
		}
		_, err := util.CreateView(ctx, conn, cbrZcycSignalView)
		return err
	}
}

// cbrCcybView — витрина индикаторов кредитного цикла ЦБ (source = имя таблицы).
// Long-форма: source, series, date, value; indicator_no и indicator_group —
// атрибуты ряда, чтобы агент мог читать показатель без расшифровки имени.
var cbrCcybView = util.View{
	Name:   "v_cbr_ccyb_indicators",
	Tables: []string{cbrCcybTable},
	Select: "SELECT 'cbr_ccyb_indicators' AS source, series, indicator_no, indicator_group, date, value " +
		"FROM cbr_ccyb_indicators FINAL",
	Comment: "АЦН-индикаторы Банка России — 24 месячных ряда кредитного цикла (13 показателей, " +
		"у части есть подстроки): достаточность капитала Н1.0/Н1.1/Н1.2, кредиты/ВВП, темпы " +
		"прироста кредитов нефинансовым организациям и физическим лицам (руб./валюта, год/квартал/" +
		"месяц), доля валютных кредитов, просроченная задолженность, IV–V категории качества, " +
		"ссуды с просрочкой свыше 90 дней и кредитные гэпы. Значения в процентах, у кредитных " +
		"гэпов — в п.п.; это уровни/темпы за период, а не индексы. Дата — первое число отчётного " +
		"месяца. Файл источника последний раз обновлялся в декабре 2024 года, последняя точка — " +
		"2024-10-01: отсутствие более поздних точек — свойство публикации, а не пропуск импорта. " +
		"Пустая ячейка источника точкой не является и нулём не считается. Имена рядов объявлены " +
		"кодом (стандарт §6.8); описания, единицы и группы — в v_series_catalog по " +
		"(source='cbr_ccyb_indicators', series)",
	Columns: map[string]string{
		"source":          "Импортёр-источник: всегда cbr_ccyb_indicators",
		"series":          "Код ряда: n10_capital, n11_capital, n12_capital, credits_gdp, nfo_rub_yoy, nfo_rub_qoq, nfo_rub_mom, nfo_fx_yoy, nfo_fx_qoq, nfo_fx_mom, nfo_fx_share, nfo_overdue_rub, nfo_overdue_fx, nfo_iv_v_share, hh_yoy, hh_qoq, hh_mom, hh_overdue, hh_90d_mortgage, hh_90d_unsecured, credit_gap_broad, credit_gap_narrow, credit_gap_narrow_nfo, credit_gap_hh",
		"indicator_no":    "Номер показателя источника (1…13): у подстрок — номер родительского показателя",
		"indicator_group": "Группа источника: bank_capital, credit_gdp, nfo_loan_growth, nfo_fx_share, nfo_overdue, nfo_iv_v_share, hh_loan_growth, hh_overdue, hh_90d, credit_gaps",
		"date":            "Отчётная дата: первое число месяца (Date). Сопоставлять по toStartOfMonth(date); пропуск месяца означает, что источник не опубликовал точку, а не ноль",
		"value":           "Значение ряда (Float64): проценты, у credit_gap_* — процентные пункты. Уровень/темп за период, а не индекс; пустая ячейка источника точкой не становится",
	},
}

// afterCbrCcyb — производные импортёра cbr_ccyb_indicators: каталог 24 рядов
// и витрина v_cbr_ccyb_indicators. Публикуются после успешного Import и на
// no-op-пути (импортёр переопределяет Import): витрина — производная таблицы,
// и пропущенный при прошлом сбое CreateView обязан вылечиться без новой версии
// файла (прецедент — cbr_mt_forecast, §8 п. 13).
func afterCbrCcyb(meta []util.SeriesMeta) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error {
		if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
		_, err := util.CreateView(ctx, conn, cbrCcybView)
		return err
	}
}

// cbrRiskWeightAddonsView — витрина матриц надбавок к коэффициентам риска.
// Ключ витрины — (matrix, valid_from, valid_to, row_code, col_code); версии
// разных архивных файлов схлопываются argMax(version), поэтому одна ячейка
// одного периода не дублируется. table_no — атрибут победившей версии.
var cbrRiskWeightAddonsView = util.View{
	Name:   "v_cbr_risk_weight_addons",
	Tables: []string{cbrRiskWeightAddonsTable},
	Select: `SELECT 'cbr_risk_weight_addons' AS source, matrix, matrix AS series,
       valid_from,
       argMax(valid_to, version) AS valid_to,
       argMax(table_no, version) AS table_no,
       row_code, col_code,
       argMax(value, version) AS value,
       argMax(is_applicable, version) AS is_applicable,
       argMax(source_file, version) AS source_file,
       argMax(source_caption, version) AS source_caption
FROM cbr_risk_weight_addons FINAL
GROUP BY matrix, valid_from, row_code, col_code`,
	Comment: "Матрицы надбавок к коэффициентам риска Банка России: розничные (matrix='retail', " +
		"Таблица 1), корпоративные (matrix='corporate', Таблица 2) и старая общая матрица " +
		"(matrix='legacy', 2013–2022). Одна строка — ячейка матрицы: надбавка в процентных " +
		"пунктах для пары (row_code, col_code). Коды — из приложения 7 Указания Банка России " +
		"№ 6960-У; машиночитаемого справочника кодов на сайте нет, поэтому коды хранятся как в " +
		"источнике. valid_from/valid_to — период, в котором возникли кредитные требования; " +
		"valid_to = 2100-01-01 означает «дальше». is_applicable = 0 — источник написал «н/п» " +
		"(надбавка не применяется), это не ноль; is_applicable = 1 и value = 0 — настоящее " +
		"нулевое значение. Архивные файлы содержат историю и пересекаются по периодам; витрина " +
		"показывает последнюю версию по version для каждого (matrix, valid_from, row_code, " +
		"col_code), включая valid_to: более поздний документ закрывает или продлевает период, " +
		"source_file называет победивший файл, source_caption — его подпись на landing. Перед " +
		"использованием читать v_series_catalog " +
		"по (source='cbr_risk_weight_addons', series=matrix)",
	Columns: map[string]string{
		"source":         "Импортёр-источник: всегда cbr_risk_weight_addons",
		"matrix":         "Вид матрицы: retail — Таблица 1 (физические лица), corporate — Таблица 2 (юридические лица), legacy — общая матрица до 2022 года",
		"series":         "То же, что matrix; имя ряда в v_series_catalog",
		"valid_from":     "Начало периода, в котором возникли кредитные требования (Date): строка «с» листа матрицы",
		"valid_to":       "Конец периода (Date); 2100-01-01 — сентинел «дальше» (в источнике «по» пустое)",
		"table_no":       "Номер таблицы источника («Таблица № 15»); у старой общей матрицы пусто — номера в листе нет",
		"row_code":       "Код строки матрицы (например, 2001.i): вид кредитного требования по приложению 7 Указания № 6960-У",
		"col_code":       "Код столбца матрицы (например, 1008.i): характеристика вида актива по приложению 7 Указания № 6960-У",
		"value":          "Надбавка к коэффициенту риска, п.п.; при is_applicable = 1 ноль — настоящее значение. При is_applicable = 0 значение не применять",
		"is_applicable":  "1 — источник дал число (включая 0); 0 — «н/п»/x/х/дефис/пусто, надбавка не применяется",
		"source_file":    "Файл-версия матрицы (basename), победившая по version; архивные файлы сохраняются в таблице отдельно",
		"source_caption": "Подпись ссылки на landing («Таблица 1 с 04.09.2026» и т.п.) — провенанс версии",
	},
}

// afterCbrRiskWeightAddons — производные импортёра cbr_risk_weight_addons:
// каталог трёх видов матриц и витрина v_cbr_risk_weight_addons. Публикуются
// после цикла файлов и на no-op-пути (Import переопределён): витрина —
// производная таблицы, пропущенный при сбое CreateView самолечится без новой
// версии источника.
func afterCbrRiskWeightAddons(meta []util.SeriesMeta) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error {
		if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
		_, err := util.CreateView(ctx, conn, cbrRiskWeightAddonsView)
		return err
	}
}

// cbrMPLView — витрина макропруденциальных лимитов. Серия — segment + класс
// кредита; одна строка — правило/полоса с лимитом. Старые форматы (v1/v2) дают
// по строке на полосу, текущий (v3) — правило с набором полос.
var cbrMPLView = util.View{
	Name:   "v_cbr_mpl",
	Tables: []string{cbrMPLTable},
	Select: `SELECT 'cbr_mpl' AS source,
       concat(segment, '_', loan_class) AS series,
       segment, loan_class, period_from, period_to, row_no, subrow_no, loan_kind,
       pdn_band, pdn_min, pdn_max,
       term_band, term_min, term_max,
       down_payment_band, down_payment_min, down_payment_max,
       ltv_band, ltv_min, ltv_max,
       amount_band, amount_min, amount_max,
       limit_pct, source_file
FROM cbr_mpl FINAL`,
	Comment: "Макропруденциальные лимиты (МПЛ) Банка России: банки с универсальной " +
		"лицензией и микрофинансовые организации, потребительские кредиты/займы и ипотека, " +
		"2023Q1 … 2026Q4. limit_pct — доля рискованных кредитов в общем объёме выдач, %; " +
		"старые форматы источника (2023-01 … 2025-06) публиковали лимит долей (0.25) — здесь " +
		"она нормализована в проценты (25). Одна строка — правило или полоса: в старых " +
		"форматах одна строка на полосу (band_idx), в текущем — правило с набором полос " +
		"(ПДН + срок + первоначальный взнос/LTV/сумма), у ипотеки подстроки наследуют лимит " +
		"и незаполненные условия родительской строки. *_band — подпись полосы как в источнике " +
		"или нормализованная ((50; 80], более 80), *_min/*_max — разобранные числовые границы; " +
		"nil означает, что границы нет (x/х/-), а не ноль. period_to = 2100-01-01 — «дальше». " +
		"Перед использованием читать v_series_catalog по (source='cbr_mpl', series)",
	Columns: map[string]string{
		"source":            "Импортёр-источник: всегда cbr_mpl",
		"series":            "concat(segment, '_', loan_class): bank_consumer, bank_consumer_with_limit, bank_consumer_other, bank_mortgage, mfo_consumer_with_limit, mfo_consumer_other, mfo_consumer_loan",
		"segment":           "Сегмент: bank — банки с универсальной лицензией, mfo — микрофинансовые организации",
		"loan_class":        "Класс: v1/v2 — consumer_with_limit/consumer_other; v3 — consumer (потребительские кредиты), mortgage (ипотека), consumer_loan (потребительские займы МФО)",
		"period_from":       "Начало периода, в котором предоставлен кредит/установлен лимит (Date)",
		"period_to":         "Конец периода (Date); 2100-01-01 — сентинел «дальше»",
		"row_no":            "Номер строки источника (для аудита); у v3 подстроки — номер родительской строки",
		"subrow_no":         "Номер подстроки v3 («1.1», «1.2»); у v1/v2 пусто",
		"loan_kind":         "Вид кредита/займа или тип МПЛ: текст источника; у v3 подстроки наследуют родительский вид",
		"pdn_band":          "Полоса ПДН: подпись источника или нормализованная ((50; 80], более 80); пусто — ограничения по ПДН нет",
		"pdn_min":           "Нижняя числовая граница ПДН, %; nil — границы нет (x/х/-), а не ноль",
		"pdn_max":           "Верхняя числовая граница ПДН, %; nil — верхней границы нет",
		"term_band":         "Полоса срока: подпись источника/нормализованная; для банков — годы, для МФО — дни",
		"term_min":          "Нижняя числовая граница срока; nil — границы нет",
		"term_max":          "Верхняя числовая граница срока; nil — верхней границы нет",
		"down_payment_band": "Полоса первоначального взноса (ипотека): подпись источника",
		"down_payment_min":  "Нижняя граница первоначального взноса, %; nil — границы нет",
		"down_payment_max":  "Верхняя граница первоначального взноса, %; nil — верхней границы нет",
		"ltv_band":          "Полоса LTV (ипотека): подпись источника",
		"ltv_min":           "Нижняя граница LTV, %; nil — границы нет",
		"ltv_max":           "Верхняя граница LTV, %; nil — верхней границы нет",
		"amount_band":       "Полоса суммы займа (МФО, тыс. руб.): подпись источника",
		"amount_min":        "Нижняя граница суммы, тыс. руб.; nil — границы нет",
		"amount_max":        "Верхняя граница суммы, тыс. руб.; nil — верхней границы нет",
		"limit_pct":         "Макропруденциальный лимит, % (доля рискованных кредитов в общем объёме выдач); старые доли нормализованы ×100",
		"source_file":       "Файл-версия источника (basename); периоды v1/v2/v3 не пересекаются, поэтому версии сосуществуют",
	},
}

// afterCbrMPL — производные импортёра cbr_mpl: каталог семи видов лимитов и
// витрина v_cbr_mpl. Публикуются после цикла файлов и на no-op-пути
// (Import переопределён): витрина — производная таблицы, пропущенный при сбое
// CreateView самолечится без новой версии источника.
func afterCbrMPL(meta []util.SeriesMeta) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error {
		if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
		_, err := util.CreateView(ctx, conn, cbrMPLView)
		return err
	}
}

// cbrMpBsuMapView — публичный кроссворд 87 кодов ОКВЭД2 → отраслевой лист
// mp_survey_data.xlsx с названием кода из классификатора Росстата. Витрина требует
// rosstat_okved2: без него CreateView пропускает её, а таблица кроссворда уже записана.
var cbrMpBsuMapView = util.View{
	Name:   "v_cbr_mp_bsu_map",
	Tables: []string{cbrMpBsuMapTable, "rosstat_okved2"},
	Select: `SELECT m.code, o.name AS okved_name, o.section,
       m.industry_sheet, m.industry_slug, m.basis, m.confidence, m.source
FROM cbr_mp_bsu_map m FINAL
LEFT JOIN (
    SELECT code, any(name) AS name, any(section) AS section
    FROM rosstat_okved2 FINAL
    WHERE level > 1
    GROUP BY code
) o ON o.code = m.code
ORDER BY m.code`,
	Comment: "Публичный кроссворд 87 стандартных кодов ОКВЭД2 (архив авторов CB_COMP) " +
		"на 11 отраслевых листов mp_survey_data.xlsx (из 16: у 87 кодов нет электричества и " +
		"водоснабжения). Это публичная аппроксимация, а не " +
		"внутренний mapping ЦБ: подкатегории обработки размечены вручную, коды 45/46/47 и " +
		"55–96 — по классам ОКВЭД2. 87 кодов схлопываются в 33 публичные серии; " +
		"код-уровневых значений в публичной книге нет. confidence: rule — по разделу/классу, " +
		"manual — производственная подкатегория, fallback — общий лист «Обработка» (в текущем " +
		"наборе не используется).",
	Columns: map[string]string{
		"code":           "Код ОКВЭД2 из публичного model.xlsx (1.0.4.2, мониторинг_код1..4)",
		"okved_name":     "Официальное название кода из rosstat_okved2; пусто, если классификатор ещё не импортирован",
		"section":        "Буква раздела ОКВЭД2 A…U из rosstat_okved2",
		"industry_sheet": "Точное имя листа публичной книги mp_survey_data.xlsx",
		"industry_slug":  "Slug листа в именах рядов cbr_survey: industry_<slug>_<indicator>[_sa]",
		"basis":          "Правило/обоснование выбора листа; для manual — конкретный код и подкатегория",
		"confidence":     "rule|manual|fallback — степень публичной определённости кроссворда",
		"source":         "Публичный источник списка кодов: figshare DOI + model.xlsx#1.0.4.2",
	},
}

// cbrMpPublicPanelView — публичная панель: каждый из 87 кодов получает SA-серию своего
// отраслевого листа (IBK, ценовые ожидания, издержки). Это не 87 код-уровневых рядов:
// коды одного листа делят одну серию, поэтому строк 87 × 3 × 297, а различных серий — 39.
var cbrMpPublicPanelView = util.View{
	Name:   "v_cbr_mp_public_panel",
	Tables: []string{cbrMpBsuMapTable, mpSurveyTable},
	Select: `SELECT m.code, m.industry_sheet, m.industry_slug, 'ibk' AS indicator, ms.date, ms.value
FROM cbr_mp_bsu_map m FINAL
INNER JOIN macro_series ms FINAL
  ON ms.source = 'cbr_survey'
 AND ms.series = concat('industry_', m.industry_slug, '_ibk_sa')
UNION ALL
SELECT m.code, m.industry_sheet, m.industry_slug, 'price_exp', ms.date, ms.value
FROM cbr_mp_bsu_map m FINAL
INNER JOIN macro_series ms FINAL
  ON ms.source = 'cbr_survey'
 AND ms.series = concat('industry_', m.industry_slug, '_price_expectations_sa')
UNION ALL
SELECT m.code, m.industry_sheet, m.industry_slug, 'costs', ms.date, ms.value
FROM cbr_mp_bsu_map m FINAL
INNER JOIN macro_series ms FINAL
  ON ms.source = 'cbr_survey'
 AND ms.series = concat('industry_', m.industry_slug, '_cost_change_sa')`,
	Comment: "Публичная панель 87 BSU-кодов: каждому коду сопоставлена SA-серия его " +
		"отраслевого листа mp_survey_data.xlsx (industry_<slug>_ibk_sa, " +
		"industry_<slug>_price_expectations_sa, industry_<slug>_cost_change_sa). " +
		"Это отраслевая аппроксимация: 87 кодов делят 33 различные серии (11 листов × 3 " +
		"индикатора), поэтому строки разных кодов одного листа совпадают. Код-уровневых " +
		"значений публичная книга не содержит; live-источника на 87 кодах нет — значения " +
		"авторской панели остаются статическим архивом 2009-01…2024-09. Для per-BSU " +
		"использовать как диагностику потери гранулярности, не как паритет.",
	Columns: map[string]string{
		"code":           "Код ОКВЭД2 (87 стандартных кодов CB_COMP)",
		"industry_sheet": "Имя отраслевого листа публичной книги",
		"industry_slug":  "Slug листа в рядах cbr_survey",
		"indicator":      "ibk — ИБК; price_exp — ценовые ожидания; costs — издержки",
		"date":           "Отчётный месяц (1-е число); SA-точка публикуется ЦБ в следующем месяце",
		"value":          "Баланс ответов, пункты (или % годовых только для отдельного ряда ожидаемого прироста цен; здесь его нет)",
	},
}

// cbrPublicationCalendarView — график публикаций ЦБ: статистическое окно вперёд и
// аналитические месячные блоки назад. Это план, а не журнал фактов: ЦБ прямо
// предупреждает, что даты могут меняться; фактическую дату релиза для исторического
// published_at дают релизные архивы (v_series_publication), а не календарь.
var cbrPublicationCalendarView = util.View{
	Name:   "v_cbr_publication_calendar",
	Tables: []string{cbrPublicationCalendarTable},
	Select: `SELECT publication_date, publication_time, title, topic, period, source_url, planned
FROM cbr_publication_calendar FINAL`,
	Comment: "График публикаций Банка России (cbr.ru/calendar/): статистическое окно " +
		"вперёд (topic='statistics', 173 строки в окне 20.10.2026 … 30.12.2026 на снимке " +
		"2026-10-20) и 22 аналитических месячных блока назад (topic='analytics', январь " +
		"2025 … октябрь 2026, 445 строк). Это ПЛАН, а не журнал фактов: ЦБ предупреждает, " +
		"что даты могут меняться; для исторического published_at использовать " +
		"v_series_publication, где дата взята из релизного архива. publication_time — " +
		"только там, где ЦБ её указал (16:00, 11:00; у аналитики и многих статистических " +
		"строк NULL). period — исходный период строки статистики («II квартал 2026 г.», " +
		"«01.10.2026») либо YYYY-MM аналитического блока. planned — снимок на момент " +
		"импорта: 1, если дата была в будущем или сегодня, иначе 0; прошлые строки не " +
		"переписываются",
	Columns: map[string]string{
		"publication_date": "Дата публикации (Date)",
		"publication_time": "Время публикации, «HH:MM»; NULL — ЦБ время не указал",
		"title":            "Название публикации/показателя; у статистики — текст ссылки, у аналитики — название строки блока",
		"topic":            "statistics — официальная статистическая информация; analytics — аналитические публикации",
		"period":           "Период наблюдения из строки статистики либо YYYY-MM аналитического блока",
		"source_url":       "Ссылка на страницу показателя/публикации на cbr.ru",
		"planned":          "1 — дата была в будущем или сегодня на момент снимка; 0 — уже прошла",
	},
}

// cbrSeriesPublicationView — generic-слой публикационных дат: одна строка = одна
// точка ряда и effective-момент, когда она стала публичной. Сейчас покрыты четыре
// ряда инфляционных ожиданий/ИПЦ (source='cbr') и два ряда ценовых ожиданий
// мониторинга предприятий (source='cbr_survey'); source/series совпадают с
// v_series_catalog, поэтому lookup идёт по тем же ключам, что и чтение значения.
var cbrSeriesPublicationView = util.View{
	Name:   "v_series_publication",
	Tables: []string{util.SeriesPublicationTable},
	Select: `SELECT source, series, period_date, published_at, quality, source_url
FROM series_publication FINAL`,
	Comment: "Публикационные даты рядов: effective-момент published_at для каждой точки " +
		"(source, series, period_date). source/series совпадают с v_series_catalog: " +
		"source='cbr' — «наблюдаемая инфляция»/«ожидаемая инфляция» (cbr_infl_exp) и " +
		"«Все товары и услуги»/«Базовый ИПЦ» (cbr_indicators_cpd); source='cbr_survey' — " +
		"price_expectations и price_expectations_sa. period_date — дата точки в таблице " +
		"источника (14-е у инфляционных ожиданий, последний день месяца у ИПЦ, 1-е у " +
		"мониторинга), месяц для сопоставления — toStartOfMonth(period_date). quality: " +
		"exact_time — известно время (текущие релизы ЦБ времени не дают, поэтому таких " +
		"строк 0); date — известна дата релиза, published_at = 23:59:59 MSK этой даты; " +
		"fallback — точной даты нет, published_at = последний день M+1 23:59:59 MSK " +
		"(M — отчётный месяц). As-of правило: точка доступна при строгом " +
		"published_at < cutoff (дата/время заседания); fallback консервативен и не должен " +
		"выглядеть как точная дата в отчётах. source_url — страница версии (date/exact_time) " +
		"или индекс релизного архива (fallback)",
	Columns: map[string]string{
		"source":       "Источник как в v_series_catalog: cbr — инфляционные ожидания и ИПЦ; cbr_survey — мониторинг предприятий",
		"series":       "Имя ряда; совпадает с series в v_series_catalog",
		"period_date":  "Дата точки в таблице источника (14-е, конец месяца, 1-е); месяц — toStartOfMonth(period_date)",
		"published_at": "Effective-момент публикации в MSK: точное время (exact_time), 23:59:59 даты релиза (date) или конец M+1 (fallback). Граница as-of строгая: published_at < cutoff",
		"quality":      "exact_time | date | fallback — точность published_at; понижение качества при повторном импорте запрещено",
		"source_url":   "Страница версии релиза (date/exact_time) либо индекс архива (fallback)",
	},
}

// --- Контур ключевой ставки: MIACR, ликвидность, прогноз ОНДКП (2026-11-14) ---

// cbrMiracView — ставки денежного рынка MIACR/MIACR-IG/MIACR-B по срокам. Длинная форма,
// чтобы агент сравнивал RUONIA и MIACR в одном запросе.
var cbrMiracView = util.View{
	Name:   "v_cbr_mirac",
	Tables: []string{cbrMiracTable},
	Select: "SELECT name, tenor, date, value FROM cbr_mirac FINAL",
	Comment: "Ставки денежного рынка ЦБ по сделкам МБК: MIACR (все московские банки), " +
		"MIACR-IG (банки с высоким кредитным рейтингом), MIACR-B (спекулятивный рейтинг), " +
		"% годовых по шести срокам. История MIACR — с 2000-08-01, MIACR-IG и MIACR-B — " +
		"с 2010-01-11: отсутствие точки означает, что источник её не публиковал, а не ноль. " +
		"Значения в форме ЦБ печатаются с прочерком, если за день сделок срока не было — " +
		"такие точки не пишутся. Обороты сделок (млн руб.) в витрину не входят.",
	Columns: map[string]string{
		"name":  "Инструмент: MIACR (все московские банки), MIACR-IG (высокий кредитный рейтинг), MIACR-B (спекулятивный)",
		"tenor": "Срок кредита: 1d (1 день), 2-7d, 8-30d, 31-90d, 91-180d, 181d-1y (181 день — 1 год)",
		"date":  "Торговый день, за который рассчитана средневзвешенная ставка",
		"value": "Средневзвешенная фактическая ставка, % годовых. Нет строки — не было сделок срока за день, а не нулевая ставка",
	},
}

// cbrLiquidityView — ликвидность банковского сектора: дневной срез и периоды усреднения.
var cbrLiquidityView = util.View{
	Name:   "v_cbr_liquidity",
	Tables: []string{cbrLiquidityTable},
	Select: "SELECT kind, series, date, value FROM cbr_liquidity FINAL",
	Comment: "Ликвидность банковского сектора ЦБ: kind='daily' — дневной срез (с 2014-01-01), " +
		"kind='averaging' — показатели периода усреднения (с 2018-01-10, дата — первый день периода). " +
		"Знак структурной позиции: дефицит (+), профицит (-) — переход к структурному дефициту " +
		"ЦБ называет автономным фактором решения по ставке. Ряды daily/* — млрд руб., кроме " +
		"явно базисных пунктов; averaging/key_rate_change_bp и averaging/ruonia_spread_bp — " +
		"базисные пункты. Единицы и смысл каждого ряда — в v_series_catalog (source='cbr_liquidity').",
	Columns: map[string]string{
		"kind":   "Вид ряда: daily — дневной срез формы /hd_base/bliquidity; averaging — период усреднения /hd_base/BSliquidityMA",
		"series": "Имя ряда: deficit, deficit_ex_corr, cb_claims*, cb_liabilities*, nonstandard_repo, corr_accounts, required_reserves, structural_deficit, liquidity_need, *_auctions, standing_net, special_net, ruonia_debt (расшифровка — title в v_series_catalog)",
		"date":   "Дата: для daily — торговый день; для averaging — первый день периода усреднения",
		"value":  "Значение, млрд руб. (кроме key_rate_change_bp и ruonia_spread_bp — базисные пункты). Знак «дефицит (+) / профицит (-)» — как в заголовке формы ЦБ",
	},
}

// cbrOndkpForecastView — прогнозы ЦБ из винтажей ОНДКП.
var cbrOndkpForecastView = util.View{
	Name:   "v_cbr_ondkp_forecast",
	Tables: []string{cbrOndkpForecastTable, cbrOndkpTable},
	// FINAL идёт после алиаса: «FROM t AS f FINAL» — так требует грамматика ClickHouse
	// (после FINAL алиас уже нельзя).
	Select: "SELECT f.release_year, f.indicator, f.year, f.lo, f.hi, o.published_at, o.url " +
		"FROM cbr_ondkp_forecast AS f FINAL LEFT JOIN cbr_ondkp AS o FINAL ON f.release_year = o.release_year",
	Comment: "Прогнозы Банка России из «Основных направлений единой государственной " +
		"денежно-кредитной политики»: винтаж (release_year — год политики), индикатор, год " +
		"прогноза и границы lo/hi (lo = hi — точка). Это винтажный ряд: один и тот же год " +
		"встречается в нескольких выпусках, as-of — release_year и published_at, а не год " +
		"прогноза. Машиночитаемая таблица базового сценария есть только у выпусков 2020–2027: " +
		"у 2000–2019 прогноз изложен прозой (см. docs/LIMITATIONS.md), поэтому строк за них нет. " +
		"Цель по инфляции с 2015 года — 4 %, прогноз старших винтажей в годы шоков от неё отклоняется.",
	Columns: map[string]string{
		"release_year": "Винтаж: год, на который принят документ (документ «на 2027» опубликован 28.10.2026). As-of по этому полю",
		"indicator":    "key_rate — среднегодовая ключевая ставка; inflation — инфляция в среднем за год; gdp — прирост ВВП",
		"year":         "Год прогноза (release_year, release_year+1, release_year+2)",
		"lo":           "Нижняя граница диапазона, %; при lo = hi — точечный прогноз",
		"hi":           "Верхняя граница диапазона, %",
		"published_at": "Дата публикации выпуска (Nullable: у страниц 2021–2027 на индексе её нет)",
		"url":          "Артефакт выпуска: PDF документа (или ZIP «Вестника» для 2000–2009)",
	},
}

// afterCbrSeries — производные импортёров с собственной длинной таблицей: каталог рядов и
// одна витрина. Публикуются на каждом прогоне, включая no-op: сбой каталога или витрины
// лечится следующим прогоном, а не ручной правкой (ARCHITECTURE §8 п. 13).
func afterCbrSeries(meta []util.SeriesMeta, view util.View) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error {
		if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
		_, err := util.CreateView(ctx, conn, view)
		return err
	}
}
