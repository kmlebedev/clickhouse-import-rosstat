package fedstat

// fedstatProduct — товар/услуга источника ЕМИСС 37426: фильтр выгрузки, код SDMX
// и стабильное имя ряда. Имена объявлены кодом (стандарт §6.8), а не берутся из
// заголовка ответа: CodeList источника содержит те же 158 названий, но имя ряда
// не должно зависеть от редакции выгрузки.
//
// historical — есть ли точки в 2008–2022 (s_OKATO=643, «Российская Федерация»);
// y2026 — есть ли точки в 2026 неделях 39–40 (s_OKATO=643004.АГ, «РФ без учёта
// новых субъектов»). Флаги ведут описание каталога: у 15 товаров нет истории,
// у 44 — нет 2026; объединение — 158.
type fedstatProduct struct {
	filterID   string
	code       string
	name       string
	historical bool
	y2026      bool
}

// fedstatProducts — полный набор фильтра «Виды товаров и услуг» (58273), 158
// записей. Порядок — по имени; он же порядок каталога и детерминированного
// батча. filterID нужен POST-форме (selectedFilterIds=58273_<filterID>), code —
// разбору SeriesKey (s_grtov=<code>).
var fedstatProducts = []fedstatProduct{
	{filterID: "1785685", code: "7976", name: "Активированный уголь, 10 таблеток", historical: false, y2026: true},
	{filterID: "1755074", code: "7995", name: "Аллохол, 50 таблеток", historical: true, y2026: false},
	{filterID: "1754983", code: "7977", name: "Алмагель, суспензия, 200 мл", historical: true, y2026: false},
	{filterID: "1754800", code: "2711", name: "Бананы, кг", historical: true, y2026: true},
	{filterID: "1755103", code: "116", name: "Баранина (кроме бескостного мяса), кг", historical: true, y2026: true},
	{filterID: "1755210", code: "7800", name: "Бензин автомобильный", historical: true, y2026: true},
	{filterID: "1709801", code: "7801", name: "Бензин автомобильный марки А-76 (АИ-80), л", historical: true, y2026: false},
	{filterID: "1709730", code: "7802", name: "Бензин автомобильный марки АИ-92, л", historical: true, y2026: true},
	{filterID: "1709750", code: "7803", name: "Бензин автомобильный марки АИ-95, л", historical: true, y2026: true},
	{filterID: "1755184", code: "7806", name: "Бензин автомобильный марки АИ-98 и выше, л", historical: true, y2026: true},
	{filterID: "1755171", code: "7989", name: "Бромгексин, 8 мг, 10 драже", historical: true, y2026: false},
	{filterID: "1754798", code: "4155", name: "Брюки для детей школьного возраста из джинсовой ткани, шт.", historical: true, y2026: true},
	{filterID: "1754919", code: "6502", name: "Бумага туалетная, рулон", historical: true, y2026: true},
	{filterID: "1754964", code: "7926", name: "Валидол, 10 таблеток", historical: true, y2026: true},
	{filterID: "1754988", code: "7923", name: "Валокордин, 20 мл", historical: true, y2026: false},
	{filterID: "1755172", code: "2401", name: "Вермишель, кг", historical: true, y2026: true},
	{filterID: "1709752", code: "2811", name: "Водка крепостью 40% об. спирта и выше, л", historical: true, y2026: false},
	{filterID: "1709799", code: "2803", name: "Водка крепостью 40% об.спирта и выше обыкновенного качества, л", historical: true, y2026: false},
	{filterID: "1848408", code: "2812", name: "Водка, л", historical: false, y2026: true},
	{filterID: "1755212", code: "9458", name: "Водоотведение, м3", historical: true, y2026: true},
	{filterID: "1755208", code: "9448", name: "Водоснабжение горячее, м3", historical: true, y2026: true},
	{filterID: "1759576", code: "9446", name: "Водоснабжение горячее, месяц с человека", historical: true, y2026: true},
	{filterID: "1759589", code: "9442", name: "Водоснабжение холодное и водоотведение, месяц с человека", historical: true, y2026: true},
	{filterID: "1755194", code: "9457", name: "Водоснабжение холодное, м3", historical: true, y2026: true},
	{filterID: "1853997", code: "9796", name: "Восстановление зуба пломбой, услуга", historical: false, y2026: true},
	{filterID: "1755070", code: "9459", name: "Газ сжиженный, м3", historical: true, y2026: false},
	{filterID: "1709756", code: "111", name: "Говядина (кроме бескостного мяса), кг", historical: true, y2026: true},
	{filterID: "1755196", code: "7804", name: "Дизельное топливо, л", historical: true, y2026: true},
	{filterID: "1755045", code: "9607", name: "Дом отдыха, пансионат, день", historical: true, y2026: true},
	{filterID: "1754936", code: "7418", name: "Доска обрезная, м3", historical: true, y2026: true},
	{filterID: "1743463", code: "2601", name: "Капуста белокочанная свежая, кг", historical: true, y2026: true},
	{filterID: "1755158", code: "1711", name: "Карамель, кг", historical: true, y2026: false},
	{filterID: "1743470", code: "2501", name: "Картофель, кг", historical: true, y2026: true},
	{filterID: "1856405", code: "1132", name: "Кефир, кг", historical: false, y2026: true},
	{filterID: "1785708", code: "206", name: "Колбаса вареная I сорта, кг", historical: true, y2026: false},
	{filterID: "1743476", code: "201", name: "Колбаса вареная, кг", historical: true, y2026: true},
	{filterID: "1709791", code: "204", name: "Колбаса полукопченая и варено-копченая, кг", historical: true, y2026: true},
	{filterID: "1755206", code: "4601", name: "Колготки женские эластичные, шт.", historical: true, y2026: true},
	{filterID: "1754956", code: "7957", name: "Комбинированные анальгетики, 10 таблеток", historical: true, y2026: false},
	{filterID: "1755178", code: "302", name: "Консервы мясные для детского питания, кг", historical: true, y2026: true},
	{filterID: "1755075", code: "301", name: "Консервы мясные,  350 г", historical: true, y2026: false},
	{filterID: "1754700", code: "1303", name: "Консервы овощные для детского питания, кг", historical: true, y2026: true},
	{filterID: "1754781", code: "1402", name: "Консервы фруктово-ягодные для детского питания, кг", historical: true, y2026: true},
	{filterID: "1743465", code: "1712", name: "Конфеты мягкие, глазированные шоколадом, кг", historical: true, y2026: false},
	{filterID: "1709725", code: "7924", name: "Корвалол, 25 мл", historical: true, y2026: true},
	{filterID: "1754794", code: "4404", name: "Костюм спортивный для детей школьного возраста, шт.", historical: true, y2026: true},
	{filterID: "1743464", code: "4771", name: "Кроссовые туфли для взрослых с верхом из искусственной кожи, пара", historical: true, y2026: false},
	{filterID: "1846522", code: "4773", name: "Кроссовые туфли для взрослых, пара", historical: true, y2026: true},
	{filterID: "1754891", code: "4744", name: "Кроссовые туфли для детей с верхом из искусственной кожи, пара", historical: true, y2026: true},
	{filterID: "1755104", code: "2307", name: "Крупа гречневая-ядрица, кг", historical: true, y2026: true},
	{filterID: "1709748", code: "114", name: "Куры охлажденные и мороженые, кг", historical: true, y2026: true},
	{filterID: "1755028", code: "7994", name: "Левомеколь, мазь, 40 г", historical: true, y2026: true},
	{filterID: "1785651", code: "7708", name: "Легковой автомобиль иностранной марки новый, шт.", historical: true, y2026: true},
	{filterID: "1755117", code: "7707", name: "Легковой автомобиль иностранной марки, собранный  на автомобильном заводе на территории России, шт.", historical: true, y2026: false},
	{filterID: "1755166", code: "7703", name: "Легковой автомобиль отечественный новый, шт.", historical: true, y2026: true},
	{filterID: "1755135", code: "7987", name: "Линекс, 10 капсул", historical: true, y2026: false},
	{filterID: "1755134", code: "2603", name: "Лук репчатый, кг", historical: true, y2026: true},
	{filterID: "1743456", code: "4502", name: "Майка, футболка мужская бельевая, шт.", historical: true, y2026: true},
	{filterID: "1709722", code: "2403", name: "Макаронные изделия из пшеничной муки высшего сорта, кг", historical: true, y2026: true},
	{filterID: "1755044", code: "1001", name: "Маргарин, кг", historical: true, y2026: true},
	{filterID: "1709737", code: "801", name: "Масло подсолнечное, кг", historical: true, y2026: false},
	{filterID: "1846527", code: "803", name: "Масло подсолнечное, л", historical: true, y2026: true},
	{filterID: "1709734", code: "701", name: "Масло сливочное, кг", historical: true, y2026: true},
	{filterID: "1755000", code: "7427", name: "Металлочерепица, м2", historical: true, y2026: false},
	{filterID: "1754906", code: "7927", name: "Метамизол натрия (Анальгин отечественный), 500 мг, 10 таблеток", historical: true, y2026: true},
	{filterID: "1755201", code: "9143", name: "Мойка легкового автомобиля, услуга", historical: false, y2026: true},
	{filterID: "1709745", code: "1111", name: "Молоко питьевое цельное пастеризованное 2,5-3,2% жирности, л", historical: true, y2026: true},
	{filterID: "1755039", code: "1112", name: "Молоко питьевое цельное стерилизованное 2,5-3,2% жирности, л", historical: true, y2026: false},
	{filterID: "1853986", code: "1131", name: "Молоко питьевое цельное ультрапастеризованное 2,5-3,2% жирности, л", historical: false, y2026: true},
	{filterID: "1759580", code: "1108", name: "Молоко цельное пастеризованное, стерилизованное 2,5-3,2% жирности, л", historical: true, y2026: false},
	{filterID: "1759578", code: "1113", name: "Молочный напиток пастеризованный 2,5-3,2% жирности, л", historical: true, y2026: false},
	{filterID: "1759587", code: "1114", name: "Молочный напиток стерилизованный 2,5-3,2% жирности, л", historical: true, y2026: false},
	{filterID: "1743475", code: "2605", name: "Морковь, кг", historical: true, y2026: true},
	{filterID: "1755145", code: "2101", name: "Мука пшеничная, кг", historical: true, y2026: true},
	{filterID: "1755080", code: "5201", name: "Мыло туалетное, 100 г", historical: true, y2026: true},
	{filterID: "1755051", code: "5001", name: "Мыло хозяйственное, 200 г", historical: true, y2026: true},
	{filterID: "1846558", code: "7980", name: "Нафазолин, 0,1%, 15 мл", historical: false, y2026: true},
	{filterID: "1754957", code: "7990", name: "Нимесулид, 10 таблеток", historical: true, y2026: true},
	{filterID: "1743468", code: "4603", name: "Носки мужские, пара", historical: true, y2026: true},
	{filterID: "1754938", code: "4604", name: "Носки, гольфы детские, пара", historical: true, y2026: false},
	{filterID: "1754777", code: "3606", name: "Обед в столовой, кафе, закусочной (кроме столовой в организации), на 1 человека", historical: true, y2026: true},
	{filterID: "1743457", code: "2621", name: "Огурцы свежие, кг", historical: true, y2026: true},
	{filterID: "1755197", code: "9449", name: "Отопление, Гкал", historical: true, y2026: true},
	{filterID: "1785748", code: "9445", name: "Отопление, м2 общей площади", historical: true, y2026: true},
	{filterID: "1755167", code: "7981", name: "Офтан катахром, 10 мл", historical: true, y2026: false},
	{filterID: "1755032", code: "5313", name: "Паста зубная, 100 г (100 мл)", historical: true, y2026: true},
	{filterID: "1754703", code: "4176", name: "Пеленки для новорожденных, шт.", historical: true, y2026: true},
	{filterID: "1853987", code: "8042", name: "Пенталгин, 24 таблетки", historical: false, y2026: true},
	{filterID: "1755149", code: "1701", name: "Печенье, кг", historical: true, y2026: true},
	{filterID: "1755198", code: "9418", name: "Плата за жилье в домах государственного и муниципального жилищных фондов, м2 общей площади", historical: true, y2026: true},
	{filterID: "1754905", code: "7403", name: "Плиты древесностружечные, ориентированно-стружечные, м2", historical: true, y2026: true},
	{filterID: "1755026", code: "6504", name: "Подгузники детские бумажные (памперсы), 10 шт.", historical: true, y2026: true},
	{filterID: "1755053", code: "9548", name: "Поездка в Грецию, поездка", historical: true, y2026: false},
	{filterID: "1755142", code: "9547", name: "Поездка в Китай, поездка", historical: true, y2026: false},
	{filterID: "1785693", code: "9541", name: "Поездка на отдых в Турцию, поездка", historical: true, y2026: false},
	{filterID: "1848518", code: "9536", name: "Поездка на отдых на Черноморское побережье России, поездка", historical: false, y2026: true},
	{filterID: "1846846", code: "9559", name: "Поездки в отдельные страны Юго-Восточной Азии, поездка", historical: false, y2026: true},
	{filterID: "1755161", code: "9261", name: "Полет в салоне экономического класса самолета, в расчете на 1000 км пути", historical: true, y2026: false},
	{filterID: "1754970", code: "7913", name: "Поливитамины без минералов отечественные, 10 шт.", historical: true, y2026: false},
	{filterID: "1755071", code: "7965", name: "Поливитамины с макро- и микроэлементами, 10 шт.", historical: true, y2026: false},
	{filterID: "1846526", code: "8029", name: "Поливитамины, 10 шт.", historical: true, y2026: false},
	{filterID: "1755033", code: "2623", name: "Помидоры свежие, кг", historical: true, y2026: true},
	{filterID: "1755152", code: "5101", name: "Порошок стиральный, кг", historical: true, y2026: true},
	{filterID: "1785650", code: "9222", name: "Проезд в городском автобусе, поездка", historical: true, y2026: true},
	{filterID: "1755205", code: "9211", name: "Проезд в городском муниципальном автобусе, поездка", historical: true, y2026: false},
	{filterID: "1755200", code: "9214", name: "Проезд в метро, поездка", historical: true, y2026: true},
	{filterID: "1755193", code: "9212", name: "Проезд в трамвае, поездка", historical: true, y2026: true},
	{filterID: "1755209", code: "9213", name: "Проезд в троллейбусе, поездка", historical: true, y2026: true},
	{filterID: "1836582", code: "9462", name: "Проживание в гостинице 1* или в мотеле, сутки с человека", historical: true, y2026: true},
	{filterID: "1836583", code: "9463", name: "Проживание в гостинице 2*, сутки с человека", historical: true, y2026: true},
	{filterID: "1836584", code: "9464", name: "Проживание в гостинице 3*, сутки с человека", historical: true, y2026: true},
	{filterID: "1836585", code: "9465", name: "Проживание в гостинице 4*-5*, сутки с человека", historical: true, y2026: true},
	{filterID: "1755146", code: "9414", name: "Проживание в гостинице, сутки с человека", historical: true, y2026: false},
	{filterID: "1836586", code: "9466", name: "Проживание в хостеле, сутки с человека", historical: true, y2026: true},
	{filterID: "1754980", code: "6503", name: "Прокладки женские гигиенические ежедневные, 10 шт.", historical: true, y2026: false},
	{filterID: "1836581", code: "6508", name: "Прокладки женские гигиенические, 10 шт.", historical: true, y2026: true},
	{filterID: "1755036", code: "2303", name: "Пшено, кг", historical: true, y2026: true},
	{filterID: "1846524", code: "8019", name: "Ренгалин, 10 таблеток", historical: true, y2026: false},
	{filterID: "1848407", code: "8031", name: "Ренгалин, 20 таблеток", historical: false, y2026: true},
	{filterID: "1755064", code: "7978", name: "Ренни, 12 таблеток", historical: true, y2026: false},
	{filterID: "1856406", code: "8053", name: "Римантадин, 10 таблеток", historical: false, y2026: true},
	{filterID: "1755068", code: "2301", name: "Рис шлифованный, кг", historical: true, y2026: true},
	{filterID: "1743472", code: "411", name: "Рыба мороженая неразделанная, кг", historical: true, y2026: true},
	{filterID: "1755088", code: "9606", name: "Санаторий, день", historical: true, y2026: true},
	{filterID: "1709795", code: "1601", name: "Сахар-песок, кг", historical: true, y2026: true},
	{filterID: "1709753", code: "113", name: "Свинина (кроме бескостного мяса), кг", historical: true, y2026: true},
	{filterID: "1754741", code: "2604", name: "Свёкла столовая, кг", historical: true, y2026: true},
	{filterID: "1755195", code: "5602", name: "Сигареты с фильтром отечественные, пачка", historical: true, y2026: false},
	{filterID: "1846523", code: "5605", name: "Сигареты с фильтром, пачка", historical: true, y2026: true},
	{filterID: "1755162", code: "7104", name: "Смартфон, шт.", historical: true, y2026: true},
	{filterID: "1755203", code: "1123", name: "Смеси сухие молочные для детского питания, кг", historical: true, y2026: true},
	{filterID: "1709793", code: "1102", name: "Сметана, кг", historical: true, y2026: true},
	{filterID: "1755092", code: "2002", name: "Соль поваренная пищевая, кг", historical: true, y2026: true},
	{filterID: "1754725", code: "4161", name: "Сорочка верхняя для мальчиков школьного возраста, шт.", historical: true, y2026: false},
	{filterID: "1743458", code: "202", name: "Сосиски, сардельки, кг", historical: true, y2026: true},
	{filterID: "1755213", code: "5701", name: "Спички, коробок", historical: true, y2026: true},
	{filterID: "1754921", code: "9116", name: "Стрижка модельная в женском зале, стрижка", historical: false, y2026: true},
	{filterID: "1754960", code: "9118", name: "Стрижка модельная в мужском зале, стрижка", historical: false, y2026: true},
	{filterID: "1754932", code: "8311", name: "Сухие корма для домашних животных, кг", historical: true, y2026: true},
	{filterID: "1709735", code: "1201", name: "Сыры сычужные твердые и мягкие, кг", historical: true, y2026: false},
	{filterID: "1848409", code: "1204", name: "Сыры твердые, полутвердые и мягкие, кг", historical: false, y2026: true},
	{filterID: "1709714", code: "1104", name: "Творог жирный, кг", historical: true, y2026: false},
	{filterID: "1846520", code: "1124", name: "Творог, кг", historical: true, y2026: true},
	{filterID: "1743480", code: "7007", name: "Телевизор цветного изображения, шт.", historical: true, y2026: true},
	{filterID: "1755101", code: "7967", name: "Троксерутин, 2% гель, 40 г", historical: true, y2026: false},
	{filterID: "1744148", code: "9475", name: "Услуги по снабжению электроэнергией", historical: true, y2026: true},
	{filterID: "1754910", code: "7968", name: "Флуоцинолона ацетонид, 0,025% мазь, 15 г", historical: true, y2026: false},
	{filterID: "1709743", code: "4503", name: "Футболка детская, шт.", historical: true, y2026: true},
	{filterID: "1709746", code: "2203", name: "Хлеб и булочные изделия из пшеничной муки 1 и 2 сортов, кг", historical: true, y2026: false},
	{filterID: "1846521", code: "2207", name: "Хлеб и булочные изделия из пшеничной муки различных сортов, кг", historical: true, y2026: true},
	{filterID: "1709733", code: "2201", name: "Хлеб из ржаной муки и из смеси муки ржаной и пшеничной, кг", historical: true, y2026: true},
	{filterID: "1755110", code: "1903", name: "Чай черный байховый, кг", historical: true, y2026: true},
	{filterID: "1754714", code: "5303", name: "Шампунь, 250 мл", historical: true, y2026: true},
	{filterID: "1754889", code: "5406", name: "Щетка зубная, шт.", historical: true, y2026: true},
	{filterID: "1754925", code: "6413", name: "Электропылесос напольный, шт.", historical: true, y2026: true},
	{filterID: "1846525", code: "8020", name: "Эргоферон, 20 таблеток", historical: true, y2026: false},
	{filterID: "1743471", code: "2701", name: "Яблоки, кг", historical: true, y2026: true},
	{filterID: "1709749", code: "1501", name: "Яйца куриные, 10 шт.", historical: true, y2026: true},
}

// fedstatProductOrder — порядок filterID в POST-форме, как у фильтра 58273 на
// странице источника (не алфавитный: Говядина, Свинина, Куры, …). Сервер
// ЕМИСС разбирает selectedFilterIds позиционно; при алфавитном порядке товаров
// он отвечает пустым GenericData без Series — проверено live 2026-10-20.
// Каталог и разбор от этого порядка не зависят: там fedstatProducts по имени.
var fedstatProductOrder = []string{
	"1709756", "1709753", "1709748", "1755103", "1743476", "1743458", "1709791", "1785708",
	"1755075", "1755178", "1743472", "1709734", "1709737", "1755044", "1709793", "1709714",
	"1759580", "1709745", "1755039", "1759578", "1759587", "1755203", "1709735", "1754700",
	"1754781", "1709749", "1709795", "1755149", "1755158", "1743465", "1755110", "1755092",
	"1755145", "1709733", "1709746", "1755068", "1755036", "1755104", "1755172", "1709722",
	"1743470", "1743463", "1755134", "1754741", "1743475", "1743457", "1755033", "1743471",
	"1754800", "1709799", "1709752", "1754777", "1754798", "1754725", "1754703", "1754794",
	"1743456", "1709743", "1755206", "1743468", "1754938", "1754891", "1743464", "1755051",
	"1755152", "1755080", "1754714", "1755032", "1754889", "1755195", "1755213", "1754925",
	"1754919", "1754980", "1755026", "1743480", "1755162", "1754905", "1754936", "1755000",
	"1755166", "1755117", "1785651", "1755210", "1709801", "1709730", "1709750", "1755196",
	"1755184", "1754970", "1754988", "1709725", "1754964", "1754906", "1754956", "1755071",
	"1755101", "1754910", "1785685", "1754983", "1755064", "1755167", "1755135", "1755171",
	"1754957", "1755028", "1755074", "1754932", "1754921", "1754960", "1755201", "1755205",
	"1755193", "1755209", "1755200", "1785650", "1755161", "1755146", "1755198", "1759589",
	"1785748", "1759576", "1755208", "1755197", "1755194", "1755212", "1755070", "1744148",
	"1785693", "1755142", "1755053", "1755088", "1755045", "1848408", "1853997", "1856405",
	"1846522", "1846527", "1853986", "1846558", "1853987", "1848518", "1846846", "1846526",
	"1836582", "1836583", "1836584", "1836585", "1836586", "1836581", "1846524", "1848407",
	"1856406", "1846523", "1848409", "1846520", "1846521", "1846525",
}

// fedstatProductByCode — индекс разбора: s_grtov → товар. Строится из того же
// среза, поэтому забытая запись в одном месте невозможна; дубль кода — паника
// при инициализации пакета, а не тихая подмена ряда.
var fedstatProductByCode = func() map[string]fedstatProduct {
	index := make(map[string]fedstatProduct, len(fedstatProducts))
	for _, product := range fedstatProducts {
		if _, ok := index[product.code]; ok {
			panic("fedstat: duplicate s_grtov code " + product.code)
		}
		index[product.code] = product
	}
	return index
}()
