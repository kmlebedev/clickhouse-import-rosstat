package polyus

// ifrsMetrics описывает статьи консолидированной отчётности МСФО (страницы 6–7
// английского релиза otchetnost-6m2026_eng.pdf). Английская версия отчитывается
// в долларах США — та же система единиц, что и у датапака, тогда как русская
// версия печатает рубли. Поэтому разбирается английская.
//
// Метки статей в этой таблице стоят в КОНЦЕ строки («значения, затем метка»), а
// не в начале, как на KPI-страницах. Поэтому Prefix здесь — хвостовой фрагмент
// строки, а не её начало: сопоставление идёт через strings.HasSuffix и общий
// разбор по самому длинному совпавшему префиксу.
//
// Метки сверены с живой фикстурой посимвольно. В частности:
//   - у маркеров списка ровно четыре пробела после дефиса («-    basic»);
//   - операционные расходы печатаются как «Operating expenses and other income
//     / (expenses)», поэтому префикс обрезан до «Operating expenses and other
//     income» — он уникален и не задевает другие статьи;
//   - «Total assets» (p.7) существует, значение 17,818.
var ifrsMetrics = []MetricDefinition{
	{Name: "gold_sales", Unit: "USD million", Prefix: []string{"Gold sales"}},
	{Name: "other_sales", Unit: "USD million", Prefix: []string{"Other sales"}},
	{Name: "total_revenue", Unit: "USD million", Prefix: []string{"Total revenue"}},
	{Name: "operating_expenses", Unit: "USD million", Prefix: []string{"Operating expenses and other income"}},
	{Name: "profit_before_tax", Unit: "USD million", Prefix: []string{"Profit before income tax"}},
	{Name: "income_tax_expense", Unit: "USD million", Prefix: []string{"Income tax expense"}},
	{Name: "profit_for_period", Unit: "USD million", Prefix: []string{"Profit for the period"}},
	{Name: "eps_basic", Unit: "USD/share", Prefix: []string{"-    basic"}},
	{Name: "eps_diluted", Unit: "USD/share", Prefix: []string{"-    diluted"}},
	{Name: "total_assets", Unit: "USD million", Prefix: []string{"Total assets"}},
}
