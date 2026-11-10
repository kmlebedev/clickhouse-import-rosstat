package main

// Общий код дашбордов пакета: ссылки на датасорс, конструкторы запросов, манифест и базовые
// настройки панелей. Хелпер, нужный одному дашборду, живёт в его файле под префиксом дашборда.
// Хелпер, нужный двум и более, переносится сюда без префикса.

import (
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/resource"
	"github.com/grafana/grafana-foundation-sdk/go/stat"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

// Панели читают только витрины v_*: у датасорса стенда нет грантов на сырые таблицы.
// Форматы запросов подтверждены живым экспортом плагина 4.3.2 (grafana/clickhouse.go):
// 0 + timeseries — временные ряды, 1 + table — категориальные бары и таблицы.
const (
	queryFormatTimeseries = 0
	queryFormatTable      = 1
	queryTypeTimeseries   = "timeseries"
	queryTypeTable        = "table"
)

// Ширина панелей в колонках сетки Grafana (всего 24).
const (
	spanThird = 8
	spanHalf  = 12
	spanFull  = 24
)

// ptr возвращает указатель на значение: необязательные поля builders SDK — указатели.
func ptr[T any](v T) *T {
	return &v
}

// datasourceRef — ссылка на датасорс стенда: у всех панелей только эти константы (проверяет Validate).
func datasourceRef() common.DataSourceRef {
	return common.DataSourceRef{Type: ptr(DatasourceType), Uid: ptr(DatasourceUID)}
}

// timeseriesQuery — запрос временного ряда (формат 0).
func timeseriesQuery(refID, sql string) *CHQueryBuilder {
	return NewCHQuery(refID, sql).Format(queryFormatTimeseries).QueryType(queryTypeTimeseries)
}

// tableQuery — категориальный/табличный запрос (формат 1).
func tableQuery(refID, sql string) *CHQueryBuilder {
	return NewCHQuery(refID, sql).Format(queryFormatTable).QueryType(queryTypeTable)
}

// Manifest оборачивает дашборд в K8s-стиль ресурс, который принимает gcx resources push.
// metadata.name и spec.uid равны (инвариант): имя берётся из uid дашборда, а непустой uid
// гарантирует Validate — main вызывает её до Manifest.
func Manifest(d dashboard.Dashboard) resource.Manifest {
	return resource.Manifest{
		ApiVersion: ApiVersion,
		Kind:       resource.DashboardKind,
		Metadata:   resource.Metadata{Name: *d.Uid},
		Spec:       d,
	}
}

// timeseriesDefaults — общая база графиков: датасорс стенда, классическая палитра, легенда-таблица
// снизу с заданными расчётами, одиночный тултип, линия 1 px без заливки и градиента, пропуски
// не соединяются. Интерполяцию, размеры и единицы добавляет обёртка дашборда.
func timeseriesDefaults(builder *timeseries.PanelBuilder, legendCalcs ...string) *timeseries.PanelBuilder {
	return builder.
		Datasource(datasourceRef()).
		ColorScheme(dashboard.NewFieldColorBuilder().Mode(dashboard.FieldColorModeIdPaletteClassic)).
		Legend(common.NewVizLegendOptionsBuilder().
			ShowLegend(true).
			Placement(common.LegendPlacementBottom).
			DisplayMode(common.LegendDisplayModeTable).
			Calcs(legendCalcs)).
		Tooltip(common.NewVizTooltipOptionsBuilder().
			Mode(common.TooltipDisplayModeSingle).
			Sort(common.SortOrderNone)).
		LineWidth(1).
		FillOpacity(0).
		GradientMode(common.GraphGradientModeNone).
		SpanNulls(common.BoolOrFloat64{Bool: ptr(false)})
}

// statDefaults — общая база stat-снимков: датасорс стенда, последнее непустое значение, авто-текст.
func statDefaults(builder *stat.PanelBuilder) *stat.PanelBuilder {
	return builder.
		Datasource(datasourceRef()).
		ReduceOptions(common.NewReduceDataOptionsBuilder().Calcs([]string{"lastNotNull"}).Values(false)).
		TextMode(common.BigValueTextModeAuto)
}

// fixedColorProp — override фиксированного цвета серии.
func fixedColorProp(color string) dashboard.DynamicConfigValue {
	return dashboard.DynamicConfigValue{
		Id:    "color",
		Value: dashboard.FieldColor{Mode: dashboard.FieldColorModeIdFixed, FixedColor: ptr(color)},
	}
}

// pointsOnlyProps — серия рисуется только точками: по одной-двум точкам сплошная линия не читается.
func pointsOnlyProps() []dashboard.DynamicConfigValue {
	return []dashboard.DynamicConfigValue{
		{Id: "custom.lineWidth", Value: 0},
		{Id: "custom.showPoints", Value: "always"},
	}
}

// axisPlacementRightProp — серия на правой оси: у неё другая размерность (индекс, % или $/барр.).
func axisPlacementRightProp() dashboard.DynamicConfigValue {
	return dashboard.DynamicConfigValue{Id: "custom.axisPlacement", Value: "right"}
}

// thresholdLineStyle — пороги рисуются линией поверх графика.
func thresholdLineStyle() *common.GraphThresholdsStyleConfigBuilder {
	return common.NewGraphThresholdsStyleConfigBuilder().Mode(common.GraphThresholdsStyleModeLine)
}

// zeroThresholds — двухцветный порог по нулю: belowColor ниже нуля, зелёный от нуля и выше.
func zeroThresholds(belowColor string) *dashboard.ThresholdsConfigBuilder {
	return dashboard.NewThresholdsConfigBuilder().
		Mode(dashboard.ThresholdsModeAbsolute).
		Steps([]dashboard.Threshold{
			{Value: nil, Color: belowColor},
			{Value: ptr(0.0), Color: "green"},
		})
}
