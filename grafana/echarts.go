package main

import (
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
)

// EChartsPanelType — ID плагина Business Charts (Volkov Labs; бывший Apache ECharts Panel).
// Типизированного билдера в grafana-foundation-sdk нет: панель собирается generic PanelBuilder
// (пункт infra/5.14, ARCHITECTURE §6.7; версия плагина — в Makefile, автоустановка в grafana-up).
const EChartsPanelType = "volkovlabs-echarts-panel"

// EChartsPluginVersion — версия плагина на стендах; формат options ниже подтверждён живым
// экспортом панели с dev-стенда на плагине 7.2.5 (конвенция Makefile — формат по живому экспорту).
const EChartsPluginVersion = "7.2.5"

// echartsPanelOptions — опции панели Business Charts (формат плагина 7.2.5): editorMode "code" —
// режим кода; getOption — тело JS-функции (context → option Apache ECharts); renderer "canvas".
type echartsPanelOptions struct {
	EditorMode string `json:"editorMode"`
	GetOption  string `json:"getOption"`
	Renderer   string `json:"renderer"`
}

// echartsPanel — custom-панель Apache ECharts: данные приходят из таргетов ClickHouse (только
// витрины v_*, фильтр — в SQL), отрисовка — getOption из data frames; литеральных данных
// в JS нет, timezone панель не конвертирует (Timezone(browser), практика §6.7).
func echartsPanel(id uint32, title, description, getOption string, targets ...*CHQueryBuilder) *dashboard.PanelBuilder {
	builder := dashboard.NewPanelBuilder().
		Type(EChartsPanelType).
		Id(id).
		PluginVersion(EChartsPluginVersion).
		Title(title).
		Description(description).
		Datasource(datasourceRef()).
		Options(echartsPanelOptions{EditorMode: "code", GetOption: getOption, Renderer: "canvas"})
	for _, target := range targets {
		builder = builder.WithTarget(target)
	}
	return builder
}
