package main

import (
	"encoding/json"
	"errors"

	"github.com/grafana/grafana-foundation-sdk/go/cog"
	"github.com/grafana/grafana-foundation-sdk/go/cog/variants"
)

const (
	// DatasourceType — ID плагина ClickHouse, DatasourceUID — UID датасорса стенда.
	// UID меняется только вместе с провижинингом (grafana/provisioning, deploy/staging).
	DatasourceType = "grafana-clickhouse-datasource"
	DatasourceUID  = "ch-rosstat"

	// DashboardUID — UID дашборда §4 золотого роадмапа; попадает и в spec, и в metadata.name.
	DashboardUID = "gold-thesis"
	// ApiVersion — apiVersion K8s-стиля ресурса, который ждёт gcx resources push.
	ApiVersion = "dashboard.grafana.app/v1beta1"

	// editorTypeQuery всегда "sql": панели пишутся руками, конструктор запросов плагина не используется.
	editorTypeQuery = "sql"
	// pluginVersionQuery — версия плагина, чей формат запроса подтверждён живым экспортом.
	pluginVersionQuery = "4.3.2"
)

// CHQuery — dataquery-вариант плагина grafana-clickhouse-datasource в форме живого экспорта 4.3.2.
// Поля размечены тегами плагина: refId/hide/rawSql/editorType/format/queryType/pluginVersion.
type CHQuery struct {
	// RefId и Hide есть у любого запроса Grafana.
	RefId string `json:"refId"`
	Hide  *bool  `json:"hide,omitempty"`

	RawSql     string `json:"rawSql"`
	EditorType string `json:"editorType"`
	// Format без omitempty: плагин всегда пишет ноль (timeseries-формат ответа).
	Format        int    `json:"format"`
	QueryType     string `json:"queryType"`
	PluginVersion string `json:"pluginVersion"`
}

func (resource CHQuery) Equals(otherCandidate variants.Dataquery) bool {
	other, ok := otherCandidate.(CHQuery)
	if !ok {
		return false
	}

	if resource.RefId != other.RefId || resource.RawSql != other.RawSql ||
		resource.EditorType != other.EditorType || resource.Format != other.Format ||
		resource.QueryType != other.QueryType || resource.PluginVersion != other.PluginVersion {
		return false
	}

	return ptrEqual(resource.Hide, other.Hide)
}

func (resource CHQuery) Validate() error {
	if resource.RefId == "" {
		return errors.New("у запроса ClickHouse пустой refId")
	}
	if resource.RawSql == "" {
		return errors.New("у запроса ClickHouse пустой rawSql")
	}
	return nil
}

// ImplementsDataqueryVariant сообщает cog, что CHQuery — вариант dataquery.
func (resource CHQuery) ImplementsDataqueryVariant() {}

// DataqueryType — ID плагина: по нему cog выбирает вариант при разборе JSON.
func (resource CHQuery) DataqueryType() string {
	return DatasourceType
}

// CHQueryVariant описывает вариант dataquery для cog: идентификатор и разборщик из JSON плагина.
func CHQueryVariant() variants.DataqueryConfig {
	return variants.DataqueryConfig{
		Identifier: DatasourceType,
		DataqueryUnmarshaler: func(raw []byte) (variants.Dataquery, error) {
			dataquery := &CHQuery{}
			if err := json.Unmarshal(raw, dataquery); err != nil {
				return nil, err
			}
			return dataquery, nil
		},
	}
}

// Проверка на этапе компиляции: CHQueryBuilder — билдер для variants.Dataquery.
var _ cog.Builder[variants.Dataquery] = (*CHQueryBuilder)(nil)

// CHQueryBuilder собирает запрос к ClickHouse для панели дашборда.
type CHQueryBuilder struct {
	internal *CHQuery
}

// NewCHQuery создаёт запрос с постоянными для плагина editorType и pluginVersion.
func NewCHQuery(refID, rawSQL string) *CHQueryBuilder {
	return &CHQueryBuilder{internal: &CHQuery{
		RefId:         refID,
		RawSql:        rawSQL,
		EditorType:    editorTypeQuery,
		PluginVersion: pluginVersionQuery,
	}}
}

// Format задаёт формат ответа плагина (0 — timeseries).
func (builder *CHQueryBuilder) Format(format int) *CHQueryBuilder {
	builder.internal.Format = format
	return builder
}

// QueryType задаёт тип запроса плагина (например, "timeseries" или "table").
func (builder *CHQueryBuilder) QueryType(queryType string) *CHQueryBuilder {
	builder.internal.QueryType = queryType
	return builder
}

func (builder *CHQueryBuilder) Build() (variants.Dataquery, error) {
	if err := builder.internal.Validate(); err != nil {
		return nil, err
	}
	return *builder.internal, nil
}

func ptrEqual[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
