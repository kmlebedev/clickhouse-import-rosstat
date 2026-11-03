#!/bin/sh
# Запускальщик mcp-grafana: mcp-grafana читает сервис-токен Grafana из
# GRAFANA_SERVICE_ACCOUNT_TOKEN, а в env-файле он лежит как GRAFANA_MCP_TOKEN.
# Это тот же токен, которым деплоятся дашборды (gcx-дашборды не видны Viewer/Editor,
# 403 на Grafana 13); MCP остаётся read-only благодаря --disable-write.
set -eu

: "${GRAFANA_URL:?GRAFANA_URL не задан}"
: "${GRAFANA_MCP_TOKEN:?GRAFANA_MCP_TOKEN не задан в /etc/clickhouse-import-rosstat.env}"
: "${GRAFANA_MCP_VERSION:?GRAFANA_MCP_VERSION не задан}"
: "${GRAFANA_MCP_ADDRESS:?GRAFANA_MCP_ADDRESS не задан}"

GRAFANA_SERVICE_ACCOUNT_TOKEN="$GRAFANA_MCP_TOKEN"
export GRAFANA_SERVICE_ACCOUNT_TOKEN

exec /root/.local/bin/uv tool run --python 3.12 \
  --from "mcp-grafana==${GRAFANA_MCP_VERSION}" \
  mcp-grafana \
  --transport streamable-http \
  --address "$GRAFANA_MCP_ADDRESS" \
  --allowed-hosts "${GRAFANA_MCP_ALLOWED_HOSTS:-127.0.0.1:8000,localhost:8000,[::1]:8000,127.0.0.1:8001,localhost:8001,[::1]:8001}" \
  --disable-write \
  --usage-stats=disabled \
  --log-level info
