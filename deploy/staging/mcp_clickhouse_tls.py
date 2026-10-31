"""Запуск mcp-clickhouse по HTTPS.

В mcp-clickhouse 0.7.0 нет переменной окружения для серверного TLS: main() пакета
вызывает mcp.run(transport, host, port) без параметров uvicorn. Этот скрипт
повторяет main() и передаёт uvicorn ssl_certfile/ssl_keyfile через uvicorn_config
FastMCP. Остальная конфигурация (бинд, токен, allowed hosts, ClickHouse) — те же
переменные CLICKHOUSE_*, что у пакета.

Без MCP_TLS_CERTFILE/MCP_TLS_KEYFILE скрипт падает: тихий откат в plain HTTP
отдал бы Bearer-токен в открытом виде.
"""

import os
import sys

from mcp_clickhouse.mcp_env import TransportType, get_mcp_config
from mcp_clickhouse.mcp_middleware_hook import setup_middleware
from mcp_clickhouse.mcp_server import mcp


def main():
    certfile = os.environ.get("MCP_TLS_CERTFILE", "")
    keyfile = os.environ.get("MCP_TLS_KEYFILE", "")
    for name, path in (("MCP_TLS_CERTFILE", certfile), ("MCP_TLS_KEYFILE", keyfile)):
        if not path or not os.path.isfile(path):
            sys.exit(f"{name} не задан или файл не найден: {path!r}")

    config = get_mcp_config()
    if config.server_transport != TransportType.HTTP.value:
        sys.exit("CLICKHOUSE_MCP_SERVER_TRANSPORT должен быть http")

    setup_middleware(mcp)
    mcp.run(
        transport=config.server_transport,
        host=config.bind_host,
        port=config.bind_port,
        uvicorn_config={"ssl_certfile": certfile, "ssl_keyfile": keyfile},
    )


if __name__ == "__main__":
    main()
