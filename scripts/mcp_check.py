import asyncio
import os
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

ENV_KEYS = [
    "CLICKHOUSE_HOST",
    "CLICKHOUSE_PORT",
    "CLICKHOUSE_SECURE",
    "CLICKHOUSE_VERIFY",
    "CLICKHOUSE_USER",
    "CLICKHOUSE_PASSWORD",
    "CLICKHOUSE_DATABASE",
]


def text(result):
    return "\n".join(c.text for c in result.content if hasattr(c, "text"))


def expect(label, ok, detail):
    print(f"{'OK  ' if ok else 'FAIL'} {label}: {detail}")
    return ok


async def main():
    env = {k: os.environ[k] for k in ENV_KEYS if k in os.environ}
    env["CLICKHOUSE_MCP_SERVER_TRANSPORT"] = "stdio"
    if "CLICKHOUSE_PASSWORD" not in env:
        print("CLICKHOUSE_PASSWORD не задан в окружении", file=sys.stderr)
        return 2
    params = StdioServerParameters(
        command="uv",
        args=["run", "--with", "mcp-clickhouse", "--python", "3.12", "mcp-clickhouse"],
        env={**os.environ, **env},
    )
    failed = 0
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = sorted(t.name for t in (await session.list_tools()).tools)
            failed += not expect("инструменты", {"list_databases", "list_tables", "run_query"} <= set(tools), ", ".join(tools))

            tables = text(await session.call_tool("list_tables", {"database": "default"}))
            failed += not expect("витрина v_bea_pce видна", "v_bea_pce" in tables, "list_tables default")

            catalog = text(await session.call_tool("run_query", {"query": "SELECT series, unit FROM v_series_catalog WHERE source = 'bea' ORDER BY series"}))
            failed += not expect("каталог BEA", "PCE_PI" in catalog and "PCE_PI_CORE" in catalog, catalog[:200])

            pce = text(await session.call_tool("run_query", {"query": "SELECT date, value FROM v_bea_pce WHERE series = 'PCE_PI' ORDER BY date DESC LIMIT 3"}))
            failed += not expect("значения PCE_PI", "rows" in pce and "Query execution failed" not in pce, pce[:200])

            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM macro_series"}))
            failed += not expect("сырая таблица закрыта", "ACCESS_DENIED" in raw or "Not enough privileges" in raw, raw[:200])

    print("ПРОВЕРКА ПРОЙДЕНА" if not failed else f"ПРОВАЛЕНО: {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
