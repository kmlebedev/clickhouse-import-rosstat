"""Smoke-проверка Grafana MCP (`make grafana-mcp-check`).

Два режима:
  * stdio (по умолчанию) — поднимает `mcp-grafana --disable-write` через uv,
    как это делает `.kimi-code/mcp.json`;
  * streamable HTTP (`--url`/`GRAFANA_MCP_URL`) — подключается к loopback-сервису
    на staging через ssh-туннель; токен уже подставлен сервисом.

Проверки: набор read-only инструментов, отсутствие write-инструмента
`update_dashboard`, доступ к Grafana (user_info), датасорс `ch-rosstat`,
поиск дашборда и сводка по uid. Значения токена нигде не печатаются.

Вход: env `GRAFANA_URL`, `GRAFANA_MCP_TOKEN` (stdio; Admin-токен — локально обычно
`GRAFANA_TEST_TOKEN`, если его аккаунт Admin), `GRAFANA_MCP_URL` (HTTP), `GRAFANA_MCP_VERSION`,
`GRAFANA_MCP_CHECK_UID`; аргументы те же.
Выход: 0 — все проверки прошли, 1 — хотя бы одна упала, 2 — не задано окружение.
Печать — строки `OK ...`/`FAIL ...` в стиле scripts/mcp_check.py.
"""

import argparse
import asyncio
import os
import sys
import tempfile

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


def text(result):
    return "\n".join(c.text for c in result.content if hasattr(c, "text"))


def expect(label, ok, detail):
    print(f"{'OK  ' if ok else 'FAIL'} {label}: {detail}")
    return ok


async def call(session, name, arguments):
    """Вызов инструмента: ошибка MCP возвращается текстом, а не роняет проверку."""
    try:
        result = await session.call_tool(name, arguments)
        body = text(result)
        if getattr(result, "isError", False) or getattr(result, "is_error", False):
            return f"ERROR: {body}"
        return body
    except Exception as exc:  # noqa: BLE001 — smoke должен показать любую ошибку
        return f"ERROR: {type(exc).__name__}: {exc}"


async def check(session, uid):
    failed = 0

    tools = sorted(t.name for t in (await session.list_tools()).tools)
    required = {"search_dashboards", "get_dashboard_summary", "list_datasources", "user_info"}
    failed += not expect("инструменты", required <= set(tools), ", ".join(tools))
    # --disable-write обязан убрать write-инструменты: иначе read-only контур не доказан.
    failed += not expect("write-инструменты отключены", "update_dashboard" not in tools,
                         "update_dashboard отсутствует" if "update_dashboard" not in tools
                         else "update_dashboard всё ещё зарегистрирован")

    info = await call(session, "user_info", {})
    failed += not expect("Grafana доступна", bool(info.strip()) and "ERROR:" not in info,
                         info[:200] if info else "пустой ответ user_info")

    datasources = await call(session, "list_datasources", {})
    failed += not expect("датасорс ch-rosstat", "ch-rosstat" in datasources,
                         datasources[:200])

    found = await call(session, "search_dashboards", {"tag": ["gold"], "limit": 10})
    found_detail = found[:200]
    if "gold-thesis" not in found and "403" not in found and "Forbidden" not in found:
        found_detail += (" — пусто: проверьте, что токен Admin и дашборд задеплоен "
                         "(gcx-дашборды не видны Viewer/Editor на Grafana 13)")
    failed += not expect("дашборд gold-thesis виден", "gold-thesis" in found, found_detail)

    summary = await call(session, "get_dashboard_summary", {"uid": uid})
    summary_detail = summary[:200]
    summary_ok = ("ERROR:" not in summary and "403" not in summary and "Forbidden" not in summary
                  and (uid in summary or "Золото" in summary))
    if "403" in summary or "Forbidden" in summary:
        summary_detail += (" — токен не Admin: gcx-дашборды на Grafana 13 видны только Admin; "
                           "задайте Admin-токен в GRAFANA_MCP_TOKEN")
    failed += not expect(f"сводка {uid}", summary_ok, summary_detail)

    return failed


async def run_stdio(args):
    token = os.environ.get("GRAFANA_MCP_TOKEN", "")
    if not token:
        print("GRAFANA_MCP_TOKEN не задан в окружении", file=sys.stderr)
        return 2

    env = {
        **os.environ,
        "GRAFANA_URL": args.grafana_url,
        "GRAFANA_SERVICE_ACCOUNT_TOKEN": token,
    }
    params = StdioServerParameters(
        command="uv",
        args=[
            "tool", "run", "--python", "3.12",
            "--from", f"mcp-grafana=={args.version}",
            "mcp-grafana", "--disable-write", "--usage-stats=disabled",
        ],
        env=env,
    )
    with tempfile.TemporaryFile(mode="w+") as server_log:
        async with stdio_client(params, errlog=server_log) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                failed = await check(session, args.uid)
        if failed:
            server_log.seek(0)
            print("--- лог сервера mcp-grafana ---", file=sys.stderr)
            print(server_log.read(), file=sys.stderr)
    return failed


async def run_http(args):
    from mcp.client.streamable_http import streamable_http_client

    async with streamable_http_client(args.url) as streams:
        read, write = streams[0], streams[1]
        async with ClientSession(read, write) as session:
            await session.initialize()
            return await check(session, args.uid)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=os.environ.get("GRAFANA_MCP_URL", ""),
                        help="streamable HTTP URL; пусто — stdio")
    parser.add_argument("--grafana-url", default=os.environ.get("GRAFANA_URL", "http://localhost:3000"))
    parser.add_argument("--uid", default=os.environ.get("GRAFANA_MCP_CHECK_UID", "gold-thesis"))
    parser.add_argument("--version", default=os.environ.get("GRAFANA_MCP_VERSION", "2.2.0"))
    args = parser.parse_args()

    try:
        failed = asyncio.run(run_http(args) if args.url else run_stdio(args))
    except Exception as exc:  # noqa: BLE001 — транспортная ошибка тоже провал smoke
        print(f"FAIL транспорт: {type(exc).__name__}: {exc}", file=sys.stderr)
        failed = 1
    if failed == 2:
        return 2
    print("ПРОВЕРКА ПРОЙДЕНА" if not failed else f"ПРОВАЛЕНО: {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
