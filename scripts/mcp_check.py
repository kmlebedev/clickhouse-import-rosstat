import asyncio
import json
import os
import sys
import tempfile

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


def result_rows(result_text):
    """Строки run_query как словари по столбцам ответа MCP ({"columns": [...], "rows": [...]}).

    None — ответ не распознан (ошибка запроса или неожиданный формат); пустой список — запрос
    прошёл, но строк нет.
    """
    try:
        payload = json.loads(result_text)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    columns, rows = payload.get("columns"), payload.get("rows")
    if not isinstance(columns, list) or not isinstance(rows, list):
        return None
    return [dict(zip(columns, row)) for row in rows]


MCP_VERSION = os.environ.get("MCP_VERSION", "0.7.0")


async def main():
    env = {k: os.environ[k] for k in ENV_KEYS if k in os.environ}
    env["CLICKHOUSE_MCP_SERVER_TRANSPORT"] = "stdio"
    if "CLICKHOUSE_PASSWORD" not in env:
        print("CLICKHOUSE_PASSWORD не задан в окружении", file=sys.stderr)
        return 2
    params = StdioServerParameters(
        command="uv",
        args=["run", "--with", f"mcp-clickhouse=={MCP_VERSION}", "--python", "3.12", "mcp-clickhouse"],
        env={**os.environ, **env},
    )
    failed = 0
    with tempfile.TemporaryFile(mode="w+") as server_log:
        failed = await check(params, server_log)
        if failed:
            server_log.seek(0)
            print("--- лог сервера mcp-clickhouse ---", file=sys.stderr)
            print(server_log.read(), file=sys.stderr)
    print("ПРОВЕРКА ПРОЙДЕНА" if not failed else f"ПРОВАЛЕНО: {failed}")
    return 1 if failed else 0


async def check(params, server_log):
    failed = 0
    async with stdio_client(params, errlog=server_log) as (read, write):
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

            for view in ("v_company_financials", "v_company_metric_sources", "v_company_operating"):
                failed += not expect(f"витрина {view} видна", view in tables, "list_tables default")

            resolved = text(await session.call_tool("run_query", {"query": "SELECT metric, period, value, source_kind, source_url FROM v_company_financials WHERE metric = 'gold_output' ORDER BY period"}))
            failed += not expect("метрики Полюса читаются", "rows" in resolved and "Query execution failed" not in resolved, resolved[:200])

            # Контур DCF: агент видит витрину v_dcf_assumptions и читает её, а сырая
            # nav_by_asset остаётся закрытой (правило 11 AGENTS.md). Строк в
            # nav_by_asset может не быть вовсе — план mine_plans ещё не засеян
            # (отдельный пункт роадмапа §7), поэтому проверяется РАЗРЕШЕНИЕ запроса,
            # а не число строк: count() == 0 на читаемой витрине — это успех, а не отказ.
            failed += not expect("витрина v_dcf_assumptions видна", "v_dcf_assumptions" in tables, "list_tables default")

            dcf = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_dcf_assumptions"}))
            failed += not expect("NPV по активам читается", "rows" in dcf and "Query execution failed" not in dcf and "ACCESS_DENIED" not in dcf, dcf[:200])

            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM nav_by_asset"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырая nav_by_asset закрыта", denied, "отказ ClickHouse на nav_by_asset (ожидаемо)" if denied else raw[:200])

            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM polyus_financial_metrics"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырые метрики Полюса закрыты", denied, "отказ ClickHouse на polyus_financial_metrics (ожидаемо)" if denied else raw[:200])

            fred_bls = text(await session.call_tool("run_query", {"query": "SELECT series FROM v_series_catalog WHERE source IN ('fred', 'bls')"}))
            expected = ["DFII10", "DGS10", "FEDFUNDS", "DTWEXBGS", "CPIAUCSL", "T5YIE",
                        "CUUR0000SA0", "CUSR0000SA0", "LNS14000000", "CES0000000001", "CES0500000003", "WPSFD4", "JTS000000000000000JOL"]
            missing = [s for s in expected if s not in fred_bls]
            failed += not expect("каталог FRED и BLS", not missing, f"нет рядов: {missing}" if missing else f"{len(expected)} рядов")

            fred = text(await session.call_tool("run_query", {"query": "SELECT date, value FROM v_fred_macro WHERE series = 'DGS10' ORDER BY date DESC LIMIT 3"}))
            failed += not expect("значения FRED DGS10", "rows" in fred and "Query execution failed" not in fred, fred[:200])

            bls = text(await session.call_tool("run_query", {"query": "SELECT date, value FROM v_bls_macro WHERE series = 'LNS14000000' ORDER BY date DESC LIMIT 3"}))
            failed += not expect("значения BLS LNS14000000", "rows" in bls and "Query execution failed" not in bls, bls[:200])

            # EIA-контур нефтяного канала: витрина v_eia_petroleum, каталог обоих рядов
            # (distillate_stocks, refinery_utilization) и обновление distillate_stocks в дашборде.
            failed += not expect("витрина v_eia_petroleum видна", "v_eia_petroleum" in tables, "list_tables default")

            eia_catalog = text(await session.call_tool("run_query", {"query": "SELECT series, unit FROM v_series_catalog WHERE source = 'eia' ORDER BY series"}))
            failed += not expect("каталог EIA", "distillate_stocks" in eia_catalog and "refinery_utilization" in eia_catalog, eia_catalog[:200])

            eia = text(await session.call_tool("run_query", {"query": "SELECT date, value FROM v_eia_petroleum WHERE series = 'distillate_stocks' ORDER BY date DESC LIMIT 3"}))
            failed += not expect("значения EIA distillate_stocks", "rows" in eia and "Query execution failed" not in eia, eia[:200])

            # Дашборд должен отдавать distillate_stocks из автоматического импорта EIA, а не из
            # ручного фолбэка: stale = 0 означает свежее наблюдение. Пустой ответ — провал
            # (импортёр ещё не прогонялся: make import STAT=eia), ошибка запроса даёт detail сырым текстом.
            dashboard = text(await session.call_tool("run_query", {"query": "SELECT indicator, value, as_of, stale FROM v_gold_dashboard WHERE indicator = 'distillate_stocks'"}))
            dashboard_rows = result_rows(dashboard)
            if dashboard_rows is None:
                dashboard_detail = dashboard[:200]
            elif not dashboard_rows:
                dashboard_detail = "строк нет: импортёр ещё не прогонялся (make import STAT=eia)"
            else:
                dashboard_detail = f"stale={dashboard_rows[0].get('stale')}, as_of={dashboard_rows[0].get('as_of')}"
            failed += not expect("дашборд distillate_stocks свежий", bool(dashboard_rows) and dashboard_rows[0].get("stale") in (0, "0"), dashboard_detail)

            # Одного stale = 0 мало: в macro_series лежит свежая ручная строка того же ряда
            # (105100 за 02.10), поэтому сломанный приоритет источников остался бы зелёным.
            # Значение дашборда обязано совпасть с последним значением EIA-витрины (105138):
            # это и есть проверка, что v_gold_dashboard берёт distillate_stocks из eia, а не
            # из ручного фолбэка. Пустой ответ/ошибка запроса — провал, detail сырым текстом.
            priority_raw = text(await session.call_tool("run_query", {"query":
                "SELECT d.value = e.value AS same, d.value AS dashboard_value, e.value AS eia_value "
                "FROM v_gold_dashboard d "
                "CROSS JOIN (SELECT argMax(value, date) AS value FROM v_eia_petroleum "
                "WHERE series = 'distillate_stocks') e "
                "WHERE d.indicator = 'distillate_stocks'"}))
            priority = result_rows(priority_raw)
            if priority is None:
                priority_detail = f"ответ не распознан: {priority_raw[:200]}"
            elif not priority:
                priority_detail = "строк нет: импортёр ещё не прогонялся (make import STAT=eia)"
            else:
                priority_detail = f"дашборд={priority[0].get('dashboard_value')}, eia={priority[0].get('eia_value')}"
            failed += not expect("дашборд distillate_stocks из EIA, а не из ручного фолбэка",
                                 bool(priority) and priority[0].get("same") in (1, "1"), priority_detail)

            # Контур прогнозов спроса: витрина консенсуса v_gold_forecasts и витрина
            # ETF-потоков WGC v_gold_etf_flows видны агенту, каталог webbridge засеян,
            # прогнозы внесены и Q3-прогноз резолвлен в v_forecast_accuracy.
            for view in ("v_gold_forecasts", "v_gold_etf_flows"):
                failed += not expect(f"витрина {view} видна", view in tables, "list_tables default")

            # Потоки WGC вносятся сессионно на приёмке, поэтому count может быть 0 —
            # проверяется РАЗРЕШЕНИЕ чтения витрины, как у v_dcf_assumptions.
            etf = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_gold_etf_flows"}))
            failed += not expect("ETF-потоки WGC читаются", "rows" in etf and "Query execution failed" not in etf and "ACCESS_DENIED" not in etf, etf[:200])

            # Каталог webbridge — 20 рядов из сида; их отсутствие ломает семантику
            # v_gold_etf_flows для агента: числа он прочитает, а что измеряет ряд и в каких
            # единицах — нет (etf_flows_month — отдельный ручной ряд v_gold_dashboard,
            # каталог webbridge на него не влияет). detail подсказывает, что применить.
            catalog_wgc = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'webbridge'"}))
            webbridge = result_rows(catalog_wgc)
            webbridge_n = webbridge[0].get("n") if webbridge else None
            if webbridge is None:
                webbridge_detail = f"ответ не распознан: {catalog_wgc[:200]}"
            elif webbridge_n == 20:
                webbridge_detail = "рядов webbridge: 20"
            else:
                webbridge_detail = f"рядов webbridge: {webbridge_n}, применить сид sql/wgc_etf_flows_catalog.sql"
            failed += not expect("каталог webbridge полон", webbridge_n == 20, webbridge_detail)

            # Контур золотых резервов ЦБ (правило 11): витрина v_cb_gold_dynamics видна
            # агенту и отдаёт ровно 7 строк — по стране словаря (POL/CZE/TUR/RUS/KAZ/UZB/CHN);
            # меньше строк означает, что витрина потеряла страну (LEFT JOIN от словаря обязан
            # держать строку даже без рядов), больше — что разбор iso3 из имени ряда дал мусор.
            # Каталог source='wgc' объединяет оба импортёра WGC: 20 рядов ETF (wgc_etf_flows)
            # + 14 рядов ЦБ (wgc_cb_reserves) = 34; ошибка запроса/несовпадение — провал
            # с подсказкой, какой импортёр прогнать или какой сид применить.
            failed += not expect("витрина v_cb_gold_dynamics видна", "v_cb_gold_dynamics" in tables, "list_tables default")

            dynamics_raw = text(await session.call_tool("run_query", {"query": "SELECT iso3, country FROM v_cb_gold_dynamics"}))
            dynamics = result_rows(dynamics_raw)
            if dynamics is None:
                dynamics_detail = f"ответ не распознан: {dynamics_raw[:200]}"
            else:
                dynamics_detail = f"стран: {len(dynamics)}"
            failed += not expect("динамика резервов ЦБ: 7 стран", bool(dynamics) and len(dynamics) == 7, dynamics_detail)

            catalog_wgc_source = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'wgc'"}))
            catalog_wgc_rows = result_rows(catalog_wgc_source)
            catalog_wgc_n = catalog_wgc_rows[0].get("n") if catalog_wgc_rows else None
            if catalog_wgc_rows is None:
                catalog_wgc_detail = f"ответ не распознан: {catalog_wgc_source[:200]}"
            elif catalog_wgc_n == 34:
                catalog_wgc_detail = "рядов wgc: 34 (20 ETF + 14 ЦБ)"
            else:
                catalog_wgc_detail = (f"рядов wgc: {catalog_wgc_n}, want 34 — прогнать "
                                      "make import STAT=wgc_etf_flows и STAT=wgc_cb_reserves")
            failed += not expect("каталог wgc полон", catalog_wgc_n == 34, catalog_wgc_detail)

            # Консенсус аналитиков — 11 строк сида. Пустой v_gold_forecasts означает
            # неприменённый сид, ошибка запроса даёт detail сырым текстом.
            forecasts_raw = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_gold_forecasts"}))
            forecasts = result_rows(forecasts_raw)
            forecasts_n = forecasts[0].get("n") if forecasts else None
            if forecasts is None:
                forecasts_detail = f"ответ не распознан: {forecasts_raw[:200]}"
            elif (forecasts_n or 0) > 0:
                forecasts_detail = f"прогнозов: {forecasts_n}"
            else:
                forecasts_detail = "прогнозов нет: применить сид sql/gold_forecasts_q4_2026_seed.sql"
            failed += not expect("прогнозы консенсуса читаются", bool(forecasts) and (forecasts_n or 0) > 0, forecasts_detail)

            # Резолв прогнозов относительно факта: countIf(is_resolved = 1) >= 1 доказывает,
            # что forecast_log заполнен бэкфиллом, а импортёр forecast_resolve прогонялся.
            accuracy_raw = text(await session.call_tool("run_query", {"query": "SELECT countIf(is_resolved = 1) AS resolved FROM v_forecast_accuracy"}))
            accuracy = result_rows(accuracy_raw)
            resolved_n = accuracy[0].get("resolved") if accuracy else None
            if accuracy is None:
                accuracy_detail = f"ответ не распознан: {accuracy_raw[:200]}"
            elif (resolved_n or 0) >= 1:
                accuracy_detail = f"resolved={resolved_n}"
            else:
                accuracy_detail = "резолвнутых прогнозов нет: бэкфилл sql/forecast_log_backfill.sql + make import STAT=forecast_resolve"
            failed += not expect("прогнозы резолвятся по факту", bool(accuracy) and (resolved_n or 0) >= 1, accuracy_detail)

            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM gold_forecasts"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырая gold_forecasts закрыта", denied, "отказ ClickHouse 497 на gold_forecasts (ожидаемо)" if denied else raw[:200])

            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM macro_series"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырая таблица закрыта", denied, "отказ ClickHouse 497 на macro_series (ожидаемо)" if denied else raw[:200])

            # Контур bold.report (спека §4.4): витрины дневного агрегата и пер-фондового
            # среза читаются агентом, каталог source='bold' описывает ровно 7 рядов, а сырая
            # bold_fund_snapshot — источник пер-фондовой витрины — агенту закрыта (как
            # macro_series: читать её агент обязан только через витрину).
            #
            # Существование витрины проверяется ЗАПРОСОМ К НЕЙ, а не поиском имени в ответе
            # list_tables: в том тексте лежат create_table_query с комментариями таблиц и
            # колонок, поэтому имя отсутствующей витрины попадало туда из COMMENT соседки —
            # проверка «имя в тексте» давала ложное OK, когда v_gold_etf_funds_daily не
            # существовала вовсе (её имя названо в комментарии v_gold_etf_flows_daily).
            # Разобранный ответ run_query доказывает и существование, и право чтения; «нет
            # витрины» и «нет прав» приходят ошибкой (UNKNOWN_TABLE / ACCESS_DENIED) и в
            # result_rows не разбираются.
            #
            # Пустая витрина — не отказ: до первого прогона bold_etf_flows (или при
            # выходном дне источника) строк нет и вовсе, поэтому значение count не
            # проверяется — только разрешение чтения, как у v_dcf_assumptions.
            for view, label in (("v_gold_etf_flows_daily", "дневной агрегат bold"),
                                ("v_gold_etf_funds_daily", "пер-фондовый срез bold")):
                bold_view = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {view}"}))
                bold_view_rows = result_rows(bold_view)
                if bold_view_rows is None:
                    bold_view_detail = f"витрина не читается: {bold_view[:200]}"
                elif bold_view_rows:
                    bold_view_detail = f"строк: {bold_view_rows[0].get('count()')}"
                else:
                    bold_view_detail = "в ответе нет строк"
                failed += not expect(f"витрина {view} существует и читается ({label})",
                                     bool(bold_view_rows), bold_view_detail)

            # Каталог source='bold' — 7 рядов ETF-контура: импортёр перепубликовывает каталог
            # на каждом прогоне (после успешного Send или в ветке no-op), поэтому «меньше 7»
            # значит ни разу не отработавший импорт (числа агент прочитает, а что измеряет
            # ряд и в каких единицах — нет), «больше 7» — ряд, которого витрина не знает.
            catalog_bold_raw = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'bold'"}))
            catalog_bold = result_rows(catalog_bold_raw)
            catalog_bold_n = catalog_bold[0].get("n") if catalog_bold else None
            if catalog_bold is None:
                catalog_bold_detail = f"ответ не распознан: {catalog_bold_raw[:200]}"
            elif catalog_bold_n == 7:
                catalog_bold_detail = "рядов bold: 7"
            else:
                catalog_bold_detail = (f"рядов bold: {catalog_bold_n}, want 7 — прогнать "
                                       "make import STAT=bold_etf_flows (каталог перепубликовывается прогоном импортёра)")
            failed += not expect("каталог bold полон", catalog_bold_n == 7, catalog_bold_detail)

            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM bold_fund_snapshot"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырой пер-фондовый срез закрыт", denied,
                                 "отказ ClickHouse 497 на bold_fund_snapshot (ожидаемо)" if denied else raw[:200])

    return failed


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
