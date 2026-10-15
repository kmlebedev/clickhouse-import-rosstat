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
                catalog_wgc_detail = (f"рядов wgc: {catalog_wgc_n}, want 34 — прогнать по одному: "
                                      "make import STAT=wgc_etf_flows, затем "
                                      "make import STAT=wgc_cb_reserves")
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

            # Контур событий РФ-канала: витрина отфильтрованных событий читается агентом,
            # а сырая news_events — источник витрины — агенту закрыта (как macro_series и
            # bold_fund_snapshot: читать её агент обязан только через витрину). Существование
            # витрины подтверждается запросом к ней, а не поиском имени в list_tables (урок bold).
            news_view = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_news_events"}))
            news_view_rows = result_rows(news_view)
            if news_view_rows is None:
                news_view_detail = f"витрина не читается: {news_view[:200]}"
            elif news_view_rows:
                news_view_detail = f"строк: {news_view_rows[0].get('count()')}"
            else:
                news_view_detail = "в ответе нет строк"
            failed += not expect("витрина v_news_events существует и читается", bool(news_view_rows), news_view_detail)

            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM news_events"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырая news_events закрыта", denied,
                                 "отказ ClickHouse 497 на news_events (ожидаемо)" if denied else raw[:200])

            # Контур СБКТС Росстандарта (правило 11): обе витрины читаются агентом, а сырая
            # gost_vehicle_safety_certificate — источник витрин — агенту закрыта (как macro_series
            # и news_events: читать реестр агент обязан только через витрины). Существование
            # витрины подтверждается ЗАПРОСОМ К НЕЙ, а не поиском имени в ответе list_tables:
            # в том тексте лежат create_table_query с комментариями таблиц и колонок, и имя
            # отсутствующей витрины попадало туда из COMMENT соседки (урок bold). Разобранный
            # ответ run_query доказывает и существование, и право чтения; «нет витрины» и «нет
            # прав» приходят ошибкой (UNKNOWN_TABLE / ACCESS_DENIED) и в result_rows не разбираются.
            #
            # Пустая витрина — не отказ: до первого прогона импортёра (или в день без публикаций)
            # строк нет и вовсе, поэтому значение count не проверяется — только разрешение
            # чтения, как у v_dcf_assumptions.
            for view, label in (("v_gost_vehicle_safety_certificate", "реестр СБКТС"),
                                ("v_gost_vehicle_safety_daily", "дневной поток выдачи СБКТС")):
                gost_view = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {view}"}))
                gost_view_rows = result_rows(gost_view)
                if gost_view_rows is None:
                    gost_view_detail = f"витрина не читается: {gost_view[:200]}"
                elif gost_view_rows:
                    gost_view_detail = f"строк: {gost_view_rows[0].get('count()')}"
                else:
                    gost_view_detail = "в ответе нет строк"
                failed += not expect(f"витрина {view} существует и читается ({label})",
                                     bool(gost_view_rows), gost_view_detail)

            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM gost_vehicle_safety_certificate"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырая gost_vehicle_safety_certificate закрыта", denied,
                                 "отказ ClickHouse 497 на gost_vehicle_safety_certificate (ожидаемо)" if denied else raw[:200])

            # Рыночный контур ожиданий по ключевой ставке MOEX KEYRATE (правило 11): витрина
            # v_keyrate_expectations читается агентом, а сырые keyrate_futures (дневные итоги
            # серий) и keyrate_contract (паспорта серий) — источники витрины — агенту закрыты
            # (как macro_series и bold_fund_snapshot: читать контур агент обязан только через
            # витрину). Существование витрины подтверждается ЗАПРОСОМ К НЕЙ, а не поиском имени
            # в ответе list_tables: в том тексте лежат create_table_query с комментариями
            # таблиц и колонок, и имя отсутствующей витрины попадало туда из COMMENT соседки
            # (урок bold). Разобранный ответ run_query доказывает и существование, и право
            # чтения; «нет витрины» и «нет прав» приходят ошибкой (UNKNOWN_TABLE /
            # ACCESS_DENIED) и в result_rows не разбираются.
            #
            # Витрина гейтится по трём таблицам-источникам (keyrate_futures, keyrate_contract,
            # cbr_key_rate), поэтому до первого прогона импортёров её может не быть вовсе, а
            # пустая cbr_key_rate даёт NULL в expected_change_pp/bp при читаемых строках, —
            # значение count не проверяется, только разрешение чтения, как у v_dcf_assumptions.
            keyrate_view = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_keyrate_expectations"}))
            keyrate_view_rows = result_rows(keyrate_view)
            if keyrate_view_rows is None:
                keyrate_view_detail = f"витрина не читается: {keyrate_view[:200]}"
            elif keyrate_view_rows:
                keyrate_view_detail = f"строк: {keyrate_view_rows[0].get('count()')}"
            else:
                keyrate_view_detail = "в ответе нет строк"
            failed += not expect("витрина v_keyrate_expectations существует и читается", bool(keyrate_view_rows), keyrate_view_detail)

            for table, label in (("keyrate_futures", "дневные итоги серий"),
                                 ("keyrate_contract", "паспорта серий")):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                failed += not expect(f"сырая {table} закрыта ({label})", denied,
                                     f"отказ ClickHouse 497 на {table} (ожидаемо)" if denied else raw[:200])

            # Контур мониторинга предприятий ЦБ (правило 11, пункт 1.3 роадмапа): витрина
            # v_cbr_survey читается агентом, сырая macro_series закрыта (проверка выше).
            # Существование витрины подтверждается запросом к ней, а не поиском имени в
            # list_tables (урок bold: в тексте list_tables имя попадает из COMMENT соседки).
            survey_view = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_cbr_survey"}))
            survey_view_rows = result_rows(survey_view)
            if survey_view_rows is None:
                survey_view_detail = f"витрина не читается: {survey_view[:200]}"
            elif survey_view_rows:
                survey_view_detail = f"строк: {survey_view_rows[0].get('count()')}"
            else:
                survey_view_detail = "в ответе нет строк"
            failed += not expect("витрина v_cbr_survey существует и читается", bool(survey_view_rows), survey_view_detail)

            survey_catalog = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_series_catalog WHERE source = 'cbr_survey'"}))
            survey_catalog_rows = result_rows(survey_catalog)
            survey_catalog_n = survey_catalog_rows[0].get("count()") if survey_catalog_rows else None
            failed += not expect("каталог cbr_survey описывает 13 рядов", survey_catalog_n == 13,
                                 f"записей: {survey_catalog_n}" if survey_catalog_rows is not None else f"каталог не читается: {survey_catalog[:200]}")

            # Контур макроэкономического опроса ЦБ (шаг 1.5 роадмапа, импортёр cbr_mo_br):
            # витрина v_cbr_mo_br читается агентом, каталог source='cbr_mo_br' непуст
            # (число рядов зависит от числа вышедших волн и их показателей, поэтому
            # проверяется не точное число, а непустота — как у v_cbr_macro ниже).
            # Сырая macro_series для этого контура уже проверена выше как закрытая;
            # волны обновляются 8 раз в год, поэтому пустая витрина означает ни разу
            # не отработавший шаг cbr_mo_br, а не отсутствие данных у источника.
            mobr_view = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_cbr_mo_br"}))
            mobr_view_rows = result_rows(mobr_view)
            mobr_view_n = mobr_view_rows[0].get("count()") if mobr_view_rows else None
            if mobr_view_rows is None:
                mobr_view_detail = f"витрина не читается: {mobr_view[:200]}"
            elif (mobr_view_n or 0) > 0:
                mobr_view_detail = f"строк: {mobr_view_n}"
            else:
                mobr_view_detail = (f"строк: {mobr_view_n} — прогнать импортёр контура "
                                    "(make import STAT=cbr_mo_br)")
            failed += not expect("витрина v_cbr_mo_br существует и читается", (mobr_view_n or 0) > 0, mobr_view_detail)

            mobr_catalog = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_series_catalog WHERE source = 'cbr_mo_br'"}))
            mobr_catalog_rows = result_rows(mobr_catalog)
            mobr_catalog_n = mobr_catalog_rows[0].get("count()") if mobr_catalog_rows else None
            failed += not expect("каталог cbr_mo_br непуст", (mobr_catalog_n or 0) > 0,
                                 f"записей: {mobr_catalog_n}" if mobr_catalog_rows is not None else f"каталог не читается: {mobr_catalog[:200]}")

            # Служебная source_publications (точки контроля публикаций, шаг 1.5) агенту
            # не выдана — как сырые macro_series и cbr_households_b_mes: читать контур
            # агент обязан только через витрины, а версии файлов и их отпечатки —
            # внутренняя кухня конвейера, а не данные для прогноза.
            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM source_publications"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("служебная source_publications закрыта", denied,
                                 "отказ ClickHouse 497 на source_publications (ожидаемо)" if denied else raw[:200])

            # Общая витрина контура ЦБ (правило 11, фаза 3 роадмапа): v_cbr_macro читается
            # агентом, а сырая cbr_households_b_mes — источник её рядов по домохозяйствам —
            # агенту закрыта (как macro_series и bold_fund_snapshot: читать контур агент
            # обязан только через витрину). Существование витрины подтверждается ЗАПРОСОМ
            # К НЕЙ, а не поиском имени в ответе list_tables: в том тексте лежат
            # create_table_query с комментариями таблиц и колонок, и имя отсутствующей
            # витрины попадало туда из COMMENT соседки (урок bold). Разобранный ответ
            # run_query доказывает и существование, и право чтения; «нет витрины» и «нет
            # прав» приходят ошибкой (UNKNOWN_TABLE / ACCESS_DENIED) и в result_rows не
            # разбираются.
            #
            # Здесь, в отличие от v_cbr_survey, значение count проверяется: ряды витрины
            # публикуют импортёры cbr/, поэтому пустая витрина означает ни разу не
            # отработавший контур (агент увидит пустую таблицу и не поймёт, что данных нет
            # из-за отсутствия прогона, а не из-за свойств ряда).
            macro_view = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_cbr_macro"}))
            macro_view_rows = result_rows(macro_view)
            macro_view_n = macro_view_rows[0].get("count()") if macro_view_rows else None
            if macro_view_rows is None:
                macro_view_detail = f"витрина не читается: {macro_view[:200]}"
            elif (macro_view_n or 0) > 0:
                macro_view_detail = f"строк: {macro_view_n}"
            else:
                macro_view_detail = (f"строк: {macro_view_n} — прогнать импортёры контура "
                                     "(make import STAT=cbr_key_rate и остальные из dagu/cbr.yaml)")
            failed += not expect("витрина v_cbr_macro существует и читается", (macro_view_n or 0) > 0, macro_view_detail)

            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM cbr_households_b_mes"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырая cbr_households_b_mes закрыта", denied,
                                 "отказ ClickHouse 497 на cbr_households_b_mes (ожидаемо)" if denied else raw[:200])

            # Ставки по кредитам нефинансовым организациям в рублях (импортёр
            # cbr_int_rate_loans, шаг 4.2 #8 роадмапа): ряды контура агент читает только
            # через общую витрину v_cbr_macro (проверка выше), а сырая таблица ему не
            # выдана. Каталог обязан описывать все 16 рядов источника, и число сверяется
            # точным равенством, а не «> 0»: 16 = 8 сроков кредита × 2 группы («Всего» и
            # «СМП»), и имя ряда код собирает из группы и срока. Меньшее число значит ни
            # разу не отработавший шаг cbr_int_rate_loans (числа агент прочитает, а что
            # измеряет ряд и в каких единицах — нет), большее — ряд, которому в листе
            # источника не соответствует колонка. Выборка по origin, а не по source: у
            # source='cbr' лежат ряды всех импортёров ЦБ, а файл loans_nonfin.xlsx —
            # источник ровно этого набора.
            loans_catalog = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_series_catalog WHERE source = 'cbr' AND origin LIKE '%loans_nonfin.xlsx%'"}))
            loans_catalog_rows = result_rows(loans_catalog)
            loans_catalog_n = loans_catalog_rows[0].get("count()") if loans_catalog_rows else None
            if loans_catalog_rows is None:
                loans_catalog_detail = f"каталог не читается: {loans_catalog[:200]}"
            elif loans_catalog_n == 16:
                loans_catalog_detail = "рядов cbr_int_rate_loans: 16 (8 сроков × 2 группы: Всего/СМП)"
            else:
                loans_catalog_detail = (f"рядов cbr_int_rate_loans: {loans_catalog_n}, want 16 (8 сроков "
                                        "× 2 группы: Всего/СМП) — прогнать make import STAT=cbr_int_rate_loans "
                                        "(каталог перепубликовывается прогоном)")
            failed += not expect("каталог cbr_int_rate_loans полон", loans_catalog_n == 16, loans_catalog_detail)

            # Сырая cbr_int_rate_loans агенту не выдана — как macro_series и
            # cbr_households_b_mes: витрина — единственное место, где агент читает ряды
            # контура (единицы и привязка даты различаются между рядами и описаны в
            # v_series_catalog). Таблицу создаёт DDL-прогон импортёра, поэтому
            # UNKNOWN_TABLE здесь — провал, а не успех: таблица обязана существовать и
            # быть закрытой.
            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM cbr_int_rate_loans"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырая cbr_int_rate_loans закрыта", denied,
                                 "отказ ClickHouse 497 на cbr_int_rate_loans (ожидаемо)" if denied else raw[:200])

            # Контур резюме обсуждения ключевой ставки ЦБ (шаг 1.2 роадмапа, импортёр
            # cbr_mp_summary): все три витрины контура (шапка, полный текст, разметка каналов)
            # читаются агентом и непусты, а три сырые таблицы — источники этих витрин — агенту
            # закрыты (как macro_series и cbr_households_b_mes: читать контур агент обязан
            # только через витрины, правило 11 AGENTS.md).
            #
            # Здесь, в отличие от v_dcf_assumptions, проверяется и значение count: строки
            # публикует импортёр cbr/, поэтому пустая витрина означает ни разу не отработавший
            # шаг cbr_mp_summary, а не отсутствие данных у источника. Существование витрины
            # подтверждается ЗАПРОСОМ К НЕЙ, а не поиском имени в ответе list_tables (урок
            # bold: в том тексте имя попадает из COMMENT соседки); «нет витрины» и «нет прав»
            # приходят ошибкой (UNKNOWN_TABLE / ACCESS_DENIED) и в result_rows не разбираются.
            for view, label in (("v_cbr_mp_summary", "шапка"),
                                ("v_cbr_mp_summary_blocks", "текст резюме"),
                                ("v_cbr_mp_summary_channels", "разметка каналов")):
                summary_view = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {view}"}))
                summary_view_rows = result_rows(summary_view)
                summary_view_n = summary_view_rows[0].get("count()") if summary_view_rows else None
                if summary_view_rows is None:
                    summary_view_detail = f"витрина не читается: {summary_view[:200]}"
                elif (summary_view_n or 0) > 0:
                    summary_view_detail = f"строк: {summary_view_n}"
                else:
                    summary_view_detail = (f"строк: {summary_view_n} — прогнать импортёр контура "
                                           "(make import STAT=cbr_mp_summary)")
                failed += not expect(f"витрина {view} существует и читается ({label})",
                                     (summary_view_n or 0) > 0, summary_view_detail)

            for table, label in (("cbr_mp_summary", "шапка"),
                                 ("cbr_mp_summary_block", "блоки"),
                                 ("cbr_mp_summary_channel", "каналы")):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                failed += not expect(f"сырая {table} закрыта ({label})", denied,
                                     f"отказ ClickHouse 497 на {table} (ожидаемо)" if denied else raw[:200])

            # Контур Росстата (фаза 3 роадмапа, задачи T4–T7): витрина v_rosstat_macro видна
            # и НЕПУСТА, каталог описывает ряды всех пяти импортёров пакета, а пять
            # сырых таблиц — источники витрины — агенту закрыты (как macro_series и
            # cbr_households_b_mes: читать контур агент обязан только через витрину,
            # правило 11 AGENTS.md).
            #
            # Здесь, в отличие от v_dcf_assumptions, пустая витрина успехом не считается:
            # строки публикуют импортёры rosstat/, поэтому ноль строк и ноль различных
            # рядов означают ни разу не отработавший контур, а не отсутствие данных у
            # источника. Число точек меняется с каждым прогоном источника, поэтому
            # точное значение count() не сверяется — проверяются count() > 0 и
            # uniqExact(series) > 0 оба разом: «витрина читается» без непустоты пропустило
            # бы пустую витрину. Существование витрины подтверждается ЗАПРОСОМ К НЕЙ, а не
            # только поиском имени в ответе list_tables: в том тексте лежат
            # create_table_query с комментариями таблиц и колонок, и имя отсутствующей
            # витрины попадало туда из COMMENT соседки (урок bold). Разобранный ответ
            # run_query доказывает и существование, и право чтения; «нет витрины» и «нет
            # прав» приходят ошибкой (UNKNOWN_TABLE / ACCESS_DENIED) и в result_rows не
            # разбираются.
            failed += not expect("витрина v_rosstat_macro видна", "v_rosstat_macro" in tables, "list_tables default")

            rosstat_view = text(await session.call_tool("run_query", {"query": "SELECT count() AS rows, uniqExact(series) AS series FROM v_rosstat_macro"}))
            rosstat_view_rows = result_rows(rosstat_view)
            rosstat_points_n = rosstat_view_rows[0].get("rows") if rosstat_view_rows else None
            rosstat_series_n = rosstat_view_rows[0].get("series") if rosstat_view_rows else None
            if rosstat_view_rows is None:
                rosstat_view_detail = f"витрина не читается: {rosstat_view[:200]}"
            elif (rosstat_points_n or 0) > 0 and (rosstat_series_n or 0) > 0:
                rosstat_view_detail = f"строк: {rosstat_points_n}, рядов: {rosstat_series_n}"
            else:
                rosstat_view_detail = (f"строк: {rosstat_points_n}, рядов: {rosstat_series_n} — прогнать "
                                       "импортёры контура по одному: make import STAT=<имя> для "
                                       "каждого из rosstat_ipc_mes, rosstat_ipc_weeks, "
                                       "rosstat_salaries_mes, rosstat_vvp_kvartal, "
                                       "rosstat_labour_force_mes")
            failed += not expect("витрина v_rosstat_macro непуста",
                                 (rosstat_points_n or 0) > 0 and (rosstat_series_n or 0) > 0, rosstat_view_detail)

            # Каталог описывает все 147 рядов источника: 4 ИПЦ (rosstat_ipc_mes) + 131 недельный
            # товар (rosstat_ipc_weeks) + 5 рядов ВВП (rosstat_vvp_kvartal) + 1 зарплата
            # (rosstat_salaries_mes) + 6 рядов рынка труда (rosstat_labour_force_mes).
            # Число сверяется точным равенством, а не «> 0»: односторонний контракт §11.4
            # стандарта требует описания каждого разобранного ряда, а меньше 147 значит
            # ни разу не отработавший импортёр (числа агент прочитает, а что измеряет ряд
            # и в каких единицах — нет). В плане фазы написано 138: это арифметика
            # 4 + 128 + 5 + 1 до fix-round задачи T7, который вернул три товара под обрывом
            # листа (fc0d28f) — в ipcWeeksProducts 131 название, а шести рядов рынка труда
            # в той арифметике нет вовсе, поэтому верное число — 147.
            # detail печатает наблюдённое число, когда оно расходится с ожиданием.
            rosstat_catalog = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'rosstat'"}))
            rosstat_catalog_rows = result_rows(rosstat_catalog)
            rosstat_catalog_n = rosstat_catalog_rows[0].get("n") if rosstat_catalog_rows else None
            if rosstat_catalog_rows is None:
                rosstat_catalog_detail = f"ответ не распознан: {rosstat_catalog[:200]}"
            elif rosstat_catalog_n == 147:
                rosstat_catalog_detail = "рядов rosstat: 147 (4 ИПЦ + 131 недельный + 5 ВВП + 1 зарплата + 6 рабочая сила)"
            else:
                rosstat_catalog_detail = (f"рядов rosstat: {rosstat_catalog_n}, want 147 — прогнать "
                                          "импортёры по одному: make import STAT=<имя> для каждого "
                                          "из rosstat_ipc_mes, rosstat_ipc_weeks, "
                                          "rosstat_salaries_mes, rosstat_vvp_kvartal, "
                                          "rosstat_labour_force_mes "
                                          "(каталог перепубликовывается каждым прогоном)")
            failed += not expect("каталог rosstat полон", rosstat_catalog_n == 147, rosstat_catalog_detail)

            # Сырые таблицы контура агенту не выданы — как macro_series и
            # cbr_households_b_mes: витрина — единственное место, где агент читает ряды
            # (единицы и привязка даты различаются между рядами и описаны в v_series_catalog).
            # До первого живого прогона импортёров таблиц может не быть вовсе, и запрос
            # отвечает UNKNOWN_TABLE, а не ACCESS_DENIED, — это провал, а не успех: таблица
            # обязана существовать и быть закрытой.
            for table, label in (("rosstat_ipc_mes", "ИПЦ помесячно"),
                                 ("rosstat_ipc_weeks", "недельные цены"),
                                 ("rosstat_salaries_mes", "зарплата помесячно"),
                                 ("rosstat_vvp_kvartal", "ВВП поквартально"),
                                 ("rosstat_labour_force_mes", "рабочая сила и безработица")):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                failed += not expect(f"сырая {table} закрыта ({label})", denied,
                                     f"отказ ClickHouse 497 на {table} (ожидаемо)" if denied else raw[:200])

            # Контур потребительских расходов Сбера (фаза 2 роадмапа «починка пустых рядов»,
            # импортёры sber_consumper_spending_index и sber_izmenenie_trat): общая витрина
            # v_bank_sber_spending читается агентом и НЕПУСТА, каталог описывает её ряды двумя
            # РАЗНЫМИ ключами source, а две сырые таблицы — источники витрины — агенту закрыты
            # (как macro_series и cbr_households_b_mes: читать контур агент обязан только через
            # витрину, правило 11 AGENTS.md).
            #
            # Здесь, в отличие от v_dcf_assumptions, пустая витрина успехом не считается:
            # строки публикуют импортёры bank/, поэтому ноль строк означает ни разу не
            # отработавший контур, а не отсутствие данных у источника — исходная жалоба §4.1
            # роадмапа и есть «обе таблицы по 0 строк при написанных импортёрах». Число точек
            # меняется с каждым прогоном источника, поэтому точное значение count() не
            # сверяется — проверяется count() > 0. Существование витрины подтверждается
            # ЗАПРОСОМ К НЕЙ, а не поиском имени в ответе list_tables: в том тексте лежат
            # create_table_query с комментариями таблиц и колонок, и имя отсутствующей витрины
            # попадало туда из COMMENT соседки (урок bold). Разобранный ответ run_query
            # доказывает и существование, и право чтения; «нет витрины» и «нет прав» приходят
            # ошибкой (UNKNOWN_TABLE / ACCESS_DENIED) и в result_rows не разбираются.
            sber_view = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_bank_sber_spending"}))
            sber_view_rows = result_rows(sber_view)
            sber_view_n = sber_view_rows[0].get("count()") if sber_view_rows else None
            if sber_view_rows is None:
                sber_view_detail = f"витрина не читается: {sber_view[:200]}"
            elif (sber_view_n or 0) > 0:
                sber_view_detail = f"строк: {sber_view_n}"
            else:
                sber_view_detail = ("строк: 0 — прогнать импортёры контура: make import "
                                    "STAT=sber_consumper_spending_index и STAT=sber_izmenenie_trat "
                                    "(витрину пересоздаёт AfterImport каждого прогона)")
            failed += not expect("витрина v_bank_sber_spending непуста", (sber_view_n or 0) > 0, sber_view_detail)

            # Одного общего count() > 0 мало: каталог и витрину пересоздаёт AfterImport ЛЮБОГО из
            # двух импортёров, поэтому зелёная витрина могла бы держать точки только одного набора —
            # ровно исходный дефект §4.1 роадмапа (пустая sber_izmenenie_trat при непустой
            # sber_consumper_spending_index) прошёл бы проверку. Поэтому наборы проверяются
            # поэлементно по source, который витрина штампует литералом, плюс uniqExact(source) = 2:
            # это разом доказывает и объединение наборов в одной витрине, и непустоту каждого из них.
            for sber_source in ("sber_consumper_spending_index", "sber_izmenenie_trat"):
                sber_source_raw = text(await session.call_tool("run_query", {"query": f"SELECT count() AS n FROM v_bank_sber_spending WHERE source = '{sber_source}'"}))
                sber_source_rows = result_rows(sber_source_raw)
                sber_source_n = sber_source_rows[0].get("n") if sber_source_rows else None
                if sber_source_rows is None:
                    sber_source_detail = f"ответ не распознан: {sber_source_raw[:200]}"
                elif (sber_source_n or 0) > 0:
                    sber_source_detail = f"точек: {sber_source_n}"
                else:
                    sber_source_detail = (f"точек: {sber_source_n} — прогнать импортёр набора: "
                                          f"make import STAT={sber_source}")
                failed += not expect(f"набор {sber_source} есть в витрине", (sber_source_n or 0) > 0, sber_source_detail)

            sber_sources_raw = text(await session.call_tool("run_query", {"query": "SELECT uniqExact(source) AS sources FROM v_bank_sber_spending"}))
            sber_sources_rows = result_rows(sber_sources_raw)
            sber_sources_n = sber_sources_rows[0].get("sources") if sber_sources_rows else None
            if sber_sources_rows is None:
                sber_sources_detail = f"ответ не распознан: {sber_sources_raw[:200]}"
            else:
                sber_sources_detail = f"различных source: {sber_sources_n} (want 2)"
            failed += not expect("витрина объединяет оба набора Сбера", sber_sources_n == 2, sber_sources_detail)

            # Каталог описывает все 5 месячных и все 46 недельных рядов — точным равенством, а не
            # «> 0» (односторонний контракт §11.4: каждый разобранный ряд обязан иметь описание).
            # Меньшее число значит ни разу не отработавший импортёр (числа агент прочитает, а что
            # измеряет ряд и в каких единицах — нет); большее — ряд, которого витрина не знает.
            # Два ключа source проверяются порознь: у наборов пересекаются имена категорий
            # («Услуги», «Продовольственные товары»), и общий ключ каталога схлопнул бы разные
            # ряды в один.
            for sber_source, sber_want in (("sber_consumper_spending_index", 5), ("sber_izmenenie_trat", 46)):
                sber_catalog = text(await session.call_tool("run_query", {"query": f"SELECT count() AS n FROM v_series_catalog WHERE source = '{sber_source}'"}))
                sber_catalog_rows = result_rows(sber_catalog)
                sber_catalog_n = sber_catalog_rows[0].get("n") if sber_catalog_rows else None
                if sber_catalog_rows is None:
                    sber_catalog_detail = f"ответ не распознан: {sber_catalog[:200]}"
                elif sber_catalog_n == sber_want:
                    sber_catalog_detail = f"рядов {sber_source}: {sber_want}"
                else:
                    sber_catalog_detail = (f"рядов {sber_source}: {sber_catalog_n}, want {sber_want} — прогнать "
                                           f"make import STAT={sber_source} (каталог перепубликовывается прогоном)")
                failed += not expect(f"каталог {sber_source} полон", sber_catalog_n == sber_want, sber_catalog_detail)

            # Сырые таблицы контура агенту не выданы — как macro_series, cbr_households_b_mes и
            # rosstat_ipc_mes: витрина — единственное место, где агент читает ряды (единицы и
            # привязка даты различаются между наборами и описаны только в v_series_catalog).
            # Обе таблицы создаёт DDL-прогон импортёра и они существовали в БД ещё до починки
            # (пустыми, §4.1 роадмапа), поэтому UNKNOWN_TABLE здесь — провал, а не успех:
            # таблица обязана существовать и быть закрытой.
            for table, label in (("sber_consumper_spending_index", "месячный индекс расходов"),
                                 ("sber_izmenenie_trat", "недельный темп по категориям")):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                failed += not expect(f"сырая {table} закрыта ({label})", denied,
                                     f"отказ ClickHouse 497 на {table} (ожидаемо)" if denied else raw[:200])

    return failed


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
