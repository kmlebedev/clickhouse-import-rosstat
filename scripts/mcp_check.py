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

            # Посты smart-lab — образцы публикаций и рыночный разбор для постов по ключевой
            # ставке (docs/KEY_RATE_PUBLICATIONS.md §12), а полный текст поста лежит в
            # description (у новостных лент там краткое описание). Проверка выше доказывает
            # только читаемость витрины; здесь важен непустой count: ноль значит, что ленты
            # smart-lab ещё не импортированы (или миграция description не прогнана на живой
            # базе), а не «в лентах нет постов» — окно 20 постов конечно и само не догонится
            # (урок eia: лента не должна молча стареть), поэтому подсказка — прогнать импорт.
            smartlab_posts = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_news_events WHERE source = 'smartlab' AND description != ''"}))
            smartlab_rows = result_rows(smartlab_posts)
            smartlab_n = smartlab_rows[0].get("count()") if smartlab_rows else None
            if smartlab_rows is None:
                smartlab_detail = f"витрина не читается: {smartlab_posts[:200]}"
            elif (smartlab_n or 0) > 0:
                smartlab_detail = f"постов smartlab: {smartlab_n}"
            else:
                smartlab_detail = (f"постов smartlab: {smartlab_n} — прогнать make import STAT=news_watch "
                                   "(витрина публикуется прогоном импортёра)")
            failed += not expect("посты smart-lab с полным текстом", (smartlab_n or 0) > 0, smartlab_detail)

            # Контур Telegram-молнии Интерфакса (пункт 1.9 роадмапа): витрины текущего
            # состояния и всех версий читаются агентом и НЕПУСТЫ — строки публикует прогон
            # news_watch (фид interfax_tg), поэтому ноль означает ни разу не отработавший
            # прогон, а не отсутствие сообщений в канале. Существование витрины
            # подтверждается ЗАПРОСОМ К НЕЙ, а не поиском имени в ответе list_tables
            # (урок bold: в том тексте имя попадает из COMMENT соседки); «нет витрины»
            # (UNKNOWN_TABLE) и «нет прав» (ACCESS_DENIED) приходят ошибкой и в
            # result_rows не разбираются.
            for view, label in (("v_news_flash", "текущее состояние Telegram-молнии"),
                                ("v_news_message_versions", "все версии, включая удаления")):
                flash_view = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {view}"}))
                flash_view_rows = result_rows(flash_view)
                flash_view_n = flash_view_rows[0].get("count()") if flash_view_rows else None
                if flash_view_rows is None:
                    flash_view_detail = f"витрина не читается: {flash_view[:200]}"
                elif (flash_view_n or 0) > 0:
                    flash_view_detail = f"строк: {flash_view_n}"
                else:
                    flash_view_detail = (f"строк: {flash_view_n} — прогнать make import STAT=news_watch "
                                         "(витрину публикует прогон импортёра)")
                failed += not expect(f"витрина {view} существует и непуста ({label})",
                                     (flash_view_n or 0) > 0, flash_view_detail)

            # Корпоративный Telegram-канал edisclosure (пункт 1.11): лента news_watch
            # пишет в ту же таблицу версий и витрину v_news_flash, поэтому
            # общая проверка выше проходит и на одних сообщениях молнии. Ноль здесь
            # означает ни разу не отработавший прогон с новой лентой, а не пустой
            # канал, — detail подсказывает команду импорта.
            edisclosure_raw = text(await session.call_tool(
                "run_query",
                {"query": "SELECT count() AS n FROM v_news_flash WHERE channel = 'edisclosure'"}))
            edisclosure = result_rows(edisclosure_raw)
            edisclosure_n = edisclosure[0].get("n") if edisclosure else None
            if edisclosure is None:
                edisclosure_detail = f"ответ не распознан: {edisclosure_raw[:200]}"
            elif (edisclosure_n or 0) > 0:
                edisclosure_detail = f"сообщений edisclosure: {edisclosure_n}"
            else:
                edisclosure_detail = (f"сообщений edisclosure: {edisclosure_n or 0} — прогнать make import "
                                      "STAT=news_watch (канал edisclosure)")
            failed += not expect("канал edisclosure в v_news_flash", (edisclosure_n or 0) > 0, edisclosure_detail)

            # Новые Telegram-каналы пакета #46 (infra/2.3): v_news_flash — единственное место,
            # где видны все каналы (v_news_events фильтрует по newsFilter и может не показать
            # канал целиком). Проверка per-channel: каждый из шести каналов присутствует и
            # каждое его сообщение получило doc-строку (annotated = messages). Так smoke-тест
            # не зеленеет от одной размеченной строки в одном канале; полное покрытие RSS
            # в v_news_events по-прежнему проверяет протокол приёмки.
            new_channels = ("('ifax_go', 'markettwits', 'newssmartlab', 'russianmacro', "
                            "'MarketOverview', 'mozgovikresearch')")
            new_channels_raw = text(await session.call_tool("run_query", {"query":
                f"SELECT channel, uniqExact(message_id) AS messages, "
                f"uniqExactIf(message_id, model != '') AS annotated "
                f"FROM v_news_flash WHERE channel IN {new_channels} GROUP BY channel"}))
            new_channels_rows = result_rows(new_channels_raw)
            required_channels = ("ifax_go", "markettwits", "newssmartlab",
                                 "russianmacro", "MarketOverview", "mozgovikresearch")
            by_channel = {}
            for row in new_channels_rows or []:
                by_channel[row.get("channel")] = (row.get("messages") or 0, row.get("annotated") or 0)
            missing = [c for c in required_channels if c not in by_channel]
            incomplete = [f"{c}: {by_channel[c][1]}/{by_channel[c][0]}"
                          for c in required_channels
                          if c in by_channel and by_channel[c][1] != by_channel[c][0]]
            if new_channels_rows is None:
                new_channels_detail = f"ответ не распознан: {new_channels_raw[:200]}"
            else:
                new_channels_detail = f"каналов: {len(by_channel)}/6, неполно: {incomplete or '—'}"
            failed += not expect("новые Telegram-каналы #46 в v_news_flash",
                                 not missing, f"нет каналов: {missing or '—'}")
            annotation_ok = bool(new_channels_rows) and not missing and not incomplete
            bootstrap_hint = (" — прогнать NEWS_LLM_PROFILE=glm NEWS_LLM_BACKLOG_LIMIT=500 "
                              "make import STAT=news_watch")
            failed += not expect("bootstrap-разметка новых каналов", annotation_ok,
                                 new_channels_detail if annotation_ok else new_channels_detail + bootstrap_hint)

            # Реестр тегов — статическая витрина: 46 слагов, из них ровно 11 каналов ставки
            # (cb_channel). Меньшее число значит ни разу не отработавший прогон news_watch
            # (витрина публикуется вместе с остальными), расхождение — реестр разошёлся с
            # news/tags.go. Ключевые пары проверяются отдельно: по ним агент расшифровывает
            # теги из v_news_flash.tags.
            tags_raw = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n, countIf(dimension = 'cb_channel') AS channels FROM v_news_tags"}))
            tags = result_rows(tags_raw)
            tags_n = tags[0].get("n") if tags else None
            tags_channels_n = tags[0].get("channels") if tags else None
            if tags is None:
                tags_detail = f"ответ не распознан: {tags_raw[:200]}"
            else:
                tags_detail = f"слагов: {tags_n} (want >= 30), cb_channel: {tags_channels_n} (want 11)"
            failed += not expect("реестр v_news_tags полон",
                                 (tags_n or 0) >= 30 and tags_channels_n == 11, tags_detail)

            # Ключевые пары реестра — контракт промпта и валидации (news/tags.go,
            # news/annotation.go): пропавшая пара сломала бы расшифровку тега агентом,
            # а не только число строк.
            tags_keys_raw = text(await session.call_tool("run_query", {"query": "SELECT dimension, tag FROM v_news_tags"}))
            tags_keys = result_rows(tags_keys_raw)
            expected_tags = {("person", "trump"), ("country", "china"),
                             ("event", "energy_transition"), ("event", "deglobalization"),
                             ("cb_channel", "budget_impulse")}
            present_tags = {(row.get("dimension"), row.get("tag")) for row in (tags_keys or [])}
            missing_tags = sorted(expected_tags - present_tags)
            if tags_keys is None:
                tags_keys_detail = f"ответ не распознан: {tags_keys_raw[:200]}"
            else:
                tags_keys_detail = (f"нет пар: {missing_tags}" if missing_tags
                                    else f"все {len(expected_tags)} пар на месте")
            failed += not expect("реестр v_news_tags содержит ключевые пары",
                                 not missing_tags, tags_keys_detail)

            # Сырые таблицы контура агенту не выданы — как macro_series и news_events:
            # версии и разметку агент читает только через витрины. Таблицы создаёт
            # DDL-прогон news_watch, поэтому UNKNOWN_TABLE здесь — провал, а не успех:
            # таблица обязана существовать и быть закрытой.
            for table, label in (("news_message_version", "версии Telegram-молнии"),
                                 ("news_annotation", "LLM-разметка Telegram-молнии")):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                failed += not expect(f"сырая {table} закрыта ({label})", denied,
                                     f"отказ ClickHouse 497 на {table} (ожидаемо)" if denied else raw[:200])

            # LLM-теги — информационная строка, а не проверка: без NEWS_LLM_MODEL разметка
            # штатно пропускается (warn импортёра), и живой прогон 1.10 ждёт мерджа 1.8.
            # Ноль тегов не увеличивает failed — иначе make mcp-check был бы красным
            # на каждой БД без LLM-переменных.
            llm_tags_raw = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_news_flash WHERE length(tags) > 0"}))
            llm_tags = result_rows(llm_tags_raw)
            llm_tags_n = llm_tags[0].get("n") if llm_tags else None
            if llm_tags is None:
                print(f"WARN LLM-теги: витрина не читается: {llm_tags_raw[:200]}")
            elif (llm_tags_n or 0) > 0:
                print(f"OK   LLM-теги: {llm_tags_n} сообщений с тегами")
            else:
                print("WARN LLM-теги: 0 (NEWS_LLM_MODEL не задан — штатная деградация; "
                      "живой прогон 1.10 после мерджа 1.8)")

            # LLM-разметка RSS — тоже информационная строка: без NEWS_LLM_PROFILE=glm и
            # ключей разметка штатно пропускается, поэтому ноль не увеличивает failed.
            # Знаменатель — только аннотируемые позиции (content_hash != ''): строки до
            # миграции 1.9 не размечены, остаются с model = '' и дефектом не считаются.
            news_llm_raw = text(await session.call_tool("run_query", {"query":
                "SELECT countIf(content_hash != '') AS n, "
                "countIf(content_hash != '' AND model != '') AS annotated "
                "FROM v_news_events WHERE transport = 'rss'"}))
            news_llm = result_rows(news_llm_raw)
            if news_llm is None:
                print(f"WARN LLM-разметка RSS: витрина не читается: {news_llm_raw[:200]}")
            else:
                news_llm_n = news_llm[0].get("n") if news_llm else None
                news_llm_annotated = news_llm[0].get("annotated") if news_llm else None
                if (news_llm_annotated or 0) > 0:
                    print(f"OK   LLM-разметка RSS: {news_llm_annotated} из {news_llm_n} позиций")
                else:
                    print(f"WARN LLM-разметка RSS: 0 из {news_llm_n} позиций — догон: "
                          "NEWS_LLM_PROFILE=glm NEWS_LLM_BACKLOG_LIMIT=500 "
                          "make import STAT=news_watch")

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

            # Сводный дашборд контура ключевой ставки (пункт key-rate/phase-4-dashboard,
            # issue #128): одна витрина на оба контура, пороги, ближайшее заседание и
            # последний прогноз с ошибкой в б.п. Проверяется не только право чтения, но и
            # состав: 5 фиксированных контуров обязаны быть (spot, fundamental, market,
            # divergence, calendar), forecast — информационная строка: без прогонов writer'а
            # треков нет, и это не FAIL. next_meeting.target_date сверяется с минимумом
            # будущих заседаний из v_events_calendar — так проверяется сам расчёт, а не
            # только наличие строки. error_bp — колонка, которой нет ни в одной другой
            # витрине; запрос к ней доказывает, что контракт ошибки доступен агенту.
            kr_dash_raw = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_key_rate_dashboard"}))
            kr_dash = result_rows(kr_dash_raw)
            kr_dash_n = kr_dash[0].get("n") if kr_dash else None
            if kr_dash is None:
                kr_dash_detail = f"витрина не читается: {kr_dash_raw[:200]}"
            elif (kr_dash_n or 0) > 0:
                kr_dash_detail = f"строк: {kr_dash_n}"
            else:
                kr_dash_detail = "строк: 0 — прогнать make import STAT=key_rate_views"
            failed += not expect("витрина v_key_rate_dashboard существует и непуста", (kr_dash_n or 0) > 0, kr_dash_detail)

            kr_contours_raw = text(await session.call_tool("run_query", {"query":
                "SELECT count(DISTINCT contour) AS n, groupUniqArray(contour) AS contours FROM v_key_rate_dashboard"}))
            kr_contours = result_rows(kr_contours_raw)
            kr_contours_n = kr_contours[0].get("n") if kr_contours else None
            kr_contours_list = kr_contours[0].get("contours") if kr_contours else None
            kr_fixed_contours = {"spot", "fundamental", "market", "divergence", "calendar"}
            kr_contours_ok = isinstance(kr_contours_list, list) and kr_fixed_contours.issubset(set(kr_contours_list))
            if kr_contours is None:
                kr_contours_detail = f"ответ не распознан: {kr_contours_raw[:200]}"
            else:
                kr_contours_detail = f"контуров: {kr_contours_n} {kr_contours_list}"
            # 5 фиксированных контуров обязательны по ИМЕНАМ, а не по числу: набор из пяти
            # чужих контуров не должен проходить проверку. forecast — информационная строка.
            failed += not expect("витрина v_key_rate_dashboard содержит 5 фиксированных контуров",
                                 kr_contours_ok, kr_contours_detail)

            kr_meeting_raw = text(await session.call_tool("run_query", {"query":
                "SELECT d.target_date AS dashboard_date, e.next_date AS calendar_date "
                "FROM v_key_rate_dashboard d "
                "CROSS JOIN (SELECT min(event_date) AS next_date FROM v_events_calendar "
                "WHERE category = 'cbr' AND event_date >= today()) e "
                "WHERE d.indicator = 'next_meeting'"}))
            kr_meeting = result_rows(kr_meeting_raw)
            if kr_meeting is None:
                kr_meeting_detail = f"ответ не распознан: {kr_meeting_raw[:200]}"
            elif not kr_meeting:
                kr_meeting_detail = "строки next_meeting нет"
            else:
                kr_meeting_detail = (f"dashboard={kr_meeting[0].get('dashboard_date')}, "
                                     f"calendar={kr_meeting[0].get('calendar_date')}")
            failed += not expect("next_meeting.target_date совпадает с ближайшим заседанием ЦБ",
                                 bool(kr_meeting) and kr_meeting[0].get("dashboard_date") == kr_meeting[0].get("calendar_date"),
                                 kr_meeting_detail)

            kr_fixed_raw = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n FROM v_key_rate_dashboard "
                "WHERE indicator IN ('base_ipc_saar', 'keyrate_implied_change_bp')"}))
            kr_fixed = result_rows(kr_fixed_raw)
            kr_fixed_n = kr_fixed[0].get("n") if kr_fixed else None
            failed += not expect("пороговые строки base_ipc_saar и keyrate_implied_change_bp на месте",
                                 kr_fixed_n == 2,
                                 f"строк: {kr_fixed_n}" if kr_fixed is not None else f"ответ не распознан: {kr_fixed_raw[:200]}")

            kr_error_raw = text(await session.call_tool("run_query", {"query":
                "SELECT error_bp FROM v_key_rate_dashboard WHERE contour = 'forecast' LIMIT 1"}))
            kr_error = result_rows(kr_error_raw)
            kr_forecast_raw = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n FROM v_key_rate_dashboard WHERE contour = 'forecast'"}))
            kr_forecast = result_rows(kr_forecast_raw)
            kr_forecast_n = kr_forecast[0].get("n") if kr_forecast else None
            if kr_forecast is None:
                kr_forecast_detail = f"ответ не распознан: {kr_forecast_raw[:200]}"
            elif (kr_forecast_n or 0) > 0:
                kr_forecast_detail = f"треков: {kr_forecast_n}"
            else:
                kr_forecast_detail = ("треков: 0 — прогнать make key-rate-forecast-log (информационно; "
                                      "без журнала витрина живёт без строк forecast)")
            print(f"OK   forecast-треки в v_key_rate_dashboard: {kr_forecast_detail}" if (kr_forecast_n or 0) > 0
                  else f"WARN forecast-треки в v_key_rate_dashboard: {kr_forecast_detail}")

            # Контракт публикации (§7, §8): у статистического трека ordered_logit в витрине
            # обязаны быть непустые probabilities и model. Строки нет — проверка проходит
            # информационно (без прогонов writer'а журнала витрина живёт без forecast), но
            # если строка есть, пустые метаданные — провал.
            kr_track_raw = text(await session.call_tool("run_query", {"query":
                "SELECT probabilities, model FROM v_key_rate_dashboard "
                "WHERE contour = 'forecast' AND indicator = 'cbr_rate:ordered_logit' LIMIT 1"}))
            kr_track = result_rows(kr_track_raw)
            if kr_track is None:
                kr_track_detail = f"ответ не распознан: {kr_track_raw[:200]}"
                kr_track_ok = False
            elif not kr_track:
                kr_track_detail = "строки cbr_rate:ordered_logit нет — прогнать make key-rate-forecast-log (информационно)"
                kr_track_ok = True
            else:
                kr_track_ok = (kr_track[0].get("probabilities") or "") != "" and (kr_track[0].get("model") or "") != ""
                kr_track_detail = (f"probabilities={'есть' if (kr_track[0].get('probabilities') or '') != '' else 'пусто'}, "
                                   f"model={kr_track[0].get('model')!r}")
            failed += not expect("probabilities/model у cbr_rate:ordered_logit непусты", kr_track_ok, kr_track_detail)

            # error_bp — часть контракта ошибки в б.п. (#127); запрос к колонке обязан
            # проходить, даже если сейчас все прогнозы ещё не свёрены и значения NULL.
            failed += not expect("колонка error_bp читается агентом", kr_error is not None,
                                 "SELECT error_bp FROM v_key_rate_dashboard" if kr_error is not None else kr_error_raw[:200])

            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM forecast_log"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырой forecast_log закрыт", denied,
                                 "отказ ClickHouse 497 на forecast_log (ожидаемо)" if denied else raw[:200])

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
            loans_catalog = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_series_catalog WHERE source = 'cbr' AND origin LIKE '%loans\\_nonfin.xlsx%'"}))
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

            # Контур платёжного баланса ЦБ (пункт 4.2 №14 роадмапа — канал «внешние условия и
            # курс», импортёры cbr_bop_est и cbr_bop_standard): витрина v_cbr_bop читается агентом
            # и НЕПУСТА, каталог описывает оба набора целиком (51 и 34 ряда), а обе сырые таблицы —
            # источники витрины — агенту закрыты (как macro_series и cbr_int_rate_loans: читать
            # контур агент обязан только через витрину, правило 11 AGENTS.md).
            #
            # Здесь, в отличие от v_cbr_survey, значение count проверяется: строки публикуют
            # импортёры cbr/ (17-й и 18-й шаги dagu/cbr.yaml), поэтому пустая витрина означает ни
            # разу не отработавший контур, а не отсутствие данных у источника. Существование
            # витрины подтверждается ЗАПРОСОМ К НЕЙ, а не поиском имени в ответе list_tables: в
            # том тексте лежат create_table_query с комментариями таблиц и колонок, и имя
            # отсутствующей витрины попадало туда из COMMENT соседки (урок bold). Разобранный
            # ответ run_query доказывает и существование, и право чтения; «нет витрины» и «нет
            # прав» приходят ошибкой (UNKNOWN_TABLE / ACCESS_DENIED) и в result_rows не
            # разбираются.
            bop_view = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_cbr_bop"}))
            bop_view_rows = result_rows(bop_view)
            bop_view_n = bop_view_rows[0].get("count()") if bop_view_rows else None
            if bop_view_rows is None:
                bop_view_detail = (f"витрина не читается: {bop_view[:200]} — на пустой БД её создаёт "
                                   "прогон импортёра (make import STAT=cbr_bop_est)")
            elif (bop_view_n or 0) > 0:
                bop_view_detail = f"строк: {bop_view_n}"
            else:
                bop_view_detail = ("строк: 0 — прогнать импортёры контура: make import "
                                   "STAT=cbr_bop_est и STAT=cbr_bop_standard (витрину "
                                   "пересоздаёт AfterImport каждого прогона)")
            failed += not expect("витрина v_cbr_bop существует и непуста", (bop_view_n or 0) > 0, bop_view_detail)

            # Каталог обязан описывать ВСЕ ряды обоих наборов — точным равенством, а не «> 0»
            # (односторонний контракт стандарта §11.4: каждый разобранный ряд обязан иметь
            # описание). Выборка по ОБОИМ полям, source И origin (LIKE с экранированными
            # подчёркиваниями, как у cbr_int_rate_loans выше): source говорит, что ряд
            # принадлежит нужному импортёру, а origin — что он разобран именно из файла этого
            # импортёра (у этих наборов origin несёт имя файла, лист и подпись строки-агрегата,
            # спека §3.7). Одного счёта по source мало: 51 (или 34) записи с верным source, но
            # origin чужого файла означали бы каталог, описывающий не тот артефакт, чьи точки
            # лежат в витрине.
            # Меньшее число значит ни разу не отработавший шаг cbr_bop_est или cbr_bop_standard —
            # прогнать make import STAT=<имя шага> (числа агент прочитает, а что измеряет ряд,
            # в каких единицах и как привязана дата точки — нет); большее — ряд, которому в листе
            # источника не соответствует строка-агрегат. 51 = 17 агрегатов × 3 вида периода
            # (месяц, квартал, накопленным итогом), 34 = 17 агрегатов × 2 листа (кварталы, годы).
            #
            # Шаблон origin в этой проверке обязан совпадать с origin, который собирает
            # cbr/bop_meta.go: там имя файла несёт сам каталог (cbrBopEstOrigin,
            # cbrBopStandardOrigin), и расхождение шаблона — например, переименование файла в
            # каталоге — даёт то же «меньше рядов», что и неотработавший шаг импортёра. То есть
            # «меньше рядов» здесь читается и как «не отработал шаг», и как «разошёлся шаблон»,
            # и в отчёте прогона эти две причины надо различать по v_series_catalog, а не по числу.
            #
            # Знак «_» в LIKE — одиночный любой символ, поэтому он экранирован (\_): без этого
            # шаблон bop_est.xlsx совпал бы и с посторонним именем файла, отличающимся от него
            # одним символом на месте подчёркивания.
            bop_catalogs = (
                ("cbr_bop_est", "bop_est.xlsx", "%bop\\_est.xlsx%", 51),
                ("cbr_bop_standard", "bal_of_payments_standart.xlsx",
                 "%bal\\_of\\_payments\\_standart.xlsx%", 34),
            )
            for bop_source, bop_file, bop_origin_like, bop_want in bop_catalogs:
                bop_catalog = text(await session.call_tool("run_query", {"query": f"SELECT count() AS n FROM v_series_catalog WHERE source = '{bop_source}' AND origin LIKE '{bop_origin_like}'"}))
                bop_catalog_rows = result_rows(bop_catalog)
                bop_catalog_n = bop_catalog_rows[0].get("n") if bop_catalog_rows else None
                if bop_catalog_rows is None:
                    bop_catalog_detail = f"ответ не распознан: {bop_catalog[:200]}"
                elif bop_catalog_n == bop_want:
                    bop_catalog_detail = f"рядов {bop_source} из {bop_file}: {bop_want}"
                else:
                    bop_catalog_detail = (f"рядов {bop_source} из {bop_file}: {bop_catalog_n}, want {bop_want} — "
                                          f"прогнать make import STAT={bop_source} (каталог перепубликовывается прогоном)")
                failed += not expect(f"каталог {bop_source} полон", bop_catalog_n == bop_want, bop_catalog_detail)

            # Сырые cbr_bop_est и cbr_bop_standard агенту не выданы — как cbr_int_rate_loans:
            # витрина — единственное место, где агент читает ряды контура (единицы двух наборов
            # различаются в 1000 раз — млрд и млн долларов США — и описаны только в
            # v_series_catalog). Таблицы создаёт DDL-прогон импортёра, поэтому UNKNOWN_TABLE
            # здесь — провал, а не успех: таблица обязана существовать и быть закрытой.
            for table, label in (("cbr_bop_est", "оценка ключевых агрегатов"),
                                 ("cbr_bop_standard", "стандартная презентация")):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                failed += not expect(f"сырая {table} закрыта ({label})", denied,
                                     f"отказ ClickHouse 497 на {table} (ожидаемо)" if denied else raw[:200])

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
                                       "rosstat_ipc_weeks_bulletins, rosstat_salaries_mes, "
                                       "rosstat_vvp_kvartal, rosstat_labour_force_mes")
            failed += not expect("витрина v_rosstat_macro непуста",
                                 (rosstat_points_n or 0) > 0 and (rosstat_series_n or 0) > 0, rosstat_view_detail)

            # Каталог описывает все 165 рядов источника: 4 ИПЦ (rosstat_ipc_mes) + 131 недельный
            # товар (rosstat_ipc_weeks) + 5 рядов ВВП (rosstat_vvp_kvartal) + 1 зарплата
            # (rosstat_salaries_mes) + 6 рядов рынка труда (rosstat_labour_force_mes) + 18 рядов
            # бюллетеней 2009–2021 (rosstat_ipc_weeks_bulletins): 1 агрегатный недельный ИПЦ
            # и 17 extra-рядов (ipcWeeksBulletinsExtraSeries), которым канонического соответствия
            # в XLSX-выгрузке нет («Сорочка верхняя для мальчиков школьного возраста» добавлена
            # расширением 2026-10-16, «Говядина тушеная» — расширением 2009–2015 в 2026-10-17),
            # — а канонические товары бюллетеней описаны общей метой 131 недельного товара и
            # новых строк каталога не дают.
            # Число сверяется точным равенством, а не «> 0»: односторонний контракт §11.4
            # стандарта требует описания каждого разобранного ряда, а меньше 165 значит
            # ни разу не отработавший импортёр (числа агент прочитает, а что измеряет ряд
            # и в каких единицах — нет). В плане фазы написано 138: это арифметика
            # 4 + 128 + 5 + 1 до fix-round задачи T7, который вернул три товара под обрывом
            # листа (fc0d28f) — в ipcWeeksProducts 131 название, а шести рядов рынка труда
            # в той арифметике нет вовсе; бюллетени добавили ещё 18, поэтому верное число — 165.
            # detail печатает наблюдённое число, когда оно расходится с ожиданием.
            rosstat_catalog = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'rosstat'"}))
            rosstat_catalog_rows = result_rows(rosstat_catalog)
            rosstat_catalog_n = rosstat_catalog_rows[0].get("n") if rosstat_catalog_rows else None
            if rosstat_catalog_rows is None:
                rosstat_catalog_detail = f"ответ не распознан: {rosstat_catalog[:200]}"
            elif rosstat_catalog_n == 165:
                rosstat_catalog_detail = ("рядов rosstat: 165 (4 ИПЦ + 131 недельный + 5 ВВП + 1 зарплата + "
                                          "6 рабочая сила + 18 бюллетеней)")
            else:
                rosstat_catalog_detail = (f"рядов rosstat: {rosstat_catalog_n}, want 165 — прогнать "
                                          "импортёры по одному: make import STAT=<имя> для каждого "
                                          "из rosstat_ipc_mes, rosstat_ipc_weeks, "
                                          "rosstat_ipc_weeks_bulletins, rosstat_salaries_mes, "
                                          "rosstat_vvp_kvartal, rosstat_labour_force_mes "
                                          "(каталог перепубликовывается каждым прогоном)")
            failed += not expect("каталог rosstat полон", rosstat_catalog_n == 165, rosstat_catalog_detail)

            # Сырые таблицы контура агенту не выданы — как macro_series и
            # cbr_households_b_mes: витрина — единственное место, где агент читает ряды
            # (единицы и привязка даты различаются между рядами и описаны в v_series_catalog).
            # До первого живого прогона импортёров таблиц может не быть вовсе, и запрос
            # отвечает UNKNOWN_TABLE, а не ACCESS_DENIED, — это провал, а не успех: таблица
            # обязана существовать и быть закрытой.
            for table, label in (("rosstat_ipc_mes", "ИПЦ помесячно"),
                                 ("rosstat_ipc_weeks", "недельные цены"),
                                 ("rosstat_ipc_weeks_bulletins", "недельные ИПЦ из бюллетеней"),
                                 ("rosstat_salaries_mes", "зарплата помесячно"),
                                 ("rosstat_vvp_kvartal", "ВВП поквартально"),
                                 ("rosstat_labour_force_mes", "рабочая сила и безработица")):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                failed += not expect(f"сырая {table} закрыта ({label})", denied,
                                     f"отказ ClickHouse 497 на {table} (ожидаемо)" if denied else raw[:200])

            # Агрегатный ряд бюллетеней виден агенту и непуст: он есть ровно в каждом выпуске
            # импортёра, поэтому ожидание точное — 502 точки (48 живых недельных выпусков
            # 2020-12-28…2021-12-27 + 454 Wayback-выпусков 2009–2021: расширения 2026-10-16/17 —
            # приложения В, Д и Е спеки, систематическая разведка scripts/harvest.py по всему
            # дереву Stg/ и снятие фильтра CDX from=2016, вернувшее 214 недель 2009–2015).
            # Меньшее число значит не доехавший прогон
            # rosstat_ipc_weeks_bulletins (или ещё идущий: точки публикуются по мере разбора
            # выпусков), поэтому detail печатает наблюдённое число; каталог этот разрыв не
            # поймает — он полон и до первых точек.
            rosstat_bulletins = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_rosstat_macro WHERE source = 'rosstat' AND series = 'Индекс потребительских цен (недельная оценка)'"}))
            rosstat_bulletins_rows = result_rows(rosstat_bulletins)
            rosstat_bulletins_n = rosstat_bulletins_rows[0].get("n") if rosstat_bulletins_rows else None
            if rosstat_bulletins_rows is None:
                rosstat_bulletins_detail = f"ответ не распознан: {rosstat_bulletins[:200]}"
            elif rosstat_bulletins_n == 502:
                rosstat_bulletins_detail = "точек агрегатного ряда: 502 (48 живых недель 2020-12-28…2021-12-27 + 454 Wayback-выпусков 2009–2021)"
            else:
                rosstat_bulletins_detail = (f"точек агрегатного ряда: {rosstat_bulletins_n}, want 502 — прогнать "
                                            "make import STAT=rosstat_ipc_weeks_bulletins")
            failed += not expect("агрегатный ряд бюллетеней rosstat непуст", rosstat_bulletins_n == 502, rosstat_bulletins_detail)

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
            sber_view = text(await session.call_tool(
                "run_query", {"query": "SELECT count() FROM v_bank_sber_spending"}))
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
                sber_source_raw = text(await session.call_tool(
                    "run_query",
                    {"query": f"SELECT count() AS n FROM v_bank_sber_spending "
                              f"WHERE source = '{sber_source}'"}))
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

            sber_sources_raw = text(await session.call_tool(
                "run_query",
                {"query": "SELECT uniqExact(source) AS sources FROM v_bank_sber_spending"}))
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
                sber_catalog = text(await session.call_tool(
                    "run_query",
                    {"query": f"SELECT count() AS n FROM v_series_catalog "
                              f"WHERE source = '{sber_source}'"}))
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

            # Банковский пакет (спека §3.6): витрина v_bank_macro читается агентом и НЕПУСТА, и в
            # ней есть точки каждого из трёх новых наборов — domrf_mortgage (ставка предложения по
            # ипотеке банков, ДОМ.РФ), rus_vtb_group_ifrs (отчётность группы ВТБ по МСФО) и
            # sber_finansovie_rezultaty (отчётность ПАО Сбербанк по РСБУ). Два набора СберИндекса
            # входят в ту же витрину, но проверены выше по v_bank_sber_spending и здесь не
            # повторяются. Ноль строк — не «нет данных у источника», а ни разу не отработавший
            # прогон bank/: строки публикуют импортёры пакета. Существование витрины подтверждается
            # ЗАПРОСОМ К НЕЙ, а не поиском имени в ответе list_tables: в том тексте лежат
            # create_table_query с комментариями таблиц и колонок, и имя отсутствующей витрины
            # попадало туда из COMMENT соседки (урок bold). «Нет витрины» (UNKNOWN_TABLE) и «нет
            # прав» (ACCESS_DENIED) приходят ошибкой и в result_rows не разбираются.
            bank_view_raw = text(await session.call_tool(
                "run_query", {"query": "SELECT count() FROM v_bank_macro"}))
            bank_view_rows = result_rows(bank_view_raw)
            bank_view_n = bank_view_rows[0].get("count()") if bank_view_rows else None
            if bank_view_rows is None:
                bank_view_detail = f"витрина не читается: {bank_view_raw[:200]}"
            elif (bank_view_n or 0) > 0:
                bank_view_detail = f"строк: {bank_view_n}"
            else:
                bank_view_detail = ("строк: 0 — прогнать импортёры пакета: make import "
                                    "STAT=domrf_mortgage, STAT=rus_vtb_group_ifrs, "
                                    "STAT=sber_finansovie_rezultaty (витрину пересоздаёт AfterImport "
                                    "каждого прогона)")
            failed += not expect("витрина v_bank_macro непуста", (bank_view_n or 0) > 0, bank_view_detail)

            # Одного общего count() > 0 мало: витрину пересоздаёт AfterImport ЛЮБОГО из пяти
            # импортёров, поэтому зелёная витрина могла бы держать точки одного набора — ровно
            # исходный дефект §4.1 роадмапа, только у банковского пакета. Поэтому наборы
            # проверяются поэлементно по source, который витрина штампует литералом (порядок
            # литералов закреплён тестом контракта bank/series_meta_test.go): это разом доказывает
            # и объединение наборов в одной витрине, и непустоту каждого из них.
            for bank_source in ("domrf_mortgage", "rus_vtb_group_ifrs", "sber_finansovie_rezultaty"):
                bank_source_raw = text(await session.call_tool(
                    "run_query",
                    {"query": f"SELECT count() AS n FROM v_bank_macro "
                              f"WHERE source = '{bank_source}'"}))
                bank_source_rows = result_rows(bank_source_raw)
                bank_source_n = bank_source_rows[0].get("n") if bank_source_rows else None
                if bank_source_rows is None:
                    bank_source_detail = f"ответ не распознан: {bank_source_raw[:200]}"
                elif (bank_source_n or 0) > 0:
                    bank_source_detail = f"точек: {bank_source_n}"
                else:
                    bank_source_detail = (f"точек: {bank_source_n} — прогнать импортёр набора: "
                                          f"make import STAT={bank_source}")
                failed += not expect(f"набор {bank_source} есть в витрине", (bank_source_n or 0) > 0, bank_source_detail)

            # Каталог маппера банковских метрик: описания рядов SBER_* и VTBR_* публикует сам
            # прогон bank_company_metrics (util.UpsertSeriesCatalog), поэтому ноль строк означает
            # ни разу не отработавший маппер — числа агент прочитает, а что измеряет ряд и в каких
            # единицах, нет. Число проверяется как «> 0», а не точным равенством: полноту словаря
            # (25 рядов: 14 SBER + 11 VTBR) держит TestBankCompanyMetricMapWellFormed, а здесь
            # доказывается доступность слоя агенту.
            bank_catalog_raw = text(await session.call_tool(
                "run_query",
                {"query": "SELECT count() AS n FROM v_series_catalog "
                          "WHERE source = 'bank_company_metrics'"}))
            bank_catalog = result_rows(bank_catalog_raw)
            bank_catalog_n = bank_catalog[0].get("n") if bank_catalog else None
            if bank_catalog is None:
                bank_catalog_detail = f"ответ не распознан: {bank_catalog_raw[:200]}"
            elif (bank_catalog_n or 0) > 0:
                bank_catalog_detail = f"рядов bank_company_metrics: {bank_catalog_n}"
            else:
                bank_catalog_detail = ("рядов: 0 — прогнать маппер: make import "
                                       "STAT=bank_company_metrics (каталог публикует прогон)")
            failed += not expect("каталог bank_company_metrics непуст", (bank_catalog_n or 0) > 0, bank_catalog_detail)

            # Компанейский слой (спека §3.7): курированные метрики банков лежат в общей
            # company_financials рядом с метриками Полюса, и агент читает их через уже выданную
            # v_company_financials — новых грантов слой не требует. Ноль строк по обеим компаниям —
            # ни разу не отработавший маппер; ошибка запроса даёт detail сырым текстом.
            bank_company_raw = text(await session.call_tool(
                "run_query",
                {"query": "SELECT count() AS n FROM v_company_financials "
                          "WHERE company IN ('SBER', 'VTBR')"}))
            bank_company = result_rows(bank_company_raw)
            bank_company_n = bank_company[0].get("n") if bank_company else None
            if bank_company is None:
                bank_company_detail = f"ответ не распознан: {bank_company_raw[:200]}"
            elif (bank_company_n or 0) > 0:
                bank_company_detail = f"строк SBER/VTBR: {bank_company_n}"
            else:
                bank_company_detail = "строк: 0 — прогнать маппер: make import STAT=bank_company_metrics"
            failed += not expect("метрики банков видны в v_company_financials", (bank_company_n or 0) > 0, bank_company_detail)

            # Фильтр по значению Enum доказывает больше, чем count() по company: тип колонки
            # витрина фиксирует на момент CREATE VIEW, поэтому ALTER таблицы (добавление 'M'=5
            # в приёмке фазы 3.1) без пересоздания витрин оставляет их на старом типе — фильтр
            # period_type = 'M' молча отдаёт 0 строк, toString(period_type) падает на 'Unexpected
            # value 5 in enum', и агент не видит банковских метрик, хотя строки по company
            # читаются. Ноль здесь — или маппер не прогонялся, или витрина старше ALTER'а:
            # пересоздать витрины прогоном make import STAT=company_views (идемпотентен,
            # пересоздаёт все три v_company_*).
            bank_month_raw = text(await session.call_tool(
                "run_query",
                {"query": "SELECT count() AS n FROM v_company_financials "
                          "WHERE company = 'SBER' AND period_type = 'M'"}))
            bank_month = result_rows(bank_month_raw)
            bank_month_n = bank_month[0].get("n") if bank_month else None
            if bank_month is None:
                bank_month_detail = f"ответ не распознан: {bank_month_raw[:200]}"
            elif (bank_month_n or 0) > 0:
                bank_month_detail = f"строк SBER с периодом 'M': {bank_month_n}"
            else:
                bank_month_detail = ("строк: 0 — или маппер не прогонялся, или витрина старше "
                                     "ALTER'а period_type: пересоздать витрины прогоном "
                                     "make import STAT=company_views")
            failed += not expect("период 'M' читается через v_company_financials", (bank_month_n or 0) > 0, bank_month_detail)

            # Одного count() > 0 мало: агенту нужен конкретный ряд с периодом и значением, а не
            # факт наличия строк. Читается последний отчётный месяц актива Сбера (ORDER BY period
            # DESC; период кодируется как YYYYMmm, поэтому лексикографический порядок совпадает с
            # хронологическим), detail печатает и период, и значение: пустой ответ — ни разу не
            # отработавший маппер, ошибка — сырым текстом.
            bank_assets_raw = text(await session.call_tool(
                "run_query",
                {"query": "SELECT metric, period, value FROM v_company_financials "
                          "WHERE company = 'SBER' AND metric = 'total_assets' "
                          "ORDER BY period DESC LIMIT 1"}))
            bank_assets = result_rows(bank_assets_raw)
            if bank_assets is None:
                bank_assets_detail = f"ответ не распознан: {bank_assets_raw[:200]}"
            elif bank_assets:
                bank_assets_detail = (f"total_assets за {bank_assets[0].get('period')}: "
                                      f"{bank_assets[0].get('value')}")
            else:
                bank_assets_detail = "строк нет — прогнать маппер: make import STAT=bank_company_metrics"
            failed += not expect("SBER total_assets читается точкой", bool(bank_assets), bank_assets_detail)

            # Форвардная витрина кредитного портфеля (пункт 4.2 №20 роадмапа, спека
            # 2026-10-17 §4): помесячные ряды SBER/VTBR из v_company_financials, дата
            # публикации из календаря bank_results и сравнение с «Кредитом экономике» ЦБ.
            # Строки публикует импортёр bank_company_metrics, а витрину создаёт
            # company_views, поэтому ноль означает, что не отработал хотя бы один из
            # прогонов, — detail называет тот, что создаёт саму витрину.
            loan_view = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_bank_loan_portfolio_forward"}))
            loan_view_rows = result_rows(loan_view)
            loan_view_n = loan_view_rows[0].get("n") if loan_view_rows else None
            if loan_view_rows is None:
                loan_view_detail = f"витрина не читается: {loan_view[:200]}"
            elif (loan_view_n or 0) > 0:
                loan_view_detail = f"строк: {loan_view_n}"
            else:
                loan_view_detail = "строк: 0 — прогнать make import STAT=company_views"
            failed += not expect("витрина v_bank_loan_portfolio_forward читается и непуста", (loan_view_n or 0) > 0, loan_view_detail)

            # Одного count() > 0 мало: агенту нужна конкретная точка с датой публикации,
            # ради которой витрина и заведена (опережающий срез до агрегата ЦБ). detail
            # печатает период, значение и publish_date; NULL здесь — не ошибка: история
            # дат публикации заводится постепенно, поэтому проверяется наличие строки,
            # а не заполненность publish_date.
            loan_point = text(await session.call_tool("run_query", {"query":
                "SELECT bank, metric, report_month, value_bln_rub, publish_date "
                "FROM v_bank_loan_portfolio_forward "
                "WHERE bank = 'SBER' AND metric = 'customer_loans' "
                "ORDER BY report_month DESC LIMIT 1"}))
            loan_point_rows = result_rows(loan_point)
            if loan_point_rows is None:
                loan_point_detail = f"ответ не распознан: {loan_point[:200]}"
            elif loan_point_rows:
                loan_point_detail = (f"SBER customer_loans за {loan_point_rows[0].get('report_month')}: "
                                     f"{loan_point_rows[0].get('value_bln_rub')}, "
                                     f"publish_date={loan_point_rows[0].get('publish_date')}")
            else:
                loan_point_detail = "строк нет — прогнать make import STAT=company_views"
            failed += not expect("SBER customer_loans читается точкой", bool(loan_point_rows), loan_point_detail)

            # publish_date витрины берётся из календаря bank_results, а он живёт сидом:
            # без сида дата всегда NULL, и агент не видит триггеров раскрытия. Не меньше
            # семи строк — это засеянный Q4 2026 (SBER/VTBR по месяцам раскрытия); порог, а
            # не равенство: новые банки будут добавлять строки, и проверка не должна ломаться.
            # Ноль называет сид, который надо применить.
            bank_results = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n FROM v_events_calendar WHERE category = 'bank_results'"}))
            bank_results_rows = result_rows(bank_results)
            bank_results_n = bank_results_rows[0].get("n") if bank_results_rows else None
            if bank_results_rows is None:
                bank_results_detail = f"ответ не распознан: {bank_results[:200]}"
            elif (bank_results_n or 0) >= 7:
                bank_results_detail = f"строк bank_results: {bank_results_n}"
            elif (bank_results_n or 0) == 0:
                bank_results_detail = ("строк: 0 — применить сид "
                                       "make ch-sql FILE=sql/events_calendar_bank_results_q4_2026.sql")
            else:
                bank_results_detail = (f"строк: {bank_results_n}, want >= 7 — проверить/применить сид "
                                       "make ch-sql FILE=sql/events_calendar_bank_results_q4_2026.sql")
            failed += not expect("календарь bank_results засеян", (bank_results_n or 0) >= 7, bank_results_detail)

            # Второй эмитент витрины: у VTBR (МСФО) ряд длиннее и содержит пропуски
            # месяцев, поэтому он же — точка поведенческой проверки ниже.
            vtb_point = text(await session.call_tool("run_query", {"query":
                "SELECT bank, metric, report_month, value_bln_rub, publish_date "
                "FROM v_bank_loan_portfolio_forward "
                "WHERE bank = 'VTBR' AND metric = 'customer_loans' "
                "ORDER BY report_month DESC LIMIT 1"}))
            vtb_point_rows = result_rows(vtb_point)
            if vtb_point_rows is None:
                vtb_point_detail = f"ответ не распознан: {vtb_point[:200]}"
            elif vtb_point_rows:
                vtb_point_detail = (f"VTBR customer_loans за {vtb_point_rows[0].get('report_month')}: "
                                    f"{vtb_point_rows[0].get('value_bln_rub')}, "
                                    f"publish_date={vtb_point_rows[0].get('publish_date')}")
            else:
                vtb_point_detail = "строк нет — прогнать make import STAT=company_views"
            failed += not expect("VTBR customer_loans читается точкой", bool(vtb_point_rows), vtb_point_detail)

            # Темпы считаются self-join по календарным месяцам, а не lagInFrame по строкам:
            # у VTBR нет 2025-02, поэтому YoY за 2026-02 обязан быть NULL. Старый lagInFrame
            # брал 12-ю строку назад и на пропуске месяца давал не NULL — эта проверка ловит
            # именно такую регрессию.
            skip_yoy = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n FROM v_bank_loan_portfolio_forward "
                "WHERE bank = 'VTBR' AND metric = 'customer_loans' "
                "AND report_month = '2026-02-01' AND yoy_pct IS NULL"}))
            skip_yoy_rows = result_rows(skip_yoy)
            skip_yoy_n = skip_yoy_rows[0].get("n") if skip_yoy_rows else None
            if skip_yoy_rows is None:
                skip_yoy_detail = f"ответ не распознан: {skip_yoy[:200]}"
            elif skip_yoy_n == 1:
                skip_yoy_detail = "yoy_pct за 2026-02 NULL при пропуске 2025-02 (self-join по месяцам)"
            else:
                skip_yoy_detail = ("строк: 0 — yoy_pct посчитан по строкам, а не по календарным "
                                   "месяцам; пересоздать витрину: make import STAT=company_views")
            failed += not expect("VTBR: пропуск месяца даёт NULL YoY", skip_yoy_n == 1, skip_yoy_detail)

            # Сырые таблицы банковского пакета агенту не выданы — как macro_series и
            # polyus_financial_metrics: ряды банковских отчётностей агент читает через витрину
            # v_bank_macro (сырые строки) и компанейский слой v_company_financials (курированные
            # метрики), наборы СберИндекса — через v_bank_sber_spending (единицы значения и
            # привязка даты описаны только в v_series_catalog). У трёх таблиц с шагом расписания
            # UNKNOWN_TABLE — провал, а не успех: их создаёт DDL-прогон импортёра, и они обязаны
            # существовать и быть закрытыми. `tbank_group_ifrs` — исключение: её импортёр убран
            # из расписания (О2) и всегда падает на PDF (О4), таблицу заводит только ручной
            # прогон `make import STAT=tbank_group_ifrs`, поэтому UNKNOWN_TABLE здесь —
            # ожидаемое состояние с пометкой «таблица появится после ручного прогона (О2)»:
            # иначе make mcp-check был бы красным на каждой БД без этого прогона. ACCESS_DENIED
            # остаётся успехом и здесь (таблица есть и закрыта), любой иной ответ — провал.
            for table, label in (("domrf_mortgage", "ставки по ипотеке ДОМ.РФ"),
                                 ("rus_vtb_group_ifrs", "МСФО группы ВТБ"),
                                 ("sber_finansovie_rezultaty", "РСБУ Сбера"),
                                 ("tbank_group_ifrs", "МСФО Т-Банка, шага в расписании нет")):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                tbank_absent = table == "tbank_group_ifrs" and "UNKNOWN_TABLE" in raw
                if denied:
                    raw_detail = f"отказ ClickHouse 497 на {table} (ожидаемо)"
                elif tbank_absent:
                    raw_detail = f"{table} отсутствует — таблица появится после ручного прогона (О2)"
                else:
                    raw_detail = raw[:200]
                failed += not expect(f"сырая {table} закрыта ({label})", denied or tbank_absent, raw_detail)

            # Контур приложений к бюллетеню ДИП ЦБ (пункт 4.2 №17 роадмапа — фаза 4, винтажный
            # слой; спека 2026-10-15 cbr-dip-bulletin §3.6, импортёр cbr_dip_bulletin): витрина
            # v_cbr_dip_bulletin читается агентом и НЕПУСТА, каталог описывает все 8 рядов
            # источника, а сырая cbr_dip_bulletin — источник витрины — агенту закрыта (как
            # macro_series и cbr_bop_est: читать контур агент обязан только через витрину,
            # правило 11 AGENTS.md).
            #
            # Здесь, в отличие от v_dcf_assumptions, значение count проверяется: строки
            # публикует импортёр cbr/, поэтому пустая витрина означает ни разу не отработавший
            # шаг cbr_dip_bulletin (набор выпусков импортируется циклом, каждый выпуск — своей
            # строкой версии), а не отсутствие данных у источника. Существование витрины
            # подтверждается ЗАПРОСОМ К НЕЙ, а не поиском имени в ответе list_tables: в том
            # тексте лежат create_table_query с комментариями таблиц и колонок, и имя
            # отсутствующей витрины попадало туда из COMMENT соседки (урок bold). Разобранный
            # ответ run_query доказывает и существование, и право чтения; «нет витрины» и «нет
            # прав» приходят ошибкой (UNKNOWN_TABLE / ACCESS_DENIED) и в result_rows не
            # разбираются.
            dip_view = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_cbr_dip_bulletin"}))
            dip_view_rows = result_rows(dip_view)
            dip_view_n = dip_view_rows[0].get("count()") if dip_view_rows else None
            if dip_view_rows is None:
                dip_view_detail = (f"витрина не читается: {dip_view[:200]} — на пустой БД её создаёт "
                                   "прогон импортёра (make import STAT=cbr_dip_bulletin)")
            elif (dip_view_n or 0) > 0:
                dip_view_detail = f"строк: {dip_view_n}"
            else:
                dip_view_detail = ("строк: 0 — прогнать импортёр контура: make import "
                                   "STAT=cbr_dip_bulletin (витрину пересоздаёт AfterImport прогона)")
            failed += not expect("витрина v_cbr_dip_bulletin существует и непуста", (dip_view_n or 0) > 0, dip_view_detail)

            # Каталог обязан описывать все 8 объявленных рядов — точным равенством, а не «> 0»
            # (односторонний контракт §11.4: каждый разобранный ряд обязан иметь описание).
            # 8 = 4 SAAR роста цен (все товары и услуги, продовольствие, непродовольствие,
            # услуги) + 2 безработицы (SA и NSA) + 2 занятости (SA и NSA); имена рядов объявляет
            # код (спека §3.2). Меньшее число значит ни разу не отработавший шаг cbr_dip_bulletin
            # (числа агент прочитает, а что измеряет ряд, в каких единицах и как привязана дата
            # точки — нет); большее — ряд, которого нет в объявленном наборе.
            #
            # Выборка по одному source, без origin: у контура один импортёр и одна страница
            # выпусков, поэтому source уникален, а origin восьми записей различается только
            # листом и подписью колонки — файл-то один. (У пары cbr_bop_est / cbr_bop_standard
            # наоборот: два разных файла, и там origin обязателен — как и у cbr_int_rate_loans.)
            dip_catalog = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'cbr_dip_bulletin'"}))
            dip_catalog_rows = result_rows(dip_catalog)
            dip_catalog_n = dip_catalog_rows[0].get("n") if dip_catalog_rows else None
            if dip_catalog_rows is None:
                dip_catalog_detail = f"ответ не распознан: {dip_catalog[:200]}"
            elif dip_catalog_n == 8:
                dip_catalog_detail = "рядов cbr_dip_bulletin: 8 (4 SAAR + 2 безработицы + 2 занятости)"
            else:
                dip_catalog_detail = (f"рядов cbr_dip_bulletin: {dip_catalog_n}, want 8 (4 SAAR + "
                                      "2 безработицы + 2 занятости) — прогнать make import "
                                      "STAT=cbr_dip_bulletin (каталог перепубликовывается прогоном)")
            failed += not expect("каталог cbr_dip_bulletin полон", dip_catalog_n == 8, dip_catalog_detail)

            # Винтажность — то, ради чего у витрины есть колонки release, release_id и
            # publication_date (спека §3.6): агент читает точку как «значение, каким его
            # опубликовал выпуск release от publication_date», а без этих колонок ревизии
            # SA-рядов выглядят фактами. Одной непустоты витрины выше мало: count() > 0 пройдёт
            # и на срезе из одного выпуска. Поэтому оба свойства проверяются разом: выпусков в
            # витрине больше одного (uniqExact(release_id) > 1) и винтажные колонки заполнены.
            # Пустота release — это '', пустота publication_date — 1970-01-01 (toDate(0)):
            # настоящей датой публикации выпуска он быть не может, а ссылку без
            # data-tooltip-content импортёр считает fail-fast (спека §3.4), поэтому пустое
            # значение здесь — не разобранный винтаж, а не свойство источника.
            dip_vintage = text(await session.call_tool("run_query", {"query":
                "SELECT uniqExact(release_id) AS releases, "
                "countIf(release = '') AS blank_release, "
                "countIf(publication_date = toDate(0)) AS blank_publication "
                "FROM v_cbr_dip_bulletin"}))
            dip_vintage_rows = result_rows(dip_vintage)
            dip_releases_n = dip_vintage_rows[0].get("releases") if dip_vintage_rows else None
            dip_blank_release_n = dip_vintage_rows[0].get("blank_release") if dip_vintage_rows else None
            dip_blank_publication_n = dip_vintage_rows[0].get("blank_publication") if dip_vintage_rows else None
            dip_vintage_ok = ((dip_releases_n or 0) > 1 and dip_blank_release_n == 0
                              and dip_blank_publication_n == 0)
            if dip_vintage_rows is None:
                dip_vintage_detail = f"витрина не читается: {dip_vintage[:200]}"
            elif dip_vintage_ok:
                dip_vintage_detail = (f"выпусков: {dip_releases_n}, пустых release: 0, "
                                      "пустых publication_date: 0")
            else:
                dip_vintage_detail = (f"выпусков: {dip_releases_n} (want > 1), пустых release: "
                                      f"{dip_blank_release_n}, пустых publication_date: "
                                      f"{dip_blank_publication_n} (want 0) — прогнать make import "
                                      "STAT=cbr_dip_bulletin (винтаж разбирается прогоном)")
            failed += not expect("витрина v_cbr_dip_bulletin винтажна", dip_vintage_ok, dip_vintage_detail)

            # Сырая cbr_dip_bulletin агенту не выдана — как macro_series и cbr_bop_est: витрина —
            # единственное место, где агент читает точки контура (винтажные колонки
            # release/release_id/publication_date и комментарии есть только у неё, а ключ сырой
            # таблицы — (series, date, release_id)). Таблицу создаёт DDL-прогон импортёра,
            # поэтому UNKNOWN_TABLE здесь — провал, а не успех: таблица обязана существовать и
            # быть закрытой.
            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM cbr_dip_bulletin"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырая cbr_dip_bulletin закрыта", denied,
                                 "отказ ClickHouse 497 на cbr_dip_bulletin (ожидаемо)" if denied else raw[:200])

            # Документы e-disclosure.ru (пункт dcf-polyus/phase-3-remaining-edisclosure-documents,
            # спека 2026-10-18 §6, импортёр edisclosure_documents): витрина v_edisclosure_documents
            # читается агентом и НЕПУСТА, сырая edisclosure_documents агенту закрыта (как
            # macro_series и cbr_dip_bulletin: документы агент читает только через витрину,
            # правило 11 AGENTS.md). Строки публикует импортёр, поэтому пустая витрина — провал:
            # это ни разу не отработавший приёмочный прогон (снапшот коллектора не импортирован),
            # а не отсутствие данных у источника. Нечитаемая витрина — тоже провал: нет вида/гранта.
            edisclosure_view = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n FROM v_edisclosure_documents"}))
            edisclosure_view_rows = result_rows(edisclosure_view)
            edisclosure_view_n = edisclosure_view_rows[0].get("n") if edisclosure_view_rows else None
            if edisclosure_view_rows is None:
                edisclosure_view_detail = (f"витрина не читается: {edisclosure_view[:200]} — на пустой БД её "
                                           "создаёт прогон импортёра (make import STAT=edisclosure_documents)")
            elif (edisclosure_view_n or 0) > 0:
                edisclosure_view_detail = f"строк: {edisclosure_view_n}"
            else:
                edisclosure_view_detail = ("строк: 0 — снапшот коллектора не импортирован: "
                                           "make edisclosure-login / make edisclosure-collect, затем "
                                           "make import STAT=edisclosure_documents")
            failed += not expect("витрина v_edisclosure_documents существует и непуста",
                                 (edisclosure_view_n or 0) > 0, edisclosure_view_detail)

            # Сырую таблицу создаёт DDL-прогон импортёра, поэтому UNKNOWN_TABLE здесь — провал,
            # а не успех: таблица обязана существовать и быть закрытой.
            raw = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM edisclosure_documents"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырая edisclosure_documents закрыта", denied,
                                 "отказ ClickHouse 497 на edisclosure_documents (ожидаемо)" if denied else raw[:200])

            # Комментарии проверяются по карточке самой витрины в разобранном list_tables, а не
            # вхождением строки в общий текст ответа: в тексте соседних витрин лежат комментарии
            # их колонок (урок bold), и «sha256 где-то в ответе» дало бы ложное OK. Имя витрины
            # уже подтверждено запросом к ней выше, поэтому отсутствие карточки здесь — неполный
            # ответ list_tables (или выключенные detailed columns), а не «витрины нет».
            # list_tables запрашивается отдельно с фильтром like: общий вызов выше отдаёт
            # первую страницу (50 объектов), и карточка витрины могла не попасть в неё —
            # фильтр по имени и page_size=500 убирают зависимость от пагинации.
            edisclosure_tables_raw = text(await session.call_tool(
                "list_tables", {"database": "default", "like": "v_edisclosure_documents",
                                "page_size": 500}))
            try:
                listed_tables = json.loads(edisclosure_tables_raw)
            except ValueError:
                listed_tables = None
            edisclosure_listed = None
            if isinstance(listed_tables, dict):
                edisclosure_listed = next((t for t in listed_tables.get("tables", [])
                                           if t.get("name") == "v_edisclosure_documents"), None)
            if edisclosure_listed is None:
                failed += not expect("комментарии витрины v_edisclosure_documents", False,
                                     "list_tables не отдал карточку витрины с комментариями")
            else:
                edisclosure_comment = edisclosure_listed.get("comment") or ""
                edisclosure_columns = {c.get("name"): (c.get("comment") or "")
                                       for c in edisclosure_listed.get("columns", [])}
                edisclosure_missing = [col for col in ("sha256", "archive_key", "publication_date")
                                       if not edisclosure_columns.get(col)]
                edisclosure_comments_ok = "e-disclosure" in edisclosure_comment and not edisclosure_missing
                edisclosure_comments_detail = (
                    f"комментарий таблицы: {'e-disclosure' if 'e-disclosure' in edisclosure_comment else 'без e-disclosure'}, "
                    f"колонки без комментария: {', '.join(edisclosure_missing) if edisclosure_missing else 'нет'}")
                failed += not expect("комментарии витрины v_edisclosure_documents",
                                     edisclosure_comments_ok, edisclosure_comments_detail)

            # Выборочная точка контура: строка PLZL/IFRS с непустыми sha256 и archive_key.
            # Строка обязательна после живого импорта — снапшот приёмки включает все три компании
            # и все четыре раздела, поэтому её отсутствие (как и пустая витрина) — провал: это
            # неполный снапшот, а не отсутствие данных у источника.
            if edisclosure_view_rows is not None:
                edisclosure_point = text(await session.call_tool("run_query", {"query":
                    "SELECT ticker, section, fileid, sha256, archive_key FROM v_edisclosure_documents "
                    "WHERE ticker = 'PLZL' AND section = 'IFRS' LIMIT 1"}))
                edisclosure_point_rows = result_rows(edisclosure_point)
                if edisclosure_point_rows is None:
                    failed += not expect("документ PLZL IFRS с версией и архивом", False,
                                         f"ответ не распознан: {edisclosure_point[:200]}")
                elif edisclosure_point_rows:
                    point = edisclosure_point_rows[0]
                    point_fileid = point.get("fileid")
                    point_sha = point.get("sha256") or ""
                    point_key = point.get("archive_key") or ""
                    point_sha_detail = (point_sha[:16] + "…") if point_sha else "пусто"
                    point_key_detail = (point_key[:80] + "…") if point_key else "пусто"
                    failed += not expect("документ PLZL IFRS с версией и архивом",
                                         bool(point_sha) and bool(point_key),
                                         f"fileid={point_fileid}, sha256={point_sha_detail}, "
                                         f"archive_key={point_key_detail}")
                else:
                    failed += not expect("документ PLZL IFRS с версией и архивом", False,
                                         "строки PLZL/IFRS нет — неполный снапшот: собрать PLZL и "
                                         "раздел IFRS (make edisclosure-collect) и прогнать "
                                         "make import STAT=edisclosure_documents")

            # Контур диапазонов среднесрочного прогноза ЦБ (пункт 4.2 №7 роадмапа KEY_RATE — эталон
            # ЦБ, фаза 2; спека 2026-10-17 cbr-mt-forecast §3.7, импортёр cbr_mt_forecast): витрина
            # v_cbr_mt_forecast читается агентом и НЕПУСТА, каталог описывает все 16 объявленных
            # показателей, витрина винтажна (больше одного выпуска, даты выпуска и заседания
            # заполнены), а сырая cbr_mt_forecast — источник витрины — агенту закрыта (как
            # macro_series и cbr_dip_bulletin: читать контур агент обязан только через витрину,
            # правило 11 AGENTS.md).
            #
            # Здесь, в отличие от v_dcf_assumptions, значение count проверяется: строки публикует
            # импортёр cbr/, поэтому пустая витрина означает ни разу не отработавший шаг
            # cbr_mt_forecast (выпуски импортируются циклом, каждый выпуск — своей строкой
            # версии), а не отсутствие данных у источника. Существование витрины подтверждается
            # ЗАПРОСОМ К НЕЙ, а не поиском имени в ответе list_tables: в том тексте лежат
            # create_table_query с комментариями таблиц и колонок, и имя отсутствующей витрины
            # попадало туда из COMMENT соседки (урок bold). Разобранный ответ run_query
            # доказывает и существование, и право чтения; «нет витрины» и «нет прав» приходят
            # ошибкой (UNKNOWN_TABLE / ACCESS_DENIED) и в result_rows не разбираются.
            mt_view = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_cbr_mt_forecast"}))
            mt_view_rows = result_rows(mt_view)
            mt_view_n = mt_view_rows[0].get("count()") if mt_view_rows else None
            if mt_view_rows is None:
                mt_view_detail = (f"витрина не читается: {mt_view[:200]} — на пустой БД её создаёт "
                                  "прогон импортёра (make import STAT=cbr_mt_forecast)")
            elif (mt_view_n or 0) > 0:
                mt_view_detail = f"строк: {mt_view_n}"
            else:
                mt_view_detail = ("строк: 0 — прогнать импортёр контура: make import "
                                  "STAT=cbr_mt_forecast (витрину пересоздаёт прогон)")
            failed += not expect("витрина v_cbr_mt_forecast существует и непуста", (mt_view_n or 0) > 0, mt_view_detail)

            # Каталог обязан описывать все 16 объявленных показателей — точным равенством, а не
            # «> 0» (односторонний контракт §11.4: каждый разобранный ряд обязан иметь описание).
            # 16 — число строк «Табл 1», объявленных кодом (cbr/mt_forecast_meta.go): 3 core (ИПЦ
            # дек. к дек., ИПЦ в среднем за год, среднегодовая ключевая ставка — те, ради которых
            # пункт существует) + 13 остальных (ВВП ×2, расходы на потребление ×2, валовое
            # накопление ×2, экспорт, импорт, М2, требования банковской системы ×4). Меньшее число
            # значит ни разу не отработавший шаг cbr_mt_forecast (числа агент прочитает, а что
            # измеряет ряд, в каких единицах и как читать пару lo/hi — нет); большее — ряд вне
            # объявленного набора (нормализация подписи в реестр не свелась бы: чужая подпись —
            # пропуск с предупреждением, а не новая запись каталога).
            #
            # Выборка по одному source, без origin: у контура один импортёр и одна книга выпуска,
            # поэтому source уникален (как у cbr_dip_bulletin), а origin шестнадцати записей
            # различается только строкой таблицы. Шесть кодов (cpi_dec, cpi_avg, key_rate_avg,
            # gdp_yoy, exports, imports) совпадают с именами рядов макроопроса ЦБ (cbr_mo_br), но
            # source у них другой — на этот счётчик консенсус аналитиков не влияет.
            mt_catalog = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'cbr_mt_forecast'"}))
            mt_catalog_rows = result_rows(mt_catalog)
            mt_catalog_n = mt_catalog_rows[0].get("n") if mt_catalog_rows else None
            if mt_catalog_rows is None:
                mt_catalog_detail = f"ответ не распознан: {mt_catalog[:200]}"
            elif mt_catalog_n == 16:
                mt_catalog_detail = "рядов cbr_mt_forecast: 16 (3 core + 13 строк «Табл 1»)"
            else:
                mt_catalog_detail = (f"рядов cbr_mt_forecast: {mt_catalog_n}, want 16 (3 core + 13 "
                                     "строк «Табл 1») — прогнать make import STAT=cbr_mt_forecast "
                                     "(каталог перепубликовывается прогоном)")
            failed += not expect("каталог cbr_mt_forecast полон", mt_catalog_n == 16, mt_catalog_detail)

            # Винтажность — то, ради чего у витрины есть колонки release_date и meeting_date (спека
            # §3.7): агент читает диапазон как «прогноз, каким его зафиксировал выпуск
            # release_date», а без этих колонок ревизии выглядели бы фактами. Одной непустоты
            # витрины выше мало: count() > 0 пройдёт и на срезе из одного выпуска. Поэтому
            # проверяются обе стороны разом: выпусков больше одного (uniqExact(release_date) > 1)
            # и винтажные даты заполнены. Пустая дата — 1970-01-01 (toDate(0)): настоящей датой
            # публикации выпуска или датой заседания она быть не может, а обе даты импортёр считает
            # fail-fast (дата в слаге и в карточке, дата заседания — в шапке блока индекса, спека
            # §3.4), поэтому пустое значение здесь — не разобранный винтаж, а не свойство источника.
            mt_vintage = text(await session.call_tool("run_query", {"query":
                "SELECT uniqExact(release_date) AS releases, "
                "countIf(release_date = toDate(0)) AS blank_release, "
                "countIf(meeting_date = toDate(0)) AS blank_meeting "
                "FROM v_cbr_mt_forecast"}))
            mt_vintage_rows = result_rows(mt_vintage)
            mt_releases_n = mt_vintage_rows[0].get("releases") if mt_vintage_rows else None
            mt_blank_release_n = mt_vintage_rows[0].get("blank_release") if mt_vintage_rows else None
            mt_blank_meeting_n = mt_vintage_rows[0].get("blank_meeting") if mt_vintage_rows else None
            mt_vintage_ok = ((mt_releases_n or 0) > 1 and mt_blank_release_n == 0
                             and mt_blank_meeting_n == 0)
            if mt_vintage_rows is None:
                mt_vintage_detail = f"витрина не читается: {mt_vintage[:200]}"
            elif mt_vintage_ok:
                mt_vintage_detail = (f"выпусков: {mt_releases_n}, пустых release_date: 0, "
                                     "пустых meeting_date: 0")
            else:
                mt_vintage_detail = (f"выпусков: {mt_releases_n} (want > 1), пустых release_date: "
                                     f"{mt_blank_release_n}, пустых meeting_date: "
                                     f"{mt_blank_meeting_n} (want 0) — прогнать make import "
                                     "STAT=cbr_mt_forecast (винтаж разбирается прогоном)")
            failed += not expect("витрина v_cbr_mt_forecast винтажна", mt_vintage_ok, mt_vintage_detail)

            # Сырая cbr_mt_forecast агенту не выдана — как macro_series и cbr_dip_bulletin: витрина —
            # единственное место, где агент читает прогнозы контура (винтажные колонки release_date и
            # meeting_date и комментарии есть только у неё, а ключ сырой таблицы — (indicator, year,
            # release_date)). Таблицу создаёт DDL-прогон импортёра, поэтому UNKNOWN_TABLE здесь —
            # провал, а не успех: таблица обязана существовать и быть закрытой.
            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM cbr_mt_forecast"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырая cbr_mt_forecast закрыта", denied,
                                 "отказ ClickHouse 497 на cbr_mt_forecast (ожидаемо)" if denied else raw[:200])

            # Сырые остатки формы 101 ЦБ (первый срез — Сбер, REGN=1481, пункт 4.2 №21;
            # спека docs/superpowers/specs/2026-10-17-cbr-form101-design.md §6). Проверки —
            # по витрине, а не по list_tables: имя читается запросом к самой витрине
            # (урок bold-блока), вторая проверка пинит, что первый срез — именно строки
            # Сбера за 2026-09-01, а не любая непустота (схема готова к другим regnum).
            form101_view = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_cbr_form101"}))
            form101_view_rows = result_rows(form101_view)
            form101_view_n = form101_view_rows[0].get("n") if form101_view_rows else None
            if form101_view_rows is None:
                form101_view_detail = f"ответ не распознан: {form101_view[:200]}"
            elif (form101_view_n or 0) > 0:
                form101_view_detail = f"строк: {form101_view_n}"
            else:
                form101_view_detail = ("строк: 0 — прогнать импортёр: make import "
                                       "STAT=cbr_form101 (витрину пересоздаёт прогон)")
            failed += not expect("витрина v_cbr_form101 существует и непуста",
                                 (form101_view_n or 0) > 0, form101_view_detail)

            # Строки Сбера за отчётную дату первого среза: regnum=1481 и report_date
            # '2026-09-01' (дата из имени файла 101-20260901.rar, а не месяц публикации).
            # Ноль строк значит, что витрина наполнена другим банком или другой датой, —
            # для приёмки пункта это провал, а не «данные есть».
            form101_sber = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n FROM v_cbr_form101 WHERE regnum = 1481 AND report_date = '2026-09-01'"}))
            form101_sber_rows = result_rows(form101_sber)
            form101_sber_n = form101_sber_rows[0].get("n") if form101_sber_rows else None
            if form101_sber_rows is None:
                form101_sber_detail = f"ответ не распознан: {form101_sber[:200]}"
            elif (form101_sber_n or 0) > 0:
                form101_sber_detail = f"строк Сбера за 2026-09-01: {form101_sber_n}"
            else:
                form101_sber_detail = ("строк Сбера за 2026-09-01: 0 — прогнать make import "
                                       "STAT=cbr_form101 (первый срез — REGN=1481)")
            failed += not expect("витрина v_cbr_form101 содержит строки Сбера за 2026-09-01",
                                 (form101_sber_n or 0) > 0, form101_sber_detail)

            # Pilot-маппинг формы 101 на кредиты ЮЛ/ФЛ (issue #98): витрина должна
            # отдавать калибровку за август 2026 (report_date 2026-09-01) и
            # эталон пресс-релиза из company_financials. Ноль строк — не
            # «нет данных», а не отработавший импортёр/сид.
            form101_loans = text(await session.call_tool("run_query", {
                "query": "SELECT corporate_loans_bln_rub, retail_loans_bln_rub, "
                         "press_release_corporate_bln_rub, press_release_retail_bln_rub, "
                         "corporate_diff_pct, retail_diff_pct, mapping_version "
                         "FROM v_cbr_form101_loans WHERE report_date = '2026-09-01' LIMIT 1"}))
            form101_loans_rows = result_rows(form101_loans)
            form101_loans_ok = False
            if form101_loans_rows is None:
                form101_loans_detail = f"ответ не распознан: {form101_loans[:200]}"
            elif form101_loans_rows:
                row = form101_loans_rows[0]
                try:
                    corp_diff = float(row.get("corporate_diff_pct"))
                    ret_diff = float(row.get("retail_diff_pct"))
                except (TypeError, ValueError):
                    corp_diff = ret_diff = None
                form101_loans_ok = (
                    row.get("mapping_version") == "cbr-809p-sber-1"
                    and corp_diff is not None and abs(corp_diff) <= 0.01
                    and ret_diff is not None and abs(ret_diff) <= 0.01)
                form101_loans_detail = (
                    f"ЮЛ {row.get('corporate_loans_bln_rub')} / "
                    f"{row.get('press_release_corporate_bln_rub')} "
                    f"({row.get('corporate_diff_pct')}), ФЛ "
                    f"{row.get('retail_loans_bln_rub')} / "
                    f"{row.get('press_release_retail_bln_rub')} "
                    f"({row.get('retail_diff_pct')}), "
                    f"{row.get('mapping_version')}")
            else:
                form101_loans_detail = ("строк нет — прогнать make import "
                                        "STAT=cbr_form101 и сид 2026M07")
            failed += not expect("v_cbr_form101_loans отдаёт cbr-809p-sber-1",
                                 form101_loans_ok, form101_loans_detail)

            # Ras-метрики cbr-809p-sber-1 формы 101 должны доезжать до форвардной витрины:
            # июль и август SBER — source_kind='ras' (июль — сид, август — импортёр).
            form101_fwd = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n FROM v_bank_loan_portfolio_forward "
                         "WHERE bank = 'SBER' AND source_kind = 'ras' "
                         "AND report_month IN ('2026-07-01', '2026-08-01') "
                         "AND metric = 'loans_corporations'"}))
            form101_fwd_rows = result_rows(form101_fwd)
            form101_fwd_n = form101_fwd_rows[0].get("n") if form101_fwd_rows else None
            if form101_fwd_rows is None:
                form101_fwd_detail = f"ответ не распознан: {form101_fwd[:200]}"
            elif (form101_fwd_n or 0) >= 2:
                form101_fwd_detail = f"строк SBER ras за июль/август: {form101_fwd_n}"
            else:
                form101_fwd_detail = ("строк: 0/1 — применить сид 2026-07 и прогнать "
                                      "make import STAT=cbr_form101")
            failed += not expect("cbr-809p-sber-1 ras виден в v_bank_loan_portfolio_forward",
                                 (form101_fwd_n or 0) >= 2, form101_fwd_detail)

            # Сырая cbr_form101 агенту не выдана — как macro_series и cbr_dip_bulletin:
            # витрина — единственное место, где агент читает остатки по счетам (комментарии
            # таблицы и каждой колонки есть только у неё). Таблицу создаёт DDL-прогон
            # импортёра, поэтому UNKNOWN_TABLE здесь — провал, а не успех.
            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM cbr_form101"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырая cbr_form101 закрыта", denied,
                                 "отказ ClickHouse 497 на cbr_form101 (ожидаемо)" if denied else raw[:200])

            # Реестр ML-экспериментов (infra/4.1): агенту выданы три витрины, а сырые
            # ml_runs/ml_predictions закрыты (как macro_series и bold_fund_snapshot:
            # паспорт прогона и fold-предсказания читаются только через витрины).
            # Существование витрины подтверждается ЗАПРОСОМ К НЕЙ, а не только поиском
            # имени в list_tables: в том тексте лежат create_table_query с комментариями
            # таблиц и колонок, и имя отсутствующей витрины попадало туда из COMMENT
            # соседки (урок bold). «Нет витрины» (UNKNOWN_TABLE) и «нет прав»
            # (ACCESS_DENIED) приходят ошибкой и в result_rows не разбираются.
            for view in ("v_ml_runs", "v_ml_leaderboard", "v_ml_error_by_fold"):
                failed += not expect(f"витрина {view} видна", view in tables, "list_tables default")

            for view, label in (("v_ml_runs", "паспорт прогона"),
                                ("v_ml_error_by_fold", "fold-предсказания")):
                ml_view = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {view}"}))
                ml_view_rows = result_rows(ml_view)
                ml_view_n = ml_view_rows[0].get("count()") if ml_view_rows else None
                if ml_view_rows is None:
                    ml_view_detail = f"витрина не читается: {ml_view[:200]}"
                else:
                    ml_view_detail = f"строк: {ml_view_n}"
                failed += not expect(f"витрина {view} существует и читается ({label})",
                                     bool(ml_view_rows), ml_view_detail)

            # Пустой реестр — информационная строка, а не FAIL: до первого прогона
            # ML_LOG=1 make cpi-nowcast (или ML_LOG=1 make key-rate-model) таблицы
            # ml_runs/ml_predictions пусты, и make mcp-check на чистой БД не должен
            # краснеть. Нечитаемая витрина — провал: значит нет ни вида, ни гранта.
            ml_board = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_ml_leaderboard"}))
            ml_board_rows = result_rows(ml_board)
            if ml_board_rows is None:
                failed += not expect("витрина v_ml_leaderboard существует и читается", False,
                                     f"витрина не читается: {ml_board[:200]}")
            elif (ml_board_rows[0].get("n") or 0) > 0:
                print(f"OK   реестр ML: строк лидерборда: {ml_board_rows[0].get('n')}")
            else:
                print("WARN реестр ML пуст — прогонов с ML_LOG=1 ещё не было "
                      "(ML_LOG=1 make cpi-nowcast / ML_LOG=1 make key-rate-model)")

            # Сырые таблицы реестра агенту не выданы — как macro_series и news_events:
            # паспорт прогона и fold-предсказания читаются только через витрины.
            # Таблицы создаёт schema-only прогон ml_registry (make import STAT=ml_registry),
            # поэтому UNKNOWN_TABLE здесь — провал, а не успех: таблица обязана
            # существовать и быть закрытой.
            for table, label in (("ml_runs", "паспорт прогона"),
                                 ("ml_predictions", "fold-предсказания")):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                failed += not expect(f"сырая {table} закрыта ({label})", denied,
                                     f"отказ ClickHouse 497 на {table} (ожидаемо)" if denied else raw[:200])

    return failed


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
