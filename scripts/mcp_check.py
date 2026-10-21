import asyncio
import json
import os
import sys
import tempfile
from datetime import date, timedelta

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

            # page_size: у list_tables страница по умолчанию — 50 записей; после добавления
            # трёх витрин point-in-time слоя v_rosstat_macro ушла на вторую страницу, и
            # проверки вида «имя есть в ответе» ложно падали. Одна страница на 1000 записей
            # покрывает схему dev-БД с запасом.
            tables = text(await session.call_tool("list_tables", {"database": "default", "page_size": 1000}))
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

            # Рыночные ряды MOEX (пункт key-rate/phase-2-brent, #113): витрина
            # v_moex_market читается агентом, сырая moex_market закрыта. Ожидается
            # минимум brent_front; RVI и медь появятся следующим пунктом.
            moex_market_raw = text(await session.call_tool("run_query", {
                "query": "SELECT series, count() AS rows, min(date) AS min_date, max(date) AS max_date "
                         "FROM v_moex_market GROUP BY series ORDER BY series"}))
            moex_market_rows = result_rows(moex_market_raw)
            if moex_market_rows is None:
                moex_market_detail = f"витрина не читается: {moex_market_raw[:200]}"
            else:
                moex_market_detail = "; ".join(
                    f"{r.get('series')}: {r.get('rows')} ({r.get('min_date')}…{r.get('max_date')})"
                    for r in moex_market_rows) or "в ответе нет строк"
            failed += not expect("витрина v_moex_market существует и непуста",
                                 bool(moex_market_rows), moex_market_detail)
            if moex_market_rows:
                failed += not expect("v_moex_market: есть brent_front",
                                     any(r.get("series") == "brent_front" for r in moex_market_rows),
                                     moex_market_detail)
            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM moex_market"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырая moex_market закрыта", denied,
                                 "отказ ClickHouse 497 на moex_market (ожидаемо)" if denied else raw[:200])

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
                                 bool(kr_meeting)
                                 and kr_meeting[0].get("dashboard_date") is not None
                                 and kr_meeting[0].get("dashboard_date") == kr_meeting[0].get("calendar_date"),
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
                probabilities = kr_track[0].get("probabilities") or ""
                try:
                    distribution = json.loads(probabilities)
                except ValueError:
                    distribution = {}
                kr_track_ok = (isinstance(distribution, dict)
                               and {"cut", "hold", "hike"} <= set(distribution)
                               and (kr_track[0].get("model") or "") != "")
                kr_track_detail = (f"probabilities={'есть' if kr_track_ok else 'пусто/неполно'}, "
                                   f"model={kr_track[0].get('model')!r}")
            failed += not expect("probabilities/model у cbr_rate:ordered_logit непусты", kr_track_ok, kr_track_detail)

            # error_bp — часть контракта ошибки в б.п. (#127); запрос к колонке обязан
            # проходить, даже если сейчас все прогнозы ещё не свёрены и значения NULL.
            failed += not expect("колонка error_bp читается агентом", kr_error is not None,
                                 "SELECT error_bp FROM v_key_rate_dashboard" if kr_error is not None else kr_error_raw[:200])

            # Live-витрина (infra/4.3): читается агентом, stage из закрытого набора,
            # staged-метрики не попали в публикационный forecast-контур. Пустая
            # витрина — информационная строка: до первого shadow/canary её нет.
            ml_live_raw = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n FROM v_ml_live_accuracy"}))
            ml_live = result_rows(ml_live_raw)
            ml_live_n = ml_live[0].get("n") if ml_live else None
            if ml_live is None:
                ml_live_detail = f"витрина не читается: {ml_live_raw[:200]}"
                ml_live_ok = False
            elif (ml_live_n or 0) == 0:
                ml_live_detail = ("строк: 0 — до первого live-прогноза (информационно; "
                                  "make import STAT=key_rate_views создаёт витрину)")
                ml_live_ok = True
            else:
                ml_live_detail = f"строк: {ml_live_n}"
                ml_live_ok = True
            failed += not expect("витрина v_ml_live_accuracy читается агентом", ml_live_ok, ml_live_detail)

            stages_raw = text(await session.call_tool("run_query", {"query":
                "SELECT groupUniqArray(stage) AS stages FROM v_ml_live_accuracy"}))
            stages = result_rows(stages_raw)
            if stages is None:
                stages_detail = f"ответ не распознан: {stages_raw[:200]}"
                stages_ok = False
            else:
                values = set((stages[0].get("stages") if stages else None) or [])
                stages_ok = values <= {"production", "shadow", "canary"}
                stages_detail = f"stage: {sorted(values) if values else 'нет строк'}"
            failed += not expect("stage в v_ml_live_accuracy — production/shadow/canary",
                                 stages_ok, stages_detail)

            staged_raw = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n FROM v_key_rate_dashboard "
                "WHERE contour = 'forecast' "
                "AND (indicator LIKE '%:shadow' OR indicator LIKE '%:canary')"}))
            staged = result_rows(staged_raw)
            staged_n = staged[0].get("n") if staged else None
            failed += not expect("staged-метрики не входят в forecast-контур v_key_rate_dashboard",
                                 staged is not None and (staged_n or 0) == 0,
                                 f"staged-строк: {staged_n}" if staged is not None
                                 else f"ответ не распознан: {staged_raw[:200]}")

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
            failed += not expect("каталог cbr_survey описывает 381 ряд", survey_catalog_n == 381,
                                 f"записей: {survey_catalog_n}" if survey_catalog_rows is not None else f"каталог не читается: {survey_catalog[:200]}")

            # 16 отраслевых листов (Task 6 #239): 368 рядов industry_*; проверяется и каталог,
            # и непустота самих строк витрины. Точное число точек не фиксируется — оно растёт
            # с каждой волной источника.
            survey_industry = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_series_catalog WHERE source = 'cbr_survey' AND startsWith(series, 'industry_')"}))
            survey_industry_rows = result_rows(survey_industry)
            survey_industry_n = survey_industry_rows[0].get("count()") if survey_industry_rows else None
            failed += not expect("каталог cbr_survey описывает 368 отраслевых рядов", survey_industry_n == 368,
                                 f"записей: {survey_industry_n}" if survey_industry_rows is not None else f"каталог не читается: {survey_industry[:200]}")

            survey_industry_view = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_cbr_survey WHERE startsWith(series, 'industry_')"}))
            survey_industry_view_rows = result_rows(survey_industry_view)
            survey_industry_view_n = survey_industry_view_rows[0].get("count()") if survey_industry_view_rows else None
            failed += not expect("витрина v_cbr_survey содержит отраслевые ряды", (survey_industry_view_n or 0) > 0,
                                 f"строк: {survey_industry_view_n}" if survey_industry_view_rows is not None else f"витрина не читается: {survey_industry_view[:200]}")

            # Task 7 (#248): публичный классификатор ОКВЭД2 и кроссворд 87 BSU-кодов.
            # 87/87 проверяется не отдельным списком в Python, а непустотой имён в
            # v_cbr_mp_bsu_map: это LEFT JOIN к rosstat_okved2, то есть пропавший в
            # классификаторе код даёт пустое имя и роняет проверку.
            okved2_view = text(await session.call_tool("run_query", {"query": "SELECT count() AS rows FROM v_rosstat_okved2"}))
            okved2_rows = result_rows(okved2_view)
            okved2_n = okved2_rows[0].get("rows") if okved2_rows else None
            failed += not expect("витрина v_rosstat_okved2: 3 102 строки классификатора", okved2_n == 3102,
                                 f"строк: {okved2_n}" if okved2_rows is not None else f"витрина не читается: {okved2_view[:200]}")

            bsu_map = text(await session.call_tool("run_query", {"query": "SELECT count() AS rows, countIf(okved_name = '') AS missing_names FROM v_cbr_mp_bsu_map"}))
            bsu_rows = result_rows(bsu_map)
            bsu_n = bsu_rows[0].get("rows") if bsu_rows else None
            bsu_missing = bsu_rows[0].get("missing_names") if bsu_rows else None
            failed += not expect("v_cbr_mp_bsu_map: 87 кодов, все с названиями из ОКВЭД2",
                                 bsu_n == 87 and bsu_missing == 0,
                                 f"строк: {bsu_n}, без имён: {bsu_missing}" if bsu_rows is not None else f"витрина не читается: {bsu_map[:200]}")

            panel = text(await session.call_tool("run_query", {"query": "SELECT count() AS rows, uniqExact(code) AS codes, uniqExact(industry_slug) AS slugs, uniqExact(indicator) AS indicators, uniqExact(date) AS dates FROM v_cbr_mp_public_panel"}))
            panel_rows = result_rows(panel)
            panel_ok = False
            panel_detail = f"витрина не читается: {panel[:200]}"
            if panel_rows:
                panel_count = panel_rows[0].get("rows") or 0
                panel_codes = panel_rows[0].get("codes")
                panel_slugs = panel_rows[0].get("slugs")
                panel_indicators = panel_rows[0].get("indicators")
                panel_dates = panel_rows[0].get("dates") or 0
                panel_ok = panel_count > 0 and panel_codes == 87 and panel_slugs == 11 and panel_indicators == 3 and panel_dates >= 297
                panel_detail = f"строк: {panel_count}, кодов: {panel_codes}, листов: {panel_slugs}, индикаторов: {panel_indicators}, дат: {panel_dates}"
            failed += not expect("v_cbr_mp_public_panel: 87 кодов × 3 индикатора × ≥297 дат, 11 листов", panel_ok, panel_detail)

            spot = text(await session.call_tool("run_query", {"query": "SELECT code, industry_slug FROM v_cbr_mp_bsu_map WHERE code IN ('10.1', '47.22', '46.32', '01.4')"}))
            spot_rows = result_rows(spot)
            spot_expected = {
                "10.1": "manufacturing_consumer",
                "47.22": "trade_retail",
                "46.32": "trade_wholesale",
                "01.4": "agriculture",
            }
            spot_ok = spot_rows is not None and {r.get("code"): r.get("industry_slug") for r in spot_rows} == spot_expected
            failed += not expect("кроссворд BSU: spot-коды 10.1/47.22/46.32/01.4", spot_ok,
                                 f"получено: {spot_rows}" if spot_rows is not None else f"витрина не читается: {spot[:200]}")

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
            # Отдельная проверка «имя есть в list_tables» здесь снята: list_tables
            # пагинируется, и после добавления v_key_rate_dashboard имя v_rosstat_macro
            # перестало попадать в первую страницу. Существование и право чтения ниже
            # доказывает запрос к самой витрине — это и есть конвенция проекта.
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
                                       "rosstat_ipc_weeks_bulletins, rosstat_bi_cpi_weekly, "
                                       "rosstat_salaries_mes, rosstat_vvp_kvartal, rosstat_labour_force_mes")
            failed += not expect("витрина v_rosstat_macro непуста",
                                 (rosstat_points_n or 0) > 0 and (rosstat_series_n or 0) > 0, rosstat_view_detail)

            # Каталог источника делится на две части. Базовые 169 рядов — 4 ИПЦ + 131
            # недельный товар + 5 ВВП + 1 зарплата + 6 рынка труда + 18 бюллетеней +
            # 4 ИЦП по ВЭД (T1 #276). T2 (#277) добавляет 5 308 товарных рядов
            # (okpd2:<код>:<тип>) и 76 весовых (ved-weight:<код>:<база>). Товарный
            # каталог растёт вместе с перечнем кодов, поэтому сверяется «>= 5 308»;
            # весовой — ровно 76 (38 кодов × 2 базы). Базовая часть остаётся точным
            # равенством 169: односторонний контракт §11.4 стандарта требует описания
            # каждого разобранного ряда, а меньше 169 значит ни разу не отработавший
            # импортёр. T2-ряды исключены из базового запроса префиксами, иначе
            # точное 169 перестало бы сходиться на любом живом прогоне.
            rosstat_catalog = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n FROM v_series_catalog WHERE source = 'rosstat' "
                "AND NOT startsWith(series, 'okpd2:') AND NOT startsWith(series, 'ved-weight:')"}))
            rosstat_catalog_rows = result_rows(rosstat_catalog)
            rosstat_catalog_n = rosstat_catalog_rows[0].get("n") if rosstat_catalog_rows else None
            if rosstat_catalog_rows is None:
                rosstat_catalog_detail = f"ответ не распознан: {rosstat_catalog[:200]}"
            elif rosstat_catalog_n == 169:
                rosstat_catalog_detail = ("базовых рядов rosstat: 169 (4 ИПЦ + 131 недельный + 5 ВВП + "
                                          "1 зарплата + 6 рабочая сила + 18 бюллетеней + 4 ИЦП по ВЭД)")
            else:
                rosstat_catalog_detail = (f"базовых рядов rosstat: {rosstat_catalog_n}, want 169 — прогнать "
                                          "импортёры по одному: make import STAT=<имя> для каждого "
                                          "из rosstat_ipc_mes, rosstat_ipc_weeks, "
                                          "rosstat_ipc_weeks_bulletins, rosstat_bi_cpi_weekly, "
                                          "rosstat_salaries_mes, rosstat_vvp_kvartal, "
                                          "rosstat_labour_force_mes, rosstat_ppi_ved "
                                          "(каталог перепубликовывается каждым прогоном)")
            failed += not expect("каталог rosstat полон (базовые ряды)", rosstat_catalog_n == 169, rosstat_catalog_detail)

            rosstat_product_catalog = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n FROM v_series_catalog WHERE source = 'rosstat' AND startsWith(series, 'okpd2:')"}))
            rosstat_product_rows = result_rows(rosstat_product_catalog)
            rosstat_product_n = rosstat_product_rows[0].get("n") if rosstat_product_rows else None
            if rosstat_product_rows is None:
                rosstat_product_detail = f"ответ не распознан: {rosstat_product_catalog[:200]}"
            elif (rosstat_product_n or 0) >= 5308:
                rosstat_product_detail = f"товарных рядов rosstat: {rosstat_product_n} (okpd2:<код>:<тип>)"
            else:
                rosstat_product_detail = (f"товарных рядов rosstat: {rosstat_product_n}, want >= 5308 — "
                                          "прогнать make import STAT=rosstat_ppi_product")
            failed += not expect("каталог rosstat_ppi_product полон", (rosstat_product_n or 0) >= 5308, rosstat_product_detail)

            rosstat_weights_catalog = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n FROM v_series_catalog WHERE source = 'rosstat' AND startsWith(series, 'ved-weight:')"}))
            rosstat_weights_rows = result_rows(rosstat_weights_catalog)
            rosstat_weights_n = rosstat_weights_rows[0].get("n") if rosstat_weights_rows else None
            if rosstat_weights_rows is None:
                rosstat_weights_detail = f"ответ не распознан: {rosstat_weights_catalog[:200]}"
            elif rosstat_weights_n == 76:
                rosstat_weights_detail = "весовых рядов rosstat: 76 (38 кодов × 2 базы)"
            else:
                rosstat_weights_detail = (f"весовых рядов rosstat: {rosstat_weights_n}, want 76 — "
                                          "прогнать make import STAT=rosstat_ppi_weights")
            failed += not expect("каталог rosstat_ppi_weights полон", rosstat_weights_n == 76, rosstat_weights_detail)

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
                                 ("rosstat_labour_force_mes", "рабочая сила и безработица"),
                                 ("rosstat_bi_cpi_weekly", "недельный агрегат ИПЦ из BI"),
                                 ("rosstat_ppi_ved", "ИЦП по ВЭД"),
                                 ("rosstat_ppi_product", "ИЦП по товарам/ОКПД2"),
                                 ("rosstat_ppi_weights", "веса ИЦП по ВЭД")):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                failed += not expect(f"сырая {table} закрыта ({label})", denied,
                                     f"отказ ClickHouse 497 на {table} (ожидаемо)" if denied else raw[:200])

            # Агрегатный ряд Росстата теперь составной: 362 точки бюллетеней до 2019-01-09
            # и 398 точек BI 2019-01-09…2026-10-12 (снимок 2026-10-20); BI приоритетен на
            # пересечении 2019–2021, поэтому в витрине одна точка на дату (760 = 362 + 398).
            # BI растёт еженедельно, поэтому проверка — «>= 760» и «max >= 2026-10-12», а не
            # точное равенство. Семь новогодних недель источник не публикует — их отсутствие
            # ожидаемо, а не провал.
            rosstat_aggregate = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n, min(date) AS mn, max(date) AS mx "
                "FROM v_rosstat_macro WHERE source = 'rosstat' AND series = 'Индекс потребительских цен (недельная оценка)'"}))
            rosstat_aggregate_rows = result_rows(rosstat_aggregate)
            rosstat_aggregate_n = rosstat_aggregate_rows[0].get("n") if rosstat_aggregate_rows else None
            rosstat_aggregate_mn = rosstat_aggregate_rows[0].get("mn") if rosstat_aggregate_rows else None
            rosstat_aggregate_mx = rosstat_aggregate_rows[0].get("mx") if rosstat_aggregate_rows else None
            if rosstat_aggregate_rows is None:
                rosstat_aggregate_detail = f"ответ не распознан: {rosstat_aggregate[:200]}"
            elif ((rosstat_aggregate_n or 0) >= 760 and rosstat_aggregate_mn == "2009-04-06"
                  and (rosstat_aggregate_mx or "") >= "2026-10-12"):
                rosstat_aggregate_detail = (f"точек агрегатного ряда: {rosstat_aggregate_n} (>= 760; "
                                            "362 бюллетеня до 2019-01-09 + 398+ BI), "
                                            f"2009-04-06…{rosstat_aggregate_mx}")
            else:
                rosstat_aggregate_detail = (f"точек агрегатного ряда: {rosstat_aggregate_n} "
                                            f"({rosstat_aggregate_mn}…{rosstat_aggregate_mx}), want >= 760 "
                                            "2009-04-06…>=2026-10-12 — прогнать "
                                            "make import STAT=rosstat_ipc_weeks_bulletins и "
                                            "make import STAT=rosstat_bi_cpi_weekly")
            failed += not expect("агрегатный ряд rosstat полон (бюллетени + BI)",
                                 (rosstat_aggregate_n or 0) >= 760 and rosstat_aggregate_mn == "2009-04-06"
                                 and (rosstat_aggregate_mx or "") >= "2026-10-12", rosstat_aggregate_detail)

            # Контур ЕМИСС 37426 (пункт key-rate/phase-4-emiss-weekly-prices, импортёр
            # fedstat_weekly_prices): витрина v_fedstat_weekly_prices читается агентом и
            # непуста. Полный срез v1 — 158 товаров, 2 определения РФ (2008–2022 и 2026),
            # 56 133 точки; 2023–2025 в источнике отсутствуют, поэтому число точное, а не
            # «> 0». Сырая таблица закрыта: агент читает контур только через витрину.
            fedstat_view = text(await session.call_tool("run_query", {"query": "SELECT count() AS rows, uniqExact(series) AS series, uniqExact(region) AS regions FROM v_fedstat_weekly_prices"}))
            fedstat_view_rows = result_rows(fedstat_view)
            fedstat_points_n = fedstat_view_rows[0].get("rows") if fedstat_view_rows else None
            fedstat_series_n = fedstat_view_rows[0].get("series") if fedstat_view_rows else None
            fedstat_regions_n = fedstat_view_rows[0].get("regions") if fedstat_view_rows else None
            if fedstat_view_rows is None:
                fedstat_view_detail = f"витрина не читается: {fedstat_view[:200]}"
            elif fedstat_points_n == 56133 and fedstat_series_n == 158 and fedstat_regions_n == 2:
                fedstat_view_detail = "строк: 56 133, рядов: 158, регионов: 2 (2008–2022 + 2026)"
            else:
                fedstat_view_detail = (f"строк: {fedstat_points_n}, рядов: {fedstat_series_n}, регионов: {fedstat_regions_n}; "
                                       "want 56 133 / 158 / 2 — прогнать make import STAT=fedstat_weekly_prices")
            failed += not expect("витрина v_fedstat_weekly_prices непуста и полна",
                                 fedstat_points_n == 56133 and fedstat_series_n == 158 and fedstat_regions_n == 2,
                                 fedstat_view_detail)

            fedstat_catalog = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'fedstat'"}))
            fedstat_catalog_rows = result_rows(fedstat_catalog)
            fedstat_catalog_n = fedstat_catalog_rows[0].get("n") if fedstat_catalog_rows else None
            # 158 (37426) + 432 (ИПП) + 9 (розница) + 18 (сельхоз) + 5 (инвестиции) + 1 105 (ИПЦ).
            fedstat_catalog_want = 1727
            if fedstat_catalog_rows is None:
                fedstat_catalog_detail = f"ответ не распознан: {fedstat_catalog[:200]}"
            elif fedstat_catalog_n == fedstat_catalog_want:
                fedstat_catalog_detail = f"рядов fedstat: {fedstat_catalog_want} (37426 + пять макро-блоков)"
            else:
                fedstat_catalog_detail = (f"рядов fedstat: {fedstat_catalog_n}, want {fedstat_catalog_want} — прогнать "
                                          "импортёры fedstat_* (каталог перепубликовывается прогоном)")
            failed += not expect("каталог fedstat полон", fedstat_catalog_n == fedstat_catalog_want, fedstat_catalog_detail)

            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM fedstat_weekly_prices"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырая fedstat_weekly_prices закрыта", denied,
                                 "отказ ClickHouse 497 на fedstat_weekly_prices (ожидаемо)" if denied else raw[:200])

            # Пять макро-витрин ЕМИСС (#310): непустота, число рядов и два определения РФ.
            # Числа — снимок research 2026-10-21; если источник добавил месяц, счётчик
            # вырастет и проверка потребует обновления (как у недельного 37426).
            fedstat_macro = [
                ("v_fedstat_ipp", 55968, 432, "ИПП 43046+57806"),
                ("v_fedstat_retail", 4638, 9, "розница 31066+31260"),
                ("v_fedstat_agri", 565, 18, "сельхоз 34128/59001/31062/43337"),
                ("v_fedstat_investment", 153, 5, "инвестиции 58089/58090/34129/40562/34475"),
                ("v_fedstat_ipc", 204038, 1105, "ИПЦ 31074"),
            ]
            for view, want_rows, want_series, label in fedstat_macro:
                resp = text(await session.call_tool("run_query", {
                    "query": f"SELECT count() AS rows, uniqExact(series) AS series, uniqExact(region) AS regions FROM {view}"
                }))
                rows = result_rows(resp)
                points = rows[0].get("rows") if rows else None
                series = rows[0].get("series") if rows else None
                regions = rows[0].get("regions") if rows else None
                ok = points == want_rows and series == want_series and regions == 2
                detail = (f"строк: {points}, рядов: {series}, регионов: {regions}; want "
                          f"{want_rows} / {want_series} / 2 — прогнать соответствующий fedstat_*-импортёр") if not ok else \
                         f"строк: {want_rows}, рядов: {want_series}, регионов: 2"
                failed += not expect(f"витрина {view} ({label}) непуста и полна", ok, detail)

            for raw_table in ("fedstat_ipp", "fedstat_retail", "fedstat_agri", "fedstat_investment", "fedstat_ipc"):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {raw_table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                failed += not expect(f"сырая {raw_table} закрыта", denied,
                                     f"отказ ClickHouse 497 на {raw_table} (ожидаемо)" if denied else raw[:200])

            # Контур Минфина (пункт key-rate/phase-2-minfin-scrape, спека 2026-10-19
            # minfin-deficit-oil-gas §4.2): витрина v_minfin_budget читается агентом и
            # НЕПУСТА, федеральный дефицит идёт отдельным рядом (source = 'minfin'), а новый
            # источник minfin_oil_gas отдаёт оба ряда — «Нефтегазовые доходы, всего» и «НДПИ».
            #
            # Строки публикуют импортёры пакета minfin/, поэтому пустая витрина означает ни
            # разу не отработавший шаг dagu/minfin.yaml, а не отсутствие данных у источника.
            # Существование витрины подтверждается ЗАПРОСОМ К НЕЙ, а не поиском имени в
            # ответе list_tables (урок bold: в том тексте имя попадает из COMMENT соседки);
            # «нет витрины» (UNKNOWN_TABLE) и «нет прав» (ACCESS_DENIED) приходят ошибкой и
            # в result_rows не разбираются.
            minfin_view = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_minfin_budget"}))
            minfin_view_rows = result_rows(minfin_view)
            minfin_view_n = minfin_view_rows[0].get("count()") if minfin_view_rows else None
            if minfin_view_rows is None:
                minfin_view_detail = (f"витрина не читается: {minfin_view[:200]} — прогнать импортёры "
                                      "пакета: make import "
                                      "STAT=minfin_fed_bud_mes,minfin_fed_bud_mesyats,minfin_oil_gas_mes")
            elif (minfin_view_n or 0) > 0:
                minfin_view_detail = f"строк: {minfin_view_n}"
            else:
                minfin_view_detail = ("строк: 0 — прогнать импортёры пакета: make import "
                                      "STAT=minfin_fed_bud_mes,minfin_fed_bud_mesyats,minfin_oil_gas_mes "
                                      "(витрину пересоздаёт AfterImport прогона)")
            failed += not expect("витрина v_minfin_budget существует и непуста",
                                 (minfin_view_n or 0) > 0, minfin_view_detail)

            # Новый источник проверяется отдельно: общего count() > 0 мало — витрину
            # пересоздаёт AfterImport любого из трёх импортёров, поэтому зелёная витрина
            # могла бы держать только старые ряды. Два ряда и не меньше 210 точек
            # (2 × 105 месяцев 2018-01…2026-09 выпуска 05.10.2026) означают, что шаг
            # minfin_oil_gas_mes отработал; число сверяется как «>=», потому что новые
            # выпуски добавляют месяцы, а пропавший ряд ловится равенством s == 2.
            minfin_oil_raw = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n, uniqExact(series) AS s FROM v_minfin_budget "
                "WHERE source = 'minfin_oil_gas'"}))
            minfin_oil = result_rows(minfin_oil_raw)
            minfin_oil_n = minfin_oil[0].get("n") if minfin_oil else None
            minfin_oil_s = minfin_oil[0].get("s") if minfin_oil else None
            if minfin_oil is None:
                minfin_oil_detail = f"ответ не распознан: {minfin_oil_raw[:200]}"
            else:
                minfin_oil_detail = (f"точек: {minfin_oil_n} (want >= 210), "
                                     f"рядов: {minfin_oil_s} (want 2)")
                if (minfin_oil_n or 0) < 210 or minfin_oil_s != 2:
                    minfin_oil_detail += " — прогнать make import STAT=minfin_oil_gas_mes"
            failed += not expect("v_minfin_budget: нефтегазовые доходы и НДПИ",
                                 bool(minfin_oil) and (minfin_oil_n or 0) >= 210 and minfin_oil_s == 2,
                                 minfin_oil_detail)

            # Каталог нового источника — ровно два ряда: «Нефтегазовые доходы, всего» и
            # «НДПИ». Точное равенство, а не «> 0»: односторонний контракт стандарта §11.4
            # требует описания каждого разобранного ряда, а третий ряд означал бы ряд,
            # которого нет в объявленном наборе.
            minfin_oil_catalog = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'minfin_oil_gas'"}))
            minfin_oil_catalog_rows = result_rows(minfin_oil_catalog)
            minfin_oil_catalog_n = minfin_oil_catalog_rows[0].get("n") if minfin_oil_catalog_rows else None
            if minfin_oil_catalog_rows is None:
                minfin_oil_catalog_detail = f"ответ не распознан: {minfin_oil_catalog[:200]}"
            elif minfin_oil_catalog_n == 2:
                minfin_oil_catalog_detail = "рядов minfin_oil_gas: 2 (Нефтегазовые доходы, всего + НДПИ)"
            else:
                minfin_oil_catalog_detail = (f"рядов minfin_oil_gas: {minfin_oil_catalog_n}, want 2 — "
                                             "прогнать make import STAT=minfin_oil_gas_mes "
                                             "(каталог перепубликовывается прогоном)")
            failed += not expect("каталог minfin_oil_gas полон", minfin_oil_catalog_n == 2,
                                 minfin_oil_catalog_detail)

            # Федеральный дефицит — отдельная строка существующей таблицы minfin_fed_bud_mes
            # (source = 'minfin'), которой до пункта не было: дефицит был только у
            # консолидированного бюджета (source = 'minfin_mesyats'). Проверяются ДВА
            # независимых утверждения:
            #   1) данные — в витрине есть точки этого ряда (запрос к v_minfin_budget, а не
            #      к каталогу: каталог публикуется независимо от того, нашёл ли парсер
            #      строку в файле, поэтому проверка одного каталога прошла бы и при пустом
            #      ряде);
            #   2) описание — ряд есть в v_series_catalog по паре (source, series), иначе
            #      агент увидит точки без единиц и привязки даты.
            minfin_deficit = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n, min(date) AS first, max(date) AS last FROM v_minfin_budget "
                "WHERE source = 'minfin' AND series = 'Дефицит (-)/Профицит (+)'"}))
            minfin_deficit_rows = result_rows(minfin_deficit)
            minfin_deficit_n = minfin_deficit_rows[0].get("n") if minfin_deficit_rows else None
            if minfin_deficit_rows is None:
                minfin_deficit_detail = f"ответ не распознан: {minfin_deficit[:200]}"
            elif (minfin_deficit_n or 0) > 0:
                minfin_deficit_detail = (f"точек: {minfin_deficit_n}, "
                                         f"{minfin_deficit_rows[0].get('first')} … {minfin_deficit_rows[0].get('last')}")
            else:
                minfin_deficit_detail = ("точек: 0 — прогнать make import STAT=minfin_fed_bud_mes "
                                         "(ряд разбирается из *_mes.xlsx)")
            failed += not expect("витрина: федеральный дефицит отдельным рядом",
                                 (minfin_deficit_n or 0) > 0, minfin_deficit_detail)

            minfin_deficit_catalog = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n FROM v_series_catalog "
                "WHERE source = 'minfin' AND series = 'Дефицит (-)/Профицит (+)'"}))
            minfin_deficit_catalog_rows = result_rows(minfin_deficit_catalog)
            minfin_deficit_catalog_n = (minfin_deficit_catalog_rows[0].get("n")
                                        if minfin_deficit_catalog_rows else None)
            if minfin_deficit_catalog_rows is None:
                minfin_deficit_catalog_detail = f"ответ не распознан: {minfin_deficit_catalog[:200]}"
            elif minfin_deficit_catalog_n == 1:
                minfin_deficit_catalog_detail = "ряд «Дефицит (-)/Профицит (+)» описан в source = 'minfin'"
            else:
                minfin_deficit_catalog_detail = (f"записей: {minfin_deficit_catalog_n}, want 1 — прогнать "
                                                 "make import STAT=minfin_fed_bud_mes "
                                                 "(каталог перепубликовывается прогоном)")
            failed += not expect("каталог: федеральный дефицит описан",
                                 minfin_deficit_catalog_n == 1, minfin_deficit_catalog_detail)

            # Сырые таблицы пакета агенту не выданы — как macro_series и cbr_int_rate_loans:
            # читать бюджетный контур агент обязан только через v_minfin_budget (единицы и
            # привязка даты описаны в v_series_catalog). Таблицы создаёт DDL-прогон
            # импортёра, поэтому UNKNOWN_TABLE здесь — провал, а не успех: таблица обязана
            # существовать и быть закрытой.
            for table, label in (("minfin_fed_bud_mes", "федеральный бюджет, месячные разности"),
                                 ("minfin_fed_bud_mesyats", "консолидированный бюджет, месячные разности"),
                                 ("minfin_oil_gas_mes", "нефтегазовые доходы и НДПИ")):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                failed += not expect(f"сырая {table} закрыта ({label})", denied,
                                     f"отказ ClickHouse 497 на {table} (ожидаемо)" if denied else raw[:200])

            # Свежесть бюджетного контура (пункт key-rate/phase-2-minfin-consolidated-freshness,
            # issue #229): консолидированный выпуск может отставать от федерального на месяц,
            # но не больше чем на два; ряд «Акцизы» не должен отставать от общего max
            # консолидированного больше чем на месяц. Именно эти два разрыва ловили
            # пропущенную строку «Акцизы ****» (2026-06 против 2026-08 у остальных рядов).
            minfin_lag = text(await session.call_tool("run_query", {"query":
                "SELECT fed_max, con_max, dateDiff('month', con_max, fed_max) AS lag_months "
                "FROM (SELECT maxIf(date, source = 'minfin') AS fed_max, "
                "maxIf(date, source = 'minfin_mesyats') AS con_max FROM v_minfin_budget)"}))
            minfin_lag_rows = result_rows(minfin_lag)
            minfin_lag_months = minfin_lag_rows[0].get("lag_months") if minfin_lag_rows else None
            minfin_fed_max = minfin_lag_rows[0].get("fed_max") if minfin_lag_rows else None
            minfin_con_max = minfin_lag_rows[0].get("con_max") if minfin_lag_rows else None
            if minfin_lag_rows is None:
                minfin_lag_detail = f"ответ не распознан: {minfin_lag[:200]}"
            else:
                minfin_lag_detail = (f"федеральный max: {minfin_fed_max}, консолидированный max: {minfin_con_max}, "
                                     f"лаг: {minfin_lag_months} мес. (want <= 2)")
                if minfin_lag_months is None or minfin_lag_months > 2:
                    minfin_lag_detail += " — прогнать make import STAT=minfin_fed_bud_mesyats"
            failed += not expect("свежесть консолидированного бюджета: лаг от федерального <= 2 мес.",
                                 bool(minfin_lag_rows) and (minfin_lag_months or 0) <= 2,
                                 minfin_lag_detail)

            minfin_akc = text(await session.call_tool("run_query", {"query":
                "SELECT akc_max, con_max, dateDiff('month', akc_max, con_max) AS lag_months "
                "FROM (SELECT maxIf(date, series = 'Акцизы') AS akc_max, max(date) AS con_max "
                "FROM v_minfin_budget WHERE source = 'minfin_mesyats')"}))
            minfin_akc_rows = result_rows(minfin_akc)
            minfin_akc_max = minfin_akc_rows[0].get("akc_max") if minfin_akc_rows else None
            minfin_akc_con_max = minfin_akc_rows[0].get("con_max") if minfin_akc_rows else None
            minfin_akc_lag = minfin_akc_rows[0].get("lag_months") if minfin_akc_rows else None
            if minfin_akc_rows is None:
                minfin_akc_detail = f"ответ не распознан: {minfin_akc[:200]}"
            else:
                minfin_akc_detail = (f"Акцизы max: {minfin_akc_max}, консолидированный max: {minfin_akc_con_max}, "
                                     f"лаг: {minfin_akc_lag} мес. (want <= 1)")
                if minfin_akc_lag is None or minfin_akc_lag > 1:
                    minfin_akc_detail += " — прогнать make import STAT=minfin_fed_bud_mesyats"
            failed += not expect("свежесть Акцизы: лаг от общего max <= 1 мес.",
                                 minfin_akc_lag is not None and minfin_akc_lag <= 1,
                                 minfin_akc_detail)
            # Долговой контур ОФЗ (пункт key-rate/phase-2-ofz-debt-stress, issue #170):
            # витрина v_ofz_stress читается и непуста, все четыре метрики имеют хотя бы
            # одну непустую точку (покрытие, спред к КБД, обслуживание/доходы, запас ОФЗ),
            # каталог описывает 20 рядов (5 аукцион + 10 долг + 5 обслуживание), сырые
            # таблицы закрыты, а последний аукцион не старше 40 дней. Пустая метрика —
            # не «ноль», а не доехавший импортёр: coverage/spread идут из аукционов,
            # service — из бюджетной строки или indicatorsBudget, stock — из структуры.
            ofz_stress = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n, countIf(coverage_ratio IS NOT NULL) AS coverage, "
                "countIf(cutoff_minus_secondary_pp IS NOT NULL) AS spread, "
                "countIf(debt_service IS NOT NULL) AS service, "
                "countIf(ofz_stock IS NOT NULL) AS stock, "
                "min(date) AS first, max(date) AS last FROM v_ofz_stress"}))
            ofz_rows = result_rows(ofz_stress)
            ofz_n = ofz_rows[0].get("n") if ofz_rows else None
            ofz_coverage = ofz_rows[0].get("coverage") if ofz_rows else None
            ofz_spread = ofz_rows[0].get("spread") if ofz_rows else None
            ofz_service = ofz_rows[0].get("service") if ofz_rows else None
            ofz_stock = ofz_rows[0].get("stock") if ofz_rows else None
            ofz_first = ofz_rows[0].get("first") if ofz_rows else None
            ofz_last = ofz_rows[0].get("last") if ofz_rows else None
            if ofz_rows is None:
                ofz_detail = (f"витрина не читается: {ofz_stress[:200]} — прогнать "
                              "make import STAT=minfin_ofz_auction,minfin_debt_stock,minfin_debt_service")
            else:
                ofz_detail = (f"строк: {ofz_n}, покрытие: {ofz_coverage}, спред: {ofz_spread}, "
                              f"обслуживание: {ofz_service}, запас ОФЗ: {ofz_stock}, "
                              f"{ofz_first} … {ofz_last}")
                if (ofz_n or 0) == 0 or (ofz_coverage or 0) == 0 or (ofz_spread or 0) == 0 \
                        or (ofz_service or 0) == 0 or (ofz_stock or 0) == 0:
                    ofz_detail += (" — прогнать make import "
                                   "STAT=minfin_ofz_auction,minfin_debt_stock,minfin_debt_service")
            failed += not expect("витрина v_ofz_stress: аукционы и четыре метрики",
                                 bool(ofz_rows) and (ofz_n or 0) > 0 and (ofz_coverage or 0) > 0
                                 and (ofz_spread or 0) > 0 and (ofz_service or 0) > 0
                                 and (ofz_stock or 0) > 0,
                                 ofz_detail)
            ofz_fresh_after = (date.today() - timedelta(days=40)).isoformat()
            failed += not expect("v_ofz_stress: последний аукцион не старше 40 дней",
                                 bool(ofz_last) and str(ofz_last) >= ofz_fresh_after,
                                 f"max date: {ofz_last} (want >= {ofz_fresh_after})")

            ofz_catalog = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n FROM v_series_catalog "
                "WHERE source IN ('minfin_ofz_auction', 'minfin_debt_stock', 'minfin_debt_service')"}))
            ofz_catalog_rows = result_rows(ofz_catalog)
            ofz_catalog_n = ofz_catalog_rows[0].get("n") if ofz_catalog_rows else None
            if ofz_catalog_rows is None:
                ofz_catalog_detail = f"ответ не распознан: {ofz_catalog[:200]}"
            elif ofz_catalog_n == 20:
                ofz_catalog_detail = "рядов контура: 20 (5 аукцион + 10 долг + 5 обслуживание)"
            else:
                ofz_catalog_detail = (f"рядов контура: {ofz_catalog_n}, want 20 — прогнать "
                                      "make import STAT=minfin_ofz_auction,minfin_debt_stock,minfin_debt_service "
                                      "(каталог перепубликовывается прогонами)")
            failed += not expect("каталог ОФЗ-контура полон", ofz_catalog_n == 20, ofz_catalog_detail)

            for table, label in (("minfin_ofz_auction", "аукционы ОФЗ"),
                                 ("minfin_debt_stock", "запас внутреннего/внешнего долга и ОФЗ"),
                                 ("minfin_debt_service", "обслуживание и факт заимствований")):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                failed += not expect(f"сырая {table} закрыта ({label})", denied,
                                     f"отказ ClickHouse 497 на {table} (ожидаемо)" if denied else raw[:200])

            # Контур ФАО (пункт infra/1.6, issue #225): витрина v_fao_food_price читается
            # агентом, содержит 6 рядов / 2646 точек (6 × 441 месяц), каталог source='fao'
            # описывает все 6 рядов, а сырая таблица агенту не выдана. Точное число 2646 —
            # контракт пункта: 441 месяц 1990-01…2026-09 в источнике, дата хранения — +1 месяц.
            fao_view = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n, uniqExact(series) AS s, max(period_date) AS max_dt "
                "FROM v_fao_food_price"}))
            fao_view_rows = result_rows(fao_view)
            fao_n = fao_view_rows[0].get("n") if fao_view_rows else None
            fao_s = fao_view_rows[0].get("s") if fao_view_rows else None
            fao_max = fao_view_rows[0].get("max_dt") if fao_view_rows else None
            if fao_view_rows is None:
                fao_view_detail = (f"витрина не читается: {fao_view[:200]} — прогнать "
                                   "make import STAT=fao_food_price_Indices_monthly")
            else:
                fao_view_detail = (f"точек: {fao_n} (want 2646), рядов: {fao_s} (want 6), "
                                   f"max period_date: {fao_max}")
                if fao_n != 2646 or fao_s != 6:
                    fao_view_detail += " — прогнать make import STAT=fao_food_price_Indices_monthly"
            failed += not expect("витрина v_fao_food_price: 6 рядов / 2646 точек",
                                 bool(fao_view_rows) and fao_n == 2646 and fao_s == 6,
                                 fao_view_detail)
            failed += not expect("v_fao_food_price: свежий месяц",
                                 bool(fao_max) and str(fao_max) >= "2026-09-01",
                                 f"max period_date: {fao_max} (want >= 2026-09-01)")

            fao_catalog = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n FROM v_series_catalog WHERE source = 'fao'"}))
            fao_catalog_rows = result_rows(fao_catalog)
            fao_catalog_n = fao_catalog_rows[0].get("n") if fao_catalog_rows else None
            if fao_catalog_rows is None:
                fao_catalog_detail = f"ответ не распознан: {fao_catalog[:200]}"
            elif fao_catalog_n == 6:
                fao_catalog_detail = "рядов source='fao': 6"
            else:
                fao_catalog_detail = (f"рядов source='fao': {fao_catalog_n}, want 6 — прогнать "
                                      "make import STAT=fao_food_price_Indices_monthly")
            failed += not expect("каталог source='fao' полон", fao_catalog_n == 6,
                                 fao_catalog_detail)

            fao_raw = text(await session.call_tool("run_query", {"query":
                "SELECT count() FROM fao_food_price_Indices_monthly"}))
            fao_denied = "ACCESS_DENIED" in fao_raw or "Not enough privileges" in fao_raw
            failed += not expect("сырая fao_food_price_Indices_monthly закрыта", fao_denied,
                                 "отказ ClickHouse 497 (ожидаемо)" if fao_denied else fao_raw[:200])

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

            # Point-in-time слой над cbr_dip_bulletin (пункт key-rate/phase-4-point-in-time,
            # спека 2026-10-19 §2): три витрины создаёт прогон cbr_dip_bulletin (ensurePitViews
            # после цикла выпусков и на no-op-пути), а сырая таблица агенту по-прежнему
            # закрыта. Проверки идут от общего к частному: непустота слоя, инвариант периода,
            # строгая граница as-of на известной ревизии SA-безработицы и версия набора.
            pit_view = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS points, uniqExact(vintage) AS vintages, "
                "uniqExact(series) AS series FROM v_point_in_time"}))
            pit_view_rows = result_rows(pit_view)
            pit_points_n = pit_view_rows[0].get("points") if pit_view_rows else None
            pit_vintages_n = pit_view_rows[0].get("vintages") if pit_view_rows else None
            pit_series_n = pit_view_rows[0].get("series") if pit_view_rows else None
            # Одной непустоты мало: count() > 0 прошёл бы и на срезе из одного выпуска.
            # 49 версий и 8 рядов — состояние приёмки спеки §4 (18 000 точек).
            pit_view_ok = ((pit_points_n or 0) > 0 and (pit_vintages_n or 0) >= 49
                           and pit_series_n == 8)
            if pit_view_rows is None:
                pit_view_detail = f"ответ не распознан: {pit_view[:200]}"
            elif not pit_view_rows:
                pit_view_detail = ("строк 0 — прогнать make import STAT=cbr_dip_bulletin "
                                   "(слой создаётся прогоном)")
            else:
                pit_view_detail = (f"точек: {pit_points_n}, версий: {pit_vintages_n} "
                                   f"(want >= 49), рядов: {pit_series_n} (want 8)")
            failed += not expect("слой v_point_in_time непуст", pit_view_ok, pit_view_detail)

            # Инвариант слоя: отчётный период не позже публикации. Срез не фильтрует периоды
            # по дате публикации (отвечает на вопрос «что опубликовано строго до D»), поэтому
            # нарушение означало бы поломку нормализации period_date/published_at.
            pit_period = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n FROM v_point_in_time WHERE period_date > published_at"}))
            pit_period_rows = result_rows(pit_period)
            pit_period_n = pit_period_rows[0].get("n") if pit_period_rows else None
            if pit_period_rows is None:
                pit_period_detail = f"ответ не распознан: {pit_period[:200]}"
            elif not pit_period_rows:
                pit_period_detail = "строк 0 в ответе — ожидалась одна строка со счётчиком"
            else:
                pit_period_detail = f"строк с period_date > published_at: {pit_period_n} (want 0)"
            failed += not expect("период не позже публикации", pit_period_n == 0, pit_period_detail)

            # Строгая граница среза: значение, опубликованное ровно в дату as_of, в срез не
            # входит. SA-безработица за 2023-11-30 впервые опубликована 2024-02-06 (выпуск
            # 24-01), поэтому as_of на день публикации обязан дать пусто (<, а не <=).
            pit_asof_boundary = text(await session.call_tool("run_query", {"query":
                "SELECT count() AS n FROM v_point_in_time_asof(as_of='2024-02-06') "
                "WHERE source='cbr_dip_bulletin' AND series='unemployment_sa' "
                "AND period_date='2023-11-30'"}))
            pit_asof_boundary_rows = result_rows(pit_asof_boundary)
            pit_asof_boundary_n = pit_asof_boundary_rows[0].get("n") if pit_asof_boundary_rows else None
            if pit_asof_boundary_rows is None:
                pit_asof_boundary_detail = f"ответ не распознан: {pit_asof_boundary[:200]}"
            elif not pit_asof_boundary_rows:
                pit_asof_boundary_detail = "строк 0 в ответе — ожидалась одна строка со счётчиком"
            else:
                pit_asof_boundary_detail = (f"строк на as_of='2024-02-06': {pit_asof_boundary_n} "
                                            "(want 0: граница строгая)")
            failed += not expect("строгая граница среза", pit_asof_boundary_n == 0,
                                 pit_asof_boundary_detail)

            # Ревизия одного периода: 2023-11-30 равно 2.885274 в выпуске 24-01 и 2.916127 в
            # 26-06. Слой обязан вернуть версию, известную строго до as_of, вместе с её
            # винтажом — иначе ревизии SA-рядов выглядели бы фактами.
            for pit_as_of, pit_want in (
                ("2024-02-07", {"value": 2.885274, "vintage": "47847", "vintage_label": "24-01"}),
                ("2026-09-02", {"value": 2.916127, "vintage": "62343", "vintage_label": "26-06"}),
            ):
                pit_asof = text(await session.call_tool("run_query", {"query":
                    f"SELECT value, vintage, vintage_label FROM v_point_in_time_asof(as_of='{pit_as_of}') "
                    "WHERE source='cbr_dip_bulletin' AND series='unemployment_sa' "
                    "AND period_date='2023-11-30'"}))
                pit_asof_rows = result_rows(pit_asof)
                pit_asof_row = pit_asof_rows[0] if pit_asof_rows else None
                pit_asof_ok = (pit_asof_rows is not None and len(pit_asof_rows) == 1
                               and pit_asof_row.get("value") == pit_want["value"]
                               and pit_asof_row.get("vintage") == pit_want["vintage"]
                               and pit_asof_row.get("vintage_label") == pit_want["vintage_label"])
                if pit_asof_rows is None:
                    pit_asof_detail = f"ответ не распознан: {pit_asof[:200]}"
                elif pit_asof_row is None:
                    pit_asof_detail = ("строк 0, want одна версия — проверить создание витрины "
                                       "и грант kimi_reader")
                else:
                    pit_asof_detail = (f"value={pit_asof_row.get('value')}, "
                                       f"vintage={pit_asof_row.get('vintage')}, "
                                       f"vintage_label={pit_asof_row.get('vintage_label')}")
                failed += not expect(
                    f"срез as_of='{pit_as_of}' отдаёт выпуск {pit_want['vintage_label']}",
                    pit_asof_ok, pit_asof_detail)

            # Ряд, исчезнувший из нового выпуска: 26-07 (2026-10-14) заменил безработицу
            # занятостью. Слой обязан сохранить последнюю известную версию unemployment_sa
            # (26-06) и одновременно отдать employment_sa уже из 26-07 — отсутствие ряда
            # значит «выпуск его не показывает», а не «ряд удалён» (спека §2.3).
            for pit_series, pit_want in (
                ("unemployment_sa", {"value": 2.239751, "vintage": "62343", "vintage_label": "26-06"}),
                ("employment_sa", {"value": 61.5059577839148, "vintage": "62444", "vintage_label": "26-07"}),
            ):
                pit_missing = text(await session.call_tool("run_query", {"query":
                    "SELECT value, vintage, vintage_label FROM v_point_in_time_asof(as_of='2026-10-15') "
                    f"WHERE source='cbr_dip_bulletin' AND series='{pit_series}' "
                    "AND period_date='2026-06-30'"}))
                pit_missing_rows = result_rows(pit_missing)
                pit_missing_row = pit_missing_rows[0] if pit_missing_rows else None
                pit_missing_ok = (pit_missing_rows is not None and len(pit_missing_rows) == 1
                                  and pit_missing_row.get("value") == pit_want["value"]
                                  and pit_missing_row.get("vintage") == pit_want["vintage"]
                                  and pit_missing_row.get("vintage_label") == pit_want["vintage_label"])
                if pit_missing_rows is None:
                    pit_missing_detail = f"{pit_series}: ответ не распознан: {pit_missing[:200]}"
                elif pit_missing_row is None:
                    pit_missing_detail = (f"{pit_series}: строк 0, want одна — последняя известная "
                                          "версия потеряна (проверить отсутствие HAVING/фильтра "
                                          "period_date в витрине)")
                else:
                    pit_missing_detail = (f"{pit_series}: value={pit_missing_row.get('value')}, "
                                          f"vintage={pit_missing_row.get('vintage')}, "
                                          f"vintage_label={pit_missing_row.get('vintage_label')}")
                failed += not expect(
                    "ряд, исчезнувший из нового выпуска, сохраняет последнюю версию",
                    pit_missing_ok, pit_missing_detail)

            # Версия набора для forecast_log.data_version: одна строка на source, не меньше
            # 18 000 точек и 49 версий, data_version — 16 hex-символов. Пустая или короткая
            # версия — провал: по ней прогноз сопоставляется с точным состоянием слоя (спека §2.4).
            pit_version = text(await session.call_tool("run_query", {"query":
                "SELECT points, vintages, data_version FROM v_point_in_time_version "
                "WHERE source='cbr_dip_bulletin'"}))
            pit_version_rows = result_rows(pit_version)
            pit_version_row = pit_version_rows[0] if pit_version_rows else None
            pit_version_ok = (pit_version_rows is not None and len(pit_version_rows) == 1
                              and (pit_version_row.get("points") or 0) >= 18000
                              and (pit_version_row.get("vintages") or 0) >= 49
                              and isinstance(pit_version_row.get("data_version"), str)
                              and len(pit_version_row.get("data_version")) == 16)
            if pit_version_rows is None:
                pit_version_detail = f"ответ не распознан: {pit_version[:200]}"
            elif pit_version_row is None:
                pit_version_detail = ("строк 0, want одна — прогнать make import "
                                      "STAT=cbr_dip_bulletin (витрину создаёт прогон)")
            else:
                pit_version_detail = (f"строк: {len(pit_version_rows)} (want 1), точек: "
                                      f"{pit_version_row.get('points')} (want >= 18000), версий: "
                                      f"{pit_version_row.get('vintages')} (want >= 49), "
                                      f"data_version: {pit_version_row.get('data_version')}")
            failed += not expect("версия набора v_point_in_time_version полна",
                                 pit_version_ok, pit_version_detail)

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

            # Контур ОФЗ (key-rate/phase-2-cbr-zcyc, импортёр cbr_zcyc): история кривой
            # бескупонной доходности ЦБ (12 теноров с 04.01.2003) читается агентом через две
            # витрины. Сырая cbr_zcyc агенту не выдана — как macro_series и cbr_form101:
            # уровни КБД и компоненты сигнала агент читает только через витрины
            # (комментарии таблицы и каждой колонки есть только у них, правило 11 AGENTS.md).
            #
            # Существование витрины подтверждается ЗАПРОСОМ К НЕЙ, а не поиском имени в
            # list_tables (урок bold: в том тексте имя отсутствующей витрины попадало из
            # COMMENT соседки); «нет витрины» и «нет прав» приходят ошибкой
            # (UNKNOWN_TABLE / ACCESS_DENIED) и в result_rows не разбираются. Пустая
            # v_ofz_curve_compare значит ни разу не отработавший шаг cbr_zcyc, а не
            # отсутствие данных у источника: строки публикует импортёр. min/max в detail
            # показывают, насколько глубоко прошёл бэкфилл (ориентир — с 04.01.2003).
            # Непустоты мало: v_ofz_curve_compare может быть непуста одним ЦБ
            # (ofz_curve ещё не импортирован), а разбор КБД — отдать часть 12
            # теноров. Поэтому тем же запросом проверяется покрытие обоих
            # источников: есть строки с moex_yield и ровно 12 теноров с cbr_yield.
            ofz_compare = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n, countIf(moex_yield IS NOT NULL) AS n_moex, "
                         "uniqExactIf(tenor, cbr_yield IS NOT NULL) AS n_tenors, "
                         "min(date) AS min_date, max(date) AS max_date FROM v_ofz_curve_compare"}))
            ofz_compare_rows = result_rows(ofz_compare)
            ofz_compare_n = ofz_compare_rows[0].get("n") if ofz_compare_rows else None
            ofz_compare_moex = ofz_compare_rows[0].get("n_moex") if ofz_compare_rows else None
            ofz_compare_tenors = ofz_compare_rows[0].get("n_tenors") if ofz_compare_rows else None
            if ofz_compare_rows is None:
                ofz_compare_detail = f"витрина не читается: {ofz_compare[:200]}"
            elif (ofz_compare_n or 0) > 0:
                ofz_compare_detail = (f"строк: {ofz_compare_n}, "
                                      f"{ofz_compare_rows[0].get('min_date')}..{ofz_compare_rows[0].get('max_date')}, "
                                      f"moex_yield: {ofz_compare_moex}, теноров ЦБ: {ofz_compare_tenors}")
            else:
                ofz_compare_detail = ("строк: 0 — прогнать make import STAT=cbr_zcyc "
                                      "(витрины пересоздаёт прогон)")
            failed += not expect("витрина v_ofz_curve_compare существует и непуста",
                                 (ofz_compare_n or 0) > 0, ofz_compare_detail)
            failed += not expect("v_ofz_curve_compare: есть MOEX и 12 теноров ЦБ",
                                 (ofz_compare_moex or 0) > 0 and ofz_compare_tenors == 12,
                                 f"moex_yield: {ofz_compare_moex}, uniqExact(tenor) по cbr_yield: "
                                 f"{ofz_compare_tenors} (want > 0 и 12) — прогнать "
                                 "make import STAT=cbr_zcyc и STAT=ofz_curve")

            # v_ofz_signal: производные спреды к ключевой ставке и реальная доходность.
            # Ноль строк — не «нет сигнала», а не отработавший импортёр или отсутствующие
            # cbr_key_rate/rosstat_ipc_mes: CreateView пропускает витрину, пока их нет,
            # и пересоздаёт её следующим прогоном (самолечение). Непустоты мало: при
            # промахе ASOF JOIN и окна ИПЦ значения были бы NULL во всей витрине, поэтому
            # тем же запросом проверяется, что каждая компонента дала хотя бы одну точку.
            ofz_signal = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n, countIf(key_rate IS NOT NULL) AS n_key_rate, "
                         "countIf(inflation_yoy IS NOT NULL) AS n_inflation, "
                         "countIf(ofz_10y IS NOT NULL) AS n_ofz_10y FROM v_ofz_signal"}))
            ofz_signal_rows = result_rows(ofz_signal)
            ofz_signal_n = ofz_signal_rows[0].get("n") if ofz_signal_rows else None
            ofz_signal_key = ofz_signal_rows[0].get("n_key_rate") if ofz_signal_rows else None
            ofz_signal_infl = ofz_signal_rows[0].get("n_inflation") if ofz_signal_rows else None
            ofz_signal_ofz10 = ofz_signal_rows[0].get("n_ofz_10y") if ofz_signal_rows else None
            if ofz_signal_rows is None:
                ofz_signal_detail = f"витрина не читается: {ofz_signal[:200]}"
            elif (ofz_signal_n or 0) > 0:
                ofz_signal_detail = (f"строк: {ofz_signal_n}, key_rate: {ofz_signal_key}, "
                                     f"inflation_yoy: {ofz_signal_infl}, ofz_10y: {ofz_signal_ofz10}")
            else:
                ofz_signal_detail = ("строк: 0 — прогнать make import STAT=cbr_zcyc "
                                     "(нужны cbr_key_rate и rosstat_ipc_mes)")
            failed += not expect("витрина v_ofz_signal существует и непуста",
                                 (ofz_signal_n or 0) > 0, ofz_signal_detail)
            failed += not expect("v_ofz_signal: есть key_rate, ИПЦ и ofz_10y",
                                 (ofz_signal_key or 0) > 0 and (ofz_signal_infl or 0) > 0
                                 and (ofz_signal_ofz10 or 0) > 0,
                                 f"key_rate: {ofz_signal_key}, inflation_yoy: {ofz_signal_infl}, "
                                 f"ofz_10y: {ofz_signal_ofz10} (want > 0 у каждой) — прогнать "
                                 "make import STAT=cbr_zcyc, cbr_key_rate и rosstat_ipc_mes")

            # key_rate до 2013-09-17 обязан быть NULL: истории ключевой ставки раньше
            # этой даты нет. Проверка пинит именно семантику NULL в ASOF LEFT JOIN:
            # при join_use_nulls=0 (значение по умолчанию) промах join подставляет в
            # не-Nullable правую колонку 0, и тогда key_rate=0, а спреды равны
            # доходности ОФЗ. Витрина оборачивает правые колонки в toNullable;
            # nulls < n здесь значит, что обёртка потерялась и агент прочитает нули
            # вместо «ставки ещё не было».
            ofz_null = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n, countIf(key_rate IS NULL) AS nulls "
                         "FROM v_ofz_signal WHERE date < '2013-09-17'"}))
            ofz_null_rows = result_rows(ofz_null)
            ofz_null_n = ofz_null_rows[0].get("n") if ofz_null_rows else None
            ofz_nulls = ofz_null_rows[0].get("nulls") if ofz_null_rows else None
            if ofz_null_rows is None:
                ofz_null_detail = f"ответ не распознан: {ofz_null[:200]}"
            else:
                ofz_null_detail = (f"строк до 2013-09-17: {ofz_null_n}, "
                                   f"из них key_rate IS NULL: {ofz_nulls}")
                if (ofz_null_n or 0) == 0:
                    ofz_null_detail += " — прогнать make import STAT=cbr_zcyc"
            failed += not expect("v_ofz_signal: key_rate до 2013-09-17 — NULL",
                                 (ofz_null_n or 0) > 0 and ofz_nulls == ofz_null_n,
                                 ofz_null_detail)

            # Каталог обязан описывать все 12 теноров КБД (0.25…30) — точным равенством, а не
            # «> 0»: 12 = набор колонок «Срок до погашения, лет» источника, который код
            # объявляет сам (стандарт §6.8). Выборка по origin, а не по source: у source='cbr'
            # лежат ряды всех импортёров ЦБ, а zcyc_params — источник ровно этого набора.
            # Подчёркивание в LIKE экранировано (\_), как у cbr_int_rate_loans и cbr_bop_*:
            # без экранирования «_» совпал бы с любым одиночным символом. Меньшее число значит
            # ни разу не отработавший шаг cbr_zcyc (агент прочитает уровни, но не узнает, что
            # измеряет ряд и в каких единицах); большее — ряд, которому в источнике не
            # соответствует колонка теноров.
            zcyc_catalog = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'cbr' AND origin LIKE '%zcyc\\_params%'"}))
            zcyc_catalog_rows = result_rows(zcyc_catalog)
            zcyc_catalog_n = zcyc_catalog_rows[0].get("n") if zcyc_catalog_rows else None
            if zcyc_catalog_rows is None:
                zcyc_catalog_detail = f"ответ не распознан: {zcyc_catalog[:200]}"
            elif zcyc_catalog_n == 12:
                zcyc_catalog_detail = "рядов cbr_zcyc: 12 (теноры 0.25…30)"
            else:
                zcyc_catalog_detail = (f"рядов cbr_zcyc: {zcyc_catalog_n}, want 12 — прогнать "
                                       "make import STAT=cbr_zcyc (каталог перепубликовывается прогоном)")
            failed += not expect("каталог cbr_zcyc полон", zcyc_catalog_n == 12, zcyc_catalog_detail)

            # Сырая cbr_zcyc агенту не выдана — как macro_series и cbr_form101: витрина —
            # единственное место, где агент читает уровни КБД. Таблицу создаёт DDL-прогон
            # импортёра, поэтому UNKNOWN_TABLE здесь — провал, а не успех.
            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM cbr_zcyc"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырая cbr_zcyc закрыта", denied,
                                 "отказ ClickHouse 497 на cbr_zcyc (ожидаемо)" if denied else raw[:200])

            # Макропруденциальные требования (key-rate/phase-2-ccyb, #115): три витрины —
            # АЦН-индикаторы, матрицы надбавок и МПЛ. Сырые cbr_ccyb_indicators,
            # cbr_risk_weight_addons и cbr_mpl агенту не выданы: читать контур он обязан
            # только через витрины (комментарии таблиц и колонок есть только у них).
            # Существование витрины подтверждается запросом к ней, а не поиском имени в
            # list_tables (урок bold); пустая витрина — не «нет данных», а неотработавший
            # шаг dagu/cbr.yaml, поэтому числа проверяются, а не только непустота.
            for view in ("v_cbr_ccyb_indicators", "v_cbr_risk_weight_addons", "v_cbr_mpl"):
                failed += not expect(f"витрина {view} видна", view in tables, "list_tables default")

            ccyb = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n, uniqExact(series) AS series, "
                         "min(date) AS min_date, max(date) AS max_date FROM v_cbr_ccyb_indicators"}))
            ccyb_rows = result_rows(ccyb)
            ccyb_n = ccyb_rows[0].get("n") if ccyb_rows else None
            ccyb_series = ccyb_rows[0].get("series") if ccyb_rows else None
            ccyb_min = ccyb_rows[0].get("min_date") if ccyb_rows else None
            ccyb_max = ccyb_rows[0].get("max_date") if ccyb_rows else None
            if ccyb_rows is None:
                ccyb_detail = f"витрина не читается: {ccyb[:200]}"
            else:
                ccyb_detail = (f"строк: {ccyb_n}, рядов: {ccyb_series}, "
                               f"{ccyb_min}..{ccyb_max} — прогнать make import STAT=cbr_ccyb_indicators")
            failed += not expect("витрина v_cbr_ccyb_indicators существует и непуста",
                                 (ccyb_n or 0) > 0, ccyb_detail)
            ccyb_catalog = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'cbr_ccyb_indicators'"}))
            ccyb_catalog_rows = result_rows(ccyb_catalog)
            ccyb_catalog_n = ccyb_catalog_rows[0].get("n") if ccyb_catalog_rows else None
            failed += not expect("каталог cbr_ccyb_indicators описывает 24 ряда",
                                 ccyb_catalog_n == 24,
                                 f"рядов: {ccyb_catalog_n}, want 24 — прогнать "
                                 "make import STAT=cbr_ccyb_indicators")

            rwa = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n, uniqExact(matrix) AS matrices, "
                         "countIf(is_applicable = 0) AS not_applicable FROM v_cbr_risk_weight_addons"}))
            rwa_rows = result_rows(rwa)
            rwa_n = rwa_rows[0].get("n") if rwa_rows else None
            rwa_matrices = rwa_rows[0].get("matrices") if rwa_rows else None
            rwa_na = rwa_rows[0].get("not_applicable") if rwa_rows else None
            if rwa_rows is None:
                rwa_detail = f"витрина не читается: {rwa[:200]}"
            else:
                rwa_detail = (f"строк: {rwa_n}, матриц: {rwa_matrices}, "
                              f"н/п: {rwa_na} — прогнать make import STAT=cbr_risk_weight_addons")
            failed += not expect("витрина v_cbr_risk_weight_addons существует и непуста",
                                 (rwa_n or 0) > 0, rwa_detail)
            failed += not expect("v_cbr_risk_weight_addons: три вида матриц",
                                 rwa_matrices == 3, rwa_detail)
            rwa_catalog = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'cbr_risk_weight_addons'"}))
            rwa_catalog_rows = result_rows(rwa_catalog)
            rwa_catalog_n = rwa_catalog_rows[0].get("n") if rwa_catalog_rows else None
            failed += not expect("каталог cbr_risk_weight_addons описывает 3 матрицы",
                                 rwa_catalog_n == 3,
                                 f"рядов: {rwa_catalog_n}, want 3 — прогнать "
                                 "make import STAT=cbr_risk_weight_addons")

            mpl = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n, uniqExact(series) AS series, "
                         "min(period_from) AS min_from, max(period_from) AS max_from FROM v_cbr_mpl"}))
            mpl_rows = result_rows(mpl)
            mpl_n = mpl_rows[0].get("n") if mpl_rows else None
            mpl_series = mpl_rows[0].get("series") if mpl_rows else None
            mpl_min = mpl_rows[0].get("min_from") if mpl_rows else None
            mpl_max = mpl_rows[0].get("max_from") if mpl_rows else None
            if mpl_rows is None:
                mpl_detail = f"витрина не читается: {mpl[:200]}"
            else:
                mpl_detail = (f"строк: {mpl_n}, рядов: {mpl_series}, "
                              f"{mpl_min}..{mpl_max} — прогнать make import STAT=cbr_mpl")
            failed += not expect("витрина v_cbr_mpl существует и непуста",
                                 (mpl_n or 0) > 0, mpl_detail)
            failed += not expect("v_cbr_mpl: семь видов лимитов", mpl_series == 7, mpl_detail)
            mpl_catalog = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'cbr_mpl'"}))
            mpl_catalog_rows = result_rows(mpl_catalog)
            mpl_catalog_n = mpl_catalog_rows[0].get("n") if mpl_catalog_rows else None
            failed += not expect("каталог cbr_mpl описывает 7 видов лимитов",
                                 mpl_catalog_n == 7,
                                 f"рядов: {mpl_catalog_n}, want 7 — прогнать make import STAT=cbr_mpl")

            for table in ("cbr_ccyb_indicators", "cbr_risk_weight_addons", "cbr_mpl"):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                failed += not expect(f"сырая {table} закрыта", denied,
                                     f"отказ ClickHouse 497 на {table} (ожидаемо)" if denied else raw[:200])

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

            # Календарь публикаций ЦБ и исторические published_at
            # (key-rate/phase-4-cbr-publication-calendar, #299). Витрины создаёт
            # импортёр cbr_publication_calendar; статистика и аналитика — две вкладки
            # cbr.ru/calendar/, quality — точность published_at. Точные даты —
            # проверенные research месяцы: 2026-09 инфляционных ожиданий и мониторинга.
            for view in ("v_cbr_publication_calendar", "v_series_publication"):
                failed += not expect(f"витрина {view} видна", view in tables, "list_tables default")

            calendar_topics = text(await session.call_tool("run_query", {
                "query": "SELECT topic, count() AS n FROM v_cbr_publication_calendar GROUP BY topic ORDER BY topic"}))
            calendar_topic_rows = result_rows(calendar_topics)
            topics = {row.get("topic"): row.get("n") for row in (calendar_topic_rows or [])}
            failed += not expect("календарь: statistics и analytics",
                                 (topics.get("statistics") or 0) >= 173 and (topics.get("analytics") or 0) > 0,
                                 calendar_topics[:200])

            qualities = text(await session.call_tool("run_query", {
                "query": "SELECT quality, count() AS n FROM v_series_publication GROUP BY quality ORDER BY quality"}))
            quality_rows = result_rows(qualities)
            quality = {row.get("quality"): row.get("n") for row in (quality_rows or [])}
            failed += not expect("published_at: date и fallback",
                                 (quality.get("date") or 0) > 0 and (quality.get("fallback") or 0) > 0,
                                 qualities[:200])

            exact = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n FROM v_series_publication "
                         "WHERE source='cbr' AND series='ожидаемая инфляция' AND period_date='2026-09-14' "
                         "AND quality='date' AND toDate(published_at)='2026-09-29'"}))
            exact_rows = result_rows(exact)
            exact_n = exact_rows[0].get("n") if exact_rows else None
            failed += not expect("cbr_infl_exp 2026-09 -> 2026-09-29", exact_n == 1, exact[:200])

            survey = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n FROM v_series_publication "
                         "WHERE source='cbr_survey' AND series='price_expectations' AND period_date='2026-09-01' "
                         "AND quality='date' AND toDate(published_at)='2026-09-15'"}))
            survey_rows = result_rows(survey)
            survey_n = survey_rows[0].get("n") if survey_rows else None
            failed += not expect("cbr_survey 2026-09 -> 2026-09-15", survey_n == 1, survey[:200])

            for table in ("cbr_publication_calendar", "series_publication"):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                failed += not expect(f"сырая {table} закрыта", denied,
                                     f"отказ ClickHouse 497 на {table} (ожидаемо)" if denied else raw[:200])

            # ИЦП: витрина v_rosstat_ppi над rosstat_ppi_ved/product/weights и ppi_sa
            # (T5 #280). Ожидаемо 571 + 336 512 + 758 + 1 744 = 339 585 строк,
            # 5 408 рядов, 20 NULL-краёв ppi_sa; каталог ppi_sa — 25 рядов
            # (5 базовых + 20 метрик витрины). Сырые таблицы закрыты.
            failed += not expect("витрина v_rosstat_ppi видна", "v_rosstat_ppi" in tables, "list_tables default")

            ppi_totals = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n, uniqExact(series) AS series, countIf(value IS NULL) AS nulls FROM v_rosstat_ppi"}))
            ppi_total_rows = result_rows(ppi_totals)
            ppi_n = ppi_total_rows[0].get("n") if ppi_total_rows else None
            ppi_series = ppi_total_rows[0].get("series") if ppi_total_rows else None
            ppi_nulls = ppi_total_rows[0].get("nulls") if ppi_total_rows else None
            failed += not expect("v_rosstat_ppi: ≥339 585 строк / ≥5 408 рядов / ≥20 NULL",
                                 (ppi_n or 0) >= 339585 and (ppi_series or 0) >= 5408 and (ppi_nulls or 0) >= 20,
                                 ppi_totals[:200])

            ppi_sources = text(await session.call_tool("run_query", {
                "query": "SELECT source, count() AS n FROM v_rosstat_ppi GROUP BY source ORDER BY source"}))
            ppi_source_rows = result_rows(ppi_sources)
            ppi_source = {row.get("source"): row.get("n") for row in (ppi_source_rows or [])}
            failed += not expect("v_rosstat_ppi: rosstat ≥337 841 / ppi_sa ≥1 744",
                                 (ppi_source.get("rosstat") or 0) >= 337841 and (ppi_source.get("ppi_sa") or 0) >= 1744,
                                 ppi_sources[:200])

            ppi_catalog = text(await session.call_tool("run_query", {
                "query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'ppi_sa'"}))
            ppi_catalog_rows = result_rows(ppi_catalog)
            ppi_catalog_n = ppi_catalog_rows[0].get("n") if ppi_catalog_rows else None
            failed += not expect("каталог ppi_sa: 25 рядов", ppi_catalog_n == 25, ppi_catalog[:200])

            ppi_spot = text(await session.call_tool("run_query", {
                "query": "SELECT date, value FROM v_rosstat_ppi WHERE source='ppi_sa' "
                         "AND series='ppi_sa:core:saar3' AND date='2026-08-31'"}))
            ppi_spot_rows = result_rows(ppi_spot)
            ppi_spot_ok = bool(ppi_spot_rows) and str(ppi_spot_rows[0].get("date")) == "2026-08-31" \
                and abs(float(ppi_spot_rows[0].get("value") or 0) - 6.0788) < 0.001
            failed += not expect("v_rosstat_ppi: core saar3 2026-08-31 = 6,0788", ppi_spot_ok, ppi_spot[:200])

            for table in ("rosstat_ppi_ved", "rosstat_ppi_product", "rosstat_ppi_weights", "ppi_sa"):
                raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
                denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
                failed += not expect(f"сырая {table} закрыта", denied,
                                     f"отказ ClickHouse 497 на {table} (ожидаемо)" if denied else raw[:200])

    return failed


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
