"""Smoke-проверка дашборда Grafana как кода (`make grafana-check`).

Контракт (бриф task-3-brief.md):
  1. GET /api/dashboards/uid/<uid> -> 200, в теле тот же uid;
  2. первые 2 панели с таргетами: для каждого таргета POST /api/ds/query и требование
     непустого ответа (frames с точками).

Запросы НЕ дублируются в скрипте: SQL берётся из самого дашборда, снятого со стенда,
иначе smoke проверял бы не то, что реально задеплоено.

Вход: env GRAFANA_URL, GRAFANA_TOKEN; аргумент --uid (по умолчанию gold-thesis).
Выход: 0 — все проверки прошли, 1 — хотя бы одна упала, 2 — не задано окружение.
Печать — строки `OK ...`/`FAIL ...` в стиле scripts/mcp_check.py; токен не выводится.
Зависимостей нет: только stdlib (urllib), как в scripts/mcp_check.py.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

# Таймаут с запасом: ds/query на холодном стенде поднимает плагин и читает витрину.
HTTP_TIMEOUT = 60

# Датасорс по умолчанию — если панель и таргет не задали свой (в артефакте он задан на панели).
# Значения совпадают с grafana/clickhouse.go (DatasourceType/DatasourceUID).
DEFAULT_DATASOURCE = {"type": "grafana-clickhouse-datasource", "uid": "ch-rosstat"}

# Сколько панелей с таргетами проверять: smoke не должен превращаться в обход всего дашборда.
PANELS_TO_CHECK = 2

# Диапазон по умолчанию, если дашборд не задал свой spec.time.
DEFAULT_TIME_FROM = "now-1y"
DEFAULT_TIME_TO = "now"


def expect(label, ok, detail):
    print(f"{'OK  ' if ok else 'FAIL'} {label}: {detail}")
    return ok


def send(req):
    """Выполняет запрос; HTTP-ошибка — тоже ответ (код + тело), а не исключение."""
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")
    except OSError as exc:
        return 0, f"{type(exc).__name__}: {exc}"


def request(method, url, token, payload=None):
    """Запрос к Grafana с токеном сервис-аккаунта. Значение токена нигде не печатается."""
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    return send(req)


def load_json(body):
    try:
        return json.loads(body)
    except ValueError:
        return None


def dashboard_spec(payload):
    """Дашборд из ответа: классический API отдаёт {"dashboard": ...}, k8s-стиль — {"spec": ...}."""
    if not isinstance(payload, dict):
        return None
    for key in ("dashboard", "spec", "resource"):
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return payload if isinstance(payload.get("panels"), list) else None


def panels_with_targets(dash):
    """Панели с SQL-таргетами в порядке отрисовки; collapsed-ряды раскрываются рекурсивно."""
    found = []

    def walk(panels):
        for panel in panels or []:
            if not isinstance(panel, dict):
                continue
            targets = [t for t in (panel.get("targets") or [])
                       if isinstance(t, dict) and t.get("rawSql")]
            if targets:
                found.append((panel, targets))
            walk(panel.get("panels"))

    walk(dash.get("panels"))
    return found


def query_payload(panel, target, dash):
    """Тело POST /api/ds/query для одного таргета панели (форма запроса — как у фронтенда)."""
    time_range = dash.get("time") or {}
    return {
        "from": time_range.get("from") or DEFAULT_TIME_FROM,
        "to": time_range.get("to") or DEFAULT_TIME_TO,
        "queries": [{
            "refId": target.get("refId") or "A",
            "datasource": target.get("datasource") or panel.get("datasource") or DEFAULT_DATASOURCE,
            "rawSql": target["rawSql"],
            "format": target.get("format", 0),
            "queryType": target.get("queryType", "timeseries"),
            "intervalMs": 86400000,
            "maxDataPoints": 1000,
        }],
    }


def frame_points(frame):
    """Точки в одном frame: длина самого длинного ряда в data.values."""
    values = (frame.get("data") or {}).get("values") or []
    lengths = [len(series) for series in values if isinstance(series, list)]
    return max(lengths) if lengths else 0


def response_points(body):
    """(точек, ошибка) по ответу ds/query: точки суммируются по refId, ошибки плагина — текстом."""
    payload = load_json(body)
    if not isinstance(payload, dict):
        return 0, f"ответ не JSON: {body[:200]}"
    results = payload.get("results")
    if not isinstance(results, dict) or not results:
        return 0, f"нет results: {body[:200]}"
    points, errors = 0, []
    for ref_id, result in results.items():
        if not isinstance(result, dict):
            errors.append(f"{ref_id}: неожиданный ответ")
            continue
        if result.get("error"):
            errors.append(f"{ref_id}: {result['error']}")
            continue
        frames = result.get("frames") or []
        counts = [frame_points(frame) for frame in frames if isinstance(frame, dict)]
        if not counts or max(counts) == 0:
            errors.append(f"{ref_id}: frames без точек")
            continue
        points += max(counts)
    return points, "; ".join(errors)


def main():
    parser = argparse.ArgumentParser(description="Smoke-проверка дашборда Grafana (uid и панели).")
    parser.add_argument("--uid", default="gold-thesis", help="uid дашборда (по умолчанию gold-thesis)")
    args = parser.parse_args()

    url = os.environ.get("GRAFANA_URL", "").rstrip("/")
    token = os.environ.get("GRAFANA_TOKEN", "")
    if not url or not token:
        print("нужны GRAFANA_URL и GRAFANA_TOKEN в окружении (значение токена не выводится)",
              file=sys.stderr)
        return 2

    failed = 0

    status, body = request("GET", f"{url}/api/dashboards/uid/{args.uid}", token)
    payload = load_json(body)
    dash = dashboard_spec(payload) if payload is not None else None
    if status != 200:
        ok, detail = False, f"HTTP {status}: {body[:200]}"
    elif dash is None:
        ok, detail = False, f"в ответе нет дашборда: {body[:200]}"
    elif dash.get("uid") != args.uid:
        ok, detail = False, f"uid в ответе {dash.get('uid')!r}, ожидался {args.uid!r}"
    else:
        ok = True
        detail = f"uid={args.uid}, title={dash.get('title')!r}, версия {dash.get('version')}"
    failed += not expect("дашборд виден", ok, detail)
    if not ok:
        print(f"ПРОВАЛЕНО: {failed}")
        return 1

    panels = panels_with_targets(dash)
    if panels:
        failed += not expect("панелей с таргетами", True,
                             f"{len(panels)}, проверяю первые {min(len(panels), PANELS_TO_CHECK)}")
    else:
        failed += not expect("панелей с таргетами", False, "ни одной панели с rawSql-таргетом")

    for panel, targets in panels[:PANELS_TO_CHECK]:
        title = panel.get("title") or f"id={panel.get('id')}"
        points, details, problems = 0, [], []
        for target in targets:
            ref_id = target.get("refId") or "A"
            ds_status, ds_body = request("POST", f"{url}/api/ds/query", token,
                                         query_payload(panel, target, dash))
            if ds_status != 200:
                problems.append(f"{ref_id}: HTTP {ds_status}: {ds_body[:200]}")
                continue
            target_points, target_error = response_points(ds_body)
            if target_error:
                problems.append(f"{ref_id}: {target_error}")
            elif target_points == 0:
                problems.append(f"{ref_id}: пустой ответ")
            else:
                details.append(f"{ref_id}: {target_points} точек")
                points += target_points
        failed += not expect(f"панель \"{title}\" вернула {points} точек",
                             bool(details) and not problems,
                             "; ".join(problems) if problems else ", ".join(details))

    print("ПРОВЕРКА ПРОЙДЕНА" if not failed else f"ПРОВАЛЕНО: {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
