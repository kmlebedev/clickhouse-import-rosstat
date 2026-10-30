#!/usr/bin/env python3
"""Smoke-проверка Grafana-алертов live-мониторинга ML (`make grafana-check`).

Проверяет, что provisioning-файл `grafana/provisioning/alerting/ml-live.yaml`
доехал до стенда и Grafana видит оба правила:
  * `ml-live-degradation` — threshold 100 б.п., severity=warning;
  * `ml-live-freshness` — threshold 3 дня, severity=warning.

Запрос — GET /api/v1/provisioning/alert-rules с Bearer-токеном сервис-аккаунта;
SQL/порог проверяются в офлайн-тесте `views/ml_live_alert_test.go`, здесь —
факт загрузки правила и его метки. Токен не выводится.

Вход: env GRAFANA_URL, GRAFANA_TOKEN. Выход: 0/1/2 (как scripts/grafana_check.py).
"""

import json
import os
import sys
import urllib.error
import urllib.request

HTTP_TIMEOUT = 30
EXPECTED = {
    "ml-live-degradation": "degradation",
    "ml-live-freshness": "freshness",
}


def expect(label, ok, detail):
    print(f"{'OK  ' if ok else 'FAIL'} {label}: {detail}")
    return ok


def request(url, token):
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")
    except OSError as exc:
        return 0, f"{type(exc).__name__}: {exc}"


def rules_by_uid(payload):
    """Правила из ответа API: список или обёртка с items/rules."""
    if isinstance(payload, list):
        return {str(rule.get("uid", "")): rule for rule in payload if isinstance(rule, dict)}
    if isinstance(payload, dict):
        for key in ("items", "rules", "alertRules"):
            value = payload.get(key)
            if isinstance(value, list):
                return {str(rule.get("uid", "")): rule for rule in value if isinstance(rule, dict)}
    return {}


def main() -> int:
    url = os.environ.get("GRAFANA_URL", "").rstrip("/")
    token = os.environ.get("GRAFANA_TOKEN", "")
    if not url or not token:
        print("нужны GRAFANA_URL и GRAFANA_TOKEN в окружении (значение токена не выводится)",
              file=sys.stderr)
        return 2

    status, body = request(f"{url}/api/v1/provisioning/alert-rules", token)
    if status != 200:
        print(f"FAIL alert-rules: HTTP {status}: {body[:200]}", file=sys.stderr)
        print("ПРОВАЛЕНО: 1")
        return 1
    try:
        payload = json.loads(body)
    except ValueError:
        print(f"FAIL alert-rules: ответ не JSON: {body[:200]}", file=sys.stderr)
        print("ПРОВАЛЕНО: 1")
        return 1

    rules = rules_by_uid(payload)
    failed = 0
    for uid, label in EXPECTED.items():
        rule = rules.get(uid)
        if rule is None:
            failed += not expect(f"алерт {uid}", False, "не найден: проверьте provisioning/reload")
            continue
        labels = rule.get("labels") or {}
        paused = bool(rule.get("isPaused") or rule.get("paused"))
        ok = labels.get("severity") == "warning" and labels.get("contour") == "ml-live" and not paused
        detail = (f"title={rule.get('title')!r}, for={rule.get('for')!r}, "
                  f"severity={labels.get('severity')!r}, contour={labels.get('contour')!r}, paused={paused}")
        failed += not expect(f"алерт {uid} ({label})", ok, detail)

    print("ПРОВЕРКА ПРОЙДЕНА" if not failed else f"ПРОВАЛЕНО: {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
