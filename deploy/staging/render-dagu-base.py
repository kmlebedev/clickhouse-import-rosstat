#!/usr/bin/env python3
"""Подставить SMTP-получателя в base DAG dagu из env-файла хоста.

Dagu 2.18.2 раскрывает ${…} в env шагов, но не в smtp/error_mail: там
значения остаются литералами. Скрипт читает /etc/clickhouse-import-rosstat.env
и заменяет плейсхолдеры __SMTP_*__/__MAIL_*__; значения не печатает.

Usage: render-dagu-base.py TEMPLATE ENV_FILE OUTPUT
"""
import os
import re
import sys

TEMPLATE, ENV_FILE, OUTPUT = sys.argv[1:4]
REQUIRED = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD", "MAIL_FROM", "MAIL_TO"]


def read_env(path):
    values = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line or line.lstrip().startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value
    return values


def yaml_quote(value):
    if any(ch in value for ch in ("\n", "\r", "\x00")):
        raise SystemExit("значение содержит управляющий символ")
    return value.replace("\\", "\\\\").replace('"', '\\"')


env = read_env(ENV_FILE)
missing = [key for key in REQUIRED if not env.get(key, "")]
if missing:
    print("не заданы в env-файле: " + ", ".join(missing), file=sys.stderr)
    raise SystemExit(2)
try:
    port = int(env["SMTP_PORT"])
except ValueError:
    print("SMTP_PORT должен быть числом", file=sys.stderr)
    raise SystemExit(2)
if not 1 <= port <= 65535:
    print("SMTP_PORT вне диапазона 1..65535", file=sys.stderr)
    raise SystemExit(2)

with open(TEMPLATE, encoding="utf-8") as f:
    text = f.read()
repl = {
    "__SMTP_HOST__": yaml_quote(env["SMTP_HOST"]),
    "__SMTP_PORT__": str(port),
    "__SMTP_USER__": yaml_quote(env["SMTP_USER"]),
    "__SMTP_PASSWORD__": yaml_quote(env["SMTP_PASSWORD"]),
    "__MAIL_FROM__": yaml_quote(env["MAIL_FROM"]),
    "__MAIL_TO__": yaml_quote(env["MAIL_TO"]),
}
for key, value in repl.items():
    text = text.replace(key, value)

left = sorted(set(re.findall(r"__[A-Z0-9_]+__", text)))
if left:
    print("остались плейсхолдеры: " + ", ".join(left), file=sys.stderr)
    raise SystemExit(2)
with open(OUTPUT, "w", encoding="utf-8") as f:
    f.write(text)
os.chmod(OUTPUT, 0o600)
print(f"rendered {OUTPUT}")
