import os
import sys
import urllib.error
import urllib.request

SQL_PATH = "sql/mcp_kimi_reader.sql"
PLACEHOLDER = "<your-password>"


def main():
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    if not password or "'" in password or "\\" in password:
        print("CLICKHOUSE_PASSWORD не задан или содержит кавычку/обратный слеш", file=sys.stderr)
        return 2
    port = os.environ.get("CH_HTTP_PORT", "8123")
    url = f"http://localhost:{port}/"
    with open(SQL_PATH, encoding="utf-8") as f:
        sql = "".join(line for line in f if not line.lstrip().startswith("--"))
    sql = sql.replace(PLACEHOLDER, password)
    for stmt in (s.strip() for s in sql.split(";")):
        if not stmt:
            continue
        label = stmt.splitlines()[0].replace(password, "***")
        try:
            urllib.request.urlopen(urllib.request.Request(url, data=stmt.encode("utf-8")), timeout=30).read()
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace").strip().replace(password, "***")
            print(f"FAIL {label}: {body}", file=sys.stderr)
            return 1
        print(f"OK   {label}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
