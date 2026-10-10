import base64
import os
import sys
import urllib.error
import urllib.request

SQL_PATH = "sql/mcp_kimi_reader.sql"
PLACEHOLDER = "<your-password>"


def setup_url():
    """Адрес ClickHouse для DDL. CH_SETUP_URL перекрывает локальный default.

    На staging у пользователя default есть пароль и скрипт запускается на самом хосте,
    поэтому адрес и креды приходят переменными, а не вычисляются из порта.
    """
    explicit = os.environ.get("CH_SETUP_URL")
    if explicit:
        return explicit
    return f"http://localhost:{os.environ.get('CH_HTTP_PORT', '8123')}/"


def auth_headers(user, password):
    """Заголовок Basic для исполнения DDL от имени user. Пустой user — без аутентификации.

    Логин исполнителя DDL и пароль создаваемого пользователя — разные сущности:
    CLICKHOUSE_PASSWORD (см. main) — пароль создаваемого kimi_reader.
    """
    if not user:
        return {}
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def sql_path():
    """Путь к SQL с гранты. CH_SETUP_SQL перекрывает путь относительно репозитория.

    На staging скрипт запускается из /tmp, поэтому относительный путь по умолчанию
    там не работает — путь задаётся переменной.
    """
    return os.environ.get("CH_SETUP_SQL", SQL_PATH)


def mask(secret, text):
    """Убирает секрет из текста сообщения.

    Слепой replace портит слова, содержащие короткий пароль ('x' ломает 'Exception'),
    поэтому заменяются только вхождения, не окружённые буквами и цифрами.
    """
    if not secret:
        return text
    import re

    return re.sub(rf"(?<![0-9A-Za-z]){re.escape(secret)}(?![0-9A-Za-z])", "***", text)


def main():
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    if not password or "'" in password or "\\" in password:
        print("CLICKHOUSE_PASSWORD не задан или содержит кавычку/обратный слеш", file=sys.stderr)
        return 2
    url = setup_url()
    setup_user = os.environ.get("CH_SETUP_USER", "")
    setup_password = os.environ.get("CH_SETUP_PASSWORD", "")
    with open(sql_path(), encoding="utf-8") as f:
        sql = "".join(line for line in f if not line.lstrip().startswith("--"))
    sql = sql.replace(PLACEHOLDER, password)
    for stmt in (s.strip() for s in sql.split(";")):
        if not stmt:
            continue
        label = mask(password, stmt.splitlines()[0])
        try:
            urllib.request.urlopen(
                urllib.request.Request(
                    url, data=stmt.encode("utf-8"), headers=auth_headers(setup_user, setup_password)
                ),
                timeout=30,
            ).read()
        except urllib.error.HTTPError as e:
            body = mask(password, e.read().decode("utf-8", "replace").strip())
            who = setup_user or "anonymous"
            print(f"FAIL {label} (исполнитель: {who}): {body}", file=sys.stderr)
            return 1
        print(f"OK   {label}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
