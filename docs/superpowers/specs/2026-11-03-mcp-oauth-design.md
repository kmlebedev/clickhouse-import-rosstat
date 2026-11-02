# MCP OAuth: Google + email allowlist (дизайн)

## Проблема

Публичный MCP `rosstat-staging` (`https://mcp.selfinvesting.ru:8443/mcp`) авторизует
static bearer-токеном `CLICKHOUSE_MCP_AUTH_TOKEN`. Плагин `rosstat-mcp` передаёт его через
`ROSSTAT_MCP_TOKEN` (`bearerTokenEnvVar` / `headers` / `bearer_token_env_var`). В Kimi Code
Desktop, запущенном из Finder, переменная не видна: GUI не наследует `.zshrc`/`.bash_profile`,
а `launchctl setenv` не переживает перезагрузку. Для обычного пользователя выдача и ввод
токена — барьер.

## Решение

Cutover на `:8443`: static bearer удаляется, включается Google OAuth через FastMCP
`GoogleProvider`. Доступ не расширяется: наследник `RosstatGoogleProvider` принимает только
`email` из `ROSSTAT_MCP_ALLOWED_GOOGLE_EMAILS` с `email_verified=true`; пустой allowlist —
ошибка старта.

`mcp-clickhouse` 0.7.0 уже умеет `FASTMCP_SERVER_AUTH`; для кастомного класса его
env-совместимость не передаёт kwargs, поэтому `RosstatGoogleProvider.__init__` сам читает
`FASTMCP_SERVER_AUTH_GOOGLE_*`.

## Безопасность

- `email_verified` обязателен; allowlist нормализуется (trim/lowercase), fail closed.
- Static bearer не используется; unit делает `UnsetEnvironment=CLICKHOUSE_MCP_AUTH_TOKEN`,
  preflight падает, если переменная всё ещё задана.
- `CLICKHOUSE_MCP_ALLOWED_ORIGINS` содержит публичный origin: consent-страница FastMCP
  отправляет POST с `Origin`, иначе `DNSRebindingProtectionMiddleware` вернёт 403.
- `FASTMCP_SERVER_AUTH_GOOGLE_JWT_SIGNING_KEY` стабилен между рестартами; иначе сессии
  инвалидируются при смене ключа.
- `kimi_reader` остаётся read-only (`readonly = 2`, только `v_*`).

## Файлы

- `deploy/staging/rosstat_mcp_auth.py` — провайдер и allowlist.
- `deploy/staging/rosstat_mcp_auth_test.py` — офлайн-тесты (8 кейсов).
- `deploy/staging/mcp-clickhouse.service` — `PYTHONPATH`, `UnsetEnvironment`.
- `deploy/staging/clickhouse-import-rosstat.env.example` — OAuth-блок.
- `Makefile` — `mcp-auth-test`, копирование провайдера, preflight/smoke.
- `deploy/staging/README.md`, `ARCHITECTURE.md` §6.5, `README.md`.
- Плагин `rosstat-mcp` 0.5.0 — манифесты без токена, Kimi `auth: "oauth"`.

## Проверка

- `make mcp-auth-test`: allowed+verified → токен; unverified/чужой → `None`; пустой
  allowlist/битая запись → `ValueError`.
- Smoke: `/health` 200; `POST /mcp` без токена → 401 + `WWW-Authenticate: resource_metadata`;
  оба `/.well-known/oauth-*` 200; полный Google-вход — вручную в клиенте.
- Регрессия: `make all` (Go), `python3 -m unittest` в плагине.
- Откат: вернуть `CLICKHOUSE_MCP_AUTH_TOKEN` и старый юнит; 0.4.x снова работает, 0.5.0 — нет.

## Ограничения

Google-аккаунт нужен каждому пользователю; Telegram Login — не OAuth 2.1, для него нужен
отдельный authorization server. Allowlist — точные email; домены/группы — отдельный этап.
