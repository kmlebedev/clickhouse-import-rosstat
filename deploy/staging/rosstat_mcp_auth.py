"""Google OAuth provider with an email allowlist for the public MCP server.

``mcp-clickhouse`` loads ``FASTMCP_SERVER_AUTH`` as a full class path. For a
custom path its FastMCP-2 compatibility layer passes no constructor kwargs, so
this class reads the standard ``FASTMCP_SERVER_AUTH_GOOGLE_*`` variables itself.
Access stays restricted: only verified Google emails from
``ROSSTAT_MCP_ALLOWED_GOOGLE_EMAILS`` are accepted. An empty allowlist is a
startup error, never an "allow everyone" mode.
"""

from __future__ import annotations

import logging
import os

from fastmcp.server.auth.auth import AccessToken
from fastmcp.server.auth.providers.google import GoogleProvider
from fastmcp.utilities.auth import parse_scopes

logger = logging.getLogger("mcp-clickhouse.auth")

DEFAULT_SCOPES = ("openid", "email", "profile")


def parse_allowed_emails(raw: str) -> frozenset[str]:
    """Parse and validate a comma-separated email allowlist."""
    emails = frozenset(part.strip().lower() for part in raw.split(",") if part.strip())
    if not emails:
        raise ValueError(
            "ROSSTAT_MCP_ALLOWED_GOOGLE_EMAILS is empty; refusing to start with open access"
        )
    invalid = sorted(email for email in emails if "@" not in email)
    if invalid:
        raise ValueError(
            "ROSSTAT_MCP_ALLOWED_GOOGLE_EMAILS contains invalid entries: " + ", ".join(invalid)
        )
    return emails


def is_email_allowed(claims: dict, allowed: frozenset[str]) -> bool:
    """Return whether Google claims describe a verified, allow-listed email."""
    verified = claims.get("email_verified")
    if verified not in (True, 1, "true", "True", "1"):
        return False
    email = claims.get("email")
    if not isinstance(email, str):
        return False
    return email.strip().lower() in allowed


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


class RosstatGoogleProvider(GoogleProvider):
    """Google OAuth provider that only admits emails from an allowlist."""

    def __init__(self) -> None:
        allowed = parse_allowed_emails(os.getenv("ROSSTAT_MCP_ALLOWED_GOOGLE_EMAILS", ""))
        raw_scopes = os.getenv(
            "FASTMCP_SERVER_AUTH_GOOGLE_REQUIRED_SCOPES", ",".join(DEFAULT_SCOPES)
        )
        scopes = parse_scopes(raw_scopes) or list(DEFAULT_SCOPES)
        super().__init__(
            client_id=_require_env("FASTMCP_SERVER_AUTH_GOOGLE_CLIENT_ID"),
            client_secret=_require_env("FASTMCP_SERVER_AUTH_GOOGLE_CLIENT_SECRET"),
            base_url=_require_env("FASTMCP_SERVER_AUTH_GOOGLE_BASE_URL"),
            required_scopes=scopes,
            jwt_signing_key=os.getenv("FASTMCP_SERVER_AUTH_GOOGLE_JWT_SIGNING_KEY") or None,
        )
        self._allowed_emails = allowed
        logger.info("Google OAuth allowlist loaded: %d email(s)", len(allowed))

    async def load_access_token(self, token: str) -> AccessToken | None:
        access = await super().load_access_token(token)
        if access is None:
            return None
        if not is_email_allowed(access.claims or {}, self._allowed_emails):
            logger.info("Rejected Google account outside ROSSTAT_MCP_ALLOWED_GOOGLE_EMAILS")
            return None
        return access
