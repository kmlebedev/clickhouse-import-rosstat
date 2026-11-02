#!/usr/bin/env python3
"""Offline tests for the Google OAuth allowlist provider.

Run from the repository root::

    uv run --with mcp-clickhouse==0.7.0 --python 3.12 \
        python deploy/staging/rosstat_mcp_auth_test.py -v
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import rosstat_mcp_auth as auth
from fastmcp.server.auth.auth import AccessToken


class AllowlistTest(unittest.TestCase):
    def test_parse_allowed_emails(self) -> None:
        self.assertEqual(
            auth.parse_allowed_emails(" A@X.com, b@y.com "),
            frozenset({"a@x.com", "b@y.com"}),
        )

    def test_empty_allowlist_rejected(self) -> None:
        with self.assertRaises(ValueError):
            auth.parse_allowed_emails(" , ")

    def test_entry_without_at_rejected(self) -> None:
        with self.assertRaises(ValueError):
            auth.parse_allowed_emails("not-an-email")

    def test_verified_listed_email_allowed(self) -> None:
        allowed = frozenset({"a@x.com"})
        self.assertTrue(auth.is_email_allowed({"email": "A@X.com", "email_verified": True}, allowed))
        self.assertTrue(auth.is_email_allowed({"email": "a@x.com", "email_verified": "true"}, allowed))

    def test_unverified_or_foreign_email_rejected(self) -> None:
        allowed = frozenset({"a@x.com"})
        self.assertFalse(auth.is_email_allowed({"email": "a@x.com", "email_verified": False}, allowed))
        self.assertFalse(auth.is_email_allowed({"email": "a@x.com"}, allowed))
        self.assertFalse(auth.is_email_allowed({"email": "b@x.com", "email_verified": True}, allowed))
        self.assertFalse(auth.is_email_allowed({"email": None, "email_verified": True}, allowed))


class ProviderTest(unittest.IsolatedAsyncioTestCase):
    async def test_load_access_token_allows_listed(self) -> None:
        async def fake_super(self, token: str) -> AccessToken:
            return AccessToken(
                token=token,
                client_id="client",
                scopes=[],
                expires_at=None,
                subject="subject",
                claims={"email": "a@x.com", "email_verified": True},
            )

        with patch.object(auth.GoogleProvider, "load_access_token", fake_super):
            provider = object.__new__(auth.RosstatGoogleProvider)
            provider._allowed_emails = frozenset({"a@x.com"})
            self.assertIsNotNone(await provider.load_access_token("token"))

    async def test_load_access_token_rejects_unlisted(self) -> None:
        async def fake_super(self, token: str) -> AccessToken:
            return AccessToken(
                token=token,
                client_id="client",
                scopes=[],
                expires_at=None,
                subject="subject",
                claims={"email": "b@x.com", "email_verified": True},
            )

        with patch.object(auth.GoogleProvider, "load_access_token", fake_super):
            provider = object.__new__(auth.RosstatGoogleProvider)
            provider._allowed_emails = frozenset({"a@x.com"})
            self.assertIsNone(await provider.load_access_token("token"))

    def test_init_fails_closed_without_allowlist(self) -> None:
        with patch.object(auth.GoogleProvider, "__init__", return_value=None), patch.dict(
            os.environ, {"ROSSTAT_MCP_ALLOWED_GOOGLE_EMAILS": ""}, clear=False
        ):
            with self.assertRaises(ValueError):
                auth.RosstatGoogleProvider()


if __name__ == "__main__":
    unittest.main()
