# Copyright 2021-present StarRocks, Inc. All rights reserved.
"""Pytest hooks and shared fixtures."""

import os

import pytest

# Cached (ok, detail_message) for integration probe; None = not yet probed.
_integration_probe_cache: tuple[bool, str] | None = None


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: requires a live StarRocks FE (skipped when unreachable unless STARROCKS_INTEGRATION_STRICT=1)",
    )
    config.addinivalue_line(
        "markers",
        "read_only: integration-style tests that only run SELECT/SHOW/DESCRIBE-style traffic (no DDL/DML)",
    )
    config.addinivalue_line(
        "markers",
        "integration_mutating: integration tests that CREATE/DROP/INSERT (need DDL privileges; skipped when STARROCKS_INTEGRATION_READ_ONLY=1)",
    )


def _run_integration_probe() -> tuple[bool, str]:
    """Return (True, '') if SHOW DATABASES succeeds, else (False, error text)."""
    global _integration_probe_cache
    if _integration_probe_cache is not None:
        return _integration_probe_cache

    from src.mcp_server_starrocks.db_client import DBClient, reset_db_connections

    reset_db_connections()
    client = DBClient()
    probe = client.execute("SHOW DATABASES")
    if probe.success:
        _integration_probe_cache = (True, "")
    else:
        _integration_probe_cache = (False, probe.error_message or "unknown error")
    return _integration_probe_cache


def pytest_runtest_setup(item):
    if os.getenv("STARROCKS_INTEGRATION_READ_ONLY", "").lower() in ("1", "true", "yes"):
        if item.get_closest_marker("integration_mutating") is not None:
            pytest.skip(
                "STARROCKS_INTEGRATION_READ_ONLY=1 — skipping integration tests that require CREATE/DROP/INSERT"
            )

    if item.get_closest_marker("integration") is None:
        return
    if os.getenv("STARROCKS_DUMMY_TEST", "").lower() in ("1", "true", "yes"):
        pytest.skip("STARROCKS_DUMMY_TEST is set — skipping integration tests")

    ok, detail = _run_integration_probe()
    if ok:
        return

    msg = f"StarRocks not reachable: {detail}"
    strict = os.getenv("STARROCKS_INTEGRATION_STRICT", "").lower() in ("1", "true", "yes")
    if strict:
        pytest.fail(
            f"{msg}\n"
            "Set STARROCKS_URL or STARROCKS_HOST/PORT/USER/PASSWORD, ensure FE is up, "
            "then unset STARROCKS_DUMMY_TEST. See README \"Testing with a real StarRocks cluster\"."
        )
    pytest.skip(msg)
