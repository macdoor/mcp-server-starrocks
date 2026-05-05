"""Tests for catalog_summary / get_catalog_summary."""

import pytest

from src.mcp_server_starrocks.db_summary_manager import DatabaseSummaryManager
from src.mcp_server_starrocks.db_client import ResultSet


class TestGetCatalogSummary:
    def test_calls_show_databases_and_per_database_summary(self, monkeypatch):
        statements = []

        class MockClient:
            def execute(self, statement, db=None, return_format="raw"):
                statements.append((statement, db))
                if "SHOW DATABASES FROM" in statement:
                    return ResultSet(
                        success=True,
                        column_names=["Database"],
                        rows=[["db_a"], ["db_b"]],
                        execution_time=0.01,
                    )
                return ResultSet(success=True, column_names=["c"], rows=[["1"]], execution_time=0.01)

        mgr = DatabaseSummaryManager(MockClient())
        captured = []

        def fake_get_database_summary(db, limit=10000, refresh=False):
            captured.append((db, limit, refresh))
            return f"MOCK_SUMMARY[{db}]"

        monkeypatch.setattr(mgr, "get_database_summary", fake_get_database_summary)

        out = mgr.get_catalog_summary("hive", limit_per_database=500, max_databases=10, refresh=True)

        assert "hive" in out
        assert "MOCK_SUMMARY[hive.db_a]" in out
        assert "MOCK_SUMMARY[hive.db_b]" in out
        assert captured == [
            ("hive.db_a", 500, True),
            ("hive.db_b", 500, True),
        ]
        assert any("SHOW DATABASES FROM `hive`" in s[0] for s in statements)
