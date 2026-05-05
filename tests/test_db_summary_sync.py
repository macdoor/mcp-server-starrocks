"""Tests for db_summary table list sync (SHOW DATA vs SHOW TABLES fallback)."""

from src.mcp_server_starrocks.db_summary_manager import DatabaseSummaryManager
from src.mcp_server_starrocks.db_client import ResultSet


def test_sync_table_list_falls_back_to_show_tables_when_show_data_fails():
    calls: list[tuple[str, str | None]] = []

    class MockClient:
        def execute(self, statement, db=None, return_format="raw"):
            calls.append((statement.strip(), db))
            if statement.strip() == "SHOW DATA":
                return ResultSet(
                    success=False,
                    error_message="SHOW DATA not supported for external catalog",
                    execution_time=0.0,
                )
            if "SHOW TABLES FROM" in statement:
                assert "`cat`.`db`" in statement
                return ResultSet(
                    success=True,
                    column_names=["Tables_in_db"],
                    rows=[["alpha"], ["beta"]],
                    execution_time=0.01,
                )
            raise AssertionError(f"unexpected statement: {statement!r}")

    mgr = DatabaseSummaryManager(MockClient())
    assert mgr._sync_table_list("cat.db", force=True) is True
    assert mgr.table_cache[("cat.db", "alpha")].name == "alpha"
    assert mgr.table_cache[("cat.db", "beta")].name == "beta"
    assert [c[0] for c in calls[:2]] == ["SHOW DATA", "SHOW TABLES FROM `cat`.`db`"]


def test_sync_table_list_show_data_success_unchanged():
    class MockClient:
        def execute(self, statement, db=None, return_format="raw"):
            if statement.strip() == "SHOW DATA":
                return ResultSet(
                    success=True,
                    column_names=["Table", "Size", "Replicas"],
                    rows=[["t1", "1 MB", "3"]],
                    execution_time=0.01,
                )
            raise AssertionError("should not fall back")

    mgr = DatabaseSummaryManager(MockClient())
    assert mgr._sync_table_list("mydb", force=True) is True
    info = mgr.table_cache[("mydb", "t1")]
    assert info.size_str == "1 MB"
    assert info.replica_count == 3
