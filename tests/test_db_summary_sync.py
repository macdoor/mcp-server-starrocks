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
    # Fallback SHOW TABLES FROM is run with a fully-qualified ref and no session USE
    # (db=None) so a broken `USE cat.db` does not prevent the fallback from running.
    assert calls == [
        ("SHOW DATA", "cat.db"),
        ("SHOW TABLES FROM `cat`.`db`", None),
    ]


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


def test_db_summary_returns_friendly_error_when_input_is_catalog():
    """db_summary(db='catalog_name') should redirect to catalog_summary, not generic error."""

    class MockClient:
        def execute(self, statement, db=None, return_format="raw"):
            s = statement.strip()
            if s == "SHOW DATA":
                return ResultSet(
                    success=False,
                    error_message="Error switching to namespace 'cat': Unknown database 'cat'",
                    execution_time=0.0,
                )
            if s.startswith("SHOW TABLES FROM"):
                return ResultSet(
                    success=False,
                    error_message="Unknown database 'cat'",
                    execution_time=0.0,
                )
            if s.startswith("SHOW DATABASES FROM"):
                assert "`cat`" in s
                return ResultSet(
                    success=True,
                    column_names=["Database"],
                    rows=[["db1"], ["db2"]],
                    execution_time=0.0,
                )
            raise AssertionError(f"unexpected statement: {statement!r}")

    mgr = DatabaseSummaryManager(MockClient())
    out = mgr.get_database_summary("cat")
    assert "appears to be a catalog" in out
    assert "catalog_summary(catalog='cat')" in out
    assert "db_summary(db='cat.<database>')" in out


def test_db_summary_returns_generic_error_when_not_catalog():
    """If the name is neither a database nor a catalog, keep the original error."""

    class MockClient:
        def execute(self, statement, db=None, return_format="raw"):
            return ResultSet(
                success=False,
                error_message="boom",
                execution_time=0.0,
            )

    mgr = DatabaseSummaryManager(MockClient())
    out = mgr.get_database_summary("missing_db")
    assert out == "Error: Failed to sync table information for database 'missing_db'"


def test_external_catalog_skip_show_data_after_first_failure():
    """Once SHOW DATA fails for one DB in a catalog, subsequent DBs in the same catalog skip it."""
    calls: list[tuple[str, str | None]] = []

    class MockClient:
        def execute(self, statement, db=None, return_format="raw"):
            s = statement.strip()
            calls.append((s, db))
            if s == "SHOW DATA":
                return ResultSet(
                    success=False,
                    error_message="SHOW DATA not supported on external catalog",
                    execution_time=0.0,
                )
            if s.startswith("SHOW TABLES FROM"):
                return ResultSet(
                    success=True,
                    column_names=["Tables"],
                    rows=[["t1"]],
                    execution_time=0.0,
                )
            raise AssertionError(f"unexpected statement: {statement!r}")

    mgr = DatabaseSummaryManager(MockClient())

    assert mgr._sync_table_list("cat.db1", force=True) is True
    assert calls == [
        ("SHOW DATA", "cat.db1"),
        ("SHOW TABLES FROM `cat`.`db1`", None),
    ]
    assert "cat" in mgr._catalog_skip_show_data

    calls.clear()
    assert mgr._sync_table_list("cat.db2", force=True) is True
    # Second DB in same catalog skips SHOW DATA entirely.
    assert calls == [("SHOW TABLES FROM `cat`.`db2`", None)]
