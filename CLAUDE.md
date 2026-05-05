# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

StarRocks Official MCP Server - A bridge between AI assistants and StarRocks databases, built using FastMCP framework. Enables direct SQL execution, database exploration, data visualization, and schema introspection through the Model Context Protocol (MCP).

## Development Commands

**Local Development:**
```bash
# Run the server directly for testing
uv run mcp-server-starrocks

# Run with test mode to verify table overview functionality
uv run mcp-server-starrocks --test

# Run in Streamable HTTP mode (recommended for integration)
export MCP_TRANSPORT_MODE=streamable-http
uv run mcp-server-starrocks
```

**Package Management:**
```bash
# Install dependencies (handled by uv automatically)
uv sync

# Install with test extras (pytest); required to run the test suite
uv sync --extra test

# Run tests (do not use `uv run pytest` alone — pytest is an optional extra)
uv run --extra test python -m pytest tests/ -v

# Integration tests only (real StarRocks: set STARROCKS_URL or host/port; unset STARROCKS_DUMMY_TEST)
# uv run --extra test python -m pytest tests/test_db_client.py -v -m integration
# CI: STARROCKS_INTEGRATION_STRICT=1 fails the job if FE is unreachable instead of skipping
# Read-only FE account: STARROCKS_INTEGRATION_READ_ONLY=1 skips DDL integration tests; or: -m "integration and read_only"

# Build package
uv build
```

## Architecture Overview

### Core Components

- **`src/mcp_server_starrocks/server.py`**: Main server implementation containing all MCP tools, resources, and database connection logic
- **`src/mcp_server_starrocks/__init__.py`**: Entry point that starts the async server

### Connection Architecture

The server supports two connection modes:
- **Standard MySQL Protocol**: Default connection using `mysql.connector` 
- **Arrow Flight SQL**: High-performance connection using ADBC drivers (enabled when `STARROCKS_FE_ARROW_FLIGHT_SQL_PORT` is set)

Connection management uses a global singleton pattern with automatic reconnection handling.

### Tool Categories

1. **Query Execution Tools**:
   - `read_query`: Execute SELECT and other result-returning queries
   - `write_query`: Execute DDL/DML commands
   - `analyze_query`: Query performance analysis via EXPLAIN ANALYZE

2. **Overview Tools with Caching**:
   - `table_overview`: Get table schema, row count, and sample data (cached); supports `catalog.database.table`
   - `db_overview`: Get overview of all tables in a database (uses table cache)
   - `db_summary` / `catalog_summary`: Database- and catalog-level summaries (`catalog_summary` runs `SHOW DATABASES FROM` then reuses `db_summary` logic)
   
3. **Visualization Tool**:
   - `query_and_plotly_chart`: Execute query and generate Plotly charts from results

4. **Catalog-wide summary**:
   - `catalog_summary`: `SHOW DATABASES FROM catalog`, then per-database summaries (same logic as `db_summary`)

### Resource Endpoints

- `starrocks:///databases`: List all databases
- `starrocks:///{db}/tables`: List tables in a database  
- `starrocks:///{db}/{table}/schema`: Get table CREATE statement
- `proc:///{path}`: Access StarRocks internal system information (similar to Linux /proc)

### Caching System

In-memory cache for table overviews using `(catalog, database, table)` cache keys (`catalog` may be empty for single-catalog `database.table` references). Cache includes both successful results and error messages. Controlled by `STARROCKS_OVERVIEW_LIMIT` environment variable (default: 20000 characters).

## Configuration

Environment variables for database connection:
- `STARROCKS_HOST`: Database host (default: localhost)
- `STARROCKS_PORT`: MySQL port (default: 9030)  
- `STARROCKS_USER`: Username (default: root)
- `STARROCKS_PASSWORD`: Password (default: empty)
- `STARROCKS_DB`: Default session namespace for `USE`: `database` or `catalog.database`
- `STARROCKS_CATALOG`: Optional; combined with `STARROCKS_DB` when `STARROCKS_DB` has no dot (same as setting `STARROCKS_DB` to `catalog.database`). When `STARROCKS_URL` is set, applies the same merge if the URL path database has no dot.
- `STARROCKS_MYSQL_AUTH_PLUGIN`: Auth plugin (e.g., mysql_clear_password)
- `STARROCKS_FE_ARROW_FLIGHT_SQL_PORT`: Enables Arrow Flight SQL mode
- `MCP_TRANSPORT_MODE`: Communication mode (stdio/streamable-http/sse)

## Code Patterns

### Error Handling
- Database errors trigger connection reset via `reset_connection()`
- All tools return string error messages rather than raising exceptions
- Cursors are always closed in finally blocks

### Security
- SQL injection prevention through parameterized queries and backtick escaping
- Plotly expressions are validated using AST parsing to prevent code injection
- Limited `eval()` usage with restricted scope for chart generation

### Async Patterns
- Tools are defined as async functions even though database operations are synchronous
- Main server runs in async context using `FastMCP.run_async()`

## Package Structure

This is a simple Python package built with hatchling:
- Single module in `src/mcp_server_starrocks/`
- Entry point defined in pyproject.toml as `mcp-server-starrocks` command
- Dependencies managed through pyproject.toml, no requirements.txt files