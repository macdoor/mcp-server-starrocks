# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import ast
import asyncio
import base64
import csv
import json
import math
import sys
import os
import traceback
import threading
import time
from fastmcp import FastMCP, Context
from fastmcp.utilities.types import Image
from fastmcp.tools.tool import ToolResult
from mcp.types import TextContent, ImageContent
from fastmcp.exceptions import ToolError
from typing import Annotated, Optional
from pydantic import Field
import plotly.express as px
import plotly.graph_objs
from loguru import logger
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware import Middleware

from .mcp_request_responder_patch import apply_mcp_request_responder_duplicate_guard
from .db_client import (
    get_db_client,
    reset_db_connections,
    ResultSet,
    PerfAnalysisInput,
    format_qualified_identifiers,
    format_use_statement,
)
from .db_summary_manager import get_db_summary_manager
from .connection_health_checker import (
    initialize_health_checker,
    start_connection_health_checker,
    stop_connection_health_checker,
    check_connection_health
)

# Configure logging
logger.remove()  # Remove default handler
logger.add(sys.stderr, level=os.getenv("LOG_LEVEL", "INFO"), 
          format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}")

apply_mcp_request_responder_duplicate_guard()

mcp = FastMCP('mcp-server-starrocks')

# a hint for soft limit, not enforced
overview_length_limit = int(os.getenv('STARROCKS_OVERVIEW_LIMIT', str(20000)))
# Global cache for table overviews: {(catalog, database, table): overview_string}; catalog may be ""
global_table_overview_cache = {}

# Directory where read_query writes result files when output_file is a relative path.
DEFAULT_OUTPUT_DIR = os.path.expanduser("~/.mcp-server-starrocks/output")
output_dir = os.path.expanduser(os.getenv('STARROCKS_MCP_OUTPUT_DIR', DEFAULT_OUTPUT_DIR))

# Transport mode, captured at server startup so tools can warn when files
# are written on a remote server filesystem rather than the user's machine.
_transport_mode = os.getenv('MCP_TRANSPORT_MODE', 'stdio')

_EXT_TO_FORMAT = {'.csv': 'csv', '.tsv': 'tsv', '.json': 'json',
                  '.jsonl': 'jsonl', '.ndjson': 'jsonl'}
_SUPPORTED_OUTPUT_FORMATS = ('csv', 'tsv', 'json', 'jsonl')


def _resolve_output_path(output_file: str) -> str:
    """Resolve output_file to an absolute path.

    Absolute paths (after ~ expansion) are used as-is. Relative paths are
    resolved against output_dir. The parent directory is created if missing.
    """
    path = os.path.expanduser(output_file)
    if not os.path.isabs(path):
        path = os.path.join(output_dir, path)
    path = os.path.abspath(path)
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    return path


def _infer_output_format(path: str, fmt: str | None) -> str:
    if fmt:
        f = fmt.lower()
        if f not in _SUPPORTED_OUTPUT_FORMATS:
            raise ValueError(
                f"Unsupported output_format '{fmt}'. Use one of: {', '.join(_SUPPORTED_OUTPUT_FORMATS)}.")
        return f
    ext = os.path.splitext(path)[1].lower()
    return _EXT_TO_FORMAT.get(ext, 'csv')


def _write_result_to_file(result: ResultSet, path: str, fmt: str) -> None:
    """Write a successful ResultSet to disk in the given format."""
    if not result.success or result.column_names is None or result.rows is None:
        raise ValueError("Cannot write empty or failed result to file.")
    if fmt in ('csv', 'tsv'):
        delim = '\t' if fmt == 'tsv' else ','
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=delim)
            writer.writerow(result.column_names)
            for row in result.rows:
                writer.writerow(row)
    elif fmt == 'json':
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(
                [dict(zip(result.column_names, row)) for row in result.rows],
                f, default=str)
    elif fmt == 'jsonl':
        with open(path, 'w', encoding='utf-8') as f:
            for row in result.rows:
                f.write(json.dumps(dict(zip(result.column_names, row)), default=str))
                f.write('\n')
    else:
        raise ValueError(
            f"Unsupported format '{fmt}'. Use one of: {', '.join(_SUPPORTED_OUTPUT_FORMATS)}.")

# Get database client instance
db_client = get_db_client()
# Get database summary manager instance
db_summary_manager = get_db_summary_manager(db_client)
# Description suffix for tools: global default namespace + per-session override
_default_sess = db_client.default_database
description_suffix = (
    (
        f". Global default namespace is `{_default_sess}` (StarRocks USE: database or catalog.database); "
        f"use set_session_db to override per session. Prefer fully qualified `catalog.database.table` in multi-catalog clusters."
    )
    if _default_sess
    else ". Use set_session_db to set a per-session default database or catalog.database."
)


def _safe_session_id(ctx: Optional[Context]) -> Optional[str]:
    """Return ctx.session_id, or None when unavailable (e.g. no active request context)."""
    if ctx is None:
        return None
    try:
        return ctx.session_id
    except (RuntimeError, AttributeError):
        return None

# Initialize connection health checker
_health_checker = initialize_health_checker(db_client)


SR_PROC_DESC = '''
Internal information exposed by StarRocks similar to linux /proc, following are some common paths:

'/frontends'	Shows the information of FE nodes.
'/backends'	Shows the information of BE nodes if this SR is non cloud native deployment.
'/compute_nodes'	Shows the information of CN nodes if this SR is cloud native deployment.
'/dbs'	Shows the information of databases.
'/dbs/<DB_ID>'	Shows the information of a database by database ID.
'/dbs/<DB_ID>/<TABLE_ID>'	Shows the information of tables by database ID.
'/dbs/<DB_ID>/<TABLE_ID>/partitions'	Shows the information of partitions by database ID and table ID.
'/transactions'	Shows the information of transactions by database.
'/transactions/<DB_ID>' Show the information of transactions by database ID.
'/transactions/<DB_ID>/running' Show the information of running transactions by database ID.
'/transactions/<DB_ID>/finished' Show the information of finished transactions by database ID.
'/jobs'	Shows the information of jobs.
'/statistic'	Shows the statistics of each database.
'/tasks'	Shows the total number of all generic tasks and the failed tasks.
'/cluster_balance'	Shows the load balance information.
'/routine_loads'	Shows the information of Routine Load.
'/colocation_group'	Shows the information of Colocate Join groups.
'/catalog'	Shows the information of catalogs.
'''


@mcp.resource(uri="starrocks:///databases", name="All Databases", description="List all databases in StarRocks",
              mime_type="text/plain")
def get_all_databases() -> str:
    logger.debug("Fetching all databases")
    result = db_client.execute("SHOW DATABASES")
    logger.debug(f"Found {len(result.rows) if result.success and result.rows else 0} databases")
    return result.to_string()


@mcp.resource(uri="starrocks:///{db}/{table}/schema", name="Table Schema",
              description="Get the schema of a table using SHOW CREATE TABLE", mime_type="text/plain")
def get_table_schema(db: str, table: str) -> str:
    logger.debug(f"Fetching schema for table {db}.{table}")
    return db_client.execute(f"SHOW CREATE TABLE {db}.{table}").to_string()


@mcp.resource(uri="starrocks:///{db}/tables", name="Database Tables",
              description="List all tables in a specific database", mime_type="text/plain")
def get_database_tables(db: str) -> str:
    logger.debug(f"Fetching tables from database {db}")
    result = db_client.execute(f"SHOW TABLES FROM {db}")
    logger.debug(f"Found {len(result.rows) if result.success and result.rows else 0} tables in {db}")
    return result.to_string()


@mcp.resource(uri="proc:///{path*}", name="System internal information", description=SR_PROC_DESC,
              mime_type="text/plain")
def get_system_internal_information(path: str) -> str:
    logger.debug(f"Fetching system information for proc path: {path}")
    return db_client.execute(f"show proc '{path}'").to_string(limit=overview_length_limit)


def _parse_table_overview_arg(table_arg: str, default_database: str | None):
    """
    Returns (catalog, database, table_name, session_namespace) or (None, None, None, error_message).
    catalog is None when only database.table (internal/default_catalog) semantics.
    """
    parts = [p for p in table_arg.strip().split(".") if p]
    if len(parts) >= 4:
        return None, None, None, (
            "Error: Too many dot-separated segments. Use `catalog.database.table` (three) or "
            "`database.table` (two), or a single table name with a session default of `catalog.database`."
        )
    if len(parts) == 3:
        c, d, t = parts[0], parts[1], parts[2]
        return c, d, t, f"{c}.{d}"
    if len(parts) == 2:
        d, t = parts[0], parts[1]
        return None, d, t, d
    if len(parts) == 1:
        t = parts[0]
        if not default_database:
            return None, None, None, (
                f"Error: Table name '{t}' only — set session default (e.g. STARROCKS_DB=catalog.database) "
                "or use database.table / catalog.database.table."
            )
        gd = [p for p in default_database.strip().split(".") if p]
        if len(gd) == 2:
            c, d = gd[0], gd[1]
            return c, d, t, f"{c}.{d}"
        if len(gd) == 1:
            d = gd[0]
            return None, d, t, d
        return None, None, None, "Error: STARROCKS_DB / default namespace must be `database` or `catalog.database`."
    return None, None, None, "Error: Missing table name."


def _get_table_details(catalog: str | None, database: str, table_name: str, limit=None):
    """
    Helper function to get description, sample rows, and count for a table.
    catalog+database+table for three-part names; catalog None means database.table only.
    Returns a formatted string. Handles DB errors internally and returns error messages.
    """
    global global_table_overview_cache
    session_ns = f"{catalog}.{database}" if catalog else database
    segs = [catalog, database, table_name] if catalog else [database, table_name]
    full_table_name = ".".join(f"`{s}`" for s in segs)
    logger.debug(f"Fetching table details for {full_table_name} (session={session_ns})")
    output_lines = []

    if not database or not table_name:
        output_lines.append(
            f"Warning: Incomplete table reference for '{table_name}'. Using potentially incorrect context.")
        logger.warning(f"Incomplete table reference catalog={catalog} database={database} table={table_name}")

    count = 0
    output_lines.append(f"--- Overview for {full_table_name} ---")

    # 1. Get Row Count
    query = f"SELECT COUNT(*) FROM {full_table_name}"
    count_result = db_client.execute(query, db=session_ns)
    if count_result.success and count_result.rows:
        count = count_result.rows[0][0]
        output_lines.append(f"\nTotal rows: {count}")
        logger.debug(f"Table {full_table_name} has {count} rows")
    else:
        output_lines.append(f"\nCould not determine total row count.")
        if not count_result.success:
            output_lines.append(f"Error: {count_result.error_message}")
            logger.error(f"Failed to get row count for {full_table_name}: {count_result.error_message}")

    # 2. Get Columns (DESCRIBE)
    if count > 0:
        query = f"DESCRIBE {full_table_name}"
        desc_result = db_client.execute(query, db=session_ns)
        if desc_result.success and desc_result.column_names and desc_result.rows:
            output_lines.append(f"\nColumns:")
            output_lines.append(desc_result.to_string(limit=limit))
        else:
            output_lines.append("(Could not retrieve column information or table has no columns).")
            if not desc_result.success:
                output_lines.append(f"Error getting columns for {full_table_name}: {desc_result.error_message}")
                return "\n".join(output_lines)

        # 3. Get Sample Rows (LIMIT 3)
        query = f"SELECT * FROM {full_table_name} LIMIT 3"
        sample_result = db_client.execute(query, db=session_ns)
        if sample_result.success and sample_result.column_names and sample_result.rows:
            output_lines.append(f"\nSample rows (limit 3):")
            output_lines.append(sample_result.to_string(limit=limit))
        else:
            output_lines.append(f"(No rows found in {full_table_name}).")
            if not sample_result.success:
                output_lines.append(f"Error getting sample rows for {full_table_name}: {sample_result.error_message}")

    overview_string = "\n".join(output_lines)
    # Update cache even if there were partial errors, so we cache the error message too
    cache_key = (catalog or "", database, table_name)
    global_table_overview_cache[cache_key] = overview_string
    return overview_string


# tools

@mcp.tool(description="Execute a SELECT query or commands that return a ResultSet. Set output_file to write the full result to disk instead of returning it inline (useful for large results)." + description_suffix)
def read_query(query: Annotated[str, Field(description="SQL query to execute")],
               db: Annotated[str|None, Field(
                   description="Optional session namespace for StarRocks USE: `database` or `catalog.database` (overrides per-session and global default).")] = None,
               output_file: Annotated[str|None, Field(
                   description="If set, write the full result to this file and return only a summary + small preview inline. Relative paths resolve against STARROCKS_MCP_OUTPUT_DIR (default: ~/.mcp-server-starrocks/output/). Absolute paths (and ~) are used as-is. Format is inferred from the file extension (.csv, .tsv, .json, .jsonl, .ndjson) unless output_format is given. NOTE: the file is written on the server's filesystem, which may not be the client machine in remote/http deployments."
               )] = None,
               output_format: Annotated[str|None, Field(
                   description="Override file format: csv|tsv|json|jsonl. If omitted, inferred from output_file extension; defaults to csv."
               )] = None,
               ctx: Context = None) -> ToolResult:
    logger.info(f"Executing read query: {query[:100]}{'...' if len(query) > 100 else ''}")
    result = db_client.execute(query, db=db, session_id=_safe_session_id(ctx))
    if not result.success:
        logger.error(f"Query failed: {result.error_message}")
        return ToolResult(content=[TextContent(type='text', text=result.to_string(limit=10000))],
                          structured_content=result.to_dict())

    rows_n = len(result.rows) if result.rows else 0
    logger.info(f"Query executed successfully, returned {rows_n} rows")

    if output_file:
        try:
            resolved = _resolve_output_path(output_file)
            fmt = _infer_output_format(resolved, output_format)
            _write_result_to_file(result, resolved, fmt)
        except Exception as e:
            logger.exception(f"Failed to write result to {output_file}")
            return ToolResult(
                content=[TextContent(type='text',
                                     text=f"Error writing result to file: {e}\n\nResult preview:\n{result.to_string(limit=2000)}")],
                structured_content=result.to_dict(),
            )

        size = os.path.getsize(resolved)
        warn = ""
        if _transport_mode != 'stdio':
            warn = (f"\nNOTE: MCP server transport is '{_transport_mode}'. "
                    f"This file is written on the server's filesystem. "
                    f"If the server is running on a remote machine, retrieve it from there.")
        preview = result.to_string(limit=2000)
        text = (f"Wrote {rows_n} rows to {resolved} ({size} bytes, format={fmt}).{warn}\n"
                f"Preview:\n{preview}")
        structured = result.to_dict()
        # full data is on disk now; drop bulky rows from the structured payload
        structured.pop('rows', None)
        structured['output_file'] = resolved
        structured['output_format'] = fmt
        structured['output_bytes'] = size
        structured['rows_written'] = rows_n
        return ToolResult(content=[TextContent(type='text', text=text)],
                          structured_content=structured)

    return ToolResult(content=[TextContent(type='text', text=result.to_string(limit=10000))],
                      structured_content=result.to_dict())


@mcp.tool(description="Execute a DDL/DML or other StarRocks command that do not have a ResultSet" + description_suffix)
def write_query(query: Annotated[str, Field(description="SQL to execute")],
                db: Annotated[str|None, Field(
                    description="Optional session namespace: `database` or `catalog.database` (StarRocks USE).")] = None,
                ctx: Context = None) -> ToolResult:
    logger.info(f"Executing write query: {query[:100]}{'...' if len(query) > 100 else ''}")
    result = db_client.execute(query, db=db, session_id=_safe_session_id(ctx))
    if not result.success:
        logger.error(f"Write query failed: {result.error_message}")
    elif result.rows_affected is not None and result.rows_affected >= 0:
        logger.info(f"Write query executed successfully, {result.rows_affected} rows affected in {result.execution_time:.2f}s")
    else:
        logger.info(f"Write query executed successfully in {result.execution_time:.2f}s")
    return ToolResult(content=[TextContent(type='text', text=result.to_string(limit=2000))],
                      structured_content=result.to_dict())

@mcp.tool(description="Analyze a query and get analyze result using query profile" + description_suffix)
def analyze_query(
        uuid: Annotated[
            str|None, Field(description="Query ID, a string composed of 32 hexadecimal digits formatted as 8-4-4-4-12")]=None,
        sql: Annotated[str|None, Field(description="Query SQL")]=None,
        db: Annotated[str|None, Field(
            description="Optional session namespace: `database` or `catalog.database`.")] = None,
        ctx: Context = None,
) -> str:
    sid = _safe_session_id(ctx)
    if uuid:
        logger.info(f"Analyzing query profile for UUID: {uuid}")
        return db_client.execute(f"ANALYZE PROFILE FROM '{uuid}'", db=db, session_id=sid).to_string()
    elif sql:
        logger.info(f"Analyzing query: {sql[:100]}{'...' if len(sql) > 100 else ''}")
        return db_client.execute(f"EXPLAIN ANALYZE {sql}", db=db, session_id=sid).to_string()
    else:
        logger.warning("Analyze query called without valid UUID or SQL")
        return f"Failed to analyze query, the reasons maybe: 1.query id is not standard uuid format; 2.the SQL statement have spelling error."


@mcp.tool(description="Run a query to get it's query dump and profile, output very large, need special tools to do further processing")
def collect_query_dump_and_profile(
        query: Annotated[str, Field(description="query to execute")],
        db: Annotated[str|None, Field(
            description="Optional session namespace: `database` or `catalog.database`.")] = None,
        ctx: Context = None,
) -> ToolResult:
    logger.info(f"Collecting query dump and profile for query: {query[:100]}{'...' if len(query) > 100 else ''}")
    result : PerfAnalysisInput = db_client.collect_perf_analysis_input(query, db=db, session_id=_safe_session_id(ctx))
    if result.get('error_message'):
        status = f"collecting query dump and profile failed, query_id={result.get('query_id')} error_message={result.get('error_message')}"
        logger.warning(status)
    else:
        status = f"collecting query dump and profile succeeded, but it's only for user/tool, not for AI, query_id={result.get('query_id')}"
        logger.info(status)
    return ToolResult(
        content=[TextContent(type='text', text=status)],
        structured_content=result,
    )


def validate_plotly_expr(expr: str):
    """
    Validates a string to ensure it represents a single call to a method
    of the 'px' object, without containing other statements or imports,
    and ensures its arguments do not contain nested function calls.

    Args:
        expr: The string expression to validate.

    Raises:
        ValueError: If the expression does not meet the security criteria.
        SyntaxError: If the expression is not valid Python syntax.
    """
    # 1. Check for valid Python syntax
    try:
        tree = ast.parse(expr)
    except SyntaxError as e:
        raise SyntaxError(f"Invalid Python syntax in expression: {e}") from e

    # 2. Check that the tree contains exactly one top-level node (statement/expression)
    if len(tree.body) != 1:
        raise ValueError("Expression must be a single statement or expression.")

    node = tree.body[0]

    # 3. Check that the single node is an expression
    if not isinstance(node, ast.Expr):
        raise ValueError(
            "Expression must be a single expression, not a statement (like assignment, function definition, import, etc.).")

    # 4. Get the actual value of the expression and check it's a function call
    expr_value = node.value
    if not isinstance(expr_value, ast.Call):
        raise ValueError("Expression must be a function call.")

    # 5. Check that the function being called is an attribute lookup (like px.scatter)
    if not isinstance(expr_value.func, ast.Attribute):
        raise ValueError("Function call must be on an object attribute (e.g., px.scatter).")

    # 6. Check that the attribute is being accessed on a simple variable name
    if not isinstance(expr_value.func.value, ast.Name):
        raise ValueError("Function call must be on a simple variable name (e.g., px.scatter, not obj.px.scatter).")

    # 7. Check that the simple variable name is 'px'
    if expr_value.func.value.id != 'px':
        raise ValueError("Function call must be on the 'px' object.")

    # Check positional arguments
    for i, arg_node in enumerate(expr_value.args):
        for sub_node in ast.walk(arg_node):
            if isinstance(sub_node, ast.Call):
                raise ValueError(f"Positional argument at index {i} contains a disallowed nested function call.")
    # Check keyword arguments
    for kw in expr_value.keywords:
        for sub_node in ast.walk(kw.value):
            if isinstance(sub_node, ast.Call):
                keyword_name = kw.arg if kw.arg else '<unknown>'
                raise ValueError(f"Keyword argument '{keyword_name}' contains a disallowed nested function call.")


def one_line_summary(text: str, limit:int=100) -> str:
    """Generate a one-line summary of the given text, truncated to the specified limit."""
    single_line = ' '.join(text.split())
    if len(single_line) > limit:
        return single_line[:limit-3] + '...'
    return single_line


@mcp.tool(description="using sql `query` to extract data from database, then using python `plotly_expr` to generate a chart for UI to display" + description_suffix)
def query_and_plotly_chart(
        query: Annotated[str, Field(description="SQL query to execute")],
        plotly_expr: Annotated[
            str, Field(description="a one function call expression, with 2 vars binded: `px` as `import plotly.express as px`, and `df` as dataframe generated by query `plotly_expr` example: `px.scatter(df, x=\"sepal_width\", y=\"sepal_length\", color=\"species\", marginal_y=\"violin\", marginal_x=\"box\", trendline=\"ols\", template=\"simple_white\")`")],
        format: Annotated[str, Field(description="chart output format, json|png|jpeg")] = "jpeg",
        db: Annotated[str|None, Field(
            description="Optional session namespace: `database` or `catalog.database`.")] = None,
        ctx: Context = None,
) -> ToolResult:
    """
    Executes an SQL query, creates a Pandas DataFrame, generates a Plotly chart
    using the provided expression, encodes the chart as a base64 PNG image,
    and returns it along with optional text.

    Args:
        query: The SQL query string to execute.
        plotly_expr: A Python string expression using 'px' (plotly.express)
                     and 'df' (the DataFrame from the query) to generate a figure.
                     Example: "px.scatter(df, x='col1', y='col2')"
        format: chat output format, json|png|jpeg, default is jpeg
        db: Optional session namespace (`database` or `catalog.database`) for StarRocks USE before the query.

    Returns:
        A list containing types.TextContent and types.ImageContent,
        or just types.TextContent in case of an error or no data.
    """
    try:
        logger.info(f'query_and_plotly_chart query:{one_line_summary(query)}, plotly:{one_line_summary(plotly_expr)} format:{format}, db:{db}')
        result = db_client.execute(query, db=db, return_format="pandas", session_id=_safe_session_id(ctx))
        errmsg = None
        if not result.success:
            errmsg = result.error_message
        elif result.pandas is None:
            errmsg = 'Query did not return data suitable for plotting.'
        else:
            df = result.pandas
            if df.empty:
                errmsg = 'Query returned no data to plot.'
        if errmsg:
            logger.warning(f"Query or data issue: {errmsg}")
            return ToolResult(
                content=[TextContent(type='text', text=f'Error: {errmsg}')],
                structured_content={'success': False, 'error_message': errmsg},
            )
        # Validate and evaluate the plotly expression using px and df
        local_vars = {'df': df}
        validate_plotly_expr(plotly_expr)
        fig : plotly.graph_objs.Figure = eval(plotly_expr, {"px": px}, local_vars)
        if format == 'json':
            # return json representation of the figure for front-end rendering
            plot_json = json.loads(fig.to_json())
            structured_content = result.to_dict()
            structured_content['data'] = plot_json['data']
            structured_content['layout'] = plot_json['layout']
            summary = result.to_string()
            return ToolResult(
                content=[
                    TextContent(type='text', text=f'{summary}\nChart Generated for UI rendering'),
                ],
                structured_content=structured_content,
            )
        else:
            if not hasattr(fig, 'to_image'):
                raise ToolError(f"The evaluated expression did not return a Plotly figure object. Result type: {type(fig)}")
            if format == 'jpg':
                format = 'jpeg'
            img_bytes = fig.to_image(format=format, width=960, height=720)
            structured_content = result.to_dict()
            structured_content['img_bytes_base64'] = base64.b64encode(img_bytes)
            return ToolResult(
                content=[
                   TextContent(type='text', text=f'dataframe data:\n{df}\nChart generated but for UI only'),
                   Image(data=img_bytes, format="jpeg").to_image_content()
                ],
                structured_content=structured_content
            )
    except Exception as err:
        return ToolResult(
            content=[TextContent(type='text', text=f'Error: {err}')],
            structured_content={'success': False, 'error_message': str(err)},
        )


@mcp.tool(description="Get an overview of a specific table: columns, sample rows (up to 3), and total row count. Uses cache unless refresh=true" + description_suffix)
def table_overview(
        table: Annotated[str, Field(
            description="Table: `table`, `database.table`, or `catalog.database.table`. Single name requires session default (via `db` arg, `set_session_db`, or STARROCKS_DB=`catalog.database`/`database`). Prefer fully qualified three-part names in multi-catalog clusters.")],
        db: Annotated[str | None, Field(
            description="Optional session namespace for a single-segment `table`: `database` or `catalog.database`. Ignored when `table` already contains dots. Overrides per-session default and STARROCKS_DB.")] = None,
        refresh: Annotated[
            bool, Field(description="Set to true to force refresh, ignoring cache. Defaults to false.")] = False,
        ctx: Context = None,
) -> str:
    try:
        logger.info(f"Getting table overview for: {table}, db={db}, refresh={refresh}")
        if not table:
            logger.error("Table overview called without table name")
            return "Error: Missing 'table' argument."

        session_default = db if db else db_client.get_session_default_db(_safe_session_id(ctx))
        catalog, database, table_name, last = _parse_table_overview_arg(table, session_default)
        if database is None or table_name is None:
            return last

        cache_key = (catalog or "", database, table_name)

        # Check cache
        if not refresh and cache_key in global_table_overview_cache:
            logger.debug(f"Using cached overview for {cache_key}")
            return global_table_overview_cache[cache_key]

        logger.debug(f"Fetching fresh overview for {cache_key}")
        # Fetch details (will also update cache)
        overview_text = _get_table_details(catalog, database, table_name, limit=overview_length_limit)
        return overview_text
    except Exception as e:
        # Reset connections on unexpected errors
        logger.exception(f"Unexpected error in table_overview for {table}")
        reset_db_connections()
        stack_trace = traceback.format_exc()
        return f"Unexpected Error executing tool 'table_overview': {type(e).__name__}: {e}\nStack Trace:\n{stack_trace}"

# comment out to prefer db_summary tool
#@mcp.tool(description="Get an overview (columns, sample rows, row count) for ALL tables in a database. Uses cache unless refresh=True" + description_suffix)
def db_overview(
        db: Annotated[str, Field(
            description="Database name. Optional: uses the default database if not provided.")] = None,
        refresh: Annotated[
            bool, Field(description="Set to true to force refresh, ignoring cache. Defaults to false.")] = False
) -> str:
    try:
        db_name = db if db else db_client.default_database
        logger.info(f"Getting database overview for: {db_name}, refresh={refresh}")
        if not db_name:
            logger.error("Database overview called without database name")
            return "Error: Database name not provided and no default database is set."

        # List tables in the database (db_name may be catalog.database)
        try:
            from_part = format_qualified_identifiers(db_name)
        except ValueError as ve:
            return f"Error: invalid database / namespace '{db_name}': {ve}"
        query = f"SHOW TABLES FROM {from_part}"
        result = db_client.execute(query, db=db_name)

        if not result.success:
            logger.error(f"Failed to list tables in database {db_name}: {result.error_message}")
            return f"Database Error listing tables in '{db_name}': {result.error_message}"

        if not result.rows:
            logger.info(f"No tables found in database {db_name}")
            return f"No tables found in database '{db_name}'."

        tables = [row[0] for row in result.rows]
        logger.info(f"Found {len(tables)} tables in database {db_name}")
        all_overviews = [f"--- Overview for Database: `{db_name}` ({len(tables)} tables) ---"]

        total_length = 0
        limit_per_table = overview_length_limit * (math.log10(len(tables)) + 1) // len(tables)  # Limit per table
        if "." in db_name:
            cat_ns, db_only = db_name.split(".", 1)
        else:
            cat_ns, db_only = None, db_name

        for table_name in tables:
            cache_key = (cat_ns or "", db_only, table_name)
            overview_text = None

            # Check cache first
            if not refresh and cache_key in global_table_overview_cache:
                logger.debug(f"Using cached overview for {cache_key}")
                overview_text = global_table_overview_cache[cache_key]
            else:
                logger.debug(f"Fetching fresh overview for {cache_key}")
                # Fetch details for this table (will update cache via _get_table_details)
                overview_text = _get_table_details(cat_ns, db_only, table_name, limit=limit_per_table)

            all_overviews.append(overview_text)
            all_overviews.append("\n")  # Add separator
            total_length += len(overview_text) + 1

        logger.info(f"Database overview completed for {db_name}, total length: {total_length}")
        return "\n".join(all_overviews)

    except Exception as e:
        # Catch any other unexpected errors during tool execution
        logger.exception(f"Unexpected error in db_overview for database {db}")
        reset_db_connections()
        stack_trace = traceback.format_exc()
        return f"Unexpected Error executing tool 'db_overview': {type(e).__name__}: {e}\nStack Trace:\n{stack_trace}"


@mcp.tool(description="Quickly get summary of a database with tables' schema and size information" + description_suffix)
def db_summary(
        db: Annotated[str|None, Field(
            description="Session namespace: `database` or `catalog.database` (same as StarRocks USE). Optional: uses STARROCKS_DB default when set.")] = None,
        limit: Annotated[int, Field(
            description="Output length limit in characters. Defaults to 10000. Higher values show more tables and details.")] = 10000,
        refresh: Annotated[bool, Field(
            description="Set to true to force refresh, ignoring cache. Defaults to false.")] = False,
        ctx: Context = None,
) -> str:
    try:
        db_name = db if db else db_client.get_session_default_db(_safe_session_id(ctx))
        logger.info(f"Getting database summary for: {db_name}, limit={limit}, refresh={refresh}")
        
        if not db_name:
            logger.error("Database summary called without database name")
            return "Error: Database name not provided and no default database is set."
        
        # Use the database summary manager
        summary = db_summary_manager.get_database_summary(db_name, limit=limit, refresh=refresh)
        logger.info(f"Database summary completed for {db_name}")
        return summary
        
    except Exception as e:
        # Reset connections on unexpected errors
        logger.exception(f"Unexpected error in db_summary for database {db}")
        reset_db_connections()
        stack_trace = traceback.format_exc()
        return f"Unexpected Error executing tool 'db_summary': {type(e).__name__}: {e}\nStack Trace:\n{stack_trace}"


@mcp.tool(description="Set or clear the default database for THIS MCP session. Subsequent tool calls without an explicit `db` argument will use this database. Pass an empty string or null to clear and fall back to the server's global default. Returns the new effective default for this session.")
def set_session_db(
        db: Annotated[str | None, Field(
            description="Session namespace to set as the per-session default: `database` or `catalog.database`. Empty/null clears the override.")] = None,
        ctx: Context = None,
) -> str:
    session_id = _safe_session_id(ctx)
    if not session_id:
        return "Error: no MCP session id available; cannot set a per-session default database."
    if not db:
        db_client.set_session_default_db(session_id, None)
        effective = db_client.get_session_default_db(session_id)
        if effective:
            return f"Cleared per-session default. Falling back to global default `{effective}`."
        return "Cleared per-session default. No global default is set."
    # Validate by issuing USE on a real connection (supports catalog.database).
    try:
        use_sql = format_use_statement(db)
    except ValueError as ve:
        return f"Invalid session namespace `{db}`: {ve}"
    probe = db_client.execute(use_sql, db=None)
    if not probe.success:
        return f"Failed to switch session to `{db}`: {probe.error_message}"
    db_client.set_session_default_db(session_id, db)
    return f"Per-session default namespace set to `{db}`."


@mcp.tool(
    description="Summarize all databases in one catalog (SHOW DATABASES FROM, then db_summary-style detail per database). "
    "Use after read_query('SHOW CATALOGS'). For a single database only, prefer db_summary(db='catalog.database')."
    + description_suffix
)
def catalog_summary(
        catalog: Annotated[str | None, Field(
            description="Catalog name (e.g. default_catalog, JDBC catalog). If omitted, uses STARROCKS_CATALOG or the catalog part of STARROCKS_DB=catalog.database when set.")] = None,
        limit_per_database: Annotated[int, Field(
            description="Character limit passed to each db_summary (default 10000).")] = 10000,
        max_databases: Annotated[int, Field(
            description="Cap how many databases to process (default 40) to avoid huge responses.")] = 40,
        refresh: Annotated[bool, Field(
            description="Set true to bypass caches when summarizing each database.")] = False,
) -> str:
    try:
        cat = (catalog or "").strip()
        if not cat:
            cat = os.getenv("STARROCKS_CATALOG", "").strip()
        if not cat and db_client.default_database and "." in db_client.default_database:
            cat = db_client.default_database.split(".", 1)[0]
        if not cat:
            return (
                "Error: catalog not specified. Pass catalog=..., or set STARROCKS_CATALOG, "
                "or STARROCKS_DB=catalog.database for inference."
            )
        logger.info(f"Getting catalog summary for: {cat}, limit_per_database={limit_per_database}, max_databases={max_databases}")
        return db_summary_manager.get_catalog_summary(
            cat,
            limit_per_database=limit_per_database,
            max_databases=max_databases,
            refresh=refresh,
        )
    except Exception as e:
        logger.exception(f"Unexpected error in catalog_summary for catalog={catalog!r}")
        reset_db_connections()
        stack_trace = traceback.format_exc()
        return f"Unexpected Error executing tool 'catalog_summary': {type(e).__name__}: {e}\nStack Trace:\n{stack_trace}"


async def main():
    parser = argparse.ArgumentParser(description='StarRocks MCP Server')
    parser.add_argument('--mode', choices=['stdio', 'sse', 'http', 'streamable-http'], 
                        default=os.getenv('MCP_TRANSPORT_MODE', 'stdio'),
                        help='Transport mode (default: stdio)')
    parser.add_argument('--host', default='localhost',
                        help='Server host (default: localhost)')
    parser.add_argument('--port', type=int, default=3000,
                        help='Server port (default: 3000)')
    parser.add_argument('--test', action='store_true',
                        help='Run in test mode')
    
    args = parser.parse_args()

    global _transport_mode
    _transport_mode = args.mode

    logger.info(f"Starting StarRocks MCP Server with mode={args.mode}, host={args.host}, port={args.port} default_db={db_client.default_database or 'None'}")
    
    if args.test:
        try:
            logger.info("Starting tool test")
            # Use the test version without tool wrapper
            result = db_client.execute("show databases").to_string()
            logger.info("Result:")
            logger.info(result)
            logger.info("Tool test completed")
        finally:
            stop_connection_health_checker()
            reset_db_connections()
        return

    # Start connection health checker
    start_connection_health_checker()
    try:
        # Add CORS middleware for HTTP transports to allow web frontend access
        if args.mode in ['http', 'streamable-http', 'sse']:
            cors_middleware = [
                Middleware(
                    CORSMiddleware,
                    allow_origins=["*"],  # Allow all origins for development. In production, specify exact origins
                    allow_credentials=True,
                    allow_methods=["*"],  # Allow all HTTP methods
                    allow_headers=["*"],  # Allow all headers
                )
            ]
            logger.info(f"CORS enabled for {args.mode} transport - allowing all origins")
            await mcp.run_async(
                transport=args.mode, 
                host=args.host, 
                port=args.port,
                middleware=cors_middleware
            )
        else:
            await mcp.run_async(transport=args.mode)
    except Exception as e:
        logger.exception("Failed to start MCP server")
        raise
    finally:
        # Stop connection health checker when server shuts down
        stop_connection_health_checker()


if __name__ == "__main__":
    asyncio.run(main())
