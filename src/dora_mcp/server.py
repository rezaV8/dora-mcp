"""MCP server exposing DORA metrics over the DuckDB star schema.

Each tool opens a short-lived read-only connection, so the server never
holds a write lock and `dora-mcp build` can refresh the database while the
server is running.
"""

from collections.abc import Callable
from contextlib import contextmanager
from typing import Annotated, Any

import duckdb
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp_types import ToolAnnotations
from pydantic import Field

from . import config, metrics

INSTRUCTIONS = """\
Answers engineering-delivery questions using the four DORA metrics, computed
from GitHub PRs, releases, and Actions runs for a fixed set of public repos.

- Call data_coverage first if you need to know which repos exist, the date
  range covered, or today's date (to turn "this month" into start/end).
- Dates are ISO YYYY-MM-DD, inclusive, in UTC. Leaving them out means the last 30 days.
- "PR lead time" usually means pr_cycle_time (opened -> merged). DORA's
  "lead time for changes" is first commit -> released (lead_time_for_changes).
- A deployment is a published GitHub release. Change failure rate is
  heuristic, based on revert, hotfix, and regression PRs. Mention this when you report it.
- For anything the tools don't cover, read describe_schema and use run_sql.
"""

mcp = MCPServer("dora-metrics", instructions=INSTRUCTIONS)
READ_ONLY = ToolAnnotations(read_only_hint=True, open_world_hint=False)

Repo = Annotated[str | None, Field(description="'owner/name' or just 'name'. Omit for all repos.")]
Start = Annotated[str | None, Field(description="Window start, YYYY-MM-DD inclusive. Default: 30 days before end.")]
End = Annotated[str | None, Field(description="Window end, YYYY-MM-DD inclusive. Default: today (UTC).")]


@contextmanager
def _connect():
    if not config.DB_PATH.exists():
        raise metrics.MetricsError(
            f"No database at {config.DB_PATH}. Run `dora-mcp ingest` and `dora-mcp build` first."
        )
    # read_only blocks writes. Disabling external access stops run_sql from
    # reading local files (read_csv('/etc/...')) or URLs.
    con = duckdb.connect(str(config.DB_PATH), read_only=True, config={"enable_external_access": False})
    try:
        yield con
    finally:
        con.close()


def _call(fn: Callable[..., dict], *args: Any, **kwargs: Any) -> dict:
    # Expected failures (bad repo name, bad dates, invalid SQL) become
    # ToolErrors, so the model sees the message and can correct itself.
    # Anything else stays opaque.
    try:
        with _connect() as con:
            return fn(con, *args, **kwargs)
    except (metrics.MetricsError, duckdb.Error) as exc:
        raise ToolError(str(exc)) from exc


@mcp.tool(annotations=READ_ONLY)
def data_coverage() -> dict:
    """List the repos in the dataset, their date coverage and row counts, and today's date."""
    return _call(metrics.data_coverage)


@mcp.tool(annotations=READ_ONLY)
def dora_summary(repo: Repo = None, start: Start = None, end: End = None) -> dict:
    """All four DORA metrics for a window, each with an approximate elite/high/medium/low band."""
    return _call(metrics.dora_summary, repo, start, end)


@mcp.tool(annotations=READ_ONLY)
def lead_time_for_changes(
    repo: Repo = None, start: Start = None, end: End = None,
    include_bots: Annotated[bool, Field(description="Include Dependabot/Renovate-style bot PRs.")] = False,
) -> dict:
    """DORA lead time for changes: hours from first commit to release, for changes released in the window.

    Returns median, p75, and p90, plus a breakdown into PR cycle time and merge-to-release time.
    """
    return _call(metrics.lead_time_for_changes, repo, start, end, include_bots)


@mcp.tool(annotations=READ_ONLY)
def pr_cycle_time(
    repo: Repo = None, start: Start = None, end: End = None,
    include_bots: Annotated[bool, Field(description="Include bot-authored PRs.")] = False,
) -> dict:
    """PR cycle time: hours from PR opened to merged, for PRs merged in the window. Usually what people mean by 'PR lead time'."""
    return _call(metrics.pr_cycle_time, repo, start, end, include_bots)


@mcp.tool(annotations=READ_ONLY)
def deployment_frequency(
    repo: Repo = None, start: Start = None, end: End = None,
    grain: Annotated[str, Field(description="Bucket size for the series: day, week, or month.")] = "week",
) -> dict:
    """How often releases ship: totals, deploys per week, and a zero-filled time series."""
    return _call(metrics.deployment_frequency, repo, start, end, grain)


@mcp.tool(annotations=READ_ONLY)
def change_failure_rate(repo: Repo = None, start: Start = None, end: End = None) -> dict:
    """Share of releases in the window that needed a remediation (revert, hotfix, or regression fix)."""
    return _call(metrics.change_failure_rate, repo, start, end)


@mcp.tool(annotations=READ_ONLY)
def time_to_restore(repo: Repo = None, start: Start = None, end: End = None) -> dict:
    """Failed deployment recovery time: hours from a failed release to the release that shipped its fix."""
    return _call(metrics.time_to_restore, repo, start, end)


@mcp.tool(annotations=READ_ONLY)
def ci_health(repo: Repo = None, start: Start = None, end: End = None) -> dict:
    """GitHub Actions health on the default branch: success rate, durations, and the busiest workflows."""
    return _call(metrics.ci_health, repo, start, end)


@mcp.tool(annotations=READ_ONLY)
def describe_schema() -> dict:
    """Columns of every dimension and fact table, for writing run_sql queries."""
    with _connect() as con:
        rows = con.execute("""
            SELECT table_name, list(column_name || ' ' || data_type ORDER BY ordinal_position)
            FROM information_schema.columns
            WHERE table_name LIKE 'dim_%' OR table_name LIKE 'fact_%'
            GROUP BY table_name ORDER BY table_name
        """).fetchall()
    return {
        "tables": {name: cols for name, cols in rows},
        "joins": "fact_*.repo_key -> dim_repo; fact_*.author_key -> dim_author; "
                 "fact_*.date_key / merged_date_key / deployed_date_key -> dim_date.date_key; "
                 "fact_change.deployment_id -> fact_deployment.deployment_id",
    }


@mcp.tool(annotations=READ_ONLY)
def run_sql(sql: Annotated[str, Field(description="A single read-only DuckDB SELECT/WITH query.")]) -> dict:
    """Run an ad-hoc read-only SQL query against the star schema. Returns at most 200 rows."""
    return _call(metrics.run_sql, sql)


def serve() -> None:
    mcp.run("stdio")
