"""DORA metric queries over the star schema.

Every function takes an inclusive [start, end] date window and an optional
repo filter. It returns plain dicts with an "overall" row and, when no repo
is given, a "by_repo" breakdown. GROUPING SETS computes both in one query.
"""

import re
from datetime import UTC, date, datetime, timedelta
from typing import Any

import duckdb

# Approximate performance bands from the DORA State of DevOps reports.
# Each entry is (band, upper bound). Lower is better except deploy frequency.
LEAD_TIME_BANDS = [("elite", 24), ("high", 24 * 7), ("medium", 24 * 30), ("low", float("inf"))]
RESTORE_BANDS = [("elite", 1), ("high", 24), ("medium", 24 * 7), ("low", float("inf"))]
CFR_BANDS = [("elite", 0.05), ("high", 0.10), ("medium", 0.15), ("low", float("inf"))]
# deploys per week, as a lower bound
DEPLOY_FREQ_BANDS = [("elite", 7), ("high", 1), ("medium", 0.25), ("low", 0)]


class MetricsError(ValueError):
    pass


def resolve_window(start: str | date | None, end: str | date | None, default_days: int = 30) -> tuple[date, date]:
    end_d = date.fromisoformat(str(end)) if end else datetime.now(UTC).date()
    start_d = date.fromisoformat(str(start)) if start else end_d - timedelta(days=default_days - 1)
    if start_d > end_d:
        raise MetricsError(f"start {start_d} is after end {end_d}")
    return start_d, end_d


def resolve_repo(con: duckdb.DuckDBPyConnection, repo: str | None) -> int | None:
    """Accept 'owner/name' or just 'name' (case-insensitive). None means all repos."""
    if not repo:
        return None
    rows = con.execute(
        "SELECT repo_key FROM dim_repo WHERE lower(full_name) = lower($r) OR lower(name) = lower($r)", {"r": repo}
    ).fetchall()
    if len(rows) != 1:
        known = [r[0] for r in con.execute("SELECT full_name FROM dim_repo ORDER BY 1").fetchall()]
        raise MetricsError(f"Unknown or ambiguous repo {repo!r}. Known repos: {known}")
    return rows[0][0]


def _dicts(cur: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _jsonable(v: Any) -> Any:
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, float):
        return round(v, 3)
    return v


def _clean(rows: list[dict]) -> list[dict]:
    return [{k: _jsonable(v) for k, v in row.items()} for row in rows]


def _band(value: float | None, bands: list[tuple[str, float]], higher_is_better: bool = False) -> str | None:
    if value is None:
        return None
    for name, bound in bands:
        if (value >= bound) if higher_is_better else (value < bound):
            return name
    return bands[-1][0]


def _grouped(con, measures: str, body: str, params: dict, repo_key: int | None) -> dict:
    rows = _clean(_dicts(con.execute(
        f"SELECT r.full_name AS repo, {measures} {body} "
        "GROUP BY GROUPING SETS ((r.full_name), ()) ORDER BY repo NULLS FIRST",
        params,
    )))
    per_repo = [r for r in rows if r["repo"] is not None]
    overall = next({k: v for k, v in r.items() if k != "repo"} for r in rows if r["repo"] is None)
    result = {"window": {"start": params["start"].isoformat(), "end": params["end"].isoformat()}, "overall": overall}
    if repo_key is None:
        result["by_repo"] = per_repo
    else:
        result["repo"] = con.execute("SELECT full_name FROM dim_repo WHERE repo_key = ?", [repo_key]).fetchone()[0]
    return result


def _params(con, repo, start, end, **extra) -> tuple[dict, int | None]:
    start_d, end_d = resolve_window(start, end)
    repo_key = resolve_repo(con, repo)
    return {"start": start_d, "end": end_d, "repo_key": repo_key, **extra}, repo_key


REPO_FILTER = "($repo_key IS NULL OR r.repo_key = $repo_key)"


def lead_time_for_changes(con, repo=None, start=None, end=None, include_bots=False) -> dict:
    """First commit -> deployed, for changes deployed within the window."""
    params, repo_key = _params(con, repo, start, end, include_bots=include_bots)
    result = _grouped(con, """
        count(*) AS changes,
        median(f.lead_time_hours) AS median_hours,
        quantile_cont(f.lead_time_hours, 0.75) AS p75_hours,
        quantile_cont(f.lead_time_hours, 0.90) AS p90_hours,
        median(f.pr_cycle_hours) AS median_pr_cycle_hours,
        median(f.merge_to_deploy_hours) AS median_merge_to_deploy_hours
    """, f"""
        FROM fact_change f
        JOIN dim_repo r USING (repo_key)
        JOIN dim_author a USING (author_key)
        JOIN dim_date d ON d.date_key = f.deployed_date_key
        WHERE d.date BETWEEN $start AND $end AND {REPO_FILTER} AND ($include_bots OR NOT a.is_bot)
    """, params, repo_key)
    result["overall"]["band"] = _band(result["overall"]["median_hours"], LEAD_TIME_BANDS)
    result["definition"] = "Hours from a PR's first commit to the first release that shipped it. Window filters on release date."
    return result


def pr_cycle_time(con, repo=None, start=None, end=None, include_bots=False) -> dict:
    """PR opened -> merged, for PRs merged within the window (deployed or not)."""
    params, repo_key = _params(con, repo, start, end, include_bots=include_bots)
    result = _grouped(con, """
        count(*) AS merged_prs,
        median(f.pr_cycle_hours) AS median_hours,
        quantile_cont(f.pr_cycle_hours, 0.75) AS p75_hours,
        quantile_cont(f.pr_cycle_hours, 0.90) AS p90_hours,
        median(f.additions + f.deletions) AS median_lines_changed,
        count(*) FILTER (WHERE f.deployment_id IS NULL) AS not_yet_deployed
    """, f"""
        FROM fact_change f
        JOIN dim_repo r USING (repo_key)
        JOIN dim_author a USING (author_key)
        JOIN dim_date d ON d.date_key = f.merged_date_key
        WHERE d.date BETWEEN $start AND $end AND {REPO_FILTER} AND ($include_bots OR NOT a.is_bot)
    """, params, repo_key)
    result["definition"] = "Hours from PR opened to merged into the default branch. Window filters on merge date."
    return result


def deployment_frequency(con, repo=None, start=None, end=None, grain="week") -> dict:
    if grain not in ("day", "week", "month"):
        raise MetricsError("grain must be one of: day, week, month")
    params, repo_key = _params(con, repo, start, end)
    days = (params["end"] - params["start"]).days + 1
    result = _grouped(con, f"""
        count(*) AS deployments,
        count(DISTINCT d.date) AS days_with_deployment,
        round(count(*) / ({days} / 7.0), 2) AS per_week
    """, f"""
        FROM fact_deployment f
        JOIN dim_repo r USING (repo_key)
        JOIN dim_date d USING (date_key)
        WHERE d.date BETWEEN $start AND $end AND {REPO_FILTER}
    """, params, repo_key)
    result["overall"]["band"] = _band(result["overall"]["per_week"], DEPLOY_FREQ_BANDS, higher_is_better=True)
    # dim_date drives a zero-filled series, so quiet periods show up as 0.
    result["series"] = _clean(_dicts(con.execute(f"""
        SELECT CAST(date_trunc('{grain}', d.date) AS DATE) AS period, count(f.deployment_id) AS deployments
        FROM dim_date d
        LEFT JOIN (SELECT f.* FROM fact_deployment f JOIN dim_repo r USING (repo_key)
                   WHERE {REPO_FILTER}) f ON f.date_key = d.date_key
        WHERE d.date BETWEEN $start AND $end
        GROUP BY ALL ORDER BY period
    """, params)))
    result["definition"] = "Count of published, non-prerelease GitHub releases in the window."
    return result


def change_failure_rate(con, repo=None, start=None, end=None) -> dict:
    params, repo_key = _params(con, repo, start, end)
    result = _grouped(con, """
        count(*) AS deployments,
        count(*) FILTER (WHERE f.is_failure) AS failed_deployments,
        round(count(*) FILTER (WHERE f.is_failure) / nullif(count(*), 0), 4) AS rate,
        list(f.tag_name ORDER BY f.deployed_at) FILTER (WHERE f.is_failure) AS failed_tags
    """, f"""
        FROM fact_deployment f
        JOIN dim_repo r USING (repo_key)
        JOIN dim_date d USING (date_key)
        WHERE d.date BETWEEN $start AND $end AND {REPO_FILTER}
    """, params, repo_key)
    result["overall"]["band"] = _band(result["overall"]["rate"], CFR_BANDS)
    result["definition"] = (
        "Share of releases followed by a remediation PR (revert, hotfix, or regression fix) "
        "merged before the next release."
    )
    return result


def time_to_restore(con, repo=None, start=None, end=None) -> dict:
    params, repo_key = _params(con, repo, start, end)
    result = _grouped(con, """
        count(*) AS failed_deployments,
        count(f.restored_at) AS restored,
        median(f.restore_hours) AS median_hours,
        max(f.restore_hours) AS max_hours
    """, f"""
        FROM fact_deployment f
        JOIN dim_repo r USING (repo_key)
        JOIN dim_date d USING (date_key)
        WHERE f.is_failure AND d.date BETWEEN $start AND $end AND {REPO_FILTER}
    """, params, repo_key)
    result["overall"]["band"] = _band(result["overall"]["median_hours"], RESTORE_BANDS)
    result["definition"] = "Hours from a failed release to the release that shipped its first remediation."
    return result


def dora_summary(con, repo=None, start=None, end=None) -> dict:
    lt = lead_time_for_changes(con, repo, start, end)
    df = deployment_frequency(con, repo, start, end)
    cfr = change_failure_rate(con, repo, start, end)
    ttr = time_to_restore(con, repo, start, end)
    return {
        "window": lt["window"],
        "repo": repo or "all",
        "lead_time_for_changes": {k: lt["overall"][k] for k in ("changes", "median_hours", "p90_hours", "band")},
        "deployment_frequency": {k: df["overall"][k] for k in ("deployments", "per_week", "band")},
        "change_failure_rate": {k: cfr["overall"][k] for k in ("deployments", "failed_deployments", "rate", "band")},
        "time_to_restore": {k: ttr["overall"][k] for k in ("failed_deployments", "median_hours", "band")},
        "caveat": "Bands are approximate DORA thresholds, applied to release-based deployments.",
    }


def ci_health(con, repo=None, start=None, end=None, top_workflows=10) -> dict:
    params, repo_key = _params(con, repo, start, end)
    body = f"""
        FROM fact_ci_run f
        JOIN dim_repo r USING (repo_key)
        JOIN dim_date d USING (date_key)
        WHERE d.date BETWEEN $start AND $end AND {REPO_FILTER}
    """
    result = _grouped(con, """
        count(*) AS runs,
        count(*) FILTER (WHERE f.conclusion = 'success') AS succeeded,
        count(*) FILTER (WHERE f.conclusion = 'failure') AS failed,
        round(count(*) FILTER (WHERE f.conclusion = 'success')
              / nullif(count(*) FILTER (WHERE f.conclusion IN ('success', 'failure')), 0), 4) AS success_rate,
        median(f.duration_minutes) AS median_duration_minutes,
        quantile_cont(f.duration_minutes, 0.9) AS p90_duration_minutes
    """, body, params, repo_key)
    result["by_workflow"] = _clean(_dicts(con.execute(f"""
        SELECT r.full_name AS repo, f.workflow_name, count(*) AS runs,
               round(count(*) FILTER (WHERE f.conclusion = 'success')
                     / nullif(count(*) FILTER (WHERE f.conclusion IN ('success', 'failure')), 0), 4) AS success_rate,
               median(f.duration_minutes) AS median_duration_minutes
        {body}
        GROUP BY ALL ORDER BY runs DESC LIMIT {int(top_workflows)}
    """, params)))
    result["definition"] = "Completed GitHub Actions runs on the default branch. Success rate ignores cancelled and skipped runs."
    return result


def data_coverage(con) -> dict:
    rows = _clean(_dicts(con.execute("""
        SELECT r.full_name AS repo, r.default_branch, r.ingested_at,
               (SELECT count(*) FROM fact_change c WHERE c.repo_key = r.repo_key) AS changes,
               (SELECT min(merged_at)::DATE FROM fact_change c WHERE c.repo_key = r.repo_key) AS first_merge,
               (SELECT max(merged_at)::DATE FROM fact_change c WHERE c.repo_key = r.repo_key) AS last_merge,
               (SELECT count(*) FROM fact_deployment d WHERE d.repo_key = r.repo_key) AS deployments,
               (SELECT max(deployed_at)::DATE FROM fact_deployment d WHERE d.repo_key = r.repo_key) AS last_deployment,
               (SELECT count(*) FROM fact_ci_run ci WHERE ci.repo_key = r.repo_key) AS ci_runs
        FROM dim_repo r ORDER BY r.full_name
    """)))
    return {"today_utc": datetime.now(UTC).date().isoformat(), "repos": rows}


_READ_ONLY = re.compile(r"^\s*(select|with|describe|show|summarize|explain)\b", re.IGNORECASE)


def run_sql(con, sql: str, max_rows: int = 200) -> dict:
    """Run one read-only statement. The connection is also opened read-only."""
    stripped = sql.strip().rstrip(";")
    if ";" in stripped or not _READ_ONLY.match(stripped):
        raise MetricsError("Only a single SELECT/WITH/DESCRIBE/SUMMARIZE statement is allowed.")
    cur = con.execute(stripped)
    cols = [c[0] for c in cur.description]
    rows = cur.fetchmany(max_rows + 1)
    return {
        "columns": cols,
        "rows": [[_jsonable(v) for v in row] for row in rows[:max_rows]],
        "truncated": len(rows) > max_rows,
    }
