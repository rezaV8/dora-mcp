"""End-to-end tests: synthetic raw JSONL -> star schema -> metrics, with hand-computed expectations.

Timeline for acme/app (all times UTC, January 2026):

  Jan 4 12:00  PR #1 first commit
  Jan 5 06:00  PR #1 merged
  Jan 5 12:00  release v1           <- ships #1 (lead time 24h)
  Jan 6 00:00  PR #2 (bot) first commit; merged Jan 6 06:00
  Jan 7 10:00  PR #3 "Revert ..." first commit; merged Jan 7 12:00  -> v1 failed
  Jan 8 12:00  release v2           <- ships #2 (60h) and #3 (26h); restores v1 after 72h
  Jan 8 00:00  PR #4 first commit; merged Jan 9 00:00
  Jan 10       PR #6 "Add regression test" merged (not a remediation) -> v3 (132h)
  Jan 14       release v3-rc1 (prerelease, ignored)
  Jan 15 12:00 release v3           <- ships #4 (180h) and #6
  Jan 16       PRs #5, #8, and #9 (reverts unreleased #8) merged, not yet released
"""

import json
import shutil
from datetime import date

import duckdb
import pytest

from mcp.server.mcpserver.exceptions import ToolError

from dora_mcp import config, metrics, server, transform

WINDOW = {"start": "2026-01-01", "end": "2026-01-31"}


def pr(number, title, first_commit, created, merged, author="alice", bot=False, labels=()):
    return {
        "repo": "acme/app", "number": number, "title": title, "author_login": author,
        "author_is_bot": bot, "created_at": created, "merged_at": merged,
        "first_commit_at": first_commit, "commit_count": 1, "additions": 10, "deletions": 2,
        "changed_files": 1, "merge_commit_sha": f"sha{number}", "labels": list(labels),
    }


def release(rid, tag, published, prerelease=False):
    return {"repo": "acme/app", "release_id": rid, "tag_name": tag, "name": tag,
            "published_at": published, "prerelease": prerelease, "draft": False, "author_login": "alice"}


def run(rid, conclusion, start, end):
    return {"repo": "acme/app", "run_id": rid, "workflow_name": "CI", "event": "push", "status": "completed",
            "conclusion": conclusion, "head_sha": "x", "run_attempt": 1, "actor_login": "alice",
            "created_at": start, "run_started_at": start, "updated_at": end}


RAW = {
    "repos": [{"repo": "acme/app", "owner": "acme", "name": "app", "default_branch": "main",
               "language": "Python", "stars": 1, "ingested_at": "2026-01-20T00:00:00Z"}],
    "pull_requests": [
        pr(1, "Add feature", "2026-01-04T12:00:00Z", "2026-01-04T18:00:00Z", "2026-01-05T06:00:00Z"),
        pr(2, "Bump deps", "2026-01-06T00:00:00Z", "2026-01-06T00:00:00Z", "2026-01-06T06:00:00Z",
           author="dependabot[bot]", bot=True),
        pr(3, 'Revert "Add feature"', "2026-01-07T10:00:00Z", "2026-01-07T11:00:00Z", "2026-01-07T12:00:00Z"),
        pr(4, "Refactor", "2026-01-08T00:00:00Z", "2026-01-08T06:00:00Z", "2026-01-09T00:00:00Z"),
        pr(5, "Docs", "2026-01-16T00:00:00Z", "2026-01-16T00:00:00Z", "2026-01-16T02:00:00Z"),
        # Not remediations: a test addition, and a revert of a change that never shipped.
        pr(6, "Add regression test for #3", "2026-01-10T00:00:00Z", "2026-01-10T00:00:00Z", "2026-01-10T04:00:00Z"),
        pr(8, "Experiment", "2026-01-16T00:00:00Z", "2026-01-16T00:00:00Z", "2026-01-16T01:00:00Z"),
        pr(9, 'Revert "Experiment"', "2026-01-16T02:00:00Z", "2026-01-16T02:00:00Z", "2026-01-16T03:00:00Z"),
    ],
    "releases": [
        release(101, "v1", "2026-01-05T12:00:00Z"),
        release(102, "v2", "2026-01-08T12:00:00Z"),
        release(103, "v3-rc1", "2026-01-14T00:00:00Z", prerelease=True),
        release(104, "v3", "2026-01-15T12:00:00Z"),
    ],
    "ci_runs": [
        run(1, "success", "2026-01-05T00:00:00Z", "2026-01-05T00:10:00Z"),
        run(2, "failure", "2026-01-06T00:00:00Z", "2026-01-06T00:20:00Z"),
        run(3, "cancelled", "2026-01-07T00:00:00Z", "2026-01-07T00:05:00Z"),
    ],
}


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    root = tmp_path_factory.mktemp("dora")
    for folder, rows in RAW.items():
        (root / "raw" / folder).mkdir(parents=True)
        (root / "raw" / folder / "acme__app.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    counts = transform.build(root / "dora.duckdb", root / "raw")
    assert counts["fact_change"] == 8 and counts["fact_deployment"] == 3 and counts["fact_ci_run"] == 3
    return root / "dora.duckdb"


@pytest.fixture(scope="module")
def con(db_path):
    with duckdb.connect(str(db_path), read_only=True) as c:
        yield c


def test_changes_map_to_first_release_after_merge(con):
    rows = dict(con.execute("""
        SELECT f.pr_number, d.tag_name FROM fact_change f
        LEFT JOIN fact_deployment d USING (deployment_id) ORDER BY 1
    """).fetchall())
    assert rows == {1: "v1", 2: "v2", 3: "v2", 4: "v3", 5: None, 6: "v3", 8: None, 9: None}


def test_lead_time_excludes_bots_by_default(con):
    result = metrics.lead_time_for_changes(con, **WINDOW)["overall"]
    assert result["changes"] == 4          # 24h, 26h, 132h, 180h
    assert result["median_hours"] == 79
    assert result["band"] == "high"


def test_lead_time_with_bots(con):
    result = metrics.lead_time_for_changes(con, include_bots=True, **WINDOW)["overall"]
    assert result["changes"] == 5          # + 60h
    assert result["median_hours"] == 60


def test_pr_cycle_time_counts_undeployed(con):
    result = metrics.pr_cycle_time(con, **WINDOW)["overall"]
    assert result["merged_prs"] == 7
    assert result["not_yet_deployed"] == 3
    assert result["median_hours"] == 2     # cycles: 12, 1, 18, 2, 4, 1, 1


def test_deployment_frequency_ignores_prereleases(con):
    result = metrics.deployment_frequency(con, grain="week", **WINDOW)
    assert result["overall"]["deployments"] == 3
    assert result["overall"]["per_week"] == round(3 / (31 / 7), 2)
    assert result["overall"]["band"] == "medium"
    assert sum(p["deployments"] for p in result["series"]) == 3
    assert any(p["deployments"] == 0 for p in result["series"])  # zero-filled via dim_date


def test_remediation_heuristic(con):
    flagged = {n for (n,) in con.execute("SELECT pr_number FROM fact_change WHERE is_remediation").fetchall()}
    assert flagged == {3, 9}  # #6 "regression test" is not a fix
    # ...and #9 reverts an unreleased PR, so it must not fail v3
    assert con.execute("SELECT is_failure FROM fact_deployment WHERE tag_name = 'v3'").fetchone() == (False,)


def test_change_failure_rate(con):
    result = metrics.change_failure_rate(con, **WINDOW)["overall"]
    assert result["failed_deployments"] == 1
    assert result["rate"] == pytest.approx(1 / 3, abs=1e-3)
    assert result["failed_tags"] == ["v1"]


def test_time_to_restore(con):
    result = metrics.time_to_restore(con, **WINDOW)["overall"]
    assert result["median_hours"] == 72
    assert result["band"] == "medium"


def test_ci_health_ignores_cancelled_in_success_rate(con):
    result = metrics.ci_health(con, **WINDOW)["overall"]
    assert result["runs"] == 3
    assert result["success_rate"] == 0.5


def test_window_filters(con):
    result = metrics.deployment_frequency(con, start="2026-01-09", end="2026-01-31")["overall"]
    assert result["deployments"] == 1


def test_repo_resolution(con):
    assert metrics.lead_time_for_changes(con, repo="app", **WINDOW)["repo"] == "acme/app"
    with pytest.raises(metrics.MetricsError, match="Known repos"):
        metrics.lead_time_for_changes(con, repo="nope", **WINDOW)


def test_by_repo_breakdown_when_unfiltered(con):
    result = metrics.change_failure_rate(con, **WINDOW)
    assert [r["repo"] for r in result["by_repo"]] == ["acme/app"]


def test_default_window_is_last_30_days():
    start, end = metrics.resolve_window(None, "2026-01-31")
    assert (start, end) == (date(2026, 1, 2), date(2026, 1, 31))


@pytest.mark.parametrize("sql", ["DROP TABLE dim_repo", "SELECT 1; DROP TABLE dim_repo", "INSERT INTO dim_repo VALUES (1)"])
def test_run_sql_rejects_writes(con, sql):
    with pytest.raises(metrics.MetricsError):
        metrics.run_sql(con, sql)


def test_run_sql_select(con):
    result = metrics.run_sql(con, "SELECT tag_name FROM fact_deployment ORDER BY deployed_at")
    assert result["rows"] == [["v1"], ["v2"], ["v3"]]


def test_server_blocks_local_file_reads(db_path, monkeypatch, tmp_path):
    # Use a copy: DuckDB won't open one file with two configs in one process.
    monkeypatch.setattr(config, "DB_PATH", shutil.copy(db_path, tmp_path / "copy.duckdb"))
    secret = tmp_path / "secret.csv"
    secret.write_text("token\nabc\n")
    with pytest.raises(ToolError, match="disabled by configuration"):
        server.run_sql(f"SELECT * FROM read_csv('{secret}')")
    assert server.run_sql("SELECT count(*) FROM dim_repo")["rows"] == [[1]]
