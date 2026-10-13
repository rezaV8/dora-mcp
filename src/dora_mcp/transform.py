"""Transform: build a star schema in DuckDB from the raw JSONL files.

    raw_*  (bronze)  ->  stg_*  (silver)  ->  dim_* / fact_*  (gold)

Grain of each fact table:
    fact_change      one row per merged PR into the default branch
    fact_deployment  one row per published, non-prerelease GitHub release
    fact_ci_run      one row per completed Actions run on the default branch

Modeling assumptions (the ones to defend in an interview):
  * A "deployment" is a GitHub release. For libraries and CLIs, publishing a
    release is the moment code reaches users.
  * A change's lead time runs from its first commit to the first deployment
    at or after its merge, matched with an ASOF join.
  * A deployment "failed" if a remediation PR (a revert, hotfix, or
    regression fix) was merged after it and before the next deployment.
    Time to restore runs from the failed deployment to the deployment that
    shipped the first remediation.
"""

from datetime import timedelta
from pathlib import Path

import duckdb

from . import config

TS = "'%Y-%m-%dT%H:%M:%SZ'"  # GitHub timestamps are UTC with a Z suffix

RAW_SOURCES = {
    "raw_repos": ("repos", {
        "repo": "VARCHAR", "owner": "VARCHAR", "name": "VARCHAR", "default_branch": "VARCHAR",
        "language": "VARCHAR", "stars": "BIGINT", "ingested_at": "VARCHAR",
    }),
    "raw_pull_requests": ("pull_requests", {
        "repo": "VARCHAR", "number": "BIGINT", "title": "VARCHAR", "author_login": "VARCHAR",
        "author_is_bot": "BOOLEAN", "created_at": "VARCHAR", "merged_at": "VARCHAR",
        "first_commit_at": "VARCHAR", "commit_count": "BIGINT", "additions": "BIGINT",
        "deletions": "BIGINT", "changed_files": "BIGINT", "merge_commit_sha": "VARCHAR",
        "labels": "VARCHAR[]",
    }),
    "raw_releases": ("releases", {
        "repo": "VARCHAR", "release_id": "BIGINT", "tag_name": "VARCHAR", "name": "VARCHAR",
        "published_at": "VARCHAR", "prerelease": "BOOLEAN", "draft": "BOOLEAN", "author_login": "VARCHAR",
    }),
    "raw_ci_runs": ("ci_runs", {
        "repo": "VARCHAR", "run_id": "BIGINT", "workflow_name": "VARCHAR", "event": "VARCHAR",
        "status": "VARCHAR", "conclusion": "VARCHAR", "head_sha": "VARCHAR", "run_attempt": "BIGINT",
        "actor_login": "VARCHAR", "created_at": "VARCHAR", "run_started_at": "VARCHAR", "updated_at": "VARCHAR",
    }),
}


def _load_raw(con: duckdb.DuckDBPyConnection, raw_dir: Path) -> None:
    for table, (folder, columns) in RAW_SOURCES.items():
        files = sorted((raw_dir / folder).glob("*.jsonl"))
        cols = ", ".join(f"{name} {dtype}" for name, dtype in columns.items())
        con.execute(f"CREATE OR REPLACE TABLE {table} ({cols})")
        for f in files:
            if f.stat().st_size:
                struct = "{" + ", ".join(f"'{k}': '{v}'" for k, v in columns.items()) + "}"
                con.execute(
                    f"INSERT INTO {table} SELECT * FROM read_json(?, format='newline_delimited', columns={struct})",
                    [str(f)],
                )


STAGING_SQL = f"""
CREATE OR REPLACE TABLE stg_deployment AS
SELECT repo, release_id AS deployment_id, tag_name,
       strptime(published_at, {TS}) AS deployed_at
FROM raw_releases
WHERE NOT draft AND NOT prerelease;

CREATE OR REPLACE TABLE stg_change AS
SELECT repo, number, title, author_login, author_is_bot,
       strptime(created_at, {TS}) AS created_at,
       strptime(merged_at, {TS}) AS merged_at,
       -- The first commit can't be later than merge time. Rebases can rewrite
       -- dates, so clamp it, and fall back to PR creation when it's missing.
       least(coalesce(strptime(first_commit_at, {TS}), strptime(created_at, {TS})),
             strptime(merged_at, {TS})) AS first_commit_at,
       commit_count, additions, deletions, changed_files, labels,
       regexp_matches(lower(title), $remediation_regex)
         OR list_has_any(list_transform(labels, l -> lower(l)), $remediation_labels) AS is_remediation,
       -- GitHub's revert button produces titles like: Revert "Original title"
       nullif(regexp_extract(title, '^Revert "(.*)"$', 1), '') AS reverted_title
FROM raw_pull_requests;

-- Which deployment shipped each change: the first release at or after the merge.
CREATE OR REPLACE TABLE stg_change_deployment AS
SELECT c.*, d.deployment_id, d.deployed_at
FROM stg_change c
ASOF LEFT JOIN stg_deployment d
  ON c.repo = d.repo AND c.merged_at <= d.deployed_at;

-- Which deployment each remediation was fixing: the latest release before it merged.
-- A revert of a change that was never released fixed nothing in production,
-- so it doesn't count. If the original PR is outside the ingested window,
-- assume it shipped.
CREATE OR REPLACE TABLE stg_remediation AS
SELECT prev.deployment_id AS failed_deployment_id, c.number AS pr_number,
       c.merged_at, c.deployed_at AS restored_at
FROM stg_change_deployment c
ASOF JOIN stg_deployment prev
  ON c.repo = prev.repo AND c.merged_at > prev.deployed_at
WHERE c.is_remediation
  AND NOT EXISTS (
      SELECT 1 FROM stg_change_deployment orig
      WHERE orig.repo = c.repo AND orig.title = c.reverted_title
        AND orig.merged_at < c.merged_at
        AND (orig.deployed_at IS NULL OR orig.deployed_at > c.merged_at)
  );
"""

STAR_SQL = """
CREATE OR REPLACE TABLE dim_repo AS
SELECT row_number() OVER (ORDER BY repo)::INTEGER AS repo_key,
       repo AS full_name, owner, name, default_branch, language, stars,
       strptime(ingested_at, '%Y-%m-%dT%H:%M:%SZ') AS ingested_at
FROM raw_repos;

CREATE OR REPLACE TABLE dim_author AS
WITH logins AS (
    SELECT author_login AS login, author_is_bot AS is_bot FROM raw_pull_requests
    UNION ALL
    SELECT actor_login, actor_login LIKE '%[bot]' FROM raw_ci_runs WHERE actor_login IS NOT NULL
)
SELECT row_number() OVER (ORDER BY login)::INTEGER AS author_key, login, bool_or(is_bot) AS is_bot
FROM logins GROUP BY login;

CREATE OR REPLACE TABLE dim_date AS
SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
       CAST(d AS DATE) AS date,
       year(d) AS year, quarter(d) AS quarter, month(d) AS month, monthname(d) AS month_name,
       CAST(date_trunc('week', d) AS DATE) AS week_start, weekofyear(d) AS iso_week,
       isodow(d) AS day_of_week, dayname(d) AS day_name, isodow(d) IN (6, 7) AS is_weekend
FROM generate_series($date_from, $date_to, INTERVAL 1 DAY) AS t(d);

CREATE OR REPLACE TABLE fact_deployment AS
SELECT d.deployment_id,
       r.repo_key,
       CAST(strftime(d.deployed_at, '%Y%m%d') AS INTEGER) AS date_key,
       d.tag_name,
       d.deployed_at,
       coalesce(cc.change_count, 0) AS change_count,
       f.failed_deployment_id IS NOT NULL AS is_failure,
       coalesce(f.remediation_count, 0) AS remediation_count,
       f.restored_at,
       round(date_diff('second', d.deployed_at, f.restored_at) / 3600.0, 2) AS restore_hours
FROM stg_deployment d
JOIN dim_repo r ON r.full_name = d.repo
LEFT JOIN (SELECT deployment_id, count(*) AS change_count
           FROM stg_change_deployment GROUP BY ALL) cc USING (deployment_id)
LEFT JOIN (SELECT failed_deployment_id, count(*) AS remediation_count, min(restored_at) AS restored_at
           FROM stg_remediation GROUP BY ALL) f ON f.failed_deployment_id = d.deployment_id;

CREATE OR REPLACE TABLE fact_change AS
SELECT r.repo_key,
       a.author_key,
       c.number AS pr_number,
       c.title,
       CAST(strftime(c.merged_at, '%Y%m%d') AS INTEGER) AS merged_date_key,
       CAST(strftime(c.deployed_at, '%Y%m%d') AS INTEGER) AS deployed_date_key,
       c.deployment_id,
       c.first_commit_at, c.created_at, c.merged_at, c.deployed_at,
       c.is_remediation,
       c.commit_count, c.additions, c.deletions, c.changed_files,
       -- DORA lead time for changes: first commit -> running in production
       round(date_diff('second', c.first_commit_at, c.deployed_at) / 3600.0, 2) AS lead_time_hours,
       -- PR cycle time: opened -> merged (the part a team controls directly)
       round(date_diff('second', c.created_at, c.merged_at) / 3600.0, 2) AS pr_cycle_hours,
       round(date_diff('second', c.merged_at, c.deployed_at) / 3600.0, 2) AS merge_to_deploy_hours
FROM stg_change_deployment c
JOIN dim_repo r ON r.full_name = c.repo
JOIN dim_author a ON a.login = c.author_login;

CREATE OR REPLACE TABLE fact_ci_run AS
SELECT ci.run_id,
       r.repo_key,
       a.author_key,
       CAST(strftime(strptime(ci.created_at, '%Y-%m-%dT%H:%M:%SZ'), '%Y%m%d') AS INTEGER) AS date_key,
       ci.workflow_name, ci.event, ci.conclusion, ci.run_attempt, ci.head_sha,
       strptime(ci.run_started_at, '%Y-%m-%dT%H:%M:%SZ') AS started_at,
       strptime(ci.updated_at, '%Y-%m-%dT%H:%M:%SZ') AS finished_at,
       round(date_diff('second', strptime(ci.run_started_at, '%Y-%m-%dT%H:%M:%SZ'),
                                 strptime(ci.updated_at, '%Y-%m-%dT%H:%M:%SZ')) / 60.0, 2) AS duration_minutes
FROM raw_ci_runs ci
JOIN dim_repo r ON r.full_name = ci.repo
LEFT JOIN dim_author a ON a.login = ci.actor_login
WHERE ci.status = 'completed';
"""


def _run(con: duckdb.DuckDBPyConnection, sql: str, params: dict) -> None:
    """Run a multi-statement script, passing each statement only its own $params."""
    for stmt in filter(str.strip, sql.split(";\n")):
        con.execute(stmt, {k: v for k, v in params.items() if f"${k}" in stmt})


def build(db_path: Path = config.DB_PATH, raw_dir: Path = config.RAW_DIR) -> dict[str, int]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.execute("BEGIN")
        _load_raw(con, raw_dir)
        _run(con, STAGING_SQL, {
            "remediation_regex": config.REMEDIATION_TITLE_REGEX,
            "remediation_labels": config.REMEDIATION_LABELS,
        })
        lo, hi = con.execute("""
            SELECT min(ts)::DATE, max(ts)::DATE FROM (
                SELECT first_commit_at AS ts FROM stg_change UNION ALL
                SELECT deployed_at FROM stg_deployment UNION ALL
                SELECT strptime(created_at, '%Y-%m-%dT%H:%M:%SZ') FROM raw_ci_runs)
        """).fetchone()
        if lo is None:
            raise RuntimeError(f"No raw data found under {raw_dir}. Run `dora-mcp ingest` first.")
        # Extend the calendar a year past the data so "this month" queries
        # zero-fill cleanly.
        _run(con, STAR_SQL, {"date_from": lo, "date_to": hi + timedelta(days=365)})
        con.execute("COMMIT")
        tables = ["dim_repo", "dim_author", "dim_date", "fact_change", "fact_deployment", "fact_ci_run"]
        return {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in tables}
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
