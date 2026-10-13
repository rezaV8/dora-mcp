# dora-mcp

Engineering delivery metrics ([DORA](https://dora.dev/guides/dora-metrics-four-keys/)) computed from
public GitHub data, modeled as a **star schema in DuckDB**, and exposed to Claude through an
**MCP server**, so you can ask questions like *"what's our median PR lead time this month?"*.

```
GitHub API ──► ingest.py ──► data/raw/*.jsonl ──► transform.py ──► DuckDB star schema ──► server.py (MCP) ──► Claude
 (REST +        (extract)      (bronze: raw,        (SQL: staging      (gold: dims +           (read-only tools)
  GraphQL)                      replayable)          + modeling)         facts)
```

## Quick start

```bash
uv sync
uv run dora-mcp ingest                    # default repos, last 180 days (uses `gh auth token` or GITHUB_TOKEN)
uv run dora-mcp ingest owner/repo --days 90   # or pick your own
uv run dora-mcp build                     # raw JSONL -> star schema in data/dora.duckdb
uv run dora-mcp report --repo uv --start 2026-09-01
uv run pytest
```

### Connect it to Claude

Claude Code:

```bash
claude mcp add dora-metrics -- uv --directory /absolute/path/to/dora-mcp run dora-mcp serve
```

Claude Desktop (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "dora-metrics": {
      "command": "uv",
      "args": ["--directory", "/absolute/path/to/dora-mcp", "run", "dora-mcp", "serve"]
    }
  }
}
```

Then ask: *"Compare uv and fastapi on all four DORA metrics for September"*, *"Which uv releases
failed last month and how long did recovery take?"*, *"Which CI workflow fails most often?"*

`scripts/smoke_test_mcp.py` starts the server over stdio the way Claude does and calls a few tools.

## Data model

```
                 dim_date ─────────────┐
                (date_key)             │
                    │                  │
   dim_repo ──► fact_change ◄── dim_author
  (repo_key)   one row per merged PR   (author_key)
      │          │ deployment_id
      │          ▼
      ├──► fact_deployment ◄── dim_date
      │    one row per release
      │
      └──► fact_ci_run ◄── dim_date, dim_author
           one row per completed Actions run on the default branch
```

| Table | Grain | Key measures |
|---|---|---|
| `fact_change` | merged PR into default branch | `lead_time_hours`, `pr_cycle_hours`, `merge_to_deploy_hours`, lines changed, `is_remediation` |
| `fact_deployment` | published, non-prerelease release | `change_count`, `is_failure`, `restore_hours` |
| `fact_ci_run` | completed workflow run | `conclusion`, `duration_minutes` |
| `dim_date` | calendar day | year/quarter/month/ISO week/weekday. Drives zero-filled time series |
| `dim_repo`, `dim_author` | repo / GitHub login | `is_bot` lets metrics exclude Dependabot-style PRs |

`fact_change` has a role-playing date dimension, with both `merged_date_key` and `deployed_date_key`
pointing at `dim_date`. Which one you filter on changes the question: "PRs merged this month" is
not the same as "changes shipped this month".

## Metric definitions

| Metric | Definition here | Tool |
|---|---|---|
| **Lead time for changes** | first commit → first release at or after merge (ASOF join) | `lead_time_for_changes` |
| PR cycle time | PR opened → merged (what most people mean by "PR lead time") | `pr_cycle_time` |
| **Deployment frequency** | releases per week, plus a zero-filled day/week/month series | `deployment_frequency` |
| **Change failure rate** | share of releases followed by a remediation PR before the next release | `change_failure_rate` |
| **Time to restore** | failed release → release that shipped the first remediation | `time_to_restore` |
| CI health | success rate (cancelled/skipped excluded) and duration percentiles per workflow | `ci_health` |

Also available: `dora_summary` (all four metrics, each with an approximate elite/high/medium/low band),
`data_coverage`, `describe_schema`, and `run_sql` for ad-hoc questions.

## Design decisions and trade-offs

- **A release counts as a deployment.** These are open-source libraries, so publishing a release is
  when users get the code. For a web service you'd use the GitHub Deployments API or deploy-job
  events instead, and only `stg_deployment` would change.
- **The raw layer is separate from the model.** Ingest writes API-shaped JSONL, and every metric
  definition lives in SQL in `transform.py`. Changing a heuristic means a 1-second rebuild, not a
  3-minute re-download. This is the bronze/silver/gold (medallion) pattern.
- **ASOF joins match changes to deployments.** "The first release at or after this merge" is a
  nearest-match join. A correlated subquery or a window function over a union would also work, but
  less clearly.
- **Medians and p90, not means.** Lead times are heavily right-skewed: a few PRs open for over a
  year would dominate a mean.
- **Bots are excluded by default.** Dependabot PRs merge in minutes and would flatter lead time.
- **GROUPING SETS** returns the overall row and the per-repo breakdown in one query.
- **The MCP surface is tools-first, with an escape hatch.** Named tools carry the definitions, so
  the model can't silently compute a metric differently. `run_sql` covers the long tail, with
  guardrails (below).

### Security of `run_sql`

Defense in depth, each layer covered by tests:

1. A statement allowlist: one statement, and it must start with SELECT/WITH/DESCRIBE/SUMMARIZE.
2. The connection is opened `read_only`. `WITH x AS (...) DELETE ...` gets past check 1 but fails here.
3. `enable_external_access=false`, so the model can't run `read_csv('~/.ssh/...')` to read local files or URLs.
4. Results are capped at 200 rows.
5. Each tool call opens a short-lived connection, so the server never holds a lock and
   `dora-mcp build` can refresh data while it runs.

## What the real data taught me

The first version flagged any PR whose title contained "revert", "hotfix", or "regression" as a
remediation. That put **uv's change failure rate at 47%**. Looking at the flagged PRs showed two
kinds of false positives:

1. *"Add regression test for #21857"* is a test addition, not a fix. The regex now needs
   `fix… regression`.
2. *`Revert "Limit CI planner checkout depth"`* reverted a change before it was ever released, so
   nothing in production broke. Reverts now count only when the reverted PR had already shipped.

After both fixes, uv's change failure rate is **18%**, and each of the remaining failures traces to
a real revert of shipped code. Takeaway: change failure rate is only as good as your definition of
"failure". In a real org you'd use incident data (e.g. Azure DevOps work items, PagerDuty) rather
than PR titles.

## Known limitations

- The GitHub Actions runs API returns at most **1,000 results** for a filtered query, so CI history
  for busy repos is truncated. To fix this, ingest incrementally (daily) and append.
- Ingest is a full refresh of the lookback window. Incremental loads would key on `updated_at`
  watermarks and MERGE into the facts.
- `first_commit_at` uses the commit's authored date, which rebases can rewrite. It's clamped to be
  no later than the merge.
- DORA bands are approximate thresholds from the State of DevOps reports, applied to
  release-based deployments.

## Scaling this up (Databricks / Fabric / Spark)

The layers map one-to-one onto a lakehouse:

| Here | Lakehouse |
|---|---|
| `data/raw/*.jsonl` | Bronze Delta tables (Auto Loader / Fabric Data Pipeline) |
| `stg_*` tables | Silver: typed and cleaned, with business rules (remediation flag) |
| `dim_*` / `fact_*` | Gold star schema, served via a SQL warehouse or Power BI semantic model |
| DuckDB ASOF JOIN | Spark doesn't have one: use a range join + `row_number()`, or `merge_asof` in pandas-on-Spark |
| full refresh | `MERGE INTO` on `(repo, pr_number)` with an `updated_at` watermark |

## Layout

```
src/dora_mcp/
  github.py     REST + GraphQL client: pagination, rate-limit backoff
  ingest.py     extract -> data/raw/{repos,pull_requests,releases,ci_runs}/*.jsonl
  transform.py  raw -> staging -> star schema (all modeling SQL lives here)
  metrics.py    metric queries + DORA bands + guarded run_sql
  server.py     MCP server (tools, instructions, read-only connections)
  config.py     repos, paths, remediation heuristic
tests/          synthetic dataset with hand-computed expected metrics
scripts/        MCP stdio smoke test
```
