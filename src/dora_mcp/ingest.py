"""Extract: pull PRs, releases, and CI runs from GitHub into raw JSONL files.

The raw layer stays close to the API response (the "bronze" layer in
medallion terms). All modeling happens later in transform.py, so the model
can be rebuilt without hitting the API again.
"""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from . import config
from .github import GitHub

PR_QUERY = """
query($owner: String!, $name: String!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(states: MERGED, first: 50, after: $cursor,
                 orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number title createdAt mergedAt updatedAt baseRefName
        additions deletions changedFiles
        author { login __typename }
        mergeCommit { oid }
        labels(first: 20) { nodes { name } }
        commits(first: 1) { totalCount nodes { commit { authoredDate } } }
      }
    }
  }
}
"""


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def _iso(ts: str | None) -> datetime | None:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")) if ts else None


def fetch_merged_prs(gh: GitHub, owner: str, name: str, base: str, since: datetime) -> list[dict]:
    rows, cursor = [], None
    while True:
        page = gh.graphql(PR_QUERY, owner=owner, name=name, cursor=cursor)["repository"]["pullRequests"]
        for pr in page["nodes"]:
            if pr["baseRefName"] != base or _iso(pr["mergedAt"]) < since:
                continue
            author = pr["author"] or {"login": "ghost", "__typename": "User"}
            commits = pr["commits"]["nodes"]
            rows.append({
                "repo": f"{owner}/{name}",
                "number": pr["number"],
                "title": pr["title"],
                "author_login": author["login"],
                "author_is_bot": author["__typename"] == "Bot" or author["login"].endswith("[bot]"),
                "created_at": pr["createdAt"],
                "merged_at": pr["mergedAt"],
                "first_commit_at": commits[0]["commit"]["authoredDate"] if commits else None,
                "commit_count": pr["commits"]["totalCount"],
                "additions": pr["additions"],
                "deletions": pr["deletions"],
                "changed_files": pr["changedFiles"],
                "merge_commit_sha": (pr["mergeCommit"] or {}).get("oid"),
                "labels": [lbl["name"] for lbl in pr["labels"]["nodes"]],
            })
        # Ordered by updatedAt desc and mergedAt <= updatedAt, so once a page's
        # oldest update predates the window, nothing older can qualify.
        oldest_update = _iso(page["nodes"][-1]["updatedAt"]) if page["nodes"] else since
        if not page["pageInfo"]["hasNextPage"] or oldest_update < since:
            return rows
        cursor = page["pageInfo"]["endCursor"]


def fetch_releases(gh: GitHub, full_name: str, since: datetime) -> list[dict]:
    rows = []
    for rel in gh.paginate(f"/repos/{full_name}/releases"):
        published = _iso(rel["published_at"])
        if published is None:  # drafts have no publish time
            continue
        # Releases come back roughly newest first (by creation, not publish
        # time), so stop once we are well past the window.
        if published < since - timedelta(days=30):
            break
        if published < since:
            continue
        rows.append({
            "repo": full_name,
            "release_id": rel["id"],
            "tag_name": rel["tag_name"],
            "name": rel["name"],
            "published_at": rel["published_at"],
            "prerelease": rel["prerelease"],
            "draft": rel["draft"],
            "author_login": (rel.get("author") or {}).get("login"),
        })
    return rows


def fetch_ci_runs(gh: GitHub, full_name: str, branch: str, since: datetime, limit: int) -> list[dict]:
    rows = []
    params = {"branch": branch, "created": f">={since:%Y-%m-%d}", "exclude_pull_requests": "true"}
    for run in gh.paginate(f"/repos/{full_name}/actions/runs", params, items_key="workflow_runs"):
        rows.append({
            "repo": full_name,
            "run_id": run["id"],
            "workflow_name": run["name"],
            "event": run["event"],
            "status": run["status"],
            "conclusion": run["conclusion"],
            "head_sha": run["head_sha"],
            "run_attempt": run.get("run_attempt", 1),
            "actor_login": (run.get("triggering_actor") or run.get("actor") or {}).get("login"),
            "created_at": run["created_at"],
            "run_started_at": run.get("run_started_at") or run["created_at"],
            "updated_at": run["updated_at"],
        })
        if len(rows) >= limit:
            break
    return rows


def ingest(repos: list[str], days: int, raw_dir: Path = config.RAW_DIR) -> None:
    gh = GitHub()
    since = datetime.now(UTC) - timedelta(days=days)
    for full_name in repos:
        owner, name = full_name.split("/")
        slug = f"{owner}__{name}"
        print(f"{full_name}: since {since:%Y-%m-%d}")

        meta = gh.get(f"/repos/{full_name}")
        branch = meta["default_branch"]
        _write_jsonl(raw_dir / "repos" / f"{slug}.jsonl", [{
            "repo": full_name,
            "owner": owner,
            "name": name,
            "default_branch": branch,
            "language": meta.get("language"),
            "stars": meta.get("stargazers_count"),
            "ingested_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }])

        prs = fetch_merged_prs(gh, owner, name, branch, since)
        _write_jsonl(raw_dir / "pull_requests" / f"{slug}.jsonl", prs)
        print(f"  {len(prs)} merged PRs")

        releases = fetch_releases(gh, full_name, since)
        _write_jsonl(raw_dir / "releases" / f"{slug}.jsonl", releases)
        print(f"  {len(releases)} releases")

        runs = fetch_ci_runs(gh, full_name, branch, since, config.MAX_CI_RUNS_PER_REPO)
        _write_jsonl(raw_dir / "ci_runs" / f"{slug}.jsonl", runs)
        print(f"  {len(runs)} CI runs on {branch}")
