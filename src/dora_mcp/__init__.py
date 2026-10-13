"""DORA metrics from GitHub data, modeled as a star schema and served over MCP."""

import argparse
import json


def main() -> None:
    parser = argparse.ArgumentParser(prog="dora-mcp", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="Pull raw data from GitHub into data/raw/")
    p_ingest.add_argument("repos", nargs="*", help="owner/name (default: a few public repos)")
    p_ingest.add_argument("--days", type=int, default=None, help="Lookback window in days")

    sub.add_parser("build", help="Build the star schema in DuckDB from data/raw/")

    p_report = sub.add_parser("report", help="Print a DORA summary to the terminal")
    p_report.add_argument("--repo")
    p_report.add_argument("--start")
    p_report.add_argument("--end")

    sub.add_parser("serve", help="Run the MCP server over stdio")

    args = parser.parse_args()

    # Lazy imports keep `serve` startup lean and the stdio channel clean.
    if args.command == "ingest":
        from . import config, ingest
        ingest.ingest(args.repos or config.DEFAULT_REPOS, args.days or config.DEFAULT_LOOKBACK_DAYS)
    elif args.command == "build":
        from . import transform
        for table, n in transform.build().items():
            print(f"{table:<18}{n:>8,} rows")
    elif args.command == "report":
        import duckdb
        from . import config, metrics
        with duckdb.connect(str(config.DB_PATH), read_only=True) as con:
            print(json.dumps(metrics.dora_summary(con, args.repo, args.start, args.end), indent=2))
    elif args.command == "serve":
        from .server import serve
        serve()
