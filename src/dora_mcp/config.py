"""Paths and defaults, overridable through environment variables."""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = Path(os.environ.get("DORA_DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"
DB_PATH = Path(os.environ.get("DORA_DB", DATA_DIR / "dora.duckdb"))

DEFAULT_REPOS = ["astral-sh/uv", "fastapi/fastapi", "microsoft/playwright-python"]
DEFAULT_LOOKBACK_DAYS = 180
MAX_CI_RUNS_PER_REPO = 3000

# A merged PR counts as a remediation, meaning it fixed a failed deployment,
# when its title or labels match these. This heuristic decides what counts
# toward change failure rate, so it is the first thing to tune per team.
# "fix ... regression" matches but "add regression test" does not. Reverts
# are also checked against whether the reverted PR was ever released
# (see transform.py).
REMEDIATION_TITLE_REGEX = r"(^revert\b)|(\bhot ?fix\b)|(\bfix\w*\b.*\bregression\b)"
REMEDIATION_LABELS = ["hotfix", "regression", "revert", "incident", "rollback"]
