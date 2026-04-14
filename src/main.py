"""Run full workflow from JSON. Configure with environment variables:

  METADATA_API_BASE   HTTP base for ingest/events (default from METADATA_API_PORT).
  METADATA_API_PORT   Used only if METADATA_API_BASE is unset (default 8010).
  METADATA_INPUT_DIR Directory for workflow JSON (default <project>/input).
  METADATA_WORKFLOW   Filename under INPUT_DIR or absolute path (default energy_siccode_bmq.json).
  METADATA_DUMP_CONTEXT  If true/1/yes/on, print context dump after success (default true).
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
for _dir in (SRC, ROOT):
    if str(_dir) not in sys.path:
        sys.path.insert(0, str(_dir))

if "METADATA_API_BASE" not in os.environ:
    _port = os.environ.get("METADATA_API_PORT", "8010").strip()
    os.environ["METADATA_API_BASE"] = f"http://127.0.0.1:{_port}"

INPUT_DIR = Path(
    os.environ.get("METADATA_INPUT_DIR", str(ROOT / "input"))
).resolve()

_workflow = os.environ.get("METADATA_WORKFLOW", "energy_siccode_bmq.json").strip()
_workflow_path = Path(_workflow)
WORKFLOW_PATH = (
    _workflow_path.resolve()
    if _workflow_path.is_absolute()
    else (INPUT_DIR / _workflow_path).resolve()
)

DUMP_CONTEXT = os.environ.get("METADATA_DUMP_CONTEXT", "true").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

from executor import execute_workflow
from metadata_reader import load_metadata


async def _run_workflow() -> None:
    metadata = load_metadata(WORKFLOW_PATH)
    await execute_workflow(metadata, dump_context=DUMP_CONTEXT)


def main() -> None:
    try:
        asyncio.run(_run_workflow())
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
