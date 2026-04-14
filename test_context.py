from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from context import Context

ctx = Context()

ctx.set("submissionId", "12345")

# print(ctx.get("submissionId"))

ctx.has("submissionId")

ctx.dump()
