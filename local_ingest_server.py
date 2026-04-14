"""Local dev API matching metadata clients: POST /ingest (JSON), GET /events.

Default port is 8010 (avoids clashing with other apps on 8000). Override:
  set METADATA_API_PORT=8000

Run (from this directory):
  python local_ingest_server.py

Or:
  python -m uvicorn local_ingest_server:app --host 127.0.0.1 --port 8010
"""

from __future__ import annotations

import os
import uuid

from fastapi import FastAPI, Query
from pydantic import BaseModel

app = FastAPI(title="metadata-local-ingest-stub")


class IngestBody(BaseModel):
    fileName: str


@app.post("/ingest")
def ingest(body: IngestBody) -> dict[str, str]:
    submission_id = f"SUB-{uuid.uuid4().hex[:12].upper()}"
    return {"submissionId": submission_id}


@app.get("/events")
def events(
    submission_id: str = Query(..., alias="submissionId"),
    stage: str = Query(...),
) -> dict[str, object]:
    """Minimal poll response so mock_utility.wait_for_event succeeds on first try."""
    return {
        "found": True,
        "submissionId": submission_id,
        "stage": stage,
    }


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("METADATA_API_PORT", "8010"))
    uvicorn.run(app, host="127.0.0.1", port=port)
