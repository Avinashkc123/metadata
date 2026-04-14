from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from unittest.mock import MagicMock, patch

from actions import action_map
from context import Context


def _ingest_mock_response(submission_id: str = "SUB-FROM-API") -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"submissionId": submission_id}
    mock_resp.raise_for_status.return_value = None
    return mock_resp


async def main() -> None:
    ctx = Context()

    with patch(
        "mock_utility.requests.post",
        return_value=_ingest_mock_response(),
    ), patch("mock_utility.wait_for_event"):
        r1 = await action_map["sendEmail"](
            {
                "action": "sendEmail",
                "input": {"file": "test-email", "mailbox": "NA_INGESTION_RC"},
            },
            ctx,
        )
        assert r1["dummy"] == "send_email"
        assert r1["submissionId"] == "SUB-FROM-API"

        sid = await action_map["getSubmissionId"](
            {"action": "getSubmissionId"},
            ctx,
        )
        assert sid == "SUB12345"

        r2 = await action_map["validateKafka"](
            {
                "action": "validateKafka",
                "input": {"stage": "100", "timeoutMs": 300000},
            },
            ctx,
        )
        assert r2 is True

        r3 = await action_map["validateKafkaJson"](
            {
                "action": "validateKafkaJson",
                "input": {
                    "stage": "1200",
                    "timeoutMs": 300000,
                    "goldenFile": "1200.json",
                },
            },
            ctx,
        )
        assert r3["dummy"] == "validate_kafka_json"

        r4 = await action_map["uiIntake"](
            {
                "action": "uiIntake",
                "input": {"operation": "openSDW"},
            },
            ctx,
        )
        assert r4["submissionId"] == "SUB12345"

    print("--- error path (expect [ACTION ERROR] + re-raise) ---")
    try:
        await action_map["sendEmail"](
            {"action": "sendEmail", "input": {}},
            ctx,
        )
    except Exception as e:
        print("caught:", type(e).__name__, "-", e)

    print("ALL TESTS OK")


if __name__ == "__main__":
    asyncio.run(main())
