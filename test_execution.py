from __future__ import annotations

import asyncio
import copy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from unittest.mock import MagicMock, patch

from context import Context
from executor import execute_workflow, resolve_input


def test_resolve_input_substitutes_context_strings() -> None:
    ctx = Context()
    ctx.set("submissionId", "SUB999")
    out = resolve_input(
        {"stage": "100", "ref": "$context.submissionId"},
        ctx,
    )
    assert out == {"stage": "100", "ref": "SUB999"}


def test_resolve_input_passthrough() -> None:
    ctx = Context()
    assert resolve_input({"a": 1, "b": "plain"}, ctx) == {"a": 1, "b": "plain"}


async def test_execute_workflow_minimal_happy_path() -> None:
    metadata: dict = {
        "workflow": "test",
        "executionMode": "full",
        "config": {},
        "steps": [
            {"action": "getSubmissionId", "input": {}},
            {
                "action": "validateKafka",
                "input": {"stage": "100", "timeoutMs": 300000},
            },
        ],
    }
    with patch("mock_utility.wait_for_event"):
        await execute_workflow(metadata)


async def test_execute_workflow_unknown_action_raises() -> None:
    metadata = {
        "steps": [
            {"action": "notARealAction", "input": {}},
        ],
    }
    try:
        await execute_workflow(metadata)
    except Exception as e:
        assert "Unknown action: notARealAction" in str(e)
    else:
        raise AssertionError("expected Exception")


async def test_execute_workflow_step_failure_raises() -> None:
    metadata = {
        "steps": [
            {"action": "sendEmail", "input": {}},
        ],
    }
    try:
        await execute_workflow(metadata)
    except Exception:
        pass
    else:
        raise AssertionError("expected Exception from sendEmail validation")


async def test_execute_workflow_does_not_mutate_metadata() -> None:
    metadata = {
        "steps": [
            {
                "action": "sendEmail",
                "input": {
                    "file": "f",
                    "mailbox": "NA_INGESTION_RC",
                    "extra": {"nested": 1},
                },
            },
        ],
    }
    snapshot = copy.deepcopy(metadata)
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"submissionId": "SUB-MOCK"}
    mock_resp.raise_for_status.return_value = None
    with patch("mock_utility.requests.post", return_value=mock_resp):
        await execute_workflow(metadata)
    assert metadata == snapshot


async def main() -> None:
    test_resolve_input_substitutes_context_strings()
    test_resolve_input_passthrough()
    print("resolve_input tests OK")

    await test_execute_workflow_minimal_happy_path()
    print("execute_workflow minimal happy path OK")

    await test_execute_workflow_unknown_action_raises()
    print("unknown action raises OK")

    await test_execute_workflow_step_failure_raises()
    print("step failure raises OK")

    await test_execute_workflow_does_not_mutate_metadata()
    print("metadata immutability OK")

    print("--- execute_workflow with dump_context=True ---")
    await execute_workflow(
        {
            "steps": [
                {"action": "getSubmissionId", "input": {}},
            ],
        },
        dump_context=True,
    )
    print("dump_context OK")

    print("ALL TESTS OK")


if __name__ == "__main__":
    asyncio.run(main())
