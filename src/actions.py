from __future__ import annotations

import asyncio
import functools
import sys
from pathlib import Path
from typing import Any

from context import Context

_METADATA_ROOT = Path(__file__).resolve().parent.parent
if str(_METADATA_ROOT) not in sys.path:
    sys.path.insert(0, str(_METADATA_ROOT))

from mock_utility import send_email_api, validate_kafka_event


def _log_action_errors(fn):
    @functools.wraps(fn)
    async def wrapper(step: dict[str, Any], context: Context) -> Any:
        try:
            return await fn(step, context)
        except Exception as e:
            print(f"[ACTION ERROR] {e}")
            raise

    return wrapper


def _input_dict(step: dict[str, Any], action: str) -> dict[str, Any]:
    if "input" not in step or step["input"] is None:
        raise Exception(f"Step {action}: missing required input input")
    inp = step["input"]
    if not isinstance(inp, dict):
        raise Exception(f"Step {action}: missing required input input")
    return inp


def _require_key(inp: dict[str, Any], action: str, field: str) -> None:
    if field not in inp or inp[field] is None:
        raise Exception(f"Step {action}: missing required input {field}")


@_log_action_errors
async def send_email(step: dict[str, Any], context: Context) -> Any:
    print("[ACTION] send_email started")
    print("[ACTION VALIDATION] checking inputs")
    if not isinstance(step, dict):
        raise Exception("Step send_email: missing required input input")
    inp = _input_dict(step, "send_email")
    _require_key(inp, "send_email", "file")
    _require_key(inp, "send_email", "mailbox")
    print("[ACTION -> API]")
    submission_id = await asyncio.to_thread(send_email_api, inp["file"])
    context.set("submissionId", submission_id)
    return {"dummy": "send_email", "input": inp, "submissionId": submission_id}


@_log_action_errors
async def get_submission_id(step: dict[str, Any], context: Context) -> Any:
    print("[ACTION] get_submission_id started")
    print("[ACTION VALIDATION] checking inputs")
    value = "SUB12345"
    print(f"[CONTEXT WRITE] submissionId={value}")
    context.set("submissionId", value)
    return value


@_log_action_errors
async def validate_kafka(step: dict[str, Any], context: Context) -> Any:
    print("[ACTION] validate_kafka started")
    print("[ACTION VALIDATION] checking inputs")
    if not isinstance(step, dict):
        raise Exception("Step validate_kafka: missing required input input")
    inp = _input_dict(step, "validate_kafka")
    _require_key(inp, "validate_kafka", "stage")
    print("[CONTEXT READ] submissionId")
    submission_id = context.get("submissionId")
    stage = inp["stage"]
    print(f"[VALIDATE] stage={stage} for submissionId={submission_id}")
    print("[ACTION -> POLL]")
    await asyncio.to_thread(validate_kafka_event, submission_id, stage)
    return True


@_log_action_errors
async def validate_kafka_json(step: dict[str, Any], context: Context) -> Any:
    print("[ACTION] validate_kafka_json started")
    print("[ACTION VALIDATION] checking inputs")
    if not isinstance(step, dict):
        raise Exception("Step validate_kafka_json: missing required input input")
    inp = _input_dict(step, "validate_kafka_json")
    _require_key(inp, "validate_kafka_json", "goldenFile")
    return {"dummy": "validate_kafka_json", "input": inp}


@_log_action_errors
async def ui_intake(step: dict[str, Any], context: Context) -> Any:
    print("[ACTION] ui_intake started")
    print("[ACTION VALIDATION] checking inputs")
    if not isinstance(step, dict):
        raise Exception("Step ui_intake: missing required input input")
    inp = _input_dict(step, "ui_intake")
    _require_key(inp, "ui_intake", "operation")
    print("[CONTEXT READ] submissionId")
    submission_id = context.get("submissionId")
    print(
        f"[ACTION] ui_intake operation={inp['operation']!r} "
        f"submissionId={submission_id!r}"
    )
    return {
        "dummy": "ui_intake",
        "input": inp,
        "submissionId": submission_id,
    }


action_map = {
    "sendEmail": send_email,
    "getSubmissionId": get_submission_id,
    "validateKafka": validate_kafka,
    "validateKafkaJson": validate_kafka_json,
    "uiIntake": ui_intake,
}
