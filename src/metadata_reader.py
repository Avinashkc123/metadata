from __future__ import annotations

import copy
import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

# Operational keys are always required. Documentation keys are optional; if
# present they are validated, if absent they are ignored.
ROOT_KEYS_OPERATIONAL: tuple[str, ...] = (
    "workflow",
    "executionMode",
    "config",
    "steps",
)
CONFIG_KEYS: frozenset[str] = frozenset(
    {
        "region",
        "lob",
        "goldDataPath",
        "documentId",
        "configFile",
        "prodCode",
        "country",
        "lobCode",
        "compareFields",
    }
)
STEP_REFERENCE_KEYS: frozenset[str] = frozenset(
    {
        "_overview",
        "available_actions",
        "mailbox_options",
        "uiIntake_operations",
        "stage_timeouts",
        "complexity_tiers",
    }
)
TEMPLATE_ACTIONS: frozenset[str] = frozenset(
    {
        "sendEmail",
        "getSubmissionId",
        "validateKafka",
        "validateKafkaJson",
        "uiIntake",
    }
)
MAILBOX_KEYS: frozenset[str] = frozenset(
    {
        "NA_INGESTION_RC",
        "NA_INGESTION_LMM_CONSTRUCTION",
        "NA_INGESTION_RC_CISA",
    }
)
UI_INTAKE_OPERATIONS: frozenset[str] = frozenset(
    {
        "openSDW",
        "searchSubmission",
        "assignReviewer",
        "loadGoldenData",
        "selectAllFields",
        "validateInsuredAndProducer",
        "validateProcessingTab",
        "validatePoliciesTab",
        "validateUwUa",
        "fillReviewerDetails",
        "submitForReview",
    }
)
STAGES_VALIDATE_KAFKA: frozenset[str] = frozenset({"100", "500", "800"})
STAGES_VALIDATE_KAFKA_JSON: frozenset[str] = frozenset({"1200", "1700", "2200"})
STAGES_UI_INTAKE: frozenset[str] = frozenset({"1700", "2200"})
STAGE_TO_GOLDEN_FILE: dict[str, str] = {
    "1200": "1200.json",
    "1700": "1700.json",
    "2200": "2200.json",
}
EXECUTION_MODES: frozenset[str] = frozenset({"full", "stage-only"})


def load_metadata(file_path: str | Path) -> Any:
    print("[DEBUG] loading metadata", file=sys.stderr, flush=True)
    path = Path(file_path)
    with path.open("r", encoding="utf-8") as f:
        metadata = json.load(f)
    print("[DEBUG] calling validation", file=sys.stderr, flush=True)
    validate_metadata(metadata)
    print("[DEBUG] calling normalization", file=sys.stderr, flush=True)
    metadata = normalize_metadata(metadata)
    return metadata


def check_placeholders(obj: Any) -> None:
    if isinstance(obj, dict):
        for v in obj.values():
            check_placeholders(v)
    elif isinstance(obj, list):
        for item in obj:
            check_placeholders(item)
    elif isinstance(obj, str):
        if "<" in obj and ">" in obj:
            raise Exception(
                f"metadata contains placeholder-like string: {obj!r}"
            )


def _must_be_str(value: Any, ctx: str, *, allow_empty: bool = False) -> None:
    if not isinstance(value, str):
        raise Exception(f"{ctx} must be a string")
    if not allow_empty and not value.strip():
        raise Exception(f"{ctx} must not be empty")


def _is_placeholder(value: str) -> bool:
    s = value.strip()
    return len(s) >= 2 and s.startswith("<") and s.endswith(">")


def _must_be_dict(value: Any, ctx: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise Exception(f"{ctx} must be a JSON object")
    return value


def _validate_template_root(metadata: dict[str, Any]) -> None:
    for key in ROOT_KEYS_OPERATIONAL:
        if key not in metadata:
            raise Exception(f"missing required key: {key!r}")
    if "_schema_version" in metadata:
        _must_be_str(metadata["_schema_version"], "_schema_version")
    if "_description" in metadata:
        _must_be_str(metadata["_description"], "_description")
    wf = metadata["workflow"]
    if not isinstance(wf, str):
        raise Exception("workflow must be a string")
    if not wf.strip():
        raise Exception("workflow must not be empty")
    if "<" in wf or ">" in wf:
        raise Exception("workflow must not contain '<' or '>'")
    print("[VALIDATION] workflow validated", file=sys.stderr, flush=True)
    em = metadata["executionMode"]
    if not isinstance(em, str):
        raise Exception("executionMode must be a string")
    if em not in EXECUTION_MODES:
        raise Exception(
            "executionMode must be 'full' or 'stage-only', "
            f"got {em!r}"
        )
    print("[VALIDATION] executionMode validated", file=sys.stderr, flush=True)
    if not isinstance(metadata["config"], dict):
        raise Exception("config must be a dictionary")
    print("[VALIDATION] config validated", file=sys.stderr, flush=True)
    steps = metadata["steps"]
    if not isinstance(steps, list) or not steps:
        raise Exception("steps must be a non-empty list")


def _validate_config_notes(notes: Any) -> None:
    ctx = "_config_notes"
    doc = _must_be_dict(notes, ctx)
    if frozenset(doc.keys()) != CONFIG_KEYS:
        raise Exception(
            f"{ctx} must contain exactly keys {sorted(CONFIG_KEYS)!r}, "
            f"got {sorted(doc.keys())!r}"
        )
    for key in CONFIG_KEYS:
        _must_be_str(doc[key], f"{ctx}.{key}", allow_empty=False)


def _validate_step_reference(ref: Any) -> None:
    ctx = "_step_reference"
    doc = _must_be_dict(ref, ctx)
    missing = STEP_REFERENCE_KEYS - frozenset(doc.keys())
    if missing:
        raise Exception(f"{ctx} missing keys: {sorted(missing)!r}")
    extra = frozenset(doc.keys()) - STEP_REFERENCE_KEYS
    if extra:
        raise Exception(f"{ctx} has unknown keys: {sorted(extra)!r}")
    _must_be_str(doc["_overview"], f"{ctx}._overview")
    for key in (
        "available_actions",
        "mailbox_options",
        "uiIntake_operations",
        "stage_timeouts",
        "complexity_tiers",
    ):
        _must_be_dict(doc[key], f"{ctx}.{key}")


def _validate_config_object(config: Any) -> None:
    ctx = "config"
    doc = _must_be_dict(config, ctx)
    missing = CONFIG_KEYS - frozenset(doc.keys())
    if missing:
        raise Exception(f"{ctx} missing keys: {sorted(missing)!r}")
    extra = frozenset(doc.keys()) - CONFIG_KEYS
    if extra:
        raise Exception(f"{ctx} has unknown keys: {sorted(extra)!r}")
    for key in CONFIG_KEYS:
        if key == "compareFields":
            continue
        _must_be_str(doc[key], f"{ctx}.{key}")
    cf = doc["compareFields"]
    if not isinstance(cf, list) or not cf:
        raise Exception(f"{ctx}.compareFields must be a non-empty array")
    for j, item in enumerate(cf):
        _must_be_str(item, f"{ctx}.compareFields[{j}]")


_STRICT_STEP_ACTIONS: frozenset[str] = frozenset(
    {"sendEmail", "getSubmissionId", "validateKafka", "validateKafkaJson", "uiIntake"}
)


def _step_action_label(step: Any) -> str:
    if not isinstance(step, dict):
        return "?"
    a = step.get("action")
    return a if isinstance(a, str) and a.strip() else "?"


def _step_msg(i: int, action_label: str, detail: str) -> str:
    return f"Step {i} ({action_label}): {detail}"


def _strict_validate_step_rules(
    i: int, step: dict[str, Any], action: str
) -> None:
    if action == "sendEmail":
        if "input" not in step or step["input"] is None:
            raise Exception(_step_msg(i, action, "missing input"))
        inp = step["input"]
        if not isinstance(inp, dict):
            raise Exception(_step_msg(i, action, "input must be a JSON object"))
        if "file" not in inp or inp["file"] is None:
            raise Exception(_step_msg(i, action, "missing input.file"))
        file_val = inp["file"]
        if not isinstance(file_val, str):
            raise Exception(_step_msg(i, action, "input.file must be a string"))
        if "<" in file_val or ">" in file_val:
            raise Exception(
                _step_msg(i, action, "input.file must not contain '<' or '>'")
            )
        if "mailbox" not in inp or inp["mailbox"] is None:
            raise Exception(_step_msg(i, action, "missing input.mailbox"))
    elif action == "validateKafka":
        if "stage" not in step or step["stage"] is None:
            raise Exception(_step_msg(i, action, "missing stage"))
        if "input" not in step or step["input"] is None:
            raise Exception(_step_msg(i, action, "missing input"))
        inp = step["input"]
        if not isinstance(inp, dict):
            raise Exception(_step_msg(i, action, "input must be a JSON object"))
        if "stage" not in inp or inp["stage"] is None:
            raise Exception(_step_msg(i, action, "missing input.stage"))
        if "timeoutMs" not in inp or inp["timeoutMs"] is None:
            raise Exception(_step_msg(i, action, "missing input.timeoutMs"))
    elif action == "validateKafkaJson":
        if "input" not in step or step["input"] is None:
            raise Exception(_step_msg(i, action, "missing input"))
        inp = step["input"]
        if not isinstance(inp, dict):
            raise Exception(_step_msg(i, action, "input must be a JSON object"))
        if "goldenFile" not in inp or inp["goldenFile"] is None:
            raise Exception(_step_msg(i, action, "missing input.goldenFile"))
    elif action == "uiIntake":
        if "input" not in step or step["input"] is None:
            raise Exception(_step_msg(i, action, "missing input"))
        inp = step["input"]
        if not isinstance(inp, dict):
            raise Exception(_step_msg(i, action, "input must be a JSON object"))
        if "operation" not in inp or inp["operation"] is None:
            raise Exception(_step_msg(i, action, "missing input.operation"))


def _validate_step_input(
    step: dict[str, Any],
    i: int,
    action: str,
    *,
    required_keys: frozenset[str],
) -> dict[str, Any]:
    if "input" not in step:
        raise Exception(_step_msg(i, action, "missing input"))
    inp = _must_be_dict(step["input"], _step_msg(i, action, "input"))
    missing = required_keys - frozenset(inp.keys())
    if missing:
        raise Exception(
            _step_msg(
                i,
                action,
                f"input missing keys: {sorted(missing)!r}",
            )
        )
    extra = frozenset(inp.keys()) - required_keys
    if extra:
        raise Exception(
            _step_msg(
                i,
                action,
                f"input has unknown keys: {sorted(extra)!r}",
            )
        )
    return inp


def _observed_step_action(step: Any) -> str:
    if not isinstance(step, dict):
        return f"not a JSON object (got {type(step).__name__})"
    if "action" not in step or step["action"] is None:
        keys = [repr(k) for k in step if k != "action"]
        hint = f"; other keys: {keys}" if keys else ""
        return f"missing or null 'action'{hint}"
    a = step["action"]
    if not isinstance(a, str):
        return f"'action' is not a string (got {type(a).__name__}: {a!r})"
    return repr(a)


def _validate_step_order(steps: list[Any]) -> None:
    if len(steps) < 2:
        raise Exception(
            "step order invalid: at least two steps required "
            "(first sendEmail, second getSubmissionId)"
        )
    first = steps[0]
    second = steps[1]
    if not isinstance(first, dict) or first.get("action") != "sendEmail":
        msg = (
            _step_msg(
                0,
                _step_action_label(first),
                "must be action 'sendEmail' (step order)",
            )
            + "; observed: "
            + _observed_step_action(first)
        )
        raise Exception(msg)
    if not isinstance(second, dict) or second.get("action") != "getSubmissionId":
        msg = (
            _step_msg(
                1,
                _step_action_label(second),
                "must be action 'getSubmissionId' (step order)",
            )
            + "; observed: "
            + _observed_step_action(second)
        )
        raise Exception(msg)


def _validate_steps(steps: list[Any], action_map: Mapping[str, Any]) -> None:
    _validate_step_order(steps)
    print("[VALIDATION] step order validated", file=sys.stderr, flush=True)
    for i, step in enumerate(steps):
        lbl = _step_action_label(step)
        if not isinstance(step, dict):
            raise Exception(_step_msg(i, lbl, "must be a JSON object"))
        if "action" not in step or step["action"] is None:
            raise Exception(_step_msg(i, lbl, "missing action"))
        action = step["action"]
        if not isinstance(action, str) or not action.strip():
            raise Exception(
                _step_msg(i, lbl, "action must be a non-empty string")
            )
        print(
            f"[VALIDATION] Step {i} action: {step['action']}",
            file=sys.stderr,
            flush=True,
        )
        if action not in action_map:
            raise Exception(
                _step_msg(
                    i,
                    action,
                    f"action {action!r} is not defined in action_map",
                )
            )
        if action in _STRICT_STEP_ACTIONS:
            _strict_validate_step_rules(i, step, action)
        if action not in TEMPLATE_ACTIONS:
            continue

        if action == "sendEmail":
            inp = _validate_step_input(
                step,
                i,
                action,
                required_keys=frozenset({"file", "lob", "mailbox"}),
            )
            _must_be_str(inp["file"], _step_msg(i, action, "input.file"))
            _must_be_str(inp["lob"], _step_msg(i, action, "input.lob"))
            _must_be_str(inp["mailbox"], _step_msg(i, action, "input.mailbox"))
            if (
                inp["mailbox"] not in MAILBOX_KEYS
                and not _is_placeholder(inp["mailbox"])
            ):
                raise Exception(
                    _step_msg(
                        i,
                        action,
                        "input.mailbox must be one of "
                        f"{sorted(MAILBOX_KEYS)!r} or a <PLACEHOLDER>, "
                        f"got {inp['mailbox']!r}",
                    )
                )
            if "output" not in step:
                raise Exception(_step_msg(i, action, "missing output"))
            _must_be_str(step["output"], _step_msg(i, action, "output"))
            if "stage" in step:
                raise Exception(_step_msg(i, action, "must not have 'stage'"))
        elif action == "getSubmissionId":
            if "input" in step and step["input"] is not None:
                _must_be_dict(step["input"], _step_msg(i, action, "input"))
            if "output" not in step:
                raise Exception(_step_msg(i, action, "missing output"))
            _must_be_str(step["output"], _step_msg(i, action, "output"))
            if "stage" in step:
                raise Exception(_step_msg(i, action, "must not have 'stage'"))
        elif action == "validateKafka":
            if "stage" not in step:
                raise Exception(_step_msg(i, action, "missing stage"))
            _must_be_str(step["stage"], _step_msg(i, action, "stage"))
            if step["stage"] not in STAGES_VALIDATE_KAFKA:
                raise Exception(
                    _step_msg(
                        i,
                        action,
                        "stage must be one of "
                        f"{sorted(STAGES_VALIDATE_KAFKA)!r}, got {step['stage']!r}",
                    )
                )
            inp = _validate_step_input(
                step,
                i,
                action,
                required_keys=frozenset({"stage", "timeoutMs"}),
            )
            _must_be_str(inp["stage"], _step_msg(i, action, "input.stage"))
            if inp["stage"] != step["stage"]:
                raise Exception(
                    _step_msg(
                        i,
                        action,
                        "input.stage must match stage "
                        f"({step['stage']!r} vs {inp['stage']!r})",
                    )
                )
            if inp["stage"] not in STAGES_VALIDATE_KAFKA:
                raise Exception(
                    _step_msg(
                        i,
                        action,
                        "input.stage must be one of "
                        f"{sorted(STAGES_VALIDATE_KAFKA)!r}",
                    )
                )
            if not isinstance(inp["timeoutMs"], int):
                raise Exception(
                    _step_msg(i, action, "input.timeoutMs must be an integer")
                )
            if "output" not in step:
                raise Exception(_step_msg(i, action, "missing output"))
            _must_be_str(step["output"], _step_msg(i, action, "output"))
        elif action == "validateKafkaJson":
            if "stage" not in step:
                raise Exception(_step_msg(i, action, "missing stage"))
            _must_be_str(step["stage"], _step_msg(i, action, "stage"))
            if step["stage"] not in STAGES_VALIDATE_KAFKA_JSON:
                raise Exception(
                    _step_msg(
                        i,
                        action,
                        "stage must be one of "
                        f"{sorted(STAGES_VALIDATE_KAFKA_JSON)!r}, "
                        f"got {step['stage']!r}",
                    )
                )
            inp = _validate_step_input(
                step,
                i,
                action,
                required_keys=frozenset({"stage", "timeoutMs", "goldenFile"}),
            )
            _must_be_str(inp["stage"], _step_msg(i, action, "input.stage"))
            if inp["stage"] != step["stage"]:
                raise Exception(
                    _step_msg(
                        i,
                        action,
                        "input.stage must match stage "
                        f"({step['stage']!r} vs {inp['stage']!r})",
                    )
                )
            if not isinstance(inp["timeoutMs"], int):
                raise Exception(
                    _step_msg(i, action, "input.timeoutMs must be an integer")
                )
            _must_be_str(inp["goldenFile"], _step_msg(i, action, "input.goldenFile"))
            expected = STAGE_TO_GOLDEN_FILE.get(step["stage"])
            if expected and inp["goldenFile"] != expected:
                raise Exception(
                    _step_msg(
                        i,
                        action,
                        "input.goldenFile for stage "
                        f"{step['stage']!r} must be {expected!r}, "
                        f"got {inp['goldenFile']!r}",
                    )
                )
            if "output" not in step:
                raise Exception(_step_msg(i, action, "missing output"))
            _must_be_str(step["output"], _step_msg(i, action, "output"))
        elif action == "uiIntake":
            if "stage" not in step:
                raise Exception(_step_msg(i, action, "missing stage"))
            _must_be_str(step["stage"], _step_msg(i, action, "stage"))
            if step["stage"] not in STAGES_UI_INTAKE:
                raise Exception(
                    _step_msg(
                        i,
                        action,
                        "stage must be one of "
                        f"{sorted(STAGES_UI_INTAKE)!r}, got {step['stage']!r}",
                    )
                )
            inp = _validate_step_input(
                step,
                i,
                action,
                required_keys=frozenset({"operation"}),
            )
            op = inp["operation"]
            _must_be_str(op, _step_msg(i, action, "input.operation"))
            if op not in UI_INTAKE_OPERATIONS:
                raise Exception(
                    _step_msg(
                        i,
                        action,
                        "input.operation must be one of "
                        f"{sorted(UI_INTAKE_OPERATIONS)!r}, got {op!r}",
                    )
                )
            if "output" in step and step["output"] is not None:
                _must_be_str(step["output"], _step_msg(i, action, "output"))

    print("[VALIDATION] steps validated", file=sys.stderr, flush=True)


def validate_metadata(
    metadata: Any, action_map: Mapping[str, Any] | None = None
) -> None:
    if action_map is None:
        action_map = dict.fromkeys(TEMPLATE_ACTIONS, True)
    print("[DEBUG] Entering validation", file=sys.stderr, flush=True)
    if not isinstance(metadata, dict):
        raise Exception("metadata must be a JSON object")
    print("[VALIDATION] placeholder check running", file=sys.stderr, flush=True)
    check_placeholders(metadata)
    print("[VALIDATION] placeholders validated", file=sys.stderr, flush=True)
    _validate_template_root(metadata)
    if "_config_notes" in metadata:
        _validate_config_notes(metadata["_config_notes"])
        print("[VALIDATION] config notes validated", file=sys.stderr, flush=True)
    if "_step_reference" in metadata:
        _validate_step_reference(metadata["_step_reference"])
        print("[VALIDATION] step reference validated", file=sys.stderr, flush=True)
    _validate_config_object(metadata["config"])
    print("[VALIDATION] config object validated", file=sys.stderr, flush=True)
    _validate_steps(metadata["steps"], action_map)


def normalize_metadata(metadata: Any) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        raise Exception("metadata must be a JSON object")
    out = copy.deepcopy(metadata)
    if "input" not in out or out["input"] is None:
        out["input"] = {}
    steps = out.get("steps")
    if isinstance(steps, list):
        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                continue
            if "input" not in step or step["input"] is None:
                step["input"] = {}
            if "id" not in step:
                step["id"] = i
    return out


def clean_metadata(metadata: Any) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        raise Exception("metadata must be a JSON object")
    allowed_top = ("workflow", "executionMode", "config", "steps")
    out: dict[str, Any] = {}
    for k in allowed_top:
        if k in metadata:
            out[k] = copy.deepcopy(metadata[k])
    steps = out.get("steps")
    if isinstance(steps, list):
        for step in steps:
            if not isinstance(step, dict):
                continue
            step.pop("_note", None)
    print("[CLEAN] execution metadata prepared (non-execution fields removed)")
    return out
