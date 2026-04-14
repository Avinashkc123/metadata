from __future__ import annotations

import copy
import json
import time
from typing import Any

from actions import action_map
from context import Context

_CONTEXT_PREFIX = "$context."


def resolve_input(input_data: Any, context: Context) -> Any:
    if isinstance(input_data, dict):
        return {k: resolve_input(v, context) for k, v in input_data.items()}
    if isinstance(input_data, list):
        return [resolve_input(item, context) for item in input_data]
    if isinstance(input_data, str) and input_data.startswith(_CONTEXT_PREFIX):
        key = input_data[len(_CONTEXT_PREFIX):]
        value = context.get(key)
        print(f"[RESOLVE] {key}={value}")
        return value
    return input_data


async def execute_workflow(
    metadata: dict[str, Any], *, dump_context: bool = False
) -> None:
    print("[EXECUTION] starting workflow")
    context = Context()
    steps = metadata.get("steps")
    if not isinstance(steps, list):
        raise Exception("Invalid metadata: steps must be list")
    for idx, step in enumerate(steps):
        action_name = step["action"]
        try:
            if action_name not in action_map:
                raise Exception(f"Unknown action: {action_name}")
            print(f"[STEP {idx} START] {action_name}")
            try:
                resolved_input = resolve_input(step.get("input", {}), context)
            except Exception as e:
                raise Exception(
                    f"Step {idx} ({action_name}) input resolution failed: {e}"
                )
            # Prevent mutation of original metadata
            step_for_action = copy.deepcopy(step)
            step_for_action["input"] = resolved_input
            details = {"action": step_for_action.get("action"), "input": resolved_input}
            print(json.dumps(details, indent=2, default=str))
            start = time.time()
            result = await action_map[action_name](step_for_action, context)
            end = time.time()
            print(f"[STEP TIME] {action_name} took {end - start:.2f}s")
            if "output" in step:
                if result is None:
                    raise Exception(
                        f"{action_name} did not return value for output"
                    )
                context.set(step["output"], result)
            print(f"[STEP {idx} END] {action_name}")
        except Exception as e:
            print(f"[STEP {idx} FAILED] {action_name} - {e}")
            raise
    print("[EXECUTION COMPLETE]")
    if dump_context:
        context.dump()
