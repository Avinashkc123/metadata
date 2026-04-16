from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


CONTROLLED_STEP_MAPPING: dict[str, str] = {
    "email is sent": "sendEmail",
    "submission id is generated": "getSubmissionId",
    "Kafka stage": "validateKafka",
    "Kafka JSON stage": "validateKafkaJson",
    "UI operation": "uiIntake",
}

CONFIG_TEMPLATE: dict[str, Any] = {
    "region": "NA",
    "lob": "RC",
    "goldDataPath": "./resources/testData/emails/BMQ",
    "documentId": "energy_siccode",
    "configFile": "./resources/testData/emails/BMQ/config.json",
    "prodCode": "8901",
    "country": "US",
    "lobCode": "30",
    "compareFields": [
        "Policy Type",
        "SIC Code",
        "Triage Color",
        "Customer Group",
        "System",
        "Status",
        "Decline Reason\\(optional\\)",
        "Product",
        "Service Branch",
    ],
}

MAILBOX_BY_REGION_LOB: dict[tuple[str, str], str] = {
    ("NA", "RC"): "NA_INGESTION_RC",
    ("NA", "LMM_CONSTRUCTION"): "NA_INGESTION_LMM_CONSTRUCTION",
    ("NA", "RC_CISA"): "NA_INGESTION_RC_CISA",
}

VALID_KAFKA_STAGES = {"100", "500", "800"}
VALID_KAFKA_JSON_STAGES = {"1200", "1700", "2200"}
VALID_UI_STAGES = {"1700", "2200"}
STAGE_TIMEOUTS_MS: dict[str, int] = {
    "100": 300000,
    "500": 300000,
    "800": 300000,
    "1200": 300000,
    "1700": 900000,
    "2200": 900000,
}
GOLDEN_FILE_BY_STAGE: dict[str, str] = {
    "1200": "1200.json",
    "1700": "1700.json",
    "2200": "2200.json",
}


def parse_feature_file(feature_path: str | Path) -> dict[str, Any]:
    path = Path(feature_path)
    if not path.exists():
        raise ValueError(f"Feature file not found: {path}")

    feature_name: str | None = None
    background_steps: list[str] = []
    compare_fields: list[str] = []
    collecting_compare_table = False
    scenarios: list[dict[str, Any]] = []
    current_section: str | None = None
    current_scenario: dict[str, Any] | None = None

    compare_header_re = re.compile(
        r"^(Given|When|Then|And)\s+compare fields are:?\s*$", re.IGNORECASE
    )
    table_row_re = re.compile(r"^\|\s*(.+?)\s*\|$")

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("Feature:"):
            feature_name = line.split(":", 1)[1].strip()
            current_section = "feature"
            continue

        if line.startswith("Background:"):
            current_section = "background"
            continue

        if line.startswith("Scenario:"):
            collecting_compare_table = False
            scenario_name = line.split(":", 1)[1].strip()
            current_scenario = {"name": scenario_name, "steps": []}
            scenarios.append(current_scenario)
            current_section = "scenario"
            continue

        if current_section == "background":
            if collecting_compare_table:
                table_match = table_row_re.match(line)
                if table_match:
                    cell = table_match.group(1).strip()
                    if cell:
                        compare_fields.append(cell)
                    continue
                collecting_compare_table = False

            if compare_header_re.match(line):
                collecting_compare_table = True
                continue

            step_match = re.match(r"^(Given|When|Then|And)\s+(.+)$", line)
            if step_match:
                background_steps.append(step_match.group(2).strip())
                continue

            raise ValueError(f"Unsupported Gherkin line in Background: {line}")

        step_match = re.match(r"^(Given|When|Then|And)\s+(.+)$", line)
        if step_match:
            step_text = step_match.group(2).strip()
            if current_section == "scenario" and current_scenario is not None:
                current_scenario["steps"].append(step_text)
            else:
                raise ValueError(f"Step found outside Background/Scenario: {line}")
            continue

        raise ValueError(f"Unsupported Gherkin line: {line}")

    if not feature_name:
        raise ValueError("Feature name is missing in .feature file")
    if not scenarios:
        raise ValueError("At least one Scenario is required")

    return {
        "feature": feature_name,
        "background_steps": background_steps,
        "compare_fields": compare_fields if compare_fields else None,
        "scenarios": scenarios,
    }


def map_step_to_action(step_text: str) -> tuple[str, dict[str, Any]]:
    email_match = re.match(r'^email is sent with file\s+"([^"]+)"$', step_text)
    if email_match:
        return CONTROLLED_STEP_MAPPING["email is sent"], {"file": email_match.group(1)}

    if step_text == "submission id is generated":
        return CONTROLLED_STEP_MAPPING["submission id is generated"], {}

    kafka_match = re.match(r'^Kafka stage\s+"?(\d+)"?\s+should be validated$', step_text)
    if kafka_match:
        return CONTROLLED_STEP_MAPPING["Kafka stage"], {"stage": kafka_match.group(1)}

    kafka_json_match = re.match(
        r'^Kafka JSON stage\s+"?(\d+)"?\s+should be validated$', step_text
    )
    if kafka_json_match:
        return CONTROLLED_STEP_MAPPING["Kafka JSON stage"], {
            "stage": kafka_json_match.group(1)
        }

    ui_match = re.match(
        r'^UI operation\s+"([^"]+)"(?:\s+at stage\s+"?(\d+)"?)?(?:\s+is performed)?$',
        step_text,
    )
    if ui_match:
        operation = ui_match.group(1)
        stage = ui_match.group(2)
        if not stage:
            stage = "2200" if operation == "submitForReview" else "1700"
        return CONTROLLED_STEP_MAPPING["UI operation"], {
            "operation": operation,
            "stage": stage,
        }

    raise ValueError(f"Unknown step (no controlled mapping): {step_text}")


def build_config(
    background_steps: list[str],
    compare_fields_override: list[str] | None = None,
) -> dict[str, Any]:
    config = dict(CONFIG_TEMPLATE)
    kv_pattern = re.compile(r"^(.+?)\s+is\s+(.+)$")

    for step in background_steps:
        match = kv_pattern.match(step)
        if not match:
            raise ValueError(f"Unsupported background step format: {step}")

        raw_key = re.sub(r"\s+", " ", match.group(1).strip()).lower()
        raw_value = match.group(2).strip().strip('"').strip("'")

        key_alias_map = {
            "region": "region",
            "lob": "lob",
            "golddatapath": "goldDataPath",
            "test data path": "goldDataPath",
            "documentid": "documentId",
            "document id": "documentId",
            "configfile": "configFile",
            "config file": "configFile",
            "prodcode": "prodCode",
            "product code": "prodCode",
            "country": "country",
            "lobcode": "lobCode",
            "lob code": "lobCode",
        }
        if raw_key not in key_alias_map:
            raise ValueError(f"Unsupported background config key: {raw_key!r}")

        config[key_alias_map[raw_key]] = raw_value

    if compare_fields_override:
        config["compareFields"] = list(compare_fields_override)

    region = str(config["region"]).strip()
    lob = str(config["lob"]).strip()
    if (region, lob) not in MAILBOX_BY_REGION_LOB:
        raise ValueError(
            f"No controlled mailbox mapping for region/lob: {region}/{lob}"
        )

    return config


def build_steps(
    scenario_steps: list[str], config: dict[str, Any]
) -> list[dict[str, Any]]:
    built_steps: list[dict[str, Any]] = []
    mailbox = MAILBOX_BY_REGION_LOB[(str(config["region"]), str(config["lob"]))]

    for step_text in scenario_steps:
        action, payload = map_step_to_action(step_text)

        if action == "sendEmail":
            built_steps.append(
                {
                    "action": "sendEmail",
                    "input": {
                        "file": payload["file"],
                        "lob": str(config["lob"]),
                        "mailbox": mailbox,
                    },
                    "output": "emailSubject",
                }
            )
        elif action == "getSubmissionId":
            built_steps.append(
                {"action": "getSubmissionId", "input": {}, "output": "submissionId"}
            )
        elif action == "validateKafka":
            stage = str(payload["stage"])
            if stage not in VALID_KAFKA_STAGES:
                raise ValueError(
                    f"Invalid Kafka stage for validateKafka: {stage}. "
                    f"Allowed: {sorted(VALID_KAFKA_STAGES)}"
                )
            built_steps.append(
                {
                    "action": "validateKafka",
                    "stage": stage,
                    "input": {"stage": stage, "timeoutMs": STAGE_TIMEOUTS_MS[stage]},
                    "output": f"kafkaMsg_{stage}",
                }
            )
        elif action == "validateKafkaJson":
            stage = str(payload["stage"])
            if stage not in VALID_KAFKA_JSON_STAGES:
                raise ValueError(
                    f"Invalid Kafka JSON stage for validateKafkaJson: {stage}. "
                    f"Allowed: {sorted(VALID_KAFKA_JSON_STAGES)}"
                )
            built_steps.append(
                {
                    "action": "validateKafkaJson",
                    "stage": stage,
                    "input": {
                        "stage": stage,
                        "timeoutMs": STAGE_TIMEOUTS_MS[stage],
                        "goldenFile": GOLDEN_FILE_BY_STAGE[stage],
                    },
                    "output": f"kafkaMsg_{stage}",
                }
            )
        elif action == "uiIntake":
            stage = str(payload["stage"])
            if stage not in VALID_UI_STAGES:
                raise ValueError(
                    f"Invalid stage for uiIntake: {stage}. "
                    f"Allowed: {sorted(VALID_UI_STAGES)}"
                )
            built_steps.append(
                {
                    "action": "uiIntake",
                    "stage": stage,
                    "input": {"operation": str(payload["operation"])},
                }
            )
        else:
            raise ValueError(f"Unhandled action from controlled mapping: {action}")

    _validate_required_steps_and_order(built_steps)
    return built_steps


def _validate_required_steps_and_order(steps: list[dict[str, Any]]) -> None:
    actions = [step.get("action") for step in steps]
    if "sendEmail" not in actions:
        raise ValueError("Missing required step: sendEmail")
    if "getSubmissionId" not in actions:
        raise ValueError("Missing required step: getSubmissionId")
    if len(steps) < 2:
        raise ValueError("Invalid sequence: at least two steps required")
    if steps[0].get("action") != "sendEmail":
        raise ValueError("Invalid sequence: sendEmail must be the first step")
    if steps[1].get("action") != "getSubmissionId":
        raise ValueError("Invalid sequence: getSubmissionId must be the second step")


def _slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_")
    return slug


def generate_metadata_json(
    feature_name: str,
    background_steps: list[str],
    scenario: dict[str, Any],
    total_scenarios: int,
    compare_fields: list[str] | None = None,
) -> dict[str, Any]:
    config = build_config(background_steps, compare_fields)
    steps = build_steps(scenario["steps"], config)

    feature_slug = _slugify(str(feature_name))
    if not feature_slug:
        raise ValueError("Could not derive workflow name from Feature")

    scenario_slug = _slugify(str(scenario.get("name", "")))
    if total_scenarios > 1:
        if not scenario_slug:
            raise ValueError("Could not derive workflow name from Scenario")
        workflow_name = f"{feature_slug}_{scenario_slug}"
    else:
        workflow_name = feature_slug

    return {
        "workflow": workflow_name,
        "executionMode": "full",
        "config": config,
        "steps": steps,
    }


def _validate_with_metadata_reader(metadata: dict[str, Any], project_root: Path) -> None:
    src_dir = project_root / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

    from metadata_reader import (  # pyright: ignore[reportMissingImports]  # pylint: disable=import-error
        validate_metadata,
    )

    validate_metadata(metadata)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert BDD (Gherkin) feature file into orchestration metadata JSON"
    )
    parser.add_argument("feature_file", help="Path to .feature file")
    parser.add_argument(
        "--output",
        help=(
            "Output JSON file path (single scenario) or directory/base filename "
            "(multiple scenarios)"
        ),
    )
    return parser


def _resolve_output_paths(
    output_arg: str | None, project_root: Path, scenarios: list[dict[str, Any]]
) -> list[Path]:
    scenario_count = len(scenarios)

    if output_arg is None:
        default_dir = project_root / "metadata"
        if scenario_count == 1:
            return [(default_dir / "generated_workflow.json").resolve()]
        return [
            (default_dir / f"generated_workflow_{i + 1}_{_slugify(s['name'])}.json").resolve()
            for i, s in enumerate(scenarios)
        ]

    output_path = Path(output_arg)
    if scenario_count == 1:
        return [output_path.resolve()]

    if output_path.suffix.lower() == ".json":
        base_dir = output_path.parent.resolve()
        base_stem = output_path.stem
        return [
            (base_dir / f"{base_stem}_{i + 1}_{_slugify(s['name'])}.json").resolve()
            for i, s in enumerate(scenarios)
        ]

    # Treat non-json output path as directory for multi-scenario output.
    output_dir = output_path.resolve()
    return [
        (output_dir / f"generated_workflow_{i + 1}_{_slugify(s['name'])}.json").resolve()
        for i, s in enumerate(scenarios)
    ]


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    feature_file = Path(args.feature_file).resolve()
    project_root = Path(__file__).resolve().parent

    try:
        parsed = parse_feature_file(feature_file)
        scenarios = parsed["scenarios"]
        output_paths = _resolve_output_paths(args.output, project_root, scenarios)

        if len(output_paths) != len(scenarios):
            raise ValueError("Internal error: output path count does not match scenarios")

        for scenario, output_file in zip(scenarios, output_paths, strict=True):
            metadata = generate_metadata_json(
                feature_name=parsed["feature"],
                background_steps=parsed["background_steps"],
                scenario=scenario,
                total_scenarios=len(scenarios),
                compare_fields=parsed.get("compare_fields"),
            )
            _validate_with_metadata_reader(metadata, project_root)

            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_file.write_text(json.dumps(metadata, indent=4), encoding="utf-8")

        print("[BDD PARSED]")
        print("[JSON GENERATED]")
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
