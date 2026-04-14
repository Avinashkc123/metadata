# Metadata Orchestrator — User Guide

This guide matches the **current Python implementation** in this repository. Entry point for running the full pipeline is **`src/main.py`** (not a `main.py` at the project root).

---

## 1. Overview

### What this system does

The system **loads a workflow definition from JSON**, **validates and normalizes** it, then **runs each step asynchronously** through a fixed set of **actions**. Steps can pass data forward using a **`Context`** (in-memory key/value store). Actions that need HTTP call **`mock_utility`** (`send_email_api`, `wait_for_event`, `validate_kafka_event`) against a backend that exposes **`POST /ingest`** and **`GET /events`**.

### High-level flow

1. **`src/main.py`** — Sets `sys.path`, optionally sets **`METADATA_API_BASE`**, resolves the workflow file path, then **`asyncio.run(execute_workflow(...))`** after **`load_metadata(...)`**.
2. **`metadata_reader.load_metadata`** — Reads UTF-8 JSON, **`validate_metadata`**, **`normalize_metadata`**, returns a dict ready to execute.
3. **`executor.execute_workflow`** — For each step: resolve **`$context.*`** in inputs, deep-copy the step, call the matching **`actions.action_map`** handler, optionally **`context.set(step["output"], result)`**.
4. **`actions`** — Async handlers (`sendEmail`, `getSubmissionId`, `validateKafka`, …) that read/write **`Context`** and call **`mock_utility`** where needed (ingest + Kafka poll).
5. **`mock_utility`** — Synchronous **`requests`** client: **`POST …/ingest`**, **`GET …/events`** (polling).
6. **`local_ingest_server.py`** — Optional **FastAPI** stub implementing those endpoints for local development.

```text
src/main.py  ->  metadata_reader.load_metadata  ->  executor.execute_workflow
  ->  actions.action_map  ->  mock_utility (HTTP)  ->  real API or local_ingest_server

Shared in memory: context.Context (one instance per run, inside execute_workflow).
```

---

## 2. Project Structure

| Path | Purpose |
|------|---------|
| **`src/main.py`** | CLI entry: env-based config, `load_metadata`, `execute_workflow`. |
| **`src/metadata_reader.py`** | `load_metadata`, `validate_metadata`, `normalize_metadata`, `clean_metadata`, `check_placeholders`; template/step validation rules. |
| **`src/executor.py`** | `execute_workflow`, `resolve_input` (`$context.` prefix). |
| **`src/context.py`** | `Context` — `set`, `get`, `has`, `update`, `dump`; forbidden keys for `set`. |
| **`src/actions.py`** | `action_map`, async action handlers; imports **`mock_utility`**; wraps errors with **`[ACTION ERROR]`**. |
| **`mock_utility.py`** | `send_email_api`, `wait_for_event`, `validate_kafka_event`; reads **`METADATA_API_BASE`**. |
| **`local_ingest_server.py`** | Dev server: **`POST /ingest`**, **`GET /events`**; port from **`METADATA_API_PORT`** (default **8010**). |
| **`input/`** | Workflow JSON files (e.g. **`energy_siccode_bmq.json`**). |
| **`requirements.txt`** | **`requests`**, **`fastapi`**, **`uvicorn[standard]`**. |

Other files (tests, `help.txt`, `orchestrator_run.txt`, etc.) are supporting material and are not required to understand the core engine.

---

## 3. Prerequisites

- **Python**: 3.10+ recommended (code uses `from __future__ import annotations` and standard library `asyncio`).
- **Libraries** (from **`requirements.txt`**):
  - **`requests`** — used by **`mock_utility`** and transitively required for **`send_email_api`** / **`wait_for_event`**.
  - **`fastapi`**, **`uvicorn[standard]`** — only needed to run **`local_ingest_server.py`** locally.

---

## 4. Setup Instructions

### Install dependencies

```bash
cd /path/to/metadata # project root containing src/ and requirements.txt
python -m venv .venv
# Windows: .venv\Scripts\activate
# Unix: source .venv/bin/activate
pip install -r requirements.txt
```

### Folder structure

- Workflow JSON files belong under **`input/`** by default (configurable via **`METADATA_INPUT_DIR`**).
- Run commands from the **project root** so paths like **`input/energy_siccode_bmq.json`** resolve correctly when using defaults.

### Where to place the metadata (workflow) file

- Default file: **`input/energy_siccode_bmq.json`** (see **`src/main.py`** — **`METADATA_WORKFLOW`** defaults to this filename under **`METADATA_INPUT_DIR`**).
- You may set **`METADATA_WORKFLOW`** to another filename under **`input/`**, or an **absolute path** to any JSON file that passes **`validate_metadata`**.

---

## 5. Configuration Guide

### `src/main.py` environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| **`METADATA_API_BASE`** | Built from port if unset: `http://127.0.0.1:{METADATA_API_PORT}` | Base URL for **`mock_utility`** (`/ingest`, `/events`). Set **before** imports resolve if you rely on defaults inside **`main.py`** (the script sets it before importing **`executor`**). |
| **`METADATA_API_PORT`** | `8010` | Used **only** when **`METADATA_API_BASE`** is not already set; builds default base URL. |
| **`METADATA_INPUT_DIR`** | `<project>/input` | Directory for workflow JSON when **`METADATA_WORKFLOW`** is relative. |
| **`METADATA_WORKFLOW`** | `energy_siccode_bmq.json` | Workflow filename under input dir, or absolute path. |
| **`METADATA_DUMP_CONTEXT`** | `true` | If `1` / `true` / `yes` / `on` (case-insensitive), **`execute_workflow(..., dump_context=True)`** prints **`[CONTEXT DUMP]`** at the end. |

### Required top-level keys in workflow JSON

Per **`metadata_reader`**: **`workflow`**, **`executionMode`**, **`config`**, **`steps`**.

- **`config`** must contain exactly the keys in **`CONFIG_KEYS`** (e.g. `region`, `lob`, `goldDataPath`, `documentId`, `configFile`, `prodCode`, `country`, `lobCode`, `compareFields`).
- **`executionMode`** must be one of: **`full`**, **`stage-only`**.

Optional documentation keys (validated only if present): **`_schema_version`**, **`_description`**, **`_config_notes`**, **`_step_reference`**.

### Step order rule (strict)

**`_validate_step_order`** requires:

- At least **two** steps.
- Step **0**: **`action`** must be **`sendEmail`**.
- Step **1**: **`action`** must be **`getSubmissionId`**.

Further rules (per-action **`input`**, **`output`**, **`stage`**, mailbox allow-list, etc.) are enforced in **`metadata_reader._validate_steps`** — use **`input/energy_siccode_bmq.json`** as a reference that passes validation.

### Example step (abbreviated)

```json
{
  "action": "validateKafka",
  "stage": "100",
  "input": {
    "stage": "100",
    "timeoutMs": 300000
  },
  "output": "kafkaMsg_100"
}
```

**`timeoutMs`** is **required** in the file for validation; the running **`validateKafka`** action does not pass it to **`mock_utility`** (poll timeout stays **15s**). See **External System / Mock Setup** below.

### Context usage: `$context.<key>`

In **`executor.resolve_input`**, any **string** value starting with **`$context.`** is replaced by **`context.get(<key>)`** where **`<key>`** is the substring after the prefix. Example: **`"$context.submissionId"`** → **`context.get("submissionId")`**.

---

## 6. External System / Mock Setup

### Role of `mock_utility.py`

- **`send_email_api(file_name)`** — **`POST {METADATA_API_BASE}/ingest`** with JSON body **`{"fileName": "<file_name>"}`**. Expects JSON response with non-empty **`submissionId`** (missing, **`null`**, or blank after **`str(...).strip()`** fails).
- **`wait_for_event(submission_id, stage, timeout=15)`** — Polls **`GET {METADATA_API_BASE}/events`** with query params **`submissionId`** and **`stage`**. On **HTTP 200** and JSON that satisfies **`_response_has_event`**, returns **`True`**. Otherwise (non-200, JSON parse failure, empty event payload, or **`requests.RequestException`**) it waits if time remains: prints **`[POLL] retrying...`**, sleeps **1** second, and loops until the deadline, then prints **`[POLL TIMEOUT]`** and **`raise Exception`**.
- **`validate_kafka_event(submission_id, stage)`** — Prints **`[VALIDATE] checking Kafka event`**, calls **`wait_for_event`** with the default **`timeout=15`** (seconds), returns **`True`**.

**Metadata vs runtime:** **`validateKafka`** and **`validateKafkaJson`** steps must include **`input.timeoutMs`** (integer) for **`metadata_reader`** validation. **`actions.validate_kafka`** does not read **`timeoutMs`** (polling always uses **`wait_for_event`**’s default **15** seconds unless **`mock_utility`** is changed). **`validate_kafka_json`** does not call **`mock_utility`** at all; **`timeoutMs`** is validation-only there as well.

**`_response_has_event(data)`** is **true** if **`data`** is a **`dict`** and any of:

- **`found` is `True`**
- **`events`** is a non-empty **`list`**
- **`event`** is not **`None`**

### `local_ingest_server.py` (dev stub)

- **`POST /ingest`** — Body: **`{"fileName": "..."}`** (Pydantic **`IngestBody`**). Response: **`{"submissionId": "SUB-..."}`**.
- **`GET /events`** — Query: **`submissionId`**, **`stage`**. Response includes **`"found": true`** so **`wait_for_event`** succeeds on first poll.

Run (from project root):

```bash
python local_ingest_server.py
```

Port: **`METADATA_API_PORT`** (default **8010**), host **127.0.0.1**.

Align clients by setting **`METADATA_API_BASE`** (e.g. `http://127.0.0.1:8010`) or rely on **`src/main.py`** defaults.

---

## 7. How to Run

### Command

From the **project root**:

```bash
python src/main.py
```

There is **no** `[MAIN]` log line in **`src/main.py`**; on failure it prints **`[ERROR] <message>`** to **stderr** and exits with code **1**.

### What happens internally (step-by-step)

1. **`src/main.py`** inserts **`src/`** and project root on **`sys.path`**, sets **`METADATA_API_BASE`** if missing, resolves **`WORKFLOW_PATH`**.
2. **`load_metadata(WORKFLOW_PATH)`** — read file → **`validate_metadata`** → **`normalize_metadata`** (stderr: **`[DEBUG]`**, **`[VALIDATION]`** lines).
3. **`execute_workflow(metadata, dump_context=...)`** — stdout: **`[EXECUTION]`**, **`[STEP n …]`**, action logs, **`[STEP TIME]`**, **`[EXECUTION COMPLETE]`**, optional **`[CONTEXT DUMP]`**.

---

## 8. Execution Flow Explained

### Step lifecycle (executor)

For each index **`idx`** in **`metadata["steps"]`**:

1. **`action_name = step["action"]`** — must exist in **`action_map`**.
2. **`[STEP {idx} START] {action_name}`**
3. **`resolved_input = resolve_input(step.get("input", {}), context)`** — deep resolution of dict/list/strings with **`$context.`**.
4. **`step_for_action = copy.deepcopy(step)`**; **`step_for_action["input"] = resolved_input`** (original **`metadata`** is not mutated).
5. Print JSON of **`action`** + resolved **`input`**.
6. **`result = await action_map[action_name](step_for_action, context)`** — timing logged as **`[STEP TIME]`**.
7. If **`"output"` in step`**: **`result` must not be `None`**; then **`context.set(step["output"], result)`**.
8. **`[STEP {idx} END]`** — or **`[STEP {idx} FAILED]`** and re-raise.

### Context flow

- **`Context`** is created once per **`execute_workflow`**.
- Actions call **`context.set`** / **`context.get`** (e.g. **`send_email`** sets **`submissionId`** from API; **`get_submission_id`** overwrites with stub **`SUB12345`**).
- **`output`** keys in JSON are **strings** used as context keys when storing the action return value.

### Input resolution

- **`$context.<key>`** only; other values pass through unchanged.
- **`context.get`** raises if the key is missing — resolution then fails with a wrapped **`Step … input resolution failed`** message.

---

## 9. Logs Explanation

| Prefix | Where | Meaning |
|--------|--------|---------|
| **`[DEBUG]`** | **stderr**, `metadata_reader` / `load_metadata` | Load/validation/normalize phases. |
| **`[VALIDATION]`** | **stderr**, `metadata_reader` | Template, placeholders, config, steps. |
| **`[EXECUTION]`** | **stdout**, `executor` | Workflow start / complete. |
| **`[STEP n START]`** / **`END`** / **`FAILED`** | **stdout**, `executor` | Per-step boundaries; **`FAILED`** includes error. |
| **`[STEP TIME]`** | **stdout**, `executor` | Wall time for the async action call. |
| **`[ACTION]`** | **stdout**, `actions` | Handler start / **`ui_intake`** details. |
| **`[ACTION VALIDATION]`** | **stdout**, `actions` | Before input checks. |
| **`[ACTION -> API]`** | **stdout**, `send_email` | Before **`send_email_api`**. |
| **`[ACTION -> POLL]`** | **stdout**, `validate_kafka` | Before **`validate_kafka_event`**. |
| **`[ACTION ERROR]`** | **stdout**, `actions` decorator | Logged then exception re-raised. |
| **`[CONTEXT SET]`** / **`GET`** / **`HAS`** / **`DUMP`** | **stdout**, `context` | Context operations. |
| **`[CONTEXT WRITE]`** | **stdout**, `get_submission_id` | Before **`context.set`** for dummy id. |
| **`[CONTEXT READ]`** | **stdout**, `validate_kafka` / `ui_intake` | Before **`context.get("submissionId")`**. |
| **`[RESOLVE]`** | **stdout**, `executor` | **`$context.`** substitution applied. |
| **`[API]`** / **`[API REQUEST]`** / **`[API RESPONSE]`** | **stdout**, `mock_utility.send_email_api` | Ingest HTTP. |
| **`[VALIDATE]`** | **stdout**, `validate_kafka` (stage line) and **`mock_utility.validate_kafka_event`** | Kafka validation line. |
| **`[POLL]`** / **`[POLL SUCCESS]`** / **`[POLL TIMEOUT]`** / **`[POLL] retrying...`** | **stdout**, `mock_utility.wait_for_event` | Event polling. |
| **`[ERROR]`** | **stderr**, `src/main.py` | Top-level failure. |

**Note:** **`[MAIN]`** is **not** printed by **`src/main.py`** in the current code.

---

## 10. Common Errors & Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| **`Unknown action: …`** | **`step["action"]`** not in **`action_map`** keys (`sendEmail`, `getSubmissionId`, `validateKafka`, `validateKafkaJson`, `uiIntake`). |
| **`Invalid context key: system metadata cannot be stored`** | **`context.set`** used with **`_step_reference`**, **`steps`**, or **`config`**. |
| **`Context key not found: …`** | **`context.get`** or **`$context.…`** resolution before the key was set. |
| **`Event poll timed out after …`** | **`wait_for_event`** — no successful **`GET /events`** response with event-shaped JSON before **`timeout`** (default **15s**). |
| **`Ingest request failed: …`** | **`send_email_api`** — connection error, HTTP error from **`raise_for_status`**, etc. |
| **`Ingest response missing submissionId`** | **`POST /ingest`** body JSON lacks usable **`submissionId`**. |
| **`metadata contains placeholder-like string: …`** (from **`check_placeholders`**) | Any **string** value anywhere in the workflow JSON that contains **both** **`<`** and **`>`** fails validation (including values like **`<MAILBOX>`**). This runs on the whole tree before per-step rules (for example **`sendEmail`** mailbox allow-list). |
| **`step order invalid`** / **`must be action 'sendEmail'`** | First step not **`sendEmail`**, or second not **`getSubmissionId`**, or fewer than two steps. |
| **`Step … input resolution failed`** | Exception during **`resolve_input`** (often missing context key for **`$context.`**). |
| **`… did not return value for output`** | Step has **`output`** but action returned **`None`**. |
| Import errors for **`mock_utility`** | Run from project root; **`actions.py`** adds project root to **`sys.path`** before importing **`mock_utility`**. |

---

## 11. How to Extend

### Add a new action (runtime)

1. Implement an **`async def my_handler(step: dict, context: Context) -> Any`** in **`src/actions.py`** (follow existing patterns; use **`@_log_action_errors`**).
2. Register **`"jsonActionName": my_handler`** in **`action_map`** (JSON **`action`** strings are **camelCase**, e.g. **`sendEmail`**).
3. Update **`metadata_reader.TEMPLATE_ACTIONS`** (and any **`_validate_steps`** / **`_strict_validate_step_rules`** branches) if **`validate_metadata`** should accept the new action — otherwise **`load_metadata`** will reject workflows using it.
4. If the handler calls HTTP, add functions in **`mock_utility.py`** or reuse existing ones; keep **`METADATA_API_BASE`** in mind.

### Add a new step in metadata

Add an object to **`steps`** with **`action`**, required **`input`** / **`stage`** / **`output`** per **`metadata_reader`** rules. **Do not** violate **`sendEmail` → `getSubmissionId`** ordering for steps0 and 1.

### Modify mock utilities

Edit **`mock_utility.py`** only — **`actions.py`** imports **`send_email_api`** and **`validate_kafka_event`** by name. After changes, ensure **`local_ingest_server.py`** (or your real API) still matches request/response shapes expected by **`send_email_api`** and **`wait_for_event`**.

---

## 12. Quick Start

1. **`pip install -r requirements.txt`**
2. **Terminal A:** `python local_ingest_server.py` (listen on **8010** by default).
3. **Terminal B:** from project root, `python src/main.py` (uses **`input/energy_siccode_bmq.json`** by default and **`dump_context`** on success).

Optional: `METADATA_WORKFLOW=my.json` or `METADATA_DUMP_CONTEXT=false` for variations.

---

*This guide was generated to reflect the repository’s Python modules as of the documented behavior above.*
