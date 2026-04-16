# `bdd_to_metadata.py` — quick help

Convert a Gherkin-style `.feature` (or `.feature.txt`) file into orchestration metadata JSON that matches `input/energy_siccode_bmq.json` shape and passes `src/metadata_reader.py` validation.

## Requirements

- Python 3 (stdlib only; no pip packages).
- Run from the **metadata project root** (the folder that contains `bdd_to_metadata.py`, `src/`, and `metadata_reader.py`), or use absolute paths to the feature file.

## Basic usage

```bash
python bdd_to_metadata.py path/to/workflow.feature
```

On success you will see:

```text
[BDD PARSED]
[JSON GENERATED]
```

Errors are printed as `[ERROR] ...` and the process exits with code `1`.

## Output location

| Situation | Default output |
|-----------|----------------|
| **One** scenario in the file | `metadata/generated_workflow.json` (under the project root) |
| **Several** scenarios | `metadata/generated_workflow_<n>_<ScenarioNameSlug>.json` |

Override with `--output` (see below).

## Custom output (`--output`)

**Single scenario**

```bash
python bdd_to_metadata.py input/my_flow.feature --output input/custom.json
```

Writes exactly that file (path is resolved from your current working directory unless you pass an absolute path).

**Multiple scenarios**

- `--output myrun.json` → files like `myrun_1_ScenarioSlug.json`, `myrun_2_OtherSlug.json` in the same directory as `myrun.json`.
- `--output out_dir` (no `.json` suffix) → files under `out_dir/` named `generated_workflow_<n>_<ScenarioSlug>.json`.

## What the feature file must contain

- **`Feature:`** — used to build `workflow` (non-alphanumeric characters become `_`).
- **`Background:`** (optional but typical) — `Given` / `And` lines become `config` overrides.
- **`Scenario:`** — one or more; each produces one JSON file when there are multiple scenarios.
- **Steps** — only patterns supported by the tool (unknown text → error).

### Required scenario step order

Every scenario must include, in order:

1. First step → **`sendEmail`** (e.g. `Given email is sent with file "…"`).
2. Second step → **`getSubmissionId`** (e.g. `When submission id is generated` or `And submission id is generated`).

Additional steps (Kafka, UI, etc.) follow in file order.

### Supported scenario step patterns (controlled mapping)

| Gherkin pattern (examples) | JSON `action` |
|----------------------------|----------------|
| `email is sent with file "…"` | `sendEmail` |
| `submission id is generated` | `getSubmissionId` |
| `Kafka stage "100" should be validated` | `validateKafka` |
| `Kafka JSON stage "1700" should be validated` | `validateKafkaJson` |
| `UI operation "openSDW" is performed` | `uiIntake` |
| `UI operation "…" at stage "2200"` | `uiIntake` (explicit stage) |

Stages must be values allowed by `metadata_reader.py` (e.g. Kafka: `100`, `500`, `800`; JSON stages: `1200`, `1700`, `2200`).  
If a UI step omits `at stage`, the default is **1700**, except **`submitForReview`** which defaults to **2200**.

### Background → `config`

Supported keys (after `Given` / `And`, form `… is …` or `… is "…"`):

- `region`, `LOB` / `lob`
- `product code`, `country`, `LOB code`
- `test data path` → `goldDataPath`
- `document id`, `config file`

**Compare fields table** — after a line like `And compare fields are:`, pipe rows add to `config.compareFields`:

```gherkin
And compare fields are:
  | Policy Type |
  | SIC Code |
```

`region` + `LOB` must map to a known mailbox pairing (same rules as the generator code).

## Validation

Before writing any file, the script calls `validate_metadata()` from `src/metadata_reader.py`. If validation fails, nothing useful is persisted for that scenario and you get `[ERROR] …`.

## Examples

```bash
cd C:\Users\AvinashChandrashekar\.cursor\projects\metadata

python bdd_to_metadata.py input\SICCode_Proc_NonBMQFlow.feature.txt --output input\custom.json
```

```bash
python bdd_to_metadata.py --help
```

## Related files

| File | Role |
|------|------|
| `src/metadata_reader.py` | Schema and step validation |
| `input/energy_siccode_bmq.json` | Reference metadata shape |
| `bdd_to_metadata.py` | Converter CLI |
