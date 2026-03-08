# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is an iwoca Accelerate interview challenge runner. It uses a TDL (Test-Driven Learning) framework where a remote challenge server sends requests and validates responses. The primary challenge is **IWC** — a priority queue system for dispatching third-party data-fetching tasks.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
PYTHONPATH=lib python -m pytest -q test/solution_tests/

# Run only IWC queue tests
PYTHONPATH=lib python -m pytest -q test/solution_tests/IWC/

# Run a single test
PYTHONPATH=lib python -m pytest -q test/solution_tests/IWC/test_queue_solution.py::test_enqueue_size_dequeue_flow

# Run the challenge runner (requires config/credentials.config)
PYTHONPATH=lib python lib/send_command_to_server.py

# Get coverage for a challenge
bash get_coverage_for_challenge.sh IWC
```

## Architecture

### Runner Framework

`lib/send_command_to_server.py` is the main entry point. It connects to a remote TDL challenge server via message queues (configured in `config/credentials.config`). The server sends method calls, which are routed through:

1. **`lib/entry_point_mapping.py`** — Maps server method names (e.g., `"enqueue"`, `"dequeue"`) to solution class methods. For IWC, it converts raw dicts to `TaskSubmission` dataclasses on input and `TaskDispatch` back to dicts on output.
2. **`lib/solutions/<CHALLENGE_ID>/`** — Each challenge has a 3-letter directory containing solution code.

### IWC Queue Challenge (`lib/solutions/IWC/`)

The focus area. Three files:

- **`task_types.py`** — `TaskSubmission` (provider, user_id, timestamp, metadata) and `TaskDispatch` (provider, user_id) dataclasses.
- **`queue_solution_legacy.py`** — The `Queue` class with all prioritization logic. This is where implementation work happens.
- **`queue_solution_entrypoint.py`** — Thin facade delegating to `Queue`. Exposes `enqueue`, `dequeue`, `size`, `age`, `purge`.

### Queue Rules (cumulative across rounds)

The queue implements these prioritization rules, applied via a sort key in `_sort_key()`:

1. **Rule of 3** — Users with 3+ tasks get HIGH priority, moving all their tasks to the front.
2. **Timestamp Ordering** — Equal-priority tasks are ordered oldest-first.
3. **Dependency Resolution** — Enqueueing `credit_check` auto-adds `companies_house` (its dependency).
4. **Task Deduplication** — Same (user_id, provider) pair can only exist once; duplicates keep the earlier timestamp.
5. **Bank Statements Deferral** — `bank_statements` tasks have `execution_order=2`, pushing them after other providers.
6. **Time-Sensitive Bank Statements** — If a `bank_statements` task's age exceeds 300 seconds (5 min), it can be promoted. FIFO breaks ties.

### Providers (defined in `queue_solution_legacy.py`)

| Provider | Dependencies | Execution Order |
|---|---|---|
| `companies_house` | none | 1 (default) |
| `credit_check` | `companies_house` | 1 (default) |
| `bank_statements` | none | 2 (deferred) |
| `id_verification` | none | 1 (default) |

### Test Utilities (`test/solution_tests/IWC/utils.py`)

Tests use a declarative DSL: `run_queue()` takes a list of action dicts built by `call_enqueue()`, `call_dequeue()`, `call_size()`, `call_age()`. Each action has an `.expect()` call specifying the expected return value. Timestamps are generated via `iso_ts(delta_minutes=N)` relative to a fixed base datetime.

### Challenge Descriptions

Round-by-round requirements are in `challenges/IWC_R1.txt` through `IWC_R5.txt`.

## Key Details

- Python 3.12 required
- `PYTHONPATH=lib` is required for all commands — the project uses bare module imports (e.g., `from solutions.IWC.task_types import ...`)
- `metadata` dict on `TaskSubmission` is used internally for mutable priority/ordering state — not part of the external API
- `dequeue()` returns a `TaskDispatch` (just provider + user_id), not the full `TaskSubmission`
- The `Queue._queue` is a plain list, re-sorted on each `dequeue()` call
