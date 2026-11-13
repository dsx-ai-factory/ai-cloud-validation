#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Evaluate a storage-scale-test sweep: did it produce real, complete numbers?

The final stage of the storage performance run. It reads the run step's result and the
machine-readable results document (results.json) that storage-scale-test's extractor writes
with --to-json, and reports the headline numbers. It passes when the sweep finished, every
cell succeeded, its cluster resources were released, and every execution's results parsed
completely with write and read rates.

Headline rates are elbencho's [first] figures, the ones storage-scale-test's own reports use.

The floors (--min-mbps, --min-iops) default to 0, which only reports the numbers, as does an
empty value. A partner sets them to its own expectations. Bandwidth is judged on the slower of the best write and
best read results, IOPS likewise.

Usage:
    python evaluate_storage_test.py --scale-test-base /path/to/storage-scale-test \\
        --run '{"success": true, "state": "SUCCESS", "results_dir": "...", ...}'

Output JSON:
{
    "success": true,
    "platform": "storage",
    "test_name": "evaluate_storage_test",
    "headline": {"write": {"best_mbps": 180.2, "best_iops": 171.0, "min_avg_latency_ms": 5.1}, "read": {...}},
    "tests": {
        "sweep_complete":      {"passed": true},
        "resources_released":  {"passed": true},
        "results_present":     {"passed": true},
        "bandwidth_meets_min": {"passed": true, "measured_mbps": 180.2, "min_mbps": 0},
        "iops_meets_min":      {"passed": true, "measured_iops": 171.0, "min_iops": 0}
    }
}
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))  # shared/storage/perf/ (for sst)

import sst

OPERATIONS = ("write", "read")
RESULTS_JSON = "results.json"
RESULTS_SCHEMA = "storage-scale-test/filesystem-batch-results"
# The extractor's default limit: EXTRACT_S_PER_CELL per cell of the sweep, at least
# EXTRACT_FLOOR_S. It takes about 1 s per cell, plus setting up its Python environment the
# first time it runs.
EXTRACT_FLOOR_S = 1800
EXTRACT_S_PER_CELL = 2


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the step's arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scale-test-base", required=True, help="storage-scale-test checkout (for its extractor)")
    parser.add_argument("--run", default="{}", help="The run_storage_test step's JSON output")
    parser.add_argument(
        "--min-mbps", type=_floor, default=0.0, help="Bandwidth floor in MB/s (0 or empty = report only)"
    )
    parser.add_argument("--min-iops", type=_floor, default=0.0, help="IOPS floor (0 or empty = report only)")
    parser.add_argument(
        "--command-timeout",
        type=int,
        default=0,
        help=f"Seconds for the report extractor (0 = {EXTRACT_S_PER_CELL} per cell, at least {EXTRACT_FLOOR_S})",
    )
    return parser.parse_args(argv)


def _floor(value: str) -> float:
    """A floor argument: a finite number, or empty for none (0)."""
    if not value.strip():
        return 0.0
    try:
        floor = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value!r} is not a number") from None
    if not math.isfinite(floor):
        raise argparse.ArgumentTypeError(f"{value!r} is not a finite number")
    return floor


def extract_timeout(command_timeout: int, run: dict[str, Any]) -> int:
    """The extractor's time limit: *command_timeout* when set, else sized from the sweep's cells."""
    if command_timeout > 0:
        return command_timeout
    total = (run.get("executions") or {}).get("total")
    cells = total if isinstance(total, int) else 0
    return max(EXTRACT_FLOOR_S, EXTRACT_S_PER_CELL * cells)


def _demo_result(min_mbps: float, min_iops: float) -> dict[str, Any]:
    """Dummy success for ISVCTL_DEMO_MODE=1."""
    headline = {
        "write": {"samples": 2, "best_mbps": 180.0, "best_iops": 170.0, "min_avg_latency_ms": 5.0},
        "read": {"samples": 2, "best_mbps": 240.0, "best_iops": 230.0, "min_avg_latency_ms": 4.0},
    }
    return {
        "success": True,
        "platform": "storage",
        "test_name": "evaluate_storage_test",
        "headline": headline,
        "tests": {
            "sweep_complete": {"passed": True},
            "resources_released": {"passed": True},
            "results_present": {"passed": True},
            "bandwidth_meets_min": {"passed": True, "measured_mbps": 180.0, "min_mbps": min_mbps},
            "iops_meets_min": {"passed": True, "measured_iops": 170.0, "min_iops": min_iops},
        },
    }


def _number(value: Any) -> float:
    """A document number as a float; null, malformed or non-finite (inf, NaN) reads as 0."""
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _load_results(base: Path, results_dir: str, timeout: int) -> tuple[dict[str, Any] | None, str | None]:
    """Run storage-scale-test's extractor with --to-json and read the document it writes.

    Returns (document, error). The document is judged on its contents, not the exit
    status, as storage-scale-test documents; a stale document is removed first so a
    failed extraction cannot be read as this run's results.
    """
    document = Path(results_dir) / RESULTS_JSON
    document.unlink(missing_ok=True)
    command = [str(base / sst.EXTRACTOR), results_dir, "--to-json"]
    proc = sst.run(command, timeout)
    if "unrecognized arguments: --to-json" in proc.stderr:
        return None, "this storage-scale-test has no --to-json; v2.1.0 or later is required"
    if proc.timed_out:
        # The results are all on disk, so the report does not need the cluster or this run.
        return None, f"extractor timed out after {timeout}s; rerun it offline: {' '.join(command)}"
    if proc.returncode != 0:
        sst.log(f"extractor exited {proc.returncode}: {proc.stderr.strip()[-500:]}")
    if not document.is_file():
        return None, f"extractor exited {proc.returncode} without writing {document}"
    try:
        loaded = json.loads(document.read_text(encoding="utf-8"))
    except ValueError as exc:
        return None, f"{document} is not JSON: {exc}"
    if not isinstance(loaded, dict) or loaded.get("schema") != RESULTS_SCHEMA:
        return None, f"{document} is not a {RESULTS_SCHEMA} document"
    if not str(loaded.get("schema_version", "")).startswith("1."):
        return None, f"{document} has unsupported schema_version {loaded.get('schema_version')}"
    return loaded, None


def _io_executions(document: dict[str, Any] | None) -> list[dict[str, Any]]:
    """The document's IO executions (the sweep runs no metadata cells)."""
    return [e for e in _executions(document) if e.get("kind") == "io"]


def _executions(document: dict[str, Any] | None) -> list[dict[str, Any]]:
    """The document's executions, skipping any entry that is not an object."""
    return [e for e in (document or {}).get("executions") or [] if isinstance(e, dict)]


def _headline(document: dict[str, Any] | None) -> dict[str, Any]:
    """Best observed bandwidth and IOPS, and lowest average latency, per operation."""
    phases = [p for e in _io_executions(document) for p in (e.get("results") or {}).get("phases") or []]
    out: dict[str, Any] = {}
    for op in OPERATIONS:
        subset = [p for p in phases if p.get("phase") == op]
        if not subset:
            continue
        rates = [p.get("elbencho_first") or {} for p in subset]
        latencies = [_number((p.get("latency_us") or {}).get("avg")) / 1000 for p in subset]
        latencies = [lat for lat in latencies if lat > 0]
        out[op] = {
            "samples": len(subset),
            "best_mbps": round(max(_number(r.get("bytes_per_s")) for r in rates) / 1e6, 1),
            "best_iops": round(max(_number(r.get("ops_per_s")) for r in rates), 1),
            "min_avg_latency_ms": round(min(latencies), 3) if latencies else None,
        }
    return out


def _incomplete_results(document: dict[str, Any] | None) -> list[str]:
    """Why ``batch_outcome.complete`` is false: each execution that did not succeed, or whose
    results did not parse completely, with storage-scale-test's problem codes.
    """
    found = []
    for execution in _executions(document):
        ledger, results = execution.get("ledger_state"), execution.get("result_state")
        if ledger != "success" or results != "complete":
            codes = ", ".join(sorted({str(p.get("code") or "?") for p in execution.get("problems") or []}))
            found.append(
                f"{execution.get('execution_id') or execution.get('coordinate_key')}: "
                f"ledger {ledger}, results {results}" + (f" ({codes})" if codes else "")
            )
    return found


def _complete(document: dict[str, Any] | None) -> bool:
    """storage-scale-test's own verdict: every execution succeeded and its results are complete."""
    return ((document or {}).get("batch_outcome") or {}).get("complete") is True


def _floor_test(measured: float | None, floor: float, unit: str) -> dict[str, Any]:
    """A floor comparison; with no measurement it fails rather than passing vacuously."""
    result: dict[str, Any] = {f"measured_{unit}": measured, f"min_{unit}": floor}
    if measured is None:
        return {**result, "passed": False, "error": "no measurement to compare"}
    passed = measured >= floor
    label = {"mbps": "MB/s", "iops": "IOPS"}.get(unit, unit)
    verdict = f"{'>=' if passed else '<'} floor {floor} {label}"
    message = "report only (no floor set)" if floor == 0 else f"{measured} {label} {verdict}"
    return {**result, "passed": passed, "message": message}


def _results_message(document: dict[str, Any] | None, incomplete: list[str], report_error: str | None) -> str:
    """Why results_present passed or failed."""
    if report_error:
        return report_error
    if incomplete:
        return "incomplete results: " + "; ".join(incomplete[:5])
    if not _complete(document):
        outcome = (document or {}).get("batch_outcome") or {}
        return f"batch_outcome.complete is not true: {outcome.get('executions')}, results {outcome.get('results')}"
    return f"{len(_io_executions(document))} executions in {RESULTS_JSON}"


def evaluate(
    run: dict[str, Any],
    document: dict[str, Any] | None,
    min_mbps: float,
    min_iops: float,
    report_error: str | None = None,
) -> dict[str, Any]:
    """Build the evaluation result from the run step's output and the results document."""
    executions = run.get("executions") if isinstance(run.get("executions"), dict) else {}
    total = executions.get("total")
    complete = run.get("state") == "SUCCESS" and executions.get("failed") == 0 and isinstance(total, int) and total > 0
    numbers = _headline(document)
    incomplete = _incomplete_results(document)
    present = (
        _complete(document)
        and not incomplete
        and all(op in numbers and numbers[op]["best_mbps"] > 0 for op in OPERATIONS)
    )
    slowest_mbps = min(numbers[op]["best_mbps"] for op in OPERATIONS) if present else None
    slowest_iops = min(numbers[op]["best_iops"] for op in OPERATIONS) if present else None
    tests = {
        "sweep_complete": {"passed": complete, "message": f"state {run.get('state')}, executions {executions}"},
        "resources_released": {"passed": run.get("resources_released") is True},
        "results_present": {"passed": present, "message": _results_message(document, incomplete, report_error)},
        "bandwidth_meets_min": _floor_test(slowest_mbps, min_mbps, "mbps"),
        "iops_meets_min": _floor_test(slowest_iops, min_iops, "iops"),
    }
    for name, test in tests.items():
        if not test["passed"] and "error" not in test:
            test["error"] = test.get("message") or f"{name} failed"
    result: dict[str, Any] = {
        "success": all(t["passed"] for t in tests.values()),
        "platform": "storage",
        "test_name": "evaluate_storage_test",
        "results_dir": run.get("results_dir"),
        "headline": numbers,
        "tests": tests,
    }
    if not result["success"]:
        failed = ", ".join(name for name, test in tests.items() if not test["passed"])
        result["error"] = f"failed: {failed}; investigate {run.get('results_dir') or 'the run step log'}"
    return result


def main(argv: list[str] | None = None) -> int:
    """Evaluate the sweep and print the step result."""
    args = _parse_args(argv)
    if sst.DEMO_MODE:
        return sst.emit(_demo_result(args.min_mbps, args.min_iops))
    failure = {"success": False, "platform": "storage", "test_name": "evaluate_storage_test"}
    try:
        run = json.loads(args.run or "{}")
    except ValueError as exc:
        return sst.emit({**failure, "error": f"--run is not JSON: {exc}"})
    if not isinstance(run, dict):
        return sst.emit({**failure, "error": f"--run is not a JSON object: {args.run}"})
    document, report_error = None, "the run step reported no results_dir"
    if run.get("results_dir"):
        timeout = extract_timeout(args.command_timeout, run)
        restore = sst.install_interrupt_handler()
        try:
            document, report_error = _load_results(Path(args.scale_test_base), str(run["results_dir"]), timeout)
        except OSError as exc:
            # An extractor that cannot run (ToolError names it and the likely cause), or an unwritable directory.
            document, report_error = None, str(exc)
        finally:
            restore()
    return sst.emit(evaluate(run, document, args.min_mbps, args.min_iops, report_error))


if __name__ == "__main__":
    sys.exit(main())
