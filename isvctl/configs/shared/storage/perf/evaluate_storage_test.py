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

The run's last test-phase step. It reads the run step's result and the
machine-readable results document (results.json) that storage-scale-test's extractor writes
with --to-json, and reports the headline numbers. It passes when the run step finished the
sweep with state SUCCESS, every execution's results parsed completely with write and read
rates, and the floors are met. Whether the sweep released its cluster resources is checked
on the teardown step's own output.

Headline rates are elbencho's [first] figures, the ones storage-scale-test's own reports use.

The floors (--min-mbps, --min-iops) default to 0, meaning no floor, as does an empty value:
that floor test then only reports the measured numbers. A provider sets them to its own expectations. Bandwidth is judged on the slower of the best
write and best read results, IOPS likewise.

Usage:
    python evaluate_storage_test.py [--scale-test-base /path/to/storage-scale-test] \\
        --run '{"success": true, "state": "SUCCESS", "results_dir": "...", ...}'

Output JSON:
{
    "success": true,
    "platform": "storage",
    "test_name": "evaluate_storage_test",
    "headline": {
        "write": {"samples": 2, "best_mbps": 180.2, "best_iops": 171.0, "min_avg_latency_ms": 5.1},
        "read": {"samples": 2, "best_mbps": 240.5, "best_iops": 229.0, "min_avg_latency_ms": 4.2}
    },
    "tests": {
        "sweep_completed": {"passed": true, "message": "SUCCESS, 2/2 executions"},
        "results_present": {"passed": true, "message": "2 executions complete: write 180.2 MB/s, ..."},
        "bandwidth_meets_min": {"passed": true, "message": "180.2 MB/s >= floor 100.0 MB/s (write ..., read ...)"},
        "iops_meets_min": {"passed": true, "message": "no floor set (perf_min_iops); 171.0 IOPS measured (write ..., read ...)"}
    }
}
A failed test has "passed": false and an "error" saying why; a failed run adds a top-level "error".
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
TERMINAL_OK = "SUCCESS"
RESULTS_JSON = "results.json"
RESULTS_SCHEMA = "storage-scale-test/filesystem-batch-results"
# Each floor's unit as shown, and the setting that sets it (isvctl/configs/shared/storage/perf/README.md).
FLOORS = {"mbps": ("MB/s", "perf_min_mbps"), "iops": ("IOPS", "perf_min_iops")}
# The extractor's default limit: EXTRACT_S_PER_CELL per cell of the sweep, at least
# EXTRACT_FLOOR_S. It takes about 1 s per cell, plus setting up its Python environment the
# first time it runs.
EXTRACT_FLOOR_S = 1800
EXTRACT_S_PER_CELL = 2


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the step's arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--scale-test-base",
        default="",
        help="storage-scale-test tree, for its extractor (default: $STORAGE_SCALE_TEST_BASE, else the vendored release)",
    )
    parser.add_argument("--run", default="{}", help="The run_storage_test step's JSON output")
    parser.add_argument(
        "--min-mbps", type=_floor, default=0.0, help="Bandwidth floor in MB/s (0 or empty = none, report only)"
    )
    parser.add_argument("--min-iops", type=_floor, default=0.0, help="IOPS floor (0 or empty = none, report only)")
    parser.add_argument(
        "--extract-timeout",
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


def _extract_timeout(extract_timeout: int, run: dict[str, Any]) -> int:
    """The extractor's time limit: *extract_timeout* when set, else sized from the sweep's cells."""
    if extract_timeout > 0:
        return extract_timeout
    total = (run.get("executions") or {}).get("total")
    cells = total if isinstance(total, int) else 0
    return max(EXTRACT_FLOOR_S, EXTRACT_S_PER_CELL * cells)


def _demo_result(min_mbps: float, min_iops: float) -> dict[str, Any]:
    """Return the demo-mode (ISVCTL_DEMO_MODE=1) dummy-success result, judged like a real one."""
    run = {"success": True, "state": TERMINAL_OK, "executions": {"total": 2, "succeeded": 2, "failed": 0}}
    best = {
        "write": {"samples": 2, "best_mbps": 180.0, "best_iops": 170.0, "min_avg_latency_ms": 5.0},
        "read": {"samples": 2, "best_mbps": 240.0, "best_iops": 230.0, "min_avg_latency_ms": 4.0},
    }
    return _result(run, best, 2, None, min_mbps, min_iops)


def _number(value: Any) -> float:
    """A document number as a float; null, malformed or non-finite (inf, NaN) reads as 0."""
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _load_results(base: Path, sweep_dir: str, timeout: int) -> tuple[dict[str, Any] | None, str | None]:
    """Run storage-scale-test's extractor with --to-json and read the document it writes.

    Returns (document, error). The document is judged on its contents, not the exit
    status, as storage-scale-test documents; a stale document is removed first so a
    failed extraction cannot be read as this run's results.
    """
    document = Path(sweep_dir) / RESULTS_JSON
    document.unlink(missing_ok=True)
    # The live-sample summary is the slow part of extraction, and nothing here reads it.
    command = [str(base / sst.EXTRACTOR), sweep_dir, "--to-json", "--live-detail", "none"]
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


def _best(document: dict[str, Any] | None) -> dict[str, Any]:
    """Best observed bandwidth and IOPS, and lowest average latency, per operation, unrounded.

    The floors and the positivity check compare these; only the headline is rounded.
    """
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
            "best_mbps": max(_number(r.get("bytes_per_s")) for r in rates) / 1e6,
            "best_iops": max(_number(r.get("ops_per_s")) for r in rates),
            "min_avg_latency_ms": min(latencies) if latencies else None,
        }
    return out


def _headline(best: dict[str, Any]) -> dict[str, Any]:
    """_best's numbers as reported: MB/s and IOPS to 0.1, latency to 0.001 ms."""
    return {
        op: {
            "samples": n["samples"],
            "best_mbps": round(n["best_mbps"], 1),
            "best_iops": round(n["best_iops"], 1),
            "min_avg_latency_ms": None if n["min_avg_latency_ms"] is None else round(n["min_avg_latency_ms"], 3),
        }
        for op, n in best.items()
    }


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


def _per_operation(headline: dict[str, Any], unit: str) -> str:
    """Each operation's best rate in *unit* (mbps or iops), e.g. ``write 180.2 MB/s, read 240.5 MB/s``."""
    label = FLOORS[unit][0]
    return ", ".join(f"{op} {numbers[f'best_{unit}']} {label}" for op, numbers in headline.items())


def _floor_test(measured: float | None, floor: float, unit: str, headline: dict[str, Any]) -> dict[str, Any]:
    """Compare the slower operation's best rate with its floor.

    With no measurement it fails rather than passing vacuously; with no floor (0) it only
    reports what was measured.
    """
    if measured is None:
        return {"passed": False, "error": "no measurement to compare"}
    label, setting = FLOORS[unit]
    if floor == 0:
        message = f"no floor set ({setting}); {round(measured, 1)} {label} measured ({_per_operation(headline, unit)})"
        return {"passed": True, "message": message}
    passed = measured >= floor
    # Shown to 0.1, or to as many digits as it takes to agree with the verdict (99.96 < 100).
    shown = next((round(measured, d) for d in range(1, 7) if (round(measured, d) >= floor) == passed), measured)
    if not passed:
        return {"passed": False, "error": f"{shown} {label} < floor {floor} {label}"}
    return {"passed": True, "message": f"{shown} {label} >= floor {floor} {label} ({_per_operation(headline, unit)})"}


def _results_error(document: dict[str, Any] | None, best: dict[str, Any], report_error: str | None) -> str | None:
    """Why the results are not complete, with write and read numbers; None when they are."""
    if report_error:
        return report_error
    if not _complete(document):
        incomplete = _incomplete_results(document)
        if incomplete:
            return "incomplete results: " + "; ".join(incomplete[:5])
        outcome = (document or {}).get("batch_outcome") or {}
        return f"batch_outcome.complete is not true: {outcome.get('executions')}, results {outcome.get('results')}"
    missing = [op for op in OPERATIONS if op not in best or best[op]["best_mbps"] <= 0]
    if missing:
        return f"no {' or '.join(missing)} results in {RESULTS_JSON}"
    # A null or non-finite IOPS reads as 0 (_number), so it is no measurement either.
    no_iops = [op for op in OPERATIONS if best[op]["best_iops"] <= 0]
    if no_iops:
        return f"no {' or '.join(no_iops)} IOPS in {RESULTS_JSON}"
    return None


def _sweep_completed(run: dict[str, Any]) -> dict[str, Any]:
    """Whether the run step finished the sweep with state SUCCESS, with its execution counts."""
    state = run.get("state") or "no state"
    executions = run.get("executions") or {}
    counts = f", {executions.get('succeeded')}/{executions.get('total')} executions" if executions else ""
    if run.get("success") is True and state == TERMINAL_OK:
        return {"passed": True, "message": f"{state}{counts}"}
    reason = run.get("error") or "the run step reported no result"
    return {"passed": False, "error": f"run step ended {state}{counts}: {reason}"}


def _result(
    run: dict[str, Any],
    best: dict[str, Any],
    executions: int,
    results_error: str | None,
    min_mbps: float,
    min_iops: float,
) -> dict[str, Any]:
    """Build the step result from the run step's output and the best numbers of *executions* executions."""
    headline = _headline(best)
    if results_error:
        results_present: dict[str, Any] = {"passed": False, "error": results_error}
        slowest_mbps = slowest_iops = None
    else:
        summary = "; ".join(
            f"{op} {n['best_mbps']} MB/s, {n['best_iops']} IOPS, {n['min_avg_latency_ms']} ms avg latency"
            for op, n in headline.items()
        )
        plural = "" if executions == 1 else "s"
        results_present = {"passed": True, "message": f"{executions} execution{plural} complete: {summary}"}
        slowest_mbps = min(best[op]["best_mbps"] for op in OPERATIONS)
        slowest_iops = min(best[op]["best_iops"] for op in OPERATIONS)
    tests = {
        "sweep_completed": _sweep_completed(run),
        "results_present": results_present,
        "bandwidth_meets_min": _floor_test(slowest_mbps, min_mbps, "mbps", headline),
        "iops_meets_min": _floor_test(slowest_iops, min_iops, "iops", headline),
    }
    result: dict[str, Any] = {
        "success": all(t["passed"] for t in tests.values()),
        "platform": "storage",
        "test_name": "evaluate_storage_test",
        "headline": headline,
        "tests": tests,
    }
    if not result["success"]:
        failed = ", ".join(name for name, test in tests.items() if not test["passed"])
        result["error"] = f"failed: {failed}; investigate {run.get('results_dir') or 'the run step log'}"
    return result


def _evaluate(
    run: dict[str, Any],
    document: dict[str, Any] | None,
    min_mbps: float,
    min_iops: float,
    report_error: str | None = None,
) -> dict[str, Any]:
    """Build the evaluation result from the run step's output and the results document."""
    best = _best(document)
    error = _results_error(document, best, report_error)
    return _result(run, best, len(_io_executions(document)), error, min_mbps, min_iops)


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
        timeout = _extract_timeout(args.extract_timeout, run)
        # The extractor holds nothing in the cluster, so an interrupt needs no teardown hint.
        restore = sst.install_interrupt_handler(hint=None)
        try:
            document, report_error = _load_results(
                sst.resolve_base(args.scale_test_base), str(run["results_dir"]), timeout
            )
        except OSError as exc:
            # An extractor that cannot run (ToolError names it and the likely cause), or an
            # unwritable directory.
            document, report_error = None, str(exc)
        finally:
            restore()
    return sst.emit(_evaluate(run, document, args.min_mbps, args.min_iops, report_error))


if __name__ == "__main__":
    sys.exit(main())
