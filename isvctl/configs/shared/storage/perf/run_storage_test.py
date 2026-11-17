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

"""Run a storage-scale-test IO sweep on Kubernetes to completion, then collect it.

One synchronous step: write env.sh plus a target file and a workload override file, submit
the sweep, wait for it to finish, and collect it. Collect runs whenever the sweep finished,
because it is what releases the claim's Lease, the coordinator Job and the worker DaemonSet.

The wait polls the sweep's status until it finishes or the deadline (--wait-timeout) passes:
auto (the default) sizes it from the sweep's cells and phase duration, or a number of seconds,
at most sst.WAIT_CAP_S either way. A poll that cannot read the status just waits for the next
one. At the deadline the run cancels the sweep, collects it and fails with TIMEOUT, unless the
sweep finished before the cancel reached it. Before submitting, it rejects (NOT_SUBMITTED) a
sweep with more cells than storage-scale-test can collect (sst.MAX_CELLS), or one that auto
cannot fit in sst.WAIT_CAP_S.

The step needs no isvctl timeout: every storage-scale-test call has its own limit (sst.py's
*_TIMEOUT_S), and a call that hits it is stopped with everything it started. Its progress
(the estimate, submit, each status check, cancel and collect) goes to a log under
<results-dir>/run-logs/, whose path the result reports as progress_log.

If the step stops before it collects, the teardown step (release_storage_test.py) releases the
sweep. Both steps hold the results root's lock while they run (REFUSED if another run or teardown
has it).

Prerequisites: the vendored storage-scale-test release, or a tree (v2.1.0+) used only by isvctl, the tools
it needs (bash 4.4+, kubectl, flock, GNU tar and timeout; isvctl's Python 3.12+), and an existing
Bound ReadWriteMany claim that the selected nodes can mount.

Usage:
    python run_storage_test.py --claim my-namespace/my-claim --node-selector storage-test=true \\
        --results-dir /path/to/results [--scale-test-base /path/to/storage-scale-test]

Output JSON:
{
    "success": true,
    "platform": "storage",
    "test_name": "run_storage_test",
    "state": "SUCCESS",
    "scale_test_version": "v2.1.0",
    "results_dir": "/path/to/results/elbencho-20260101Z000000",
    "executions": {"total": 4, "succeeded": 4, "failed": 0},
    "resources_released": true,
    "progress_log": "/path/to/results/run-logs/run-20260101T000000Z-1234.log"
}
A failed run adds "error", and "collect_error", "failed_execution" and "failed_log" when they
apply.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))  # shared/storage/perf/ (for sst)

import sst

TERMINAL_OK = "SUCCESS"
REFUSED = "REFUSED"
AUTO = "auto"
# The directory inside the claim the sweep writes under.
TEST_PATH = "scale-test"
# Submit fails in this phase while an earlier sweep still holds the claim's Lease.
CLAIM_LEASE_PHASE = "pvc-lease"
SETTINGS_HINT = (
    "storage-scale-test names its own variables (ELBENCHO_*); the perf_* settings set them "
    "(see docs/guides/storage-performance.md#settings)"
)
CLAIM_HELD_HINT = (
    "an earlier sweep holds the claim: the teardown step releases it if it is in this results root; rerun after that"
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the step's arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--scale-test-base",
        default="",
        help="storage-scale-test tree used only by isvctl (default: $STORAGE_SCALE_TEST_BASE, else the vendored release)",
    )
    parser.add_argument("--kubeconfig", default="", help="Kubeconfig for the cluster (default: kubectl's own)")
    parser.add_argument("--claim", default="", help="Existing claim to test, as namespace/claim")
    parser.add_argument("--node-selector", default="", help="Worker node labels, e.g. storage-test=true")
    parser.add_argument("--nodes", default="1", help="Node counts to sweep, storage-scale-test --nodes syntax")
    # No WRITE,READ io-size pairs: this list is comma-separated, and storage-scale-test v2.1.0 cannot
    # collect a pair on Kubernetes (the comma in its result file names fails its archive check).
    parser.add_argument("--io-sizes", default="r4K,1M", help="Comma-separated IO sizes (r prefix = random)")
    parser.add_argument("--threads", default="1", help="Comma-separated threads per node")
    parser.add_argument("--iodepth", default="1", help="Comma-separated IO depths")
    parser.add_argument("--duration", type=int, default=10, help="Seconds per write and read phase")
    parser.add_argument(
        "--results-dir", required=True, help="Results root: storage-scale-test writes one sweep directory per run here"
    )
    parser.add_argument(
        "--wait-timeout", default=AUTO, help="Seconds to wait for the sweep: auto (sized from the sweep) or N"
    )
    parser.add_argument("--live-interval-ms", type=int, default=1000, help="elbencho live statistics interval (ms)")
    return parser.parse_args(argv)


def _validate(args: argparse.Namespace) -> str | None:
    """Why the arguments cannot run, or None when they can.

    storage-scale-test checks the claim's format and the sweep settings (IO sizes, threads, IO
    depths, duration, live interval, nodes) itself, before it touches the cluster; this checks
    only what it does not, or checks too late.
    """
    if not args.claim.strip():
        return (
            "--claim is empty; it must name an existing claim as namespace/claim: set STORAGE_PERF_CLAIM "
            "(the perf_claim setting)"
        )
    if not args.node_selector.strip():
        return (
            "--node-selector is empty; the sweep runs only on labelled nodes: set STORAGE_PERF_NODE_SELECTOR "
            "(the perf_node_selector setting), e.g. storage-test=true"
        )
    if args.wait_timeout != AUTO and not (
        _is_whole_number(args.wait_timeout) and 1 <= int(args.wait_timeout) <= sst.WAIT_CAP_S
    ):
        return (
            f"--wait-timeout is {args.wait_timeout}; it must be auto or a number of seconds from 1 to {sst.WAIT_CAP_S}"
        )
    return None


def _is_whole_number(value: str) -> bool:
    """True for a whole number written in ASCII digits."""
    return re.fullmatch(r"[0-9]+", value) is not None


def _plan(args: argparse.Namespace, progress: sst.ProgressLog) -> tuple[float, str | None]:
    """Count the sweep's cells and set the deadline; return (deadline_s, problem)."""
    cells = sst.count_cells(args.nodes, args.io_sizes.split(","), args.threads.split(","), args.iodepth.split(","))
    estimate = sst.estimate_s(cells, args.duration)
    deadline_s = sst.auto_wait_s(cells, args.duration) if args.wait_timeout == AUTO else int(args.wait_timeout)
    progress.event("estimate", cells=cells, estimate_s=estimate, deadline_s=deadline_s)
    if cells > sst.MAX_CELLS:
        return deadline_s, f"the sweep has {cells} cells; storage-scale-test can collect at most {sst.MAX_CELLS}"
    if args.wait_timeout == AUTO and estimate > sst.WAIT_CAP_S:
        return deadline_s, (
            f"the sweep needs about {estimate} s, more than the {sst.WAIT_CAP_S} s a run waits; shrink the sweep"
        )
    return deadline_s, None


def _demo_result() -> dict[str, Any]:
    """Return the demo-mode (ISVCTL_DEMO_MODE=1) dummy-success result."""
    return {
        "success": True,
        "platform": "storage",
        "test_name": "run_storage_test",
        "state": TERMINAL_OK,
        "scale_test_version": "demo",
        "results_dir": "/tmp/isvctl-demo/elbencho-demo",
        "executions": {"total": 2, "succeeded": 2, "failed": 0},
        "resources_released": True,
    }


def _env_settings(args: argparse.Namespace) -> list[str]:
    """The env.sh settings isvctl owns: substrate, results location and Pod placement."""
    return [
        "export EXECUTION_SUBSTRATE=kubectl",
        f"export RESULTS_DIR={sst.bash_quote(args.results_dir)}",
        f"export LOGS_DIR={sst.bash_quote(str(Path(args.results_dir) / 'logs'))}",
        f"export KUBECTL_NODE_SELECTOR={sst.bash_quote(args.node_selector)}",
    ]


def _write_inputs(args: argparse.Namespace, workdir: Path) -> tuple[Path, Path]:
    """Write the target file (what to test) and the workload override file (how to test)."""
    workdir.mkdir(parents=True, exist_ok=True)
    target = workdir / "target.env"
    target.write_text(f"TEST_DIRS=([{sst.bash_quote(f'{args.claim}:{TEST_PATH}')}]=1)\n", encoding="utf-8")
    workload = workdir / "workload.env"
    workload.write_text(
        "\n".join(
            [
                f"ELBENCHO_SCALE_IO_SIZES={sst.bash_array(args.io_sizes.split(','))}",
                f"ELBENCHO_SCALE_THREAD_LIST={sst.bash_array(args.threads.split(','))}",
                f"ELBENCHO_IODEPTH_LIST={sst.bash_array(args.iodepth.split(','))}",
                f"ELBENCHO_SCALE_READ_WRITE_DURATION={args.duration}",
                f"ELBENCHO_LIVEINT={args.live_interval_ms}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return target, workload


def _submit(
    args: argparse.Namespace, base: Path, env: dict[str, str], progress: sst.ProgressLog
) -> tuple[str | None, str]:
    """Write the inputs and submit the sweep; return (sweep_dir, error)."""
    sst.install_env_sh(base, _env_settings(args))
    target, workload = _write_inputs(args, Path(args.results_dir).resolve())
    cmd = [str(base / sst.LAUNCHER), "--target", str(target), "--env-override", str(workload), "--nodes", args.nodes]
    sst.log("submitting: " + " ".join(cmd))
    proc = sst.run(cmd, sst.COMMAND_TIMEOUT_S, env)
    sweep_dir = sst.parse_kv(proc.stdout).get("STORAGE_SCALE_TEST_RESULTS_DIR")
    progress.event("submit", rc=proc.returncode, sweep_dir=sweep_dir)
    if proc.returncode != 0 or not sweep_dir:
        sst.log(proc.stdout[-2000:])
        sst.log(proc.stderr[-4000:])
        error = f"submit exited {proc.returncode}: {sst.describe_failure(proc.stderr)}"
        diagnostic = sst.diagnostics(proc.stderr)
        if diagnostic.get("PHASE") == CLAIM_LEASE_PHASE:
            # storage-scale-test suggests retrying, but a retry fails until the earlier sweep is released.
            error += f"; {CLAIM_HELD_HINT}"
        elif not diagnostic:
            # No diagnostic block: storage-scale-test's settings checks print only Error: lines.
            error += f"; {SETTINGS_HINT}"
        return sweep_dir, error
    return sweep_dir, ""


def _finish(result: dict[str, Any], status: dict[str, str]) -> None:
    """Copy the final lifecycle view into the result."""
    result["state"] = status.get("STATE", result.get("state"))
    counts = {}
    for name in ("total", "succeeded", "failed"):
        value = status.get(f"EXECUTIONS_{name.upper()}")
        if value is not None and value.isdigit():
            counts[name] = int(value)
    if counts:
        result["executions"] = counts


def _run_sweep(args: argparse.Namespace, result: dict[str, Any], progress: sst.ProgressLog) -> None:
    """Check the arguments, then submit, wait and collect under the results root's lock."""
    invalid = _validate(args)
    if invalid:
        result["error"] = invalid
        return
    deadline_s, problem = _plan(args, progress)
    if problem:
        result["error"] = problem
        return
    base = sst.resolve_base(args.scale_test_base)
    if not (base / sst.LAUNCHER).is_file():
        result["error"] = sst.missing_tree(base)
        return
    result["scale_test_version"] = sst.scale_test_version(base)
    # Held until the step ends: a teardown never acts on a sweep this step is still running.
    lock = sst.run_lock(args.results_dir)
    if lock is None:
        result["state"] = REFUSED
        result["error"] = f"another run or teardown is using {args.results_dir}"
        return
    try:
        _run_locked(args, base, deadline_s, result, progress)
    finally:
        sst.unlock(lock)


def _run_locked(
    args: argparse.Namespace, base: Path, deadline_s: float, result: dict[str, Any], progress: sst.ProgressLog
) -> None:
    """Submit, wait, cancel at the deadline, and collect; _run_sweep holds the lock."""
    env = sst.child_env(args.kubeconfig)
    # env.sh lives in the checkout, which runs from other working directories may share, and
    # submit is the only command that reads it.
    checkout = sst.checkout_lock(base, sst.COMMAND_TIMEOUT_S + sst.TERM_GRACE_S + sst.DRAIN_S)
    if checkout is None:
        result["state"] = REFUSED
        result["error"] = f"another run is still submitting from {base}; try again once it has"
        return
    try:
        sweep_dir, error = _submit(args, base, env, progress)
    finally:
        sst.unlock(checkout)
    if sweep_dir:
        result["results_dir"] = sweep_dir
    if error:
        # Whatever submit left in the results root, teardown finds there (sst.unreleased).
        result["error"] = error
        return

    finished, status = sst.wait_for_terminal(base, sweep_dir, env, deadline_s=deadline_s, progress=progress)
    _finish(result, status)
    cancelled_outside = status.get("STATE") == sst.CANCEL_REQUESTED
    if status.get("STATE") == sst.SUBMISSION_FAILED:
        result["error"] = f"submission failed in-cluster and was rolled back; see {sweep_dir}"
        result["resources_released"] = True
        return
    if not finished or cancelled_outside:
        # At the deadline, or after a cancel from outside this run: stop the sweep first, since
        # collect refuses a running one.
        stopped = sst.cancel(base, sweep_dir, env, progress=progress)
        if not finished and stopped is not None and stopped.get("STATE") == TERMINAL_OK:
            # The sweep finished before the cancel reached it, so its results are complete.
            finished = True
            _finish(result, stopped)
        elif not finished:
            result["state"] = "TIMEOUT"
            result["error"] = f"sweep did not finish within {deadline_s:.0f}s (--wait-timeout {args.wait_timeout})"
        if stopped is None:
            # The teardown step cancels and collects it again.
            result["resources_released"] = False
            result["collect_error"] = sst.cancel_failed(base, sweep_dir)
            result.setdefault("error", f"sweep was cancelled outside this run; see {sweep_dir}")
            return
    collected = sst.collect(base, sweep_dir, env, progress=progress)
    result["resources_released"] = collected["resources_released"]
    for key in ("failed_execution", "failed_log", "collect_error"):
        if key in collected:
            result[key] = collected[key]
    # The status that confirmed collect is the final view; asking again adds nothing.
    if finished and collected.get("final_status"):
        _finish(result, collected["final_status"])
    result["success"] = finished and result.get("state") == TERMINAL_OK and collected["resources_released"]
    if not result["success"] and "error" not in result:
        ended = "was cancelled outside this run" if cancelled_outside else f"ended {result.get('state')}"
        result["error"] = f"sweep {ended}; see {sweep_dir}"


def main(argv: list[str] | None = None) -> int:
    """Run the sweep and print the step result."""
    args = _parse_args(argv)
    if sst.DEMO_MODE:
        return sst.emit(_demo_result())
    result: dict[str, Any] = {
        "success": False,
        "platform": "storage",
        "test_name": "run_storage_test",
        "state": "NOT_SUBMITTED",
        "resources_released": None,
    }
    try:
        progress = sst.ProgressLog(Path(args.results_dir).resolve() / "run-logs")
    except OSError as exc:
        result["error"] = f"cannot create the progress log: {exc}"
        return sst.emit(result)
    result["progress_log"] = str(progress.path)
    restore = sst.install_interrupt_handler(progress)
    try:
        _run_sweep(args, result, progress)
    except (OSError, ValueError) as exc:
        result["error"] = str(exc)
    finally:
        restore()
    return sst.emit(result)


if __name__ == "__main__":
    sys.exit(main())
