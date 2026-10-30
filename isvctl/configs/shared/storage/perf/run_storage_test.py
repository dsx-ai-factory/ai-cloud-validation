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
the sweep, wait for it to finish, and collect it. The sweep runs in-cluster, so losing API
access while waiting is treated as "state unknown" and retried until --wait-timeout, not as
a failure. Collect runs whenever the sweep finished, because it is what releases the claim's
Lease, the coordinator Job and the worker DaemonSet.

The sweep's handle is written to --state-file as soon as it is submitted. If this step is
killed before it collects, the teardown step (release_storage_test.py) reads that file and
cancels and collects instead. --time-budget keeps that the exception: set below the step's
timeout, it makes the step stop waiting early enough to cancel, collect and report itself.

Prerequisites: a storage-scale-test checkout (v2.1.0 or later) used only by isvctl, the tools
it needs (bash 4.4+, kubectl, flock, GNU tar and timeout; isvctl's Python 3.12+), and an existing
Bound ReadWriteMany claim that the selected nodes can mount.

Usage:
    python run_storage_test.py --scale-test-base /path/to/storage-scale-test \\
        --claim my-namespace/my-claim --node-selector storage-test=true \\
        --results-dir /path/to/results --state-file /path/to/state.json

Output JSON:
{
    "success": true,
    "platform": "storage",
    "test_name": "run_storage_test",
    "state": "SUCCESS",
    "results_dir": "/path/to/results/elbencho-20260101Z000000",
    "executions": {"total": 4, "succeeded": 4, "failed": 0},
    "resources_released": true
}
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))  # shared/storage/perf/ (for sst)

import sst

TERMINAL_OK = "SUCCESS"
# Command timeouts the wait leaves free in the step's budget: one cancel and one collect.
RESERVED_CALLS = 2


def _parse_args() -> argparse.Namespace:
    """Parse the step's arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scale-test-base", required=True, help="storage-scale-test checkout used only by isvctl")
    parser.add_argument("--kubeconfig", default="", help="Kubeconfig for the cluster (default: kubectl's own)")
    parser.add_argument("--claim", required=True, help="Claim to test, as namespace/claim")
    parser.add_argument("--test-path", default="scale-test", help="Directory inside the claim to write under")
    parser.add_argument("--node-selector", required=True, help="Worker node labels, e.g. storage-test=true")
    parser.add_argument("--nodes", default="1", help="Node counts to sweep, storage-scale-test --nodes syntax")
    parser.add_argument("--io-sizes", default="r4K,1M", help="Comma-separated IO sizes (r prefix = random)")
    parser.add_argument("--threads", default="1", help="Comma-separated threads per node")
    parser.add_argument("--iodepth", default="1", help="Comma-separated IO depths")
    parser.add_argument("--duration", type=int, default=10, help="Seconds per write and read phase")
    parser.add_argument("--run-as-user", default="", help="UID for the benchmark Pods (default: 2000)")
    parser.add_argument("--run-as-group", default="", help="GID for the benchmark Pods (default: 2000)")
    parser.add_argument("--image", default="", help="elbencho image (default: the one storage-scale-test pins)")
    parser.add_argument("--results-dir", required=True, help="Where storage-scale-test writes results")
    parser.add_argument(
        "--state-file", required=True, help="Sweep handle for teardown; the generated inputs are written beside it"
    )
    parser.add_argument("--wait-timeout", type=int, default=900, help="Seconds to wait for the sweep")
    parser.add_argument("--interval", type=int, default=15, help="Seconds between status checks")
    parser.add_argument("--command-timeout", type=int, default=600, help="Seconds for each storage-scale-test call")
    parser.add_argument(
        "--time-budget", type=int, default=0, help="Seconds for the whole step; set below its timeout (0 = no limit)"
    )
    return parser.parse_args()


def _demo_result() -> dict[str, Any]:
    """Dummy success for ISVCTL_DEMO_MODE=1."""
    return {
        "success": True,
        "platform": "storage",
        "test_name": "run_storage_test",
        "state": TERMINAL_OK,
        "results_dir": "/tmp/isvctl-demo/elbencho-demo",
        "executions": {"total": 2, "succeeded": 2, "failed": 0},
        "resources_released": True,
    }


def _env_settings(args: argparse.Namespace, test_dir_key: str) -> list[str]:
    """The env.sh settings isvctl owns: substrate, results location and Pod placement."""
    settings = [
        "export EXECUTION_SUBSTRATE=kubectl",
        f"export RESULTS_DIR={sst.bash_quote(args.results_dir)}",
        f"export LOGS_DIR={sst.bash_quote(str(Path(args.results_dir) / 'logs'))}",
        f"export KUBECTL_NODE_SELECTOR={sst.bash_quote(args.node_selector)}",
        # The target file supplies TEST_DIRS for the run; env.sh keeps the same value so
        # storage-scale-test's own validate_env.sh also works against this checkout.
        f"TEST_DIRS=([{sst.bash_quote(test_dir_key)}]=1)",
    ]
    optional = {
        "KUBECTL_RUN_AS_USER": args.run_as_user,
        "KUBECTL_RUN_AS_GROUP": args.run_as_group,
        "KUBECTL_ELBENCHO_IMAGE": args.image,
    }
    settings += [f"export {name}={sst.bash_quote(value)}" for name, value in optional.items() if value]
    return settings


def _write_inputs(args: argparse.Namespace, workdir: Path, test_dir_key: str) -> tuple[Path, Path]:
    """Write the target file (what to test) and the workload override file (how to test)."""
    workdir.mkdir(parents=True, exist_ok=True)
    target = workdir / "target.env"
    target.write_text(f"TEST_DIRS=([{sst.bash_quote(test_dir_key)}]=1)\n", encoding="utf-8")
    workload = workdir / "workload.env"
    workload.write_text(
        "\n".join(
            [
                f"ELBENCHO_SCALE_IO_SIZES={sst.bash_array(args.io_sizes.split(','))}",
                f"ELBENCHO_SCALE_THREAD_LIST={sst.bash_array(args.threads.split(','))}",
                f"ELBENCHO_IODEPTH_LIST={sst.bash_array(args.iodepth.split(','))}",
                f"ELBENCHO_SCALE_READ_WRITE_DURATION={args.duration}",
                # env.sh.template already enables the extended live CSVs (per-interval,
                # per-client samples) that storage-scale-test's fairness reporting reads.
                "",
            ]
        ),
        encoding="utf-8",
    )
    return target, workload


def _submit(
    args: argparse.Namespace, base: Path, budget: sst.Budget, env: dict[str, str]
) -> tuple[str | None, str | None, str]:
    """Submit the sweep; return (results_dir, attempt, error)."""
    test_dir_key = f"{args.claim}:{args.test_path}"
    sst.install_env_sh(base, _env_settings(args, test_dir_key))
    target, workload = _write_inputs(args, Path(args.state_file).resolve().parent, test_dir_key)
    cmd = [*sst.launcher(base), "--target", str(target), "--env-override", str(workload), "--nodes", args.nodes]
    sst.log("submitting: " + " ".join(cmd))
    proc = sst.run(cmd, budget.call_timeout(), env)
    printed = sst.parse_kv(proc.stdout)
    results_dir = printed.get("STORAGE_SCALE_TEST_RESULTS_DIR")
    error = ""
    if proc.returncode != 0 or not results_dir:
        sst.log(proc.stdout[-2000:])
        sst.log(proc.stderr[-4000:])
        error = f"submit exited {proc.returncode}: {sst.describe_failure(proc.stderr)}"
    return results_dir, printed.get("STORAGE_SCALE_TEST_ATTEMPT_ID"), error


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


def _run_sweep(args: argparse.Namespace, result: dict[str, Any]) -> None:
    """Submit, wait and collect, filling in *result*."""
    base = Path(args.scale_test_base).resolve()
    if not (base / sst.LAUNCHER).is_file():
        result["error"] = f"{base} is not a storage-scale-test checkout (no {sst.LAUNCHER})"
        return
    env = sst.child_env(args.kubeconfig)
    budget = sst.Budget(args.command_timeout, args.time_budget)
    # Recorded before submitting, replacing any earlier run's record: submit takes the claim's
    # Lease before it prints the results directory, so a submit killed part-way must still
    # leave teardown a way to find the sweep (the new elbencho-* directory under the root).
    root = Path(args.results_dir).resolve()
    pending = {
        "scale_test_base": str(base),
        "kubeconfig": args.kubeconfig,
        "results_root": str(root),
        "existing": sorted(p.name for p in root.glob("elbencho-*")),
        "collected": False,
    }
    sst.write_state(args.state_file, pending)
    results_dir, attempt, error = _submit(args, base, budget, env)
    handle = {**pending, "results_dir": results_dir, "attempt": attempt}
    if results_dir:
        result["results_dir"] = results_dir
        sst.write_state(args.state_file, handle)
    if error:
        result["error"] = error
        if not results_dir:
            return

    reserve = RESERVED_CALLS * args.command_timeout
    wait_s = min(args.wait_timeout, budget.remaining() - reserve)
    finished, status = sst.wait_for_terminal(base, results_dir, wait_s, args.interval, budget, env)
    _finish(result, status)
    if not finished:
        limit = (
            f"--wait-timeout {args.wait_timeout}s"
            if wait_s >= args.wait_timeout
            else f"--time-budget {args.time_budget}s, less {reserve}s kept to cancel and collect"
        )
        result["state"] = "TIMEOUT"
        result["error"] = f"sweep did not finish within {limit}; cancelling it"
        sst.cancel(base, results_dir, budget, env)
    elif status.get("STATE") == sst.CANCEL_REQUESTED:
        # Cancelled from outside this run; finish the cancel so collect can release it.
        result["error"] = "the sweep was cancelled outside this run"
        sst.cancel(base, results_dir, budget, env)
    elif status.get("STATE") == sst.SUBMISSION_FAILED:
        result["error"] = f"submission failed in-cluster and was rolled back; see {results_dir}"
        result["resources_released"] = True
        sst.write_state(args.state_file, {**handle, "collected": True})
        return
    collected = sst.collect(base, results_dir, budget, env)
    result["resources_released"] = collected["resources_released"]
    for key in ("failed_execution", "failed_log", "collect_error"):
        if key in collected:
            result[key] = collected[key]
    rc, final = sst.query(base, results_dir, budget, env)
    if rc == 0 and finished:
        _finish(result, final)
    sst.write_state(args.state_file, {**handle, "collected": collected["resources_released"]})
    result["success"] = (
        finished and result.get("state") == TERMINAL_OK and collected["resources_released"] and not error
    )
    if not result["success"] and "error" not in result:
        result["error"] = f"sweep ended {result.get('state')}; see {results_dir}"


def main() -> int:
    """Run the sweep and print the step result."""
    args = _parse_args()
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
        _run_sweep(args, result)
    except (OSError, ValueError) as exc:
        result["error"] = str(exc)
    return sst.emit(result)


if __name__ == "__main__":
    sys.exit(main())
