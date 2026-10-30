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

"""Release a storage-scale-test sweep that run_storage_test.py did not collect.

Teardown safety net. run_storage_test.py records the sweep in --state-file before it submits
(the results root) and again once submitted (the results directory), and marks it collected
once its cluster resources are released. When submit was killed before printing the results
directory, this step finds the sweep as the new elbencho-* directory under the root. If that step was
killed first (for example by its own step timeout), the sweep may still be running and holding
the claim's Lease, the coordinator Job and the worker DaemonSet. This step cancels it if it is
still running, then collects it, which is what releases those resources.

Nothing to do (success) when there is no state file or the sweep was already collected.

Usage:
    python release_storage_test.py --state-file /path/to/state.json

Output JSON:
{
    "success": true,
    "platform": "storage",
    "test_name": "release_storage_test",
    "released": true,
    "message": "already collected; nothing to release"
}
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))  # shared/storage/perf/ (for sst)

import sst


def _parse_args() -> argparse.Namespace:
    """Parse the step's arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--state-file", required=True, help="State file written by run_storage_test.py")
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
        "test_name": "release_storage_test",
        "released": True,
        "message": "already collected; nothing to release",
    }


def _release(state: dict[str, Any], state_file: str, budget: sst.Budget) -> dict[str, Any]:
    """Cancel the sweep if it is still running, then collect it."""
    base = Path(state["scale_test_base"])
    results_dir = state["results_dir"]
    env = sst.child_env(state.get("kubeconfig"))
    result: dict[str, Any] = {"results_dir": results_dir, "released": False}
    if not sst.has_attempt(results_dir):
        sst.write_state(state_file, {**state, "collected": True})
        return {**result, "released": True, "message": "submit stopped before creating any cluster resource"}
    rc, status = sst.query(base, results_dir, budget, env)
    if rc != 0:
        result["error"] = f"cannot read the sweep's state ({status.get('DIAGNOSTIC_REASON', 'status failed')})"
        return result
    if status.get("STATE") == sst.SUBMISSION_FAILED:
        sst.write_state(state_file, {**state, "collected": True})
        return {**result, "released": True, "message": "submission failed and storage-scale-test rolled it back"}
    if status.get("NEXT_ACTION") == sst.NEXT_WAIT or status.get("STATE") == sst.CANCEL_REQUESTED:
        sst.log(f"sweep still {status.get('STATE')}; cancelling it")
        if not sst.cancel(base, results_dir, budget, env):
            result["error"] = "the sweep did not stop; it still holds the claim"
            return result
    collected = sst.collect(base, results_dir, budget, env)
    result["released"] = collected["resources_released"]
    if collected["resources_released"]:
        sst.write_state(state_file, {**state, "collected": True})
        result["message"] = "collected; cluster resources released"
    else:
        result["error"] = collected.get("collect_error", "collect did not release the sweep's resources")
    return result


def _find_unrecorded(state: dict[str, Any]) -> dict[str, Any] | None:
    """The record of a sweep whose submit never printed its results directory, if one exists."""
    root = Path(state.get("results_root") or "")
    if not state.get("results_root") or not root.is_dir():
        return None
    new = sorted(p.name for p in root.glob("elbencho-*") if p.name not in set(state.get("existing") or []))
    return {**state, "results_dir": str(root / new[-1])} if new else None


def main() -> int:
    """Release the sweep if needed and print the step result."""
    args = _parse_args()
    if sst.DEMO_MODE:
        return sst.emit(_demo_result())
    result: dict[str, Any] = {"success": False, "platform": "storage", "test_name": "release_storage_test"}
    state = sst.read_state(args.state_file)
    if state is None:
        result.update({"success": True, "released": True, "message": "no sweep recorded; nothing to release"})
    elif state.get("collected"):
        result.update({"success": True, "released": True, "message": "already collected; nothing to release"})
    elif not state.get("results_dir") and (unrecorded := _find_unrecorded(state)) is None:
        result.update({"success": True, "released": True, "message": "submit created no sweep; nothing to release"})
    else:
        budget = sst.Budget(args.command_timeout, args.time_budget)
        result.update(_release(state if state.get("results_dir") else unrecorded, args.state_file, budget))
        result["success"] = result.get("released") is True
    return sst.emit(result)


if __name__ == "__main__":
    sys.exit(main())
