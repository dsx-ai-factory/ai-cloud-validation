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

"""Release the storage-scale-test sweeps run_storage_test.py did not collect.

Teardown safety net. If the run step was stopped before it collected (for example by Ctrl-C),
its sweep may still be running and holding the claim's Lease, the coordinator Job and the
worker DaemonSet. storage-scale-test keeps each sweep's lifecycle in its own sweep directory,
so this step checks every one under the results root (--results-dir, sst.unreleased), and for
each that is unreleased cancels it if it may still be running, then collects it, which is what
releases those resources. Like the run step, it limits each call itself and needs no isvctl
timeout.

Nothing to do (success) when no sweep there holds anything, or when the results root does
not exist: nothing has run from this directory, and the message names the path so a teardown
run from the wrong directory shows. It holds the results root's lock while it runs; if another
run or teardown holds it, it touches nothing and reports released false (the sweep is that
run's to release). A sweep whose status cannot be read (the cluster is out of reach) is left
alone: rerun `--phase teardown` once the cluster answers.

Usage:
    python release_storage_test.py --results-dir /path/to/results \\
        [--scale-test-base /path/to/storage-scale-test] [--kubeconfig /path/to/kubeconfig]

Output JSON:
{
    "success": true,
    "platform": "storage",
    "test_name": "release_storage_test",
    "released": true,
    "message": "no sweep under /path/to/results holds cluster resources"
}
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))  # shared/storage/perf/ (for sst)

import sst


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the step's arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--results-dir",
        required=True,
        type=sst.absolute_dir,
        help="Results root: storage-scale-test writes one sweep directory per run here",
    )
    parser.add_argument(
        "--scale-test-base",
        default="",
        help="storage-scale-test tree (default: $STORAGE_SCALE_TEST_BASE, else the vendored release)",
    )
    parser.add_argument("--kubeconfig", default="", help="Kubeconfig for the cluster (default: kubectl's own)")
    return parser.parse_args(argv)


def _demo_result() -> dict[str, Any]:
    """Return the demo-mode (ISVCTL_DEMO_MODE=1) dummy-success result."""
    return {
        "success": True,
        "platform": "storage",
        "test_name": "release_storage_test",
        "released": True,
        "message": "no sweep under /tmp/isvctl-demo/results holds cluster resources",
    }


def _release(base: Path, sweep_dir: str, env: dict[str, str], kind: str, status: dict[str, str]) -> str | None:
    """Cancel one sweep if it may still run, then collect it; None once released, else why not."""
    if kind == "unreachable":
        # Cancel and collect would only use up their own timeouts.
        reason = status.get("DIAGNOSTIC_REASON", "status timed out")
        return f"cannot read the state of {sweep_dir} ({reason}); rerun `--phase teardown` once the cluster answers"
    if kind != "ok" or status.get("NEXT_ACTION") == sst.NEXT_WAIT or status.get("STATE") == sst.CANCEL_REQUESTED:
        # Collect refuses a running sweep, so one that may still run is cancelled first. The
        # cluster answers even when the state is unreadable; cancel checks for itself.
        sst.log(f"{sweep_dir}: sweep {status.get('STATE') or 'state unreadable'}; cancelling it")
        if sst.cancel(base, sweep_dir, env) is None:
            return sst.cancel_failed(base, sweep_dir)
    collected = sst.collect(base, sweep_dir, env)
    return None if collected["resources_released"] else collected["collect_error"]


def _release_all(args: argparse.Namespace, base: Path, result: dict[str, Any]) -> None:
    """Release every unreleased sweep under the results root."""
    env = sst.child_env(args.kubeconfig)
    found = sst.unreleased(base, args.results_dir, env)
    errors = [error for d, kind, status in found if (error := _release(base, d, env, kind, status))]
    if errors:
        result.update({"released": False, "error": "; ".join(errors)})
    elif found:
        dirs = ", ".join(d for d, _, _ in found)
        result.update({"released": True, "message": f"collected; cluster resources released. Results: {dirs}"})
    else:
        result.update({"released": True, "message": f"no sweep under {args.results_dir} holds cluster resources"})


def main(argv: list[str] | None = None) -> int:
    """Release what the sweeps hold and print the step result."""
    args = _parse_args(argv)
    if sst.DEMO_MODE:
        return sst.emit(_demo_result())
    result: dict[str, Any] = {"success": False, "platform": "storage", "test_name": "release_storage_test"}
    if not Path(args.results_dir).is_dir():
        # Not created here: the run step creates it first, so nothing ran from this directory.
        message = f"no results root at {args.results_dir}; nothing has run from this directory"
        return sst.emit({**result, "success": True, "released": True, "message": message})
    try:
        lock = sst.run_lock(args.results_dir)
    except OSError as exc:
        return sst.emit({**result, "released": False, "error": f"cannot lock {args.results_dir}: {exc}"})
    if lock is None:
        # Its sweep may be running; only the run that holds the lock may cancel it.
        error = f"another run or teardown is using {args.results_dir}; not touching its sweeps"
        return sst.emit({**result, "released": False, "error": error})
    restore = sst.install_interrupt_handler()
    base = sst.resolve_base(args.scale_test_base)
    try:
        _release_all(args, base, result)
    except OSError as exc:
        # A launcher that cannot run (ToolError names it).
        error = str(exc) if (base / sst.LAUNCHER).is_file() else sst.missing_tree(base)
        result.update({"released": False, "error": error})
    finally:
        restore()
        sst.unlock(lock)
    result["success"] = result.get("released") is True
    return sst.emit(result)


if __name__ == "__main__":
    sys.exit(main())
