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

"""Find the storage-scale-test tree the storage performance steps run.

By default it is the pinned release that `make vendor-storage-scale-test` (or `make vendor`)
extracts into isvtest/vendor/storage-scale-test. The source release is all the Kubernetes
sweep needs on this machine: elbencho runs in the benchmark Pods from the image that release
pins. --base points at another tree instead (for example a development checkout); isvctl
writes that tree's env.sh, so dedicate it to isvctl.

With --state-file it also fails setup early, before any fixture is created, when that file
records a sweep that was never released (or cannot be read): the run step would refuse to start
anyway, and the teardown step, which still runs, releases it. It then marks the file's directory
as used by this isvctl run (sst.claim_active_run), and fails while another live isvctl run holds
that mark, so a second run from the same directory stops before setup_cluster repins the
kubeconfig there.

Usage:
    python find_storage_scale_test.py [--base /path/to/storage-scale-test] [--state-file state.json]

Output JSON:
{
    "success": true,
    "platform": "storage",
    "test_name": "find_storage_scale_test",
    "scale_test_base": "/path/to/repo/isvtest/vendor/storage-scale-test",
    "version": "v2.1.0"
}
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))  # shared/storage/perf/ (for sst)

import sst

VENDORED = Path(__file__).resolve().parents[5] / "isvtest" / "vendor" / "storage-scale-test"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the step's arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", default="", help="storage-scale-test tree to use instead of the vendored one")
    parser.add_argument("--state-file", default="", help="run_storage_test.py's state file, to check before setup")
    return parser.parse_args(argv)


def _demo_result() -> dict[str, Any]:
    """Return the demo-mode (ISVCTL_DEMO_MODE=1) dummy-success result."""
    return {
        "success": True,
        "platform": "storage",
        "test_name": "find_storage_scale_test",
        "scale_test_base": "/tmp/isvctl-demo/storage-scale-test",
        "version": "demo",
    }


def _claim(state_file: str) -> str | None:
    """Claim the state file's directory for this isvctl run (sst.claim_active_run); why not, or None."""
    try:
        lock = sst.state_lock(state_file)
        if lock is None:
            return f"another run or teardown is using {state_file}"
        try:
            return sst.claim_active_run(state_file)
        finally:
            sst.unlock(lock)
    except OSError as exc:
        return f"cannot mark {Path(state_file).parent} as used by this run: {exc.strerror or exc}"


def main(argv: list[str] | None = None) -> int:
    """Locate the tree and print the step result."""
    args = _parse_args(argv)
    if sst.DEMO_MODE:
        return sst.emit(_demo_result())
    result: dict[str, Any] = {"success": False, "platform": "storage", "test_name": "find_storage_scale_test"}
    # Read-only: run_storage_test.py makes the same check under the state file's lock.
    try:
        earlier = sst.needs_release(sst.read_state(args.state_file)) if args.state_file else None
    except sst.StateError as exc:
        result["error"] = sst.state_error_message(exc)
        return sst.emit(result)
    if earlier is not None:
        result["error"] = f"{sst.unreleased_message(earlier, args.state_file)}, unless another run is still using it"
        return sst.emit(result)
    # Before setup_cluster repins the kubeconfig beside the state file: one run per directory.
    busy = _claim(args.state_file) if args.state_file else None
    if busy:
        result["error"] = busy
        return sst.emit(result)
    base = Path(args.base).expanduser().resolve() if args.base else VENDORED
    if not (base / sst.LAUNCHER).is_file():
        hint = "" if args.base else "; run `make vendor` (or `make vendor-storage-scale-test`)"
        result["error"] = f"no storage-scale-test at {base}{hint}"
        return sst.emit(result)
    # The version script runs in its own session; stop it if the step is interrupted.
    restore = sst.install_interrupt_handler()
    try:
        version = sst.tree_version(base)
    except OSError as exc:
        # bash missing from PATH (ToolError names it and the likely cause).
        result["error"] = str(exc)
        return sst.emit(result)
    finally:
        restore()
    result.update({"success": True, "scale_test_base": str(base), "version": version})
    return sst.emit(result)


if __name__ == "__main__":
    sys.exit(main())
