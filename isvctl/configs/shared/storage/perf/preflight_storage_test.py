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

"""Check the cluster can run the sweep, with storage-scale-test's own validate_env.sh.

Runs before run_storage_test and fails fast: a claim that does not mount, a test directory
the workload's UID/GID cannot write, an image the nodes cannot pull, a node selector that
matches no node, or missing RBAC. validate_env.sh checks each claim and runs one short,
deadline-bounded validation Job that mounts them (it sets runAsUser/runAsGroup, never
fsGroup); the Job cleans itself up. Nothing is left on the cluster.

validate_env.sh checks only env.sh's own target, so this step writes env.sh with the claim
in TEST_DIRS while validate_env.sh runs, then writes it back without the claim, as
run_storage_test writes it. It holds the checkout's lock throughout, like a submit.

Usage:
    python preflight_storage_test.py --claim my-namespace/my-claim --node-selector storage-test=true \\
        --results-dir /path/to/results [--scale-test-base /path/to/storage-scale-test]

Output JSON:
{
    "success": true,
    "platform": "storage",
    "test_name": "preflight_storage_test",
    "message": "validate_env.sh passed: <its summary lines>"
}
A failed check reports "error": validate_env.sh's errors.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))  # shared/storage/perf/ (for sst)

import sst

# What validate_env.sh prints before its list of errors, one per line.
FAILED_HEADER = "Validation failed with the following errors:"
# kubectl's client/server version-skew warning, which validate_env.sh passes on inside an error.
SKEW_WARNING = "exceeds the supported minor version skew"
PASSED = "All validation checks passed successfully"


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
    parser.add_argument("--results-dir", required=True, help="Results root, as run_storage_test uses it")
    return parser.parse_args(argv)


def _summary(stdout: str) -> str:
    """validate_env.sh's summary of what passed: its ``...: OK`` lines."""
    return "; ".join(line.strip() for line in stdout.splitlines() if line.strip().endswith(": OK"))


def _errors(ran: sst.Ran) -> str:
    """validate_env.sh's errors (the lines after FAILED_HEADER), else the end of its output."""
    lines = ran.stdout.splitlines()
    if FAILED_HEADER in lines:
        found = [
            line.strip(" -")
            for line in lines[lines.index(FAILED_HEADER) + 1 :]
            if line.strip() and SKEW_WARNING not in line
        ]
        if found:
            return "; ".join(found)
    return (ran.stderr.strip() or ran.stdout.strip())[-1000:] or f"exited {ran.returncode} with no output"


def _check(args: argparse.Namespace, result: dict[str, Any]) -> None:
    """Write env.sh for the claim and run validate_env.sh under the checkout's lock."""
    for value, setting in ((args.claim, "--claim (perf_claim)"), (args.node_selector, "--node-selector")):
        if not value.strip():
            result["error"] = f"{setting} is empty; set STORAGE_PERF_CLAIM and STORAGE_PERF_NODE_SELECTOR"
            return
    base = sst.resolve_base(args.scale_test_base)
    if not (base / sst.VALIDATE_ENV).is_file():
        result["error"] = sst.missing_tree(base)
        return
    checkout = sst.checkout_lock(base, sst.COMMAND_TIMEOUT_S + sst.TERM_GRACE_S + sst.DRAIN_S)
    if checkout is None:
        result["error"] = f"another run is still submitting from {base}; try again once it has"
        return
    settings = sst.env_settings(args.results_dir, args.node_selector)
    try:
        sst.install_env_sh(base, [*settings, sst.test_dirs(args.claim)])
        ran = sst.run([str(base / sst.VALIDATE_ENV)], sst.COMMAND_TIMEOUT_S, sst.child_env(args.kubeconfig))
    finally:
        # Leave env.sh as the run step writes it, without this claim: someone running
        # validate_env.sh or the launcher by hand from the checkout should not target it.
        try:
            sst.install_env_sh(base, settings)
        finally:
            sst.unlock(checkout)
    if ran.timed_out:
        result["error"] = f"validate_env.sh did not finish within {sst.COMMAND_TIMEOUT_S}s"
    elif ran.returncode != 0 or PASSED not in ran.stdout:
        result["error"] = f"validate_env.sh failed: {_errors(ran)}"
    else:
        result["success"] = True
        result["message"] = f"validate_env.sh passed: {_summary(ran.stdout) or PASSED}"


def main(argv: list[str] | None = None) -> int:
    """Run the check and print the step result."""
    args = _parse_args(argv)
    result: dict[str, Any] = {"success": False, "platform": "storage", "test_name": "preflight_storage_test"}
    if sst.DEMO_MODE:
        return sst.emit({**result, "success": True, "message": "validate_env.sh passed (demo)"})
    # validate_env.sh holds no cluster resources once it is stopped: no teardown hint.
    restore = sst.install_interrupt_handler(hint=None)
    try:
        _check(args, result)
    except (OSError, ValueError) as exc:
        result["error"] = str(exc)
    finally:
        restore()
    return sst.emit(result)


if __name__ == "__main__":
    sys.exit(main())
