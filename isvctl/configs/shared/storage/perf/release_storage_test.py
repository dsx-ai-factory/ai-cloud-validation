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
directory, this step finds the sweep as the new elbencho-* directory under the root. If that
step was stopped first (for example by Ctrl-C), the sweep may still be running and holding
the claim's Lease, the coordinator Job and the worker DaemonSet. This step cancels it if it
is still running, then collects it, which is what releases those resources. Like the run
step, it limits each call itself and needs no isvctl timeout, and it appends its outcome to
the run's run log.

Nothing to do (success) when there is no state file or the sweep was already collected.

It holds the lock beside the state file while it runs. If another run or teardown holds it,
it touches nothing and reports released false (the sweep is that run's to release). It also
releases a sweep an earlier run left behind, which is how a run refused over that sweep
heals: its own teardown releases it. It uses the run's own kubeconfig copy (deleted once the
sweep is released) and, before acting, checks that it still points at the context and server
the sweep was submitted to. It touches nothing while another live isvctl run uses the state
file's directory (sst.claim_active_run), or when the record cannot be read, or when several
results directories could be the sweep of a submit that never named its own. With --abandon,
a sweep it cannot release has its record (and interruption marker) renamed aside, so the next
run is not refused; the step still fails, and says how to release the sweep by hand.

While such an archived record (state.abandoned-<UTC>.json) exists, its sweep may still use the
claim and filesystem: the output lists it in abandoned_records, and the fixture teardown steps
keep the fixtures. Delete the archived record once that sweep has been collected.

Usage:
    python release_storage_test.py --state-file /path/to/state.json [--abandon]

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
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))  # shared/storage/perf/ (for sst)

import sst

# What storage-scale-test's IDENTITY_MISMATCH names when the cluster, not a helper Pod, differs.
CLUSTER_IDENTITY_KINDS = frozenset({"Namespace", "PersistentVolumeClaim", "PersistentVolume"})
# Smallest accepted values, as in run_storage_test.py; --collect-timeout's depends on
# --status-timeout (see _validate).
MINIMUMS = (
    ("--status-timeout", 300),
    ("--command-timeout", 60),
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the step's arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--state-file", required=True, help="State file written by run_storage_test.py")
    parser.add_argument("--command-timeout", type=int, default=600, help="Seconds for each cancel call")
    parser.add_argument("--status-timeout", type=int, default=300, help="Seconds for each status check")
    parser.add_argument(
        "--collect-timeout", type=int, default=8400, help="Seconds for collect, all retries and checks together"
    )
    parser.add_argument(
        "--abandon",
        action="store_true",
        help="If the sweep cannot be released, move its record aside so the next run can start",
    )
    return parser.parse_args(argv)


def _validate(args: argparse.Namespace) -> str | None:
    """Why the arguments cannot run, or None when they can."""
    for flag, minimum in MINIMUMS:
        value = getattr(args, flag.removeprefix("--").replace("-", "_"))
        if value < minimum:
            return f"{flag} is {value}; it must be at least {minimum}"
    for flag in ("--status-timeout", "--command-timeout", "--collect-timeout"):
        value = getattr(args, flag.removeprefix("--").replace("-", "_"))
        if value > sst.MAX_SECONDS:
            return f"{flag} is {value}; it must be at most {sst.MAX_SECONDS} (30 days)"
    reserve = sst.COLLECT_RESERVE_S + sst.MIN_UPSTREAM_COLLECT_S
    if args.collect_timeout < args.status_timeout + reserve:
        return (
            f"--collect-timeout is {args.collect_timeout}; it must be at least "
            f"{args.status_timeout + reserve} (--status-timeout + {reserve})"
        )
    return None


def _demo_result() -> dict[str, Any]:
    """Return the demo-mode (ISVCTL_DEMO_MODE=1) dummy-success result."""
    return {
        "success": True,
        "platform": "storage",
        "test_name": "release_storage_test",
        "released": True,
        "message": "already collected; nothing to release",
    }


def _cancel(
    base: Path,
    results_dir: str,
    env: dict[str, str],
    args: argparse.Namespace,
    run_log: sst.RunLog | None,
    attempts: int = 3,
) -> bool:
    """Cancel the sweep with the step's call limits."""
    return sst.cancel(
        base,
        results_dir,
        env,
        command_timeout=args.command_timeout,
        status_timeout=args.status_timeout,
        attempts=attempts,
        run_log=run_log,
    )


def _release(
    state: dict[str, Any], state_file: str, args: argparse.Namespace, run_log: sst.RunLog | None
) -> dict[str, Any]:
    """Cancel the sweep if it is still running, then collect it."""
    base = Path(state["scale_test_base"])
    results_dir = state["results_dir"]
    env = sst.child_env(sst.sweep_kubeconfig(state))
    result: dict[str, Any] = {"results_dir": results_dir, "released": False}
    if not sst.has_attempt(results_dir):
        if state.get("attempt"):
            # Submit printed the attempt, so the sweep exists; only its local copy is gone.
            result["error"] = _lost_results(state)
            return result
        sst.write_state(state_file, {**state, "collected": True})
        return {**result, "released": True, "message": "submit stopped before creating any cluster resource"}
    mismatch = _other_cluster(state, env)
    if mismatch:
        result["error"] = mismatch
        return result
    asked = sst.CLOCK.monotonic()
    kind, status = sst.query(
        base, results_dir, env, status_timeout=args.status_timeout, lock_window_s=sst.LOCK_WINDOW_S
    )
    if run_log is not None:
        run_log.event(
            "poll",
            kind=kind,
            state=status.get("STATE"),
            next_action=status.get("NEXT_ACTION"),
            reason=status.get("DIAGNOSTIC_REASON"),
            resource_kind=status.get("DIAGNOSTIC_RESOURCE_KIND"),
            status_s=round(sst.CLOCK.monotonic() - asked, 1),
            busy_retries=status.get("BUSY_RETRIES"),
        )
    if kind in ("access", "slow"):
        # Cancel and collect would only use up their own timeouts.
        result["error"] = f"cannot read the sweep's state ({status.get('DIAGNOSTIC_REASON', 'status timed out')})"
        return result
    if (
        status.get("DIAGNOSTIC_REASON") == sst.IDENTITY_MISMATCH
        and status.get("DIAGNOSTIC_RESOURCE_KIND") in CLUSTER_IDENTITY_KINDS
    ):
        # storage-scale-test refuses cancel and collect for the same reason.
        result["error"] = (
            f"storage-scale-test found the sweep's {status['DIAGNOSTIC_RESOURCE_KIND']} is not the one it "
            "recorded (IDENTITY_MISMATCH): the kubeconfig may point at another cluster, or the claim was "
            "recreated. Not touching the sweep"
        )
        return result
    stopped = True
    if kind != "ok":
        # The cluster answers; cancel and collect check the sweep for themselves.
        sst.log(f"cannot read the sweep's state ({kind}); cancelling and collecting it anyway")
        stopped = _cancel(base, results_dir, env, args, run_log, attempts=1)
    elif status.get("STATE") == sst.SUBMISSION_FAILED:
        sst.write_state(state_file, {**state, "collected": True})
        return {**result, "released": True, "message": "submission failed and storage-scale-test rolled it back"}
    elif status.get("NEXT_ACTION") == sst.NEXT_WAIT or status.get("STATE") == sst.CANCEL_REQUESTED:
        sst.log(f"sweep still {status.get('STATE')}; cancelling it")
        if not _cancel(base, results_dir, env, args, run_log):
            result["error"] = "the sweep did not stop; it still holds the claim"
            return result
    collected = sst.collect(
        base,
        results_dir,
        env,
        collect_timeout=args.collect_timeout,
        status_timeout=args.status_timeout,
        run_log=run_log,
    )
    result["released"] = collected["resources_released"]
    if collected["resources_released"]:
        sst.write_state(state_file, {**state, "collected": True})
        result["message"] = "collected; cluster resources released"
    elif not stopped:
        # collect's own remedy (collect again) cannot work while the sweep runs on.
        result["error"] = sst.cancel_failed(base, results_dir, "running teardown again tries both again")
    else:
        result["error"] = collected.get("collect_error", "collect did not release the sweep's resources")
    return result


def _lost_results(state: dict[str, Any]) -> str:
    """Why a sweep whose results directory is gone cannot be released here, and what to do."""
    results_dir = state["results_dir"]
    launcher = Path(state["scale_test_base"]) / sst.LAUNCHER
    return (
        f"{results_dir} no longer holds attempt {state['attempt']} (kubernetes/current-attempt is gone), so the "
        "sweep cannot be released from here: storage-scale-test's --cancel and --collect need that results "
        f"directory. Restore a copy of it (a CI artifact, for example) to {results_dir} and run teardown again, or, "
        f"with KUBECONFIG set to the sweep's cluster, run `{launcher} --cancel <copy>` and then "
        f"`{launcher} --collect <copy>`. With no copy left, storage-scale-test cannot release the sweep: "
        "remove what it holds by hand (`kubectl get all,leases,networkpolicies -A "
        "-l app.kubernetes.io/name=storage-scale-test` lists it), then run teardown with "
        'perf_abandon_unreleased: "true"'
    )


def _other_cluster(state: dict[str, Any], env: dict[str, str]) -> str | None:
    """Why the kubeconfig is not the one the sweep was submitted with, or None when it is.

    A record without a cluster (from an older run, or one whose kubeconfig kubectl could not
    read) is not checked; storage-scale-test's own identity check still applies.
    """
    recorded = state.get("cluster")
    if not isinstance(recorded, dict):
        return None
    wanted = {"context": recorded.get("context"), "server": recorded.get("server")}
    current = sst.cluster_identity(env)
    if current == wanted:
        return None
    where = f"context {wanted['context']} at {wanted['server']}"
    if current is None:
        return f"cannot read which cluster the kubeconfig points at; the sweep runs on {where}. Not touching it"
    return (
        f"the sweep runs on {where}, but the kubeconfig now points at context {current['context']} at "
        f"{current['server']}. Not touching it; release it with a kubeconfig for the original cluster"
    )


def _abandon(state_file: str, record: dict[str, Any], result: dict[str, Any]) -> None:
    """Move the record and its interruption marker aside, so the next run is not refused."""
    stamp = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    path = Path(state_file)
    archived = path.with_name(f"{path.stem}.abandoned-{stamp}{path.suffix}")
    path.replace(archived)
    marker = sst.marker_path(state_file)
    if marker.exists():
        marker.replace(marker.with_name(f"{marker.name}.abandoned-{stamp}"))
    results_dir = record.get("results_dir") or record.get("results_root")
    launcher = Path(record["scale_test_base"]) / sst.LAUNCHER
    # The run's own kubeconfig copy stays: it is how to reach the sweep's cluster by hand.
    snapshot = record.get("kubeconfig_snapshot")
    kubeconfig = f"KUBECONFIG={snapshot}" if snapshot else "KUBECONFIG set to the sweep's cluster"
    sst.log(f"abandoned the sweep in {results_dir}; its record is now {archived}")
    result["abandoned"] = True
    result["error"] = (
        f"{result.get('error', 'the sweep was not released')}. Abandoned: the record is now {archived}, so a run "
        "that uses an existing claim can start; a run that would create its fixtures is refused while their "
        f"records are kept. The sweep may still be running and hold cluster resources; with {kubeconfig}, "
        f"stop it with `{launcher} --cancel {results_dir}` (or wait until "
        f"`{launcher} --status {results_dir}` shows it finished), then release it with "
        f"`{launcher} --collect {results_dir}`. Once you have collected it, delete {archived}"
        + (f" and {snapshot}" if snapshot else "")
        + ": until then teardown keeps the claim and filesystem, which the sweep may still use"
    )


def main(argv: list[str] | None = None) -> int:
    """Release the sweep if needed and print the step result."""
    args = _parse_args(argv)
    if sst.DEMO_MODE:
        return sst.emit(_demo_result())
    result: dict[str, Any] = {"success": False, "platform": "storage", "test_name": "release_storage_test"}
    invalid = _validate(args)
    if invalid:
        return sst.emit({**result, "released": False, "error": invalid})
    if not Path(args.state_file).parent.is_dir():
        result.update({"success": True, "released": True, "message": "no sweep recorded; nothing to release"})
        return sst.emit(result)
    try:
        lock = sst.state_lock(args.state_file)
    except OSError as exc:
        where = Path(args.state_file).parent / sst.STATE_LOCK
        return sst.emit({**result, "released": False, "error": f"cannot lock {where}: {exc.strerror or exc}"})
    if lock is None:
        # Its sweep may be running; only the run that holds the lock may cancel it.
        holder = sst.describe_owner(sst.peek_state(args.state_file))
        result.update(
            {
                "released": False,
                "error": "another run or teardown is using this state file; not touching its sweep"
                + (f" (recorded by {holder})" if holder else ""),
            }
        )
        return sst.emit(result)
    try:
        _release_locked(args, result)
    finally:
        sst.unlock(lock)
    return sst.emit(result)


def _unreadable(state: dict[str, Any], state_file: str, result: dict[str, Any]) -> None:
    """Report a record the step cannot act on. Not abandoned either: what it names cannot be
    trusted to say how to release its sweep.
    """
    result.update({"released": False, "error": sst.unreleased_message(state, state_file)})


def _release_locked(args: argparse.Namespace, result: dict[str, Any]) -> None:
    """main(), once this step holds the state file's lock."""
    busy = sst.claim_active_run(args.state_file)
    if busy:
        # Another live isvctl run's sweep and fixtures; released false keeps the fixtures too.
        result.update({"released": False, "error": busy})
        return
    try:
        state = sst.read_state(args.state_file)
    except sst.StateError as exc:
        # Not abandoned either: nothing in it can be trusted to say how to release a sweep.
        result.update({"released": False, "error": sst.state_error_message(exc)})
        return
    if state is None:
        result.update({"success": True, "released": True, "message": "no sweep recorded; nothing to release"})
    elif state.get("collected"):
        result.update({"success": True, "released": True, "message": "already collected; nothing to release"})
    elif sst.record_problem(state, need_base=False):
        _unreadable(state, args.state_file, result)
    elif not state.get("results_dir") and (candidates := sst.ambiguous_candidates(state)):
        # Not abandoned either: which sweep to release by hand is the user's call.
        result.update({"released": False, "error": sst.ambiguous_message(state, candidates, args.state_file)})
    elif not state.get("results_dir") and (unrecorded := sst.find_unrecorded(state)) is None:
        result.update({"success": True, "released": True, "message": "submit created no sweep; nothing to release"})
    elif sst.record_problem(state):
        _unreadable(state, args.state_file, result)
    else:
        record = state if state.get("results_dir") else unrecorded
        run_log = sst.RunLog.reopen(state.get("run_log"))
        restore = sst.install_interrupt_handler(run_log)
        try:
            result.update(_release(record, args.state_file, args, run_log))
        except OSError as exc:
            # A launcher that cannot run (ToolError names it), or a state file that cannot be
            # written.
            result.update({"released": False, "error": str(exc)})
        finally:
            restore()
        result["success"] = result.get("released") is True
        if not result["success"]:
            marker = sst.read_marker(args.state_file) or {}
            if marker.get("interrupted_utc"):
                result["error"] = f"{result.get('error')} (the run was interrupted at {marker['interrupted_utc']})"
            if args.abandon:
                _abandon(args.state_file, record, result)
        if run_log is not None:
            run_log.event("release", outcome="released" if result["success"] else "not_released")
            run_log.close()
    if result["success"]:
        sst.remove_marker(args.state_file)
        # Never before: until the sweep is released, the snapshot is how teardown reaches its cluster.
        sst.remove_snapshot(state, args.state_file)
    abandoned = sst.abandoned_records(args.state_file)
    if abandoned:
        result["abandoned_records"] = abandoned
        if result["success"]:
            result["message"] = (
                f"{result['message']}; keeping the claim and filesystem, which the abandoned sweep in "
                f"{', '.join(abandoned)} may still use: once you have collected that sweep, delete "
                f"{' and '.join(abandoned)} so a later teardown can remove them"
            )


if __name__ == "__main__":
    sys.exit(main())
