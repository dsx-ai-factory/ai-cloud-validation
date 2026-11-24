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
sweep finished before the cancel reached it; either way the result has "deadline_reached": true.
The deadline counts from when the sweep started (after submit, or a batch's --start), not from
when the step started. Before submitting, it rejects (NOT_SUBMITTED) a
sweep with more cells than storage-scale-test can collect (sst.MAX_CELLS), or one that auto
cannot fit in sst.WAIT_CAP_S.

The step needs no isvctl timeout: every storage-scale-test call has its own limit (sst.py's
*_TIMEOUT_S), and a call that hits it is stopped with everything it started. Its progress
(the estimate, submit, each status check, cancel and collect) goes to a log under
<results-dir>/run-logs/, whose path the result reports as progress_log.

With --groups (the perf_groups setting) it runs a storage-scale-test batch instead: one workload
override file per group, --batch with the first group, --append for each further one, then
--start; the wait, cancel and collect are the same on the batch directory. The result then
lists each group (group_id, its settings and floors) under "groups" for the evaluate step.

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
    "results_dir": "/path/to/results/elbencho-20260101Z000000",
    "executions": {"total": 4, "succeeded": 4, "failed": 0},
    "resources_released": true,
    "progress_log": "/path/to/results/run-logs/run-20260101T000000Z-1234.log"
}
A failed run adds "error": why it failed, then, when they apply, the first failed execution
and its log, and what to run to release a sweep that still holds cluster resources.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import Any, NamedTuple

sys.path.insert(0, str(Path(__file__).resolve().parent))  # shared/storage/perf/ (for sst)

import sst

TERMINAL_OK = "SUCCESS"
# The per-group settings --groups may give. Each workload setting defaults to the option of the
# same name; the floors (min_mbps, min_iops) apply to that group only and default to none.
WORKLOAD_KEYS = ("nodes", "io_sizes", "threads", "iodepth", "duration")
FLOOR_KEYS = ("min_mbps", "min_iops")
GROUP_KEYS = frozenset(WORKLOAD_KEYS + FLOOR_KEYS)
# A batch makes one launcher call per group plus one to start it, all under the checkout lock, so
# another run waits for up to that many calls.
MAX_GROUPS = 20
SUBMIT_WAIT_S = (MAX_GROUPS + 1) * (sst.COMMAND_TIMEOUT_S + sst.TERM_GRACE_S + sst.DRAIN_S)
REFUSED = "REFUSED"
AUTO = "auto"
# Submit fails in this phase while an earlier sweep still holds the claim's Lease.
CLAIM_LEASE_PHASE = "pvc-lease"
SETTINGS_HINT = (
    "storage-scale-test names its own variables (ELBENCHO_*); the perf_* settings set them "
    "(see isvctl/configs/shared/storage/perf/README.md#settings)"
)
GROUP_HINT = (
    "a group's keys set storage-scale-test's variables of the same kind (io_sizes sets ELBENCHO_SCALE_IO_SIZES, "
    "and so on; see isvctl/configs/shared/storage/perf/README.md#batches)"
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
    parser.add_argument(
        "--groups",
        default="",
        help=(
            "Run a storage-scale-test batch: a JSON list of groups, each an object with any of "
            f"{', '.join(sorted(GROUP_KEYS))}; a key a group leaves out takes the matching option "
            "above. Empty (the default) runs one plain sweep from the options above."
        ),
    )
    return parser.parse_args(argv)


class Group(NamedTuple):
    """One group of a batch, or the whole of a plain sweep: what storage-scale-test sweeps."""

    nodes: str
    io_sizes: str
    threads: str
    iodepth: str
    duration: int
    min_mbps: float | None = None
    min_iops: float | None = None

    def cells(self) -> int:
        """The group's cells (node counts x IO sizes x threads x IO depths)."""
        return sst.count_cells(self.nodes, self.io_sizes.split(","), self.threads.split(","), self.iodepth.split(","))


def _groups(args: argparse.Namespace) -> list[Group]:
    """The sweep's groups: one per --groups entry, or the options above alone without --groups.

    A group's workload key that is missing or null takes the option of the same name. Raises
    ValueError for a --groups value that is not a list of 1 to MAX_GROUPS objects with known keys
    and usable values.
    """
    defaults = {key: getattr(args, key) for key in WORKLOAD_KEYS}
    if not args.groups.strip():
        return [Group(**defaults)]
    try:
        entries = json.loads(args.groups)
    except ValueError as exc:
        raise ValueError(f"--groups is not JSON: {exc}") from exc
    if not isinstance(entries, list) or not all(isinstance(e, dict) for e in entries):
        raise ValueError("--groups must be a JSON list of objects")
    if not 1 <= len(entries) <= MAX_GROUPS:
        raise ValueError(f"--groups has {len(entries)} groups; a batch takes 1 to {MAX_GROUPS}")
    groups = []
    for index, entry in enumerate(entries, 1):
        unknown = sorted(set(entry) - GROUP_KEYS)
        if unknown:
            raise ValueError(f"--groups entry {index} has unknown keys {unknown}; allowed: {sorted(GROUP_KEYS)}")
        values = dict(defaults)
        for key, value in entry.items():
            if value is not None:
                try:
                    values[key] = _group_value(key, value)
                except ValueError as exc:
                    raise ValueError(f"--groups entry {index}: {key} {exc}") from None
        groups.append(Group(**values))
    return groups


def _group_value(key: str, value: Any) -> Any:
    """One --groups value as this step uses it; ValueError (completing "<key> ...") when unusable.

    Only what the step computes with is checked here (duration, the floors): storage-scale-test
    rejects a bad sweep value (an IO size, thread count, ...) itself before it touches the cluster.
    """
    if key == "duration":
        if isinstance(value, bool) or not _is_whole_number(str(value)) or int(value) < 1:
            raise ValueError(f"is {json.dumps(value)}; give a whole number of seconds, 1 or more")
        return int(value)
    if key in FLOOR_KEYS:
        try:
            floor = None if isinstance(value, bool) else float(value)
        except (TypeError, ValueError):
            floor = None
        if floor is None or not math.isfinite(floor) or floor < 0:
            raise ValueError(f"is {json.dumps(value)}; give a number, 0 or more")
        return floor
    return ",".join(str(item) for item in value) if isinstance(value, list) else str(value)


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


def _plan(args: argparse.Namespace, groups: list[Group], progress: sst.ProgressLog) -> tuple[float, str | None]:
    """Count the sweep's cells and set the deadline; return (deadline_s, problem)."""
    per_group = [(group.cells(), group.duration) for group in groups]
    cells = sum(n for n, _ in per_group)
    estimate = sst.estimate_s(per_group)
    deadline_s = sst.auto_wait_s(estimate) if args.wait_timeout == AUTO else int(args.wait_timeout)
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
        "results_dir": "/tmp/isvctl-demo/elbencho-demo",
        "executions": {"total": 2, "succeeded": 2, "failed": 0},
        "resources_released": True,
    }


def _write_inputs(args: argparse.Namespace, groups: list[Group], workdir: Path) -> tuple[Path, list[Path]]:
    """Write the target file (what to test) and one workload override file (how to test) per group.

    Workload files an earlier run left in *workdir* are removed first, so the inputs there are
    always this run's.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    for stale in workdir.glob("workload*.env"):
        stale.unlink()
    target = workdir / "target.env"
    target.write_text(sst.test_dirs(args.claim) + "\n", encoding="utf-8")
    if not args.groups.strip():
        return target, [_write_workload(args, groups[0], workdir / "workload.env")]
    return target, [
        _write_workload(args, group, workdir / f"workload-{index:02d}.env") for index, group in enumerate(groups, 1)
    ]


def _write_workload(args: argparse.Namespace, group: Group, workload: Path) -> Path:
    """Write one group's workload override file."""
    workload.write_text(
        "\n".join(
            [
                f"ELBENCHO_SCALE_IO_SIZES={sst.bash_array(group.io_sizes.split(','))}",
                f"ELBENCHO_SCALE_THREAD_LIST={sst.bash_array(group.threads.split(','))}",
                f"ELBENCHO_IODEPTH_LIST={sst.bash_array(group.iodepth.split(','))}",
                f"ELBENCHO_SCALE_READ_WRITE_DURATION={group.duration}",
                f"ELBENCHO_LIVEINT={args.live_interval_ms}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return workload


def _submit(
    args: argparse.Namespace,
    groups: list[Group],
    base: Path,
    env: dict[str, str],
    progress: sst.ProgressLog,
) -> tuple[str | None, str, list[str]]:
    """Write the inputs and submit the sweep, or prepare and start a batch (--groups).

    Returns (sweep or batch directory, error, each batch group's ID as storage-scale-test numbered it).
    """
    sst.install_env_sh(base, sst.env_settings(args.results_dir, args.node_selector))
    target, workloads = _write_inputs(args, groups, Path(args.results_dir))
    launcher = str(base / sst.LAUNCHER)
    if not args.groups.strip():
        cmd = [launcher, "--target", str(target), "--env-override", str(workloads[0]), "--nodes", groups[0].nodes]
        sweep_dir, error, _ = _call(cmd, env, progress, "submit", "STORAGE_SCALE_TEST_RESULTS_DIR")
        return sweep_dir, error, []
    # A batch: --batch makes a draft with the first group, --append adds the rest, --start runs them.
    # A draft holds nothing on the cluster, so a failure before --start leaves nothing to release.
    batch_dir: str | None = None
    group_ids: list[str] = []
    for index, (group, workload) in enumerate(zip(groups, workloads, strict=True)):
        where = [launcher, "--batch"] if batch_dir is None else [launcher, "--append", batch_dir]
        cmd = [*where, "--target", str(target), "--env-override", str(workload), "--nodes", group.nodes]
        event = where[1].removeprefix("--")
        printed_dir, error, printed = _call(
            cmd, env, progress, event, sst.BATCH_RESULTS_KEY, label=f"group {index + 1} of {len(groups)} ({event})"
        )
        batch_dir = batch_dir or printed_dir
        if error:
            return batch_dir, error, group_ids
        group_ids.append(printed.get(sst.BATCH_GROUP_KEY, f"{index + 1:04d}"))
    # --start checks the cluster and creates the attempt; it takes about a minute and prints nothing until then.
    progress.event("starting", sweep_dir=batch_dir)
    started, error, _ = _call(
        [launcher, "--start", str(batch_dir)], env, progress, "start", "STORAGE_SCALE_TEST_RESULTS_DIR"
    )
    return started or batch_dir, error, group_ids


def _call(
    cmd: list[str], env: dict[str, str], progress: sst.ProgressLog, event: str, key: str, label: str = ""
) -> tuple[str | None, str, dict[str, str]]:
    """Run one submitting launcher call; return (the directory it printed as *key*, error, all it printed).

    *label* names the call in the error (a batch's group, say); it defaults to *event*.
    """
    sst.log(f"{event}: " + " ".join(cmd))
    proc = sst.run(cmd, sst.COMMAND_TIMEOUT_S, env)
    printed = sst.parse_kv(proc.stdout)
    sweep_dir = printed.get(key)
    progress.event(event, rc=proc.returncode, sweep_dir=sweep_dir)
    if proc.returncode == 0 and sweep_dir:
        return sweep_dir, "", printed
    sst.log(proc.stdout[-2000:])
    sst.log(proc.stderr[-4000:])
    error = f"{label or event} exited {proc.returncode}: {sst.describe_failure(proc.stderr)}"
    diagnostic = sst.diagnostics(proc.stderr)
    if diagnostic.get("PHASE") == CLAIM_LEASE_PHASE:
        # storage-scale-test suggests retrying, but a retry fails until the earlier sweep is released.
        error += f"; {CLAIM_HELD_HINT}"
    elif not diagnostic:
        # No diagnostic block: storage-scale-test's settings checks print only Error: lines.
        error += f"; {GROUP_HINT if label else SETTINGS_HINT}"
    return sweep_dir, error, printed


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
    try:
        groups = _groups(args)
    except ValueError as exc:
        result["error"] = str(exc)
        return
    deadline_s, problem = _plan(args, groups, progress)
    if problem:
        result["error"] = problem
        return
    base = sst.resolve_base(args.scale_test_base)
    if not (base / sst.LAUNCHER).is_file():
        result["error"] = sst.missing_tree(base)
        return
    # Held until the step ends: a teardown never acts on a sweep this step is still running.
    lock = sst.run_lock(args.results_dir)
    if lock is None:
        result["state"] = REFUSED
        result["error"] = f"another run or teardown is using {args.results_dir}"
        return
    try:
        _run_locked(args, groups, base, deadline_s, result, progress)
    finally:
        sst.unlock(lock)


def _run_locked(
    args: argparse.Namespace,
    groups: list[Group],
    base: Path,
    deadline_s: float,
    result: dict[str, Any],
    progress: sst.ProgressLog,
) -> None:
    """Submit, wait, cancel at the deadline, and collect; _run_sweep holds the lock."""
    env = sst.child_env(args.kubeconfig)
    # env.sh lives in the checkout, which runs from other working directories may share, and
    # the submitting calls (a submit, or a batch's --batch, --appends and --start) read it.
    checkout = sst.checkout_lock(base, SUBMIT_WAIT_S)
    if checkout is None:
        result["state"] = REFUSED
        result["error"] = f"another run is still submitting from {base}; try again once it has"
        return
    try:
        sweep_dir, error, group_ids = _submit(args, groups, base, env, progress)
    finally:
        sst.unlock(checkout)
    if sweep_dir:
        result["results_dir"] = sweep_dir
    if group_ids:
        # The evaluate step reports each group, and judges it against its own floors, by these.
        result["groups"] = [
            {"group_id": gid, **{k: v for k, v in group._asdict().items() if v is not None}}
            for gid, group in zip(group_ids, groups, strict=False)
        ]
    if error or not sweep_dir:
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
        if not finished:
            # Recorded even when the sweep turns out to have finished first, which the cancel then finds.
            result["deadline_reached"] = True
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
            reason = result.get("error") or f"sweep was cancelled outside this run; see {sweep_dir}"
            result["error"] = f"{reason}; {sst.cancel_failed(base, sweep_dir)}"
            return
    collected = sst.collect(base, sweep_dir, env, progress=progress)
    result["resources_released"] = collected["resources_released"]
    # The status that confirmed collect is the final view; asking again adds nothing. After a
    # TIMEOUT it has the counts of what finished before the cancel, and TIMEOUT stays the state.
    if collected.get("final_status"):
        state = result.get("state")
        _finish(result, collected["final_status"])
        if not finished:
            result["state"] = state
    result["success"] = finished and result.get("state") == TERMINAL_OK and collected["resources_released"]
    if not result["success"]:
        ended = "was cancelled outside this run" if cancelled_outside else f"ended {result.get('state')}"
        reason = result.get("error") or f"sweep {ended}; see {sweep_dir}"
        result["error"] = "; ".join([reason, *_collect_details(collected)])


def _collect_details(collected: dict[str, Any]) -> list[str]:
    """What collect said about a failed sweep: its first failed execution, and how to release it."""
    details = []
    if "failed_execution" in collected:
        details.append(f"first failed execution {collected['failed_execution']}, log {collected.get('failed_log')}")
    if "collect_error" in collected:
        details.append(collected["collect_error"])
    return details


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
    if result["state"] in ("NOT_SUBMITTED", REFUSED) and result.get("error"):
        # isvctl shows a failed step's JSON only in its report, so the log also says why nothing ran.
        progress.event(f"{result['state']}: {result['error']}")
    return sst.emit(result)


if __name__ == "__main__":
    sys.exit(main())
