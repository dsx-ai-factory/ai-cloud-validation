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

--wait-timeout auto (the default) sizes the wait from the sweep's cells and phase duration,
and extends it if cells take longer than estimated, up to --wait-cap; a number waits that
many seconds; 0 sets no limit. In every mode the wait also ends when the sweep makes no
progress for --stall-timeout, when its state cannot be read for --unreachable-grace (the
sweep runs on in-cluster, so a short outage is waited out), or when storage-scale-test
repeatedly fails to report it. The run then cancels and collects the sweep, except when the
cluster cannot be reached at all: it then fails with UNREACHABLE and leaves the sweep to the
teardown step. Before submitting, it refuses a sweep whose live CSVs or file count would
exceed what storage-scale-test collects.

The step needs no isvctl timeout: every storage-scale-test call has its own limit
(--command-timeout, --status-timeout, and --collect-timeout for all of collect), and a call
that hits it is stopped with everything it started. Each status check, cancel and collect is
logged to a run log under --run-log-dir, whose path the result reports.

The sweep's handle is written to --state-file as soon as it is submitted. If this step is
interrupted before it collects, the teardown step (release_storage_test.py) reads that file
and cancels and collects instead; an interruption is also recorded beside it (`interrupted`).
The step holds a lock beside the state file while it runs, and refuses (REFUSED) while
another run or teardown holds it, or while the file records an earlier sweep that was never
released: this run's teardown step then releases that sweep. It drives the sweep through its
own copy of --kubeconfig beside the state file (so a later repin of that file cannot move it),
and records the copy and the cluster (context and server) it points at, which teardown checks
before it acts.

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
    "resources_released": true,
    "run_log": "/path/to/results/run-logs/run-20260101T000000Z-1234.log"
}
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
# Smallest accepted values, as (argument, minimum). A status check on a healthy cluster takes
# about 30 s and up to about 100 s. --collect-timeout's minimum depends on --status-timeout
# (see _validate).
MINIMUMS = (
    ("--status-timeout", 300),
    ("--interval", 15),
    ("--command-timeout", 60),
    ("--duration", 1),
    ("--wait-cap", sst.WAIT_FLOOR_S),
    ("--live-interval-ms", 1),
)
AUTO = "auto"
# Arguments in seconds, each at most sst.MAX_SECONDS (30 days).
SECONDS = (
    "--duration",
    "--wait-timeout",
    "--wait-cap",
    "--stall-timeout",
    "--unreachable-grace",
    "--interval",
    "--command-timeout",
    "--status-timeout",
    "--collect-timeout",
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
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
    parser.add_argument(
        "--wait-timeout",
        default=AUTO,
        help="Seconds to wait for the sweep: auto (sized from the sweep), N, or 0 (no limit)",
    )
    parser.add_argument("--wait-cap", type=int, default=43200, help="Longest wait auto may choose, in seconds")
    parser.add_argument(
        "--stall-timeout", default=AUTO, help="Seconds without progress before the sweep counts as stuck (auto or N)"
    )
    parser.add_argument(
        "--unreachable-grace", type=int, default=1800, help="Seconds the sweep's state may be unreadable while waiting"
    )
    parser.add_argument("--live-interval-ms", type=int, default=1000, help="elbencho live statistics interval (ms)")
    parser.add_argument(
        "--live-csv-extended",
        type=_switch,
        default=1,
        help="1 (or true) writes per-interval live CSVs (needed for fairness), 0 (or false) not",
    )
    parser.add_argument("--interval", type=int, default=60, help="Seconds between status checks")
    parser.add_argument(
        "--command-timeout", type=int, default=600, help="Seconds for each submit and cancel call to storage-scale-test"
    )
    parser.add_argument("--status-timeout", type=int, default=300, help="Seconds for each status check")
    parser.add_argument(
        "--collect-timeout", type=int, default=8400, help="Seconds for collect, all retries and checks together"
    )
    parser.add_argument(
        "--run-log-dir", default="", help="Where to write the run log (default: <results-dir>/run-logs)"
    )
    return parser.parse_args(argv)


def _switch(value: str) -> int | str:
    """A 0/1 argument; true and false (any case, as YAML booleans render) mean 1 and 0.

    Anything else is returned as given, for _validate to name.
    """
    return {"1": 1, "0": 0, "true": 1, "false": 0}.get(value.strip().lower(), value)


def _validate(args: argparse.Namespace) -> str | None:
    """Why the arguments cannot run, or None when they can."""
    if not args.node_selector.strip():
        return (
            "--node-selector is empty; the sweep runs only on labelled nodes: set STORAGE_PERF_NODE_SELECTOR "
            "(the perf_node_selector setting), e.g. storage-test=true"
        )
    for flag, minimum in MINIMUMS:
        value = getattr(args, flag.removeprefix("--").replace("-", "_"))
        if value < minimum:
            return f"{flag} is {value}; it must be at least {minimum}"
    # Longer limits are mistakes, and a huge one would overflow the wait's clock arithmetic.
    for flag in SECONDS:
        value = getattr(args, flag.removeprefix("--").replace("-", "_"))
        if (isinstance(value, int) or _is_seconds(value)) and int(value) > sst.MAX_SECONDS:
            return f"{flag} is {value}; it must be at most {sst.MAX_SECONDS} (30 days)"
    # Collect needs room for one status check, its reserve and the 60 s storage-scale-test
    # accepts as its own deadline.
    reserve = sst.COLLECT_RESERVE_S + sst.MIN_UPSTREAM_COLLECT_S
    if args.collect_timeout < args.status_timeout + reserve:
        return (
            f"--collect-timeout is {args.collect_timeout}; it must be at least "
            f"{args.status_timeout + reserve} (--status-timeout + {reserve})"
        )
    if args.wait_timeout != AUTO and not _is_seconds(args.wait_timeout):
        return f"--wait-timeout is {args.wait_timeout}; it must be auto or a number of seconds, at least 0"
    if args.stall_timeout != AUTO and not (_is_seconds(args.stall_timeout) and int(args.stall_timeout) > 0):
        return f"--stall-timeout is {args.stall_timeout}; it must be auto or a number of seconds, at least 1"
    # Two missed polls and one slow status check are not an outage.
    grace = 2 * args.interval + args.status_timeout
    if args.unreachable_grace < grace:
        return (
            f"--unreachable-grace is {args.unreachable_grace}; it must be at least {grace} "
            "(2 x --interval + --status-timeout)"
        )
    if args.live_csv_extended not in (0, 1):
        return f"--live-csv-extended is {args.live_csv_extended}; it must be 0 or 1"
    # The size estimate reads every list; storage-scale-test would refuse an empty entry anyway.
    for flag, numbers in (("--io-sizes", False), ("--threads", True), ("--iodepth", True)):
        value = getattr(args, flag.removeprefix("--").replace("-", "_"))
        entries = value.split(",")
        if not all(entries) or (numbers and not all(_is_seconds(e) and int(e) > 0 for e in entries)):
            kind = "positive whole numbers" if numbers else "entries"
            return f"{flag} is {value!r}; it must be a comma-separated list of {kind}, with no empty entry"
    return None


def _is_seconds(value: str) -> bool:
    """True for a whole number written in ASCII digits (``00`` is 0)."""
    return re.fullmatch(r"[0-9]+", value) is not None


def _plan(args: argparse.Namespace, run_log: sst.RunLog) -> tuple[sst.WaitPlan, str | None, str | None]:
    """Size the wait and the collection before submitting; return (plan, refusal, warning)."""
    io_sizes, threads, iodepths = args.io_sizes.split(","), args.threads.split(","), args.iodepth.split(",")
    # Counted first, so an absurd sweep is refused without listing its node counts or estimating it.
    cells = sst.count_cells(args.nodes, io_sizes, threads, iodepths)
    too_many = cells is not None and cells > sst.MAX_CELLS
    node_counts = None if too_many else sst.parse_node_spec(args.nodes)
    auto = args.wait_timeout == AUTO
    initial = sst.initial_estimate(cells, args.duration) if cells is not None else None
    size, members = None, None
    if node_counts is not None:
        size, members = sst.estimate_collection(
            node_counts, io_sizes, threads, iodepths, args.duration, args.live_interval_ms, args.live_csv_extended == 1
        )
    refusal = warning = None
    remedies = "raise --live-interval-ms, set --live-csv-extended 0 (no live CSVs), or shrink the sweep"
    if too_many:
        refusal = (
            f"the sweep has {cells} cells; storage-scale-test collects at most 50,000 files, "
            f"which allows {sst.MAX_CELLS} cells. Shrink the sweep"
        )
    elif auto and initial is not None and initial > args.wait_cap:
        refusal = (
            f"the sweep needs about {initial} s, more than --wait-cap {args.wait_cap} s; "
            "raise the cap, set --wait-timeout to a number of seconds (0 = no limit), or shrink the sweep"
        )
    elif size is not None and size > sst.REFUSE_BYTES:
        refusal = (
            f"the sweep's live CSVs would be about {size / (1 << 30):.2f} GiB, above the "
            f"{sst.REFUSE_BYTES / (1 << 30):.1f} GiB limit (storage-scale-test collects at most 2 GiB); {remedies}"
        )
    elif size is not None and size > sst.WARN_BYTES:
        warning = (
            f"the sweep's live CSVs would be about {size / (1 << 30):.2f} GiB of the 2 GiB "
            f"storage-scale-test collects; {remedies}"
        )
        sst.log(f"warning: {warning}")
    # Any zero (0, 00) sets no limit.
    wait_s = None if auto or int(args.wait_timeout) == 0 else int(args.wait_timeout)
    run_log.event(
        "estimate",
        cells=cells,
        initial_s=initial,
        deadline_s=round(sst.auto_deadline_s(initial, args.wait_cap)) if auto else wait_s,
        bytes=size,
        members=members,
        refusal=refusal,
        warning=warning,
    )
    plan = sst.WaitPlan(
        auto=auto,
        wait_s=wait_s,
        cap_s=args.wait_cap,
        stall_s=None if args.stall_timeout == AUTO else float(args.stall_timeout),
        grace_s=args.unreachable_grace,
        duration_s=args.duration,
        cells=cells,
        interval_s=args.interval,
        status_timeout=args.status_timeout,
    )
    return plan, refusal, warning


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
                # The extended live CSVs (per-interval, per-client samples) feed
                # storage-scale-test's fairness reporting, and are most of what collect copies.
                f"ELBENCHO_LIVEINT={args.live_interval_ms}",
                f"ELBENCHO_LIVE_CSV_EXTENDED={args.live_csv_extended}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return target, workload


def _submit(
    args: argparse.Namespace, base: Path, env: dict[str, str], run_log: sst.RunLog
) -> tuple[str | None, str | None, str]:
    """Submit the sweep; return (results_dir, attempt, error)."""
    test_dir_key = f"{args.claim}:{args.test_path}"
    sst.install_env_sh(base, _env_settings(args, test_dir_key))
    target, workload = _write_inputs(args, Path(args.state_file).resolve().parent, test_dir_key)
    cmd = [*sst.launcher(base), "--target", str(target), "--env-override", str(workload), "--nodes", args.nodes]
    sst.log("submitting: " + " ".join(cmd))
    began = sst.CLOCK.monotonic()
    proc = sst.run(cmd, args.command_timeout, env)
    printed = sst.parse_kv(proc.stdout)
    results_dir = printed.get("STORAGE_SCALE_TEST_RESULTS_DIR")
    run_log.event(
        "submit",
        rc=proc.returncode,
        seconds=round(sst.CLOCK.monotonic() - began, 1),
        results_dir_name=Path(results_dir).name if results_dir else None,
        attempt=printed.get("STORAGE_SCALE_TEST_ATTEMPT_ID"),
    )
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


def _cancel(
    args: argparse.Namespace, base: Path, results_dir: str, env: dict[str, str], run_log: sst.RunLog, attempts: int = 3
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


def _refuse(result: dict[str, Any], run_log: sst.RunLog, refusal: str) -> None:
    """Fail before submitting, and say why in the run log too."""
    result["error"] = refusal
    run_log.event("refused", refusal=refusal)


def _run_sweep(args: argparse.Namespace, result: dict[str, Any], run_log: sst.RunLog) -> None:
    """Submit, wait and collect, filling in *result*.

    Refusals come before the state file is written, so a refused run leaves an earlier
    run's record as it was, for its own teardown to release.
    """
    invalid = _validate(args)
    if invalid:
        _refuse(result, run_log, invalid)
        return
    plan, refusal, warning = _plan(args, run_log)
    if refusal:
        # The estimate line already carries it.
        result["error"] = refusal
        return
    if warning:
        result["warning"] = warning
    base = Path(args.scale_test_base).resolve()
    if not (base / sst.LAUNCHER).is_file():
        _refuse(result, run_log, f"{base} is not a storage-scale-test checkout (no {sst.LAUNCHER})")
        return
    # Held until the step ends: a teardown never acts on a sweep this step is still running.
    lock = sst.state_lock(args.state_file)
    if lock is None:
        holder = sst.describe_owner(sst.peek_state(args.state_file))
        result["state"] = REFUSED
        _refuse(
            result,
            run_log,
            f"another run or teardown is using {args.state_file}"
            + (f" (its sweep was recorded by {holder})" if holder else ""),
        )
        return
    try:
        _run_locked(args, base, plan, result, run_log)
    finally:
        sst.unlock(lock)


def _run_locked(
    args: argparse.Namespace, base: Path, plan: sst.WaitPlan, result: dict[str, Any], run_log: sst.RunLog
) -> None:
    """_run_sweep, once this step holds the state file's lock."""
    try:
        state = sst.read_state(args.state_file)
        earlier = sst.needs_release(state)
    except sst.StateError as exc:
        result["state"] = REFUSED
        _refuse(result, run_log, sst.state_error_message(exc))
        return
    if earlier is not None:
        # Overwriting the record would leave nothing to release that sweep with.
        result["state"] = REFUSED
        _refuse(result, run_log, sst.unreleased_message(earlier, args.state_file))
        return
    # The sweep runs on this run's own copy of the pinned kubeconfig (see sst.snapshot_kubeconfig).
    try:
        snapshot = sst.snapshot_kubeconfig(args.kubeconfig, args.state_file) if args.kubeconfig else ""
    except OSError as exc:
        _refuse(result, run_log, f"cannot copy the kubeconfig {args.kubeconfig}: {exc.strerror or exc}")
        return
    env = sst.child_env(snapshot)
    cluster = sst.cluster_identity(env)
    if cluster is None:
        sst.log("warning: cannot read the kubeconfig's context and server; teardown will not check them")
    # env.sh lives in the checkout, which runs from other working directories may share; it is
    # read only at submit.
    checkout = sst.checkout_lock(base, args.command_timeout + sst.TERM_GRACE_S + sst.DRAIN_S)
    if checkout is None:
        sst.remove_snapshot({"kubeconfig_snapshot": snapshot}, args.state_file)
        result["state"] = REFUSED
        _refuse(result, run_log, f"another run is still submitting from {base}; try again once it has")
        return
    try:
        # Recorded before submitting, replacing the earlier (released) record: submit takes the
        # claim's Lease before it prints the results directory, so a submit killed part-way must
        # still leave teardown a way to find the sweep (the new elbencho-* directory under the
        # root).
        root = Path(args.results_dir).resolve()
        pending = {
            "scale_test_base": str(base),
            "kubeconfig": args.kubeconfig,
            "kubeconfig_snapshot": snapshot,
            "results_root": str(root),
            "existing": sorted(p.name for p in root.glob("elbencho-*")),
            "collected": False,
            "run_log": str(run_log.path),
            "owner": sst.owner(),
            "cluster": cluster,
        }
        sst.remove_marker(args.state_file)
        # The record replaced holds nothing (needs_release), so neither does its snapshot.
        sst.remove_snapshot(state, args.state_file)
        sst.write_state(args.state_file, pending)
        sst.on_interrupt(lambda: sst.write_marker(args.state_file, result.get("results_dir")))
        results_dir, attempt, error = _submit(args, base, env, run_log)
    finally:
        sst.unlock(checkout)
    handle = {**pending, "results_dir": results_dir, "attempt": attempt}
    if results_dir:
        result["results_dir"] = results_dir
        sst.write_state(args.state_file, handle)
    if error:
        result["error"] = error
        if not results_dir:
            return

    outcome = sst.wait_for_terminal(base, results_dir, env, plan=plan, run_log=run_log)
    status = outcome.status
    _finish(result, status)
    if outcome.verdict == "unreachable":
        # Cancel and collect would each use up their timeout; teardown tries again.
        result["state"] = "UNREACHABLE"
        result["resources_released"] = False
        result["error"] = (
            f"cannot reach the cluster to read the sweep's state "
            f"({status.get('DIAGNOSTIC_REASON') or 'status check timed out'}); it may still be running and "
            f"holds the claim. Teardown tries to release it; once access is back, run the same command with "
            f"--phase teardown, or run `{base / sst.LAUNCHER} --status {results_dir}`, then `--cancel` (until it "
            "stops) if it is still running, then `--collect`"
        )
        return
    finished = outcome.verdict == "terminal"
    # Whether every cancel this step made completed; collect still runs after one that did not,
    # since the sweep may have stopped on its own and collect checks for itself.
    stopped = True
    # After the cluster was out of reach, or with a status that keeps failing: one try each.
    attempts = 1 if outcome.verdict == "status_failed" or (outcome.probed and not finished) else 3
    if outcome.verdict == "deadline":
        result["state"] = "TIMEOUT"
        if outcome.why == "cap":
            limit = f"--wait-cap {args.wait_cap}s (the sweep needs about {outcome.estimate_s:.0f}s)"
        elif args.wait_timeout == AUTO:
            limit = f"--wait-timeout auto ({outcome.deadline_s:.0f}s, at most --wait-cap {args.wait_cap}s)"
        else:
            limit = f"--wait-timeout {outcome.deadline_s:.0f}s"
        result["error"] = f"sweep did not finish within {limit}; cancelling it"
        stopped = _cancel(args, base, results_dir, env, run_log, attempts)
    elif outcome.verdict == "stalled":
        result["state"] = "STALLED"
        result["error"] = (
            f"the sweep made no progress for {outcome.since_progress_s:.0f}s "
            f"(--stall-timeout {outcome.window_s:.0f}s); cancelling it"
        )
        stopped = _cancel(args, base, results_dir, env, run_log, attempts)
    elif outcome.verdict == "status_failed":
        result["state"] = "STATUS_FAILED"
        detail = status.get("DIAGNOSTIC_MESSAGE") or "no reason given"
        result["error"] = (
            f"storage-scale-test could not report the sweep's state ({outcome.why}): {detail}; cancelling it"
        )
        stopped = _cancel(args, base, results_dir, env, run_log, attempts)
    elif status.get("STATE") == sst.CANCEL_REQUESTED:
        # Cancelled from outside this run; finish the cancel so collect can release it.
        result["error"] = "the sweep was cancelled outside this run"
        stopped = _cancel(args, base, results_dir, env, run_log)
    elif status.get("STATE") == sst.SUBMISSION_FAILED:
        result["error"] = f"submission failed in-cluster and was rolled back; see {results_dir}"
        result["resources_released"] = True
        sst.write_state(args.state_file, {**handle, "collected": True})
        sst.remove_snapshot(handle, args.state_file)
        return
    collected = sst.collect(
        base,
        results_dir,
        env,
        collect_timeout=args.collect_timeout,
        status_timeout=args.status_timeout,
        attempts=attempts,
        run_log=run_log,
    )
    result["resources_released"] = collected["resources_released"]
    for key in ("failed_execution", "failed_log", "collect_error"):
        if key in collected:
            result[key] = collected[key]
    if not stopped and not collected["resources_released"]:
        # collect's own remedy (collect again) cannot work while the sweep runs on.
        result["collect_error"] = sst.cancel_failed(
            base, results_dir, "teardown's release_storage_test tries both again"
        )
        result["error"] = f"{result['error']}; the cancel did not stop the sweep"
    # The status that confirmed collect is the final view; asking again adds nothing.
    if finished and collected.get("final_status"):
        _finish(result, collected["final_status"])
    sst.write_state(args.state_file, {**handle, "collected": collected["resources_released"]})
    if collected["resources_released"]:
        sst.remove_snapshot(handle, args.state_file)
    result["success"] = (
        finished and result.get("state") == TERMINAL_OK and collected["resources_released"] and not error
    )
    if not result["success"] and "error" not in result:
        result["error"] = f"sweep ended {result.get('state')}; see {results_dir}"


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
        run_log = sst.RunLog.create(Path(args.run_log_dir or Path(args.results_dir) / "run-logs").resolve())
    except OSError as exc:
        result["error"] = f"cannot create the run log: {exc}"
        return sst.emit(result)
    result["run_log"] = str(run_log.path)
    restore = sst.install_interrupt_handler(run_log)
    try:
        _run_sweep(args, result, run_log)
    except (OSError, ValueError) as exc:
        result["error"] = str(exc)
    finally:
        restore()
        run_log.close()
    return sst.emit(result)


if __name__ == "__main__":
    sys.exit(main())
