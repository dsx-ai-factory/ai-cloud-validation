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

"""run_storage_test: check the arguments, submit, wait, cancel at the deadline, collect."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from .conftest import (
    CANCEL_REQUESTED,
    CLEANUP,
    FINISHED,
    NEVER,
    RESULTS_NAME,
    RUN,
    RUNNING,
    SST,
    SUBMISSION_FAILED,
    FakeClock,
    FakeLauncher,
    InProcess,
    assert_cancelled_before_collect,
    assert_schema,
    diag,
    events,
    hold,
    left_behind,
    lock_is_free,
    ran,
    release_step,
    reply,
    run_step,
)

# --- A sweep that finishes ---


def test_run_submits_waits_and_collects(perf: InProcess, launcher: FakeLauncher) -> None:
    """A finished sweep is collected, and the status that confirmed collect is the final view."""
    rc, out = run_step(perf, launcher.base)

    assert rc == 0, out
    assert out["state"] == "SUCCESS" and out["resources_released"] is True
    assert out["executions"] == {"total": 2, "succeeded": 2, "failed": 0}
    assert out["results_dir"].endswith(RESULTS_NAME)
    assert launcher.operations() == ["submit", "--status", "--status", "--collect", "--status"]
    assert launcher.calls[0].endswith("--nodes 1")
    collect_env = launcher.envs[launcher.operations().index("--collect")]
    assert collect_env[SST.COLLECT_DEADLINE_ENV] == str(SST.COLLECT_DEADLINE_S)
    assert SST.unreleased(launcher.base, str(launcher.base / "results"), {}) == []
    assert_schema("run_storage_test", out)


@pytest.mark.parametrize("state", ["FAILED", "CANCELLED"])
def test_a_sweep_that_ends_unsuccessful_fails_the_step(state: str, perf: InProcess, launcher: FakeLauncher) -> None:
    """A sweep that finished FAILED or CANCELLED is collected, and the step fails naming the state."""
    launcher.statuses = [RUNNING, FINISHED.replace("STATE=SUCCESS", f"STATE={state}")]

    rc, out = run_step(perf, launcher.base)

    assert rc == 1 and out["success"] is False
    assert out["state"] == state and out["resources_released"] is True
    assert out["error"] == f"sweep ended {state}; see {out['results_dir']}"


def test_collect_passes_on_where_the_failed_cells_are(
    perf: InProcess, launcher: FakeLauncher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Collect names the first failed cell and its log; the run step reports both."""
    collect = launcher._collect

    def collect_with_failures(sweep_dir: str) -> Any:
        """Collect, printing the failed cell as storage-scale-test does (FAILED_LOG with %q)."""
        collect(sweep_dir)
        return ran(1, "FAILED_EXECUTION=0002\nFAILED_LOG=/r/logs/0002\\ write.log\n")

    monkeypatch.setattr(launcher, "_collect", collect_with_failures)
    launcher.statuses = [FINISHED.replace("STATE=SUCCESS", "STATE=FAILED")]

    _, out = run_step(perf, launcher.base)

    assert out["error"] == (
        f"sweep ended FAILED; see {out['results_dir']}; first failed execution 0002, log /r/logs/0002 write.log"
    )


def test_run_does_not_collect_a_rolled_back_submission(perf: InProcess, launcher: FakeLauncher) -> None:
    """SUBMISSION_FAILED means storage-scale-test already removed what submit created."""
    launcher.statuses = [SUBMISSION_FAILED]

    rc, out = run_step(perf, launcher.base)

    assert rc == 1 and out["state"] == "SUBMISSION_FAILED" and out["resources_released"] is True
    assert out["error"] == f"submission failed in-cluster and was rolled back; see {out['results_dir']}"
    assert "--collect" not in launcher.operations()


# --- Waiting ---


@pytest.mark.parametrize(
    ("failure", "kind"), [("FAIL", "unreachable"), ("FAIL:POD_UNSCHEDULABLE", "failed")], ids=["unreachable", "other"]
)
def test_a_poll_without_a_readable_status_waits_for_the_next(
    failure: str, kind: str, perf: InProcess, launcher: FakeLauncher
) -> None:
    """The sweep runs on in-cluster, so a failed status check is logged and the wait goes on."""
    launcher.statuses = [RUNNING, failure, failure, FINISHED]

    rc, out = run_step(perf, launcher.base)

    assert rc == 0, out
    assert [p["kind"] for p in events(out, "poll")] == ["ok", kind, kind, "ok"]
    assert "--cancel" not in launcher.operations()


@pytest.mark.parametrize("failure", ["FAIL", "SILENT"], ids=["unreachable", "no-reason"])
def test_unreadable_status_still_ends_at_the_deadline(failure: str, perf: InProcess, launcher: FakeLauncher) -> None:
    """With no readable status, the deadline ends the wait; the sweep is then cancelled and collected."""
    launcher.statuses = [RUNNING, failure]

    rc, out = run_step(perf, launcher.base, "--wait-timeout", "600")

    assert rc == 1 and out["state"] == "TIMEOUT" and out["resources_released"] is True
    assert out["error"] == "sweep did not finish within 600s (--wait-timeout 600)"
    assert len(events(out, "poll")) == 600 // SST.POLL_INTERVAL_S + 1
    assert_cancelled_before_collect(launcher)


def test_each_poll_writes_one_progress_log_line(perf: InProcess, launcher: FakeLauncher) -> None:
    """The progress log, under the results root, has one numbered line per status check."""
    launcher.statuses = [RUNNING, RUNNING, FINISHED]

    rc, out = run_step(perf, launcher.base)

    assert rc == 0, out
    assert Path(out["progress_log"]).parent == launcher.base / "results" / "run-logs"
    lines = Path(out["progress_log"]).read_text().splitlines()
    assert [line.split("  ")[2] for line in lines] == ["estimate", "submit", "poll", "poll", "poll", "collect"]
    polls = events(out, "poll")
    assert [p["n"] for p in polls] == ["1", "2", "3"]
    assert (polls[0]["state"], polls[0]["next_action"], polls[2]["next_action"]) == ("RUNNING", "WAIT", "COLLECT")


def test_a_poll_gives_up_on_a_busy_lock_sooner_than_cancel(
    checkout: Path, launcher: FakeLauncher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A wait-loop status check retries a busy lock for POLL_LOCK_WINDOW_S, then the next poll tries."""
    clock = FakeClock()
    monkeypatch.setattr(SST, "CLOCK", clock)
    launcher.lock_busy = NEVER

    kind, _ = SST.query(checkout, str(left_behind(checkout)), {})

    assert kind == "failed" and clock.elapsed == SST.POLL_LOCK_WINDOW_S


# --- The deadline: cancel, then collect ---


def test_the_wait_never_sleeps_past_the_deadline(perf: InProcess, launcher: FakeLauncher) -> None:
    """A 5 s deadline is checked 5 s after the first poll, not a whole poll interval later."""
    sleeps: list[float] = []

    class Recording(FakeClock):
        def sleep(self, seconds: float) -> None:
            """Record each sleep, then advance."""
            sleeps.append(seconds)
            super().sleep(seconds)

    perf.clock = Recording()
    launcher.statuses = [RUNNING]

    rc, out = run_step(perf, launcher.base, "--wait-timeout", "5")

    assert rc == 1 and out["state"] == "TIMEOUT"
    assert sleeps[0] == 5 and len(events(out, "poll")) == 2


def test_a_sweep_that_outlives_the_wait_is_cancelled_and_collected(perf: InProcess, launcher: FakeLauncher) -> None:
    """At --wait-timeout the sweep is cancelled, then collected, and the step fails with TIMEOUT."""
    launcher.statuses = [RUNNING]

    rc, out = run_step(perf, launcher.base, "--wait-timeout", "60")

    assert rc == 1 and out["state"] == "TIMEOUT" and out["resources_released"] is True
    # The counts are collect's final view, not the last RUNNING poll (0 succeeded).
    assert out["executions"] == {"total": 2, "succeeded": 2, "failed": 0}
    assert_cancelled_before_collect(launcher)
    assert_schema("run_storage_test", out)


def test_a_sweep_that_finished_before_its_cancel_is_a_normal_finish(
    perf: InProcess, launcher: FakeLauncher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cancel that reaches a sweep already finished leaves it SUCCESS: no TIMEOUT, no error."""
    launcher.statuses = [RUNNING]
    monkeypatch.setattr(launcher, "_cancel", lambda sweep_dir: launcher.statuses.append(FINISHED) or ran())

    rc, out = run_step(perf, launcher.base, "--wait-timeout", "60")

    assert rc == 0, out
    assert out["state"] == "SUCCESS" and "error" not in out
    assert_cancelled_before_collect(launcher)


def test_a_cancel_that_does_not_stop_the_sweep_skips_collect(perf: InProcess, launcher: FakeLauncher) -> None:
    """Collect would refuse a running sweep; the result says to cancel, then collect (or teardown)."""
    launcher.statuses = [RUNNING]
    launcher.cancels_needed = NEVER

    rc, out = run_step(perf, launcher.base, "--wait-timeout", "60")

    assert rc == 1 and out["state"] == "TIMEOUT" and out["resources_released"] is False
    assert "--collect" not in launcher.operations()
    assert launcher.operations().count("--cancel") == SST.CANCEL_ATTEMPTS
    path, sweep_dir = launcher.base / SST.LAUNCHER, out["results_dir"]
    assert (
        out["error"]
        == f"sweep did not finish within 60s (--wait-timeout 60); {SST.cancel_failed(launcher.base, sweep_dir)}"
    )
    assert out["error"].index(f"`{path} --collect {sweep_dir}`") > out["error"].index("--cancel")


@pytest.mark.parametrize(
    ("cancels_needed", "released", "error"),
    [(1, True, "sweep was cancelled outside this run"), (NEVER, False, "sweep was cancelled outside this run")],
    ids=["cancel-completes", "cancel-never-completes"],
)
def test_a_cancel_made_outside_the_run_is_finished_here(
    cancels_needed: int, released: bool, error: str, perf: InProcess, launcher: FakeLauncher
) -> None:
    """A sweep cancelled from elsewhere stops in CANCEL_REQUESTED; the step completes the cancel."""
    launcher.statuses = [CANCEL_REQUESTED]
    launcher.cancels_needed = cancels_needed

    rc, out = run_step(perf, launcher.base)

    assert rc == 1 and out["resources_released"] is released
    assert out["error"].startswith(f"{error}; see {out['results_dir']}")
    assert ("--collect" in launcher.operations()) is released


def test_an_outside_cancel_that_lost_the_race_reports_no_error(
    perf: InProcess, launcher: FakeLauncher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A sweep cancelled from outside that had already finished ends SUCCESS, with no error."""
    launcher.statuses = [CANCEL_REQUESTED]
    monkeypatch.setattr(launcher, "_cancel", lambda sweep_dir: launcher.statuses.append(FINISHED) or ran())

    rc, out = run_step(perf, launcher.base)

    assert rc == 0, out
    assert out["state"] == "SUCCESS" and "error" not in out


# --- Collect ---


def test_collect_retries_while_cleanup_is_pending(perf: InProcess, launcher: FakeLauncher) -> None:
    """A collect can publish the results yet leave cleanup pending; collecting again finishes it."""
    launcher.cleanup_pending = 1

    rc, out = run_step(perf, launcher.base)

    assert rc == 0 and out["resources_released"] is True
    assert launcher.operations().count("--collect") == 2


def test_collect_reports_cleanup_that_never_finishes(perf: InProcess, launcher: FakeLauncher) -> None:
    """After its attempts, collect says the sweep still holds resources and how to release them."""
    launcher.cleanup_pending = NEVER

    rc, out = run_step(perf, launcher.base)

    assert rc == 1 and out["resources_released"] is False
    assert out["error"] == (
        f"sweep ended SUCCESS; see {out['results_dir']}; "
        "the sweep still holds cluster resources (RESULT_COLLECTION=CLEANUP_PENDING); "
        f"run `{launcher.base / SST.LAUNCHER} --collect {out['results_dir']}` to release them"
    )
    assert launcher.operations().count("--collect") == SST.COLLECT_ATTEMPTS


def test_collect_stops_when_storage_scale_test_does_not_ask_for_another(scripted: Any) -> None:
    """A collect whose status is neither COMPLETE nor asks to collect again is not repeated."""
    leftover = reply("STATE=FAILED RESULT_COLLECTION=PENDING NEXT_ACTION=INSPECT")
    fake = scripted({"--collect": [(ran(), 1)], "--status": [(ran(0, leftover), 1)]})

    out = SST.collect(Path("/base"), "/r", {})

    assert [op for op, _ in fake.calls] == ["--collect", "--status"]
    assert out["resources_released"] is False
    assert out["collect_error"].startswith("the sweep still holds cluster resources (RESULT_COLLECTION=PENDING)")


def test_collect_makes_at_most_two_bounded_attempts(scripted: Any) -> None:
    """Each attempt and its confirming status have fixed limits, so collect ends within 8400 s."""
    fake = scripted({"--collect": [(ran(124, timed_out=True), 0)], "--status": [(ran(0, reply(CLEANUP)), 300)]})

    out = SST.collect(Path("/base"), "/r", {})

    assert fake.calls == [("--collect", SST.COLLECT_TIMEOUT_S), ("--status", SST.STATUS_TIMEOUT_S)] * 2
    assert fake.clock.elapsed == 8400
    assert out["resources_released"] is False


def test_collect_says_when_it_could_not_confirm_the_release(scripted: Any) -> None:
    """A collect that no status could confirm says so, rather than that resources are held."""
    scripted({"--collect": [(ran(), 10)], "--status": [(ran(1, stderr=diag("API_UNAVAILABLE")), 1)]})

    out = SST.collect(Path("/base"), "/r", {})

    assert out["resources_released"] is False
    assert out["collect_error"].startswith("could not confirm that collect released the sweep's cluster resources")
    assert "--status /r" in out["collect_error"]


@pytest.mark.parametrize("operation", ["cancel", "collect"])
def test_cancel_and_collect_wait_out_a_busy_lock(
    operation: str, launcher: FakeLauncher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A killed call's leftover can hold the sweep's lock for a while; cancel and collect outlast it."""
    clock = FakeClock()
    monkeypatch.setattr(SST, "CLOCK", clock)
    sweep_dir = str(left_behind(launcher.base))
    launcher.statuses = [FINISHED]
    launcher.lock_busy = 3

    if operation == "cancel":
        assert SST.cancel(launcher.base, sweep_dir, {}) is not None
    else:
        assert SST.collect(launcher.base, sweep_dir, {})["resources_released"] is True
    assert clock.elapsed == 3 * SST.LOCK_RETRY_INTERVAL_S


def test_a_lifecycle_call_gives_up_on_a_lock_held_past_its_window(
    launcher: FakeLauncher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retries stop once the lock window has passed; the refusal is returned."""
    clock = FakeClock()
    monkeypatch.setattr(SST, "CLOCK", clock)
    launcher.lock_busy = NEVER

    result = SST.lifecycle(
        launcher.base, "--cancel", str(left_behind(launcher.base)), {}, timeout_s=60, lock_window_s=SST.LOCK_WINDOW_S
    )

    assert SST.LOCK_BUSY in result.stderr and clock.elapsed == SST.LOCK_WINDOW_S


# --- Before submit: arguments, size, tree ---

WAIT = "it must be auto or a number of seconds from 1 to 43200"
CLAIM = "it must name an existing claim as namespace/claim: set STORAGE_PERF_CLAIM (the perf_claim setting)"
SELECTOR = (
    "the sweep runs only on labelled nodes: set STORAGE_PERF_NODE_SELECTOR (the perf_node_selector setting), "
    "e.g. storage-test=true"
)


@pytest.mark.parametrize(
    ("args", "error"),
    [
        pytest.param(["--node-selector", " "], f"--node-selector is empty; {SELECTOR}", id="node-selector-empty"),
        pytest.param(["--wait-timeout", "0"], f"--wait-timeout is 0; {WAIT}", id="wait-0"),
        pytest.param(["--wait-timeout", "43201"], f"--wait-timeout is 43201; {WAIT}", id="wait-over-12h"),
        pytest.param(["--wait-timeout", "²"], f"--wait-timeout is ²; {WAIT}", id="wait-not-ascii"),
        pytest.param(
            ["--nodes", "1-5625", "--io-sizes", "1M"],
            "the sweep has 5625 cells; storage-scale-test can collect at most 5624",
            id="too-many-cells",
        ),
        pytest.param(
            ["--nodes", "1-2", "--duration", "40000"],
            "the sweep needs about 320170 s, more than the 43200 s a run waits; shrink the sweep",
            id="auto-wait-over-cap",
        ),
    ],
)
def test_a_sweep_that_cannot_work_is_rejected_before_submit(
    args: list[str], error: str, perf: InProcess, launcher: FakeLauncher
) -> None:
    """A bad argument or an oversized sweep is rejected (NOT_SUBMITTED), naming what it accepts."""
    rc, out = run_step(perf, launcher.base, *args)

    assert rc == 1 and out["state"] == "NOT_SUBMITTED"
    assert out["error"] == error
    assert launcher.calls == []
    assert_schema("run_storage_test", out)


def test_a_missing_claim_is_rejected_before_anything_is_logged(perf: InProcess, launcher: FakeLauncher) -> None:
    """perf_claim is required: an empty one is rejected before the plan is logged."""
    rc, out = run_step(perf, launcher.base, "--claim", "")

    assert rc == 1 and out["error"] == f"--claim is empty; {CLAIM}"
    assert Path(out["progress_log"]).read_text() == ""


def test_auto_wait_sizes_the_deadline_from_the_sweep(perf: InProcess, launcher: FakeLauncher) -> None:
    """8 cells of 20 s phases: 150 + 8 x 45 = 510 s estimated, waited for twice that."""
    launcher.statuses = [RUNNING]

    rc, out = run_step(perf, launcher.base, "--nodes", "1-2", "--threads", "1,2", "--duration", "20")

    assert rc == 1 and out["error"] == "sweep did not finish within 1020s (--wait-timeout auto)"
    assert events(out, "estimate") == [{"cells": "8", "estimate_s": "510", "deadline_s": "1020"}]
    assert SST.auto_wait_s(1, 1) == SST.WAIT_FLOOR_S
    assert SST.auto_wait_s(10_000, 1) == SST.WAIT_CAP_S


@pytest.mark.parametrize("how", ["flag", "env"])
def test_run_uses_an_explicit_tree(
    how: str, perf: InProcess, launcher: FakeLauncher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--scale-test-base, or STORAGE_SCALE_TEST_BASE, picks the tree (the fake answers only for its own)."""
    checkout = launcher.base
    monkeypatch.setenv("STORAGE_SCALE_TEST_BASE", str(checkout) if how == "env" else "/not/this/one")
    tree = [f"--scale-test-base={checkout}"] if how == "flag" else []

    rc, out = perf("run_storage_test", *tree, "--claim=a/b", "--node-selector=a=b", f"--results-dir={checkout}/r")

    assert rc == 0, out
    assert launcher.operations()[0] == "submit"


def test_run_says_how_to_vendor_a_missing_release(
    perf: InProcess, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With no vendored release the run fails before submitting, and names the make target."""
    monkeypatch.setattr(SST, "VENDORED", tmp_path / "missing")
    monkeypatch.delenv("STORAGE_SCALE_TEST_BASE", raising=False)

    rc, out = perf("run_storage_test", "--claim=a/b", "--node-selector=a=b", f"--results-dir={tmp_path / 'results'}")

    assert rc == 1 and out["state"] == "NOT_SUBMITTED"
    assert (
        out["error"]
        == f"no storage-scale-test at {tmp_path / 'missing'}; run `make vendor` (or `make vendor-storage-scale-test`)"
    )


def test_run_reports_a_progress_log_it_cannot_create(perf: InProcess, launcher: FakeLauncher, tmp_path: Path) -> None:
    """A results root that cannot hold the progress log fails the step, as JSON, before anything runs."""
    blocked = tmp_path / "file"
    blocked.write_text("")

    rc, out = perf("run_storage_test", "--claim=a/b", "--node-selector=a=b", f"--results-dir={blocked}")

    assert rc == 1 and out["error"].startswith("cannot create the progress log: ")
    assert launcher.calls == []


# --- The inputs storage-scale-test reads ---


def test_run_refuses_an_env_sh_it_did_not_write(perf: InProcess, launcher: FakeLauncher) -> None:
    """A user's own env.sh is never overwritten."""
    (launcher.base / "env.sh").write_text("export EXECUTION_SUBSTRATE=slurm\n")

    rc, out = run_step(perf, launcher.base)

    assert rc == 1 and "not written by isvctl" in out["error"]
    assert (launcher.base / "env.sh").read_text() == "export EXECUTION_SUBSTRATE=slurm\n"
    assert launcher.calls == []
    assert_schema("run_storage_test", out)


def test_run_refuses_an_env_sh_that_is_a_symlink(perf: InProcess, launcher: FakeLauncher, tmp_path: Path) -> None:
    """An env.sh symlink is refused even when it points at isvctl's own output: it would write elsewhere."""
    target = tmp_path / "elsewhere.sh"
    target.write_text(f"{SST.GENERATED_MARKER}\n")
    (launcher.base / "env.sh").symlink_to(target)

    rc, out = run_step(perf, launcher.base)

    assert rc == 1 and "not written by isvctl" in out["error"]
    assert target.read_text() == f"{SST.GENERATED_MARKER}\n"


@pytest.mark.parametrize(
    ("phase", "hint"), [("pvc-lease", True), ("node-discovery", False)], ids=["claim-held", "other-failure"]
)
def test_a_failed_submit_reports_storage_scale_tests_error(
    phase: str, hint: bool, perf: InProcess, launcher: FakeLauncher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The step passes on submit's own error; only a held claim adds what to do about it."""
    monkeypatch.setattr(launcher, "submit", lambda args: ran(1, stderr=diag("API_UNAVAILABLE", phase=phase)))

    rc, out = run_step(perf, launcher.base)

    assert rc == 1 and out["state"] == "NOT_SUBMITTED"
    message = f"submit exited 1: the operation failed (API_UNAVAILABLE, phase {phase})"
    assert out["error"] == (f"{message}; {RUN.CLAIM_HELD_HINT}" if hint else message)


def test_a_rejected_setting_shows_every_error_and_where_settings_come_from(
    perf: InProcess, launcher: FakeLauncher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """storage-scale-test checks the settings itself; its checks print only Error: lines, all kept."""
    errors = (
        "Error: ELBENCHO_SCALE_THREAD_LIST[0]='0' is not a positive integer\n"
        "Error: Elbencho configuration validation failed. Fix the errors above and try again.\n"
    )
    monkeypatch.setattr(launcher, "submit", lambda args: ran(1, stderr=errors))

    rc, out = run_step(perf, launcher.base, "--threads", "0")

    assert rc == 1 and out["state"] == "NOT_SUBMITTED"
    assert out["error"] == (
        "submit exited 1: ELBENCHO_SCALE_THREAD_LIST[0]='0' is not a positive integer; "
        f"Elbencho configuration validation failed. Fix the errors above and try again.; {RUN.SETTINGS_HINT}"
    )


def test_a_submit_over_a_held_claim_is_released_by_its_teardown(
    perf: InProcess, launcher: FakeLauncher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A submit over a held claim fails; its teardown releases the earlier sweep, so the next run goes ahead."""
    checkout = launcher.base
    earlier = left_behind(checkout, "elbencho-20251231Z000000")
    launcher.statuses = [RUNNING]
    submit = launcher.submit

    def held(args: list[str]) -> Any:
        """Fail as submit does over a held claim, leaving a rolled-back sweep directory."""
        launcher.fixed[str(left_behind(checkout, "elbencho-20260101Z999999"))] = SUBMISSION_FAILED
        return ran(1, stderr="Error: PVC perf-ns/perf-claim is reserved\n" + diag("API_UNAVAILABLE", phase="pvc-lease"))

    monkeypatch.setattr(launcher, "submit", held)
    rc, run = run_step(perf, checkout)
    assert rc == 1 and run["error"].endswith(RUN.CLAIM_HELD_HINT)

    rc, release = release_step(perf, checkout)
    assert rc == 0 and release["message"] == f"collected; cluster resources released. Results: {earlier}"

    monkeypatch.setattr(launcher, "submit", submit)
    launcher.statuses = [FINISHED]
    rc, again = run_step(perf, checkout)
    assert rc == 0 and again["state"] == "SUCCESS", again


# --- Isolation: the run lock, the checkout lock, the kubeconfig ---


def test_a_second_run_fails_fast_while_the_first_holds_the_lock(perf: InProcess, launcher: FakeLauncher) -> None:
    """While another run or teardown holds the results root's lock, a run submits and writes nothing."""
    results = launcher.base / "results"
    fd = hold(results / SST.RUN_LOCK)
    try:
        rc, out = run_step(perf, launcher.base)
    finally:
        os.close(fd)

    assert rc == 1 and out["state"] == "REFUSED" and out["resources_released"] is None
    assert out["error"] == f"another run or teardown is using {results}"
    assert perf.clock.elapsed == 0 and launcher.calls == [] and not (launcher.base / "env.sh").exists()
    assert_schema("run_storage_test", out)


@pytest.mark.parametrize("step", ["run_storage_test", "release_storage_test"])
def test_the_run_lock_is_held_for_every_launcher_call(
    step: str, perf: InProcess, launcher: FakeLauncher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each step holds the lock from its first launcher call until after its last, then frees it."""
    lock = launcher.base / "results" / SST.RUN_LOCK
    free: list[bool] = []

    def run(cmd: list[str], timeout: float, env: dict[str, str] | None = None) -> Any:
        """Record whether the run lock is free at each launcher call, then let FakeLauncher answer it."""
        if cmd[0] == str(launcher.base / SST.LAUNCHER):
            free.append(lock_is_free(lock))
        return launcher(cmd, timeout, env)

    monkeypatch.setattr(SST, "run", run)
    if step == "release_storage_test":
        left_behind(launcher.base)
        rc, out = release_step(perf, launcher.base)
    else:
        rc, out = run_step(perf, launcher.base)

    assert rc == 0, out
    assert "--collect" in launcher.operations() and not any(free)
    assert lock_is_free(lock)


class OtherSubmitFinishes(FakeClock):
    """A fake clock on which another run's submit finishes at the first wait."""

    def __init__(self, other: list[int]) -> None:
        """*other* holds the descriptor of the other run's checkout lock."""
        super().__init__()
        self.other = other

    def sleep(self, seconds: float) -> None:
        """Release the other run's checkout lock, then advance the fake time."""
        while self.other:
            os.close(self.other.pop())
        super().sleep(seconds)


def test_a_run_waits_for_another_runs_submit_from_the_same_checkout(
    perf: InProcess, launcher: FakeLauncher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Runs from two working directories share the checkout's env.sh: a run waits for the other's
    submit, then writes env.sh and submits."""
    other = [hold(launcher.base / SST.CHECKOUT_LOCK)]
    perf.clock = OtherSubmitFinishes(other)

    rc, out = run_step(perf, launcher.base)

    assert rc == 0, out
    assert perf.clock.elapsed == SST.LOCK_RETRY_INTERVAL_S + SST.POLL_INTERVAL_S
    assert lock_is_free(launcher.base / SST.CHECKOUT_LOCK)


def test_a_run_gives_up_on_a_busy_checkout_after_one_submit_time(perf: InProcess, launcher: FakeLauncher) -> None:
    """While another run keeps submitting from the checkout, a run waits one submit's time, then
    refuses before writing env.sh."""
    fd = hold(launcher.base / SST.CHECKOUT_LOCK)
    try:
        rc, out = run_step(perf, launcher.base)
    finally:
        os.close(fd)

    assert rc == 1 and out["state"] == "REFUSED"
    assert out["error"] == f"another run is still submitting from {launcher.base.resolve()}; try again once it has"
    assert perf.clock.elapsed >= SST.COMMAND_TIMEOUT_S + SST.TERM_GRACE_S + SST.DRAIN_S
    assert not (launcher.base / "env.sh").exists() and launcher.calls == []


@pytest.mark.parametrize(
    ("args", "kubeconfig"), [([], None), (["--kubeconfig=/pinned"], "/pinned")], ids=["kubectls-own", "pinned"]
)
def test_every_call_uses_the_kubeconfig_it_is_given(
    args: list[str], kubeconfig: str | None, perf: InProcess, launcher: FakeLauncher
) -> None:
    """Submit and every lifecycle command get --kubeconfig as KUBECONFIG; without it, none is set."""
    rc, out = run_step(perf, launcher.base, *args)

    assert rc == 0, out
    assert {env.get("KUBECONFIG") for env in launcher.envs} == {kubeconfig}


def test_a_relative_results_root_and_kubeconfig_are_made_absolute_before_submit(
    perf: InProcess, launcher: FakeLauncher, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """storage-scale-test changes directory, so env.sh, its inputs and KUBECONFIG must not stay relative."""
    workdir = tmp_path / "step-workdir"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    base = launcher.base
    args = ["--claim=perf-ns/perf-claim", "--node-selector=storage-test=true"]

    rc, out = perf(
        "run_storage_test", f"--scale-test-base={base}", *args, "--results-dir=rel-results", "--kubeconfig=cluster.yaml"
    )

    assert rc == 0, out
    root = workdir / "rel-results"
    env_sh = (base / "env.sh").read_text()
    assert f"export RESULTS_DIR={SST.bash_quote(str(root))}\n" in env_sh
    assert f"export LOGS_DIR={SST.bash_quote(str(root / 'logs'))}\n" in env_sh
    submit = launcher.calls[0].split()
    assert submit[1] == str(root / "target.env") and submit[3] == str(root / "workload.env")
    assert out["progress_log"].startswith(str(root / "run-logs"))
    assert {env["KUBECONFIG"] for env in launcher.envs} == {str(workdir / "cluster.yaml")}
