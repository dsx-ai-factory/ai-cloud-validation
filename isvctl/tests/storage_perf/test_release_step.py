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

"""release_storage_test (the teardown step) and the rule it uses, sst.unreleased."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from .conftest import (
    CANCEL_REQUESTED,
    CLEANUP,
    COMPLETE,
    FINISHED,
    MODULES,
    NEVER,
    RUNNING,
    SST,
    SUBMISSION_FAILED,
    FakeLauncher,
    InProcess,
    assert_cancelled_before_collect,
    assert_schema,
    diag,
    hold,
    left_behind,
    ran,
    release_step,
    reply,
)

# --- Which sweeps are unreleased (sst.unreleased) ---


@pytest.mark.parametrize(
    ("status", "unreleased"),
    [
        pytest.param(COMPLETE, False, id="collected"),
        # A collected sweep whose cells failed reports RESUME, not NONE; it still holds nothing.
        pytest.param("STATE=FAILED RESULT_COLLECTION=COMPLETE NEXT_ACTION=RESUME", False, id="collected-failed"),
        # Also what a submit that died part-way (PREPARED) reports: --status rolls it back first.
        pytest.param(SUBMISSION_FAILED, False, id="rolled-back"),
        pytest.param(RUNNING, True, id="running"),
        pytest.param(FINISHED, True, id="finished-not-collected"),
        pytest.param(CLEANUP, True, id="cleanup-pending"),
        pytest.param(CANCEL_REQUESTED, True, id="cancel-requested"),
        pytest.param("FAIL", True, id="unreachable"),
        pytest.param("SILENT", True, id="no-reason"),
    ],
)
def test_a_sweep_is_released_only_once_collected_or_rolled_back(
    status: str, unreleased: bool, launcher: FakeLauncher
) -> None:
    """COMPLETE or SUBMISSION_FAILED hold nothing; anything else may hold the claim."""
    sweep_dir = left_behind(launcher.base)
    launcher.statuses = [status]

    found = SST.unreleased(launcher.base, str(launcher.base / "results"), {})

    assert [d for d, _, _ in found] == ([str(sweep_dir)] if unreleased else [])
    assert launcher.operations() == ["--status"]


@pytest.mark.parametrize(
    ("stdout", "stderr", "unreleased"),
    [
        # Only a successful --status is believed: a failed one that printed a state may hold things.
        pytest.param(reply(SUBMISSION_FAILED), diag("LEDGER_INCONSISTENT"), True, id="failed-but-printed-a-state"),
        # --status checks the claim first, so it fails once the claim is replaced; the state on disk
        # says whether the rollback had finished.
        pytest.param(
            "", diag("IDENTITY_MISMATCH", local_state="SUBMISSION_FAILED"), False, id="claim-gone-rolled-back"
        ),
        pytest.param("", diag("IDENTITY_MISMATCH", local_state="SUBMITTED"), True, id="claim-gone-running"),
        pytest.param("", diag("IDENTITY_MISMATCH", local_state="PREPARED"), True, id="claim-gone-rollback-unfinished"),
    ],
)
def test_a_failed_status_is_judged_by_the_state_on_disk(
    stdout: str, stderr: str, unreleased: bool, checkout: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed --status releases a sweep only when its state on disk says it was rolled back."""
    sweep_dir = left_behind(checkout)
    monkeypatch.setattr(SST, "run", lambda cmd, timeout, env=None: ran(1, stdout, stderr))

    found = SST.unreleased(checkout, str(checkout / "results"), {})

    assert [(d, kind) for d, kind, _ in found] == ([(str(sweep_dir), "failed")] if unreleased else [])


def test_a_directory_without_an_attempt_holds_nothing(launcher: FakeLauncher) -> None:
    """Submit publishes kubernetes/current-attempt before it touches the cluster.

    A sweep directory without it, or anything that is not an elbencho-* directory, is skipped
    without a status check.
    """
    (launcher.base / "results" / "elbencho-20260101Z000000" / "executions").mkdir(parents=True)
    (launcher.base / "results" / "run-logs").mkdir()

    assert SST.unreleased(launcher.base, str(launcher.base / "results"), {}) == []
    assert launcher.calls == []


# --- Release ---


def test_release_with_nothing_held_is_a_no_op(perf: InProcess, checkout: Path) -> None:
    """Nothing was submitted, so there is nothing to release."""
    (checkout / "results").mkdir()

    rc, out = release_step(perf, checkout)

    assert rc == 0 and out["released"] is True
    assert out["message"] == f"no sweep under {checkout / 'results'} holds cluster resources"
    assert_schema("release_storage_test", out)


def test_release_names_a_missing_results_root_without_creating_it(perf: InProcess, checkout: Path) -> None:
    """The run step creates the results root first, so a missing one means nothing ran here.

    The path in the message shows a teardown started from the wrong directory.
    """
    rc, out = release_step(perf, checkout)

    assert rc == 0 and out["released"] is True
    assert out["message"] == f"no results root at {checkout / 'results'}; nothing has run from this directory"
    assert not (checkout / "results").exists()


def test_release_cancels_and_collects_a_sweep_left_running(perf: InProcess, launcher: FakeLauncher) -> None:
    """A run killed before collect leaves its sweep running; teardown cancels it, then collects."""
    launcher.statuses = [RUNNING]
    sweep_dir = left_behind(launcher.base)

    rc, out = release_step(perf, launcher.base)

    assert rc == 0 and out["message"] == f"collected; cluster resources released. Results: {sweep_dir}"
    assert_cancelled_before_collect(launcher)


def test_release_collects_a_finished_sweep_without_cancelling(perf: InProcess, launcher: FakeLauncher) -> None:
    """A finished sweep only needs collecting."""
    launcher.statuses = [FINISHED]
    left_behind(launcher.base)

    rc, out = release_step(perf, launcher.base)

    assert rc == 0 and out["released"] is True
    assert launcher.operations() == ["--status", "--collect", "--status"]


def test_release_finds_an_older_sweep_among_collected_ones(perf: InProcess, launcher: FakeLauncher) -> None:
    """Teardown checks every sweep directory, not only the newest."""
    older = left_behind(launcher.base, "elbencho-20260101Z000000")
    for name in ("elbencho-20260102Z000000", "elbencho-20260103Z000000"):
        launcher.fixed[str(left_behind(launcher.base, name))] = COMPLETE
    launcher.statuses = [FINISHED]

    rc, out = release_step(perf, launcher.base)

    assert rc == 0 and out["message"] == f"collected; cluster resources released. Results: {older}"
    assert launcher.calls_on("--collect") == [str(older)]


def test_release_reports_every_sweep_it_could_not_release(
    perf: InProcess, checkout: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One sweep that will not release does not stop the others; each failure is reported."""
    stuck, fine, also_stuck = (left_behind(checkout, f"elbencho-2026010{n}Z000000") for n in (1, 2, 3))
    collected: set[str] = set()

    def run(cmd: list[str], timeout: float, env: dict[str, str] | None = None) -> Any:
        """Finished sweeps; only *fine* collects, the others keep cleanup pending."""
        operation, sweep_dir = cmd[1], cmd[2]
        if operation == "--collect":
            collected.add(sweep_dir)
            return ran()
        done = sweep_dir in collected
        return ran(stdout=reply(COMPLETE if done and sweep_dir == str(fine) else CLEANUP if done else FINISHED))

    monkeypatch.setattr(SST, "run", run)

    rc, out = release_step(perf, checkout)

    assert rc == 1 and out["released"] is False
    assert collected == {str(stuck), str(fine), str(also_stuck)}
    assert out["error"].count("the sweep still holds cluster resources") == 2
    assert str(stuck) in out["error"] and str(also_stuck) in out["error"] and str(fine) not in out["error"]


def test_release_repeats_a_cancel_left_in_cancel_requested(perf: InProcess, launcher: FakeLauncher) -> None:
    """Cancel is two-phase; storage-scale-test's answer to CANCEL_REQUESTED is to cancel again."""
    launcher.statuses = [RUNNING]
    launcher.cancels_needed = 2
    left_behind(launcher.base)

    rc, out = release_step(perf, launcher.base)

    assert rc == 0 and out["released"] is True
    assert launcher.operations().count("--cancel") == 2


def test_release_cancels_a_sweep_whose_state_is_unreadable(perf: InProcess, launcher: FakeLauncher) -> None:
    """With the state unreadable (but the cluster answering), release tries to cancel first.

    A cancel that does not stop the sweep skips collect, and the error says to cancel, then collect.
    """
    launcher.statuses = ["FAIL:LEDGER_INCONSISTENT"]
    launcher.cancels_needed = NEVER
    sweep_dir = left_behind(launcher.base)

    rc, out = release_step(perf, launcher.base)

    assert rc == 1 and out["released"] is False
    assert out["error"] == SST.cancel_failed(launcher.base, str(sweep_dir))
    assert "--collect" not in launcher.operations()


@pytest.mark.parametrize(
    ("reply_", "reason"),
    [(ran(124, timed_out=True), "status timed out"), (ran(1, stderr=diag("API_UNAVAILABLE")), "API_UNAVAILABLE")],
    ids=["timed-out", "no-access"],
)
def test_release_leaves_a_sweep_when_the_cluster_is_out_of_reach(
    reply_: Any, reason: str, perf: InProcess, checkout: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After asking again a few times, the sweep is left for a later teardown: cancel and collect
    could only use up their timeouts."""
    operations: list[str] = []
    monkeypatch.setattr(SST, "run", lambda cmd, timeout, env=None: operations.append(cmd[1]) or reply_)
    sweep_dir = left_behind(checkout)

    rc, out = release_step(perf, checkout)

    assert rc == 1 and out["released"] is False
    assert out["error"] == (
        f"cannot read the state of {sweep_dir} ({reason}); rerun `--phase teardown` once the cluster answers"
    )
    assert operations == ["--status"] * (1 + MODULES["release_storage_test"].UNREACHABLE_RETRIES)


def test_release_asks_again_when_one_status_goes_unanswered(perf: InProcess, launcher: FakeLauncher) -> None:
    """One unanswered --status (a passing API error) does not leave the sweep holding the claim."""
    launcher.statuses = ["FAIL", FINISHED]
    sweep_dir = left_behind(launcher.base)

    rc, out = release_step(perf, launcher.base)

    assert rc == 0 and out["message"] == f"collected; cluster resources released. Results: {sweep_dir}"
    assert launcher.operations()[:3] == ["--status", "--status", "--collect"]


def test_release_never_cancels_while_another_run_holds_the_lock(perf: InProcess, launcher: FakeLauncher) -> None:
    """The sweep may be the lock holder's, still running: release touches nothing."""
    results = launcher.base / "results"
    left_behind(launcher.base)
    fd = hold(results / SST.RUN_LOCK)
    try:
        rc, out = release_step(perf, launcher.base)
    finally:
        os.close(fd)

    assert rc == 1 and out["released"] is False
    assert out["error"] == f"another run or teardown is using {results}; not touching its sweeps"
    assert launcher.calls == []
    assert_schema("release_storage_test", out)


def test_release_says_how_to_vendor_a_missing_release(
    perf: InProcess, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, real_sst_run: None
) -> None:
    """With a sweep to release and no vendored release, teardown names the make target."""
    monkeypatch.setattr(SST, "VENDORED", tmp_path / "missing")
    monkeypatch.delenv("STORAGE_SCALE_TEST_BASE", raising=False)
    left_behind(tmp_path)

    rc, out = perf("release_storage_test", f"--results-dir={tmp_path / 'results'}")

    assert rc == 1 and out["released"] is False
    vendor = "run `make vendor` (or `make vendor-storage-scale-test`)"
    assert out["error"] == f"no storage-scale-test at {tmp_path / 'missing'}; {vendor}"


# The two tests below fake the PermissionError the OS raises for a results root without write
# or list permission, rather than chmod it: root (as in CI containers) ignores directory
# permissions, so a chmod would not fail there.


def test_release_with_an_unwritable_results_root_gives_json(
    perf: InProcess, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A results root the step cannot lock fails with the reason, as JSON."""
    directory = tmp_path / "results"
    directory.mkdir()
    lock = directory / SST.RUN_LOCK
    real_open = os.open

    def open_denied(path: Any, *args: Any, **kwargs: Any) -> int:
        """Refuse to create the run lock, as an unwritable directory would."""
        if Path(path) == lock:
            raise PermissionError(13, "Permission denied", str(path))
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(os, "open", open_denied)
    rc, out = perf("release_storage_test", f"--results-dir={directory}")

    assert rc == 1 and out["released"] is False
    assert out["error"].startswith(f"cannot lock {directory}: ")
    assert_schema("release_storage_test", out)


def test_release_with_an_unlistable_results_root_is_not_released(
    perf: InProcess, launcher: FakeLauncher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A results root the step can lock but not list fails; it never reads as nothing held."""
    directory = launcher.base / "results"
    directory.mkdir(exist_ok=True)
    real_iterdir = Path.iterdir

    def iterdir_denied(self: Path) -> Any:
        """Refuse to list the results root, as a directory without read permission would."""
        if self == directory:
            raise PermissionError(13, "Permission denied", str(self))
        return real_iterdir(self)

    monkeypatch.setattr(Path, "iterdir", iterdir_denied)
    rc, out = perf("release_storage_test", f"--results-dir={directory}", f"--scale-test-base={launcher.base}")

    assert rc == 1 and out["released"] is False
    assert "Permission denied" in out["error"]
    assert_schema("release_storage_test", out)


def test_release_finds_a_batch_left_running(perf: InProcess, launcher: FakeLauncher) -> None:
    """A started batch (filesystem-batch-*) is released like a plain sweep."""
    launcher.statuses = [RUNNING]
    batch_dir = left_behind(launcher.base, "filesystem-batch-20260101Z000000")

    rc, out = release_step(perf, launcher.base)

    assert rc == 0 and out["message"] == f"collected; cluster resources released. Results: {batch_dir}"
    assert_cancelled_before_collect(launcher)


def test_release_takes_plain_sweeps_and_batches_oldest_first(perf: InProcess, launcher: FakeLauncher) -> None:
    """Sweeps are released in the order they ran, by datestamp, whichever kind each is."""
    launcher.statuses = [RUNNING]
    newer = left_behind(launcher.base, "elbencho-20260102Z000000")
    older = left_behind(launcher.base, "filesystem-batch-20260101Z000000")

    found = SST.unreleased(launcher.base, str(launcher.base / "results"), {})

    assert [d for d, _, _ in found] == [str(older), str(newer)]
