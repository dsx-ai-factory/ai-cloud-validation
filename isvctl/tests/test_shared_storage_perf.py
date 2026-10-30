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

"""Tests for the shared storage performance scripts (configs/shared/storage/perf).

The scripts drive storage-scale-test, so these tests run them against a fake checkout whose
launcher plays back a scripted Kubernetes lifecycle: submit, ``--status``, ``--cancel`` and
``--collect``, recording each call.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

PERF_DIR = Path(__file__).resolve().parents[1] / "configs" / "shared" / "storage" / "perf"

# The fake launcher. Its behaviour is set by files in the checkout:
#   statuses  - one `--status` reply per line, played in order (the last one repeats); a line
#               "FAIL" exits 1 with an API_UNAVAILABLE diagnostic, like a lost connection
#   cancelled - created by `--cancel`; `--status` then reports a cancelled, uncollected sweep
#   cancels_needed   - `--cancel` calls before the cancel completes (default 1); until then
#                      the sweep is left in CANCEL_REQUESTED
#   cancel_requested - `--status` reports CANCEL_REQUESTED (NEXT_ACTION=INSPECT), and exits 1
#                      as storage-scale-test does for an incomplete cancel
#   lock_busy        - lifecycle commands refused as if another held the lock, this many times
#   collected - created by `--collect`; once present, `--status` reports the collected view
#   cleanup_pending  - `--collect` calls that publish results but leave cleanup pending
#   collect_hangs    - `--collect` never returns
#   calls     - every invocation's arguments, one per line
#   submit_dies - submit creates its results directory, then exits without printing it
#   no_attempt  - submit stops before publishing an attempt (kubernetes/current-attempt);
#                 `--status` on a directory without one fails as storage-scale-test does
FAKE_LAUNCHER = r"""#!/usr/bin/env bash
base="$(cd "$(dirname "$0")/../.." && pwd)"
echo "$*" >> "$base/calls"
busy=$(cat "$base/lock_busy" 2>/dev/null || echo 0)
if [[ "$1" =~ ^--(status|cancel|collect)$ ]] && (( busy > 0 )); then
  echo $((busy - 1)) > "$base/lock_busy"
  echo "Error: another Kubernetes lifecycle operation is active for $2/kubernetes" >&2
  exit 1
fi
case "$1" in
  --status)
    if [[ ! -f "$2/kubernetes/current-attempt" ]]; then
      echo "Error: result directory has no valid current Kubernetes attempt: $2" >&2
      exit 1
    fi
    if [[ -f "$base/collected" ]]; then
      printf 'STATE=%s\nEXECUTIONS_TOTAL=2\nEXECUTIONS_SUCCEEDED=2\nEXECUTIONS_FAILED=0\n' "$(cat "$base/final")"
      printf 'RESULT_COLLECTION=COMPLETE\nNEXT_ACTION=NONE\n'
      exit 0
    fi
    if [[ -f "$base/cleanup_started" ]]; then
      printf 'STATE=%s\nEXECUTIONS_TOTAL=2\n' "$(cat "$base/final")"
      printf 'RESULT_COLLECTION=CLEANUP_PENDING\nNEXT_ACTION=COLLECT\n'
      exit 0
    fi
    if [[ -f "$base/cancelled" ]]; then
      printf 'STATE=CANCELLED\nEXECUTIONS_TOTAL=2\nRESULT_COLLECTION=PENDING\nNEXT_ACTION=COLLECT\n'
      exit 0
    fi
    if [[ -f "$base/cancel_requested" ]]; then
      printf 'STATE=CANCEL_REQUESTED\nNEXT_ACTION=INSPECT\n'
      echo "STORAGE_SCALE_TEST_DIAGNOSTIC_REASON=CANCELLATION_INCOMPLETE" >&2
      exit 1
    fi
    n=$(cat "$base/status_index" 2>/dev/null || echo 1)
    line=$(sed -n "${n}p" "$base/statuses")
    [[ -n "$line" ]] || line=$(tail -n 1 "$base/statuses")
    echo $((n + 1)) > "$base/status_index"
    if [[ "$line" == FAIL ]]; then
      echo "STORAGE_SCALE_TEST_DIAGNOSTIC_REASON=API_UNAVAILABLE" >&2
      exit 1
    fi
    echo "$line" | tr ' ' '\n'
    ;;
  --collect)
    [[ -f "$base/collect_hangs" ]] && exec sleep 60
    pending=$(cat "$base/cleanup_pending" 2>/dev/null || echo 0)
    if (( pending > 0 )); then
      echo $((pending - 1)) > "$base/cleanup_pending"
      touch "$base/cleanup_started"
    else
      rm -f "$base/cleanup_started"
      touch "$base/collected"
    fi
    ;;
  --cancel)
    needed=$(cat "$base/cancels_needed" 2>/dev/null || echo 1)
    echo $((needed - 1)) > "$base/cancels_needed"
    if (( needed > 1 )); then
      touch "$base/cancel_requested"
    else
      rm -f "$base/cancel_requested"
      touch "$base/cancelled"
      echo CANCELLED > "$base/final"
    fi
    ;;
  *)
    mkdir -p "$base/results/elbencho-20260101Z000000"
    if [[ ! -f "$base/no_attempt" ]]; then
      mkdir -p "$base/results/elbencho-20260101Z000000/kubernetes"
      echo 0123abcd > "$base/results/elbencho-20260101Z000000/kubernetes/current-attempt"
    fi
    [[ -f "$base/submit_dies" ]] && exit 137
    echo "STORAGE_SCALE_TEST_RESULTS_DIR=$base/results/elbencho-20260101Z000000"
    echo "STORAGE_SCALE_TEST_ATTEMPT_ID=0123abcd"
    ;;
esac
"""

RUNNING = "STATE=RUNNING EXECUTIONS_TOTAL=2 EXECUTIONS_SUCCEEDED=0 NEXT_ACTION=WAIT"
FINISHED = "STATE=SUCCESS EXECUTIONS_TOTAL=2 EXECUTIONS_SUCCEEDED=2 RESULT_COLLECTION=PENDING NEXT_ACTION=COLLECT"


def _load(name: str) -> ModuleType:
    """Load a perf script as a module; each script puts its own directory on sys.path for sst."""
    spec = importlib.util.spec_from_file_location(f"shared_perf_{name}", PERF_DIR / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


# The line in env.sh.template where integrations insert their settings.
OVERRIDES_ANCHOR = _load("sst").OVERRIDES_ANCHOR


@pytest.fixture
def checkout(tmp_path: Path) -> Path:
    """A fake storage-scale-test checkout whose launcher finishes after one WAIT."""
    base = tmp_path / "storage-scale-test"
    launcher = base / "storage-tests" / "fs" / "nv-elbencho-sweep.sh"
    launcher.parent.mkdir(parents=True)
    launcher.write_text(FAKE_LAUNCHER)
    launcher.chmod(launcher.stat().st_mode | stat.S_IXUSR)
    (base / "env.sh.template").write_text(
        f'SCALE_TEST_BASE=x\nTEST_DIRS=()\n{OVERRIDES_ANCHOR}\nsource "${{SCALE_TEST_BASE}}/lib/env_base.sh"\n'
    )
    (base / "statuses").write_text(f"{RUNNING}\n{FINISHED}\n")
    (base / "final").write_text("SUCCESS\n")
    return base


def _run_script(name: str, *args: str) -> tuple[int, dict[str, Any]]:
    """Run a perf script outside demo mode and return its exit code and JSON."""
    env = {k: v for k, v in os.environ.items() if k != "ISVCTL_DEMO_MODE"}
    proc = subprocess.run(
        [sys.executable, str(PERF_DIR / f"{name}.py"), *args],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=60,
    )
    return proc.returncode, json.loads(proc.stdout)


def _run_storage_test(base: Path, state_file: Path, *extra: str) -> tuple[int, dict[str, Any]]:
    """Run run_storage_test.py against the fake checkout."""
    return _run_script(
        "run_storage_test",
        f"--scale-test-base={base}",
        "--claim",
        "perf-ns/perf-claim",
        "--node-selector=storage-test=true",
        "--results-dir",
        str(base / "results"),
        "--state-file",
        str(state_file),
        "--interval",
        "0",
        *extra,
    )


def test_run_submits_waits_and_collects(checkout: Path, tmp_path: Path) -> None:
    """A finished sweep is collected, and the state file records that it was."""
    state_file = tmp_path / "run" / "state.json"
    rc, out = _run_storage_test(checkout, state_file)

    assert rc == 0, out
    assert out["state"] == "SUCCESS"
    assert out["resources_released"] is True
    assert out["executions"] == {"total": 2, "succeeded": 2, "failed": 0}
    assert out["results_dir"].endswith("elbencho-20260101Z000000")
    assert json.loads(state_file.read_text())["collected"] is True
    calls = (checkout / "calls").read_text().splitlines()
    assert calls[0].startswith("--target ")
    assert "--env-override" in calls[0] and calls[0].endswith("--nodes 1")
    assert any(c.startswith("--collect ") for c in calls)


def test_run_writes_env_sh_and_input_files(checkout: Path, tmp_path: Path) -> None:
    """Settings go before the line that loads defaults; target and workload are split."""
    state_file = tmp_path / "run" / "state.json"
    _run_storage_test(checkout, state_file, "--io-sizes", "r4K,1M", "--duration", "10")

    env_sh = (checkout / "env.sh").read_text()
    assert env_sh.index("export EXECUTION_SUBSTRATE=kubectl") < env_sh.index(OVERRIDES_ANCHOR)
    assert "export KUBECTL_NODE_SELECTOR='storage-test=true'" in env_sh
    assert f"export LOGS_DIR='{checkout / 'results' / 'logs'}'" in env_sh
    target = (state_file.parent / "target.env").read_text()
    assert target == "TEST_DIRS=(['perf-ns/perf-claim:scale-test']=1)\n"
    workload = (state_file.parent / "workload.env").read_text()
    assert "ELBENCHO_SCALE_IO_SIZES=('r4K' '1M')" in workload
    assert "ELBENCHO_SCALE_READ_WRITE_DURATION=10" in workload


def test_run_refuses_an_env_sh_it_did_not_write(checkout: Path, tmp_path: Path) -> None:
    """A user's own env.sh is never overwritten."""
    (checkout / "env.sh").write_text("export EXECUTION_SUBSTRATE=slurm\n")
    rc, out = _run_storage_test(checkout, tmp_path / "state.json")

    assert rc == 1
    assert "not written by isvctl" in out["error"]
    assert (checkout / "env.sh").read_text() == "export EXECUTION_SUBSTRATE=slurm\n"
    assert not (checkout / "calls").exists()


def test_run_keeps_waiting_through_lost_api_access(checkout: Path, tmp_path: Path) -> None:
    """A failed --status means "unknown", not "failed": the sweep runs on in-cluster."""
    (checkout / "statuses").write_text(f"{RUNNING}\nFAIL\nFAIL\n{FINISHED}\n")
    rc, out = _run_storage_test(checkout, tmp_path / "state.json")

    assert rc == 0, out
    assert out["state"] == "SUCCESS"


def test_run_cancels_and_collects_a_sweep_that_outlives_the_wait(checkout: Path, tmp_path: Path) -> None:
    """At --wait-timeout the sweep is cancelled and collected, and the step fails."""
    (checkout / "statuses").write_text(f"{RUNNING}\n")
    state_file = tmp_path / "state.json"
    rc, out = _run_storage_test(checkout, state_file, "--wait-timeout", "0")

    assert rc == 1
    assert out["state"] == "TIMEOUT"
    assert out["resources_released"] is True
    calls = (checkout / "calls").read_text()
    assert "--cancel " in calls and "--collect " in calls
    assert json.loads(state_file.read_text())["collected"] is True


def test_run_stops_within_its_time_budget(checkout: Path, tmp_path: Path) -> None:
    """No call runs past --time-budget, so the step reports instead of being killed."""
    (checkout / "statuses").write_text(f"{RUNNING}\n")
    (checkout / "collect_hangs").touch()
    started = time.monotonic()
    rc, out = _run_storage_test(checkout, tmp_path / "state.json", "--time-budget", "3")

    assert time.monotonic() - started < 15
    assert rc == 1
    assert out["state"] == "TIMEOUT"
    assert "--time-budget 3s" in out["error"]
    assert out["resources_released"] is False
    assert "time budget ran out" in out["collect_error"]


def test_run_finishes_a_cancel_made_outside_it(checkout: Path, tmp_path: Path) -> None:
    """A sweep cancelled from elsewhere stops in CANCEL_REQUESTED; the step completes the
    cancel and collects, rather than reporting the sweep as leaked.
    """
    (checkout / "cancel_requested").touch()
    rc, out = _run_storage_test(checkout, tmp_path / "state.json")

    assert rc == 1
    assert out["state"] == "CANCELLED"
    assert out["error"] == "the sweep was cancelled outside this run"
    assert out["resources_released"] is True
    assert "collect_error" not in out


def test_run_does_not_collect_a_rolled_back_submission(checkout: Path, tmp_path: Path) -> None:
    """SUBMISSION_FAILED means storage-scale-test already removed what submit created."""
    (checkout / "statuses").write_text("STATE=SUBMISSION_FAILED EXECUTIONS_TOTAL=2 NEXT_ACTION=INSPECT\n")
    state_file = tmp_path / "state.json"
    rc, out = _run_storage_test(checkout, state_file)

    assert rc == 1
    assert out["state"] == "SUBMISSION_FAILED"
    assert out["resources_released"] is True
    assert "--collect" not in (checkout / "calls").read_text()
    assert json.loads(state_file.read_text())["collected"] is True


def test_collect_retries_while_cleanup_is_pending(checkout: Path, tmp_path: Path) -> None:
    """A collect can publish the results yet leave cleanup pending; collecting again finishes it."""
    (checkout / "cleanup_pending").write_text("1\n")
    rc, out = _run_storage_test(checkout, tmp_path / "state.json")

    assert rc == 0, out
    assert out["resources_released"] is True
    assert (checkout / "calls").read_text().count("--collect ") == 2


def test_collect_reports_cleanup_that_never_finishes(checkout: Path, tmp_path: Path) -> None:
    """After its retries, collect says the sweep still holds resources and how to release them."""
    (checkout / "cleanup_pending").write_text("9\n")
    rc, out = _run_storage_test(checkout, tmp_path / "state.json")

    assert rc == 1
    assert out["resources_released"] is False
    assert "RESULT_COLLECTION=CLEANUP_PENDING" in out["collect_error"]
    assert "--collect " in out["collect_error"]
    assert (checkout / "calls").read_text().count("--collect ") == 3


def test_release_without_a_recorded_sweep_is_a_no_op(tmp_path: Path) -> None:
    """Nothing was submitted, so there is nothing to release."""
    rc, out = _run_script("release_storage_test", "--state-file", str(tmp_path / "missing.json"))

    assert rc == 0
    assert out["released"] is True


def test_a_submit_killed_before_printing_its_results_dir_is_still_released(checkout: Path, tmp_path: Path) -> None:
    """Submit holds the Lease before it prints the handle; teardown finds the sweep anyway.

    An earlier run's collected record must not hide the new sweep.
    """
    (checkout / "submit_dies").touch()
    (checkout / "statuses").write_text(f"{RUNNING}\n")
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"results_dir": "/old/elbencho-x", "collected": True}))

    _, run = _run_storage_test(checkout, state_file)
    rc, out = _run_script("release_storage_test", "--state-file", str(state_file))

    assert run["success"] is False
    assert rc == 0, out
    assert out["released"] is True
    calls = (checkout / "calls").read_text()
    assert f"--cancel {checkout}/results/elbencho-20260101Z000000" in calls
    assert "--collect " in calls


def test_release_ignores_results_dirs_from_earlier_runs(checkout: Path, tmp_path: Path) -> None:
    """A submit that created nothing leaves only earlier runs' directories, which are not touched."""
    (checkout / "results" / "elbencho-20250101Z000000").mkdir(parents=True)
    state_file = tmp_path / "state.json"
    pending = {
        "scale_test_base": str(checkout),
        "results_root": str(checkout / "results"),
        "existing": ["elbencho-20250101Z000000"],
        "collected": False,
    }
    state_file.write_text(json.dumps(pending))

    rc, out = _run_script("release_storage_test", "--state-file", str(state_file))

    assert rc == 0, out
    assert out["message"] == "submit created no sweep; nothing to release"
    assert not (checkout / "calls").exists()


def test_release_accepts_a_submit_that_never_published_an_attempt(checkout: Path, tmp_path: Path) -> None:
    """storage-scale-test cannot inspect such a directory, and it owns no cluster resource."""
    (checkout / "submit_dies").touch()
    (checkout / "no_attempt").touch()
    state_file = tmp_path / "state.json"

    _run_storage_test(checkout, state_file)
    rc, out = _run_script("release_storage_test", "--state-file", str(state_file))

    assert rc == 0, out
    assert out["message"] == "submit stopped before creating any cluster resource"
    assert "--cancel" not in (checkout / "calls").read_text()


def test_release_collects_a_sweep_the_run_step_left_behind(checkout: Path, tmp_path: Path) -> None:
    """A run killed before collect leaves an uncollected handle; teardown cancels and collects."""
    (checkout / "statuses").write_text(f"{RUNNING}\n")
    results_dir = checkout / "results" / "elbencho-20260101Z000000"
    (results_dir / "kubernetes").mkdir(parents=True)
    (results_dir / "kubernetes" / "current-attempt").write_text("0123abcd\n")
    state_file = tmp_path / "state.json"
    handle = {"scale_test_base": str(checkout), "results_dir": str(results_dir), "collected": False}
    state_file.write_text(json.dumps(handle))

    rc, out = _run_script("release_storage_test", "--state-file", str(state_file))

    assert rc == 0, out
    assert out["released"] is True
    calls = (checkout / "calls").read_text()
    assert "--cancel " in calls and "--collect " in calls
    assert json.loads(state_file.read_text())["collected"] is True


def _left_behind(checkout: Path, tmp_path: Path) -> Path:
    """The state file of a run killed before it collected its sweep."""
    results_dir = checkout / "results" / "elbencho-20260101Z000000"
    (results_dir / "kubernetes").mkdir(parents=True)
    (results_dir / "kubernetes" / "current-attempt").write_text("0123abcd\n")
    state_file = tmp_path / "state.json"
    handle = {"scale_test_base": str(checkout), "results_dir": str(results_dir), "collected": False}
    state_file.write_text(json.dumps(handle))
    return state_file


def test_release_repeats_a_cancel_left_in_cancel_requested(checkout: Path, tmp_path: Path) -> None:
    """Cancel is two-phase; storage-scale-test's answer to CANCEL_REQUESTED is to cancel again."""
    (checkout / "statuses").write_text(f"{RUNNING}\n")
    (checkout / "cancels_needed").write_text("2\n")
    state_file = _left_behind(checkout, tmp_path)

    rc, out = _run_script("release_storage_test", "--state-file", str(state_file))

    assert rc == 0, out
    assert out["released"] is True
    assert (checkout / "calls").read_text().count("--cancel ") == 2


def test_release_waits_for_a_busy_lock(checkout: Path, tmp_path: Path) -> None:
    """storage-scale-test refuses a command while another holds the lock; release retries it."""
    (checkout / "lock_busy").write_text("1\n")
    state_file = _left_behind(checkout, tmp_path)

    rc, out = _run_script("release_storage_test", "--state-file", str(state_file))

    assert rc == 0, out
    assert out["released"] is True


def test_release_finishes_a_cancel_reported_with_exit_1(checkout: Path, tmp_path: Path) -> None:
    """--status reports an incomplete cancel (CANCEL_REQUESTED) and exits 1; that is a known state."""
    (checkout / "cancel_requested").touch()
    state_file = _left_behind(checkout, tmp_path)

    rc, out = _run_script("release_storage_test", "--state-file", str(state_file))

    assert rc == 0, out
    assert out["released"] is True
    assert "--cancel " in (checkout / "calls").read_text()


def test_release_reports_a_cancel_that_never_completes(checkout: Path, tmp_path: Path) -> None:
    """A sweep that will not stop still holds the claim, and release says so."""
    (checkout / "statuses").write_text(f"{RUNNING}\n")
    (checkout / "cancels_needed").write_text("9\n")
    state_file = _left_behind(checkout, tmp_path)

    rc, out = _run_script("release_storage_test", "--state-file", str(state_file))

    assert rc == 1
    assert out["released"] is False
    assert out["error"] == "the sweep did not stop; it still holds the claim"
    assert "--collect" not in (checkout / "calls").read_text()
    assert json.loads(state_file.read_text())["collected"] is False


def test_release_accepts_a_rolled_back_submission(checkout: Path, tmp_path: Path) -> None:
    """Nothing to collect after SUBMISSION_FAILED: storage-scale-test released it all."""
    (checkout / "statuses").write_text("STATE=SUBMISSION_FAILED EXECUTIONS_TOTAL=2 NEXT_ACTION=INSPECT\n")
    state_file = _left_behind(checkout, tmp_path)

    rc, out = _run_script("release_storage_test", "--state-file", str(state_file))

    assert rc == 0, out
    assert out["message"] == "submission failed and storage-scale-test rolled it back"
    assert "--collect" not in (checkout / "calls").read_text()


def _phase(op: str, bytes_per_s: float, ops_per_s: float, avg_us: float) -> dict[str, Any]:
    """One results.json IO phase."""
    rates = {"bytes_per_s": bytes_per_s, "ops_per_s": ops_per_s, "elapsed_ms": 10000}
    return {"phase": op, "elbencho_first": rates, "elbencho_last": rates, "latency_us": {"avg": avg_us}}


def _document(*executions: dict[str, Any]) -> dict[str, Any]:
    """A results.json document; by default one complete write-then-read execution."""
    if not executions:
        phases = [_phase("write", 104_857_600, 200, 5000), _phase("read", 157_286_400, 300, 4000)]
        executions = (
            {
                "execution_id": "0001",
                "kind": "io",
                "ledger_state": "success",
                "result_state": "complete",
                "problems": [],
                "results": {"phases": phases},
            },
        )
    complete = all(e.get("ledger_state") == "success" and e.get("result_state") == "complete" for e in executions)
    return {
        "schema": "storage-scale-test/filesystem-batch-results",
        "schema_version": "1.0",
        "batch_outcome": {"complete": complete},
        "executions": list(executions),
    }


RUN_OK: dict[str, Any] = {
    "state": "SUCCESS",
    "executions": {"total": 2, "succeeded": 2, "failed": 0},
    "resources_released": True,
    "results_dir": "/results/elbencho-x",
}


def test_evaluate_reports_numbers_without_a_floor() -> None:
    """With floors of 0 a complete sweep passes and reports the slower operation."""
    out = _load("evaluate_storage_test").evaluate(RUN_OK, _document(), 0.0, 0.0)

    assert out["success"] is True
    assert out["headline"]["write"]["best_mbps"] == 104.9
    assert out["headline"]["read"]["min_avg_latency_ms"] == 4.0
    assert out["tests"]["bandwidth_meets_min"]["measured_mbps"] == 104.9
    assert out["tests"]["iops_meets_min"]["message"] == "report only (no floor set)"


def test_evaluate_enforces_a_partner_floor() -> None:
    """A floor above the slower operation fails the bandwidth test only."""
    out = _load("evaluate_storage_test").evaluate(RUN_OK, _document(), 120.0, 0.0)

    assert out["success"] is False
    assert out["tests"]["bandwidth_meets_min"]["passed"] is False
    assert out["tests"]["bandwidth_meets_min"]["error"] == "104.9 MB/s < floor 120.0 MB/s"
    assert out["tests"]["iops_meets_min"]["passed"] is True


def test_evaluate_fails_when_an_operation_has_no_results() -> None:
    """Missing read results fail results_present, and the floors cannot pass vacuously."""
    document = _document()
    phases = document["executions"][0]["results"]["phases"]
    document["executions"][0]["results"]["phases"] = [p for p in phases if p["phase"] == "write"]
    out = _load("evaluate_storage_test").evaluate(RUN_OK, document, 0.0, 0.0)

    assert out["tests"]["results_present"]["passed"] is False
    assert out["tests"]["bandwidth_meets_min"]["passed"] is False


def test_evaluate_fails_an_execution_whose_results_are_incomplete() -> None:
    """An execution storage-scale-test could not fully parse fails results_present, with its reason."""
    broken = {
        "execution_id": "0002",
        "kind": "io",
        "ledger_state": "success",
        "result_state": "partial",
        "problems": [{"code": "phase_missing", "detail": "no read", "phase": "read"}],
        "results": {"phases": []},
    }
    out = _load("evaluate_storage_test").evaluate(RUN_OK, _document(*_document()["executions"], broken), 0.0, 0.0)

    assert out["tests"]["results_present"]["passed"] is False
    assert "0002: ledger success, results partial (phase_missing)" in out["tests"]["results_present"]["error"]


def test_evaluate_fails_an_execution_that_did_not_succeed() -> None:
    """Complete-looking results from a failed execution do not count (batch_outcome.complete)."""
    failed = {**_document()["executions"][0], "execution_id": "0002", "ledger_state": "failed"}
    out = _load("evaluate_storage_test").evaluate(RUN_OK, _document(*_document()["executions"], failed), 0.0, 0.0)

    assert out["tests"]["results_present"]["passed"] is False
    assert "0002: ledger failed, results complete" in out["tests"]["results_present"]["error"]


def test_evaluate_trusts_batch_outcome_over_the_executions() -> None:
    """storage-scale-test's own verdict wins, e.g. when it found a problem outside any execution."""
    document = {**_document(), "batch_outcome": {"complete": False, "executions": {"total": 1}}}
    out = _load("evaluate_storage_test").evaluate(RUN_OK, document, 0.0, 0.0)

    assert out["tests"]["results_present"]["passed"] is False
    assert "batch_outcome.complete is not true" in out["tests"]["results_present"]["error"]


@pytest.mark.parametrize(
    ("run", "failed_test"),
    [
        ({**RUN_OK, "state": "FAILED"}, "sweep_complete"),
        ({**RUN_OK, "executions": {"total": 2, "succeeded": 1, "failed": 1}}, "sweep_complete"),
        ({**RUN_OK, "resources_released": False}, "resources_released"),
    ],
)
def test_evaluate_fails_an_incomplete_or_unreleased_sweep(run: dict[str, Any], failed_test: str) -> None:
    """Numbers from a failed or leaked sweep do not pass."""
    out = _load("evaluate_storage_test").evaluate(run, _document(), 0.0, 0.0)

    assert out["success"] is False
    assert out["tests"][failed_test]["passed"] is False


def _fake_extractor(base: Path, script: str) -> None:
    """Install a fake utils/extract-elbencho.py running the given bash body."""
    extractor = base / "utils" / "extract-elbencho.py"
    extractor.parent.mkdir(parents=True, exist_ok=True)
    extractor.write_text(f"#!/usr/bin/env bash\n{script}\n")
    extractor.chmod(extractor.stat().st_mode | stat.S_IXUSR)


def test_evaluate_reads_the_document_the_extractor_writes(checkout: Path, tmp_path: Path) -> None:
    """The step runs the extractor with --to-json and judges the document, not its exit status."""
    results = tmp_path / "elbencho-x"
    results.mkdir()
    (tmp_path / "doc.json").write_text(json.dumps(_document()))
    _fake_extractor(checkout, f'echo "$*" > "{tmp_path}/args"; cp "{tmp_path}/doc.json" "$1/results.json"; exit 1')
    rc, out = _run_script(
        "evaluate_storage_test",
        f"--scale-test-base={checkout}",
        "--run",
        json.dumps({**RUN_OK, "results_dir": str(results)}),
    )

    assert rc == 0, out
    assert out["success"] is True
    assert (tmp_path / "args").read_text().split() == [str(results), "--to-json"]


def test_evaluate_never_reads_a_stale_document(checkout: Path, tmp_path: Path) -> None:
    """A results.json left by an earlier extraction is not mistaken for this run's results."""
    results = tmp_path / "elbencho-x"
    results.mkdir()
    (results / "results.json").write_text(json.dumps(_document()))
    _fake_extractor(checkout, "exit 1")
    _, out = _run_script(
        "evaluate_storage_test",
        f"--scale-test-base={checkout}",
        "--run",
        json.dumps({**RUN_OK, "results_dir": str(results)}),
    )

    assert out["tests"]["results_present"]["passed"] is False
    assert "without writing" in out["tests"]["results_present"]["error"]


def test_evaluate_names_a_release_without_to_json(checkout: Path, tmp_path: Path) -> None:
    """An extractor that predates --to-json fails with a clear reason."""
    _fake_extractor(checkout, 'echo "extract-elbencho.py: error: unrecognized arguments: --to-json" >&2; exit 2')
    _, out = _run_script(
        "evaluate_storage_test",
        f"--scale-test-base={checkout}",
        "--run",
        json.dumps({**RUN_OK, "results_dir": str(tmp_path)}),
    )

    assert out["success"] is False
    assert "v2.1.0 or later" in out["tests"]["results_present"]["error"]


@pytest.mark.parametrize("anchors", [0, 2])
def test_compose_env_sh_needs_exactly_one_overrides_anchor(anchors: int) -> None:
    """Like storage-scale-test's own CI, refuse a template without exactly one anchor."""
    sst = _load("sst")
    template = "TEST_DIRS=()\n" + f"{sst.OVERRIDES_ANCHOR}\n" * anchors
    with pytest.raises(ValueError, match="not supported"):
        sst.compose_env_sh(template, ["export EXECUTION_SUBSTRATE=kubectl"])


def test_compose_env_sh_matches_the_vendored_template() -> None:
    """The pinned release's env.sh.template has the anchor isvctl inserts at."""
    template = PERF_DIR.parents[4] / "isvtest" / "vendor" / "storage-scale-test" / "env.sh.template"
    if not template.is_file():
        pytest.skip("storage-scale-test is not vendored; run `make vendor`")
    out = _load("sst").compose_env_sh(template.read_text(encoding="utf-8"), ["export EXECUTION_SUBSTRATE=kubectl"])
    assert out.index("export EXECUTION_SUBSTRATE=kubectl") < out.index('source "${SCALE_TEST_BASE}/lib/env_base.sh"')


def test_find_uses_an_explicit_tree(checkout: Path) -> None:
    """--base selects a tree other than the vendored release; its version is the tree's own resolver's."""
    resolver = checkout / "lib" / "project_version.sh"
    resolver.parent.mkdir()
    resolver.write_text('[[ "$1" == resolve ]] && echo v9.9.9-modified\n')
    rc, out = _run_script("find_storage_scale_test", f"--base={checkout}")

    assert rc == 0, out
    assert out["scale_test_base"] == str(checkout)
    assert out["version"] == "v9.9.9-modified"


def test_find_says_how_to_vendor_a_missing_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """With no vendored release the step fails and names the make target."""
    find = _load("find_storage_scale_test")
    monkeypatch.setattr(find, "VENDORED", tmp_path / "missing")
    monkeypatch.setattr(sys, "argv", ["find_storage_scale_test.py"])
    monkeypatch.setattr(find.sst, "DEMO_MODE", False)

    assert find.main() == 1
    assert "run `make vendor` (or `make vendor-storage-scale-test`)" in json.loads(capsys.readouterr().out)["error"]


def test_parse_kv_unquotes_shell_quoted_diagnostics() -> None:
    """storage-scale-test writes diagnostic values and FAILED_LOG with printf %q."""
    fields = _load("sst").parse_kv(
        "STATE=RUNNING\nSTORAGE_SCALE_TEST_DIAGNOSTIC_ACTION=retry\\ --cancel\nFAILED_LOG=/r/a\\ b.log\n"
    )

    assert fields == {
        "STATE": "RUNNING",
        "STORAGE_SCALE_TEST_DIAGNOSTIC_ACTION": "retry --cancel",
        "FAILED_LOG": "/r/a b.log",
    }


def test_parse_kv_drops_a_key_it_must_act_on_when_repeated() -> None:
    """An ambiguous NEXT_ACTION reads as unknown rather than whichever came last."""
    fields = _load("sst").parse_kv("STATE=RUNNING\nNEXT_ACTION=WAIT\nNEXT_ACTION=COLLECT\n")

    assert fields == {"STATE": "RUNNING"}


def test_describe_failure_prefers_the_error_and_diagnostic() -> None:
    """A failed command is explained by its Error: line plus reason and phase, not a raw tail."""
    sst = _load("sst")
    stderr = (
        "STORAGE_SCALE_TEST_DIAGNOSTIC_PHASE=cluster-identity\n"
        "Error: kubectl could not reach the API server using the active context\n"
        "STORAGE_SCALE_TEST_DIAGNOSTIC_REASON=API_UNAVAILABLE\n"
        "STORAGE_SCALE_TEST_DIAGNOSTIC_MAY_STILL_BE_RUNNING=no\n"
    )

    assert sst.describe_failure(stderr) == (
        "kubectl could not reach the API server using the active context (API_UNAVAILABLE, phase cluster-identity)"
    )


def test_describe_failure_says_what_to_do_next() -> None:
    """storage-scale-test's next safe action, running risk and evidence path are passed on."""
    stderr = (
        "Error: cancellation is incomplete\n"
        "STORAGE_SCALE_TEST_DIAGNOSTIC_REASON=CANCELLATION_INCOMPLETE\n"
        "STORAGE_SCALE_TEST_DIAGNOSTIC_MAY_STILL_BE_RUNNING=unknown\n"
        "STORAGE_SCALE_TEST_DIAGNOSTIC_PATH=/r/kubernetes/diagnostics/status-1\n"
        "STORAGE_SCALE_TEST_DIAGNOSTIC_SAFE_NEXT_ACTION=retry\\ --cancel\\ for\\ the\\ same\\ results\\ directory\n"
    )

    assert _load("sst").describe_failure(stderr) == (
        "cancellation is incomplete (CANCELLATION_INCOMPLETE); may still be running: unknown; "
        "next: retry --cancel for the same results directory; diagnostics: /r/kubernetes/diagnostics/status-1"
    )


MY_ISV_STORAGE = Path(__file__).resolve().parents[1] / "configs" / "providers" / "my-isv" / "scripts" / "storage"


def _run_stub(name: str, *args: str, demo: bool, path: Path | None = None) -> dict[str, Any]:
    """Run a my-isv perf fixture stub, in or out of demo mode, and return its JSON."""
    env = {k: v for k, v in os.environ.items() if k != "ISVCTL_DEMO_MODE"}
    if demo:
        env["ISVCTL_DEMO_MODE"] = "1"
    if path is not None:
        env["PATH"] = f"{path}{os.pathsep}{env['PATH']}"
    proc = subprocess.run(
        [sys.executable, str(MY_ISV_STORAGE / f"{name}.py"), *args],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=60,
    )
    return json.loads(proc.stdout)


@pytest.mark.parametrize(
    ("create", "delete", "id_flag"),
    [
        ("create_perf_filesystem", "delete_perf_filesystem", "--filesystem-id="),
        ("provision_perf_storage", "deprovision_perf_storage", "--claim="),
    ],
)
def test_a_standalone_teardown_removes_what_setup_recorded(
    create: str, delete: str, id_flag: str, tmp_path: Path
) -> None:
    """`--phase teardown` has no setup outputs, so the delete stubs fall back to the record."""
    record = tmp_path / "inputs" / f"{create}.json"
    _run_stub(create, "--record", str(record), demo=True)
    assert json.loads(record.read_text())["created"] is True

    out = _run_stub(delete, id_flag, f"--record={record}", demo=True)

    assert out["deleted"] is True
    assert not record.exists()


@pytest.mark.parametrize(
    ("delete", "id_flag"), [("delete_perf_filesystem", "--filesystem-id="), ("deprovision_perf_storage", "--claim=")]
)
def test_a_standalone_teardown_leaves_a_reused_fixture_alone(delete: str, id_flag: str, tmp_path: Path) -> None:
    """A fixture setup reused ("created": false) is never deleted, even from the record."""
    record = tmp_path / "record.json"
    record.write_text(json.dumps({"filesystem_id": "fs-1", "namespace": "ns", "claim_name": "c", "created": False}))

    out = _run_stub(delete, id_flag, f"--record={record}", demo=False)

    assert out["success"] is True
    assert out["deleted"] is False
    assert not record.exists()


def _fake_kubectl(tmp_path: Path, pvc: dict[str, Any]) -> Path:
    """A kubectl on PATH whose `get pvc` prints ``pvc`` and records its arguments."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (tmp_path / "pvc.json").write_text(json.dumps(pvc))
    kubectl = bin_dir / "kubectl"
    kubectl.write_text(f'#!/bin/sh\necho "$@" >> {tmp_path / "calls"}\ncat {tmp_path / "pvc.json"}\n')
    kubectl.chmod(0o755)
    return bin_dir


USABLE_CLAIM = {
    "spec": {"accessModes": ["ReadWriteMany"], "volumeMode": "Filesystem", "volumeName": "pv-perf"},
    "status": {"phase": "Bound"},
}


@pytest.mark.parametrize(("stub", "field", "value"), [
    ("create_perf_filesystem", "filesystem_id", "pv-perf"),
    ("provision_perf_storage", "claim_name", "perf-claim"),
])
def test_an_existing_claim_is_adopted_not_created(stub: str, field: str, value: str, tmp_path: Path) -> None:
    """--existing-claim reads the claim (read-only) and reports it as not created."""
    bin_dir = _fake_kubectl(tmp_path, USABLE_CLAIM)

    out = _run_stub(stub, "--namespace=perf-ns", "--existing-claim=perf-claim", demo=False, path=bin_dir)

    assert out["success"] is True, out
    assert out[field] == value and out["created"] is False
    assert (tmp_path / "calls").read_text().split()[:5] == ["-n", "perf-ns", "get", "pvc", "perf-claim"]
    _assert_schema(stub, out)


@pytest.mark.parametrize("stub", ["create_perf_filesystem", "provision_perf_storage"])
def test_demo_mode_never_contacts_a_cluster(stub: str, tmp_path: Path) -> None:
    """Under ISVCTL_DEMO_MODE=1 an exported STORAGE_PERF_CLAIM is adopted on paper, with no kubectl call."""
    bin_dir = _fake_kubectl(tmp_path, USABLE_CLAIM)

    out = _run_stub(stub, "--existing-claim=perf-claim", demo=True, path=bin_dir)

    assert out["success"] is True and out["created"] is False
    assert not (tmp_path / "calls").exists()
    _assert_schema(stub, out)


@pytest.mark.parametrize("stub", ["create_perf_filesystem", "provision_perf_storage"])
def test_setup_keeps_the_record_of_a_fixture_an_earlier_run_kept(stub: str, tmp_path: Path) -> None:
    """A record saying an earlier run created a fixture is not overwritten: teardown needs it to remove that fixture."""
    record = tmp_path / "record.json"
    record.write_text(json.dumps({"success": True, "created": True, "claim_name": "kept", "filesystem_id": "kept"}))
    bin_dir = _fake_kubectl(tmp_path, USABLE_CLAIM)

    out = _run_stub(stub, "--existing-claim=perf-claim", f"--record={record}", demo=False, path=bin_dir)

    assert out["success"] is False
    assert "--phase teardown" in out["error"]
    assert json.loads(record.read_text())["created"] is True
    assert not (tmp_path / "calls").exists()
    _assert_schema(stub, out)


@pytest.mark.parametrize("stub", ["create_perf_filesystem", "provision_perf_storage"])
def test_an_unusable_existing_claim_fails_setup(stub: str, tmp_path: Path) -> None:
    """A claim the sweep cannot share across nodes stops setup with the reason."""
    claim = {**USABLE_CLAIM, "spec": {**USABLE_CLAIM["spec"], "accessModes": ["ReadWriteOnce"]}}
    bin_dir = _fake_kubectl(tmp_path, claim)

    out = _run_stub(stub, "--existing-claim=perf-claim", demo=False, path=bin_dir)

    assert out["success"] is False
    assert "accessModes=['ReadWriteOnce']" in out["error"]
    _assert_schema(stub, out)


# ─── Plumbing: ACV config -> step arguments -> storage-scale-test inputs ─────

CONFIG_DIR = Path(__file__).resolve().parents[1] / "configs" / "providers" / "my-isv" / "config"
SETUP_OUTPUTS = {
    "find_storage_scale_test": {"scale_test_base": "/opt/storage-scale-test"},
    "setup_cluster": {
        "kubeconfig_path": "/tmp/kubeconfig",
        "storage_perf": {"namespace": "", "claim": "", "node_selector": "storage-test=true", "node_count": 2},
    },
    "create_perf_filesystem": {"filesystem_id": "fs-1", "created": True},
    "provision_perf_storage": {"namespace": "perf-ns", "claim_name": "perf-claim", "created": True},
}


def _render(step_name: str, set_values: list[str] | None = None, outputs: dict | None = None) -> list[str]:
    """Render one perf overlay step's arguments the way isvctl does, with optional --set values."""
    from isvctl.config.merger import merge_yaml_files
    from isvctl.config.schema import RunConfig
    from isvctl.orchestrator.context import Context
    from isvctl.orchestrator.step_executor import StepExecutor

    files = [CONFIG_DIR / "storage.yaml", CONFIG_DIR / "overlays" / "storage-perf.yaml"]
    config = RunConfig.model_validate(merge_yaml_files(files, set_values))
    context = Context(config)
    for name, output in (SETUP_OUTPUTS if outputs is None else outputs).items():
        context.set_step_output(name, output)
    step = next(s for s in config.commands["storage"].steps if s.name == step_name)
    return StepExecutor()._render_args(step.args, context)


def _flag(args: list[str], name: str) -> str:
    """The value passed for ``name``, as ``--name value`` or ``--name=value``."""
    for index, arg in enumerate(args):
        if arg == name:
            return args[index + 1]
        if arg.startswith(name + "="):
            return arg.split("=", 1)[1]
    raise AssertionError(f"{name} not in {args}")


def test_run_step_receives_the_default_settings() -> None:
    """Setup outputs and the overlay's default settings reach run_storage_test."""
    args = _render("run_storage_test")

    assert _flag(args, "--scale-test-base") == "/opt/storage-scale-test"
    assert _flag(args, "--kubeconfig") == "/tmp/kubeconfig"
    assert _flag(args, "--claim") == "perf-ns/perf-claim"
    assert _flag(args, "--node-selector") == "storage-test=true"
    assert [_flag(args, f) for f in ("--nodes", "--io-sizes", "--threads", "--iodepth", "--duration")] == [
        "1",
        "r4K,1M",
        "1",
        "1",
        "10",
    ]
    assert _flag(args, "--wait-timeout") == "900"
    assert _flag(args, "--time-budget") == "2280"


def test_set_values_reach_the_run_and_evaluate_steps() -> None:
    """`--set tests.settings.<name>=...` overrides flow into the step command lines."""
    sets = [
        "tests.settings.perf_node_selector=pool=perf",
        "tests.settings.perf_nodes=1,2",
        "tests.settings.perf_io_sizes=r64K,4M",
        "tests.settings.perf_threads=2,8",
        "tests.settings.perf_iodepth=1,4",
        "tests.settings.perf_duration_s=30",
        "tests.settings.perf_wait_timeout_s=600",
        "tests.settings.perf_min_mbps=500",
        "tests.settings.perf_min_iops=2000",
    ]
    run = _render("run_storage_test", sets, {**SETUP_OUTPUTS, "setup_cluster": {}})
    evaluate = _render("evaluate_storage_test", sets, {**SETUP_OUTPUTS, "run_storage_test": {"state": "SUCCESS"}})

    assert [_flag(run, f) for f in ("--nodes", "--io-sizes", "--threads", "--iodepth", "--duration")] == [
        "1,2",
        "r64K,4M",
        "2,8",
        "1,4",
        "30",
    ]
    assert _flag(run, "--wait-timeout") == "600"
    assert _flag(run, "--node-selector") == "pool=perf"
    assert [_flag(evaluate, "--min-mbps"), _flag(evaluate, "--min-iops")] == ["500", "2000"]
    assert json.loads(_flag(evaluate, "--run")) == {"state": "SUCCESS"}


def test_a_standalone_teardown_renders_without_setup_outputs() -> None:
    """`--phase teardown` has no setup outputs; the teardown steps still render, using the records."""
    deprovision = _render("deprovision_perf_storage", outputs={})
    delete = _render("delete_perf_filesystem", outputs={})
    release = _render("release_storage_test", outputs={})

    assert "--created" not in deprovision and "--created" not in delete
    assert _flag(deprovision, "--claim") == "" and _flag(delete, "--filesystem-id") == ""
    assert _flag(deprovision, "--record").endswith("_output/storage-perf/inputs/claim.json")
    assert _flag(delete, "--record").endswith("_output/storage-perf/inputs/filesystem.json")
    assert _flag(release, "--state-file").endswith("_output/storage-perf/inputs/state.json")


def test_the_fixtures_adopt_the_claim_setup_cluster_reports() -> None:
    """setup_cluster's storage_perf claim and namespace reach both fixtures as --existing-claim."""
    outputs = {
        **SETUP_OUTPUTS,
        "setup_cluster": {"storage_perf": {"namespace": "perf-ns", "claim": "perf-claim", "node_selector": ""}},
    }
    for step in ("create_perf_filesystem", "provision_perf_storage"):
        args = _render(step, outputs=outputs)
        assert _flag(args, "--existing-claim") == "perf-claim"
        assert _flag(args, "--namespace") == "perf-ns"
        assert _flag(args, "--kubeconfig") == ""


def test_without_a_reported_claim_the_fixtures_create_their_own(monkeypatch: pytest.MonkeyPatch) -> None:
    """No storage_perf claim and no STORAGE_PERF_*: --existing-claim renders empty, the namespace defaults."""
    for name in ("STORAGE_PERF_CLAIM", "STORAGE_PERF_NAMESPACE"):
        monkeypatch.delenv(name, raising=False)
    for step in ("create_perf_filesystem", "provision_perf_storage"):
        args = _render(step, outputs={**SETUP_OUTPUTS, "setup_cluster": {}})
        assert _flag(args, "--existing-claim") == ""
        assert _flag(args, "--namespace") == "storage-perf"


def test_storage_perf_variables_apply_when_setup_cluster_reports_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A provider's own setup_cluster may omit storage_perf; the STORAGE_PERF_* settings fill it in."""
    monkeypatch.setenv("STORAGE_PERF_CLAIM", "env-claim")
    monkeypatch.setenv("STORAGE_PERF_NAMESPACE", "env-ns")
    monkeypatch.setenv("STORAGE_PERF_NODE_SELECTOR", "pool=env")
    outputs = {**SETUP_OUTPUTS, "setup_cluster": {}}

    for step in ("create_perf_filesystem", "provision_perf_storage"):
        args = _render(step, outputs=outputs)
        assert [_flag(args, "--existing-claim"), _flag(args, "--namespace")] == ["env-claim", "env-ns"]
    assert _flag(_render("run_storage_test", outputs=outputs), "--node-selector") == "pool=env"


def test_setup_cluster_values_win_over_the_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """One precedence rule for every storage_perf value: what setup_cluster reports, then the setting."""
    monkeypatch.setenv("STORAGE_PERF_NODE_SELECTOR", "pool=env")
    assert _flag(_render("run_storage_test"), "--node-selector") == "storage-test=true"


def test_rendered_settings_reach_storage_scale_test_inputs(checkout: Path, tmp_path: Path) -> None:
    """End to end: --set values -> rendered args -> the files storage-scale-test reads."""
    sets = [
        "tests.settings.perf_node_selector=pool=perf",
        "tests.settings.perf_nodes=2",
        "tests.settings.perf_io_sizes=r64K,4M",
        "tests.settings.perf_threads=4",
        "tests.settings.perf_iodepth=1,4",
        "tests.settings.perf_duration_s=30",
        f"tests.settings.perf_results_dir={tmp_path / 'results'}",
        f"tests.settings.perf_state_file={tmp_path / 'inputs' / 'state.json'}",
    ]
    args = _render(
        "run_storage_test",
        sets,
        {**SETUP_OUTPUTS, "setup_cluster": {}, "find_storage_scale_test": {"scale_test_base": str(checkout)}},
    )

    rc, out = _run_script("run_storage_test", *args, "--interval", "0")

    assert rc == 0, out
    workload = (tmp_path / "inputs" / "workload.env").read_text()
    assert "ELBENCHO_SCALE_IO_SIZES=('r64K' '4M')" in workload
    assert "ELBENCHO_SCALE_THREAD_LIST=('4')" in workload
    assert "ELBENCHO_IODEPTH_LIST=('1' '4')" in workload
    assert "ELBENCHO_SCALE_READ_WRITE_DURATION=30" in workload
    assert (tmp_path / "inputs" / "target.env").read_text() == "TEST_DIRS=(['perf-ns/perf-claim:scale-test']=1)\n"
    env_sh = (checkout / "env.sh").read_text()
    assert "export KUBECTL_NODE_SELECTOR='pool=perf'" in env_sh
    assert f"export RESULTS_DIR='{tmp_path / 'results'}'" in env_sh
    submit = (checkout / "calls").read_text().splitlines()[0]
    assert submit.endswith("--nodes 2")


# ─── Output schemas: every real step output validates; missing fields are rejected ─

PERF_SCRIPTS = {
    "find_storage_scale_test": (PERF_DIR, []),
    "create_perf_filesystem": (MY_ISV_STORAGE, []),
    "provision_perf_storage": (MY_ISV_STORAGE, []),
    "run_storage_test": (
        PERF_DIR,
        ["--scale-test-base=/x", "--claim=a/b", "--node-selector=a=b", "--results-dir=/x", "--state-file=/x"],
    ),
    "evaluate_storage_test": (PERF_DIR, ["--scale-test-base=/x"]),
    "release_storage_test": (PERF_DIR, ["--state-file=/x"]),
    "deprovision_perf_storage": (MY_ISV_STORAGE, ["--created"]),
    "delete_perf_filesystem": (MY_ISV_STORAGE, ["--created"]),
}


def _assert_schema(step: str, output: dict[str, Any], valid: bool = True) -> None:
    """Validate a step output against the schema isvctl picks for that step name."""
    from isvctl.config.output_schemas import get_schema_for_step, validate_output

    schema = get_schema_for_step(step)
    assert schema is not None and schema.startswith("storage_perf_"), schema
    ok, errors = validate_output(output, schema)
    assert ok is valid, (step, output, errors)


def _script_output(step: str, demo: bool, *extra: str) -> dict[str, Any]:
    """Run one perf script or stub and return its JSON."""
    directory, args = PERF_SCRIPTS[step]
    env = {k: v for k, v in os.environ.items() if k != "ISVCTL_DEMO_MODE"}
    if demo:
        env["ISVCTL_DEMO_MODE"] = "1"
    proc = subprocess.run(
        [sys.executable, str(directory / f"{step}.py"), *args, *extra],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=60,
    )
    return json.loads(proc.stdout)


@pytest.mark.parametrize("step", sorted(PERF_SCRIPTS))
def test_demo_outputs_match_their_schemas(step: str) -> None:
    """`make demo-storage` must pass schema validation for every perf step."""
    _assert_schema(step, _script_output(step, demo=True))


@pytest.mark.parametrize(
    "step", ["create_perf_filesystem", "provision_perf_storage", "deprovision_perf_storage", "delete_perf_filesystem"]
)
def test_unimplemented_template_failures_match_their_schemas(step: str) -> None:
    """A stub's failure envelope still validates, so its error reaches the report."""
    out = _script_output(step, demo=False)
    assert out["success"] is False
    _assert_schema(step, out)


def test_real_run_evaluate_and_release_outputs_match_their_schemas(checkout: Path, tmp_path: Path) -> None:
    """Success, timeout and refusal outputs from the real scripts all validate."""
    _, ok = _run_storage_test(checkout, tmp_path / "ok" / "state.json")
    _assert_schema("run_storage_test", ok)

    (checkout / "collected").unlink()
    (checkout / "status_index").unlink()
    (checkout / "statuses").write_text(f"{RUNNING}\n")
    _, timed_out = _run_storage_test(checkout, tmp_path / "timeout" / "state.json", "--wait-timeout", "0")
    assert timed_out["state"] == "TIMEOUT"
    _assert_schema("run_storage_test", timed_out)

    (checkout / "env.sh").write_text("export EXECUTION_SUBSTRATE=slurm\n")
    _, refused = _run_storage_test(checkout, tmp_path / "refused" / "state.json")
    assert refused["state"] == "NOT_SUBMITTED"
    _assert_schema("run_storage_test", refused)

    evaluate = _load("evaluate_storage_test")
    _assert_schema("evaluate_storage_test", evaluate.evaluate(RUN_OK, _document(), 0.0, 0.0))
    _assert_schema("evaluate_storage_test", evaluate.evaluate(RUN_OK, None, 0.0, 0.0, "no results_dir"))
    _assert_schema("evaluate_storage_test", _script_output("evaluate_storage_test", False, "--run=not json"))

    _, release = _run_script("release_storage_test", "--state-file", str(tmp_path / "missing.json"))
    _assert_schema("release_storage_test", release)


@pytest.mark.parametrize(
    ("step", "output"),
    [
        ("create_perf_filesystem", {"success": True, "platform": "storage", "created": True}),
        ("create_perf_filesystem", {"success": True, "platform": "storage", "filesystem_id": "", "created": True}),
        ("provision_perf_storage", {"success": True, "platform": "storage", "namespace": "ns", "created": True}),
        (
            "run_storage_test",
            {
                "success": True,
                "platform": "storage",
                "test_name": "run_storage_test",
                "state": "TIMEOUT",
                "resources_released": True,
                "results_dir": "/r",
                "executions": {"total": 1},
            },
        ),
        (
            "evaluate_storage_test",
            {
                "success": True,
                "platform": "storage",
                "test_name": "evaluate_storage_test",
                "headline": {},
                "tests": {"x": {}},
            },
        ),
        ("release_storage_test", {"success": True, "platform": "storage", "test_name": "release_storage_test"}),
        ("delete_perf_filesystem", {"success": True, "platform": "storage"}),
    ],
)
def test_schemas_reject_outputs_missing_what_later_steps_read(step: str, output: dict[str, Any]) -> None:
    """A step that reports success without the fields later steps depend on fails at the step."""
    _assert_schema(step, output, valid=False)


@pytest.mark.parametrize(
    ("delete", "id_flag"), [("delete_perf_filesystem", "--filesystem-id="), ("deprovision_perf_storage", "--claim=")]
)
def test_skip_destroy_keeps_a_created_fixture_and_its_record(delete: str, id_flag: str, tmp_path: Path) -> None:
    """STORAGE_SKIP_TEARDOWN keeps what setup created, and the record so a later teardown can remove it."""
    record = tmp_path / "record.json"
    record.write_text(json.dumps({"filesystem_id": "fs-1", "namespace": "ns", "claim_name": "c", "created": True}))

    out = _run_stub(delete, id_flag, f"--record={record}", "--skip-destroy", demo=True)

    assert out["success"] is True
    assert out["deleted"] is False and out["resources_deleted"] == []
    assert "preserved (--skip-destroy)" in out["message"]
    assert record.exists()
    _assert_schema(delete, out)


@pytest.mark.parametrize("skip", [True, False])
def test_storage_skip_teardown_reaches_the_fixture_teardown_steps(skip: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    """STORAGE_SKIP_TEARDOWN=true renders --skip-destroy for both fixture teardown steps, and nothing otherwise."""
    if skip:
        monkeypatch.setenv("STORAGE_SKIP_TEARDOWN", "true")
    else:
        monkeypatch.delenv("STORAGE_SKIP_TEARDOWN", raising=False)
    for step in ("deprovision_perf_storage", "delete_perf_filesystem"):
        assert ("--skip-destroy" in _render(step)) is skip


# ─── my-isv scripts/k8s/setup.sh: cluster guard, pinned kubeconfig, demo mode ─────

K8S_SETUP = Path(__file__).resolve().parents[1] / "configs" / "providers" / "my-isv" / "scripts" / "k8s" / "setup.sh"
FAKE_KUBECONFIG = "apiVersion: v1\nkind: Config\ncurrent-context: ctx-a\n"


def _run_setup(tmp_path: Path, **env: str) -> subprocess.CompletedProcess[str]:
    """Run setup.sh against a fake kubectl (via KUBECTL) whose current context is ctx-a."""
    kubectl = tmp_path / "fake-kubectl"
    kubectl.write_text(
        "#!/bin/sh\n"
        f'echo "$@" >> {tmp_path / "calls"}\n'
        'case "$*" in\n'
        '  "config current-context") echo ctx-a ;;\n'
        f'  "config view --minify --flatten") printf \'{FAKE_KUBECONFIG}\' ;;\n'
        "esac\n"
    )
    kubectl.chmod(0o755)
    base = {k: v for k, v in os.environ.items() if k not in ("ISVCTL_DEMO_MODE", "K8S_EXPECTED_CONTEXT", "K8S_KUBECONFIG_OUT")}
    return subprocess.run(
        ["bash", str(K8S_SETUP)],
        capture_output=True,
        text=True,
        env={**base, "KUBECTL": str(kubectl), **env},
        check=False,
        timeout=60,
    )


needs_jq = pytest.mark.skipif(shutil.which("jq") is None, reason="setup.sh requires jq")


@needs_jq
def test_setup_pins_the_current_context_for_the_rest_of_the_run(tmp_path: Path) -> None:
    """K8S_KUBECONFIG_OUT gets a private copy of the current context, reported as kubeconfig_path."""
    pinned = tmp_path / "inputs" / "kubeconfig"

    proc = _run_setup(tmp_path, K8S_EXPECTED_CONTEXT="ctx-a", K8S_KUBECONFIG_OUT=str(pinned))

    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)
    assert out["cluster_name"] == "ctx-a" and out["kubeconfig_path"] == str(pinned)
    assert pinned.read_text() == FAKE_KUBECONFIG.replace("\\n", "\n")
    assert pinned.stat().st_mode & 0o077 == 0


@needs_jq
def test_setup_without_a_pin_reports_kubectls_own_kubeconfig(tmp_path: Path) -> None:
    """No K8S_KUBECONFIG_OUT: kubeconfig_path is empty, meaning kubectl's own."""
    proc = _run_setup(tmp_path)

    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["kubeconfig_path"] == ""


def test_setup_stops_on_an_unexpected_context(tmp_path: Path) -> None:
    """K8S_EXPECTED_CONTEXT naming another context stops setup before it inventories or pins anything."""
    pinned = tmp_path / "kubeconfig"

    proc = _run_setup(tmp_path, K8S_EXPECTED_CONTEXT="ctx-b", K8S_KUBECONFIG_OUT=str(pinned))

    assert proc.returncode == 1
    assert "current context is 'ctx-a', expected K8S_EXPECTED_CONTEXT='ctx-b'" in proc.stderr
    assert not pinned.exists()
    assert (tmp_path / "calls").read_text().splitlines() == ["config current-context"]


def test_setup_demo_mode_never_contacts_a_cluster(tmp_path: Path) -> None:
    """ISVCTL_DEMO_MODE=1 prints a sample inventory without running kubectl."""
    proc = _run_setup(tmp_path, ISVCTL_DEMO_MODE="1", K8S_KUBECONFIG_OUT=str(tmp_path / "kubeconfig"))

    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["success"] is True
    assert not (tmp_path / "calls").exists()
