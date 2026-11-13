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
``--collect``, recording each call. The scripts mostly run in-process on a fake clock, so
minutes of polling take no time; the launcher itself always runs as a real subprocess.
"""

from __future__ import annotations

import fcntl
import importlib.util
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

PERF_DIR = Path(__file__).resolve().parents[1] / "configs" / "shared" / "storage" / "perf"

# The fake launcher. Its behaviour is set by files in the checkout:
#   statuses  - one `--status` reply per line, played in order (the last one repeats); a line
#               "FAIL" exits 1 with an API_UNAVAILABLE diagnostic, like a lost connection,
#               "FAIL:<REASON>[:<KIND>]" exits 1 with that reason (and resource kind), "SILENT"
#               exits 1 with no reason, and "BUSY" is refused as if another lifecycle command
#               held the lock
#   cancelled - created by `--cancel`; `--status` then reports a cancelled, uncollected sweep
#   cancels_needed   - `--cancel` calls before the cancel completes (default 1); until then
#                      the sweep is left in CANCEL_REQUESTED
#   cancel_requested - `--status` reports CANCEL_REQUESTED (NEXT_ACTION=INSPECT), and exits 1
#                      as storage-scale-test does for an incomplete cancel
#   lock_busy        - lifecycle commands refused as if another held the lock, this many times
#   collected - created by `--collect`; once present, `--status` reports the collected view
#   cleanup_pending  - `--collect` calls that publish results but leave cleanup pending
#   collect_deadlines - written by `--collect`: the KUBECTL_COLLECTION_TIMEOUT_SECONDS it got
#   collect_refused  - `--collect` exits 1 without collecting, as for a sweep that has not stopped
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
    case "$line" in
      FAIL) echo "STORAGE_SCALE_TEST_DIAGNOSTIC_REASON=API_UNAVAILABLE" >&2; exit 1 ;;
      FAIL:*)
        failure=${line#FAIL:}
        echo "STORAGE_SCALE_TEST_DIAGNOSTIC_REASON=${failure%%:*}" >&2
        [[ "$failure" != *:* ]] || echo "STORAGE_SCALE_TEST_DIAGNOSTIC_RESOURCE_KIND=${failure#*:}" >&2
        exit 1 ;;
      SILENT) exit 1 ;;
      BUSY) echo "Error: another Kubernetes lifecycle operation is active for $2/kubernetes" >&2; exit 1 ;;
    esac
    echo "$line" | tr ' ' '\n'
    ;;
  --collect)
    echo "${KUBECTL_COLLECTION_TIMEOUT_SECONDS:-unset}" >> "$base/collect_deadlines"
    if [[ -f "$base/collect_refused" ]]; then
      echo "Error: the attempt has not stopped; cancel it first" >&2
      exit 1
    fi
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


# kubectl for the run step's cluster identity (`kubectl config view --minify -o json`): it prints
# KUBECONFIG, which the tests write as JSON (see _kubeconfig), or one fixed cluster without it.
FAKE_KUBECTL = r"""#!/usr/bin/env bash
[[ "$1 $2" == "config view" ]] || exit 1
if [[ -n "${KUBECONFIG:-}" ]]; then exec cat "$KUBECONFIG"; fi
echo '{"current-context": "ctx-a", "contexts": [{"name": "ctx-a", "context": {"cluster": "a"}}],'
echo ' "clusters": [{"name": "a", "cluster": {"server": "https://a.example:443"}}]}'
"""


@pytest.fixture(autouse=True)
def fake_kubectl(tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every script finds the fake kubectl first, and no kubeconfig of the machine running the tests."""
    bin_dir = tmp_path_factory.mktemp("bin")
    kubectl = bin_dir / "kubectl"
    kubectl.write_text(FAKE_KUBECTL)
    kubectl.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.delenv("KUBECONFIG", raising=False)


def _kubeconfig(path: Path, context: str = "ctx-a", server: str = "https://a.example:443") -> Path:
    """A kubeconfig (JSON, which kubectl also reads) whose current context points at *server*."""
    path.write_text(
        json.dumps(
            {
                "current-context": context,
                "contexts": [{"name": context, "context": {"cluster": f"{context}-cluster", "user": "u"}}],
                "clusters": [{"name": f"{context}-cluster", "cluster": {"server": server}}],
                "users": [{"name": "u", "user": {"token": "not-a-real-token"}}],
            }
        )
    )
    return path


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


class FakeClock:
    """Stands in for sst.CLOCK: sleeping only moves the time forward."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        """The fake time."""
        return self.now

    def sleep(self, seconds: float) -> None:
        """Advance the fake time."""
        self.now += seconds


def _run_inprocess(
    name: str,
    argv: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    clock: FakeClock | None = None,
) -> tuple[int, dict[str, Any]]:
    """Run a perf script's main() in this process on a fake clock; return its exit code and JSON."""
    module = _load(name)
    monkeypatch.setattr(module.sst, "CLOCK", clock or FakeClock())
    monkeypatch.setattr(module.sst, "DEMO_MODE", False)
    capsys.readouterr()
    rc = module.main(argv)
    return rc, json.loads(capsys.readouterr().out)


class InProcess:
    """Runs perf scripts in-process, sharing one fake clock across a test."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
        self.monkeypatch = monkeypatch
        self.capsys = capsys
        self.clock = FakeClock()

    def __call__(self, name: str, *argv: str) -> tuple[int, dict[str, Any]]:
        """Run one script with these arguments."""
        return _run_inprocess(name, list(argv), self.monkeypatch, self.capsys, self.clock)


@pytest.fixture
def perf(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> InProcess:
    """Run perf scripts in-process on a fake clock."""
    return InProcess(monkeypatch, capsys)


def _run_storage_test(perf: InProcess, base: Path, state_file: Path, *extra: str) -> tuple[int, dict[str, Any]]:
    """Run run_storage_test.py in-process against the fake checkout."""
    return perf(
        "run_storage_test",
        f"--scale-test-base={base}",
        "--claim",
        "perf-ns/perf-claim",
        "--node-selector=storage-test=true",
        "--results-dir",
        str(base / "results"),
        "--state-file",
        str(state_file),
        *extra,
    )


def test_run_submits_waits_and_collects(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """A finished sweep is collected, and the state file records that it was."""
    state_file = tmp_path / "run" / "state.json"
    rc, out = _run_storage_test(perf, checkout, state_file)

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


def test_run_writes_env_sh_and_input_files(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """Settings go before the line that loads defaults; target and workload are split."""
    state_file = tmp_path / "run" / "state.json"
    _run_storage_test(perf, checkout, state_file, "--io-sizes", "r4K,1M", "--duration", "10")

    env_sh = (checkout / "env.sh").read_text()
    assert env_sh.index("export EXECUTION_SUBSTRATE=kubectl") < env_sh.index(OVERRIDES_ANCHOR)
    assert "export KUBECTL_NODE_SELECTOR='storage-test=true'" in env_sh
    assert f"export LOGS_DIR='{checkout / 'results' / 'logs'}'" in env_sh
    target = (state_file.parent / "target.env").read_text()
    assert target == "TEST_DIRS=(['perf-ns/perf-claim:scale-test']=1)\n"
    workload = (state_file.parent / "workload.env").read_text()
    assert "ELBENCHO_SCALE_IO_SIZES=('r4K' '1M')" in workload
    assert "ELBENCHO_SCALE_READ_WRITE_DURATION=10" in workload


def test_run_refuses_an_env_sh_it_did_not_write(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """A user's own env.sh is never overwritten."""
    (checkout / "env.sh").write_text("export EXECUTION_SUBSTRATE=slurm\n")
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json")

    assert rc == 1
    assert "not written by isvctl" in out["error"]
    assert (checkout / "env.sh").read_text() == "export EXECUTION_SUBSTRATE=slurm\n"
    assert not (checkout / "calls").exists()


def test_run_keeps_waiting_through_lost_api_access(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """Lost API access shorter than the grace is waited out: the sweep runs on in-cluster."""
    (checkout / "statuses").write_text(f"{RUNNING}\nFAIL\nFAIL\n{FINISHED}\n")
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json")

    assert rc == 0, out
    assert out["state"] == "SUCCESS"
    assert [line.split("kind=")[1].split()[0] for line in _run_log(out) if "  poll  " in line] == [
        "ok",
        "access",
        "access",
        "ok",
    ]


def test_run_cancels_and_collects_a_sweep_that_outlives_the_wait(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """At --wait-timeout the sweep is cancelled and collected, and the step fails."""
    (checkout / "statuses").write_text(f"{RUNNING}\n")
    state_file = tmp_path / "state.json"
    rc, out = _run_storage_test(perf, checkout, state_file, "--wait-timeout", "60")

    assert rc == 1
    assert out["state"] == "TIMEOUT"
    assert out["resources_released"] is True
    calls = (checkout / "calls").read_text()
    assert "--cancel " in calls and "--collect " in calls
    assert json.loads(state_file.read_text())["collected"] is True


def test_run_reports_a_cancel_that_did_not_stop_the_sweep(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """A cancel that never completes leaves the sweep holding the claim: collect cannot release it,
    so the result says the cancel failed and to cancel before collecting.
    """
    (checkout / "statuses").write_text(f"{RUNNING}\n")
    (checkout / "cancels_needed").write_text("9\n")
    (checkout / "collect_refused").touch()
    state_file = tmp_path / "state.json"
    rc, out = _run_storage_test(perf, checkout, state_file, "--wait-timeout", "60")

    assert rc == 1
    assert out["state"] == "TIMEOUT"
    assert out["resources_released"] is False
    results_dir = out["results_dir"]
    launcher = checkout / "storage-tests" / "fs" / "nv-elbencho-sweep.sh"
    assert "cancel did not stop the sweep" in out["error"]
    collect_error = out["collect_error"]
    assert "cancel did not stop the sweep" in collect_error
    cancel_at = collect_error.index(f"`{launcher} --cancel {results_dir}`")
    assert collect_error.index(f"`{launcher} --collect {results_dir}`") > cancel_at
    assert "release_storage_test" in collect_error
    assert json.loads(state_file.read_text())["collected"] is False


def test_run_finishes_a_cancel_made_outside_it(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """A sweep cancelled from elsewhere stops in CANCEL_REQUESTED; the step completes the
    cancel and collects, rather than reporting the sweep as leaked.
    """
    (checkout / "cancel_requested").touch()
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json")

    assert rc == 1
    assert out["state"] == "CANCELLED"
    assert out["error"] == "the sweep was cancelled outside this run"
    assert out["resources_released"] is True
    assert "collect_error" not in out


def test_run_does_not_collect_a_rolled_back_submission(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """SUBMISSION_FAILED means storage-scale-test already removed what submit created."""
    (checkout / "statuses").write_text("STATE=SUBMISSION_FAILED EXECUTIONS_TOTAL=2 NEXT_ACTION=INSPECT\n")
    state_file = tmp_path / "state.json"
    rc, out = _run_storage_test(perf, checkout, state_file)

    assert rc == 1
    assert out["state"] == "SUBMISSION_FAILED"
    assert out["resources_released"] is True
    assert "--collect" not in (checkout / "calls").read_text()
    assert json.loads(state_file.read_text())["collected"] is True


def test_collect_retries_while_cleanup_is_pending(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """A collect can publish the results yet leave cleanup pending; collecting again finishes it."""
    (checkout / "cleanup_pending").write_text("1\n")
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json")

    assert rc == 0, out
    assert out["resources_released"] is True
    assert (checkout / "calls").read_text().count("--collect ") == 2


def test_collect_reports_cleanup_that_never_finishes(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """After its retries, collect says the sweep still holds resources and how to release them."""
    (checkout / "cleanup_pending").write_text("9\n")
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json")

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


def test_a_submit_killed_before_printing_its_results_dir_is_still_released(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """Submit holds the Lease before it prints the handle; teardown finds the sweep anyway.

    An earlier run's collected record must not hide the new sweep.
    """
    (checkout / "submit_dies").touch()
    (checkout / "statuses").write_text(f"{RUNNING}\n")
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"results_dir": "/old/elbencho-x", "collected": True}))

    _, run = _run_storage_test(perf, checkout, state_file)
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


def test_release_accepts_a_submit_that_never_published_an_attempt(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """storage-scale-test cannot inspect such a directory, and it owns no cluster resource."""
    (checkout / "submit_dies").touch()
    (checkout / "no_attempt").touch()
    state_file = tmp_path / "state.json"

    _run_storage_test(perf, checkout, state_file)
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


def test_release_waits_for_a_busy_lock(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """storage-scale-test refuses a command while another holds the lock; release retries it
    every 5 s, for as long as a killed call's leftover can hold the lock.
    """
    (checkout / "lock_busy").write_text("3\n")
    state_file = _left_behind(checkout, tmp_path)

    rc, out = perf("release_storage_test", "--state-file", str(state_file))

    assert rc == 0, out
    assert out["released"] is True
    assert perf.clock.now - 1000.0 == 15


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


def test_release_reports_a_cancel_that_failed_before_an_unconfirmed_collect(checkout: Path, tmp_path: Path) -> None:
    """With the sweep's state unreadable, release cancels and collects anyway; when the cancel did not
    stop it and collect cannot release it, the error says to cancel before collecting.
    """
    (checkout / "statuses").write_text("FAIL:LEDGER_INCONSISTENT\n")
    (checkout / "cancels_needed").write_text("9\n")
    (checkout / "collect_refused").touch()
    state_file = _left_behind(checkout, tmp_path)
    results_dir = json.loads(state_file.read_text())["results_dir"]
    launcher = checkout / "storage-tests" / "fs" / "nv-elbencho-sweep.sh"

    rc, out = _run_script("release_storage_test", "--state-file", str(state_file))

    assert rc == 1
    assert out["released"] is False
    error = out["error"]
    assert "cancel did not stop the sweep" in error
    assert error.index(f"`{launcher} --collect {results_dir}`") > error.index(f"`{launcher} --cancel {results_dir}`")
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


@pytest.mark.parametrize(
    ("stub", "field", "value"),
    [
        ("create_perf_filesystem", "filesystem_id", "pv-perf"),
        ("provision_perf_storage", "claim_name", "perf-claim"),
    ],
)
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


def _load_k8s_claim() -> ModuleType:
    """The my-isv scripts' shared claim helpers (scripts/common/k8s_claim.py), as a module."""
    path = MY_ISV_STORAGE.parent / "common" / "k8s_claim.py"
    spec = importlib.util.spec_from_file_location("my_isv_k8s_claim", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _only_on_path(tmp_path: Path, *tools: str) -> Path:
    """A PATH directory holding just *tools*, each a fake that logs its arguments and prints a usable claim."""
    bin_dir = tmp_path / "only-bin"
    bin_dir.mkdir()
    (tmp_path / "pvc.json").write_text(json.dumps(USABLE_CLAIM))
    for tool in tools:
        fake = bin_dir / tool
        fake.write_text(f'#!/bin/sh\necho {tool} "$@" >> {tmp_path / "calls"}\n/bin/cat {tmp_path / "pvc.json"}\n')
        fake.chmod(0o755)
    return bin_dir


def _run_stub_with_path(name: str, path: Path, *args: str, kubectl: str | None = None) -> dict[str, Any]:
    """Run a my-isv perf fixture stub (not in demo mode) with PATH set to *path* alone."""
    env = {k: v for k, v in os.environ.items() if k not in ("ISVCTL_DEMO_MODE", "KUBECTL")}
    env["PATH"] = str(path)
    if kubectl is not None:
        env["KUBECTL"] = kubectl
    proc = subprocess.run(
        [sys.executable, str(MY_ISV_STORAGE / f"{name}.py"), *args],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    return json.loads(proc.stdout)


@pytest.mark.parametrize(
    ("tools", "kubectl", "expected"),
    [
        (("kubectl", "microk8s"), None, "kubectl"),
        (("microk8s",), None, "microk8s kubectl"),
        (("microk8s",), "  ", "microk8s kubectl"),
        (("kubectl", "oc"), "oc", "oc"),
    ],
    ids=["kubectl", "microk8s-fallback", "blank-override", "override"],
)
def test_the_claim_lookup_finds_kubectl_like_setup_sh(
    tools: tuple[str, ...], kubectl: str | None, expected: str, tmp_path: Path
) -> None:
    """KUBECTL if it is not blank, else kubectl, else `microk8s kubectl` - the order scripts/k8s/setup.sh uses."""
    bin_dir = _only_on_path(tmp_path, *tools)

    out = _run_stub_with_path(
        "provision_perf_storage", bin_dir, "--namespace=perf-ns", "--existing-claim=perf-claim", kubectl=kubectl
    )

    assert out["success"] is True, out
    assert (tmp_path / "calls").read_text().startswith(f"{expected} -n perf-ns get pvc perf-claim")


def test_the_claim_lookup_without_kubectl_names_the_override(tmp_path: Path) -> None:
    """With neither kubectl nor microk8s on PATH, setup fails and says to set KUBECTL."""
    out = _run_stub_with_path(
        "create_perf_filesystem", _only_on_path(tmp_path), "--namespace=perf-ns", "--existing-claim=perf-claim"
    )

    assert out["success"] is False
    assert "Neither kubectl nor microk8s found. Set KUBECTL to override." in out["error"]
    _assert_schema("create_perf_filesystem", out)


def test_the_cluster_identity_uses_the_same_kubectl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """cluster_identity (and so other_cluster) reads the kubeconfig through the same kubectl as the claim lookup."""
    kubeconfig = _kubeconfig(tmp_path / "kubeconfig", "ctx-m", "https://m.example:16443")
    bin_dir = tmp_path / "m-bin"
    bin_dir.mkdir()
    microk8s = bin_dir / "microk8s"
    microk8s.write_text('#!/bin/sh\n[ "$1 $2 $3" = "kubectl config view" ] && exec /bin/cat "$KUBECONFIG"\nexit 1\n')
    microk8s.chmod(0o755)
    monkeypatch.setenv("PATH", str(bin_dir))
    monkeypatch.delenv("KUBECTL", raising=False)
    claims = _load_k8s_claim()

    assert claims.cluster_identity(str(kubeconfig)) == {"context": "ctx-m", "server": "https://m.example:16443"}
    assert claims.other_cluster({"context": "ctx-m", "server": "https://m.example:16443"}, str(kubeconfig)) == ""


@pytest.mark.parametrize(
    ("claim", "problem"),
    [
        (USABLE_CLAIM, ""),
        ({**USABLE_CLAIM, "status": {"phase": "Pending"}}, "claim not usable: phase=Pending"),
        (None, "kubectl get pvc perf-ns/perf-claim failed"),
    ],
    ids=["usable", "unusable", "missing"],
)
def test_adopt_claim_returns_a_usable_claim_or_why_not(
    claim: dict[str, Any] | None, problem: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """adopt_claim is get_claim then claim_problem: the claim when the sweep can use it, else None and why."""
    bin_dir = _only_on_path(tmp_path, "kubectl")
    if claim is None:
        (bin_dir / "kubectl").write_text("#!/bin/sh\nexit 1\n")
    else:
        (tmp_path / "pvc.json").write_text(json.dumps(claim))
    monkeypatch.setenv("PATH", str(bin_dir))
    monkeypatch.delenv("KUBECTL", raising=False)

    pvc, error = _load_k8s_claim().adopt_claim("", "perf-ns", "perf-claim")

    if problem:
        assert pvc is None and error.startswith(problem)
    else:
        assert pvc == claim and error == ""


def test_write_record_then_read_record_round_trips(tmp_path: Path) -> None:
    """write_record creates the record's directory; read_record returns what was written."""
    claims = _load_k8s_claim()
    record = tmp_path / "deep" / "record.json"

    claims.write_record(str(record), {"claim_name": "c", "created": True})

    assert json.loads(record.read_text(encoding="utf-8")) == {"claim_name": "c", "created": True}
    assert claims.read_record(str(record)) == (True, {"claim_name": "c", "created": True})


def test_read_record_keeps_what_the_file_holds(tmp_path: Path) -> None:
    """No record is (False, {}); a non-object comes back as stored; a corrupt one raises, as the scripts always did."""
    claims = _load_k8s_claim()
    record = tmp_path / "record.json"

    assert claims.read_record("") == (False, {})
    assert claims.read_record(str(record)) == (False, {})
    record.write_text("[1, 2]")
    assert claims.read_record(str(record)) == (True, [1, 2])
    record.write_text("{not json")
    with pytest.raises(ValueError):
        claims.read_record(str(record))


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
    assert _flag(args, "--wait-timeout") == "auto"
    assert [_flag(args, f) for f in ("--wait-cap", "--stall-timeout", "--unreachable-grace")] == [
        "43200",
        "auto",
        "1800",
    ]
    assert [_flag(args, "--live-interval-ms"), _flag(args, "--live-csv-extended")] == ["1000", "1"]
    assert [_flag(args, "--status-timeout"), _flag(args, "--collect-timeout")] == ["300", "8400"]
    assert "--time-budget" not in args
    release = _render("release_storage_test")
    assert [_flag(release, "--status-timeout"), _flag(release, "--collect-timeout")] == ["300", "8400"]


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


def test_rendered_settings_reach_storage_scale_test_inputs(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
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

    rc, out = perf("run_storage_test", *args)

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


# ─── Limits: every call bounded, interrupts stop the call, collect has one budget ─────

# Started by a command under test: leaves its process group, takes the lock file, records its
# pid and sleeps, still holding the command's stdout and stderr. storage-scale-test's kubectl
# under `timeout` does the same.
ORPHAN = """
import fcntl, os, sys, time
os.setpgid(0, 0)
lock = open(sys.argv[1], "w")
fcntl.flock(lock, fcntl.LOCK_EX)
open(sys.argv[2], "w").write(str(os.getpid()))
time.sleep(float(sys.argv[3]))
"""


def _lock_is_free(path: Path) -> bool:
    """True when nothing holds an exclusive flock on *path*."""
    fd = os.open(path, os.O_RDWR | os.O_CREAT)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False
    finally:
        os.close(fd)


def _alive(pid: int) -> bool:
    """True while process *pid* exists."""
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def _run_with_orphan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, orphan_s: float
) -> tuple[Any, Any, int, float, list[Any]]:
    """Time out a command whose orphan holds the lock and the pipes; return (sst, ran, pid, seconds,
    the call's Popen objects).
    """
    sst = _load("sst")
    monkeypatch.setattr(sst, "TERM_GRACE_S", 1)
    monkeypatch.setattr(sst, "DRAIN_S", 1)
    started_procs: list[Any] = []

    class Recorded(subprocess.Popen):  # type: ignore[type-arg]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            started_procs.append(self)

    monkeypatch.setattr(sst.subprocess, "Popen", Recorded)
    script = tmp_path / "orphan.py"
    script.write_text(ORPHAN)
    lock, pid_file = tmp_path / "lock", tmp_path / "orphan.pid"
    command = f'"{sys.executable}" "{script}" "{lock}" "{pid_file}" {orphan_s} & sleep 60'
    started = time.monotonic()
    ran = sst.run(["bash", "-c", command], 1)
    elapsed = time.monotonic() - started
    return sst, ran, int(pid_file.read_text()), elapsed, started_procs


def test_a_timed_out_call_returns_despite_an_orphan_holding_its_pipes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A leftover outside the call's process group cannot keep the call from returning; the call
    closes its end of the pipes rather than leave them to the leftover.
    """
    sst, ran, pid, elapsed, procs = _run_with_orphan(tmp_path, monkeypatch, 30)
    try:
        assert ran.timed_out is True and ran.returncode == 124
        assert "timed out after 1s" in ran.stderr
        assert elapsed < 1 + sst.TERM_GRACE_S + sst.DRAIN_S + 1.5
        assert _alive(pid)
        (proc,) = procs
        assert proc.stdout.closed and proc.stderr.closed
    finally:
        os.kill(pid, signal.SIGKILL)


def test_the_lock_frees_when_the_orphan_exits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The leftover holds the results directory's lock only until it exits."""
    _, ran, pid, _, _ = _run_with_orphan(tmp_path, monkeypatch, 5)
    assert ran.timed_out is True
    assert not _lock_is_free(tmp_path / "lock")
    deadline = time.monotonic() + 15
    while not _lock_is_free(tmp_path / "lock") and time.monotonic() < deadline:
        time.sleep(0.2)
    assert _lock_is_free(tmp_path / "lock")
    assert not _alive(pid)


def _hanging_checkout(tmp_path: Path, ignore_term: bool = False) -> Path:
    """A checkout whose launcher records its pid and never returns (and, if asked, ignores SIGTERM)."""
    base = tmp_path / "hanging"
    launcher = base / "storage-tests" / "fs" / "nv-elbencho-sweep.sh"
    launcher.parent.mkdir(parents=True)
    trap = "trap '' TERM\n" if ignore_term else ""
    launcher.write_text(f'#!/usr/bin/env bash\n{trap}echo $$ > "{base}/pid"\nexec sleep 60\n')
    launcher.chmod(0o755)
    (base / "env.sh.template").write_text(f"TEST_DIRS=()\n{OVERRIDES_ANCHOR}\n")
    return base


@pytest.mark.parametrize("ignore_term", [False, True])
def test_an_interrupt_stops_the_running_call_within_two_seconds(ignore_term: bool, tmp_path: Path) -> None:
    """Ctrl-C stops the storage-scale-test call in flight and exits 130 before isvctl's 2 s kill; a
    call that ignores SIGTERM is killed.
    """
    base = _hanging_checkout(tmp_path, ignore_term)
    env = {k: v for k, v in os.environ.items() if k != "ISVCTL_DEMO_MODE"}
    args = [f"--scale-test-base={base}", "--claim=a/b", "--node-selector=a=b"]
    args += [f"--results-dir={tmp_path / 'results'}", f"--state-file={tmp_path / 'state.json'}"]
    proc = subprocess.Popen(
        [sys.executable, str(PERF_DIR / "run_storage_test.py"), *args, f"--run-log-dir={tmp_path / 'logs'}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    deadline = time.monotonic() + 20
    while not (base / "pid").is_file() and time.monotonic() < deadline:
        time.sleep(0.1)
    launcher_pid = int((base / "pid").read_text())

    proc.send_signal(signal.SIGINT)
    started = time.monotonic()
    rc = proc.wait(timeout=10)

    assert time.monotonic() - started < 2
    assert rc == 130
    (log,) = (tmp_path / "logs").iterdir()
    assert "  interrupt  signal=SIGINT" in log.read_text()
    deadline = time.monotonic() + 2
    while _alive(launcher_pid) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not _alive(launcher_pid)


@pytest.mark.parametrize(
    "step", ["run_storage_test", "release_storage_test", "evaluate_storage_test", "find_storage_scale_test"]
)
def test_the_interrupt_handler_is_restored_after_main_returns(
    step: str, perf: InProcess, checkout: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every step that starts storage-scale-test calls (each in its own session) handles Ctrl-C and
    SIGTERM while they run; afterwards they behave as before.
    """
    sst = _load(step).sst
    real_run = sst.run
    during: list[tuple[Any, Any]] = []

    def spy(cmd: list[str], timeout: float, env: dict[str, str] | None = None) -> Any:
        during.append((signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)))
        return real_run(cmd, timeout, env)

    monkeypatch.setattr(sst, "run", spy)
    _fake_extractor(checkout, "exit 1")
    argv = {
        "run_storage_test": None,
        "release_storage_test": ["--state-file", str(_left_behind(checkout, tmp_path))],
        "evaluate_storage_test": [f"--scale-test-base={checkout}", "--run", json.dumps(RUN_OK)],
        "find_storage_scale_test": [f"--base={checkout}"],
    }[step]
    before = (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM))

    if argv is None:
        # Not beside the release case's left-behind record, which would refuse the run.
        _run_storage_test(perf, checkout, tmp_path / "run" / "state.json")
    else:
        perf(step, *argv)

    assert during
    for on_int, on_term in during:
        assert on_int is on_term and on_int not in (*before, signal.SIG_DFL, signal.default_int_handler)
    assert (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)) == before


def test_the_interrupt_handler_exits_130_even_when_it_cannot_report(monkeypatch: pytest.MonkeyPatch) -> None:
    """A signal that lands mid-write can make reporting fail; the handler still exits 130."""
    sst = _load("sst")
    exits: list[int] = []

    def fake_exit(code: int) -> None:
        exits.append(code)
        raise SystemExit(code)

    class BrokenLog:
        def event(self, name: str, **fields: Any) -> None:
            raise RuntimeError("reentrant call")

    monkeypatch.setattr(sst.os, "_exit", fake_exit)
    restore = sst.install_interrupt_handler(BrokenLog())  # type: ignore[arg-type]
    try:
        handler = signal.getsignal(signal.SIGTERM)
        assert callable(handler)
        with pytest.raises(SystemExit):
            handler(signal.SIGTERM, None)
    finally:
        restore()

    assert exits == [130]


class Scripted:
    """Stands in for sst.run: one reply list per operation, each call taking fake time.

    A reply is ``(Ran, seconds)``; a timed-out reply takes the call's whole timeout. The last
    reply for an operation repeats.
    """

    def __init__(self, sst: ModuleType, clock: FakeClock, replies: dict[str, list[tuple[Any, float]]]) -> None:
        self.sst = sst
        self.clock = clock
        self.replies = replies
        self.calls: list[tuple[str, float, dict[str, str] | None]] = []

    def __call__(self, cmd: list[str], timeout: float, env: dict[str, str] | None = None) -> Any:
        """Reply to one launcher call."""
        operation = cmd[1] if len(cmd) > 1 and cmd[1].startswith("--") else "submit"
        self.calls.append((operation, timeout, env))
        queue = self.replies[operation]
        ran, seconds = queue.pop(0) if len(queue) > 1 else queue[0]
        self.clock.now += timeout if ran.timed_out else seconds
        return ran

    def deadlines(self) -> list[str]:
        """The deadline each --collect passed to storage-scale-test."""
        return [env["KUBECTL_COLLECTION_TIMEOUT_SECONDS"] for op, _, env in self.calls if op == "--collect" and env]


COMPLETE = "STATE=SUCCESS\nEXECUTIONS_TOTAL=2\nRESULT_COLLECTION=COMPLETE\nNEXT_ACTION=NONE\n"
CLEANUP = "STATE=SUCCESS\nEXECUTIONS_TOTAL=2\nRESULT_COLLECTION=CLEANUP_PENDING\nNEXT_ACTION=COLLECT\n"


def _scripted(monkeypatch: pytest.MonkeyPatch, replies: dict[str, list[tuple[Any, float]]]) -> tuple[Any, Scripted]:
    """A fresh sst whose launcher calls are scripted, on a fake clock."""
    sst = _load("sst")
    clock = FakeClock()
    scripted = Scripted(sst, clock, replies)
    monkeypatch.setattr(sst, "CLOCK", clock)
    monkeypatch.setattr(sst, "run", scripted)
    return sst, scripted


def test_collect_shares_one_budget_across_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    """A collect that uses its whole call leaves no budget for a second attempt."""
    sst = _load("sst")
    sst, scripted = _scripted(
        monkeypatch,
        {"--collect": [(sst.Ran(124, "", "timed out", True), 0)], "--status": [(sst.Ran(0, CLEANUP, ""), 300)]},
    )

    out = sst.collect(Path("/base"), "/r", {}, collect_timeout=8400, status_timeout=300)

    assert [(op, timeout) for op, timeout, _ in scripted.calls] == [("--collect", 8100), ("--status", 300)]
    assert out["resources_released"] is False
    assert "collect budget exhausted" in out["collect_error"]
    assert "--collect /r" in out["collect_error"]


def test_collect_passes_its_deadline_to_storage_scale_test(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """storage-scale-test gets the budget less one status check and the reserve as its own deadline."""
    rc, _ = _run_storage_test(perf, checkout, tmp_path / "state.json")

    assert rc == 0
    assert (checkout / "collect_deadlines").read_text().splitlines() == ["7200"]


def test_collect_never_passes_a_deadline_below_60_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    """storage-scale-test rejects a deadline below 60 s, so collect stops rather than pass one."""
    sst = _load("sst")
    replies = {"--collect": [(sst.Ran(0, "", ""), 10)], "--status": [(sst.Ran(0, COMPLETE, ""), 1)]}
    sst, scripted = _scripted(monkeypatch, replies)

    assert sst.collect(Path("/base"), "/r", {}, collect_timeout=1260, status_timeout=300)["resources_released"]
    assert scripted.deadlines() == ["60"]

    scripted.calls.clear()
    out = sst.collect(Path("/base"), "/r", {}, collect_timeout=1259, status_timeout=300)
    assert scripted.calls == []
    assert "collect budget exhausted" in out["collect_error"]


def test_collect_confirms_within_what_is_left_of_its_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    """The confirming --status and its lock retries get only what is left of the budget; a collect
    whose outcome could not be confirmed in it says so, rather than that resources are held.
    """
    sst = _load("sst")
    busy = sst.Ran(1, "", f"Error: {sst.LOCK_BUSY} for /r/kubernetes")
    complete = sst.Ran(0, COMPLETE, "")

    # A collect that ran 50 s past its own timeout leaves 250 s for the status.
    sst, scripted = _scripted(monkeypatch, {"--collect": [(sst.Ran(0, "", ""), 8150)], "--status": [(complete, 1)]})
    assert sst.collect(Path("/base"), "/r", {}, collect_timeout=8400, status_timeout=300)["resources_released"]
    assert [(op, timeout) for op, timeout, _ in scripted.calls] == [("--collect", 8100), ("--status", 250)]

    # 50 s left and the lock busy: the retries stop with the budget, and no call starts after it.
    sst, scripted = _scripted(monkeypatch, {"--collect": [(sst.Ran(0, "", ""), 8350)], "--status": [(busy, 1)]})
    out = sst.collect(Path("/base"), "/r", {}, collect_timeout=8400, status_timeout=300)
    assert scripted.clock.now - 1000.0 <= 8400 + sst.LOCK_RETRY_INTERVAL_S
    statuses = [timeout for op, timeout, _ in scripted.calls if op == "--status"]
    assert len(statuses) > 1 and max(statuses) <= 50
    assert out["resources_released"] is False
    assert out["collect_error"].startswith("could not confirm that collect released the sweep's cluster resources")
    assert "--status /r" in out["collect_error"] and "the sweep still holds" not in out["collect_error"]

    # Nothing left: no status check is started at all.
    sst, scripted = _scripted(monkeypatch, {"--collect": [(sst.Ran(0, "", ""), 8400)], "--status": [(complete, 1)]})
    out = sst.collect(Path("/base"), "/r", {}, collect_timeout=8400, status_timeout=300)
    assert [op for op, _, _ in scripted.calls] == ["--collect"]
    assert "could not confirm" in out["collect_error"] and "collect budget exhausted" in out["collect_error"]


def test_collect_recomputes_its_deadline_after_a_busy_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Time spent waiting for the lock comes out of the deadline and timeout of the retry."""
    sst = _load("sst")
    busy = sst.Ran(1, "", f"Error: {sst.LOCK_BUSY} for /r/kubernetes")
    replies = {"--collect": [(busy, 100), (sst.Ran(0, "", ""), 10)], "--status": [(sst.Ran(0, COMPLETE, ""), 1)]}
    sst, scripted = _scripted(monkeypatch, replies)

    out = sst.collect(Path("/base"), "/r", {}, collect_timeout=8400, status_timeout=300)

    assert out["resources_released"] is True
    assert scripted.deadlines() == ["7200", "7095"]
    assert [timeout for op, timeout, _ in scripted.calls if op == "--collect"] == [8100, 7995]


@pytest.mark.parametrize("operation", ["cancel", "collect"])
def test_cancel_and_collect_wait_out_a_busy_lock_for_130_seconds(
    operation: str, checkout: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A killed call's leftover can hold the lock for up to about 125 s; cancel and collect outlast it."""
    sst = _load("sst")
    clock = FakeClock()
    monkeypatch.setattr(sst, "CLOCK", clock)
    _left_behind(checkout, tmp_path)
    results_dir = str(checkout / "results" / "elbencho-20260101Z000000")
    (checkout / "lock_busy").write_text("26\n")

    if operation == "cancel":
        assert sst.cancel(checkout, results_dir, dict(os.environ), command_timeout=600, status_timeout=300)
    else:
        out = sst.collect(checkout, results_dir, dict(os.environ), collect_timeout=8400, status_timeout=300)
        assert out["resources_released"] is True
    assert clock.now - 1000.0 == 130

    (checkout / "lock_busy").write_text("99\n")
    ran = sst.lifecycle(
        checkout, f"--{operation}", results_dir, {}, timeout_s=lambda: 60, lock_window_s=sst.LOCK_WINDOW_S
    )
    assert sst.LOCK_BUSY in ran.stderr and ran.busy_retries == 26


def test_run_reports_the_status_collect_confirmed_without_asking_again(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """The status that confirmed collect is the run's final view; no further --status follows it."""
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json")

    assert rc == 0, out
    assert out["state"] == "SUCCESS" and out["executions"] == {"total": 2, "succeeded": 2, "failed": 0}
    calls = (checkout / "calls").read_text().splitlines()
    last_collect = max(i for i, call in enumerate(calls) if call.startswith("--collect "))
    assert [c.split()[0] for c in calls[last_collect + 1 :]] == ["--status"]


@pytest.mark.parametrize(
    ("run", "command_timeout", "expected"),
    [
        ({}, "0", 1800),
        ({"executions": {"total": 0}}, "0", 1800),
        ({"executions": {"total": 8}}, "0", 1800),
        ({"executions": {"total": 5624}}, "0", 11248),
        ({"executions": {"total": 5624}}, "30", 30),
    ],
)
def test_evaluate_scales_its_extract_timeout_with_the_cells(
    run: dict[str, Any], command_timeout: str, expected: int, perf: InProcess, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The extractor gets 2 s per cell, at least 1800 s, unless a timeout is given; a timeout says
    how to rerun it offline.
    """
    sst = _load("evaluate_storage_test").sst
    timeouts: list[float] = []

    def timed_out(cmd: list[str], timeout: float, env: dict[str, str] | None = None) -> Any:
        timeouts.append(timeout)
        return sst.Ran(124, "", f"timed out after {timeout}s", True)

    monkeypatch.setattr(sst, "run", timed_out)
    rc, out = perf(
        "evaluate_storage_test",
        "--scale-test-base=/sst",
        f"--command-timeout={command_timeout}",
        "--run",
        json.dumps({**RUN_OK, **run, "results_dir": "/r/elbencho-x"}),
    )

    assert rc == 1
    assert timeouts == [expected]
    assert out["tests"]["results_present"]["error"] == (
        f"extractor timed out after {expected}s; rerun it offline: "
        "/sst/utils/extract-elbencho.py /r/elbencho-x --to-json"
    )


def _run_log(out: dict[str, Any]) -> list[str]:
    """The lines of the run log a run step reported."""
    return Path(out["run_log"]).read_text().splitlines()


def test_each_poll_writes_one_run_log_line(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """Every status check while waiting is one `poll` line, numbered, with its kind and counts."""
    (checkout / "statuses").write_text(f"{RUNNING}\n{RUNNING}\n{FINISHED}\n")
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json")

    assert rc == 0, out
    assert Path(out["run_log"]).parent == checkout / "results" / "run-logs"
    polls = [line for line in _run_log(out) if "  poll  " in line]
    assert [line.split("  poll  ")[1].split()[0] for line in polls] == ["n=1", "n=2", "n=3"]
    assert "kind=ok state=RUNNING next_action=WAIT" in polls[0]
    assert "next_action=COLLECT" in polls[2]
    events = [line.split("  ")[2] for line in _run_log(out)]
    assert events == ["estimate", "submit", "poll", "poll", "poll", "stop", "collect"]


def test_the_run_log_keeps_only_allow_listed_fields(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """Nothing outside the allow-list reaches the log: not upstream output, not the kubeconfig."""
    (checkout / "statuses").write_text(f"{RUNNING} API_TOKEN=s3cr3t-token-value\n{FINISHED}\n")
    kubeconfig = tmp_path / "private-kubeconfig"
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", f"--kubeconfig={kubeconfig}")
    run_log = _load("sst").RunLog.reopen(out["run_log"])
    assert run_log is not None
    run_log.event("poll", token="s3cr3t-token-value", state="multi\nline")
    run_log.close()

    assert rc == 0, out
    text = Path(out["run_log"]).read_text()
    assert "s3cr3t" not in text and "private-kubeconfig" not in text
    assert text.splitlines()[-1].endswith("  poll  state=multi_line")
    keys = {pair.split("=")[0] for line in text.splitlines() for pair in line.split("  ")[3].split()}
    assert keys <= _load("sst").RUN_LOG_KEYS


def test_release_appends_to_the_run_log(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """Teardown's status check, cancel, collect and outcome go to the run's own log, with elapsed
    times that go on from the run's start.
    """
    state_file = _left_behind(checkout, tmp_path)
    log_path = tmp_path / "run-logs" / "run-20260101T000000Z-1.log"
    log_path.parent.mkdir()
    log_path.write_text(f"{datetime.now(UTC) - timedelta(seconds=100):%Y-%m-%dT%H:%M:%SZ}  +0  submit  rc=0\n")
    state_file.write_text(json.dumps({**json.loads(state_file.read_text()), "run_log": str(log_path)}))

    rc, out = perf("release_storage_test", "--state-file", str(state_file))

    assert rc == 0, out
    lines = log_path.read_text().splitlines()
    assert [line.split("  ")[2] for line in lines] == ["submit", "poll", "cancel", "collect", "release"]
    assert "  poll  kind=ok state=RUNNING next_action=WAIT" in lines[1]
    assert lines[-1].endswith("release  outcome=released")
    assert all(int(line.split("  ")[1].lstrip("+")) >= 100 for line in lines[1:])


def test_two_runs_started_in_the_same_second_get_separate_run_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A second log never overwrites or appends to the first; past -9 the run fails instead."""
    sst = _load("sst")

    class Frozen(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> Frozen:
            return cls(2026, 1, 1, tzinfo=UTC)

    monkeypatch.setattr(sst, "datetime", Frozen)
    first = sst.RunLog.create(tmp_path)
    second = sst.RunLog.create(tmp_path)
    first.event("submit", rc=0)
    second.event("submit", rc=1)

    assert second.path.stem == first.path.stem + "-1"
    assert first.path.read_text().endswith("rc=0\n") and second.path.read_text().endswith("rc=1\n")
    for n in range(2, 10):
        (tmp_path / f"{first.path.stem}-{n}.log").touch()
    with pytest.raises(FileExistsError, match=r"-9\.log"):
        sst.RunLog.create(tmp_path)


@pytest.mark.parametrize(
    ("settings", "error"),
    [
        (["--status-timeout", "299"], "--status-timeout is 299; it must be at least 300"),
        (["--collect-timeout", "1259"], "--collect-timeout is 1259; it must be at least 1260 (--status-timeout + 960)"),
        (
            ["--status-timeout", "600", "--collect-timeout", "1260"],
            "--collect-timeout is 1260; it must be at least 1560 (--status-timeout + 960)",
        ),
        (["--interval", "14"], "--interval is 14; it must be at least 15"),
        (["--duration", "0"], "--duration is 0; it must be at least 1"),
    ],
)
def test_invalid_time_settings_fail_before_submit(
    settings: list[str], error: str, perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """A limit too small to work is refused, naming the argument and its minimum, before anything
    runs; the run log says so too.
    """
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", *settings)

    assert rc == 1
    assert out["state"] == "NOT_SUBMITTED"
    assert out["error"] == error
    assert not (checkout / "calls").exists()
    assert not (tmp_path / "state.json").exists()
    assert [line.split("  ", 2)[2] for line in _run_log(out)] == ["refused  refusal=" + "_".join(error.split())]


def test_the_collect_minimum_follows_the_status_timeout(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """A raised --status-timeout raises collect's minimum with it, so collect can always run; release
    applies the same minimum.
    """
    limits = ["--status-timeout", "600", "--collect-timeout", "1560"]
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", *limits)

    assert rc == 0, out
    assert (checkout / "collect_deadlines").read_text().splitlines() == ["60"]

    (checkout / "calls").unlink()
    release = ["--state-file", str(tmp_path / "state.json"), "--status-timeout=600", "--collect-timeout=1559"]
    rc, out = perf("release_storage_test", *release)
    assert rc == 1 and out["released"] is False
    assert out["error"] == "--collect-timeout is 1559; it must be at least 1560 (--status-timeout + 960)"
    assert not (checkout / "calls").exists()


@pytest.mark.parametrize("refusal", [("--status-timeout", "10"), ("--scale-test-base=/no/such/checkout",)])
def test_a_refused_run_leaves_the_earlier_state_file_untouched(
    refusal: tuple[str, ...], perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """The record teardown needs for an earlier sweep survives a run that never submits."""
    state_file = tmp_path / "state.json"
    earlier = b'{"results_dir": "/r/elbencho-earlier", "collected": false}'
    state_file.write_bytes(earlier)

    rc, out = _run_storage_test(perf, checkout, state_file, *refusal)

    assert rc == 1 and out["state"] == "NOT_SUBMITTED"
    assert state_file.read_bytes() == earlier


def test_self_limiting_steps_have_no_isvctl_timeout() -> None:
    """The benchmark steps bound every call themselves; an isvctl timeout would only cut them short."""
    from isvctl.config.merger import merge_yaml_files
    from isvctl.config.schema import RunConfig

    files = [CONFIG_DIR / "storage.yaml", CONFIG_DIR / "overlays" / "storage-perf.yaml"]
    steps = {s.name: s for s in RunConfig.model_validate(merge_yaml_files(files, None)).commands["storage"].steps}

    for name in ("run_storage_test", "evaluate_storage_test", "release_storage_test"):
        assert steps[name].timeout is None, name


def test_the_scripts_still_run_as_programs(checkout: Path, tmp_path: Path) -> None:
    """Each step still runs as `python <script>`, as isvctl starts it."""
    (checkout / "statuses").write_text(f"{FINISHED}\n")
    rc, run = _run_script(
        "run_storage_test",
        f"--scale-test-base={checkout}",
        "--claim=perf-ns/perf-claim",
        "--node-selector=storage-test=true",
        f"--results-dir={checkout / 'results'}",
        f"--state-file={tmp_path / 'state.json'}",
    )
    assert rc == 0, run

    (tmp_path / "doc.json").write_text(json.dumps(_document()))
    _fake_extractor(checkout, f'cp "{tmp_path}/doc.json" "$1/results.json"')
    rc, evaluate = _run_script("evaluate_storage_test", f"--scale-test-base={checkout}", f"--run={json.dumps(run)}")
    assert rc == 0, evaluate

    rc, release = _run_script("release_storage_test", "--state-file", str(tmp_path / "state.json"))
    assert rc == 0 and release["message"] == "already collected; nothing to release"


# ─── Waiting: status classification, wait modes, guards, collection size ─────

VENDORED = PERF_DIR.parents[4] / "isvtest" / "vendor" / "storage-scale-test"


def _diag(reason: str, kind: str = "", name: str = "") -> str:
    """A storage-scale-test diagnostic block on stderr, quoted as it prints it."""
    return (
        "Error: the operation failed\n"
        f"STORAGE_SCALE_TEST_DIAGNOSTIC_REASON={reason}\n"
        f"STORAGE_SCALE_TEST_DIAGNOSTIC_RESOURCE_KIND={kind or "''"}\n"
        f"STORAGE_SCALE_TEST_DIAGNOSTIC_RESOURCE_NAME={name or "''"}\n"
    )


def test_status_reasons_cover_the_vendored_vocabulary() -> None:
    """Every reason storage-scale-test can report has exactly one class."""
    functions = VENDORED / "storage-tests" / "fs" / "kubectl" / "_nv-elbencho-kubectl-functions.sh"
    if not functions.is_file():
        pytest.skip("storage-scale-test is not vendored; run `make vendor`")
    text = functions.read_text(encoding="utf-8")
    body = text[text.index("kubectl_report_lifecycle_error() {") :]
    body = body[: body.index("\n}\n")]
    match = re.search(r'"\$reason" =~ \^\(([A-Z_|]+)\)\$', body)
    assert match, "the reason check moved; update the classes in sst.py"
    vocabulary = set(match.group(1).split("|"))

    sst = _load("sst")
    classes = [sst.REASONS_ACCESS, sst.REASONS_TRANSIENT, sst.REASONS_DETERMINISTIC]
    classes.append(frozenset({sst.IDENTITY_MISMATCH, sst.CANCELLATION_INCOMPLETE}))
    assert sum(len(c) for c in classes) == len(set().union(*classes)) == 19
    assert set().union(*classes) == vocabulary


def _status_cases() -> list[tuple[str, int, str, str, bool]]:
    """(expected kind, exit code, stdout, stderr, timed out), in the classification's order."""
    sst = _load("sst")
    cases = [
        ("ok", 0, "STATE=RUNNING\nNEXT_ACTION=WAIT\n", "", False),
        ("ok", 1, "STATE=CANCEL_REQUESTED\nNEXT_ACTION=INSPECT\n", _diag("CANCELLATION_INCOMPLETE"), False),
        ("busy", 1, "", f"Error: {sst.LOCK_BUSY} for /r/kubernetes\n", False),
        ("slow", 124, "", _diag("TIMEOUT") + "timed out after 300s", True),
        *(("access", 1, "", _diag(reason), False) for reason in sorted(sst.REASONS_ACCESS)),
        *(("transient", 1, "", _diag(reason), False) for reason in sorted(sst.REASONS_TRANSIENT)),
        ("transient", 1, "", _diag("CANCELLATION_INCOMPLETE"), False),
        ("transient", 1, "", _diag("A_REASON_FROM_A_LATER_RELEASE"), False),
        ("transient", 0, "STATE=RUNNING\nEXECUTIONS_TOTAL=2\n", "", False),
        ("unknown", 1, "", "", False),
        ("unknown", 1, "", "Error: exec into the status helper failed\n", False),
        *(("deterministic", 1, "", _diag(reason), False) for reason in sorted(sst.REASONS_DETERMINISTIC)),
        ("deterministic", 1, "", _diag("IDENTITY_MISMATCH", "PersistentVolumeClaim", "perf-claim"), False),
    ]
    return cases


@pytest.mark.parametrize(("kind", "rc", "stdout", "stderr", "timed_out"), _status_cases())
def test_status_is_classified_in_order(kind: str, rc: int, stdout: str, stderr: str, timed_out: bool) -> None:
    """Each --status reply gets the first class that matches (see sst.classify)."""
    sst = _load("sst")
    assert sst.classify(sst.Ran(rc, stdout, stderr, timed_out))[0] == kind


def test_ledger_inconsistent_with_a_state_is_not_ok() -> None:
    """LEDGER_INCONSISTENT prints the same shape as an incomplete cancel, but no state can be trusted."""
    sst = _load("sst")
    ran = sst.Ran(1, "STATE=CANCEL_REQUESTED\nNEXT_ACTION=INSPECT\n", _diag("LEDGER_INCONSISTENT"))

    assert sst.classify(ran)[0] == "deterministic"


@pytest.mark.parametrize(
    ("kind", "name", "expected"),
    [
        ("Pod", "storage-scale-test-status-0123abcd", "transient"),
        ("Pod", "storage-scale-test-collector-89abcdef", "transient"),
        ("Lease", "perf-claim-lease", "deterministic"),
        ("Job", "storage-scale-test-coordinator", "deterministic"),
    ],
)
def test_identity_mismatch_on_a_helper_pod_is_transient(kind: str, name: str, expected: str) -> None:
    """Each command recreates its helper Pod; a mismatch on what the sweep owns does not go away."""
    sst = _load("sst")
    assert sst.classify(sst.Ran(1, "", _diag("IDENTITY_MISMATCH", kind, name)))[0] == expected


# A --status reply and a failed one as storage-scale-test v2.1.0 prints them (paths shortened).
STATUS_SAMPLE = """ATTEMPT=0123abcd
STATE=RUNNING
EXECUTION_SCOPE=CURRENT_ATTEMPT
PROGRESS_SOURCE=PVC
EXECUTIONS_TOTAL=6
EXECUTIONS_PENDING=2
EXECUTIONS_RUNNING=1
EXECUTIONS_SUCCEEDED=3
EXECUTIONS_FAILED=0
RESULT_COLLECTION=PENDING
NEXT_ACTION=WAIT
"""
STATUS_FAILURE_SAMPLE = """Warning: transient Kubernetes observation failed (attempt 1/3); retrying
Unable to connect to the server: context deadline exceeded (Client.Timeout exceeded while awaiting headers)
STORAGE_SCALE_TEST_DIAGNOSTIC_OPERATION=kubectl-observe
STORAGE_SCALE_TEST_DIAGNOSTIC_PHASE=api-observation
STORAGE_SCALE_TEST_DIAGNOSTIC_REASON=TIMEOUT
STORAGE_SCALE_TEST_DIAGNOSTIC_RESOURCE_KIND=''
STORAGE_SCALE_TEST_DIAGNOSTIC_MAY_STILL_BE_RUNNING=unknown
STORAGE_SCALE_TEST_DIAGNOSTIC_SAFE_NEXT_ACTION=retry\\ the\\ same\\ lifecycle\\ command
Error: kubectl could not reach the API server using the active context
STORAGE_SCALE_TEST_DIAGNOSTIC_OPERATION=verify-cluster
STORAGE_SCALE_TEST_DIAGNOSTIC_PHASE=cluster-identity
STORAGE_SCALE_TEST_DIAGNOSTIC_LOCAL_STATE=SUBMITTED
STORAGE_SCALE_TEST_DIAGNOSTIC_REASON=TIMEOUT
STORAGE_SCALE_TEST_DIAGNOSTIC_RESOURCE_KIND=''
STORAGE_SCALE_TEST_DIAGNOSTIC_RESOURCE_NAME=''
STORAGE_SCALE_TEST_DIAGNOSTIC_LOCAL_STATE_PATH=/results/elbencho-x/kubernetes/attempts/0123abcd/state.sh
STORAGE_SCALE_TEST_DIAGNOSTIC_MAY_STILL_BE_RUNNING=unknown
STORAGE_SCALE_TEST_DIAGNOSTIC_PATH=''
STORAGE_SCALE_TEST_DIAGNOSTIC_SAFE_NEXT_ACTION=retry\\ the\\ same\\ lifecycle\\ command
"""


def test_status_parser_reads_a_recorded_status_sample() -> None:
    """Real replies: counts and progress from a good one; class, reason and message from a failed one."""
    sst = _load("sst")
    kind, fields = sst.classify(sst.Ran(0, STATUS_SAMPLE, ""))
    guards = sst.Guards(last_ok=0, last_progress=0, window=1800, grace=1800)

    assert kind == "ok"
    assert guards.observe(kind, fields, 60) is None
    assert (guards.max_done, guards.max_started, guards.last_progress) == (3, 4, 60)

    kind, fields = sst.classify(sst.Ran(1, "", STATUS_FAILURE_SAMPLE))
    assert kind == "access"
    assert fields["DIAGNOSTIC_REASON"] == "TIMEOUT" and "DIAGNOSTIC_RESOURCE_KIND" not in fields
    assert fields["DIAGNOSTIC_MESSAGE"] == (
        "kubectl could not reach the API server using the active context (TIMEOUT, phase cluster-identity); "
        "may still be running: unknown; next: retry the same lifecycle command"
    )


@pytest.mark.parametrize(
    ("spec", "counts"),
    [
        ("1", [1]),
        ("1-4", [1, 2, 3, 4]),
        ("1-10+4", [1, 5, 9, 10]),
        ("3-10+2,13", [3, 5, 7, 9, 10, 13]),
        ("16,8,4,2,1", [16, 8, 4, 2, 1]),
        ("2-2", [2]),
        ("0", None),
        ("2-1", None),
        ("1,,2", None),
        ("", None),
        ("all", None),
    ],
)
def test_node_spec_counts_cells(spec: str, counts: list[int] | None) -> None:
    """The node spec reads as storage-scale-test reads it; cells multiply by IO sizes, threads, depths."""
    sst = _load("sst")
    assert sst.parse_node_spec(spec) == counts
    cells = sst.count_cells(spec, ["r4K", "1M"], ["1", "2"], ["1"])
    assert cells == (None if counts is None else 4 * len(counts))


def _line(state: str = "RUNNING", total: int = 2, done: int = 0, pending: int | None = None, nxt: str = "WAIT") -> str:
    """A fake --status reply with execution counts."""
    counts = f"EXECUTIONS_TOTAL={total} EXECUTIONS_SUCCEEDED={done} EXECUTIONS_FAILED=0"
    counts += f" EXECUTIONS_PENDING={pending}" if pending is not None else ""
    return f"STATE={state} {counts} NEXT_ACTION={nxt}"


def _events(out: dict[str, Any], event: str) -> list[dict[str, str]]:
    """The fields of each run log line for *event*."""
    found = []
    for line in _run_log(out):
        parts = line.split("  ")
        if parts[2] == event:
            found.append(dict(pair.split("=", 1) for pair in (parts[3].split() if len(parts) > 3 else [])))
    return found


@pytest.mark.parametrize("zero", ["0", "00"])
def test_run_waits_without_a_deadline_when_wait_timeout_is_0(
    zero: str, perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """0 sets no wait limit: a sweep that takes longer than any automatic wait still finishes."""
    (checkout / "statuses").write_text(f"{RUNNING}\n" * 20 + f"{FINISHED}\n")

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", "--wait-timeout", zero)

    assert rc == 0, out
    assert perf.clock.now - 1000.0 >= 1200
    assert all("deadline_s" not in poll for poll in _events(out, "poll"))


def test_auto_wait_sizes_the_deadline_from_the_sweep(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """8 cells of 20 s phases: 150 + 8 x 45 = 510 s estimated, waited for twice that."""
    (checkout / "statuses").write_text(f"{RUNNING}\n")
    sweep = ["--nodes", "1-2", "--threads", "1,2", "--duration", "20"]

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", *sweep)

    assert rc == 1
    assert out["state"] == "TIMEOUT"
    assert (
        out["error"]
        == "sweep did not finish within --wait-timeout auto (1020s, at most --wait-cap 43200s); cancelling it"
    )
    (estimate,) = _events(out, "estimate")
    assert estimate == {"cells": "8", "initial_s": "510", "deadline_s": "1020", "bytes": "88000", "members": "70"}
    polls = _events(out, "poll")
    assert {p["deadline_s"] for p in polls} == {"1020"}
    assert int(polls[-1]["since_ok_s"]) == 0 and len(polls) == 1020 // 60 + 1


def test_auto_wait_deadline_keeps_the_safety_factor_and_never_shrinks(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """Cells slower than estimated push the deadline out to twice the projection; faster ones never pull it in."""
    statuses = [_line(done=0)] * 10 + [_line(done=1)] * 10 + [_line("SUCCESS", done=2, nxt="COLLECT")]
    (checkout / "statuses").write_text("\n".join(statuses) + "\n")

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json")

    assert rc == 0, out
    assert _events(out, "deadline") == [{"old_s": "900", "new_s": "2400", "why": "cells"}]
    deadlines = [int(p["deadline_s"]) for p in _events(out, "poll")]
    assert deadlines == sorted(deadlines) and deadlines[0] == 900 and deadlines[-1] == 2400
    sst = _load("sst")
    assert sst.auto_deadline_s(200, 43200, elapsed_s=10, done=1, total=2) == 900
    assert sst.auto_deadline_s(30000, 43200) == 43200


def test_auto_wait_refuses_a_sweep_longer_than_its_cap_before_submitting(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """An estimate beyond --wait-cap is refused with both numbers, before anything is submitted; a
    node spec it cannot read is checked at the sweep's first count instead.
    """
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", "--nodes", "1-2", "--duration", "40000")

    assert rc == 1
    assert out["state"] == "NOT_SUBMITTED"
    assert "about 320170 s, more than --wait-cap 43200 s" in out["error"]
    assert not (checkout / "calls").exists() and not (tmp_path / "state.json").exists()
    assert "refusal" in _events(out, "estimate")[0]

    (checkout / "statuses").write_text(f"{RUNNING}\n")
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", "--nodes", "all", "--duration", "40000")

    assert rc == 1
    assert out["state"] == "TIMEOUT"
    assert out["error"].startswith("sweep did not finish within --wait-cap 43200s (the sweep needs about 160160s)")
    assert _events(out, "stop") == [{"why": "cap"}] and out["resources_released"] is True

    # A sweep already finished at its first count is collected as normal.
    (checkout / "statuses").write_text(f"{FINISHED}\n")
    (checkout / "final").write_text("SUCCESS\n")
    for name in ("cancelled", "collected", "status_index"):
        (checkout / name).unlink(missing_ok=True)
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", "--nodes", "all", "--duration", "40000")
    assert rc == 0, out


@pytest.mark.parametrize("nodes", ["1", "all"])
def test_auto_wait_keeps_the_initial_estimate_without_counts(
    nodes: str, perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """Without execution counts the deadline stays the first estimate (at least 900 s, also for an unreadable spec)."""
    (checkout / "statuses").write_text("STATE=RUNNING NEXT_ACTION=WAIT\n")

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", "--nodes", nodes)

    assert rc == 1
    assert out["state"] == "TIMEOUT"
    assert _events(out, "deadline") == []
    assert {p["deadline_s"] for p in _events(out, "poll")} == {"900"}
    assert perf.clock.now - 1000.0 == 900  # the last check lands on the deadline


def test_run_stops_a_sweep_that_makes_no_progress(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """No cell done or started for --stall-timeout: cancelled, collected, STALLED."""
    (checkout / "statuses").write_text(f"{RUNNING}\n")

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", "--stall-timeout", "150")

    assert rc == 1
    assert out["state"] == "STALLED"
    # Judged at the poll after the window ran out: the error gives the time it actually saw.
    assert "made no progress for 180s (--stall-timeout 150s)" in out["error"]
    assert out["resources_released"] is True
    assert (checkout / "calls").read_text().count("--cancel ") == 1
    assert _events(out, "stop") == [{"why": "stalled"}]


def test_cells_that_start_count_as_progress(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """A long cell that has started is progress, even before any cell is done."""
    statuses = [_line(pending=2)] * 2 + [_line(pending=1)] * 3 + [FINISHED]
    (checkout / "statuses").write_text("\n".join(statuses) + "\n")

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", "--stall-timeout", "150")

    assert rc == 0, out
    assert [p["since_progress_s"] for p in _events(out, "poll")][:5] == ["0", "60", "0", "60", "120"]


def test_the_stall_estimate_never_drops_below_the_initial_one(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """auto: three cells' time, at least 1800 s; fast cells never shrink it, slow ones grow it."""
    sst = _load("sst")
    assert sst.stall_window_s(10) == 1800
    assert sst.stall_window_s(600) == 3615
    assert sst.stall_window_s(600, elapsed_s=100, done=10) == 3615
    assert sst.stall_window_s(10, elapsed_s=3000, done=1) == 9000

    statuses = [_line(done=0), _line(done=1), _line("SUCCESS", done=2, nxt="COLLECT")]
    (checkout / "statuses").write_text("\n".join(statuses) + "\n")
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", "--duration", "600")

    assert rc == 0, out
    assert {p["window_s"] for p in _events(out, "poll")} == {"3615"}


def _guards(**overrides: Any) -> Any:
    """Guards started at t=0 with a 1800 s window and grace, no deadline."""
    sst = _load("sst")
    return sst.Guards(**{"last_ok": 0, "last_progress": 0, "window": 1800, "grace": 1800, **overrides})


WAITING = {"STATE": "RUNNING", "EXECUTIONS_TOTAL": "2", "EXECUTIONS_SUCCEEDED": "0", "EXECUTIONS_FAILED": "0"}


@pytest.mark.parametrize(
    ("setup", "kind", "fields", "now", "why"),
    [
        # 1: finished wins over a passed deadline and a stall.
        ({"deadline": 10}, "ok", {**WAITING, "NEXT_ACTION": "COLLECT"}, 5000, "terminal"),
        # 2: deterministic wins over the unknown limit and an outage.
        ({"unknown": 4, "outage_since": 0}, "deterministic", {}, 5000, "deterministic"),
        # 3: the unknown limit wins over an outage and the watchdog.
        ({"unknown": 4, "outage_since": 0}, "unknown", {}, 5000, "unknown_limit"),
        # 4: an outage wins over the watchdog and the deadline.
        ({"outage_since": 0, "deadline": 10}, "access", {}, 1800, "outage"),
        # 5: the watchdog wins over the deadline.
        ({"deadline": 10}, "busy", {}, 1800, "watchdog"),
        # 6: a stall (judged on a good check) wins over the deadline.
        ({"deadline": 10}, "ok", {**WAITING, "NEXT_ACTION": "WAIT"}, 1800, "stalled"),
        # 7: the deadline, on a good check that made progress, or on any other check.
        ({"deadline": 10}, "ok", {**WAITING, "EXECUTIONS_SUCCEEDED": "1", "NEXT_ACTION": "WAIT"}, 1800, "deadline"),
        ({"deadline": 10}, "busy", {}, 60, "deadline"),
        ({}, "busy", {}, 60, None),
    ],
)
def test_guards_apply_in_precedence_order(
    setup: dict[str, Any], kind: str, fields: dict[str, str], now: float, why: str | None
) -> None:
    """With several guards due at once, the first in precedence order decides."""
    assert _guards(**setup).observe(kind, fields, now) == why


def _until_stopped(guards: Any, pattern: list[str], interval: float = 60, limit: float = 10_000) -> tuple[str, float]:
    """Feed *pattern* (repeating) to *guards* one check per *interval* after a good check at 0."""
    ok = {**WAITING, "NEXT_ACTION": "WAIT"}
    assert guards.observe("ok", ok, 0) is None
    now = 0.0
    for index in range(int(limit // interval)):
        now += interval
        kind = pattern[index % len(pattern)]
        why = guards.observe(kind, ok if kind == "ok" else {}, now)
        if why:
            return why, now
    raise AssertionError(f"{pattern} never ended the wait")


@pytest.mark.parametrize(
    ("pattern", "why", "at"),
    [
        (["unknown", "busy"], "unknown_limit", 540),
        (["unknown", "slow"], "unknown_limit", 540),
        (["slow", "busy"], "watchdog", 1800),
        (["busy"], "watchdog", 1800),
        (["access", "transient"], "outage", 660),
        (["transient"], "outage", 660),
        (["slow"], "outage", 720),
    ],
)
def test_mixed_status_failures_still_end_a_wait_without_a_deadline(pattern: list[str], why: str, at: float) -> None:
    """With no deadline, any mix of failed checks still ends the wait (grace 600 s, window 1800 s)."""
    assert _until_stopped(_guards(grace=600), pattern) == (why, at)


@pytest.mark.parametrize("failure", ["unknown", "access", "busy"])
def test_ok_polls_between_failures_still_stall_without_progress(failure: str) -> None:
    """Good checks that show no progress stall the sweep, whatever failures come between them."""
    assert _until_stopped(_guards(window=600), [failure, "ok"]) == ("stalled", 600)


def test_a_single_silent_failure_before_the_first_ok_keeps_waiting(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """A reason-less failure right after submit is counted, not acted on."""
    (checkout / "statuses").write_text(f"SILENT\n{RUNNING}\n{FINISHED}\n")

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json")

    assert rc == 0, out
    assert [p["kind"] for p in _events(out, "poll")] == ["unknown", "ok", "ok"]
    assert "--cancel" not in (checkout / "calls").read_text()


def test_silent_failures_count_until_the_next_ok() -> None:
    """Reason-less failures count since the last good check, before the first one too, and other
    failures in between do not reset the count.
    """
    guards = _guards()
    assert [guards.observe("unknown", {}, t) for t in (60, 120, 180, 240)] == [None] * 4
    assert guards.observe("unknown", {}, 300) == "unknown_limit"

    guards = _guards()
    for t in (60, 120, 180, 240):
        guards.observe("unknown", {}, t)
    assert guards.observe("ok", {**WAITING, "NEXT_ACTION": "WAIT"}, 300) is None
    assert [guards.observe(kind, {}, t) for kind, t in zip(["unknown"] * 4 + ["busy"], range(360, 660, 60))] == [
        None
    ] * 5
    assert guards.observe("unknown", {}, 660) == "unknown_limit"


def test_an_outage_longer_than_the_stall_window_is_unreachable_not_stalled(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """An outage is not a stall: no progress can be read, so the run waits for the grace, not the window."""
    (checkout / "statuses").write_text(f"{RUNNING}\nFAIL\n")
    limits = ["--stall-timeout", "120", "--unreachable-grace", "420"]

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", *limits)

    assert rc == 1
    assert out["state"] == "UNREACHABLE"
    assert _events(out, "stop")[0]["why"] in ("outage", "watchdog")
    assert perf.clock.now - 1000.0 >= 420


def test_a_busy_lock_is_not_an_outage() -> None:
    """Another command holding the lock means the cluster answers; only the watchdog ends it."""
    guards = _guards()
    assert all(guards.observe("busy", {}, t) is None for t in range(60, 1800, 60))
    assert guards.outage_since is None
    assert guards.observe("ok", {**WAITING, "EXECUTIONS_SUCCEEDED": "1", "NEXT_ACTION": "WAIT"}, 1790) is None


def test_transient_status_errors_do_not_cancel_a_running_sweep(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """A helper Pod that could not start is retried by the next check; the sweep is left alone."""
    (checkout / "statuses").write_text(f"{RUNNING}\nFAIL:POD_UNSCHEDULABLE\nFAIL:IMAGE_PULL\n{FINISHED}\n")

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json")

    assert rc == 0, out
    assert [p["kind"] for p in _events(out, "poll")] == ["ok", "transient", "transient", "ok"]
    assert "--cancel" not in (checkout / "calls").read_text()


def test_a_deterministic_status_error_cancels_and_collects(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """A failure asking again cannot fix stops the wait: one cancel, then collect."""
    (checkout / "statuses").write_text(f"{RUNNING}\nFAIL:LEDGER_INCONSISTENT\n")
    # A cancel that needs repeating is not repeated: status cannot be trusted to say it did.
    (checkout / "cancels_needed").write_text("2\n")

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json")

    assert rc == 1
    assert out["state"] == "STATUS_FAILED"
    assert "(deterministic)" in out["error"] and "LEDGER_INCONSISTENT" in out["error"]
    assert out["resources_released"] is True
    calls = (checkout / "calls").read_text()
    assert calls.count("--cancel ") == 1 and calls.count("--collect ") == 1


def test_repeated_silent_status_failures_cancel_and_collect(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """Five reason-less failures since the last good check: the cluster answers but cannot say; cancel and collect."""
    (checkout / "statuses").write_text(f"{RUNNING}\nSILENT\n")
    (checkout / "cancels_needed").write_text("2\n")

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json")

    assert rc == 1
    assert out["state"] == "STATUS_FAILED"
    assert _events(out, "stop") == [{"why": "unknown_limit"}]
    assert [p["kind"] for p in _events(out, "poll")].count("unknown") == 5
    assert out["resources_released"] is True
    assert (checkout / "calls").read_text().count("--cancel ") == 1


def test_unreachable_after_the_grace_skips_cancel_and_collect(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """Out of reach past the grace: fail UNREACHABLE, leave the sweep recorded for teardown, try nothing."""
    (checkout / "statuses").write_text(f"{RUNNING}\nFAIL\n")
    state_file = tmp_path / "state.json"

    rc, out = _run_storage_test(perf, checkout, state_file, "--wait-timeout", "0")

    assert rc == 1
    assert out["state"] == "UNREACHABLE" and out["resources_released"] is False
    assert out["results_dir"] in out["error"] and "--phase teardown" in out["error"]
    calls = (checkout / "calls").read_text()
    assert "--cancel" not in calls and "--collect" not in calls
    assert json.loads(state_file.read_text())["collected"] is False
    assert _events(out, "cancel") == [] and _events(out, "collect") == []
    assert _events(out, "stop") == [{"why": "watchdog"}] and perf.clock.now - 1000.0 == 1800
    assert _events(out, "poll")[-1]["kind"] == "access"


def _outage_then(checkout: Path, probe: str) -> None:
    """Statuses: one good check, then out of reach until the default grace ends, then *probe*."""
    polls_until_grace = 1800 // 60
    (checkout / "statuses").write_text(f"{RUNNING}\n" + "FAIL\n" * polls_until_grace + f"{probe}\n")


@pytest.mark.parametrize(
    ("probe", "kind"),
    [
        ("SILENT", "unknown"),
        ("FAIL:POD_UNSCHEDULABLE", "transient"),
        ("FAIL:LEDGER_INCONSISTENT", "deterministic"),
        ("BUSY", "busy"),
        (RUNNING, "ok"),
    ],
)
def test_a_silent_probe_after_the_grace_still_tries_cancel_and_collect(
    probe: str, kind: str, perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """A probe that finds the cluster answering, with any failure or a sweep still running, ends in
    STATUS_FAILED after an outage: one cancel and one collect, each tried once.
    """
    _outage_then(checkout, probe)
    (checkout / "cancels_needed").write_text("2\n")

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", "--wait-timeout", "0")

    assert rc == 1
    assert out["state"] == "STATUS_FAILED"
    assert _events(out, "stop") == [{"why": "watchdog"}]
    assert _events(out, "poll")[-1]["kind"] == kind
    assert out["resources_released"] is True
    calls = (checkout / "calls").read_text()
    assert calls.count("--cancel ") == 1 and calls.count("--collect ") == 1


def test_a_good_probe_after_the_deadline_times_out(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """The deadline passed during a failed check, and the probe finds the sweep still running: TIMEOUT,
    with one cancel and one collect.
    """
    (checkout / "statuses").write_text(f"{RUNNING}\nFAIL\n{RUNNING}\n")
    (checkout / "cancels_needed").write_text("2\n")

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", "--wait-timeout", "60")

    assert rc == 1
    assert out["state"] == "TIMEOUT"
    assert out["error"] == "sweep did not finish within --wait-timeout 60s; cancelling it"
    assert _events(out, "estimate")[0]["deadline_s"] == "60"
    assert _events(out, "stop") == [{"why": "deadline"}]
    assert [p["kind"] for p in _events(out, "poll")] == ["ok", "access", "ok"]
    calls = (checkout / "calls").read_text()
    assert calls.count("--cancel ") == 1 and calls.count("--collect ") == 1


def test_a_probe_that_finds_the_sweep_finished_collects_normally(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """The sweep finished during the outage: collect it as if nothing happened."""
    _outage_then(checkout, FINISHED)

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", "--wait-timeout", "0")

    assert rc == 0, out
    assert out["state"] == "SUCCESS" and out["resources_released"] is True
    assert "--cancel" not in (checkout / "calls").read_text()


def test_a_silent_status_failure_still_tries_to_release(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """Teardown gives up only when the cluster is out of reach; otherwise cancel and collect see for themselves."""
    (checkout / "statuses").write_text(f"SILENT\n{RUNNING}\n")
    state_file = _left_behind(checkout, tmp_path)

    rc, out = perf("release_storage_test", "--state-file", str(state_file))

    assert rc == 0, out
    assert out["released"] is True
    calls = (checkout / "calls").read_text()
    assert calls.count("--cancel ") == 1 and "--collect " in calls

    (checkout / "statuses").write_text("FAIL\n")
    for name in ("collected", "cancelled", "status_index", "calls"):
        (checkout / name).unlink(missing_ok=True)
    state_file.write_text(json.dumps({**json.loads(state_file.read_text()), "collected": False}))
    rc, out = perf("release_storage_test", "--state-file", str(state_file))
    assert rc == 1 and out["error"] == "cannot read the sweep's state (API_UNAVAILABLE)"
    assert "--cancel" not in (checkout / "calls").read_text()


def test_release_leaves_a_sweep_whose_status_timed_out(
    perf: InProcess, checkout: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A status check that timed out means the cluster is out of reach: no cancel and no collect."""
    sst = _load("release_storage_test").sst
    operations: list[str] = []

    def timed_out(cmd: list[str], timeout: float, env: dict[str, str] | None = None) -> Any:
        operations.append(cmd[1])
        return sst.Ran(124, "", f"timed out after {timeout:.0f}s", True)

    monkeypatch.setattr(sst, "run", timed_out)
    rc, out = perf("release_storage_test", "--state-file", str(_left_behind(checkout, tmp_path)))

    assert rc == 1 and out["released"] is False
    assert out["error"] == "cannot read the sweep's state (status timed out)"
    assert operations == ["--status"]


@pytest.mark.parametrize(
    ("nodes", "io_sizes", "threads", "duration", "expected"),
    [
        # 1 and 2 nodes x 2 IO sizes x 1 and 2 threads, 20 s phases.
        ([1, 2], ["r4K", "1M"], ["1", "2"], 20, (88_000, 70)),
        # 1 and 2 nodes x 4, 8 and 16 threads, 20 s phases.
        ([1, 2], ["1M"], ["4", "8", "16"], 20, (160_000, 54)),
    ],
)
def test_size_estimate_counts_threads_only_for_single_node_cells(
    nodes: list[int], io_sizes: list[str], threads: list[str], duration: int, expected: tuple[int, int]
) -> None:
    """A one-node cell writes a row per thread; a multi-node cell a row per node."""
    sst = _load("sst")
    assert sst.estimate_collection(nodes, io_sizes, threads, ["1"], duration, 1000, True) == expected


def test_size_estimate_is_zero_without_extended_live_csv() -> None:
    """No extended live CSVs, no per-interval rows; the file count stays."""
    sst = _load("sst")
    assert sst.estimate_collection([1, 2], ["r4K", "1M"], ["1", "2"], ["1"], 3600, 1000, False) == (0, 70)


def test_run_warns_about_a_large_collection(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """Above 1 GiB the sweep still runs, and the result says why it is large and what shrinks it."""
    (checkout / "statuses").write_text(f"{FINISHED}\n")
    sweep = ["--io-sizes", "1M", "--duration", "3300", "--live-interval-ms", "1"]

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", *sweep)

    assert rc == 0, out
    assert out["warning"].startswith("the sweep's live CSVs would be about 1.23 GiB")
    assert "--live-interval-ms" in out["warning"]
    # isvctl shows a step's stderr only when it fails, so the run log carries the warning too.
    assert _events(out, "estimate")[0]["warning"] == "_".join(out["warning"].split())
    _assert_schema("run_storage_test", out)


def test_run_refuses_a_collection_over_the_cap(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """Above 1.8 GiB the sweep would fail at collect, after running for hours; refuse it up front."""
    sweep = ["--wait-timeout", "0", "--live-interval-ms", "1", "--duration", "36000"]

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", *sweep)

    assert rc == 1
    assert out["state"] == "NOT_SUBMITTED"
    assert out["error"].startswith("the sweep's live CSVs would be about 26.82 GiB, above the 1.8 GiB limit")
    assert "--live-csv-extended 0" in out["error"]
    assert not (checkout / "calls").exists()


def test_run_refuses_too_many_cells(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """More than 5,624 cells exceeds the files storage-scale-test collects."""
    sweep = ["--nodes", "1-5625", "--io-sizes", "1M", "--wait-timeout", "0"]

    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", *sweep)

    assert rc == 1
    assert out["error"].startswith("the sweep has 5625 cells")
    assert not (checkout / "calls").exists()


def test_live_csv_settings_reach_the_workload_file(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """perf_live_interval_ms and perf_live_csv_extended reach storage-scale-test's workload file."""
    sets = [
        "tests.settings.perf_live_interval_ms=500",
        "tests.settings.perf_live_csv_extended=0",
        "tests.settings.perf_node_selector=pool=perf",
        f"tests.settings.perf_results_dir={tmp_path / 'results'}",
        f"tests.settings.perf_state_file={tmp_path / 'inputs' / 'state.json'}",
    ]
    outputs = {**SETUP_OUTPUTS, "setup_cluster": {}, "find_storage_scale_test": {"scale_test_base": str(checkout)}}
    args = _render("run_storage_test", sets, outputs)

    rc, out = perf("run_storage_test", *args)

    assert rc == 0, out
    workload = (tmp_path / "inputs" / "workload.env").read_text()
    assert "ELBENCHO_LIVEINT=500\n" in workload and "ELBENCHO_LIVE_CSV_EXTENDED=0\n" in workload


@pytest.mark.parametrize(
    ("flag", "value", "message"),
    [
        ("--wait-cap", "899", "it must be at least 900"),
        ("--stall-timeout", "0", "it must be auto or a number of seconds, at least 1"),
        ("--unreachable-grace", "419", "it must be at least 420 (2 x --interval + --status-timeout)"),
        ("--live-interval-ms", "0", "it must be at least 1"),
        ("--live-csv-extended", "2", "it must be 0 or 1"),
        ("--wait-timeout", "soon", "it must be auto or a number of seconds, at least 0"),
        ("--wait-timeout", "\u00b2", "it must be auto or a number of seconds, at least 0"),
        ("--stall-timeout", "\u0661\u0662\u0660", "it must be auto or a number of seconds, at least 1"),
    ],
)
def test_invalid_wait_settings_fail_before_submit(
    flag: str, value: str, message: str, perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """A wait setting that cannot work is refused, naming the argument and what it accepts."""
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", flag, value)

    assert rc == 1
    assert out["state"] == "NOT_SUBMITTED"
    assert out["error"] == f"{flag} is {value}; {message}"
    assert not (checkout / "calls").exists()


@pytest.mark.parametrize(("flag", "value"), [("--threads", "1,2,"), ("--iodepth", "1,x"), ("--io-sizes", "1M,,r4K")])
def test_a_malformed_list_fails_before_submit(
    flag: str, value: str, perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """A list with an empty or non-numeric entry is refused, naming the setting, before anything runs."""
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", flag, value)

    assert rc == 1
    assert out["state"] == "NOT_SUBMITTED"
    assert out["error"].startswith(f"{flag} is {value!r}; it must be a comma-separated list of ")
    assert not (checkout / "calls").exists()
    assert [line.split("  ")[2] for line in _run_log(out)] == ["refused"]


@pytest.mark.parametrize(
    ("statuses", "extra", "state"),
    [
        (f"{RUNNING}\n", ["--stall-timeout", "120"], "STALLED"),
        (f"{RUNNING}\nFAIL\n", [], "UNREACHABLE"),
        (f"{RUNNING}\nSILENT\n", [], "STATUS_FAILED"),
    ],
)
def test_new_run_states_match_the_schema(
    statuses: str, extra: list[str], state: str, perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """The run's own failure states are valid run_storage_test outputs."""
    (checkout / "statuses").write_text(statuses)

    _, out = _run_storage_test(perf, checkout, tmp_path / "state.json", *extra)

    assert out["state"] == state
    _assert_schema("run_storage_test", out)


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


# deprovision_perf_storage's output once it removed the claim: delete_perf_filesystem waits for it.
CLAIM_DELETED = {"deprovision_perf_storage": {"success": True}}


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


def test_real_run_evaluate_and_release_outputs_match_their_schemas(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """Success, timeout and refusal outputs from the real scripts all validate."""
    _, ok = _run_storage_test(perf, checkout, tmp_path / "ok" / "state.json")
    _assert_schema("run_storage_test", ok)

    (checkout / "collected").unlink()
    (checkout / "status_index").unlink()
    (checkout / "statuses").write_text(f"{RUNNING}\n")
    _, timed_out = _run_storage_test(perf, checkout, tmp_path / "timeout" / "state.json", "--wait-timeout", "60")
    assert timed_out["state"] == "TIMEOUT"
    _assert_schema("run_storage_test", timed_out)

    (checkout / "env.sh").write_text("export EXECUTION_SUBSTRATE=slurm\n")
    _, refused = _run_storage_test(perf, checkout, tmp_path / "refused" / "state.json")
    assert refused["state"] == "NOT_SUBMITTED"
    _assert_schema("run_storage_test", refused)

    evaluate = _load("evaluate_storage_test")
    _assert_schema("evaluate_storage_test", evaluate.evaluate(RUN_OK, _document(), 0.0, 0.0))
    _assert_schema("evaluate_storage_test", evaluate.evaluate(RUN_OK, None, 0.0, 0.0, "no results_dir"))
    _assert_schema("evaluate_storage_test", _script_output("evaluate_storage_test", False, "--run=not json"))

    _, release = _run_script("release_storage_test", "--state-file", str(tmp_path / "missing.json"))
    _assert_schema("release_storage_test", release)
    _, invalid = perf("release_storage_test", "--state-file", str(tmp_path / "missing.json"), "--status-timeout=1")
    assert invalid["released"] is False
    _assert_schema("release_storage_test", invalid)


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
    """After a confirmed release, STORAGE_SKIP_TEARDOWN=true renders --skip-destroy for both fixture
    teardown steps, and nothing otherwise.
    """
    if skip:
        monkeypatch.setenv("STORAGE_SKIP_TEARDOWN", "true")
    else:
        monkeypatch.delenv("STORAGE_SKIP_TEARDOWN", raising=False)
    outputs = {**SETUP_OUTPUTS, "release_storage_test": {"released": True}, **CLAIM_DELETED}
    for step in ("deprovision_perf_storage", "delete_perf_filesystem"):
        assert ("--skip-destroy" in _render(step, outputs=outputs)) is skip


# ─── Run isolation: state lock, refusal and self-heal, abandon, cluster identity, Ctrl-C marker ─────

A_CLUSTER = {"context": "ctx-a", "server": "https://a.example:443"}
FIXTURE_TEARDOWN = ("deprovision_perf_storage", "delete_perf_filesystem")


def _hold(path: Path) -> int:
    """Lock *path* as another run would; closing the returned descriptor releases it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return fd


def _record(state_file: Path, **fields: Any) -> bytes:
    """Add *fields* to the record in *state_file*; return its new bytes."""
    state_file.write_text(json.dumps({**json.loads(state_file.read_text()), **fields}))
    return state_file.read_bytes()


def test_a_second_run_fails_fast_while_the_first_holds_the_lock(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """While another run or teardown holds the state file's lock, a run waits for nothing, submits
    nothing and writes nothing, and says who recorded the sweep.
    """
    state_file = tmp_path / "state.json"
    owner = {"pid": 4242, "hostname": "host-a", "started_utc": "2026-01-01T00:00:00Z"}
    state_file.write_text(json.dumps({"collected": True, "owner": owner}))
    before = state_file.read_bytes()
    fd = _hold(tmp_path / ".lock")
    try:
        rc, out = _run_storage_test(perf, checkout, state_file)
    finally:
        os.close(fd)

    assert rc == 1
    assert out["state"] == "REFUSED" and out["resources_released"] is None
    assert out["error"] == (
        f"another run or teardown is using {state_file} "
        "(its sweep was recorded by pid 4242 on host-a since 2026-01-01T00:00:00Z)"
    )
    assert perf.clock.now == 1000.0
    assert not (checkout / "calls").exists() and not (checkout / "env.sh").exists()
    assert state_file.read_bytes() == before
    assert _run_log(out)[-1].split("  ")[2] == "refused"
    _assert_schema("run_storage_test", out)


def test_release_never_cancels_while_another_run_holds_the_lock(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """The sweep may be the lock holder's, still running: release touches nothing, not even with
    --abandon, and reports it as not released.
    """
    state_file = _left_behind(checkout, tmp_path)
    before = state_file.read_bytes()
    fd = _hold(tmp_path / ".lock")
    try:
        rc, out = perf("release_storage_test", "--state-file", str(state_file), "--abandon")
    finally:
        os.close(fd)

    assert rc == 1
    assert out["released"] is False and "abandoned" not in out
    assert out["error"] == "another run or teardown is using this state file; not touching its sweep"
    assert not (checkout / "calls").exists()
    assert state_file.read_bytes() == before and not list(tmp_path.glob("*.abandoned-*"))
    _assert_schema("release_storage_test", out)


@pytest.mark.parametrize("step", ["run_storage_test", "release_storage_test"])
def test_the_state_lock_is_held_from_reading_the_record_to_the_end(
    step: str, perf: InProcess, checkout: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each step holds the lock from before it reads the state file until after its last call, and
    frees it when it returns.
    """
    sst = _load(step).sst
    lock = tmp_path / ".lock"
    real_run, real_read = sst.run, sst.read_state
    seen: list[tuple[str, bool]] = []

    def run(cmd: list[str], timeout: float, env: dict[str, str] | None = None) -> Any:
        seen.append((cmd[1], _lock_is_free(lock)))
        return real_run(cmd, timeout, env)

    def read_state(path: str) -> Any:
        seen.append(("read_state", _lock_is_free(lock)))
        return real_read(path)

    monkeypatch.setattr(sst, "run", run)
    monkeypatch.setattr(sst, "read_state", read_state)
    if step == "run_storage_test":
        rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json")
    else:
        rc, out = perf("release_storage_test", "--state-file", str(_left_behind(checkout, tmp_path)))

    assert rc == 0, out
    assert seen[0][0] == "read_state" and seen[-2][0] == "--collect"
    assert [op for op, free in seen if free] == []
    assert _lock_is_free(lock)


def test_env_sh_writes_are_serialised_across_working_directories(
    perf: InProcess, checkout: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Runs from two working directories share the checkout's env.sh: a run waits for another's
    submit, holds the checkout's lock while it writes env.sh and submits, and gives up, before
    writing anything, after the time one submit may take.
    """
    sst = _load("run_storage_test").sst
    lock = checkout / ".isvctl.lock"
    other = [_hold(lock)]
    seen: list[tuple[str, bool, bool]] = []
    real_install, real_run = sst.install_env_sh, sst.run

    def install_env_sh(base: Path, settings: list[str]) -> Any:
        seen.append(("env.sh", not other, _lock_is_free(lock)))
        return real_install(base, settings)

    def run(cmd: list[str], timeout: float, env: dict[str, str] | None = None) -> Any:
        if cmd[1:2] == ["--target"]:
            seen.append(("submit", not other, _lock_is_free(lock)))
        return real_run(cmd, timeout, env)

    class OtherSubmitFinishes(FakeClock):
        def sleep(self, seconds: float) -> None:
            while other:
                os.close(other.pop())
            super().sleep(seconds)

    monkeypatch.setattr(sst, "install_env_sh", install_env_sh)
    monkeypatch.setattr(sst, "run", run)
    perf.clock = OtherSubmitFinishes()
    rc, out = _run_storage_test(perf, checkout, tmp_path / "a" / "state.json")

    assert rc == 0, out
    assert seen == [("env.sh", True, False), ("submit", True, False)]
    assert _lock_is_free(lock)

    seen.clear()
    (checkout / "calls").unlink()
    perf.clock = FakeClock()
    fd = _hold(lock)
    try:
        rc, out = _run_storage_test(perf, checkout, tmp_path / "b" / "state.json")
    finally:
        os.close(fd)

    assert rc == 1
    assert out["state"] == "REFUSED"
    assert out["error"] == f"another run is still submitting from {checkout.resolve()}; try again once it has"
    assert perf.clock.now - 1000.0 >= 600 + sst.TERM_GRACE_S + sst.DRAIN_S
    assert seen == [] and not (checkout / "calls").exists()
    assert not (tmp_path / "b" / "state.json").exists()


def _unrecorded(checkout: Path, tmp_path: Path) -> Path:
    """The state file of a run whose submit published an attempt but was killed before printing it."""
    results_dir = checkout / "results" / "elbencho-20260101Z000000"
    (results_dir / "kubernetes").mkdir(parents=True)
    (results_dir / "kubernetes" / "current-attempt").write_text("0123abcd\n")
    state_file = tmp_path / "state.json"
    pending = {"scale_test_base": str(checkout), "results_root": str(checkout / "results"), "existing": []}
    state_file.write_text(json.dumps({**pending, "collected": False}))
    return state_file


@pytest.mark.parametrize("left", [_left_behind, _unrecorded], ids=["recorded", "submit-killed"])
def test_run_refuses_an_unreleased_earlier_sweep(left: Any, perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """A record of a sweep that may still hold the claim is never overwritten: the run refuses
    before it writes or submits anything, and says its teardown will release that sweep.
    """
    state_file = left(checkout, tmp_path)
    before = state_file.read_bytes()

    rc, out = _run_storage_test(perf, checkout, state_file)

    assert rc == 1
    assert out["state"] == "REFUSED" and out["resources_released"] is None
    assert out["error"] == (
        f"an earlier sweep (results dir {checkout / 'results' / 'elbencho-20260101Z000000'}) was not released. "
        "This run's teardown step will try to release it"
    )
    assert state_file.read_bytes() == before
    assert not (checkout / "calls").exists() and not (checkout / "env.sh").exists()
    assert _run_log(out)[-1].split("  ")[2] == "refused"
    _assert_schema("run_storage_test", out)


def _nothing_new(checkout: Path, tmp_path: Path) -> Path:
    """A pending record whose submit created no results directory at all."""
    (checkout / "results" / "elbencho-20250101Z000000").mkdir(parents=True)
    state_file = tmp_path / "state.json"
    pending = {"results_root": str(checkout / "results"), "existing": ["elbencho-20250101Z000000"]}
    state_file.write_text(json.dumps({**pending, "collected": False}))
    return state_file


def _no_attempt(checkout: Path, tmp_path: Path) -> Path:
    """A record whose submit stopped before it published an attempt."""
    (checkout / "results" / "elbencho-20250101Z000000").mkdir(parents=True)
    state_file = tmp_path / "state.json"
    record = {"results_dir": str(checkout / "results" / "elbencho-20250101Z000000"), "collected": False}
    state_file.write_text(json.dumps(record))
    return state_file


@pytest.mark.parametrize("left", [_nothing_new, _no_attempt], ids=["nothing-new", "no-attempt"])
def test_run_does_not_refuse_a_pending_record_whose_submit_created_nothing(
    left: Any, perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """Such a record holds no cluster resource, so refusing it would block every later run."""
    state_file = left(checkout, tmp_path)

    rc, out = _run_storage_test(perf, checkout, state_file)

    assert rc == 0, out
    record = json.loads(state_file.read_text())
    assert record["results_dir"] == out["results_dir"] and record["collected"] is True


def test_a_refused_runs_teardown_releases_the_earlier_sweep(
    perf: InProcess, checkout: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Self-heal: a run refused over an interrupted run's sweep releases that sweep in its own
    teardown, its fixture teardown then runs normally, and the next run is not refused.
    """
    monkeypatch.delenv("STORAGE_SKIP_TEARDOWN", raising=False)
    (checkout / "statuses").write_text(f"{RUNNING}\n")
    state_file = _left_behind(checkout, tmp_path)
    earlier = {"pid": 1, "hostname": "another-host", "started_utc": "2026-01-01T00:00:00Z"}
    _record(state_file, owner=earlier, cluster=A_CLUSTER)
    results_dir = json.loads(state_file.read_text())["results_dir"]
    _load("sst").write_marker(str(state_file), results_dir)

    _, refused = _run_storage_test(perf, checkout, state_file)
    rc, release = perf("release_storage_test", "--state-file", str(state_file))

    assert refused["state"] == "REFUSED"
    assert rc == 0, release
    assert release["released"] is True and release["results_dir"] == results_dir
    calls = (checkout / "calls").read_text()
    assert f"--cancel {results_dir}" in calls and f"--collect {results_dir}" in calls
    assert json.loads(state_file.read_text())["collected"] is True
    assert not (tmp_path / "interrupted").exists()
    for step in FIXTURE_TEARDOWN:
        outputs = {**SETUP_OUTPUTS, "release_storage_test": release, **CLAIM_DELETED}
        assert "--skip-destroy" not in _render(step, outputs=outputs)
    rc, find = perf("find_storage_scale_test", f"--base={checkout}", "--state-file", str(state_file))
    assert rc == 0, find
    _, again = _run_storage_test(perf, checkout, state_file)
    assert again["state"] != "REFUSED" and "results_dir" in again


def test_setup_fails_early_on_an_unreleased_sweep(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """The first setup step refuses, read-only, before any fixture is created; the overlay passes it
    the state file, and it passes once the sweep is released.
    """
    state_file = _left_behind(checkout, tmp_path)
    before = state_file.read_bytes()

    rc, out = perf("find_storage_scale_test", f"--base={checkout}", "--state-file", str(state_file))

    assert rc == 1
    assert out["error"] == (
        f"an earlier sweep (results dir {checkout / 'results' / 'elbencho-20260101Z000000'}) was not released. "
        "This run's teardown step will try to release it, unless another run is still using it"
    )
    assert "scale_test_base" not in out
    _assert_schema("find_storage_scale_test", out)
    assert state_file.read_bytes() == before and not (tmp_path / ".lock").exists()
    assert not (checkout / "calls").exists()

    _record(state_file, collected=True)
    rc, out = perf("find_storage_scale_test", f"--base={checkout}", "--state-file", str(state_file))
    assert rc == 0, out
    assert _flag(_render("find_storage_scale_test"), "--state-file").endswith("_output/storage-perf/inputs/state.json")


def test_abandon_archives_the_record_and_marker_and_still_fails_the_release(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """A sweep that cannot be released no longer blocks the next run, but the step fails and says
    how to release the sweep by hand; nothing is deleted.
    """
    (checkout / "statuses").write_text("FAIL\n")
    state_file = _left_behind(checkout, tmp_path)
    record = state_file.read_bytes()
    results_dir = json.loads(record)["results_dir"]
    _load("sst").write_marker(str(state_file), results_dir)
    marker = (tmp_path / "interrupted").read_bytes()

    rc, out = perf("release_storage_test", "--state-file", str(state_file), "--abandon")

    assert rc == 1
    assert out["released"] is False and out["abandoned"] is True
    (archived,) = tmp_path.glob("state.abandoned-*.json")
    (archived_marker,) = tmp_path.glob("interrupted.abandoned-*")
    assert archived.read_bytes() == record and archived_marker.read_bytes() == marker
    assert not state_file.exists() and not (tmp_path / "interrupted").exists()
    assert out["error"].startswith("cannot read the sweep's state (API_UNAVAILABLE) (the run was interrupted at ")
    assert f"the record is now {archived}" in out["error"]
    launcher = checkout / "storage-tests" / "fs" / "nv-elbencho-sweep.sh"
    # A sweep that was never cancelled is usually still running, and --collect refuses a running sweep.
    cancel, collect = f"`{launcher} --cancel {results_dir}`", f"`{launcher} --collect {results_dir}`"
    assert cancel in out["error"] and collect in out["error"]
    assert out["error"].index(cancel) < out["error"].index(collect)
    _assert_schema("release_storage_test", out)
    rc, find = perf("find_storage_scale_test", f"--base={checkout}", "--state-file", str(state_file))
    assert rc == 0, find


def test_abandon_releases_normally_when_it_can(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """--abandon only ever acts on a release that failed."""
    state_file = _left_behind(checkout, tmp_path)

    rc, out = perf("release_storage_test", "--state-file", str(state_file), "--abandon")

    assert rc == 0, out
    assert out["released"] is True and "abandoned" not in out
    assert json.loads(state_file.read_text())["collected"] is True
    assert not list(tmp_path.glob("*.abandoned-*"))


def test_without_abandon_an_unreleasable_record_stays(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """By default a failed release keeps the record, so the next teardown can try again."""
    (checkout / "statuses").write_text("FAIL\n")
    state_file = _left_behind(checkout, tmp_path)
    before = state_file.read_bytes()

    rc, out = perf("release_storage_test", "--state-file", str(state_file))

    assert rc == 1 and out["released"] is False and "abandoned" not in out
    assert state_file.read_bytes() == before and not list(tmp_path.glob("*.abandoned-*"))


def test_the_abandon_setting_reaches_the_release_step() -> None:
    """perf_abandon_unreleased is off by default; "true" passes --abandon."""
    assert "--abandon" not in _render("release_storage_test")
    for value in ("true", '"true"', "True"):
        assert "--abandon" in _render("release_storage_test", [f"tests.settings.perf_abandon_unreleased={value}"])
    assert "--abandon" not in _render("release_storage_test", ["tests.settings.perf_abandon_unreleased=false"])


def test_run_records_the_cluster_identity(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """The record names the kubeconfig's context and server, and who wrote it; never a credential.
    A kubeconfig kubectl cannot read leaves the cluster out instead of failing the run.
    """
    kubeconfig = _kubeconfig(tmp_path / "kubeconfig", "ctx-b", "https://b.example:6443")
    state_file = tmp_path / "state.json"

    rc, out = _run_storage_test(perf, checkout, state_file, f"--kubeconfig={kubeconfig}")

    assert rc == 0, out
    record = json.loads(state_file.read_text())
    assert record["cluster"] == {"context": "ctx-b", "server": "https://b.example:6443"}
    assert record["owner"]["pid"] == os.getpid() and record["owner"]["started_utc"].endswith("Z")
    assert "not-a-real-token" not in state_file.read_text()

    rc, out = _run_storage_test(perf, checkout, state_file, f"--kubeconfig={tmp_path / 'missing'}")
    assert rc == 0, out
    assert json.loads(state_file.read_text())["cluster"] is None


@pytest.mark.parametrize(
    ("context", "server"),
    [("ctx-b", "https://a.example:443"), ("ctx-a", "https://b.example:443"), (None, None)],
    ids=["other-context", "other-server", "unreadable"],
)
def test_release_refuses_a_kubeconfig_for_another_context_or_server(
    context: str | None, server: str | None, perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """A repinned kubeconfig that points elsewhere, even at the same server through another context,
    is never used on the sweep; with the original back, release goes ahead.
    """
    kubeconfig = _kubeconfig(tmp_path / "kubeconfig")
    state_file = _left_behind(checkout, tmp_path)
    before = _record(state_file, kubeconfig=str(kubeconfig), cluster=A_CLUSTER)
    if context is None:
        kubeconfig.write_text("not a kubeconfig")
    else:
        _kubeconfig(kubeconfig, context, server or "")

    rc, out = perf("release_storage_test", "--state-file", str(state_file))

    assert rc == 1 and out["released"] is False
    assert "the sweep runs on context ctx-a at https://a.example:443" in out["error"]
    assert out["error"].endswith("Not touching it" if context is None else "for the original cluster")
    if context is not None:
        assert f"now points at context {context} at {server}" in out["error"]
    assert not (checkout / "calls").exists() and state_file.read_bytes() == before

    _kubeconfig(kubeconfig)
    rc, out = perf("release_storage_test", "--state-file", str(state_file))
    assert rc == 0 and out["released"] is True


def test_release_stops_on_an_identity_mismatch_storage_scale_test_reports(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """storage-scale-test's own check (the claim's identity) refuses cancel and collect too; release
    says what it means and does not try them.
    """
    (checkout / "statuses").write_text("FAIL:IDENTITY_MISMATCH:PersistentVolumeClaim\n")
    state_file = _left_behind(checkout, tmp_path)

    rc, out = perf("release_storage_test", "--state-file", str(state_file))

    assert rc == 1 and out["released"] is False
    assert "the kubeconfig may point at another cluster, or the claim was recreated" in out["error"]
    calls = (checkout / "calls").read_text()
    assert "--cancel" not in calls and "--collect" not in calls


def _waiting_checkout(tmp_path: Path) -> Path:
    """A checkout whose launcher submits, then hangs in its first --status, recording its pid."""
    base = tmp_path / "waiting"
    launcher = base / "storage-tests" / "fs" / "nv-elbencho-sweep.sh"
    launcher.parent.mkdir(parents=True)
    launcher.write_text(
        "#!/usr/bin/env bash\n"
        'base="$(cd "$(dirname "$0")/../.." && pwd)"\n'
        'if [[ "$1" == --status ]]; then echo $$ > "$base/pid"; exec sleep 60; fi\n'
        'dir="$base/results/elbencho-20260101Z000000"\n'
        'mkdir -p "$dir/kubernetes" && echo 0123abcd > "$dir/kubernetes/current-attempt"\n'
        'echo "STORAGE_SCALE_TEST_RESULTS_DIR=$dir"\n'
        "echo STORAGE_SCALE_TEST_ATTEMPT_ID=0123abcd\n"
    )
    launcher.chmod(0o755)
    (base / "env.sh.template").write_text(f"TEST_DIRS=()\n{OVERRIDES_ANCHOR}\n")
    return base


def test_an_interrupt_writes_a_marker_without_touching_state_json(tmp_path: Path) -> None:
    """Ctrl-C while waiting records the time and the results directory in a file of its own; the
    state file keeps the record teardown needs, byte for byte.
    """
    base = _waiting_checkout(tmp_path)
    state_file = tmp_path / "inputs" / "state.json"
    env = {k: v for k, v in os.environ.items() if k != "ISVCTL_DEMO_MODE"}
    args = [f"--scale-test-base={base}", "--claim=a/b", "--node-selector=a=b"]
    args += [f"--results-dir={base / 'results'}", f"--state-file={state_file}"]
    proc = subprocess.Popen(
        [sys.executable, str(PERF_DIR / "run_storage_test.py"), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    deadline = time.monotonic() + 20
    while not (base / "pid").is_file() and time.monotonic() < deadline:
        time.sleep(0.1)
    before = state_file.read_bytes()

    proc.send_signal(signal.SIGINT)
    started = datetime.now(UTC).replace(microsecond=0)

    assert proc.wait(timeout=10) == 130
    assert state_file.read_bytes() == before
    marker = json.loads((tmp_path / "inputs" / "interrupted").read_text())
    assert marker["results_dir"] == json.loads(before)["results_dir"]
    stamp = datetime.strptime(marker["interrupted_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    assert abs((stamp - started).total_seconds()) <= 5


def test_the_refusal_names_the_interruption_time(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """The run, setup and a failed release all name when the earlier run was interrupted; a release
    that succeeds removes the marker.
    """
    (checkout / "statuses").write_text("FAIL\n")
    state_file = _left_behind(checkout, tmp_path)
    marker = {
        "interrupted_utc": "2026-01-01T00:00:00Z",
        "results_dir": json.loads(state_file.read_text())["results_dir"],
    }
    (tmp_path / "interrupted").write_text(json.dumps(marker))
    when = "interrupted at 2026-01-01T00:00:00Z"

    _, run = _run_storage_test(perf, checkout, state_file)
    _, find = perf("find_storage_scale_test", f"--base={checkout}", "--state-file", str(state_file))
    _, failed = perf("release_storage_test", "--state-file", str(state_file))

    assert f"{marker['results_dir']}; {when})" in run["error"]
    assert f"{marker['results_dir']}; {when})" in find["error"]
    assert failed["error"].endswith(f"(the run was {when})")
    assert (tmp_path / "interrupted").exists()

    (checkout / "statuses").write_text(f"{FINISHED}\n")
    (checkout / "status_index").unlink()
    rc, released = perf("release_storage_test", "--state-file", str(state_file))
    assert rc == 0 and released["released"] is True
    assert not (tmp_path / "interrupted").exists()


UNCONFIRMED_RELEASES = [
    pytest.param({"release_storage_test": {"released": False}}, id="released-false"),
    pytest.param({"release_storage_test": {"released": "true"}}, id="released-string"),
    pytest.param({"release_storage_test": {"success": False}}, id="key-missing"),
    pytest.param({}, id="step-absent"),
]


@pytest.mark.parametrize("release", UNCONFIRMED_RELEASES)
def test_fixture_teardown_keeps_the_claim_when_release_did_not_confirm(
    release: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Deleting the claim or filesystem under a sweep that may still run would leave the sweep
    uncollectable; both fixture teardown steps keep them unless release said `released: true`.
    Rendered as isvctl renders it: release's output arrives after the run's settings are rendered.
    """
    monkeypatch.delenv("STORAGE_SKIP_TEARDOWN", raising=False)
    for step in FIXTURE_TEARDOWN:
        args = _render(step, outputs={**SETUP_OUTPUTS, **release})
        assert args.count("--skip-destroy") == 1, (step, args)
        assert "--created" in args


@pytest.mark.parametrize("released", [True, False])
def test_fixture_teardown_honours_the_skip_teardown_setting_once(
    released: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """STORAGE_SKIP_TEARDOWN=true keeps the fixtures whatever release reported, with one flag."""
    monkeypatch.setenv("STORAGE_SKIP_TEARDOWN", "true")
    for step in FIXTURE_TEARDOWN:
        args = _render(step, outputs={**SETUP_OUTPUTS, "release_storage_test": {"released": released}})
        assert args.count("--skip-destroy") == 1, (step, args)


@pytest.mark.parametrize("skip", [None, "false"])
def test_fixture_teardown_deletes_normally_after_a_confirmed_release(
    skip: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After `released: true`, the fixtures this run created are deleted, as before."""
    if skip is None:
        monkeypatch.delenv("STORAGE_SKIP_TEARDOWN", raising=False)
    else:
        monkeypatch.setenv("STORAGE_SKIP_TEARDOWN", skip)
    for step in FIXTURE_TEARDOWN:
        args = _render(step, outputs={**SETUP_OUTPUTS, "release_storage_test": {"released": True}, **CLAIM_DELETED})
        assert "--skip-destroy" not in args and "--created" in args
        assert "" not in args


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
        f"  \"config view --minify --flatten\") printf '{FAKE_KUBECONFIG}' ;;\n"
        "esac\n"
    )
    kubectl.chmod(0o755)
    base = {
        k: v
        for k, v in os.environ.items()
        if k not in ("ISVCTL_DEMO_MODE", "K8S_EXPECTED_CONTEXT", "K8S_KUBECONFIG_OUT")
    }
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


# ─── Teardown safety: the cluster a fixture is deleted on, abandoned sweeps, lost results ─────

TEARDOWN_STEPS = ("release_storage_test", "deprovision_perf_storage", "delete_perf_filesystem", "teardown_cluster")


def test_teardown_steps_fall_back_to_the_pinned_kubeconfig() -> None:
    """Without setup outputs (`--phase teardown`, or a run refused before setup_cluster), every
    teardown step that takes a kubeconfig gets the pinned one, never the shell's current context.
    """
    for step in TEARDOWN_STEPS:
        args = _render(step, outputs={})
        if any(arg.startswith("--kubeconfig") for arg in args):
            assert _flag(args, "--kubeconfig").endswith("_output/storage-perf/inputs/kubeconfig"), (step, args)
    assert _flag(_render("deprovision_perf_storage"), "--kubeconfig") == "/tmp/kubeconfig"


def _claim_kubectl(tmp_path: Path) -> Path:
    """A kubectl that reads the kubeconfig like the shared fake, and answers `get pvc` with a usable claim."""
    bin_dir = tmp_path / "claim-bin"
    bin_dir.mkdir()
    (tmp_path / "pvc.json").write_text(json.dumps(USABLE_CLAIM))
    kubectl = bin_dir / "kubectl"
    kubectl.write_text(
        f'#!/usr/bin/env bash\necho "$@" >> {tmp_path / "calls"}\n'
        f'if [[ "$1 $2" == "config view" ]]; then exec cat "$KUBECONFIG"; fi\ncat {tmp_path / "pvc.json"}\n'
    )
    kubectl.chmod(0o755)
    return bin_dir


def test_provision_records_the_kubeconfig_and_cluster_it_used(tmp_path: Path) -> None:
    """The claim record names the kubeconfig and its context and server (never a credential), so
    teardown can tell whether it is about to delete on the same cluster.
    """
    kubeconfig = _kubeconfig(tmp_path / "kubeconfig", "ctx-b", "https://b.example:6443")
    record = tmp_path / "claim.json"

    out = _run_stub(
        "provision_perf_storage",
        f"--kubeconfig={kubeconfig}",
        "--existing-claim=perf-claim",
        f"--record={record}",
        demo=False,
        path=_claim_kubectl(tmp_path),
    )

    assert out["success"] is True, out
    recorded = json.loads(record.read_text())
    assert recorded["kubeconfig"] == str(kubeconfig)
    assert recorded["cluster"] == {"context": "ctx-b", "server": "https://b.example:6443"}
    assert "not-a-real-token" not in record.read_text()
    _assert_schema("provision_perf_storage", out)


@pytest.mark.parametrize(
    ("context", "server"),
    [("ctx-b", "https://a.example:443"), ("ctx-a", "https://b.example:443"), (None, None)],
    ids=["other-context", "other-server", "unreadable"],
)
def test_deprovision_refuses_to_delete_a_claim_on_another_cluster(
    context: str | None, server: str | None, tmp_path: Path
) -> None:
    """A kubeconfig repinned to another cluster never reaches the provider's delete: the step fails,
    names both clusters and keeps the record for a teardown with the original kubeconfig.
    """
    kubeconfig = _kubeconfig(tmp_path / "kubeconfig")
    record = tmp_path / "claim.json"
    claim = {"namespace": "ns", "claim_name": "c", "created": True, "kubeconfig": str(kubeconfig)}
    record.write_text(json.dumps({**claim, "cluster": A_CLUSTER}))
    if context is None:
        kubeconfig.write_text("not a kubeconfig")
    else:
        _kubeconfig(kubeconfig, context, server or "")

    out = _run_stub("deprovision_perf_storage", f"--kubeconfig={kubeconfig}", f"--record={record}", demo=False)

    assert out["success"] is False and out["deleted"] is False
    assert "the claim was created on context ctx-a at https://a.example:443" in out["error"]
    if context is not None:
        assert f"points at context {context} at {server}" in out["error"]
    assert "Not deleting it" in out["error"]
    assert json.loads(record.read_text())["cluster"] == A_CLUSTER
    _assert_schema("deprovision_perf_storage", out)

    _kubeconfig(kubeconfig)
    out = _run_stub("deprovision_perf_storage", f"--record={record}", demo=False)
    # Same cluster (from the recorded kubeconfig): it reaches the provider's delete.
    assert out["error"] == "Not implemented - delete the claim provision_perf_storage created"


def _abandoned(tmp_path: Path) -> Path:
    """An archived record, as --abandon leaves it beside the state file."""
    archived = tmp_path / "state.abandoned-20260101T000000Z.json"
    archived.write_text(json.dumps({"scale_test_base": "/x", "results_dir": "/x/r", "collected": False}))
    return archived


@pytest.mark.parametrize("state", [None, {"collected": True}], ids=["no-record", "collected"])
def test_release_reports_abandoned_records_beside_the_state_file(
    state: dict[str, Any] | None, perf: InProcess, tmp_path: Path
) -> None:
    """An abandoned sweep may still use the claim and filesystem, so a later teardown names its
    record, and says how to let the fixtures go, instead of reporting them free.
    """
    archived = _abandoned(tmp_path)
    state_file = tmp_path / "state.json"
    if state is not None:
        state_file.write_text(json.dumps({"scale_test_base": "/x", "results_dir": "/x/s", **state}))

    rc, out = perf("release_storage_test", "--state-file", str(state_file))

    assert rc == 0 and out["released"] is True
    assert out["abandoned_records"] == [str(archived)]
    assert f"once you have collected that sweep, delete {archived}" in out["message"]
    _assert_schema("release_storage_test", out)

    archived.unlink()
    _, out = perf("release_storage_test", "--state-file", str(state_file))
    assert "abandoned_records" not in out


def test_fixture_teardown_keeps_the_fixtures_while_an_abandoned_record_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A released sweep does not free the fixtures while an abandoned one may still use them."""
    monkeypatch.delenv("STORAGE_SKIP_TEARDOWN", raising=False)
    release = {"released": True, "abandoned_records": ["/x/state.abandoned-20260101T000000Z.json"]}
    outputs = {**SETUP_OUTPUTS, "release_storage_test": release, **CLAIM_DELETED}
    for step in FIXTURE_TEARDOWN:
        assert _render(step, outputs=outputs).count("--skip-destroy") == 1, step


def test_abandon_says_to_delete_the_archived_record_once_the_sweep_is_collected(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """The abandoned record keeps the fixtures; the message says when to delete it, and that only a
    run using an existing claim can start meanwhile.
    """
    (checkout / "statuses").write_text("FAIL\n")
    state_file = _left_behind(checkout, tmp_path)

    rc, out = perf("release_storage_test", "--state-file", str(state_file), "--abandon")

    assert rc == 1
    (archived,) = tmp_path.glob("state.abandoned-*.json")
    assert out["abandoned_records"] == [str(archived)]
    assert f"Once you have collected it, delete {archived}" in out["error"]
    assert "the next run can start" not in out["error"]
    assert "a run that uses an existing claim can start" in out["error"]


def _attempt_without_results(checkout: Path, tmp_path: Path) -> tuple[Path, str]:
    """A record whose submit printed an attempt, and whose local results directory was then deleted."""
    state_file = _left_behind(checkout, tmp_path)
    _record(state_file, attempt="0123abcd")
    results_dir = json.loads(state_file.read_text())["results_dir"]
    shutil.rmtree(results_dir)
    return state_file, results_dir


def test_a_recorded_attempt_still_needs_release_without_its_results_dir(
    perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """Submit printed an attempt, so the sweep exists in-cluster whatever is on disk: setup refuses,
    and release reports it unreleased, says how to recover, and never marks it collected.
    """
    state_file, results_dir = _attempt_without_results(checkout, tmp_path)
    before = state_file.read_bytes()

    rc, find = perf("find_storage_scale_test", f"--base={checkout}", "--state-file", str(state_file))
    assert rc == 1 and "was not released" in find["error"]

    rc, out = perf("release_storage_test", "--state-file", str(state_file))

    assert rc == 1 and out["released"] is False
    assert f"{results_dir} no longer holds attempt 0123abcd" in out["error"]
    assert "--collect" in out["error"] and "perf_abandon_unreleased" in out["error"]
    assert state_file.read_bytes() == before
    assert not (checkout / "calls").exists()
    _assert_schema("release_storage_test", out)


# ─── Tools that cannot run, state files that cannot be used: always a JSON result ─────


@pytest.mark.parametrize("problem", ["missing", "not-executable"])
def test_evaluate_names_an_extractor_it_cannot_run(problem: str, checkout: Path, tmp_path: Path) -> None:
    """A missing or non-executable extractor fails results_present with the path and the likely cause."""
    extractor = checkout / "utils" / "extract-elbencho.py"
    if problem == "not-executable":
        extractor.parent.mkdir(parents=True)
        extractor.write_text("#!/usr/bin/env python3\n")
        extractor.chmod(0o644)
    results = tmp_path / "results"
    results.mkdir()
    run = json.dumps({**RUN_OK, "results_dir": str(results)})

    rc, out = _run_script("evaluate_storage_test", f"--scale-test-base={checkout}", f"--run={run}")

    assert rc == 1 and out["success"] is False
    message = out["tests"]["results_present"]["message"]
    assert message.startswith(f"cannot run {extractor}: ")
    assert ("chmod +x" in message) is (problem == "not-executable")
    _assert_schema("evaluate_storage_test", out)


def test_release_names_a_launcher_it_cannot_run(perf: InProcess, checkout: Path, tmp_path: Path) -> None:
    """A launcher without its executable bit fails the release with its path, and keeps the record."""
    launcher = checkout / "storage-tests" / "fs" / "nv-elbencho-sweep.sh"
    launcher.chmod(0o644)
    state_file = _left_behind(checkout, tmp_path)
    before = state_file.read_bytes()

    rc, out = perf("release_storage_test", "--state-file", str(state_file))

    assert rc == 1 and out["released"] is False
    assert out["error"].startswith(f"cannot run {launcher}: it is not executable (chmod +x it)")
    assert state_file.read_bytes() == before
    _assert_schema("release_storage_test", out)


def test_find_names_a_tool_it_cannot_run(perf: InProcess, checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The version check runs bash; without it, find reports that instead of a traceback."""
    monkeypatch.setenv("PATH", str(checkout / "no-such-bin"))

    rc, out = perf("find_storage_scale_test", f"--base={checkout}")

    assert rc == 1 and out["error"].startswith("cannot run bash: ")
    _assert_schema("find_storage_scale_test", out)


@pytest.mark.parametrize("content", ["[1]", "5", '"x"', "true"])
def test_a_state_file_that_is_not_an_object_reads_as_no_record(
    content: str, perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """Valid JSON that is not a record is treated like unreadable JSON: there is no sweep to release."""
    state_file = tmp_path / "state.json"
    state_file.write_text(content)

    _, release = perf("release_storage_test", "--state-file", str(state_file))
    _, find = perf("find_storage_scale_test", f"--base={checkout}", "--state-file", str(state_file))

    assert release["released"] is True and release["message"] == "no sweep recorded; nothing to release"
    assert find["success"] is True


@pytest.mark.parametrize(
    ("fields", "problem"),
    [
        ({"scale_test_base": None}, "it names no scale_test_base"),
        ({"results_dir": 5}, "its results_dir is not a string"),
    ],
)
def test_a_record_the_steps_cannot_read_is_reported_not_released(
    fields: dict[str, Any], problem: str, perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """A hand-edited record with a missing or odd field fails every step with the reason; nothing
    treats it as released, and nothing overwrites it.
    """
    state_file = _left_behind(checkout, tmp_path)
    before = _record(state_file, **fields)
    expected = f"{state_file} records a sweep the steps cannot read ({problem})"

    rc, release = perf("release_storage_test", "--state-file", str(state_file))
    assert rc == 1 and release["released"] is False and release["error"].startswith(expected)
    _assert_schema("release_storage_test", release)
    rc, find = perf("find_storage_scale_test", f"--base={checkout}", "--state-file", str(state_file))
    assert rc == 1 and find["error"].startswith(expected)
    rc, run = _run_storage_test(perf, checkout, state_file)
    assert rc == 1 and run["state"] == "REFUSED" and run["error"].startswith(expected)
    assert state_file.read_bytes() == before
    assert not (checkout / "calls").exists()


def test_release_with_an_unwritable_state_directory_gives_json(tmp_path: Path) -> None:
    """A state directory the step cannot lock in fails with the reason, as JSON."""
    directory = tmp_path / "inputs"
    directory.mkdir()
    (directory / "state.json").write_text("{}")
    directory.chmod(0o500)
    try:
        rc, out = _run_script("release_storage_test", "--state-file", str(directory / "state.json"))
    finally:
        directory.chmod(0o700)

    assert rc == 1 and out["released"] is False
    assert out["error"].startswith(f"cannot lock {directory / '.lock'}: ")
    _assert_schema("release_storage_test", out)


# ─── Settings that render empty, odd values, absurd sizes ─────


def test_the_filesystem_is_kept_unless_the_claim_was_deleted(monkeypatch: pytest.MonkeyPatch) -> None:
    """Deleting the filesystem under a claim that is still there would strand the claim, so
    delete_perf_filesystem keeps it unless deprovision_perf_storage succeeded.
    """
    monkeypatch.delenv("STORAGE_SKIP_TEARDOWN", raising=False)
    released = {**SETUP_OUTPUTS, "release_storage_test": {"released": True}}
    for deprovision in ({}, {"deprovision_perf_storage": {"success": False}}):
        assert "--skip-destroy" in _render("delete_perf_filesystem", outputs={**released, **deprovision})
    ok = {**released, **CLAIM_DELETED}
    assert "--skip-destroy" not in _render("delete_perf_filesystem", outputs=ok)


def test_empty_set_values_render_empty_not_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """`--set tests.settings.<name>=` makes the setting null; it renders as empty (or the namespace
    default), never as the string "None".
    """
    for name in ("STORAGE_PERF_CLAIM", "STORAGE_PERF_NAMESPACE", "STORAGE_PERF_NODE_SELECTOR"):
        monkeypatch.delenv(name, raising=False)
    names = ("perf_claim", "perf_namespace", "perf_node_selector", "perf_min_mbps", "perf_min_iops")
    sets = [f"tests.settings.{name}=" for name in names]
    outputs = {**SETUP_OUTPUTS, "setup_cluster": {}}

    for step in ("create_perf_filesystem", "provision_perf_storage"):
        args = _render(step, sets, outputs)
        assert [_flag(args, "--existing-claim"), _flag(args, "--namespace")] == ["", "storage-perf"], step
    assert _flag(_render("create_perf_filesystem", sets, outputs), "--node-selector") == ""
    assert _flag(_render("run_storage_test", sets, outputs), "--node-selector") == ""
    evaluate = _render("evaluate_storage_test", sets, outputs)
    assert [_flag(evaluate, "--min-mbps"), _flag(evaluate, "--min-iops")] == ["", ""]
    assert not any(
        "None" in arg for step in ("create_perf_filesystem", "run_storage_test") for arg in _render(step, sets, outputs)
    )


def test_an_empty_floor_means_no_floor() -> None:
    """An empty perf_min_mbps or perf_min_iops reports only, like 0."""
    args = _load("evaluate_storage_test")._parse_args(["--scale-test-base=/x", "--min-mbps=", "--min-iops="])

    assert (args.min_mbps, args.min_iops) == (0.0, 0.0)


@pytest.mark.parametrize("value", ["nan", "inf", "-inf"])
def test_a_non_finite_floor_is_refused(value: str) -> None:
    """A floor that no rate can be compared with is a usage error, not a pass or NaN in the output."""
    with pytest.raises(SystemExit):
        _load("evaluate_storage_test")._parse_args(["--scale-test-base=/x", f"--min-mbps={value}"])


@pytest.mark.parametrize(("value", "expected"), [("true", 1), ("True", 1), ("false", 0), ("1", 1), ("0", 0)])
def test_live_csv_extended_accepts_true_and_false(value: str, expected: int) -> None:
    """`--set tests.settings.perf_live_csv_extended=true` renders as "True"; it means 1."""
    run = _load("run_storage_test")
    base = ["--scale-test-base=/x", "--claim=a/b", "--node-selector=a=b", "--results-dir=/r", "--state-file=/s"]
    args = run._parse_args([*base, "--live-csv-extended", value])

    assert args.live_csv_extended == expected and run._validate(args) is None
    rendered = _render("run_storage_test", ["tests.settings.perf_live_csv_extended=true"])
    assert (
        run._parse_args([*base, "--live-csv-extended", _flag(rendered, "--live-csv-extended")]).live_csv_extended == 1
    )


def test_an_empty_node_selector_stops_setup_before_any_fixture_is_created(tmp_path: Path) -> None:
    """With no node selector the sweep cannot run, so create_perf_filesystem refuses before it creates
    anything, naming where the selector comes from; the run step refuses it too.
    """
    record = tmp_path / "filesystem.json"

    out = _run_stub("create_perf_filesystem", "--node-selector=", f"--record={record}", demo=False)

    assert out["success"] is False
    assert "STORAGE_PERF_NODE_SELECTOR" in out["error"] and "perf_node_selector" in out["error"]
    assert not record.exists()
    _assert_schema("create_perf_filesystem", out)
    out = _run_stub("create_perf_filesystem", "--node-selector=pool=perf", demo=False)
    assert out["error"] == "Not implemented - create the filesystem for the performance sweep"
    run = _load("run_storage_test")
    base = ["--scale-test-base=/x", "--claim=a/b", "--results-dir=/r", "--state-file=/s"]
    assert "--node-selector is empty" in run._validate(run._parse_args([*base, "--node-selector= "]))


def test_evaluate_treats_non_finite_rates_as_invalid() -> None:
    """A rate of inf or NaN is not a measurement: results_present fails and the output is strict JSON."""
    doc = _document()
    doc["executions"][0]["results"]["phases"][0]["elbencho_first"] = {"bytes_per_s": "inf", "ops_per_s": "NaN"}
    out = _load("evaluate_storage_test").evaluate(RUN_OK, doc, 0.0, 0.0)

    assert out["tests"]["results_present"]["passed"] is False
    text = json.dumps(out)
    assert "Infinity" not in text and "NaN" not in text
    json.loads(text, parse_constant=lambda name: pytest.fail(f"non-standard JSON constant {name}"))


@pytest.mark.parametrize(
    "flag", ["--wait-timeout", "--stall-timeout", "--wait-cap", "--unreachable-grace", "--collect-timeout"]
)
def test_time_settings_over_30_days_are_refused_before_submit(
    flag: str, perf: InProcess, checkout: Path, tmp_path: Path
) -> None:
    """A huge limit is a mistake, and would overflow the wait's clock arithmetic: refused up front."""
    rc, out = _run_storage_test(perf, checkout, tmp_path / "state.json", f"{flag}={'9' * 400}")

    assert rc == 1 and out["state"] == "NOT_SUBMITTED"
    assert out["error"].startswith(f"{flag} is {'9' * 400}; it must be at most 2592000 (30 days)")
    assert not (checkout / "calls").exists()


def test_release_refuses_time_settings_over_30_days(perf: InProcess, tmp_path: Path) -> None:
    """The release step bounds its own limits the same way."""
    rc, out = perf("release_storage_test", "--state-file", str(tmp_path / "state.json"), "--collect-timeout=9999999")

    assert rc == 1 and out["error"] == "--collect-timeout is 9999999; it must be at most 2592000 (30 days)"


def test_a_huge_sweep_is_refused_without_listing_its_cells(monkeypatch: pytest.MonkeyPatch) -> None:
    """A sweep far over the cell limit is refused from its count alone, before any estimate."""
    run, sst = _load("run_storage_test"), _load("sst")
    monkeypatch.setattr(run.sst, "estimate_collection", lambda *a, **k: pytest.fail("estimated an absurd sweep"))
    monkeypatch.setattr(run.sst, "parse_node_spec", lambda spec: pytest.fail("listed the node counts"))
    base = ["--scale-test-base=/x", "--claim=a/b", "--node-selector=a=b", "--results-dir=/r", "--state-file=/s"]
    args = run._parse_args([*base, "--nodes=1-1000000000000", "--io-sizes=1M,4M"])

    _, refusal, _ = run._plan(args, type("Log", (), {"event": lambda *a, **k: None})())

    assert refusal is not None and refusal.startswith("the sweep has 2000000000000 cells")
    assert sst.count_node_counts("1-10+4,3") == len(sst.parse_node_spec("1-10+4,3")) == 5
    assert sst.count_node_counts("1-x") is None


@pytest.mark.parametrize("run_arg", ["null", "[]", "5", '"x"'])
def test_evaluate_refuses_a_run_argument_that_is_not_an_object(run_arg: str) -> None:
    """--run must be the run step's JSON object; anything else is a JSON error, not a traceback."""
    rc, out = _run_script("evaluate_storage_test", "--scale-test-base=/x", f"--run={run_arg}")

    assert rc == 1 and out["error"].startswith("--run is not a JSON object")
    _assert_schema("evaluate_storage_test", out)
