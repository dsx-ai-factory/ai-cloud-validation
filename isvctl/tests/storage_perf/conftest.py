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

"""Shared fixtures and fakes for the storage performance step tests.

The steps run storage-scale-test through one seam, ``sst.run(cmd, timeout, env)``. The tests
load the steps as modules and replace that seam with FakeLauncher, which plays back a scripted
sweep (submit, ``--status``, ``--cancel``, ``--collect``). The steps run in-process on a fake
clock, so hours of polling take no time.

No test reaches a real cluster: the autouse fixture ``no_unplanned_commands`` unsets KUBECONFIG
and makes sst.run fail the test for any command until the test installs a fake, or asks for the
real sst.run (``real_sst_run``) to run commands it wrote itself. Steps run as programs get a
kubectl on PATH that only fails.
"""

from __future__ import annotations

import fcntl
import importlib.util
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, NoReturn

import pytest

from isvctl.config.output_schemas import get_schema_for_step, validate_output

from ..conftest import ISVCTL_ROOT

PERF_DIR = ISVCTL_ROOT / "configs" / "shared" / "storage" / "perf"
VENDORED = ISVCTL_ROOT.parent / "isvtest" / "vendor" / "storage-scale-test"
STEPS = ("run_storage_test", "release_storage_test", "evaluate_storage_test")


def _load_steps() -> dict[str, ModuleType]:
    """Load the steps as modules, plus the ``sst`` module they share, leaving sys.path as it was.

    Each step puts its own directory on sys.path and imports ``sst``; both are undone once the
    steps are loaded (they keep their reference to the one ``sst``). ISVCTL_DEMO_MODE is unset
    while sst reads it at import.
    """
    earlier_sst = sys.modules.pop("sst", None)
    loaded: dict[str, ModuleType] = {}
    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.delenv("ISVCTL_DEMO_MODE", raising=False)
            patch.syspath_prepend(str(PERF_DIR))
            for name in STEPS:
                spec = importlib.util.spec_from_file_location(f"storage_perf_{name}", PERF_DIR / f"{name}.py")
                assert spec is not None and spec.loader is not None
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                loaded[name] = module
            loaded["sst"] = sys.modules["sst"]
    finally:
        sys.modules.pop("sst", None)
        if earlier_sst is not None:
            sys.modules["sst"] = earlier_sst
    assert all(loaded[name].sst is loaded["sst"] for name in STEPS)
    return loaded


MODULES = _load_steps()
SST = MODULES["sst"]
RUN = MODULES["run_storage_test"]
EVALUATE = MODULES["evaluate_storage_test"]
REAL_SST_RUN = SST.run

# --status replies, written on one line ("KEY=value KEY=value") as FakeLauncher.statuses takes them.
RUNNING = "STATE=RUNNING EXECUTIONS_TOTAL=2 EXECUTIONS_SUCCEEDED=0 NEXT_ACTION=WAIT"
FINISHED = "STATE=SUCCESS EXECUTIONS_TOTAL=2 EXECUTIONS_SUCCEEDED=2 RESULT_COLLECTION=PENDING NEXT_ACTION=COLLECT"
COMPLETE = "STATE=SUCCESS EXECUTIONS_TOTAL=2 RESULT_COLLECTION=COMPLETE NEXT_ACTION=NONE"
CLEANUP = "STATE=SUCCESS EXECUTIONS_TOTAL=2 RESULT_COLLECTION=CLEANUP_PENDING NEXT_ACTION=COLLECT"
SUBMISSION_FAILED = "STATE=SUBMISSION_FAILED EXECUTIONS_TOTAL=2 NEXT_ACTION=INSPECT"
# An incomplete cancel: storage-scale-test reports it with exit 1 (see FakeLauncher).
CANCEL_REQUESTED = "CANCEL_REQUESTED"

RESULTS_NAME = "elbencho-20260101Z000000"
ATTEMPT = "0123abcd"
# cancels_needed / cleanup_pending: a cancel or a cleanup that never finishes.
NEVER = 99
UNPLANNED_COMMAND = "unexpected command run from a test"
LIFECYCLE = ("--status", "--cancel", "--collect")

RUN_OK: dict[str, Any] = {
    "state": "SUCCESS",
    "executions": {"total": 2, "succeeded": 2, "failed": 0},
    "resources_released": True,
    "results_dir": "/results/elbencho-x",
}


def reply(line: str) -> str:
    """A one-line status reply as storage-scale-test prints it, one KEY=value per line."""
    return line.replace(" ", "\n") + "\n"


def diag(reason: str, local_state: str = "", phase: str = "") -> str:
    """A storage-scale-test diagnostic block on stderr, quoted as it prints it."""
    text = f"Error: the operation failed\nSTORAGE_SCALE_TEST_DIAGNOSTIC_REASON={reason}\n"
    text += f"STORAGE_SCALE_TEST_DIAGNOSTIC_LOCAL_STATE={local_state or "''"}\n"
    if phase:
        text += f"STORAGE_SCALE_TEST_DIAGNOSTIC_PHASE={phase}\n"
    return text


def ran(returncode: int = 0, stdout: str = "", stderr: str = "", timed_out: bool = False) -> Any:
    """A finished storage-scale-test call, as sst.run returns it."""
    return SST.Ran(returncode, stdout, stderr, timed_out)


def _unplanned_run(cmd: list[str], timeout: float, env: dict[str, str] | None = None) -> NoReturn:
    """sst.run while no test chose another: every command fails the test, kubectl above all."""
    raise AssertionError(f"{UNPLANNED_COMMAND}: {cmd}")


@pytest.fixture(autouse=True)
def no_unplanned_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    """No test runs a command it did not plan for, so none reaches a real kubectl or kubeconfig."""
    for name in ("KUBECONFIG", "ISVCTL_DEMO_MODE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(SST, "run", _unplanned_run)


@pytest.fixture
def real_sst_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """This test starts real processes through the real sst.run (only commands it wrote itself)."""
    monkeypatch.setattr(SST, "run", REAL_SST_RUN)


class FakeLauncher:
    """Stands in for sst.run: this checkout's launcher (including ``--version``) and extractor.

    The launcher plays back one sweep:

    - ``statuses``: the ``--status`` replies, one per call, the last one repeating. Besides a
      reply line: "FAIL" exits 1 with reason API_UNAVAILABLE (a lost connection), "FAIL:<REASON>"
      with that reason, "SILENT" with none, and CANCEL_REQUESTED reports an incomplete cancel.
    - ``cancels_needed``: ``--cancel`` calls before the cancel completes; until then ``--status``
      reports CANCEL_REQUESTED. After it, ``--status`` reports a cancelled sweep.
    - ``cleanup_pending``: ``--collect`` calls that publish the results but leave cleanup pending.
    - ``lock_busy``: lifecycle calls refused as if another command held the sweep's lock.
    - ``fixed``: sweep directory -> the ``--status`` reply of another sweep, which holds nothing.

    Like storage-scale-test, ``--collect`` refuses a sweep that has not stopped. Each submit
    starts a new sweep in a new sweep directory with its current-attempt file. ``calls`` records
    each launcher call's arguments and ``envs`` its environment.
    """

    def __init__(self, base: Path) -> None:
        """Script the default sweep: one WAIT, then finished."""
        self.base = base
        self.statuses = [RUNNING, FINISHED]
        self.cancels_needed = 1
        self.cleanup_pending = 0
        self.lock_busy = 0
        self.fixed: dict[str, str] = {}
        self.calls: list[str] = []
        self.envs: list[dict[str, str]] = []
        self.sweep_dirs: list[Path] = []
        self.version: str | None = None
        self.extractor: Any = None
        self._start_sweep()

    def _start_sweep(self) -> None:
        """A new sweep: nothing polled, cancelled or collected."""
        self.polled = 0
        self.final = "SUCCESS"
        self.cancel_pending = False
        self.cancelled = False
        self.cleanup_started = False
        self.collected = False

    def operations(self) -> list[str]:
        """The launcher's calls by operation: ``submit``, ``--status``, ``--cancel`` or ``--collect``."""
        return [call.split()[0] if call.startswith(LIFECYCLE) else "submit" for call in self.calls]

    def calls_on(self, operation: str) -> list[str]:
        """The sweep directories *operation* was called on."""
        return [call.split()[1] for call in self.calls if call.startswith(operation + " ")]

    def __call__(self, cmd: list[str], timeout: float, env: dict[str, str] | None = None) -> Any:
        """Answer one sst.run call."""
        if cmd == [str(self.base / SST.LAUNCHER), "--version"]:
            return ran(stdout=f"{self.version}\n") if self.version else ran(127)
        if cmd[0] == str(self.base / SST.LAUNCHER):
            self.calls.append(" ".join(cmd[1:]))
            self.envs.append(env or {})
            return self._launcher(cmd[1:])
        if cmd[0] == str(self.base / SST.EXTRACTOR) and self.extractor is not None:
            return self.extractor(cmd[1:])
        raise AssertionError(f"unexpected command: {cmd}")

    def _launcher(self, args: list[str]) -> Any:
        """One launcher call: submit, or a lifecycle command on one sweep directory."""
        if not args or args[0] not in LIFECYCLE:
            return self.submit(args)
        operation, sweep_dir = args
        if self.lock_busy > 0:
            self.lock_busy -= 1
            return ran(1, stderr=f"Error: {SST.LOCK_BUSY} for {sweep_dir}/kubernetes\n")
        if sweep_dir in self.fixed:
            assert operation == "--status", f"{operation} on {sweep_dir}, which holds nothing"
            return ran(stdout=reply(self.fixed[sweep_dir]))
        return {"--status": self._status, "--cancel": self._cancel, "--collect": self._collect}[operation](sweep_dir)

    def _state(self) -> str:
        """The sweep's STATE, from the last status reply played."""
        played = self.statuses[: max(self.polled, 1)]
        return next((s.split()[0].removeprefix("STATE=") for s in reversed(played) if s.startswith("STATE=")), "")

    def _status(self, sweep_dir: str) -> Any:
        """``--status``."""
        if not (Path(sweep_dir) / SST.CURRENT_ATTEMPT).is_file():
            return ran(1, stderr=f"Error: result directory has no valid current Kubernetes attempt: {sweep_dir}\n")
        if self.collected:
            return ran(
                stdout=reply(
                    f"STATE={self.final} EXECUTIONS_TOTAL=2 EXECUTIONS_SUCCEEDED=2 EXECUTIONS_FAILED=0 "
                    "RESULT_COLLECTION=COMPLETE NEXT_ACTION=NONE"
                )
            )
        if self.cleanup_started:
            return ran(stdout=reply(f"STATE={self.final} RESULT_COLLECTION=CLEANUP_PENDING NEXT_ACTION=COLLECT"))
        if self.cancelled:
            return ran(stdout=reply("STATE=CANCELLED EXECUTIONS_TOTAL=2 RESULT_COLLECTION=PENDING NEXT_ACTION=COLLECT"))
        line = CANCEL_REQUESTED if self.cancel_pending else self.statuses[min(self.polled, len(self.statuses) - 1)]
        self.polled += 1
        if line == CANCEL_REQUESTED:
            return ran(1, reply("STATE=CANCEL_REQUESTED NEXT_ACTION=INSPECT"), diag("CANCELLATION_INCOMPLETE"))
        if line == "SILENT":
            return ran(1)
        if line.startswith("FAIL"):
            return ran(1, stderr=diag(line.partition(":")[2] or "API_UNAVAILABLE"))
        return ran(stdout=reply(line))

    def _stopped(self) -> bool:
        """Whether the sweep has stopped, so that ``--collect`` may act on it."""
        if self.cancelled or self.cleanup_started or self.collected:
            return True
        played = self.statuses[min(max(self.polled - 1, 0), len(self.statuses) - 1)]
        return re.search(r"\bNEXT_ACTION=(COLLECT|NONE)\b", played) is not None

    def _collect(self, sweep_dir: str) -> Any:
        """``--collect``: refused until the sweep has stopped."""
        if not self._stopped():
            return ran(1, stderr="Error: the attempt has not stopped; cancel it first\n")
        self.final = "CANCELLED" if self.cancelled else self._state()
        self.cleanup_started = self.cleanup_pending > 0
        self.cleanup_pending -= 1
        self.collected = not self.cleanup_started
        return ran()

    def _cancel(self, sweep_dir: str) -> Any:
        """``--cancel``: completes on the ``cancels_needed``-th call."""
        self.cancels_needed -= 1
        self.cancel_pending = self.cancels_needed > 0
        self.cancelled = not self.cancel_pending
        return ran()

    def submit(self, args: list[str]) -> Any:
        """Submit: a new sweep in a new sweep directory, with its current-attempt file."""
        assert args[0::2] == ["--target", "--env-override", "--nodes"], args
        assert Path(args[1]).is_file() and Path(args[3]).is_file(), args
        if self.sweep_dirs and self.collected:
            self.fixed[str(self.sweep_dirs[-1])] = COMPLETE
        self._start_sweep()
        sweep_dir = left_behind(self.base, f"elbencho-20260101Z{len(self.sweep_dirs):06d}")
        self.sweep_dirs.append(sweep_dir)
        return ran(stdout=f"STORAGE_SCALE_TEST_RESULTS_DIR={sweep_dir}\nSTORAGE_SCALE_TEST_ATTEMPT_ID={ATTEMPT}\n")


class FakeClock:
    """Stands in for sst.CLOCK: sleeping only moves the time forward."""

    def __init__(self) -> None:
        """Start at zero."""
        self.elapsed = 0.0

    def monotonic(self) -> float:
        """The fake time."""
        return self.elapsed

    def sleep(self, seconds: float) -> None:
        """Advance the fake time; fail a wait that would never end instead of hanging the test."""
        self.elapsed += seconds
        assert self.elapsed < 31 * 24 * 3600, "waited more than 31 days of fake time"


class Scripted:
    """Stands in for sst.run: one reply list per operation, each call taking fake time.

    A reply is ``(Ran, seconds)``; a timed-out reply takes the call's whole timeout. The last
    reply for an operation repeats.
    """

    def __init__(self, clock: FakeClock, replies: dict[str, list[tuple[Any, float]]]) -> None:
        """Script *replies* on *clock*."""
        self.clock = clock
        self.replies = replies
        self.calls: list[tuple[str, float]] = []

    def __call__(self, cmd: list[str], timeout: float, env: dict[str, str] | None = None) -> Any:
        """Reply to one launcher call."""
        self.calls.append((cmd[1], timeout))
        queue = self.replies[cmd[1]]
        result, seconds = queue.pop(0) if len(queue) > 1 else queue[0]
        self.clock.elapsed += timeout if result.timed_out else seconds
        return result


@pytest.fixture
def scripted(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Script sst's launcher calls on a fresh fake clock: ``scripted({operation: replies})``."""

    def install(replies: dict[str, list[tuple[Any, float]]]) -> Scripted:
        """Install a Scripted fake with these replies."""
        fake = Scripted(FakeClock(), replies)
        monkeypatch.setattr(SST, "CLOCK", fake.clock)
        monkeypatch.setattr(SST, "run", fake)
        return fake

    return install


@pytest.fixture
def checkout(tmp_path: Path) -> Path:
    """A storage-scale-test checkout: the files the steps read, and a launcher that only fails."""
    base = tmp_path / "storage-scale-test"
    write_launcher(base, "#!/bin/sh\necho 'FakeLauncher answers this launcher in-process' >&2\nexit 2\n")
    return base


@pytest.fixture
def launcher(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> FakeLauncher:
    """The checkout's launcher, replaced in-process by a FakeLauncher."""
    fake = FakeLauncher(checkout)
    monkeypatch.setattr(SST, "run", fake)
    return fake


class InProcess:
    """Runs a step's main() in this process, on one fake clock per test."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
        """Use one fake clock for every step the test runs."""
        self.monkeypatch = monkeypatch
        self.capsys = capsys
        self.clock = FakeClock()

    def __call__(self, step: str, *argv: str) -> tuple[int, dict[str, Any]]:
        """Run *step* with these arguments, outside demo mode; return its exit code and JSON."""
        self.monkeypatch.setattr(SST, "CLOCK", self.clock)
        self.monkeypatch.setattr(SST, "DEMO_MODE", False)
        self.capsys.readouterr()
        rc = MODULES[step].main(list(argv))
        return rc, json.loads(self.capsys.readouterr().out)


@pytest.fixture
def perf(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> InProcess:
    """Run steps in-process on a fake clock."""
    return InProcess(monkeypatch, capsys)


def run_step(perf: InProcess, base: Path, *extra: str) -> tuple[int, dict[str, Any]]:
    """Run run_storage_test against the checkout; its results root is base/results."""
    args = ["--claim=perf-ns/perf-claim", "--node-selector=storage-test=true", f"--results-dir={base / 'results'}"]
    return perf("run_storage_test", f"--scale-test-base={base}", *args, *extra)


def release_step(perf: InProcess, base: Path, *extra: str) -> tuple[int, dict[str, Any]]:
    """Run release_storage_test over base/results."""
    return perf("release_storage_test", f"--results-dir={base / 'results'}", f"--scale-test-base={base}", *extra)


def evaluate_step(perf: InProcess, base: Path, sweep_dir: Path | str) -> tuple[int, dict[str, Any]]:
    """Run evaluate_storage_test on a run that reports *sweep_dir*."""
    run = json.dumps({**RUN_OK, "results_dir": str(sweep_dir)})
    return perf("evaluate_storage_test", f"--scale-test-base={base}", f"--run={run}")


def left_behind(base: Path, name: str = RESULTS_NAME) -> Path:
    """A sweep directory under base/results whose sweep was submitted (it has a current attempt)."""
    sweep_dir = base / "results" / name
    (sweep_dir / SST.CURRENT_ATTEMPT).parent.mkdir(parents=True, exist_ok=True)
    (sweep_dir / SST.CURRENT_ATTEMPT).write_text(f"{ATTEMPT}\n")
    return sweep_dir


def events(out: dict[str, Any], event: str) -> list[dict[str, str]]:
    """The fields of each progress log line for *event* (``<time>  +<s>  <event>  k=v ...``)."""
    found = []
    for line in Path(out["progress_log"]).read_text().splitlines():
        parts = line.split("  ")
        if parts[2] == event:
            found.append(dict(pair.split("=", 1) for pair in (parts[3].split() if len(parts) > 3 else [])))
    return found


def assert_cancelled_before_collect(launcher: FakeLauncher) -> None:
    """The sweep was cancelled before its first collect: collect refuses a running sweep."""
    operations = launcher.operations()
    assert "--cancel" in operations and "--collect" in operations, operations
    assert operations.index("--cancel") < operations.index("--collect"), operations


def assert_schema(step: str, output: dict[str, Any], valid: bool = True) -> None:
    """Validate a step output against the schema isvctl picks for that step name."""
    schema = get_schema_for_step(step)
    assert schema is not None and schema.startswith("storage_perf_"), schema
    ok, errors = validate_output(output, schema)
    assert ok is valid, (step, output, errors)


def document(*executions: dict[str, Any]) -> dict[str, Any]:
    """A results.json document; by default one complete write-then-read execution."""
    if not executions:
        executions = (execution(write=(104_857_600, 200)),)
    complete = all(e["ledger_state"] == "success" and e["result_state"] == "complete" for e in executions)
    return {
        "schema": EVALUATE.RESULTS_SCHEMA,
        "schema_version": "1.0",
        "batch_outcome": {"complete": complete},
        "executions": list(executions),
    }


def execution(
    write: tuple[float, float], read: tuple[float, float] = (157_286_400, 300), **fields: Any
) -> dict[str, Any]:
    """One complete IO execution whose write and read run at (bytes/s, ops/s)."""

    def phase(op: str, rates: tuple[float, float], avg_us: float) -> dict[str, Any]:
        """One results.json IO phase."""
        sample = {"bytes_per_s": rates[0], "ops_per_s": rates[1], "elapsed_ms": 10000}
        return {"phase": op, "elbencho_first": sample, "elbencho_last": sample, "latency_us": {"avg": avg_us}}

    return {
        "execution_id": "0001",
        "kind": "io",
        "ledger_state": "success",
        "result_state": "complete",
        "problems": [],
        "results": {"phases": [phase("write", write, 5000), phase("read", read, 4000)]},
        **fields,
    }


# --- Launchers and processes ---


def write_launcher(base: Path, script: str) -> Path:
    """Write *script* as the checkout's launcher, with the env.sh template the run step needs."""
    path = base / SST.LAUNCHER
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(script)
    path.chmod(0o755)
    (base / SST.ENV_TEMPLATE).write_text(f"TEST_DIRS=()\n{SST.OVERRIDES_ANCHOR}\n")
    return base


def program_env(tmp_path: Path, demo: bool = False) -> dict[str, str]:
    """The environment for a step run as a program: a kubectl that only fails, no kubeconfig."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    (bin_dir / "kubectl").write_text("#!/bin/sh\necho 'kubectl is not available in these tests' >&2\nexit 1\n")
    (bin_dir / "kubectl").chmod(0o755)
    env = {k: v for k, v in os.environ.items() if k not in ("ISVCTL_DEMO_MODE", "KUBECONFIG", "KUBECTL")}
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    if demo:
        env["ISVCTL_DEMO_MODE"] = "1"
    return env


def run_program(tmp_path: Path, step: str, *args: str, demo: bool = False) -> tuple[int, dict[str, Any]]:
    """Run a step as `python <step>.py`, as isvctl starts it; return its exit code and JSON."""
    proc = subprocess.run(
        [sys.executable, str(PERF_DIR / f"{step}.py"), *args],
        capture_output=True,
        text=True,
        env=program_env(tmp_path, demo),
        check=False,
        timeout=60,
    )
    return proc.returncode, json.loads(proc.stdout)


def lock_is_free(path: Path) -> bool:
    """True when nothing holds an exclusive flock on *path*."""
    fd = os.open(path, os.O_RDWR | os.O_CREAT)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False
    finally:
        os.close(fd)


def hold(path: Path) -> int:
    """Lock *path* as another run would; closing the returned descriptor releases it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return fd
