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

"""Real processes: the steps run as programs, every call bounded, and Ctrl-C stops the call.

These start real processes, because what they check only exists there: a timed-out call's
process group and a leftover outside it, Ctrl-C reaching a running step, a command that cannot be
started. They run only commands the tests wrote, never kubectl.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from .conftest import (
    ATTEMPT,
    COMPLETE,
    FINISHED,
    PERF_DIR,
    RESULTS_NAME,
    RUN_OK,
    SST,
    FakeLauncher,
    InProcess,
    assert_schema,
    document,
    evaluate_step,
    left_behind,
    program_env,
    ran,
    release_step,
    run_program,
    run_step,
    write_launcher,
)

# A real launcher for steps run as programs, by mode:
# - finished: submit creates a sweep that has already finished; collect collects it.
# - hang: every call records its pid and sleeps (with "trap '' TERM" too in hang-ignoring-term).
# - hang-at-status: submit works; --status records its pid and sleeps.
LAUNCHERS = {
    "finished": f"""
case "$1" in
  --status) if [[ -f "$base/collected" ]]; then echo '{COMPLETE}' | tr ' ' '\\n'; else echo '{FINISHED}' | tr ' ' '\\n'; fi ;;
  --collect) touch "$base/collected" ;;
  *) submit ;;
esac
""",
    "hang": 'echo $$ > "$base/pid"; exec sleep 60\n',
    "hang-ignoring-term": "trap '' TERM\n" + 'echo $$ > "$base/pid"; exec sleep 60\n',
    "hang-at-status": 'if [[ "$1" == --status ]]; then echo $$ > "$base/pid"; exec sleep 60; fi\nsubmit\n',
}


def real_checkout(tmp_path: Path, mode: str) -> Path:
    """A checkout with a real Bash launcher (see LAUNCHERS) and an extractor that writes document()."""
    base = tmp_path / mode
    submit = (
        "submit() {\n"
        f'  dir="$base/results/{RESULTS_NAME}"; mkdir -p "$dir/kubernetes"; echo {ATTEMPT} > "$dir/kubernetes/current-attempt"\n'
        '  echo "STORAGE_SCALE_TEST_RESULTS_DIR=$dir"\n'
        "}\n"
    )
    write_launcher(base, f'#!/usr/bin/env bash\nbase="$(cd "$(dirname "$0")/../.." && pwd)"\n{submit}{LAUNCHERS[mode]}')
    (base / "doc.json").write_text(json.dumps(document()))
    extractor = base / SST.EXTRACTOR
    extractor.parent.mkdir(parents=True, exist_ok=True)
    extractor.write_text(f'#!/usr/bin/env bash\ncp "{base / "doc.json"}" "$1/results.json"\n')
    extractor.chmod(0o755)
    return base


def alive(pid: int) -> bool:
    """True while process *pid* exists."""
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def wait_until(condition: Any, seconds: float = 10) -> bool:
    """Wait (really) until *condition()* is true, up to *seconds*; return it."""
    deadline = time.monotonic() + seconds
    while not condition() and time.monotonic() < deadline:
        time.sleep(0.05)
    return bool(condition())


def read_pid(path: Path, what: str) -> int:
    """The pid *what* writes to *path*, waiting (really) up to 20 s for the file to hold one."""

    def pid() -> int | None:
        try:
            return int(path.read_text())
        except (OSError, ValueError):
            return None

    if not wait_until(lambda: pid() is not None, 20):
        pytest.fail(f"{what} never wrote its pid to {path}")
    return pid()


class Reaper:
    """The process groups and processes a test started, killed whatever happens to the test."""

    def __init__(self) -> None:
        """Nothing registered yet."""
        self.pid_files: list[Path] = []
        self.groups: list[int] = []
        self.procs: list[subprocess.Popen[bytes]] = []

    def kill_all(self) -> None:
        """Kill everything registered; what has already exited is skipped."""
        groups = list(self.groups)
        for path in self.pid_files:
            if path.is_file() and path.read_text().strip().isdigit():
                groups.append(int(path.read_text()))
        for pgid in groups:
            try:
                os.killpg(pgid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        for proc in self.procs:
            if proc.poll() is None:
                proc.kill()
            proc.communicate()


@pytest.fixture
def reaper() -> Iterator[Reaper]:
    """Kills, at the end of the test, every process group a real-process test registered."""
    reaper = Reaper()
    yield reaper
    reaper.kill_all()


# --- The steps run as programs ---


def test_the_steps_run_as_programs(tmp_path: Path) -> None:
    """Each step runs as `python <step>.py`, as isvctl starts it, against a real launcher."""
    base = real_checkout(tmp_path, "finished")
    results = base / "results"

    rc, run = run_program(
        tmp_path,
        "run_storage_test",
        f"--scale-test-base={base}",
        "--claim=perf-ns/perf-claim",
        "--node-selector=storage-test=true",
        f"--results-dir={results}",
    )
    assert rc == 0, run

    rc, evaluate = run_program(
        tmp_path, "evaluate_storage_test", f"--scale-test-base={base}", f"--run={json.dumps(run)}"
    )
    assert rc == 0, evaluate

    rc, release = run_program(tmp_path, "release_storage_test", f"--results-dir={results}", f"--scale-test-base={base}")
    assert rc == 0 and release["message"] == f"no sweep under {results} holds cluster resources"


# --- Every call is bounded ---


def test_a_call_returns_its_exit_code_and_output(real_sst_run: None) -> None:
    """A command that fails is a result, not an exception: the steps read its exit code and output."""
    assert SST.run(["sh", "-c", "echo out; echo err >&2; exit 3"], 10) == ran(3, "out\n", "err\n")


# Started by a command under test: leaves the command's process group, records its pid and
# sleeps, still holding the command's stdout and stderr, as storage-scale-test's kubectl under
# `timeout` can.
ORPHAN = """
import os, sys, time
os.setpgid(0, 0)
open(sys.argv[1], "w").write(str(os.getpid()))
time.sleep(30)
"""


def test_a_timed_out_call_returns_despite_a_leftover_holding_its_pipes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reaper: Reaper, real_sst_run: None
) -> None:
    """A leftover outside the call's process group cannot keep the call from returning: the call
    closes its end of the pipes rather than wait for it."""
    monkeypatch.setattr(SST, "TERM_GRACE_S", 0.2)
    monkeypatch.setattr(SST, "DRAIN_S", 0.2)
    script, pid_file = tmp_path / "orphan.py", tmp_path / "orphan.pid"
    script.write_text(ORPHAN)
    reaper.pid_files.append(pid_file)
    started = time.monotonic()

    result = SST.run(["bash", "-c", f'"{sys.executable}" "{script}" "{pid_file}" & sleep 60'], 0.5)

    assert result.timed_out is True and result.returncode == 124
    assert "timed out after" in result.stderr
    # The promise is timeout + TERM_GRACE_S + DRAIN_S (0.9 s); the bound leaves slack for a loaded
    # machine and is still far below the 30 s the leftover holds the pipes.
    assert time.monotonic() - started < 0.9 + 5
    assert alive(read_pid(pid_file, "the leftover"))


@pytest.mark.parametrize("child", ["holds-the-pipes", "ignores-sigterm"])
def test_no_process_of_a_timed_out_call_outlives_it(
    child: str, tmp_path: Path, reaper: Reaper, real_sst_run: None
) -> None:
    """A child the command started in its process group dies with the timed-out call: one that holds
    the call's output is killed with the group, and so is one that ignores SIGTERM after the leader
    has exited."""
    pid_file = tmp_path / "child.pid"
    reaper.pid_files.append(pid_file)
    start = "sleep 60" if child == "holds-the-pipes" else "bash -c \"trap '' TERM; exec sleep 60\" >/dev/null 2>&1"

    result = SST.run(["bash", "-c", f'{start} & echo $! > "{pid_file}"; wait'], 1)

    assert result.timed_out is True
    pid = int(pid_file.read_text())
    assert wait_until(lambda: not alive(pid)), "the call's child outlived it"


# --- Ctrl-C ---


def _start_run(tmp_path: Path, base: Path, reaper: Reaper) -> subprocess.Popen[bytes]:
    """Start run_storage_test as a program against *base*, as isvctl would."""
    args = [f"--scale-test-base={base}", "--claim=a/b", "--node-selector=a=b", f"--results-dir={base / 'results'}"]
    proc = subprocess.Popen(
        [sys.executable, str(PERF_DIR / "run_storage_test.py"), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=program_env(tmp_path),
    )
    reaper.procs.append(proc)
    return proc


@pytest.mark.parametrize("mode", ["hang", "hang-ignoring-term"])
def test_an_interrupt_stops_the_running_call_within_two_seconds(mode: str, tmp_path: Path, reaper: Reaper) -> None:
    """Ctrl-C stops the call in flight and exits 130 before isvctl's 2 s kill; a call that ignores
    SIGTERM is killed."""
    base = real_checkout(tmp_path, mode)
    # sst.run starts the launcher in a session of its own, so its pid is its process group.
    reaper.pid_files.append(base / "pid")
    proc = _start_run(tmp_path, base, reaper)
    launcher_pid = read_pid(base / "pid", "the launcher")

    proc.send_signal(signal.SIGINT)
    started = time.monotonic()
    rc = proc.wait(timeout=10)

    # isvctl kills the step 2 s after forwarding Ctrl-C; the step's own budget is 0.5 s.
    assert time.monotonic() - started < 2
    assert rc == 130
    assert wait_until(lambda: not alive(launcher_pid)), "the launcher outlived the interrupt"


def test_an_interrupt_leaves_the_sweep_and_says_how_to_release(tmp_path: Path, reaper: Reaper) -> None:
    """Ctrl-C while waiting leaves the sweep for teardown and writes the rerun hint.

    isvctl does not show an interrupted step's stderr, so the progress log carries the hint too.
    """
    base = real_checkout(tmp_path, "hang-at-status")
    reaper.pid_files.append(base / "pid")
    proc = _start_run(tmp_path, base, reaper)
    read_pid(base / "pid", "the launcher's --status")

    proc.send_signal(signal.SIGINT)

    assert proc.wait(timeout=10) == 130
    assert (base / "results" / RESULTS_NAME / SST.CURRENT_ATTEMPT).is_file()
    hint = f"interrupted (SIGINT); {SST.TEARDOWN_HINT}"
    assert hint in proc.stderr.read().decode()
    [progress_log] = (base / "results" / "run-logs").glob("run-*.log")
    assert progress_log.read_text().splitlines()[-1].endswith(hint)


@pytest.mark.parametrize("step", ["run_storage_test", "release_storage_test", "evaluate_storage_test"])
def test_each_step_handles_interrupts_only_while_it_runs(
    step: str, perf: InProcess, launcher: FakeLauncher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every step that runs storage-scale-test handles Ctrl-C and SIGTERM while its calls run, and
    puts the earlier handlers back when it returns."""
    before = (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM))
    during: list[tuple[Any, Any]] = []

    def spy(cmd: list[str], timeout: float, env: dict[str, str] | None = None) -> Any:
        """Record the handlers in place during the call, then let FakeLauncher answer it."""
        during.append((signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)))
        return launcher(cmd, timeout, env)

    monkeypatch.setattr(SST, "run", spy)
    launcher.extractor = lambda args: ran(1)
    if step == "run_storage_test":
        run_step(perf, launcher.base)
    elif step == "release_storage_test":
        left_behind(launcher.base)
        release_step(perf, launcher.base)
    else:
        evaluate_step(perf, launcher.base, RUN_OK["results_dir"])

    assert during and all(on_int not in (*before, signal.SIG_DFL) for on_int, _ in during)
    assert all(on_term not in before for _, on_term in during)
    assert (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)) == before


@pytest.mark.parametrize(
    ("hint", "write_fails"),
    [(SST.TEARDOWN_HINT, False), (None, False), (SST.TEARDOWN_HINT, True)],
    ids=["run-or-teardown", "evaluate-no-hint", "cannot-report"],
)
def test_the_interrupt_handler_reports_and_exits_130(
    hint: str | None, write_fails: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The handler writes the teardown hint only when given one (evaluate holds nothing), and exits
    130 even when a signal landing mid-write makes reporting fail."""
    written: list[str] = []

    def write(fd: int, data: bytes) -> int:
        """Record what the handler writes, or fail as a write interrupted mid-way can."""
        if write_fails:
            raise RuntimeError("reentrant call")
        written.append(data.decode())
        return len(data)

    def exit_(code: int) -> None:
        """Stop the handler as os._exit would, keeping the exit code."""
        raise SystemExit(code)

    monkeypatch.setattr(SST.os, "write", write)
    monkeypatch.setattr(SST.os, "_exit", exit_)
    restore = SST.install_interrupt_handler(hint=hint)
    try:
        with pytest.raises(SystemExit) as exited:
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
    finally:
        restore()

    assert exited.value.code == 130
    expected = [] if write_fails else [f"interrupted (SIGTERM){f'; {hint}' if hint else ''}\n"]
    assert written == expected


# --- Tools that cannot run: always a JSON result ---


@pytest.mark.parametrize("problem", ["missing", "not-executable"])
def test_evaluate_names_an_extractor_it_cannot_run(
    problem: str, perf: InProcess, checkout: Path, tmp_path: Path, real_sst_run: None
) -> None:
    """A missing or non-executable extractor fails results_present, naming the path and the likely
    cause (the operating system's own refusal, through the real sst.run)."""
    extractor = checkout / SST.EXTRACTOR
    if problem == "not-executable":
        extractor.parent.mkdir(parents=True)
        extractor.write_text("#!/usr/bin/env python3\n")
        extractor.chmod(0o644)

    rc, out = evaluate_step(perf, checkout, tmp_path)

    assert rc == 1
    message = out["tests"]["results_present"]["error"]
    assert message.startswith(f"cannot run {extractor}: ")
    assert ("chmod +x" in message) is (problem == "not-executable")
    assert_schema("evaluate_storage_test", out)


def test_release_names_a_launcher_it_cannot_run(perf: InProcess, checkout: Path, real_sst_run: None) -> None:
    """A launcher without its executable bit fails the release with its path."""
    (checkout / SST.LAUNCHER).chmod(0o644)
    left_behind(checkout)

    rc, out = release_step(perf, checkout)

    assert rc == 1 and out["released"] is False
    assert out["error"].startswith(f"cannot run {checkout / SST.LAUNCHER}: it is not executable (chmod +x it)")
    assert_schema("release_storage_test", out)
