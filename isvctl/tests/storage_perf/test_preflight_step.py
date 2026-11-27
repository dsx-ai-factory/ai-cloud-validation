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

"""preflight_storage_test: storage-scale-test's validate_env.sh for the claim and nodes."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from .conftest import MODULES, SST, InProcess, assert_schema, hold, lock_is_free, ran

PREFLIGHT = MODULES["preflight_storage_test"]
PASSED_OUTPUT = (
    "Kubernetes: perf-ns/perf-claim:scale-test, 2 nodes: OK\n"
    "Filesystem roots (claim:scale-test): OK\n"
    "All validation checks passed successfully\n"
)


@pytest.fixture
def base(checkout: Path) -> Path:
    """The checkout, with a validate_env.sh."""
    (checkout / SST.VALIDATE_ENV).write_text("#!/bin/sh\nexit 2\n")
    return checkout


def preflight(perf: InProcess, base: Path, *extra: str) -> tuple[int, dict[str, Any]]:
    """Run preflight_storage_test against the checkout for perf-ns/perf-claim."""
    args = ["--claim=perf-ns/perf-claim", "--node-selector=storage-test=true", f"--results-dir={base / 'results'}"]
    return perf("preflight_storage_test", f"--scale-test-base={base}", *args, *extra)


def fake_validate(monkeypatch: pytest.MonkeyPatch, reply: Any) -> list[tuple[list[str], dict[str, str], str]]:
    """Answer sst.run with *reply*; return the calls (command, environment, env.sh then) it saw."""
    calls: list[tuple[list[str], dict[str, str], str]] = []

    def run(cmd: list[str], timeout: float, env: dict[str, str] | None = None) -> Any:
        env_sh = Path(cmd[0]).parent / SST.ENV_SH
        calls.append((cmd, env or {}, env_sh.read_text() if env_sh.is_file() else ""))
        return reply

    monkeypatch.setattr(SST, "run", run)
    return calls


def test_preflight_runs_validate_env_with_the_claim_in_env_sh(
    perf: InProcess, base: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """validate_env.sh checks env.sh's own target, so env.sh names the claim in TEST_DIRS."""
    calls = fake_validate(monkeypatch, ran(stdout=PASSED_OUTPUT))

    rc, out = preflight(perf, base, "--kubeconfig=/tmp/kc")

    assert rc == 0, out
    assert out["message"] == (
        "validate_env.sh passed: Kubernetes: perf-ns/perf-claim:scale-test, 2 nodes: OK; "
        "Filesystem roots (claim:scale-test): OK"
    )
    assert [cmd for cmd, _, _ in calls] == [[str(base / SST.VALIDATE_ENV)]]
    assert calls[0][1]["KUBECONFIG"] == "/tmp/kc"
    during = calls[0][2]
    assert SST.test_dirs("perf-ns/perf-claim") in during
    assert "export KUBECTL_NODE_SELECTOR='storage-test=true'" in during
    # Afterwards env.sh is as the run step writes it: without the claim.
    after = (base / SST.ENV_SH).read_text()
    assert "perf-claim" not in after and "export KUBECTL_NODE_SELECTOR='storage-test=true'" in after
    assert lock_is_free(base / SST.CHECKOUT_LOCK)
    assert_schema("preflight_storage_test", out)


def test_preflight_reports_validate_envs_errors(perf: InProcess, base: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed validation fails the step with each error validate_env.sh listed."""
    stdout = (
        "Kubernetes: OK\n"
        f"{PREFLIGHT.FAILED_HEADER}\n"
        "  - PVC perf-ns/perf-claim is not Bound\n"
        "  - no Ready node matches storage-test=true\n"
    )
    fake_validate(monkeypatch, ran(1, stdout=stdout))

    rc, out = preflight(perf, base)

    assert rc == 1 and out["success"] is False
    assert out["error"] == (
        "validate_env.sh failed: PVC perf-ns/perf-claim is not Bound; no Ready node matches storage-test=true"
    )
    assert_schema("preflight_storage_test", out)


@pytest.mark.parametrize(
    ("reply", "error"),
    [
        (ran(1, stderr="kubectl: not found\n"), "validate_env.sh failed: kubectl: not found"),
        (ran(0, stdout="something else\n"), "validate_env.sh failed: something else"),
        (ran(124, timed_out=True), f"validate_env.sh did not finish within {SST.COMMAND_TIMEOUT_S}s"),
    ],
    ids=["no-error-list", "no-pass-line", "timed-out"],
)
def test_preflight_fails_without_a_clear_pass(
    reply: Any, error: str, perf: InProcess, base: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only exit 0 with validate_env.sh's pass line passes; anything else fails with what it printed."""
    fake_validate(monkeypatch, reply)

    rc, out = preflight(perf, base)

    assert rc == 1 and out["error"] == error


@pytest.mark.parametrize("missing", ["--claim=", "--node-selector="])
def test_preflight_needs_a_claim_and_nodes(missing: str, perf: InProcess, base: Path) -> None:
    """Without a claim or a node selector there is nothing to check; nothing runs."""
    rc, out = preflight(perf, base, missing)

    assert rc == 1 and out["error"].endswith("set STORAGE_PERF_CLAIM and STORAGE_PERF_NODE_SELECTOR")


def test_preflight_names_a_missing_tree(perf: InProcess, tmp_path: Path) -> None:
    """A tree with no validate_env.sh is named, with how to vendor it."""
    rc, out = preflight(perf, tmp_path / "nowhere")

    assert rc == 1 and out["error"].startswith(f"no storage-scale-test at {(tmp_path / 'nowhere').resolve()}")


def test_preflight_waits_for_another_runs_submit(perf: InProcess, base: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Another run holding the checkout (writing env.sh, submitting) past one call's time fails the step."""
    calls = fake_validate(monkeypatch, ran(stdout=PASSED_OUTPUT))
    fd = hold(base / SST.CHECKOUT_LOCK)
    try:
        rc, out = preflight(perf, base)
    finally:
        os.close(fd)

    assert rc == 1 and out["error"] == f"another run is still submitting from {base}; try again once it has"
    assert calls == []


def test_preflight_drops_kubectls_version_skew_warning(
    perf: InProcess, base: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """kubectl's version-skew warning is noise in front of the real error; it is left out."""
    stdout = (
        f"{PREFLIGHT.FAILED_HEADER}\n"
        "  Warning: version difference between client (1.36) and server (1.34) exceeds the supported minor "
        "version skew of +/-1\n"
        '  Error from server (NotFound): persistentvolumeclaims "nope" not found\n'
    )
    fake_validate(monkeypatch, ran(1, stdout=stdout))

    rc, out = preflight(perf, base)

    assert rc == 1
    assert (
        out["error"] == 'validate_env.sh failed: Error from server (NotFound): persistentvolumeclaims "nope" not found'
    )
