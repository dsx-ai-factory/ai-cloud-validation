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

"""The my-isv storage-perf overlay and the step output schemas."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from isvctl.config.merger import merge_yaml_files
from isvctl.config.schema import RunConfig
from isvctl.orchestrator.context import Context
from isvctl.orchestrator.step_executor import StepExecutor

from .conftest import ISVCTL_ROOT, SST, STEPS, FakeLauncher, InProcess, assert_schema, run_program

CONFIG_DIR = ISVCTL_ROOT / "configs" / "providers" / "my-isv" / "config"
OVERLAY_FILES = [CONFIG_DIR / "storage.yaml", CONFIG_DIR / "overlays" / "storage-perf.yaml"]
SETUP_OUTPUTS = {
    "setup_cluster": {
        "kubeconfig_path": "/tmp/kubeconfig",
        "storage_perf": {"claim": "perf-ns/perf-claim", "node_selector": "storage-test=true"},
    },
}


def _config(sets: list[str] | None = None) -> RunConfig:
    """The my-isv storage config with the perf overlay, as isvctl merges them, with optional --set values."""
    return RunConfig.model_validate(merge_yaml_files(OVERLAY_FILES, sets))


def _render(step_name: str, sets: list[str] | None = None, outputs: dict[str, Any] | None = None) -> list[str]:
    """Render one overlay step's arguments the way isvctl does, with optional --set values."""
    config = _config(sets)
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


# --- The overlay: settings -> step arguments -> storage-scale-test inputs ---


def test_run_step_receives_the_default_settings() -> None:
    """Setup outputs and the overlay's defaults reach run_storage_test."""
    args = _render("run_storage_test")

    assert _flag(args, "--kubeconfig") == "/tmp/kubeconfig"
    assert (_flag(args, "--claim"), _flag(args, "--node-selector")) == ("perf-ns/perf-claim", "storage-test=true")
    flags = ("--nodes", "--io-sizes", "--threads", "--iodepth", "--duration", "--wait-timeout", "--live-interval-ms")
    assert [_flag(args, f) for f in flags] == ["1", "r4K,1M", "1", "1", "10", "auto", "1000"]


def test_set_values_reach_storage_scale_tests_inputs(perf: InProcess, launcher: FakeLauncher, tmp_path: Path) -> None:
    """End to end: --set values -> rendered args -> env.sh and the files storage-scale-test reads."""
    results = tmp_path / "results"
    sets = [
        "tests.settings.perf_claim=perf-ns/perf-claim",
        "tests.settings.perf_node_selector=pool=perf",
        "tests.settings.perf_nodes=2",
        "tests.settings.perf_io_sizes=r64K,4M",
        "tests.settings.perf_threads=4",
        "tests.settings.perf_iodepth=1,4",
        "tests.settings.perf_duration_s=30",
        "tests.settings.perf_live_interval_ms=500",
        f"tests.settings.perf_results_dir={results}",
    ]

    rc, out = perf("run_storage_test", *_render("run_storage_test", sets, {}), f"--scale-test-base={launcher.base}")

    assert rc == 0, out
    workload = (results / "workload.env").read_text()
    for line in (
        "ELBENCHO_SCALE_IO_SIZES=('r64K' '4M')",
        "ELBENCHO_SCALE_THREAD_LIST=('4')",
        "ELBENCHO_IODEPTH_LIST=('1' '4')",
        "ELBENCHO_SCALE_READ_WRITE_DURATION=30",
        "ELBENCHO_LIVEINT=500",
    ):
        assert f"{line}\n" in workload
    assert (results / "target.env").read_text() == "TEST_DIRS=(['perf-ns/perf-claim:scale-test']=1)\n"
    env_sh = (launcher.base / "env.sh").read_text()
    assert env_sh.index("export EXECUTION_SUBSTRATE=kubectl") < env_sh.index(SST.OVERRIDES_ANCHOR)
    for line in (
        "export KUBECTL_NODE_SELECTOR='pool=perf'",
        f"export RESULTS_DIR='{results}'",
        f"export LOGS_DIR='{results / 'logs'}'",
    ):
        assert line in env_sh
    assert launcher.calls[0].endswith("--nodes 2")


def test_set_values_reach_the_evaluate_step() -> None:
    """The floors and the run step's output reach evaluate_storage_test."""
    sets = ["tests.settings.perf_min_mbps=500", "tests.settings.perf_min_iops=2000"]

    args = _render("evaluate_storage_test", sets, {"run_storage_test": {"state": "SUCCESS"}})

    assert (_flag(args, "--min-mbps"), _flag(args, "--min-iops")) == ("500", "2000")
    assert json.loads(_flag(args, "--run")) == {"state": "SUCCESS"}


def test_empty_set_values_render_empty_not_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """`--set tests.settings.<name>=` makes the setting null; it renders empty, never "None"."""
    for name in ("STORAGE_PERF_CLAIM", "STORAGE_PERF_NODE_SELECTOR"):
        monkeypatch.delenv(name, raising=False)
    sets = [f"tests.settings.{n}=" for n in ("perf_claim", "perf_node_selector", "perf_min_mbps", "perf_min_iops")]

    run = _render("run_storage_test", sets, {"setup_cluster": {}})
    evaluate = _render("evaluate_storage_test", sets, {"setup_cluster": {}})

    assert (_flag(run, "--claim"), _flag(run, "--node-selector")) == ("", "")
    assert (_flag(evaluate, "--min-mbps"), _flag(evaluate, "--min-iops")) == ("", "")
    assert not any("None" in arg for arg in [*run, *evaluate])


@pytest.mark.parametrize(
    ("outputs", "expected"),
    [({"setup_cluster": {}}, ("env-ns/env-claim", "pool=env")), (None, ("perf-ns/perf-claim", "storage-test=true"))],
    ids=["settings-fill-in", "setup-cluster-wins"],
)
def test_storage_perf_values_come_from_setup_cluster_then_the_settings(
    outputs: dict[str, Any] | None, expected: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """What setup_cluster reports as storage_perf wins; the STORAGE_PERF_* settings fill in the rest."""
    monkeypatch.setenv("STORAGE_PERF_CLAIM", "env-ns/env-claim")
    monkeypatch.setenv("STORAGE_PERF_NODE_SELECTOR", "pool=env")

    args = _render("run_storage_test", outputs=outputs)

    assert (_flag(args, "--claim"), _flag(args, "--node-selector")) == expected


@pytest.mark.parametrize(
    "outputs",
    [{}, {"setup_cluster": {}}, {"setup_cluster": {"kubeconfig_path": None}}],
    ids=["no-setup", "none", "null"],
)
def test_without_a_setup_kubeconfig_run_and_teardown_inherit_kubeconfig(outputs: dict[str, Any]) -> None:
    """With no kubeconfig_path from setup (`--phase teardown`, say), --kubeconfig is empty, never "None"."""
    run = _render("run_storage_test", outputs=outputs)
    release = _render("release_storage_test", outputs=outputs)

    assert _flag(run, "--kubeconfig") == _flag(release, "--kubeconfig") == ""
    assert _flag(release, "--results-dir").endswith("_output/storage-perf/results")


def test_a_kubeconfig_setup_reports_wins() -> None:
    """A provider setup that creates its cluster reports kubeconfig_path; run and teardown use it."""
    assert _flag(_render("run_storage_test"), "--kubeconfig") == "/tmp/kubeconfig"
    assert _flag(_render("release_storage_test"), "--kubeconfig") == "/tmp/kubeconfig"
    assert _flag(_render("preflight_storage_test"), "--kubeconfig") == "/tmp/kubeconfig"


def test_self_limiting_steps_have_no_isvctl_timeout() -> None:
    """The perf steps bound every call themselves; an isvctl timeout would only cut them short."""
    steps = {s.name: s for s in _config().commands["storage"].steps}

    assert [steps[name].timeout for name in STEPS] == [None] * len(STEPS)


# --- Output schemas ---


@pytest.mark.parametrize("step", STEPS)
def test_demo_outputs_match_their_schemas(step: str, tmp_path: Path) -> None:
    """Each perf step's demo-mode output (ISVCTL_DEMO_MODE=1) passes schema validation."""
    args = {
        "preflight_storage_test": ["--claim=a/b", "--node-selector=a=b", "--results-dir=/x"],
        "run_storage_test": ["--claim=a/b", "--node-selector=a=b", "--results-dir=/x"],
        "evaluate_storage_test": [],
        "release_storage_test": ["--results-dir=/x"],
    }[step]

    assert_schema(step, run_program(tmp_path, step, *args, demo=True)[1])


@pytest.mark.parametrize(
    ("step", "output"),
    [
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
    ],
    ids=["run-success-not-success", "evaluate-test-no-passed", "release-no-released"],
)
def test_schemas_reject_outputs_missing_what_later_steps_read(step: str, output: dict[str, Any]) -> None:
    """A step that reports success without the fields later steps depend on fails at the step."""
    assert_schema(step, output, valid=False)


def test_preflight_checks_what_the_run_sweeps() -> None:
    """The preflight step gets the same claim, node selector and results root as the run step."""
    preflight, run = _render("preflight_storage_test"), _render("run_storage_test")

    for flag in ("--claim", "--node-selector", "--results-dir", "--kubeconfig"):
        assert _flag(preflight, flag) == _flag(run, flag)


def test_preflight_runs_before_the_sweep_and_stops_the_phase_when_it_fails() -> None:
    """It is the test phase's first perf step and not continue_on_failure, so a failure skips the sweep."""
    steps = [s for s in _config().commands["storage"].steps if s.phase == "test"]
    names = [s.name for s in steps]

    assert names.index("preflight_storage_test") < names.index("run_storage_test")
    assert next(s for s in steps if s.name == "preflight_storage_test").continue_on_failure is False
