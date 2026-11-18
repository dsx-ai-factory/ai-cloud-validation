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

"""Unit tests for the storage performance validation."""

from __future__ import annotations

from typing import Any

import pytest

from isvtest.validations.storage_perf import SUBTESTS, StoragePerfSweepCheck

pytestmark = pytest.mark.unit

RESULTS = (
    "2 executions complete: write 7123.0 MB/s, 4064.0 IOPS, 3.877 ms avg latency; "
    "read 7628.4 MB/s, 11757.0 IOPS, 1.343 ms avg latency"
)
PASSING = {
    "sweep_completed": {"passed": True, "message": "SUCCESS, 2/2 executions"},
    "results_present": {"passed": True, "message": RESULTS},
    "bandwidth_meets_min": {
        "passed": True,
        "message": "7123.0 MB/s >= floor 5000.0 MB/s (write 7123.0 MB/s, read 7628.4 MB/s)",
    },
    "iops_meets_min": {
        "passed": True,
        "message": "4064.0 IOPS >= floor 1000.0 IOPS (write 4064.0 IOPS, read 11757.0 IOPS)",
    },
}


def _run(**overrides: dict[str, Any]) -> dict[str, Any]:
    """Run the check on an evaluate_storage_test output: the passing tests, with *overrides*."""
    tests = {**PASSING, **overrides}
    config = {"step_output": {"success": True, "platform": "storage", "tests": tests}}
    return StoragePerfSweepCheck(config=config).execute()


def test_a_passing_sweep_reports_every_stage_with_its_numbers() -> None:
    """One subtest per stage, with the step's message; the check's own message has the headline."""
    result = _run()

    assert result["passed"] is True
    assert result["output"] == f"Sweep completed; {RESULTS}"
    messages = {s["name"]: s["message"] for s in result["subtests"]}
    assert list(messages) == list(SUBTESTS)
    assert all(s["passed"] for s in result["subtests"])
    assert messages == {name: test["message"] for name, test in PASSING.items()}


def test_a_stage_with_no_floor_passes_with_the_numbers() -> None:
    """A floor of 0 only reports: the check passes and the subtest message keeps the measured numbers."""
    message = "no floor set (perf_min_mbps); 7123.0 MB/s measured (write 7123.0 MB/s, read 7628.4 MB/s)"

    result = _run(bandwidth_meets_min={"passed": True, "message": message})

    assert result["passed"]
    assert {s["name"]: s["message"] for s in result["subtests"]}["bandwidth_meets_min"] == message


@pytest.mark.parametrize(
    ("name", "error"),
    [
        ("sweep_completed", "run step ended TIMEOUT: sweep did not finish within 900s"),
        ("results_present", "incomplete results: 0002: ledger failed"),
        ("bandwidth_meets_min", "7123.0 MB/s < floor 100000.0 MB/s"),
        ("iops_meets_min", "4064.0 IOPS < floor 9000.0 IOPS"),
    ],
)
def test_any_failed_stage_fails_the_sweep_with_its_reason(name: str, error: str) -> None:
    """A failed stage fails its subtest and the check, with evaluate_storage_test's reason; the rest still report."""
    result = _run(**{name: {"passed": False, "error": error}})

    assert result["passed"] is False
    assert f"{name}: {error}" in result["error"]
    outcomes = {s["name"]: s["passed"] for s in result["subtests"]}
    assert outcomes == {n: n != name for n in SUBTESTS}


def test_a_missing_evaluate_result_fails() -> None:
    """No tests block (the evaluate step failed before evaluating) fails rather than passing."""
    result = StoragePerfSweepCheck(config={"step_output": {"success": False, "platform": "storage"}}).execute()

    assert result["passed"] is False


def test_a_missing_stage_fails() -> None:
    """An evaluate output without one of the stages (an older step) fails that subtest."""
    tests = {k: v for k, v in PASSING.items() if k != "sweep_completed"}
    result = StoragePerfSweepCheck(config={"step_output": {"success": True, "tests": tests}}).execute()

    assert result["passed"] is False
    assert "sweep_completed: test not found" in result["error"]
