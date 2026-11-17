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

HEADLINE = {
    "write": {"samples": 2, "best_mbps": 7123.0, "best_iops": 4064.0, "min_avg_latency_ms": 3.877},
    "read": {"samples": 2, "best_mbps": 7628.4, "best_iops": 11757.0, "min_avg_latency_ms": 1.343},
}
PASSING = {
    "sweep_completed": {"passed": True, "message": "SUCCESS, 2/2 executions"},
    "results_present": {"passed": True},
    "bandwidth_meets_min": {"passed": True, "measured_mbps": 7123.0, "min_mbps": 0.0},
    "iops_meets_min": {"passed": True, "measured_iops": 4064.0, "min_iops": 0.0},
}


def _run(**overrides: dict[str, Any]) -> dict[str, Any]:
    """Run the check on an evaluate_storage_test output: the passing tests, with *overrides*."""
    tests = {**PASSING, **overrides}
    config = {"step_output": {"success": True, "platform": "storage", "headline": HEADLINE, "tests": tests}}
    return StoragePerfSweepCheck(config=config).execute()


def test_a_passing_sweep_reports_every_stage_with_its_numbers() -> None:
    """One subtest per stage, each with its measured numbers; the check's own message has the headline."""
    result = _run()

    assert result["passed"] is True
    assert "write: 7123.0 MB/s, 4064.0 IOPS, 3.877 ms avg latency" in result["output"]
    messages = {s["name"]: s["message"] for s in result["subtests"]}
    assert list(messages) == list(SUBTESTS)
    assert all(s["passed"] for s in result["subtests"])
    assert messages["sweep_completed"] == "SUCCESS, 2/2 executions"
    assert "read: 7628.4 MB/s, 11757.0 IOPS" in messages["results_present"]
    assert messages["bandwidth_meets_min"] == (
        "7123.0 MB/s (report only, no floor set); write 7123.0 MB/s, read 7628.4 MB/s"
    )
    assert messages["iops_meets_min"] == "4064.0 IOPS (report only, no floor set); write 4064.0 IOPS, read 11757.0 IOPS"


def test_a_set_floor_is_shown() -> None:
    """With a floor set, the subtest says what it was compared with."""
    result = _run(bandwidth_meets_min={"passed": True, "measured_mbps": 7123.0, "min_mbps": 5000.0})

    bandwidth = next(s for s in result["subtests"] if s["name"] == "bandwidth_meets_min")
    assert bandwidth["message"].startswith("7123.0 MB/s (>= floor 5000.0 MB/s)")


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
    config = {"step_output": {"success": True, "headline": HEADLINE, "tests": tests}}
    result = StoragePerfSweepCheck(config=config).execute()

    assert result["passed"] is False
    assert "sweep_completed: test not found" in result["error"]
