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

"""Unit tests for the storage performance validations."""

from __future__ import annotations

from typing import Any

import pytest

from isvtest.validations.storage_perf import (
    StoragePerfBandwidthFloorCheck,
    StoragePerfIopsFloorCheck,
    StoragePerfResultsPresentCheck,
)

pytestmark = pytest.mark.unit

HEADLINE = {
    "write": {"samples": 2, "best_mbps": 7123.0, "best_iops": 4064.0, "min_avg_latency_ms": 3.877},
    "read": {"samples": 2, "best_mbps": 7628.4, "best_iops": 11757.0, "min_avg_latency_ms": 1.343},
}


def _evaluate_output(**tests: dict[str, Any]) -> dict[str, Any]:
    """An evaluate_storage_test output with the given tests and the L1 headline."""
    return {"step_output": {"success": True, "platform": "storage", "headline": HEADLINE, "tests": tests}}


def test_results_present_reports_the_headline_numbers() -> None:
    """A pass shows each operation's best bandwidth, IOPS and latency."""
    result = StoragePerfResultsPresentCheck(config=_evaluate_output(results_present={"passed": True})).execute()

    assert result["passed"] is True
    assert "write: 7123.0 MB/s, 4064.0 IOPS, 3.877 ms avg latency" in result["output"]
    assert "read: 7628.4 MB/s" in result["output"]


def test_results_present_fails_with_the_evaluate_reason() -> None:
    """Incomplete results fail with the reason evaluate_storage_test gave."""
    result = StoragePerfResultsPresentCheck(
        config=_evaluate_output(results_present={"passed": False, "error": "incomplete results: 0002: ledger failed"})
    ).execute()

    assert result["passed"] is False
    assert "0002: ledger failed" in result["error"]


@pytest.mark.parametrize(
    ("cls", "test_name", "test", "expected"),
    [
        (
            StoragePerfBandwidthFloorCheck,
            "bandwidth_meets_min",
            {"passed": True, "measured_mbps": 7123.0, "min_mbps": 0.0},
            "7123.0 MB/s (report only, no floor set); write 7123.0 MB/s, read 7628.4 MB/s",
        ),
        (
            StoragePerfBandwidthFloorCheck,
            "bandwidth_meets_min",
            {"passed": True, "measured_mbps": 7123.0, "min_mbps": 5000.0},
            "7123.0 MB/s (>= floor 5000.0 MB/s)",
        ),
        (
            StoragePerfIopsFloorCheck,
            "iops_meets_min",
            {"passed": True, "measured_iops": 4064.0, "min_iops": 0.0},
            "4064.0 IOPS (report only, no floor set); write 4064.0 IOPS, read 11757.0 IOPS",
        ),
    ],
)
def test_floor_checks_report_the_measured_value(cls: type, test_name: str, test: dict[str, Any], expected: str) -> None:
    """A floor check's pass message carries the measured value, the floor and the per-operation figures."""
    result = cls(config=_evaluate_output(**{test_name: test})).execute()

    assert result["passed"] is True
    assert expected in result["output"]


@pytest.mark.parametrize(
    ("cls", "test_name", "test"),
    [
        (
            StoragePerfBandwidthFloorCheck,
            "bandwidth_meets_min",
            {
                "passed": False,
                "measured_mbps": 7123.0,
                "min_mbps": 100000.0,
                "error": "7123.0 MB/s < floor 100000.0 MB/s",
            },
        ),
        (
            StoragePerfIopsFloorCheck,
            "iops_meets_min",
            {"passed": False, "measured_iops": 4064.0, "min_iops": 9000.0, "error": "4064.0 IOPS < floor 9000.0 IOPS"},
        ),
    ],
)
def test_floor_checks_fail_with_the_measured_value(cls: type, test_name: str, test: dict[str, Any]) -> None:
    """A missed floor fails with the measured value and the floor, not just expected/got."""
    result = cls(config=_evaluate_output(**{test_name: test})).execute()

    assert result["passed"] is False
    assert test["error"] in result["error"]


def test_a_missing_evaluate_result_fails() -> None:
    """No tests block (the evaluate step failed before evaluating) fails rather than passing."""
    result = StoragePerfBandwidthFloorCheck(config={"step_output": {"success": False, "platform": "storage"}}).execute()

    assert result["passed"] is False


@pytest.mark.parametrize(
    ("cls", "test_name", "test"),
    [
        (StoragePerfResultsPresentCheck, "results_present", {"passed": True}),
        (
            StoragePerfBandwidthFloorCheck,
            "bandwidth_meets_min",
            {"passed": True, "measured_mbps": 1.0, "min_mbps": 0.0},
        ),
        (StoragePerfIopsFloorCheck, "iops_meets_min", {"passed": True, "measured_iops": 1.0, "min_iops": 0.0}),
    ],
)
def test_checks_report_no_subtests(cls: type, test_name: str, test: dict[str, Any]) -> None:
    """With subtests, isvctl's summary prints "N subtests passed" instead of the measured numbers."""
    result = cls(config=_evaluate_output(**{test_name: test})).execute()

    assert result["passed"] is True
    assert result["subtests"] == []
