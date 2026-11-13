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

"""Storage performance validations for the storage-scale-test sweep.

Provider-neutral assertions over ``evaluate_storage_test``'s output: its ``tests``
results and its ``headline`` (per operation: best MB/s, best IOPS, lowest average
latency). Unlike a generic composite, each check puts the measured numbers in the
report, so a run passing with report-only floors still shows them. Each check reads a
single result, so it reports no subtests: isvctl's run summary shows a passing check's
own message only when it has none (otherwise it prints "N subtests passed").

Classes map to the storage performance requirements in ``docs/test-plan.yaml``:

    - StoragePerfResultsPresentCheck   SPERF07-01  complete write and read results
    - StoragePerfBandwidthFloorCheck   SPERF08-01  bandwidth meets the configured floor
    - StoragePerfIopsFloorCheck        SPERF08-02  IOPS meets the configured floor
"""

from __future__ import annotations

from typing import Any, ClassVar

from isvtest.core.validation import BaseValidation


def _step_output(validation: BaseValidation) -> dict[str, Any]:
    """Return the evaluate step's output."""
    return validation.config.get("step_output", {})


def _headline(validation: BaseValidation, field: str, unit: str) -> str:
    """Format the step's headline as ``write 7123.0 MB/s, read 7628.4 MB/s``, or ``""``."""
    headline = _step_output(validation).get("headline") or {}
    parts = [f"{op} {numbers[field]} {unit}" for op, numbers in headline.items() if field in numbers]
    return ", ".join(parts)


def _require(validation: BaseValidation, test_name: str, fail_label: str) -> bool:
    """Fail the check unless the step's ``tests[test_name]`` passed; True if it did."""
    tests = _step_output(validation).get("tests")
    if not tests:
        validation.set_failed("No 'tests' in step output")
        return False
    result = tests.get(test_name) or {}
    if not result.get("passed"):
        validation.set_failed(f"{fail_label}: {test_name}: {result.get('error', 'test not found')}")
        return False
    return True


def _floor_message(validation: BaseValidation, test_name: str, field: str, unit: str) -> str:
    """Format the measured value against its floor, plus the per-operation figures."""
    test = (_step_output(validation).get("tests") or {}).get(test_name, {})
    floor = test.get(f"min_{field}", 0)
    verdict = "report only, no floor set" if not floor else f">= floor {floor} {unit}"
    per_op = _headline(validation, f"best_{field}", unit)
    return f"{test.get(f'measured_{field}', 'N/A')} {unit} ({verdict})" + (f"; {per_op}" if per_op else "")


class StoragePerfResultsPresentCheck(BaseValidation):
    """Validate the sweep reported complete write and read results (SPERF07-01).

    Config:
        step_output: The evaluate_storage_test step output

    Step output:
        tests: dict with results_present
        headline: per operation samples, best_mbps, best_iops, min_avg_latency_ms
    """

    description: ClassVar[str] = "Check the collected reports hold real write and read results"

    def run(self) -> None:
        """Validate results are complete, reporting the headline numbers."""
        if not _require(self, "results_present", "Storage performance results incomplete"):
            return
        headline = _step_output(self).get("headline") or {}
        summary = "; ".join(
            f"{op}: {n.get('best_mbps')} MB/s, {n.get('best_iops')} IOPS, {n.get('min_avg_latency_ms')} ms avg latency"
            for op, n in headline.items()
        )
        self.set_passed(f"Complete results ({summary})" if summary else "Complete results")


class StoragePerfBandwidthFloorCheck(BaseValidation):
    """Validate measured bandwidth meets the configured floor (SPERF08-01).

    Config:
        step_output: The evaluate_storage_test step output

    Step output:
        tests: dict with bandwidth_meets_min (measured_mbps, min_mbps)
    """

    description: ClassVar[str] = "Check measured bandwidth meets the configured floor (0 = report only)"

    def run(self) -> None:
        """Validate bandwidth against the floor, reporting the measured MB/s."""
        if not _require(self, "bandwidth_meets_min", "Bandwidth below the configured floor"):
            return
        self.set_passed(_floor_message(self, "bandwidth_meets_min", "mbps", "MB/s"))


class StoragePerfIopsFloorCheck(BaseValidation):
    """Validate measured IOPS meets the configured floor (SPERF08-02).

    Config:
        step_output: The evaluate_storage_test step output

    Step output:
        tests: dict with iops_meets_min (measured_iops, min_iops)
    """

    description: ClassVar[str] = "Check measured IOPS meets the configured floor (0 = report only)"

    def run(self) -> None:
        """Validate IOPS against the floor, reporting the measured IOPS."""
        if not _require(self, "iops_meets_min", "IOPS below the configured floor"):
            return
        self.set_passed(_floor_message(self, "iops_meets_min", "iops", "IOPS"))
