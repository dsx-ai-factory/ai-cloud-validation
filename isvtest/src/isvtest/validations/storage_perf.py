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

"""Storage performance validation for the storage-scale-test sweep.

A provider-neutral assertion over ``evaluate_storage_test``'s output: its ``tests``
results and its ``headline`` (per operation: best MB/s, best IOPS, lowest average
latency). One sweep is one test: ``StoragePerfSweepCheck`` (HSS02-02 in
``docs/test-plan.yaml``) reports each stage as a subtest, with the measured numbers in
its message, so a run passing with report-only floors still shows them:

    - sweep_completed       the run step finished the sweep with state SUCCESS
    - results_present       complete write and read results
    - bandwidth_meets_min   bandwidth meets the configured floor (0 = report only)
    - iops_meets_min        IOPS meets the configured floor (0 = report only)

Releasing the sweep is checked separately (HSS07-04, on the teardown step), since it
happens in a later phase.
"""

from __future__ import annotations

from typing import Any, ClassVar

from isvtest.core.validation import BaseValidation

SUBTESTS = ("sweep_completed", "results_present", "bandwidth_meets_min", "iops_meets_min")


def _step_output(validation: BaseValidation) -> dict[str, Any]:
    """Return the evaluate step's output."""
    return validation.config.get("step_output", {})


def _headline(validation: BaseValidation, field: str, unit: str) -> str:
    """Format the step's headline as ``write 7123.0 MB/s, read 7628.4 MB/s``, or ``""``."""
    headline = _step_output(validation).get("headline") or {}
    parts = [f"{op} {numbers[field]} {unit}" for op, numbers in headline.items() if field in numbers]
    return ", ".join(parts)


def _results_message(validation: BaseValidation) -> str:
    """Format complete results as each operation's best bandwidth, IOPS and latency."""
    headline = _step_output(validation).get("headline") or {}
    summary = "; ".join(
        f"{op}: {n.get('best_mbps')} MB/s, {n.get('best_iops')} IOPS, {n.get('min_avg_latency_ms')} ms avg latency"
        for op, n in headline.items()
    )
    return f"Complete results ({summary})" if summary else "Complete results"


def _floor_message(validation: BaseValidation, test_name: str, field: str, unit: str) -> str:
    """Format the measured value against its floor, plus the per-operation figures."""
    test = (_step_output(validation).get("tests") or {}).get(test_name, {})
    floor = test.get(f"min_{field}", 0)
    verdict = "report only, no floor set" if not floor else f">= floor {floor} {unit}"
    per_op = _headline(validation, f"best_{field}", unit)
    return f"{test.get(f'measured_{field}', 'N/A')} {unit} ({verdict})" + (f"; {per_op}" if per_op else "")


class StoragePerfSweepCheck(BaseValidation):
    """Validate one storage-scale-test sweep, one subtest per stage (HSS02-02).

    Config:
        step_output: The evaluate_storage_test step output

    Step output:
        tests: dict with sweep_completed, results_present, bandwidth_meets_min
               (measured_mbps, min_mbps) and iops_meets_min (measured_iops, min_iops)
        headline: per operation samples, best_mbps, best_iops, min_avg_latency_ms
    """

    description: ClassVar[str] = (
        "Check the sweep completed with real write and read results that meet the floors (0 = report only)"
    )

    def run(self) -> None:
        """Report each stage as a subtest; pass only if all of them passed."""
        tests = _step_output(self).get("tests")
        if not tests:
            self.set_failed("No 'tests' in step output")
            return
        messages = {
            "sweep_completed": (tests.get("sweep_completed") or {}).get("message", ""),
            "results_present": _results_message(self),
            "bandwidth_meets_min": _floor_message(self, "bandwidth_meets_min", "mbps", "MB/s"),
            "iops_meets_min": _floor_message(self, "iops_meets_min", "iops", "IOPS"),
        }
        failed = []
        for name in SUBTESTS:
            result = tests.get(name) or {}
            if result.get("passed"):
                self.report_subtest(name, True, messages[name])
            else:
                error = result.get("error", "test not found")
                failed.append(f"{name}: {error}")
                self.report_subtest(name, False, error)
        if failed:
            self.set_failed("Storage performance sweep failed: " + "; ".join(failed))
            return
        self.set_passed(f"Sweep completed; {messages['results_present']}")
