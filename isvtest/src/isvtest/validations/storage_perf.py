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

A provider-neutral assertion over ``evaluate_storage_test``'s output. One sweep is one
test: ``StoragePerfSweepCheck`` (HSS02-02 in ``docs/test-plan.yaml``) reports each entry
of the step's ``tests`` as a subtest, with the step's message, which carries the measured
numbers:

    - sweep_completed       the run step finished the sweep with state SUCCESS
    - results_present       complete write and read results
    - bandwidth_meets_min   bandwidth meets the configured floor (reports only with no floor)
    - iops_meets_min        IOPS meets the configured floor (reports only with no floor)

A floor of 0 is no floor: its subtest passes and only reports the measured numbers.
Releasing the sweep is teardown, checked on the release step's own output.
"""

from __future__ import annotations

from typing import ClassVar

from isvtest.core.validation import BaseValidation, check_required_tests

SUBTESTS = ("sweep_completed", "results_present", "bandwidth_meets_min", "iops_meets_min")


class StoragePerfSweepCheck(BaseValidation):
    """Validate one storage-scale-test sweep, one subtest per stage (HSS02-02).

    Config:
        step_output: The evaluate_storage_test step output

    Step output:
        tests: sweep_completed, results_present, bandwidth_meets_min and iops_meets_min, each
               with passed and a message, or an error
    """

    description: ClassVar[str] = "Check the sweep completed with real write and read results that meet the floors"

    def run(self) -> None:
        """Report each stage as a subtest; pass only if all of them passed."""
        if not check_required_tests(self, list(SUBTESTS), "Storage performance sweep failed"):
            return
        results = self.config["step_output"]["tests"]["results_present"].get("message", "")
        self.set_passed(f"Sweep completed; {results}")
