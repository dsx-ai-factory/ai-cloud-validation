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

"""Tests for subtest -> JUnit XML injection."""

import xml.etree.ElementTree as ET
from pathlib import Path
from types import SimpleNamespace
from typing import cast

from isvtest.testing.subtests import (
    SubTestReport,
    _inject_pass_messages_into_junit,
    _inject_subtests_into_junit,
)


def _parent_junit(tmp_path: Path, parent_name: str, *, reported_tests: int = 1) -> Path:
    """Write a minimal pytest-style JUnit with a single parent testcase."""
    suite = ET.Element(
        "testsuite",
        attrib={"name": "phase", "tests": str(reported_tests), "skipped": "0", "failures": "0"},
    )
    ET.SubElement(suite, "testcase", attrib={"name": parent_name, "classname": "", "time": "0.000"})
    path = tmp_path / "junit.xml"
    ET.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)
    return path


def _stub_report(
    parent_nodeid: str,
    subtest_msg: str,
    *,
    skipped: bool = False,
    failed: bool = False,
    longrepr: object | None = None,
    duration: float = 0.0,
    message: str | None = None,
) -> SimpleNamespace:
    """Build the minimum surface that ``_inject_subtests_into_junit`` reads.

    The real ``SubTestReport`` is a pytest TestReport subclass that's awkward
    to construct in isolation; the injector only touches a handful of fields.
    """
    return SimpleNamespace(
        nodeid=parent_nodeid,
        duration=duration,
        failed=failed,
        skipped=skipped,
        longrepr=longrepr,
        context=SimpleNamespace(msg=subtest_msg, message=message),
    )


def test_skipped_subtest_uses_real_message_from_longrepr(tmp_path: Path) -> None:
    """The injector pulls the human reason out of longrepr, not a canned line."""
    junit = _parent_junit(tmp_path, "K8sCsiTenantScopedCredentialsCheck")
    real_message = "No CSIDriver objects present"
    report = _stub_report(
        "::K8sCsiTenantScopedCredentialsCheck",
        "serviceaccount-rbac-scoped",
        skipped=True,
        longrepr=("/some/file.py", 0, real_message),
    )

    _inject_subtests_into_junit(junit, cast(list[SubTestReport], [report]))

    cases = list(ET.parse(junit).iter("testcase"))
    subtest_case = next(c for c in cases if "::serviceaccount-rbac-scoped" in (c.get("name") or ""))
    skipped = subtest_case.find("skipped")
    assert skipped is not None
    assert skipped.get("message") == real_message


def test_skipped_subtest_falls_back_when_longrepr_is_missing(tmp_path: Path) -> None:
    """No longrepr -> fall back to the canned 'Subtest X skipped' string."""
    junit = _parent_junit(tmp_path, "ParentCheck")
    report = _stub_report(
        "::ParentCheck",
        "noisy-subtest",
        skipped=True,
        longrepr=None,
    )

    _inject_subtests_into_junit(junit, cast(list[SubTestReport], [report]))

    subtest_case = next(c for c in ET.parse(junit).iter("testcase") if "::noisy-subtest" in (c.get("name") or ""))
    skipped = subtest_case.find("skipped")
    assert skipped is not None
    assert skipped.get("message") == "Subtest noisy-subtest skipped"


def test_injected_subtests_reconcile_precounted_junit_totals(tmp_path: Path) -> None:
    """pytest's pre-counted subtest report must not be counted again after injection."""
    junit = _parent_junit(tmp_path, "ParentCheck", reported_tests=2)
    report = _stub_report("::ParentCheck", "probe", failed=True, longrepr="probe failed")

    _inject_subtests_into_junit(junit, cast(list[SubTestReport], [report]))

    suite = ET.parse(junit).getroot()
    assert len(suite.findall("testcase")) == 2
    assert suite.get("tests") == "2"
    assert suite.get("failures") == "1"
    assert suite.get("errors") == "0"
    assert suite.get("skipped") == "0"


def _case(junit: Path, name: str) -> ET.Element:
    """The testcase named *name* in *junit*."""
    return next(c for c in ET.parse(junit).iter("testcase") if c.get("name") == name)


def test_passed_subtest_message_goes_to_system_out(tmp_path: Path) -> None:
    """A passed subtest's message (e.g. its measured numbers) is kept as <system-out>."""
    junit = _parent_junit(tmp_path, "ParentCheck")
    report = _stub_report("::ParentCheck", "bandwidth", message="190.8 MB/s >= floor 100.0 MB/s")

    _inject_subtests_into_junit(junit, cast(list[SubTestReport], [report]))

    case = _case(junit, "ParentCheck::bandwidth")
    system_out = case.find("system-out")
    assert system_out is not None and system_out.text == "190.8 MB/s >= floor 100.0 MB/s"
    assert ET.parse(junit).getroot().get("tests") == "2"


def test_passed_subtest_without_message_has_no_system_out(tmp_path: Path) -> None:
    """No message, no element: the passed subtest stays an empty testcase."""
    junit = _parent_junit(tmp_path, "ParentCheck")

    _inject_subtests_into_junit(junit, cast(list[SubTestReport], [_stub_report("::ParentCheck", "probe")]))

    assert list(_case(junit, "ParentCheck::probe")) == []


def test_failed_and_skipped_subtests_do_not_get_system_out(tmp_path: Path) -> None:
    """A failed or skipped subtest reports as before: its message is in <failure>/<skipped> only."""
    junit = _parent_junit(tmp_path, "ParentCheck")
    reports = [
        _stub_report("::ParentCheck", "broken", failed=True, longrepr="probe failed", message="probe failed"),
        _stub_report("::ParentCheck", "off", skipped=True, longrepr=("f", 0, "no floor"), message="no floor"),
    ]

    _inject_subtests_into_junit(junit, cast(list[SubTestReport], reports))

    assert [child.tag for child in _case(junit, "ParentCheck::broken")] == ["failure"]
    assert [child.tag for child in _case(junit, "ParentCheck::off")] == ["skipped"]


def test_parent_pass_message_goes_to_system_out(tmp_path: Path) -> None:
    """A passing check's message reaches its (normalized) testcase as <system-out>."""
    junit = _parent_junit(tmp_path, "StoragePerfSweepCheck")

    _inject_pass_messages_into_junit(
        junit, {"tests/test_validations.py::test_validation[StoragePerfSweepCheck]": "Sweep completed"}
    )

    system_out = _case(junit, "StoragePerfSweepCheck").find("system-out")
    assert system_out is not None and system_out.text == "Sweep completed"


def test_parent_pass_message_skips_a_testcase_that_did_not_pass(tmp_path: Path) -> None:
    """A testcase that failed or was skipped keeps only its own outcome element."""
    junit = _parent_junit(tmp_path, "ParentCheck")
    tree = ET.parse(junit)
    ET.SubElement(next(tree.iter("testcase")), "skipped", attrib={"message": "skipped"})
    tree.write(junit, encoding="utf-8", xml_declaration=True)

    _inject_pass_messages_into_junit(junit, {"::test_validation[ParentCheck]": "should not appear"})

    assert [child.tag for child in _case(junit, "ParentCheck")] == ["skipped"]
