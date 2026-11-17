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

"""sst: reading storage-scale-test's output, counting a sweep's cells, composing env.sh."""

from __future__ import annotations

import re

import pytest

from .conftest import SST, VENDORED, diag, ran, reply

# A --status reply and a failed one as storage-scale-test v2.1.0 prints them (paths shortened).
STATUS_SAMPLE = """ATTEMPT=0123abcd
STATE=RUNNING
EXECUTION_SCOPE=CURRENT_ATTEMPT
PROGRESS_SOURCE=PVC
EXECUTIONS_TOTAL=6
EXECUTIONS_PENDING=2
EXECUTIONS_RUNNING=1
EXECUTIONS_SUCCEEDED=3
EXECUTIONS_FAILED=0
RESULT_COLLECTION=PENDING
NEXT_ACTION=WAIT
"""

STATUS_FAILURE_SAMPLE = """Warning: transient Kubernetes observation failed (attempt 1/3); retrying
Unable to connect to the server: context deadline exceeded (Client.Timeout exceeded while awaiting headers)
STORAGE_SCALE_TEST_DIAGNOSTIC_OPERATION=kubectl-observe
STORAGE_SCALE_TEST_DIAGNOSTIC_PHASE=api-observation
STORAGE_SCALE_TEST_DIAGNOSTIC_REASON=TIMEOUT
STORAGE_SCALE_TEST_DIAGNOSTIC_MAY_STILL_BE_RUNNING=unknown
STORAGE_SCALE_TEST_DIAGNOSTIC_SAFE_NEXT_ACTION=retry\\ the\\ same\\ lifecycle\\ command
Error: kubectl could not reach the API server using the active context
STORAGE_SCALE_TEST_DIAGNOSTIC_OPERATION=verify-cluster
STORAGE_SCALE_TEST_DIAGNOSTIC_PHASE=cluster-identity
STORAGE_SCALE_TEST_DIAGNOSTIC_LOCAL_STATE=SUBMITTED
STORAGE_SCALE_TEST_DIAGNOSTIC_REASON=TIMEOUT
STORAGE_SCALE_TEST_DIAGNOSTIC_RESOURCE_KIND=''
STORAGE_SCALE_TEST_DIAGNOSTIC_LOCAL_STATE_PATH=/results/elbencho-x/kubernetes/attempts/0123abcd/state.sh
STORAGE_SCALE_TEST_DIAGNOSTIC_MAY_STILL_BE_RUNNING=unknown
STORAGE_SCALE_TEST_DIAGNOSTIC_PATH=''
STORAGE_SCALE_TEST_DIAGNOSTIC_SAFE_NEXT_ACTION=retry\\ the\\ same\\ lifecycle\\ command
"""

INCOMPLETE_CANCEL = reply("STATE=CANCEL_REQUESTED NEXT_ACTION=INSPECT")

# --- --status replies ---


@pytest.mark.parametrize(
    ("kind", "result"),
    [
        pytest.param("ok", ran(0, reply("STATE=RUNNING NEXT_ACTION=WAIT")), id="state"),
        pytest.param("ok", ran(1, INCOMPLETE_CANCEL, diag("CANCELLATION_INCOMPLETE")), id="incomplete-cancel"),
        pytest.param("unreachable", ran(124, stderr="timed out after 300s", timed_out=True), id="our-timeout"),
        *(
            pytest.param("unreachable", ran(1, stderr=diag(reason)), id=f"access-{reason}")
            for reason in sorted(SST.REASONS_ACCESS)
        ),
        # The same shape as an incomplete cancel, but no state can be trusted.
        pytest.param("failed", ran(1, INCOMPLETE_CANCEL, diag("LEDGER_INCONSISTENT")), id="ledger-inconsistent"),
        pytest.param("failed", ran(1, stderr=f"Error: {SST.LOCK_BUSY} for /r/kubernetes\n"), id="lock-busy"),
        pytest.param("failed", ran(1, stderr=diag("POD_UNSCHEDULABLE")), id="helper-pod"),
        pytest.param("failed", ran(0, reply("STATE=RUNNING EXECUTIONS_TOTAL=2")), id="no-next-action"),
        pytest.param("failed", ran(1), id="no-reason"),
    ],
)
def test_status_replies_are_classified(kind: str, result: object) -> None:
    """Only a reply that carries the sweep's state is ok; access failures are unreachable."""
    assert SST.classify(result)[0] == kind


def test_a_captured_v2_1_status_is_read() -> None:
    """Real replies: the state and counts from a good one; the class, reason and summary of a failed one."""
    kind, fields = SST.classify(ran(0, STATUS_SAMPLE))
    assert kind == "ok"
    assert (fields["NEXT_ACTION"], fields["EXECUTIONS_SUCCEEDED"], fields["EXECUTIONS_TOTAL"]) == ("WAIT", "3", "6")

    kind, fields = SST.classify(ran(1, stderr=STATUS_FAILURE_SAMPLE))
    assert kind == "unreachable"
    assert (fields["DIAGNOSTIC_REASON"], fields["DIAGNOSTIC_LOCAL_STATE"]) == ("TIMEOUT", "SUBMITTED")
    assert SST.describe_failure(STATUS_FAILURE_SAMPLE) == (
        "kubectl could not reach the API server using the active context (TIMEOUT, phase cluster-identity); "
        "may still be running: unknown; next: retry the same lifecycle command"
    )


def test_describe_failure_says_what_to_do_next() -> None:
    """storage-scale-test's next safe action, running risk and evidence path are passed on."""
    stderr = (
        "Error: cancellation is incomplete\n"
        "STORAGE_SCALE_TEST_DIAGNOSTIC_REASON=CANCELLATION_INCOMPLETE\n"
        "STORAGE_SCALE_TEST_DIAGNOSTIC_MAY_STILL_BE_RUNNING=unknown\n"
        "STORAGE_SCALE_TEST_DIAGNOSTIC_PATH=/r/kubernetes/diagnostics/status-1\n"
        "STORAGE_SCALE_TEST_DIAGNOSTIC_SAFE_NEXT_ACTION=retry\\ --cancel\\ for\\ the\\ same\\ results\\ directory\n"
    )

    assert SST.describe_failure(stderr) == (
        "cancellation is incomplete (CANCELLATION_INCOMPLETE); may still be running: unknown; "
        "next: retry --cancel for the same results directory; diagnostics: /r/kubernetes/diagnostics/status-1"
    )


def test_parse_kv_unquotes_shell_quoted_diagnostics() -> None:
    """storage-scale-test writes diagnostic values and FAILED_LOG with printf %q."""
    fields = SST.parse_kv(
        "STATE=RUNNING\nSTORAGE_SCALE_TEST_DIAGNOSTIC_ACTION=retry\\ --cancel\nFAILED_LOG=/r/a\\ b.log\n"
    )

    assert fields == {
        "STATE": "RUNNING",
        "STORAGE_SCALE_TEST_DIAGNOSTIC_ACTION": "retry --cancel",
        "FAILED_LOG": "/r/a b.log",
    }


def test_status_reasons_cover_the_vendored_vocabulary() -> None:
    """The reasons the steps act on are ones the pinned storage-scale-test reports."""
    functions = VENDORED / "storage-tests" / "fs" / "kubectl" / "_nv-elbencho-kubectl-functions.sh"
    if not functions.is_file():
        pytest.skip("storage-scale-test is not vendored; run `make vendor`")
    text = functions.read_text(encoding="utf-8")
    body = text[text.index("kubectl_report_lifecycle_error() {") :]
    match = re.search(r'"\$reason" =~ \^\(([A-Z_|]+)\)\$', body[: body.index("\n}\n")])
    assert match, "the reason check moved; update the reasons in sst.py"

    assert {*SST.REASONS_ACCESS, SST.CANCELLATION_INCOMPLETE} <= set(match.group(1).split("|"))


# --- Counting a sweep's cells ---


@pytest.mark.parametrize(
    ("spec", "counts"),
    [
        ("1", 1),
        ("1-4", 4),
        ("1-10+4", 4),  # 1, 5, 9, 10
        ("3-10+2,13", 6),  # 3, 5, 7, 9, 10, 13
        ("16,8,4,2,1", 5),
        ("1-1000000000000", 1_000_000_000_000),  # counted, never listed
        ("0", 0),  # storage-scale-test rejects the specs this cannot read
        ("2-1", 0),
        ("1,,2", 0),
        ("all", 0),
    ],
    ids=["1", "1-4", "1-10+4", "3-10+2,13", "list", "huge", "0", "2-1", "empty-entry", "all"],
)
def test_node_spec_counts_cells(spec: str, counts: int) -> None:
    """The node spec reads as storage-scale-test reads it: a range includes its start and its end.

    Cells multiply by IO sizes, threads and depths.
    """
    assert SST.count_node_counts(spec) == counts
    assert SST.count_cells(spec, ["r4K", "1M"], ["1", "2"], ["1"]) == (None if counts is None else 4 * counts)


# --- env.sh ---


@pytest.mark.parametrize("anchors", [0, 2])
def test_compose_env_sh_needs_exactly_one_overrides_anchor(anchors: int) -> None:
    """Like storage-scale-test's own CI, refuse a template without exactly one anchor."""
    template = "TEST_DIRS=()\n" + f"{SST.OVERRIDES_ANCHOR}\n" * anchors

    with pytest.raises(ValueError, match="not supported"):
        SST.compose_env_sh(template, ["export EXECUTION_SUBSTRATE=kubectl"])


def test_compose_env_sh_matches_the_vendored_template() -> None:
    """The pinned release's env.sh.template has the anchor isvctl inserts at, before the defaults load."""
    template = VENDORED / "env.sh.template"
    if not template.is_file():
        pytest.skip("storage-scale-test is not vendored; run `make vendor`")

    out = SST.compose_env_sh(template.read_text(encoding="utf-8"), ["export EXECUTION_SUBSTRATE=kubectl"])

    assert out.index("export EXECUTION_SUBSTRATE=kubectl") < out.index('source "${SCALE_TEST_BASE}/lib/env_base.sh"')
