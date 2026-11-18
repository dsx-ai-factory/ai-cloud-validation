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

"""evaluate_storage_test: the extractor's results.json, judged against optional floors."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from .conftest import (
    EVALUATE,
    RUN_OK,
    SST,
    FakeLauncher,
    InProcess,
    assert_schema,
    document,
    evaluate_step,
    execution,
    ran,
)

# --- Judging a document ---


def test_evaluate_reports_a_floor_of_zero_with_the_numbers() -> None:
    """A floor of 0 is no floor: its test passes and says what was measured."""
    out = EVALUATE._evaluate(RUN_OK, document(), 0.0, 0.0)

    assert out["success"] is True
    assert out["headline"]["write"]["best_mbps"] == 104.9
    assert out["headline"]["read"]["min_avg_latency_ms"] == 4.0
    assert out["tests"]["results_present"] == {
        "passed": True,
        "message": "1 execution complete: write 104.9 MB/s, 200.0 IOPS, 5.0 ms avg latency; "
        "read 157.3 MB/s, 300.0 IOPS, 4.0 ms avg latency",
    }
    assert out["tests"]["bandwidth_meets_min"] == {
        "passed": True,
        "message": "no floor set (perf_min_mbps); 104.9 MB/s measured (write 104.9 MB/s, read 157.3 MB/s)",
    }
    assert out["tests"]["iops_meets_min"]["message"] == (
        "no floor set (perf_min_iops); 200.0 IOPS measured (write 200.0 IOPS, read 300.0 IOPS)"
    )
    assert_schema("evaluate_storage_test", out)


@pytest.mark.parametrize(
    ("run", "expected"),
    [
        (RUN_OK, {"passed": True, "message": "SUCCESS, 2/2 executions"}),
        (
            {**RUN_OK, "success": False, "state": "TIMEOUT", "error": "sweep did not finish within 900s"},
            {"passed": False, "error": "run step ended TIMEOUT, 2/2 executions: sweep did not finish within 900s"},
        ),
        ({}, {"passed": False, "error": "run step ended no state: the run step reported no result"}),
    ],
    ids=["success", "timeout", "no-run"],
)
def test_evaluate_reports_whether_the_run_completed_the_sweep(run: dict[str, Any], expected: dict[str, Any]) -> None:
    """sweep_completed passes only for a run step that succeeded with state SUCCESS, and fails the step otherwise."""
    out = EVALUATE._evaluate(run, document(), 0.0, 0.0)

    assert out["tests"]["sweep_completed"] == expected
    assert out["success"] is expected["passed"]


def test_evaluate_enforces_a_configured_floor() -> None:
    """A floor above the slower operation fails the bandwidth test only."""
    out = EVALUATE._evaluate(RUN_OK, document(), 120.0, 150.0)

    assert out["success"] is False
    assert out["tests"]["bandwidth_meets_min"] == {"passed": False, "error": "104.9 MB/s < floor 120.0 MB/s"}
    assert out["tests"]["iops_meets_min"] == {
        "passed": True,
        "message": "200.0 IOPS >= floor 150.0 IOPS (write 200.0 IOPS, read 300.0 IOPS)",
    }


def test_evaluate_compares_floors_before_rounding() -> None:
    """Floors are compared with the measured rates, not the rounded headline.

    99.96 MB/s is below a 100 MB/s floor though it shows as 100.0; 100.04 IOPS meets a floor of
    100.01 though it shows as 100.0. A failure shows enough digits to say why.
    """
    doc = document(execution(write=(99.96e6, 100.04), read=(900e6, 900)))

    out = EVALUATE._evaluate(RUN_OK, doc, 100.0, 100.01)

    assert out["headline"]["write"]["best_mbps"] == 100.0 and out["headline"]["write"]["best_iops"] == 100.0
    assert out["tests"]["bandwidth_meets_min"]["error"] == "99.96 MB/s < floor 100.0 MB/s"
    assert out["tests"]["iops_meets_min"]["message"].startswith("100.04 IOPS >= floor 100.01 IOPS (")


def test_evaluate_counts_a_rate_that_rounds_to_zero_as_a_result() -> None:
    """0.04 MB/s is a measured result: the headline shows 0.0, and results_present still passes."""
    out = EVALUATE._evaluate(RUN_OK, document(execution(write=(0.04e6, 10))), 0.0, 0.0)

    assert out["headline"]["write"]["best_mbps"] == 0.0
    assert out["tests"]["results_present"]["passed"] is True and out["success"] is True


@pytest.mark.parametrize(
    ("doc", "error"),
    [
        pytest.param(
            document(
                execution(
                    write=(1e6, 1),
                    execution_id="0002",
                    result_state="partial",
                    problems=[{"code": "phase_missing", "detail": "no read", "phase": "read"}],
                )
            ),
            "0002: ledger success, results partial (phase_missing)",
            id="incomplete-results",
        ),
        pytest.param(
            document(execution(write=(1e6, 1), execution_id="0002", ledger_state="failed")),
            "0002: ledger failed, results complete",
            id="execution-failed",
        ),
        pytest.param(
            {**document(), "batch_outcome": {"complete": False}},
            "batch_outcome.complete is not true",
            id="batch-outcome-wins",
        ),
    ],
)
def test_evaluate_fails_results_that_are_not_complete(doc: dict[str, Any], error: str) -> None:
    """results_present fails, with the reason, for incomplete or failed executions; storage-scale-test's
    own batch_outcome wins over the executions."""
    out = EVALUATE._evaluate(RUN_OK, doc, 0.0, 0.0)

    assert out["tests"]["results_present"]["passed"] is False
    assert error in out["tests"]["results_present"]["error"]


def test_evaluate_fails_when_an_operation_has_no_results() -> None:
    """Missing read results fail results_present, and the floors cannot pass vacuously."""
    doc = document()
    doc["executions"][0]["results"]["phases"].pop()

    out = EVALUATE._evaluate(RUN_OK, doc, 0.0, 0.0)

    assert out["tests"]["results_present"] == {"passed": False, "error": "no read results in results.json"}
    assert out["tests"]["bandwidth_meets_min"]["passed"] is False


def test_evaluate_treats_non_finite_rates_as_invalid() -> None:
    """A rate of inf or NaN is not a measurement: results_present fails and the output is strict JSON."""
    doc = document()
    doc["executions"][0]["results"]["phases"][0]["elbencho_first"] = {"bytes_per_s": "inf", "ops_per_s": "NaN"}

    out = EVALUATE._evaluate(RUN_OK, doc, 0.0, 0.0)

    assert out["tests"]["results_present"]["passed"] is False
    json.loads(json.dumps(out), parse_constant=lambda name: pytest.fail(f"non-standard JSON constant {name}"))


# --- Arguments ---


def test_an_empty_floor_means_no_floor() -> None:
    """An empty perf_min_mbps or perf_min_iops means no floor, like 0."""
    args = EVALUATE._parse_args(["--scale-test-base=/x", "--min-mbps=", "--min-iops="])

    assert (args.min_mbps, args.min_iops) == (0.0, 0.0)


def test_a_non_finite_floor_is_refused() -> None:
    """A floor no rate can be compared with is a usage error, not a pass or NaN in the output."""
    with pytest.raises(SystemExit):
        EVALUATE._parse_args(["--scale-test-base=/x", "--min-mbps=nan"])


@pytest.mark.parametrize(
    ("run", "error"),
    [("not json", "--run is not JSON: "), ("[]", "--run is not a JSON object")],
    ids=["not-json", "not-an-object"],
)
def test_evaluate_refuses_a_run_argument_it_cannot_read(run: str, error: str, perf: InProcess) -> None:
    """--run must be the run step's JSON object; anything else is a JSON error, not a traceback."""
    rc, out = perf("evaluate_storage_test", "--scale-test-base=/x", f"--run={run}")

    assert rc == 1 and out["error"].startswith(error)
    assert_schema("evaluate_storage_test", out)


# --- The extractor ---


def test_evaluate_reads_the_document_the_extractor_writes(
    perf: InProcess, launcher: FakeLauncher, tmp_path: Path
) -> None:
    """The step runs the extractor with --to-json and judges the document, not its exit status."""
    seen: list[list[str]] = []

    def extractor(args: list[str]) -> Any:
        """Record the call and write a complete document, then exit 1 as the extractor may."""
        seen.append(args)
        (Path(args[0]) / "results.json").write_text(json.dumps(document()))
        return ran(1)

    launcher.extractor = extractor

    rc, out = evaluate_step(perf, launcher.base, tmp_path)

    assert rc == 0 and out["success"] is True
    assert seen == [[str(tmp_path), "--to-json", "--live-detail", "none"]]


@pytest.mark.parametrize(
    ("written", "error"),
    [
        pytest.param(None, "extractor exited 1 without writing", id="nothing-written"),
        pytest.param("{not json", "is not JSON", id="not-json"),
        pytest.param(
            json.dumps({**document(), "schema": "other"}),
            f"is not a {EVALUATE.RESULTS_SCHEMA} document",
            id="wrong-schema",
        ),
        pytest.param(
            json.dumps({**document(), "schema_version": "2.0"}),
            "has unsupported schema_version 2.0",
            id="unsupported-version",
        ),
    ],
)
def test_evaluate_rejects_a_document_it_cannot_trust(
    written: str | None, error: str, perf: InProcess, launcher: FakeLauncher, tmp_path: Path
) -> None:
    """A missing, unreadable or unfamiliar results.json fails results_present, saying why.

    A results.json left by an earlier extraction is removed first, so it is never mistaken for
    this run's.
    """
    (tmp_path / "results.json").write_text(json.dumps(document()))

    def extractor(args: list[str]) -> Any:
        """Write *written* (if any) as the document, and exit 1."""
        if written is not None:
            (Path(args[0]) / "results.json").write_text(written)
        return ran(1)

    launcher.extractor = extractor

    rc, out = evaluate_step(perf, launcher.base, tmp_path)

    assert rc == 1
    assert error in out["tests"]["results_present"]["error"]
    assert_schema("evaluate_storage_test", out)


def test_evaluate_names_a_release_without_to_json(perf: InProcess, launcher: FakeLauncher, tmp_path: Path) -> None:
    """An extractor that predates --to-json fails with a clear reason."""
    launcher.extractor = lambda args: ran(2, stderr="extract-elbencho.py: error: unrecognized arguments: --to-json\n")

    _, out = evaluate_step(perf, launcher.base, tmp_path)

    assert "v2.1.0 or later" in out["tests"]["results_present"]["error"]


@pytest.mark.parametrize(
    ("cells", "extract_timeout", "expected"),
    [(None, "0", 1800), (8, "0", 1800), (5624, "0", 11248), (5624, "30", 30)],
    ids=["no-counts", "8-cells", "5624-cells", "explicit"],
)
def test_evaluate_scales_its_extract_timeout_with_the_cells(
    cells: int | None, extract_timeout: str, expected: int, perf: InProcess, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The extractor gets 2 s per cell, at least 1800 s, unless a timeout is given; a timeout says
    how to rerun it offline."""
    timeouts: list[float] = []
    monkeypatch.setattr(SST, "run", lambda cmd, timeout, env=None: timeouts.append(timeout) or ran(124, timed_out=True))
    run = {**RUN_OK, "results_dir": "/r/elbencho-x"}
    if cells is None:
        del run["executions"]
    else:
        run["executions"] = {"total": cells}

    rc, out = perf(
        "evaluate_storage_test",
        "--scale-test-base=/sst",
        f"--extract-timeout={extract_timeout}",
        f"--run={json.dumps(run)}",
    )

    assert rc == 1 and timeouts == [expected]
    assert out["tests"]["results_present"]["error"] == (
        f"extractor timed out after {expected}s; rerun it offline: /sst/utils/extract-elbencho.py /r/elbencho-x --to-json --live-detail none"
    )
