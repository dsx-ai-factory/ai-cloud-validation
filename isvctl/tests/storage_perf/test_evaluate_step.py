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


@pytest.mark.parametrize("ops", ["NaN", "inf", None], ids=["nan", "inf", "null"])
def test_evaluate_fails_a_bandwidth_without_a_real_iops(ops: Any) -> None:
    """A real bandwidth with a non-finite or missing IOPS is no IOPS measurement: results_present fails."""
    doc = document()
    doc["executions"][0]["results"]["phases"][0]["elbencho_first"] = {"bytes_per_s": 1e8, "ops_per_s": ops}

    out = EVALUATE._evaluate(RUN_OK, doc, 0.0, 0.0)

    assert out["tests"]["results_present"] == {"passed": False, "error": "no write IOPS in results.json"}


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


def test_evaluate_reads_a_batch_document_from_its_reports_directory(
    perf: InProcess, launcher: FakeLauncher, tmp_path: Path
) -> None:
    """For a batch, the extractor writes reports/results.json; a stale one there is removed first."""
    batch_dir = tmp_path / "filesystem-batch-20260101Z000000"
    (batch_dir / EVALUATE.BATCH_REPORTS).mkdir(parents=True)
    (batch_dir / EVALUATE.BATCH_MANIFEST).write_text("")
    stale = batch_dir / EVALUATE.BATCH_REPORTS / "results.json"
    stale.write_text(json.dumps(document()))
    groups = document(
        execution(write=(10_000_000, 2_000), group_id="0001"), execution(write=(500_000_000, 400), group_id="0002")
    )

    def extractor(args: list[str]) -> Any:
        """Write the batch's document where storage-scale-test does, after checking the stale one is gone."""
        assert not stale.exists()
        stale.write_text(json.dumps(groups))
        return ran()

    launcher.extractor = extractor

    rc, out = evaluate_step(perf, launcher.base, batch_dir)

    assert rc == 0, out
    assert sorted(out["groups"]) == ["0001", "0002"]


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


def test_evaluate_reports_each_batch_group_and_floors_the_best() -> None:
    """A batch's groups run different workloads: each gets its own headline; floors use the best."""
    doc = document(
        execution(write=(10_000_000, 2_000), group_id="0001"),
        execution(write=(500_000_000, 400), read=(600_000_000, 500), group_id="0002"),
    )

    out = EVALUATE._evaluate(RUN_OK, doc, 0.0, 0.0)

    assert out["success"] is True
    assert out["groups"]["0001"]["write"] == {
        "samples": 1,
        "best_mbps": 10.0,
        "best_iops": 2000.0,
        "min_avg_latency_ms": 5.0,
    }
    assert out["groups"]["0002"]["read"]["best_mbps"] == 600.0
    assert out["headline"]["write"]["best_mbps"] == 500.0 and out["headline"]["write"]["best_iops"] == 2000.0
    assert_schema("evaluate_storage_test", out)


def test_evaluate_leaves_out_groups_for_a_single_sweep() -> None:
    """A plain sweep (one group) reports only its headline."""
    assert "groups" not in EVALUATE._evaluate(RUN_OK, document(execution(write=(1, 1), group_id="0001")), 0.0, 0.0)


def _two_groups() -> dict[str, Any]:
    """A batch document: a small-IO group (0001) and a large-IO group (0002)."""
    return document(
        execution(write=(10_000_000, 2_000), read=(20_000_000, 3_000), group_id="0001"),
        execution(write=(500_000_000, 400), read=(600_000_000, 500), group_id="0002"),
    )


def test_a_batch_summarises_each_group_not_a_mix_of_them() -> None:
    """results_present names each group's own numbers; a line across groups would pair one
    group's MB/s with another's IOPS."""
    out = EVALUATE._evaluate(RUN_OK, _two_groups(), 0.0, 0.0)

    assert out["tests"]["results_present"]["message"] == (
        "2 executions complete in 2 groups: "
        "group 0001: write 10.0 MB/s, 2000.0 IOPS, 5.0 ms avg latency; read 20.0 MB/s, 3000.0 IOPS, 4.0 ms avg latency"
        " | group 0002: write 500.0 MB/s, 400.0 IOPS, 5.0 ms avg latency; read 600.0 MB/s, 500.0 IOPS, 4.0 ms avg latency"
    )


def test_a_group_floor_judges_that_group_only() -> None:
    """Group floors from the run step: 0001 meets its IOPS floor; 0002 misses its bandwidth floor,
    though the sweep-wide numbers (0002's MB/s with 0001's IOPS) would meet both."""
    run = {
        **RUN_OK,
        "groups": [
            {"group_id": "0001", "io_sizes": "r4K", "min_iops": 1000.0},
            {"group_id": "0002", "io_sizes": "1M", "min_mbps": 550.0},
        ],
    }

    out = EVALUATE._evaluate(run, _two_groups(), 0.0, 0.0)

    assert out["success"] is False
    assert out["tests"]["bandwidth_meets_min"] == {
        "passed": False,
        "error": "group 0002: 500.0 MB/s < floor 550.0 MB/s",
    }
    assert out["tests"]["iops_meets_min"] == {
        "passed": True,
        "message": "group 0001: 2000.0 IOPS >= floor 1000.0 IOPS (write 2000.0 IOPS, read 3000.0 IOPS)",
    }
    assert_schema("evaluate_storage_test", out)


def test_a_group_floor_without_results_fails() -> None:
    """A group that set a floor but has no results (it never ran) cannot pass it."""
    run = {**RUN_OK, "groups": [{"group_id": "0001"}, {"group_id": "0002", "min_mbps": 1.0}]}
    doc = document(execution(write=(10_000_000, 2_000), group_id="0001"))

    out = EVALUATE._evaluate(run, doc, 0.0, 0.0)

    assert out["tests"]["bandwidth_meets_min"] == {"passed": False, "error": "group 0002: no measurement to compare"}


def test_evaluate_does_not_extract_a_run_that_was_never_submitted(
    perf: InProcess, launcher: FakeLauncher, tmp_path: Path
) -> None:
    """A run rejected before it ran (bad input) fails fast: its draft directory is not extracted."""

    def extractor(args: list[str]) -> Any:
        """Never called."""
        raise AssertionError(f"extracted {args}")

    launcher.extractor = extractor
    run = json.dumps({"success": False, "state": "NOT_SUBMITTED", "results_dir": str(tmp_path), "error": "bad"})

    rc, out = perf("evaluate_storage_test", f"--scale-test-base={launcher.base}", f"--run={run}")

    assert rc == 1
    assert out["tests"]["results_present"] == {
        "passed": False,
        "error": "nothing ran: the run step rejected the sweep before submitting it",
    }


def test_a_sweep_wide_floor_is_reported_with_the_group_floors() -> None:
    """With perf_min_iops set too, the IOPS check reports the sweep-wide verdict, then each group's."""
    run = {**RUN_OK, "groups": [{"group_id": "0001", "min_iops": 1000.0}, {"group_id": "0002"}]}

    out = EVALUATE._evaluate(run, _two_groups(), 0.0, 400.0)

    assert out["tests"]["iops_meets_min"]["message"] == (
        "2000.0 IOPS >= floor 400.0 IOPS (write 2000.0 IOPS, read 3000.0 IOPS); "
        "group 0001: 2000.0 IOPS >= floor 1000.0 IOPS (write 2000.0 IOPS, read 3000.0 IOPS)"
    )


def test_group_floors_judge_the_groups_that_ran_when_another_failed() -> None:
    """A batch that stopped at group 0002: results_present fails, but group 0001 still meets or
    misses its own floor, and 0002, which has no results, cannot pass its floor."""
    failed = {**execution(write=(1, 1), group_id="0002"), "ledger_state": "failed", "result_state": "not_run"}
    failed["results"] = None
    doc = document(execution(write=(10_000_000, 2_000), read=(20_000_000, 3_000), group_id="0001"), failed)
    run = {**RUN_OK, "groups": [{"group_id": "0001", "min_iops": 1000.0}, {"group_id": "0002", "min_mbps": 1.0}]}

    out = EVALUATE._evaluate(run, doc, 0.0, 0.0)

    assert out["tests"]["results_present"]["passed"] is False
    assert out["tests"]["iops_meets_min"] == {
        "passed": True,
        "message": "group 0001: 2000.0 IOPS >= floor 1000.0 IOPS (write 2000.0 IOPS, read 3000.0 IOPS)",
    }
    assert out["tests"]["bandwidth_meets_min"] == {"passed": False, "error": "group 0002: no measurement to compare"}
    assert out["groups"]["0001"]["write"]["best_iops"] == 2000.0


def test_a_one_group_batch_is_reported_per_group() -> None:
    """A batch of one group still reports its group, like a batch of several."""
    run = {**RUN_OK, "groups": [{"group_id": "0001", "io_sizes": "r4K"}]}

    out = EVALUATE._evaluate(run, document(execution(write=(10_000_000, 2_000), group_id="0001")), 0.0, 0.0)

    assert sorted(out["groups"]) == ["0001"]
    assert out["tests"]["results_present"]["message"].startswith("1 execution complete in 1 group: group 0001: ")
