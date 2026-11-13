#!/usr/bin/env python3
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

"""Performance fixture teardown: remove the claim provision_perf_storage created.

Provider-agnostic template - replace the TODO block with your platform's implementation.
Called in the teardown phase of config/overlays/storage-perf.yaml, after release_storage_test
has released the sweep's hold on the claim and before delete_perf_filesystem. Delete the PVC
(and any static PV) provision_perf_storage created; if it reused an existing claim
("created": false), succeed without deleting anything.

A teardown run on its own (`--phase teardown`) has no setup outputs, so --claim is empty;
--record then names the file provision_perf_storage wrote, and that file is removed once
teardown is done with it. The record also names the cluster the claim was created on: if
--kubeconfig (or, without one, the recorded kubeconfig) now points at another context or API
server, nothing is deleted, the step fails and the record is kept.

Required JSON output fields:
  success            (bool) - true once the claim is gone (or was left in place on purpose)
  platform           (str)  - "storage"
  test_name          (str)  - "deprovision_perf_storage"
  deleted            (bool) - true if this step deleted it
  resources_deleted  (list) - identifiers of what this step deleted
  message            (str)  - human-readable summary

With --skip-destroy (STORAGE_SKIP_TEARDOWN=true) nothing is deleted and the record is kept,
so a later teardown can still remove it.

Usage:
    python deprovision_perf_storage.py --kubeconfig <path> --namespace storage-perf \\
        --claim perf-claim [--created] [--record <path>] [--skip-destroy]
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

# Allow importing provider-local helpers from scripts/common/.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.k8s_claim import other_cluster, read_record

# ISVCTL_DEMO_MODE=1 enables demo-success output (used by `make demo-test`).
DEMO_MODE = os.environ.get("ISVCTL_DEMO_MODE") == "1"


def main() -> int:
    """Delete the performance claim if this run created it; print JSON."""
    parser = argparse.ArgumentParser(description="Performance fixture teardown: remove the claim")
    parser.add_argument("--kubeconfig", default="", help="Kubeconfig emitted by setup_cluster")
    parser.add_argument("--namespace", default="", help="Namespace of the claim")
    parser.add_argument("--claim", default="", help="Claim provision_perf_storage reported")
    parser.add_argument("--created", action="store_true", help="provision_perf_storage created the claim")
    parser.add_argument("--record", default="", help="File provision_perf_storage recorded its result in")
    parser.add_argument("--skip-destroy", action="store_true", help="Keep it (STORAGE_SKIP_TEARDOWN=true)")
    args = parser.parse_args()

    record = Path(args.record) if args.record else None
    _, recorded = read_record(args.record)
    if not args.claim and recorded:
        args.namespace, args.claim = recorded.get("namespace", ""), recorded.get("claim_name", "")
        args.created = bool(recorded.get("created"))
    kubeconfig = args.kubeconfig or recorded.get("kubeconfig") or ""

    result: dict[str, Any] = {
        "success": False,
        "platform": "storage",
        "test_name": "deprovision_perf_storage",
        "deleted": False,
        "resources_deleted": [],
        "message": "",
    }

    if args.created and args.skip_destroy:
        result.update({"success": True, "message": f"Claim {args.namespace}/{args.claim} preserved (--skip-destroy)"})
        print(json.dumps(result, indent=2))
        return 0
    if not args.created:
        result.update({"success": True, "message": "Claim was not created by this run; left in place"})
    elif mismatch := other_cluster(recorded.get("cluster"), kubeconfig):
        result["error"] = mismatch
    else:
        # ╔══════════════════════════════════════════════════════════════════╗
        # ║  TODO: Replace this block with your platform's implementation     ║
        # ║                                                                   ║
        # ║  1. Delete PVC args.claim in args.namespace, and any static PV    ║
        # ║     created for it                                                ║
        # ║  2. Wait until both are gone                                      ║
        # ║  3. result["deleted"] = True; result["success"] = True            ║
        # ╚══════════════════════════════════════════════════════════════════╝
        if DEMO_MODE:
            result.update({"success": True, "deleted": True, "message": "Claim deleted"})
            result["resources_deleted"].append(f"{args.namespace}/{args.claim}")
        else:
            result["error"] = "Not implemented - replace with your platform's claim delete logic"

    if result["success"] and record:
        record.unlink(missing_ok=True)
    print(json.dumps(result, indent=2))
    return 0 if result["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
