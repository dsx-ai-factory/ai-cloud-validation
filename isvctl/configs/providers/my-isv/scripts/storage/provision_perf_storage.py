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

"""Performance fixture: expose the perf filesystem to the cluster as a claim.

Provider-agnostic template - replace the TODO block with your platform's implementation.
Called in the setup phase of config/overlays/storage-perf.yaml, after create_perf_filesystem
(SPERF02). A fixture: the claim stays up for run_storage_test, and deprovision_perf_storage
removes it in teardown.

Expose the filesystem create_perf_filesystem reported as a PVC that is Bound, ReadWriteMany
and Filesystem-mode, and that every node matching the sweep's node selector can mount - for
example a static PersistentVolume for your CSI driver whose volume handle is the filesystem
ID, plus a claim bound to it. Reusing an existing claim is fine: report "created": false and
deprovision_perf_storage leaves it alone; --existing-claim (the perf_claim setting) does
this here, after checking the claim is usable. The sweep writes only under its test path (default
"scale-test") inside the claim, which must be writable by the benchmark Pods' UID/GID
(default 2000). With --record the result is also written to that file, so a teardown run on
its own (`--phase teardown`, which has no setup outputs) still knows what to delete.

Required JSON output fields:
  success     (bool) - true once the claim is Bound
  platform    (str)  - "storage"
  test_name   (str)  - "provision_perf_storage"
  namespace   (str)  - namespace of the claim (consumed by later steps)
  claim_name  (str)  - name of the claim (consumed by later steps)
  created     (bool) - true if this run created it (teardown deletes only those)

Usage:
    python provision_perf_storage.py --kubeconfig <path> --filesystem-id fs-0123456789 \\
        --namespace storage-perf [--record <path>]
    python provision_perf_storage.py --namespace storage-perf --existing-claim perf-claim
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

# Allow importing provider-local helpers from scripts/common/.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.k8s_claim import claim_problem, get_claim, recorded_creation

# ISVCTL_DEMO_MODE=1 enables demo-success output (used by `make demo-test`).
DEMO_MODE = os.environ.get("ISVCTL_DEMO_MODE") == "1"


def main() -> int:
    """Expose the performance filesystem as a Bound ReadWriteMany claim; print JSON."""
    parser = argparse.ArgumentParser(description="Performance fixture: expose the filesystem as a claim")
    parser.add_argument("--kubeconfig", default="", help="Kubeconfig emitted by setup_cluster")
    parser.add_argument("--filesystem-id", default="", help="Filesystem create_perf_filesystem reported")
    parser.add_argument("--namespace", default="storage-perf", help="Namespace for the claim")
    parser.add_argument("--record", default="", help="File to record the result in for teardown")
    parser.add_argument("--existing-claim", default="", help="Reuse this claim instead of creating one")
    args = parser.parse_args()

    result: dict[str, Any] = {
        "success": False,
        "platform": "storage",
        "test_name": "provision_perf_storage",
        "namespace": args.namespace,
        "claim_name": "",
        "created": False,
    }

    # ╔══════════════════════════════════════════════════════════════════╗
    # ║  TODO: Replace this block with your platform's implementation     ║
    # ║                                                                   ║
    # ║  1. Create a PV for args.filesystem_id and a PVC bound to it in   ║
    # ║     args.namespace (or find an existing claim to reuse)           ║
    # ║  2. Wait until the PVC is Bound                                   ║
    # ║  3. result["claim_name"] = claim_name                             ║
    # ║  4. result["created"] = True only if this run created it          ║
    # ║  5. result["success"] = True                                      ║
    # ╚══════════════════════════════════════════════════════════════════╝

    previous = recorded_creation(args.record)
    if previous:
        result["error"] = previous
    elif DEMO_MODE and args.existing_claim:
        result.update({"success": True, "claim_name": args.existing_claim})
    elif DEMO_MODE:
        result.update({"success": True, "claim_name": "demo-perf-claim", "created": True})
    elif args.existing_claim:
        pvc, error = get_claim(args.kubeconfig, args.namespace, args.existing_claim)
        if pvc is not None:
            error = claim_problem(pvc)
        if error:
            result["error"] = error
        else:
            result.update({"success": True, "claim_name": args.existing_claim})
    else:
        result["error"] = "Not implemented - expose the filesystem as a Bound ReadWriteMany PVC"

    if result["success"] and args.record:
        Path(args.record).parent.mkdir(parents=True, exist_ok=True)
        Path(args.record).write_text(json.dumps(result), encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0 if result["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
