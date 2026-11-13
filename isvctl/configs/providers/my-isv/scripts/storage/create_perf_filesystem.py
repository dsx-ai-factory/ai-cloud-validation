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

"""Performance fixture: create the filesystem the storage-scale-test sweep runs against.

Provider-agnostic template - replace the TODO block with your platform's storage API calls.
Called first in the setup phase of config/overlays/storage-perf.yaml (SPERF01). Unlike the
self-contained HSS scripts this is a fixture: the filesystem stays up for the sweep,
provision_perf_storage exposes it to the cluster as a claim, and delete_perf_filesystem
removes it in teardown.

Create the high-speed filesystem (or find an existing one), wait until it is ready to mount,
and make it reachable from the cluster nodes the sweep will use. Reusing an existing
filesystem is fine: report "created": false and teardown leaves it alone. With
--existing-claim (the perf_claim setting) this already happens here: the
filesystem is the volume bound to that claim, and nothing is created. With --record the
result is also written to that file, so a teardown run on its own (`--phase teardown`, which
has no setup outputs) still knows what to delete. Given an empty --node-selector it stops
before creating anything: the sweep runs only on labelled nodes.

Required JSON output fields:
  success        (bool) - true once the filesystem is ready to mount
  platform       (str)  - "storage"
  test_name      (str)  - "create_perf_filesystem"
  filesystem_id  (str)  - identifier of the filesystem (consumed by later steps)
  capacity_gib   (int)  - its capacity
  created        (bool) - true if this run created it (teardown deletes only those)

Usage:
    python create_perf_filesystem.py --name isv-storage-perf --capacity-gib 1200 [--record <path>]
    python create_perf_filesystem.py --namespace storage-perf --existing-claim perf-claim
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

# Allow importing provider-local helpers from scripts/common/.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.k8s_claim import adopt_claim, recorded_creation, write_record

# ISVCTL_DEMO_MODE=1 enables demo-success output (used by `make demo-test`).
DEMO_MODE = os.environ.get("ISVCTL_DEMO_MODE") == "1"


def main() -> int:
    """Create (or find) the performance filesystem; print JSON."""
    parser = argparse.ArgumentParser(description="Performance fixture: create the filesystem")
    parser.add_argument("--name", default="isv-storage-perf", help="Name for the filesystem")
    parser.add_argument("--capacity-gib", type=int, default=1200, help="Capacity to provision")
    parser.add_argument("--record", default="", help="File to record the result in for teardown")
    parser.add_argument("--kubeconfig", default="", help="Kubeconfig emitted by setup_cluster (default: kubectl's own)")
    parser.add_argument("--namespace", default="storage-perf", help="Namespace of --existing-claim")
    parser.add_argument("--existing-claim", default="", help="Reuse the volume bound to this claim")
    parser.add_argument("--node-selector", default=None, help="The sweep's node selector; empty stops setup")
    args = parser.parse_args()

    result: dict[str, Any] = {
        "success": False,
        "platform": "storage",
        "test_name": "create_perf_filesystem",
        "filesystem_id": "",
        "capacity_gib": args.capacity_gib,
        "created": False,
    }

    # The sweep runs only on labelled nodes; without a selector nothing should be created for it.
    if args.node_selector is not None and not args.node_selector.strip() and not DEMO_MODE:
        result["error"] = (
            "no node selector for the sweep: set STORAGE_PERF_NODE_SELECTOR (the perf_node_selector setting), "
            "e.g. storage-test=true, or report storage_perf.node_selector from setup_cluster"
        )
        print(json.dumps(result, indent=2))
        return 1

    # ╔══════════════════════════════════════════════════════════════════╗
    # ║  TODO: Replace this block with your platform's implementation     ║
    # ║                                                                   ║
    # ║  1. CreateFilesystem(args.name, args.capacity_gib), or find an    ║
    # ║     existing one to reuse                                         ║
    # ║  2. Wait until it is available and mountable from the cluster     ║
    # ║  3. result["filesystem_id"] = filesystem_id                       ║
    # ║  4. result["created"] = True only if this run created it          ║
    # ║  5. result["success"] = True                                      ║
    # ╚══════════════════════════════════════════════════════════════════╝

    previous = recorded_creation(args.record)
    if previous:
        result["error"] = previous
    elif DEMO_MODE and args.existing_claim:
        result.update({"success": True, "filesystem_id": "demo-perf-volume"})
    elif DEMO_MODE:
        result.update({"success": True, "filesystem_id": "demo-perf-fs", "created": True})
    elif args.existing_claim:
        pvc, error = adopt_claim(args.kubeconfig, args.namespace, args.existing_claim)
        if pvc is None:
            result["error"] = error
        else:
            result.update({"success": True, "filesystem_id": pvc["spec"]["volumeName"]})
    else:
        result["error"] = "Not implemented - create the filesystem for the performance sweep"

    if result["success"] and args.record:
        write_record(args.record, result)
    print(json.dumps(result, indent=2))
    return 0 if result["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
