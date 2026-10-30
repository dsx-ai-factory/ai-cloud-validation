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

"""Performance fixture teardown: delete the filesystem create_perf_filesystem created.

Provider-agnostic template - replace the TODO block with your platform's storage API calls.
Called in the teardown phase of config/overlays/storage-perf.yaml, after
deprovision_perf_storage has removed the claim. If the filesystem was reused rather than
created ("created": false), succeed without deleting anything.

A teardown run on its own (`--phase teardown`) has no setup outputs, so --filesystem-id is
empty; --record then names the file create_perf_filesystem wrote, and that file is removed
once teardown is done with it.

Required JSON output fields:
  success            (bool) - true once the filesystem is gone (or was left in place on purpose)
  platform           (str)  - "storage"
  test_name          (str)  - "delete_perf_filesystem"
  deleted            (bool) - true if this step deleted it
  resources_deleted  (list) - identifiers of what this step deleted
  message            (str)  - human-readable summary

With --skip-destroy (STORAGE_SKIP_TEARDOWN=true) nothing is deleted and the record is kept,
so a later teardown can still remove it.

Usage:
    python delete_perf_filesystem.py --filesystem-id fs-0123456789 [--created] [--record <path>] [--skip-destroy]
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

# ISVCTL_DEMO_MODE=1 enables demo-success output (used by `make demo-test`).
DEMO_MODE = os.environ.get("ISVCTL_DEMO_MODE") == "1"


def main() -> int:
    """Delete the performance filesystem if this run created it; print JSON."""
    parser = argparse.ArgumentParser(description="Performance fixture teardown: delete the filesystem")
    parser.add_argument("--filesystem-id", default="", help="Filesystem create_perf_filesystem reported")
    parser.add_argument("--created", action="store_true", help="create_perf_filesystem created it")
    parser.add_argument("--record", default="", help="File create_perf_filesystem recorded its result in")
    parser.add_argument("--skip-destroy", action="store_true", help="Keep it (STORAGE_SKIP_TEARDOWN=true)")
    args = parser.parse_args()

    record = Path(args.record) if args.record else None
    if not args.filesystem_id and record and record.is_file():
        recorded = json.loads(record.read_text(encoding="utf-8"))
        args.filesystem_id, args.created = recorded.get("filesystem_id", ""), bool(recorded.get("created"))

    result: dict[str, Any] = {
        "success": False,
        "platform": "storage",
        "test_name": "delete_perf_filesystem",
        "deleted": False,
        "resources_deleted": [],
        "message": "",
    }

    if args.created and args.skip_destroy:
        result.update({"success": True, "message": f"Filesystem {args.filesystem_id} preserved (--skip-destroy)"})
        print(json.dumps(result, indent=2))
        return 0
    if not args.created:
        result.update({"success": True, "message": "filesystem was not created by this run; left in place"})
    else:
        # ╔══════════════════════════════════════════════════════════════════╗
        # ║  TODO: Replace this block with your platform's implementation     ║
        # ║                                                                   ║
        # ║  1. DeleteFilesystem(args.filesystem_id)                          ║
        # ║  2. Wait until it is gone                                         ║
        # ║  3. result["deleted"] = True; result["success"] = True            ║
        # ╚══════════════════════════════════════════════════════════════════╝
        if DEMO_MODE:
            result.update({"success": True, "deleted": True, "message": "Filesystem deleted"})
            result["resources_deleted"].append(args.filesystem_id)
        else:
            result["error"] = "Not implemented - delete the filesystem create_perf_filesystem created"

    if result["success"] and record:
        record.unlink(missing_ok=True)
    print(json.dumps(result, indent=2))
    return 0 if result["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
