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

"""Helpers for the performance fixtures: look up an existing claim (read-only), and guard records.

Given --existing-claim (the perf_claim setting, STORAGE_PERF_CLAIM as shipped, or a
storage_perf.claim that setup_cluster reports), create_perf_filesystem and
provision_perf_storage adopt that claim instead of creating their own, and report
"created": false so teardown leaves it alone.
"""

import json
import os
import shlex
import subprocess
from pathlib import Path
from typing import Any


def get_claim(kubeconfig: str, namespace: str, name: str) -> tuple[dict[str, Any] | None, str]:
    """The claim as `kubectl get -o json` returns it, or None and why not.

    KUBECTL overrides the CLI with a shell-quoted prefix, as for scripts/k8s/setup.sh.
    """
    kubectl = shlex.split(os.environ.get("KUBECTL", "kubectl"))
    env = dict(os.environ)
    if kubeconfig:
        env["KUBECONFIG"] = os.path.expanduser(kubeconfig)
    try:
        proc = subprocess.run(
            [*kubectl, "-n", namespace, "get", "pvc", name, "-o", "json", "--request-timeout=20s"],
            capture_output=True,
            text=True,
            env=env,
            check=False,
            timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return None, f"kubectl get pvc {namespace}/{name}: {exc}"
    if proc.returncode != 0:
        return None, proc.stderr.strip()[-300:] or f"kubectl get pvc {namespace}/{name} failed"
    try:
        return json.loads(proc.stdout), ""
    except ValueError:
        return None, f"kubectl get pvc {namespace}/{name} did not return JSON"


def claim_problem(pvc: dict[str, Any]) -> str:
    """Why the sweep cannot use this claim, or "" when it is Bound, ReadWriteMany and Filesystem-mode."""
    phase = pvc.get("status", {}).get("phase")
    modes = pvc.get("spec", {}).get("accessModes", [])
    volume_mode = pvc.get("spec", {}).get("volumeMode", "Filesystem")
    if phase != "Bound" or "ReadWriteMany" not in modes or volume_mode != "Filesystem":
        return f"claim not usable: phase={phase} accessModes={modes} volumeMode={volume_mode}"
    return ""


def recorded_creation(record: str) -> str:
    """Why setup must not run yet, or "": an earlier run's record says it created a fixture.

    Teardown finds what to delete through the record, so overwriting it - with an adopted
    claim, or a new fixture - would leave the earlier one behind with nothing pointing at it.
    """
    if not record:
        return ""
    try:
        previous = json.loads(Path(record).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ""
    if isinstance(previous, dict) and previous.get("created"):
        return f"{record} records a fixture an earlier run created and kept; run with --phase teardown first"
    return ""
