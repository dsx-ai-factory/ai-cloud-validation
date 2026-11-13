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

"""Helpers for the performance fixtures: look up an existing claim (read-only), and keep records.

Given --existing-claim (the perf_claim setting, STORAGE_PERF_CLAIM as shipped, or a
storage_perf.claim that setup_cluster reports), create_perf_filesystem and
provision_perf_storage adopt that claim (adopt_claim) instead of creating their own, and
report "created": false so teardown leaves it alone. Setup writes its result to --record
(write_record) and a standalone teardown reads it back (read_record). provision_perf_storage
also records which cluster it used (cluster_identity), and deprovision_perf_storage deletes
only on that one (other_cluster).
"""

import json
import os
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Any


def kubectl_command() -> list[str]:
    """The kubectl CLI, found in the order scripts/k8s/setup.sh uses.

    KUBECTL, unless blank, is a shell-quoted prefix used as given (e.g. "oc" or
    "microk8s kubectl"); otherwise kubectl if it is on PATH, then `microk8s kubectl`.
    Raises FileNotFoundError when there is neither.
    """
    override = os.environ.get("KUBECTL", "")
    if override.strip():
        return shlex.split(override)
    if shutil.which("kubectl"):
        return ["kubectl"]
    if shutil.which("microk8s"):
        return ["microk8s", "kubectl"]
    raise FileNotFoundError("Neither kubectl nor microk8s found. Set KUBECTL to override.")


def _kubectl(kubeconfig: str) -> tuple[list[str], dict[str, str]]:
    """The kubectl command (kubectl_command) and its environment, pinned to *kubeconfig* when given."""
    env = dict(os.environ)
    if kubeconfig:
        env["KUBECONFIG"] = os.path.expanduser(kubeconfig)
    return kubectl_command(), env


def get_claim(kubeconfig: str, namespace: str, name: str) -> tuple[dict[str, Any] | None, str]:
    """The claim as `kubectl get -o json` returns it, or None and why not."""
    try:
        kubectl, env = _kubectl(kubeconfig)
        proc = subprocess.run(
            [*kubectl, "-n", namespace, "get", "pvc", name, "-o", "json", "--request-timeout=20s"],
            capture_output=True,
            text=True,
            env=env,
            check=False,
            timeout=60,
        )
    except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
        return None, f"kubectl get pvc {namespace}/{name}: {exc}"
    if proc.returncode != 0:
        return None, proc.stderr.strip()[-300:] or f"kubectl get pvc {namespace}/{name} failed"
    try:
        return json.loads(proc.stdout), ""
    except ValueError:
        return None, f"kubectl get pvc {namespace}/{name} did not return JSON"


def cluster_identity(kubeconfig: str) -> dict[str, str] | None:
    """The context and API server *kubeconfig* (or kubectl's default) points at; None if kubectl cannot say.

    A local read (``kubectl config view --minify``) that keeps no credential. Both are kept,
    because a shared access proxy can serve several clusters at one URL.
    """
    try:
        kubectl, env = _kubectl(kubeconfig)
        proc = subprocess.run(
            [*kubectl, "config", "view", "--minify", "-o", "json"],
            capture_output=True,
            text=True,
            env=env,
            check=False,
            timeout=30,
        )
        view = json.loads(proc.stdout) if proc.returncode == 0 else {}
        context = view["current-context"]
        cluster = next(c["context"]["cluster"] for c in view["contexts"] if c["name"] == context)
        server = next(c["cluster"]["server"] for c in view["clusters"] if c["name"] == cluster)
    except (OSError, subprocess.TimeoutExpired, ValueError, KeyError, TypeError, StopIteration):
        return None
    if not isinstance(context, str) or not isinstance(server, str) or not context or not server:
        return None
    return {"context": context, "server": server}


def other_cluster(recorded: Any, kubeconfig: str) -> str:
    """Why teardown must not delete with *kubeconfig*: it is not the cluster *recorded* names; else "".

    A record without a cluster (demo mode, or one kubectl could not read) is not checked.
    """
    if not isinstance(recorded, dict):
        return ""
    wanted = {"context": recorded.get("context"), "server": recorded.get("server")}
    current = cluster_identity(kubeconfig)
    if current == wanted:
        return ""
    where = f"the claim was created on context {wanted['context']} at {wanted['server']}"
    which = kubeconfig or "kubectl's default kubeconfig"
    if current is None:
        return f"cannot read which cluster {which} points at; {where}. Not deleting it"
    return (
        f"{where}, but {which} points at context {current['context']} at {current['server']}. "
        "Not deleting it; run teardown with a kubeconfig for the original cluster"
    )


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


def adopt_claim(kubeconfig: str, namespace: str, name: str) -> tuple[dict[str, Any] | None, str]:
    """The existing claim, when the sweep can use it; else None and why not (get_claim, claim_problem)."""
    pvc, error = get_claim(kubeconfig, namespace, name)
    if pvc is not None:
        error = claim_problem(pvc)
    return (None, error) if error else (pvc, "")


def write_record(record: str, content: dict[str, Any]) -> None:
    """Write a setup step's result to *record*, for a standalone teardown to read back."""
    Path(record).parent.mkdir(parents=True, exist_ok=True)
    Path(record).write_text(json.dumps(content), encoding="utf-8")


def read_record(record: str) -> tuple[bool, Any]:
    """(True, the JSON *record* holds) when the file exists, else (False, {}).

    The content is returned as stored: a file that is not JSON raises ValueError, and one that
    holds something other than an object is the caller's to handle.
    """
    path = Path(record) if record else None
    if path is None or not path.is_file():
        return False, {}
    return True, json.loads(path.read_text(encoding="utf-8"))
