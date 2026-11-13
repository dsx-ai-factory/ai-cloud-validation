#!/bin/bash
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

# K8s Inventory Stub - Queries real cluster and outputs inventory JSON
#
# Requirements:
#   - kubectl OR microk8s configured and accessible
#   - jq for JSON processing
#   - nvidia GPU operator installed (for GPU detection)

set -eo pipefail

# ISVCTL_DEMO_MODE=1 prints a sample inventory without touching a cluster (make demo-storage).
if [ "${ISVCTL_DEMO_MODE:-}" = "1" ]; then
    cat << 'EOF'
{
  "success": true,
  "platform": "kubernetes",
  "offered_versions": [],
  "cluster_name": "demo-cluster",
  "kubernetes": {"node_count": 2, "nodes": ["demo-node-1", "demo-node-2"], "gpu_node_count": 0, "total_gpus": 0},
  "csi": {"block_storage_class": "demo-block", "shared_fs_storage_class": "demo-shared", "nfs_storage_class": "demo-nfs"},
  "kubeconfig_path": ""
}
EOF
    exit 0
fi

# Detect kubectl command
if [[ "${KUBECTL:-}" =~ [^[:space:]] ]]; then
    :  # already set from environment; skip detection
elif command -v kubectl &> /dev/null; then
    KUBECTL="kubectl"
elif command -v microk8s &> /dev/null; then
    KUBECTL="microk8s kubectl"
else
    echo "Error: Neither kubectl nor microk8s found. Set KUBECTL to override." >&2
    exit 1
fi

CLUSTER_NAME=$($KUBECTL config current-context 2>/dev/null || echo "unknown")
# K8S_EXPECTED_CONTEXT stops the run unless kubectl points at that context, so a run cannot
# land on whatever cluster the shell happens to point at.
if [ -n "${K8S_EXPECTED_CONTEXT:-}" ] && [ "$CLUSTER_NAME" != "$K8S_EXPECTED_CONTEXT" ]; then
    echo "Error: current context is '$CLUSTER_NAME', expected K8S_EXPECTED_CONTEXT='$K8S_EXPECTED_CONTEXT'" >&2
    exit 1
fi

# K8S_KUBECONFIG_OUT=<path> pins the cluster for the rest of the run: the current context,
# credentials included, is written there (mode 600) and reported as kubeconfig_path, so
# later steps - and tools that call plain kubectl - stay on it even if the shell's context
# changes. Keep the path somewhere private and out of version control. The copy is written to a
# private temp file beside it and moved over it only once complete, so an earlier copy is
# replaced whole (ending mode 600 whatever its mode was) or, if the write fails, left as it was.
KUBECONFIG_PATH=""
if [ -n "${K8S_KUBECONFIG_OUT:-}" ]; then
    KUBECONFIG_DIR="$(dirname "$K8S_KUBECONFIG_OUT")"
    mkdir -p "$KUBECONFIG_DIR"
    KUBECONFIG_TMP=""
    if ! KUBECONFIG_TMP="$(umask 077 && mktemp "$KUBECONFIG_DIR/.kubeconfig.XXXXXX")" \
        || ! $KUBECTL config view --minify --flatten > "$KUBECONFIG_TMP" \
        || ! mv -f "$KUBECONFIG_TMP" "$K8S_KUBECONFIG_OUT"; then
        [ -n "$KUBECONFIG_TMP" ] && rm -f "$KUBECONFIG_TMP"
        echo "Error: could not write the current context's kubeconfig to $K8S_KUBECONFIG_OUT" >&2
        exit 1
    fi
    KUBECONFIG_PATH="$K8S_KUBECONFIG_OUT"
fi
DEFAULT_GPU_NS="unknown"
REQUIRE_JQ="true"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_common.sh"
