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

"""Tests for the Kubernetes GPU stress pod spec."""

import yaml

from isvtest.config.settings import DEFAULT_PYTORCH_IMAGE
from isvtest.core.k8s import NODE_PINNING_NODE_SELECTOR
from isvtest.workloads.k8s_stress import K8sGpuStressWorkload


def test_stress_pod_runs_a_script_the_pytorch_image_can_import() -> None:
    """NGC PyTorch images dropped CuPy in 25.09, so the pod must only need torch."""
    pod_yaml = K8sGpuStressWorkload()._create_pod_yaml(
        pod_name="gpu-stress",
        node_name="node-1",
        gpu_count=4,
        namespace="default",
        image=DEFAULT_PYTORCH_IMAGE,
        runtime=30,
        memory_gb=1,
    )
    assert "import torch" in pod_yaml
    assert "cupy" not in pod_yaml.lower()
    assert f"image: {DEFAULT_PYTORCH_IMAGE}" in pod_yaml
    assert 'nvidia.com/gpu: "4"' in pod_yaml


def _stress_pod(**kwargs) -> str:
    return K8sGpuStressWorkload()._create_pod_yaml(
        pod_name="gpu-stress",
        node_name="node-1",
        gpu_count=1,
        namespace="default",
        image=DEFAULT_PYTORCH_IMAGE,
        runtime=30,
        memory_gb=1,
        **kwargs,
    )


def test_stress_pod_is_pinned_with_node_name_by_default() -> None:
    """Default placement bypasses the scheduler, as before."""
    pod = yaml.safe_load(_stress_pod())
    assert pod["spec"]["nodeName"] == "node-1"
    assert "nodeSelector" not in pod["spec"]


def test_stress_pod_can_be_placed_through_the_scheduler() -> None:
    """node_selector placement leaves scheduling to the cluster's (GPU-aware) scheduler."""
    pod = yaml.safe_load(_stress_pod(node_pinning=NODE_PINNING_NODE_SELECTOR))
    assert pod["spec"]["nodeSelector"] == {"kubernetes.io/hostname": "node-1"}
    assert "nodeName" not in pod["spec"]
