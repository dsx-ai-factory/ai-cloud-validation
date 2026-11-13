<!-- GENERATED FILE - DO NOT EDIT BY HAND. Source: storage-performance-requirements.yaml. Run `make plan`. -->

# Storage Performance and Scale Validation

> Structured source of record: `storage-performance-requirements.yaml` (version placeholder-2026-10-27).
> Owner: not specified.
> Edit the YAML, not this file.

## Provider Fixtures

| Req ID | Requirement Area | Description | Status |
| :----- | :--------------- | :---------- | :----- |
| SPERF01 | Filesystem creation | The provider creates (or supplies) the high-speed filesystem under test through its own API, and removes anything it created when the run ends. | draft |
| SPERF02 | Filesystem exposed to Kubernetes | The filesystem is exposed to the cluster as a Bound, ReadWriteMany, Filesystem-mode PersistentVolumeClaim that every node selected for the benchmark can mount. | draft |

## Execution

| Req ID | Requirement Area | Description | Status |
| :----- | :--------------- | :---------- | :----- |
| SPERF03 | Run to completion | An IO benchmark sweep runs to completion against the claim within a single validation run, using a pinned release of the benchmark tool. | draft |
| SPERF04 | Configurable workload | The sweep's workload (node counts, IO sizes, threads, IO depth, phase duration) is configurable from the provider configuration without editing the benchmark tool. | draft |
| SPERF05 | Resource release | Every cluster resource the benchmark creates (claim reservation, coordinator, workers, network policies) is released on every exit path, including failure, timeout and interruption of the validation run. | draft |
| SPERF06 | Diagnosable failures | A failed or incomplete sweep reports why and where its results and logs are, so the failure can be investigated. | draft |

## Results

| Req ID | Requirement Area | Description | Status |
| :----- | :--------------- | :---------- | :----- |
| SPERF07 | Real numbers reported | A completed sweep reports measured write and read bandwidth and IOPS, and average latency, in machine-readable form. | draft |
| SPERF08 | Pass criteria | A run passes when the sweep completed, every cell succeeded, its resources were released and real write and read results were reported. Optional provider-configured floors are applied on top; none is defined here. | draft |

## Scale

| Req ID | Requirement Area | Description | Status |
| :----- | :--------------- | :---------- | :----- |
| SPERF09 | Multiple targets and workloads | Several filesystems and workload variants can be tested in one prepared batch, with results reported per target. | planned |
| SPERF10 | Metadata performance | A metadata sweep (create, stat, delete rates) runs to completion and reports real operation rates. | planned |
| SPERF11 | Multi-node scaling | A sweep across increasing node counts reports how bandwidth and IOPS scale with the number of client nodes. | planned |
