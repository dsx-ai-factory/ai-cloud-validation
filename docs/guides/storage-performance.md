# Storage Performance

The storage suite's Performance group runs an IO benchmark sweep against a
high-speed filesystem mounted in a Kubernetes cluster, and checks that the sweep
ran to completion, released everything it held, and reported real write and read
numbers. The benchmark is
[storage-scale-test](https://github.com/NVIDIA/storage-scale-test) (elbencho on
Kubernetes); the shared steps in `isvctl/configs/shared/storage/perf/` drive it,
so a provider implements only the filesystem and claim fixtures.

There is no official performance bar. A pass means the sweep ran and reported
numbers; a provider can set its own floors (see [Floors](#floors)).

## Running it

```bash
make vendor                           # once: pinned test tools, incl. storage-scale-test, into isvtest/vendor/
export STORAGE_PERF_NODE_SELECTOR=storage-test=true
uv run isvctl test run \
    -f isvctl/configs/providers/my-isv/config/storage.yaml \
    -f isvctl/configs/providers/my-isv/config/overlays/storage-perf.yaml \
    --capability kubernetes --label performance
```

- The overlay replaces the storage config's step list, so this run does only the
  performance sweep. Run the plain storage config separately for the functional
  checks.
- Always select `--label performance`. Labels choose checks, not steps, but
  without the label the suite's live Kubernetes checks would also run, and some of
  them create claims.
- Needed locally, as storage-scale-test requires them: bash 4.4+, kubectl,
  `flock`, GNU `tar` and GNU `timeout`, and `sha256sum` or `shasum`. On Linux these
  are usually present; on macOS run `brew install bash coreutils gnu-tar flock` and
  put Homebrew's bash first on `PATH`. Its reports need Python 3.12+, which `uv`
  already provides: `make vendor` and `isvctl` both run under the project's Python.
  Elbencho itself runs in the benchmark Pods from the image the release pins.
  `make vendor-storage-scale-test-elbencho` also fetches the release's pinned
  elbencho binaries; only non-Kubernetes sweeps need them.
- `STORAGE_SCALE_TEST_BASE=/path/to/checkout` uses another storage-scale-test tree
  (v2.1.0 or later). isvctl writes that tree's `env.sh`, so dedicate the tree to
  isvctl.
- `make demo-storage` runs the same overlay in demo mode, without a cluster.
- `make clean-vendor` removes the vendored tools (`make clean` leaves them), and
  `make vendor` downloads them again. `make vendor` also checks the release with its own
  `lib/project_version.sh` (every file against its `SOURCE_SHA256`) and prepares the Python
  environment its report extractor uses.
- Optional preflight: storage-scale-test's `validate_env.sh`, run from the vendored tree
  after a run has written its `env.sh`, checks the claim mount, image and UID/GID with a
  short-lived Job, so it touches the cluster. Submit already validates the test directories.

## What runs

| Phase | Step | Does |
|---|---|---|
| setup | `find_storage_scale_test` | Finds the vendored release (or `STORAGE_SCALE_TEST_BASE`) |
| setup | `setup_cluster` | Provider: the cluster and its kubeconfig |
| setup | `create_perf_filesystem` | Provider: the filesystem under test |
| setup | `provision_perf_storage` | Provider: a claim for it |
| test | `run_storage_test` | Submits the sweep, waits for it, collects it |
| test | `evaluate_storage_test` | Reads the results and compares them with the floors |
| teardown | `release_storage_test` | Releases a sweep the run step did not collect |
| teardown | `deprovision_perf_storage`, `delete_perf_filesystem`, `teardown_cluster` | Provider: removes what setup created |

| Check | Test ID | Step | Passes when |
|---|---|---|---|
| `StoragePerfFilesystemCreatedCheck` | `SPERF01-01` | `create_perf_filesystem` | The step succeeded and reported `filesystem_id` |
| `StoragePerfClaimProvidedCheck` | `SPERF02-01` | `provision_perf_storage` | The step succeeded and reported `namespace` and `claim_name` |
| `StoragePerfSweepCompletedCheck` | `SPERF03-01` | `run_storage_test` | The sweep finished with state `SUCCESS` |
| `StoragePerfSweepReleasedCheck` | `SPERF05-01` | `release_storage_test` | The sweep's cluster resources were released, by either step |
| `StoragePerfResultsPresentCheck` | `SPERF07-01` | `evaluate_storage_test` | `batch_outcome.complete` is true, with write and read numbers |
| `StoragePerfBandwidthFloorCheck` | `SPERF08-01` | `evaluate_storage_test` | Bandwidth meets `perf_min_mbps` |
| `StoragePerfIopsFloorCheck` | `SPERF08-02` | `evaluate_storage_test` | IOPS meets `perf_min_iops` |

The three results checks are classes (`isvtest/src/isvtest/validations/storage_perf.py`)
rather than generic composites, so the report shows the measured numbers: for example
`StoragePerfBandwidthFloorCheck: PASSED - 7123.0 MB/s (report only, no floor set); write
7123.0 MB/s, read 7628.4 MB/s`, or, below a floor, `7123.0 MB/s < floor 100000.0 MB/s`.

The test IDs are placeholders, defined in `docs/test-plan.yaml` and mapped to the
placeholder requirements in `docs/requirements/storage-performance-requirements.yaml`
until the official storage performance requirements are onboarded.

Every step's JSON output is validated against a schema in
`isvctl/src/isvctl/config/output_schemas.py` (`storage_perf_tool`, `_filesystem`,
`_claim`, `_run`, `_evaluate`, `_release`, `_fixture_teardown`). A step that reports
success without the fields later steps read (for example `filesystem_id`, or
`claim_name`) fails at that step; failure output always validates, so its error
reaches the report.

The sweep runs in-cluster: storage-scale-test only submits it, `--status` reports
its progress, and `--collect` copies the results back and releases what it holds
(the claim's Lease, the coordinator Job, the worker DaemonSet and its network
policies). `run_storage_test` does all three synchronously, so one step covers the
sweep from submit to release.

## Provider fixtures

A provider implements four scripts; the my-isv stubs in
`isvctl/configs/providers/my-isv/scripts/storage/` show the contract.

| Script | Reports (required on success) | Notes |
|---|---|---|
| `create_perf_filesystem.py` | `filesystem_id`, `capacity_gib`, `created` | Create the filesystem, or find one to reuse |
| `provision_perf_storage.py` | `namespace`, `claim_name`, `created` | A Bound, ReadWriteMany, Filesystem-mode claim every selected node can mount |
| `deprovision_perf_storage.py` | `deleted` | Deletes the claim only if `created` |
| `delete_perf_filesystem.py` | `deleted` | Deletes the filesystem only if `created` |

- Report `"created": false` for a fixture you reused; teardown then leaves it in
  place. `STORAGE_SKIP_TEARDOWN=true` also keeps the filesystem and claim a run
  created (with `--skip-destroy`, like the other storage teardown steps), and keeps
  their record files so a later teardown can still remove them.
- The sweep writes only under `scale-test/` inside the claim, as UID/GID 2000 by
  default, so that directory must be writable by them.
- **Record files.** The create and provision steps also write their result to
  `--record` (`_output/storage-perf/inputs/filesystem.json` and `claim.json`).
  `isvctl test run ... --phase teardown` has no setup outputs, so the delete
  steps fall back to those files to find what to remove, and delete the file once
  done. Keep that behaviour when you replace the stubs.

## Settings

Set these in your provider config's `tests.settings`, or per run with
`--set tests.settings.<name>=<value>`.

| Setting | Default | Meaning |
|---|---|---|
| `perf_node_selector` | `$STORAGE_PERF_NODE_SELECTOR` | Labels of the nodes that run the benchmark, e.g. `storage-test=true` |
| `perf_namespace` | `storage-perf` | Namespace for the claim |
| `perf_nodes` | `1` | Node counts to sweep (storage-scale-test `--nodes` syntax, e.g. `1,2`) |
| `perf_io_sizes` | `r4K,1M` | IO sizes; an `r` prefix means random IO |
| `perf_threads` | `1` | Threads per node |
| `perf_iodepth` | `1` | IO depths |
| `perf_duration_s` | `10` | Seconds per write and read phase |
| `perf_wait_timeout_s` | `900` | How long `run_storage_test` waits for the sweep |
| `perf_min_mbps`, `perf_min_iops` | `0` | Floors; `0` reports only |

The defaults are a smoke run: one node, two IO sizes, 10 s per phase. It proves
the sweep runs and reports real numbers, not sustained performance. For longer
sweeps, storage-scale-test's
[BENCHMARK_RECIPES_FILESYSTEM.md](https://github.com/NVIDIA/storage-scale-test/blob/main/BENCHMARK_RECIPES_FILESYSTEM.md)
has ready-made shapes, starting with its quick validation recipe.

### Floors

`evaluate_storage_test` reports a headline per operation: the best bandwidth
(MB/s, 10^6 bytes) and IOPS across the sweep's cells, and the lowest average
latency. The floor checks compare the slower operation's best result with
`perf_min_mbps` and `perf_min_iops`. With a floor of `0` they pass and only report
the numbers. Set floors from your own expectations for the sweep you configure: a
small random IO size gives far higher IOPS and far lower bandwidth than 1M.

### Time limits

A longer sweep needs three numbers raised together, in the overlay:

- `perf_wait_timeout_s`: how long to wait for the sweep.
- `run_storage_test`'s `--time-budget` (default 2280): the whole step's time.
  The step keeps 1200 s of it (two 600 s command timeouts) to cancel and collect,
  so the wait is cut short once the budget minus 1200 s runs out. Keep the budget
  at least `perf_wait_timeout_s` plus 1200 s plus the time submit takes.
- `run_storage_test`'s `timeout` (default 2400): keep it above the budget.

A step that runs out of budget stops waiting, cancels and collects the sweep, and
reports `TIMEOUT`. A step that isvctl kills at its `timeout` reports nothing; the
teardown step then releases the sweep from the state file (below).
`release_storage_test` has the same pair: `--time-budget 1680` under a 1800 s
`timeout`.

## Results

Each run writes under `_output/storage-perf/` in the directory isvctl runs from:

- `results/elbencho-<UTC timestamp>/` is storage-scale-test's results directory:
  logs, per-cell CSVs, its reports, and `results.json`, the machine-readable
  document `evaluate_storage_test` reads (see storage-scale-test's
  [MACHINE_READABLE_RESULTS.md](https://github.com/NVIDIA/storage-scale-test/blob/main/docs/MACHINE_READABLE_RESULTS.md),
  including its jq spot checks). `evaluate_storage_test` passes `results_present`
  only when the document's own `batch_outcome.complete` is true.
- `inputs/` holds the generated target and workload files, the fixture record
  files, and `state.json`, the sweep's handle for teardown.

`run_storage_test` writes `state.json` before it submits and again once submit
prints the results directory, and marks it collected once the sweep's resources
are released. `release_storage_test` reads it, so it can release a sweep even
when the run step was killed, including part-way through submit.

## When a run is interrupted

| What happened | What to expect | What to do |
|---|---|---|
| The sweep outlived `perf_wait_timeout_s` or the time budget | `run_storage_test` cancels and collects it and fails with `TIMEOUT` | Raise the time limits, or shrink the sweep |
| isvctl killed `run_storage_test` at its `timeout` | No output from the step; `StoragePerfSweepCompletedCheck` is skipped. Teardown releases the sweep and `StoragePerfSweepReleasedCheck` reports whether it did | Raise the step's `timeout` above its `--time-budget` |
| The run was interrupted (Ctrl-C), so teardown never ran | The sweep keeps running in-cluster and holds the claim | Rerun the same command with `--phase teardown` |
| Cluster API access was lost while waiting | Logged as "state unknown"; the step keeps waiting, since the sweep runs on in-cluster | Nothing, unless access stays lost past the wait |
| Cluster API access was lost during submit or teardown | Release fails and says so. What submit created stays in the cluster, holding the claim, until something reaches the cluster again | Once access is back, run `--phase teardown`, or `--status` on the results directory (below) |
| The machine that ran isvctl is gone, e.g. a CI runner that died mid-sweep | The sweep holds the claim indefinitely: its Lease has no expiry, and storage-scale-test releases it only from the results directory | From any machine with the cluster's kubeconfig, run `--collect` on a copy of the results directory (it need not be at the original path). In CI, keep `_output/storage-perf/` as an artifact even when the job fails |
| Someone cancelled the sweep from outside the run | The run step finishes the cancel and collects it, and fails | Nothing |
| Submit failed in-cluster (`SUBMISSION_FAILED`) | storage-scale-test already removed what it created; nothing to collect | Read the error in the results directory |

## Debugging a sweep

The launcher is `isvtest/vendor/storage-scale-test/storage-tests/fs/nv-elbencho-sweep.sh`
(or the same path under `STORAGE_SCALE_TEST_BASE`). Point the commands at the
results directory from the run step's output or `state.json`, with `KUBECONFIG`
set to the cluster the run used.

```bash
L=isvtest/vendor/storage-scale-test/storage-tests/fs/nv-elbencho-sweep.sh
D=_output/storage-perf/results/elbencho-<timestamp>
$L --status "$D"       # STATE, NEXT_ACTION, execution counts; recovers an interrupted submit
$L --collect "$D"      # copy the results back and release the sweep's resources
$L --cancel "$D"       # stop a running sweep (collect still has to release it)
kubectl get all,leases,networkpolicies -A -l app.kubernetes.io/name=storage-scale-test
```

- Release a sweep only with `--collect` (or `--phase teardown`). Do not delete its
  Job, DaemonSet, Lease or network policies by hand: storage-scale-test journals
  what it creates and expects to remove it itself.
- `NEXT_ACTION` says what to do next: `WAIT`, `COLLECT`, or `INSPECT`. `INSPECT`
  with `STATE=CANCEL_REQUESTED` means run `--cancel` again.
- A lifecycle command such as `--cancel` refuses with "another Kubernetes
  lifecycle operation is active" while another command (for example the run
  step's own `--status`) holds the sweep's lock; retry it.
- Failed lifecycle commands leave `kubernetes/attempts/<attempt>/diagnostics/<operation>-<UTC>/SUMMARY.txt`
  in the results directory, indexing the events, Pod logs and resource
  descriptions they captured.
- The run step's output carries `failed_execution` and `failed_log` for a failed
  cell, and `collect_error` (with the command to finish the job) when the sweep
  still holds resources.

storage-scale-test's
[KUBERNETES_ELBENCHO_LIFECYCLE.md](https://github.com/NVIDIA/storage-scale-test/blob/main/docs/KUBERNETES_ELBENCHO_LIFECYCLE.md)
describes every lifecycle state.
