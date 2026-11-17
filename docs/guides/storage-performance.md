# Storage Performance

The storage suite's Performance group runs an IO benchmark sweep against an
existing claim on a high-speed filesystem mounted in a Kubernetes cluster, and
checks that the sweep ran to completion, released everything it held, and
reported real write and read numbers. The benchmark is
[storage-scale-test](https://github.com/NVIDIA/storage-scale-test) (elbencho on
Kubernetes); the shared steps in `isvctl/configs/shared/storage/perf/` drive it,
so a provider only names the claim and the nodes to use.

There is no official performance bar. A pass means the sweep ran and reported
numbers; a provider can set its own floors (see [Floors](#floors)).

## Running it

```bash
make vendor                           # once: pinned test tools, incl. storage-scale-test, into isvtest/vendor/
export STORAGE_PERF_CLAIM=storage-perf/perf-claim      # an existing claim, as namespace/claim
export STORAGE_PERF_NODE_SELECTOR=storage-test=true    # the nodes that run the benchmark
uv run isvctl test run \
    -f isvctl/configs/providers/my-isv/config/storage.yaml \
    -f isvctl/configs/providers/my-isv/config/overlays/storage-perf.yaml \
    --capability kubernetes --label performance
```

- The overlay replaces the storage config's step list, so this run does only the
  performance sweep. Run the plain storage config separately for the functional
  checks.
- The claim must be Bound, ReadWriteMany and Filesystem-mode, and mountable on
  every selected node. The sweep writes only under `scale-test/` inside it, as
  UID/GID 2000, so that directory must be writable by them.
- `setup_cluster` is `scripts/k8s/setup.sh`: it inventories the cluster kubectl
  points at and creates nothing. Check kubectl's context before you start (see
  [The cluster](#the-cluster)).
- Always select `--label performance`. Labels choose checks, not steps, but
  without the label the suite's live Kubernetes checks would also run, and some of
  them create claims.
- Needed locally, as storage-scale-test requires them: bash 4.4+, kubectl,
  `flock`, GNU `tar` and GNU `timeout`, and `sha256sum` or `shasum`. On Linux these
  are usually present; on macOS run `brew install bash coreutils gnu-tar flock` and
  put Homebrew's bash first on `PATH`. Its reports need Python 3.12+, which `uv`
  already provides. Elbencho itself runs in the benchmark Pods from the image the
  release pins.
- `STORAGE_SCALE_TEST_BASE=/path/to/checkout` uses another storage-scale-test tree
  (v2.1.0 or later). isvctl writes that tree's `env.sh`, so dedicate the tree to
  isvctl.
- `make clean-vendor` removes the vendored tools (`make clean` leaves them), and
  `make vendor` downloads them again. `make vendor` also checks the release with its own
  `lib/project_version.sh` (every file against its `SOURCE_SHA256`) and prepares the Python
  environment its report extractor uses.

## What runs

The sweep runs in-cluster: storage-scale-test only submits it, `--status` reports
its progress, and `--collect` copies the results back and releases what it holds
(the claim's Lease, the coordinator Job, the worker DaemonSet and its network
policies). `run_storage_test` does all three synchronously, so one step covers the
sweep from submit to release.

| Phase | Step | Does |
|---|---|---|
| setup | `setup_cluster` | Provider: inventories the cluster; may also report its kubeconfig and the claim and nodes to use (`storage_perf`) |
| test | `run_storage_test` | Submits the sweep, waits for it, collects it |
| test | `evaluate_storage_test` | Reads the results and compares them with the floors |
| teardown | `release_storage_test` | Releases every sweep under the results root that still holds cluster resources |
| teardown | `teardown_cluster` | Provider: removes what setup created (nothing, as shipped) |

| Check | Test ID | Step | Passes when |
|---|---|---|---|
| `StoragePerfSweepCompletedCheck` | `SPERF03-01` | `run_storage_test` | The sweep finished with state `SUCCESS` |
| `StoragePerfSweepReleasedCheck` | `SPERF05-01` | `release_storage_test` | No sweep still holds cluster resources |
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

A step that exits 0 has its JSON output validated against a schema in
`isvctl/src/isvctl/config/output_schemas.py` (`storage_perf_run`, `_evaluate`,
`_release`). A failing step (non-zero exit) is not validated: isvctl reports the
`error` from its JSON as it is.

## The cluster

Every step, and storage-scale-test's own `kubectl` calls, use `KUBECONFIG`, or
kubectl's current context when it is unset. So run `--phase teardown` with the same kubeconfig and context as the
run, and do not switch context while a run is going. To keep a whole run on one
cluster, point `KUBECONFIG` at a file for that cluster. storage-scale-test also
checks that each lifecycle command reaches the cluster its sweep runs on. A
provider that creates its cluster replaces `setup_cluster` and reports that
cluster's `kubeconfig_path`, which the run and teardown steps then use.

## Settings

Set these in your provider config's `tests.settings`, or per run with
`--set tests.settings.<name>=<value>`. An empty value means the same as an unset
variable.

| Setting | Default | Meaning |
|---|---|---|
| `perf_claim` | `$STORAGE_PERF_CLAIM` | Required. The existing claim to test, as `namespace/claim` |
| `perf_node_selector` | `$STORAGE_PERF_NODE_SELECTOR` | Required. Labels of the nodes that run the benchmark, e.g. `storage-test=true` |
| `perf_nodes` | `1` | Node counts to sweep (storage-scale-test `--nodes` syntax, e.g. `1,2`) |
| `perf_io_sizes` | `r4K,1M` | IO sizes; an `r` prefix means random IO. No `WRITE,READ` pairs: storage-scale-test v2.1.0 cannot collect them on Kubernetes |
| `perf_threads` | `1` | Threads per node |
| `perf_iodepth` | `1` | IO depths |
| `perf_duration_s` | `10` | Seconds per write and read phase |
| `perf_wait_timeout_s` | `auto` | How long `run_storage_test` waits for the sweep: `auto` or seconds, at most 12 h; see [Time limits](#time-limits) |
| `perf_live_interval_ms` | `1000` | elbencho's live statistics interval; its per-interval CSVs feed storage-scale-test's own reports and are what grows a collect |
| `perf_min_mbps`, `perf_min_iops` | `0` | Floors; `0` (or empty) reports only |

storage-scale-test checks these values itself before it touches the cluster; a bad
one is rejected (`NOT_SUBMITTED`) with its errors, which name its own variables
(`ELBENCHO_SCALE_THREAD_LIST` for `perf_threads`, say).

A provider whose `setup_cluster` discovers the claim and nodes from its own APIs
reports them in a `storage_perf` block (`claim` as `namespace/claim`,
`node_selector`); a value reported there wins over the setting. Without a claim or
a node selector from either, `run_storage_test` fails before it submits anything.

The defaults are a smoke run: one node, two IO sizes, 10 s per phase. It proves
the sweep runs and reports real numbers, not sustained performance. For longer
sweeps, storage-scale-test's
[BENCHMARK_RECIPES_FILESYSTEM.md](https://github.com/NVIDIA/storage-scale-test/blob/v2.1.0/BENCHMARK_RECIPES_FILESYSTEM.md)
has ready-made shapes, starting with its quick validation recipe.

### Floors

`evaluate_storage_test` reports a headline per operation: the best bandwidth
(MB/s, 10^6 bytes) and IOPS across the sweep's cells, and the lowest average
latency. The floor checks compare the slower operation's best result with
`perf_min_mbps` and `perf_min_iops`. With a floor of `0` they pass and only report
the numbers. Set floors from your own expectations for the sweep you configure: a
small random IO size gives far higher IOPS and far lower bandwidth than 1M.

### Time limits

The deadline (`perf_wait_timeout_s`) limits only how long `run_storage_test` waits
for the sweep to finish. It counts from the end of submit (up to 600 s), not its start:

1. The run submits the sweep and checks its status every 60 s. A check that cannot
   read the state (the cluster is out of reach, say) says nothing about the sweep,
   which runs on in-cluster, so the run just checks again; the progress log records
   each miss.
2. If the sweep finishes before the deadline, the run collects it (which releases
   its cluster resources) and reports its state.
3. If the deadline passes first, the run cancels the sweep, then collects what
   finished, and fails with `TIMEOUT`, unless the sweep finished before the cancel
   reached it. The last wait is cut short at the deadline, but a status check already
   running can take up to 300 s, so the cancel can start up to about 5 minutes late.
   If the cancel or collect fails (the cluster is still out of reach, say), the run says so and
   the teardown step tries again.

`auto` (the default) sizes the deadline from the sweep's cells and phase duration:
twice the estimate, at least 900 s and at most 12 h. A sweep `auto` cannot fit in
12 h is rejected (`NOT_SUBMITTED`). A number waits that many seconds, up to 12 h. A
status error that persists (for example after the claim was replaced) also just
waits out the deadline.

The steps have no isvctl `timeout`; each storage-scale-test call has its own limit:
300 s for a status check, 600 s for submit and for each cancel (up to 3), and 3900 s
for each collect (up to 2), each confirmed by a status check. So each step has its
own bound:

| Step | At most |
|---|---|
| `run_storage_test` | the deadline, plus about 3.5 h to submit, cancel and collect |
| `evaluate_storage_test` | the report extractor's limit: 1800 s, or 2 s per cell if larger |
| `release_storage_test` | about 3.5 h for each sweep it releases (they are released one at a time) |

Set any CI job limit above the sum, or teardown can be cut short. In practice these
limits are rarely reached: storage-scale-test gives up on each kubectl call that
cannot reach the cluster after 30 s.

Each run writes a progress log to `_output/storage-perf/results/run-logs/`, one
line per estimate, submit, status check, cancel and collect. isvctl does not print
a passing step's progress, so read this file to see how a long run is doing. It
holds no credentials.

storage-scale-test collects at most 50,000 files per sweep, 8 per cell, so the run
rejects (`NOT_SUBMITTED`) a sweep of more than 5,624 cells, 10% under that limit.
It also collects at most 2 GiB; the per-interval live CSVs are what grows with a
long sweep, so raise `perf_live_interval_ms` if a collect fails on size.

Use cluster credentials that stay valid for the whole sweep.

### Long runs

A multi-hour run is the same pipeline with larger settings; nothing else changes.
For example, this sweep of four cells at 900 s per phase ran for about 2 h 15 min
on one node:

```bash
uv run isvctl test run ... --label performance \
    --set tests.settings.perf_nodes=1 \
    --set tests.settings.perf_io_sizes=r4K,4K,r8K,8K \
    --set tests.settings.perf_threads=63 \
    --set tests.settings.perf_iodepth=1 \
    --set tests.settings.perf_duration_s=900 \
    --set tests.settings.perf_live_interval_ms=25
```

Its live CSVs came to about 1.25 GiB, under the 2 GiB collect limit; a 25 ms live
interval is what makes them large, so keep the default 1000 ms unless you need
that resolution. Collecting took under 2 min; `evaluate_storage_test` took about
10 min and about 400 MiB of memory on the machine running isvctl.

## Locks and unreleased sweeps

Each sweep's lifecycle lives in its own sweep directory
(`_output/storage-perf/results/elbencho-<UTC timestamp>/`), and the teardown step
reads it with `--status`. A sweep holds nothing once it is collected
(`RESULT_COLLECTION=COMPLETE`) or rolled back (`SUBMISSION_FAILED`), or when
submit stopped before it created anything (no `kubernetes/current-attempt`). Any
other sweep, including one whose status cannot be read, is unreleased: it may
still hold the claim. `release_storage_test` cancels and collects every
unreleased sweep under the results root, not just this run's. A collected sweep
answers from its own files; any other costs one status check against the
cluster, until you remove its directory.

storage-scale-test allows one sweep per claim at a time (a Lease on the claim).
While an earlier sweep still holds it (for example after a Ctrl-C), submit fails
with `PVC <claim> is reserved by Kubernetes attempt ...` and rolls back.
storage-scale-test suggests retrying, but a retry fails until the earlier sweep
is released, so the run step's error says so. If that sweep was left in this
results root, the teardown step (`release_storage_test`) then releases it, and
the next run goes ahead; a sweep started from another directory is that
directory's to release.

`run_storage_test` and `release_storage_test` each hold a lock in the results
root (`_output/storage-perf/results/.lock`) while they run, so a teardown never
cancels a sweep a run in the same directory is still waiting on. A run fails with
`REFUSED`, before it submits anything, if another run or teardown holds it; the
teardown step then leaves that run's sweeps alone. Run `--phase teardown` from the
run's directory: a teardown that finds no results root there reports
`no results root at <path>` and releases nothing. The lock protects the sweeps, not
the whole run: start one isvctl run per directory at a time. To run in parallel,
start each run from its own directory.

Runs from different directories may share a storage-scale-test checkout: the
launcher reads its settings from `env.sh` in the checkout root, so each run holds
the checkout's lock while it writes `env.sh` and submits. It waits for another
run's submit for up to one submit's time (about 10 min), then fails with
`REFUSED`.

## Results

Each run writes under `_output/storage-perf/` in the directory isvctl runs from:

- `results/elbencho-<UTC timestamp>/` is a sweep directory, storage-scale-test's results:
  logs, per-cell CSVs, its reports, and `results.json`, the machine-readable
  document `evaluate_storage_test` reads (see storage-scale-test's
  [MACHINE_READABLE_RESULTS.md](https://github.com/NVIDIA/storage-scale-test/blob/v2.1.0/docs/MACHINE_READABLE_RESULTS.md),
  including its jq spot checks). `evaluate_storage_test` passes `results_present`
  only when the document's own `batch_outcome.complete` is true.
- `results/` also holds the generated target and workload files, the progress
  logs (`run-logs/`) and the steps' lock file (`.lock`).

In CI, keep `_output/storage-perf/results/` as an artifact even when the job
fails, so a sweep can still be released from a copy of its sweep directory.

To evaluate a sweep that the teardown step collected (the run step was interrupted), run
the evaluate step on its sweep directory; it writes `results.json` and
`report.txt` there, and reports the headline numbers and `results_present`:

```bash
python isvctl/configs/shared/storage/perf/evaluate_storage_test.py \
    --run '{"results_dir": "_output/storage-perf/results/elbencho-<timestamp>"}'
```

## When a run fails

`run_storage_test` reports storage-scale-test's own `STATE` (for example
`SUCCESS`, `FAILED`, `CANCELLED` or `SUBMISSION_FAILED`), or one of its own:
`NOT_SUBMITTED` (rejected before submit because a setting was invalid or the sweep
was too large, or submit failed), `REFUSED` (another run or teardown holds the results root's lock, or
another run is submitting from the same checkout) or `TIMEOUT` (the sweep outlived
its wait, and was cancelled and collected).

| What happened | What to do |
|---|---|
| The run was interrupted (Ctrl-C), so teardown never ran. The sweep keeps running in-cluster and holds the claim; isvctl does not show the interrupted step's output, so the rerun hint is the last line of the progress log | Rerun the same command with `--phase teardown`, from the same directory |
| Rejected before submit (`NOT_SUBMITTED`): a setting is invalid or the sweep is too large | Fix the setting the error names |
| Submit failed (`NOT_SUBMITTED`) because an earlier sweep holds the claim | Rerun; the teardown step has already released it if it was in this results root |
| The sweep outlived its wait (`TIMEOUT`) | Raise `perf_wait_timeout_s`, or shrink the sweep |
| Teardown reported `released: false`: the cluster was out of reach, or `KUBECONFIG` or the context now points at another cluster | Once the cluster answers, run `--phase teardown` with the run's kubeconfig and context |
| Teardown reported `released: false` because another run or teardown holds the results root's lock | Wait for it to finish, then run `--phase teardown` |
| The claim was deleted or replaced while its sweep still ran (`IDENTITY_MISMATCH`), the sweep directory was lost, or the machine that ran isvctl is gone | See [Releasing a sweep teardown cannot](#releasing-a-sweep-teardown-cannot) |

## Debugging a sweep

The launcher is `isvtest/vendor/storage-scale-test/storage-tests/fs/nv-elbencho-sweep.sh`
(or the same path under `STORAGE_SCALE_TEST_BASE`). Point the commands at the
run step's `results_dir` (its sweep directory), or the newest under
`_output/storage-perf/results/`, with `KUBECONFIG` set to the cluster the run
used.

```bash
L=isvtest/vendor/storage-scale-test/storage-tests/fs/nv-elbencho-sweep.sh
D=_output/storage-perf/results/elbencho-<timestamp>
$L --status "$D"       # STATE, NEXT_ACTION, execution counts; finishes rolling back an interrupted submit
$L --collect "$D"      # copy the results back and release the sweep's resources
$L --cancel "$D"       # stop a running sweep (collect still has to release it)
kubectl get all,leases,networkpolicies -A -l app.kubernetes.io/name=storage-scale-test
```

- Release a sweep only with `--collect` (or `--phase teardown`). Do not delete its
  Job, DaemonSet, Lease or network policies by hand: storage-scale-test journals
  what it creates and expects to remove it itself. For the exceptions, see
  [Releasing a sweep teardown cannot](#releasing-a-sweep-teardown-cannot).
- The run step's output carries `failed_execution` and `failed_log` for a failed
  cell, and `collect_error` (with the command to finish the job) when the sweep
  still holds resources.

Links to storage-scale-test's docs are to the pinned release
(`STORAGE_SCALE_TEST_VERSION` in the Makefile). It documents the rest: what
`NEXT_ACTION` asks for, lock refusals and the diagnostics a failed command leaves
in the sweep directory, in
[FILESYSTEM_TESTING.md](https://github.com/NVIDIA/storage-scale-test/blob/v2.1.0/docs/FILESYSTEM_TESTING.md#kubernetes-access-and-troubleshooting),
and every lifecycle state in
[KUBERNETES_ELBENCHO_LIFECYCLE.md](https://github.com/NVIDIA/storage-scale-test/blob/v2.1.0/docs/KUBERNETES_ELBENCHO_LIFECYCLE.md).

### Releasing a sweep teardown cannot

storage-scale-test releases a sweep only from its sweep directory, on the claim it
started on. From any machine with the cluster's kubeconfig, run `--status`,
`--cancel` (until it stops) and `--collect` on a copy of the sweep directory. With
no copy left, or with the claim gone, remove what the sweep holds by hand (the
`kubectl get` above lists it), then remove its sweep directory.
