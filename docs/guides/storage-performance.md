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
# On a cluster that already mounts the filesystem as a claim, also name it:
export STORAGE_PERF_NAMESPACE=storage-perf STORAGE_PERF_CLAIM=perf-claim
uv run isvctl test run \
    -f isvctl/configs/providers/my-isv/config/storage.yaml \
    -f isvctl/configs/providers/my-isv/config/overlays/storage-perf.yaml \
    --capability kubernetes --label performance
```

- The overlay replaces the storage config's step list, so this run does only the
  performance sweep. Run the plain storage config separately for the functional
  checks.
- `setup_cluster` is `scripts/k8s/setup.sh`: it inventories the cluster kubectl
  points at and creates nothing, and pins it for the rest of the run (see
  [The cluster and its claim](#the-cluster-and-its-claim)). Set
  `K8S_EXPECTED_CONTEXT=<context>` to stop the run unless kubectl points at that
  context.
- Always select `--label performance`. Labels choose checks, not steps, but
  without the label the suite's live Kubernetes checks would also run, and some of
  them create claims.
- Needed locally, as storage-scale-test requires them: bash 4.4+, kubectl,
  `flock`, GNU `tar` and GNU `timeout`, and `sha256sum` or `shasum`. On Linux these
  are usually present; on macOS run `brew install bash coreutils gnu-tar flock` and
  put Homebrew's bash first on `PATH`. Its reports need Python 3.12+, which `uv`
  already provides: `make vendor` and `isvctl` both run under the project's Python.
  Elbencho itself runs in the benchmark Pods from the image the release pins.
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
| setup | `setup_cluster` | Provider: the cluster, and the claim and nodes to use (`storage_perf`) |
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

A step that exits 0 has its JSON output validated against a schema in
`isvctl/src/isvctl/config/output_schemas.py` (`storage_perf_tool`, `_filesystem`,
`_claim`, `_run`, `_evaluate`, `_release`, `_fixture_teardown`). A step that reports
success without the fields later steps read (for example `filesystem_id`, or
`claim_name`) fails at that step. A failing step (non-zero exit) is not validated:
isvctl reports the `error` from its JSON as it is.

The sweep runs in-cluster: storage-scale-test only submits it, `--status` reports
its progress, and `--collect` copies the results back and releases what it holds
(the claim's Lease, the coordinator Job, the worker DaemonSet and its network
policies). `run_storage_test` does all three synchronously, so one step covers the
sweep from submit to release.

## The cluster and its claim

**The cluster.** `setup_cluster` writes the current context, credentials
included, to `_output/storage-perf/inputs/kubeconfig` (mode 600; `_output/` is
git-ignored) and reports it as `kubeconfig_path`. Every later step, and
storage-scale-test's own `kubectl` calls, use that file, so the run stays on the
cluster setup checked even if the shell's context changes. `run_storage_test`
copies it once more, to a private file of its own (`inputs/kubeconfig.<random>`,
mode 600), and drives the sweep through that copy; a later `--phase teardown`
releases the sweep through the same copy, so it acts on the sweep's cluster even
after another run repinned `inputs/kubeconfig`. Teardown steps that run without
`setup_cluster`'s output (`--phase teardown`, or a run refused before setup) use
that pinned file, never the shell's current context. A provider that creates its
cluster replaces `setup_cluster` and reports its own `kubeconfig_path`.

**What the sweep uses on it** comes from whoever knows the cluster, through three
settings:

| Setting | As shipped, from | Read by |
|---|---|---|
| `perf_namespace` | `STORAGE_PERF_NAMESPACE`, else `storage-perf` | Both fixtures |
| `perf_claim` | `STORAGE_PERF_CLAIM` | Both fixtures, as `--existing-claim`; empty means they create their own |
| `perf_node_selector` | `STORAGE_PERF_NODE_SELECTOR` | `run_storage_test` |

`--set tests.settings.<name>=...` or an `-f` override file replaces the
environment value; an empty value means the same as an unset variable. A provider
whose `setup_cluster` discovers these from its own APIs reports them in a
`storage_perf` block (`namespace`, `claim`, `node_selector`); a value reported
there wins over the setting. With no node selector from either, setup stops at
`create_perf_filesystem`, before anything is created.

With a claim named, `create_perf_filesystem` reports the volume bound to it and
`provision_perf_storage` checks that it is Bound, ReadWriteMany and
Filesystem-mode. Both only read the claim and report `"created": false`, so
teardown leaves it in place. Neither overwrites a record that says an earlier run
created a fixture and kept it: run `--phase teardown` first, so that fixture is
not left behind.

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
  their record files so a later teardown can still remove them. Teardown also keeps
  them whenever `release_storage_test` did not confirm that the sweep released its
  cluster resources, or reported `abandoned_records` (see
  [One run at a time](#one-run-at-a-time)): deleting the claim under a running sweep
  would leave the sweep impossible to collect. `delete_perf_filesystem` also keeps
  the filesystem unless `deprovision_perf_storage` succeeded.
- `provision_perf_storage` records the kubeconfig and the cluster it points at
  (context and API server, no credentials) in its record file.
  `deprovision_perf_storage` deletes the claim only while its kubeconfig still points
  at that cluster; otherwise it fails, names both, and keeps the record. Keep that
  check in front of your delete.
- The sweep writes only under `scale-test/` inside the claim, as UID/GID 2000 by
  default, so that directory must be writable by them.
- **Record files.** The create and provision steps also write their result to
  `--record` (`_output/storage-perf/inputs/filesystem.json` and `claim.json`).
  `isvctl test run ... --phase teardown` has no setup outputs, so the delete
  steps fall back to those files to find what to remove, and delete the file once
  done. Keep that behaviour when you replace the stubs.
- **Shared helpers.** The stubs import
  `isvctl/configs/providers/my-isv/scripts/common/k8s_claim.py`, which holds the
  parts you should keep: `adopt_claim` (fetch an existing claim and check it is
  usable), `write_record` / `read_record` (the record files),
  `recorded_creation` (refuse setup while an earlier run's fixture is still
  recorded), `cluster_identity` / `other_cluster` (the cluster check above) and
  `kubectl_command` (`KUBECTL`, then `kubectl`, then `microk8s kubectl`, as
  `scripts/k8s/setup.sh` does). Replace only the TODO blocks.

## Settings

Set these in your provider config's `tests.settings`, or per run with
`--set tests.settings.<name>=<value>`.

| Setting | Default | Meaning |
|---|---|---|
| `perf_node_selector` | `$STORAGE_PERF_NODE_SELECTOR` | Labels of the nodes that run the benchmark, e.g. `storage-test=true` |
| `perf_namespace` | `$STORAGE_PERF_NAMESPACE`, else `storage-perf` | Namespace for the claim |
| `perf_claim` | `$STORAGE_PERF_CLAIM` | Existing claim to adopt; empty creates one |
| `perf_nodes` | `1` | Node counts to sweep (storage-scale-test `--nodes` syntax, e.g. `1,2`) |
| `perf_io_sizes` | `r4K,1M` | IO sizes; an `r` prefix means random IO |
| `perf_threads` | `1` | Threads per node |
| `perf_iodepth` | `1` | IO depths |
| `perf_duration_s` | `10` | Seconds per write and read phase |
| `perf_wait_timeout_s` | `auto` | How long `run_storage_test` waits for the sweep: `auto`, seconds, or `0` (no limit); see [Time limits](#time-limits) |
| `perf_wait_cap_s` | `43200` | Longest wait `auto` may choose (at least 900) |
| `perf_stall_timeout_s` | `auto` | How long the sweep may make no progress: `auto` (three cells' time, at least 1800 s) or seconds |
| `perf_unreachable_grace_s` | `1800` | How long the sweep's state may be unreadable before the run stops waiting (at least `perf_status_timeout_s` + 120, for two 60 s status intervals) |
| `perf_status_timeout_s` | `300` | Limit for each storage-scale-test status check (at least 300) |
| `perf_collect_timeout_s` | `8400` | Limit for collecting the sweep, all retries together (at least `perf_status_timeout_s` + 960) |
| `perf_live_interval_ms` | `1000` | elbencho's live statistics interval |
| `perf_live_csv_extended` | `1` | `1` writes the per-interval live CSVs storage-scale-test's fairness reporting reads; `0` does not |
| `perf_min_mbps`, `perf_min_iops` | `0` | Floors; `0` (or empty) reports only |
| `perf_abandon_unreleased` | `false` | `true`: when teardown cannot release a recorded sweep, move its record aside so a run that uses an existing claim can start; see [One run at a time](#one-run-at-a-time) |

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

`perf_wait_timeout_s` sets how long the run waits for the sweep. `auto` (the
default) sizes the wait from the sweep's cells and phase duration, and extends it
if cells take longer than estimated, up to `perf_wait_cap_s`. A number waits that
many seconds. `0` sets no wait limit.

In every mode the run stops waiting if the sweep makes no progress for
`perf_stall_timeout_s`, if the run cannot read the sweep's state for
`perf_unreachable_grace_s`, or if storage-scale-test repeatedly fails to report
the state. When it stops waiting, it cancels the sweep and collects it, which
releases the sweep's cluster resources. The exception is a cluster that cannot be
reached: the run then fails with `UNREACHABLE` without trying, the teardown step
tries again, and the table below says what to run once access is back. If cancel
or collect fails, the run fails and says so, the teardown step tries again, and
both print the command that releases the sweep by hand.

Every time setting is at most 2592000 s (30 days); a longer one is refused before
anything is submitted.

`run_storage_test`, `evaluate_storage_test` and `release_storage_test` have no
isvctl `timeout`. Their own limits bound them: `perf_status_timeout_s` for each
status check, `perf_collect_timeout_s` for collecting (all retries together), and,
for the report extractor, 1800 s or 2 s per cell, whichever is larger.

Each run writes a progress log to `_output/storage-perf/results/run-logs/`, one
line per status check, cancel and collect. isvctl does not print a passing step's
progress, so read this file to see how a long run is doing. It holds no
credentials.

storage-scale-test collects at most 2 GiB (uncompressed) and 50,000 files per
sweep. Before submitting, the run estimates the size of the per-interval live
CSVs. It warns above 1 GiB (in the step result and the run log's `estimate`
line) and refuses above 1.8 GiB. To shrink the estimate, raise
`perf_live_interval_ms`, set `perf_live_csv_extended: "0"` (no live CSVs), or
shrink the sweep. It also refuses a sweep of more than 5,624 cells, which would
exceed the file limit.

Use cluster credentials that stay valid for the whole sweep. If a run is
interrupted (Ctrl-C), its teardown does not run: run the same command with
`--phase teardown` to release the sweep.

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

What to expect from a run of that shape:

- **Size.** Its live CSVs came to about 1.25 GiB, under the 2 GiB collect limit;
  the run warned before submitting, since the estimate (about 1.7 GiB) is
  deliberately high. A 25 ms live interval is what makes them large; keep the
  default 1000 ms unless you need that resolution.
- **Time.** `auto` set the wait from the sweep and never needed to extend it.
  Collecting took under 2 min; `evaluate_storage_test` took about 10 min and
  about 400 MiB of memory on the machine running isvctl.
- **Interruptions.** A loss of cluster access shorter than
  `perf_unreachable_grace_s` (30 min by default) only delays the status checks;
  the run carries on once access is back. Credentials that expire mid-run are
  not covered: renew them before they lapse, or use credentials that outlive the
  sweep.
- **Progress.** Follow the run log in `_output/storage-perf/results/run-logs/`;
  isvctl prints nothing for the step until it ends.

### One run at a time

Runs started from the same directory share `_output/storage-perf/inputs/state.json`,
the record teardown uses to release a sweep. `run_storage_test` and
`release_storage_test` each hold a lock beside it while they run. A run fails with
`REFUSED`, before it submits anything, if another run or teardown holds that lock,
or if the record names an earlier sweep that was never released (for example after
a Ctrl-C). In the second case setup already stops at `find_storage_scale_test`,
before any fixture is created, and the refused run's own teardown releases the
earlier sweep, as `--phase teardown` would. Runs from different directories may
share a storage-scale-test checkout: each holds the checkout's lock while it writes
`env.sh` and submits, and waits for another run's submit to finish.

Only one isvctl run may use a directory at a time. `find_storage_scale_test`, the
first setup step, marks the directory as used by its isvctl process
(`inputs/active-run`) before `setup_cluster` repins `inputs/kubeconfig`, and fails
while another live isvctl process holds that mark. So a second run started from
the same directory stops there, before it can point the first run's later steps at
another cluster, and its own teardown reports `released: false` and leaves the
first run's sweep and fixtures alone. A standalone `--phase teardown` marks the
directory the same way. The mark needs no cleanup: it lapses when its isvctl
process exits, Ctrl-C included. A mark written on another host (a directory shared
between machines) cannot be checked, so it blocks until you delete it; the error
names the file. To run in parallel, start each run from its own directory.

`run_storage_test` drives the sweep through its own copy of the pinned kubeconfig
(`inputs/kubeconfig.<random>`, mode 600, named in `state.json` as
`kubeconfig_snapshot`), and records the copy's current context and API server.
`release_storage_test` uses the same copy, touches the sweep only while it still
points at both, and deletes it once the sweep is released.

If a recorded sweep cannot be released (for example the cluster is gone), every
later run is refused. Set `perf_abandon_unreleased: "true"` and run `--phase
teardown`: it tries a normal release first, and if that fails it renames the
record to `state.abandoned-<UTC>.json`. The step still fails, and prints the
commands that release the sweep by hand: `--cancel` first, since a sweep that was
never cancelled is usually still running and `--collect` refuses a running sweep,
then `--collect`. Do not delete `state.json` yourself.

The abandoned sweep may still use the claim and filesystem, so while any
`state.abandoned-*.json` exists, `release_storage_test` lists it in
`abandoned_records` and teardown keeps the fixtures and their record files. A run
that uses an existing claim (`perf_claim`) can start meanwhile; a run that would
create its fixtures is refused at setup while those records are kept. Once you have
collected the abandoned sweep with the printed `--collect` command, delete its
`state.abandoned-<UTC>.json` and the kubeconfig copy the message names (the printed
commands use it), and the next `--phase teardown` removes the fixtures.

`release_storage_test` also refuses, with `released: false`, a `state.json` it
cannot read (not JSON, not a JSON object, or unreadable), and setup and the run
refuse it too: it may be a damaged record of a sweep that still holds the claim.
Abandoning does not apply, since it needs a readable record; see the table below.

## Results

Each run writes under `_output/storage-perf/` in the directory isvctl runs from:

- `results/elbencho-<UTC timestamp>/` is storage-scale-test's results directory:
  logs, per-cell CSVs, its reports, and `results.json`, the machine-readable
  document `evaluate_storage_test` reads (see storage-scale-test's
  [MACHINE_READABLE_RESULTS.md](https://github.com/NVIDIA/storage-scale-test/blob/main/docs/MACHINE_READABLE_RESULTS.md),
  including its jq spot checks). `evaluate_storage_test` passes `results_present`
  only when the document's own `batch_outcome.complete` is true.
- `inputs/` holds the generated target and workload files, the fixture record
  files, the pinned kubeconfig, and `state.json`, the sweep's handle for teardown,
  with its lock file (`.lock`), `active-run` (the isvctl run using the directory),
  the sweep's own kubeconfig copy (`kubeconfig.<random>`) until it is released, and,
  after a Ctrl-C, `interrupted`, the time of the interruption.

`run_storage_test` writes `state.json` before it submits and again once submit
prints the results directory, and marks it collected once the sweep's resources
are released. `release_storage_test` reads it, so it can release a sweep even
when the run step was killed, including part-way through submit.

## When a run is interrupted

| What happened | What to expect | What to do |
|---|---|---|
| The sweep outlived its wait (`perf_wait_timeout_s`) | `run_storage_test` cancels and collects it and fails with `TIMEOUT` | Raise `perf_wait_timeout_s` (or `perf_wait_cap_s` for `auto`), or shrink the sweep |
| The sweep made no progress for `perf_stall_timeout_s` | `run_storage_test` cancels and collects it and fails with `STALLED` | Look at the results directory and the run log; raise `perf_stall_timeout_s` only for cells that legitimately take longer |
| storage-scale-test could not report the sweep's state (`STATUS_FAILED`) | `run_storage_test` cancels and collects it once; its error carries storage-scale-test's own explanation | Follow that explanation; the run log has each status check's reason |
| The run was interrupted (Ctrl-C), so teardown never ran | The sweep keeps running in-cluster and holds the claim; `inputs/interrupted` records when | Rerun the same command with `--phase teardown` |
| An earlier sweep was never released (`REFUSED`) | Setup stops before creating anything, and this run's teardown releases the earlier sweep | Nothing if teardown reports `released: true`; rerun. Otherwise fix what its error names and run `--phase teardown`, or abandon the record (see [One run at a time](#one-run-at-a-time)) |
| Another run or teardown is using the state file (`REFUSED`) | Nothing is submitted; this run's teardown leaves the other run's sweep alone while that run holds the lock | Wait for the other run, or start this one from another directory |
| Another isvctl run is using this directory | `find_storage_scale_test` fails before `setup_cluster` repins anything; this run's teardown reports `released: false` and leaves the other run's sweep and fixtures alone | Wait for the other run, or start this one from another directory. If the error names a run that is no longer running (for example on a host that is gone), delete `inputs/active-run` |
| The sweep's kubeconfig copy (`kubeconfig_snapshot` in `state.json`) is missing or no longer points at its cluster | `release_storage_test` refuses to act and reports `released: false` | Put a kubeconfig for the original cluster at that path (mode 600) and run `--phase teardown` |
| `state.json` cannot be read (not JSON, not a JSON object, or unreadable) | Setup, the run and teardown all refuse and name the file; teardown reports `released: false` | Check whether a sweep still holds cluster resources (`kubectl get all,leases,networkpolicies -A -l app.kubernetes.io/name=storage-scale-test`) and release it by hand from its results directory, then move the file aside |
| Submit was killed before naming its results directory, and more than one new `elbencho-*` directory appeared since | Teardown does not guess: it reports `released: false` and lists them; setup and the run refuse with the same list | Run `--status` on each listed directory, then `--cancel` (if still running) and `--collect` on this run's sweep, then move `state.json` aside |
| Cluster API access was lost while waiting | Logged in the run log; the step keeps waiting, since the sweep runs on in-cluster | Nothing, unless access stays lost for `perf_unreachable_grace_s` |
| Cluster API access stayed lost for `perf_unreachable_grace_s` (`UNREACHABLE`) | `run_storage_test` fails without cancelling or collecting; teardown tries to release the sweep and fails while the cluster is out of reach | Once access is back, run the same command with `--phase teardown` |
| Cluster API access was lost during submit or teardown | Release fails and says so. What submit created stays in the cluster, holding the claim, until something reaches the cluster again | Once access is back, run `--phase teardown`, or `--status` on the results directory (below) |
| The machine that ran isvctl is gone, e.g. a CI runner that died mid-sweep | The sweep holds the claim indefinitely: its Lease has no expiry, and storage-scale-test releases it only from the results directory | From any machine with the cluster's kubeconfig, run `--status` on a copy of the results directory (it need not be at the original path). If the sweep is still running, run `--cancel` (again, until `--status` shows it stopped), then `--collect`. In CI, keep `_output/storage-perf/results/` as an artifact even when the job fails. Never archive `_output/storage-perf/inputs/`: it holds the pinned kubeconfig, the sweep's copy of it, and their credentials |
| The local results directory was deleted while its sweep was still recorded | Setup refuses, and `release_storage_test` reports `released: false`: storage-scale-test releases a sweep only from its results directory | Restore a copy to the recorded path and run `--phase teardown`, or run `--status` on the copy, then `--cancel` (until it stops) if it is still running, then `--collect`; with no copy left, remove what the sweep holds by hand, then abandon the record (see [One run at a time](#one-run-at-a-time)) |
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
  what it creates and expects to remove it itself. The one exception is a sweep
  whose results directory is lost with no copy left, which storage-scale-test can
  no longer release.
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
