# kube-reaper

[![ci](https://github.com/Inblade/kube-reaper/actions/workflows/ci.yml/badge.svg)](https://github.com/Inblade/kube-reaper/actions/workflows/ci.yml)
[![Go 1.26](https://img.shields.io/badge/go-1.26-00ADD8?logo=go)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A small, cluster-wide **janitor operator** for Kubernetes. It reclaims pods that
get stuck (terminating, evicted, failed) and cleans up finished or stuck Jobs —
the debris that accumulates on busy clusters and slowly eats scheduler headroom,
`kubectl get pods` readability and, eventually, resource quotas.

Written in Go with `client-go` (no CRD), it runs a single active leader with
hot-standby replicas, rate-limits its deletions, exports Prometheus metrics, and
never touches namespaces on a denylist.

> Why not just `ttlSecondsAfterFinished` or `kube-janitor`? See
> [Design decisions](#design-decisions) — short version: native TTL only covers
> *finished* Jobs and must be set per-object; this centralises the policy and
> also handles evicted/terminating pods and **status-less "stuck" Jobs** that
> native mechanisms leave behind.

## What it cleans

| Task | Selection | Default interval |
|------|-----------|------------------|
| `terminating_pods` | `metadata.deletionTimestamp != nil` (stuck Terminating) | 20m |
| `evicted_pods`     | `status.reason == "Evicted"` | 15m |
| `failed_pods`      | `status.phase == Failed` | 30m |
| `succeeded_jobs`   | `succeeded > 0 && active == 0` | 1h |
| `failed_jobs`      | `failed > 0 && active == 0`, **or** status-less & older than grace (`stuck`) | 5m |

Failed/stuck Jobs are removed with **foreground propagation**, and their leftover
pods (`job-name=<name>`) are deleted explicitly.

## How it works

```
main
 ├─ metrics server  :8080/metrics
 ├─ health server   :8081/healthz,/readyz
 └─ leader election (Lease, coordination.k8s.io)
      └─ (leader only) Reaper.Run
           ├─ goroutine: terminating_pods loop ─┐
           ├─ goroutine: evicted_pods loop      │  each: List (paged) → filter
           ├─ goroutine: failed_pods loop       ├─ → rate-limited Delete
           ├─ goroutine: succeeded_jobs loop    │  shared token-bucket limiter
           └─ goroutine: failed_jobs loop ──────┘
```

- **Leader election** so N replicas don't duplicate work against the API server;
  standbys take over on leader loss (`ReleaseOnCancel`).
- **Paged listing** (`Limit`/`Continue`) bounds memory and per-call API/etcd load.
- **Shared rate limiter** (`golang.org/x/time/rate`) caps the aggregate delete
  QPS so a big sweep can't overwhelm the API server.
- **Idempotent deletes** — `NotFound` is treated as success, so repeated or
  concurrent runs are safe.
- **Dry-run** logs what it *would* delete (and increments a `*_dryrun` metric)
  for safe rollout.

## Metrics

| Metric | Type | Labels |
|--------|------|--------|
| `reaper_deletions_total` | counter | `kind, namespace, reason` |
| `reaper_deletion_errors_total` | counter | `kind, namespace, error_type` |
| `reaper_task_runs_total` | counter | `task, outcome` |
| `reaper_task_last_run_timestamp_seconds` | gauge | `task` |
| `reaper_task_duration_seconds` | histogram | `task` |
| `reaper_is_leader` | gauge | — |

Useful alerts: task hasn't completed in `N × interval`
(`time() - reaper_task_last_run_timestamp_seconds{task="failed_jobs"} > ...`),
or a rising `reaper_deletion_errors_total{error_type="forbidden"}` (RBAC drift).

## Configuration

All via environment variables (12-factor); defaults in parentheses.

| Var | Default | |
|-----|---------|--|
| `REAPER_TERMINATING_INTERVAL` | `20m` | per-task intervals |
| `REAPER_EVICTED_INTERVAL` | `15m` | |
| `REAPER_FAILED_POD_INTERVAL` | `30m` | |
| `REAPER_SUCCEEDED_JOB_INTERVAL` | `1h` | |
| `REAPER_FAILED_JOB_INTERVAL` | `5m` | |
| `REAPER_STUCK_JOB_GRACE` | `2m` | age before a status-less Job counts as stuck |
| `REAPER_DENY_NAMESPACES` | `kube-system` | comma-separated, never touched |
| `REAPER_DELETE_QPS` / `REAPER_DELETE_BURST` | `10` / `20` | delete rate limit |
| `REAPER_LIST_PAGE_SIZE` | `500` | List page size |
| `REAPER_DRY_RUN` | `false` | also via `--dry-run` flag |
| `REAPER_LEADER_ELECTION` | `true` | disable to run standalone (local dev) |

## Install

```bash
kubectl apply -f deploy/rbac.yaml
kubectl apply -f deploy/deployment.yaml
# observe first:
kubectl -n kube-reaper set env deploy/kube-reaper REAPER_DRY_RUN=true
```

RBAC is least-privilege: cluster-wide `list`/`delete` on pods and jobs only, plus
`leases` in its own namespace for leader election.

## Run locally

```bash
make test          # unit tests (fake clientset)
make run-dry       # runs against your kubeconfig, dry-run, no leader election
```

## Design decisions

**Why `client-go` polling and not controller-runtime + a CRD?**
There's no user-facing desired state to reconcile — the policy lives in config,
not in a custom resource. A CRD/webhook would add surface area for no benefit. If
per-namespace policies were needed, a `ReapPolicy` CR reconciled by
controller-runtime would be the right call; that trade-off isn't worth it here.

**Why not native `ttlSecondsAfterFinished`?**
It only covers *finished* Jobs and has to be set on every Job. kube-reaper
centralises the policy without editing manifests, and additionally handles cases
native TTL doesn't: evicted/terminating pods and status-less **stuck** Jobs
(scheduling/admission failures that never produce a status).

**Why List-all each cycle instead of an informer/watch?**
Simplicity and predictable, cache-free memory. Paging keeps peak API/etcd cost
bounded. On very large clusters the next step is a field selector
(`status.phase=Failed`) or a shared informer with a local cache — a deliberate,
documented trade-off rather than premature optimisation.

## Limitations

- List-all scales linearly with object count; see above for the informer path.
- Force-deleting terminating pods (`gracePeriod=0`) can *mask* a real problem
  (dead node / hung finalizer) — pair it with alerting, don't only sweep.
- No metric yet for objects skipped by the denylist.

## License

MIT — see [LICENSE](LICENSE).
