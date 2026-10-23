# Metrics

The watcher serves [Prometheus](https://prometheus.io) metrics at `GET /metrics` on
[`LISTEN_ADDR`](configuration.md#listen_addr) (`:8080` by default), next to `/healthz`, in the Prometheus text
format. `run --dry-run` serves no HTTP and so no metrics. `/healthz` is described in
[configuration.md](configuration.md#health-check).

The endpoint has no authentication and no TLS, and the metrics name your source paths and containers. Keep
`LISTEN_ADDR` on a network you trust: scrape it from the Docker network the watcher runs in rather than publishing
the port.

## Summary

| Metric | Type | Labels | What it tells |
|---|---|---|---|
| [`kopia_watcher_last_success_timestamp_seconds`](#kopia_watcher_last_success_timestamp_seconds) | gauge | `path` | Start of the source's last complete snapshot, scheduled ones included |
| [`kopia_watcher_snapshot_duration_seconds`](#kopia_watcher_snapshot_duration_seconds) | histogram | — | How long the watcher's own snapshots ran |
| [`kopia_watcher_snapshot_failures_total`](#kopia_watcher_snapshot_failures_total) | counter | `path` | The watcher's own snapshots that saved no complete snapshot |
| [`kopia_watcher_reconcile_errors_total`](#kopia_watcher_reconcile_errors_total) | counter | `kind` | Problems of the reconciler |
| [`kopia_watcher_managed_sources`](#kopia_watcher_managed_sources-and-kopia_watcher_source_paused) | gauge | `state` | Managed sources, active and paused |
| [`kopia_watcher_source_paused`](#kopia_watcher_managed_sources-and-kopia_watcher_source_paused) | gauge | `path` | 1 while a source is paused, 0 while it is active |
| [`kopia_watcher_source_owner`](#kopia_watcher_source_owner) | gauge | `path`, `container` | 1 for each container that backs up a source |
| [`kopia_watcher_notifications_total`](#kopia_watcher_notifications_total) | counter | `result` | Notifications to `NOTIFY_WEBHOOK_URL`: sent, failed, dropped |
| `kopia_watcher_build_info` | gauge | `version` | Always 1; the version of the watcher |

The metrics of the Go runtime (`go_*`) and of the process (`process_*`) come with them.

The labels hold only source paths and container names, never IDs of tasks, snapshots or containers, so the
number of series grows with your sources and containers and with nothing else.

## Sources

A source counts as managed once a reconcile pass has brought its policy in line with the labels of its
containers. The first full reconcile, right after the watcher has reached kopia server, does that for every
source, so the series of the sources appear shortly after the watcher starts, also after a restart, whose
counters start again from 0.

`path` is the path of the source as kopia server sees it, after [`HOST_PREFIX`](configuration.md#host_prefix):
the path the logs and the Kopia web UI show. Sources of another kopia server identity in `STATE_FILE`, and
sources outside [`WATCHER_PATH_PREFIX`](configuration.md#watcher_path_prefix), are not managed and have no
series.

When the watcher stops managing a source, its series go with the pass over its path that finds out, instead of
staying at their last value: when `kopia.cleanup-on-remove` has deleted the source, kopia server no longer knows
it or refused to register it, or `WATCHER_PATH_PREFIX` no longer covers it. A pass that fails before it can
tell, say while kopia server is down, leaves the series as they were.

## Metrics

### `kopia_watcher_last_success_timestamp_seconds`

The start time of the source's last complete snapshot, in seconds since the epoch, by kopia server's clock.
Complete means kopia server saved the snapshot without an incomplete reason: a checkpoint of an upload that is
still running or a canceled snapshot does not count, while a snapshot kopia server saved complete with errors in
some files does.

It counts every snapshot of the source, not only the watcher's own: kopia server takes most of them on
schedule, and snapshots taken in the web UI or with the kopia CLI count too. The watcher reads it from kopia
server's status of the source at every pass over its path, so at least every
[`RECONCILE_INTERVAL`](configuration.md#reconcile_interval), and right after each of its own snapshots ends.
When the last snapshot is a checkpoint or incomplete otherwise, the watcher lists the source's snapshots to find
the last complete one, once for each such last snapshot.

A source without a complete snapshot, such as one just registered, has no series.

### `kopia_watcher_snapshot_duration_seconds`

How long the snapshots the watcher queued ran: at a container's start or stop, at the watcher's start with
[`SNAPSHOT_ON_WATCHER_START`](configuration.md#snapshot_on_watcher_start), or once more after kopia server lost
a run. It is the time from the start to the end of the snapshot task in kopia server, so a snapshot that waited
for an upload slot does not count its wait.

Only snapshots that ran to the end count: those that saved a snapshot, complete, incomplete or with errors, and
those that saved none because nothing had changed (with `ignoreIdenticalSnapshots` in the policy). Failed and
canceled snapshots and lost runs count in `kopia_watcher_snapshot_failures_total` only. Snapshots kopia server
takes on schedule do not count: the watcher does not follow them.

The buckets are 1 s, 5 s, 15 s, 30 s, 1 min, 5 min, 15 min, 30 min, 1 h, 2 h, 4 h and 8 h. There is no `path`
label, to keep the number of series low; kopia server's task list shows the time of each snapshot.

### `kopia_watcher_snapshot_failures_total`

The snapshots the watcher queued of a source that saved no complete snapshot:

- the task failed;
- the upload was canceled, in the web UI or by a pause;
- the snapshot is incomplete, or has errors that the policy does not ignore;
- kopia server lost the run, as when it restarts, and the watcher finds no snapshot of it. The watcher then
  takes the snapshot once more, and that run counts again if it fails too. A run that kopia server only lost
  track of does not count: its task can leave kopia server's list of the last 50 tasks before the watcher sees
  it end, say while the watcher restarts, and the watcher then finds the run's snapshot.

A failure counts when the snapshot ends, also while the container that asked for it is gone. The series of a
source starts at 0 when the source becomes managed, so that `increase()` sees its first failure. A failure
before the first scrape of a new source is the exception: Prometheus first sees the series at 1, so the alert
below also looks for new series above 0.

Snapshots kopia server takes on schedule do not count. kopia server reports their failures in its task list and
its notification profiles; for the metrics, `kopia_watcher_last_success_timestamp_seconds` falls behind.

### `kopia_watcher_reconcile_errors_total`

The problems the reconciler ran into, by `kind`. Every kind is there from the start, at 0.

| `kind` | What went wrong | Counted |
|---|---|---|
| `unavailable` | kopia server cannot be reached, or has no repository connected | at every attempt; the path is retried with a growing delay |
| `temporary` | kopia server failed in a way that is retried, or its identity changed | at every attempt |
| `docker` | the containers could not be listed | at every attempt, a full reconcile's too |
| `config` | kopia server rejects the schedule, the policy or the path the labels ask for | once when it comes up, and again at every full reconcile while it lasts |
| `labels` | labels of a container have errors | once per container and set of labels; for the source of a running container also the way `config` is |
| `owners_disagree` | running containers that share a path ask for different policies | as `config` |
| `failed` | kopia server failed in a way retrying does not fix, such as wrong credentials or a read-only repository | as `config` |
| `state` | `STATE_FILE` refused a change | as `config` |

The log has the details of each, at warning or error level.

### `kopia_watcher_managed_sources` and `kopia_watcher_source_paused`

`kopia_watcher_managed_sources` counts the managed sources by `state`: `active`, or `paused` while none of
their containers runs. Both series are always there. `kopia_watcher_source_paused` is 1 for each paused source
and 0 for each active one: a source whose containers are stopped on purpose takes no scheduled snapshots, so
alerts on old snapshots leave it out.

### `kopia_watcher_source_owner`

1 for each container whose labels name the source's path, stopped ones included. `container` is the container
name, which stays the same when docker compose recreates the container. A source whose containers are all gone
has no series here; it stays managed and paused until the cleanup, if any, deletes it.

Join it to show the containers of a source, for example the last success with one series per container:

```promql
kopia_watcher_source_owner * on(path) group_left kopia_watcher_last_success_timestamp_seconds
```

### `kopia_watcher_notifications_total`

The notifications to [`NOTIFY_WEBHOOK_URL`](configuration.md#notifications), by what became of them. Every
`result` starts at 0, also without a webhook.

| `result` | Counted when |
|---|---|
| `sent` | the webhook answered with a 2xx |
| `failed` | every attempt failed, or the webhook answered in a way that trying again does not fix, such as 400 or 404; the log tells why |
| `dropped` | 64 notifications were waiting already, or the watcher stopped before it was sent |

## Scraping

The example in [examples/](../examples/) does not publish the watcher's port. A Prometheus in the same Docker
network scrapes it by the service name:

```yaml
scrape_configs:
  - job_name: kopia-docker-watcher
    static_configs:
      - targets: ["watcher:8080"]
```

The metrics change at the reconcile passes, so the default scrape interval of a minute is plenty.

## Alerts

Examples to adapt; the thresholds assume daily snapshots.

```yaml
groups:
  - name: kopia-docker-watcher
    rules:
      # No complete snapshot for over a day, except of paused sources.
      - alert: KopiaBackupStale
        expr: |
          (time() - kopia_watcher_last_success_timestamp_seconds > 26 * 3600)
            unless on(path) (kopia_watcher_source_paused == 1)
        for: 15m
        annotations:
          summary: "No complete snapshot of {{ $labels.path }} for over 26 hours"

      # An active source that has never had a complete snapshot.
      - alert: KopiaBackupMissing
        expr: |
          (kopia_watcher_source_paused == 0)
            unless on(path) kopia_watcher_last_success_timestamp_seconds
        for: 26h
        annotations:
          summary: "No complete snapshot of {{ $labels.path }} yet"

      # The second part catches a failure before the first scrape of a new
      # source, which increase() cannot see.
      - alert: KopiaWatcherSnapshotFailed
        expr: |
          increase(kopia_watcher_snapshot_failures_total[1h]) > 0
            or (kopia_watcher_snapshot_failures_total > 0 unless kopia_watcher_snapshot_failures_total offset 1h)
        annotations:
          summary: "A snapshot of {{ $labels.path }} the watcher took did not complete"

      # Fires when a problem comes up. Most are counted again at every full
      # reconcile while they last, label errors only while they hold back
      # the source of a running container. Otherwise, as for a container
      # whose path cannot be used, they count once, and the alert resolves
      # while the problem stays.
      - alert: KopiaWatcherErrors
        expr: increase(kopia_watcher_reconcile_errors_total{kind!~"temporary|unavailable|docker"}[30m]) > 0
        annotations:
          summary: "The watcher cannot apply the labels ({{ $labels.kind }}); see its log"

      # The notifications do not reach the webhook.
      - alert: KopiaWatcherNotificationsFailing
        expr: increase(kopia_watcher_notifications_total{result!="sent"}[1h]) > 0
        annotations:
          summary: "Notifications to NOTIFY_WEBHOOK_URL were {{ $labels.result }}; see the watcher's log"

      - alert: KopiaWatcherDown
        expr: up{job="kopia-docker-watcher"} == 0
        for: 5m
```
