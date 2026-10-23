# Configuration

The watcher is configured with environment variables. An empty variable is the same as an unset one, so a
compose file can pass `${VAR}` through without forcing a value. Values are trimmed of surrounding spaces, except
`KOPIA_SERVER_USERNAME` and `KOPIA_SERVER_PASSWORD`, which are taken as they are.

At start the watcher checks every variable and reports all problems at once, one log line each, then exits with
status 1. The configuration it runs with is logged at start, without secrets: only whether the password is set,
and only the scheme and host of `NOTIFY_WEBHOOK_URL`.

Durations are Go durations: `90s`, `10m`, `1h30m`, `24h`. There is no `d` unit; write `168h` for a week.

Booleans of this page accept `true`, `True`, `TRUE`, `t`, `T`, `1` and `false`, `False`, `FALSE`, `f`, `F`, `0`.
(Labels differ, see [labels.md](labels.md#booleans).)

## Summary

| Variable | Default | Values |
|---|---|---|
| [`KOPIA_SERVER_URL`](#kopia_server_url) | required | `http://` or `https://` URL |
| [`KOPIA_SERVER_USERNAME`](#kopia_server_username-and-kopia_server_password) | `kopia` if a password is set | kopia server's `--server-username` |
| [`KOPIA_SERVER_PASSWORD`](#kopia_server_username-and-kopia_server_password) | — | kopia server's `--server-password` |
| [`KOPIA_SERVER_CERT_FINGERPRINT`](#tls) | — | 64 hex digits, colons allowed |
| [`KOPIA_SERVER_CA_FILE`](#tls) | — | path to a PEM file |
| [`KOPIA_SERVER_INSECURE_SKIP_VERIFY`](#tls) | `false` | boolean |
| [`HOST_PREFIX`](#host_prefix) | — | absolute path |
| [`WATCHER_PATH_PREFIX`](#watcher_path_prefix) | — (no limit) | absolute path |
| [`LABEL_PREFIX`](#label_prefix) | `kopia` | letters, digits, `.`, `-`, `_` |
| [`RECONCILE_INTERVAL`](#reconcile_interval) | `5m` | duration, at least `10s` |
| [`SNAPSHOT_ON_WATCHER_START`](#snapshot_on_watcher_start) | `false` | boolean |
| [`CLEANUP_GRACE`](#cleanup_grace) | `24h` | duration, at least `10m` |
| [`STATE_FILE`](#state_file) | `/var/lib/kopia-docker-watcher/state.json` | path |
| [`LISTEN_ADDR`](#listen_addr) | `:8080` | `host:port` or `:port` |
| [`LOG_LEVEL`](#log_level) | `info` | `debug`, `info`, `warning`, `error` |
| [`NOTIFY_WEBHOOK_URL`](#notify_webhook_url) | — (no notifications) | `http://` or `https://` URL |
| [`NOTIFY_WEBHOOK_FORMAT`](#notify_webhook_format) | `json` | `json`, `ntfy`, `slack`, `telegram` |
| [`NOTIFY_ERROR_AFTER`](#notify_error_after) | `15m` | duration, at least `1m` |
| [`KOPIA_THROTTLE_UPLOAD_BYTES_PER_SECOND`](#throttle) | — (kopia server's) | bytes per second, units allowed |
| [`KOPIA_THROTTLE_DOWNLOAD_BYTES_PER_SECOND`](#throttle) | — (kopia server's) | bytes per second, units allowed |
| [`KOPIA_THROTTLE_READS_PER_SECOND`](#throttle) | — (kopia server's) | number |
| [`KOPIA_THROTTLE_WRITES_PER_SECOND`](#throttle) | — (kopia server's) | number |
| [`KOPIA_THROTTLE_LISTS_PER_SECOND`](#throttle) | — (kopia server's) | number |
| [`KOPIA_THROTTLE_CONCURRENT_READS`](#throttle) | — (kopia server's) | whole number |
| [`KOPIA_THROTTLE_CONCURRENT_WRITES`](#throttle) | — (kopia server's) | whole number |
| [`DOCKER_HOST` and others](#docker) | local socket | as for the docker CLI |
| [`KOPIA_EXEC_CONTAINER`](#variables-of-the-python-version) | — | no longer supported: an error |
| [`KOPIA_PASSWORD`](#variables-of-the-python-version) | — | ignored with a warning |

## kopia server

### `KOPIA_SERVER_URL`

The address of kopia server, for example `https://kopia:51515`. Required.

- The scheme must be `http` or `https`, and the URL must have a host.
- A path is allowed, for kopia server behind a reverse proxy; a trailing slash is dropped.
- No query, no fragment, and no credentials in the URL: use the variables below.

Plain `http://` works with kopia server started with `--insecure`. Every request carries the password, so use
it only on a network nobody else can listen on.

### `KOPIA_SERVER_USERNAME` and `KOPIA_SERVER_PASSWORD`

The user and password of kopia server's web UI, sent with HTTP Basic auth: the `--server-username` and
`--server-password` kopia server was started with.

- Only the `--server-username` user can use the UI API that the watcher needs. Users added with
  `kopia server user add` cannot: kopia server answers 403, and the watcher stops with "kopia server denied
  access".
- `KOPIA_SERVER_USERNAME` defaults to `kopia`, kopia server's default `--server-username`, when a password is
  set.
- A username without a password is an error.
- With neither set, the watcher warns at start: this only works with kopia server started with
  `--without-password`.
- Wrong credentials stop the watcher at start with "kopia server rejected the credentials"; it does not retry.

The watcher logs in like the web UI does and fetches a CSRF token, so kopia server does not need
`--disable-csrf-token-checks`.

### TLS

With an `https://` URL the watcher verifies kopia server's certificate in one of these ways. At most one of the
three variables may be set, and only with an `https://` URL.

| Variable | Verification |
|---|---|
| none | The system roots verify the certificate and the host name, as for any HTTPS site. |
| `KOPIA_SERVER_CERT_FINGERPRINT` | The SHA-256 fingerprint of the certificate kopia server presents must match. The chain and the host name are not checked. Use this for kopia server's self-signed certificate. |
| `KOPIA_SERVER_CA_FILE` | The certificate must be signed by a certificate in this PEM file, which replaces the system roots, and must be valid for the host name of the URL. |
| `KOPIA_SERVER_INSECURE_SKIP_VERIFY=true` | No verification at all. Anyone between the watcher and kopia server can read the password. |

The fingerprint is the hex value kopia server logs when it generates its certificate:

```
SERVER CERT SHA256: 3f0c9d…
```

Upper case and colons between the bytes are accepted, so the value after `Fingerprint=` in the output of
`openssl x509 -in tls.crt -noout -fingerprint -sha256` works as well. A value starting with `SHA256:`, the format
the Python version's README suggested, is rejected with a hint: it is not this fingerprint. A certificate that
does not match stops the watcher; the log names the fingerprint the server presented.

With `--tls-generate-cert` alone, kopia server generates a new in-memory certificate, and so a new fingerprint,
on every start. To keep one, start kopia server once with `--tls-generate-cert --tls-cert-file … --tls-key-file …`
pointing at files that persist, then drop `--tls-generate-cert`: kopia server refuses to start when it is set and
the files already exist. See the [README](../README.md#requirements) and the
[example](../examples/docker-compose.yml).

## Paths

### `HOST_PREFIX`

Prepended to every path of the `kopia.path` label, and to the host path of every mount `kopia.volumes` picks, to
make the path kopia server backs up. Empty by default: the path is used as it is.

Labels name paths on the Docker host. Set `HOST_PREFIX` when kopia server sees the host's files under another
directory, for example when its container mounts the host root at `/host`:

```yaml
kopia:
  volumes:
    - /:/host:ro
watcher:
  environment:
    HOST_PREFIX: /host
```

`kopia.path=/srv/app/data` then becomes the source `/host/srv/app/data`, and the volume `shop_data` of
`kopia.volumes=data` the source `/host/var/lib/docker/volumes/shop_data/_data`. With the host directories mounted at
the same paths inside kopia server, as in the [example](../examples/docker-compose.yml), leave it empty.

- Must be an absolute path. A trailing slash is dropped; `/` means no prefix.
- With a prefix, a label path counts from it, so `srv/app` and `/srv/app` are the same. `..` must not lead out of
  the prefix.
- Without a prefix, a label path must be absolute.
- The path is cleaned: `/srv/app/` and `/srv//app` are the source `/srv/app`.

The watcher does not need to see the paths itself.

### `WATCHER_PATH_PREFIX`

Limits the sources the watcher may touch to those under this path. Empty by default: no limit.

- Must be an absolute path; `/` means no limit.
- It applies to source paths, after `HOST_PREFIX`. With `HOST_PREFIX=/host` and `WATCHER_PATH_PREFIX=/host/srv`,
  `kopia.path=/srv/app` is allowed and `kopia.path=/etc` is not.
- A label path outside the prefix is a label error: the container's labels are not applied. `validate` reports
  it too.
- The same holds for the host path of a mount that `kopia.volumes` picks: with
  `WATCHER_PATH_PREFIX=/var/lib/docker/volumes`, only the container's volumes are backed up, not its bind mounts.
- A source in `STATE_FILE` outside the prefix, for example after the prefix was narrowed, is left alone: the
  watcher neither pauses it, nor cleans it up, nor deletes anything of it. It also forgets that source's owners,
  so that if the prefix is widened again the source is only paused until a container backs it up again.

Use it as a safety net when several watchers, or a watcher and people, share one kopia server: a watcher never
registers, pauses or deletes a source outside its prefix.

## Labels

### `LABEL_PREFIX`

The prefix of the labels the watcher reads. Default `kopia`, so the labels are `kopia.enable`, `kopia.path`, and
so on. With `LABEL_PREFIX=backup` they are `backup.enable`, `backup.path`, …

Letters, digits, `.`, `-` and `_`, starting and ending with a letter or digit. Labels without the prefix are
ignored. See [labels.md](labels.md).

Changing the prefix of a running setup makes the containers with the old labels lose their paths: their sources
are paused. The watcher does not clean them up while those containers still exist; with
`kopia.cleanup-on-remove=policy` or `all` it logs that their labels no longer name the path and asks whether
`LABEL_PREFIX`, `HOST_PREFIX` or `WATCHER_PATH_PREFIX` changed.

## Behavior

### `RECONCILE_INTERVAL`

How often the watcher reconciles every path, on top of reacting to container events. Default `5m`, minimum
`10s`.

A full reconcile catches what an event missed, retries paths whose labels kopia server rejected, describes
scheduled snapshots, and puts back fields from the labels that were changed in the web UI (the schedule and the
pause; see [migration.md](migration.md#behavior-changes)). Every policy write makes kopia server refresh the whole
repository, but the watcher writes a policy only when it changes.

### `SNAPSHOT_ON_WATCHER_START`

When `true`, the first full reconcile after the watcher starts treats every running container as just started:
those with `kopia.snapshot-on-start=true` get a snapshot, unless their `kopia.min-interval` has not passed since
the last snapshot. Default `false`: a restart of the watcher takes no snapshots, unlike the Python version.

### `CLEANUP_GRACE`

How long a source must have been without any container before `kopia.cleanup-on-remove=policy` or `all` deletes
anything of it. Default `24h`, minimum `10m`.

The time counts from when the watcher first sees that no container it remembers as an owner exists any more:
removed, not only stopped, since a stopped container still owns its source. After a downtime of the watcher,
that is when it notices. A container recreated under the name of an owner counts as the same owner. A container that names the path again before the grace has passed calls the
cleanup off. See [labels.md](labels.md#cleanup).

### `STATE_FILE`

Where the watcher keeps what it has to remember: the policy fields it applied, the schedule a pause replaced,
the snapshots it waits for, and which containers own each source with the cleanup mode their labels asked for.
Default `/var/lib/kopia-docker-watcher/state.json`.

- Put its directory on a volume. In the image, `/var/lib/kopia-docker-watcher` belongs to the user the watcher
  runs as, so a named volume mounted there is writable. Do not bind-mount a single file: the watcher replaces
  the file atomically through a temporary file in the same directory.
- The watcher writes the file at start and stops if it cannot.
- It takes a lock on `STATE_FILE.lock`: only one watcher may use a state file. `run --dry-run` reads the file
  without the lock and writes nothing.
- A file that is not valid state (invalid JSON, no version) is copied aside as `STATE_FILE.corrupt-<time>`, and
  the watcher starts with an empty state. Entries that cannot be read are dropped, and the file is copied aside
  the same way.
- A file written by a newer version of the watcher stops it at start, `run --dry-run` too: run the newer watcher,
  or move the file away. So does a file the watcher cannot read at all, such as one without read permission.

Losing the state is not fatal, but the watcher then treats every source like one it meets for the first time:
it does not know the schedule a pause replaced, so it unpauses a source with the schedule of the labels only,
and it does not remember the containers removed meanwhile, so it leaves their sources as they are and never
cleans them up.

### `LISTEN_ADDR`

The address of the watcher's HTTP server. Default `:8080`. It serves:

- `GET /healthz`, the health of the watcher: see [Health check](#health-check).
- `GET /metrics`, the Prometheus metrics: see [metrics.md](metrics.md).

Neither has authentication or TLS, and the metrics name your source paths and containers: keep the address on a
network you trust, and do not publish the port beyond it.

The image's `HEALTHCHECK` runs `kopia-docker-watcher healthcheck`, which reads `LISTEN_ADDR` and requests
`/healthz` there, on the loopback address when `LISTEN_ADDR` listens on all interfaces (`:port`, `0.0.0.0`,
`[::]`). `run --dry-run` serves no HTTP.

### Health check

`GET /healthz` answers with the state of three components, in JSON:

| Component | Checked by | Fails when |
|---|---|---|
| `kopia` | asking kopia server for its repository status every 15 seconds; while the watcher waits for kopia server at start, its attempts | kopia server does not answer, rejects the watcher, or has no repository connected |
| `docker` | pinging the Docker daemon every 15 seconds (`GET /_ping`, which docker-socket-proxy allows by default) | Docker does not answer |
| `reconcile` | looking at the reconcile queue every 15 seconds | a pass has run for longer than twice `RECONCILE_INTERVAL`, at least 10 minutes, or paths have waited that long while no pass ended |

The checks run in the background, so `/healthz` answers at once and polling it costs kopia server and Docker
nothing. A component is `failing` once its checks have failed for 45 seconds without a success in between, so a
single lost request does not count. Until the watcher has reached kopia server, `docker` and `reconcile` are
`pending`, which is no failure; `kopia` tells then.

| `status` | HTTP | When |
|---|---|---|
| `ok` | 200 | no component is failing |
| `starting` | 200 | within 2 minutes of the start, while a component is failing or pending |
| `unhealthy` | 503 | a component is failing, after the first 2 minutes |

```json
{
  "status": "unhealthy",
  "components": {
    "docker": {"status": "ok", "last_ok": "2026-10-23T12:00:15Z"},
    "kopia": {
      "status": "failing",
      "error": "kopia server: GET /api/v1/repo/status: dial tcp 172.18.0.2:51515: connect: connection refused",
      "last_ok": "2026-10-23T11:58:45Z",
      "failing_since": "2026-10-23T11:59:00Z"
    },
    "reconcile": {"status": "ok", "last_ok": "2026-10-23T12:00:15Z"}
  }
}
```

`error` shows the last failed check also while the component is still `ok`. A component that fails or recovers
is logged once, as "health check failing" and "health check passes again". The image's `HEALTHCHECK` marks the
container unhealthy after three failed probes 30 seconds apart; a watcher that is unhealthy keeps running and
recovers by itself once kopia server or Docker are back.

### `LOG_LEVEL`

`debug`, `info` (default), `warn` or `warning`, `error`, in any case. `critical` and `fatal`, accepted by the
Python version, mean `error`. Logs are JSON lines on stderr.

## Notifications

The watcher can post a notification when a path starts failing and when it works again. It complements the
notification profiles of kopia server, which report the snapshots kopia server takes on its schedule; the watcher
reports what only it sees:

- **a snapshot the watcher took** that failed, or whose end is unknown because kopia server lost track of it, as
  when it restarted. The next snapshot of the path the watcher takes that succeeds, or that saves nothing because
  nothing changed, sends "works again". Incomplete and canceled snapshots change nothing;
- **a [hook](labels.md#hooks) that failed**: a `kopia.pre-exec` command, after which no snapshot was taken, or a
  `kopia.post-exec` command. Each hook of a path is followed apart; its next run that succeeds sends "works
  again";
- **labels that cannot be applied** to a path: kopia server rejects the schedule, the policy or the path,
  containers that share the path disagree, a container's labels have errors, or `STATE_FILE` refuses a change.
  The first pass that applies the labels sends "works again".
- **kopia server failing for a path** for longer than `NOTIFY_ERROR_AFTER`, after the watcher has started, and
  Docker failing for a path whose pass was under way. The watcher retries these; if a retry succeeds in time,
  nothing is sent at all.

A Docker daemon that is down, and a kopia server the watcher cannot reach at its start, keep the watcher from
handling any path, so they send no notification: they show in the [health check](#health-check) and in
[`kopia_watcher_reconcile_errors_total`](metrics.md#kopia_watcher_reconcile_errors_total)`{kind="docker"}`.

Each path sends one notification when it starts failing and one when it works again, not one per attempt and
not again at every full reconcile. The watcher remembers this in memory only: a problem that lasts across a
restart of the watcher is notified once more.

Notifications wait in a queue and go out one at a time, so the watcher never waits for the webhook. Each attempt
has 10 seconds. A failed one is tried again after 2, 4 and 8 seconds, or after the `Retry-After` the webhook
asks for, up to a minute, when the webhook did not answer, answered 408 or 429, or answered with a 5xx; any
other answer counts as a failure at once. When 64 notifications wait, new ones are dropped; on a stop, those
that wait are dropped. [`kopia_watcher_notifications_total`](metrics.md#kopia_watcher_notifications_total)
counts them. Redirects are not followed.

### `NOTIFY_WEBHOOK_URL`

Where the notifications go: an `http://` or `https://` URL with a host and no fragment; a query is allowed. Unset,
no notifications are sent. User and password in the URL (`https://user:pass@host/…`) are sent as HTTP Basic auth.

The URL may hold a token, so the watcher never logs it: the start-up log, the log lines about notifications and
their errors show only its scheme and host.

### `NOTIFY_WEBHOOK_FORMAT`

How the notification is posted. Default `json`.

| Format | URL | Request |
|---|---|---|
| `json` | any | `POST` of the JSON object below, `Content-Type: application/json` |
| `ntfy` | the topic, e.g. `https://ntfy.sh/my-backups` | the [publish request](https://docs.ntfy.sh/publish/): the message as the body, `Title`, `Priority` (`high` for a failure, `default` for a recovery) and `Tags` (`warning`, `white_check_mark`) as headers |
| `slack` | an [incoming webhook](https://api.slack.com/messaging/webhooks), `https://hooks.slack.com/services/…` | `{"text": "*title*\nmessage"}` |
| `telegram` | `https://api.telegram.org/bot<token>/sendMessage?chat_id=<chat>` | [`sendMessage`](https://core.telegram.org/bots/api#sendmessage) with the parameters of the query, such as `chat_id` and `message_thread_id`, and the text in a JSON body; the query is not sent |

For ntfy with an access token, put it in the URL as the
[`auth` query parameter](https://docs.ntfy.sh/publish/#query-param), or use user and password in the URL. With
`telegram`, a URL without `sendMessage` or without `chat_id` is a configuration error.

The `json` format posts:

```json
{
  "event": "snapshot_failed",
  "status": "failing",
  "title": "Backup failed: /srv/app/data",
  "message": "The snapshot of /srv/app/data on nas (app-db-1) failed: unable to read directory: permission denied.",
  "path": "/srv/app/data",
  "containers": ["app-db-1"],
  "host": "nas",
  "reason": "failed",
  "error": "unable to read directory: permission denied",
  "since": "2026-10-23T03:00:12Z",
  "time": "2026-10-23T03:00:12Z",
  "watcher_version": "v0.2.0"
}
```

| Field | Meaning |
|---|---|
| `event` | `snapshot_failed`, `snapshot_recovered`, `hook_failed`, `hook_recovered`, `reconcile_failed` or `reconcile_recovered` |
| `status` | `failing` or `resolved` |
| `title`, `message` | the text the other formats send |
| `path` | the source path |
| `containers` | the containers that back the path up, if the watcher manages it; may be empty |
| `host` | kopia server's host name, the one its sources carry |
| `reason` | for a snapshot its outcome (`failed`, `unknown`, `succeeded`, `identical`); for a hook `pre-exec` or `post-exec`; for the labels the kind of error, as the `kind` of [`kopia_watcher_reconcile_errors_total`](metrics.md#kopia_watcher_reconcile_errors_total); empty for `reconcile_recovered` |
| `error` | what went wrong, at most 1000 characters, for a hook with the container first (`db: exited with code 1`); empty for a recovery |
| `since` | when the path started failing |
| `time` | when the notification was made |

### `NOTIFY_ERROR_AFTER`

How long an error the watcher retries, such as kopia server being unavailable, must last for a path, without a
successful pass in between, before the watcher notifies it. Default `15m`, minimum `1m`. Errors the watcher does not retry, such as a schedule kopia server
rejects, and problems with the labels are notified at once.

While kopia server is down every path fails, so each path the watcher handles sends its own notification.

## Throttle

The `KOPIA_THROTTLE_` variables set kopia server's limits on its repository storage, through
`PUT /api/v1/repo/throttle`, as the Kopia web UI does. Unset, the watcher leaves the limits alone: use this
**only when nothing else manages them**, such as the `kopia_throttle` resource of the Terraform provider (see
[terraform.md](terraform.md)). Once any of them is set, the watcher owns the limits it sets and puts them back at
every start.

| Variable | Kopia's field | Value |
|---|---|---|
| `KOPIA_THROTTLE_UPLOAD_BYTES_PER_SECOND` | `maxUploadSpeedBytesPerSecond` | bytes per second |
| `KOPIA_THROTTLE_DOWNLOAD_BYTES_PER_SECOND` | `maxDownloadSpeedBytesPerSecond` | bytes per second |
| `KOPIA_THROTTLE_READS_PER_SECOND` | `readsPerSecond` | storage reads per second |
| `KOPIA_THROTTLE_WRITES_PER_SECOND` | `writesPerSecond` | storage writes per second |
| `KOPIA_THROTTLE_LISTS_PER_SECOND` | `listsPerSecond` | storage listings per second |
| `KOPIA_THROTTLE_CONCURRENT_READS` | `concurrentReads` | reads at a time |
| `KOPIA_THROTTLE_CONCURRENT_WRITES` | `concurrentWrites` | writes at a time |

- `0` means no limit, so `KOPIA_THROTTLE_UPLOAD_BYTES_PER_SECOND=0` lifts a limit set before.
- The rates are numbers, fractions allowed. The byte rates take a unit: `k`, `M`, `G` or `T` for powers of 1000,
  `Ki`, `Mi`, `Gi` or `Ti` for powers of 1024, with or without `B`, in any case: `500k`, `10MB`, `10MiB`.
  These are bytes, not bits.
- When kopia server is ready, the watcher reads the limits, sets those of the variables and keeps the others as
  kopia server has them, and writes only if something changes. kopia server keeps them in its repository
  configuration, so they survive its restarts. A failure that kopia server may get over is tried again at the
  next full reconcile.
- kopia server connected to a repository server instead of a storage has no limits to set; the watcher logs an
  error and goes on.

## Docker

The watcher reaches Docker like the docker CLI does: `DOCKER_HOST`, `DOCKER_API_VERSION`, `DOCKER_CERT_PATH` and
`DOCKER_TLS_VERIFY`. Unset, it uses the local socket `/var/run/docker.sock`. With a docker-socket-proxy, set
`DOCKER_HOST=tcp://docker-socket-proxy:2375`.

What the watcher asks of Docker:

| Requests | For | docker-socket-proxy |
|---|---|---|
| `GET /containers/json`, `GET /containers/{id}/json` | the containers and their labels | `CONTAINERS=1` |
| `GET /events` | the container events | `EVENTS=1` |
| `GET /_ping`, `GET /version` | the health check and the API version | allowed by default |
| `POST /containers/{id}/exec`, `POST /exec/{id}/start`, `GET /exec/{id}/json` | [hooks](labels.md#hooks) only | `EXEC=1` and `POST=1` |

Without hooks the watcher only reads, and the proxy can keep `POST=0`. With hooks it needs `POST=1`, which the
proxy does not narrow down further: it then lets through every `POST` to the sections it allows, so with
`CONTAINERS=1` also starting, stopping and removing containers. That is still less than the socket itself, which
allows everything. A hook needs the upgraded connection of `POST /exec/{id}/start`, which docker-socket-proxy
passes through.

Unless `DOCKER_API_VERSION` sets it, the watcher negotiates the API version with the daemon; any daemon with API
1.40 (Docker 19.03) or later works. The [health check](#health-check) pings the daemon (`/_ping`).

## Variables of the Python version

| Variable | In the watcher |
|---|---|
| `KOPIA_EXEC_CONTAINER` | **An error.** The watcher refuses to start: it talks to kopia server only over its HTTP API and never runs `kopia` in a container. Remove the variable and set `KOPIA_SERVER_URL`. |
| `KOPIA_PASSWORD` | **Ignored, with a warning.** It is the repository password; kopia server opens the repository itself and needs it, the watcher does not. Set it on kopia server only. |
| `KOPIA_SERVER_URL`, `KOPIA_SERVER_USERNAME`, `KOPIA_SERVER_PASSWORD`, `HOST_PREFIX`, `LOG_LEVEL` | The same meaning. |
| `KOPIA_SERVER_CERT_FINGERPRINT` | Now verified. The Python version turned TLS verification off for any value, so a value that worked before is most likely wrong now. |

See [migration.md](migration.md).
