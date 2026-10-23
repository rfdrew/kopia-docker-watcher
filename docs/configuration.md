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
| [`NOTIFY_WEBHOOK_URL`](#notify_webhook_url) | — | not used in v0.1.0 |
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

- `GET /healthz`, which answers `{"status":"ok"}` while the process serves HTTP. It does not check Docker,
  kopia server or the reconcile loop yet.
- `GET /metrics`, the Prometheus metrics: see [metrics.md](metrics.md).

Neither has authentication or TLS, and the metrics name your source paths and containers: keep the address on a
network you trust, and do not publish the port beyond it.

The image's `HEALTHCHECK` runs `kopia-docker-watcher healthcheck`, which reads `LISTEN_ADDR` and requests
`/healthz` there, on the loopback address when `LISTEN_ADDR` listens on all interfaces (`:port`, `0.0.0.0`,
`[::]`). `run --dry-run` serves no HTTP.

### `LOG_LEVEL`

`debug`, `info` (default), `warn` or `warning`, `error`, in any case. `critical` and `fatal`, accepted by the
Python version, mean `error`. Logs are JSON lines on stderr.

### `NOTIFY_WEBHOOK_URL`

Reserved for notifications about errors. **v0.1.0 sends nothing to it**, and the watcher logs a warning at
start when it is set. The value is only checked: an
`http://` or `https://` URL with a host and no fragment; a query is allowed. Errors and the start-up log never
show more of it than the scheme and the host, since the URL may contain a token.

## Docker

The watcher reaches Docker like the docker CLI does: `DOCKER_HOST`, `DOCKER_API_VERSION`, `DOCKER_CERT_PATH` and
`DOCKER_TLS_VERIFY`. Unset, it uses the local socket `/var/run/docker.sock`. With a docker-socket-proxy, set
`DOCKER_HOST=tcp://docker-socket-proxy:2375`.

Unless `DOCKER_API_VERSION` sets it, the watcher negotiates the API version with the daemon; any daemon with API
1.40 (Docker 19.03) or later works.

## Variables of the Python version

| Variable | In the watcher |
|---|---|
| `KOPIA_EXEC_CONTAINER` | **An error.** The watcher refuses to start: it talks to kopia server only over its HTTP API and never runs `kopia` in a container. Remove the variable and set `KOPIA_SERVER_URL`. |
| `KOPIA_PASSWORD` | **Ignored, with a warning.** It is the repository password; kopia server opens the repository itself and needs it, the watcher does not. Set it on kopia server only. |
| `KOPIA_SERVER_URL`, `KOPIA_SERVER_USERNAME`, `KOPIA_SERVER_PASSWORD`, `HOST_PREFIX`, `LOG_LEVEL` | The same meaning. |
| `KOPIA_SERVER_CERT_FINGERPRINT` | Now verified. The Python version turned TLS verification off for any value, so a value that worked before is most likely wrong now. |

See [migration.md](migration.md).
