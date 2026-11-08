English | [Українська](../uk/configuration.md)

# Configuration

This page is also translated into Ukrainian: [uk/configuration.md](../uk/configuration.md). When you change it,
update the translation in the same pull request or say in the pull request that it lags behind (see
[Documentation](../../README.md#documentation)).

The watcher is configured with environment variables. An empty variable is the same as an unset one, so a
compose file can pass `${VAR}` through without forcing a value. Values are trimmed of surrounding spaces, except
`KOPIA_SERVER_USERNAME`, `KOPIA_SERVER_PASSWORD`, `NOTIFY_SMTP_USERNAME` and `NOTIFY_SMTP_PASSWORD`, which are
taken as they are.

At start the watcher checks every variable and reports all problems at once, one log line each, then exits with
status 1. The configuration it runs with is logged at start, without secrets: only whether the password is set,
only the scheme and host of `NOTIFY_WEBHOOK_URL`, and of the e-mail only the server, the TLS mode and the number
of recipients.

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
| [`CONTROL_SOCKET`](#control_socket) | `control.sock` next to `STATE_FILE` | absolute path, at most 103 bytes, its directory 89 |
| [`LISTEN_ADDR`](#listen_addr) | `:8080` | `host:port` or `:port` |
| [`LOG_LEVEL`](#log_level) | `info` | `debug`, `info`, `warning`, `error` |
| [`NOTIFY_WEBHOOK_URL`](#notify_webhook_url) | — (no webhook) | `http://` or `https://` URL |
| [`NOTIFY_WEBHOOK_FORMAT`](#notify_webhook_format) | `json` | `json`, `ntfy`, `slack`, `telegram` |
| [`NOTIFY_SMTP_HOST`](#notify_smtp_host-and-notify_smtp_port) | — (no e-mail) | host name or IP address |
| [`NOTIFY_SMTP_PORT`](#notify_smtp_host-and-notify_smtp_port) | `587`, or `465` with `NOTIFY_SMTP_TLS=tls` | 1–65535 |
| [`NOTIFY_SMTP_TLS`](#notify_smtp_tls) | `starttls` | `starttls`, `tls`, `none` |
| [`NOTIFY_SMTP_USERNAME`](#notify_smtp_username-and-notify_smtp_password) | — (no login) | the login on the SMTP server |
| [`NOTIFY_SMTP_PASSWORD`](#notify_smtp_username-and-notify_smtp_password) | — | its password |
| [`NOTIFY_SMTP_FROM`](#notify_smtp_from-and-notify_smtp_to) | required with `NOTIFY_SMTP_HOST` | e-mail address |
| [`NOTIFY_SMTP_TO`](#notify_smtp_from-and-notify_smtp_to) | required with `NOTIFY_SMTP_HOST` | e-mail addresses separated by commas |
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
the files already exist. See the [README](../../README.md#requirements) and the
[example](../../examples/docker-compose.yml).

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
the same paths inside kopia server, as in the [example](../../examples/docker-compose.yml), leave it empty.

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

### `CONTROL_SOCKET`

The unix socket through which the `list`, `trigger` and `restore` commands ask the running watcher. Default
`control.sock` in the directory of `STATE_FILE`, `/var/lib/kopia-docker-watcher/control.sock` in the image.

- The commands run in the watcher's container with `docker exec`, and read `CONTROL_SOCKET`, or `STATE_FILE`,
  from the same environment, so they find the socket without options. See [restore.md](restore.md).
- Only the user the watcher runs as can connect: the socket is created in a temporary directory only that user
  can enter, given mode `0600` and only then moved into place, whatever the umask. Its directory is created with
  mode `0700` if it is missing.
- The watcher serves the socket from its start; until kopia server is ready, the commands get "the watcher is
  starting". It removes the socket when it stops. A socket left behind by a watcher that crashed is replaced; a
  watcher that still answers at the path, or a file there that is not a socket, keeps the new watcher from
  serving the socket.
- A watcher that cannot serve the socket logs an error and runs on without it; the commands then fail with "no
  watcher answers". A path set in `CONTROL_SOCKET` that is not absolute, longer than a unix socket address takes
  (103 bytes), or in a directory longer than 89 bytes (the temporary directory has to fit too), is a
  configuration error; a default path next to `STATE_FILE` that is too long is only a warning, so set
  `CONTROL_SOCKET` then.
- The socket is a file: put it on a file system that supports unix sockets. A named volume or a bind mount of a
  local directory does; some network file systems do not.

Nothing that controls the watcher is served on `LISTEN_ADDR`.

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

The watcher can notify when a path starts failing and when it works again: it posts to a webhook
([`NOTIFY_WEBHOOK_URL`](#notify_webhook_url)), sends an e-mail through an SMTP server
([`NOTIFY_SMTP_HOST`](#notify_smtp_host-and-notify_smtp_port)), or both. It complements the notification
profiles of kopia server, e-mail ones included, which report the snapshots kopia server takes on its schedule;
the watcher reports what only it sees:

- **a snapshot the watcher took** that failed, or whose end is unknown because kopia server lost track of it, as
  when it restarted. The next snapshot of the path the watcher takes that succeeds, or that saves nothing because
  nothing changed, sends "works again". Incomplete and canceled snapshots change nothing;
- **a [hook](labels.md#hooks) that failed**: a `kopia.pre-exec` command, after which no snapshot was taken, or a
  `kopia.post-exec` command. Each hook of each container is followed apart; its next run that succeeds sends
  "works again". A container that no longer backs the path up, or no longer has hooks, is forgotten;
- **a container the watcher [stopped or paused](labels.md#stopping-and-pausing) for a snapshot and cannot bring
  back**. The watcher keeps trying; once the container runs again, because the watcher started it or someone
  else did, it sends "works again". When the watcher stops trying because the container was removed, or someone
  else started or stopped it, the "works again" says so;
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

Each notification goes to every channel that is set up, the webhook and the e-mail. Each channel has a queue of
its own and sends one notification at a time, so the watcher never waits for either, and a webhook or an SMTP
server that is slow or down holds back only its own notifications. A failed attempt is tried again after 2, 4
and 8 seconds, four attempts in all, when trying again may help; what that is depends on the channel, see
[the webhook](#notify_webhook_url) and [the e-mail](#notify_smtp_host-and-notify_smtp_port). When 64
notifications wait for a channel, new ones are dropped for it; on a stop, those that wait are dropped.
[`kopia_watcher_notifications_total`](metrics.md#kopia_watcher_notifications_total) counts them by channel.

### `NOTIFY_WEBHOOK_URL`

Where the notifications are posted: an `http://` or `https://` URL with a host and no fragment; a query is
allowed. Unset, nothing is posted. User and password in the URL (`https://user:pass@host/…`) are sent as HTTP
Basic auth.

The URL may hold a token, so the watcher never logs it: the start-up log, the log lines about notifications and
their errors show only its scheme and host.

Each attempt has 10 seconds. A failed one is tried again, after the `Retry-After` the webhook asks for if that
is longer, up to a minute, when the webhook did not answer, answered 408 or 429, or answered with a 5xx; any
other answer counts as a failure at once. Redirects are not followed.

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
| `event` | `snapshot_failed`, `snapshot_recovered`, `hook_failed`, `hook_recovered`, `return_failed`, `return_recovered`, `reconcile_failed` or `reconcile_recovered` |
| `status` | `failing` or `resolved` |
| `title`, `message` | the text the other formats send |
| `path` | the source path |
| `containers` | the containers that back the path up, if the watcher manages it; may be empty |
| `host` | kopia server's host name, the one its sources carry |
| `reason` | for a snapshot its outcome (`failed`, `unknown`, `succeeded`, `identical`); for a hook `pre-exec` or `post-exec`; for a container brought back `stop` or `pause`; for the labels the kind of error, as the `kind` of [`kopia_watcher_reconcile_errors_total`](metrics.md#kopia_watcher_reconcile_errors_total); empty for `reconcile_recovered` |
| `container` | for a hook, the container it ran in; for `return_failed` and `return_recovered`, the container; left out for the other events |
| `error` | what went wrong, at most 1000 characters; empty for a recovery |
| `since` | when the path started failing |
| `time` | when the notification was made |

### `NOTIFY_SMTP_HOST` and `NOTIFY_SMTP_PORT`

The SMTP server that sends the e-mail: a host name or an IP address, without `smtp://` and without a port, such
as `smtp.example.com`, or `postfix` for a relay in the watcher's Docker network. Unset, no e-mail is sent; set,
[`NOTIFY_SMTP_FROM` and `NOTIFY_SMTP_TO`](#notify_smtp_from-and-notify_smtp_to) are required. The port is
`NOTIFY_SMTP_PORT`: 587 by default, the submission port, or 465 with `NOTIFY_SMTP_TLS=tls`.

Each notification is one e-mail to all the recipients, in a connection of its own. An attempt has 30 seconds,
from the connection to the server's answer to the message. It is tried again when the connection fails or times
out, or the server answers with a 4xx, such as 421 or 451. A 5xx, such as a login or a recipient the server
refuses, a certificate that does not verify, and a server without STARTTLS or AUTH where they are needed count
as a failure at once. A recipient the server refuses fails the e-mail for all of them, so that a mistyped address
in `NOTIFY_SMTP_TO` shows as failed notifications. The log line shows the server's answer.

### `NOTIFY_SMTP_TLS`

How the connection to the SMTP server is protected. Default `starttls`.

| Mode | Connection | Usual port |
|---|---|---|
| `starttls` | starts in plain text and switches to TLS with STARTTLS before anything else; a server that does not offer STARTTLS fails the e-mail | 587 |
| `tls` | TLS from the start (implicit TLS, SMTPS) | 465 |
| `none` | plain text throughout, for a relay on a network you trust; see the [login](#notify_smtp_username-and-notify_smtp_password) | 25 |

With `starttls` and `tls` the watcher uses TLS 1.2 or later and checks the server's certificate against the
system's certificate authorities and against `NOTIFY_SMTP_HOST`, so a self-signed certificate, or a host name the
certificate does not hold, fails the e-mail. A mode that does not fit the port either fails or waits until the
attempt times out, so the watcher warns at start about port 465 with `starttls` and about `tls` with port 587 or
25.

### `NOTIFY_SMTP_USERNAME` and `NOTIFY_SMTP_PASSWORD`

The login on the SMTP server, if it wants one; unset, the watcher does not log in. Both are taken as they are,
spaces included, and one without the other is a configuration error. Providers with two-step sign-in usually want
an app password here.

The watcher logs in with AUTH PLAIN, or with AUTH LOGIN when the server offers only that. A server that offers
neither, or no AUTH at all, fails the e-mail.

The password is never logged. With `NOTIFY_SMTP_TLS=none` it would cross the network unencrypted, so a login with
`none` is a configuration error unless `NOTIFY_SMTP_HOST` is `localhost`, `127.0.0.1` or `::1`. A relay without
TLS goes without a login.

### `NOTIFY_SMTP_FROM` and `NOTIFY_SMTP_TO`

The sender and the recipients. `NOTIFY_SMTP_FROM` is one address, with a name if you like:
`backups@example.com` or `Kopia backups <backups@example.com>`. Many servers send only from the address of the
login. `NOTIFY_SMTP_TO` is one or more addresses separated by commas, such as
`ops@example.com, On call <oncall@example.com>`; a name with a comma goes in double quotes.

The e-mail is plain text in UTF-8:

- the subject is the `title` of the [`json` format](#notify_webhook_format), such as
  `Backup hook failed: /srv/app/data`;
- the body is its `message`, then the fields of the `json` format that are set, one per line: `event`,
  `status`, `path`, `containers`, `container`, `host`, `reason`, `error`, `since`, `time` and
  `watcher_version`;
- `Date` is when the notification was made, and `Auto-Submitted: auto-generated` keeps vacation replies away.

```text
The pre-exec hook in app-db-1 for /srv/app/data on nas (app-db-1) failed, so no snapshot was taken: exited with code 1.

event: hook_failed
status: failing
path: /srv/app/data
containers: app-db-1
container: app-db-1
host: nas
reason: pre-exec
error: exited with code 1
since: 2026-11-08T03:00:05Z
time: 2026-11-08T03:00:05Z
watcher_version: v0.3.0
```

### `NOTIFY_ERROR_AFTER`

How long an error the watcher retries, such as kopia server being unavailable, must last for a path, without a
successful pass in between, before the watcher notifies it. Default `15m`, minimum `1m`. Errors the watcher does not retry, such as a schedule kopia server
rejects, and problems with the labels are notified at once.

While kopia server is down every path fails, so each path the watcher handles sends its own notification, on
each channel.

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
| `POST /containers/{id}/stop`, `/start`, `/pause`, `/unpause` | [stopping and pausing](labels.md#stopping-and-pausing) only | `CONTAINERS=1` and `POST=1` |

Without hooks, `kopia.stop-during-backup` and `kopia.pause-during-backup`, the watcher only reads, and the proxy
can keep `POST=0`. With them it needs `POST=1`, which the proxy does not narrow down further: it then lets through
every `POST` to the sections it allows, so with `CONTAINERS=1` also creating and removing containers. That is
still less than the socket itself, which allows everything. A watcher that gets 403 for a stop takes no snapshot
and logs the error; one that gets it for a start keeps trying until the proxy allows it.

A hook's output comes over the upgraded connection of `POST /exec/{id}/start`, which docker-socket-proxy passes
through, but closes once nothing has gone over it for 10 minutes (HAProxy's `timeout client` and `timeout
server`; set `timeout tunnel` in a proxy of your own for more). A hook that is silent for longer, such as
`pg_dump -f` of a large database, still works: when the output ends early, the watcher asks Docker every few
seconds whether the command has ended, until `kopia.hook-timeout`. Only the output after the cut is not
logged.

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
