# kopia-docker-watcher

kopia-docker-watcher manages [Kopia](https://kopia.io) backups from Docker container labels. Put
`kopia.enable=true` and `kopia.path=/srv/app/data` on a container, and the watcher registers that path as a
snapshot source in your kopia server, keeps its schedule in line with the labels, pauses it while the container
is stopped, and takes snapshots when the container starts or stops.

It is the Go successor of the Python `kopia-docker-operator`. The labels are compatible; the deployment of the
watcher itself is not. See [docs/migration.md](docs/migration.md).

## How it works

```
 Docker Engine                 kopia-docker-watcher                     kopia server
 ─────────────                 ────────────────────                     ────────────
 container events  ──────────▶  queue of source paths  ──── REST API ──▶  policies (schedule, pause)
 (start, die, destroy, …)       one reconcile pass per path              sources/upload, snapshots/edit
 container list    ◀──────────  full reconcile every RECONCILE_INTERVAL  snapshots/delete
                                STATE_FILE: what was applied, owners
```

- The watcher talks to kopia server only over its HTTP API, the one the Kopia web UI uses. It never runs the
  `kopia` CLI, never opens the repository and never reads your files: **kopia server reads them**, so every path
  in `kopia.path` must be visible inside the kopia server container.
- A Docker event only tells the watcher which container to look at. Each pass reads the current state of all
  containers and brings the source of one path in line with the labels of every container that names that path,
  stopped containers included. A full reconcile every `RECONCILE_INTERVAL` (5 minutes by default) catches
  anything an event missed.
- The policy of a path is merged, not replaced: the watcher writes only the fields its labels set (in v0.1.0 the
  schedule) and the pause. Retention, compression, ignore rules and anything else you set in the Kopia web UI stay
  as they are.
- What the watcher has applied, the snapshots it waits for, and which containers own each source are kept in
  `STATE_FILE`, a small JSON file that must live on a volume.

## What v0.1.0 does

- Registers a source per path of `kopia.path` and keeps `scheduling.cron` in line with `kopia.schedule`.
- Pauses a source (`scheduling.manual=true`) while all its containers are stopped and restores the previous
  schedule when one starts again. A crash-looping container does not toggle the pause.
- Takes a snapshot at a container start (`kopia.snapshot-on-start`, rate-limited by `kopia.min-interval`) and
  at a stop (`kopia.snapshot-on-stop`), follows it to the end and logs the result.
- Writes `kopia.description` and `kopia.tags` into the snapshot description, for its own snapshots and for
  scheduled ones.
- Several containers may share a path: the source is paused only when all of them are stopped.
- Cleans up the sources of removed containers as `kopia.cleanup-on-remove` asks, after `CLEANUP_GRACE`: pause
  (the default), delete the policy of a source that has no snapshots, or delete the source with its snapshots
  (pinned ones stay unless `kopia.cleanup-delete-pinned=true`). When containers share a path, the most cautious
  mode among them applies.
- Verifies kopia server's TLS certificate by its SHA-256 fingerprint, a CA file or the system roots.
- `validate` checks labels before you deploy them; `run --dry-run` prints what the watcher would do without
  changing anything.

Not in v0.1.0:

- **No `/metrics`.** `LISTEN_ADDR` serves only `/healthz`, which answers 200 while the process serves HTTP; it
  does not check Docker, kopia server or the reconcile loop.
- **No webhook.** `NOTIFY_WEBHOOK_URL` is read and validated, but nothing is sent to it yet.
- No policy labels beyond the schedule (retention, compression, ignore rules, …), no `kopia.volumes`, no hooks
  (`pre-exec`, `stop-during-backup`, …). These label names are reserved: the watcher warns that they are not
  supported by this version and ignores them.
- No snapshot tags: kopia server cannot set them through its API yet, so `kopia.tags` go into the description.

## Requirements

**Docker.** Any Docker Engine with API 1.40 (Docker 19.03) or later. The watcher needs read access to the
container list, container inspect and the event stream: through `/var/run/docker.sock`, or through a
[docker-socket-proxy](https://github.com/Tecnativa/docker-socket-proxy) that allows `CONTAINERS` and `EVENTS`
(see [examples/docker-compose.yml](examples/docker-compose.yml)).

**kopia server.** The watcher is tested against the latest Kopia release, 0.23.1. kopia server must run with:

- **the web UI enabled**, that is, without `--no-ui`. The watcher uses the UI API (`/api/v1/…`), which `--no-ui`
  turns off together with the UI. The watcher then stops with "kopia server does not serve its UI API".
- **the watcher's credentials being the server user**: `KOPIA_SERVER_USERNAME` and `KOPIA_SERVER_PASSWORD` must
  be the `--server-username` and `--server-password` of kopia server. Users added with `kopia server user add`
  are repository users and cannot use the UI API; kopia server answers them with 403. Without
  `--server-username`, kopia server's user is `kopia`, which is also the watcher's default when only
  `KOPIA_SERVER_PASSWORD` is set.
- **no need for `--disable-csrf-token-checks`.** The watcher gets a CSRF token like the web UI does and gets a
  new one after kopia server restarts, so you can keep the CSRF checks on.
- **the paths visible.** Every path in `kopia.path` must exist inside the kopia server container, at the same
  path or under `HOST_PREFIX` (see [HOST_PREFIX](docs/configuration.md#host_prefix)). kopia server checks that
  the path exists when the watcher registers it.
- **a stable host name.** kopia server registers sources as `user@host` of its repository connection. Set
  `hostname:` on the kopia server container, or create the repository with `--override-hostname`, so that a
  recreated container does not register every path again under a new name.
- **a TLS certificate that persists** across restarts, if you use HTTPS. With `--tls-generate-cert` and no
  certificate files kopia server makes a new certificate, and so a new fingerprint, on every start. Generate it
  once into `--tls-cert-file` and `--tls-key-file` on a volume, then start kopia server with those files and
  without `--tls-generate-cert` (kopia server refuses to overwrite existing files).

**The fingerprint.** When kopia server generates its certificate it logs

```
SERVER CERT SHA256: 3f0c9d…(64 hex digits)
```

Put that hex value into `KOPIA_SERVER_CERT_FINGERPRINT`. Colons and upper case are accepted, so for an existing
certificate the value after `Fingerprint=` in the output of `openssl x509 -in tls.crt -noout -fingerprint -sha256`
works too. The `SHA256:…`
format the Python version's README suggested is rejected with a hint. If the fingerprint does not match, the
watcher stops and logs the fingerprint the server presented.

## Quick start

The example in [examples/](examples/) runs kopia server, the watcher and a sample container on one host.

```bash
cd examples
cp .env.example .env
# Set the passwords in .env, and DOCKER_GID to the group of the Docker socket:
stat -c %g /var/run/docker.sock
docker compose up -d kopia
docker compose logs kopia | grep 'SERVER CERT SHA256'
# Put the hex value into KOPIA_SERVER_CERT_FINGERPRINT in .env, then:
docker compose up -d
docker compose logs -f watcher
```

Then open the Kopia web UI at `https://localhost:51515` (the browser warns about the self-signed certificate)
and find the source `/srv/kopia-docker-watcher-example/data` with its first snapshot.

Before you label your own containers, check the labels:

```bash
docker compose exec watcher kopia-docker-watcher validate --all
docker compose run --rm watcher run --dry-run
```

## Commands

The image runs `kopia-docker-watcher run` by default.

| Command | What it does |
|---|---|
| `run [--strict]` | Start the watcher: wait until kopia server has a repository connected, then reconcile on every container event and every `RECONCILE_INTERVAL`. Stops on SIGTERM or SIGINT. |
| `run --dry-run [--strict]` | Reconcile every path once and print the policy changes, the next scheduled snapshot times and the cleanup it would do. Writes nothing to kopia server or `STATE_FILE`, serves no HTTP, and can run next to a running watcher. kopia server must already have a repository connected: the dry run does not wait for it. Exits 1 if it found a problem. |
| `validate [--strict] label=value… \| -` | Check labels given as arguments, or one per line on stdin with `-`. |
| `validate [--strict] --container NAME…` | Check the labels of containers. |
| `validate [--strict] --all` | Check every container with `kopia.enable`, stopped ones included. |
| `healthcheck` | Exit 0 if the watcher in the same container answers on `/healthz`; used by the image's `HEALTHCHECK`. |
| `version` | Print the version. |

`--strict` turns every label warning into an error. Without `KOPIA_SERVER_URL`, `validate` checks only the
labels; with it, `validate` needs the full watcher configuration (credentials, TLS) and a kopia server with a
connected repository, and kopia server also normalizes every path and computes the next snapshot times of the
schedule, which rejects a bad cron expression. Neither checks that a path exists. `validate` exits 0 if the
labels can be applied (warnings allowed), 1 if not or if Docker or kopia server could not be asked, and 2 on a
usage or configuration error. `run --help` and `validate --help` print the full usage and exit codes.

Logs are JSON lines on stderr.

## Documentation

- [docs/configuration.md](docs/configuration.md): environment variables.
- [docs/labels.md](docs/labels.md): container labels.
- [docs/migration.md](docs/migration.md): moving from the Python `kopia-docker-operator`.
- [examples/](examples/): a compose file with kopia server, the watcher and a labeled container.

## Image

Releases publish `ghcr.io/rfdrew/kopia-docker-watcher` for linux/amd64 and linux/arm64, tagged with the version
(`0.1.0`), `major.minor` (`0.1`) and `latest`; pre-releases get only their own tag. The image is distroless and
runs as the non-root user 65532, which needs access to the Docker socket (see the example).

## License

[Apache License 2.0](LICENSE).
