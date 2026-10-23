# Migrating from kopia-docker-operator

kopia-docker-watcher replaces the Python `kopia-docker-operator`. Both register container paths as sources in
kopia server through its HTTP API, so the watcher takes over the sources the operator registered, with their
snapshot history.

**The labels of your applications stay as they are.** The names, defaults and boolean values of `kopia.enable`,
`kopia.path`, `kopia.schedule`, `kopia.snapshot-on-start`, `kopia.snapshot-on-stop`, `kopia.description` and
`kopia.tags` are the same. **The deployment of the watcher itself changes**, and so does some behavior.

## What changes in the deployment

| Python operator | Watcher |
|---|---|
| Runs as root and reads `/var/run/docker.sock` | Runs as user 65532: add the socket's group with `group_add`, or use a docker-socket-proxy with read access to `containers` and `events`. See the [example](../examples/docker-compose.yml). |
| No state | Needs a volume for `STATE_FILE` at `/var/lib/kopia-docker-watcher`. |
| `KOPIA_PASSWORD` in its environment | Not needed: kopia server opens the repository. The watcher warns if it is set. |
| Data volumes (`/var/lib/docker/volumes`, `/:/host`) and a kopia config volume | Not needed: kopia server reads the files. Remove them. |
| `KOPIA_EXEC_CONTAINER` (exec mode) | Removed. The watcher refuses to start while it is set. |
| `KOPIA_SERVER_CERT_FINGERPRINT` turned TLS verification off, whatever its value | The fingerprint is verified. It is the hex value after `SERVER CERT SHA256:` in the kopia server log, not the `SHA256:…` value the operator's README suggested. A value that worked before is most likely wrong now. |
| kopia server needed `--disable-csrf-token-checks` for the API calls to work | Not needed: the watcher handles CSRF tokens. You can remove the flag. |
| — | A `stop_grace_period` of 10 s or more (the default) lets the watcher save its state on shutdown. |

`KOPIA_SERVER_URL`, `KOPIA_SERVER_USERNAME`, `KOPIA_SERVER_PASSWORD`, `HOST_PREFIX` and `LOG_LEVEL` keep their
meaning. **Keep `HOST_PREFIX` as it was**, so that the source paths stay the same: with a different prefix the
watcher registers new sources, and the old ones with their snapshots lose their containers.

kopia server itself must meet the [requirements](../README.md#requirements): the web UI enabled, the watcher's
credentials being `--server-username` and `--server-password`, the paths visible, a stable host name, and a
persistent TLS certificate.

## Behavior changes

- **A restart of the watcher takes no snapshots of containers that kept running.** The operator took a snapshot
  of every running container with `kopia.snapshot-on-start=true` (the default) at each of its restarts. Set
  `SNAPSHOT_ON_WATCHER_START=true` to keep that, rate-limited by `kopia.min-interval`. Containers that started or
  stopped while the watcher was down, and running containers whose source the operator left paused, still get
  their start or stop snapshot at the first pass.
- **Start snapshots are rate-limited.** A container start takes a snapshot only if `kopia.min-interval` (default
  `10m`) has passed since the last one, and not while one is running. Stop snapshots are not limited.
- **`kill` is not a stop.** The operator treated `docker kill -s HUP` as a stop and paused the source of a
  running container. Only `die` of a container that is then stopped counts. A crash-looping container, restarted
  by its restart policy, does not pause its source and takes no stop snapshots.
- **Tags go into the description.** The operator wrote `kopia.description`, `kopia.tags` and its own labels
  (`managed-by`, `container`, `compose-project`) into `policy.labels`, which kopia server silently drops, so
  none of it ever showed up. The watcher writes the description of each snapshot, with the tags as
  `[key=value …]` after it, because kopia server cannot set snapshot tags through its API yet. It also describes
  scheduled snapshots taken after it started managing the source; older snapshots are left as they are.
- **The operator's pauses are lifted.** The operator paused a source on every stop and `kill` and lifted the
  pause only on the next start event; sources of containers that run now may still be paused. The watcher sets
  the pause from the state of the containers alone: the source of a running container is unpaused at the first
  pass, a source whose containers are all stopped is paused. As the watcher does not know the schedule such a
  pause replaced, it unpauses with the schedule of the labels. A pause you set in the web UI on the path of a
  running container is lifted too.
- **Fields from the labels are put back; other web UI edits are kept.** The operator replaced the whole policy
  on every start and stop, so retention, compression and ignore rules set in the web UI were lost. The watcher
  merges: it writes only `scheduling.cron` (from `kopia.schedule`, `0 * * * *` when the label is absent), the
  pause, and the fields of the [policy labels](labels.md#policy-labels), which the operator did not have.
  Everything else in the policy stays. But it puts those fields back at every full reconcile, every 5
  minutes by default, where the operator did so only at the next start: a schedule changed in the web UI is
  reverted within minutes. Set `kopia.schedule=""` to manage the schedule in the web UI instead.
- **Unknown labels give a warning.** The operator ignored unknown `kopia.*` labels and non-boolean values
  silently. The watcher still accepts them, but logs a warning naming the label, with a hint for a likely typo.
  `validate --strict` and `run --strict` make the warnings errors.
- **The fingerprint is verified**, see above.
- **Removed containers are cleaned up as `kopia.cleanup-on-remove` asks.** The operator left the sources of
  removed containers in kopia server forever. The watcher pauses them by default and deletes nothing. With
  `kopia.cleanup-on-remove=all` it deletes their snapshots and policy after `CLEANUP_GRACE`, including the
  snapshots the operator took (pinned ones stay unless `kopia.cleanup-delete-pinned=true`). `policy` deletes only
  the policy of a source without snapshots, so it leaves the operator's sources, which have snapshots, paused. The
  watcher only cleans up sources whose containers it has seen: containers removed before the watcher first ran
  leave their sources as they are. See [labels.md](labels.md#cleanup).
- **Paths removed from `kopia.path` lose their container**: their sources are paused, and cleaned up as
  `kopia.cleanup-on-remove` asks. The operator left them as they were.
- **A path shared by several containers** is paused only when all of them are stopped.
- **Paths are normalized**: a trailing slash no longer makes kopia server reject the path.

## Steps

The commands assume the watcher runs as the service `watcher` of a compose project, as in the
[example](../examples/docker-compose.yml).

1. **Stop the Python operator** and remove its container, so that it does not fight the watcher over the
   policies:

   ```bash
   docker compose stop kopia-operator
   docker compose rm -f kopia-operator
   ```

   Nothing in kopia server changes: the sources, their policies and snapshots stay, and so do the operator's
   pauses until the watcher runs.

2. **Prepare kopia server.** Check the [requirements](../README.md#requirements). If kopia server generates its
   certificate on every start, switch to certificate files on a volume first; the fingerprint changes once
   then. Take the fingerprint from the log line `SERVER CERT SHA256:`.

3. **Add the watcher service** to your compose file (see the table above and the example) and fill in its
   environment. Keep `HOST_PREFIX` as it was.

4. **Check the labels** of your containers. Without starting the watcher:

   ```bash
   docker compose run --rm watcher validate --all
   docker compose run --rm watcher validate --container app --container db
   ```

   With `KOPIA_SERVER_URL` set, this also checks the connection, the credentials and the fingerprint, and has
   kopia server normalize every path and compute the next snapshot times of every schedule. Fix the errors; read
   the warnings, which the operator would have ignored silently. Add `--strict` to see what strict mode would
   reject. `validate` does not check that a path exists.

5. **See what the watcher would do**:

   ```bash
   docker compose run --rm watcher run --dry-run
   ```

   It reconciles every path once and prints, per source, the policy it would write (new sources, schedule
   changes, the operator's pauses it would lift), the next scheduled snapshot times and the cleanup it would do.
   It does not show the start or stop snapshots it would queue. It writes nothing to kopia server or
   `STATE_FILE`, and exits 1 if it found a problem.

6. **Start the watcher** and follow its log:

   ```bash
   docker compose up -d watcher
   docker compose logs -f watcher
   ```

   It waits for kopia server, logs "connected to kopia server" and "watching containers", and then one line per
   change it makes. Errors name the container or the path. Two lines sum up its start:

   - "first full reconcile": how many containers it found, how many enable the backup, how many paths that gives;
   - "first full reconcile done", once each of those paths had a pass: how many sources it registered, updated,
     found up to date, paused, skipped or failed on. With no container enabling the backup it adds a hint about
     the `kopia.enable` label.

7. **Check the Kopia web UI**:

   - The sources are the ones the operator registered, under the same `user@host`, not duplicates under a new
     path or host. Duplicates mean `HOST_PREFIX` or the host name of kopia server changed.
   - The sources of running containers are no longer paused; those of stopped containers are.
   - The schedule of each source is the one of its `kopia.schedule` label.
   - Retention, compression and other settings you set in the web UI are still there.
   - New snapshots have the description of `kopia.description`, or the container name, with the tags in
     brackets.

To go back, stop the watcher and start the operator again. The watcher's changes are ordinary policy fields and
snapshot descriptions; the operator ignores the descriptions and replaces the policies at the next start
events, as before.
