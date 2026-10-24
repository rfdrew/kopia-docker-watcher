English | [Українська](uk/restore.md)

# list, trigger and restore

Three commands ask the running watcher what it manages, take a snapshot through it and restore one. They run in
the watcher's container, with `docker exec`, and talk to the watcher there through its unix socket
[`CONTROL_SOCKET`](configuration.md#control_socket):

```bash
docker exec kopia-docker-watcher kopia-docker-watcher list
# or, with docker compose:
docker compose exec watcher kopia-docker-watcher list
```

They need the running watcher: a snapshot goes through its queue, with the hooks, the downtime and the minimum
interval of the labels, and a restore that stops containers needs the watcher to start them again. When no
watcher answers, they fail with "no watcher answers at the control socket" and do nothing. While the watcher
waits for kopia server at its start, they fail with "the watcher is starting".

Each command exits 0 on success, 1 when it failed or the watcher could not be asked, and 2 on a usage error.
`--help` prints its full usage.

`CONTAINER` is a container's name, or the start of its ID (at least four characters). `PATH` is a source path as
`list` shows it, the path kopia server snapshots, or a path of `kopia.path`, which `HOST_PREFIX` is put in front
of.

## list

```
$ docker compose exec watcher kopia-docker-watcher list
PATH                  CONTAINERS  PAUSED  SCHEDULE  NEXT SNAPSHOT     LAST SUCCESS      STATUS  PROBLEM
/host/srv/app         app         no      kopia     2026-10-25 02:00  2026-10-24 02:00  IDLE    -
/host/srv/db          db          no      watcher   2026-10-25 03:00  2026-10-24 03:01  IDLE    -
/host/srv/old         old         yes     kopia     -                 2026-10-20 02:00  IDLE    -
```

For every source the watcher manages, and every path whose last reconcile had a problem:

- `CONTAINERS`: the containers that back the path up, stopped ones included.
- `PAUSED`: whether the source is paused because none of them runs.
- `SCHEDULE`: who takes the scheduled snapshots. `kopia`: kopia server, by the policy. `watcher`: the watcher
  itself, for a source with [hooks](labels.md#hooks) or [stop or pause](labels.md#stopping-and-pausing).
  `NEXT SNAPSHOT` is when the next is due; `-` when none is, as while paused.
- `LAST SUCCESS`: the start of the last complete snapshot.
- `STATUS`: kopia server's status of the source (`IDLE`, `PENDING`, `UPLOADING`, …), followed by the phase of a
  snapshot with hooks under way and the containers held down for it.
- `PROBLEM`: `yes` when the last reconcile of the path had a problem; the problems are printed after the table.

Times are in the container's time zone (`TZ`). `--json` prints the same as JSON, times in RFC 3339. When kopia
server does not answer, `list` warns on stderr and leaves out its status and times.

## trigger

```
$ docker compose exec watcher kopia-docker-watcher trigger app
snapshot of /host/srv/app asked for; waiting for it to end
/host/srv/app: snapshot saved (started 2026-10-24 14:02:11 +02:00, kopia task 2a)
```

`trigger` takes a snapshot of each path of a container, or of one path, and waits until it has ended. The
snapshot goes through the watcher's queue like the snapshot at a container's start:

- with the [hooks](labels.md#hooks) of the path's running containers, `kopia.pre-exec` before and
  `kopia.post-exec` after, and with [stop-during-backup and pause-during-backup](labels.md#stopping-and-pausing);
  the result comes once the post-exec hooks have run. When a pre-exec hook fails, no snapshot is taken;
- not before `kopia.min-interval` has passed since the last snapshot or the watcher's last trigger, unless
  `--force` is given. For a path, the shortest `kopia.min-interval` of its containers counts;
- not while a snapshot of the source is pending or uploading: that one counts, and `trigger` says so;
- after a snapshot with hooks of the source that is under way, and after a [restore](#restore) with `--stop`
  of it.

The outcomes: `snapshot saved`, `nothing changed since the last snapshot, none saved` (with a policy that ignores
identical snapshots), `snapshot saved incomplete or with errors`, `snapshot failed`, `snapshot canceled`, `no
snapshot taken` with why, and `failed` with the error. `trigger` exits 0 when every snapshot was saved or found
nothing changed and no post-exec hook failed.

`--no-wait` returns once the snapshot is asked for; the watcher's log tells how it ended. Interrupting
`trigger` stops the wait, not the snapshot.

A snapshot the watcher takes by hand is followed and described like its other snapshots, and counts for
`kopia.min-interval` of the next start snapshot.

## restore

```
$ docker compose exec watcher kopia-docker-watcher restore --target /restore/app app
restoring snapshot 9a1f3c… of root@nas:/host/srv/app, taken 2026-10-24 02:00:03 +02:00 (app), into /restore/app on kopia server
restored 1200 files, 31 directories, 512.0 MiB (task 2b)
restored 4810 files, 120 directories, 1.9 GiB (task 2b)
kopia server's restore task 2b ended: success
restore done: 4810 files, 120 directories, 1.9 GiB restored into /restore/app (kopia task 2b)
```

`restore` restores a snapshot of a source into a directory on kopia server with kopia server's
`POST /api/v1/restore`, and follows kopia server's restore task until it ends. The task shows in KopiaUI under
Tasks, and can be canceled there.

### Where the files go

**kopia server writes the files**, not the watcher. `--target` is a path inside the kopia server container, like
the source paths `list` shows, and the volume it lies on must be **mounted into kopia server writable**. A volume
kopia server only backs up is usually mounted read-only, and a restore into it fails with "read-only file
system". Two ways:

- Restore into a separate directory, then move the files yourself. Give kopia server a volume for that:

  ```yaml
  services:
    kopia:
      volumes:
        - /var/lib/docker/volumes:/host/var/lib/docker/volumes:ro
        - restore:/restore            # writable, for restores
  volumes:
    restore:
  ```

  ```bash
  docker compose exec watcher kopia-docker-watcher restore --target /restore/app app
  ```

- Restore in place, into the source path itself, which then has to be mounted writable: drop `:ro` from that
  mount, at least while you restore. Use `--stop` and `--overwrite` (see below).

The directory is created if it does not exist. The files get the owners, permissions and times they had in the
snapshot; kopia server usually runs as root, which can set them.

### Which snapshot

By default the newest complete snapshot. Incomplete ones, checkpoints of an upload and canceled snapshots, are
taken only by their ID. Give a snapshot as the last argument: its manifest ID, or the start of it, at least six
characters. In the kopia server container, `kopia snapshot list --all --manifest-id --incomplete --show-identical`
lists them; without `--manifest-id` it shows the root object IDs instead, which `restore` does not take.

```bash
docker compose exec watcher kopia-docker-watcher restore --target /restore/app app 9a1f3c
```

### Nothing is overwritten by default

Without `--overwrite`, kopia server refuses a `--target` that exists and is not empty, before it writes anything
there, and `restore` fails with "non-empty directory already exists". Restore into a new or empty directory, or
pass `--overwrite`.

With `--overwrite`, kopia server restores into the directory as it is: it replaces the files and symlinks the
snapshot has, writing each file to a temporary file first and renaming it, so that no file is left half
written. **Files the snapshot does not have stay**: the restore does not make the directory equal to the
snapshot. If that matters, restore into an empty directory and swap it in.

A restore that fails half-way leaves the files it has written.

### `--stop`: stop the containers while restoring

Restoring under a running application can corrupt it: a database that has its files open, an application that
writes while the files change. `--stop` stops the path's running containers before the restore, with every
running container that depends on them by `kopia.depends-on`, dependents first, as
[stop-during-backup](labels.md#stopping-and-pausing) does, and starts them again in the reverse order once
kopia server's restore task has ended, whether it succeeded or not. Containers that are restarting, as in a crash
loop, or paused are stopped too, and come back running:

```bash
docker compose exec watcher kopia-docker-watcher restore --stop --overwrite --target /host/srv/db db
```

- The watcher records the containers it stopped in `STATE_FILE` before it stops them. Their stop and start
  events are its own: the source is not paused, and no snapshot is taken at the stop or the start.
- While the restore runs, the watcher takes no snapshot of the source: snapshots it is asked for, scheduled ones
  of a source with hooks included, wait until the restore has ended. **kopia server's own schedule is not held
  back**: a source on kopia server's schedule (`SCHEDULE kopia` in `list`) can be snapshotted in the middle of the
  restore. Restore when no snapshot is due, as `list` shows.
- Interrupting `restore` stops only the output: the watcher follows the restore and starts the containers when
  it has ended. To stop the restore, cancel its task in KopiaUI; the containers come back then.
- No container comes back while kopia server's restore task still runs: the watcher stores the task with the
  containers it stopped, and asks kopia server about it before it starts one, every 10 seconds until the task
  has ended. When the watcher stops during the restore, it cancels the task, waits a few seconds for kopia server
  to finish the files it is writing, and starts the containers if the task has ended by then; otherwise they stay
  down and the watcher starts them after its next start, once the task has ended. After a crash of the watcher,
  kopia server's task goes on, and the watcher starts the containers after its next start, once the task has
  ended. A task kopia server no longer knows, as after its restart, counts as ended. While the watcher cannot
  ask kopia server, the containers stay down, and that is logged and [notified](configuration.md#notifications)
  like a container that cannot be brought back. A canceled restore leaves the files it has written.
- A container that someone else starts meanwhile is left running; one that cannot be started is tried again,
  logged and notified like after a snapshot.
- `--stop` is refused while a snapshot with hooks of the source, or another restore of it, is under way, and
  while a container it needs is held down for a snapshot: try again once it is done. Stopping and starting
  containers needs `POST=1` with docker-socket-proxy (see [Docker](configuration.md#docker)).

Without `--stop`, a restore whose target is the source path, or lies inside it, warns when containers of the
path run.

### Output and exit status

`restore` prints the snapshot it restores, the progress as kopia server reports it, and the result. It exits 0
when kopia server's task succeeded, and 1 otherwise, also when the watcher could not be asked or no snapshot
matched.
