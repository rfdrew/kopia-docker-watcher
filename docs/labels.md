# Container labels

The watcher reads the labels of every container, running or stopped, that have the prefix `LABEL_PREFIX`
(`kopia` by default). This page uses the default prefix; with `LABEL_PREFIX=backup` read `backup.enable` for
`kopia.enable`, and so on.

```yaml
services:
  app:
    image: example/app
    volumes:
      - /srv/app/data:/data
    labels:
      kopia.enable: "true"
      kopia.path: "/srv/app/data"
      kopia.schedule: "0 3 * * *"
      kopia.snapshot-on-stop: "true"
      kopia.description: "app data"
      kopia.tags: "env:prod,app:example"
```

Check labels before you deploy them:

```bash
kopia-docker-watcher validate kopia.enable=true kopia.path=/srv/app/data kopia.schedule='0 3 * * *'
kopia-docker-watcher validate --container app
kopia-docker-watcher validate --all
```

## Summary

| Label | Default | Value |
|---|---|---|
| [`kopia.enable`](#kopiaenable) | `false` | [boolean](#booleans) |
| [`kopia.path`](#kopiapath) | required with `enable` | paths, separated by `,` |
| [`kopia.schedule`](#kopiaschedule) | `0 * * * *` | cron expressions, separated by `;`; empty to inherit |
| [`kopia.snapshot-on-start`](#kopiasnapshot-on-start) | `true` | boolean |
| [`kopia.snapshot-on-stop`](#kopiasnapshot-on-stop) | `false` | boolean |
| [`kopia.min-interval`](#kopiamin-interval) | `10m` | Go duration, `0` for no minimum |
| [`kopia.description`](#kopiadescription) | the container name | text |
| [`kopia.tags`](#kopiatags) | — | `key:value` pairs, separated by `,` |
| [`kopia.cleanup-on-remove`](#kopiacleanup-on-remove) | `pause` | `pause`, `policy` or `all` |
| [`kopia.cleanup-delete-pinned`](#kopiacleanup-delete-pinned) | `false` | boolean |

The names, defaults and boolean values of the Python version's labels (`enable`, `path`, `schedule`,
`snapshot-on-start`, `snapshot-on-stop`, `description`, `tags`) are kept. `min-interval`, `cleanup-on-remove` and
`cleanup-delete-pinned` are new.

## Warnings, errors and strict mode

The watcher reads all labels of a container and reports every problem, naming the label. A problem is either a
warning or an error.

- **A warning** is a problem the watcher works around, the way the Python version silently did: an unknown label
  is ignored, a value that is not a boolean counts as `false`, an invalid `min-interval` keeps the default. The
  rest of the labels apply.
- **An error** means the labels of the container cannot be applied: `kopia.path` missing while `kopia.enable` is
  `true`, or, in strict mode, any warning.

What happens to a container with an error:

- A source kopia server does not know yet is not registered.
- A source it manages keeps the policy fields it last applied. The pause still follows the container: the source
  is paused while the container is stopped and unpaused while it runs.
- `kopia.description` and `kopia.tags` are not used; its snapshots are described by the container name.
- `kopia.snapshot-on-start`, `kopia.snapshot-on-stop`, `kopia.min-interval`, `kopia.cleanup-on-remove` and
  `kopia.cleanup-delete-pinned` still apply as long as that label itself has no error. A cleanup label with an
  error counts as `pause`.

The problems of a container are logged once, as "container labels have errors" at error level or "container
labels have warnings" at warning level, and again only when they change.

**Strict mode** (`run --strict`, `validate --strict`) makes every warning an error. Use it to catch typos in CI
or before a deploy; without it the watcher accepts every compose file the Python version accepted.

**A path that cannot be used** is an error of that path only: other paths of the same label still work. A path
cannot be used when it is not absolute (and `HOST_PREFIX` is not set), leads out of `HOST_PREFIX` with `..`, or
lies outside `WATCHER_PATH_PREFIX`.

**Unknown labels** under the prefix are a warning with a hint where there is one: `kopia.snapshot_on_stop` gets
"did you mean kopia.snapshot-on-stop?". Labels planned for later releases are a warning "not supported by this
version" and ignored: `kopia.interval`, `kopia.time-of-day`, `kopia.run-missed`, `kopia.retention.*`,
`kopia.ignore`, `kopia.ignore-files`, `kopia.ignore-cache-dirs`, `kopia.max-file-size`, `kopia.one-file-system`,
`kopia.ignore-file-errors`, `kopia.ignore-dir-errors`, `kopia.ignore-unknown-types`, `kopia.compression.*`,
`kopia.upload.parallel-files`, `kopia.policy-json`, `kopia.volumes`, `kopia.exclude-volumes`, `kopia.pre-exec`,
`kopia.post-exec`, `kopia.stop-during-backup`, `kopia.pause-during-backup`, `kopia.pin-on-stop` and
`kopia.depends-on`.

### Booleans

As in the Python version, only `true` in any case is true. `false` in any case is false. Any other value is
false with a warning: an empty value, `yes`, `1`, and also ` true` with spaces around it.

## Labels

### `kopia.enable`

`true` turns the backup of the container on. Without it the other labels have no effect, though the watcher
still reports problems with them.

### `kopia.path`

The paths to back up, separated by commas: `/srv/app/data,/srv/app/config`. Required when `kopia.enable` is
`true`.

- Paths are host paths. Each one becomes a source of kopia server: `HOST_PREFIX` is prepended, and the result is
  cleaned (`/srv/app/` becomes `/srv/app`). See [configuration.md](configuration.md#host_prefix).
- kopia server reads the files, so the path must exist inside the kopia server container. kopia server checks
  that when the watcher registers the source; `validate` does not.
- Spaces around items and empty items are ignored. A path listed twice is a warning.

**Shared paths.** Several containers may name the same path; it is one source. Its owners are all containers
that name it with `kopia.enable=true`, stopped ones included. The source is paused only when none of them runs.
Their policy labels (`kopia.schedule`) must agree: the running owners decide, or all owners when none runs. When
they disagree, the source keeps the policy fields last applied, and the watcher logs an error while an owner
runs, a warning when all are stopped.

### `kopia.schedule`

When kopia server takes scheduled snapshots: one or more cron expressions separated by `;`, written to
`scheduling.cron` of the path's policy. Default `0 * * * *`, every hour, as in the Python version.

- `;` separates expressions because they contain spaces and commas: `0 3 * * *;30 12 * * 1-5`.
- An empty value, `kopia.schedule=""`, sets no cron: the path inherits the schedule of its parent policies
  (host or global). If the watcher had set a cron before, it removes it.
- kopia server checks the expressions, in its own time zone. The watcher asks it before it writes the policy.
  `validate` asks it too, and shows the next snapshot times, when `KOPIA_SERVER_URL` is set; without it,
  `validate` does not check cron expressions. An expression kopia server rejects is an error of the path: the source
  keeps its previous schedule, and the watcher tries again when the labels change and at every full reconcile.

The watcher owns `scheduling.cron` of the path's policy and puts it back at every full reconcile if it was
changed in the web UI. Other scheduling fields you set in the web UI, such as an interval or times of day, stay.
To manage the schedule in the web UI, set `kopia.schedule=""`.

### `kopia.snapshot-on-start`

`true` (the default) takes a snapshot when the container starts, if the container still runs when the watcher
handles the event.

- No snapshot while one of the source is pending or uploading.
- No snapshot before `kopia.min-interval` has passed.
- A restart of the watcher takes no snapshots, unless `SNAPSHOT_ON_WATCHER_START=true`.
- One-off containers of `docker compose run` take none.
- A start the watcher missed while it was down counts when the watcher sees that the source should no longer be
  paused.

### `kopia.snapshot-on-stop`

`true` takes a snapshot when the container stops. Default `false`.

The snapshot runs after the container has stopped, so it reads the files at rest. The pause is applied first and
does not wait for the snapshot.

- A stop is the `die` event of a container that is then stopped, or gone. `docker kill -s HUP` and other signals
  that leave the container running are no stop.
- A container that dies and is restarted by its restart policy has not stopped: a crash loop takes no stop
  snapshots and does not pause the source.
- `kopia.min-interval` does not hold back a stop snapshot. While a snapshot uploads, kopia server runs the stop
  snapshot after it; while one is pending, that one counts, as it reads the files after the stop.
- A stop the watcher missed while it was down counts when the watcher sees that the source should be paused.

The labels of the container whose event it is decide: with several owners, each container's own
`snapshot-on-start` and `snapshot-on-stop` apply to its own starts and stops.

### `kopia.min-interval`

The shortest time between snapshots that a container start may ask for. Default `10m`; `0` means no minimum. A
Go duration: `30m`, `1h`, `24h` (there is no `d` unit).

It counts from the later of the start of the source's last snapshot and the last time the watcher queued one,
which also counts a snapshot that failed. It does not apply to stop snapshots or to kopia server's scheduled
snapshots.

An empty, invalid or negative value keeps the default, with a warning.

### `kopia.description`

The description of the source's snapshots. Default: the container name. Spaces around it are trimmed.

The watcher sets it with kopia server's `snapshots/edit` after a snapshot has finished:

- on the snapshots the watcher takes, at starts and stops;
- at each full reconcile, on the source's snapshots that have no description yet and started after the watcher
  began to manage the source, scheduled ones included.

Snapshots taken before the watcher managed the source, and snapshots that already have a description, are left
as they are. A snapshot that kopia server skipped as identical to the previous one gets nothing.

With several owners, the description lists each owner's description (or name) once, in the order of the
container names, separated by `, `.

### `kopia.tags`

`key:value` pairs separated by commas: `env:prod,app:example`. The value may contain `:`.

kopia server cannot set snapshot tags through its API yet. The watcher therefore **writes the tags into the
description**, as `key=value` in brackets after it, and logs a warning once per path:

```
app data [env=prod app=example]
```

- Spaces around items and empty items are ignored.
- An item without `:` or with an empty key is ignored, with a warning.
- A key given twice keeps its last value, with a warning.

## Cleanup

Docker forgets the labels of a container once it is removed. The watcher therefore remembers, in `STATE_FILE`,
every container that backs a source up and what its cleanup labels ask for. When a source has no container left,
because all of them were removed (`docker compose down`, `docker rm`) or no longer name the path with
`kopia.enable=true`, the source is paused, and `kopia.cleanup-on-remove` decides what else becomes of it.

A stopped container still owns its source: `docker compose stop` only pauses it.

### `kopia.cleanup-on-remove`

| Mode | What happens once the source has no container left |
|---|---|
| `pause` (default) | The source is paused. Its policy and snapshots stay. Nothing is deleted, ever. |
| `policy` | After `CLEANUP_GRACE`: if the source has no snapshots, its policy is deleted and kopia server forgets the source. A source with snapshots stays paused: without its policy it would inherit the schedule of the host or global policy and start taking snapshots again. |
| `all` | After `CLEANUP_GRACE`: the source is deleted with all its snapshots and its policy. Pinned snapshots stay, unless `kopia.cleanup-delete-pinned` lets them go (below). |

The value is case-insensitive. An empty or unknown value means `pause`, with a warning: a typo never deletes
snapshots.

**What `all` deletes.** Right before the delete the watcher lists the source's snapshots, all of them, including
those kopia server shows merged as identical.

- **No pinned snapshot, or `cleanup-delete-pinned` allows deleting them**: kopia server's `snapshots/delete` with
  `deleteSourceAndPolicy` deletes every snapshot of the source, its policy, and the source itself. The state
  forgets the source.
- **Pinned snapshots that must stay**: the snapshots without a pin are deleted by their IDs, at most 1000 per
  request. The pinned snapshots and the paused policy stay, so the source stays in kopia server, paused. When
  every snapshot is pinned, nothing is deleted.
- kopia server shows the pins of identical snapshots on the oldest of them. Such a snapshot counts as pinned and
  stays, even if its own pin is on a newer copy. This errs on the side of keeping data.
- A snapshot pinned between the listing and the delete stays in the second case and goes in the first.
- **`all` deletes every snapshot of the source, not only those the watcher took.** A source that existed before
  a container first named it, such as one the Python version registered, is managed like any other: `all`
  deletes the snapshots it had before too.

**When the cleanup runs.** Nothing is deleted:

- before `CLEANUP_GRACE` (default `24h`, minimum `10m`) has passed since the source lost its last container. A
  container that names the path again before then calls the cleanup off;
- while a snapshot the watcher queued, such as the stop snapshot of `docker compose down`, has not finished, or
  while any snapshot of the source is pending or uploading;
- while a container the state remembers as an owner still exists, by its ID or its name, even though its labels
  no longer name the path. That happens when `LABEL_PREFIX`, `HOST_PREFIX` or `WATCHER_PATH_PREFIX` changed; the
  watcher logs a warning. A container recreated under the same name is the same service;
- for a source outside `WATCHER_PATH_PREFIX`;
- for a source the watcher has no owners on record for: a source it never managed, or one it managed before
  `STATE_FILE` was lost;
- for a source of another kopia server identity (`user@host`) than the one kopia server has now.

**Several owners.** The most cautious mode wins (`pause`, then `policy`, then `all`), among the last owners of
the source and those removed within 10 minutes before them, so the order in which `docker compose down` removes
containers does not matter. Pinned snapshots go only if every one of them allows it.

**Changing the mode.** Docker labels cannot change: recreate the container with the new label. The watcher
records the new mode at the next pass that sees the container.

Every deletion is logged with the source, the mode and the number of snapshots deleted; a failed deletion is
logged with what it was about to delete, as kopia server may have carried it out anyway. `run --dry-run` prints
for each abandoned source what the cleanup would delete, and when.

### `kopia.cleanup-delete-pinned`

`true` lets `kopia.cleanup-on-remove=all` delete pinned snapshots too, and with them the whole source. Default
`false`. With any other cleanup mode it has no effect, and the watcher warns about it.
