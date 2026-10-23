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
      kopia.retention.daily: "14"
      kopia.compression.algorithm: "zstd"
      kopia.ignore: "*.tmp,cache/"
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

The [policy labels](#policy-labels) set fields of the path's Kopia policy. Each is unset by default: the path
inherits the field from its parent policies.

| Label | Value | Kopia policy field |
|---|---|---|
| [`kopia.interval`](#kopiainterval) | Go duration, at least `1m` | `scheduling.intervalSeconds` |
| [`kopia.time-of-day`](#kopiatime-of-day) | `HH:MM` times, separated by `,` | `scheduling.timeOfDay` |
| [`kopia.run-missed`](#kopiarun-missed) | boolean | `scheduling.runMissed` |
| [`kopia.retention.latest`](#kopiaretention) | whole number, `0` or more | `retention.keepLatest` |
| [`kopia.retention.hourly`](#kopiaretention) | whole number | `retention.keepHourly` |
| [`kopia.retention.daily`](#kopiaretention) | whole number | `retention.keepDaily` |
| [`kopia.retention.weekly`](#kopiaretention) | whole number | `retention.keepWeekly` |
| [`kopia.retention.monthly`](#kopiaretention) | whole number | `retention.keepMonthly` |
| [`kopia.retention.annual`](#kopiaretention) | whole number | `retention.keepAnnual` |
| [`kopia.retention.ignore-identical`](#kopiaretentionignore-identical) | boolean | `retention.ignoreIdenticalSnapshots` |
| [`kopia.ignore`](#kopiaignore) | ignore rules, separated by `,` | `files.ignore` |
| [`kopia.ignore-files`](#kopiaignore-files) | file names, separated by `,` | `files.ignoreDotFiles` |
| [`kopia.ignore-cache-dirs`](#kopiaignore-cache-dirs) | boolean | `files.ignoreCacheDirs` |
| [`kopia.max-file-size`](#kopiamax-file-size) | [size](#sizes) | `files.maxFileSize` |
| [`kopia.one-file-system`](#kopiaone-file-system) | boolean | `files.oneFileSystem` |
| [`kopia.ignore-file-errors`](#error-handling) | boolean | `errorHandling.ignoreFileErrors` |
| [`kopia.ignore-dir-errors`](#error-handling) | boolean | `errorHandling.ignoreDirectoryErrors` |
| [`kopia.ignore-unknown-types`](#error-handling) | boolean | `errorHandling.ignoreUnknownTypes` |
| [`kopia.compression.algorithm`](#kopiacompressionalgorithm) | algorithm name or `none` | `compression.compressorName` |
| [`kopia.compression.min-size`](#kopiacompressionmin-size-and-max-size) | size | `compression.minSize` |
| [`kopia.compression.max-size`](#kopiacompressionmin-size-and-max-size) | size | `compression.maxSize` |
| [`kopia.compression.only`](#kopiacompressiononly-and-never) | file extensions, separated by `,` | `compression.onlyCompress` |
| [`kopia.compression.never`](#kopiacompressiononly-and-never) | file extensions, separated by `,` | `compression.neverCompress` |
| [`kopia.upload.parallel-files`](#kopiauploadparallel-files) | whole number, `1` or more | `upload.maxParallelFileReads` |
| [`kopia.policy-json`](#kopiapolicy-json) | JSON object | any field, merged last |

The names, defaults and boolean values of the Python version's labels (`enable`, `path`, `schedule`,
`snapshot-on-start`, `snapshot-on-stop`, `description`, `tags`) are kept. `min-interval`, `cleanup-on-remove`,
`cleanup-delete-pinned` and the policy labels are new.

## Warnings, errors and strict mode

The watcher reads all labels of a container and reports every problem, naming the label. A problem is either a
warning or an error.

- **A warning** is a problem the watcher works around, the way the Python version silently did: an unknown label
  is ignored, a value that is not a boolean counts as `false`, an invalid `min-interval` keeps the default. The
  rest of the labels apply.
- **An error** means the labels of the container cannot be applied: `kopia.path` missing while `kopia.enable` is
  `true`, a policy label whose value cannot be read (`kopia.retention.daily=7d`, `kopia.interval=30s`, a
  `kopia.policy-json` that is not a JSON object), a compression algorithm kopia server does not have, or, in
  strict mode, any warning.

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
"did you mean kopia.snapshot-on-stop?", and `kopia.compression` gets "it is a group of labels such as
kopia.compression.algorithm". Labels planned for later releases are a warning "not supported by this version" and
ignored: `kopia.volumes`, `kopia.exclude-volumes`, `kopia.pre-exec`, `kopia.post-exec`,
`kopia.stop-during-backup`, `kopia.pause-during-backup`, `kopia.pin-on-stop` and `kopia.depends-on`.

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
Their policy fields (`kopia.schedule` and the [policy labels](#policy-labels)) must agree: the running owners
decide, or all owners when none runs. They agree when they ask for the same fields with the same values, however
the labels are written: `kopia.retention.daily=7` and `kopia.policy-json={"retention":{"keepDaily":7}}` agree.
When they disagree, the source keeps the policy fields last applied, and the watcher logs an error naming the
containers and fields while an owner runs, a warning when all are stopped.

### `kopia.schedule`

When kopia server takes scheduled snapshots: one or more cron expressions separated by `;`, written to
`scheduling.cron` of the path's policy. Default `0 * * * *`, every hour, as in the Python version, unless another
label sets a schedule: with `kopia.interval` or `kopia.time-of-day` (or a cron, interval or times of day in
`kopia.policy-json`, or a `scheduling` section that is not an object, such as `null`) and no `kopia.schedule`
label, no cron is set, since kopia server would still snapshot every hour.

- `;` separates expressions because they contain spaces and commas: `0 3 * * *;30 12 * * 1-5`.
- An empty value, `kopia.schedule=""`, sets no cron: the path inherits the schedule of its parent policies
  (host or global). If the watcher had set a cron before, it removes it.
- kopia server checks the expressions, in its own time zone. The watcher asks it before it writes the policy.
  `validate` asks it too, and shows the next snapshot times, when `KOPIA_SERVER_URL` is set; without it,
  `validate` does not check cron expressions. An expression kopia server rejects is an error of the path: the source
  keeps its previous schedule, and the watcher tries again when the labels change and at every full reconcile.

The watcher owns `scheduling.cron` of the path's policy when it sets one: from a non-empty `kopia.schedule`, or
the default when no label sets a schedule. It then puts the cron back at every full reconcile if it was changed
in the web UI. With `kopia.schedule=""`, or with `kopia.interval` or `kopia.time-of-day` and no `kopia.schedule`
label, it sets no cron and leaves one set in the web UI alone. Other scheduling fields you set in the web UI,
such as an interval or times of day, stay unless a label sets them.

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

## Policy labels

These labels set fields of the policy of each path in `kopia.path`, the policy kopia server keeps for that path
alone. They never touch the host or global policy.

- **Empty means inherit.** An absent label and a label with an empty value both leave the field to the parent
  policies: those of parent directories, then `user@host`, the host and the global policy.
- **The watcher owns only the fields its labels set.** Every other field of the policy, set in the web UI, the
  kopia CLI or by Terraform, stays as it is. A field a label sets is put back at every full reconcile if it was
  changed elsewhere.
- **A removed label takes its field out.** When a label is removed or emptied, the watcher removes the field it
  set from the policy, so the path inherits it again. It knows which fields it set from `STATE_FILE`.
- **An invalid value is an error** of the container's labels: none of its policy fields apply (see
  [above](#warnings-errors-and-strict-mode)), and the message names the label. Booleans are the exception: they
  are read like [all booleans](#booleans), an invalid one counts as `false` with a warning.
- Lists are separated by `,`; spaces around items and empty items are ignored, and an item listed twice is a
  warning.
- `validate` prints the policy fields the labels set, under "policy fields", with their Kopia names and JSON
  values.

`actions` (commands kopia server runs before and after a snapshot) cannot be set by any label: they would run on
kopia server, which has to allow them. Set them on kopia server.

### Sizes

A size is a number of bytes with an optional unit, upper or lower case, with or without a space: `500MB`,
`1GiB`, `4 KiB`, `1.5GB`, `1000`. `B`, `KB`, `MB`, `GB`, `TB` count in powers of 1000, `KiB`, `MiB`, `GiB`,
`TiB` in powers of 1024. The result must be a whole number of bytes. `0` is an error: kopia does not store a
size of zero, so leave the label empty to inherit the size instead.

### `kopia.interval`

Take a snapshot every interval: a Go duration such as `30m`, `6h` or `24h` (there is no `d` unit), written to
`scheduling.intervalSeconds`. At least `1m`, the finest step of a cron schedule; a shorter interval would start
a snapshot as soon as the last one ended. It must be a whole number of seconds.

kopia server runs the source at whichever comes first of the interval, the times of day and the cron
expressions. Without a `kopia.schedule` label, `kopia.interval` turns the default hourly cron off.

```yaml
kopia.interval: "6h"
```

### `kopia.time-of-day`

Take a snapshot at these times of day, in kopia server's time zone: `HH:MM` in 24-hour time, separated by `,`.
`2:30` is read as `02:30`. Written to `scheduling.timeOfDay`, sorted. Without a `kopia.schedule` label it turns
the default hourly cron off.

kopia adds the times of day of the parent policies to the path's own. To use only the path's, add
`kopia.policy-json={"scheduling":{"noParentTimeOfDay":true}}`.

```yaml
kopia.time-of-day: "02:30,14:00"
```

### `kopia.run-missed`

`true` takes a snapshot right away when a scheduled time of day or cron run was missed, for example because kopia
server was down. Written to `scheduling.runMissed`. kopia's default is `true`.

### Schedule, pause and checks

`kopia.schedule`, `kopia.interval`, `kopia.time-of-day` and `kopia.run-missed` all go into the `scheduling`
section, which the pause replaces: while all containers of a path are stopped, the section is
`{"manual": true}`, and the watcher keeps the whole section aside in `STATE_FILE`, with fields set in the web UI,
and puts it back when a container starts. Labels changed while the containers are stopped go into the kept
section.

Before it writes a new schedule, the watcher asks kopia server for the next snapshot times of the policy as it
would be (`policy/resolve`), also while the source is paused. `validate` with `KOPIA_SERVER_URL` shows those
times.

### `kopia.retention.*`

How many snapshots of each kind kopia keeps: `kopia.retention.latest`, `.hourly`, `.daily`, `.weekly`,
`.monthly` and `.annual`, written to `retention.keepLatest`, `keepHourly`, `keepDaily`, `keepWeekly`,
`keepMonthly` and `keepAnnual`. A whole number from `0` (keep none of that kind) up to 2147483647.

```yaml
kopia.retention.latest: "10"
kopia.retention.daily: "14"
kopia.retention.weekly: "8"
kopia.retention.monthly: "12"
```

### `kopia.retention.ignore-identical`

`true` makes kopia skip a snapshot whose content is the same as the previous one's. Written to
`retention.ignoreIdenticalSnapshots`.

### `kopia.ignore`

Ignore rules in `.gitignore` syntax, separated by `,`: `*.tmp,cache/,/logs/*.log`. Written to `files.ignore`. A
rule that contains `,` cannot be written in this label; use `kopia.policy-json` for it.

The path's list replaces the list it would inherit: kopia takes the rules of the most specific policy that has
any.

### `kopia.ignore-files`

The names of the files that hold ignore rules, in every directory: `.kopiaignore,.gitignore`. Written to
`files.ignoreDotFiles`. A name with `/` is an error.

The path's list replaces the inherited one, including kopia's default `.kopiaignore`: list `.kopiaignore` too to
keep it.

### `kopia.ignore-cache-dirs`

`true` skips directories that hold a `CACHEDIR.TAG` file. Written to `files.ignoreCacheDirs`.

### `kopia.max-file-size`

Skip files larger than this [size](#sizes): `500MB`. Written to `files.maxFileSize`.

### `kopia.one-file-system`

`true` keeps the snapshot from crossing into other mounted file systems below the path. Written to
`files.oneFileSystem`.

### Error handling

`kopia.ignore-file-errors`, `kopia.ignore-dir-errors` and `kopia.ignore-unknown-types` (booleans) make a snapshot
go on past files it cannot read, directories it cannot list, and entries that are neither files, directories
nor symbolic links. Written to `errorHandling.ignoreFileErrors`, `ignoreDirectoryErrors` and
`ignoreUnknownTypes`.

### `kopia.compression.algorithm`

The compression algorithm of the path's files, written to `compression.compressorName`: a name kopia server
supports, such as `zstd`, `zstd-fastest`, `s2-default`, `gzip` or `pgzip`, or `none` to turn compression off. The
name is case-sensitive.

The watcher asks kopia server for its algorithms (`GET /api/v1/repo/algorithms`) the first time a label names
one, and keeps the answer until it restarts. An algorithm kopia server does not have is an error, with a hint
such as "did you mean zstd?"; a deprecated one is applied with a warning. `validate` checks the name too when
`KOPIA_SERVER_URL` is set.

```yaml
kopia.compression.algorithm: "zstd"
```

### `kopia.compression.min-size` and `max-size`

Compress only files of at least `min-size` and at most `max-size` ([sizes](#sizes)). Written to
`compression.minSize` and `compression.maxSize`. A `max-size` below `min-size` is an error.

### `kopia.compression.only` and `never`

File extensions with their dot, separated by `,`: `.txt,.log`. Written to `compression.onlyCompress` and
`compression.neverCompress`. An item without the leading dot, such as `txt` or `*.txt`, or with more than one
dot, such as `.tar.gz`, is an error: kopia compares the last extension only.

`kopia.compression.never` keeps files with these extensions uncompressed. Despite its name,
`kopia.compression.only` does not limit compression to its extensions: kopia (as of 0.23) only makes its
extensions compressed even when a never-list names them, and files with other extensions are compressed as
well. To compress fewer files, list the others in `kopia.compression.never`, or use `kopia.compression.min-size`
and `max-size`.

kopia adds the extensions of the parent policies to the path's own. To use only the path's, add
`kopia.policy-json={"compression":{"noParentOnlyCompress":true}}` (or `noParentNeverCompress`).

### `kopia.upload.parallel-files`

How many files kopia reads at the same time: a whole number of at least `1`. Written to
`upload.maxParallelFileReads`. kopia's default is the number of CPUs of kopia server.

### `kopia.policy-json`

Any part of a Kopia policy as a JSON object, for fields no label sets: `{"files":{"noParentIgnore":true}}`.
It is merged after the other labels: objects field by field, any other value (a list, a number, `null`) as a
whole. Where it sets the same field as a label, it wins. Its fields are owned like those of the other labels: a
field removed from the JSON, or the whole label removed, is taken out of the policy again.

```yaml
kopia.policy-json: '{"files":{"noParentIgnore":true},"logging":{"entries":{"snapshotted":5}}}'
```

It is an error when:

- the value is not one JSON object;
- it sets `actions`, even an empty object: those commands run on kopia server;
- it sets `scheduling.manual`, the pause, which belongs to the watcher;
- a key is spelled in another case than kopia's, such as `"Cron"` or `"keepdaily"`, or two keys of one object
  differ only in case: kopia server reads keys without regard to case, so they would be the same field to it
  and not to the watcher. Keys are checked for the sections and for the fields of the sections the labels set;
- a key has a dot, such as `{"files.ignore":[…]}`: nest the objects instead;
- it sets a field as a value where a label sets a field inside it, such as `{"retention":null}` together with
  `kopia.retention.daily`.

kopia server checks the policy when the watcher writes it and rejects some fields on a path policy, such as
`upload.maxParallelSnapshots`; then the source keeps the policy fields last applied, and the watcher logs the
error. kopia server drops fields it does not know: such a field never shows in the policy, and the watcher does
not write it again and again.

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
