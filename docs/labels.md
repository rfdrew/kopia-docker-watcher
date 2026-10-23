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
| [`kopia.path`](#kopiapath) | required with `enable`, unless `volumes` is set | paths, separated by `,` |
| [`kopia.volumes`](#kopiavolumes) | — | volume names, mount destinations or `*`, separated by `,` |
| [`kopia.exclude-volumes`](#kopiaexclude-volumes) | — | volume names or mount destinations, separated by `,` |
| [`kopia.schedule`](#kopiaschedule) | `0 * * * *` | cron expressions, separated by `;`; empty to inherit |
| [`kopia.snapshot-on-start`](#kopiasnapshot-on-start) | `true` | boolean |
| [`kopia.snapshot-on-stop`](#kopiasnapshot-on-stop) | `false` | boolean |
| [`kopia.min-interval`](#kopiamin-interval) | `10m` | Go duration, `0` for no minimum |
| [`kopia.description`](#kopiadescription) | the container name | text |
| [`kopia.tags`](#kopiatags) | — | `key:value` pairs, separated by `,` |
| [`kopia.cleanup-on-remove`](#kopiacleanup-on-remove) | `pause` | `pause`, `policy` or `all` |
| [`kopia.cleanup-delete-pinned`](#kopiacleanup-delete-pinned) | `false` | boolean |
| [`kopia.pre-exec`](#kopiapre-exec) | — | [command](#commands): JSON array or shell command |
| [`kopia.post-exec`](#kopiapost-exec) | — | command |
| [`kopia.hook-timeout`](#kopiahook-timeout) | `10m` | Go duration |
| [`kopia.stop-during-backup`](#kopiastop-during-backup) | `false` | boolean |
| [`kopia.pause-during-backup`](#kopiapause-during-backup) | `false` | boolean |
| [`kopia.max-downtime`](#kopiamax-downtime) | `1h` | Go duration |
| [`kopia.depends-on`](#kopiadepends-on) | — | container or compose service names, separated by `,` |

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
`snapshot-on-start`, `snapshot-on-stop`, `description`, `tags`) are kept. `volumes`, `exclude-volumes`,
`min-interval`, `cleanup-on-remove`, `cleanup-delete-pinned`, the policy labels, the [hooks](#hooks) and
[stopping and pausing](#stopping-and-pausing) are new.

## Warnings, errors and strict mode

The watcher reads all labels of a container and reports every problem, naming the label. A problem is either a
warning or an error.

- **A warning** is a problem the watcher works around, the way the Python version silently did: an unknown label
  is ignored, a value that is not a boolean counts as `false`, an invalid `min-interval` keeps the default. The
  rest of the labels apply.
- **An error** means the labels of the container cannot be applied: `kopia.path` missing while `kopia.enable` is
  `true` and `kopia.volumes` is not set, a volume or mount destination in `kopia.volumes` that the container does
  not have, a policy label whose value cannot be read (`kopia.retention.daily=7d`, `kopia.interval=30s`, a
  `kopia.policy-json` that is not a JSON object), a compression algorithm kopia server does not have, a hook
  command or `kopia.hook-timeout` that cannot be read, `kopia.stop-during-backup` together with
  `kopia.pause-during-backup`, a `kopia.max-downtime` that cannot be read, a name in `kopia.depends-on` that is not
  a container name, or, in strict mode, any warning.

What happens to a container with an error:

- A source kopia server does not know yet is not registered.
- A source it manages keeps the policy fields it last applied. The pause still follows the container: the source
  is paused while the container is stopped and unpaused while it runs.
- `kopia.description` and `kopia.tags` are not used; its snapshots are described by the container name.
- `kopia.snapshot-on-start`, `kopia.snapshot-on-stop`, `kopia.min-interval`, `kopia.cleanup-on-remove` and
  `kopia.cleanup-delete-pinned` still apply as long as that label itself has no error. A cleanup label with an
  error counts as `pause`.
- A hook whose label, or `kopia.hook-timeout`, has an error does not run: a `kopia.pre-exec` with an error takes
  no snapshot instead of one without it, a `kopia.post-exec` with an error fails after the snapshot.
- A container whose `kopia.stop-during-backup`, `kopia.pause-during-backup` or `kopia.max-downtime` has an error
  is not stopped or paused, and no snapshot of its sources is taken instead of one with it running.

The problems of a container are logged once, as "container labels have errors" at error level or "container
labels have warnings" at warning level, and again only when they change.

**Strict mode** (`run --strict`, `validate --strict`) makes every warning an error. Use it to catch typos in CI
or before a deploy; without it the watcher accepts every compose file the Python version accepted.

**A path that cannot be used** is an error of that path only: other paths of the same label still work. A path
cannot be used when it is not absolute (and `HOST_PREFIX` is not set), leads out of `HOST_PREFIX` with `..`, or
lies outside `WATCHER_PATH_PREFIX`. The same holds for the source of a mount that `kopia.volumes` picks.

**Unknown labels** under the prefix are a warning with a hint where there is one: `kopia.snapshot_on_stop` gets
"did you mean kopia.snapshot-on-stop?", and `kopia.compression` gets "it is a group of labels such as
kopia.compression.algorithm". A label planned for a later release is a warning "not supported by this version" and
ignored: `kopia.pin-on-stop`.

### Booleans

As in the Python version, only `true` in any case is true. `false` in any case is false. Any other value is
false with a warning: an empty value, `yes`, `1`, and also ` true` with spaces around it.

## Labels

### `kopia.enable`

`true` turns the backup of the container on. Without it the other labels have no effect, though the watcher
still reports problems with them.

### `kopia.path`

The paths to back up, separated by commas: `/srv/app/data,/srv/app/config`. Required when `kopia.enable` is
`true`, unless [`kopia.volumes`](#kopiavolumes) is set; with both, the container backs up the paths of both.

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

### `kopia.volumes`

The volumes and bind mounts of the container to back up, separated by commas, instead of writing their host paths
into `kopia.path`. Each item is one of:

- **a volume name**: `data`. For a container of a docker compose project, the short name of the compose file
  matches too: in project `shop`, `data` is the volume `shop_data`. The full name, `shop_data`, or the name of an
  external volume, works as well. Bind mounts have no name.
- **a mount destination**, the absolute path inside the container: `/var/lib/postgresql/data`. It matches a
  volume or a bind mount.
- **`*`**: every mount the container can write to. Read-only mounts, such as `/etc/localtime:/etc/localtime:ro`,
  only count when listed by name or destination.

The watcher reads the mounts from Docker, so the label works the same on every host:

```yaml
services:
  db:
    image: postgres:17
    volumes:
      - data:/var/lib/postgresql/data
      - ./config:/etc/postgresql
    labels:
      kopia.enable: "true"
      kopia.volumes: "data,/etc/postgresql"
volumes:
  data:
```

- **The source is the mount's path on the Docker host**, as `docker inspect` shows it under `Mounts`: for a
  local volume `/var/lib/docker/volumes/<name>/_data` (another directory under a different Docker data root or
  rootless Docker), for a bind mount its host path. From there it is a path like those of `kopia.path`:
  `HOST_PREFIX` is prepended, and the result must be absolute and lie within `WATCHER_PATH_PREFIX`.
- **kopia server must see these paths.** For volumes, mount the volume directory into the kopia server
  container, read-only, at the same path, or under the directory `HOST_PREFIX` names:
  `/var/lib/docker/volumes:/var/lib/docker/volumes:ro`. See the [example](../examples/docker-compose.yml).
- **Not backed up, with a warning:** a tmpfs and any other mount that is neither a volume nor a bind mount, a
  volume of a driver other than `local` (its data has no directory on the host), a socket such as
  `/var/run/docker.sock` (a bind mount whose host path ends in `.sock`), and a mount Docker reports no host path
  for. With `*` these warnings come for every such mount the container can write to; list them in
  [`kopia.exclude-volumes`](#kopiaexclude-volumes) to silence them. A tmpfs of `docker run --tmpfs` or the compose
  `tmpfs:` key is not among the mounts Docker reports: it is neither picked nor warned about, and naming it is an
  error (in `kopia.exclude-volumes`, a warning).
- **A `local` volume with `driver_opts`**, such as an NFS or CIFS share, is mounted by Docker on the host at its
  `_data` directory only while a container uses it. A kopia server container that mounts `/var/lib/docker/volumes`
  with the default propagation does not see such a mount and backs up an empty directory. Mount the directory with
  `rslave` propagation (`/var/lib/docker/volumes:/var/lib/docker/volumes:ro,rslave`), or back up the share where
  it lives.
- **An error:** an item that names no volume or mount destination of the container, and one that is not a volume
  name (letters, digits, `_`, `.` and `-`, starting with a letter or digit), an absolute path or `*`. A label that
  picks nothing to back up is a warning, or an error when `kopia.path` is not set either.
- Spaces around items and empty items are ignored, a trailing slash of a destination too. An item listed twice
  is a warning. Two mounts with the same host path are one source.
- **Sources from volumes are sources like any other**: the policy labels, the pause, the snapshots at a start and
  a stop, and the cleanup work the same. A volume that several containers mount is a [shared
  path](#kopiapath): with `kopia.volumes`, each of them is an owner.
- Docker's events do not carry the mounts. When a container with `kopia.volumes` starts, stops or is removed, the
  watcher queues the sources it last saw for that container and those the state records it as an owner of, and,
  unless it was removed, lists the containers to find the sources its mounts make now. A container removed
  before the watcher listed it even once, such as a `docker run --rm` of a few seconds, leaves no trace of its
  mounts: its sources get no events, so no snapshot at its stop, and the next full reconcile
  (`RECONCILE_INTERVAL`) pauses them.
- `validate --container` and `validate --all` list the sources the mounts make, naming the mount of each:
  `volume shop_data at /var/lib/postgresql/data -> /var/lib/docker/volumes/shop_data/_data`. With labels given on
  the command line there is no container, and `validate` says so instead.

### `kopia.exclude-volumes`

Mounts that [`kopia.volumes`](#kopiavolumes) must leave out, separated by commas, in the same forms but `*`:
volume names, compose short names included, and mount destinations. Mostly useful with `*`:

```yaml
labels:
  kopia.enable: "true"
  kopia.volumes: "*"
  kopia.exclude-volumes: "cache,/var/run/docker.sock"
```

- An excluded mount is neither backed up nor warned about.
- An item that names no mount of the container is a warning, and ignored. A mount `kopia.volumes` lists by name
  or destination and `kopia.exclude-volumes` excludes is an error.
- Without `kopia.volumes` the label has no effect, which is a warning.

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
- With [hooks](#hooks), `kopia.pre-exec` runs before the snapshot, right after the container started. A database
  may not take connections yet: make the hook wait for it, or set `kopia.snapshot-on-start=false`.

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
- [Hooks](#hooks) do not run around a stop snapshot: they cannot run in a container that has stopped. The
  watcher logs a warning and takes the snapshot without them. While the `kopia.pre-exec` hooks of another
  container of the path run, the stop snapshot waits for them.

The labels of the container whose event it is decide: with several owners, each container's own
`snapshot-on-start` and `snapshot-on-stop` apply to its own starts and stops.

### `kopia.min-interval`

The shortest time between snapshots that a container start may ask for. Default `10m`; `0` means no minimum. A
Go duration: `30m`, `1h`, `24h` (there is no `d` unit).

It counts from the later of the start of the source's last snapshot and the last time the watcher queued one,
which also counts a snapshot that failed, and a snapshot with hooks whose `kopia.pre-exec` failed. It does not
apply to stop snapshots or to scheduled snapshots, kopia server's or the watcher's own of a source with
[hooks](#hooks).

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

These labels set fields of the policy of each source of the container, from `kopia.path` and `kopia.volumes`, the
policy kopia server keeps for that path alone. They never touch the host or global policy.

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

As in kopia server, a missed run is taken only when the next scheduled time is more than 30 minutes away, and an
interval takes a missed run anyway. The watcher's own schedule of a source with [hooks](#hooks) follows the same
rules for the times it missed.

### Schedule, pause and checks

`kopia.schedule`, `kopia.interval`, `kopia.time-of-day` and `kopia.run-missed` all go into the `scheduling`
section, which the pause replaces: while all containers of a path are stopped, the section is
`{"manual": true}`, and the watcher keeps the whole section aside in `STATE_FILE`, with fields set in the web UI,
and puts it back when a container starts. Labels changed while the containers are stopped go into the kept
section.

Before it writes a new schedule, the watcher asks kopia server for the next snapshot times of the policy as it
would be (`policy/resolve`), also while the source is paused. `validate` with `KOPIA_SERVER_URL` shows those
times.

A source with [hooks](#hooks) keeps `{"manual": true}` while its containers run too: the watcher takes its
scheduled snapshots itself, at the times of the kept section.

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

## Hooks

Commands the watcher runs inside the container around the snapshots of its sources, as `docker exec` does:
`kopia.pre-exec` before a snapshot, `kopia.post-exec` after it. Use them to leave consistent files for kopia
server to read: dump a database into the backed-up directory, make a cache write itself to disk.

```yaml
services:
  db:
    image: postgres:17
    environment:
      POSTGRES_PASSWORD: example
    volumes:
      - /srv/db/data:/var/lib/postgresql/data
      - /srv/db/dump:/dump
    labels:
      kopia.enable: "true"
      # The dump, not the live data files.
      kopia.path: "/srv/db/dump"
      kopia.schedule: "0 3 * * *"
      # Right after a start the database may not take connections yet.
      kopia.snapshot-on-start: "false"
      kopia.pre-exec: '["pg_dump", "-U", "postgres", "-f", "/dump/postgres.sql", "postgres"]'
      kopia.post-exec: '["rm", "-f", "/dump/postgres.sql"]'
      kopia.hook-timeout: "30m"
```

More examples: `kopia.pre-exec: "mysqldump --all-databases -uroot -p\"$$MYSQL_ROOT_PASSWORD\" > /dump/all.sql"`
(a shell command; `$$` keeps docker compose from expanding the variable, so the container's own environment
does), `kopia.pre-exec: '["redis-cli", "save"]'`.

### A snapshot with hooks

1. `kopia.pre-exec` runs in every running container of the path that has one, one after the other in the order
   of the container names, each within its `kopia.hook-timeout`.
2. When one fails, no snapshot is taken. A hook fails when it exits with a code other than 0, does not finish
   within its timeout, or cannot run: the container is paused or restarting, the command does not exist, or the
   hook labels have an error. The hooks left are not run.
3. Otherwise the containers with [`kopia.stop-during-backup`](#kopiastop-during-backup) are stopped, with the
   containers that depend on them, and those with [`kopia.pause-during-backup`](#kopiapause-during-backup) are
   paused. When one cannot be, no snapshot is taken.
4. The watcher queues the snapshot and follows it to its end, as any other. When kopia server refuses to queue
   it for a reason that does not pass, such as a read-only repository, no snapshot is taken.
5. Once the snapshot has ended, however it ended, and also when no snapshot was taken, the stopped containers are
   started and the paused ones unpaused, then `kopia.post-exec` runs in every running container of the path that
   has one. All of them run, also when one fails.

A label error of a hook makes that hook fail when it is due: a broken `kopia.pre-exec` (or `kopia.hook-timeout`
next to it) takes no snapshot, a broken `kopia.post-exec` fails after the snapshot. Such a hook is logged as
"pre-exec hook not run" or "post-exec hook not run".

Every hook that ran is logged when it ends, "pre-exec hook finished" at info level or "pre-exec hook failed" at
error level, with the container, the duration and the last 4 KiB of what the command wrote to stdout and stderr,
up to then for a hook that ran out of time. The command itself is not logged: it may hold a password. A failure
counts in [`kopia_watcher_hook_failures_total`](metrics.md#kopia_watcher_hook_failures_total) and, with
`NOTIFY_WEBHOOK_URL`, is [notified](configuration.md#notifications) once, as is the next success of that hook in
that container.

Hooks run around the snapshots the watcher takes: scheduled ones (below) and those at a container start
(`kopia.snapshot-on-start`, `SNAPSHOT_ON_WATCHER_START`). They do not run around a snapshot at a stop, nor around
one started in the Kopia web UI or with the kopia CLI, which the watcher does not see coming. A snapshot at a stop
of one container that comes while the `kopia.pre-exec` hooks of another run waits until they are done and the
snapshot with hooks is queued, so that it does not read a dump half written.

There is one snapshot with hooks at a time per source. A container start during one waits until it ends and is
then taken if `kopia.min-interval` allows. `STATE_FILE` keeps how far it got, so a restart of the watcher takes it
up: when the watcher stopped while `kopia.pre-exec` ran, no snapshot is taken and `kopia.post-exec` runs; when it
stopped while `kopia.post-exec` ran, that runs again. Write hooks that can run twice. When the watcher stops, it
gives running hooks the same few seconds as its passes, and starts no new ones.

### The watcher's schedule

kopia server would take its scheduled snapshots without the hooks. So once a container of a path, running or
stopped, has `kopia.pre-exec` or `kopia.post-exec`, even one with an error, the watcher takes the scheduled
snapshots of the path's source itself:

- The `scheduling` section of the path's policy stays `{"manual": true}`, also while the containers run; the
  Kopia web UI shows the source as manual. The section it replaces, with the fields of `kopia.schedule`,
  `kopia.interval`, `kopia.time-of-day` and `kopia.run-missed`, is kept in `STATE_FILE`, as during a pause. Change
  the schedule of such a source with the labels or in its parent policies: a schedule set on the path in the web
  UI is replaced by `manual` at the next full reconcile.
- The watcher asks kopia server for the next time (`policy/resolve`) with the policy the path would have without
  hooks, so cron expressions, intervals, times of day and inherited schedules mean what they mean to kopia
  server, in its time zone. It keeps the next time in `STATE_FILE`, and works it out again after each scheduled
  snapshot, when the labels change, which drops a time of the old schedule that is due, and at every full
  reconcile, which also sees changes of the parent policies. A full reconcile keeps a time due within a minute,
  in case kopia server's clock is a little ahead. When kopia server's clock is far behind the watcher's, the
  watcher logs a warning and follows its own clock.
- While all containers of the path are stopped, no scheduled snapshot is taken. Nothing is written to kopia
  server for that: the policy is manual already. A source whose containers are all removed keeps the watcher's
  schedule, paused, until a container comes back or the cleanup removes the source.
- A time that passed while the watcher was not running, while all containers were stopped, or while a snapshot
  with hooks ran, is a missed one. As kopia server does with `runMissed`, also for a time that came during an
  upload, it is taken right away when the schedule has an interval, or when `runMissed` is on (kopia's default)
  and the next time is more than 30 minutes away; otherwise it is skipped, with a line in the log.
- `kopia.min-interval` does not hold back a scheduled snapshot, as it does not hold back kopia server's.
- Once the containers of the path have no hooks, kopia server takes the schedule back: the kept section returns to
  the policy, as after a pause.

### Commands

A hook is a command in one of two forms:

- **a JSON array of strings**, run as it is, without a shell: `["pg_dump", "-U", "app", "-f", "/dump/app.sql"]`.
  Use it for images without a shell.
- **any other text**, run with `sh -c`: `pg_dump -U app > /dump/app.sql && sync`, with redirections, `&&` and
  the variables of the container. The image must have `sh`: distroless and `scratch` images have none, and the
  hook then fails as the command does not exist.

A value that starts with `[` is always the JSON array; a mistake in it is an error of the labels. A shell
command that starts with `[ … ]` can use `test …` instead. An empty value is no hook.

The command runs as `docker exec` runs it without options: as the container's user, in its working directory,
with its environment, without a terminal or input.

### `kopia.pre-exec`

The command to run before each snapshot the watcher takes. See [hooks](#hooks).

### `kopia.post-exec`

The command to run after each snapshot the watcher takes, whatever became of it. See [hooks](#hooks).

### `kopia.hook-timeout`

How long each hook command of the container may run: a Go duration greater than zero, such as `30s`, `10m` or
`1h`. Default `10m`. A value that cannot be read is an error of the labels: a timeout far shorter than meant
would fail every snapshot.

Docker cannot stop a command started with `docker exec`. After the timeout the watcher stops waiting and counts
the hook as failed, but the command may still run in the container. For a command that may hang, use a timeout
of its own where the image has one, such as `timeout 600 pg_dump …`.

A docker-socket-proxy may close a connection that is silent for long: Tecnativa's closes it after 10 minutes. The
watcher then goes on asking Docker whether the command has ended, until the timeout; only the output after the cut
is lost. See [Docker](configuration.md#docker).

Without `kopia.pre-exec` and `kopia.post-exec` the label has no effect, with a warning.

## Stopping and pausing

Some data can only be copied consistently while nothing writes to it: the data files of a database that has no
dump command, an application that keeps its state in files it rewrites. With `kopia.stop-during-backup` the
watcher stops the container for each snapshot it takes of the container's sources, and starts it again after;
with `kopia.pause-during-backup` it freezes the container's processes instead (`docker pause`), which is quicker
but leaves what the processes hold in memory unwritten.

```yaml
services:
  app:
    image: example/app
    volumes:
      - /srv/app/data:/data
    labels:
      kopia.enable: "true"
      kopia.path: "/srv/app/data"
      kopia.schedule: "30 3 * * *"
      kopia.snapshot-on-start: "false"
      kopia.stop-during-backup: "true"
      kopia.max-downtime: "20m"
  worker:
    image: example/worker
    depends_on: [app]
    labels:
      # Stopped before app, started after it.
      kopia.depends-on: "app"
```

Like a hook, either label puts the container's sources on [the watcher's schedule](#the-watchers-schedule): the
scheduled snapshots, and those at a start, are taken by the watcher, which runs the [steps](#a-snapshot-with-hooks)
around them. A snapshot at a stop is taken without stopping anything: the container has stopped.

- **Order.** The pre-exec hooks run first, while the containers run, so a dump can be written. Then the
  containers that depend on a container to stop go down, then the container itself, then the containers to pause.
  After the snapshot they come back the other way round, before the post-exec hooks.
- **The stop** is `docker stop`: the container's stop signal, then `SIGKILL` after its stop timeout,
  `stop_grace_period` in compose, 10 seconds unless set. The watcher waits for it as long as Docker takes, so a
  container is down, not on its way, when the next one goes or the snapshot is queued. Only a running container is
  stopped or paused; one that is stopped, paused or restarting is left as it is.
- **The return.** The containers come back once the snapshot has ended, failed or not; when no snapshot could be
  taken; at the latest `kopia.max-downtime` after the watcher began to take them down, also while kopia server
  cannot be reached, with a warning: a snapshot that goes on then reads files they change. When the watcher stops
  (`docker stop`, `SIGTERM`) it brings them back at once, within the few seconds it has for its shutdown, and a
  snapshot it had not queued yet is not taken. After a crash of the watcher, or a stop that left no time, it brings
  them back when it starts again, before anything else.
- **What it remembers.** `STATE_FILE` lists every container the watcher holds down, written before the container
  goes down, with when Docker last started it. So the watcher does not start a container that someone else
  started and stopped meanwhile, nor one that is gone, and one it paused that someone else stopped stays stopped.
  A container whose stop a crash or a stop of the watcher cut short, and that still runs when the watcher comes
  back, is looked at again until it is down, for up to 15 minutes. While `STATE_FILE` cannot be written, as on a
  full disk, no container is stopped or paused and no snapshot of their sources is taken: a crash would leave them
  down.
- **Failures.** A container that cannot be stopped or paused takes no snapshot; those already down come back. A
  container that cannot be started again, say because its port is taken, is tried again with a growing delay, up
  to 5 minutes, until it runs or someone else starts or removes it. Each failure is logged at error level and
  counts in [`kopia_watcher_return_failures_total`](metrics.md#kopia_watcher_return_failures_total); with
  `NOTIFY_WEBHOOK_URL` the first one is [notified](configuration.md#notifications), and so is its return.
- **Its own events.** The stop and start of a container by the watcher are not starts and stops of the container
  for the rest of the labels: they take no `kopia.snapshot-on-stop` or `kopia.snapshot-on-start` snapshot, and while
  it is down the container still counts as running, so its sources are not paused. The same holds for every
  container that goes down with it. A paused container counts as running anyway.
- **One snapshot at a time.** A container is held down for one snapshot at a time, until the post-exec hooks of
  that snapshot have run in it. A snapshot of another source that needs it, because the container backs that
  source up too or goes down with it, waits until then, and no snapshot takes a container down while a hook of
  another source runs in it.

`kopia.max-downtime` should be longer than the stop timeout of the containers and than a snapshot usually takes:
[`kopia_watcher_backup_downtime_seconds`](metrics.md#kopia_watcher_backup_downtime_seconds) shows how long they
are down. With a docker-socket-proxy, stopping and pausing needs `CONTAINERS=1` and `POST=1` (see
[Docker](configuration.md#docker)).

### `kopia.stop-during-backup`

`true` stops the container, and every running container that depends on it by [`kopia.depends-on`](#kopiadepends-on),
for each snapshot the watcher takes of its sources. See [stopping and pausing](#stopping-and-pausing). A
[boolean](#booleans); in strict mode a value that is not one is an error, and the sources' snapshots are not
taken.

### `kopia.pause-during-backup`

`true` pauses the container for each snapshot the watcher takes of its sources. The containers that depend on it
stay as they are. Cannot be used with `kopia.stop-during-backup`: the two together are an error, and no snapshot of
the container's sources is taken.

### `kopia.max-downtime`

How long the watcher keeps the containers down for one snapshot at most, from when it begins to stop or pause
them: a Go duration greater than zero, such as `15m`, `1h` or `6h`. Default `1h`. It holds whatever kopia server
does. When several containers of a path ask for downtime, the shortest applies. The stops of a group that take
longer leave no snapshot: the watcher takes nothing more down once the time has passed, and brings back what it
took. A value that cannot be read is an error of the labels; without
`kopia.stop-during-backup` or `kopia.pause-during-backup` the label has no effect, with a warning.

### `kopia.depends-on`

The containers this container depends on, separated by `,`: when the watcher stops one of them for a snapshot,
it stops this container first and starts it after, and does the same for the containers that depend on this one.
A name matches a container of that name and, for a container of a docker compose project, the containers of that
compose service in the same project, so `kopia.depends-on: "db"` names `shop-db-1` in project `shop`. The
container needs no other label.

A name that matches no container, an item that is no name (`db cache` instead of `db,cache`), or containers that
depend on each other in a circle, are errors: no container of the group is stopped and no snapshot is taken. A
full reconcile logs them once, and again when they change, and each snapshot they keep from being taken is
logged and counted as a label error of its path; `validate --container` checks the names against the containers.
Compose's own `depends_on` is not read: write `kopia.depends-on` where a container should go down with another.

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
