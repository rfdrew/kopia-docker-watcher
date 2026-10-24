package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/control"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
	"github.com/rfdrew/kopia-docker-watcher/internal/restore"
)

const listUsage = `Usage:
  kopia-docker-watcher list [--json]

Lists the sources the running watcher manages, and the paths whose last
reconcile had a problem: the path, its containers, whether it is paused,
who takes its scheduled snapshots and when the next one is due, the start
of its last complete snapshot, kopia server's status of it and the last
problem. Run it in the watcher's container:

  docker exec kopia-docker-watcher kopia-docker-watcher list

Options:
  --json  print the list as JSON

Exit status: 0 on success, 1 if the watcher cannot be asked, 2 on a usage
error.
`

const triggerUsage = `Usage:
  kopia-docker-watcher trigger [--force] [--no-wait] CONTAINER|PATH

Asks the running watcher for a snapshot of each path of a container, or of
one source path, and waits until it has ended. The snapshot goes through
the watcher's queue like the one at a container's start: with the
pre-exec and post-exec hooks, stop-during-backup and pause-during-backup
of the path's containers, and not before the min-interval label allows
it. A snapshot that is pending or uploading already counts. Run it in the
watcher's container:

  docker exec kopia-docker-watcher kopia-docker-watcher trigger app

CONTAINER is a container's name or the start of its ID; PATH a source
path as list shows it, or a path label's path.

Options:
  --force    take the snapshot even if min-interval has not passed
  --no-wait  return once the snapshot is asked for, without its result

Interrupting the command stops the wait, not the snapshot.

Exit status: 0 if every snapshot was saved or found nothing changed (or,
with --no-wait, was asked for), 1 otherwise, 2 on a usage error.
`

const restoreUsage = `Usage:
  kopia-docker-watcher restore --target DIR [--overwrite] [--stop] CONTAINER|PATH [SNAPSHOT]

Restores a snapshot of a source into the directory DIR on kopia server,
through the running watcher, with kopia server's POST /api/v1/restore, and
follows kopia server's restore task until it ends. kopia server writes the
files: DIR is a path on kopia server, and the volume it lies on must be
mounted into kopia server writable. See docs/en/restore.md.

SNAPSHOT is a snapshot ID, or a start of it of at least six characters;
by default the newest complete snapshot. CONTAINER and PATH are as for
trigger; a container must back up one path.

By default nothing is overwritten: a DIR that exists and is not empty is
refused, so restore into a new directory and move the files yourself, or
pass --overwrite.

Options:
  --target DIR  the directory on kopia server to restore into (required)
  --overwrite   restore into a DIR that is not empty, replacing the files
                and symlinks there; files the snapshot does not have stay
  --stop        stop the path's running containers, with those that
                depend on them, while the restore runs, and start them
                again after it, as stop-during-backup does

Interrupting the command stops following the restore; with --stop the
watcher still follows it and starts the containers once it has ended.

Exit status: 0 if the restore succeeded, 1 otherwise, 2 on a usage error.
`

// controlTimeout bounds the list command, which does not wait for
// anything.
const controlTimeout = time.Minute

// controlFlags parses args with fs, the flags before, between and after
// the positional arguments, and returns those. It reports a usage error
// to stderr; ok is false then, or after --help.
func controlFlags(fs *flag.FlagSet, args []string, usage string, d deps) (pos []string, code int, ok bool) {
	fs.SetOutput(d.stderr)
	fs.Usage = func() {}

	for {
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				_, _ = fmt.Fprint(d.stdout, usage)
				return nil, 0, false
			}

			_, _ = fmt.Fprintf(d.stderr, "\n%s", usage)

			return nil, 2, false
		}

		rest := fs.Args()
		if len(rest) == 0 {
			return pos, 0, true
		}

		pos = append(pos, rest[0])
		args = rest[1:]
	}
}

// ask sends req to the running watcher, printing the progress messages to
// stdout, and returns the result, or reports the error to stderr.
func ask(ctx context.Context, cmd string, req control.Request, d deps) (control.Message, bool) {
	sock := config.ControlSocket(d.getenv)

	res, err := control.DoDial(ctx, sock, d.dialControl, req, func(m control.Message) { printProgress(d.stdout, m) })
	if err != nil {
		if errors.Is(err, context.Canceled) {
			err = errors.New("interrupted")
		}

		_, _ = fmt.Fprintf(d.stderr, "%s: %v\n", cmd, err)

		return res, false
	}

	return res, true
}

// printProgress prints a progress message.
func printProgress(w io.Writer, m control.Message) {
	switch {
	case m.RestoreProgress != nil:
		p := m.RestoreProgress
		_, _ = fmt.Fprintf(w, "restored %d files, %d directories, %s (task %s)\n", p.Files, p.Dirs, formatBytes(p.Bytes), p.TaskID)
	case m.Text != "":
		_, _ = fmt.Fprintln(w, m.Text)
	}
}

func runList(ctx context.Context, args []string, d deps) int {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "print the list as JSON")

	pos, code, ok := controlFlags(fs, args, listUsage, d)
	if !ok {
		return code
	}

	if len(pos) > 0 {
		_, _ = fmt.Fprintf(d.stderr, "list: unexpected argument %q\n\n%s", pos[0], listUsage)
		return 2
	}

	ctx, cancel := context.WithTimeout(ctx, controlTimeout)
	defer cancel()

	res, ok := ask(ctx, "list", control.Request{Command: control.CommandList}, d)
	if !ok {
		return 1
	}

	rep := reconcile.SourcesReport{Sources: []reconcile.SourceReport{}}
	if res.Sources != nil {
		rep = *res.Sources
	}

	if rep.Warning != "" {
		_, _ = fmt.Fprintln(d.stderr, "list: warning:", rep.Warning)
	}

	if *asJSON {
		enc := json.NewEncoder(d.stdout)
		enc.SetIndent("", "  ")

		if err := enc.Encode(rep); err != nil {
			_, _ = fmt.Fprintln(d.stderr, "list:", err)
			return 1
		}

		return 0
	}

	printSources(d.stdout, rep.Sources)

	return 0
}

// printSources prints the sources as a table, then their problems.
func printSources(w io.Writer, sources []reconcile.SourceReport) {
	if len(sources) == 0 {
		_, _ = fmt.Fprintln(w, "The watcher manages no source.")
		return
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "PATH\tCONTAINERS\tPAUSED\tSCHEDULE\tNEXT SNAPSHOT\tLAST SUCCESS\tSTATUS\tPROBLEM")

	for _, s := range sources {
		paused := "no"
		if s.Paused {
			paused = "yes"
		}

		status := orDash(s.Status)
		if s.HookRun != "" {
			status += ", hooks: " + s.HookRun
		}

		if len(s.Held) > 0 {
			status += ", held: " + strings.Join(s.Held, " ")
		}

		problem := "-"
		if s.Error != "" {
			problem = "yes"
		}

		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", s.Path, orDash(strings.Join(s.Owners, ",")), paused,
			orDash(s.Schedule), formatTime(s.NextSnapshot), formatTime(s.LastSuccess), status, problem)
	}

	_ = tw.Flush()

	for _, s := range sources {
		if s.Error != "" {
			_, _ = fmt.Fprintf(w, "\n%s: %s\n", s.Path, s.Error)
		}
	}
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}

	return s
}

// formatTime shows t in the local time zone, "-" for zero.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}

	return t.Local().Format("2006-01-02 15:04")
}

func runTrigger(ctx context.Context, args []string, d deps) int {
	fs := flag.NewFlagSet("trigger", flag.ContinueOnError)
	force := fs.Bool("force", false, "take the snapshot even if min-interval has not passed")
	noWait := fs.Bool("no-wait", false, "return once the snapshot is asked for")

	pos, code, ok := controlFlags(fs, args, triggerUsage, d)
	if !ok {
		return code
	}

	if len(pos) != 1 {
		_, _ = fmt.Fprintf(d.stderr, "trigger: give one container or path\n\n%s", triggerUsage)
		return 2
	}

	res, ok := ask(ctx, "trigger", control.Request{Command: control.CommandTrigger, Target: pos[0], Force: *force, NoWait: *noWait}, d)
	if !ok {
		return 1
	}

	code = 0

	for _, t := range res.Triggers {
		_, _ = fmt.Fprintln(d.stdout, describeTrigger(t))

		if t.Outcome != control.OutcomeQueued && !t.OK() {
			code = 1
		}
	}

	return code
}

// describeTrigger says how a trigger ended.
func describeTrigger(t reconcile.TriggerResult) string {
	var b strings.Builder

	b.WriteString(t.Path + ": ")

	switch t.Outcome {
	case control.OutcomeQueued:
		b.WriteString("snapshot asked for")
	case string(backend.SnapshotSucceeded):
		b.WriteString("snapshot saved")
	case string(backend.SnapshotIdentical):
		b.WriteString("nothing changed since the last snapshot, none saved")
	case string(backend.SnapshotIncomplete):
		b.WriteString("snapshot saved incomplete or with errors")
	case string(backend.SnapshotFailed):
		b.WriteString("snapshot failed")
	case string(backend.SnapshotCanceled):
		b.WriteString("snapshot canceled")
	case string(backend.SnapshotUnknown):
		b.WriteString("how the snapshot ended is not known")
	case reconcile.TriggerSkipped:
		b.WriteString("no snapshot taken")
	default:
		b.WriteString("failed")
	}

	var details []string

	if !t.SnapshotStart.IsZero() {
		details = append(details, "started "+t.SnapshotStart.Local().Format(timeLayout))
	}

	if t.TaskID != "" {
		details = append(details, "kopia task "+t.TaskID)
	}

	if t.Hooks {
		details = append(details, "with hooks")
	}

	if len(details) > 0 {
		b.WriteString(" (" + strings.Join(details, ", ") + ")")
	}

	if t.Message != "" {
		b.WriteString(": " + t.Message)
	}

	if t.PostError != "" {
		b.WriteString("; post-exec hook failed: " + t.PostError)
	}

	return b.String()
}

func runRestore(ctx context.Context, args []string, d deps) int {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	into := fs.String("target", "", "the directory on kopia server to restore into")
	overwrite := fs.Bool("overwrite", false, "restore into a directory that is not empty, replacing files")
	stop := fs.Bool("stop", false, "stop the path's running containers while the restore runs")

	pos, code, ok := controlFlags(fs, args, restoreUsage, d)
	if !ok {
		return code
	}

	switch {
	case len(pos) == 0 || len(pos) > 2:
		_, _ = fmt.Fprintf(d.stderr, "restore: give a container or path, and optionally a snapshot ID\n\n%s", restoreUsage)
		return 2
	case *into == "":
		_, _ = fmt.Fprintf(d.stderr, "restore: --target is required: the directory on kopia server to restore into\n\n%s", restoreUsage)
		return 2
	}

	if err := restore.CheckTarget(*into); err != nil {
		_, _ = fmt.Fprintf(d.stderr, "restore: --target: %v\n", err)
		return 2
	}

	req := control.Request{Command: control.CommandRestore, Target: pos[0], Into: *into, Overwrite: *overwrite, Stop: *stop}
	if len(pos) == 2 {
		req.Snapshot = pos[1]
	}

	res, ok := ask(ctx, "restore", req, d)
	if !ok {
		return 1
	}

	if res.Restore == nil {
		_, _ = fmt.Fprintln(d.stderr, "restore: the watcher answered without a result")
		return 1
	}

	r := res.Restore

	if r.OK() {
		_, _ = fmt.Fprintf(d.stdout, "restore done: %d files, %d directories, %s restored into %s (kopia task %s)\n",
			r.Files, r.Dirs, formatBytes(r.Bytes), r.Target, r.TaskID)

		return 0
	}

	msg := r.Error
	if msg == "" {
		msg = "the restore did not succeed"
	}

	_, _ = fmt.Fprintf(d.stderr, "restore failed: %s (kopia task %s, %d files restored into %s)\n", msg, orDash(r.TaskID), r.Files, r.Target)

	return 1
}

// formatBytes shows n bytes in binary units.
func formatBytes(n int64) string {
	const unit = 1024

	if n < unit {
		return fmt.Sprintf("%d B", n)
	}

	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
