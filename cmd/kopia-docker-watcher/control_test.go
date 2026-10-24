package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
)

// command runs a command against the watcher wr runs, in the way docker
// exec runs it in the watcher's container: same environment, the control
// socket of the watcher.
func (wr *watcherRun) command(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()

	td := newTestDeps(wr.td.getenv)
	td.dialControl = wr.td.control.Dial

	code = run(t.Context(), args, td.deps)

	return code, td.stdout.String(), td.stderr.String()
}

// restores returns the restore requests kopia server got.
func (f *runKopia) restoreRequests() []kopiaclient.RestoreRequest {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.restores)
}

// list, trigger and restore talk to the running watcher through its
// control socket; trigger goes through the queue and the minimum interval.
func TestControlCommands(t *testing.T) {
	k := newRunKopia(t)
	e := newWatchEngine(appContainer())

	wr := startWatcher(t, k, e, watcherOptions{})
	wr.waitLog(t, "source registered")
	wr.waitLog(t, "serving the control socket")

	if want := filepath.Join(filepath.Dir(wr.state), "control.sock"); wr.td.controlPath != want {
		t.Errorf("control socket at %q, want %q", wr.td.controlPath, want)
	}

	code, out, errOut := wr.command(t, "list")
	if code != 0 || !strings.Contains(out, "PATH") || !strings.Contains(out, "/srv/app") || !strings.Contains(out, " app ") {
		t.Fatalf("list = %d\n%s%s", code, out, errOut)
	}

	code, out, errOut = wr.command(t, "list", "--json")
	if code != 0 {
		t.Fatalf("list --json = %d: %s", code, errOut)
	}

	var rep reconcile.SourcesReport
	if err := json.Unmarshal([]byte(out), &rep); err != nil || len(rep.Sources) != 1 || rep.Sources[0].Path != "/srv/app" ||
		rep.Sources[0].Source != fakeUser+"@"+fakeHost+":/srv/app" || rep.Sources[0].Schedule != reconcile.ScheduleKopia {
		t.Fatalf("list --json: %v\n%s", err, out)
	}

	code, out, errOut = wr.command(t, "trigger", "app")
	if code != 0 || !strings.Contains(out, "/srv/app: snapshot saved") || !strings.Contains(out, "kopia task 1") {
		t.Fatalf("trigger = %d\n%s%s", code, out, errOut)
	}

	// The default minimum interval holds the next one back.
	code, out, errOut = wr.command(t, "trigger", "/srv/app")
	if code != 1 || !strings.Contains(out, "no snapshot taken") || !strings.Contains(out, "--force") {
		t.Fatalf("second trigger = %d\n%s%s", code, out, errOut)
	}

	code, out, errOut = wr.command(t, "trigger", "--no-wait", "--force", "app")
	if code != 0 || strings.TrimSpace(out) != "/srv/app: snapshot asked for" {
		t.Fatalf("trigger --no-wait = %d\n%s%s", code, out, errOut)
	}

	waitFor(t, "the forced snapshot", func() bool {
		k.mu.Lock()
		defer k.mu.Unlock()

		return len(k.uploads) == 2
	})

	code, _, errOut = wr.command(t, "trigger", "nope")
	if code != 1 || !strings.Contains(errOut, `no container named "nope"`) {
		t.Fatalf("trigger nope = %d: %s", code, errOut)
	}

	// The newest snapshot by default, into a new directory, nothing
	// overwritten.
	code, out, errOut = wr.command(t, "restore", "app", "--target", "/restore/app")
	if code != 0 || !strings.Contains(out, "restore done: 3 files, 1 directories, 2.0 KiB restored into /restore/app") ||
		!strings.Contains(out, "restoring snapshot snap2 of root@nas:/srv/app") {
		t.Fatalf("restore = %d\n%s%s", code, out, errOut)
	}

	reqs := k.restoreRequests()
	if len(reqs) != 1 || reqs[0].Root != "root2" || reqs[0].Filesystem.TargetPath != "/restore/app" ||
		reqs[0].Filesystem.OverwriteFiles || reqs[0].Filesystem.OverwriteDirectories || reqs[0].Filesystem.OverwriteSymlinks ||
		reqs[0].Options.RestoreDirEntryAtDepth != kopiaclient.FullDepth {
		t.Fatalf("restore requests = %+v", reqs)
	}

	// A directory that is not empty is refused without --overwrite. The
	// first snapshot by its ID, which its description changed.
	k.mu.Lock()
	k.nonEmpty["/srv/app"] = true
	first := k.snaps["/srv/app"][0].ID
	k.mu.Unlock()

	code, out, errOut = wr.command(t, "restore", "--target", "/srv/app", "app", first)
	if code != 1 || !strings.Contains(errOut, "non-empty directory already exists") || !strings.Contains(errOut, "--overwrite") ||
		!strings.Contains(out, "warning: app run") {
		t.Fatalf("restore over files = %d\n%s%s", code, out, errOut)
	}

	code, out, errOut = wr.command(t, "restore", "--target", "/srv/app", "--overwrite", "app", first)
	if code != 0 {
		t.Fatalf("restore --overwrite = %d\n%s%s", code, out, errOut)
	}

	reqs = k.restoreRequests()
	if len(reqs) != 3 || reqs[1].Root != "root1" || reqs[1].Filesystem.OverwriteFiles ||
		reqs[2].Root != "root1" || !reqs[2].Filesystem.OverwriteFiles || !reqs[2].Filesystem.OverwriteDirectories {
		t.Fatalf("restore requests = %+v", reqs)
	}

	code, _, errOut = wr.command(t, "restore", "--target", "/x", "app", "snapnope")
	if code != 1 || !strings.Contains(errOut, `no snapshot "snapnope"`) {
		t.Fatalf("restore of an unknown snapshot = %d: %s", code, errOut)
	}

	wr.cancel()

	if code := wr.wait(t, 10*time.Second); code != 0 {
		t.Fatalf("exit code = %d; log:\n%s", code, wr.td.stderr)
	}
}

// Usage errors exit 2 without asking the watcher.
func TestControlUsage(t *testing.T) {
	for _, args := range [][]string{
		{"list", "extra"},
		{"list", "--bogus"},
		{"trigger"},
		{"trigger", "a", "b"},
		{"restore", "app"},
		{"restore", "--target", "relative", "app"},
		{"restore", "--target", "/", "app"},
		{"restore", "--target", "/x"},
		{"restore", "--target", "/x", "app", "snap", "extra"},
	} {
		td := newTestDeps(env())

		if code := run(t.Context(), args, td.deps); code != 2 {
			t.Errorf("%q: exit code %d, want 2; stderr: %s", args, code, td.stderr)
		}
	}

	for _, cmd := range []string{"list", "trigger", "restore"} {
		td := newTestDeps(env())

		if code := run(t.Context(), []string{cmd, "--help"}, td.deps); code != 0 ||
			!strings.Contains(td.stdout.String(), "kopia-docker-watcher "+cmd) {
			t.Errorf("%s --help: %d, %q", cmd, code, td.stdout)
		}
	}
}

// Without a running watcher, the commands fail and say why; they do not
// go around it.
func TestControlWithoutWatcher(t *testing.T) {
	dir := t.TempDir()

	for _, args := range [][]string{
		{"list"},
		{"trigger", "app"},
		{"restore", "--target", "/restore", "app"},
	} {
		td := newTestDeps(env("STATE_FILE", filepath.Join(dir, "state.json")))
		td.dialControl = nil

		code := run(t.Context(), args, td.deps)
		if code != 1 || !strings.Contains(td.stderr.String(), "no watcher answers at the control socket "+filepath.Join(dir, "control.sock")) ||
			!strings.Contains(td.stderr.String(), "docker exec") {
			t.Errorf("%q: exit code %d, stderr: %s", args, code, td.stderr)
		}
	}

	if _, err := os.Stat(filepath.Join(dir, "control.sock")); err == nil {
		t.Error("a command made the socket")
	}
}

// Until kopia server is ready the watcher answers that it is starting.
func TestControlWhileStarting(t *testing.T) {
	k := newRunKopia(t)
	k.statuses = []reply{{status: 503}, {status: 503}, {status: 503}, {status: 503}, {status: 503}, {status: 503}}

	wr := startWatcher(t, k, newWatchEngine(), watcherOptions{retry: time.Second})
	wr.waitLog(t, "serving the control socket")

	code, _, errOut := wr.command(t, "list")
	if code != 1 || !strings.Contains(errOut, "the watcher is starting") {
		t.Fatalf("list = %d: %s", code, errOut)
	}
}
