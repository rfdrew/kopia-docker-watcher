package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
)

var (
	srcA = backend.Source{Host: "kopia", UserName: "root", Path: "/data/a"}
	srcB = backend.Source{Host: "kopia", UserName: "root", Path: "/data/b"}
)

func applied(cron, saved string) backend.Applied {
	var a backend.Applied
	if cron != "" {
		a.Patch.Scheduling.Cron = []string{cron}
	}

	if saved != "" {
		a.SavedScheduling = json.RawMessage(saved)
	}

	return a
}

// setApplied is how the reconciler stores what it applied.
func setApplied(s *Store, src backend.Source, a backend.Applied) error {
	return s.Update(src, func(r *Record) error {
		r.Applied = a

		return nil
	})
}

// logBuffer collects log output.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.buf.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.buf.String()
}

func newLogger() (*slog.Logger, *logBuffer) {
	var lb logBuffer

	return slog.New(slog.NewTextHandler(&lb, nil)), &lb
}

// open opens a store and closes it at the end of the test.
func open(t *testing.T, path string, opts Options) *Store {
	t.Helper()

	s, err := Open(path, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	return s
}

// reopen closes s and opens its file again.
func reopen(t *testing.T, s *Store) *Store {
	t.Helper()

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	return open(t, s.Path(), Options{})
}

func assertApplied(t *testing.T, s *Store, src backend.Source, want backend.Applied) {
	t.Helper()

	got, ok := s.Get(src)
	if !ok {
		t.Fatalf("Get(%s): no record", src)
	}

	if !got.Equal(want) {
		gj, _ := json.Marshal(got)
		wj, _ := json.Marshal(want)
		t.Errorf("Get(%s) = %s, want %s", src, gj, wj)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	return string(data)
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}

	return names
}

func TestOpenNewFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "dir", "state.json")

	s := open(t, path, Options{})
	if len(s.Sources()) != 0 {
		t.Errorf("Sources = %v", s.Sources())
	}

	// Open writes the empty state, which proves the file can be written.
	if got := readFile(t, path); got != "{\n  \"version\": 1,\n  \"sources\": []\n}\n" {
		t.Errorf("file = %q", got)
	}

	if names := dirNames(t, filepath.Dir(path)); !slices.Equal(names, []string{"state.json", "state.json.lock"}) {
		t.Errorf("directory = %v", names)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("file mode = %v, want 0600", perm)
	}
}

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := open(t, path, Options{})

	a := applied("0 * * * *", `{"cron":["0 * * * *"],"runMissed":true}`)
	a.StoredHash = "sha256:abc"
	b := applied("", `{}`)
	b.Pending = &backend.PolicyPatch{Raw: json.RawMessage(`{"files":{"ignore":["<a&b>"]}}`)}

	if err := setApplied(s, srcB, b); err != nil {
		t.Fatal(err)
	}

	if err := setApplied(s, srcA, a); err != nil {
		t.Fatal(err)
	}

	if got, want := s.Sources(), []backend.Source{srcA, srcB}; !reflect.DeepEqual(got, want) {
		t.Errorf("Sources = %v, want %v", got, want)
	}

	want := `{
  "version": 1,
  "sources": [
    {
      "source": {
        "host": "kopia",
        "userName": "root",
        "path": "/data/a"
      },
      "patch": {
        "scheduling": {
          "cron": [
            "0 * * * *"
          ]
        }
      },
      "savedScheduling": {
        "cron": [
          "0 * * * *"
        ],
        "runMissed": true
      },
      "storedHash": "sha256:abc"
    },
    {
      "source": {
        "host": "kopia",
        "userName": "root",
        "path": "/data/b"
      },
      "savedScheduling": {},
      "pending": {
        "raw": {
          "files": {
            "ignore": [
              "<a&b>"
            ]
          }
        }
      }
    }
  ]
}
`
	if got := readFile(t, path); got != want {
		t.Errorf("file =\n%s\nwant\n%s", got, want)
	}

	s = reopen(t, s)
	assertApplied(t, s, srcA, a)
	assertApplied(t, s, srcB, b)
}

// The snapshot fields round-trip next to Applied, and each part of the
// watcher changes only its own fields.
func TestSnapshotFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := open(t, path, Options{})

	t0 := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	zero := time.Time{}
	wait := SnapshotWait{
		ID: "2026-10-18T12:00:00Z",
		Ticket: backend.SnapshotTicket{Source: srcA, TriggeredAt: t0, LastSnapshotStart: t0.Add(-time.Hour),
			TasksBefore: []string{"3"}, TaskID: "4", TaskDescription: "root@kopia:/data/a at 2026-10-18T12:00:00Z"},
		Stop:        true,
		Reason:      "stop of app",
		Description: "app [env=prod]",
		Result:      &SnapshotResult{Outcome: backend.SnapshotSucceeded, TaskID: "4", SnapshotStart: t0.Add(time.Second)},
	}

	if err := s.Update(srcA, func(r *Record) error {
		r.SnapshotWaits = []SnapshotWait{wait}
		r.LastTriggered = t0
		r.AnnotateAfter = &zero

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// The reconciler sets Applied without touching them.
	if err := setApplied(s, srcA, applied("0 * * * *", "")); err != nil {
		t.Fatal(err)
	}

	// The encoded form of a wait is part of the file.
	var file struct {
		Sources []map[string]json.RawMessage `json:"sources"`
	}

	if err := json.Unmarshal([]byte(readFile(t, path)), &file); err != nil {
		t.Fatal(err)
	}

	e := file.Sources[0]
	assertCompact(t, "snapshotWaits", e["snapshotWaits"], `[{"id":"2026-10-18T12:00:00Z","ticket":{"source":{"host":"kopia",`+
		`"userName":"root","path":"/data/a"},"triggeredAt":"2026-10-18T12:00:00Z","lastSnapshotStart":"2026-10-18T11:00:00Z",`+
		`"tasksBefore":["3"],"taskID":"4","taskDescription":"root@kopia:/data/a at 2026-10-18T12:00:00Z"},"stop":true,`+
		`"reason":"stop of app","description":"app [env=prod]","result":{"outcome":"succeeded","taskID":"4",`+
		`"snapshotStart":"2026-10-18T12:00:01Z"}}]`)
	assertCompact(t, "lastTriggered", e["lastTriggered"], `"2026-10-18T12:00:00Z"`)
	assertCompact(t, "annotateAfter", e["annotateAfter"], `"0001-01-01T00:00:00Z"`)

	s = reopen(t, s)

	got, ok := s.Get(srcA)
	if !ok || len(got.SnapshotWaits) != 1 || !reflect.DeepEqual(got.SnapshotWaits[0], wait) || !got.LastTriggered.Equal(t0) ||
		got.AnnotateAfter == nil || !got.AnnotateAfter.IsZero() || !got.Patch.Equal(applied("0 * * * *", "").Patch) {
		t.Errorf("record = %+v", got)
	}

	// A record without the fields has no mark: nil, not the zero time.
	if err := setApplied(s, srcB, applied("", "")); err != nil {
		t.Fatal(err)
	}

	s = reopen(t, s)

	if got, _ := s.Get(srcB); got.AnnotateAfter != nil || got.SnapshotWaits != nil || !got.LastTriggered.IsZero() {
		t.Errorf("record without snapshot fields = %+v", got)
	}

	if strings.Contains(readFile(t, path), "null") {
		t.Errorf("empty fields written:\n%s", readFile(t, path))
	}
}

// A snapshot wait the reconciler cannot use, as one a newer watcher wrote,
// does not make its entry unreadable.
func TestSnapshotWaitsAreNotValidated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	content := `{"version":1,"sources":[{"source":{"host":"kopia","userName":"root","path":"/data/a"},` +
		`"patch":{"scheduling":{"cron":["0 * * * *"]}},"savedScheduling":{"cron":["0 * * * *"]},` +
		`"snapshotWaits":[{"id":"x","ticket":{"source":{"path":"/data/a"},"triggeredAt":"0001-01-01T00:00:00Z"}}]}]}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	log, logs := newLogger()
	s := open(t, path, Options{Logger: log})

	got, ok := s.Get(srcA)
	if !ok || len(got.SnapshotWaits) != 1 || got.SnapshotWaits[0].ID != "x" || got.SavedScheduling == nil {
		t.Errorf("record = %+v; log:\n%s", got, logs)
	}

	if strings.Contains(logs.String(), "cannot be read") {
		t.Errorf("entry reported as unreadable:\n%s", logs)
	}
}

// A wait that cannot be decoded is dropped on its own, and snapshot fields
// that cannot be decoded are dropped without the entry's policy fields.
func TestSnapshotFieldsSalvaged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	content := `{"version":1,"sources":[` +
		`{"source":{"host":"kopia","userName":"root","path":"/data/a"},"savedScheduling":{"cron":["0 * * * *"]},` +
		`"snapshotWaits":[{"id":"bad","ticket":{"tasksBefore":[3]}},{"id":"good","ticket":{"triggeredAt":"2026-10-18T12:00:00Z"}}]},` +
		`{"source":{"host":"kopia","userName":"root","path":"/data/b"},"savedScheduling":{"cron":["5 * * * *"]},"lastTriggered":5}]}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	log, logs := newLogger()
	s := open(t, path, Options{Logger: log})

	a, ok := s.Get(srcA)
	if !ok || a.SavedScheduling == nil || len(a.SnapshotWaits) != 1 || a.SnapshotWaits[0].ID != "good" {
		t.Errorf("record of a = %+v", a)
	}

	b, ok := s.Get(srcB)
	if !ok || string(b.SavedScheduling) != `{"cron":["5 * * * *"]}` || !b.LastTriggered.IsZero() {
		t.Errorf("record of b = %+v", b)
	}

	for _, want := range []string{"1 snapshot waits cannot be decoded", "the snapshot state cannot be decoded", "copy"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log lacks %q:\n%s", want, logs)
		}
	}
}

// A record stays, also when it holds nothing: the store lists every source
// the watcher manages. Only Delete removes it.
func TestRecordsStayUntilDeleted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := open(t, path, Options{})

	if err := setApplied(s, srcA, backend.Applied{}); err != nil {
		t.Fatal(err)
	}

	if _, ok := s.Get(srcA); !ok {
		t.Fatal("empty record not stored")
	}

	s = reopen(t, s)

	if got := s.Sources(); !reflect.DeepEqual(got, []backend.Source{srcA}) {
		t.Fatalf("Sources after reopen = %v", got)
	}

	if err := s.Delete(srcA); err != nil {
		t.Fatal(err)
	}

	if err := s.Delete(srcB); err != nil {
		t.Fatal(err)
	}

	if got := reopen(t, s).Sources(); len(got) != 0 {
		t.Errorf("Sources after Delete = %v", got)
	}
}

// A change that changes nothing does not write the file.
func TestUnchangedDoesNotWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := open(t, path, Options{})

	if err := setApplied(s, srcA, applied("0 * * * *", `{"cron": ["0 * * * *"]}`)); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	// Same meaning, other whitespace: unchanged.
	if err := setApplied(s, srcA, applied("0 * * * *", `{"cron":["0 * * * *"]}`)); err != nil {
		t.Fatal(err)
	}

	if err := s.Delete(srcB); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("an unchanged record wrote the file: %v", err)
	}

	if err := setApplied(s, srcA, applied("5 * * * *", "")); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Errorf("a changed record did not write the file: %v", err)
	}
}

func TestCopies(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "state.json"), Options{})

	in := applied("0 * * * *", `{"cron":["0 * * * *"]}`)
	if err := setApplied(s, srcA, in); err != nil {
		t.Fatal(err)
	}

	in.Patch.Scheduling.Cron[0] = "changed after Update"
	in.SavedScheduling[2] = 'X'

	got, _ := s.Get(srcA)
	got.Patch.Scheduling.Cron[0] = "changed after Get"
	got.SavedScheduling[2] = 'Y'

	assertApplied(t, s, srcA, applied("0 * * * *", `{"cron":["0 * * * *"]}`))
}

func TestInvalidInput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := open(t, path, Options{})

	if err := setApplied(s, backend.Source{Host: "kopia", Path: "/data"}, applied("0 * * * *", "")); err == nil {
		t.Error("incomplete source: no error")
	}

	if err := setApplied(s, srcA, applied("", `"not an object"`)); err == nil {
		t.Error("invalid saved section: no error")
	}

	bad := backend.Applied{Patch: backend.PolicyPatch{Raw: json.RawMessage(`{"scheduling":{"manual":true}}`)}}
	if err := setApplied(s, srcA, bad); err == nil {
		t.Error("invalid patch: no error")
	}

	if got := s.Sources(); len(got) != 0 {
		t.Errorf("invalid records were stored: %v", got)
	}
}

func TestUpdate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := open(t, path, Options{})

	if err := setApplied(s, srcA, applied("0 * * * *", "")); err != nil {
		t.Fatal(err)
	}

	boom := errors.New("boom")

	err := s.Update(srcA, func(r *Record) error {
		r.Patch.Scheduling.Cron = []string{"never stored"}

		return boom
	})
	if !errors.Is(err, boom) {
		t.Errorf("Update = %v, want boom", err)
	}

	assertApplied(t, s, srcA, applied("0 * * * *", ""))

	// Concurrent read-modify-write loses no update.
	const n = 50

	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			err := s.Update(srcB, func(r *Record) error {
				r.Patch.Scheduling.Cron = append(r.Patch.Scheduling.Cron, fmt.Sprintf("%d * * * *", i))

				return nil
			})
			if err != nil {
				t.Error(err)
			}
		})
	}

	wg.Wait()

	got, _ := reopen(t, s).Get(srcB)
	if len(got.Patch.Scheduling.Cron) != n {
		t.Errorf("%d crons after %d concurrent updates", len(got.Patch.Scheduling.Cron), n)
	}
}

func TestConcurrentUse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := open(t, path, Options{})

	const workers = 8

	var wg sync.WaitGroup
	for w := range workers {
		src := backend.Source{Host: "kopia", UserName: "root", Path: fmt.Sprintf("/data/%d", w)}

		wg.Go(func() {
			for i := range 20 {
				if err := setApplied(s, src, applied(fmt.Sprintf("%d * * * *", i), `{}`)); err != nil {
					t.Error(err)
				}

				if _, ok := s.Get(src); !ok {
					t.Errorf("Get(%s): no record", src)
				}

				_ = s.Sources()
				_ = s.Err()
			}
		})
	}

	wg.Wait()

	s = reopen(t, s)
	if got := len(s.Sources()); got != workers {
		t.Errorf("%d sources after reopen, want %d", got, workers)
	}

	for _, src := range s.Sources() {
		assertApplied(t, s, src, applied("19 * * * *", `{}`))
	}
}

func corruptCopies(t *testing.T, path string) []string {
	t.Helper()

	matches, err := filepath.Glob(path + CorruptSuffix + "*")
	if err != nil {
		t.Fatal(err)
	}

	return matches
}

func TestCorruptFile(t *testing.T) {
	tests := []struct {
		name, content string
	}{
		{"invalid JSON", `{"version":1,"sources":[`},
		{"trailing garbage", `{"version":1,"sources":[]}x`},
		{"array", `[]`},
		{"null", `null`},
		{"no version", `{"sources":[]}`},
		{"zero version", `{"version":0,"sources":[]}`},
		{"string version", `{"version":"1"}`},
		{"fraction version", `{"version":1.5}`},
		{"sources not an array", `{"version":1,"sources":{}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.json")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatal(err)
			}

			log, lb := newLogger()

			s := open(t, path, Options{Logger: log})
			if got := s.Sources(); len(got) != 0 {
				t.Errorf("Sources = %v", got)
			}

			copies := corruptCopies(t, path)
			if len(copies) != 1 || readFile(t, copies[0]) != tt.content {
				t.Fatalf("copies = %v", copies)
			}

			if !strings.Contains(lb.String(), "level=WARN") || !strings.Contains(lb.String(), "corrupt") {
				t.Errorf("log = %q", lb.String())
			}

			// The file now holds the empty state, and the store works.
			if got := readFile(t, path); !strings.Contains(got, `"version": 1`) {
				t.Errorf("file = %q", got)
			}

			if err := setApplied(s, srcA, applied("0 * * * *", "")); err != nil {
				t.Fatal(err)
			}

			assertApplied(t, reopen(t, s), srcA, applied("0 * * * *", ""))
		})
	}
}

// A second corrupt file gets its own copy.
func TestCorruptCopiesAreKept(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")

	for _, content := range []string{"first", "second"} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}

		s := open(t, path, Options{})
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}

	copies := corruptCopies(t, path)

	var contents []string
	for _, c := range copies {
		contents = append(contents, readFile(t, c))
	}

	slices.Sort(contents)

	if !slices.Equal(contents, []string{"first", "second"}) {
		t.Errorf("copies %v hold %v", copies, contents)
	}
}

// Entries that cannot be read are dropped one by one; the rest is kept.
func TestBadEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	content := `{"version":1,"sources":[
		{"source":{"host":"kopia","userName":"root","path":"/data/a"},"patch":{"scheduling":{"cron":["0 * * * *"]}}},
		{"source":{"host":"kopia","path":"/data/x"}},
		{"source":{"host":"kopia","userName":"root","path":"/data/a"}},
		{"source":{"host":"kopia","userName":"root","path":"/data/c"},"savedScheduling":5},
		{"source":{"host":"kopia","userName":"root","path":"/data/d"},"patch":{"raw":{"scheduling":{"manual":true}}}},
		{"source":{"host":"kopia","userName":"root","path":"/data/e"},"patch":"x"},
		{"source":"x"},
		[],
		{"patch":{}},
		{"source":{"host":"kopia","userName":"root","path":"/data/b"},"savedScheduling":{}}
	]}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	log, lb := newLogger()
	s := open(t, path, Options{Logger: log})

	if got := s.Sources(); !reflect.DeepEqual(got, []backend.Source{srcA, srcB}) {
		t.Errorf("Sources = %v", got)
	}

	assertApplied(t, s, srcA, applied("0 * * * *", ""))
	assertApplied(t, s, srcB, applied("", `{}`))

	if copies := corruptCopies(t, path); len(copies) != 1 || readFile(t, copies[0]) != content {
		t.Errorf("copies = %v", copies)
	}

	if strings.Count(lb.String(), "entry ") != 8 {
		t.Errorf("log = %q, want 8 dropped entries", lb.String())
	}
}

func TestCorruptCopyFails(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	path := filepath.Join(dir, "state.json")

	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	if os.Geteuid() == 0 {
		t.Skip("root can write to a read-only directory")
	}

	// The lock file exists, so only the copy needs a writable directory.
	for name, content := range map[string]string{path: "{", path + LockSuffix: ""} {
		if err := os.WriteFile(name, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = os.Chmod(dir, 0o750) })

	if _, err := Open(path, Options{}); err == nil || !strings.Contains(err.Error(), "copying it aside failed") {
		t.Fatalf("Open = %v", err)
	}

	if got := readFile(t, path); got != "{" {
		t.Errorf("state file = %q, want it untouched", got)
	}
}

func TestEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte(" \n"), 0o600); err != nil {
		t.Fatal(err)
	}

	log, lb := newLogger()

	s := open(t, path, Options{Logger: log})
	if got := s.Sources(); len(got) != 0 {
		t.Errorf("Sources = %v", got)
	}

	if !strings.Contains(lb.String(), "level=WARN") {
		t.Errorf("log = %q", lb.String())
	}

	if copies := corruptCopies(t, path); len(copies) != 0 {
		t.Errorf("an empty file was copied: %v", copies)
	}
}

func TestNewerVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	content := `{"version":2,"sources":{"changed":"shape"}}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, ro := range []bool{false, true} {
		if _, err := Open(path, Options{ReadOnly: ro}); !errors.Is(err, ErrNewerVersion) {
			t.Errorf("ReadOnly %v: Open = %v, want ErrNewerVersion", ro, err)
		}
	}

	if got := readFile(t, path); got != content {
		t.Errorf("file changed to %q", got)
	}

	if copies := corruptCopies(t, path); len(copies) != 0 {
		t.Errorf("a newer file was copied: %v", copies)
	}

	// The lock was released.
	if err := os.WriteFile(path, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}

	open(t, path, Options{})
}

func TestUnreadableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.Mkdir(path, 0o750); err != nil {
		t.Fatal(err)
	}

	for _, ro := range []bool{false, true} {
		if _, err := Open(path, Options{ReadOnly: ro}); err == nil {
			t.Errorf("ReadOnly %v: Open of a directory: no error", ro)
		}
	}

	if _, err := Open("", Options{}); err == nil {
		t.Error("Open of an empty path: no error")
	}
}

func TestUnwritableDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write to a read-only directory")
	}

	dir := filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = os.Chmod(dir, 0o750) })

	_, err := Open(filepath.Join(dir, "state.json"), Options{})
	if err == nil || !strings.Contains(err.Error(), "cannot be written by uid") {
		t.Errorf("Open = %v", err)
	}

	// With the lock file in place, the first write is what fails.
	if err := os.Chmod(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "state.json"+LockSuffix), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}

	_, err = Open(filepath.Join(dir, "state.json"), Options{})
	if err == nil || !strings.Contains(err.Error(), "cannot be written by uid") || !strings.Contains(err.Error(), "temporary file") {
		t.Errorf("Open = %v", err)
	}

	// A read-only store does not need to write.
	if _, err := Open(filepath.Join(dir, "state.json"), Options{ReadOnly: true}); err != nil {
		t.Errorf("read-only Open = %v", err)
	}
}

// Fields a newer watcher wrote survive a rewrite by this one.
func TestUnknownFieldsAreKept(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	content := `{"version":1,"containers":{"x":{"stoppedByWatcher":true}},"sources":[` +
		`{"source":{"host":"kopia","userName":"root","path":"/data/a"},"patch":{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepDaily":7}},` +
		`"pending":{},"cleanup":"policy","waits":[{"taskID":"1"}]}]}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	s := open(t, path, Options{})

	want := applied("0 * * * *", "")
	want.Pending = &backend.PolicyPatch{}
	assertApplied(t, s, srcA, want)

	if err := setApplied(s, srcA, applied("5 * * * *", "")); err != nil {
		t.Fatal(err)
	}

	var got struct {
		Containers json.RawMessage `json:"containers"`
		Sources    []map[string]json.RawMessage
	}

	if err := json.Unmarshal([]byte(readFile(t, path)), &got); err != nil {
		t.Fatal(err)
	}

	assertCompact(t, "containers", got.Containers, `{"x":{"stoppedByWatcher":true}}`)

	e := got.Sources[0]
	assertCompact(t, "cleanup", e["cleanup"], `"policy"`)
	assertCompact(t, "waits", e["waits"], `[{"taskID":"1"}]`)
	assertCompact(t, "patch", e["patch"], `{"scheduling":{"cron":["5 * * * *"]}}`)

	if _, ok := e["pending"]; ok {
		t.Error("pending is a known field and was cleared, but is still there")
	}

	// Still there after another round.
	s = reopen(t, s)
	if err := setApplied(s, srcA, applied("6 * * * *", "")); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(readFile(t, path), `"stoppedByWatcher": true`) || !strings.Contains(readFile(t, path), `"cleanup": "policy"`) {
		t.Errorf("unknown fields lost:\n%s", readFile(t, path))
	}
}

func assertCompact(t *testing.T, name string, got json.RawMessage, want string) {
	t.Helper()

	var b bytes.Buffer
	if err := json.Compact(&b, got); err != nil {
		t.Fatalf("%s: %v", name, err)
	}

	if b.String() != want {
		t.Errorf("%s = %s, want %s", name, b.String(), want)
	}
}

// Every key the code knows is in knownEntryKeys, so none of them is kept
// as an unknown field next to the real one.
func TestKnownEntryKeys(t *testing.T) {
	mark := time.Unix(1, 0)
	r := Record{
		Applied: backend.Applied{
			Patch:           backend.PolicyPatch{Scheduling: backend.SchedulingPatch{Cron: []string{"x"}}},
			SavedScheduling: json.RawMessage(`{}`),
			Pending:         &backend.PolicyPatch{},
			StoredHash:      "h",
		},
		SnapshotWaits:           []SnapshotWait{{ID: "1", Ticket: backend.SnapshotTicket{Source: srcA, TriggeredAt: mark}}},
		LastTriggered:           mark,
		AnnotateAfter:           &mark,
		StopAbsorbedAt:          mark,
		StopAbsorbedDescription: "d",
		StopAbsorbedLastStart:   mark,
	}

	data, err := json.Marshal(entryForTest{Source: srcA, Record: r})
	if err != nil {
		t.Fatal(err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}

	for k := range fields {
		if !knownEntryKeys[k] {
			t.Errorf("key %q is not known", k)
		}
	}

	if len(fields) != len(knownEntryKeys) {
		t.Errorf("record has %d keys, known keys are %v", len(fields), knownEntryKeys)
	}
}

type entryForTest struct {
	Source backend.Source `json:"source"`
	Record
}

func TestJSONKeys(t *testing.T) {
	type inner struct {
		A int `json:"a,omitempty"`
		B int
	}

	type ptrInner struct {
		P int `json:"p"`
	}

	type outer struct {
		inner
		*ptrInner
		C       int `json:"c"`
		D       int `json:"-"`
		E       inner
		Named   inner `json:"named"`
		private int
	}

	_ = outer{}.private

	got := jsonKeys(reflect.TypeFor[outer]())
	want := map[string]bool{"a": true, "B": true, "p": true, "c": true, "E": true, "named": true}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("jsonKeys = %v, want %v", got, want)
	}
}

func TestReadOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	w := open(t, path, Options{})
	if err := setApplied(w, srcA, applied("0 * * * *", "")); err != nil {
		t.Fatal(err)
	}

	// A read-only store opens next to a writing one.
	ro := open(t, path, Options{ReadOnly: true})
	assertApplied(t, ro, srcA, applied("0 * * * *", ""))

	if err := setApplied(ro, srcB, applied("0 * * * *", "")); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Update = %v, want ErrReadOnly", err)
	}

	if err := ro.Delete(srcA); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Delete = %v, want ErrReadOnly", err)
	}

	if err := ro.Flush(); err != nil {
		t.Errorf("Flush = %v", err)
	}

	if err := ro.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// A corrupt file is reported and left alone.
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}

	log, lb := newLogger()

	ro = open(t, path, Options{ReadOnly: true, Logger: log})
	if got := ro.Sources(); len(got) != 0 {
		t.Errorf("Sources = %v", got)
	}

	if !strings.Contains(lb.String(), "level=WARN") {
		t.Errorf("log = %q", lb.String())
	}

	if got := readFile(t, path); got != "{" {
		t.Errorf("read-only Open changed the file to %q", got)
	}

	if names := dirNames(t, dir); !slices.Equal(names, []string{"state.json", "state.json.lock"}) {
		t.Errorf("directory = %v", names)
	}

	// Nothing at all is created for a missing file.
	missing := filepath.Join(t.TempDir(), "none", "state.json")
	open(t, missing, Options{ReadOnly: true})

	if _, err := os.Stat(filepath.Dir(missing)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("read-only Open created the directory: %v", err)
	}
}

func TestLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := open(t, path, Options{})

	if _, err := Open(path, Options{}); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open = %v, want ErrLocked", err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if err := setApplied(s, srcA, backend.Applied{}); !errors.Is(err, ErrClosed) {
		t.Errorf("Update after Close = %v, want ErrClosed", err)
	}

	if err := s.Delete(srcA); !errors.Is(err, ErrClosed) {
		t.Errorf("Delete after Close = %v, want ErrClosed", err)
	}

	if err := s.Close(); err != nil {
		t.Errorf("second Close = %v", err)
	}

	open(t, path, Options{})
}

// Temporary files of interrupted writes are removed when the store opens.
func TestLeftoversRemoved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	for _, name := range []string{"state.json.tmp-123", "state.json.tmp-x", "other.tmp-1"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("partial"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	open(t, path, Options{})

	if names := dirNames(t, dir); !slices.Equal(names, []string{"other.tmp-1", "state.json", "state.json.lock"}) {
		t.Errorf("directory = %v", names)
	}
}

func TestGlobEscape(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "we*ird[1]?")
	path := filepath.Join(dir, "state.json")

	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path+tmpSuffix+"1", nil, 0o600); err != nil {
		t.Fatal(err)
	}

	open(t, path, Options{})

	if names := dirNames(t, dir); !slices.Equal(names, []string{"state.json", "state.json.lock"}) {
		t.Errorf("directory = %v", names)
	}
}

// A failed write keeps the change in memory and is reported; the next
// change or Flush writes it once writing works again.
func TestWriteFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write to a read-only directory")
	}

	dir := filepath.Join(t.TempDir(), "state")
	path := filepath.Join(dir, "state.json")
	log, lb := newLogger()
	s := open(t, path, Options{Logger: log})

	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = os.Chmod(dir, 0o750) })

	a := applied("0 * * * *", "")

	err := setApplied(s, srcA, a)
	if !errors.Is(err, ErrNotPersisted) {
		t.Fatalf("Update = %v, want ErrNotPersisted", err)
	}

	assertApplied(t, s, srcA, a)

	if s.Err() == nil {
		t.Error("Err = nil after a failed write")
	}

	if err := s.Flush(); !errors.Is(err, ErrNotPersisted) {
		t.Errorf("Flush = %v", err)
	}

	if strings.Count(lb.String(), "writing the state file failed") != 1 {
		t.Errorf("log = %q, want one warning", lb.String())
	}

	if names := dirNames(t, dir); !slices.Equal(names, []string{"state.json", "state.json.lock"}) {
		t.Errorf("directory = %v", names)
	}

	if err := os.Chmod(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	// An unchanged record is written, since the file is behind.
	if err := setApplied(s, srcA, a); err != nil {
		t.Fatal(err)
	}

	if s.Err() != nil || !strings.Contains(lb.String(), "written again") {
		t.Errorf("Err = %v, log = %q", s.Err(), lb.String())
	}

	assertApplied(t, reopen(t, s), srcA, a)
}

func TestFlushAndClose(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write to a read-only directory")
	}

	dir := filepath.Join(t.TempDir(), "state")
	path := filepath.Join(dir, "state.json")
	s := open(t, path, Options{})

	// Flush with nothing pending does not write.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Flush wrote an up-to-date store: %v", err)
	}

	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = os.Chmod(dir, 0o750) })

	if err := setApplied(s, srcA, applied("0 * * * *", "")); err == nil {
		t.Fatal("Update: no error")
	}

	if err := os.Chmod(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	// Close writes what is pending.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	assertApplied(t, open(t, path, Options{}), srcA, applied("0 * * * *", ""))
}

// A key that differs from a known one only in case is the known field to
// encoding/json; it is not kept as an unknown field next to it.
func TestCaseVariantKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	content := `{"version":1,"sources":[{"source":{"host":"kopia","userName":"root","path":"/data/a"},` +
		`"savedscheduling":{"cron":["1 * * * *"],"runMissed":true},"Patch":{"scheduling":{"cron":["1 * * * *"]}}}]}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	s := open(t, path, Options{})
	assertApplied(t, s, srcA, applied("1 * * * *", `{"cron":["1 * * * *"],"runMissed":true}`))

	if err := setApplied(s, srcA, applied("5 * * * *", "")); err != nil {
		t.Fatal(err)
	}

	got := readFile(t, path)
	if strings.Contains(got, "savedscheduling") || strings.Contains(got, `"Patch"`) || strings.Contains(got, "1 * * * *") {
		t.Errorf("stale case-variant keys kept:\n%s", got)
	}

	assertApplied(t, reopen(t, s), srcA, applied("5 * * * *", ""))
}

// null means the same as a missing field.
func TestNullFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	content := `{"version":1,"sources":[{"source":{"host":"kopia","userName":"root","path":"/data/a"},` +
		`"patch":{"scheduling":{"cron":["1 * * * *"]},"raw":null},"savedScheduling":null,"pending":null,"storedHash":null}]}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	s := open(t, path, Options{})
	assertApplied(t, s, srcA, applied("1 * * * *", ""))

	if got := readFile(t, path); strings.Contains(got, "null") {
		t.Errorf("nulls written back:\n%s", got)
	}
}

// Temporary files of a corrupt-file copy have the same name pattern as
// those of the state file, so a crash during the copy leaves nothing that
// the next Open does not remove.
func TestCorruptCopyTempNames(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}

	var patterns []string

	createTemp = func(dir, pattern string) (*os.File, error) {
		patterns = append(patterns, pattern)

		return os.CreateTemp(dir, pattern)
	}

	t.Cleanup(func() { createTemp = os.CreateTemp })

	open(t, path, Options{})

	if len(patterns) != 2 {
		t.Fatalf("patterns = %v, want the copy and the first write", patterns)
	}

	for _, p := range patterns {
		if p != "state.json"+tmpSuffix+"*" {
			t.Errorf("temporary file pattern %q", p)
		}
	}
}

// A Delete while the file is behind writes it, like an unchanged Update.
func TestDeleteWritesWhenBehind(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write to a read-only directory")
	}

	dir := filepath.Join(t.TempDir(), "state")
	path := filepath.Join(dir, "state.json")
	s := open(t, path, Options{})

	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = os.Chmod(dir, 0o750) })

	if err := setApplied(s, srcA, applied("0 * * * *", "")); err == nil {
		t.Fatal("Update: no error")
	}

	if err := os.Chmod(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	if err := s.Delete(srcB); err != nil || s.Err() != nil {
		t.Errorf("Delete = %v, Err = %v", err, s.Err())
	}

	if !strings.Contains(readFile(t, path), "/data/a") {
		t.Errorf("file not written: %s", readFile(t, path))
	}

	// A Delete that could not be written, retried: the record leaves the
	// file too.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}

	if err := s.Delete(srcA); !errors.Is(err, ErrNotPersisted) {
		t.Fatalf("Delete = %v, want ErrNotPersisted", err)
	}

	if err := os.Chmod(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	if err := s.Delete(srcA); err != nil || s.Err() != nil {
		t.Errorf("retried Delete = %v, Err = %v", err, s.Err())
	}

	if strings.Contains(readFile(t, path), "/data/a") {
		t.Errorf("deleted record still in the file: %s", readFile(t, path))
	}
}

// A write that fails after the temporary file exists removes it.
func TestTempRemovedOnFailedRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	s := open(t, path, Options{})

	// The state file is replaced by a non-empty directory: the rename fails.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(filepath.Join(path, "x"), 0o750); err != nil {
		t.Fatal(err)
	}

	if err := setApplied(s, srcA, applied("0 * * * *", "")); !errors.Is(err, ErrNotPersisted) {
		t.Fatalf("Update = %v, want ErrNotPersisted", err)
	}

	for _, n := range dirNames(t, dir) {
		if strings.Contains(n, tmpSuffix) {
			t.Errorf("temporary file left: %s", n)
		}
	}
}

// Flush after Close writes nothing: the lock is no longer held.
func TestFlushAfterClose(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write to a read-only directory")
	}

	dir := filepath.Join(t.TempDir(), "state")
	path := filepath.Join(dir, "state.json")
	s := open(t, path, Options{})

	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = os.Chmod(dir, 0o750) })

	if err := setApplied(s, srcA, applied("0 * * * *", "")); err == nil {
		t.Fatal("Update: no error")
	}

	if err := s.Close(); err == nil {
		t.Fatal("Close: no error")
	}

	if err := os.Chmod(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	before := readFile(t, path)

	if err := s.Flush(); err != nil {
		t.Errorf("Flush after Close = %v", err)
	}

	if got := readFile(t, path); got != before {
		t.Errorf("Flush after Close wrote the file:\n%s", got)
	}

	// The lock was released all the same.
	open(t, path, Options{})
}

// No field of Record shares a JSON name with a field of backend.Applied or
// with "source"; encoding/json would silently drop one of them.
func TestRecordKeysDoNotCollide(t *testing.T) {
	applied := jsonKeys(reflect.TypeFor[backend.Applied]())
	own := map[string]bool{"source": true}

	for f := range reflect.TypeFor[Record]().Fields() {
		if f.Anonymous {
			continue
		}

		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "" {
			name = f.Name
		}

		if applied[name] || own[name] {
			t.Errorf("Record field %s uses the JSON name %q twice", f.Name, name)
		}

		own[name] = true
	}

	if len(knownEntryKeys) != len(applied)+len(own) {
		t.Errorf("knownEntryKeys = %v, want the keys of Applied, Record and source", knownEntryKeys)
	}
}

// An entry is read as it is: <, > and & in raw values stay as they are.
func TestRawValuesReadAsTheyAre(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	content := `{"version":1,"sources":[{"source":{"host":"kopia","userName":"root","path":"/data/a"},` +
		`"savedScheduling":{"x":"a && b 2>&1 <c>"}}]}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	s := open(t, path, Options{})

	if got, _ := s.Get(srcA); string(got.SavedScheduling) != `{"x":"a && b 2>&1 <c>"}` {
		t.Errorf("saved scheduling = %s", got.SavedScheduling)
	}

	if !strings.Contains(readFile(t, path), `"a && b 2>&1 <c>"`) {
		t.Errorf("file:\n%s", readFile(t, path))
	}
}

// Every list of snapshot waits is cleaned, under any key encoding/json
// reads it from, and the entry decodes the same way every time.
func TestBadWaitsUnderEveryKey(t *testing.T) {
	raw := json.RawMessage(`{"source":{"host":"kopia","userName":"root","path":"/data/a"},"lastTriggered":"2026-10-18T12:00:00Z",` +
		`"SnapshotWaits":[{"id":5}],"snapshotWaits":[{"id":"ok","ticket":{"triggeredAt":"2026-10-18T12:00:00Z"}},{"id":6}]}`)

	for range 50 {
		_, e, note, err := decodeEntry(raw)
		if err != nil {
			t.Fatal(err)
		}

		r := mustDecode(e.record)
		if len(r.SnapshotWaits) != 1 || r.SnapshotWaits[0].ID != "ok" || r.LastTriggered.IsZero() || !strings.Contains(note, "2 snapshot waits") {
			t.Fatalf("record = %+v, note %q", r, note)
		}
	}
}

// An entry that cannot be read even without its snapshot fields is
// reported by the field that keeps it from decoding.
func TestUnreadableEntryNamesItsField(t *testing.T) {
	raw := json.RawMessage(`{"source":{"host":"kopia","userName":"root","path":"/data/a"},"patch":"x","lastTriggered":5}`)

	_, _, _, err := decodeEntry(raw)
	if err == nil || !strings.Contains(err.Error(), "Record.patch") {
		t.Errorf("error = %v", err)
	}
}
