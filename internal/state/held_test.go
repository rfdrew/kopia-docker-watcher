package state

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

func heldDB(at time.Time) Held {
	return Held{ID: "9b1d", Name: "db", Mode: HeldStop, Source: srcA, Run: "run-1", At: at,
		StartedAt: at.Add(-time.Hour)}
}

// The held containers are kept in the file, before the sources, and read
// back as they were.
func TestHeldRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := open(t, path, Options{})

	if got := s.Held(); got != nil {
		t.Fatalf("held of a new store = %+v", got)
	}

	at := time.Date(2026, 10, 19, 8, 0, 2, 0, time.UTC)
	db := heldDB(at)
	web := Held{ID: "77aa", Name: "web", Mode: HeldPause, Source: srcB, Run: "run-2", Order: 1, At: at, Released: true}

	if err := s.UpdateHeld(func(h []Held) ([]Held, error) { return append(h, db, web), nil }); err != nil {
		t.Fatal(err)
	}

	if err := setApplied(s, srcA, applied("0 * * * *", "")); err != nil {
		t.Fatal(err)
	}

	content := readFile(t, path)
	if i, j := strings.Index(content, `"held"`), strings.Index(content, `"sources"`); i < 0 || j < i {
		t.Fatalf("held not before the sources:\n%s", content)
	}

	s = reopen(t, s)

	if got := s.Held(); !reflect.DeepEqual(got, []Held{db, web}) {
		t.Fatalf("held = %+v", got)
	}

	// A copy: changing it changes nothing.
	got := s.Held()
	got[0].Name = "x"

	if s.Held()[0].Name != "db" {
		t.Fatal("Held returned the store's own slice")
	}

	// Removing them all removes the field.
	if err := s.UpdateHeld(func([]Held) ([]Held, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}

	if strings.Contains(readFile(t, path), `"held"`) {
		t.Fatalf("empty held still written:\n%s", readFile(t, path))
	}
}

// An update that fails or would store something invalid changes nothing;
// one that changes nothing does not write.
func TestUpdateHeld(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := open(t, path, Options{})
	db := heldDB(time.Date(2026, 10, 19, 8, 0, 0, 0, time.UTC))

	if err := s.UpdateHeld(func(h []Held) ([]Held, error) { return append(h, db), nil }); err != nil {
		t.Fatal(err)
	}

	boom := errors.New("boom")
	if err := s.UpdateHeld(func([]Held) ([]Held, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Fatalf("error = %v", err)
	}

	bad := []func([]Held) ([]Held, error){
		func(h []Held) ([]Held, error) { return append(h, db), nil },
		func(h []Held) ([]Held, error) { h[0].ID = ""; return h, nil },
		func(h []Held) ([]Held, error) { h[0].Mode = "kill"; return h, nil },
		func(h []Held) ([]Held, error) { h[0].Source.Path = ""; return h, nil },
	}

	for i, fn := range bad {
		if err := s.UpdateHeld(fn); err == nil {
			t.Errorf("update %d stored an invalid list", i)
		}
	}

	if got := s.Held(); !reflect.DeepEqual(got, []Held{db}) {
		t.Fatalf("held = %+v", got)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.UpdateHeld(func(h []Held) ([]Held, error) { return h, nil }); err != nil {
		t.Fatal(err)
	}

	if after, _ := os.Stat(path); !after.ModTime().Equal(info.ModTime()) {
		t.Error("an update that changed nothing wrote the file")
	}

	ro := open(t, path, Options{ReadOnly: true})
	if err := ro.UpdateHeld(func(h []Held) ([]Held, error) { return h, nil }); !errors.Is(err, ErrReadOnly) {
		t.Errorf("read-only: %v", err)
	}

	if got := ro.Held(); !reflect.DeepEqual(got, []Held{db}) {
		t.Errorf("read-only held = %+v", got)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if err := s.UpdateHeld(func(h []Held) ([]Held, error) { return h, nil }); !errors.Is(err, ErrClosed) {
		t.Errorf("closed: %v", err)
	}
}

// Held containers that cannot be read are dropped on their own, with a
// copy of the file; the sources are not touched.
func TestBadHeld(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	content := `{"version":1,"held":[
		{"id":"a","name":"a","mode":"stop","source":{"host":"kopia","userName":"root","path":"/data/a"},"run":"r","at":"2026-10-19T08:00:00Z"},
		{"id":"","mode":"stop","source":{"host":"kopia","userName":"root","path":"/data/a"}},
		{"id":"b","mode":"freeze","source":{"host":"kopia","userName":"root","path":"/data/a"}},
		{"id":"c","mode":"pause","source":{"host":"kopia"}},
		{"id":"a","name":"again","mode":"pause","source":{"host":"kopia","userName":"root","path":"/data/a"}},
		{"id":5},
		"x"
	],"sources":[{"source":{"host":"kopia","userName":"root","path":"/data/a"},"patch":{"scheduling":{"cron":["0 * * * *"]}}}]}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	log, lb := newLogger()
	s := open(t, path, Options{Logger: log})

	if got := s.Held(); len(got) != 1 || got[0].ID != "a" || got[0].Name != "a" {
		t.Fatalf("held = %+v", got)
	}

	assertApplied(t, s, srcA, applied("0 * * * *", ""))

	if copies := corruptCopies(t, path); len(copies) != 1 {
		t.Errorf("copies = %v", copies)
	}

	if n := len(regexp.MustCompile(`held container \d+:`).FindAllString(lb.String(), -1)); n != 6 {
		t.Errorf("log = %q, want 6 dropped held containers", lb.String())
	}

	// A list that is no list is dropped whole.
	if err := os.WriteFile(path, []byte(`{"version":1,"held":{},"sources":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if got := open(t, path, Options{Logger: log}).Held(); got != nil {
		t.Errorf("held = %+v", got)
	}
}
