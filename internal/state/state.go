// Package state keeps what the watcher has to remember about its sources
// between reconciles and across restarts: a JSON file at STATE_FILE.
//
// Kopia cannot hold this for the watcher: kopia server drops the labels of
// a policy, so nothing in a policy says which of its fields came from
// container labels. For every source the watcher has reconciled, the store
// keeps a Record, which holds backend.Applied: the policy patch last
// applied, the scheduling section a pause replaced, an apply in progress,
// the fingerprint of the stored policy. It also holds what the snapshot
// lifecycle needs across restarts: the snapshots the watcher queued and
// still waits for, when it last queued one, and the mark after which
// snapshots get a description. A record stays until it is deleted, also
// when it holds nothing else, so the store lists every source the watcher
// manages.
//
// # Using the store
//
// Each part of the watcher changes only its own fields of a record, with
// Update, never by writing back a whole record it read earlier: the
// reconciler sets r.Applied while, later on, a snapshot waiter may set its
// own fields of the same record. Every change is written to the file at
// once. When that fails the change stays in memory, the error wraps
// ErrNotPersisted, and the next change or Flush writes the file again;
// Err reports the failure meanwhile, for /healthz. Close flushes and
// releases the file.
//
// Only one watcher may write a state file: Open takes an exclusive lock on
// STATE_FILE.lock and fails with ErrLocked if another process holds it.
// --dry-run opens the store read-only, which takes no lock and writes
// nothing, so it can run next to the watcher.
//
// # File
//
// The file is a JSON object with a schema version and one entry per
// source, sorted by source:
//
//	{
//	  "version": 1,
//	  "sources": [
//	    {
//	      "source": {"host": "kopia", "userName": "root", "path": "/data/app"},
//	      "patch": {"scheduling": {"cron": ["0 * * * *"]}},
//	      "savedScheduling": {"cron": ["0 * * * *"], "runMissed": true},
//	      "storedHash": "sha256:…",
//	      "snapshotWaits": [{"ticket": {…}, "stop": true, "reason": "stop of app", "description": "app"}],
//	      "lastTriggered": "2026-10-18T12:00:00Z",
//	      "annotateAfter": "2026-10-18T11:00:00Z",
//	      "stopAbsorbedAt": "2026-10-18T12:30:00Z"
//	    }
//	  ]
//	}
//
// Every change rewrites the whole file atomically: a temporary file in the
// same directory is written, synced and renamed over STATE_FILE, so a crash
// leaves either the old or the new file. Open writes the file once as
// well, so a STATE_FILE that cannot be written (a read-only or root-owned
// volume, a single file bind-mounted in place of its directory) stops the
// watcher at start instead of losing every change later.
//
// A file that cannot be read as a state file at all (invalid JSON, no
// version) is copied to STATE_FILE.corrupt-<time> and the watcher starts
// with an empty state, logging a warning. An entry that cannot be read (no
// complete source, the same source twice, an invalid record) is dropped
// with a warning, and the file is copied aside the same way. Losing state
// is not fatal: the watcher then treats a source like one it meets for the
// first time. A file with a newer version than this watcher knows is not
// touched: Open fails, in read-only mode too.
//
// # Extending the schema
//
// Fields this watcher does not know, written by a newer one, are kept as
// they are: at the top level of the file and at the top level of each
// entry. Going back to an older watcher for a while therefore loses
// nothing, though its records are not kept up to date meanwhile; newer
// code must treat its own fields as possibly stale. A field added inside
// an existing object (inside "patch", say) is not kept by an older
// watcher, so data whose loss matters goes into a new field of the entry
// or of the file.
//
// New per-source data goes into Record as an optional field with omitempty
// or omitzero, so files without it still load. Data that is not per source
// goes into a new top-level field of the file. Fields are never renamed or
// given a new meaning in place; Version is raised, with a migration in
// load, only for a change an older watcher would misread. Planned
// additions: owners and the cleanup mode per source, and containers the
// watcher stopped itself. Records of another kopia server identity than
// the current one must never be cleaned up automatically.
package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
)

// Version is the schema version this watcher writes and the newest it
// reads.
const Version = 1

// Suffixes of the files next to STATE_FILE.
const (
	// CorruptSuffix starts the name of a copy of a file that could not be
	// read whole; the time follows.
	CorruptSuffix = ".corrupt-"
	// LockSuffix names the lock file.
	LockSuffix = ".lock"
	// tmpSuffix starts the names of temporary files.
	tmpSuffix = ".tmp-"
)

var (
	// ErrReadOnly is returned by every change to a store opened read-only.
	ErrReadOnly = errors.New("state store is read-only")
	// ErrNewerVersion is returned by Open for a file written by a newer
	// watcher.
	ErrNewerVersion = errors.New("state file was written by a newer version of the watcher")
	// ErrLocked is returned by Open when another process holds the lock.
	ErrLocked = errors.New("state file is in use by another watcher")
	// ErrNotPersisted is wrapped by the error of a change that is kept in
	// memory but could not be written to the file.
	ErrNotPersisted = errors.New("state change is kept in memory but not written to the state file")
	// ErrClosed is returned by every change after Close.
	ErrClosed = errors.New("state store is closed")
)

// Record is what the store keeps for one source.
type Record struct {
	// Applied is what the watcher last applied to the source's policy. Its
	// fields are stored at the top level of the source's entry.
	backend.Applied

	// A field added here must not be named "source", which holds the
	// entry's source, nor share a JSON name with a field of
	// backend.Applied: encoding/json would quietly drop one of the two.

	// SnapshotWaits are the snapshots the watcher queued and has not
	// finished with yet, in the order they were queued: those it still
	// waits for, and those whose result a pass has yet to act on. After a
	// restart the watcher waits for the first again and acts on the
	// others. The reconciler skips a wait it cannot use and leaves it as
	// it is, and a wait that cannot be decoded is dropped on its own, so
	// a wait never makes the entry unreadable.
	SnapshotWaits []SnapshotWait `json:"snapshotWaits,omitempty"`

	// LastTriggered is when the watcher last queued a snapshot of the
	// source; zero if never. The minimum interval between snapshots counts
	// from it as well as from the last saved snapshot: a snapshot that
	// fails saves none.
	LastTriggered time.Time `json:"lastTriggered,omitzero"`

	// AnnotateAfter is the mark of the source's snapshot descriptions: a
	// snapshot that started after it and has no description gets the one
	// of the path's containers, and the mark then moves past it. It is set
	// to the start of the source's last snapshot when the watcher first
	// manages the source, so the snapshots taken before stay as they are,
	// or to the time the watcher looked if kopia server reported none. Nil
	// means no mark yet, as in a record written before marks existed, and
	// is never read as "annotate everything".
	AnnotateAfter *time.Time `json:"annotateAfter,omitempty"`

	// StopAbsorbedAt is when a container stopped while a snapshot of the
	// source was pending, which then counts as the stop snapshot; zero if
	// none. If kopia server loses that run, the stop snapshot is taken
	// once more. StopAbsorbedDescription is the description worked out at
	// the stop, for that snapshot. StopAbsorbedLastStart is the start of
	// the source's last snapshot at the stop by kopia server's clock: only
	// a snapshot that started later can be the pending one.
	StopAbsorbedAt          time.Time `json:"stopAbsorbedAt,omitzero"`
	StopAbsorbedDescription string    `json:"stopAbsorbedDescription,omitempty"`
	StopAbsorbedLastStart   time.Time `json:"stopAbsorbedLastStart,omitzero"`
}

// SnapshotWait is a snapshot the watcher queued and has not finished with.
//
// Its fields are closed: an older watcher that rewrites the record drops
// fields it does not know inside a wait, so new data about a wait goes
// into a new field of the entry, keyed by the wait's ID.
type SnapshotWait struct {
	// ID tells the waits of a source apart.
	ID string `json:"id"`
	// Ticket follows the snapshot, as WaitSnapshot last updated it. Its
	// JSON form is part of STATE_FILE.
	Ticket backend.SnapshotTicket `json:"ticket"`
	// Stop marks a snapshot taken because a container stopped.
	Stop bool `json:"stop,omitempty"`
	// StopAt is when the last stop that this queued snapshot also counts
	// for happened; zero if none. Only a snapshot that started after it
	// counts for that stop.
	StopAt time.Time `json:"stopAt,omitzero"`
	// Reason says for the log why it was taken, e.g. "start of app".
	Reason string `json:"reason,omitempty"`
	// Description is what the snapshot is described as once it is saved,
	// worked out when it was queued: by the time it ends, the containers
	// may be gone.
	Description string `json:"description,omitempty"`
	// Retried marks a snapshot taken once more because the first one was
	// lost; it is not taken a third time.
	Retried bool `json:"retried,omitempty"`
	// Logged marks a result that was logged already, for a wait a pass
	// could not finish with yet.
	Logged bool `json:"logged,omitempty"`
	// Result is how the snapshot ended, once the watcher knows; nil while
	// it waits. A pass acts on it and then removes the wait.
	Result *SnapshotResult `json:"result,omitempty"`
}

// SnapshotResult is how a snapshot the watcher waited for ended.
type SnapshotResult struct {
	Outcome backend.SnapshotOutcome `json:"outcome"`
	TaskID  string                  `json:"taskID,omitempty"`
	// Error is the error message of a failed snapshot.
	Error string `json:"error,omitempty"`
	// SnapshotStart is the start of the snapshot the task saved; zero if
	// it saved none.
	SnapshotStart time.Time `json:"snapshotStart,omitzero"`
}

// Validate reports whether r can be stored. The snapshot waits are not
// checked: the reconciler skips those it cannot use.
func (r Record) Validate() error {
	return r.Applied.Validate()
}

// Options configure Open.
type Options struct {
	// Logger receives warnings about a corrupt file and failed writes; nil
	// discards them.
	Logger *slog.Logger
	// ReadOnly opens the store for --dry-run: no lock is taken, nothing is
	// written, renamed or created, a corrupt file is only reported, and
	// every change returns ErrReadOnly. A watcher writing the file at the
	// same time replaces it whole, so the read sees one complete version.
	ReadOnly bool
}

// Store is the state of all sources, kept in memory and written to the
// file on every change. It is safe for concurrent use.
type Store struct {
	path     string
	log      *slog.Logger
	readOnly bool

	mu sync.Mutex
	// records holds every record in its encoded form, so a Get always
	// returns a deep copy and an unchanged record is found by comparing
	// bytes.
	records map[backend.Source]entry
	// extra holds the top-level fields of the file this version does not
	// know.
	extra map[string]json.RawMessage
	// writeErr is the error of the last write; nil when the file is up to
	// date. While it is set, memory is ahead of the file.
	writeErr error
	lock     *os.File
	closed   bool
}

// entry is one source of the file: the encoded Record and the fields of
// the entry this version does not know.
type entry struct {
	record json.RawMessage
	extra  map[string]json.RawMessage
}

// Open loads the state from path. A missing file is an empty state; so is
// an empty one, with a warning. Unless opts.ReadOnly, Open takes the lock,
// removes the leftovers of interrupted writes and writes the file once.
//
// Open fails if another watcher holds the lock (ErrLocked), if the file was
// written by a newer watcher (ErrNewerVersion), or if the file cannot be
// read or written.
func Open(path string, opts Options) (*Store, error) {
	if path == "" {
		return nil, errors.New("state file path is empty")
	}

	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	s := &Store{
		path:     path,
		log:      log,
		readOnly: opts.ReadOnly,
		records:  map[backend.Source]entry{},
		extra:    map[string]json.RawMessage{},
	}

	if s.readOnly {
		if err := s.load(); err != nil {
			return nil, err
		}

		return s, nil
	}

	if err := s.openForWriting(); err != nil {
		if s.lock != nil {
			_ = s.lock.Close()
		}

		return nil, err
	}

	return s, nil
}

func (s *Store) openForWriting() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return s.notWritable(err)
	}

	if err := s.takeLock(); err != nil {
		return err
	}

	s.removeLeftovers()

	if err := s.load(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.writeLocked(); err != nil {
		return s.notWritable(err)
	}

	return nil
}

// notWritable explains a failure to write STATE_FILE.
func (s *Store) notWritable(err error) error {
	return fmt.Errorf("state file %s cannot be written by uid %d, gid %d: %w; mount a writable directory "+
		"(not a single file) owned by that user at %s, or set STATE_FILE", s.path, os.Getuid(), os.Getgid(), err, filepath.Dir(s.path))
}

// takeLock locks STATE_FILE.lock for this process.
func (s *Store) takeLock() error {
	name := s.path + LockSuffix

	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return s.notWritable(err)
	}

	err = lockFile(f)

	switch {
	case err == nil:
		s.lock = f
	case errors.Is(err, errLockUnsupported):
		s.lock = f
		s.log.Warn("the state file cannot be locked; make sure only one watcher uses it", "path", name, "error", err.Error())
	case errors.Is(err, ErrLocked):
		_ = f.Close()

		return fmt.Errorf("%w: %s is locked; run only one watcher per state file", ErrLocked, name)
	default:
		_ = f.Close()

		return fmt.Errorf("lock %s: %w", name, err)
	}

	return nil
}

// removeLeftovers deletes temporary files of writes that were interrupted.
// The lock is held, so no other process is writing one.
func (s *Store) removeLeftovers() {
	matches, err := filepath.Glob(globEscape(s.path) + tmpSuffix + "*")
	if err != nil {
		return
	}

	for _, m := range matches {
		if err := os.Remove(m); err == nil {
			s.log.Info("removed a temporary file of an interrupted state write", "path", m)
		}
	}
}

// globEscape escapes the pattern characters of a path for filepath.Glob.
func globEscape(p string) string {
	var b strings.Builder

	for _, r := range p {
		if strings.ContainsRune(`*?[\`, r) {
			b.WriteRune('\\')
		}

		b.WriteRune(r)
	}

	return b.String()
}

// Path returns the file the store is kept in.
func (s *Store) Path() string { return s.path }

// Get returns a copy of the record of src; false if there is none.
func (s *Store) Get(src backend.Source) (Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.records[src]
	if !ok {
		return Record{}, false
	}

	return mustDecode(e.record), true
}

// Sources returns the sources with a record, sorted.
func (s *Store) Sources() []backend.Source {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.SortedFunc(maps.Keys(s.records), backend.Source.Compare)
}

// Update changes the record of src, creating it if there is none, and
// writes the file. fn gets a copy of the record (the zero Record for a new
// one) and changes the fields it owns. If fn returns an error, nothing is
// changed and the error is returned. The file is not written when the
// record does not change and the file is up to date.
//
// fn runs with the store locked: it must not call the store, and should
// not wait for anything.
func (s *Store) Update(src backend.Source, fn func(r *Record) error) error {
	if err := src.Validate(); err != nil {
		return fmt.Errorf("state: %w", err)
	}

	if s.readOnly {
		return ErrReadOnly
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	old, had := s.records[src]

	var r Record
	if had {
		r = mustDecode(old.record)
	}

	if err := fn(&r); err != nil {
		return err
	}

	if err := r.Validate(); err != nil {
		return fmt.Errorf("state: record of %s: %w", src, err)
	}

	enc, err := encodeRecord(r)
	if err != nil {
		return fmt.Errorf("state: record of %s: %w", src, err)
	}

	if had && bytes.Equal(enc, old.record) && s.writeErr == nil {
		return nil
	}

	s.records[src] = entry{record: enc, extra: old.extra}

	return s.writeLocked()
}

// Delete removes the record of src and writes the file.
func (s *Store) Delete(src backend.Source) error {
	if s.readOnly {
		return ErrReadOnly
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	if _, had := s.records[src]; !had && s.writeErr == nil {
		return nil
	}

	delete(s.records, src)

	return s.writeLocked()
}

// Flush writes the file if an earlier write failed.
func (s *Store) Flush() error {
	if s.readOnly {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || s.writeErr == nil {
		return nil
	}

	return s.writeLocked()
}

// Err returns the error of the last write, nil when the file holds every
// change.
func (s *Store) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.writeErr
}

// Close writes what an earlier write failed to write and releases the
// lock. Later changes return ErrClosed.
func (s *Store) Close() error {
	if s.readOnly {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	var err error
	if s.writeErr != nil {
		err = s.writeLocked()
	}

	s.closed = true

	if s.lock != nil {
		_ = s.lock.Close()
		s.lock = nil
	}

	return err
}

// load reads the file into the store.
func (s *Store) load() error {
	data, err := os.ReadFile(s.path)

	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("read state file: %w", err)
	case len(bytes.TrimSpace(data)) == 0:
		s.log.Warn("state file is empty, starting with an empty state", "path", s.path)

		return nil
	}

	records, extra, problems, err := decode(data)

	switch {
	case errors.Is(err, ErrNewerVersion):
		return fmt.Errorf("state file %s: %w; run the newer watcher, or move the file away to start with an empty state", s.path, err)
	case err != nil:
		return s.keepCorrupt(data, "state file is corrupt, starting with an empty state", []string{err.Error()})
	}

	s.records, s.extra = records, extra

	if len(problems) > 0 {
		return s.keepCorrupt(data, "state file has entries that cannot be read in full; what cannot be read is dropped", problems)
	}

	return nil
}

// keepCorrupt reports a file that could not be read whole, and copies it
// aside unless the store is read-only.
func (s *Store) keepCorrupt(data []byte, msg string, problems []string) error {
	attrs := []any{"path", s.path, "problems", problems}

	if s.readOnly {
		s.log.Warn(msg, attrs...)

		return nil
	}

	copyPath, err := s.corruptCopyPath()
	if err == nil {
		err = writeAtomic(copyPath, filepath.Base(s.path), data)
	}

	if err != nil {
		return fmt.Errorf("state file %s cannot be read (%s), and copying it aside failed: %w",
			s.path, strings.Join(problems, "; "), err)
	}

	s.log.Warn(msg, append(attrs, "copy", copyPath)...)

	return nil
}

// corruptCopyPath returns a name for a copy of a corrupt file that is not
// taken yet.
func (s *Store) corruptCopyPath() (string, error) {
	base := s.path + CorruptSuffix + time.Now().UTC().Format("20060102T150405Z")

	for i := range 100 {
		p := base
		if i > 0 {
			p = fmt.Sprintf("%s-%d", base, i)
		}

		if _, err := os.Lstat(p); errors.Is(err, fs.ErrNotExist) {
			return p, nil
		}
	}

	return "", fmt.Errorf("too many copies of corrupt state files named %s*", base)
}

// knownEntryKeys are the keys of an entry this version reads.
var knownEntryKeys = func() map[string]bool {
	keys := jsonKeys(reflect.TypeFor[Record]())
	keys["source"] = true

	return keys
}()

// jsonKeys returns the JSON names of the fields of struct type t, with the
// fields of embedded structs and struct pointers without a name promoted,
// as encoding/json does.
func jsonKeys(t reflect.Type) map[string]bool {
	out := map[string]bool{}

	for f := range t.Fields() {
		tag := f.Tag.Get("json")
		if tag == "-" {
			continue
		}

		name, _, _ := strings.Cut(tag, ",")

		if t := f.Type; f.Anonymous && name == "" {
			if t.Kind() == reflect.Pointer {
				t = t.Elem()
			}

			if t.Kind() == reflect.Struct {
				maps.Copy(out, jsonKeys(t))

				continue
			}
		}

		if !f.IsExported() {
			continue
		}

		if name == "" {
			name = f.Name
		}

		out[name] = true
	}

	return out
}

// decode parses a state file. An error means the file cannot be used at
// all; problems are entries that were dropped.
func decode(data []byte) (records map[backend.Source]entry, extra map[string]json.RawMessage, problems []string, err error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return nil, nil, nil, fmt.Errorf("not a JSON object: %w", err)
	}

	if top == nil {
		return nil, nil, nil, errors.New("not a JSON object: null")
	}

	var version int
	if raw, ok := top["version"]; !ok || json.Unmarshal(raw, &version) != nil || version <= 0 {
		return nil, nil, nil, errors.New("no valid schema version")
	}

	if version > Version {
		return nil, nil, nil, fmt.Errorf("%w: version %d, this watcher reads up to %d", ErrNewerVersion, version, Version)
	}

	var raws []json.RawMessage
	if err := json.Unmarshal(cmpRaw(top["sources"]), &raws); err != nil {
		return nil, nil, nil, fmt.Errorf("sources: %w", err)
	}

	extra = map[string]json.RawMessage{}

	for k, v := range top {
		if k != "version" && k != "sources" {
			extra[k] = v
		}
	}

	records = make(map[backend.Source]entry, len(raws))

	for i, raw := range raws {
		src, e, note, err := decodeEntry(raw)
		if err != nil {
			problems = append(problems, fmt.Sprintf("entry %d: %v", i, err))

			continue
		}

		if note != "" {
			problems = append(problems, fmt.Sprintf("entry %d: %s: %s", i, src, note))
		}

		if _, dup := records[src]; dup {
			problems = append(problems, fmt.Sprintf("entry %d: %s is listed again", i, src))

			continue
		}

		records[src] = e
	}

	return records, extra, problems, nil
}

// cmpRaw returns raw, or null for a missing value.
func cmpRaw(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return json.RawMessage("null")
	}

	return raw
}

// snapshotKeys are the keys of an entry that hold the snapshot lifecycle
// rather than the policy. An entry whose snapshot fields cannot be decoded
// keeps its policy fields.
var snapshotKeys = func() map[string]bool {
	keys := jsonKeys(reflect.TypeFor[Record]())
	for k := range jsonKeys(reflect.TypeFor[backend.Applied]()) {
		delete(keys, k)
	}

	return keys
}()

// decodeEntry decodes one entry of the file. note tells what of it could
// not be read and was dropped: snapshot waits that do not decode, or all
// the snapshot fields if they keep the entry from decoding.
func decodeEntry(raw json.RawMessage) (src backend.Source, e entry, note string, err error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return backend.Source{}, entry{}, "", fmt.Errorf("not a JSON object: %w", err)
	}

	if fields == nil {
		return backend.Source{}, entry{}, "", errors.New("not a JSON object: null")
	}

	if err := json.Unmarshal(cmpRaw(fields["source"]), &src); err != nil {
		return backend.Source{}, entry{}, "", fmt.Errorf("source: %w", err)
	}

	if err := src.Validate(); err != nil {
		return backend.Source{}, entry{}, "", err
	}

	var (
		notes []string
		r     Record
	)

	// An entry is decoded as it is, unless a snapshot wait or another
	// snapshot field keeps it from decoding: then that part is dropped.
	dropped := dropBadWaits(fields)
	if dropped > 0 {
		notes = append(notes, fmt.Sprintf("%d snapshot waits cannot be decoded", dropped))
	}

	err = json.Unmarshal(raw, &r)
	if dropped > 0 {
		r = Record{}
		err = unmarshalFields(fields, &r)
	}

	if err != nil {
		for k := range fields {
			if isSnapshotKey(k) {
				delete(fields, k)
			}
		}

		r = Record{}
		if err2 := unmarshalFields(fields, &r); err2 != nil {
			return src, entry{}, "", fmt.Errorf("%s: %w", src, err2)
		}

		notes = append(notes, "the snapshot state cannot be decoded: "+err.Error())
	}

	// A null value means the same as a missing one.
	r.SavedScheduling = nilIfNull(r.SavedScheduling)
	r.Patch.Raw = nilIfNull(r.Patch.Raw)

	if r.Pending != nil {
		r.Pending.Raw = nilIfNull(r.Pending.Raw)
	}

	if err := r.Validate(); err != nil {
		return src, entry{}, "", fmt.Errorf("%s: %w", src, err)
	}

	enc, err := encodeRecord(r)
	if err != nil {
		return src, entry{}, "", fmt.Errorf("%s: %w", src, err)
	}

	e = entry{record: enc}

	for k, v := range fields {
		if isKnownEntryKey(k) {
			continue
		}

		if e.extra == nil {
			e.extra = map[string]json.RawMessage{}
		}

		e.extra[k] = v
	}

	return src, e, strings.Join(notes, "; "), nil
}

// dropBadWaits removes from the snapshot waits of fields, under any key
// encoding/json reads them from, the waits that do not decode, all of them
// if the value is no list, and returns how many.
func dropBadWaits(fields map[string]json.RawMessage) int {
	dropped := 0

	for k, raw := range fields {
		if !strings.EqualFold(k, "snapshotWaits") {
			continue
		}

		var list []json.RawMessage
		if err := json.Unmarshal(raw, &list); err != nil {
			delete(fields, k)
			dropped++

			continue
		}

		var (
			keep []json.RawMessage
			bad  int
		)

		for _, item := range list {
			var w SnapshotWait
			if json.Unmarshal(item, &w) != nil {
				bad++
				continue
			}

			keep = append(keep, item)
		}

		if bad > 0 {
			fields[k] = mustEncode(keep)
			dropped += bad
		}
	}

	return dropped
}

// unmarshalFields decodes the fields of an entry into r. The fields are
// encoded again without escaping <, > and &, so raw values decode as
// they were.
func unmarshalFields(fields map[string]json.RawMessage, r *Record) error {
	return json.Unmarshal(mustEncode(fields), r)
}

// isSnapshotKey reports whether encoding/json decodes key into a snapshot
// field of a Record.
func isSnapshotKey(key string) bool {
	for k := range snapshotKeys {
		if strings.EqualFold(k, key) {
			return true
		}
	}

	return false
}

// isKnownEntryKey reports whether encoding/json decodes key into a field of
// an entry: it matches field names regardless of case, so a key such as
// "Patch" is the patch and must not be kept next to it as an unknown one.
func isKnownEntryKey(key string) bool {
	for k := range knownEntryKeys {
		if strings.EqualFold(k, key) {
			return true
		}
	}

	return false
}

// nilIfNull returns nil for a JSON null.
func nilIfNull(raw json.RawMessage) json.RawMessage {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}

	return raw
}

// writeLocked writes all records to the file atomically and keeps track of
// failures. s.mu is held.
func (s *Store) writeLocked() error {
	err := writeAtomic(s.path, filepath.Base(s.path), s.encodeLocked())

	switch {
	case err != nil && s.writeErr == nil:
		s.log.Warn("writing the state file failed; changes are kept in memory until a write succeeds",
			"path", s.path, "error", err.Error())
	case err == nil && s.writeErr != nil:
		s.log.Info("state file written again", "path", s.path)
	}

	s.writeErr = err

	if err != nil {
		return fmt.Errorf("%w: %w", ErrNotPersisted, err)
	}

	return nil
}

// encodeLocked returns the file's content. s.mu is held.
func (s *Store) encodeLocked() []byte {
	var b bytes.Buffer

	fmt.Fprintf(&b, `{"version":%d,"sources":[`, Version)

	for i, src := range slices.SortedFunc(maps.Keys(s.records), backend.Source.Compare) {
		if i > 0 {
			b.WriteByte(',')
		}

		e := s.records[src]

		b.WriteString(`{"source":`)
		b.Write(mustEncode(src))

		if inner := bytes.TrimSuffix(bytes.TrimPrefix(e.record, []byte("{")), []byte("}")); len(inner) > 0 {
			b.WriteByte(',')
			b.Write(inner)
		}

		writeFields(&b, e.extra)
		b.WriteByte('}')
	}

	b.WriteByte(']')
	writeFields(&b, s.extra)
	b.WriteByte('}')

	var out bytes.Buffer
	if err := json.Indent(&out, b.Bytes(), "", "  "); err != nil {
		panic(fmt.Sprintf("state: encoded state is not valid JSON: %v", err))
	}

	out.WriteByte('\n')

	return out.Bytes()
}

// writeFields writes ,"key":value for each field, sorted by key.
func writeFields(b *bytes.Buffer, fields map[string]json.RawMessage) {
	for _, k := range slices.Sorted(maps.Keys(fields)) {
		b.WriteByte(',')
		b.Write(mustEncode(k))
		b.WriteByte(':')
		b.Write(fields[k])
	}
}

// encodeRecord returns the compact JSON object of a record, without
// escaping <, > and &.
func encodeRecord(r Record) (json.RawMessage, error) {
	var b bytes.Buffer

	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)

	if err := enc.Encode(r); err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}

	return bytes.TrimRight(b.Bytes(), "\n"), nil
}

// mustEncode encodes a value that always encodes: a source or a string.
func mustEncode(v any) []byte {
	var b bytes.Buffer

	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)

	if err := enc.Encode(v); err != nil {
		panic(fmt.Sprintf("state: encode %T: %v", v, err))
	}

	return bytes.TrimRight(b.Bytes(), "\n")
}

// mustDecode decodes a record the store encoded itself.
func mustDecode(raw json.RawMessage) Record {
	var r Record
	if err := json.Unmarshal(raw, &r); err != nil {
		panic(fmt.Sprintf("state: stored record does not decode: %v", err))
	}

	return r
}

// createTemp is os.CreateTemp; tests replace it to see the names.
var createTemp = os.CreateTemp

// writeAtomic replaces path with data: a temporary file in the same
// directory is written, synced and renamed over path, then the directory
// is synced so that the rename survives a power loss. The temporary file
// is named after tmpBase, the base name of STATE_FILE, so that
// removeLeftovers finds it after a crash, and it is removed if anything
// fails.
func writeAtomic(path, tmpBase string, data []byte) (err error) {
	dir := filepath.Dir(path)

	tmp, err := createTemp(dir, tmpBase+tmpSuffix+"*")
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}

	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("write temporary file: %w", err)
	}

	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temporary file: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary file: %w", err)
	}

	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("replace %s: %w", filepath.Base(path), err)
	}

	syncDir(dir)

	return nil
}

// syncDir makes a rename in dir durable. It is best effort: some file
// systems cannot sync a directory, and the file is complete either way.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}

	_ = d.Sync()
	_ = d.Close()
}
