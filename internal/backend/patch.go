package backend

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// PolicyPatch is the part of a path's policy the watcher owns: the fields
// the labels of the source's owners set. Every other field of the policy
// belongs to whoever else edits it (the web UI, the kopia CLI, Terraform)
// and is kept as it is.
//
// The typed sections mirror the sections of a Kopia policy: their JSON
// names are Kopia's, so the JSON form of a section is the part of the
// policy it sets. A field the labels leave to inheritance is not in the
// patch: a nil pointer, an empty list or string, or a zero size. When a
// field that was in the last applied patch is no longer in the new one,
// Merge takes it out of the policy, so the policy inherits it again.
//
// The pause is not part of the patch: it is Desired.Paused, and it is
// always the watcher's.
//
// The JSON form of a PolicyPatch is stored in STATE_FILE as the last
// applied patch. Fields are only ever added, with omitempty or omitzero,
// and never renamed.
type PolicyPatch struct {
	Scheduling    SchedulingPatch    `json:"scheduling,omitzero"`
	Retention     RetentionPatch     `json:"retention,omitzero"`
	Files         FilesPatch         `json:"files,omitzero"`
	ErrorHandling ErrorHandlingPatch `json:"errorHandling,omitzero"`
	Compression   CompressionPatch   `json:"compression,omitzero"`
	Upload        UploadPatch        `json:"upload,omitzero"`

	// Raw is a JSON object merged into the policy after the typed fields,
	// for labels that carry policy JSON as is. Objects in it are merged
	// field by field; any other value, arrays and null included, replaces
	// the field it names and is owned as a whole. Raw wins over a typed
	// field with the same name. It must not set scheduling.manual, which
	// belongs to the pause, or actions, the commands kopia server would
	// run. kopia server drops fields it does not know and zero values of
	// plain fields, so such a field never reads back as written;
	// Applied.StoredHash keeps that from causing a write on every
	// reconcile.
	Raw json.RawMessage `json:"raw,omitempty"`
}

// SchedulingPatch is the scheduling section of a PolicyPatch.
type SchedulingPatch struct {
	// Cron is written to scheduling.cron. Empty means the watcher does not
	// own the field, as when the schedule label is set to an empty value
	// to inherit the schedule.
	Cron []string `json:"cron,omitempty"`
	// IntervalSeconds is written to scheduling.intervalSeconds; zero means
	// not owned.
	IntervalSeconds int64 `json:"intervalSeconds,omitempty"`
	// TimeOfDay is written to scheduling.timeOfDay.
	TimeOfDay []TimeOfDay `json:"timeOfDay,omitempty"`
	// RunMissed is written to scheduling.runMissed.
	RunMissed *bool `json:"runMissed,omitempty"`
}

// TimeOfDay is one time of day of a schedule, in kopia server's time zone.
type TimeOfDay struct {
	Hour   int `json:"hour"`
	Minute int `json:"min"`
}

// RetentionPatch is the retention section of a PolicyPatch. A count of
// zero is owned: it keeps no snapshot of that kind.
type RetentionPatch struct {
	KeepLatest               *int  `json:"keepLatest,omitempty"`
	KeepHourly               *int  `json:"keepHourly,omitempty"`
	KeepDaily                *int  `json:"keepDaily,omitempty"`
	KeepWeekly               *int  `json:"keepWeekly,omitempty"`
	KeepMonthly              *int  `json:"keepMonthly,omitempty"`
	KeepAnnual               *int  `json:"keepAnnual,omitempty"`
	IgnoreIdenticalSnapshots *bool `json:"ignoreIdenticalSnapshots,omitempty"`
}

// FilesPatch is the files section of a PolicyPatch.
type FilesPatch struct {
	// Ignore are the ignore rules, in .gitignore syntax.
	Ignore []string `json:"ignore,omitempty"`
	// IgnoreDotFiles are the names of the files that hold ignore rules,
	// such as .kopiaignore.
	IgnoreDotFiles  []string `json:"ignoreDotFiles,omitempty"`
	IgnoreCacheDirs *bool    `json:"ignoreCacheDirs,omitempty"`
	// MaxFileSize is in bytes; zero means not owned.
	MaxFileSize   int64 `json:"maxFileSize,omitempty"`
	OneFileSystem *bool `json:"oneFileSystem,omitempty"`
}

// ErrorHandlingPatch is the errorHandling section of a PolicyPatch.
type ErrorHandlingPatch struct {
	IgnoreFileErrors      *bool `json:"ignoreFileErrors,omitempty"`
	IgnoreDirectoryErrors *bool `json:"ignoreDirectoryErrors,omitempty"`
	IgnoreUnknownTypes    *bool `json:"ignoreUnknownTypes,omitempty"`
}

// CompressionPatch is the compression section of a PolicyPatch.
type CompressionPatch struct {
	// CompressorName is a compression algorithm kopia server supports
	// (Backend.CompressionAlgorithms), or "none".
	CompressorName string `json:"compressorName,omitempty"`
	// MinSize and MaxSize are in bytes; zero means not owned.
	MinSize int64 `json:"minSize,omitempty"`
	MaxSize int64 `json:"maxSize,omitempty"`
	// OnlyCompress and NeverCompress are file extensions with the dot,
	// such as .txt.
	OnlyCompress  []string `json:"onlyCompress,omitempty"`
	NeverCompress []string `json:"neverCompress,omitempty"`
}

// UploadPatch is the upload section of a PolicyPatch.
type UploadPatch struct {
	MaxParallelFileReads *int `json:"maxParallelFileReads,omitempty"`
}

// IsZero reports whether s sets nothing.
func (s SchedulingPatch) IsZero() bool {
	return len(s.Cron) == 0 && s.IntervalSeconds == 0 && len(s.TimeOfDay) == 0 && s.RunMissed == nil
}

// IsZero reports whether r sets nothing.
func (r RetentionPatch) IsZero() bool { return r == RetentionPatch{} }

// IsZero reports whether f sets nothing.
func (f FilesPatch) IsZero() bool {
	return len(f.Ignore) == 0 && len(f.IgnoreDotFiles) == 0 && f.IgnoreCacheDirs == nil && f.MaxFileSize == 0 &&
		f.OneFileSystem == nil
}

// IsZero reports whether e sets nothing.
func (e ErrorHandlingPatch) IsZero() bool { return e == ErrorHandlingPatch{} }

// IsZero reports whether c sets nothing.
func (c CompressionPatch) IsZero() bool {
	return c.CompressorName == "" && c.MinSize == 0 && c.MaxSize == 0 && len(c.OnlyCompress) == 0 &&
		len(c.NeverCompress) == 0
}

// IsZero reports whether u sets nothing.
func (u UploadPatch) IsZero() bool { return u == UploadPatch{} }

// IsZero reports whether p sets nothing.
func (p PolicyPatch) IsZero() bool {
	return p.Scheduling.IsZero() && p.Retention.IsZero() && p.Files.IsZero() && p.ErrorHandling.IsZero() &&
		p.Compression.IsZero() && p.Upload.IsZero() && isNull(p.Raw)
}

// Field is one policy field a patch owns: the path of object keys from the
// policy root, and the compact JSON value to store there.
type Field struct {
	Path  []string
	Value json.RawMessage
}

// Name returns the dotted name of the field, e.g. "scheduling.cron".
func (f Field) Name() string { return strings.Join(f.Path, ".") }

// manualField is the scheduling field of the pause.
var manualField = []string{"scheduling", "manual"}

// actionsSection is the section of the commands kopia server runs before
// and after a snapshot. The watcher never sets it: the commands would run
// on kopia server, which has to allow them.
const actionsSection = "actions"

// Fields returns the fields p owns, sorted by path: the typed fields, then
// the leaves of Raw on top. An invalid patch is an ErrConfig error.
func (p PolicyPatch) Fields() ([]Field, error) {
	byName := map[string]Field{}

	typed := p
	typed.Raw = nil

	enc, err := encode(typed)
	if err != nil {
		return nil, configError(err)
	}

	// The sections' JSON names are Kopia's and unset fields are omitted,
	// so the leaves of the typed sections are the fields they own; a list
	// such as timeOfDay is one leaf.
	fields, err := leaves(enc)
	if err != nil {
		return nil, configError(err)
	}

	for _, f := range fields {
		byName[f.Name()] = f
	}

	if !isNull(p.Raw) {
		raw, err := rawFields(p.Raw)
		if err != nil {
			return nil, err
		}

		for _, f := range raw {
			byName[f.Name()] = f
		}
	}

	out := make([]Field, 0, len(byName))
	for _, f := range byName {
		out = append(out, f)
	}

	slices.SortFunc(out, func(a, b Field) int { return slices.Compare(a.Path, b.Path) })

	for i := 1; i < len(out); i++ {
		if isPrefix(out[i-1].Path, out[i].Path) {
			return nil, configError(fmt.Errorf("policy field %s is set both as a value and as an object with %s", out[i-1].Name(), out[i].Name()))
		}
	}

	return out, nil
}

// rawFields returns the leaves of a Raw patch, checking that it is a JSON
// object that sets neither the pause nor actions.
func rawFields(raw json.RawMessage) ([]Field, error) {
	if !isObject(raw) {
		return nil, configError(fmt.Errorf("raw policy must be a JSON object, got %s", truncate(raw)))
	}

	obj, err := parseObject(raw)
	if err != nil {
		return nil, configError(fmt.Errorf("raw policy: %w", err))
	}

	// An empty object has no leaves, so these are checked by key.
	if _, ok := obj[actionsSection]; ok {
		return nil, configError(errors.New("raw policy must not set actions: they would run commands on kopia server"))
	}

	if _, ok := obj.get(manualField); ok {
		return nil, configError(errors.New("raw policy must not set scheduling.manual: the watcher pauses a source when all its containers are stopped"))
	}

	fields, err := leaves(raw)
	if err != nil {
		return nil, configError(fmt.Errorf("raw policy: %w", err))
	}

	return fields, nil
}

// Validate reports an invalid patch as an ErrConfig error.
func (p PolicyPatch) Validate() error {
	_, err := p.Fields()

	return err
}

// Equal reports whether p and q set the same fields to the same values.
// Two invalid patches are never equal.
func (p PolicyPatch) Equal(q PolicyPatch) bool {
	pf, err := p.Fields()
	if err != nil {
		return false
	}

	qf, err := q.Fields()
	if err != nil {
		return false
	}

	return slices.EqualFunc(pf, qf, func(a, b Field) bool {
		if !slices.Equal(a.Path, b.Path) {
			return false
		}

		same, err := sameJSON(a.Value, b.Value)

		return err == nil && same
	})
}

// isPrefix reports whether a is a proper prefix of b. Sorted paths put a
// prefix right before the paths it is a prefix of.
func isPrefix(a, b []string) bool {
	return len(a) < len(b) && slices.Equal(a, b[:len(a)])
}

func configError(err error) error {
	return &Error{Op: "PolicyPatch", Kind: ErrConfig, Err: err}
}

// Clone returns a deep copy of p.
func (p PolicyPatch) Clone() PolicyPatch {
	out := p

	out.Scheduling.Cron = slices.Clone(p.Scheduling.Cron)
	out.Scheduling.TimeOfDay = slices.Clone(p.Scheduling.TimeOfDay)
	out.Scheduling.RunMissed = clonePtr(p.Scheduling.RunMissed)

	out.Retention = RetentionPatch{
		KeepLatest:               clonePtr(p.Retention.KeepLatest),
		KeepHourly:               clonePtr(p.Retention.KeepHourly),
		KeepDaily:                clonePtr(p.Retention.KeepDaily),
		KeepWeekly:               clonePtr(p.Retention.KeepWeekly),
		KeepMonthly:              clonePtr(p.Retention.KeepMonthly),
		KeepAnnual:               clonePtr(p.Retention.KeepAnnual),
		IgnoreIdenticalSnapshots: clonePtr(p.Retention.IgnoreIdenticalSnapshots),
	}

	out.Files.Ignore = slices.Clone(p.Files.Ignore)
	out.Files.IgnoreDotFiles = slices.Clone(p.Files.IgnoreDotFiles)
	out.Files.IgnoreCacheDirs = clonePtr(p.Files.IgnoreCacheDirs)
	out.Files.OneFileSystem = clonePtr(p.Files.OneFileSystem)

	out.ErrorHandling = ErrorHandlingPatch{
		IgnoreFileErrors:      clonePtr(p.ErrorHandling.IgnoreFileErrors),
		IgnoreDirectoryErrors: clonePtr(p.ErrorHandling.IgnoreDirectoryErrors),
		IgnoreUnknownTypes:    clonePtr(p.ErrorHandling.IgnoreUnknownTypes),
	}

	out.Compression.OnlyCompress = slices.Clone(p.Compression.OnlyCompress)
	out.Compression.NeverCompress = slices.Clone(p.Compression.NeverCompress)

	out.Upload.MaxParallelFileReads = clonePtr(p.Upload.MaxParallelFileReads)

	out.Raw = slices.Clone(p.Raw)

	return out
}

// clonePtr returns a copy of *v in a new variable; nil for nil.
func clonePtr[T any](v *T) *T {
	if v == nil {
		return nil
	}

	c := *v

	return &c
}

// union returns a patch that owns the fields of a and b, with b's value
// where both own a field. Where a field of one is an object holding fields
// of the other, the outer field is kept. It is used to remember every field
// that may be in a policy after an unconfirmed apply.
func union(a, b PolicyPatch) (PolicyPatch, error) {
	af, err := a.Fields()
	if err != nil {
		return PolicyPatch{}, err
	}

	bf, err := b.Fields()
	if err != nil {
		return PolicyPatch{}, err
	}

	byName := make(map[string]Field, len(af)+len(bf))
	for _, f := range append(af, bf...) {
		byName[f.Name()] = f
	}

	all := make([]Field, 0, len(byName))
	for _, f := range byName {
		all = append(all, f)
	}

	slices.SortFunc(all, func(x, y Field) int { return slices.Compare(x.Path, y.Path) })

	obj := object{}

	var outer []string

	for _, f := range all {
		if outer != nil && isPrefix(outer, f.Path) {
			continue
		}

		if err := obj.set(f.Path, f.Value); err != nil {
			return PolicyPatch{}, err
		}

		outer = f.Path
	}

	if len(obj) == 0 {
		return PolicyPatch{}, nil
	}

	raw, err := encode(obj)
	if err != nil {
		return PolicyPatch{}, err
	}

	return PolicyPatch{Raw: raw}, nil
}
