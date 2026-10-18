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
// A field the labels leave to inheritance is not in the patch. When a field
// that was in the last applied patch is no longer in the new one, Merge
// takes it out of the policy, so the policy inherits it again.
//
// The pause is not part of the patch: it is Desired.Paused, and it is
// always the watcher's.
//
// The JSON form of a PolicyPatch is stored in STATE_FILE as the last
// applied patch. Fields are only ever added, with omitempty or omitzero,
// and never renamed.
type PolicyPatch struct {
	Scheduling SchedulingPatch `json:"scheduling,omitzero"`

	// Raw is a JSON object merged into the policy after the typed fields,
	// for labels that carry policy JSON as is. Objects in it are merged
	// field by field; any other value, arrays and null included, replaces
	// the field it names and is owned as a whole. Raw wins over a typed
	// field with the same name. It must not set scheduling.manual, which
	// belongs to the pause. kopia server drops fields it does not know and
	// zero values of plain fields, so such a field never reads back as
	// written; Applied.StoredHash keeps that from causing a write on every
	// reconcile.
	Raw json.RawMessage `json:"raw,omitempty"`
}

// SchedulingPatch is the scheduling section of a PolicyPatch.
type SchedulingPatch struct {
	// Cron is written to scheduling.cron. Empty means the watcher does not
	// own the field, as when the schedule label is set to an empty value
	// to inherit the schedule.
	Cron []string `json:"cron,omitempty"`
}

// IsZero reports whether s sets nothing.
func (s SchedulingPatch) IsZero() bool { return len(s.Cron) == 0 }

// IsZero reports whether p sets nothing.
func (p PolicyPatch) IsZero() bool {
	return p.Scheduling.IsZero() && isNull(p.Raw)
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

// Fields returns the fields p owns, sorted by path: the typed fields, then
// the leaves of Raw on top. An invalid patch is an ErrConfig error.
func (p PolicyPatch) Fields() ([]Field, error) {
	byName := map[string]Field{}

	if len(p.Scheduling.Cron) > 0 {
		v, err := encode(p.Scheduling.Cron)
		if err != nil {
			return nil, configError(err)
		}

		f := Field{Path: []string{"scheduling", "cron"}, Value: v}
		byName[f.Name()] = f
	}

	if !isNull(p.Raw) {
		if !isObject(p.Raw) {
			return nil, configError(fmt.Errorf("raw policy must be a JSON object, got %s", truncate(p.Raw)))
		}

		raw, err := leaves(p.Raw)
		if err != nil {
			return nil, configError(fmt.Errorf("raw policy: %w", err))
		}

		for _, f := range raw {
			if slices.Equal(f.Path, manualField) {
				return nil, configError(errors.New("raw policy must not set scheduling.manual: the watcher pauses a source when all its containers are stopped"))
			}

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

func (p PolicyPatch) clone() PolicyPatch {
	return PolicyPatch{
		Scheduling: SchedulingPatch{Cron: slices.Clone(p.Scheduling.Cron)},
		Raw:        slices.Clone(p.Raw),
	}
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
