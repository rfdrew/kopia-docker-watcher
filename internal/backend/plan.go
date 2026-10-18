package backend

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// Desired is what the owners of a path want for its source.
type Desired struct {
	Source Source
	// Patch holds the policy fields from the owners' labels. The owners
	// must agree on them; when they do not, the reconciler keeps the last
	// applied patch.
	Patch PolicyPatch
	// Paused is true when no owner is running.
	Paused bool
	// Owners are the names of the containers that use the path, sorted.
	// They are for logs and the plan; kopia server keeps nothing of them.
	Owners []string
}

// Clone returns a deep copy of d.
func (d Desired) Clone() Desired {
	out := d
	out.Patch = d.Patch.Clone()
	out.Owners = slices.Clone(d.Owners)

	return out
}

// Applied is what the watcher has to remember about a source between
// reconciles, because the policy alone does not tell. internal/state
// stores it per source, and its JSON form is part of STATE_FILE: fields
// are only ever added, with omitempty or omitzero, and never renamed.
type Applied struct {
	// Patch is the patch last applied to the source and confirmed. Fields
	// it owns that a new patch does not are removed from the policy. It is
	// what the reconciler falls back to when new labels are rejected or
	// owners disagree.
	Patch PolicyPatch `json:"patch,omitzero"`

	// SavedScheduling is the scheduling section the pause replaced, put
	// back when the source is unpaused. Nil when the source is not paused
	// by the watcher, or when the section is not known: a source paused by
	// the Python version or in the web UI, or a lost STATE_FILE.
	// Unpausing such a source leaves only the patch's scheduling fields.
	// An empty object means the section was empty.
	SavedScheduling json.RawMessage `json:"savedScheduling,omitempty"`

	// Pending marks an apply that was started but not confirmed: the
	// reconciler stores Plan.Intent, which sets it, before ApplyPlan, and
	// the Applied ApplyPlan returns clears it. It owns the fields of every
	// unconfirmed apply since the last confirmed one, so it may mix
	// several label sets. While it is set kopia server may or may not have
	// the policy, so the next plan writes the policy even if it looks
	// unchanged (a POST that registered a source needs the PUT after it),
	// and fields of Pending that the new patch does not own are removed
	// like those of Patch.
	Pending *PolicyPatch `json:"pending,omitempty"`

	// StoredHash is the Fingerprint of the policy as kopia server stored
	// it after the watcher's last write or check; empty if not known.
	// kopia server drops unknown fields and zero values of plain fields
	// and fills in defaults when it stores a policy, so what it returns
	// can differ from what the watcher wrote. When the policy still has
	// this fingerprint and the desired state is the one last applied, the
	// difference is kopia server's own and nothing is written. Fingerprints
	// are salted per process, so after a restart (perhaps with a new
	// watcher or kopia server) the policy is written once more. A kopia
	// server upgraded while the watcher runs is noticed only at the next
	// change of the policy, the labels or the pause.
	StoredHash string `json:"storedHash,omitempty"`
}

// Clone returns a deep copy of a.
func (a Applied) Clone() Applied {
	out := a
	out.Patch = a.Patch.Clone()
	out.SavedScheduling = slices.Clone(a.SavedScheduling)

	if a.Pending != nil {
		p := a.Pending.Clone()
		out.Pending = &p
	}

	return out
}

// IsZero reports whether a holds nothing.
func (a Applied) IsZero() bool {
	return a.Patch.IsZero() && a.SavedScheduling == nil && a.Pending == nil && a.StoredHash == ""
}

// Validate reports whether a is well formed.
func (a Applied) Validate() error {
	if err := a.Patch.Validate(); err != nil {
		return fmt.Errorf("patch: %w", err)
	}

	if a.Pending != nil {
		if err := a.Pending.Validate(); err != nil {
			return fmt.Errorf("pending patch: %w", err)
		}
	}

	if a.SavedScheduling != nil {
		if !isObject(a.SavedScheduling) {
			return fmt.Errorf("saved scheduling section is not a JSON object: %s", truncate(a.SavedScheduling))
		}

		if _, err := parseObject(a.SavedScheduling); err != nil {
			return fmt.Errorf("saved scheduling section: %w", err)
		}
	}

	return nil
}

// Equal reports whether a and b hold the same: patches with the same
// fields and values, the same saved section and pending patch, the same
// fingerprint.
func (a Applied) Equal(b Applied) bool {
	if !a.Patch.Equal(b.Patch) || a.StoredHash != b.StoredHash {
		return false
	}

	if (a.Pending == nil) != (b.Pending == nil) || (a.Pending != nil && !a.Pending.Equal(*b.Pending)) {
		return false
	}

	if (a.SavedScheduling == nil) != (b.SavedScheduling == nil) {
		return false
	}

	if a.SavedScheduling == nil {
		return true
	}

	same, err := sameJSON(a.SavedScheduling, b.SavedScheduling)

	return err == nil && same
}

// paused reports whether the watcher paused the source with a.
func (a Applied) paused() bool { return a.SavedScheduling != nil }

// Action is what ApplyPlan does.
type Action int

// Actions.
const (
	// ActionNone: kopia server already has the source with this policy.
	ActionNone Action = iota

	// ActionRegister: kopia server does not list the source. ApplyPlan
	// registers it with POST /api/v1/sources and the policy, which fails
	// with ErrConfig if the path does not exist on kopia server, then
	// writes the same policy with PUT /api/v1/policy: the POST does not
	// refresh kopia server, whose scheduler would not see the source's
	// schedule for hours.
	ActionRegister

	// ActionUpdate: the source is known and its policy has to be written,
	// because it changes or because the last apply was not confirmed.
	// ApplyPlan writes it with PUT /api/v1/policy.
	ActionUpdate
)

func (a Action) String() string {
	switch a {
	case ActionNone:
		return "none"
	case ActionRegister:
		return "register"
	case ActionUpdate:
		return "update"
	default:
		return fmt.Sprintf("Action(%d)", int(a))
	}
}

// Plan is what ApplyPlan will do to one source. It is plain data: the
// reconciler logs it, prints it for --dry-run, checks the schedule of
// Unpaused, stores Intent and applies it.
type Plan struct {
	Source Source
	Action Action
	// Known reports whether kopia server listed the source.
	Known bool
	// Status is the source's status when the plan was made; nil if kopia
	// server does not know the source.
	Status *SourceStatus
	// Current is the policy defined for the path itself when the plan was
	// made, nil if there was none.
	Current json.RawMessage
	Merged
}

// Clone returns a deep copy of p.
func (p Plan) Clone() Plan {
	out := p
	out.Current = slices.Clone(p.Current)
	out.Policy = slices.Clone(p.Policy)
	out.Unpaused = slices.Clone(p.Unpaused)
	out.Intent = p.Intent.Clone()
	out.Applied = p.Applied.Clone()

	if p.Status != nil {
		st := p.Status.Clone()
		out.Status = &st
	}

	return out
}

// NewPlan plans a source from what kopia server has: whether it knows the
// source (with its status, nil if not known) and the policy defined for
// the path (nil if none). Every Backend plans with it, so they all decide
// alike: an unknown source is registered, a changed policy is updated, and
// nothing else is sent.
func NewPlan(want Desired, last Applied, known bool, status *SourceStatus, current json.RawMessage) (Plan, error) {
	if err := want.Source.Validate(); err != nil {
		return Plan{}, &Error{Op: "PlanSource", Source: want.Source, Err: err}
	}

	m, err := Merge(current, want, last)
	if err != nil {
		// Only an invalid desired patch is ErrConfig; Merge returns it as
		// PolicyPatch's own error.
		var be *Error
		if errors.As(err, &be) && be.Kind != nil {
			return Plan{}, &Error{Op: "PlanSource", Source: want.Source, Kind: be.Kind, Err: be.Err}
		}

		return Plan{}, &Error{Op: "PlanSource", Source: want.Source, Err: err}
	}

	if status != nil {
		st := status.Clone()
		status = &st
	}

	p := Plan{
		Source:  want.Source,
		Known:   known,
		Status:  status,
		Current: slices.Clone(current),
		Merged:  m,
	}

	switch {
	case !known:
		p.Action = ActionRegister
	case m.Changed:
		p.Action = ActionUpdate
	default:
		p.Action = ActionNone
	}

	return p, nil
}

// Replan plans p's source again for another desired state, from what
// kopia server had when p was made, without asking it again. The
// reconciler uses it to fall back to the last applied patch when the new
// one fails PreviewSchedule.
func Replan(p Plan, want Desired, last Applied) (Plan, error) {
	if want.Source != p.Source {
		return Plan{}, &Error{Op: "Replan", Source: want.Source, Err: fmt.Errorf("plan is for %s", p.Source)}
	}

	return NewPlan(want, last, p.Known, p.Status, p.Current)
}
