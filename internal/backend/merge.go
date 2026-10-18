package backend

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// Merged is the result of Merge.
type Merged struct {
	// Policy is the policy to store for the path.
	Policy json.RawMessage

	// Unpaused is Policy as it would be without the pause, with the
	// scheduling section the pause keeps aside. It equals Policy when the
	// source is not paused. PreviewSchedule takes it, so that a schedule
	// is checked while the source is paused too.
	Unpaused json.RawMessage

	// Changed reports whether the policy has to be written: it differs
	// from the current one, or the last apply was not confirmed. A missing
	// policy counts as an empty one, and null values and empty objects as
	// missing fields. A difference that kopia server made itself when it
	// stored the watcher's last write does not count (Applied.StoredHash).
	Changed bool

	// Intent is what to store before ApplyPlan. It keeps what a failed or
	// half-done apply must not lose: the scheduling section a pause is
	// about to replace, the saved section of a source not yet unpaused,
	// the last confirmed patch, and Pending with the fields that may now
	// be in the policy, which also makes the next plan write again.
	Intent Applied

	// Applied is what to store once Policy is stored. ApplyPlan returns it
	// with StoredHash set. Without a change it is the state to keep as it
	// is, with the fingerprint of the current policy.
	Applied Applied
}

// pausedScheduling is the only scheduling section Kopia accepts with
// manual set: manual cannot be combined with any other scheduling field,
// not even an empty cron list.
var pausedScheduling = json.RawMessage(`{"manual":true}`)

// Merge computes the policy to store for a path from the policy stored now
// (nil if there is none), the desired state and what the watcher applied
// last. It changes nothing; NewPlan uses it.
//
// Fields the patches do not own are kept exactly as they are, including
// fields this package does not know. The steps:
//
//  1. Scheduling: if the current policy is paused (scheduling.manual is
//     true, set by the watcher, the Python version or the web UI), the
//     section is replaced by the one the watcher saved when it paused the
//     source, or, with none saved, by an empty one, so that only the
//     fields of the patch remain: merging a cron into {"manual": true}
//     would be rejected by kopia server and leave the source paused. A
//     saved section never brings manual back.
//  2. Fields of last.Patch and last.Pending that want.Patch does not own
//     are removed, so the policy inherits them again; an object the
//     removal leaves empty goes too.
//  3. Fields of want.Patch are set. This gives Unpaused.
//  4. If want.Paused, the scheduling section of Unpaused is kept in
//     Applied.SavedScheduling and replaced by {"manual": true}. Otherwise
//     Policy is Unpaused and nothing is saved.
//
// The pause follows want.Paused alone: a pause set in the web UI on the
// path of a running container is lifted, and a source the watcher did not
// pause is paused when its containers stop.
//
// An invalid want.Patch is an ErrConfig error. A current policy that is not
// a JSON object, or an invalid last state, is an error of neither kind.
func Merge(current json.RawMessage, want Desired, last Applied) (Merged, error) {
	newFields, err := want.Patch.Fields()
	if err != nil {
		return Merged{}, err
	}

	// An invalid stored state is not the labels' fault: the error is of
	// neither kind.
	oldFields, err := last.Patch.Fields()
	if err != nil {
		return Merged{}, fmt.Errorf("last applied patch: %v", err) //nolint:errorlint // not ErrConfig
	}

	if last.Pending != nil {
		pending, err := last.Pending.Fields()
		if err != nil {
			return Merged{}, fmt.Errorf("pending patch: %v", err) //nolint:errorlint // not ErrConfig
		}

		oldFields = append(oldFields, pending...)
	}

	policy, err := parseObject(current)
	if err != nil {
		return Merged{}, fmt.Errorf("current policy: %w", err)
	}

	if err := unpause(policy, last.SavedScheduling); err != nil {
		return Merged{}, err
	}

	owned := make(map[string]bool, len(newFields))
	for _, f := range newFields {
		owned[f.Name()] = true
	}

	for _, f := range oldFields {
		if owned[f.Name()] {
			continue
		}

		if err := policy.remove(f.Path); err != nil {
			return Merged{}, fmt.Errorf("current policy: %w", err)
		}
	}

	for _, f := range newFields {
		if err := policy.set(f.Path, f.Value); err != nil {
			// The labels ask for a field inside one the policy holds as
			// something other than an object.
			return Merged{}, configError(err)
		}
	}

	unpaused, err := encode(policy)
	if err != nil {
		return Merged{}, err
	}

	m := Merged{
		Policy:   unpaused,
		Unpaused: unpaused,
		Applied:  Applied{Patch: want.Patch.Clone()},
	}

	if want.Paused {
		saved, ok := policy["scheduling"]
		if !ok || isNull(saved) {
			saved = json.RawMessage(`{}`)
		}

		m.Applied.SavedScheduling = compact(saved)
		policy["scheduling"] = pausedScheduling

		if m.Policy, err = encode(policy); err != nil {
			return Merged{}, err
		}
	}

	same, err := sameJSON(current, m.Policy)
	if err != nil {
		return Merged{}, fmt.Errorf("current policy: %w", err)
	}

	currentHash, err := Fingerprint(current)
	if err != nil {
		return Merged{}, fmt.Errorf("current policy: %w", err)
	}

	if !same && last.Pending == nil && last.StoredHash != "" && currentHash == last.StoredHash &&
		want.Paused == last.paused() && want.Patch.Equal(last.Patch) {
		// The policy is as kopia server stored the watcher's last write,
		// and the watcher wants what it wrote then. Nothing is written, so
		// Policy is the current one.
		same = true

		if m.Policy, err = encode(policy0(current)); err != nil {
			return Merged{}, err
		}
	}

	m.Changed = !same || last.Pending != nil

	if !m.Changed {
		m.Applied.StoredHash = currentHash
	}

	if m.Intent, err = intent(want, last, m.Applied); err != nil {
		return Merged{}, err
	}

	return m, nil
}

// intent is the state to store before applying: the last confirmed patch,
// the new patch as pending with an earlier unconfirmed one folded in, and
// the saved section of whichever pause may be in force.
func intent(want Desired, last, applied Applied) (Applied, error) {
	in := Applied{Patch: last.Patch.Clone()}

	pending := want.Patch.Clone()

	if last.Pending != nil {
		u, err := union(*last.Pending, want.Patch)
		if err != nil {
			return Applied{}, fmt.Errorf("pending patch: %v", err) //nolint:errorlint // not ErrConfig
		}

		pending = u
	}

	in.Pending = &pending

	in.SavedScheduling = slices.Clone(applied.SavedScheduling)
	if in.SavedScheduling == nil {
		in.SavedScheduling = slices.Clone(last.SavedScheduling)
	}

	return in, nil
}

// policy0 decodes the current policy for output; a missing one is empty.
func policy0(current json.RawMessage) any {
	if isNull(current) {
		return map[string]any{}
	}

	return compact(current)
}

// unpause replaces the scheduling section of a paused policy by saved, or
// by an empty section if nothing was saved. The manual field never stays
// in the result.
func unpause(policy object, saved json.RawMessage) error {
	cur, ok := policy["scheduling"]
	if !ok || !isPaused(cur) {
		return nil
	}

	section, err := parseObject(saved)
	if err != nil {
		return fmt.Errorf("saved scheduling section: %w", err)
	}

	delete(section, "manual")

	enc, err := encode(section)
	if err != nil {
		return err
	}

	policy["scheduling"] = enc

	return nil
}

// isPaused reports whether a scheduling section has manual set to true.
func isPaused(section json.RawMessage) bool {
	if !isObject(section) {
		return false
	}

	obj, err := parseObject(section)
	if err != nil {
		return false
	}

	return bytes.Equal(compact(obj["manual"]), []byte("true"))
}

// Fingerprint returns a hash of a policy that two policies share exactly
// when they mean the same to Kopia, in the sense of Merged.Changed: key
// order, whitespace, null values and empty objects do not matter. A
// missing policy has the fingerprint of an empty one.
//
// The hash is salted with a random value per process, so a fingerprint
// from an earlier run of the watcher never matches: Applied.StoredHash is
// trusted only by the process that computed it.
func Fingerprint(policy json.RawMessage) (string, error) {
	c, err := canonical(policy)
	if err != nil {
		return "", err
	}

	enc, err := encode(c)
	if err != nil {
		return "", err
	}

	h := sha256.New()
	h.Write(fingerprintSalt[:])
	h.Write(enc)

	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// fingerprintSalt makes fingerprints differ between runs.
var fingerprintSalt = func() (salt [16]byte) {
	_, _ = rand.Read(salt[:])

	return salt
}()

// Change is one field that differs between two policies.
type Change struct {
	// Field is the dotted field name, e.g. "scheduling.cron".
	Field string
	// Old and New are the compact JSON values; nil if the field is not set
	// on that side.
	Old, New json.RawMessage
}

// Diff lists the leaf fields that differ between two policies, sorted by
// field name. Objects are compared field by field, any other value as a
// whole. Null values count as missing, as for Merged.Changed. It is meant
// for logs and for the plan --dry-run prints.
func Diff(from, to json.RawMessage) ([]Change, error) {
	a, err := leafMap(from)
	if err != nil {
		return nil, fmt.Errorf("diff: %w", err)
	}

	b, err := leafMap(to)
	if err != nil {
		return nil, fmt.Errorf("diff: %w", err)
	}

	var out []Change

	for name, old := range a {
		nv, ok := b[name]
		if !ok {
			out = append(out, Change{Field: name, Old: old})

			continue
		}

		if same, err := sameJSON(old, nv); err != nil || !same {
			out = append(out, Change{Field: name, Old: old, New: nv})
		}
	}

	for name, nv := range b {
		if _, ok := a[name]; !ok {
			out = append(out, Change{Field: name, New: nv})
		}
	}

	slices.SortFunc(out, func(x, y Change) int { return strings.Compare(x.Field, y.Field) })

	return out, nil
}

// leafMap returns the non-null leaves of a policy by dotted name.
func leafMap(raw json.RawMessage) (map[string]json.RawMessage, error) {
	fields, err := leaves(raw)
	if err != nil {
		return nil, err
	}

	m := make(map[string]json.RawMessage, len(fields))

	for _, f := range fields {
		if !isNull(f.Value) {
			m[f.Name()] = f.Value
		}
	}

	return m, nil
}
