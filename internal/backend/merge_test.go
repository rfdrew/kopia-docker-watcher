package backend

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

var testSource = Source{Host: "kopia", UserName: "root", Path: "/data/app"}

// assertJSON compares JSON exactly by meaning: key order and whitespace do
// not matter, everything else does (nulls, empty objects, number text).
func assertJSON(t *testing.T, name string, got json.RawMessage, want string) {
	t.Helper()

	decode := func(b []byte) any {
		t.Helper()

		if len(b) == 0 {
			return nil
		}

		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()

		var v any
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("%s: decode %s: %v", name, b, err)
		}

		return v
	}

	if !reflect.DeepEqual(decode(got), decode([]byte(want))) {
		t.Errorf("%s = %s\nwant %s", name, got, want)
	}
}

func cronPatch(crons ...string) PolicyPatch {
	return PolicyPatch{Scheduling: SchedulingPatch{Cron: crons}}
}

func ptr[T any](v T) *T { return &v }

func mustFingerprint(t *testing.T, policy string) string {
	t.Helper()

	h, err := Fingerprint(json.RawMessage(policy))
	if err != nil {
		t.Fatal(err)
	}

	return h
}

func TestMerge(t *testing.T) {
	tests := []struct {
		name    string
		current string
		want    Desired
		last    Applied

		policy   string
		unpaused string // "" means the same as policy
		saved    string // "" means nil
		changed  bool
	}{
		{
			name:    "new source",
			want:    Desired{Patch: cronPatch("0 * * * *")},
			policy:  `{"scheduling":{"cron":["0 * * * *"]}}`,
			changed: true,
		},
		{
			name:    "fields the patch does not own survive, unknown ones included",
			current: `{"retention":{"keepLatest":5},"files":{"maxFileSize":9223372036854775807},"fieldFromTheFuture":{"x":[1,2.50]},"scheduling":{"cron":["0 3 * * *"],"intervalSeconds":3600}}`,
			want:    Desired{Patch: cronPatch("0 * * * *")},
			policy:  `{"retention":{"keepLatest":5},"files":{"maxFileSize":9223372036854775807},"fieldFromTheFuture":{"x":[1,2.50]},"scheduling":{"cron":["0 * * * *"],"intervalSeconds":3600}}`,
			changed: true,
		},
		{
			name:    "unchanged",
			current: `{"retention":{},"scheduling":{"cron":["0 * * * *"]},"files":{}}`,
			want:    Desired{Patch: cronPatch("0 * * * *")},
			last:    Applied{Patch: cronPatch("0 * * * *")},
			policy:  `{"retention":{},"scheduling":{"cron":["0 * * * *"]},"files":{}}`,
		},
		{
			name:    "a field the labels no longer set is removed",
			current: `{"scheduling":{"cron":["0 * * * *"],"intervalSeconds":3600}}`,
			want:    Desired{},
			last:    Applied{Patch: cronPatch("0 * * * *")},
			policy:  `{"scheduling":{"intervalSeconds":3600}}`,
			changed: true,
		},
		{
			name:    "a field the watcher never set is kept",
			current: `{"scheduling":{"cron":["0 3 * * *"]}}`,
			want:    Desired{},
			policy:  `{"scheduling":{"cron":["0 3 * * *"]}}`,
		},
		{
			name:    "no policy and nothing to set",
			want:    Desired{},
			policy:  `{}`,
			changed: false,
		},
		{
			name:     "pause saves the section",
			current:  `{"retention":{"keepDaily":7},"scheduling":{"cron":["0 3 * * *"],"runMissed":true}}`,
			want:     Desired{Patch: cronPatch("0 * * * *"), Paused: true},
			last:     Applied{Patch: cronPatch("0 3 * * *")},
			policy:   `{"retention":{"keepDaily":7},"scheduling":{"manual":true}}`,
			unpaused: `{"retention":{"keepDaily":7},"scheduling":{"cron":["0 * * * *"],"runMissed":true}}`,
			saved:    `{"cron":["0 * * * *"],"runMissed":true}`,
			changed:  true,
		},
		{
			name:     "pause of a policy without a scheduling section",
			current:  `{"retention":{"keepDaily":7}}`,
			want:     Desired{Paused: true},
			policy:   `{"retention":{"keepDaily":7},"scheduling":{"manual":true}}`,
			unpaused: `{"retention":{"keepDaily":7}}`,
			saved:    `{}`,
			changed:  true,
		},
		{
			name:     "still paused: new labels go to the saved section only",
			current:  `{"scheduling":{"manual":true}}`,
			want:     Desired{Patch: cronPatch("30 2 * * *"), Paused: true},
			last:     Applied{Patch: cronPatch("0 * * * *"), SavedScheduling: json.RawMessage(`{"cron":["0 * * * *"],"timeOfDay":[{"hour":1,"min":0}]}`)},
			policy:   `{"scheduling":{"manual":true}}`,
			unpaused: `{"scheduling":{"cron":["30 2 * * *"],"timeOfDay":[{"hour":1,"min":0}]}}`,
			saved:    `{"cron":["30 2 * * *"],"timeOfDay":[{"hour":1,"min":0}]}`,
		},
		{
			name:    "unpause restores the saved section with the labels on top",
			current: `{"retention":{"keepDaily":7},"scheduling":{"manual":true}}`,
			want:    Desired{Patch: cronPatch("30 2 * * *")},
			last:    Applied{Patch: cronPatch("0 * * * *"), SavedScheduling: json.RawMessage(`{"cron":["0 * * * *"],"timeOfDay":[{"hour":1,"min":0}]}`)},
			policy:  `{"retention":{"keepDaily":7},"scheduling":{"cron":["30 2 * * *"],"timeOfDay":[{"hour":1,"min":0}]}}`,
			changed: true,
		},
		{
			name:    "unpause of an empty saved section",
			current: `{"scheduling":{"manual":true}}`,
			want:    Desired{},
			last:    Applied{SavedScheduling: json.RawMessage(`{}`)},
			policy:  `{"scheduling":{}}`,
			changed: true,
		},
		{
			name:    "unpause without a saved section keeps only the labels' fields",
			current: `{"scheduling":{"manual":true},"retention":{"keepLatest":3}}`,
			want:    Desired{Patch: cronPatch("0 * * * *")},
			policy:  `{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepLatest":3}}`,
			changed: true,
		},
		{
			name:    "unpause without a saved section and an inherited schedule",
			current: `{"scheduling":{"manual":true}}`,
			want:    Desired{},
			policy:  `{"scheduling":{}}`,
			changed: true,
		},
		{
			name:    "a saved section that was unpaused elsewhere is dropped",
			current: `{"scheduling":{"cron":["0 5 * * *"]}}`,
			want:    Desired{Patch: cronPatch("0 * * * *")},
			last:    Applied{Patch: cronPatch("0 * * * *"), SavedScheduling: json.RawMessage(`{"cron":["0 * * * *"],"intervalSeconds":60}`)},
			policy:  `{"scheduling":{"cron":["0 * * * *"]}}`,
			changed: true,
		},
		{
			name:     "a saved section never brings manual back",
			current:  `{"scheduling":{"manual":true}}`,
			want:     Desired{Paused: true},
			last:     Applied{SavedScheduling: json.RawMessage(`{"manual":true,"cron":["0 * * * *"]}`)},
			policy:   `{"scheduling":{"manual":true}}`,
			unpaused: `{"scheduling":{"cron":["0 * * * *"]}}`,
			saved:    `{"cron":["0 * * * *"]}`,
		},
		{
			name:    "raw fields are merged and win over typed ones",
			current: `{"retention":{"keepLatest":10,"keepDaily":3},"files":{"ignore":["*.tmp"]}}`,
			want: Desired{Patch: PolicyPatch{
				Scheduling: SchedulingPatch{Cron: []string{"0 * * * *"}},
				Raw:        json.RawMessage(`{"retention":{"keepDaily":7},"files":{"ignore":[".git"]},"scheduling":{"cron":["15 * * * *"]}}`),
			}},
			policy:  `{"retention":{"keepLatest":10,"keepDaily":7},"files":{"ignore":[".git"]},"scheduling":{"cron":["15 * * * *"]}}`,
			changed: true,
		},
		{
			name:    "a raw field that is no longer set is removed",
			current: `{"retention":{"keepLatest":10,"keepDaily":7}}`,
			want:    Desired{Patch: PolicyPatch{Raw: json.RawMessage(`{"retention":{"keepLatest":10}}`)}},
			last:    Applied{Patch: PolicyPatch{Raw: json.RawMessage(`{"retention":{"keepLatest":10,"keepDaily":7}}`)}},
			policy:  `{"retention":{"keepLatest":10}}`,
			changed: true,
		},
		{
			name:    "a null raw field is set as null",
			current: `{"retention":{"keepDaily":7}}`,
			want:    Desired{Patch: PolicyPatch{Raw: json.RawMessage(`{"retention":{"keepDaily":null}}`)}},
			policy:  `{"retention":{"keepDaily":null}}`,
			changed: true,
		},
		{
			name:    "removing a field from a missing section changes nothing",
			current: `{"retention":{}}`,
			want:    Desired{},
			last:    Applied{Patch: cronPatch("0 * * * *")},
			policy:  `{"retention":{}}`,
		},
		{
			name:    "an object the removal empties goes too",
			current: `{"actions":{"beforeSnapshotRoot":{"script":"x.sh"},"afterSnapshotRoot":{"script":"y.sh"}}}`,
			want:    Desired{},
			last:    Applied{Patch: PolicyPatch{Raw: json.RawMessage(`{"actions":{"beforeSnapshotRoot":{"script":"x.sh"}}}`)}},
			policy:  `{"actions":{"afterSnapshotRoot":{"script":"y.sh"}}}`,
			changed: true,
		},
		{
			name:    "a removal that empties a section removes the section",
			current: `{"scheduling":{"cron":["0 * * * *"]}}`,
			want:    Desired{},
			last:    Applied{Patch: cronPatch("0 * * * *")},
			policy:  `{}`,
			changed: true,
		},
		{
			name:    "fields of an unconfirmed apply are removed too",
			current: `{"scheduling":{"cron":["5 * * * *"]},"retention":{"keepDaily":7}}`,
			want:    Desired{},
			last: Applied{
				Patch:   cronPatch("0 * * * *"),
				Pending: &PolicyPatch{Raw: json.RawMessage(`{"scheduling":{"cron":["5 * * * *"]},"retention":{"keepDaily":7}}`)},
			},
			policy:  `{}`,
			changed: true,
		},
		{
			name:    "an unconfirmed apply is written again even if the policy looks right",
			current: `{"scheduling":{"cron":["0 * * * *"]}}`,
			want:    Desired{Patch: cronPatch("0 * * * *")},
			last:    Applied{Pending: ptr(cronPatch("0 * * * *"))},
			policy:  `{"scheduling":{"cron":["0 * * * *"]}}`,
			changed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var current json.RawMessage
			if tt.current != "" {
				current = json.RawMessage(tt.current)
			}

			tt.want.Source = testSource

			got, err := Merge(current, tt.want, tt.last)
			if err != nil {
				t.Fatal(err)
			}

			assertJSON(t, "Policy", got.Policy, tt.policy)

			unpaused := tt.unpaused
			if unpaused == "" {
				unpaused = tt.policy
			}

			assertJSON(t, "Unpaused", got.Unpaused, unpaused)

			if tt.saved == "" {
				if got.Applied.SavedScheduling != nil {
					t.Errorf("SavedScheduling = %s, want nil", got.Applied.SavedScheduling)
				}
			} else {
				assertJSON(t, "SavedScheduling", got.Applied.SavedScheduling, tt.saved)
			}

			if !got.Applied.Patch.Equal(tt.want.Patch) {
				t.Errorf("Applied.Patch = %+v, want %+v", got.Applied.Patch, tt.want.Patch)
			}

			if got.Changed != tt.changed {
				t.Errorf("Changed = %v, want %v", got.Changed, tt.changed)
			}
		})
	}
}

// Merging the result again with the same input changes nothing: a full
// reconcile does not write a policy it has just written.
func TestMergeIsIdempotent(t *testing.T) {
	current := json.RawMessage(`{"retention":{"keepDaily":7},"scheduling":{"cron":["0 3 * * *"],"runMissed":true},"x":1}`)

	for _, paused := range []bool{false, true} {
		want := Desired{Source: testSource, Patch: cronPatch("0 * * * *"), Paused: paused}

		first, err := Merge(current, want, Applied{})
		if err != nil {
			t.Fatal(err)
		}

		second, err := Merge(first.Policy, want, first.Applied)
		if err != nil {
			t.Fatal(err)
		}

		if second.Changed {
			t.Errorf("paused=%v: second merge changed %s to %s", paused, first.Policy, second.Policy)
		}

		assertJSON(t, "Unpaused", second.Unpaused, string(first.Unpaused))
		assertJSON(t, "SavedScheduling", second.Applied.SavedScheduling, string(first.Applied.SavedScheduling))
	}
}

// Pausing and unpausing gives back the policy as it was.
func TestMergePauseRoundTrip(t *testing.T) {
	start := json.RawMessage(`{"retention":{"keepDaily":7},"scheduling":{"cron":["0 * * * *"],"timeOfDay":[{"hour":2,"min":30}],"runMissed":true}}`)
	patch := cronPatch("0 * * * *")

	paused, err := Merge(start, Desired{Source: testSource, Patch: patch, Paused: true}, Applied{Patch: patch})
	if err != nil {
		t.Fatal(err)
	}

	resumed, err := Merge(paused.Policy, Desired{Source: testSource, Patch: patch}, paused.Applied)
	if err != nil {
		t.Fatal(err)
	}

	assertJSON(t, "resumed policy", resumed.Policy, string(start))

	if resumed.Applied.SavedScheduling != nil {
		t.Errorf("SavedScheduling after unpause = %s", resumed.Applied.SavedScheduling)
	}
}

// Text with <, > and & is kept as it is, not escaped for HTML.
func TestMergeKeepsText(t *testing.T) {
	got, err := Merge(json.RawMessage(`{"files":{"ignore":["<a&b>"]}}`),
		Desired{Source: testSource, Patch: cronPatch("0 * * * * # <nightly & more>")}, Applied{})
	if err != nil {
		t.Fatal(err)
	}

	for _, s := range []string{`"<a&b>"`, `"0 * * * * # <nightly & more>"`} {
		if !strings.Contains(string(got.Policy), s) {
			t.Errorf("policy %s does not contain %s", got.Policy, s)
		}
	}
}

func TestMergeErrors(t *testing.T) {
	tests := []struct {
		name    string
		current string
		want    Desired
		last    Applied
		config  bool // the error is ErrConfig
	}{
		{
			name:   "raw is not an object",
			want:   Desired{Patch: PolicyPatch{Raw: json.RawMessage(`[1]`)}},
			config: true,
		},
		{
			name:   "raw sets manual",
			want:   Desired{Patch: PolicyPatch{Raw: json.RawMessage(`{"scheduling":{"manual":true}}`)}},
			config: true,
		},
		{
			name:   "raw is invalid JSON",
			want:   Desired{Patch: PolicyPatch{Raw: json.RawMessage(`{"a":`)}},
			config: true,
		},
		{
			name:   "raw sets a field and a field under it",
			want:   Desired{Patch: PolicyPatch{Raw: json.RawMessage(`{"scheduling":5}`), Scheduling: SchedulingPatch{Cron: []string{"0 * * * *"}}}},
			config: true,
		},
		{
			name:    "current policy is not an object",
			current: `[]`,
			want:    Desired{Patch: cronPatch("0 * * * *")},
		},
		{
			name:    "current section is not an object",
			current: `{"scheduling":"daily"}`,
			want:    Desired{Patch: cronPatch("0 * * * *")},
		},
		{
			name:    "saved section is not an object",
			current: `{"scheduling":{"manual":true}}`,
			want:    Desired{},
			last:    Applied{SavedScheduling: json.RawMessage(`"x"`)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var current json.RawMessage
			if tt.current != "" {
				current = json.RawMessage(tt.current)
			}

			_, err := Merge(current, tt.want, tt.last)
			if err == nil {
				t.Fatal("no error")
			}

			if IsConfig(err) != tt.config {
				t.Errorf("IsConfig(%v) = %v, want %v", err, IsConfig(err), tt.config)
			}

			if IsTemporary(err) {
				t.Errorf("IsTemporary(%v) = true", err)
			}
		})
	}
}

func TestDiff(t *testing.T) {
	got, err := Diff(
		json.RawMessage(`{"scheduling":{"cron":["0 * * * *"],"runMissed":true},"retention":{"keepDaily":7,"keepLatest":null},"files":{}}`),
		json.RawMessage(`{"scheduling":{"manual":true},"retention":{"keepDaily":7},"upload":{"maxParallelFileReads":4}}`),
	)
	if err != nil {
		t.Fatal(err)
	}

	want := []Change{
		{Field: "scheduling.cron", Old: json.RawMessage(`["0 * * * *"]`)},
		{Field: "scheduling.manual", New: json.RawMessage(`true`)},
		{Field: "scheduling.runMissed", Old: json.RawMessage(`true`)},
		{Field: "upload.maxParallelFileReads", New: json.RawMessage(`4`)},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Diff = %s\nwant %s", fmtChanges(got), fmtChanges(want))
	}

	if _, err := Diff(json.RawMessage(`[]`), nil); err == nil {
		t.Error("Diff of a non-object: no error")
	}
}

func fmtChanges(cs []Change) string {
	var b strings.Builder
	for _, c := range cs {
		b.WriteString(c.Field + ": " + string(c.Old) + " -> " + string(c.New) + "; ")
	}

	return b.String()
}

func TestErrorKinds(t *testing.T) {
	cause := errors.New("boom")
	err := error(&Error{Op: "ApplyPlan", Source: testSource, Kind: ErrConfig, Err: cause})

	if !IsConfig(err) || IsTemporary(err) || !errors.Is(err, cause) {
		t.Errorf("kinds of %v", err)
	}

	if got, want := err.Error(), "ApplyPlan root@kopia:/data/app: configuration error: boom"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}

	plain := &Error{Op: "ServerIdentity", Err: cause}
	if IsConfig(plain) || IsTemporary(plain) {
		t.Errorf("kinds of %v", plain)
	}

	if got, want := plain.Error(), "ServerIdentity: boom"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

// Applied.StoredHash is set to the fingerprint of the current policy when
// nothing has to be written, and left empty otherwise: ApplyPlan sets it.
func TestMergeStoredHash(t *testing.T) {
	current := `{"scheduling":{"cron":["0 * * * *"]},"files":{}}`

	m, err := Merge(json.RawMessage(current), Desired{Source: testSource, Patch: cronPatch("0 * * * *")}, Applied{})
	if err != nil {
		t.Fatal(err)
	}

	if m.Changed || m.Applied.StoredHash != mustFingerprint(t, current) {
		t.Errorf("Changed = %v, StoredHash = %q", m.Changed, m.Applied.StoredHash)
	}

	m, err = Merge(json.RawMessage(current), Desired{Source: testSource, Patch: cronPatch("5 * * * *")}, Applied{})
	if err != nil {
		t.Fatal(err)
	}

	if !m.Changed || m.Applied.StoredHash != "" {
		t.Errorf("Changed = %v, StoredHash = %q", m.Changed, m.Applied.StoredHash)
	}
}

// When kopia server stored the watcher's last write in its own form, the
// difference to what the watcher would write is not a change, as long as
// the desired state is the one last applied and nobody touched the policy.
func TestMergeKopiaNormalization(t *testing.T) {
	patch := PolicyPatch{Raw: json.RawMessage(`{"scheduling":{"timeOfDay":[{"hour":3}]},"files":{"maxFileSize":0}}`)}
	// What kopia server returns after storing it: min filled in, the zero
	// maxFileSize dropped.
	stored := `{"scheduling":{"timeOfDay":[{"hour":3,"min":0}]},"files":{}}`
	last := Applied{Patch: patch, StoredHash: mustFingerprint(t, stored)}

	m, err := Merge(json.RawMessage(stored), Desired{Source: testSource, Patch: patch}, last)
	if err != nil {
		t.Fatal(err)
	}

	if m.Changed {
		t.Errorf("Changed after kopia's own normalization: %s -> %s", stored, m.Policy)
	}

	if m.Applied.StoredHash != last.StoredHash {
		t.Errorf("StoredHash = %q, want %q", m.Applied.StoredHash, last.StoredHash)
	}

	otherPatch := PolicyPatch{Raw: json.RawMessage(`{"scheduling":{"timeOfDay":[{"hour":4}]}}`)}

	for name, tc := range map[string]struct {
		current string
		want    Desired
		last    Applied
	}{
		"policy edited since": {
			current: `{"scheduling":{"timeOfDay":[{"hour":3,"min":0}]},"files":{},"retention":{"keepDaily":1}}`,
			want:    Desired{Patch: patch},
			last:    last,
		},
		"labels changed": {
			current: stored,
			want:    Desired{Patch: otherPatch},
			last:    last,
		},
		"pause changed": {
			current: stored,
			want:    Desired{Patch: patch, Paused: true},
			last:    last,
		},
		"no fingerprint": {
			current: stored,
			want:    Desired{Patch: patch},
			last:    Applied{Patch: patch},
		},
	} {
		tc.want.Source = testSource

		m, err := Merge(json.RawMessage(tc.current), tc.want, tc.last)
		if err != nil {
			t.Fatal(err)
		}

		if !m.Changed {
			t.Errorf("%s: Changed = false", name)
		}
	}
}

func TestMergeIntent(t *testing.T) {
	webUI := `{"cron":["0 * * * *"],"intervalSeconds":21600,"runMissed":true}`

	// Pausing: the section about to be replaced is in the intent.
	m, err := Merge(json.RawMessage(`{"scheduling":`+webUI+`}`),
		Desired{Source: testSource, Patch: cronPatch("0 * * * *"), Paused: true},
		Applied{Patch: cronPatch("0 * * * *"), StoredHash: "sha256:old"})
	if err != nil {
		t.Fatal(err)
	}

	assertJSON(t, "Intent.SavedScheduling", m.Intent.SavedScheduling, webUI)

	if m.Intent.Pending == nil || !m.Intent.Pending.Equal(cronPatch("0 * * * *")) {
		t.Errorf("Intent.Pending = %+v", m.Intent.Pending)
	}

	if !m.Intent.Patch.Equal(cronPatch("0 * * * *")) || m.Intent.StoredHash != "" {
		t.Errorf("Intent = %+v", m.Intent)
	}

	// Unpausing: the saved section stays until the apply is confirmed.
	m, err = Merge(json.RawMessage(`{"scheduling":{"manual":true}}`),
		Desired{Source: testSource, Patch: cronPatch("5 * * * *")},
		Applied{Patch: cronPatch("0 * * * *"), SavedScheduling: json.RawMessage(webUI)})
	if err != nil {
		t.Fatal(err)
	}

	assertJSON(t, "Intent.SavedScheduling", m.Intent.SavedScheduling, webUI)

	if m.Applied.SavedScheduling != nil {
		t.Errorf("Applied.SavedScheduling = %s", m.Applied.SavedScheduling)
	}

	// An earlier unconfirmed patch is folded into the intent's patch.
	m, err = Merge(json.RawMessage(`{}`),
		Desired{Source: testSource, Patch: cronPatch("10 * * * *")},
		Applied{Patch: cronPatch("0 * * * *"), Pending: &PolicyPatch{Raw: json.RawMessage(`{"retention":{"keepDaily":7}}`)}})
	if err != nil {
		t.Fatal(err)
	}

	got, err := m.Intent.Patch.Fields()
	if err != nil {
		t.Fatal(err)
	}

	if names := fieldNames(got); !reflect.DeepEqual(names, []string{"retention.keepDaily", "scheduling.cron"}) {
		t.Errorf("Intent.Patch fields = %v", names)
	}
}

func fieldNames(fs []Field) []string {
	out := make([]string, 0, len(fs))
	for _, f := range fs {
		out = append(out, f.Name())
	}

	return out
}

// The write-ahead state keeps a pause safe whether or not kopia server
// stored the paused policy before the apply failed.
func TestMergeAfterFailedApply(t *testing.T) {
	webUI := `{"cron":["0 * * * *"],"timeOfDay":[{"hour":1,"min":0}],"runMissed":true}`
	before := json.RawMessage(`{"scheduling":` + webUI + `}`)
	want := Desired{Source: testSource, Patch: cronPatch("0 * * * *"), Paused: true}
	last := Applied{Patch: cronPatch("0 * * * *")}

	pause, err := Merge(before, want, last)
	if err != nil {
		t.Fatal(err)
	}

	// Stored by the reconciler before ApplyPlan, which then failed.
	stored := pause.Intent

	for name, current := range map[string]json.RawMessage{
		"kopia server stored the pause": pause.Policy,
		"kopia server did not":          before,
	} {
		again, err := Merge(current, want, stored)
		if err != nil {
			t.Fatal(err)
		}

		if !again.Changed {
			t.Errorf("%s: the pause is not written again", name)
		}

		assertJSON(t, name+": Policy", again.Policy, `{"scheduling":{"manual":true}}`)
		assertJSON(t, name+": SavedScheduling", again.Applied.SavedScheduling, webUI)

		// The owner starts again: the web UI's fields come back.
		resumed, err := Merge(again.Policy, Desired{Source: testSource, Patch: want.Patch}, again.Applied)
		if err != nil {
			t.Fatal(err)
		}

		assertJSON(t, name+": resumed", resumed.Policy, string(before))
	}
}
