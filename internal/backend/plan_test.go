package backend

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestNewPlanActions(t *testing.T) {
	want := Desired{Source: testSource, Patch: cronPatch("0 * * * *")}
	same := json.RawMessage(`{"scheduling":{"cron":["0 * * * *"]}}`)
	other := json.RawMessage(`{"scheduling":{"cron":["5 * * * *"]}}`)
	status := &SourceStatus{Source: testSource, Status: StatusIdle}

	tests := []struct {
		name    string
		known   bool
		current json.RawMessage
		last    Applied
		action  Action
	}{
		{"unknown source", false, nil, Applied{}, ActionRegister},
		{"unknown source with the right policy", false, same, Applied{}, ActionRegister},
		{"known, policy changes", true, other, Applied{}, ActionUpdate},
		{"known, no policy yet", true, nil, Applied{}, ActionUpdate},
		{"known, policy as wanted", true, same, Applied{}, ActionNone},
		{"known, last apply not confirmed", true, same, Applied{Pending: ptr(cronPatch("0 * * * *"))}, ActionUpdate},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var st *SourceStatus
			if tt.known {
				st = status
			}

			p, err := NewPlan(want, tt.last, tt.known, st, tt.current)
			if err != nil {
				t.Fatal(err)
			}

			if p.Action != tt.action {
				t.Errorf("Action = %v, want %v", p.Action, tt.action)
			}

			if p.Source != testSource || p.Known != tt.known || p.Status != st {
				t.Errorf("plan = %+v", p)
			}

			assertJSON(t, "Policy", p.Policy, string(same))
		})
	}
}

func TestNewPlanKeepsCurrent(t *testing.T) {
	current := json.RawMessage(`{"x":1}`)

	p, err := NewPlan(Desired{Source: testSource}, Applied{}, true, nil, current)
	if err != nil {
		t.Fatal(err)
	}

	current[2] = 'y'

	if string(p.Current) != `{"x":1}` {
		t.Errorf("Current = %s, shares memory with the argument", p.Current)
	}
}

func TestNewPlanErrors(t *testing.T) {
	_, err := NewPlan(Desired{Source: Source{Path: "/data"}}, Applied{}, true, nil, nil)

	var be *Error
	if !errors.As(err, &be) || be.Op != "PlanSource" || IsConfig(err) {
		t.Errorf("incomplete source: %v", err)
	}

	bad := Desired{Source: testSource, Patch: PolicyPatch{Raw: json.RawMessage(`{"scheduling":{"manual":true}}`)}}

	_, err = NewPlan(bad, Applied{}, true, nil, nil)
	if !errors.As(err, &be) || be.Op != "PlanSource" || be.Source != testSource || !IsConfig(err) {
		t.Errorf("invalid patch: %v", err)
	}

	_, err = NewPlan(Desired{Source: testSource}, Applied{}, true, nil, json.RawMessage(`"x"`))
	if !errors.As(err, &be) || IsConfig(err) || IsTemporary(err) {
		t.Errorf("current policy not an object: %v", err)
	}
}

// Replan falls back to the last applied patch without asking kopia server
// again.
func TestReplan(t *testing.T) {
	current := json.RawMessage(`{"scheduling":{"cron":["0 * * * *"]}}`)
	last := Applied{Patch: cronPatch("0 * * * *")}

	p, err := NewPlan(Desired{Source: testSource, Patch: cronPatch("0 25 * * *")}, last, true, nil, current)
	if err != nil {
		t.Fatal(err)
	}

	if p.Action != ActionUpdate {
		t.Fatalf("Action = %v", p.Action)
	}

	back, err := Replan(p, Desired{Source: testSource, Patch: last.Patch}, last)
	if err != nil {
		t.Fatal(err)
	}

	if back.Action != ActionNone || !back.Known || string(back.Current) != string(current) {
		t.Errorf("Replan = %+v", back)
	}

	if _, err := Replan(p, Desired{Source: Source{Host: "kopia", UserName: "root", Path: "/other"}}, last); err == nil {
		t.Error("Replan for another source: no error")
	}
}

func TestActionString(t *testing.T) {
	for a, want := range map[Action]string{ActionNone: "none", ActionRegister: "register", ActionUpdate: "update", Action(7): "Action(7)"} {
		if got := a.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", int(a), got, want)
		}
	}
}

func TestApplied(t *testing.T) {
	var zero Applied
	if !zero.IsZero() || zero.Validate() != nil {
		t.Errorf("zero Applied: IsZero %v, Validate %v", zero.IsZero(), zero.Validate())
	}

	for name, a := range map[string]Applied{
		"patch":   {Patch: cronPatch("0 * * * *")},
		"saved":   {SavedScheduling: json.RawMessage(`{}`)},
		"pending": {Pending: &PolicyPatch{}},
		"hash":    {StoredHash: "sha256:x"},
	} {
		if a.IsZero() {
			t.Errorf("%s: IsZero", name)
		}

		if err := a.Validate(); err != nil {
			t.Errorf("%s: Validate: %v", name, err)
		}
	}

	for name, a := range map[string]Applied{
		"bad patch":   {Patch: PolicyPatch{Raw: json.RawMessage(`5`)}},
		"bad pending": {Pending: &PolicyPatch{Raw: json.RawMessage(`{"scheduling":{"manual":true}}`)}},
		"saved array": {SavedScheduling: json.RawMessage(`[]`)},
		"saved bad":   {SavedScheduling: json.RawMessage(`{"a":`)},
	} {
		if err := a.Validate(); err == nil {
			t.Errorf("%s: Validate: no error", name)
		}
	}
}

func TestAppliedEqual(t *testing.T) {
	base := Applied{
		Patch:           cronPatch("0 * * * *"),
		SavedScheduling: json.RawMessage(`{"cron": ["0 * * * *"], "runMissed": true}`),
		Pending:         ptr(cronPatch("5 * * * *")),
		StoredHash:      "sha256:a",
	}

	same := Applied{
		Patch:           PolicyPatch{Raw: json.RawMessage(`{"scheduling":{"cron":["0 * * * *"]}}`)},
		SavedScheduling: json.RawMessage(`{"runMissed":true,"cron":["0 * * * *"]}`),
		Pending:         ptr(cronPatch("5 * * * *")),
		StoredHash:      "sha256:a",
	}

	if !base.Equal(same) || !same.Equal(base) {
		t.Error("equal Applied reported different")
	}

	for name, other := range map[string]Applied{
		"patch":      {Patch: cronPatch("1 * * * *"), SavedScheduling: base.SavedScheduling, Pending: base.Pending, StoredHash: base.StoredHash},
		"saved":      {Patch: base.Patch, SavedScheduling: json.RawMessage(`{}`), Pending: base.Pending, StoredHash: base.StoredHash},
		"no saved":   {Patch: base.Patch, Pending: base.Pending, StoredHash: base.StoredHash},
		"pending":    {Patch: base.Patch, SavedScheduling: base.SavedScheduling, Pending: ptr(cronPatch("6 * * * *")), StoredHash: base.StoredHash},
		"no pending": {Patch: base.Patch, SavedScheduling: base.SavedScheduling, StoredHash: base.StoredHash},
		"hash":       {Patch: base.Patch, SavedScheduling: base.SavedScheduling, Pending: base.Pending, StoredHash: "sha256:b"},
	} {
		if base.Equal(other) || other.Equal(base) {
			t.Errorf("%s: different Applied reported equal", name)
		}
	}

	if !(Applied{}).Equal(Applied{}) {
		t.Error("zero values reported different")
	}
}

func TestSource(t *testing.T) {
	if got := testSource.String(); got != "root@kopia:/data/app" {
		t.Errorf("String = %q", got)
	}

	if err := testSource.Validate(); err != nil {
		t.Error(err)
	}

	for _, s := range []Source{{UserName: "u", Path: "/p"}, {Host: "h", Path: "/p"}, {Host: "h", UserName: "u"}} {
		if s.Validate() == nil {
			t.Errorf("Validate(%+v): no error", s)
		}
	}

	a := Source{Host: "a", UserName: "z", Path: "/z"}
	b := Source{Host: "b", UserName: "a", Path: "/a"}
	c := Source{Host: "b", UserName: "b", Path: "/a"}
	d := Source{Host: "b", UserName: "b", Path: "/b"}

	for _, pair := range [][2]Source{{a, b}, {b, c}, {c, d}} {
		if pair[0].Compare(pair[1]) >= 0 || pair[1].Compare(pair[0]) <= 0 {
			t.Errorf("Compare(%v, %v) is not ordered", pair[0], pair[1])
		}
	}

	if same := a; a.Compare(same) != 0 {
		t.Error("Compare of equal sources is not 0")
	}

	data, err := json.Marshal(testSource)
	if err != nil {
		t.Fatal(err)
	}

	if string(data) != `{"host":"kopia","userName":"root","path":"/data/app"}` {
		t.Errorf("JSON = %s", data)
	}
}
