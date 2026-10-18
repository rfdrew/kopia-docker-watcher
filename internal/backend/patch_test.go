package backend

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestPolicyPatchFields(t *testing.T) {
	p := PolicyPatch{
		Scheduling: SchedulingPatch{Cron: []string{"0 * * * *", "30 2 * * *"}},
		Raw:        json.RawMessage(`{"retention": {"keepDaily": 7, "keepLatest": null}, "files": {"ignore": [".git", "*.tmp"]}, "noParent": true, "upload": {}}`),
	}

	got, err := p.Fields()
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		"files.ignore=[\".git\",\"*.tmp\"]",
		"noParent=true",
		"retention.keepDaily=7",
		"retention.keepLatest=null",
		"scheduling.cron=[\"0 * * * *\",\"30 2 * * *\"]",
	}

	var names []string
	for _, f := range got {
		names = append(names, f.Name()+"="+string(f.Value))
	}

	if !reflect.DeepEqual(names, want) {
		t.Errorf("Fields =\n%s\nwant\n%s", strings.Join(names, "\n"), strings.Join(want, "\n"))
	}
}

func TestPolicyPatchRawWins(t *testing.T) {
	p := PolicyPatch{
		Scheduling: SchedulingPatch{Cron: []string{"0 * * * *"}},
		Raw:        json.RawMessage(`{"scheduling":{"cron":["15 * * * *"]}}`),
	}

	got, err := p.Fields()
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 1 || string(got[0].Value) != `["15 * * * *"]` {
		t.Errorf("Fields = %v", got)
	}
}

func TestPolicyPatchInvalid(t *testing.T) {
	for name, p := range map[string]PolicyPatch{
		"raw array":       {Raw: json.RawMessage(`[]`)},
		"raw string":      {Raw: json.RawMessage(`"x"`)},
		"raw broken":      {Raw: json.RawMessage(`{"a":`)},
		"raw manual":      {Raw: json.RawMessage(`{"scheduling":{"manual":false}}`)},
		"raw value+child": {Raw: json.RawMessage(`{"scheduling":1}`), Scheduling: SchedulingPatch{Cron: []string{"0 * * * *"}}},
	} {
		err := p.Validate()
		if err == nil {
			t.Errorf("%s: no error", name)

			continue
		}

		if !IsConfig(err) {
			t.Errorf("%s: %v is not ErrConfig", name, err)
		}

		if same := p; p.Equal(same) {
			t.Errorf("%s: an invalid patch equals itself", name)
		}
	}

	for name, p := range map[string]PolicyPatch{
		"zero":      {},
		"raw null":  {Raw: json.RawMessage(`null`)},
		"raw empty": {Raw: json.RawMessage(`{}`)},
	} {
		if err := p.Validate(); err != nil {
			t.Errorf("%s: %v", name, err)
		}

		if fs, _ := p.Fields(); len(fs) != 0 {
			t.Errorf("%s: Fields = %v", name, fs)
		}
	}
}

func TestPolicyPatchIsZero(t *testing.T) {
	for name, tc := range map[string]struct {
		p    PolicyPatch
		zero bool
	}{
		"zero":       {PolicyPatch{}, true},
		"empty cron": {PolicyPatch{Scheduling: SchedulingPatch{Cron: []string{}}}, true},
		"raw null":   {PolicyPatch{Raw: json.RawMessage(`null`)}, true},
		"cron":       {cronPatch("0 * * * *"), false},
		"raw":        {PolicyPatch{Raw: json.RawMessage(`{}`)}, false},
	} {
		if got := tc.p.IsZero(); got != tc.zero {
			t.Errorf("%s: IsZero = %v", name, got)
		}
	}
}

func TestPolicyPatchEqual(t *testing.T) {
	a := cronPatch("0 * * * *")
	b := PolicyPatch{Raw: json.RawMessage(`{"scheduling": {"cron": [ "0 * * * *" ]}}`)}

	if !a.Equal(b) || !b.Equal(a) {
		t.Error("the same field set typed and raw is not equal")
	}

	for name, c := range map[string]PolicyPatch{
		"other value":  cronPatch("5 * * * *"),
		"more fields":  {Scheduling: a.Scheduling, Raw: json.RawMessage(`{"retention":{"keepDaily":1}}`)},
		"fewer fields": {},
		"other order":  cronPatch("0 * * * *", "5 * * * *"),
	} {
		if a.Equal(c) || c.Equal(a) {
			t.Errorf("%s: equal", name)
		}
	}
}

// The JSON form is part of STATE_FILE.
func TestPolicyPatchJSON(t *testing.T) {
	for _, tc := range []struct {
		p    PolicyPatch
		want string
	}{
		{PolicyPatch{}, `{}`},
		{PolicyPatch{Scheduling: SchedulingPatch{Cron: []string{}}}, `{}`},
		{cronPatch("0 * * * *"), `{"scheduling":{"cron":["0 * * * *"]}}`},
		{PolicyPatch{Raw: json.RawMessage(`{"a":1}`)}, `{"raw":{"a":1}}`},
	} {
		data, err := json.Marshal(tc.p)
		if err != nil {
			t.Fatal(err)
		}

		if string(data) != tc.want {
			t.Errorf("JSON = %s, want %s", data, tc.want)
		}

		var back PolicyPatch
		if err := json.Unmarshal(data, &back); err != nil {
			t.Fatal(err)
		}

		if !back.Equal(tc.p) {
			t.Errorf("round trip of %s = %+v", data, back)
		}
	}
}

func TestUnion(t *testing.T) {
	a := PolicyPatch{
		Scheduling: SchedulingPatch{Cron: []string{"0 * * * *"}},
		Raw:        json.RawMessage(`{"files":{"ignore":["a"]},"retention":{"keepDaily":1}}`),
	}
	b := PolicyPatch{Raw: json.RawMessage(`{"retention":{"keepDaily":2,"keepLatest":3},"files":5}`)}

	u, err := union(a, b)
	if err != nil {
		t.Fatal(err)
	}

	assertJSON(t, "union", u.Raw, `{"files":5,"retention":{"keepDaily":2,"keepLatest":3},"scheduling":{"cron":["0 * * * *"]}}`)

	if err := u.Validate(); err != nil {
		t.Error(err)
	}

	empty, err := union(PolicyPatch{}, PolicyPatch{})
	if err != nil || !empty.IsZero() {
		t.Errorf("union of empty patches = %+v, %v", empty, err)
	}

	if _, err := union(PolicyPatch{Raw: json.RawMessage(`[]`)}, PolicyPatch{}); err == nil {
		t.Error("union with an invalid patch: no error")
	}
}

func TestFingerprint(t *testing.T) {
	same := []string{
		`{"scheduling":{"cron":["0 * * * *"]},"retention":{}}`,
		`{ "retention": {"keepDaily": null}, "scheduling": { "cron": [ "0 * * * *" ] } }`,
		`{"scheduling":{"cron":["0 * * * *"]},"files":{"x":{}}}`,
	}

	want := mustFingerprint(t, same[0])
	for _, s := range same[1:] {
		if got := mustFingerprint(t, s); got != want {
			t.Errorf("Fingerprint(%s) = %s, want %s", s, got, want)
		}
	}

	if !strings.HasPrefix(want, "sha256:") || len(want) != len("sha256:")+64 {
		t.Errorf("Fingerprint = %q", want)
	}

	for _, s := range []string{
		`{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepDaily":7}}`,
		`{"scheduling":{"cron":["0 * * * * "]}}`,
		`{"scheduling":{"cron":[]}}`,
		`{"x":1.0}`,
	} {
		if mustFingerprint(t, s) == want {
			t.Errorf("Fingerprint(%s) matches", s)
		}
	}

	if mustFingerprint(t, `{"x":1}`) == mustFingerprint(t, `{"x":1.0}`) {
		t.Error("1 and 1.0 have the same fingerprint: numbers must be compared by their text")
	}

	empty := mustFingerprint(t, `{}`)
	if h, err := Fingerprint(nil); err != nil || h != empty {
		t.Errorf("Fingerprint(nil) = %q, %v; want %q", h, err, empty)
	}

	if _, err := Fingerprint(json.RawMessage(`{"a":`)); err == nil {
		t.Error("Fingerprint of broken JSON: no error")
	}
}
