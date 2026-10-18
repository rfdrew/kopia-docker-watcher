package backend

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestParseObject(t *testing.T) {
	for _, in := range []string{"", " ", "null", " null ", "{}"} {
		obj, err := parseObject(json.RawMessage(in))
		if err != nil || obj == nil || len(obj) != 0 {
			t.Errorf("parseObject(%q) = %v, %v", in, obj, err)
		}
	}

	for _, in := range []string{"[]", `"x"`, "1", "{", `{"a":}`} {
		if _, err := parseObject(json.RawMessage(in)); err == nil {
			t.Errorf("parseObject(%q): no error", in)
		}
	}
}

func TestObjectSet(t *testing.T) {
	obj, err := parseObject(json.RawMessage(`{"a":{"b":1,"keep":"<x&y>"},"n":null,"s":"str"}`))
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		path  []string
		value string
	}{
		{[]string{"a", "c"}, `2`},
		{[]string{"n", "x"}, `true`},
		{[]string{"new", "deep", "leaf"}, `"v"`},
		{[]string{"top"}, `[1]`},
	} {
		if err := obj.set(tc.path, json.RawMessage(tc.value)); err != nil {
			t.Fatalf("set %v: %v", tc.path, err)
		}
	}

	got, err := encode(obj)
	if err != nil {
		t.Fatal(err)
	}

	assertJSON(t, "object", got, `{"a":{"b":1,"c":2,"keep":"<x&y>"},"n":{"x":true},"new":{"deep":{"leaf":"v"}},"s":"str","top":[1]}`)

	if !strings.Contains(string(got), `"<x&y>"`) {
		t.Errorf("text was escaped: %s", got)
	}

	if err := obj.set([]string{"s", "x"}, json.RawMessage(`1`)); err == nil || err.Error() != `policy field s is not an object: "str"` {
		t.Errorf("set below a string: %v", err)
	}

	if err := obj.set([]string{"a", "b", "x"}, json.RawMessage(`1`)); err == nil || err.Error() != "policy field a.b is not an object: 1" {
		t.Errorf("set below a number: %v", err)
	}
}

func TestObjectGetRemove(t *testing.T) {
	obj, err := parseObject(json.RawMessage(`{"a":{"b":{"c":1},"d":2},"s":"x"}`))
	if err != nil {
		t.Fatal(err)
	}

	if v, ok := obj.get([]string{"a", "b", "c"}); !ok || string(v) != "1" {
		t.Errorf("get a.b.c = %s, %v", v, ok)
	}

	for _, p := range [][]string{{"x"}, {"a", "x"}, {"s", "x"}, {"a", "d", "x"}} {
		if _, ok := obj.get(p); ok {
			t.Errorf("get %v: found", p)
		}

		if err := obj.remove(p); err != nil {
			t.Errorf("remove %v: %v", p, err)
		}
	}

	if err := obj.remove([]string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
	}

	got, _ := encode(obj)
	assertJSON(t, "after removing a.b.c", got, `{"a":{"d":2},"s":"x"}`)

	if err := obj.remove([]string{"a", "d"}); err != nil {
		t.Fatal(err)
	}

	got, _ = encode(obj)
	assertJSON(t, "after removing a.d", got, `{"s":"x"}`)

	// An empty object that held nothing to remove stays.
	obj, _ = parseObject(json.RawMessage(`{"a":{"e":{}}}`))
	if err := obj.remove([]string{"a", "e", "x"}); err != nil {
		t.Fatal(err)
	}

	got, _ = encode(obj)
	assertJSON(t, "after removing a missing a.e.x", got, `{"a":{"e":{}}}`)
}

func TestLeaves(t *testing.T) {
	fs, err := leaves(json.RawMessage(`{"b":{"y":[1,{"z":2}],"x":{}},"a":null,"c":{"d":{"e":"f"}}}`))
	if err != nil {
		t.Fatal(err)
	}

	var got []string
	for _, f := range fs {
		got = append(got, f.Name()+"="+string(f.Value))
	}

	want := `a=null b.y=[1,{"z":2}] c.d.e="f"`
	if strings.Join(got, " ") != want {
		t.Errorf("leaves = %v, want %s", got, want)
	}

	if _, err := leaves(json.RawMessage(`{"a":{"b":}}`)); err == nil {
		t.Error("leaves of broken JSON: no error")
	}
}

func TestSameJSON(t *testing.T) {
	for _, pair := range [][2]string{
		{``, `{}`},
		{`null`, `{"a":null,"b":{}}`},
		{`{"a":{"b":{"c":{}}}}`, `{}`},
		{`{"a":[1,2]}`, ` { "a" : [ 1 , 2 ] } `},
	} {
		same, err := sameJSON(json.RawMessage(pair[0]), json.RawMessage(pair[1]))
		if err != nil || !same {
			t.Errorf("sameJSON(%s, %s) = %v, %v", pair[0], pair[1], same, err)
		}
	}

	for _, pair := range [][2]string{
		{`{"a":[]}`, `{}`},
		{`{"a":[null]}`, `{"a":[]}`},
		{`{"a":false}`, `{}`},
		{`{"a":0}`, `{}`},
		{`{"a":1}`, `{"a":1.0}`},
	} {
		same, err := sameJSON(json.RawMessage(pair[0]), json.RawMessage(pair[1]))
		if err != nil || same {
			t.Errorf("sameJSON(%s, %s) = %v, %v", pair[0], pair[1], same, err)
		}
	}

	if _, err := sameJSON(json.RawMessage(`{`), nil); err == nil {
		t.Error("sameJSON of broken JSON: no error")
	}
}

func TestTruncate(t *testing.T) {
	short := `{"a":1}`
	if got := truncate(json.RawMessage(short)); got != short {
		t.Errorf("truncate(%s) = %s", short, got)
	}

	long := `"` + strings.Repeat("é", 40) + `"`

	got := truncate(json.RawMessage(long))
	if !utf8.ValidString(got) || !strings.HasSuffix(got, "…") || len(got) > 64+len("…") {
		t.Errorf("truncate = %q", got)
	}
}
