package backend

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"
)

// object is a JSON object whose values are kept as raw JSON. A policy is
// decoded only along the fields the watcher touches, so every other field,
// including ones this package does not know, is written back as it came,
// and numbers keep their exact text.
type object map[string]json.RawMessage

// parseObject decodes a JSON object. Empty input and null are an empty
// object; any other value is an error.
func parseObject(raw json.RawMessage) (object, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return object{}, nil
	}

	if raw[0] != '{' {
		return nil, fmt.Errorf("not a JSON object: %s", truncate(raw))
	}

	var obj object
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("decode JSON object: %w", err)
	}

	if obj == nil {
		obj = object{}
	}

	return obj, nil
}

// isObject reports whether raw is a JSON object.
func isObject(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)

	return len(raw) > 0 && raw[0] == '{'
}

// isNull reports whether raw is empty or the JSON null.
func isNull(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)

	return len(raw) == 0 || bytes.Equal(raw, []byte("null"))
}

// encode writes v as compact JSON without escaping <, > and &, so values
// such as glob patterns keep their text.
func encode(v any) (json.RawMessage, error) {
	var buf bytes.Buffer

	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("encode JSON: %w", err)
	}

	return json.RawMessage(bytes.TrimRight(buf.Bytes(), "\n")), nil
}

// get returns the value at path; false if a field on the way is missing or
// not an object.
func (o object) get(path []string) (json.RawMessage, bool) {
	if len(path) == 0 {
		return nil, false
	}

	v, ok := o[path[0]]
	if !ok {
		return nil, false
	}

	if len(path) == 1 {
		return v, true
	}

	if !isObject(v) {
		return nil, false
	}

	child, err := parseObject(v)
	if err != nil {
		return nil, false
	}

	return child.get(path[1:])
}

// set stores value at path, creating the objects on the way. A missing or
// null field on the way becomes an object; any other value there is an
// error, since replacing it would drop data the watcher does not own.
func (o object) set(path []string, value json.RawMessage) error {
	return o.setAt(path, 0, value)
}

// setAt stores value at path[i:] of o, the object at path[:i].
func (o object) setAt(path []string, i int, value json.RawMessage) error {
	key := path[i]
	if i == len(path)-1 {
		o[key] = value

		return nil
	}

	cur, ok := o[key]
	if ok && !isNull(cur) && !isObject(cur) {
		return fmt.Errorf("policy field %s is not an object: %s", strings.Join(path[:i+1], "."), truncate(cur))
	}

	child, err := parseObject(cur)
	if err != nil {
		return fmt.Errorf("policy field %s: %w", strings.Join(path[:i+1], "."), err)
	}

	if err := child.setAt(path, i+1, value); err != nil {
		return err
	}

	enc, err := encode(child)
	if err != nil {
		return err
	}

	o[key] = enc

	return nil
}

// remove deletes the value at path. A missing field, or a value on the way
// that is not an object, means there is nothing to delete. An object on
// the way that the removal leaves empty is deleted too: in some places of
// a policy an empty object is not the same as none (an empty action
// command under actions overrides the inherited one), and Kopia writes the
// sections it always has back by itself.
func (o object) remove(path []string) error {
	switch len(path) {
	case 0:
		return nil
	case 1:
		delete(o, path[0])

		return nil
	}

	cur, ok := o[path[0]]
	if !ok || !isObject(cur) {
		return nil
	}

	child, err := parseObject(cur)
	if err != nil {
		return fmt.Errorf("policy field %s: %w", path[0], err)
	}

	if _, found := child.get(path[1:]); !found {
		return nil
	}

	if err := child.remove(path[1:]); err != nil {
		return err
	}

	if len(child) == 0 {
		delete(o, path[0])

		return nil
	}

	enc, err := encode(child)
	if err != nil {
		return err
	}

	o[path[0]] = enc

	return nil
}

// leaves returns the leaf fields of a JSON object: objects are descended
// into, any other value (arrays and null included) is a leaf. An empty
// object has no leaves. The result is sorted by path.
func leaves(raw json.RawMessage) ([]Field, error) {
	obj, err := parseObject(raw)
	if err != nil {
		return nil, err
	}

	var out []Field

	if err := collectLeaves(obj, nil, &out); err != nil {
		return nil, err
	}

	slices.SortFunc(out, func(a, b Field) int { return slices.Compare(a.Path, b.Path) })

	return out, nil
}

func collectLeaves(obj object, prefix []string, out *[]Field) error {
	for k, v := range obj {
		path := append(slices.Clip(prefix), k)

		if !isObject(v) {
			*out = append(*out, Field{Path: path, Value: compact(v)})

			continue
		}

		child, err := parseObject(v)
		if err != nil {
			return fmt.Errorf("%s: %w", strings.Join(path, "."), err)
		}

		if err := collectLeaves(child, path, out); err != nil {
			return err
		}
	}

	return nil
}

// compact returns raw without insignificant whitespace; raw is returned as
// is if it does not parse.
func compact(raw json.RawMessage) json.RawMessage {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return raw
	}

	return json.RawMessage(buf.Bytes())
}

// sameJSON reports whether a and b mean the same policy to Kopia: object
// key order and whitespace do not matter, and neither do null values and
// empty objects, which Kopia reads as unset. Empty input is an empty object.
// Numbers are compared by their text.
func sameJSON(a, b json.RawMessage) (bool, error) {
	ca, err := canonical(a)
	if err != nil {
		return false, err
	}

	cb, err := canonical(b)
	if err != nil {
		return false, err
	}

	return reflect.DeepEqual(ca, cb), nil
}

func canonical(raw json.RawMessage) (any, error) {
	if isNull(raw) {
		return map[string]any{}, nil
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("decode JSON: %w", err)
	}

	if v == nil {
		return map[string]any{}, nil
	}

	return prune(v), nil
}

// prune drops null values and empty objects from objects, recursively.
// Arrays are compared as they are.
func prune(v any) any {
	t, ok := v.(map[string]any)
	if !ok {
		return v
	}

	for k, child := range t {
		child = prune(child)
		if m, isMap := child.(map[string]any); child == nil || (isMap && len(m) == 0) {
			delete(t, k)

			continue
		}

		t[k] = child
	}

	return t
}

// truncate shortens raw JSON for an error message.
func truncate(raw json.RawMessage) string {
	const n = 64

	s := string(bytes.TrimSpace(raw))
	if len(s) <= n {
		return s
	}

	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}

	return s[:cut] + "…"
}
