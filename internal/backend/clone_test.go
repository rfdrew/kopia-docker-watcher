package backend

import (
	"reflect"
	"testing"
	"time"
)

// fill sets every field reachable from v to a non-zero value: one element
// in each slice, an allocated value behind each pointer.
func fill(v reflect.Value) {
	switch v.Kind() {
	case reflect.Struct:
		if v.Type() == reflect.TypeFor[time.Time]() {
			v.Set(reflect.ValueOf(time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)))

			return
		}

		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				fill(v.Field(i))
			}
		}
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		fill(v.Elem())
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		fill(v.Index(0))
	case reflect.String:
		v.SetString("x")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int64:
		v.SetInt(1)
	case reflect.Uint8:
		v.SetUint('x')
	default:
		panic("fill: unhandled kind " + v.Kind().String())
	}
}

// assertNoSharing fails if a and b share the memory behind any slice or
// pointer.
func assertNoSharing(t *testing.T, path string, a, b reflect.Value) {
	t.Helper()

	switch a.Kind() {
	case reflect.Struct:
		for i := range a.NumField() {
			if a.Type().Field(i).IsExported() {
				assertNoSharing(t, path+"."+a.Type().Field(i).Name, a.Field(i), b.Field(i))
			}
		}
	case reflect.Pointer:
		if !a.IsNil() && a.Pointer() == b.Pointer() {
			t.Errorf("%s: pointer shared", path)

			return
		}

		if !a.IsNil() {
			assertNoSharing(t, path, a.Elem(), b.Elem())
		}
	case reflect.Slice:
		if a.Len() > 0 && a.Pointer() == b.Pointer() {
			t.Errorf("%s: slice shared", path)

			return
		}

		for i := range a.Len() {
			assertNoSharing(t, path, a.Index(i), b.Index(i))
		}
	default:
	}
}

// Every Clone copies every field, also fields added later, and shares no
// memory with the original.
func TestClone(t *testing.T) {
	check := func(name string, orig any, clone func() any) {
		t.Helper()

		fill(reflect.ValueOf(orig).Elem())

		c := clone()
		o := reflect.ValueOf(orig).Elem().Interface()

		if !reflect.DeepEqual(o, c) {
			t.Errorf("%s: clone differs:\n%#v\n%#v", name, o, c)
		}

		assertNoSharing(t, name, reflect.ValueOf(o), reflect.ValueOf(c))
	}

	var (
		patch   PolicyPatch
		applied Applied
		desired Desired
		plan    Plan
		status  SourceStatus
	)

	check("PolicyPatch", &patch, func() any { return patch.Clone() })
	check("Applied", &applied, func() any { return applied.Clone() })
	check("Desired", &desired, func() any { return desired.Clone() })
	check("Plan", &plan, func() any { return plan.Clone() })
	check("SourceStatus", &status, func() any { return status.Clone() })
}
