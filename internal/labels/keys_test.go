package labels

import (
	"regexp"
	"slices"
	"strings"
	"testing"
)

// keyRE allows lowercase dot-separated segments with single dashes inside.
var keyRE = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*(\.[a-z0-9]+(-[a-z0-9]+)*)*$`)

// The schema checks cover the planned keys too, so that a key added now
// does not clash with one planned for later.
func TestSchema(t *testing.T) {
	all := knownKeys()

	seen := map[string]bool{}

	for _, k := range all {
		if seen[k] {
			t.Errorf("key %q is listed twice", k)
		}

		seen[k] = true

		if !keyRE.MatchString(k) {
			t.Errorf("key %q is not lowercase dot-separated words joined by dashes", k)
		}
	}

	// A key is never both a value and a group: kopia.compression cannot
	// coexist with kopia.compression.min-size.
	for _, parent := range all {
		for _, child := range all {
			if strings.HasPrefix(child, parent+".") {
				t.Errorf("key %q is both a value and the parent of %q", parent, child)
			}
		}
	}

	for _, k := range keys {
		if planned[k.name] {
			t.Errorf("key %q is both supported and planned", k.name)
		}

		if k.parse == nil {
			t.Errorf("key %q has no parser", k.name)
		}
	}

	// Every exported key constant is supported.
	for _, k := range []string{KeyEnable, KeyPath, KeySchedule, KeySnapshotOnStart, KeySnapshotOnStop, KeyDescription, KeyTags, KeyMinInterval} {
		if _, ok := keyByName[k]; !ok {
			t.Errorf("Key constant %q has no parser", k)
		}
	}

	if len(keyByName) != len(keys) || len(planned) != len(plannedKeys) {
		t.Error("lookup maps do not match the key lists")
	}
}

// Every Python version label is supported under its old name.
func TestPythonLabelsSupported(t *testing.T) {
	for _, k := range []string{"enable", "path", "schedule", "snapshot-on-start", "snapshot-on-stop", "description", "tags"} {
		if _, ok := keyByName[k]; !ok {
			t.Errorf("kopia.%s of the Python version is not supported", k)
		}
	}

	if !slices.Contains(plannedKeys, "compression.algorithm") || slices.Contains(knownKeys(), "compression") {
		t.Error("the compression algorithm must be kopia.compression.algorithm")
	}
}
