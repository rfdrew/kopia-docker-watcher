package labels

import (
	"slices"
	"strings"
)

// Label keys, relative to the prefix: kopia.enable is KeyEnable under the
// default prefix.
const (
	KeyEnable          = "enable"
	KeyPath            = "path"
	KeyVolumes         = "volumes"
	KeyExcludeVolumes  = "exclude-volumes"
	KeySchedule        = "schedule"
	KeySnapshotOnStart = "snapshot-on-start"
	KeySnapshotOnStop  = "snapshot-on-stop"
	KeyDescription     = "description"
	KeyTags            = "tags"
	KeyMinInterval     = "min-interval"

	KeyCleanupOnRemove     = "cleanup-on-remove"
	KeyCleanupDeletePinned = "cleanup-delete-pinned"
)

// key is one supported label: its name relative to the prefix and how its
// value goes into the ContainerSpec. The parser reports problems with the
// value under the full label name.
type key struct {
	name  string
	parse func(p *parser, value string)
}

// keys are the supported labels. A nested key such as retention.daily is
// one more entry with a dotted name; its value goes to a nested field of
// ContainerSpec.Policy.
var keys = slices.Concat(coreKeys, policyKeys)

// coreKeys are the labels of everything but the policy fields.
var coreKeys = []key{
	{KeyEnable, func(p *parser, v string) { p.spec.Enable = p.bool(v) }},
	{KeyPath, (*parser).paths},
	{KeyVolumes, func(p *parser, v string) { p.spec.Volumes = p.volumes(v, false) }},
	{KeyExcludeVolumes, func(p *parser, v string) { p.spec.ExcludeVolumes = p.volumes(v, true) }},
	{KeySchedule, (*parser).schedule},
	{KeySnapshotOnStart, func(p *parser, v string) { p.spec.SnapshotOnStart = p.bool(v) }},
	{KeySnapshotOnStop, func(p *parser, v string) { p.spec.SnapshotOnStop = p.bool(v) }},
	{KeyDescription, func(p *parser, v string) { p.spec.Description = strings.TrimSpace(v) }},
	{KeyTags, (*parser).tags},
	{KeyMinInterval, (*parser).minInterval},
	{KeyCleanupOnRemove, (*parser).cleanup},
	{KeyCleanupDeletePinned, func(p *parser, v string) { p.spec.CleanupDeletePinned = p.bool(v) }},
}

// plannedKeys are labels of later releases. They are ignored with a "not
// supported by this version" warning rather than reported as unknown, and
// the schema tests check them together with keys, so that a key added now
// cannot clash with one of them later.
var plannedKeys = []string{
	// Hooks around a snapshot.
	"pre-exec",
	"post-exec",
	"stop-during-backup",
	"pause-during-backup",
	"pin-on-stop",
	"depends-on",
}

var (
	keyByName = func() map[string]key {
		m := make(map[string]key, len(keys))
		for _, k := range keys {
			m[k.name] = k
		}

		return m
	}()

	planned = func() map[string]bool {
		m := make(map[string]bool, len(plannedKeys))
		for _, k := range plannedKeys {
			m[k] = true
		}

		return m
	}()
)

// knownKeys returns the supported and the planned keys.
func knownKeys() []string {
	names := make([]string, 0, len(keys)+len(plannedKeys))
	for _, k := range keys {
		names = append(names, k.name)
	}

	return append(names, plannedKeys...)
}

// suggest returns a hint for an unknown key: the known key it differs from
// only in case or in '_' for '-', or the first label of the group it names,
// such as compression.algorithm for compression.
func suggest(prefix, rel string) string {
	norm := func(s string) string { return strings.ReplaceAll(strings.ToLower(s), "_", "-") }

	known := knownKeys()

	if i := slices.IndexFunc(known, func(k string) bool { return norm(k) == norm(rel) }); i >= 0 {
		return "did you mean " + Name(prefix, known[i]) + "?"
	}

	if i := slices.IndexFunc(known, func(k string) bool { return strings.HasPrefix(norm(k), norm(rel)+".") }); i >= 0 {
		return "it is a group of labels such as " + Name(prefix, known[i])
	}

	return ""
}
