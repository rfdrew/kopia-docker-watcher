// Package labels parses the labels of a Docker container into a
// ContainerSpec.
//
// Labels are "<prefix>.<key>=<value>" with the prefix from LABEL_PREFIX,
// kopia by default. Keys are dot-separated and may be nested, such as
// kopia.retention.daily, the way fields are nested in a Kopia policy. A key
// is never both a value and the parent of other keys: the compression
// algorithm is kopia.compression.algorithm, not kopia.compression, because
// kopia.compression.min-size and its siblings live under it.
//
// The names, defaults and boolean values of the Python version's labels are
// kept. Parsing never stops at the first problem: every problem is an Issue
// naming the label. Problems the Python version silently ignored, such as
// unknown keys and values that are not booleans, are warnings; strict mode
// makes every warning an error.
package labels

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/rfdrew/kopia-docker-watcher/internal/config"
)

// DefaultSchedule is the cron expression used when the schedule label is
// absent, as in the Python version.
const DefaultSchedule = "0 * * * *"

// ContainerSpec is what the labels of one container ask for.
type ContainerSpec struct {
	// Enable turns the backup of the container on.
	Enable bool

	// Paths are the paths of the path label as written, in label order and
	// without duplicates. HOST_PREFIX, the absolute path check and cleaning
	// are applied where the source path is built.
	Paths []string

	SnapshotOnStart bool
	SnapshotOnStop  bool

	// Description is the snapshot description; empty means the container
	// name.
	Description string

	// Tags are the key:value pairs of the tags label in label order, each
	// key once.
	Tags []Tag

	// Policy holds the fields written to the Kopia policy of every path.
	Policy Policy
}

// Tag is one key:value pair of the tags label.
type Tag struct {
	Key   string
	Value string
}

// Policy mirrors the sections of a Kopia policy that labels can set. A
// field left at its zero value is inherited from the parent policies:
// those of parent directories, then user@host, host and global.
type Policy struct {
	Scheduling Scheduling
}

// Scheduling is the scheduling section of the policy.
type Scheduling struct {
	// Cron lists the cron expressions of the schedule label, separated by
	// ';' in the label. It is DefaultSchedule when the label is absent and
	// empty, meaning inherit, when the label is set to an empty value.
	Cron []string
}

// Default returns the spec of a container without labels.
func Default() ContainerSpec {
	return ContainerSpec{
		SnapshotOnStart: true,
		Policy: Policy{
			Scheduling: Scheduling{Cron: []string{DefaultSchedule}},
		},
	}
}

// Options control parsing.
type Options struct {
	// Prefix is the label prefix without the trailing dot, normally
	// config.Config.LabelPrefix. Empty means config.DefaultLabelPrefix.
	Prefix string

	// Strict makes every warning an error.
	Strict bool
}

// Severity says whether an Issue keeps the container from being backed up.
type Severity int

const (
	// Warning is a problem the parser works around, as the Python version
	// did, e.g. by ignoring the label.
	Warning Severity = iota + 1

	// Error means the labels cannot be applied.
	Error
)

func (s Severity) String() string {
	switch s {
	case Warning:
		return "warning"
	case Error:
		return "error"
	default:
		return fmt.Sprintf("Severity(%d)", int(s))
	}
}

// Issue is a problem with one label.
type Issue struct {
	// Label is the full label name, e.g. kopia.enable.
	Label    string
	Severity Severity
	Message  string
}

func (i Issue) String() string {
	return i.Label + ": " + i.Message
}

// Issues are the problems found in the labels of one container, ordered by
// label name.
type Issues []Issue

// HasErrors reports whether any issue is an error.
func (is Issues) HasErrors() bool {
	return slices.ContainsFunc(is, func(i Issue) bool { return i.Severity == Error })
}

// Name returns the full label name of a key, e.g. kopia.enable.
func Name(prefix, key string) string {
	return prefix + "." + key
}

// Parse reads the labels under the prefix and ignores all others. It
// returns the spec, with defaults for absent labels, and every problem
// found. Labels with errors are skipped or applied in part, so the spec is
// only meant to be applied when the issues have no errors.
func Parse(labels map[string]string, opts Options) (ContainerSpec, Issues) {
	p := &parser{prefix: opts.Prefix, strict: opts.Strict, spec: Default()}
	if p.prefix == "" {
		p.prefix = config.DefaultLabelPrefix
	}

	for _, name := range slices.Sorted(maps.Keys(labels)) {
		rel, ok := strings.CutPrefix(name, p.prefix+".")
		if !ok {
			continue
		}

		p.label = name

		k, ok := keyByName[rel]
		if !ok {
			p.unknown(rel)
			continue
		}

		k.parse(p, labels[name])
	}

	if p.spec.Enable && len(p.spec.Paths) == 0 {
		p.label = Name(p.prefix, KeyPath)
		p.errorf("required when %s is true", Name(p.prefix, KeyEnable))
	}

	slices.SortStableFunc(p.issues, func(a, b Issue) int { return strings.Compare(a.Label, b.Label) })

	return p.spec, p.issues
}

type parser struct {
	prefix string
	strict bool
	spec   ContainerSpec
	issues Issues

	// label is the full name of the label being parsed; issues are
	// reported under it.
	label string
}

func (p *parser) errorf(format string, args ...any) {
	p.issues = append(p.issues, Issue{Label: p.label, Severity: Error, Message: fmt.Sprintf(format, args...)})
}

// warn reports a problem the parser works around. The message is what,
// then how it is worked around, then the hint, each part optional except
// the first. In strict mode it is an error and the message leaves the
// workaround out; the spec still gets it, but a spec with errors is not
// meant to be applied.
func (p *parser) warn(what, workaround, hint string) {
	is := Issue{Label: p.label, Severity: Warning, Message: what}

	if p.strict {
		is.Severity = Error
	} else if workaround != "" {
		is.Message += ", " + workaround
	}

	if hint != "" {
		is.Message += "; " + hint
	}

	p.issues = append(p.issues, is)
}

func (p *parser) unknown(rel string) {
	if planned[rel] {
		p.warn("not supported by this version", "ignored", "")
		return
	}

	p.warn("unknown label", "ignored", suggest(p.prefix, rel))
}

// bool parses a boolean the way the Python version did: only "true" in any
// case is true. Anything but "true" and "false" is false with a warning,
// including " true" with spaces, which the Python version did not trim.
func (p *parser) bool(v string) bool {
	switch s := strings.TrimSpace(v); {
	case strings.EqualFold(v, "true"):
		return true
	case strings.EqualFold(v, "false"):
		return false
	case s == "":
		p.warn("empty value", "treated as false", "use true or false")
	case strings.EqualFold(s, "true") || strings.EqualFold(s, "false"):
		p.warn(fmt.Sprintf("%q is not a boolean", v), "treated as false", "remove the spaces around it")
	default:
		p.warn(fmt.Sprintf("%q is not a boolean", v), "treated as false", "use true or false")
	}

	return false
}

// paths parses a comma-separated list of paths. As in the Python version
// items are trimmed and empty ones skipped.
func (p *parser) paths(v string) {
	p.spec.Paths = nil

	for item := range strings.SplitSeq(v, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		if slices.Contains(p.spec.Paths, item) {
			p.warn(fmt.Sprintf("%q is listed more than once", item), "", "")
			continue
		}

		p.spec.Paths = append(p.spec.Paths, item)
	}
}

// schedule parses cron expressions separated by ';'. They contain spaces
// and commas, so neither can separate them. No expression means inherit.
// The expressions themselves are left to kopia server to check.
func (p *parser) schedule(v string) {
	p.spec.Policy.Scheduling.Cron = nil

	for item := range strings.SplitSeq(v, ";") {
		if item = strings.TrimSpace(item); item != "" {
			p.spec.Policy.Scheduling.Cron = append(p.spec.Policy.Scheduling.Cron, item)
		}
	}
}

// tags parses comma-separated key:value pairs. The value may contain ':'.
// As in the Python version empty items are skipped and a repeated key keeps
// its last value.
func (p *parser) tags(v string) {
	p.spec.Tags = nil

	for item := range strings.SplitSeq(v, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		key, value, ok := strings.Cut(item, ":")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)

		switch {
		case !ok:
			p.warn(fmt.Sprintf("tag %q is not key:value", item), "ignored", "")
			continue
		case key == "":
			p.warn(fmt.Sprintf("tag %q has an empty key", item), "ignored", "")
			continue
		}

		if i := slices.IndexFunc(p.spec.Tags, func(t Tag) bool { return t.Key == key }); i >= 0 {
			p.warn(fmt.Sprintf("tag key %q is repeated", key), "the last value is used", "")
			p.spec.Tags[i].Value = value

			continue
		}

		p.spec.Tags = append(p.spec.Tags, Tag{Key: key, Value: value})
	}
}
