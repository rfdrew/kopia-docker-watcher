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
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/config"
)

// DefaultSchedule is the cron expression used when the schedule label is
// absent, as in the Python version, and no other label sets a schedule:
// an interval, times of day, or one of them or cron in policy-json.
const DefaultSchedule = "0 * * * *"

// DefaultMinInterval is the shortest time between two snapshots of a
// source that a container start asks for, when the min-interval label is
// absent.
const DefaultMinInterval = 10 * time.Minute

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

	// MinInterval is the shortest time between the last snapshot of a
	// source and one that a start of this container asks for; zero means
	// no minimum. A snapshot at a stop is taken regardless.
	MinInterval time.Duration

	// Description is the snapshot description; empty means the container
	// name.
	Description string

	// Tags are the key:value pairs of the tags label in label order, each
	// key once.
	Tags []Tag

	// Policy holds the fields written to the Kopia policy of every path.
	// A field left unset is inherited from the parent policies: those of
	// parent directories, then user@host, host and global. The schedule
	// is DefaultSchedule unless a label sets one.
	Policy backend.PolicyPatch

	// CleanupOnRemove is what becomes of the source of each path once no
	// container backs it up any more.
	CleanupOnRemove CleanupMode

	// CleanupDeletePinned lets CleanupAll delete pinned snapshots too.
	// Without it, a source with a pinned snapshot keeps that snapshot and
	// its paused policy.
	CleanupDeletePinned bool
}

// CleanupMode is what becomes of a source once no container backs it up
// any more, the value of the cleanup-on-remove label. Docker forgets the
// labels of a container it removes, so the watcher remembers the mode of
// every container that backs a source up. A mode other than CleanupPause
// acts only after CLEANUP_GRACE without a container, only on a source the
// state records owners of, and never on one of another kopia server
// identity. A source kopia server had before a container first named it,
// as one the Python version made, is managed like any other: CleanupAll
// deletes the snapshots it had before too.
type CleanupMode string

// Cleanup modes, from the most cautious.
const (
	// CleanupPause pauses the source and keeps its policy and snapshots;
	// the default.
	CleanupPause CleanupMode = "pause"
	// CleanupPolicy deletes the policy of a source that has no snapshots,
	// which makes kopia server forget the source. A source with snapshots
	// stays paused: without its policy it would inherit the schedule of
	// the host or global policy.
	CleanupPolicy CleanupMode = "policy"
	// CleanupAll deletes the source with its snapshots and its policy. A
	// pinned snapshot stays unless CleanupDeletePinned allows it to go;
	// the source then keeps it and its paused policy, and only the
	// snapshots without a pin are deleted.
	CleanupAll CleanupMode = "all"
)

// cleanupHint names the values of the cleanup-on-remove label.
const cleanupHint = "use pause, policy or all"

// Tag is one key:value pair of the tags label.
type Tag struct {
	Key   string
	Value string
}

// Default returns the spec of a container without labels.
func Default() ContainerSpec {
	return ContainerSpec{
		SnapshotOnStart: true,
		MinInterval:     DefaultMinInterval,
		Policy: backend.PolicyPatch{
			Scheduling: backend.SchedulingPatch{Cron: []string{DefaultSchedule}},
		},
		CleanupOnRemove: CleanupPause,
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

	// DefaultSchedule is for a container that asks for no schedule: next
	// to an interval or times of day it would still snapshot every hour.
	if !p.scheduleLabel && p.setsSchedule {
		p.spec.Policy.Scheduling.Cron = nil
	}

	p.checkPolicy()

	if p.spec.Enable && len(p.spec.Paths) == 0 {
		p.label = Name(p.prefix, KeyPath)
		p.errorf("required when %s is true", Name(p.prefix, KeyEnable))
	}

	if p.spec.CleanupDeletePinned && p.spec.CleanupOnRemove != CleanupAll {
		p.label = Name(p.prefix, KeyCleanupDeletePinned)
		p.warn("has no effect unless "+Name(p.prefix, KeyCleanupOnRemove)+" is all", "", "")
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

	// scheduleLabel reports that the schedule label is set, even to an
	// empty value; setsSchedule that another label sets a schedule: an
	// interval, times of day, or one of them or cron in policy-json.
	scheduleLabel, setsSchedule bool
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
	p.scheduleLabel = true

	for item := range strings.SplitSeq(v, ";") {
		if item = strings.TrimSpace(item); item != "" {
			p.spec.Policy.Scheduling.Cron = append(p.spec.Policy.Scheduling.Cron, item)
		}
	}
}

// minIntervalHint names the accepted values of the min-interval label.
const minIntervalHint = "use a Go duration such as 10m, 1h or 24h (d is not a unit), or 0 for no minimum"

// minInterval parses a Go duration such as 10m or 1h30m; zero means no
// minimum. The label only paces snapshots, so a value that is empty, does
// not parse or is negative keeps the default with a warning rather than
// keeping the labels from being applied.
func (p *parser) minInterval(v string) {
	workaround := "the default " + DefaultMinInterval.String() + " is used"

	s := strings.TrimSpace(v)
	if s == "" {
		p.warn("empty value", workaround, minIntervalHint)
		return
	}

	d, err := time.ParseDuration(s)

	switch {
	case err != nil:
		p.warn(fmt.Sprintf("invalid duration %q", v), workaround, minIntervalHint)
	case d < 0:
		p.warn(fmt.Sprintf("negative duration %q", v), workaround, minIntervalHint)
	default:
		p.spec.MinInterval = d
	}
}

// cleanup parses the cleanup mode, in any case. A value that is empty or
// not a mode keeps the source paused, the most cautious mode, with a
// warning: the label decides whether snapshots are deleted, so a typo must
// not delete them.
func (p *parser) cleanup(v string) {
	workaround := "the source is only paused"

	switch s := CleanupMode(strings.ToLower(strings.TrimSpace(v))); s {
	case CleanupPause, CleanupPolicy, CleanupAll:
		p.spec.CleanupOnRemove = s
	case "":
		p.warn("empty value", workaround, cleanupHint)
	default:
		p.warn(fmt.Sprintf("unknown cleanup mode %q", v), workaround, cleanupHint)
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
