package labels

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/config"
)

// Label keys of the containers the watcher stops or pauses around the
// snapshots of their sources, relative to the prefix.
const (
	KeyStopDuringBackup  = "stop-during-backup"
	KeyPauseDuringBackup = "pause-during-backup"
	KeyMaxDowntime       = "max-downtime"
	KeyDependsOn         = "depends-on"
)

// DefaultMaxDowntime bounds how long the watcher keeps a container stopped
// or paused for one snapshot when the max-downtime label is absent.
const DefaultMaxDowntime = time.Hour

// downtimeKeys are the labels of the containers the watcher stops or
// pauses around the snapshots of their sources, and of those that go down
// with them.
var downtimeKeys = []key{
	{KeyStopDuringBackup, func(p *parser, v string) { p.spec.StopDuringBackup = p.bool(v) }},
	{KeyPauseDuringBackup, func(p *parser, v string) { p.spec.PauseDuringBackup = p.bool(v) }},
	{KeyMaxDowntime, (*parser).maxDowntime},
	{KeyDependsOn, (*parser).dependsOn},
}

// maxDowntimeHint names the accepted values of the max-downtime label.
const maxDowntimeHint = "use a Go duration greater than zero such as 15m, 1h or 6h"

// maxDowntime parses how long a container may stay down for a snapshot. A
// value that does not parse is an error: a bound far shorter than meant
// would bring the container back before every snapshot is done.
func (p *parser) maxDowntime(v string) {
	p.maxDowntimeLabel = true

	s := strings.TrimSpace(v)
	d, err := time.ParseDuration(s)

	switch {
	case s == "":
		p.errorf("empty value; %s", maxDowntimeHint)
	case err != nil:
		p.errorf("invalid duration %q; %s", v, maxDowntimeHint)
	case d <= 0:
		p.errorf("%q is not greater than zero; %s", v, maxDowntimeHint)
	default:
		p.spec.MaxDowntime = d
	}
}

// containerName is what Docker accepts as a container name.
var containerName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

// DependsOn returns what the depends-on label of a container names, as
// Parse reads it into ContainerSpec.DependsOn, without the rest of the
// labels; nil without the label.
func DependsOn(containerLabels map[string]string, prefix string) []string {
	if prefix == "" {
		prefix = config.DefaultLabelPrefix
	}

	v, ok := containerLabels[Name(prefix, KeyDependsOn)]
	if !ok {
		return nil
	}

	p := &parser{prefix: prefix}
	p.dependsOn(v)

	return p.spec.DependsOn
}

// dependsOn parses the containers the container depends on: names
// separated by commas, trimmed, empty items skipped. Whether they exist is
// up to the reconciler, which sees the containers.
func (p *parser) dependsOn(v string) {
	p.spec.DependsOn = nil

	for item := range strings.SplitSeq(v, ",") {
		item = strings.TrimSpace(item)

		switch {
		case item == "":
		case !containerName.MatchString(item):
			p.errorf("%q is not a container or service name", item)
		case slices.Contains(p.spec.DependsOn, item):
			p.warn(fmt.Sprintf("%q is listed more than once", item), "", "")
		default:
			p.spec.DependsOn = append(p.spec.DependsOn, item)
		}
	}
}

// checkDowntime rejects stopping and pausing together, warns about a
// max-downtime that has no effect, and puts a container that is stopped or
// paused around its snapshots on the watcher's schedule. So does a stop or
// pause label with an error, as in strict mode: its snapshots then fail
// rather than run with the container up.
func (p *parser) checkDowntime() {
	stop, pause := Name(p.prefix, KeyStopDuringBackup), Name(p.prefix, KeyPauseDuringBackup)

	if p.spec.StopDuringBackup && p.spec.PauseDuringBackup {
		p.label = pause
		p.errorf("cannot be used with %s: a container is either stopped or paused", stop)
	}

	labelError := slices.ContainsFunc(p.issues, func(is Issue) bool {
		return is.Severity == Error && (is.Label == stop || is.Label == pause)
	})

	if p.spec.StopDuringBackup || p.spec.PauseDuringBackup || labelError {
		p.spec.Hooks = true
	}

	if p.maxDowntimeLabel && !p.spec.StopDuringBackup && !p.spec.PauseDuringBackup {
		p.label = Name(p.prefix, KeyMaxDowntime)
		p.warn(fmt.Sprintf("has no effect without %s or %s", stop, pause), "", "")
	}
}
