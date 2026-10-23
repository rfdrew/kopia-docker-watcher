package labels

import (
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"

	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
)

// VolumeAll is the item of the volumes label that picks every mount the
// container can write to.
const VolumeAll = "*"

// ComposeProjectLabel is the label docker compose puts the project name in.
// A compose volume "data" of project "shop" is the Docker volume
// "shop_data".
const ComposeProjectLabel = "com.docker.compose.project"

// volumeName is a Docker volume name, or the short name of a compose
// volume, which may be a single character.
var volumeName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

// volumesHint names the forms of an item of the volumes labels.
const volumesHint = "use volume names, mount destinations such as /var/lib/postgresql/data, or *"

// volumes parses a comma-separated list of mounts: VolumeAll, volume names
// and mount destinations, which are absolute paths inside the container.
// Items are trimmed and empty ones skipped, as in the path label. An item
// that is none of these is an error: it could pick no mount, or another
// one. exclude reports the exclude-volumes label, where VolumeAll is an
// error too.
func (p *parser) volumes(v string, exclude bool) []string {
	var out []string

	for item := range strings.SplitSeq(v, ",") {
		item = strings.TrimSpace(item)

		switch {
		case item == "":
			continue
		case item == VolumeAll && exclude:
			p.errorf("%q would exclude every mount; remove %s instead", item, Name(p.prefix, KeyVolumes))
			continue
		case item == VolumeAll:
		case strings.HasPrefix(item, "/"):
			item = path.Clean(item)
		case !volumeName.MatchString(item):
			p.errorf("%q is not a volume name or an absolute mount destination; %s", item, volumesHint)
			continue
		}

		if slices.Contains(out, item) {
			p.warn(fmt.Sprintf("%q is listed more than once", item), "", "")
			continue
		}

		out = append(out, item)
	}

	if !exclude && slices.Contains(out, VolumeAll) && len(out) > 1 {
		p.warn(fmt.Sprintf("%q already picks every mount the container can write to", VolumeAll), "", "")
	}

	return out
}

// VolumePath is a host path of a mount the volumes label picks.
type VolumePath struct {
	// Path is the mount's source on the Docker host: a volume's data
	// directory, or the source of a bind mount. HOST_PREFIX, the absolute
	// path check and WATCHER_PATH_PREFIX are applied where the source path
	// is built, as for the paths of the path label.
	Path string
	// Mount is the mount it comes from.
	Mount docker.Mount
}

// String describes the mount, e.g. volume shop_data at /data.
func (v VolumePath) String() string { return DescribeMount(v.Mount) }

// DescribeMount names a mount for messages, e.g. "volume shop_data at
// /data" or "bind mount /srv/app at /data".
func DescribeMount(m docker.Mount) string {
	switch m.Type {
	case "volume":
		return fmt.Sprintf("volume %s at %s", m.Name, m.Destination)
	case "bind":
		return fmt.Sprintf("bind mount %s at %s", m.Source, m.Destination)
	default:
		return fmt.Sprintf("%s mount at %s", orUnknown(m.Type), m.Destination)
	}
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}

	return s
}

// VolumePaths returns the host paths of the mounts of c that the volumes
// label of spec picks, without those exclude-volumes picks, in label order
// with the mounts of VolumeAll sorted by destination, each path once. spec
// is what Parse returned for c's labels; opts are the options it was
// called with.
//
// A volume name matches a volume mount of that name, or, for a container
// of docker compose project P, the volume P_name: the short name of the
// compose file. A destination matches the mount at that path inside the
// container. VolumeAll matches every mount the container can write to;
// read-only ones, such as /etc/localtime, only count when listed by name
// or destination.
//
// A listed item that matches no mount is an error, as is a mount the label
// lists by name or destination that is excluded too. A mount that has no
// directory on the host to back up is skipped with a warning, whether it
// was listed or picked by VolumeAll: a tmpfs or another mount type that is
// neither volume nor bind, a volume of a driver other than local, a socket
// such as the Docker socket, and a mount Docker reports no source for. So
// is an exclude-volumes item that matches no mount. Excluding a mount
// silences its warning. A label that picks no mount at all is a warning,
// or an error when the path label names no path either.
//
// The issues are reported under the volumes labels; none come for a spec
// without volumes.
func VolumePaths(spec ContainerSpec, c docker.Container, opts Options) ([]VolumePath, Issues) {
	if len(spec.Volumes) == 0 {
		return nil, nil
	}

	p := &parser{prefix: opts.Prefix, strict: opts.Strict}
	if p.prefix == "" {
		p.prefix = config.DefaultLabelPrefix
	}

	project := c.Labels[ComposeProjectLabel]
	excluded := make([]bool, len(c.Mounts))

	p.label = Name(p.prefix, KeyExcludeVolumes)

	for _, item := range spec.ExcludeVolumes {
		found := false

		for i, m := range c.Mounts {
			if mountMatches(item, m, project) {
				excluded[i], found = true, true
			}
		}

		if !found {
			p.warn(noMount(item, project), "ignored", "")
		}
	}

	p.label = Name(p.prefix, KeyVolumes)

	var out []VolumePath

	add := func(m docker.Mount) {
		if why := unusable(m); why != "" {
			p.warn(DescribeMount(m)+" "+why, "not backed up", "")
			return
		}

		if !slices.ContainsFunc(out, func(v VolumePath) bool { return v.Path == m.Source }) {
			out = append(out, VolumePath{Path: m.Source, Mount: m})
		}
	}

	for _, item := range spec.Volumes {
		if item == VolumeAll {
			for _, i := range byDestination(c.Mounts) {
				if m := c.Mounts[i]; !excluded[i] && m.RW {
					add(m)
				}
			}

			continue
		}

		found := false

		for i, m := range c.Mounts {
			if !mountMatches(item, m, project) {
				continue
			}

			found = true

			if excluded[i] {
				p.errorf("%s lists %q, and %s excludes it", Name(p.prefix, KeyVolumes), item, Name(p.prefix, KeyExcludeVolumes))
				continue
			}

			add(m)
		}

		if !found {
			p.errorf("%s", noMount(item, project))
		}
	}

	if len(out) == 0 && !p.issues.HasErrors() {
		if len(spec.Paths) == 0 {
			p.errorf("picks no mount to back up, and %s is not set", Name(p.prefix, KeyPath))
		} else {
			p.warn("picks no mount to back up", "", "")
		}
	}

	slices.SortStableFunc(p.issues, func(a, b Issue) int { return strings.Compare(a.Label, b.Label) })

	return out, p.issues
}

// mountMatches reports whether item, a volume name or a destination, picks m, a mount of
// a container of compose project project.
func mountMatches(item string, m docker.Mount, project string) bool {
	switch {
	case strings.HasPrefix(item, "/"):
		return path.Clean(m.Destination) == item
	default:
		return m.Type == "volume" && m.Name != "" && (m.Name == item || project != "" && m.Name == project+"_"+item)
	}
}

// noMount says that item picks no mount.
func noMount(item, project string) string {
	switch {
	case strings.HasPrefix(item, "/"):
		return fmt.Sprintf("the container has no mount at %s", item)
	case project != "":
		return fmt.Sprintf("the container has no volume %s or %s", item, project+"_"+item)
	default:
		return fmt.Sprintf("the container has no volume %s", item)
	}
}

// unusable says why m has no directory on the host to back up; empty if
// it has one.
func unusable(m docker.Mount) string {
	switch {
	case m.Type != "volume" && m.Type != "bind":
		return "has no path on the host"
	case m.Type == "volume" && m.Driver != "" && m.Driver != "local":
		return fmt.Sprintf("uses the volume driver %s, whose data has no path on the host", m.Driver)
	case m.Source == "":
		return "has no source path that Docker reports"
	case isSocket(m.Source):
		return "is a socket"
	default:
		return ""
	}
}

// isSocket reports whether a host path looks like a Unix socket, such as
// /var/run/docker.sock: its name ends in .sock. The watcher cannot look at
// the host's files, so the name is all it goes by.
func isSocket(p string) bool {
	return strings.HasSuffix(p, ".sock")
}

// byDestination returns the indexes of mounts sorted by destination.
func byDestination(mounts []docker.Mount) []int {
	idx := make([]int, len(mounts))
	for i := range idx {
		idx[i] = i
	}

	slices.SortStableFunc(idx, func(a, b int) int { return strings.Compare(mounts[a].Destination, mounts[b].Destination) })

	return idx
}
