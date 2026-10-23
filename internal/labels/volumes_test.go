package labels

import (
	"slices"
	"testing"

	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
)

func TestParseVolumes(t *testing.T) {
	tests := []struct {
		name             string
		labels           map[string]string
		volumes, exclude []string
		issues           []string
	}{
		{
			name:    "names, destinations and all",
			labels:  map[string]string{"kopia.enable": "true", "kopia.volumes": " data , /var/lib/postgresql/data/ ,, shop_cache.1"},
			volumes: []string{"data", "/var/lib/postgresql/data", "shop_cache.1"},
		},
		{
			name:    "volumes stand in for the path label",
			labels:  map[string]string{"kopia.enable": "true", "kopia.volumes": "*", "kopia.exclude-volumes": "cache,/tmp"},
			volumes: []string{"*"},
			exclude: []string{"cache", "/tmp"},
		},
		{
			name:    "duplicates after cleaning",
			labels:  map[string]string{"kopia.enable": "true", "kopia.volumes": "/data,/data/,data"},
			volumes: []string{"/data", "data"},
			issues:  []string{`warning kopia.volumes: "/data" is listed more than once`},
		},
		{
			name:    "all with more",
			labels:  map[string]string{"kopia.enable": "true", "kopia.volumes": "*,data"},
			volumes: []string{"*", "data"},
			issues:  []string{`warning kopia.volumes: "*" already picks every mount the container can write to`},
		},
		{
			name:    "bad items",
			labels:  map[string]string{"kopia.enable": "true", "kopia.volumes": "data,../x,_x,a b"},
			volumes: []string{"data"},
			issues: []string{
				`error kopia.volumes: "../x" is not a volume name or an absolute mount destination; use volume names, mount destinations such as /var/lib/postgresql/data, or *`,
				`error kopia.volumes: "_x" is not a volume name or an absolute mount destination; use volume names, mount destinations such as /var/lib/postgresql/data, or *`,
				`error kopia.volumes: "a b" is not a volume name or an absolute mount destination; use volume names, mount destinations such as /var/lib/postgresql/data, or *`,
			},
		},
		{
			name:    "all cannot be excluded",
			labels:  map[string]string{"kopia.enable": "true", "kopia.volumes": "*", "kopia.exclude-volumes": "*"},
			volumes: []string{"*"},
			issues:  []string{`error kopia.exclude-volumes: "*" would exclude every mount; remove kopia.volumes instead`},
		},
		{
			name:    "exclude without volumes",
			labels:  map[string]string{"kopia.enable": "true", "kopia.path": "/data", "kopia.exclude-volumes": "cache"},
			exclude: []string{"cache"},
			issues:  []string{"warning kopia.exclude-volumes: has no effect without kopia.volumes"},
		},
		{
			name:   "empty volumes is no volumes",
			labels: map[string]string{"kopia.enable": "true", "kopia.volumes": " , "},
			issues: []string{"error kopia.path: required when kopia.enable is true, unless kopia.volumes is set"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, issues := Parse(tt.labels, Options{})

			if !slices.Equal(got.Volumes, tt.volumes) || !slices.Equal(got.ExcludeVolumes, tt.exclude) {
				t.Errorf("volumes = %q, exclude = %q; want %q, %q", got.Volumes, got.ExcludeVolumes, tt.volumes, tt.exclude)
			}

			if gotIssues := issueStrings(issues); !slices.Equal(gotIssues, tt.issues) {
				t.Errorf("issues:\n got %q\nwant %q", gotIssues, tt.issues)
			}
		})
	}
}

// The mounts of a container of compose project shop.
var (
	dataVolume = docker.Mount{Type: "volume", Name: "shop_data", Driver: "local", Source: "/var/lib/docker/volumes/shop_data/_data",
		Destination: "/var/lib/postgresql/data", RW: true}
	cacheVolume = docker.Mount{Type: "volume", Name: "shop_cache", Driver: "local", Source: "/var/lib/docker/volumes/shop_cache/_data",
		Destination: "/cache", RW: true}
	externalVolume = docker.Mount{Type: "volume", Name: "media", Driver: "local", Source: "/var/lib/docker/volumes/media/_data",
		Destination: "/media", RW: true}
	configBind  = docker.Mount{Type: "bind", Source: "/srv/shop/config", Destination: "/etc/shop", RW: true}
	timeBind    = docker.Mount{Type: "bind", Source: "/etc/localtime", Destination: "/etc/localtime"}
	socketBind  = docker.Mount{Type: "bind", Source: "/var/run/docker.sock", Destination: "/var/run/docker.sock", RW: true}
	tmpfs       = docker.Mount{Type: "tmpfs", Destination: "/run", RW: true}
	nfsVolume   = docker.Mount{Type: "volume", Name: "shop_nfs", Driver: "rclone", Source: "/mnt/rclone/shop_nfs", Destination: "/nfs", RW: true}
	shopMounts  = []docker.Mount{tmpfs, socketBind, dataVolume, configBind, cacheVolume, externalVolume, timeBind, nfsVolume}
	shopProject = map[string]string{ComposeProjectLabel: "shop"}
)

func TestVolumePaths(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		mounts []docker.Mount
		strict bool
		paths  []string
		issues []string
	}{
		{
			name:   "no volumes label",
			labels: map[string]string{"kopia.path": "/data"},
			mounts: shopMounts,
		},
		{
			name:   "compose short name, full name and destination",
			labels: map[string]string{"kopia.volumes": "data,media,/etc/shop/"},
			mounts: shopMounts,
			paths:  []string{dataVolume.Source, externalVolume.Source, configBind.Source},
		},
		{
			name:   "full name of a compose volume",
			labels: map[string]string{"kopia.volumes": "shop_cache"},
			mounts: shopMounts,
			paths:  []string{cacheVolume.Source},
		},
		{
			name:   "a read-only mount counts when listed",
			labels: map[string]string{"kopia.volumes": "/etc/localtime"},
			mounts: shopMounts,
			paths:  []string{timeBind.Source},
		},
		{
			name:   "all, sorted by destination, without read-only mounts and with warnings for the rest",
			labels: map[string]string{"kopia.volumes": "*"},
			mounts: shopMounts,
			paths:  []string{cacheVolume.Source, configBind.Source, externalVolume.Source, dataVolume.Source},
			issues: []string{
				"warning kopia.volumes: volume shop_nfs at /nfs uses the volume driver rclone, whose data has no path on the host, not backed up",
				"warning kopia.volumes: tmpfs mount at /run has no path on the host, not backed up",
				"warning kopia.volumes: bind mount /var/run/docker.sock at /var/run/docker.sock is a socket, not backed up",
			},
		},
		{
			name:   "exclusions silence the warnings",
			labels: map[string]string{"kopia.volumes": "*", "kopia.exclude-volumes": "nfs,/run,/var/run/docker.sock,cache,/etc/shop"},
			mounts: shopMounts,
			paths:  []string{externalVolume.Source, dataVolume.Source},
		},
		{
			name:   "strict makes them errors",
			labels: map[string]string{"kopia.volumes": "*", "kopia.exclude-volumes": "nfs"},
			mounts: shopMounts,
			strict: true,
			paths:  []string{cacheVolume.Source, configBind.Source, externalVolume.Source, dataVolume.Source},
			issues: []string{
				"error kopia.volumes: tmpfs mount at /run has no path on the host",
				"error kopia.volumes: bind mount /var/run/docker.sock at /var/run/docker.sock is a socket",
			},
		},
		{
			name:   "listed mounts without a host path",
			labels: map[string]string{"kopia.volumes": "data,/run,nfs,/var/run/docker.sock"},
			mounts: shopMounts,
			paths:  []string{dataVolume.Source},
			issues: []string{
				"warning kopia.volumes: tmpfs mount at /run has no path on the host, not backed up",
				"warning kopia.volumes: volume shop_nfs at /nfs uses the volume driver rclone, whose data has no path on the host, not backed up",
				"warning kopia.volumes: bind mount /var/run/docker.sock at /var/run/docker.sock is a socket, not backed up",
			},
		},
		{
			name:   "no source",
			labels: map[string]string{"kopia.volumes": "data,odd"},
			mounts: []docker.Mount{dataVolume, {Type: "volume", Name: "odd", Destination: "/odd", RW: true}},
			paths:  []string{dataVolume.Source},
			issues: []string{"warning kopia.volumes: volume odd at /odd has no source path that Docker reports, not backed up"},
		},
		{
			name:   "unknown names and destinations",
			labels: map[string]string{"kopia.volumes": "data,nope,/nope"},
			mounts: shopMounts,
			paths:  []string{dataVolume.Source},
			issues: []string{
				"error kopia.volumes: the container has no volume nope or shop_nope",
				"error kopia.volumes: the container has no mount at /nope",
			},
		},
		{
			name:   "a bind mount has no name",
			labels: map[string]string{"kopia.volumes": "config"},
			mounts: []docker.Mount{configBind},
			issues: []string{"error kopia.volumes: the container has no volume config or shop_config"},
		},
		{
			name:   "listed and excluded",
			labels: map[string]string{"kopia.volumes": "data,cache", "kopia.exclude-volumes": "/cache,nope"},
			mounts: shopMounts,
			paths:  []string{dataVolume.Source},
			issues: []string{
				"warning kopia.exclude-volumes: the container has no volume nope or shop_nope, ignored",
				`error kopia.volumes: kopia.volumes lists "cache", and kopia.exclude-volumes excludes it`,
			},
		},
		{
			name:   "nothing picked without a path label",
			labels: map[string]string{"kopia.volumes": "*"},
			mounts: []docker.Mount{tmpfs, timeBind},
			issues: []string{
				"warning kopia.volumes: tmpfs mount at /run has no path on the host, not backed up",
				"error kopia.volumes: picks no mount to back up, and kopia.path is not set",
			},
		},
		{
			name:   "nothing picked next to a path label",
			labels: map[string]string{"kopia.path": "/data", "kopia.volumes": "*"},
			issues: []string{"warning kopia.volumes: picks no mount to back up"},
		},
		{
			name:   "the same source twice",
			labels: map[string]string{"kopia.volumes": "/a,/b"},
			mounts: []docker.Mount{
				{Type: "bind", Source: "/srv/x", Destination: "/a", RW: true},
				{Type: "bind", Source: "/srv/x", Destination: "/b", RW: true},
			},
			paths: []string{"/srv/x"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lbls := map[string]string{"kopia.enable": "true"}
			for k, v := range tt.labels {
				lbls[k] = v
			}

			if tt.mounts != nil {
				lbls[ComposeProjectLabel] = shopProject[ComposeProjectLabel]
			}

			opts := Options{Strict: tt.strict}

			spec, parseIssues := Parse(lbls, opts)
			if parseIssues.HasErrors() {
				t.Fatalf("parse: %q", issueStrings(parseIssues))
			}

			got, issues := VolumePaths(spec, docker.Container{Labels: lbls, Mounts: tt.mounts}, opts)

			var paths []string
			for _, v := range got {
				paths = append(paths, v.Path)
			}

			if !slices.Equal(paths, tt.paths) {
				t.Errorf("paths = %q, want %q", paths, tt.paths)
			}

			if gotIssues := issueStrings(issues); !slices.Equal(gotIssues, tt.issues) {
				t.Errorf("issues:\n got %q\nwant %q", gotIssues, tt.issues)
			}
		})
	}
}

// Without a compose project only full names match.
func TestVolumePathsWithoutProject(t *testing.T) {
	lbls := map[string]string{"kopia.enable": "true", "kopia.volumes": "data,shop_data"}

	spec, _ := Parse(lbls, Options{})

	got, issues := VolumePaths(spec, docker.Container{Labels: lbls, Mounts: shopMounts}, Options{})
	if len(got) != 1 || got[0].Path != dataVolume.Source || got[0].String() != "volume shop_data at /var/lib/postgresql/data" {
		t.Errorf("paths = %+v", got)
	}

	if want := []string{"error kopia.volumes: the container has no volume data"}; !slices.Equal(issueStrings(issues), want) {
		t.Errorf("issues = %q", issueStrings(issues))
	}
}

// The issues are reported under the label prefix.
func TestVolumePathsPrefix(t *testing.T) {
	lbls := map[string]string{"backup.enable": "true", "backup.volumes": "nope"}
	opts := Options{Prefix: "backup"}

	spec, _ := Parse(lbls, opts)

	_, issues := VolumePaths(spec, docker.Container{Labels: lbls}, opts)
	if want := []string{"error backup.volumes: the container has no volume nope"}; !slices.Equal(issueStrings(issues), want) {
		t.Errorf("issues = %q", issueStrings(issues))
	}
}
