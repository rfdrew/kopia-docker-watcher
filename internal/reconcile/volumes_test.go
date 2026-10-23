package reconcile

import (
	"slices"
	"strings"
	"testing"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
)

// The mounts of a container of compose project shop.
const (
	shopData   = "/var/lib/docker/volumes/shop_data/_data"
	shopConfig = "/srv/shop/config"
	media      = "/var/lib/docker/volumes/media/_data"
	shopLabel  = labels.ComposeProjectLabel + "=shop"
)

var (
	dataMount   = docker.Mount{Type: "volume", Name: "shop_data", Driver: "local", Source: shopData, Destination: "/data", RW: true}
	configMount = docker.Mount{Type: "bind", Source: shopConfig, Destination: "/etc/shop", RW: true}
	mediaMount  = docker.Mount{Type: "volume", Name: "media", Driver: "local", Source: media, Destination: "/media", RW: true}
	tmpfsMount  = docker.Mount{Type: "tmpfs", Destination: "/run", RW: true}
)

// mount gives container id the mounts.
func (f *fakeEngine) mount(id string, mounts ...docker.Mount) {
	f.mu.Lock()
	defer f.mu.Unlock()

	c, ok := f.containers[id]
	if !ok {
		panic("no container " + id)
	}

	c.Mounts = slices.Clone(mounts)
	f.containers[id] = c
}

// runMounted runs a container with the mounts.
func (f *fakeEngine) runMounted(name string, mounts []docker.Mount, labels ...string) string {
	id := f.run(name, labels...)
	f.mount(id, mounts...)

	return id
}

func volumesLabel(v string) string { return "kopia.volumes=" + v }

// The volumes label makes sources of the volume and bind mounts it picks,
// next to the path label, under HOST_PREFIX.
func TestVolumesMakeSources(t *testing.T) {
	for _, tt := range []struct {
		name   string
		prefix string
	}{
		{"no prefix", ""},
		{"HOST_PREFIX", "/host"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, withHostPrefix(tt.prefix))
			h.engine.runMounted("shop-db-1", []docker.Mount{tmpfsMount, dataMount, configMount},
				enable, shopLabel, pathLabel(app), volumesLabel("data,/etc/shop"), scheduleLabel("30 2 * * *"))

			paths, err := h.r.paths(t.Context())
			want := []string{tt.prefix + app, tt.prefix + shopConfig, tt.prefix + shopData}

			if err != nil || !slices.Equal(paths, want) {
				t.Fatalf("paths = %v, %v; want %v", paths, err, want)
			}

			for _, p := range want {
				h.mustPass(p)
				h.mustPolicy(p, `{"scheduling":{"cron":["30 2 * * *"]}}`)

				if o := h.record(p).Owners; len(o) != 1 || o[0].Name != "shop-db-1" {
					t.Errorf("owners of %s = %+v", p, o)
				}
			}

			if h.logged("container labels have") {
				t.Errorf("problems reported:\n%s", h.logs)
			}
		})
	}
}

// Mounts the label cannot back up are reported once as warnings; the
// others are backed up.
func TestVolumesWarnings(t *testing.T) {
	h := newHarness(t)
	h.engine.runMounted("shop-db-1", []docker.Mount{tmpfsMount, dataMount,
		{Type: "volume", Name: "shop_nfs", Driver: "rclone", Source: "/mnt/x", Destination: "/nfs", RW: true},
		{Type: "bind", Source: "/var/run/docker.sock", Destination: "/var/run/docker.sock", RW: true},
	}, enable, shopLabel, volumesLabel("*"))

	for range 2 {
		paths, err := h.r.paths(t.Context())
		if err != nil || !slices.Equal(paths, []string{shopData}) {
			t.Fatalf("paths = %v, %v", paths, err)
		}
	}

	if h.count("container labels have warnings") != 1 || !h.logged("container labels have warnings",
		"tmpfs mount at /run has no path on the host", "uses the volume driver rclone", "is a socket") {
		t.Errorf("log:\n%s", h.logs)
	}

	h.mustPass(shopData)
	h.mustPolicy(shopData, `{"scheduling":{"cron":["0 * * * *"]}}`)
}

// A volume source outside WATCHER_PATH_PREFIX is an error and makes no
// source, like a label path outside it.
func TestVolumesWatcherPathPrefix(t *testing.T) {
	h := newHarness(t, withWatcherPrefix("/var/lib/docker/volumes"))
	h.engine.runMounted("shop-db-1", []docker.Mount{dataMount, configMount}, enable, shopLabel, volumesLabel("*"))

	paths, err := h.r.paths(t.Context())
	if err != nil || !slices.Equal(paths, []string{shopData}) {
		t.Fatalf("paths = %v, %v", paths, err)
	}

	if !h.logged("container labels have errors", "kopia.volumes: "+`\"`+shopConfig+`\" makes the source path`, "outside WATCHER_PATH_PREFIX") {
		t.Errorf("not reported:\n%s", h.logs)
	}

	h.mustPass(shopData)
	h.mustPass(shopConfig)
	h.mustNoSource(shopConfig)
}

// A volume the label names that the container does not have is an error
// of the labels: none of the container's sources is registered.
func TestVolumesUnknown(t *testing.T) {
	h := newHarness(t)
	h.engine.runMounted("shop-db-1", []docker.Mount{dataMount}, enable, shopLabel, pathLabel(app), volumesLabel("data,nope"))

	if _, err := h.r.paths(t.Context()); err != nil {
		t.Fatal(err)
	}

	if !h.logged("container labels have errors", "the container has no volume nope or shop_nope") {
		t.Errorf("not reported:\n%s", h.logs)
	}

	for _, p := range []string{app, shopData} {
		if res := h.pass(p); len(res.problems) == 0 {
			t.Errorf("pass over %s: no problems", p)
		}

		h.mustNoSource(p)
	}
}

// A volume two containers share is one source with two owners, whose
// policies must agree.
func TestVolumesShared(t *testing.T) {
	h := newHarness(t)
	h.engine.runMounted("web", []docker.Mount{mediaMount}, enable, volumesLabel("media"))
	worker := h.engine.runMounted("worker", []docker.Mount{mediaMount, configMount}, enable, volumesLabel("/media,/etc/shop"))

	paths, err := h.r.paths(t.Context())
	if err != nil || !slices.Equal(paths, []string{shopConfig, media}) {
		t.Fatalf("paths = %v, %v", paths, err)
	}

	h.mustPass(media)

	if o := h.record(media).Owners; len(o) != 2 || o[0].Name != "web" || o[1].Name != "worker" {
		t.Errorf("owners = %+v", o)
	}

	h.engine.remove(worker)
	h.engine.runMounted("worker", []docker.Mount{mediaMount}, enable, volumesLabel("media"), scheduleLabel("30 2 * * *"))

	res := h.mustPass(media)
	if len(res.problems) != 1 || !strings.Contains(res.problems[0].Error(), "ask for different policies") {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustPolicy(media, `{"scheduling":{"cron":["0 * * * *"]}}`)
}

// Events carry no mounts. Once the state records the container as an
// owner of a volume's source, its events queue that source at once: the
// stop snapshot is taken and the source paused, and after the container
// is removed the source stays paused.
func TestVolumeEventsFromState(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	id := h.engine.runMounted("shop-db-1", []docker.Mount{dataMount}, enable, shopLabel, volumesLabel("data"), onStop)

	h.mustPass(shopData)

	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))

	h.mustPass(shopData)

	if !h.paused(shopData) {
		t.Error("not paused")
	}

	if w := h.onlyWait(shopData); !w.Stop || w.Reason != "stop of shop-db-1" {
		t.Errorf("wait = %+v", w)
	}

	h.r.HandleEvent(h.engine.remove(id))

	if evs := h.r.takePending(shopData); len(evs) != 1 || evs[0].Kind != docker.KindRemoved {
		t.Fatalf("pending = %+v", evs)
	}
}

// The events of a container the watcher has not seen yet queue its volume
// sources after a listing, in the order of the events; its later events
// and its removal queue them at once.
func TestVolumeEventsOfNewContainer(t *testing.T) {
	h := newHarness(t)
	passes := h.start()

	id := h.engine.runMounted("shop-db-1", []docker.Mount{dataMount, configMount}, enable, shopLabel,
		volumesLabel("data,/etc/shop"), onStop, noInterval)
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))

	waitPasses(t, passes, shopData, shopConfig)
	eventually(t, "the registration", func() bool { return h.fake.Known(h.src(shopData)) && h.fake.Known(h.src(shopConfig)) })

	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))

	waitPass(t, passes, shopData, succeeded)
	eventually(t, "the stop snapshot and the pause", func() bool {
		return h.paused(shopData) && len(h.fake.CallsFor(h.src(shopData), backendtest.TriggerSnapshot)) == 1
	})

	h.r.HandleEvent(h.engine.remove(id))
	waitPass(t, passes, shopConfig, succeeded)

	h.r.mu.Lock()
	_, cached := h.r.mounts[id]
	h.r.mu.Unlock()

	if cached {
		t.Error("the mounts of the removed container are still cached")
	}

	if !h.paused(shopConfig) {
		t.Error("not paused after the removal")
	}
}

// A container whose events queue a path through the path label too gets
// the event once for it.
func TestVolumeEventsOncePerPath(t *testing.T) {
	h := newHarness(t)
	id := h.engine.runMounted("app", []docker.Mount{{Type: "bind", Source: app, Destination: "/data", RW: true}},
		enable, pathLabel(app), volumesLabel("/data"))

	h.mustPass(app)
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))

	if evs := h.r.takePending(app); len(evs) != 1 {
		t.Errorf("pending = %+v", evs)
	}

	h.r.mu.Lock()
	defer h.r.mu.Unlock()

	if len(h.r.inspects) != 1 || !slices.Equal(h.r.inspects[0].queued, []string{app}) {
		t.Errorf("inspects = %+v", h.r.inspects)
	}
}
