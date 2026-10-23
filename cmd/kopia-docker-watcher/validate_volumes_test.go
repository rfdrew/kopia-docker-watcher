package main

import (
	"strings"
	"testing"

	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
)

// shopContainer is a container of compose project shop with a volume, a
// bind mount and a tmpfs, whose labels back up the first two.
func shopContainer(volumes string) docker.Container {
	return docker.Container{ID: "5a5a5a5a5a5a5a5a", Name: "shop-db-1", State: docker.StateRunning,
		Labels: map[string]string{
			"kopia.enable": "true", "kopia.volumes": volumes, "kopia.exclude-volumes": "/run",
			"com.docker.compose.project": "shop",
		},
		Mounts: []docker.Mount{
			{Type: "volume", Name: "shop_data", Driver: "local", Source: "/var/lib/docker/volumes/shop_data/_data", Destination: "/data", RW: true},
			{Type: "bind", Source: "/srv/shop/config", Destination: "/etc/shop", RW: true},
			{Type: "tmpfs", Destination: "/run", RW: true},
		},
	}
}

func TestValidateVolumes(t *testing.T) {
	tests := []struct {
		name    string
		env     []string
		kopia   bool
		volumes string
		args    []string
		code    int
		stdout  []string
		absent  []string
	}{
		{
			name:    "sources from the mounts",
			volumes: "*",
			args:    []string{"--container", "shop-db-1"},
			stdout: []string{
				"kopia.volumes *",
				"kopia.exclude-volumes /run",
				"bind mount /srv/shop/config at /etc/shop -> /srv/shop/config",
				"volume shop_data at /data -> /var/lib/docker/volumes/shop_data/_data",
			},
			absent: []string{"warning", "error"},
		},
		{
			name:    "under HOST_PREFIX, normalized by kopia server",
			env:     kopiaEnv("HOST_PREFIX", "/host"),
			kopia:   true,
			volumes: "data",
			args:    []string{"--container", "shop-db-1"},
			stdout:  []string{"volume shop_data at /data -> root@nas:/host/var/lib/docker/volumes/shop_data/_data"},
			absent:  []string{"/srv/shop/config"},
		},
		{
			name:    "outside WATCHER_PATH_PREFIX",
			env:     []string{"WATCHER_PATH_PREFIX", "/var/lib/docker/volumes"},
			volumes: "data,/etc/shop",
			args:    []string{"--all"},
			code:    1,
			stdout: []string{
				"volume shop_data at /data -> /var/lib/docker/volumes/shop_data/_data",
				`error: kopia.volumes: "/srv/shop/config" makes the source path /srv/shop/config, which is outside WATCHER_PATH_PREFIX /var/lib/docker/volumes`,
			},
		},
		{
			name:    "unknown volume",
			volumes: "data,nope",
			args:    []string{"--container", "shop-db-1"},
			code:    1,
			stdout:  []string{"error: kopia.volumes: the container has no volume nope or shop_nope"},
		},
		{
			name:   "labels without a container",
			args:   []string{"kopia.enable=true", "kopia.volumes=data"},
			stdout: []string{"kopia.volumes: the paths of the mounts are only known for a container; use --container or --all"},
			absent: []string{"sources:", "error"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vr := validateRun{env: tt.env, engine: &fakeEngine{containers: []docker.Container{shopContainer(tt.volumes)}}}
			if tt.kopia {
				vr.kopia = newFakeKopia(t)
			}

			code, stdout, stderr := vr.run(t, tt.args...)
			checkOutput(t, code, stdout, stderr, tt.code, tt.stdout, nil)

			for _, l := range tt.absent {
				if strings.Contains(stdout, l) {
					t.Errorf("stdout has %q:\n%s", l, stdout)
				}
			}
		})
	}
}
