package main

import (
	"strings"
	"testing"

	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
)

// validate shows require-healthy and warns about a running container
// without a health check, for which the label holds back no snapshot.
func TestValidateRequireHealthy(t *testing.T) {
	const (
		row  = "kopia.require-healthy true"
		note = "health: the snapshots of these sources wait while the container runs and Docker does not report it " +
			"healthy, but for one at a stop; the watcher takes the scheduled snapshots itself, kopia server's schedule " +
			"of them stays off"
		problem = "kopia.require-healthy: the container has no health check (HEALTHCHECK); the label holds back none of " +
			"its snapshots, but the watcher still takes the scheduled ones itself"
	)

	tests := []struct {
		name   string
		state  docker.State
		health docker.Health
		args   []string
		code   int
		stdout []string
		absent []string
	}{
		{
			name:   "healthy",
			state:  docker.StateRunning,
			health: docker.HealthHealthy,
			args:   []string{"--container", "db"},
			stdout: []string{row, note},
			absent: []string{problem},
		},
		{
			name:   "no health check",
			state:  docker.StateRunning,
			health: docker.HealthNone,
			args:   []string{"--container", "db"},
			stdout: []string{row, "warning: " + problem},
		},
		{
			name:   "no health check, strict",
			state:  docker.StateRunning,
			health: docker.HealthNone,
			args:   []string{"--strict", "--container", "db"},
			code:   1,
			stdout: []string{"error: " + problem},
		},
		{
			name:   "stopped",
			state:  docker.StateExited,
			health: docker.HealthNone,
			args:   []string{"--all"},
			stdout: []string{row},
			absent: []string{problem},
		},
		{
			name:   "labels without a container",
			args:   []string{"kopia.enable=true", "kopia.path=/data", "kopia.require-healthy=true"},
			stdout: []string{row, note},
			absent: []string{problem},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := docker.Container{ID: "d0d0d0d0d0d0d0d0", Name: "db", State: tt.state, Health: tt.health,
				Labels: map[string]string{"kopia.enable": "true", "kopia.path": "/data", "kopia.require-healthy": "true"}}

			vr := validateRun{engine: &fakeEngine{containers: []docker.Container{c}}}

			code, stdout, stderr := vr.run(t, tt.args...)
			checkOutput(t, code, stdout, stderr, tt.code, tt.stdout, nil)

			for _, l := range tt.absent {
				if strings.Contains(stdout, l) {
					t.Errorf("stdout has %q:\n%s", l, stdout)
				}
			}
		})
	}

	// Without the label there is neither the row nor the note.
	_, stdout, _ := runValidateTest(t, env(), "", "kopia.enable=true", "kopia.path=/data")
	if strings.Contains(stdout, "require-healthy") || strings.Contains(stdout, "health:") {
		t.Errorf("stdout without the label:\n%s", stdout)
	}
}
