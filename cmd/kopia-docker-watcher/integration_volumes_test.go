//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/testcontainers/testcontainers-go"

	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiatest"
)

// TestIntegrationVolumes runs the watcher on a container whose volumes
// label picks a bind mount of a temporary directory and an anonymous
// volume. kopia server sees the host paths under HOST_PREFIX: the test
// creates them in its container, since it does not share the host's files.
// Docker reports the sources the watcher turns into source paths.
func TestIntegrationVolumes(t *testing.T) {
	s := kopiatest.Start(t, kopiatest.Options{})

	c, err := kopiaclient.New(kopiaclient.Options{
		URL:             s.URL,
		Username:        kopiatest.UIUser,
		Password:        kopiatest.UIPassword,
		CertFingerprint: s.CertFingerprint,
		Timeout:         time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()

	hostDir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	ctr, err := testcontainers.Run(ctx, "busybox:1.37",
		testcontainers.WithCmd("sleep", "600"),
		testcontainers.WithLabels(map[string]string{
			"kopia.enable":            "true",
			"kopia.volumes":           "*",
			"kopia.schedule":          "30 4 * * *",
			"kopia.snapshot-on-start": "false",
		}),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.Binds = append(hc.Binds, hostDir+":/srv/data")
			hc.Mounts = append(hc.Mounts, mount.Mount{Type: mount.TypeVolume, Target: "/vol"})
			hc.Tmpfs = map[string]string{"/run/tmp": ""}
		}),
	)
	testcontainers.CleanupContainer(t, ctr)

	if err != nil {
		t.Fatalf("start busybox: %v", err)
	}

	info, err := ctr.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var volumeDir string

	for _, m := range info.Mounts {
		if m.Destination == "/vol" {
			volumeDir = m.Source
		}
	}

	if volumeDir == "" {
		t.Fatalf("no volume at /vol: %+v", info.Mounts)
	}

	srcs := []kopiaclient.SourceInfo{
		{Host: kopiatest.Hostname, UserName: kopiatest.Username, Path: kopiatest.DataDir + hostDir},
		{Host: kopiatest.Hostname, UserName: kopiatest.Username, Path: kopiatest.DataDir + volumeDir},
	}

	for _, src := range srcs {
		s.Exec(t, "mkdir", "-p", src.Path)
	}

	td := newTestDeps(env(
		"KOPIA_SERVER_URL", s.URL,
		"KOPIA_SERVER_USERNAME", kopiatest.UIUser,
		"KOPIA_SERVER_PASSWORD", kopiatest.UIPassword,
		"KOPIA_SERVER_CERT_FINGERPRINT", s.CertFingerprint,
		"HOST_PREFIX", kopiatest.DataDir,
		"STATE_FILE", filepath.Join(t.TempDir(), "state.json"),
		"LOG_LEVEL", "debug",
	))
	td.newDocker = newDocker

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	done := make(chan int, 1)

	go func() { done <- run(runCtx, []string{"run"}, td.deps) }()

	// waitScheduling waits until the scheduling section of every source's
	// policy is want.
	waitScheduling := func(what, want string) {
		t.Helper()

		for deadline := time.Now().Add(2 * time.Minute); ; {
			left := slices.DeleteFunc(slices.Clone(srcs), func(src kopiaclient.SourceInfo) bool {
				return schedulingOf(ctx, t, c, src) == want
			})
			if len(left) == 0 {
				return
			}

			select {
			case code := <-done:
				t.Fatalf("watcher exited with %d while waiting for %s; log:\n%s", code, what, td.stderr)
			case <-time.After(500 * time.Millisecond):
			}

			if time.Now().After(deadline) {
				t.Fatalf("%s: %v without scheduling %s; log:\n%s", what, left, want, td.stderr)
			}
		}
	}

	waitScheduling("registration", `{"cron":["30 4 * * *"]}`)

	stopTimeout := 2 * time.Second
	if err := ctr.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop busybox: %v", err)
	}

	waitScheduling("pause", `{"manual":true}`)

	stop()

	select {
	case code := <-done:
		if code != 0 {
			t.Errorf("watcher exit code = %d; log:\n%s", code, td.stderr)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("watcher did not stop; log:\n%s", td.stderr)
	}
}

// schedulingOf returns the scheduling section of src's policy, compact,
// or "" while there is none.
func schedulingOf(ctx context.Context, t *testing.T, c *kopiaclient.Client, src kopiaclient.SourceInfo) string {
	t.Helper()

	pol, err := c.GetPolicy(ctx, src)
	if errors.Is(err, kopiaclient.ErrNotFound) {
		return ""
	}

	if err != nil {
		t.Fatalf("get policy: %v", err)
	}

	var p struct {
		Scheduling json.RawMessage `json:"scheduling"`
	}

	if err := json.Unmarshal(pol, &p); err != nil {
		t.Fatalf("policy %s: %v", pol, err)
	}

	out, err := json.Marshal(p.Scheduling)
	if err != nil {
		t.Fatal(err)
	}

	return string(out)
}
