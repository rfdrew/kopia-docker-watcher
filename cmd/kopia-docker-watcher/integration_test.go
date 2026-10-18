//go:build integration

package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"

	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiatest"
)

// TestIntegrationValidate runs validate against a real kopia server and a
// real Docker Engine, and makes sure it registers nothing.
func TestIntegrationValidate(t *testing.T) {
	s := kopiatest.Start(t, kopiatest.Options{})

	kopiaEnv := []string{
		"KOPIA_SERVER_URL", s.URL,
		"KOPIA_SERVER_USERNAME", kopiatest.UIUser,
		"KOPIA_SERVER_PASSWORD", kopiatest.UIPassword,
		"KOPIA_SERVER_CERT_FINGERPRINT", s.CertFingerprint,
		"HOST_PREFIX", kopiatest.DataDir,
	}

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

	waitRepository(t, c)

	before := sourcePaths(t, c)

	validate := func(t *testing.T, args ...string) (int, string, string) {
		t.Helper()

		td := newTestDeps(env(kopiaEnv...))
		td.newDocker = newDocker

		code := run(t.Context(), append([]string{"validate"}, args...), td.deps)

		return code, td.stdout.String(), td.stderr.String()
	}

	source := kopiatest.Username + "@" + kopiatest.Hostname + ":" + kopiatest.HelloDir

	t.Run("labels", func(t *testing.T) {
		code, stdout, stderr := validate(t, "kopia.enable=true", "kopia.path=hello/", "kopia.schedule=0 2 * * *")
		checkOutput(t, code, stdout, stderr, 0, []string{
			"hello/ -> " + source,
			"whether the paths exist is not checked: kopia server checks that only when it registers a source",
		}, nil)
		checkUpcoming(t, stdout)
	})

	t.Run("bad cron", func(t *testing.T) {
		code, stdout, stderr := validate(t, "kopia.enable=true", "kopia.path=/hello", "kopia.schedule=0 2 * * *;61 * * * *")
		checkOutput(t, code, stdout, stderr, 1, []string{
			`error: kopia.schedule: kopia server rejects the schedule: invalid cron expression "61 * * * *"`,
		}, nil)
	})

	t.Run("missing path is not an error", func(t *testing.T) {
		code, stdout, stderr := validate(t, "kopia.enable=true", "kopia.path=/missing")
		checkOutput(t, code, stdout, stderr, 0, []string{
			"/missing -> " + kopiatest.Username + "@" + kopiatest.Hostname + ":" + kopiatest.DataDir + "/missing",
		}, nil)
	})

	t.Run("container", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()

		ctr, err := testcontainers.Run(ctx, "busybox:1.37",
			testcontainers.WithCmd("sleep", "600"),
			testcontainers.WithLabels(map[string]string{
				"kopia.enable":   "true",
				"kopia.path":     "/hello",
				"kopia.schedule": "30 4 * * *",
			}),
		)
		testcontainers.CleanupContainer(t, ctr)

		if err != nil {
			t.Fatalf("start busybox: %v", err)
		}

		id := ctr.GetContainerID()

		code, stdout, stderr := validate(t, "--container", id)
		checkOutput(t, code, stdout, stderr, 0, []string{"kopia.schedule 30 4 * * *", "/hello -> " + source}, nil)
		checkUpcoming(t, stdout)

		if !strings.Contains(stdout, "("+id[:12]+", running)") {
			t.Errorf("no container header for %s:\n%s", id, stdout)
		}
	})

	if after := sourcePaths(t, c); !slices.Equal(after, before) {
		t.Errorf("validate changed the sources: before %q, after %q", before, after)
	}

	src := kopiaclient.SourceInfo{Host: kopiatest.Hostname, UserName: kopiatest.Username, Path: kopiatest.HelloDir}
	if _, err := c.GetPolicy(t.Context(), src); !errors.Is(err, kopiaclient.ErrNotFound) {
		t.Errorf("validate left a policy for %s: err = %v", src, err)
	}
}

func waitRepository(t *testing.T, c *kopiaclient.Client) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	for {
		st, err := c.RepoStatus(ctx)
		if err == nil && st.Connected {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("repository not connected: status %+v, error %v", st, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func sourcePaths(t *testing.T, c *kopiaclient.Client) []string {
	t.Helper()

	resp, err := c.ListSources(t.Context(), kopiaclient.SourceInfo{})
	if err != nil {
		t.Fatal(err)
	}

	var out []string
	for _, s := range resp.Sources {
		out = append(out, s.Source.String())
	}

	slices.Sort(out)

	return out
}

// checkUpcoming finds the next snapshot times in the output and parses
// them back.
func checkUpcoming(t *testing.T, stdout string) {
	t.Helper()

	const prefix = "    next snapshots (kopia server time): "

	for _, line := range strings.Split(stdout, "\n") {
		list, ok := strings.CutPrefix(line, prefix)
		if !ok {
			continue
		}

		times := strings.Split(list, ", ")
		if len(times) != upcomingSnapshots {
			t.Errorf("%d snapshot times, want %d: %q", len(times), upcomingSnapshots, line)
		}

		for _, s := range times {
			if _, err := time.Parse(timeLayout, s); err != nil {
				t.Errorf("snapshot time %q: %v", s, err)
			}
		}

		return
	}

	t.Errorf("no next snapshot times:\n%s", stdout)
}
