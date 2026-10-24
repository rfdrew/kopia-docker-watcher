//go:build integration

package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"

	"github.com/rfdrew/kopia-docker-watcher/internal/kopiatest"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
)

// TestIntegrationTriggerRestore runs the watcher against a real kopia
// server and a real Docker Engine: trigger takes a snapshot of a
// container's path through the watcher, and restore puts it back into a
// directory on kopia server, refusing a directory that is not empty
// without --overwrite.
func TestIntegrationTriggerRestore(t *testing.T) {
	s := kopiatest.Start(t, kopiatest.Options{})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()

	ctr, err := testcontainers.Run(ctx, "busybox:1.37",
		testcontainers.WithCmd("sleep", "600"),
		testcontainers.WithLabels(map[string]string{
			"kopia.enable":            "true",
			"kopia.path":              "/hello",
			"kopia.schedule":          "30 4 * * *",
			"kopia.snapshot-on-start": "false",
		}),
	)
	testcontainers.CleanupContainer(t, ctr)

	if err != nil {
		t.Fatalf("start busybox: %v", err)
	}

	envs := env(
		"KOPIA_SERVER_URL", s.URL,
		"KOPIA_SERVER_USERNAME", kopiatest.UIUser,
		"KOPIA_SERVER_PASSWORD", kopiatest.UIPassword,
		"KOPIA_SERVER_CERT_FINGERPRINT", s.CertFingerprint,
		"HOST_PREFIX", kopiatest.DataDir,
		"STATE_FILE", filepath.Join(t.TempDir(), "state.json"),
	)

	td := newTestDeps(envs)
	td.newDocker = newDocker

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	done := make(chan int, 1)

	go func() { done <- run(runCtx, []string{"run"}, td.deps) }()

	command := func(args ...string) (int, string, string) {
		t.Helper()

		cd := newTestDeps(envs)
		cd.dialControl = td.control.Dial

		code := run(ctx, args, cd.deps)

		return code, cd.stdout.String(), cd.stderr.String()
	}

	// Once the source is registered and kopia server's own first snapshot,
	// which runMissed takes at once, has ended.
	deadline := time.Now().Add(3 * time.Minute)

	for {
		code, out, _ := command("list", "--json")

		var rep reconcile.SourcesReport
		if code == 0 && json.Unmarshal([]byte(out), &rep) == nil && len(rep.Sources) == 1 &&
			!rep.Sources[0].LastSuccess.IsZero() && rep.Sources[0].Status == "IDLE" {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("the source was not ready: %s\nlog:\n%s", out, td.stderr)
		}

		select {
		case code := <-done:
			t.Fatalf("run returned %d; log:\n%s", code, td.stderr)
		case <-time.After(time.Second):
		}
	}

	s.Exec(t, "sh", "-c", "echo changed > "+kopiatest.HelloDir+"/hello.txt")

	info, err := ctr.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	name := strings.TrimPrefix(info.Name, "/")

	code, out, errOut := command("trigger", "--force", name)
	if code != 0 || !strings.Contains(out, kopiatest.HelloDir+": snapshot saved") {
		t.Fatalf("trigger = %d\n%s%s\nlog:\n%s", code, out, errOut, td.stderr)
	}

	target := kopiatest.DataDir + "/restored"

	code, out, errOut = command("restore", "--target", target, name)
	if code != 0 || !strings.Contains(out, "restore done") {
		t.Fatalf("restore = %d\n%s%s\nlog:\n%s", code, out, errOut, td.stderr)
	}

	if got := strings.TrimSpace(s.Exec(t, "cat", target+"/hello.txt")); got != "changed" {
		t.Fatalf("restored hello.txt = %q", got)
	}

	// The target is not empty now: nothing is overwritten without
	// --overwrite.
	s.Exec(t, "sh", "-c", "echo local > "+target+"/hello.txt")

	code, out, errOut = command("restore", "--target", target, name)
	if code != 1 || !strings.Contains(errOut, "--overwrite") {
		t.Fatalf("restore into a full directory = %d\n%s%s", code, out, errOut)
	}

	if got := strings.TrimSpace(s.Exec(t, "cat", target+"/hello.txt")); got != "local" {
		t.Fatalf("hello.txt overwritten without --overwrite: %q", got)
	}

	code, out, errOut = command("restore", "--target", target, "--overwrite", name)
	if code != 0 {
		t.Fatalf("restore --overwrite = %d\n%s%s", code, out, errOut)
	}

	if got := strings.TrimSpace(s.Exec(t, "cat", target+"/hello.txt")); got != "changed" {
		t.Fatalf("hello.txt after --overwrite = %q", got)
	}

	stop()

	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit code = %d; log:\n%s", code, td.stderr)
		}
	case <-time.After(time.Minute):
		t.Fatal("run did not stop")
	}
}
