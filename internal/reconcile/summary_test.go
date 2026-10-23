package reconcile

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
)

// waitLogLine waits for the first log line with msg and returns its
// fields.
func waitLogLine(t *testing.T, h *harness, msg string) map[string]any {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		for line := range strings.SplitSeq(h.logs.String(), "\n") {
			var m map[string]any
			if json.Unmarshal([]byte(line), &m) == nil && m["msg"] == msg {
				return m
			}
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("no %q log line:\n%s", msg, h.logs)

	return nil
}

func assertCounts(t *testing.T, line map[string]any, want map[string]float64) {
	t.Helper()

	for k, v := range want {
		if got, _ := line[k].(float64); got != v {
			t.Errorf("%s = %v, want %v; line %v", k, line[k], v, line)
		}
	}
}

// The first full reconcile logs at INFO what it found and, once each of
// its paths had a pass, what it did. A path whose only owner never ran is
// not registered: it counts as skipped.
func TestFirstScanSummary(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))
	h.engine.create("db", docker.StateExited, enable, pathLabel("/data/db"))
	h.engine.run("web")

	h.start()
	h.r.ReconcileAll()

	assertCounts(t, waitLogLine(t, h, "first full reconcile"), map[string]float64{
		"containers": 3, "backup_enabled": 2, "paths": 2,
	})

	done := waitLogLine(t, h, "first full reconcile done")
	assertCounts(t, done, map[string]float64{
		"paths": 2, "registered": 1, "updated": 0, "up_to_date": 0, "paused": 0, "skipped": 1, "failed": 0,
	})

	if _, ok := done["hint"]; ok {
		t.Errorf("hint although containers enable the backup: %v", done)
	}
}

// A start that finds the sources as their labels want them counts them up
// to date, and counts those it pauses because their owners stopped.
func TestFirstScanSummaryUpToDateAndPaused(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))
	dbID := h.engine.run("db", enable, pathLabel("/data/db"))

	h.mustPass(app)
	h.mustPass("/data/db")
	h.engine.setState(dbID, docker.StateExited)

	h.start()
	h.r.ReconcileAll()

	assertCounts(t, waitLogLine(t, h, "first full reconcile done"), map[string]float64{
		"paths": 2, "registered": 0, "updated": 1, "up_to_date": 1, "paused": 1, "failed": 0,
	})
}

// Only the first full reconcile is summarized.
func TestFirstScanSummaryOnce(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))

	passes := h.start()
	h.r.ReconcileAll()
	waitLogLine(t, h, "first full reconcile done")

	h.r.ReconcileAll()
	waitPass(t, passes, app, succeeded)
	waitPass(t, passes, app, succeeded)

	if n := strings.Count(h.logs.String(), `"msg":"first full reconcile done"`); n != 1 {
		t.Errorf("%d summaries, want 1", n)
	}
}

// A start that finds no container enabling the backup says so at once.
func TestFirstScanSummaryNothingEnabled(t *testing.T) {
	h := newHarness(t)
	h.engine.run("web")

	h.start()
	h.r.ReconcileAll()

	done := waitLogLine(t, h, "first full reconcile done")
	assertCounts(t, done, map[string]float64{"paths": 0, "registered": 0})

	if hint, _ := done["hint"].(string); hint != "no container has the label kopia.enable=true" {
		t.Errorf("hint = %q", hint)
	}
}
