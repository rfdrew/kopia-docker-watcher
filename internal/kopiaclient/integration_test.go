//go:build integration

package kopiaclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/kopiatest"
)

func newIntegrationClient(t *testing.T, s *kopiatest.Server, mod ...func(*Options)) *Client {
	t.Helper()

	opts := Options{
		URL:             s.URL,
		Username:        kopiatest.UIUser,
		Password:        kopiatest.UIPassword,
		CertFingerprint: s.CertFingerprint,
		Timeout:         time.Minute,
		UserAgent:       "kopia-docker-watcher-integration-test",
	}

	for _, m := range mod {
		m(&opts)
	}

	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}

	return c
}

func waitConnected(t *testing.T, c *Client) *RepoStatus {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	for {
		st, err := c.RepoStatus(ctx)
		if err == nil && st.Connected {
			return st
		}

		select {
		case <-ctx.Done():
			t.Fatalf("repository not connected: status %+v, error %v", st, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (c *Client) currentCSRFToken() string {
	c.csrfMu.Lock()
	defer c.csrfMu.Unlock()

	return c.csrfToken
}

// syncBuffer collects log output written from several goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.b.String()
}

func TestIntegrationAuthAndCSRF(t *testing.T) {
	s := kopiatest.Start(t, kopiatest.Options{})

	var logBuf syncBuffer

	c := newIntegrationClient(t, s, func(o *Options) {
		o.Logger = slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	})

	st := waitConnected(t, c)
	if st.Hostname != kopiatest.Hostname || st.Username != kopiatest.Username || st.Storage != "filesystem" {
		t.Fatalf("status = %+v", st)
	}

	// The assumptions the client is built on, checked against the release.
	t.Run("request without CSRF token is 401 without WWW-Authenticate", func(t *testing.T) {
		resp, err := c.send(t.Context(), request{method: http.MethodGet, path: "repo/status"}, nil, "")
		if err != nil {
			t.Fatal(err)
		}

		if !resp.isCSRFRejection() {
			t.Fatalf("status %d, WWW-Authenticate %q: kopia no longer signals CSRF failures this way",
				resp.status, resp.header.Get("WWW-Authenticate"))
		}
	})

	t.Run("stale token is refetched once", func(t *testing.T) {
		c.csrfMu.Lock()
		c.csrfToken = strings.Repeat("0", 64)
		c.csrfMu.Unlock()

		if _, err := c.RepoStatus(t.Context()); err != nil {
			t.Fatal(err)
		}

		if tok := c.currentCSRFToken(); tok == strings.Repeat("0", 64) || tok == "" {
			t.Fatalf("token not refreshed: %q", tok)
		}
	})

	t.Run("server restart invalidates the token", func(t *testing.T) {
		before := c.currentCSRFToken()
		rejections := strings.Count(logBuf.String(), "rejected the CSRF token")

		s.Restart(t)

		st := waitConnected(t, c)
		if !st.Connected {
			t.Fatalf("status = %+v", st)
		}

		if c.currentCSRFToken() == before {
			t.Error("token unchanged after kopia server restart")
		}

		if got := strings.Count(logBuf.String(), "rejected the CSRF token") - rejections; got != 1 {
			t.Errorf("CSRF rejections after restart = %d, want 1:\n%s", got, logBuf.String())
		}
	})

	t.Run("wrong password", func(t *testing.T) {
		bad := newIntegrationClient(t, s, func(o *Options) { o.Password = "wrong" })

		_, err := bad.RepoStatus(t.Context())
		if !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("err = %v, want ErrUnauthorized", err)
		}
	})

	t.Run("non-UI user", func(t *testing.T) {
		ctl := newIntegrationClient(t, s, func(o *Options) {
			o.Username, o.Password = kopiatest.ControlUser, kopiatest.ControlPassword
		})

		_, err := ctl.RepoStatus(t.Context())
		if !errors.Is(err, ErrAccessDenied) {
			t.Fatalf("err = %v, want ErrAccessDenied", err)
		}
	})

	t.Run("certificate fingerprint mismatch", func(t *testing.T) {
		other := newIntegrationClient(t, s, func(o *Options) { o.CertFingerprint = strings.Repeat("ab", 32) })

		_, err := other.RepoStatus(t.Context())

		var mismatch *CertificateMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("err = %v, want CertificateMismatchError", err)
		}

		if mismatch.Got != s.CertFingerprint {
			t.Errorf("reported fingerprint %s, server prints %s", mismatch.Got, s.CertFingerprint)
		}
	})
}

func TestIntegrationUIDisabled(t *testing.T) {
	s := kopiatest.Start(t, kopiatest.Options{DisableUI: true})
	c := newIntegrationClient(t, s, func(o *Options) { o.CertFingerprint = "" })

	_, err := c.RepoStatus(t.Context())
	if !errors.Is(err, ErrUIDisabled) {
		t.Fatalf("err = %v, want ErrUIDisabled", err)
	}
}

func waitSnapshot(t *testing.T, c *Client, src SourceInfo) SourceStatus {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	for {
		resp, err := c.ListSources(ctx, src)
		if err == nil && len(resp.Sources) == 1 {
			s := resp.Sources[0]
			if s.LastSnapshot != nil && s.Status == SourceStatusIdle {
				return s
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf("no snapshot of %s: sources %+v, error %v", src, resp, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// TestIntegrationSourceLifecycle walks one source through every method
// the watcher uses, checking the behavior ROADMAP 9.1 relies on.
func TestIntegrationSourceLifecycle(t *testing.T) {
	s := kopiatest.Start(t, kopiatest.Options{})
	c := newIntegrationClient(t, s)
	ctx := t.Context()

	waitConnected(t, c)

	all, err := c.ListSources(ctx, SourceInfo{})
	if err != nil {
		t.Fatal(err)
	}

	if all.LocalHost != kopiatest.Hostname || all.LocalUsername != kopiatest.Username {
		t.Fatalf("server identity = %s@%s", all.LocalUsername, all.LocalHost)
	}

	src := SourceInfo{Host: all.LocalHost, UserName: all.LocalUsername, Path: kopiatest.HelloDir}

	t.Run("paths/resolve cleans the path without checking it", func(t *testing.T) {
		for in, want := range map[string]string{
			kopiatest.HelloDir + "/":           kopiatest.HelloDir,
			kopiatest.HelloDir + "/../hello/.": kopiatest.HelloDir,
			"/no/such/dir/":                    "/no/such/dir",
		} {
			got, err := c.ResolvePath(ctx, in)
			if err != nil {
				t.Fatal(err)
			}

			if got != (SourceInfo{Host: src.Host, UserName: src.UserName, Path: want}) {
				t.Errorf("ResolvePath(%q) = %+v", in, got)
			}
		}
	})

	t.Run("missing policy is NOT_FOUND", func(t *testing.T) {
		if _, err := c.GetPolicy(ctx, src); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})

	t.Run("POST sources checks the path", func(t *testing.T) {
		_, err := c.CreateSource(ctx, CreateSourceRequest{Path: kopiatest.DataDir + "/missing", Policy: json.RawMessage(`{}`)})
		if !errors.Is(err, ErrPathNotFound) {
			t.Fatalf("err = %v, want ErrPathNotFound", err)
		}
	})

	t.Run("invalid policies are ErrInvalidPolicy", func(t *testing.T) {
		trailing := src
		trailing.Path += "/"

		err := c.SetPolicy(ctx, trailing, json.RawMessage(`{}`))
		if !errors.Is(err, ErrInvalidPolicy) || IsRetryable(err) {
			t.Errorf("trailing slash: err = %v, want non-retryable ErrInvalidPolicy", err)
		}

		err = c.SetPolicy(ctx, src, json.RawMessage(`{"scheduling":{"manual":true,"cron":[]}}`))
		if !errors.Is(err, ErrInvalidPolicy) {
			t.Errorf("manual with cron: err = %v, want ErrInvalidPolicy", err)
		}

		err = c.SetPolicy(ctx, src, json.RawMessage(`{"scheduling":{"cron":["not a cron"]}}`))
		if !errors.Is(err, ErrInvalidPolicy) {
			t.Errorf("bad cron: err = %v, want ErrInvalidPolicy", err)
		}
	})

	t.Run("policy/resolve previews the schedule", func(t *testing.T) {
		res, err := c.ResolvePolicy(ctx, src, ResolvePolicyRequest{
			Updates: json.RawMessage(`{"scheduling":{"cron":["0 3 * * *"]}}`), NumUpcomingSnapshotTimes: 3,
		})
		if err != nil {
			t.Fatal(err)
		}

		if len(res.UpcomingSnapshotTimes) != 3 || res.SchedulingError != "" {
			t.Errorf("resolve = %+v", res)
		}

		res, err = c.ResolvePolicy(ctx, src, ResolvePolicyRequest{
			Updates: json.RawMessage(`{"scheduling":{"cron":["not a cron"]}}`), NumUpcomingSnapshotTimes: 1,
		})
		if err != nil {
			t.Fatal(err)
		}

		if res.SchedulingError == "" {
			t.Errorf("no scheduling error for a bad cron: %+v", res)
		}
	})

	t.Run("register, snapshot, inspect", func(t *testing.T) {
		if _, err := c.CreateSource(ctx, CreateSourceRequest{Path: src.Path, Policy: json.RawMessage(`{"scheduling":{"manual":true}}`)}); err != nil {
			t.Fatal(err)
		}

		if err := c.SetPolicy(ctx, src, json.RawMessage(`{"scheduling":{"manual":true},"retention":{"keepLatest":7}}`)); err != nil {
			t.Fatal(err)
		}

		raw, err := c.GetPolicy(ctx, src)
		if err != nil {
			t.Fatal(err)
		}

		var pol struct {
			Scheduling struct{ Manual bool } `json:"scheduling"`
			Retention  struct {
				KeepLatest int `json:"keepLatest"`
			} `json:"retention"`
		}

		if err := json.Unmarshal(raw, &pol); err != nil || !pol.Scheduling.Manual || pol.Retention.KeepLatest != 7 {
			t.Fatalf("policy = %s (%v)", raw, err)
		}

		if err := c.StartSnapshot(ctx, src); err != nil {
			t.Fatal(err)
		}

		st := waitSnapshot(t, c, src)
		if !st.Schedule.Manual || st.LastSnapshot.IncompleteReason != "" || st.LastSnapshot.Source != src {
			t.Errorf("source status = %+v", st)
		}

		tasks, err := c.ListTasks(ctx)
		if err != nil {
			t.Fatal(err)
		}

		var task *TaskInfo

		for i := range tasks {
			if tasks[i].Kind == TaskKindSnapshot && strings.HasPrefix(tasks[i].Description, src.String()+" at ") {
				task = &tasks[i]
				break
			}
		}

		if task == nil || task.Status != TaskSuccess {
			t.Fatalf("no successful snapshot task for %s in %+v", src, tasks)
		}

		if got, err := c.GetTask(ctx, task.ID); err != nil || got.Description != task.Description {
			t.Errorf("GetTask = %+v, %v", got, err)
		}

		logs, err := c.TaskLogs(ctx, task.ID)
		if err != nil {
			t.Errorf("TaskLogs: %v", err)
		}

		for _, e := range logs {
			if e.Time.IsZero() || e.Message == "" {
				t.Errorf("task log entry not decoded: %s", e.Raw)
				break
			}
		}

		snaps, err := c.ListSnapshots(ctx, src, true)
		if err != nil {
			t.Fatal(err)
		}

		if len(snaps.Snapshots) != 1 || snaps.Snapshots[0].Summary == nil || snaps.Snapshots[0].Summary.TotalFileCount != 1 ||
			!snaps.Snapshots[0].StartTime.Equal(st.LastSnapshot.StartTime) {
			t.Fatalf("snapshots = %+v", snaps)
		}

		// Editing replaces the manifest: the ID changes.
		desc := "kopia-docker-watcher integration test"

		edited, err := c.EditSnapshots(ctx, EditSnapshotsRequest{
			Snapshots: []string{snaps.Snapshots[0].ID}, Description: &desc, AddPins: []string{"keep"},
		})
		if err != nil {
			t.Fatal(err)
		}

		if len(edited) != 1 || edited[0].ID == snaps.Snapshots[0].ID || edited[0].Description != desc ||
			len(edited[0].Pins) != 1 || edited[0].Pins[0] != "keep" {
			t.Errorf("edited = %+v (old ID %s)", edited, snaps.Snapshots[0].ID)
		}
	})

	t.Run("unknown sources and tasks are NOT_FOUND", func(t *testing.T) {
		other := src
		other.Path = kopiatest.DataDir + "/nowhere"

		if err := c.StartSnapshot(ctx, other); !errors.Is(err, ErrNotFound) {
			t.Errorf("sources/upload: err = %v, want ErrNotFound", err)
		}

		if err := c.DeleteSnapshots(ctx, DeleteSnapshotsRequest{Source: other, DeleteSourceAndPolicy: true}); !errors.Is(err, ErrNotFound) {
			t.Errorf("snapshots/delete: err = %v, want ErrNotFound", err)
		}

		if _, err := c.GetTask(ctx, "999999"); !errors.Is(err, ErrNotFound) {
			t.Errorf("tasks/{id}: err = %v, want ErrNotFound", err)
		}
	})

	t.Run("delete policy keeps the source", func(t *testing.T) {
		if err := c.DeletePolicy(ctx, src); err != nil {
			t.Fatal(err)
		}

		if _, err := c.GetPolicy(ctx, src); !errors.Is(err, ErrNotFound) {
			t.Errorf("GetPolicy after delete: err = %v", err)
		}

		resp, err := c.ListSources(ctx, src)
		if err != nil || len(resp.Sources) != 1 {
			t.Errorf("source with snapshots should stay: %+v, %v", resp, err)
		}
	})

	t.Run("delete source and policy", func(t *testing.T) {
		if err := c.DeleteSnapshots(ctx, DeleteSnapshotsRequest{Source: src, DeleteSourceAndPolicy: true}); err != nil {
			t.Fatal(err)
		}

		resp, err := c.ListSources(ctx, src)
		if err != nil || len(resp.Sources) != 0 {
			t.Errorf("sources after delete = %+v, %v", resp, err)
		}

		snaps, err := c.ListSnapshots(ctx, src, true)
		if err != nil || len(snaps.Snapshots) != 0 {
			t.Errorf("snapshots after delete = %+v, %v", snaps, err)
		}
	})

	// Last: disconnecting ends the repository for this server.
	t.Run("without a repository", func(t *testing.T) {
		if err := c.call(ctx, request{method: http.MethodPost, path: "repo/disconnect", body: struct{}{}}, nil); err != nil {
			t.Fatal(err)
		}

		st, err := c.RepoStatus(ctx)
		if err != nil || st.Connected {
			t.Fatalf("repo/status = %+v, %v; want 200 with connected=false", st, err)
		}

		_, err = c.ListSources(ctx, SourceInfo{})
		if !errors.Is(err, ErrNotConnected) || !IsRetryable(err) {
			t.Errorf("sources: err = %v, want retryable ErrNotConnected", err)
		}

		if _, err := c.ListTasks(ctx); err != nil {
			t.Errorf("tasks work without a repository: %v", err)
		}
	})
}
