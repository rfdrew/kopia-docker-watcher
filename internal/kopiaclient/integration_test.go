//go:build integration

package kopiaclient

import (
	"bytes"
	"context"
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
