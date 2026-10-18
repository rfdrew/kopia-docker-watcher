package kopiaclient

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"testing"
)

func TestRepoStatusFetchesCSRFTokenOnce(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET repo/status", statusRoute)
	c := newTestClient(t, f)

	for range 3 {
		st, err := c.RepoStatus(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		if !st.Connected || st.Hostname != "nas" || st.Username != "root" || st.Storage != "filesystem" {
			t.Fatalf("status = %+v", st)
		}
	}

	if pages, calls := f.counts(); pages != 1 || calls != 3 {
		t.Fatalf("page loads = %d, API calls = %d; want 1 and 3", pages, calls)
	}

	call := f.lastCall(t)
	if call.Method != http.MethodGet || call.Path != "/api/v1/repo/status" {
		t.Errorf("call = %s %s", call.Method, call.Path)
	}

	if user, pass, ok := (&http.Request{Header: call.Header}).BasicAuth(); !ok || user != "admin" || pass != "secret" {
		t.Errorf("basic auth = %q %q %v", user, pass, ok)
	}

	if call.Header.Get(CSRFTokenHeader) == "" {
		t.Error("no CSRF token header")
	}

	if got := call.Header.Get("User-Agent"); got != "test-agent" {
		t.Errorf("User-Agent = %q", got)
	}
}

func TestCSRFTokenRefetchedAfterServerRestart(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET repo/status", statusRoute)
	c := newTestClient(t, f)

	if _, err := c.RepoStatus(t.Context()); err != nil {
		t.Fatal(err)
	}

	f.restart()

	if _, err := c.RepoStatus(t.Context()); err != nil {
		t.Fatalf("after restart: %v", err)
	}

	// initial page + one refetch; API: first call, rejected call, retry.
	if pages, calls := f.counts(); pages != 2 || calls != 3 {
		t.Fatalf("page loads = %d, API calls = %d; want 2 and 3", pages, calls)
	}
}

func TestCSRFRejectedTwiceIsError(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET repo/status", statusRoute)
	f.set(func(f *fakeKopia) { f.rejectCSRF = true })
	c := newTestClient(t, f)

	_, err := c.RepoStatus(t.Context())
	if !errors.Is(err, ErrCSRF) {
		t.Fatalf("err = %v, want ErrCSRF", err)
	}

	if IsRetryable(err) {
		t.Error("CSRF rejection must not be retryable")
	}

	if pages, calls := f.counts(); pages != 2 || calls != 2 {
		t.Fatalf("page loads = %d, API calls = %d; want 2 and 2 (one retry only)", pages, calls)
	}

	// The next call starts over with one more refetch, not a loop.
	if _, err := c.RepoStatus(t.Context()); !errors.Is(err, ErrCSRF) {
		t.Fatalf("second call err = %v", err)
	}

	if pages, calls := f.counts(); pages != 3 || calls != 4 {
		t.Fatalf("page loads = %d, API calls = %d; want 3 and 4", pages, calls)
	}
}

func TestWrongPasswordIsUnauthorizedWithoutRetry(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET repo/status", statusRoute)
	c := newTestClient(t, f, func(o *Options) { o.Password = "wrong" })

	_, err := c.RepoStatus(t.Context())
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}

	if IsRetryable(err) {
		t.Error("ErrUnauthorized must not be retryable")
	}

	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnauthorized || apiErr.Op != "GET /" {
		t.Fatalf("err = %#v", err)
	}

	if pages, calls := f.counts(); pages != 0 || calls != 0 {
		t.Fatalf("page loads = %d, API calls = %d; want no successful page load and no API call", pages, calls)
	}
}

func TestUnauthorizedOnAPIIsNotTreatedAsCSRF(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET repo/status", statusRoute)
	c := newTestClient(t, f)

	if _, err := c.RepoStatus(t.Context()); err != nil {
		t.Fatal(err)
	}

	// The password changes on the server: 401 with WWW-Authenticate.
	f.set(func(f *fakeKopia) { f.users["admin"] = "rotated" })

	if _, err := c.RepoStatus(t.Context()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}

	if pages, calls := f.counts(); pages != 1 {
		t.Fatalf("page loads = %d (API calls %d); a 401 with WWW-Authenticate must not refetch the token", pages, calls)
	}
}

func TestMissingCredentialsIsUnauthorized(t *testing.T) {
	f := newFakeKopia()
	c := newTestClient(t, f, func(o *Options) { o.Username, o.Password = "", "" })

	if _, err := c.RepoStatus(t.Context()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
}

func TestNonUIUserIsAccessDenied(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET repo/status", statusRoute)
	c := newTestClient(t, f, func(o *Options) { o.Username, o.Password = "server-control", "control-secret" })

	_, err := c.RepoStatus(t.Context())
	if !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("err = %v, want ErrAccessDenied", err)
	}

	if IsRetryable(err) {
		t.Error("ErrAccessDenied must not be retryable")
	}

	if !strings.Contains(err.Error(), "--server-username") {
		t.Errorf("error does not explain the cause: %v", err)
	}
}

func TestUIDisabled(t *testing.T) {
	f := newFakeKopia()
	f.set(func(f *fakeKopia) { f.uiDisabled = true })
	c := newTestClient(t, f)

	_, err := c.RepoStatus(t.Context())
	if !errors.Is(err, ErrUIDisabled) {
		t.Fatalf("err = %v, want ErrUIDisabled", err)
	}

	if !strings.Contains(err.Error(), "--no-ui") {
		t.Errorf("error does not mention --no-ui: %v", err)
	}
}

func TestPageWithoutCSRFToken(t *testing.T) {
	f := newFakeKopia()
	f.set(func(f *fakeKopia) { f.noMetaTag = true })
	c := newTestClient(t, f)

	_, err := c.RepoStatus(t.Context())
	if !errors.Is(err, ErrCSRF) || !strings.Contains(err.Error(), "KOPIA_SERVER_URL") {
		t.Fatalf("err = %v, want ErrCSRF with a hint", err)
	}
}

func TestWithoutPasswordSendsNoAuthorization(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET repo/status", statusRoute)
	f.set(func(f *fakeKopia) { f.users = nil })
	c := newTestClient(t, f, func(o *Options) { o.Username, o.Password = "", "" })

	if _, err := c.RepoStatus(t.Context()); err != nil {
		t.Fatal(err)
	}

	if h := f.lastCall(t).Header.Get("Authorization"); h != "" {
		t.Errorf("Authorization = %q, want none", h)
	}
}

func TestBaseURLPathPrefix(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET repo/status", statusRoute)
	c := newTestClient(t, http.StripPrefix("/kopia", f), func(o *Options) { o.URL = "https://proxy.example.com/kopia/" })

	if _, err := c.RepoStatus(t.Context()); err != nil {
		t.Fatal(err)
	}

	if pages, _ := f.counts(); pages != 1 {
		t.Fatalf("page loads = %d", pages)
	}
}

func TestConcurrentCallsShareTokenRefetch(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET repo/status", statusRoute)
	c := newTestClient(t, f)

	if _, err := c.RepoStatus(t.Context()); err != nil {
		t.Fatal(err)
	}

	f.restart()

	var wg sync.WaitGroup

	errs := make(chan error, 20)

	for range 20 {
		wg.Go(func() {
			_, err := c.RepoStatus(t.Context())
			errs <- err
		})
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	if pages, _ := f.counts(); pages != 2 {
		t.Fatalf("page loads = %d, want 2: concurrent rejections must share one refetch", pages)
	}
}

func TestTransportErrorIsRetryable(t *testing.T) {
	c, err := newClient(Options{URL: "http://kopia:51515"}, errTransport{syscall.ECONNREFUSED})
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.RepoStatus(t.Context())
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("err = %v", err)
	}

	if !IsRetryable(err) {
		t.Error("connection refused must be retryable")
	}
}

func TestCanceledContextIsNotRetryable(t *testing.T) {
	f := newFakeKopia()
	c := newTestClient(t, f)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := c.RepoStatus(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}

	if IsRetryable(err) {
		t.Error("canceled context must not be retryable")
	}
}

func TestNewWithTransport(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET repo/status", statusRoute)

	c, err := New(Options{URL: "http://kopia:51515", Username: "admin", Password: "secret", Transport: handlerTransport{f}})
	if err != nil {
		t.Fatal(err)
	}

	st, err := c.RepoStatus(t.Context())
	if err != nil || !st.Connected {
		t.Fatalf("RepoStatus = %+v, %v", st, err)
	}
}

func TestNewRejectsBadOptions(t *testing.T) {
	for _, opts := range []Options{
		{URL: "kopia:51515"},
		{URL: "ftp://kopia"},
		{URL: "https://"},
		{URL: "https://kopia", CertFingerprint: "abcd"},
		{URL: "https://kopia", CertFingerprint: strings.Repeat("a", 64), InsecureSkipVerify: true},
		{URL: "https://kopia", CAFile: "/nonexistent/ca.pem"},
		{URL: "https://kopia", InsecureSkipVerify: true, Transport: errTransport{syscall.ECONNREFUSED}},
	} {
		if _, err := New(opts); err == nil {
			t.Errorf("New(%+v): expected error", opts)
		}
	}
}
