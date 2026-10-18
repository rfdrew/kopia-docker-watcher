package kopiaclient

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestErrorClassification(t *testing.T) {
	apiErr := func(status int, code, msg string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) { writeAPIError(w, status, code, msg) }
	}

	text := func(status int, body string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) { http.Error(w, body, status) }
	}

	tests := []struct {
		name      string
		h         http.HandlerFunc // nil: route not registered
		kind      error
		code      string
		retryable bool
	}{
		{"NOT_FOUND as 400 (policy get)", apiErr(400, CodeNotFound, "policy not found"), ErrNotFound, CodeNotFound, false},
		{"NOT_FOUND as 404 (sources/upload)", apiErr(404, CodeNotFound, "no source matching the provided filters"), ErrNotFound, CodeNotFound, false},
		{"404 without JSON: route missing", nil, ErrUIDisabled, "", false},
		{"NOT_CONNECTED", apiErr(400, CodeNotConnected, "not connected"), ErrNotConnected, CodeNotConnected, true},
		{"MALFORMED_REQUEST", apiErr(400, CodeMalformedRequest, "malformed request body"), ErrMalformedRequest, CodeMalformedRequest, false},
		{"PATH_NOT_FOUND", apiErr(400, CodePathNotFound, "path does not exist"), ErrPathNotFound, CodePathNotFound, false},
		{"ACCESS_DENIED", apiErr(403, CodeAccessDenied, "access is denied"), ErrAccessDenied, CodeAccessDenied, false},
		{
			"policy validation failure",
			apiErr(500, CodeInternal, "internal server error: unable to set policy: failed to validate policy: invalid scheduling policy: manual cannot be combined with other scheduling policies"),
			ErrInvalidPolicy, CodeInternal, false,
		},
		{
			"trailing slash in policy path",
			apiErr(500, CodeInternal, `internal server error: unable to set policy: invalid policy path: path "/data/" ends with a separator`),
			ErrInvalidPolicy, CodeInternal, false,
		},
		{
			"initial policy of POST /sources rejected",
			apiErr(500, CodeInternal, `internal server error: unable to set initial policy: failed to validate policy: invalid cron expression "61 * * * *"`),
			ErrInvalidPolicy, CodeInternal, false,
		},
		{
			"repository write failure",
			apiErr(500, CodeInternal, "internal server error: unable to set policy: error writing policy manifest: blob storage unavailable"),
			ErrInternal, CodeInternal, true,
		},
		{"STORAGE_CONNECTION", apiErr(400, CodeStorageConnection, "unable to connect to storage"), ErrUnavailable, CodeStorageConnection, true},
		{"unknown code", apiErr(400, CodeAlreadyConnected, "already connected"), nil, CodeAlreadyConnected, false},
		{"500 without JSON", text(500, "error reading request body"), ErrInternal, "", true},
		{"413 without JSON", text(413, "error reading request body"), nil, "", false},
		{"502 from a proxy", text(502, "<html>Bad Gateway</html>"), ErrUnavailable, "", true},
		{"503 from a proxy", text(503, "Service Unavailable"), ErrUnavailable, "", true},
		{"405 method not allowed", text(405, ""), nil, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeKopia()
			if tt.h != nil {
				f.handle("GET repo/status", tt.h)
			}

			_, err := newTestClient(t, f).RepoStatus(t.Context())

			var e *Error
			if !errors.As(err, &e) {
				t.Fatalf("err = %v (%T), want *Error", err, err)
			}

			if e.Unwrap() != tt.kind { //nolint:errorlint // comparing the exact kind
				t.Errorf("kind = %v, want %v", e.Unwrap(), tt.kind)
			}

			if tt.kind != nil && !errors.Is(err, tt.kind) {
				t.Errorf("errors.Is(err, %v) = false", tt.kind)
			}

			if e.Code != tt.code {
				t.Errorf("Code = %q, want %q", e.Code, tt.code)
			}

			if e.Op != "GET /api/v1/repo/status" {
				t.Errorf("Op = %q", e.Op)
			}

			if got := IsRetryable(err); got != tt.retryable {
				t.Errorf("IsRetryable = %v, want %v", got, tt.retryable)
			}
		})
	}
}

func TestErrorMessage(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET repo/status", func(w http.ResponseWriter, _ *http.Request) {
		writeAPIError(w, 400, CodeNotFound, "policy not found")
	})

	_, err := newTestClient(t, f).RepoStatus(t.Context())

	want := "kopia server: GET /api/v1/repo/status: 400 Bad Request (NOT_FOUND): not found: policy not found"
	if err == nil || err.Error() != want {
		t.Fatalf("err = %v\nwant  %s", err, want)
	}
}

func TestLongNonJSONBodyIsTruncated(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET repo/status", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, strings.Repeat("ж", 1000))
	})

	_, err := newTestClient(t, f).RepoStatus(t.Context())

	var e *Error
	if !errors.As(err, &e) {
		t.Fatal(err)
	}

	if len(e.Message) > maxMessageLen+len("…") || !strings.HasSuffix(e.Message, "…") {
		t.Errorf("message not truncated: %d bytes", len(e.Message))
	}

	if !strings.HasPrefix(e.Message, "жж") || strings.ContainsRune(e.Message, '�') {
		t.Errorf("truncation broke UTF-8: %q", e.Message[:10])
	}
}

func TestUndecodableSuccessBody(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET repo/status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "<html>not json</html>")
	})

	_, err := newTestClient(t, f).RepoStatus(t.Context())
	if !errors.Is(err, ErrUnexpectedResponse) {
		t.Fatalf("err = %v, want ErrUnexpectedResponse", err)
	}

	if IsRetryable(err) {
		t.Error("ErrUnexpectedResponse must not be retryable")
	}
}

func TestCertificateMismatchIsNotRetryable(t *testing.T) {
	err := &CertificateMismatchError{Want: "aa", Got: "bb"}
	if IsRetryable(err) {
		t.Error("certificate mismatch must not be retryable")
	}

	if !strings.Contains(err.Error(), "fingerprint is bb, expected aa") {
		t.Errorf("message = %q", err.Error())
	}
}
