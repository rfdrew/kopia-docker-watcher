package kopiaclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"
)

// Error kinds. Every *Error unwraps to at most one of them, so callers use
// errors.Is(err, kopiaclient.ErrNotFound) and friends.
var (
	// ErrUnauthorized: 401 with WWW-Authenticate, wrong or missing credentials.
	ErrUnauthorized = errors.New("kopia server rejected the credentials")
	// ErrCSRF: 401 without WWW-Authenticate even after fetching a fresh
	// token, or no token on the UI page.
	ErrCSRF = errors.New("kopia server rejected the CSRF token")
	// ErrAccessDenied: 403. The UI API only admits the --server-username user.
	ErrAccessDenied = errors.New("access denied: only the kopia server --server-username user may use the UI API")
	// ErrUIDisabled: 404 without a JSON body. The UI page and the whole UI
	// API are only registered when kopia server runs with --ui (the default).
	ErrUIDisabled = errors.New("kopia server UI API is not available: is kopia server running with --ui=false, or is KOPIA_SERVER_URL wrong?")

	ErrNotFound         = errors.New("not found")                // code NOT_FOUND, with status 400 or 404
	ErrNotConnected     = errors.New("repository not connected") // code NOT_CONNECTED
	ErrMalformedRequest = errors.New("malformed request")        // code MALFORMED_REQUEST
	ErrPathNotFound     = errors.New("path not found")           // code PATH_NOT_FOUND

	// ErrInvalidPolicy: 500 INTERNAL from a policy write rejected by
	// validation (bad cron, manual combined with a schedule, trailing slash
	// in the path). A configuration error: retrying will not help.
	ErrInvalidPolicy = errors.New("invalid policy")
	// ErrInternal: any other 500, including a failed repository write.
	ErrInternal = errors.New("kopia server internal error")
	// ErrUnavailable: 408, 429, 502, 503, 504 or STORAGE_CONNECTION.
	ErrUnavailable = errors.New("kopia server unavailable")
	// ErrUnexpectedResponse: a successful status with a body that does not
	// decode, or a null where an object is required.
	ErrUnexpectedResponse = errors.New("unexpected response from kopia server")
)

// API error codes, from kopia's internal/serverapi.
const (
	CodeInternal           = "INTERNAL"
	CodeAlreadyConnected   = "ALREADY_CONNECTED"
	CodeAlreadyInitialized = "ALREADY_INITIALIZED"
	CodeInvalidPassword    = "INVALID_PASSWORD"
	CodeInvalidToken       = "INVALID_TOKEN"
	CodeMalformedRequest   = "MALFORMED_REQUEST"
	CodeNotConnected       = "NOT_CONNECTED"
	CodeNotFound           = "NOT_FOUND"
	CodeNotInitialized     = "NOT_INITIALIZED"
	CodePathNotFound       = "PATH_NOT_FOUND"
	CodeStorageConnection  = "STORAGE_CONNECTION"
	CodeAccessDenied       = "ACCESS_DENIED"
)

// Messages kopia wraps policy validation failures in
// (snapshot/policy/policy_manager.go, SetPolicy). A repository write failure
// has the same code INTERNAL and status 500 but a different text.
var invalidPolicyMessages = []string{
	"failed to validate policy",
	"invalid policy path",
}

// Error is a failed kopia server request.
type Error struct {
	// Op is the request, e.g. "PUT /api/v1/policy".
	Op string
	// StatusCode is the HTTP status.
	StatusCode int
	// Code is the API error code from the JSON body, empty if the body was
	// not a JSON error.
	Code string
	// Message is the "error" field of the JSON body, or the start of a
	// non-JSON body.
	Message string

	kind error
}

func (e *Error) Error() string {
	var b strings.Builder

	b.WriteString("kopia server: ")
	b.WriteString(e.Op)

	if e.StatusCode != 0 {
		fmt.Fprintf(&b, ": %d %s", e.StatusCode, http.StatusText(e.StatusCode))
	}

	if e.Code != "" {
		b.WriteString(" (")
		b.WriteString(e.Code)
		b.WriteString(")")
	}

	if e.kind != nil {
		b.WriteString(": ")
		b.WriteString(e.kind.Error())
	}

	if e.Message != "" {
		b.WriteString(": ")
		b.WriteString(e.Message)
	}

	return b.String()
}

// Unwrap returns the error kind (ErrNotFound, ...) or nil.
func (e *Error) Unwrap() error { return e.kind }

// CertificateMismatchError is returned when the server certificate does not
// match KOPIA_SERVER_CERT_FINGERPRINT.
type CertificateMismatchError struct {
	Want, Got string // lowercase hex SHA-256
}

func (e *CertificateMismatchError) Error() string {
	return fmt.Sprintf("kopia server certificate SHA-256 fingerprint is %s, expected %s", e.Got, e.Want)
}

// IsRetryable reports whether a failed call may succeed later without a
// configuration change: network errors, timeouts, NOT_CONNECTED, 5xx other
// than a rejected policy. Credentials, CSRF, access, TLS verification and
// request errors are not retryable. A canceled context is not retryable
// either: the caller is shutting down.
func IsRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}

	var apiErr *Error
	if errors.As(err, &apiErr) {
		return errors.Is(apiErr.kind, ErrNotConnected) || errors.Is(apiErr.kind, ErrInternal) ||
			errors.Is(apiErr.kind, ErrUnavailable)
	}

	var (
		mismatch    *CertificateMismatchError
		certVerify  *tls.CertificateVerificationError
		unknownAuth x509.UnknownAuthorityError
		hostname    x509.HostnameError
		invalid     x509.CertificateInvalidError
	)

	if errors.As(err, &mismatch) || errors.As(err, &certVerify) || errors.As(err, &unknownAuth) ||
		errors.As(err, &hostname) || errors.As(err, &invalid) {
		return false
	}

	// Anything else is a transport problem: connection refused, reset,
	// DNS, timeout.
	return true
}

type errorBody struct {
	Code  string `json:"code"`
	Error string `json:"error"`
}

// maxMessageLen limits how much of a non-JSON body ends up in an error.
const maxMessageLen = 256

// classify turns a non-2xx response into an *Error. Kopia sends JSON error
// bodies with Content-Type text/plain, so the body is parsed regardless of
// the header.
func classify(op string, resp *response) *Error {
	e := &Error{Op: op, StatusCode: resp.status}

	var eb errorBody
	if err := json.Unmarshal(resp.body, &eb); err == nil && eb.Code != "" {
		e.Code = eb.Code
		e.Message = eb.Error
	} else {
		e.Message = truncate(strings.TrimSpace(string(resp.body)), maxMessageLen)
	}

	switch {
	case resp.status == http.StatusUnauthorized && !resp.isCSRFRejection():
		e.kind = ErrUnauthorized
	case resp.status == http.StatusUnauthorized:
		e.kind = ErrCSRF
	case resp.status == http.StatusForbidden || e.Code == CodeAccessDenied:
		e.kind = ErrAccessDenied
	case e.Code != "":
		e.kind = kindForCode(e.Code, e.Message)
	default:
		e.kind = kindForStatus(resp.status)
	}

	return e
}

func kindForCode(code, message string) error {
	switch code {
	case CodeNotFound:
		return ErrNotFound
	case CodeNotConnected:
		return ErrNotConnected
	case CodeMalformedRequest:
		return ErrMalformedRequest
	case CodePathNotFound:
		return ErrPathNotFound
	case CodeStorageConnection:
		return ErrUnavailable
	case CodeInternal:
		for _, m := range invalidPolicyMessages {
			if strings.Contains(message, m) {
				return ErrInvalidPolicy
			}
		}

		return ErrInternal
	default:
		return nil
	}
}

func kindForStatus(status int) error {
	switch {
	case status == http.StatusNotFound:
		return ErrUIDisabled
	case status == http.StatusRequestTimeout, status == http.StatusTooManyRequests,
		status == http.StatusBadGateway, status == http.StatusServiceUnavailable, status == http.StatusGatewayTimeout:
		return ErrUnavailable
	case status >= http.StatusInternalServerError:
		return ErrInternal
	default:
		return nil
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}

	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}

	return s[:n] + "…"
}
