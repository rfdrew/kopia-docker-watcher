// Package kopiaclient is a client for the HTTP API of kopia server.
//
// The client speaks to the UI API (/api/v1/...), the same one the kopia web
// interface uses, so it authenticates as the --server-username user and
// carries a CSRF token: GET / sets a session cookie and embeds
// <meta name="kopia-csrf-token">, whose value goes into X-Kopia-Csrf-Token.
// The token is an HMAC of the session ID with a key kopia server picks at
// random on every start, so after a restart the server answers 401 without
// WWW-Authenticate; the client then fetches a new token once and retries.
//
// Types are defined here and mirror the JSON of kopia releases; the client
// does not depend on the kopia module.
package kopiaclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

// CSRFTokenHeader carries the CSRF token on UI API requests.
const CSRFTokenHeader = "X-Kopia-Csrf-Token" //nolint:gosec // header name, not a credential

// DefaultTimeout bounds a single request. Policy writes make kopia server
// refresh the whole repository before answering, so it is generous.
const DefaultTimeout = 2 * time.Minute

// maxResponseSize bounds a response body read into memory.
const maxResponseSize = 32 << 20

// Options configure a Client.
type Options struct {
	// URL is the base URL of kopia server, e.g. https://kopia:51515. A path
	// prefix is kept, for a server behind a reverse proxy.
	URL string

	// Username and Password are sent with Basic auth on every request. Leave
	// both empty for a server started with --without-password.
	Username string
	Password string

	// At most one of the TLS options may be set.

	// CertFingerprint pins the server's leaf certificate by its SHA-256, in
	// hex as kopia server prints it after "SERVER CERT SHA256:".
	CertFingerprint string
	// CAFile is a PEM file with the CA certificates that verify the server
	// instead of the system roots.
	CAFile string
	// InsecureSkipVerify disables certificate verification.
	InsecureSkipVerify bool

	// Timeout bounds every request; DefaultTimeout if zero.
	Timeout time.Duration
	// UserAgent is sent with every request.
	UserAgent string
	// Logger receives debug logs of requests; nothing is logged if nil.
	Logger *slog.Logger
}

// Client talks to one kopia server. It is safe for concurrent use.
type Client struct {
	base      *url.URL
	username  string
	password  string
	userAgent string
	http      *http.Client
	log       *slog.Logger

	csrfMu    sync.Mutex
	csrfToken string
}

// New creates a client. It does not contact the server.
func New(opts Options) (*Client, error) {
	tlsCfg, err := tlsConfig(opts)
	if err != nil {
		return nil, err
	}

	tr, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("http.DefaultTransport is not an *http.Transport")
	}

	tr = tr.Clone()
	tr.TLSClientConfig = tlsCfg

	return newClient(opts, tr)
}

// newClient creates a client over the given transport; tests pass a fake one.
func newClient(opts Options, rt http.RoundTripper) (*Client, error) {
	base, err := url.Parse(opts.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid kopia server URL: %w", err)
	}

	if (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, fmt.Errorf("invalid kopia server URL %q: need http(s)://host[:port]", base.Redacted())
	}

	base.Path = strings.TrimRight(base.Path, "/")
	base.RawPath = ""

	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("create cookie jar: %w", err)
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}

	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	return &Client{
		base:      base,
		username:  opts.Username,
		password:  opts.Password,
		userAgent: opts.UserAgent,
		http:      &http.Client{Transport: rt, Jar: jar, Timeout: timeout},
		log:       log,
	}, nil
}

// request describes one UI API call.
type request struct {
	method string
	path   string // below /api/v1/, e.g. "repo/status"
	query  url.Values
	body   any // marshaled to JSON unless nil
}

// call performs an API request and decodes a successful response into out,
// unless out is nil. A 401 without WWW-Authenticate means the CSRF token is
// no longer valid; the token is fetched again once and the request repeated.
func (c *Client) call(ctx context.Context, r request, out any) error {
	op := r.method + " " + c.apiPath(r.path)

	var body []byte

	if r.body != nil {
		b, err := json.Marshal(r.body)
		if err != nil {
			return fmt.Errorf("kopia server: %s: encode request: %w", op, err)
		}

		body = b
	}

	token, err := c.csrf(ctx, "")
	if err != nil {
		return err
	}

	resp, err := c.send(ctx, r, body, token)
	if err != nil {
		return fmt.Errorf("kopia server: %s: %w", op, err)
	}

	if resp.isCSRFRejection() {
		c.log.DebugContext(ctx, "kopia server rejected the CSRF token, fetching a new one", "op", op)

		if token, err = c.csrf(ctx, token); err != nil {
			return err
		}

		if resp, err = c.send(ctx, r, body, token); err != nil {
			return fmt.Errorf("kopia server: %s: %w", op, err)
		}
	}

	if resp.status < 200 || resp.status > 299 {
		return classify(op, resp)
	}

	if out == nil {
		return nil
	}

	if err := json.Unmarshal(resp.body, out); err != nil {
		return &Error{Op: op, StatusCode: resp.status, Message: "decode response: " + err.Error(), kind: ErrUnexpectedResponse}
	}

	return nil
}

// response is a fully read HTTP response.
type response struct {
	status int
	header http.Header
	body   []byte
}

// isCSRFRejection: kopia answers a bad or missing CSRF token with 401 and,
// unlike for bad credentials, no WWW-Authenticate.
func (r *response) isCSRFRejection() bool {
	return r.status == http.StatusUnauthorized && r.header.Get("WWW-Authenticate") == ""
}

func (c *Client) apiPath(p string) string {
	return c.base.Path + "/api/v1/" + p
}

// send performs one HTTP request and reads the whole response body.
func (c *Client) send(ctx context.Context, r request, body []byte, token string) (*response, error) {
	u := *c.base
	u.Path = c.apiPath(r.path)
	u.RawQuery = r.query.Encode()

	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, r.method, u.String(), rd)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set(CSRFTokenHeader, token)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return c.do(req)
}

func (c *Client) do(req *http.Request) (*response, error) {
	if c.username != "" || c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}

	start := time.Now()

	resp, err := c.http.Do(req)
	if err != nil {
		c.log.DebugContext(req.Context(), "kopia request failed", "method", req.Method, "path", req.URL.Path, "error", err)
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize+1))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if len(body) > maxResponseSize {
		return nil, fmt.Errorf("response larger than %d bytes", maxResponseSize)
	}

	c.log.DebugContext(req.Context(), "kopia request", "method", req.Method, "path", req.URL.Path,
		"status", resp.StatusCode, "duration", time.Since(start))

	return &response{status: resp.StatusCode, header: resp.Header, body: body}, nil
}

// csrf returns the current CSRF token. If there is none yet, or the current
// one equals stale, which the server just rejected, a new one is fetched.
// Concurrent callers that saw the same rejection share one fetch.
func (c *Client) csrf(ctx context.Context, stale string) (string, error) {
	c.csrfMu.Lock()
	defer c.csrfMu.Unlock()

	if c.csrfToken != "" && c.csrfToken != stale {
		return c.csrfToken, nil
	}

	token, err := c.fetchCSRF(ctx)
	if err != nil {
		c.csrfToken = ""
		return "", err
	}

	c.csrfToken = token

	return token, nil
}

var csrfMetaRE = regexp.MustCompile(`<meta\s+name="kopia-csrf-token"\s+content="([^"]+)"`)

// fetchCSRF loads the UI page. Kopia sets the session cookie, kept in the
// cookie jar, and embeds the token for that session in a meta tag.
func (c *Client) fetchCSRF(ctx context.Context) (string, error) {
	u := *c.base
	u.Path = c.base.Path + "/"
	op := "GET " + u.Path

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return "", fmt.Errorf("kopia server: %s: build request: %w", op, err)
	}

	req.Header.Set("Accept", "text/html")

	resp, err := c.do(req)
	if err != nil {
		return "", fmt.Errorf("kopia server: %s: %w", op, err)
	}

	if resp.status != http.StatusOK {
		return "", classify(op, resp)
	}

	m := csrfMetaRE.FindSubmatch(resp.body)
	if m == nil {
		return "", &Error{
			Op: op, StatusCode: resp.status, kind: ErrCSRF,
			Message: "the page has no kopia-csrf-token meta tag; does KOPIA_SERVER_URL point at kopia server?",
		}
	}

	return string(m[1]), nil
}
