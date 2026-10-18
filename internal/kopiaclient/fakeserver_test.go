package kopiaclient

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeKopia mimics the authentication, CSRF and error behavior of kopia
// server (internal/server/server.go, server_authz_checks.go) closely enough
// to exercise the client without a network.
type fakeKopia struct {
	mu sync.Mutex

	uiUser     string
	users      map[string]string // username -> password; nil means --without-password
	signingKey string
	uiDisabled bool
	noMetaTag  bool
	// rejectCSRF makes every CSRF check fail, as with a proxy that drops
	// the session cookie.
	rejectCSRF bool

	routes map[string]http.HandlerFunc // "GET repo/status"

	pageLoads int
	apiCalls  []*recordedCall
}

type recordedCall struct {
	Method, Path, Query string
	Header              http.Header
	Body                []byte
}

func newFakeKopia() *fakeKopia {
	return &fakeKopia{
		uiUser:     "admin",
		users:      map[string]string{"admin": "secret", "server-control": "control-secret"},
		signingKey: randomHex(),
		routes:     map[string]http.HandlerFunc{},
	}
}

// restart changes the signing key like a kopia server restart does, which
// invalidates every issued CSRF token.
func (f *fakeKopia) restart() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.signingKey = randomHex()
}

func (f *fakeKopia) handle(route string, h http.HandlerFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.routes[route] = h
}

func (f *fakeKopia) set(fn func(f *fakeKopia)) {
	f.mu.Lock()
	defer f.mu.Unlock()

	fn(f)
}

func (f *fakeKopia) counts() (pageLoads, apiCalls int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.pageLoads, len(f.apiCalls)
}

func (f *fakeKopia) lastCall(t *testing.T) *recordedCall {
	t.Helper()

	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.apiCalls) == 0 {
		t.Fatal("no API calls recorded")
	}

	return f.apiCalls[len(f.apiCalls)-1]
}

func (f *fakeKopia) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	uiDisabled := f.uiDisabled
	f.mu.Unlock()

	// With --no-ui neither the page nor the UI API is registered.
	if uiDisabled {
		http.NotFound(w, r)
		return
	}

	if !strings.HasPrefix(r.URL.Path, "/api/v1/") {
		f.servePage(w, r)
		return
	}

	f.serveAPI(w, r)
}

func (f *fakeKopia) servePage(w http.ResponseWriter, r *http.Request) {
	user, ok := f.authenticate(w, r)
	if !ok {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.users != nil && user != f.uiUser {
		http.Error(w, "UI Access denied. See https://github.com/kopia/kopia/issues/880#issuecomment-798421751 for more information.", http.StatusForbidden)
		return
	}

	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	f.pageLoads++

	sessionID := ""
	if c, err := r.Cookie("Kopia-Session-Cookie"); err == nil {
		sessionID = c.Value
	} else {
		sessionID = randomHex()
		http.SetCookie(w, &http.Cookie{Name: "Kopia-Session-Cookie", Value: sessionID, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
	}

	page := `<!doctype html><html><head><title>KopiaUI</title></head><body></body></html>`
	if !f.noMetaTag {
		page = strings.Replace(page, "</head>", `<meta name="kopia-csrf-token" content="`+f.tokenLocked(sessionID)+`" /></head>`, 1)
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, page)
}

func (f *fakeKopia) serveAPI(w http.ResponseWriter, r *http.Request) {
	user, ok := f.authenticate(w, r)
	if !ok {
		return
	}

	body, _ := io.ReadAll(r.Body)

	f.mu.Lock()
	f.apiCalls = append(f.apiCalls, &recordedCall{
		Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Header: r.Header.Clone(), Body: body,
	})

	csrfOK := !f.rejectCSRF && f.validCSRFLocked(r)
	route := f.routes[r.Method+" "+strings.TrimPrefix(r.URL.Path, "/api/v1/")]
	isUIUser := f.users == nil || user == f.uiUser
	f.mu.Unlock()

	if !csrfOK {
		http.Error(w, "Invalid or missing CSRF token.\n", http.StatusUnauthorized)
		return
	}

	if !isUIUser {
		writeAPIError(w, http.StatusForbidden, CodeAccessDenied, "access is denied")
		return
	}

	if route == nil {
		http.NotFound(w, r) // gorilla/mux answers unknown routes with plain text
		return
	}

	route(w, r)
}

// authenticate mirrors Server.isAuthenticated: a 401 for missing or wrong
// credentials always carries WWW-Authenticate.
func (f *fakeKopia) authenticate(w http.ResponseWriter, r *http.Request) (string, bool) {
	f.mu.Lock()
	users := f.users
	f.mu.Unlock()

	if users == nil {
		return "", true
	}

	user, pass, ok := r.BasicAuth()
	if !ok {
		w.Header().Set("WWW-Authenticate", `Basic realm="Kopia"`)
		http.Error(w, "Missing credentials.\n", http.StatusUnauthorized)

		return "", false
	}

	if want, known := users[user]; !known || want != pass {
		w.Header().Set("WWW-Authenticate", `Basic realm="Kopia"`)
		http.Error(w, "Access denied.\n", http.StatusUnauthorized)

		return "", false
	}

	return user, true
}

func (f *fakeKopia) tokenLocked(sessionID string) string {
	h := hmac.New(sha256.New, []byte(f.signingKey))
	_, _ = io.WriteString(h, sessionID)

	return hex.EncodeToString(h.Sum(nil))
}

func (f *fakeKopia) validCSRFLocked(r *http.Request) bool {
	c, err := r.Cookie("Kopia-Session-Cookie")
	if err != nil {
		return false
	}

	token := r.Header.Get(CSRFTokenHeader)

	return token != "" && hmac.Equal([]byte(token), []byte(f.tokenLocked(c.Value)))
}

// writeAPIError writes an error the way kopia does: a JSON body labeled
// text/plain.
func writeAPIError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "error": msg})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func randomHex() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

// handlerTransport serves requests with an http.Handler in-process.
type handlerTransport struct{ h http.Handler }

func (t handlerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := req.Context().Err(); err != nil {
		return nil, err
	}

	r := req.Clone(req.Context())
	if r.Body == nil {
		r.Body = http.NoBody
	}

	rec := httptest.NewRecorder()
	t.h.ServeHTTP(rec, r)

	resp := rec.Result()
	resp.Request = req

	return resp, nil
}

// errTransport fails every request.
type errTransport struct{ err error }

func (t errTransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, t.err }

// newTestClient returns a client wired to f with the UI user's credentials.
func newTestClient(t *testing.T, f http.Handler, mod ...func(*Options)) *Client {
	t.Helper()

	opts := Options{URL: "http://kopia:51515", Username: "admin", Password: "secret", UserAgent: "test-agent"}
	for _, m := range mod {
		m(&opts)
	}

	c, err := newClient(opts, handlerTransport{f})
	if err != nil {
		t.Fatal(err)
	}

	return c
}

// statusRoute answers GET repo/status with a connected repository.
func statusRoute(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		"connected": true, "hostname": "nas", "username": "root", "storage": "filesystem",
		"supportsContentCompression": true,
	})
}
