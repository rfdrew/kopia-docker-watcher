//go:build integration

// Package kopiatest starts kopia server in Docker for integration tests.
//
// The server uses a filesystem repository inside the container, Basic auth
// and CSRF checks as in production, and a TLS certificate generated on first
// start and kept for restarts. The image is DefaultImage unless KOPIA_IMAGE
// names another one, e.g. to try a new kopia release before switching to it.
package kopiatest

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

// DefaultImage is the latest kopia release, the only version the watcher
// supports. It moves to each new release.
const DefaultImage = "kopia/kopia:0.23.1"

// Credentials and identity of the test server.
const (
	UIUser          = "admin"
	UIPassword      = "secret-ui-password"
	ControlUser     = "server-control"
	ControlPassword = "secret-control-password"

	// Hostname and Username are the client options of the repository:
	// kopia server registers its sources as Username@Hostname.
	Hostname = "kdw-it"
	Username = "watcher"

	// DataDir exists in the container and holds HelloDir with one file.
	DataDir  = "/data"
	HelloDir = DataDir + "/hello"

	port = "51515/tcp"

	// serverStarted is printed by kopia server once it listens.
	serverStarted = "SERVER ADDRESS:"
)

// Options select the server variant.
type Options struct {
	// DisableUI starts kopia server with --no-ui and plain HTTP.
	DisableUI bool
}

// Server is a running kopia server container.
type Server struct {
	// URL is the base URL reachable from the test, https:// unless the UI
	// is disabled.
	URL string
	// CertFingerprint is the SHA-256 of the server certificate in hex.
	CertFingerprint string
	// Image is the kopia image the server runs.
	Image string

	ctr *testcontainers.DockerContainer
}

// Image returns the kopia image under test.
func Image() string {
	if v := os.Getenv("KOPIA_IMAGE"); v != "" {
		return v
	}

	return DefaultImage
}

// The entrypoint creates the repository and the test data once, then runs
// kopia server in a loop, so Restart can stop the process through the
// control API without changing the container's published port. A server
// that keeps failing to start ends the container, so the wait for it fails
// at once instead of timing out.
const script = `set -e
mkdir -p ` + HelloDir + `
echo hello > ` + HelloDir + `/hello.txt
if [ ! -f /app/config/repository.config ]; then
  kopia repository create filesystem --path=/repository \
    --override-hostname=` + Hostname + ` --override-username=` + Username + `
fi
failures=0
while true; do
  if [ "$KDW_DISABLE_UI" = "1" ]; then
    mode="--insecure --no-ui"
  else
    mode="--tls-cert-file=/app/config/server.crt --tls-key-file=/app/config/server.key"
    if [ ! -f /app/config/server.crt ]; then
      mode="$mode --tls-generate-cert --tls-generate-rsa-key-size=2048"
    fi
  fi
  if kopia server start --address=0.0.0.0:51515 $mode \
      --server-username=` + UIUser + ` --server-password="$KDW_UI_PASSWORD" \
      --server-control-password="$KDW_CONTROL_PASSWORD"; then
    failures=0
  else
    failures=$((failures + 1))
    if [ "$failures" -ge 3 ]; then
      echo "kopia server failed to start $failures times in a row" >&2
      exit 1
    fi
  fi
  sleep 0.2
done
`

var fingerprintRE = regexp.MustCompile(`SERVER CERT SHA256: ([0-9a-f]{64})`)

// Start runs kopia server and stops it when the test ends.
func Start(t testing.TB, opts Options) *Server {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	disableUI := "0"
	if opts.DisableUI {
		disableUI = "1"
	}

	img := Image()

	ctr, err := testcontainers.Run(ctx, img,
		testcontainers.WithEntrypoint("/bin/sh", "-c", script),
		testcontainers.WithEnv(map[string]string{
			"KOPIA_PASSWORD":       "secret-repository-password",
			"KDW_UI_PASSWORD":      UIPassword,
			"KDW_CONTROL_PASSWORD": ControlPassword,
			"KDW_DISABLE_UI":       disableUI,
		}),
		testcontainers.WithExposedPorts(port),
		testcontainers.WithWaitStrategy(wait.ForLog(serverStarted).WithStartupTimeout(3*time.Minute)),
	)
	testcontainers.CleanupContainer(t, ctr)

	if err != nil {
		t.Fatalf("start %s: %v", img, err)
	}

	s := &Server{Image: img, ctr: ctr}

	scheme := "https"
	if opts.DisableUI {
		scheme = "http"
	}

	if s.URL, err = ctr.PortEndpoint(ctx, port, scheme); err != nil {
		t.Fatalf("server endpoint: %v", err)
	}

	if !opts.DisableUI {
		m := fingerprintRE.FindStringSubmatch(s.logs(ctx, t))
		if m == nil {
			t.Fatalf("no certificate fingerprint in the kopia server log:\n%s", s.logs(ctx, t))
		}

		s.CertFingerprint = m[1]
	}

	t.Logf("kopia server %s at %s", img, s.URL)

	return s
}

// Exec runs a command in the container and returns its output; a non-zero
// exit fails the test.
func (s *Server) Exec(t testing.TB, cmd ...string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	code, r, err := s.ctr.Exec(ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("exec %q: %v", cmd, err)
	}

	out, _ := io.ReadAll(r)
	if code != 0 {
		t.Fatalf("exec %q: exit code %d: %s", cmd, code, out)
	}

	return string(out)
}

// Restart stops the kopia server process through the control API and waits
// until the entrypoint loop has started it again. The new process picks a
// new random signing key, so every CSRF token issued before is invalid.
func (s *Server) Restart(t testing.TB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	before := strings.Count(s.logs(ctx, t), serverStarted)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL+"/api/v1/control/shutdown", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	req.SetBasicAuth(ControlUser, ControlPassword)

	// Test-only client: the server certificate is self-signed.
	hc := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test server with a self-signed certificate
	}}

	resp, err := hc.Do(req)
	if err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("shutdown: %s", resp.Status)
	}

	for strings.Count(s.logs(ctx, t), serverStarted) <= before {
		select {
		case <-ctx.Done():
			t.Fatalf("kopia server did not come back:\n%s", s.logs(context.Background(), t))
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (s *Server) logs(ctx context.Context, t testing.TB) string {
	t.Helper()

	rc, err := s.ctr.Logs(ctx)
	if err != nil {
		t.Fatalf("container logs: %v", err)
	}
	defer rc.Close()

	var b bytes.Buffer
	if _, err := io.Copy(&b, rc); err != nil {
		t.Fatalf("container logs: %v", err)
	}

	return b.String()
}

// String identifies the server in test output.
func (s *Server) String() string {
	return fmt.Sprintf("%s at %s", s.Image, s.URL)
}
