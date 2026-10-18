//go:build unix

package kopiaclient

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type testCert struct {
	der  []byte
	cert *x509.Certificate
	key  crypto.Signer
}

func (c testCert) fingerprint() string {
	sum := sha256.Sum256(c.der)
	return hex.EncodeToString(sum[:])
}

func (c testCert) pemFile(t *testing.T) string {
	t.Helper()

	p := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.der}), 0o600); err != nil {
		t.Fatal(err)
	}

	return p
}

// newCert issues a certificate for "kopia", self-signed when parent is nil.
func newCert(t *testing.T, parent *testCert, isCA bool) testCert {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "kopia"},
		DNSNames:              []string{"kopia"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  isCA,
	}

	signerCert, signerKey := tmpl, crypto.Signer(key)
	if parent != nil {
		signerCert, signerKey = parent.cert, parent.key
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, signerCert, key.Public(), signerKey)
	if err != nil {
		t.Fatal(err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}

	return testCert{der: der, cert: cert, key: key}
}

func serverTLS(leaf testCert, chain ...testCert) *tls.Config {
	c := tls.Certificate{Certificate: [][]byte{leaf.der}, PrivateKey: leaf.key}
	for _, x := range chain {
		c.Certificate = append(c.Certificate, x.der)
	}

	return &tls.Config{Certificates: []tls.Certificate{c}, MinVersion: tls.VersionTLS12}
}

// handshake runs a TLS handshake over an in-memory pipe and returns the
// client side error.
func handshake(t *testing.T, opts Options, srv *tls.Config) error {
	t.Helper()

	cfg, err := tlsConfig(opts)
	if err != nil {
		t.Fatal(err)
	}

	cfg.ServerName = "kopia"

	cc, sc := connPair(t)
	defer cc.Close()
	defer sc.Close()

	go func() {
		_ = tls.Server(sc, srv).HandshakeContext(t.Context())
		sc.Close()
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	return tls.Client(cc, cfg).HandshakeContext(ctx)
}

func TestFingerprintPinning(t *testing.T) {
	leaf := newCert(t, nil, false)
	other := newCert(t, nil, false)

	if err := handshake(t, Options{CertFingerprint: leaf.fingerprint()}, serverTLS(leaf)); err != nil {
		t.Fatalf("matching fingerprint: %v", err)
	}

	// VerifyPeerCertificate does not run on resumed sessions.
	if cfg, err := tlsConfig(Options{CertFingerprint: leaf.fingerprint()}); err != nil || !cfg.SessionTicketsDisabled || cfg.ClientSessionCache != nil {
		t.Fatalf("session resumption must be off with a pinned certificate: %v", err)
	}

	colons := strings.ToUpper(leaf.fingerprint()[:2]) + ":" + leaf.fingerprint()[2:]
	if err := handshake(t, Options{CertFingerprint: colons}, serverTLS(leaf)); err != nil {
		t.Fatalf("fingerprint with colons and upper case: %v", err)
	}

	err := handshake(t, Options{CertFingerprint: other.fingerprint()}, serverTLS(leaf))

	var mismatch *CertificateMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("err = %v, want CertificateMismatchError", err)
	}

	if mismatch.Got != leaf.fingerprint() || mismatch.Want != other.fingerprint() {
		t.Errorf("mismatch = %+v", mismatch)
	}

	if IsRetryable(err) {
		t.Error("fingerprint mismatch must not be retryable")
	}
}

// A forged leaf followed by a copy of the pinned certificate must fail:
// only the leaf proves possession of the private key.
func TestFingerprintPinsLeafOnly(t *testing.T) {
	pinned := newCert(t, nil, false)
	forged := newCert(t, nil, false)

	err := handshake(t, Options{CertFingerprint: pinned.fingerprint()}, serverTLS(forged, pinned))

	var mismatch *CertificateMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("err = %v, want CertificateMismatchError", err)
	}
}

func TestCAFile(t *testing.T) {
	ca := newCert(t, nil, true)
	leaf := newCert(t, &ca, false)
	otherCA := newCert(t, nil, true)

	if err := handshake(t, Options{CAFile: ca.pemFile(t)}, serverTLS(leaf)); err != nil {
		t.Fatalf("leaf signed by the CA: %v", err)
	}

	err := handshake(t, Options{CAFile: otherCA.pemFile(t)}, serverTLS(leaf))
	if err == nil {
		t.Fatal("leaf signed by another CA was accepted")
	}

	var unknown x509.UnknownAuthorityError
	if !errors.As(err, &unknown) {
		t.Errorf("err = %v, want x509.UnknownAuthorityError", err)
	}

	if IsRetryable(err) {
		t.Error("certificate verification failure must not be retryable")
	}
}

func TestCAFileWithoutCertificates(t *testing.T) {
	p := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(p, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := tlsConfig(Options{CAFile: p}); err == nil || !strings.Contains(err.Error(), "no PEM certificates") {
		t.Fatalf("err = %v", err)
	}
}

func TestSystemRootsRejectSelfSigned(t *testing.T) {
	leaf := newCert(t, nil, false)

	if err := handshake(t, Options{}, serverTLS(leaf)); err == nil {
		t.Fatal("self-signed certificate accepted without any TLS option")
	}
}

func TestInsecureSkipVerify(t *testing.T) {
	leaf := newCert(t, nil, false)

	if err := handshake(t, Options{InsecureSkipVerify: true}, serverTLS(leaf)); err != nil {
		t.Fatal(err)
	}
}

func TestTLSOptionsAreExclusive(t *testing.T) {
	fp := strings.Repeat("ab", 32)

	for _, opts := range []Options{
		{CertFingerprint: fp, CAFile: "/ca.pem"},
		{CertFingerprint: fp, InsecureSkipVerify: true},
		{CAFile: "/ca.pem", InsecureSkipVerify: true},
	} {
		if _, err := tlsConfig(opts); err == nil || !strings.Contains(err.Error(), "only one of") {
			t.Errorf("tlsConfig(%+v) err = %v", opts, err)
		}
	}
}

// connPair returns two connected sockets without binding a port. Unlike
// net.Pipe they are buffered, so a side that aborts the handshake with an
// alert does not block on a peer that is still writing.
func connPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()

	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}

	conn := func(fd int) net.Conn {
		f := os.NewFile(uintptr(fd), "socketpair")
		defer f.Close()

		c, err := net.FileConn(f)
		if err != nil {
			t.Fatal(err)
		}

		return c
	}

	return conn(fds[0]), conn(fds[1])
}

// pipeListener connects an http.Transport to an http.Server in memory.
type pipeListener struct {
	pair   func() (net.Conn, net.Conn)
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func newPipeListener(t *testing.T) *pipeListener {
	return &pipeListener{
		pair:   func() (net.Conn, net.Conn) { return connPair(t) },
		conns:  make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *pipeListener) Addr() net.Addr { return &net.UnixAddr{Name: "pipe", Net: "pipe"} }

func (l *pipeListener) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	client, server := l.pair()

	select {
	case l.conns <- server:
		return client, nil
	case <-l.closed:
		return nil, net.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// End to end: the real transport, TLS with a pinned certificate, the cookie
// jar and the CSRF flow against the fake kopia server.
func TestClientOverTLSWithFingerprint(t *testing.T) {
	leaf := newCert(t, nil, false)

	f := newFakeKopia()
	f.handle("GET repo/status", statusRoute)

	l := newPipeListener(t)
	srv := &http.Server{Handler: f, ReadHeaderTimeout: 10 * time.Second}

	go func() { _ = srv.Serve(tls.NewListener(l, serverTLS(leaf))) }()

	t.Cleanup(func() { _ = srv.Close() })

	newPinned := func(fp string) *Client {
		c, err := New(Options{URL: "https://kopia:51515", Username: "admin", Password: "secret", CertFingerprint: fp})
		if err != nil {
			t.Fatal(err)
		}

		tr, ok := c.http.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("transport is %T", c.http.Transport)
		}

		tr.DialContext = l.DialContext
		tr.Proxy = nil // ignore HTTPS_PROXY of the test environment

		return c
	}

	st, err := newPinned(leaf.fingerprint()).RepoStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if !st.Connected {
		t.Fatalf("status = %+v", st)
	}

	_, err = newPinned(newCert(t, nil, false).fingerprint()).RepoStatus(t.Context())

	var mismatch *CertificateMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("err = %v, want CertificateMismatchError through the client", err)
	}

	if IsRetryable(err) {
		t.Error("mismatch through the client must not be retryable")
	}
}
