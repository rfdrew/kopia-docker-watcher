package kopiaclient

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
)

// tlsConfig builds the client TLS configuration. At most one of the options
// may be set; with none, the system roots verify the server.
func tlsConfig(opts Options) (*tls.Config, error) {
	set := 0

	for _, b := range []bool{opts.CertFingerprint != "", opts.CAFile != "", opts.InsecureSkipVerify} {
		if b {
			set++
		}
	}

	if set > 1 {
		return nil, errors.New("only one of CertFingerprint, CAFile and InsecureSkipVerify may be set")
	}

	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	switch {
	case opts.CertFingerprint != "":
		want, err := decodeFingerprint(opts.CertFingerprint)
		if err != nil {
			return nil, err
		}

		// Chain and hostname checks are replaced by pinning the leaf
		// certificate, the only one the handshake proves the server holds
		// the key for. Matching any certificate of the presented chain, as
		// kopia's own client does, would accept a forged leaf followed by a
		// copy of the real certificate. VerifyPeerCertificate is skipped on
		// resumed sessions, so resumption is off: every connection is a full
		// handshake and gets verified.
		cfg.InsecureSkipVerify = true
		cfg.SessionTicketsDisabled = true
		cfg.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("kopia server presented no certificate")
			}

			got := sha256.Sum256(rawCerts[0])
			if subtle.ConstantTimeCompare(got[:], want) != 1 {
				return &CertificateMismatchError{Want: hex.EncodeToString(want), Got: hex.EncodeToString(got[:])}
			}

			return nil
		}

	case opts.CAFile != "":
		pem, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("CA file %s contains no PEM certificates", opts.CAFile)
		}

		cfg.RootCAs = pool

	case opts.InsecureSkipVerify:
		cfg.InsecureSkipVerify = true
	}

	return cfg, nil
}

// decodeFingerprint accepts hex with optional colons, in any case.
func decodeFingerprint(s string) ([]byte, error) {
	b, err := hex.DecodeString(strings.ReplaceAll(strings.TrimSpace(s), ":", ""))
	if err != nil || len(b) != sha256.Size {
		return nil, fmt.Errorf("invalid SHA-256 certificate fingerprint %q: expected 64 hex digits", s)
	}

	return b, nil
}
