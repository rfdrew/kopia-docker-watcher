// Package smtptest is a fake SMTP server for tests. It speaks enough SMTP
// for net/smtp, STARTTLS, implicit TLS and AUTH PLAIN and LOGIN included,
// over in-memory connections, so that tests need no listener.
package smtptest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"io"
	"math/big"
	"net"
	"net/textproto"
	"slices"
	"strings"
	"sync"
	"time"
)

// Hostname is the name in the server's certificate, besides localhost,
// 127.0.0.1 and ::1.
const Hostname = "mail.example.com"

// Hang is a reply that makes the server say nothing more and wait until
// the client goes.
const Hang = "HANG"

// Server is a fake SMTP server; set its fields before the first Dial.
type Server struct {
	// ImplicitTLS makes every connection TLS from the start. StartTLS
	// offers STARTTLS on a connection that is not TLS yet.
	ImplicitTLS bool
	StartTLS    bool
	// Auth are the AUTH mechanisms offered: PLAIN, LOGIN or others, which
	// are refused. With StartTLS they are offered only after it.
	Auth []string
	// Username and Password are the credentials the server takes.
	Username string
	Password string
	// Replies replace the replies to the next uses of a command, by its
	// verb, such as MAIL; CONNECT is the greeting. An empty reply is the
	// usual one; Hang hangs. A reply of 4xx or 5xx to CONNECT ends the
	// connection.
	Replies map[string][]string

	mu    sync.Mutex
	dials []string
	mails []Mail
}

// Mail is a message the server took.
type Mail struct {
	// TLS reports whether the connection was TLS by then.
	TLS bool
	// Hello is the name the client gave in EHLO.
	Hello string
	// Mechanism, Username and Password are those of the AUTH that
	// succeeded, if any.
	Mechanism string
	Username  string
	Password  string
	// From and To are the addresses of MAIL FROM and RCPT TO.
	From string
	To   []string
	// Data is the message, with LF line ends and the dots unstuffed.
	Data string
}

// Dial connects to the server; it has the signature of
// net.Dialer.DialContext. ctx bounds the TLS handshakes of the server.
func (s *Server) Dial(ctx context.Context, _, address string) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.dials = append(s.dials, address)
	s.mu.Unlock()

	client, server := Pipe()

	go s.serve(ctx, server)

	return client, nil
}

// Dials returns the addresses of the connections so far.
func (s *Server) Dials() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.dials)
}

// Mails returns the messages taken so far.
func (s *Server) Mails() []Mail {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.mails)
}

// reply returns the reply that replaces the usual one to the next use of
// verb, if any.
func (s *Server) reply(verb string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rs := s.Replies[verb]
	if len(rs) == 0 {
		return "", false
	}

	s.Replies[verb] = rs[1:]

	return rs[0], rs[0] != ""
}

// session is one connection.
type session struct {
	s    *Server
	ctx  context.Context
	raw  net.Conn
	text *textproto.Conn
	mail Mail
}

func (s *Server) serve(ctx context.Context, raw net.Conn) {
	// Closing the connection beneath TLS does not wait for the client to
	// read a close_notify.
	defer raw.Close()

	ss := &session{s: s, ctx: ctx, raw: raw, text: textproto.NewConn(raw)}

	if s.ImplicitTLS {
		tc := tls.Server(raw, serverTLS())
		if err := tc.HandshakeContext(ctx); err != nil {
			return
		}

		ss.text, ss.mail.TLS = textproto.NewConn(tc), true
	}

	greeting := "220 " + Hostname + " ESMTP smtptest"
	if r, ok := s.reply("CONNECT"); ok {
		greeting = r
	}

	if !ss.send(greeting) || strings.HasPrefix(greeting, "4") || strings.HasPrefix(greeting, "5") {
		return
	}

	for {
		line, err := ss.text.ReadLine()
		if err != nil {
			return
		}

		verb, arg, _ := strings.Cut(line, " ")
		verb = strings.ToUpper(verb)

		if r, ok := s.reply(verb); ok {
			if !ss.send(r) {
				return
			}

			continue
		}

		if !ss.handle(verb, arg) {
			return
		}
	}
}

// send writes a reply; false when the session is over.
func (ss *session) send(reply string) bool {
	if reply == Hang {
		_, _ = io.Copy(io.Discard, ss.raw)
		return false
	}

	return ss.text.PrintfLine("%s", reply) == nil
}

// handle answers a command; false when the session is over.
func (ss *session) handle(verb, arg string) bool {
	switch verb {
	case "EHLO", "HELO":
		ss.mail.Hello = arg

		lines := []string{Hostname}

		if ss.s.StartTLS && !ss.mail.TLS {
			lines = append(lines, "STARTTLS")
		}

		if len(ss.s.Auth) > 0 && (ss.mail.TLS || !ss.s.StartTLS) {
			lines = append(lines, "AUTH "+strings.Join(ss.s.Auth, " "))
		}

		for i, l := range lines {
			sep := "-"
			if i == len(lines)-1 {
				sep = " "
			}

			if !ss.send("250" + sep + l) {
				return false
			}
		}

		return true
	case "STARTTLS":
		if !ss.s.StartTLS || ss.mail.TLS {
			return ss.send("502 5.5.1 STARTTLS not offered")
		}

		if !ss.send("220 2.0.0 Ready to start TLS") {
			return false
		}

		tc := tls.Server(ss.raw, serverTLS())
		if err := tc.HandshakeContext(ss.ctx); err != nil {
			return false
		}

		// The client starts over with EHLO.
		ss.text, ss.mail = textproto.NewConn(tc), Mail{TLS: true}

		return true
	case "AUTH":
		return ss.auth(arg)
	case "MAIL":
		ss.mail.From = address(arg)
		return ss.send("250 2.1.0 Ok")
	case "RCPT":
		ss.mail.To = append(ss.mail.To, address(arg))
		return ss.send("250 2.1.5 Ok")
	case "DATA":
		if !ss.send("354 End data with <CR><LF>.<CR><LF>") {
			return false
		}

		data, err := ss.text.ReadDotBytes()
		if err != nil {
			return false
		}

		ss.mail.Data = string(data)

		ss.s.mu.Lock()
		ss.s.mails = append(ss.s.mails, ss.mail)
		ss.s.mu.Unlock()

		ss.mail.From, ss.mail.To, ss.mail.Data = "", nil, ""

		return ss.send("250 2.0.0 Ok: queued")
	case "RSET":
		ss.mail.From, ss.mail.To = "", nil
		return ss.send("250 2.0.0 Ok")
	case "NOOP":
		return ss.send("250 2.0.0 Ok")
	case "QUIT":
		_ = ss.send("221 2.0.0 Bye")
		return false
	default:
		return ss.send("502 5.5.2 Error: command not recognized")
	}
}

// auth runs AUTH PLAIN or LOGIN.
func (ss *session) auth(arg string) bool {
	mech, initial, _ := strings.Cut(arg, " ")
	mech = strings.ToUpper(mech)

	if !slices.Contains(ss.s.Auth, mech) || (ss.s.StartTLS && !ss.mail.TLS) {
		return ss.send("504 5.5.4 Unrecognized authentication type")
	}

	var user, pass string

	switch mech {
	case "PLAIN":
		if initial == "" {
			if !ss.send("334 ") {
				return false
			}

			line, err := ss.text.ReadLine()
			if err != nil {
				return false
			}

			initial = line
		}

		b, _ := base64.StdEncoding.DecodeString(initial)
		if parts := strings.Split(string(b), "\x00"); len(parts) == 3 {
			user, pass = parts[1], parts[2]
		}
	case "LOGIN":
		for i, prompt := range []string{"Username:", "Password:"} {
			if !ss.send("334 " + base64.StdEncoding.EncodeToString([]byte(prompt))) {
				return false
			}

			line, err := ss.text.ReadLine()
			if err != nil {
				return false
			}

			b, _ := base64.StdEncoding.DecodeString(line)
			if i == 0 {
				user = string(b)
			} else {
				pass = string(b)
			}
		}
	default:
		return ss.send("504 5.5.4 Unrecognized authentication type")
	}

	if user != ss.s.Username || pass != ss.s.Password {
		return ss.send("535 5.7.8 Error: authentication failed")
	}

	ss.mail.Mechanism, ss.mail.Username, ss.mail.Password = mech, user, pass

	return ss.send("235 2.7.0 Authentication successful")
}

// address is the address in the angle brackets of MAIL FROM:<…> or
// RCPT TO:<…>.
func address(arg string) string {
	_, after, _ := strings.Cut(arg, "<")
	a, _, _ := strings.Cut(after, ">")

	return a
}

// ClientTLS returns a TLS configuration that trusts the server's
// certificate.
func ClientTLS() *tls.Config {
	pool := x509.NewCertPool()
	pool.AddCert(certificate().Leaf)

	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
}

func serverTLS() *tls.Config {
	return &tls.Config{Certificates: []tls.Certificate{certificate()}, MinVersion: tls.VersionTLS12}
}

// certificate is the server's self-signed certificate, made once.
var certificate = sync.OnceValue(func() tls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: Hostname},
		DNSNames:              []string{Hostname, "localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}

	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		panic(err)
	}

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
})
