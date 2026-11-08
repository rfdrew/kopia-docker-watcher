package notify

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/version"
)

// SMTPOptions configure the notifications by e-mail.
type SMTPOptions struct {
	// Host is the SMTP server, a host name or an IP address; required.
	Host string
	// Port is the server's port; zero means config.DefaultNotifySMTPTLSPort
	// with TLS config.SMTPTLS, config.DefaultNotifySMTPPort otherwise.
	Port int
	// TLS is one of config.SMTPTLSModes; empty means config.SMTPStartTLS.
	TLS string

	// Username and Password log in with AUTH PLAIN, or AUTH LOGIN if the
	// server offers only that; empty Username sends no AUTH. Over a
	// connection without TLS only a server on localhost gets them.
	Username string
	Password string

	// From is the sender, To the recipients; required.
	From *mail.Address
	To   []*mail.Address

	// LocalName is the name the watcher gives in EHLO; empty means the host
	// name of the machine.
	LocalName string
	// Dial connects to the server; nil means a net.Dialer.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
	// TLSConfig is the base of the TLS configuration; nil means the
	// system's roots. An empty ServerName means Host.
	TLSConfig *tls.Config
}

// smtpSender sends the notifications by e-mail: each one in a connection
// of its own, to every recipient in one message. A recipient the server
// refuses fails the whole message, so that no one gets it, rather than
// some.
type smtpSender struct {
	opts SMTPOptions
	// addr is host:port, to connect to and to log.
	addr string
	tls  *tls.Config
}

// permanentError is an error that trying again cannot fix, such as a
// server that does not offer what the configuration asks for.
type permanentError struct{ error }

func (e permanentError) Unwrap() error { return e.error }

func newSMTP(opts SMTPOptions) (*smtpSender, error) {
	if opts.TLS == "" {
		opts.TLS = config.SMTPStartTLS
	}

	if opts.Port == 0 {
		opts.Port = config.DefaultNotifySMTPPort
		if opts.TLS == config.SMTPTLS {
			opts.Port = config.DefaultNotifySMTPTLSPort
		}
	}

	switch {
	case opts.Host == "":
		return nil, errors.New("notify: no SMTP host")
	case opts.Port < 1 || opts.Port > 65535:
		return nil, fmt.Errorf("notify: invalid SMTP port %d", opts.Port)
	case !slices.Contains(config.SMTPTLSModes(), opts.TLS):
		return nil, fmt.Errorf("notify: unknown SMTP TLS mode %q", opts.TLS)
	case opts.From == nil || len(opts.To) == 0:
		return nil, errors.New("notify: an e-mail needs a sender and a recipient")
	case opts.Username != "" && opts.TLS == config.SMTPNoTLS && !config.IsLocalhost(opts.Host):
		return nil, errors.New("notify: an SMTP login without TLS would send the password unencrypted")
	}

	if opts.LocalName == "" {
		opts.LocalName = "localhost"
		if h, err := os.Hostname(); err == nil && h != "" {
			opts.LocalName = h
		}
	}

	if opts.Dial == nil {
		opts.Dial = (&net.Dialer{}).DialContext
	}

	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if opts.TLSConfig != nil {
		tc = opts.TLSConfig.Clone()
		tc.MinVersion = max(tc.MinVersion, tls.VersionTLS12)
	}

	if tc.ServerName == "" {
		tc.ServerName = opts.Host
	}

	return &smtpSender{
		opts: opts,
		addr: net.JoinHostPort(opts.Host, strconv.Itoa(opts.Port)),
		tls:  tc,
	}, nil
}

// send implements sender: an SMTP reply of 4xx and a connection that
// fails or times out are tried again; a reply of 5xx, a certificate that
// does not verify and a server that lacks STARTTLS or AUTH are not.
func (s *smtpSender) send(ctx context.Context, e Event) (time.Duration, error) {
	err := s.deliver(ctx, s.message(e))
	if err == nil {
		return 0, nil
	}

	var (
		pe permanentError
		te *textproto.Error
		ve *tls.CertificateVerificationError
		re tls.RecordHeaderError
	)

	switch {
	case errors.As(err, &pe), errors.As(err, &ve), errors.As(err, &re):
		return -1, err
	case errors.As(err, &te) && te.Code >= 500:
		return -1, err
	default:
		return 0, err
	}
}

// deliver connects to the server and sends msg. ctx bounds the whole
// conversation: its deadline holds for every read and write, and its end
// closes the connection.
func (s *smtpSender) deliver(ctx context.Context, msg []byte) error {
	raw, err := s.opts.Dial(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	// Closing the TLS connection would first try to tell the server, which
	// may not listen; the end of the conversation closes the one beneath.
	defer raw.Close()

	if d, ok := ctx.Deadline(); ok {
		_ = raw.SetDeadline(d)
	}

	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer stop()

	conn := raw

	if s.opts.TLS == config.SMTPTLS {
		tc := tls.Client(conn, s.tls)
		if err := tc.HandshakeContext(ctx); err != nil {
			return fmt.Errorf("TLS handshake (does the server start in plain text and offer STARTTLS?): %w", err)
		}

		conn = tc
	}

	c, err := smtp.NewClient(conn, s.opts.Host)
	if err != nil {
		return fmt.Errorf("greeting: %w", err)
	}

	if err := c.Hello(s.opts.LocalName); err != nil {
		return fmt.Errorf("EHLO: %w", err)
	}

	if s.opts.TLS == config.SMTPStartTLS {
		if ok, _ := c.Extension("STARTTLS"); !ok {
			return permanentError{errors.New("the server does not offer STARTTLS; " +
				"use TLS from the start if it expects that, or no TLS")}
		}

		if err := c.StartTLS(s.tls); err != nil {
			return fmt.Errorf("STARTTLS: %w", err)
		}
	}

	if s.opts.Username != "" {
		a, err := s.auth(c)
		if err != nil {
			return err
		}

		if err := c.Auth(a); err != nil {
			return fmt.Errorf("AUTH: %w", err)
		}
	}

	if err := c.Mail(s.opts.From.Address); err != nil {
		return fmt.Errorf("MAIL FROM: %w", err)
	}

	for _, to := range s.opts.To {
		if err := c.Rcpt(to.Address); err != nil {
			return fmt.Errorf("RCPT TO <%s>: %w", to.Address, err)
		}
	}

	w, err := c.Data()
	if err != nil {
		return fmt.Errorf("DATA: %w", err)
	}

	if _, err := w.Write(msg); err != nil {
		return fmt.Errorf("DATA: %w", err)
	}

	// The server's reply to the end of the message says whether it took
	// it.
	if err := w.Close(); err != nil {
		return fmt.Errorf("DATA: %w", err)
	}

	// The server has the message: a failed QUIT changes nothing.
	_ = c.Quit()

	return nil
}

// auth picks the AUTH mechanism: PLAIN, or LOGIN if the server offers only
// that. Both refuse a connection without TLS to a server other than
// localhost.
func (s *smtpSender) auth(c *smtp.Client) (smtp.Auth, error) {
	ok, params := c.Extension("AUTH")
	if !ok {
		return nil, permanentError{errors.New("the server does not offer AUTH, but a username is set")}
	}

	mechs := strings.Fields(strings.ToUpper(params))

	switch {
	case slices.Contains(mechs, "PLAIN"):
		return smtp.PlainAuth("", s.opts.Username, s.opts.Password, s.opts.Host), nil
	case slices.Contains(mechs, "LOGIN"):
		return &loginAuth{username: s.opts.Username, password: s.opts.Password, host: s.opts.Host}, nil
	default:
		return nil, permanentError{fmt.Errorf("the server offers AUTH %s, but only PLAIN and LOGIN are supported", params)}
	}
}

// loginAuth is the LOGIN mechanism, which net/smtp lacks: the username
// and the password in answer to two challenges.
type loginAuth struct {
	username, password, host string
	step                     int
}

// Start implements smtp.Auth.
func (a *loginAuth) Start(server *smtp.ServerInfo) (string, []byte, error) {
	if !server.TLS && !config.IsLocalhost(server.Name) {
		return "", nil, errors.New("unencrypted connection")
	}

	if server.Name != a.host {
		return "", nil, errors.New("wrong host name")
	}

	a.step = 0

	return "LOGIN", nil, nil
}

// Next implements smtp.Auth. The challenges are prompts such as
// "Username:", which servers word differently, so only their order
// counts.
func (a *loginAuth) Next(_ []byte, more bool) ([]byte, error) {
	if !more {
		return nil, nil
	}

	a.step++

	switch a.step {
	case 1:
		return []byte(a.username), nil
	case 2:
		return []byte(a.password), nil
	default:
		return nil, errors.New("unexpected challenge after the password")
	}
}

// maxHeaderLine is the length a header line should not exceed: 78 in RFC
// 5322 (2.1.1), 76 for a line with an encoded word in RFC 2047 (2).
const maxHeaderLine = 76

// message is the e-mail of e: the title as the subject and the message
// with the fields of the json format as the body, in UTF-8 and
// quoted-printable.
func (s *smtpSender) message(e Event) []byte {
	date := e.Time
	if date.IsZero() {
		date = time.Now()
	}

	to := make([]string, len(s.opts.To))
	for i, a := range s.opts.To {
		to[i] = a.String()
	}

	domain := s.opts.From.Address[strings.LastIndex(s.opts.From.Address, "@")+1:]

	var b bytes.Buffer

	for _, h := range [][2]string{
		{"Date", date.UTC().Format(time.RFC1123Z)},
		{"From", s.opts.From.String()},
		{"To", strings.Join(to, ", ")},
		// Q encoding keeps the subject readable where it is ASCII, and
		// encodes line breaks, so that nothing of e can add a header.
		{"Subject", mime.QEncoding.Encode("utf-8", e.Title())},
		{"Message-ID", "<" + rand.Text() + "@" + domain + ">"},
		{"MIME-Version", "1.0"},
		{"Content-Type", "text/plain; charset=utf-8"},
		{"Content-Transfer-Encoding", "quoted-printable"},
		// No vacation replies to an automatic message (RFC 3834).
		{"Auto-Submitted", "auto-generated"},
	} {
		b.WriteString(foldHeader(h[0] + ": " + h[1]))
		b.WriteString("\r\n")
	}

	b.WriteString("\r\n")

	qp := quotedprintable.NewWriter(&b)
	_, _ = qp.Write([]byte(mailBody(e)))
	_ = qp.Close()

	return b.Bytes()
}

// foldHeader folds a header line longer than maxHeaderLine at its spaces,
// right after the name if the first word does not fit. A word longer than
// that, such as a long path in a subject without spaces, stays whole on a
// line of its own.
func foldHeader(line string) string {
	if len(line) <= maxHeaderLine {
		return line
	}

	var b strings.Builder

	n := 0

	for i, word := range strings.Split(line, " ") {
		if i > 0 {
			if n > 1 && n+1+len(word) > maxHeaderLine {
				b.WriteString("\r\n")

				n = 0
			}

			b.WriteByte(' ')

			n++
		}

		b.WriteString(word)

		n += len(word)
	}

	return b.String()
}

// mailBody is the message of e followed by the fields of the json format,
// those that are set.
func mailBody(e Event) string {
	var b strings.Builder

	b.WriteString(e.Message())
	b.WriteString("\n\n")

	field := func(name, value string) {
		if value != "" {
			b.WriteString(name + ": " + value + "\n")
		}
	}

	field("event", string(e.Kind))
	field("status", e.status())
	field("path", e.Path)
	field("containers", strings.Join(e.Containers, ", "))
	field("container", e.Container)
	field("host", e.Host)
	field("reason", e.Reason)
	field("error", e.Error)

	if !e.Since.IsZero() {
		field("since", stamp(e.Since))
	}

	if !e.Time.IsZero() {
		field("time", stamp(e.Time))
	}

	field("watcher_version", version.Version)

	return b.String()
}
