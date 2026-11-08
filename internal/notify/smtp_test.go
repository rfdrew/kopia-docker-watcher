package notify

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/smtp"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
	"github.com/rfdrew/kopia-docker-watcher/internal/smtptest"
	"github.com/rfdrew/kopia-docker-watcher/internal/version"
)

const smtpPassword = "s3cret-smtp-pa55word"

// smtpServer is a server that offers STARTTLS and AUTH PLAIN and LOGIN,
// and takes the credentials withSMTP logs in with.
func smtpServer() *smtptest.Server {
	return &smtptest.Server{StartTLS: true, Auth: []string{"PLAIN", "LOGIN"}, Username: "watcher", Password: smtpPassword}
}

// withSMTP sends by e-mail to srv, STARTTLS and a login by default.
func withSMTP(srv *smtptest.Server, mod ...func(*SMTPOptions)) func(*Options) {
	return func(o *Options) {
		so := &SMTPOptions{
			Host:      smtptest.Hostname,
			Username:  "watcher",
			Password:  smtpPassword,
			From:      &mail.Address{Name: "Backups", Address: "backups@example.com"},
			To:        []*mail.Address{{Address: "ops@example.com"}, {Name: "On call", Address: "oncall@example.com"}},
			LocalName: "watcher.test",
			Dial:      srv.Dial,
			TLSConfig: smtptest.ClientTLS(),
		}

		for _, fn := range mod {
			fn(so)
		}

		o.SMTP = so
	}
}

// smtpOnly leaves the webhook out.
func smtpOnly(o *Options) { o.Webhook = nil }

func hookEvent() Event {
	return Event{
		Kind: HookFailed, Path: "/data/äpp", Containers: []string{"db", "api"}, Host: "nas", Reason: reconcile.HookPreExec,
		Container: "db", Error: "exited with code 1", Since: time.Date(2026, 11, 8, 3, 0, 0, 0, time.UTC),
		Time: time.Date(2026, 11, 8, 3, 0, 5, 0, time.UTC),
	}
}

// readMail parses a message the server took and decodes its body.
func readMail(t *testing.T, m smtptest.Mail) (*mail.Message, string) {
	t.Helper()

	msg, err := mail.ReadMessage(strings.NewReader(m.Data))
	if err != nil {
		t.Fatalf("parse %q: %v", m.Data, err)
	}

	if msg.Header.Get("Content-Transfer-Encoding") != "quoted-printable" {
		t.Fatalf("header = %v", msg.Header)
	}

	body, err := io.ReadAll(quotedprintable.NewReader(msg.Body))
	if err != nil {
		t.Fatal(err)
	}

	return msg, string(body)
}

// With STARTTLS the watcher logs in over TLS and sends one message to every
// recipient: the title as the subject, the message and the fields of the
// event as the body.
func TestSMTPStartTLS(t *testing.T) {
	srv := smtpServer()
	f := newFixture(t, "", "", smtpOnly, withSMTP(srv))
	f.run(t)

	e := hookEvent()
	f.n.enqueue(e)
	f.waitChannel(t, ChannelSMTP, map[Result]int{ResultSent: 1})

	if got := srv.Dials(); len(got) != 1 || got[0] != "mail.example.com:587" {
		t.Errorf("dials = %v", got)
	}

	m := srv.Mails()[0]
	if !m.TLS || m.Hello != "watcher.test" || m.Mechanism != "PLAIN" || m.Username != "watcher" || m.Password != smtpPassword {
		t.Errorf("session = %+v", m)
	}

	if m.From != "backups@example.com" || strings.Join(m.To, ",") != "ops@example.com,oncall@example.com" {
		t.Errorf("envelope = %s -> %v", m.From, m.To)
	}

	msg, body := readMail(t, m)

	subject, err := new(mime.WordDecoder).DecodeHeader(msg.Header.Get("Subject"))
	if err != nil || subject != e.Title() {
		t.Errorf("subject = %q (%v), want %q", subject, err, e.Title())
	}

	if date, err := msg.Header.Date(); err != nil || !date.Equal(e.Time) {
		t.Errorf("date = %v, %v", date, err)
	}

	for k, want := range map[string]string{
		"From":           `"Backups" <backups@example.com>`,
		"To":             `<ops@example.com>, "On call" <oncall@example.com>`,
		"MIME-Version":   "1.0",
		"Content-Type":   "text/plain; charset=utf-8",
		"Auto-Submitted": "auto-generated",
	} {
		if got := msg.Header.Get(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}

	if id := msg.Header.Get("Message-ID"); !regexp.MustCompile(`^<[A-Z2-7]{26}@example\.com>$`).MatchString(id) {
		t.Errorf("Message-ID = %q", id)
	}

	want := e.Message() + "\n\n" +
		"event: hook_failed\n" +
		"status: failing\n" +
		"path: /data/äpp\n" +
		"containers: db, api\n" +
		"container: db\n" +
		"host: nas\n" +
		"reason: pre-exec\n" +
		"error: exited with code 1\n" +
		"since: 2026-11-08T03:00:00Z\n" +
		"time: 2026-11-08T03:00:05Z\n" +
		"watcher_version: " + version.Version + "\n"
	if body != want {
		t.Errorf("body = %q, want %q", body, want)
	}

	// Each message has an ID of its own.
	f.n.enqueue(e)
	f.waitChannel(t, ChannelSMTP, map[Result]int{ResultSent: 2})

	msg2, _ := readMail(t, srv.Mails()[1])
	if msg2.Header.Get("Message-ID") == msg.Header.Get("Message-ID") {
		t.Errorf("the same Message-ID twice: %s", msg.Header.Get("Message-ID"))
	}

	if strings.Contains(f.logs.String(), smtpPassword) {
		t.Errorf("the log shows the password:\n%s", f.logs)
	}
}

// With implicit TLS the connection is TLS from the start, on port 465 by
// default, and a server that offers only LOGIN gets that.
func TestSMTPImplicitTLS(t *testing.T) {
	srv := &smtptest.Server{ImplicitTLS: true, Auth: []string{"LOGIN"}, Username: "watcher", Password: smtpPassword}
	f := newFixture(t, "", "", smtpOnly, withSMTP(srv, func(o *SMTPOptions) { o.TLS = config.SMTPTLS }))
	f.run(t)

	f.n.enqueue(hookEvent())
	f.waitChannel(t, ChannelSMTP, map[Result]int{ResultSent: 1})

	if got := srv.Dials(); len(got) != 1 || got[0] != "mail.example.com:465" {
		t.Errorf("dials = %v", got)
	}

	if m := srv.Mails()[0]; !m.TLS || m.Mechanism != "LOGIN" || m.Username != "watcher" || m.Password != smtpPassword {
		t.Errorf("session = %+v", m)
	}
}

// Without TLS the watcher logs in only to a server on localhost, and
// without a username not at all.
func TestSMTPWithoutTLS(t *testing.T) {
	t.Run("login on localhost", func(t *testing.T) {
		srv := &smtptest.Server{Auth: []string{"PLAIN"}, Username: "watcher", Password: smtpPassword}
		f := newFixture(t, "", "", smtpOnly, withSMTP(srv, func(o *SMTPOptions) {
			o.Host, o.Port, o.TLS = "localhost", 25, config.SMTPNoTLS
		}))
		f.run(t)

		f.n.enqueue(hookEvent())
		f.waitChannel(t, ChannelSMTP, map[Result]int{ResultSent: 1})

		if m := srv.Mails()[0]; m.TLS || m.Mechanism != "PLAIN" || srv.Dials()[0] != "localhost:25" {
			t.Errorf("session = %+v, dials = %v", m, srv.Dials())
		}
	})

	t.Run("relay", func(t *testing.T) {
		srv := &smtptest.Server{}
		f := newFixture(t, "", "", smtpOnly, withSMTP(srv, func(o *SMTPOptions) {
			o.Host, o.TLS, o.Username, o.Password = "relay", config.SMTPNoTLS, "", ""
		}))
		f.run(t)

		f.n.enqueue(hookEvent())
		f.waitChannel(t, ChannelSMTP, map[Result]int{ResultSent: 1})

		if m := srv.Mails()[0]; m.TLS || m.Mechanism != "" || srv.Dials()[0] != "relay:587" {
			t.Errorf("session = %+v, dials = %v", m, srv.Dials())
		}
	})

	_, err := New(Options{SMTP: &SMTPOptions{
		Host: "mail.example.com", TLS: config.SMTPNoTLS, Username: "watcher", Password: smtpPassword,
		From: &mail.Address{Address: "a@example.com"}, To: []*mail.Address{{Address: "b@example.com"}},
	}})
	if err == nil || !strings.Contains(err.Error(), "unencrypted") || strings.Contains(err.Error(), smtpPassword) {
		t.Errorf("New = %v", err)
	}
}

// A failure that trying again cannot fix fails the notification at once.
func TestSMTPPermanentFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		srv  *smtptest.Server
		mod  func(*SMTPOptions)
		want string
	}{
		{
			name: "no STARTTLS",
			srv:  &smtptest.Server{Auth: []string{"PLAIN"}},
			want: "the server does not offer STARTTLS",
		},
		{
			name: "wrong password",
			srv:  &smtptest.Server{StartTLS: true, Auth: []string{"PLAIN"}, Username: "watcher", Password: "other"},
			want: "AUTH: 535",
		},
		{
			name: "no AUTH",
			srv:  &smtptest.Server{StartTLS: true},
			want: "the server does not offer AUTH",
		},
		{
			name: "unknown AUTH",
			srv:  &smtptest.Server{StartTLS: true, Auth: []string{"CRAM-MD5", "XOAUTH2"}},
			want: "the server offers AUTH CRAM-MD5 XOAUTH2, but only PLAIN and LOGIN are supported",
		},
		{
			name: "recipient refused",
			srv: &smtptest.Server{StartTLS: true, Auth: []string{"PLAIN"}, Username: "watcher", Password: smtpPassword,
				Replies: map[string][]string{"RCPT": {"", "550 5.1.1 <oncall@example.com>: Recipient address rejected"}}},
			want: "RCPT TO <oncall@example.com>: 550",
		},
		{
			name: "message refused",
			srv: &smtptest.Server{StartTLS: true, Auth: []string{"PLAIN"}, Username: "watcher", Password: smtpPassword,
				Replies: map[string][]string{"DATA": {"554 5.7.1 Rejected"}}},
			want: "DATA: 554",
		},
		{
			name: "untrusted certificate",
			srv:  smtpServer(),
			mod: func(o *SMTPOptions) {
				o.TLSConfig = &tls.Config{RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12}
			},
			want: "STARTTLS: tls: failed to verify certificate",
		},
		{
			name: "TLS to a server in plain text",
			srv:  smtpServer(),
			mod:  func(o *SMTPOptions) { o.TLS = config.SMTPTLS },
			want: "does the server start in plain text and offer STARTTLS?",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mod := func(*SMTPOptions) {}
			if tc.mod != nil {
				mod = tc.mod
			}

			f := newFixture(t, "", "", smtpOnly, withSMTP(tc.srv, mod))
			f.run(t)

			f.n.enqueue(hookEvent())
			f.waitChannel(t, ChannelSMTP, map[Result]int{ResultFailed: 1})

			if n := len(tc.srv.Dials()); n != 1 {
				t.Errorf("%d connections, want 1", n)
			}

			if n := len(tc.srv.Mails()); n != 0 {
				t.Errorf("%d messages taken", n)
			}

			logs := f.logs.String()
			if !strings.Contains(logs, tc.want) || !strings.Contains(logs, "smtp=mail.example.com:") {
				t.Errorf("the log lacks %q:\n%s", tc.want, logs)
			}

			if strings.Contains(logs, smtpPassword) {
				t.Errorf("the log shows the password:\n%s", logs)
			}
		})
	}
}

// A temporary failure is tried again, each time in a new connection.
func TestSMTPRetries(t *testing.T) {
	t.Run("greylisted", func(t *testing.T) {
		srv := smtpServer()
		srv.Replies = map[string][]string{"MAIL": {"451 4.7.1 Try again later", "451 4.7.1 Try again later"}}

		f := newFixture(t, "", "", smtpOnly, withSMTP(srv))
		f.run(t)

		f.n.enqueue(hookEvent())
		f.waitChannel(t, ChannelSMTP, map[Result]int{ResultSent: 1})

		if n, m := len(srv.Dials()), len(srv.Mails()); n != 3 || m != 1 {
			t.Errorf("%d connections, %d messages; want 3, 1", n, m)
		}

		if !strings.Contains(f.logs.String(), "MAIL FROM: 451") {
			t.Errorf("log:\n%s", f.logs)
		}
	})

	t.Run("busy", func(t *testing.T) {
		srv := smtpServer()
		srv.Replies = map[string][]string{"CONNECT": {"421 4.3.2 Too many connections"}}

		f := newFixture(t, "", "", smtpOnly, withSMTP(srv))
		f.run(t)

		f.n.enqueue(hookEvent())
		f.waitChannel(t, ChannelSMTP, map[Result]int{ResultSent: 1})

		if n := len(srv.Dials()); n != 2 {
			t.Errorf("%d connections, want 2", n)
		}
	})

	t.Run("unreachable", func(t *testing.T) {
		var dials atomic.Int32

		f := newFixture(t, "", "", smtpOnly, withSMTP(smtpServer(), func(o *SMTPOptions) {
			o.Dial = func(context.Context, string, string) (net.Conn, error) {
				dials.Add(1)
				return nil, errors.New("connection refused")
			}
		}))
		f.run(t)

		f.n.enqueue(hookEvent())
		f.waitChannel(t, ChannelSMTP, map[Result]int{ResultFailed: 1})

		if n := dials.Load(); n != DefaultAttempts {
			t.Errorf("%d dials, want %d", n, DefaultAttempts)
		}

		if !strings.Contains(f.logs.String(), "connect: connection refused") {
			t.Errorf("log:\n%s", f.logs)
		}
	})

	t.Run("silent", func(t *testing.T) {
		srv := smtpServer()
		srv.Replies = map[string][]string{"CONNECT": {smtptest.Hang, smtptest.Hang}}

		f := newFixture(t, "", "", smtpOnly, withSMTP(srv), func(o *Options) {
			o.Timeout, o.Attempts = 50*time.Millisecond, 2
		})
		f.run(t)

		f.n.enqueue(hookEvent())
		f.waitChannel(t, ChannelSMTP, map[Result]int{ResultFailed: 1})

		if n := len(srv.Dials()); n != 2 {
			t.Errorf("%d connections, want 2", n)
		}

		if !strings.Contains(f.logs.String(), "deadline exceeded") && !strings.Contains(f.logs.String(), "timeout") {
			t.Errorf("log:\n%s", f.logs)
		}
	})
}

// Each channel has its own queue and sender: one that hangs holds back
// none of the other's notifications.
func TestChannelsApart(t *testing.T) {
	events := func(n int) []Event {
		out := make([]Event, n)
		for i := range out {
			out[i] = Event{Kind: SnapshotFailed, Path: "/data/app" + string(rune('0'+i))}
		}

		return out
	}

	t.Run("SMTP hangs", func(t *testing.T) {
		dialed := make(chan struct{}, 10)

		f := newFixture(t, config.WebhookJSON, "https://hooks.example.com/x", func(o *Options) {
			o.QueueSize, o.Timeout = 2, time.Minute
		}, withSMTP(smtpServer(), func(o *SMTPOptions) {
			o.Dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
				dialed <- struct{}{}
				<-ctx.Done()

				return nil, ctx.Err()
			}
		}))

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})

		go func() {
			f.n.Run(ctx)
			close(done)
		}()

		// The SMTP sender takes the first and hangs; then its queue holds
		// two and drops the rest, while the webhook sends each.
		for i, e := range events(5) {
			f.n.enqueue(e)

			if i == 0 {
				<-dialed
			}

			f.waitResults(t, map[Result]int{ResultSent: i + 1})
		}

		if n := f.metrics.get(ChannelSMTP, ResultSent) + f.metrics.get(ChannelSMTP, ResultFailed); n != 0 {
			t.Errorf("%d e-mails ended, want none yet", n)
		}

		f.waitChannel(t, ChannelSMTP, map[Result]int{ResultDropped: 2})

		// The stop drops the one in the sender and the two that wait.
		cancel()
		<-done

		f.waitChannel(t, ChannelSMTP, map[Result]int{ResultDropped: 5, ResultSent: 0, ResultFailed: 0})
		f.waitResults(t, map[Result]int{ResultSent: 5, ResultDropped: 0})
	})

	t.Run("webhook hangs", func(t *testing.T) {
		srv := smtpServer()
		f := newFixture(t, config.WebhookJSON, "https://hooks.example.com/x", func(o *Options) {
			o.Timeout = time.Minute
		}, withSMTP(srv))
		f.hook.block = make(chan struct{})
		f.run(t)

		for _, e := range events(3) {
			f.n.enqueue(e)
		}

		f.waitChannel(t, ChannelSMTP, map[Result]int{ResultSent: 3})

		if n := len(f.hook.recorded()); n != 0 {
			t.Errorf("%d webhook requests, want none yet", n)
		}

		close(f.hook.block)
		f.waitResults(t, map[Result]int{ResultSent: 3})
	})
}

// Every channel gets each notification once, and its results start at
// zero.
func TestChannels(t *testing.T) {
	f := newFixture(t, config.WebhookJSON, "https://hooks.example.com/x", withSMTP(smtpServer()))

	if got := f.metrics.channels; !slices.Equal(got, []Channel{ChannelWebhook, ChannelSMTP}) {
		t.Errorf("channels = %v", got)
	}

	f.n.SnapshotFinished("/data/app", backend.SnapshotResult{Outcome: backend.SnapshotFailed, Error: "disk full"})
	f.n.SnapshotFinished("/data/app", backend.SnapshotResult{Outcome: backend.SnapshotFailed, Error: "disk full"})
	f.n.SnapshotFinished("/data/app", backend.SnapshotResult{Outcome: backend.SnapshotSucceeded})

	for _, o := range f.n.outboxes {
		var got []Kind

		for len(o.queue) > 0 {
			got = append(got, (<-o.queue).Kind)
		}

		if !slices.Equal(got, []Kind{SnapshotFailed, SnapshotRecovered}) {
			t.Errorf("%s: events = %v", o.channel, got)
		}
	}

	f = newFixture(t, "", "", smtpOnly, withSMTP(smtpServer()))
	if got := f.metrics.channels; !slices.Equal(got, []Channel{ChannelSMTP}) {
		t.Errorf("channels = %v", got)
	}

	if _, err := New(Options{}); err == nil {
		t.Error("New without a channel succeeded")
	}
}

func TestSMTPOptionErrors(t *testing.T) {
	from, to := &mail.Address{Address: "a@example.com"}, []*mail.Address{{Address: "b@example.com"}}

	for name, o := range map[string]SMTPOptions{
		"no host":      {From: from, To: to},
		"bad port":     {Host: "mail", Port: 70000, From: from, To: to},
		"bad TLS mode": {Host: "mail", TLS: "ssl", From: from, To: to},
		"no sender":    {Host: "mail", To: to},
		"no recipient": {Host: "mail", From: from},
	} {
		if _, err := New(Options{SMTP: &o}); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
}

// The message stays within the line lengths of RFC 5322 and gives nothing
// of the event a way to add a header.
func TestSMTPMessage(t *testing.T) {
	var to []*mail.Address
	for _, a := range []string{"one", "two", "three", "four", "five", "six", "seven", "eight"} {
		to = append(to, &mail.Address{Name: "Team " + a, Address: a + "@example.com"})
	}

	s, err := newSMTP(SMTPOptions{Host: "mail", From: &mail.Address{Address: "backups@example.com"}, To: to})
	if err != nil {
		t.Fatal(err)
	}

	e := hookEvent()
	e.Path = "/data/ünïcödé/" + strings.Repeat("deep/", 20) + "app\r\nBcc: evil@example.com"
	e.Error = strings.Repeat("x", 300)

	raw := string(s.message(e))

	header, _, _ := strings.Cut(raw, "\r\n\r\n")
	for line := range strings.SplitSeq(raw, "\r\n") {
		if len(line) > 76 {
			t.Errorf("line of %d characters: %q", len(line), line)
		}
	}

	for line := range strings.SplitSeq(header, "\r\n") {
		if line == "" || strings.TrimSpace(line) == "" {
			t.Errorf("empty line in the header:\n%s", header)
		}
	}

	msg, err := mail.ReadMessage(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}

	if bcc := msg.Header.Get("Bcc"); bcc != "" {
		t.Errorf("Bcc = %q", bcc)
	}

	subject, err := new(mime.WordDecoder).DecodeHeader(msg.Header.Get("Subject"))
	if err != nil || subject != e.Title() {
		t.Errorf("subject = %q (%v), want %q", subject, err, e.Title())
	}

	list, err := msg.Header.AddressList("To")
	if err != nil || len(list) != len(to) || list[7].Name != "Team eight" {
		t.Errorf("To = %v, %v", list, err)
	}

	body, err := io.ReadAll(quotedprintable.NewReader(msg.Body))
	if err != nil || !strings.Contains(strings.ReplaceAll(string(body), "\r\n", "\n"), "error: "+e.Error+"\n") {
		t.Errorf("body = %q, %v", body, err)
	}
}

func TestLoginAuth(t *testing.T) {
	a := &loginAuth{username: "watcher", password: smtpPassword, host: "mail"}

	if _, _, err := a.Start(&smtp.ServerInfo{Name: "mail"}); err == nil || err.Error() != "unencrypted connection" {
		t.Errorf("Start without TLS = %v", err)
	}

	if mech, resp, err := a.Start(&smtp.ServerInfo{Name: "mail", TLS: true}); mech != "LOGIN" || resp != nil || err != nil {
		t.Errorf("Start = %q, %q, %v", mech, resp, err)
	}

	for i, want := range []string{"watcher", smtpPassword} {
		if got, err := a.Next([]byte("Prompt:"), true); string(got) != want || err != nil {
			t.Errorf("Next %d = %q, %v", i, got, err)
		}
	}

	if _, err := a.Next([]byte("Prompt:"), true); err == nil {
		t.Error("a third challenge succeeded")
	}

	if got, err := a.Next(nil, false); got != nil || err != nil {
		t.Errorf("Next at the end = %q, %v", got, err)
	}
}
