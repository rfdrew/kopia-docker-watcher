// Package notify sends a notification to NOTIFY_WEBHOOK_URL when a path
// starts failing and when it is fine again.
//
// A Notifier follows two things per path: the snapshots the watcher
// queued, through reconcile.Metrics, and the passes over it, through
// reconcile.Observer. Each is either fine or failing, and only a change
// is notified, once:
//
//   - a snapshot that failed or whose end is unknown makes the snapshots
//     of the path failing; the next one that succeeds, or that saved
//     nothing as nothing changed, makes them fine again. Incomplete and
//     canceled snapshots change nothing. Only the snapshots the watcher
//     queued count: those kopia server takes on its schedule are for the
//     notification profiles of kopia server;
//   - a pass that ends with an error, or with a problem with the labels,
//     makes the path failing: at once for an error the watcher does not
//     retry, such as a policy kopia server rejects, and for an error it
//     retries only once the path has failed for ErrorAfter; the next pass
//     without either makes it fine again. A path that failed for less
//     than that and is fine again is not notified at all.
//
// What a Notifier remembers is lost when the watcher restarts, so a
// problem that lasts is notified once more after a restart.
//
// Notifications wait in a queue for one sender, which tries each up to
// Attempts times, with a delay that doubles, and Timeout for each attempt;
// the reconciler never waits for it. A full queue drops the notification.
// The URL is never logged, nor shown in an error, as it may hold a token:
// only its scheme and host are.
package notify

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
)

// Defaults of Options.
const (
	DefaultTimeout       = 10 * time.Second
	DefaultAttempts      = 4
	DefaultRetryDelay    = 2 * time.Second
	DefaultMaxRetryDelay = time.Minute
	DefaultQueueSize     = 64
)

// maxErrorLen bounds the error text in a notification.
const maxErrorLen = 1000

// Result is what became of a notification, for the metrics.
type Result string

// Results.
const (
	// ResultSent: the webhook accepted it.
	ResultSent Result = "sent"
	// ResultFailed: every attempt failed, or one failed in a way that
	// trying again does not fix.
	ResultFailed Result = "failed"
	// ResultDropped: the queue was full, or the watcher stopped before it
	// was sent.
	ResultDropped Result = "dropped"
)

// Results returns every Result.
func Results() []Result { return []Result{ResultSent, ResultFailed, ResultDropped} }

// Metrics counts the notifications by what became of them.
type Metrics interface {
	Notification(r Result)
}

// Options configure a Notifier.
type Options struct {
	// URL is NOTIFY_WEBHOOK_URL; required.
	URL string
	// Format is one of config.WebhookFormats; empty means json.
	Format string
	// ErrorAfter is how long an error the watcher retries must last before
	// it is notified; zero means config.DefaultNotifyErrorAfter.
	ErrorAfter time.Duration

	// Timeout bounds one attempt; Attempts is how many are made. The
	// delay between them starts at RetryDelay and doubles; a Retry-After
	// of the webhook may make it longer, up to MaxRetryDelay. Zero means
	// the default.
	Timeout       time.Duration
	Attempts      int
	RetryDelay    time.Duration
	MaxRetryDelay time.Duration
	// QueueSize is how many notifications may wait for the sender.
	QueueSize int

	// Transport sends the requests; nil means http.DefaultTransport.
	Transport http.RoundTripper
	// Logger receives the log; nil discards it.
	Logger *slog.Logger
	// Metrics counts the notifications; nil counts nothing.
	Metrics Metrics
	// Now is the clock of ErrorAfter; nil means time.Now.
	Now func() time.Time
}

// Notifier sends the notifications. Create it with New and start its
// sender with Run. It implements reconcile.Metrics and reconcile.Observer
// and is safe for concurrent use.
type Notifier struct {
	opts   Options
	url    *url.URL
	safe   string
	client *http.Client
	log    *slog.Logger
	queue  chan Event

	mu sync.Mutex
	// host is kopia server's host name, for the text.
	host string
	// owners are the containers of each managed path.
	owners map[string][]string
	// snapshots and passes hold the failing paths; reasons the reasons of
	// the errors a pass reported, until its PassDone.
	snapshots map[string]*track
	passes    map[string]*track
	reasons   map[string][]reconcile.Reason
}

// track is a failing path.
type track struct {
	since    time.Time
	notified bool
}

var (
	_ reconcile.Metrics  = (*Notifier)(nil)
	_ reconcile.Observer = (*Notifier)(nil)
)

// New returns a Notifier.
func New(opts Options) (*Notifier, error) {
	u, err := url.Parse(opts.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("notify: the webhook URL is not an http or https URL")
	}

	if opts.Format == "" {
		opts.Format = config.WebhookJSON
	}

	if !slices.Contains(config.WebhookFormats(), opts.Format) {
		return nil, fmt.Errorf("notify: unknown format %q", opts.Format)
	}

	opts.ErrorAfter = orDefault(opts.ErrorAfter, config.DefaultNotifyErrorAfter)
	opts.Timeout = orDefault(opts.Timeout, DefaultTimeout)
	opts.RetryDelay = orDefault(opts.RetryDelay, DefaultRetryDelay)
	opts.MaxRetryDelay = max(orDefault(opts.MaxRetryDelay, DefaultMaxRetryDelay), opts.RetryDelay)

	if opts.Attempts <= 0 {
		opts.Attempts = DefaultAttempts
	}

	if opts.QueueSize <= 0 {
		opts.QueueSize = DefaultQueueSize
	}

	if opts.Transport == nil {
		opts.Transport = http.DefaultTransport
	}

	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}

	if opts.Now == nil {
		opts.Now = time.Now
	}

	n := &Notifier{
		opts: opts,
		url:  u,
		safe: u.Scheme + "://" + u.Host,
		client: &http.Client{
			Transport: opts.Transport,
			// A redirect would send the notification, and maybe the token
			// in the URL, where the URL does not point.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		queue:     make(chan Event, opts.QueueSize),
		owners:    map[string][]string{},
		snapshots: map[string]*track{},
		passes:    map[string]*track{},
		reasons:   map[string][]reconcile.Reason{},
	}
	n.log = opts.Logger.With("webhook", n.safe, "format", opts.Format)

	return n, nil
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}

	return d
}

// SetHost sets the host name the notifications name: kopia server's,
// which its sources carry.
func (n *Notifier) SetHost(host string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.host = host
}

// ReconcileError implements reconcile.Metrics: it keeps the reason for the
// PassDone of the pass. Errors about no path, such as a full reconcile
// that cannot list the containers, are not notified: no pass follows them.
func (n *Notifier) ReconcileError(path string, reason reconcile.Reason) {
	if path == "" {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	n.reasons[path] = append(n.reasons[path], reason)
}

// retried reports whether the watcher retries errors of a reason.
func retried(r reconcile.Reason) bool {
	return r == reconcile.ReasonTemporary || r == reconcile.ReasonUnavailable || r == reconcile.ReasonDocker
}

// Source implements reconcile.Metrics: it keeps the containers of the
// path.
func (n *Notifier) Source(path string, s reconcile.SourceMetrics) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.owners[path] = slices.Clone(s.Owners)
}

// LastSuccess implements reconcile.Metrics; it does nothing.
func (n *Notifier) LastSuccess(string, time.Time) {}

// ForgetSource implements reconcile.Metrics: the watcher no longer manages
// the path, so its snapshots are forgotten, failing or not.
func (n *Notifier) ForgetSource(path string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.owners, path)
	delete(n.snapshots, path)
}

// SnapshotFinished implements reconcile.Metrics.
func (n *Notifier) SnapshotFinished(path string, res backend.SnapshotResult) {
	now := n.opts.Now()

	n.mu.Lock()
	defer n.mu.Unlock()

	t := n.snapshots[path]

	switch res.Outcome {
	case backend.SnapshotFailed, backend.SnapshotUnknown:
		if t != nil {
			return
		}

		n.snapshots[path] = &track{since: now, notified: true}
		n.enqueue(n.event(SnapshotFailed, path, now, now, string(res.Outcome), res.Error))
	case backend.SnapshotSucceeded, backend.SnapshotIdentical:
		if t == nil {
			return
		}

		delete(n.snapshots, path)
		n.enqueue(n.event(SnapshotRecovered, path, now, t.since, string(res.Outcome), ""))
	}
}

// PassDone implements reconcile.Observer.
func (n *Notifier) PassDone(path string, rep reconcile.PassReport) {
	now := n.opts.Now()

	n.mu.Lock()
	defer n.mu.Unlock()

	reasons := n.reasons[path]
	delete(n.reasons, path)

	t := n.passes[path]

	if !rep.Failed() {
		if t != nil && t.notified {
			n.enqueue(n.event(ReconcileRecovered, path, now, t.since, "", ""))
		}

		delete(n.passes, path)

		return
	}

	if t == nil {
		t = &track{since: now}
		n.passes[path] = t
	}

	// A problem with the labels is not retried, even in a pass that ends
	// with an error that is.
	if t.notified || (rep.Retry && len(rep.Problems) == 0 && now.Sub(t.since) < n.opts.ErrorAfter) {
		return
	}

	t.notified = true

	// The reason of an error that is not retried tells more.
	var reason reconcile.Reason

	for _, r := range reasons {
		if reason == "" || (retried(reason) && !retried(r)) {
			reason = r
		}
	}

	if reason == "" {
		reason = reconcile.ReasonFailed
		if rep.Err == nil {
			reason = reconcile.ReasonLabels
		}
	}

	n.enqueue(n.event(ReconcileFailed, path, now, t.since, string(reason), passError(rep)))
}

// passError is the text of what went wrong in a pass.
func passError(rep reconcile.PassReport) string {
	var msgs []string

	if rep.Err != nil {
		msgs = append(msgs, rep.Err.Error())
	}

	for _, p := range rep.Problems {
		msgs = append(msgs, p.Error())
	}

	return strings.Join(msgs, "; ")
}

// event makes an event about path; n.mu is held.
func (n *Notifier) event(kind Kind, path string, now, since time.Time, reason, errText string) Event {
	return Event{
		Kind:       kind,
		Path:       path,
		Containers: slices.Clone(n.owners[path]),
		Host:       n.host,
		Reason:     reason,
		Error:      truncate(errText, maxErrorLen),
		Since:      since,
		Time:       now,
	}
}

// enqueue hands e to the sender without waiting; a full queue drops it.
func (n *Notifier) enqueue(e Event) {
	select {
	case n.queue <- e:
	default:
		n.count(ResultDropped)
		n.log.Warn("dropping a notification: too many are waiting to be sent", "event", string(e.Kind), "path", e.Path)
	}
}

func (n *Notifier) count(r Result) {
	if n.opts.Metrics != nil {
		n.opts.Metrics.Notification(r)
	}
}

// Run sends the queued notifications until ctx is done; those still
// waiting then are dropped.
func (n *Notifier) Run(ctx context.Context) {
	for {
		select {
		case e := <-n.queue:
			n.deliver(ctx, e)
		case <-ctx.Done():
			dropped := 0

			for {
				select {
				case <-n.queue:
					dropped++

					n.count(ResultDropped)
				default:
					if dropped > 0 {
						n.log.Warn("dropping the notifications not sent before the stop", "count", dropped)
					}

					return
				}
			}
		}
	}
}

// deliver sends e, trying again after a temporary failure.
func (n *Notifier) deliver(ctx context.Context, e Event) {
	delay := n.opts.RetryDelay

	for attempt := 1; ; attempt++ {
		wait, err := n.send(ctx, e)

		switch {
		case err == nil:
			n.count(ResultSent)
			n.log.Info("notification sent", "event", string(e.Kind), "path", e.Path)

			return
		case ctx.Err() != nil:
			n.count(ResultDropped)
			return
		case wait < 0 || attempt >= n.opts.Attempts:
			n.count(ResultFailed)
			n.log.Warn("cannot send the notification", "event", string(e.Kind), "path", e.Path,
				"attempts", attempt, "error", err.Error())

			return
		}

		wait = min(max(wait, delay), n.opts.MaxRetryDelay)
		n.log.Debug("cannot send the notification; trying again", "event", string(e.Kind), "path", e.Path,
			"attempt", attempt, "delay", wait.String(), "error", err.Error())

		t := time.NewTimer(wait)

		select {
		case <-ctx.Done():
			t.Stop()
			n.count(ResultDropped)

			return
		case <-t.C:
		}

		delay = min(2*delay, n.opts.MaxRetryDelay)
	}
}

// send makes one attempt. On failure wait is how long the webhook asked to
// wait before the next one, zero if it did not ask, negative if trying
// again cannot help.
func (n *Notifier) send(ctx context.Context, e Event) (wait time.Duration, err error) {
	ctx, cancel := context.WithTimeout(ctx, n.opts.Timeout)
	defer cancel()

	req, err := n.request(ctx, e)
	if err != nil {
		return -1, err
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return 0, n.redact(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return 0, nil
	}

	err = fmt.Errorf("%s %s: %s", req.Method, n.safe, resp.Status)
	if msg := strings.TrimSpace(string(body)); msg != "" {
		err = fmt.Errorf("%w: %s", err, truncate(msg, 200))
	}

	switch {
	case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode == http.StatusRequestTimeout,
		resp.StatusCode >= 500:
		return retryAfter(resp.Header.Get("Retry-After")), err
	default:
		return -1, err
	}
}

// redact drops the URL, which may hold a token, from an error of the HTTP
// client.
func (n *Notifier) redact(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s %s: %w", ue.Op, n.safe, ue.Err)
	}

	return err
}

// retryAfter reads a Retry-After header in seconds; zero if there is none.
func retryAfter(v string) time.Duration {
	s, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || s <= 0 {
		return 0
	}

	return time.Duration(s) * time.Second
}

// truncate cuts s to at most n runes, marking the cut.
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}

	return string(r[:n-1]) + "…"
}
