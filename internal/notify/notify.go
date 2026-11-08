// Package notify sends a notification when a path starts failing and when
// it is fine again: to NOTIFY_WEBHOOK_URL, by e-mail through
// NOTIFY_SMTP_HOST, or both.
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
//   - a hook command that fails, as when it exits with a code other than
//     zero, makes that hook of that container failing; the next run of it
//     that succeeds makes it fine again. Each hook of each container is
//     followed apart, and forgotten when the container no longer backs the
//     path up or has no hooks;
//   - a container the watcher stopped or paused for a snapshot and cannot
//     bring back makes that container failing; once it runs again it is
//     fine again;
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
// Each notification goes to every channel. A channel has its own queue
// and its own sender, which tries each notification up to Attempts times,
// with a delay that doubles, and Timeout for each attempt, so a channel
// that is slow or down never holds back another; the reconciler never
// waits for either. A full queue drops the notification for its channel.
// Neither the webhook URL, which may hold a token, nor the SMTP password
// is ever logged or shown in an error.
package notify

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
)

// Defaults of Options.
const (
	// DefaultTimeout bounds a request to the webhook, DefaultSMTPTimeout a
	// whole SMTP conversation, from the connection to the end of the
	// message.
	DefaultTimeout       = 10 * time.Second
	DefaultSMTPTimeout   = 30 * time.Second
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
	// ResultSent: the webhook or the SMTP server accepted it.
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

// Metrics counts the notifications by channel and by what became of them.
type Metrics interface {
	// NotificationChannel starts the counts of a channel at zero; New calls
	// it for each channel it sends to.
	NotificationChannel(c Channel)
	// Notification counts a notification of a channel.
	Notification(c Channel, r Result)
}

// Options configure a Notifier.
type Options struct {
	// Webhook posts the notifications to NOTIFY_WEBHOOK_URL, SMTP sends
	// them by e-mail; nil leaves the channel out. One is required.
	Webhook *WebhookOptions
	SMTP    *SMTPOptions

	// ErrorAfter is how long an error the watcher retries must last before
	// it is notified; zero means config.DefaultNotifyErrorAfter.
	ErrorAfter time.Duration

	// Each channel uses these on its own. Timeout bounds one attempt:
	// zero means DefaultTimeout for the webhook and DefaultSMTPTimeout for
	// SMTP. Attempts is how many are made. The delay between them starts
	// at RetryDelay and doubles; a Retry-After of the webhook may make it
	// longer, up to MaxRetryDelay. Zero means the default.
	Timeout       time.Duration
	Attempts      int
	RetryDelay    time.Duration
	MaxRetryDelay time.Duration
	// QueueSize is how many notifications may wait for the sender of a
	// channel.
	QueueSize int

	// Logger receives the log; nil discards it.
	Logger *slog.Logger
	// Metrics counts the notifications; nil counts nothing.
	Metrics Metrics
	// Now is the clock of ErrorAfter; nil means time.Now.
	Now func() time.Time
}

// Notifier sends the notifications. Create it with New and start its
// senders with Run. It implements reconcile.Metrics and reconcile.Observer
// and is safe for concurrent use.
type Notifier struct {
	opts Options
	// outboxes hold a queue for each channel.
	outboxes []*outbox

	mu sync.Mutex
	// host is kopia server's host name, for the text.
	host string
	// owners are the containers of each managed path.
	owners map[string][]string
	// snapshots and passes hold the failing paths, hooks the failing hooks
	// by hookKey; reasons the reasons of the errors a pass reported, until
	// its PassDone.
	snapshots map[string]*track
	passes    map[string]*track
	hooks     map[hookKey]*track
	returns   map[returnKey]*track
	reasons   map[string][]reconcile.Reason
}

// returnKey is a container the watcher took down for a snapshot of a path.
type returnKey struct{ path, container string }

// hookKey is a hook of a container of a path.
type hookKey struct{ path, hook, container string }

// track is a failing path.
type track struct {
	since    time.Time
	notified bool
}

var (
	_ reconcile.Metrics  = (*Notifier)(nil)
	_ reconcile.Observer = (*Notifier)(nil)
)

// New returns a Notifier that sends to the channels opts sets up.
func New(opts Options) (*Notifier, error) {
	if opts.Webhook == nil && opts.SMTP == nil {
		return nil, errors.New("notify: neither a webhook nor SMTP is set up")
	}

	opts.ErrorAfter = orDefault(opts.ErrorAfter, config.DefaultNotifyErrorAfter)
	opts.RetryDelay = orDefault(opts.RetryDelay, DefaultRetryDelay)
	opts.MaxRetryDelay = max(orDefault(opts.MaxRetryDelay, DefaultMaxRetryDelay), opts.RetryDelay)

	if opts.Attempts <= 0 {
		opts.Attempts = DefaultAttempts
	}

	if opts.QueueSize <= 0 {
		opts.QueueSize = DefaultQueueSize
	}

	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}

	if opts.Now == nil {
		opts.Now = time.Now
	}

	n := &Notifier{
		opts:      opts,
		owners:    map[string][]string{},
		snapshots: map[string]*track{},
		passes:    map[string]*track{},
		hooks:     map[hookKey]*track{},
		returns:   map[returnKey]*track{},
		reasons:   map[string][]reconcile.Reason{},
	}

	if opts.Webhook != nil {
		w, err := newWebhook(*opts.Webhook)
		if err != nil {
			return nil, err
		}

		n.addOutbox(ChannelWebhook, w, DefaultTimeout, "webhook", w.safe, "format", w.format)
	}

	if opts.SMTP != nil {
		s, err := newSMTP(*opts.SMTP)
		if err != nil {
			return nil, err
		}

		n.addOutbox(ChannelSMTP, s, DefaultSMTPTimeout, "smtp", s.addr)
	}

	return n, nil
}

// addOutbox adds a channel; logAttrs say where it sends to, without
// secrets.
func (n *Notifier) addOutbox(c Channel, s sender, timeout time.Duration, logAttrs ...any) {
	n.outboxes = append(n.outboxes, &outbox{
		channel: c,
		sender:  s,
		timeout: orDefault(n.opts.Timeout, timeout),
		queue:   make(chan Event, n.opts.QueueSize),
		log:     n.opts.Logger.With(append([]any{"channel", string(c)}, logAttrs...)...),
		opts:    &n.opts,
	})

	if n.opts.Metrics != nil {
		n.opts.Metrics.NotificationChannel(c)
	}
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

	// The failing hooks of containers that no longer back the path up, or
	// no longer have hooks, are forgotten: a failure after they come back
	// is a new one.
	for k := range n.hooks {
		if k.path == path && (!s.Hooks || !slices.Contains(s.Owners, k.container)) {
			delete(n.hooks, k)
		}
	}
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

	for k := range n.hooks {
		if k.path == path {
			delete(n.hooks, k)
		}
	}

	for k := range n.returns {
		if k.path == path {
			delete(n.returns, k)
		}
	}
}

// ContainerReturned implements reconcile.Metrics: a container the watcher
// cannot bring back after a snapshot of a path makes it failing, its
// return fine again.
func (n *Notifier) ContainerReturned(path string, res reconcile.ReturnResult) {
	now := n.opts.Now()
	key := returnKey{path: path, container: res.Container}

	n.mu.Lock()
	defer n.mu.Unlock()

	t := n.returns[key]

	switch {
	case res.Error != "":
		if t != nil {
			return
		}

		n.returns[key] = &track{since: now, notified: true}

		e := n.event(ReturnFailed, path, now, now, res.Mode, res.Error)
		e.Container = res.Container
		n.enqueue(e)
	case t != nil:
		delete(n.returns, key)

		e := n.event(ReturnRecovered, path, now, t.since, res.Mode, "")
		e.Container, e.Note = res.Container, res.Note
		n.enqueue(e)
	}
}

// HookFinished implements reconcile.Metrics: a hook of a container of a
// path that fails makes it failing, the next run of it that succeeds fine
// again, each hook of each container on its own.
func (n *Notifier) HookFinished(path string, res reconcile.HookResult) {
	now := n.opts.Now()
	key := hookKey{path: path, hook: res.Hook, container: res.Container}

	n.mu.Lock()
	defer n.mu.Unlock()

	t := n.hooks[key]

	switch {
	case res.Error != "":
		if t != nil {
			return
		}

		n.hooks[key] = &track{since: now, notified: true}

		e := n.event(HookFailed, path, now, now, res.Hook, res.Error)
		e.Container = res.Container
		n.enqueue(e)
	case t != nil:
		delete(n.hooks, key)

		e := n.event(HookRecovered, path, now, t.since, res.Hook, "")
		e.Container = res.Container
		n.enqueue(e)
	}
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

// enqueue hands e to the sender of every channel without waiting.
func (n *Notifier) enqueue(e Event) {
	for _, o := range n.outboxes {
		o.enqueue(e)
	}
}

// Run sends the queued notifications, each channel on its own, until ctx
// is done; those still waiting then are dropped.
func (n *Notifier) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for _, o := range n.outboxes {
		wg.Go(func() { o.run(ctx) })
	}

	wg.Wait()
}

// truncate cuts s to at most n runes, marking the cut.
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}

	return string(r[:n-1]) + "…"
}
