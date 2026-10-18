package docker

import (
	"context"
	"io"
	"log/slog"
	"time"
)

// Defaults of Watcher.
const (
	DefaultMinDelay = time.Second
	DefaultMaxDelay = 30 * time.Second

	// defaultSettle is how long a quiet subscription has to stay up to
	// count as working. The daemon answers /events at once and sends its
	// replay right after the headers.
	defaultSettle = 2 * time.Second

	// replayOverlap is how far before the last delivered event a new
	// subscription starts. The daemon stamps an event before it adds it to
	// its buffer, and publishes it after, so an event stamped just before
	// the last delivered one can sit behind it in the buffer, still
	// unpublished; a replay from the exact time of the last event stops
	// short of it.
	replayOverlap = 5 * time.Second
)

// Watcher follows container events across broken connections, so the
// watcher never ends up alive but deaf. When a subscription ends it
// subscribes again after an exponential delay, from shortly before the
// last event it delivered (see Engine.Events for what the daemon replays).
//
// Events already delivered are skipped. One that an earlier subscription
// delivered showing up again also proves that nothing was lost in between:
// the daemon drops its oldest events first and keeps none across its own
// restart, and its replay brings everything it added after that event.
// When a new subscription brings an event later than the last delivered
// one before that proof, or nothing within a short settle time, Watcher
// sends KindResync first. The first subscription has nothing to resume
// and always starts with KindResync, and so does the one after a
// subscription that ended before it knew whether anything was lost.
type Watcher struct {
	engine   Engine
	log      *slog.Logger
	minDelay time.Duration
	maxDelay time.Duration
	settle   time.Duration
	// wait sleeps for d or until ctx is done; false means ctx is done.
	wait func(ctx context.Context, d time.Duration) bool
}

// NewWatcher returns a Watcher with the default delays. A nil logger
// discards the log.
func NewWatcher(e Engine, log *slog.Logger) *Watcher {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	return &Watcher{
		engine:   e,
		log:      log,
		minDelay: DefaultMinDelay,
		maxDelay: DefaultMaxDelay,
		settle:   defaultSettle,
		wait:     sleep,
	}
}

// Run passes events to handle until ctx is done. handle is called from
// Run's goroutine, one event at a time, and must return quickly: the
// daemon drops events for a subscriber that falls behind.
func (w *Watcher) Run(ctx context.Context, handle func(Event)) {
	var (
		cur   cursor
		delay time.Duration
	)

	for {
		worked, err := w.follow(ctx, &cur, handle)
		if ctx.Err() != nil {
			return
		}

		if worked || delay == 0 {
			delay = w.minDelay
		} else {
			delay = min(2*delay, w.maxDelay)
		}

		w.log.Warn("docker event stream ended, reconnecting", "error", err.Error(), "delay", delay.String())

		if !w.wait(ctx, delay) {
			return
		}
	}
}

// follow runs one subscription until it ends and reports whether it
// worked: it brought an event or stayed up for the settle time.
func (w *Watcher) follow(ctx context.Context, cur *cursor, handle func(Event)) (worked bool, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	since := cur.begin()
	defer cur.release()

	events, errs := w.engine.Events(ctx, since)

	settle := time.NewTimer(w.settle)
	defer settle.Stop()

	// resolved: this subscription is known to continue the previous one,
	// or KindResync has been sent. handled: it passed events on.
	resolved, handled := false, false
	resync := func() {
		resolved, cur.unproven = true, false

		w.log.Info("docker event stream connected, resyncing", sinceAttr(since))
		handle(Event{Kind: KindResync})
	}

	defer func() {
		if !resolved && handled {
			cur.unproven = true
		}
	}()

	for {
		select {
		case ev := <-events:
			worked = true

			if cur.delivered(ev) {
				if !resolved && cur.proves(ev) {
					resolved = true

					w.log.Info("docker event stream resumed", sinceAttr(since))
				}

				continue
			}

			// An event older than the last delivered one is a replayed one
			// that was missed before, and the proof may still follow. A
			// later one means the replay does not reach back far enough.
			if !resolved && (cur.unproven || cur.later(ev)) {
				resync()
			}

			cur.record(ev)
			handled = true

			w.log.Debug("docker event", "action", ev.Action, "container", ev.ContainerID, "name", ev.Name)
			handle(ev)
		case <-settle.C:
			worked = true

			// The replay has arrived by now.
			cur.release()

			if !resolved {
				resync()
			}
		case err := <-errs:
			if err == nil {
				err = io.EOF
			}

			return worked, err
		case <-ctx.Done():
			return worked, ctx.Err()
		}
	}
}

// cursor remembers the delivered events a replay can bring again: those
// from replayOverlap before the latest one on, and while a replay runs
// everything from its since on.
type cursor struct {
	last time.Time
	// seen maps each remembered event to the subscription that delivered
	// it.
	seen map[eventKey]int
	// sub numbers the current subscription.
	sub int
	// hold is the since of the current subscription until its replay is
	// over. After a backward step of the daemon's clock a replayed event
	// can be stamped far after a delivered one that the same replay still
	// brings; the delivered one must not be forgotten in between.
	hold time.Time
	// unproven is set when a subscription passed events on but ended
	// before it knew whether anything was lost; its events prove nothing
	// to the next one.
	unproven bool
}

type eventKey struct {
	action, id string
	nano       int64
}

func keyOf(ev Event) eventKey {
	return eventKey{ev.Action, ev.ContainerID, ev.Time.UnixNano()}
}

// since is where the next subscription starts; zero before any event.
func (c *cursor) since() time.Time {
	if len(c.seen) == 0 {
		return time.Time{}
	}

	return c.last.Add(-replayOverlap)
}

// begin starts a subscription and returns its since.
func (c *cursor) begin() time.Time {
	c.sub++
	c.hold = c.since()

	return c.hold
}

// release ends the hold of the current subscription's replay.
func (c *cursor) release() {
	c.hold = time.Time{}
}

// delivered reports whether ev was already passed on.
func (c *cursor) delivered(ev Event) bool {
	_, ok := c.seen[keyOf(ev)]
	return ok
}

// proves reports whether ev, already delivered, proves that the current
// subscription continues the previous ones: an earlier subscription
// delivered it, and none since has left a gap open. A second copy within
// one subscription only shows that the event was added just as the
// subscription began.
func (c *cursor) proves(ev Event) bool {
	sub, ok := c.seen[keyOf(ev)]
	return ok && sub < c.sub && !c.unproven
}

// later reports whether ev is later than every delivered event.
func (c *cursor) later(ev Event) bool {
	return len(c.seen) == 0 || ev.Time.After(c.last)
}

func (c *cursor) record(ev Event) {
	if c.seen == nil {
		c.seen = make(map[eventKey]int)
	}

	if ev.Time.After(c.last) {
		c.last = ev.Time

		floor := c.floor()
		for k := range c.seen {
			if k.nano < floor.UnixNano() {
				delete(c.seen, k)
			}
		}
	}

	// An event below the floor is not replayed again.
	if !ev.Time.Before(c.floor()) {
		c.seen[keyOf(ev)] = c.sub
	}
}

// floor is the time below which delivered events can be forgotten.
func (c *cursor) floor() time.Time {
	f := c.last.Add(-replayOverlap)
	if !c.hold.IsZero() && c.hold.Before(f) {
		f = c.hold
	}

	return f
}

func sinceAttr(t time.Time) slog.Attr {
	if t.IsZero() {
		return slog.String("since", "now")
	}

	return slog.Time("since", t)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
