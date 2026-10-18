package docker

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/moby/moby/api/types/events"
)

// fakeEngine hands every Events subscription to the test.
type fakeEngine struct {
	subs chan *fakeSub
}

type fakeSub struct {
	ctx    context.Context
	since  time.Time
	events chan Event
	errs   chan error
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{subs: make(chan *fakeSub)}
}

func (*fakeEngine) Containers(context.Context) ([]Container, error) {
	return nil, errors.New("not implemented")
}

func (*fakeEngine) Inspect(context.Context, string) (Container, error) {
	return Container{}, errors.New("not implemented")
}

func (f *fakeEngine) Events(ctx context.Context, since time.Time) (<-chan Event, <-chan error) {
	s := &fakeSub{ctx: ctx, since: since, events: make(chan Event), errs: make(chan error, 1)}

	go func() {
		select {
		case f.subs <- s:
		case <-ctx.Done():
		}
	}()

	return s.events, s.errs
}

func (f *fakeEngine) nextSub(t *testing.T) *fakeSub {
	t.Helper()

	select {
	case s := <-f.subs:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("no subscription")
		return nil
	}
}

func (s *fakeSub) send(t *testing.T, evs ...Event) {
	t.Helper()

	for _, ev := range evs {
		select {
		case s.events <- ev:
		case <-time.After(5 * time.Second):
			t.Fatalf("watcher did not take %+v", ev)
		}
	}
}

func (s *fakeSub) fail(err error) {
	s.errs <- err
}

// watchRun is a running Watcher with its delivered events and the delays
// it waited between subscriptions.
type watchRun struct {
	got    chan Event
	delays chan time.Duration
}

// startWatcher runs a Watcher on e until the test ends. settle is the
// quiet time after which a subscription counts as established; the delays
// between subscriptions are recorded instead of slept.
func startWatcher(t *testing.T, e Engine, settle time.Duration) *watchRun {
	t.Helper()

	w := NewWatcher(e, nil)
	w.settle = settle

	r := &watchRun{got: make(chan Event, 100), delays: make(chan time.Duration, 100)}
	w.wait = func(ctx context.Context, d time.Duration) bool {
		r.delays <- d
		return ctx.Err() == nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)
		w.Run(ctx, func(ev Event) { r.got <- ev })
	}()

	t.Cleanup(func() {
		cancel()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})

	return r
}

func (r *watchRun) next(t *testing.T) Event {
	t.Helper()

	select {
	case ev := <-r.got:
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("no event delivered")
		return Event{}
	}
}

// expect checks the next delivered events, by kind and container.
func (r *watchRun) expect(t *testing.T, want ...Event) {
	t.Helper()

	for _, w := range want {
		got := r.next(t)
		if got.Kind != w.Kind || got.ContainerID != w.ContainerID || got.Action != w.Action || !got.Time.Equal(w.Time) {
			t.Fatalf("delivered %v %s %s at %d, want %v %s %s at %d",
				got.Kind, got.Action, got.ContainerID, got.Time.UnixNano(),
				w.Kind, w.Action, w.ContainerID, w.Time.UnixNano())
		}
	}
}

func (r *watchRun) nextDelay(t *testing.T) time.Duration {
	t.Helper()

	select {
	case d := <-r.delays:
		return d
	case <-time.After(5 * time.Second):
		t.Fatal("no reconnect")
		return 0
	}
}

var resync = Event{Kind: KindResync}

func ev(kind Kind, action, id string, nano int64) Event {
	return Event{Kind: kind, Action: action, ContainerID: id, Name: id, Time: time.Unix(0, nano)}
}

func TestWatcherFirstSubscriptionStartsWithResync(t *testing.T) {
	e := newFakeEngine()
	r := startWatcher(t, e, time.Hour)

	sub := e.nextSub(t)
	if !sub.since.IsZero() {
		t.Errorf("since = %v, want zero", sub.since)
	}

	start := ev(KindStarted, "start", "a", 100)
	sub.send(t, start)
	r.expect(t, resync, start)
}

func TestWatcherQuietFirstSubscriptionResyncsAfterSettle(t *testing.T) {
	e := newFakeEngine()
	r := startWatcher(t, e, 10*time.Millisecond)

	e.nextSub(t)
	r.expect(t, resync)

	select {
	case got := <-r.got:
		t.Fatalf("unexpected %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatcherResumesSinceLastEvent(t *testing.T) {
	e := newFakeEngine()
	r := startWatcher(t, e, time.Hour)

	a := ev(KindStarted, "start", "a", 100)
	b := ev(KindDied, "die", "b", 200)
	c := ev(KindChanged, "create", "c", 200) // the same nanosecond as b
	d := ev(KindRemoved, "destroy", "b", 300)
	f := ev(KindStarted, "start", "c", 400)

	sub := e.nextSub(t)
	sub.send(t, a, b, c)
	r.expect(t, resync, a, b, c)
	sub.fail(io.EOF)

	if delay := r.nextDelay(t); delay != DefaultMinDelay {
		t.Errorf("delay = %v, want %v", delay, DefaultMinDelay)
	}

	sub = e.nextSub(t)
	if !sub.since.Equal(time.Unix(0, 200)) {
		t.Errorf("since = %d, want 200", sub.since.UnixNano())
	}

	// The replay repeats b and c; their arrival proves nothing was lost,
	// so there is no resync.
	sub.send(t, b, c, d, f)
	r.expect(t, d, f)
}

func TestWatcherResyncsAfterGap(t *testing.T) {
	e := newFakeEngine()
	r := startWatcher(t, e, time.Hour)

	a := ev(KindStarted, "start", "a", 100)
	b := ev(KindDied, "die", "a", 500)

	sub := e.nextSub(t)
	sub.send(t, a)
	r.expect(t, resync, a)
	sub.fail(errors.New("connection reset"))
	r.nextDelay(t)

	// The daemon no longer has a: the replay starts later.
	sub = e.nextSub(t)
	sub.send(t, b)
	r.expect(t, resync, b)
}

func TestWatcherResyncsWhenReconnectHasNoReplay(t *testing.T) {
	e := newFakeEngine()
	r := startWatcher(t, e, 20*time.Millisecond)

	a := ev(KindStarted, "start", "a", 100)

	sub := e.nextSub(t)
	sub.send(t, a)
	r.expect(t, resync, a)
	sub.fail(io.EOF)
	r.nextDelay(t)

	// A restarted daemon replays nothing.
	sub = e.nextSub(t)
	if !sub.since.Equal(a.Time) {
		t.Errorf("since = %v, want %v", sub.since, a.Time)
	}

	r.expect(t, resync)
}

func TestWatcherBackoff(t *testing.T) {
	e := newFakeEngine()
	r := startWatcher(t, e, time.Hour)

	refused := errors.New("connection refused")

	for _, want := range []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 30 * time.Second, 30 * time.Second,
	} {
		e.nextSub(t).fail(refused)

		if got := r.nextDelay(t); got != want {
			t.Fatalf("delay = %v, want %v", got, want)
		}
	}

	// A subscription that worked resets the delay.
	sub := e.nextSub(t)
	sub.send(t, ev(KindStarted, "start", "a", 100))
	r.expect(t, resync, ev(KindStarted, "start", "a", 100))
	sub.fail(io.EOF)

	if got := r.nextDelay(t); got != time.Second {
		t.Fatalf("delay after a working subscription = %v, want 1s", got)
	}

	e.nextSub(t).fail(refused)

	if got := r.nextDelay(t); got != 2*time.Second {
		t.Fatalf("delay = %v, want 2s", got)
	}
}

func TestWatcherStopsSubscriptionOnCancel(t *testing.T) {
	e := newFakeEngine()
	w := NewWatcher(e, nil)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)
		w.Run(ctx, func(Event) {})
	}()

	sub := e.nextSub(t)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}

	if sub.ctx.Err() == nil {
		t.Error("subscription context not canceled")
	}
}

func TestWatcherStopsWhileWaiting(t *testing.T) {
	e := newFakeEngine()
	w := NewWatcher(e, nil)
	w.minDelay = time.Hour

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)
		w.Run(ctx, func(Event) {})
	}()

	e.nextSub(t).fail(errors.New("connection refused"))
	// Run is now in its hour-long wait.
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestCursor(t *testing.T) {
	var c cursor

	a := ev(KindStarted, "start", "a", 100)
	if c.delivered(a) || !c.since().IsZero() {
		t.Fatal("empty cursor knows an event")
	}

	b := ev(KindDied, "die", "a", 200)
	b2 := ev(KindChanged, "create", "b", 200)

	for _, e := range []Event{a, b, b2} {
		c.record(e)
	}

	if !c.since().Equal(time.Unix(0, 200)) {
		t.Errorf("since = %v", c.since())
	}

	for _, e := range []Event{b, b2} {
		if !c.delivered(e) {
			t.Errorf("%+v not delivered", e)
		}
	}

	// Only the last nanosecond is replayed, and only the same event is a
	// repeat there.
	for _, e := range []Event{a, ev(KindStarted, "start", "a", 200), ev(KindDied, "die", "a", 300)} {
		if c.delivered(e) {
			t.Errorf("%+v counted as delivered", e)
		}
	}

	// An event that trails the latest one does not move the cursor.
	c.record(ev(KindStarted, "start", "c", 150))

	if !c.since().Equal(time.Unix(0, 200)) || !c.delivered(b) {
		t.Errorf("cursor moved back: since %v", c.since())
	}

	c.record(ev(KindRemoved, "destroy", "a", 300))

	if c.delivered(b) || !c.delivered(ev(KindRemoved, "destroy", "a", 300)) {
		t.Error("cursor did not move on")
	}
}

// TestWatcherWithDocker runs Watcher on the SDK client against the fake
// daemon: a failed request, a stream that ends, and a reconnect with
// since and replay.
func TestWatcherWithDocker(t *testing.T) {
	f := newFakeDocker()
	f.set(func(f *fakeDocker) { f.failEvents = 1 })
	r := startWatcher(t, newFakeDockerClient(t, f), time.Hour)

	msg := func(action events.Action, id string, nano int64) events.Message {
		return events.Message{
			Type:     events.ContainerEventType,
			Action:   action,
			Actor:    events.Actor{ID: id, Attributes: map[string]string{"name": id, "kopia.enable": "true"}},
			Time:     nano / int64(time.Second),
			TimeNano: nano,
		}
	}

	const (
		t1 = 1_760_000_000_000_000_100
		t2 = 1_760_000_000_000_000_200
		t3 = 1_760_000_001_000_000_000
	)

	// The first request fails with 500.
	if d := r.nextDelay(t); d != DefaultMinDelay {
		t.Errorf("delay = %v", d)
	}

	sub := f.nextSub(t)
	if sub.query.Has("since") {
		t.Errorf("first subscription has since: %v", sub.query)
	}

	sub.send <- msg(events.ActionStart, "a", t1)
	sub.send <- msg(events.ActionKill, "b", t2)
	sub.send <- msg(events.ActionDie, "b", t2)
	r.expect(t, resync, ev(KindStarted, "start", "a", t1), ev(KindDied, "die", "b", t2))
	close(sub.send)

	if d := r.nextDelay(t); d != DefaultMinDelay {
		t.Errorf("delay = %v", d)
	}

	sub = f.nextSub(t)
	if got := sub.query.Get("since"); got != "1760000000.000000200" {
		t.Errorf("since = %q", got)
	}

	sub.send <- msg(events.ActionDie, "b", t2)
	sub.send <- msg(events.ActionDestroy, "b", t3)

	got := r.next(t)
	if got.Kind != KindRemoved || got.ContainerID != "b" || got.Labels["kopia.enable"] != "true" {
		t.Fatalf("delivered %+v, want the removal of b with its labels", got)
	}
}
