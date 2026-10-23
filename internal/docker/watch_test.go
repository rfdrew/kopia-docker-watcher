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

func (*fakeEngine) Exec(context.Context, string, []string) (ExecResult, error) {
	return ExecResult{}, errors.New("not implemented")
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

// base keeps event times realistic, so a since before them stays after
// 1970.
const base = int64(1_760_000_000_000_000_000)

func at(nano int64) time.Time {
	return time.Unix(0, base+nano)
}

func ev(kind Kind, action, id string, nano int64) Event {
	return Event{Kind: kind, Action: action, ContainerID: id, Name: id, Time: at(nano)}
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

	// The new subscription starts a little before the last event.
	sub = e.nextSub(t)
	if want := at(200).Add(-replayOverlap); !sub.since.Equal(want) {
		t.Errorf("since = %v, want %v", sub.since, want)
	}

	// The replay repeats a, b and c; their arrival proves nothing was
	// lost, so there is no resync.
	sub.send(t, a, b, c, d, f)
	r.expect(t, d, f)
}

func TestWatcherDeliversEventMissedBeforeLast(t *testing.T) {
	e := newFakeEngine()
	r := startWatcher(t, e, time.Hour)

	// The daemon stamped m before l and buffered it first, but published
	// it after l; the stream broke in between.
	m := ev(KindDied, "die", "m", 290)
	l := ev(KindStarted, "start", "l", 300)
	n := ev(KindStarted, "start", "m", 400)

	sub := e.nextSub(t)
	sub.send(t, l)
	r.expect(t, resync, l)
	sub.fail(io.EOF)
	r.nextDelay(t)

	// The replay brings both in buffer order: m is delivered, l proves
	// that nothing else is missing.
	sub = e.nextSub(t)
	sub.send(t, m, l, n)
	r.expect(t, m, n)
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
	if want := a.Time.Add(-replayOverlap); !sub.since.Equal(want) {
		t.Errorf("since = %v, want %v", sub.since, want)
	}

	r.expect(t, resync)
}

func TestWatcherResyncsOnce(t *testing.T) {
	e := newFakeEngine()
	r := startWatcher(t, e, 10*time.Millisecond)

	a := ev(KindStarted, "start", "a", 100)

	sub := e.nextSub(t)
	sub.send(t, a)
	r.expect(t, resync, a)

	// The settle timer runs out later; that is no reason for another one.
	select {
	case got := <-r.got:
		t.Fatalf("unexpected %v %s", got.Kind, got.Action)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestWatcherQuietSubscriptionsKeepTheShortDelay(t *testing.T) {
	e := newFakeEngine()
	r := startWatcher(t, e, 10*time.Millisecond)

	// A proxy that cuts idle streams: each subscription stays up past the
	// settle time without an event, so each counts as working.
	for range 3 {
		sub := e.nextSub(t)
		r.expect(t, resync)
		sub.fail(io.EOF)

		if d := r.nextDelay(t); d != DefaultMinDelay {
			t.Fatalf("delay = %v, want %v", d, DefaultMinDelay)
		}
	}
}

func TestWatcherDuplicateInOneSubscriptionProvesNothing(t *testing.T) {
	e := newFakeEngine()
	r := startWatcher(t, e, time.Hour)

	a := ev(KindStarted, "start", "a", 0)
	o := ev(KindStarted, "start", "o", -2*int64(time.Second))
	n := ev(KindStarted, "start", "n", int64(time.Second))

	sub := e.nextSub(t)
	sub.send(t, a)
	r.expect(t, resync, a)
	sub.fail(io.EOF)
	r.nextDelay(t)

	// After a backward clock step the replay stops short of a and brings
	// only o, which the daemon then also sends live: o was added just as
	// the subscription began. Its second copy proves nothing.
	sub = e.nextSub(t)
	sub.send(t, o, o, n)
	r.expect(t, o, resync, n)
}

func TestWatcherUnprovenSubscriptionLeavesResync(t *testing.T) {
	e := newFakeEngine()
	r := startWatcher(t, e, time.Hour)

	a := ev(KindStarted, "start", "a", 0)
	o := ev(KindStarted, "start", "o", -2*int64(time.Second))
	n := ev(KindStarted, "start", "n", int64(time.Second))

	sub := e.nextSub(t)
	sub.send(t, a)
	r.expect(t, resync, a)
	sub.fail(io.EOF)
	r.nextDelay(t)

	// This subscription only brings o, older than a, and ends before it
	// is clear whether anything was lost.
	sub = e.nextSub(t)
	sub.send(t, o)
	r.expect(t, o)
	sub.fail(io.EOF)
	r.nextDelay(t)

	// o coming again is no proof: the gap before it was never closed.
	sub = e.nextSub(t)
	sub.send(t, o, n)
	r.expect(t, resync, n)
}

func TestWatcherKeepsReplayedEventsAfterClockStep(t *testing.T) {
	e := newFakeEngine()
	r := startWatcher(t, e, time.Hour)

	// z was stamped before the daemon's clock stepped back by 9s, y after.
	y := ev(KindDied, "die", "y", 0)
	z := ev(KindStarted, "start", "z", 9*int64(time.Second))
	w := ev(KindStarted, "start", "w", 10*int64(time.Second))

	sub := e.nextSub(t)
	sub.send(t, y)
	r.expect(t, resync, y)
	sub.fail(io.EOF)
	r.nextDelay(t)

	// The replay brings z first, far after y, and then y again, which must
	// still be known as delivered.
	sub = e.nextSub(t)
	sub.send(t, z, y, w)
	r.expect(t, resync, z, w)
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

	waiting := make(chan struct{})
	w.wait = func(ctx context.Context, d time.Duration) bool {
		close(waiting)
		return sleep(ctx, d)
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)
		w.Run(ctx, func(Event) {})
	}()

	e.nextSub(t).fail(errors.New("connection refused"))

	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not wait to reconnect")
	}

	// Run is in its hour-long wait now.
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestSleep(t *testing.T) {
	if !sleep(t.Context(), time.Millisecond) {
		t.Error("sleep reported a canceled context")
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if sleep(ctx, time.Hour) {
		t.Error("sleep did not stop on the canceled context")
	}
}

func TestCursor(t *testing.T) {
	var c cursor

	a := ev(KindStarted, "start", "a", 100)
	if c.delivered(a) || !c.later(a) || !c.since().IsZero() {
		t.Fatal("empty cursor knows an event")
	}

	b := ev(KindDied, "die", "a", 200)
	b2 := ev(KindChanged, "create", "b", 200) // the same nanosecond as b

	for _, e := range []Event{a, b, b2} {
		c.record(e)
	}

	if want := at(200).Add(-replayOverlap); !c.since().Equal(want) {
		t.Errorf("since = %v, want %v", c.since(), want)
	}

	for _, e := range []Event{a, b, b2} {
		if !c.delivered(e) {
			t.Errorf("%s %s at %d not delivered", e.Action, e.ContainerID, e.Time.UnixNano()-base)
		}
	}

	// The same action of the same container at another time is another
	// event.
	for _, e := range []Event{ev(KindStarted, "start", "a", 200), ev(KindDied, "die", "a", 300)} {
		if c.delivered(e) {
			t.Errorf("%s %s at %d counted as delivered", e.Action, e.ContainerID, e.Time.UnixNano()-base)
		}
	}

	if c.later(ev(KindStarted, "start", "c", 150)) || c.later(ev(KindStarted, "start", "c", 200)) ||
		!c.later(ev(KindStarted, "start", "c", 201)) {
		t.Error("later is wrong")
	}

	// An event that trails the latest one within the overlap is
	// remembered, and the cursor stays.
	trail := ev(KindStarted, "start", "c", 150)
	c.record(trail)

	if !c.delivered(trail) || !c.since().Equal(at(200).Add(-replayOverlap)) {
		t.Errorf("trailing event: delivered %v, since %v", c.delivered(trail), c.since())
	}

	// Once the latest event is more than the overlap ahead, the older ones
	// are forgotten: no replay brings them.
	far := ev(KindRemoved, "destroy", "a", 201+int64(replayOverlap))
	c.record(far)

	for _, e := range []Event{a, b, b2, trail} {
		if c.delivered(e) {
			t.Errorf("%s %s at %d still remembered", e.Action, e.ContainerID, e.Time.UnixNano()-base)
		}
	}

	if !c.delivered(far) {
		t.Error("latest event forgotten")
	}

	old := ev(KindStarted, "start", "d", 150)
	if c.record(old); c.delivered(old) {
		t.Error("event older than the overlap remembered")
	}
}

func TestCursorProof(t *testing.T) {
	var c cursor

	c.begin()

	a := ev(KindStarted, "start", "a", 100)
	c.record(a)

	if c.proves(a) {
		t.Error("an event proves its own subscription")
	}

	c.release()

	if since := c.begin(); !since.Equal(a.Time.Add(-replayOverlap)) {
		t.Errorf("since = %v", since)
	}

	if !c.proves(a) {
		t.Error("an event of the previous subscription is no proof")
	}

	c.unproven = true
	if c.proves(a) {
		t.Error("proof accepted after a subscription left a gap open")
	}
}

func TestCursorHoldsReplayWindow(t *testing.T) {
	var c cursor

	c.begin()

	y := ev(KindDied, "die", "y", 0)
	c.record(y)
	c.release()

	// During the next replay an event far after y must not push y out.
	c.begin()

	z := ev(KindStarted, "start", "z", 9*int64(time.Second))
	c.record(z)

	if !c.delivered(y) {
		t.Fatal("y forgotten while the replay runs")
	}

	// Once the replay is over the window is the overlap again.
	c.release()

	w := ev(KindStarted, "start", "w", 10*int64(time.Second))
	c.record(w)

	if c.delivered(y) || !c.delivered(z) || !c.delivered(w) {
		t.Errorf("after the replay: y %v, z %v, w %v", c.delivered(y), c.delivered(z), c.delivered(w))
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
			Time:     (base + nano) / int64(time.Second),
			TimeNano: base + nano,
		}
	}

	const (
		t1 = 100
		t2 = 200
		t3 = 1_000_000_000
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
	// Five seconds before the last event, to the nanosecond.
	if got := sub.query.Get("since"); got != "1759999995.000000200" {
		t.Errorf("since = %q", got)
	}

	sub.send <- msg(events.ActionDie, "b", t2)
	sub.send <- msg(events.ActionDestroy, "b", t3)

	got := r.next(t)
	if got.Kind != KindRemoved || got.ContainerID != "b" || got.Labels["kopia.enable"] != "true" {
		t.Fatalf("delivered %+v, want the removal of b with its labels", got)
	}
}
