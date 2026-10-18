package reconcile

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"
)

// getNow takes a path that is ready now.
func getNow(t *testing.T, q *queue) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	p, ok := q.get(ctx)
	if !ok {
		t.Fatal("no path ready")
	}

	return p
}

// assertEmpty checks that no path is ready.
func assertEmpty(t *testing.T, q *queue) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if p, ok := q.get(ctx); ok {
		t.Fatalf("unexpected path %s", p)
	}
}

func TestQueueOrderAndDedup(t *testing.T) {
	q := newQueue()
	q.add("/a")
	q.add("/b")
	q.add("/a")

	if got := []string{getNow(t, q), getNow(t, q)}; !slices.Equal(got, []string{"/a", "/b"}) {
		t.Errorf("got %v", got)
	}

	assertEmpty(t, q)
}

// A path added during its pass is handed out again after done, not
// before.
func TestQueueDirtyDuringPass(t *testing.T) {
	q := newQueue()
	q.add("/a")

	p := getNow(t, q)
	q.add("/a")
	q.add("/a")
	assertEmpty(t, q)

	q.done(p)

	if p := getNow(t, q); p != "/a" {
		t.Fatalf("got %s", p)
	}

	q.done("/a")
	assertEmpty(t, q)
}

func TestQueueAddAfter(t *testing.T) {
	q := newQueue()
	q.addAfter("/a", 30*time.Millisecond)
	assertEmpty(t, q)

	if p := getNow(t, q); p != "/a" {
		t.Fatalf("got %s", p)
	}
}

// The earliest delayed add wins, and the later one does not fire again.
func TestQueueAddAfterKeepsEarliest(t *testing.T) {
	q := newQueue()
	q.addAfter("/a", time.Hour)
	q.addAfter("/a", 20*time.Millisecond)
	q.addAfter("/a", time.Hour)

	start := time.Now()

	p := getNow(t, q)
	if p != "/a" || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("got %s after %v", p, time.Since(start))
	}

	q.done(p)

	q.mu.Lock()
	n := len(q.waiting)
	q.mu.Unlock()

	if n != 0 {
		t.Errorf("%d delayed adds left", n)
	}
}

// A delayed add that comes due during the pass of its path is not lost.
func TestQueueAddAfterDuringPass(t *testing.T) {
	q := newQueue()
	q.add("/a")

	p := getNow(t, q)
	q.addAfter("/a", time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	assertEmpty(t, q)

	q.done(p)

	if p := getNow(t, q); p != "/a" {
		t.Fatalf("got %s", p)
	}
}

func TestQueueClose(t *testing.T) {
	q := newQueue()
	q.addAfter("/a", time.Millisecond)

	var wg sync.WaitGroup

	for range 3 {
		wg.Go(func() {
			if p, ok := q.get(context.Background()); ok {
				q.done(p)
			}
		})
	}

	time.Sleep(20 * time.Millisecond)
	q.close()

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workers still waiting after close")
	}

	q.add("/b")
	q.addAfter("/c", time.Millisecond)

	if _, ok := q.get(context.Background()); ok {
		t.Error("get after close")
	}
}

func TestQueueGetCanceled(t *testing.T) {
	q := newQueue()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, ok := q.get(ctx); ok {
		t.Error("get with a canceled context")
	}
}

// Several workers drain many paths, and no path is handed out twice at
// once.
func TestQueueWorkers(t *testing.T) {
	q := newQueue()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu     sync.Mutex
		active = map[string]bool{}
		passes = map[string]int{}
		wg     sync.WaitGroup
	)

	for range 4 {
		wg.Go(func() {
			for {
				p, ok := q.get(ctx)
				if !ok {
					return
				}

				mu.Lock()
				if active[p] {
					t.Errorf("two passes over %s at once", p)
				}

				active[p] = true
				passes[p]++
				mu.Unlock()

				time.Sleep(time.Millisecond)

				mu.Lock()
				active[p] = false
				mu.Unlock()

				q.done(p)
			}
		})
	}

	paths := []string{"/a", "/b", "/c", "/d", "/e", "/f"}
	for range 50 {
		for _, p := range paths {
			q.add(p)
		}
	}

	deadline := time.Now().Add(5 * time.Second)

	for {
		q.mu.Lock()
		idle := len(q.ready) == 0 && len(q.processing) == 0
		q.mu.Unlock()

		if idle || time.Now().After(deadline) {
			break
		}

		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	wg.Wait()

	for _, p := range paths {
		if passes[p] == 0 {
			t.Errorf("%s never handed out", p)
		}
	}
}

// A replaced timer that fires anyway leaves the timer that replaced it
// armed, so delayed adds never multiply.
func TestQueueReplacedTimer(t *testing.T) {
	q := newQueue()
	q.addAfter("/a", time.Hour)

	q.mu.Lock()
	stale := q.waiting["/a"]
	q.mu.Unlock()

	q.addAfter("/a", time.Hour/2)

	// The stale timer fires although it was stopped.
	q.fire("/a", stale)
	assertEmpty(t, q)

	q.mu.Lock()
	n, w := len(q.waiting), q.waiting["/a"]
	q.mu.Unlock()

	if n != 1 || w == stale {
		t.Fatalf("%d delayed adds, stale one kept: %v", n, w == stale)
	}

	q.fire("/a", w)

	if p := getNow(t, q); p != "/a" {
		t.Fatalf("got %s", p)
	}
}

// A retry replaces an earlier delayed add, and later delayed and full
// reconcile adds wait for it.
func TestQueueRetryTakesPrecedence(t *testing.T) {
	q := newQueue()
	q.addAfter("/a", time.Millisecond)
	q.retryAfter("/a", time.Hour)
	q.addAfter("/a", time.Millisecond)
	q.addAfter("/a", 0)
	q.addUnlessRetrying("/a")
	time.Sleep(20 * time.Millisecond)
	assertEmpty(t, q)

	// An explicit add does not wait.
	q.add("/a")

	if p := getNow(t, q); p != "/a" {
		t.Fatalf("got %s", p)
	}
}

// A full reconcile during the pass of a path that waits for nothing queues
// it again; one during a pass that ends in a retry leaves it to the retry.
func TestQueueAddUnlessRetryingDuringPass(t *testing.T) {
	q := newQueue()
	q.add("/a")

	p := getNow(t, q)
	q.addUnlessRetrying(p)
	q.done(p)

	if p := getNow(t, q); p != "/a" {
		t.Fatalf("got %s", p)
	}

	q.addUnlessRetrying(p)
	q.retryAfter(p, time.Hour)
	q.done(p)
	assertEmpty(t, q)
}

// get hands out nothing once its context is done.
func TestQueueGetAfterCancelWithReadyPaths(t *testing.T) {
	q := newQueue()
	q.add("/a")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, ok := q.get(ctx); ok {
		t.Error("handed out a path after cancel")
	}
}
