package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
)

// overlapBackend wraps a Backend and records calls about one source that
// overlap: one pass makes its calls one after the other, so an overlap
// means two passes over one path at once.
type overlapBackend struct {
	backend.Backend

	mu       sync.Mutex
	active   map[string]int
	overlaps []string
}

func (b *overlapBackend) enter(src backend.Source) func() {
	b.mu.Lock()
	if b.active[src.Path] > 0 {
		b.overlaps = append(b.overlaps, src.Path)
	}

	b.active[src.Path]++
	b.mu.Unlock()

	// Widen the window for an overlap.
	time.Sleep(50 * time.Microsecond)

	return func() {
		b.mu.Lock()
		b.active[src.Path]--
		b.mu.Unlock()
	}
}

func (b *overlapBackend) PlanSource(ctx context.Context, want backend.Desired, last backend.Applied) (backend.Plan, error) {
	defer b.enter(want.Source)()
	return b.Backend.PlanSource(ctx, want, last)
}

func (b *overlapBackend) ApplyPlan(ctx context.Context, plan backend.Plan) (backend.Applied, error) {
	defer b.enter(plan.Source)()
	return b.Backend.ApplyPlan(ctx, plan)
}

func (b *overlapBackend) PreviewSchedule(ctx context.Context, src backend.Source, policy json.RawMessage, n int) (backend.SchedulePreview, error) {
	defer b.enter(src)()
	return b.Backend.PreviewSchedule(ctx, src, policy, n)
}

// Many events, full reconciles and state changes at once over a few shared
// paths, with several workers: passes over one path never overlap, there
// is no data race, and everything settles in the right state.
func TestNoConcurrentPassesOverOnePath(t *testing.T) {
	ob := &overlapBackend{active: map[string]int{}}

	h := newHarness(t, func(o *Options) {
		ob.Backend = o.Backend
		o.Backend = ob
		o.Workers = 8
		o.EventDelay = time.Microsecond
	})

	paths := []string{"/data/a", "/data/b", "/data/c"}

	var ids []string

	for i := range 12 {
		p := paths[i%len(paths)]
		ids = append(ids, h.engine.run(fmt.Sprintf("c%02d", i), enable, pathLabel(p+",/data/shared")))
	}

	passes := h.start()

	var wg sync.WaitGroup

	for g := range 4 {
		wg.Go(func() {
			for i := range 200 {
				id := ids[(g*7+i)%len(ids)]

				switch i % 5 {
				case 0:
					h.engine.setState(id, docker.StateExited)
					h.r.HandleEvent(h.engine.event(docker.KindDied, id))
				case 1:
					h.engine.setState(id, docker.StateRunning)
					h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
				case 2:
					h.r.ReconcileAll()
				case 3:
					h.r.Enqueue(paths...)
				default:
					h.r.Enqueue("/data/shared")
				}
			}
		})
	}

	wg.Wait()

	// Settle: every container runs, and one last round of passes.
	for _, id := range ids {
		h.engine.setState(id, docker.StateRunning)
	}

	h.r.ReconcileAll()
	waitPasses(t, passes, append(paths, "/data/shared")...)
	h.r.ReconcileAll()
	waitPasses(t, passes, append(paths, "/data/shared")...)

	for _, p := range append(paths, "/data/shared") {
		eventually(t, p+" unpaused", func() bool {
			pol, _ := h.fake.Policy(h.src(p))
			return string(pol) == `{"scheduling":{"cron":["0 * * * *"]}}`
		})
	}

	ob.mu.Lock()
	defer ob.mu.Unlock()

	if len(ob.overlaps) > 0 {
		t.Errorf("passes overlapped on %v", ob.overlaps)
	}
}
