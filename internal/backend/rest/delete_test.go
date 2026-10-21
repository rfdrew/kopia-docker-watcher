package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
)

// deleteRequests returns the bodies of the snapshots/delete requests.
func deleteRequests(t *testing.T, f *fakeKopia) []kopiaclient.DeleteSnapshotsRequest {
	t.Helper()

	var out []kopiaclient.DeleteSnapshotsRequest

	for _, r := range f.requestsTo("POST snapshots/delete") {
		var req kopiaclient.DeleteSnapshotsRequest
		if err := json.Unmarshal(r.Body, &req); err != nil {
			t.Fatal(err)
		}

		out = append(out, req)
	}

	return out
}

func pinned(s kopiaclient.Snapshot, pins ...string) kopiaclient.Snapshot {
	s.Pins = pins

	return s
}

// deleteFixture is a source with two snapshots and a checkpoint, the
// second pinned if pin is set, under a paused policy.
func deleteFixture(t *testing.T, pin bool) (*fakeKopia, *Backend) {
	t.Helper()

	f := newFakeKopia()
	b := newBackend(t, f)
	t0 := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)

	f.setPolicy(info("/data/app"), `{"scheduling":{"manual":true}}`)
	f.set(func(f *fakeKopia) {
		second := snapshot("b", t0.Add(time.Hour), t0.Add(time.Hour+time.Minute), "")
		if pin {
			second = pinned(second, "before-upgrade")
		}

		f.snapshots[info("/data/app")] = []kopiaclient.Snapshot{
			snapshot("a", t0, t0.Add(time.Minute), ""),
			snapshot("a-checkpoint", t0, t0.Add(30*time.Second), "checkpoint"),
			second,
		}
		f.sources[info("/data/app")].snapshots = true
	})
	f.resetRequests()

	return f, b
}

// A source without pinned snapshots goes as a whole, with its policy.
func TestDeleteSnapshotsWholeSource(t *testing.T) {
	f, b := deleteFixture(t, false)

	res, err := b.DeleteSnapshots(t.Context(), src("/data/app"), false)
	if err != nil {
		t.Fatal(err)
	}

	if res != (backend.DeleteResult{Deleted: 3, Source: true}) {
		t.Errorf("result = %+v", res)
	}

	assertRoutes(t, f, "GET sources", "GET snapshots", "POST snapshots/delete")

	if reqs := deleteRequests(t, f); len(reqs) != 1 || !reqs[0].DeleteSourceAndPolicy || len(reqs[0].SnapshotManifestIDs) != 0 ||
		reqs[0].Source != info("/data/app") {
		t.Errorf("delete requests = %+v", reqs)
	}

	if r := f.requestsTo("GET snapshots"); r[0].Query.Get("all") != "1" || target(r[0].Query) != info("/data/app") {
		t.Errorf("listing query = %v", r[0].Query)
	}

	if _, found := f.policy(info("/data/app")); found || f.known(info("/data/app")) {
		t.Error("the source or its policy is left")
	}
}

// A pinned snapshot stays, and so does the policy: only the others go, by
// ID, checkpoints included.
func TestDeleteSnapshotsKeepsPinned(t *testing.T) {
	f, b := deleteFixture(t, true)

	res, err := b.DeleteSnapshots(t.Context(), src("/data/app"), false)
	if err != nil {
		t.Fatal(err)
	}

	if res != (backend.DeleteResult{Deleted: 2, Kept: 1}) {
		t.Errorf("result = %+v", res)
	}

	reqs := deleteRequests(t, f)
	if len(reqs) != 1 || reqs[0].DeleteSourceAndPolicy || !slices.Equal(slices.Sorted(slices.Values(reqs[0].SnapshotManifestIDs)), []string{"a", "a-checkpoint"}) {
		t.Errorf("delete requests = %+v", reqs)
	}

	mustPolicy(t, f, info("/data/app"), `{"scheduling":{"manual":true}}`)

	left, err := b.ListSnapshots(t.Context(), src("/data/app"))
	if err != nil || len(left) != 1 || !slices.Equal(left[0].Pins, []string{"before-upgrade"}) || !left[0].Pinned() {
		t.Errorf("left = %+v, %v", left, err)
	}

	// Only pinned snapshots are left: nothing more to delete, no request.
	f.resetRequests()

	res, err = b.DeleteSnapshots(t.Context(), src("/data/app"), false)
	if err != nil || res != (backend.DeleteResult{Kept: 1}) {
		t.Errorf("second delete = %+v, %v", res, err)
	}

	assertRoutes(t, f, "GET sources", "GET snapshots")
}

// kopia server reports the pins of a snapshot also on the oldest snapshot
// with the same content: that one counts as pinned and stays too.
func TestDeleteSnapshotsSameContentAsPinned(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	t0 := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	same := func(id string, at time.Time, pins ...string) kopiaclient.Snapshot {
		s := pinned(snapshot(id, at, at, ""), pins...)
		s.RootID = "root-same"

		return s
	}

	f.setPolicy(info("/data/app"), `{"scheduling":{"manual":true}}`)
	f.set(func(f *fakeKopia) {
		f.snapshots[info("/data/app")] = []kopiaclient.Snapshot{
			same("first", t0), snapshot("other", t0.Add(time.Minute), t0.Add(time.Minute), ""), same("pinned", t0.Add(2*time.Minute), "keep"),
		}
		f.sources[info("/data/app")].snapshots = true
	})

	listed, err := b.ListSnapshots(t.Context(), src("/data/app"))
	if err != nil || len(listed) != 3 || !listed[0].Pinned() || listed[1].Pinned() || !listed[2].Pinned() {
		t.Fatalf("listed = %+v, %v", listed, err)
	}

	res, err := b.DeleteSnapshots(t.Context(), src("/data/app"), false)
	if err != nil || res != (backend.DeleteResult{Deleted: 1, Kept: 2}) {
		t.Errorf("result = %+v, %v", res, err)
	}

	if reqs := deleteRequests(t, f); len(reqs) != 1 || !slices.Equal(reqs[0].SnapshotManifestIDs, []string{"other"}) {
		t.Errorf("delete requests = %+v", reqs)
	}
}

// With leave to delete pinned snapshots, the source goes as a whole.
func TestDeleteSnapshotsPinnedAllowed(t *testing.T) {
	f, b := deleteFixture(t, true)

	res, err := b.DeleteSnapshots(t.Context(), src("/data/app"), true)
	if err != nil || res != (backend.DeleteResult{Deleted: 3, Source: true}) {
		t.Errorf("result = %+v, %v", res, err)
	}

	if reqs := deleteRequests(t, f); len(reqs) != 1 || !reqs[0].DeleteSourceAndPolicy {
		t.Errorf("delete requests = %+v", reqs)
	}

	if f.known(info("/data/app")) {
		t.Error("the source is left")
	}
}

// A source without snapshots goes with its policy too.
func TestDeleteSnapshotsNoSnapshots(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)

	f.setPolicy(info("/data/app"), `{"scheduling":{"manual":true}}`)

	res, err := b.DeleteSnapshots(t.Context(), src("/data/app"), false)
	if err != nil || res != (backend.DeleteResult{Source: true}) {
		t.Errorf("result = %+v, %v", res, err)
	}

	if _, found := f.policy(info("/data/app")); found {
		t.Error("the policy is left")
	}
}

// A source kopia server does not know, before the delete or by the
// answer to it, has nothing to delete.
func TestDeleteSnapshotsUnknownSource(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)

	res, err := b.DeleteSnapshots(t.Context(), src("/data/none"), false)
	if err != nil || res != (backend.DeleteResult{}) {
		t.Errorf("result = %+v, %v", res, err)
	}

	assertRoutes(t, f, "GET sources")

	f, b = deleteFixture(t, false)
	f.inject("POST snapshots/delete", injection{status: http.StatusNotFound, body: `{"code":"NOT_FOUND","error":"unknown source"}`})

	res, err = b.DeleteSnapshots(t.Context(), src("/data/app"), false)
	if err != nil || res != (backend.DeleteResult{}) {
		t.Errorf("forgotten in between: result = %+v, %v", res, err)
	}
}

func TestDeleteSnapshotsErrors(t *testing.T) {
	s := src("/data/app")

	t.Run("another identity", func(t *testing.T) {
		f, b := deleteFixture(t, false)
		other := backend.Source{Host: fakeHost, UserName: "other", Path: "/data/app"}

		_, err := b.DeleteSnapshots(t.Context(), other, true)
		if !errors.Is(err, backend.ErrWrongIdentity) {
			t.Errorf("err = %v, want ErrWrongIdentity", err)
		}

		assertRoutes(t, f, "GET sources")
	})

	t.Run("relative path", func(t *testing.T) {
		f, b := deleteFixture(t, false)

		_, err := b.DeleteSnapshots(t.Context(), src("data/app"), true)
		assertError(t, err, opDeleteSnapshots, src("data/app"), nil)
		assertRoutes(t, f)
	})

	t.Run("listing fails", func(t *testing.T) {
		f, b := deleteFixture(t, false)
		f.inject("GET snapshots", injection{status: http.StatusInternalServerError, body: `{"code":"INTERNAL","error":"boom"}`})

		_, err := b.DeleteSnapshots(t.Context(), s, false)
		assertError(t, err, opDeleteSnapshots, s, backend.ErrTemporary)
		assertRoutes(t, f, "GET sources", "GET snapshots")
	})

	t.Run("delete fails", func(t *testing.T) {
		f, b := deleteFixture(t, false)
		f.inject("POST snapshots/delete", injection{status: http.StatusInternalServerError,
			body: `{"code":"INTERNAL","error":"internal server error: unable to delete snapshot"}`})

		res, err := b.DeleteSnapshots(t.Context(), s, false)
		assertError(t, err, opDeleteSnapshots, s, backend.ErrTemporary)

		if res != (backend.DeleteResult{}) {
			t.Errorf("result = %+v", res)
		}
	})

	t.Run("read-only repository", func(t *testing.T) {
		f, b := deleteFixture(t, false)
		f.inject("POST snapshots/delete", injection{status: http.StatusInternalServerError,
			body: `{"code":"INTERNAL","error":"internal server error: storage is read-only"}`})

		_, err := b.DeleteSnapshots(t.Context(), s, false)
		assertError(t, err, opDeleteSnapshots, s, nil)
	})

	t.Run("unreachable", func(t *testing.T) {
		f, b := deleteFixture(t, false)
		f.inject("POST snapshots/delete", injection{status: http.StatusBadGateway, body: "bad gateway"})

		_, err := b.DeleteSnapshots(t.Context(), s, false)
		assertError(t, err, opDeleteSnapshots, s, backend.ErrUnavailable)
	})

	// A 404 while the caller stops waiting is no "nothing to delete".
	t.Run("canceled while kopia server answers 404", func(t *testing.T) {
		f, b := deleteFixture(t, false)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		f.set(func(f *fakeKopia) {
			f.hook = func(r fakeRequest) {
				if r.Route == "POST snapshots/delete" {
					cancel()
				}
			}
		})
		f.inject("POST snapshots/delete", injection{status: http.StatusNotFound, body: `{"code":"NOT_FOUND","error":"unknown source"}`})

		_, err := b.DeleteSnapshots(ctx, s, false)
		assertError(t, err, opDeleteSnapshots, s, nil)

		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	})
}

// The IDs of a source with many snapshots go in several requests, each
// small enough for kopia server; a failed request ends the delete with
// what the requests before it deleted.
func TestDeleteSnapshotsInChunks(t *testing.T) {
	setup := func(t *testing.T) (*fakeKopia, *Backend) {
		t.Helper()

		f := newFakeKopia()
		b := newBackend(t, f)
		t0 := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)

		f.setPolicy(info("/data/app"), `{"scheduling":{"manual":true}}`)
		f.set(func(f *fakeKopia) {
			snaps := []kopiaclient.Snapshot{pinned(snapshot("kept", t0, t0, ""), "keep")}

			for i := range 2*maxDeleteIDs + 500 {
				at := t0.Add(time.Duration(i+1) * time.Minute)
				snaps = append(snaps, snapshot(fmt.Sprintf("%032x", i), at, at, ""))
			}

			f.snapshots[info("/data/app")] = snaps
			f.sources[info("/data/app")].snapshots = true
		})
		f.resetRequests()

		return f, b
	}

	t.Run("all chunks", func(t *testing.T) {
		f, b := setup(t)

		res, err := b.DeleteSnapshots(t.Context(), src("/data/app"), false)
		if err != nil || res != (backend.DeleteResult{Deleted: 2*maxDeleteIDs + 500, Kept: 1}) {
			t.Fatalf("result = %+v, %v", res, err)
		}

		reqs := f.requestsTo("POST snapshots/delete")
		if len(reqs) != 3 {
			t.Fatalf("%d delete requests", len(reqs))
		}

		for _, r := range reqs {
			if len(r.Body) > maxFakeBody {
				t.Errorf("request of %d bytes", len(r.Body))
			}
		}

		if left := f.snapshots[info("/data/app")]; len(left) != 1 || left[0].ID != "kept" {
			t.Errorf("left = %d snapshots", len(left))
		}
	})

	t.Run("second chunk fails", func(t *testing.T) {
		f, b := setup(t)
		f.inject("POST snapshots/delete", injection{after: true, status: http.StatusOK, body: `{}`})
		f.inject("POST snapshots/delete", injection{status: http.StatusInternalServerError, body: `{"code":"INTERNAL","error":"boom"}`})

		res, err := b.DeleteSnapshots(t.Context(), src("/data/app"), false)
		assertError(t, err, opDeleteSnapshots, src("/data/app"), backend.ErrTemporary)

		if res != (backend.DeleteResult{Deleted: maxDeleteIDs, Kept: 1}) {
			t.Errorf("result = %+v", res)
		}
	})
}
