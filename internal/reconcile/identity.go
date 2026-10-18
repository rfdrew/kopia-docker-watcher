package reconcile

import (
	"context"
	"fmt"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
)

// source returns the source of path p under kopia server's identity, which
// is asked once and again after ErrWrongIdentity.
func (r *Reconciler) source(ctx context.Context, p string) (backend.Source, error) {
	r.mu.Lock()
	id := r.identity
	r.mu.Unlock()

	if !id.known {
		userName, host, err := r.backend.ServerIdentity(ctx)
		if err != nil {
			return backend.Source{}, err
		}

		if userName == "" || host == "" {
			return backend.Source{}, &backend.Error{Op: "ServerIdentity",
				Err: fmt.Errorf("kopia server reports an incomplete identity %q@%q", userName, host)}
		}

		id = identity{userName: userName, host: host, known: true}

		r.mu.Lock()
		old := r.identity
		r.identity = id
		r.mu.Unlock()

		switch {
		case old.userName == "":
			r.checkStateIdentity(userName, host)
		case old.userName != userName || old.host != host:
			r.log.Warn("kopia server's identity changed; reconciling every path under the new one",
				"old", old.userName+"@"+old.host, "new", userName+"@"+host)
			r.ReconcileAll()
		}
	}

	return backend.Source{Host: id.host, UserName: id.userName, Path: p}, nil
}

// checkStateIdentity warns when the state holds sources of another
// identity than kopia server's: they are left alone, and the paths are
// registered again under the new identity, which splits their snapshot
// history. A kopia server whose host name changes on every recreation does
// that.
func (r *Reconciler) checkStateIdentity(userName, host string) {
	other := map[string]int{}
	own := 0

	for _, src := range r.store.Sources() {
		if src.UserName == userName && src.Host == host {
			own++
		} else {
			other[src.UserName+"@"+src.Host]++
		}
	}

	if len(other) > 0 {
		r.log.Warn("the state has sources of another kopia server identity; they are left alone, and their paths "+
			"are registered again under the current one. If kopia server's host name changed by accident, give it a fixed one",
			"identity", userName+"@"+host, "sources", own, "other_identities", other)
	}
}

// forgetIdentity makes the next pass ask kopia server for its identity
// again, unless another pass already did since src was built.
func (r *Reconciler) forgetIdentity(src backend.Source) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.identity.userName == src.UserName && r.identity.host == src.Host {
		r.identity.known = false
	}
}
