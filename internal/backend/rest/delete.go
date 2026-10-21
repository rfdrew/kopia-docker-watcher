package rest

import (
	"context"
	"errors"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
)

// DeleteSnapshots implements backend.Backend with snapshots/delete. The
// snapshots of the source are listed with GET /api/v1/snapshots and all=1
// right before the delete: a source with a pinned snapshot that has to stay
// gets snapshots/delete with the manifest IDs of the others, and its policy
// stays; any other source gets snapshots/delete with
// deleteSourceAndPolicy, which deletes every snapshot kopia server lists
// then, removes the policy, and stops the source, canceling an upload. A
// source kopia server does not know, before the delete or by its answer
// ("unknown source"), has nothing to delete.
//
// kopia server checks no pins. With deleteSourceAndPolicy it deletes the
// manifests it lists itself, so a snapshot pinned after the listing goes
// too. With IDs such a snapshot stays: the edit that pinned it saved it
// under a new ID, which the listing did not see.
func (b *Backend) DeleteSnapshots(ctx context.Context, src backend.Source, pinned bool) (backend.DeleteResult, error) {
	status, err := b.lookup(ctx, opDeleteSnapshots, src)
	if err != nil || status == nil {
		return backend.DeleteResult{}, err
	}

	snaps, err := b.snapshots(ctx, opDeleteSnapshots, src)
	if err != nil {
		return backend.DeleteResult{}, err
	}

	var (
		ids []string
		res backend.DeleteResult
	)

	for _, s := range snaps {
		if !pinned && len(s.Pins) > 0 {
			res.Kept++
			continue
		}

		ids = append(ids, s.ID)
	}

	req := kopiaclient.DeleteSnapshotsRequest{Source: toInfo(src)}

	switch {
	case res.Kept == 0:
		req.DeleteSourceAndPolicy = true
	case len(ids) == 0:
		// Only pinned snapshots, which stay.
		return res, nil
	default:
		req.SnapshotManifestIDs = ids
	}

	if err := b.client.DeleteSnapshots(ctx, req); err != nil {
		if errors.Is(err, kopiaclient.ErrNotFound) && ctx.Err() == nil {
			// kopia server forgot the source since the listing.
			return backend.DeleteResult{}, nil
		}

		return backend.DeleteResult{}, fail(ctx, opDeleteSnapshots, src, reqDeleteSnapshots, err)
	}

	res.Deleted = len(ids)
	res.Source = req.DeleteSourceAndPolicy

	return res, nil
}
