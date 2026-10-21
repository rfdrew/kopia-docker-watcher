package rest

import (
	"context"
	"errors"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
)

// maxDeleteIDs bounds the manifest IDs of one snapshots/delete request.
// kopia server reads at most 100,000 bytes of a request and refuses a
// larger one with a 413; an ID takes about 35 bytes in the request.
const maxDeleteIDs = 1000

// DeleteSnapshots implements backend.Backend with snapshots/delete. The
// snapshots of the source are listed with GET /api/v1/snapshots and all=1
// right before the delete: a source with a pinned snapshot that has to stay
// gets snapshots/delete with the manifest IDs of the others, at most
// maxDeleteIDs a request, and its policy stays; any other source gets
// snapshots/delete with deleteSourceAndPolicy, which deletes every snapshot
// kopia server lists then, removes the policy, and stops the source,
// canceling an upload. A source kopia server does not know, before the
// delete or by its answer ("unknown source"), has nothing to delete. When a
// request fails, the result counts the snapshots of the requests before it.
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

	if res.Kept == 0 {
		switch err := b.deleteSnapshots(ctx, src, kopiaclient.DeleteSnapshotsRequest{Source: toInfo(src), DeleteSourceAndPolicy: true}); {
		case errors.Is(err, errUnknownSource):
			return backend.DeleteResult{}, nil
		case err != nil:
			return backend.DeleteResult{}, err
		}

		return backend.DeleteResult{Deleted: len(ids), Source: true}, nil
	}

	// Only the snapshots without a pin; when there are none, nothing.
	for len(ids) > 0 {
		chunk := ids[:min(len(ids), maxDeleteIDs)]
		ids = ids[len(chunk):]

		err := b.deleteSnapshots(ctx, src, kopiaclient.DeleteSnapshotsRequest{Source: toInfo(src), SnapshotManifestIDs: chunk})

		switch {
		case errors.Is(err, errUnknownSource):
			return res, nil
		case err != nil:
			return res, err
		}

		res.Deleted += len(chunk)
	}

	return res, nil
}

// errUnknownSource: kopia server does not know the source a delete names.
var errUnknownSource = errors.New("unknown source")

// deleteSnapshots sends one snapshots/delete. kopia server's "unknown
// source" answer is errUnknownSource, unless ctx is done: the caller
// stopped waiting, and the answer does not tell.
func (b *Backend) deleteSnapshots(ctx context.Context, src backend.Source, req kopiaclient.DeleteSnapshotsRequest) error {
	err := b.client.DeleteSnapshots(ctx, req)

	switch {
	case err == nil:
		return nil
	case errors.Is(err, kopiaclient.ErrNotFound) && ctx.Err() == nil:
		return errUnknownSource
	default:
		return fail(ctx, opDeleteSnapshots, src, reqDeleteSnapshots, err)
	}
}
