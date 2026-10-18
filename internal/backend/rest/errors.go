package rest

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
)

// request is the kind of kopia server request an error came from. Which
// errors are the source's configuration depends on it.
type request int

const (
	// reqRead: GET /api/v1/sources, GET /api/v1/policy, paths/resolve,
	// GET /api/v1/tasks.
	reqRead request = iota
	// reqWritePolicy: PUT /api/v1/policy.
	reqWritePolicy
	// reqDeletePolicy: DELETE /api/v1/policy.
	reqDeletePolicy
	// reqCreateSource: POST /api/v1/sources.
	reqCreateSource
	// reqResolvePolicy: POST /api/v1/policy/resolve.
	reqResolvePolicy
	// reqUpload: POST /api/v1/sources/upload.
	reqUpload
)

// Texts of kopia server's 500 answers (internal/server/api_error.go,
// api_sources.go).
const (
	// msgNotWritable: the repository is connected read-only.
	msgNotWritable = "repository is not writable"
	// msgInitialPolicy prefixes every failure of POST /api/v1/sources to
	// store the policy; any other 500 of that call comes from os.Stat of
	// the path.
	msgInitialPolicy = "unable to set initial policy"
)

// fail wraps err, an error of a kopia server request made for the Backend
// method op, in a *backend.Error of the kind package backend documents for
// it. When ctx is done the error is of neither kind and wraps ctx.Err(),
// whatever the request failed with: the caller stopped waiting.
func fail(ctx context.Context, op string, src backend.Source, req request, err error) error {
	if cerr := ctx.Err(); cerr != nil {
		if !errors.Is(err, cerr) {
			err = fmt.Errorf("%w: %w", cerr, err)
		}

		return &backend.Error{Op: op, Source: src, Err: err}
	}

	return &backend.Error{Op: op, Source: src, Kind: kind(req, err), Err: err}
}

// kind maps an error of internal/kopiaclient to backend.ErrConfig,
// backend.ErrUnavailable, backend.ErrTemporary or nil.
func kind(req request, err error) error {
	var apiErr *kopiaclient.Error
	if !errors.As(err, &apiErr) {
		// Not an answer of kopia server: a transport error is retried
		// unless it is about TLS verification; a request the client
		// refused to send, or an answer too large to read, is not.
		if kopiaclient.IsRetryable(err) {
			return backend.ErrUnavailable
		}

		return nil
	}

	configReq := req == reqWritePolicy || req == reqCreateSource || req == reqResolvePolicy

	switch {
	case errors.Is(err, kopiaclient.ErrInvalidPolicy):
		// A 500 with "failed to validate policy" or "invalid policy path".
		if configReq {
			return backend.ErrConfig
		}

		return backend.ErrTemporary
	case errors.Is(err, kopiaclient.ErrMalformedRequest):
		// kopia server could not decode the policy into its own types.
		if configReq {
			return backend.ErrConfig
		}

		return nil
	case errors.Is(err, kopiaclient.ErrPathNotFound):
		if req == reqCreateSource {
			return backend.ErrConfig
		}

		return nil
	case errors.Is(err, kopiaclient.ErrInternal):
		switch {
		case strings.Contains(apiErr.Message, msgNotWritable):
			return nil
		case req == reqCreateSource && apiErr.Code == kopiaclient.CodeInternal && !strings.Contains(apiErr.Message, msgInitialPolicy):
			// kopia server could not stat the path, e.g. permission denied.
			return backend.ErrConfig
		default:
			return backend.ErrTemporary
		}
	case errors.Is(err, kopiaclient.ErrUnavailable), errors.Is(err, kopiaclient.ErrNotConnected):
		return backend.ErrUnavailable
	case errors.Is(err, kopiaclient.ErrNotFound):
		// sources/upload right after a policy write whose refresh failed:
		// kopia server picks the source up at its next refresh.
		if req == reqUpload {
			return backend.ErrTemporary
		}

		return nil
	default:
		return nil
	}
}
