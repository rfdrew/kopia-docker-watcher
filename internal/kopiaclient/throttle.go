package kopiaclient

import (
	"context"
	"errors"
	"net/http"
)

// ErrNoStorage means that the call needs kopia server to open the repository storage
// itself, and it is connected to a repository server instead; kopia
// answers STORAGE_CONNECTION. Retrying does not help.
var ErrNoStorage = errors.New("kopia server is connected to a repository server, not to the repository storage")

// ThrottleLimits are kopia server's limits on its repository storage
// (throttling.Limits in kopia's repo/blob/throttling); zero means no
// limit. Kopia keeps them in its repository configuration file, so they
// survive a restart.
type ThrottleLimits struct {
	ReadsPerSecond         float64 `json:"readsPerSecond,omitempty"`
	WritesPerSecond        float64 `json:"writesPerSecond,omitempty"`
	ListsPerSecond         float64 `json:"listsPerSecond,omitempty"`
	UploadBytesPerSecond   float64 `json:"maxUploadSpeedBytesPerSecond,omitempty"`
	DownloadBytesPerSecond float64 `json:"maxDownloadSpeedBytesPerSecond,omitempty"`
	ConcurrentReads        int     `json:"concurrentReads,omitempty"`
	ConcurrentWrites       int     `json:"concurrentWrites,omitempty"`
}

// Throttle returns the limits with GET /api/v1/repo/throttle.
func (c *Client) Throttle(ctx context.Context) (*ThrottleLimits, error) {
	var l ThrottleLimits
	if err := c.call(ctx, request{method: http.MethodGet, path: "repo/throttle"}, &l); err != nil {
		return nil, throttleError(err)
	}

	return &l, nil
}

// SetThrottle replaces all limits with PUT /api/v1/repo/throttle. A
// negative limit is ErrMalformedRequest.
func (c *Client) SetThrottle(ctx context.Context, l ThrottleLimits) error {
	return throttleError(c.call(ctx, request{method: http.MethodPut, path: "repo/throttle", body: l}, nil))
}

// throttleError turns STORAGE_CONNECTION, which kopia answers when it has
// no storage of its own to throttle, into ErrNoStorage.
func throttleError(err error) error {
	var e *Error
	if !errors.As(err, &e) || e.Code != CodeStorageConnection {
		return err
	}

	out := *e
	out.kind = ErrNoStorage

	return &out
}
