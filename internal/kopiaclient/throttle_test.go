package kopiaclient

import (
	"errors"
	"net/http"
	"testing"
)

func TestThrottle(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET repo/throttle", respond(`{"readsPerSecond": 10, "maxUploadSpeedBytesPerSecond": 1048576, "concurrentWrites": 2}`))
	f.handle("PUT repo/throttle", respond(`{}`))
	c := newTestClient(t, f)

	got, err := c.Throttle(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if want := (ThrottleLimits{ReadsPerSecond: 10, UploadBytesPerSecond: 1 << 20, ConcurrentWrites: 2}); *got != want {
		t.Errorf("Throttle = %+v, want %+v", *got, want)
	}

	if err := c.SetThrottle(t.Context(), ThrottleLimits{DownloadBytesPerSecond: 500, ConcurrentReads: 4}); err != nil {
		t.Fatal(err)
	}

	call := f.lastCall(t)
	if call.Method != http.MethodPut || call.Path != "/api/v1/repo/throttle" {
		t.Errorf("call = %s %s", call.Method, call.Path)
	}

	// Zero limits are left out, as kopia leaves them out.
	assertJSONBody(t, call, `{"maxDownloadSpeedBytesPerSecond": 500, "concurrentReads": 4}`)
}

// kopia server connected to a repository server has no throttle.
func TestThrottleNoStorage(t *testing.T) {
	f := newFakeKopia()
	noStorage := func(w http.ResponseWriter, _ *http.Request) {
		writeAPIError(w, http.StatusBadRequest, CodeStorageConnection, "no direct storage connection")
	}
	f.handle("GET repo/throttle", noStorage)
	f.handle("PUT repo/throttle", noStorage)
	c := newTestClient(t, f)

	_, err := c.Throttle(t.Context())
	if !errors.Is(err, ErrNoStorage) || IsRetryable(err) {
		t.Errorf("Throttle err = %v, want ErrNoStorage, not retryable", err)
	}

	var e *Error
	if !errors.As(err, &e) || e.Code != CodeStorageConnection || e.Message != "no direct storage connection" {
		t.Errorf("err = %#v", err)
	}

	err = c.SetThrottle(t.Context(), ThrottleLimits{})
	if !errors.Is(err, ErrNoStorage) || IsRetryable(err) {
		t.Errorf("SetThrottle err = %v, want ErrNoStorage, not retryable", err)
	}

	// Other errors stay as they are.
	f.handle("PUT repo/throttle", func(w http.ResponseWriter, _ *http.Request) {
		writeAPIError(w, http.StatusBadRequest, CodeMalformedRequest, "unable to set limits: invalid limit")
	})

	if err := c.SetThrottle(t.Context(), ThrottleLimits{}); !errors.Is(err, ErrMalformedRequest) {
		t.Errorf("err = %v, want ErrMalformedRequest", err)
	}
}
