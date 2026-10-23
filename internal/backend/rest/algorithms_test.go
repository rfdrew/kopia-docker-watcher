package rest

import (
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
)

// The algorithms are asked once and kept; a failed answer is not kept.
func TestCompressionAlgorithms(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)

	f.inject("GET repo/algorithms", injection{status: http.StatusServiceUnavailable, body: "unavailable"})

	_, err := b.CompressionAlgorithms(t.Context())
	assertError(t, err, opAlgorithms, backend.Source{}, backend.ErrUnavailable)

	want := []backend.Algorithm{
		{Name: "deflate-default", Deprecated: true}, {Name: "gzip"}, {Name: "s2-default"}, {Name: "zstd"},
	}

	for range 3 {
		got, err := b.CompressionAlgorithms(t.Context())
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("CompressionAlgorithms = %+v, %v; want %+v", got, err, want)
		}

		// The caller's copy is its own.
		got[0].Name = "changed"
	}

	if n := len(f.requestsTo("GET repo/algorithms")); n != 2 {
		t.Errorf("%d requests, want 2: the failed one and one kept", n)
	}
}

func TestCompressionAlgorithmsNone(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)

	f.inject("GET repo/algorithms", injection{status: http.StatusOK, body: `{"compression":[{"id":""}]}`})

	_, err := b.CompressionAlgorithms(t.Context())
	assertError(t, err, opAlgorithms, backend.Source{}, nil)

	if !errors.Is(err, kopiaclient.ErrUnexpectedResponse) {
		t.Errorf("err = %v, want ErrUnexpectedResponse", err)
	}

	// Not kept either.
	if _, err := b.CompressionAlgorithms(t.Context()); err != nil {
		t.Errorf("second call: %v", err)
	}
}
