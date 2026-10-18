package backend

import (
	"errors"
	"fmt"
	"strings"
)

// Error kinds. A failed Backend call is an *Error whose Kind is one of
// them or nil; errors.Is(err, kind) tells the caller what to do. An error
// of neither kind is not about the source and will not go away by itself:
// wrong credentials, CSRF, TLS verification, access denied, a kopia server
// without the UI API, a read-only repository, an answer that does not
// decode. The reconciler reports it and tries again at the next full
// reconcile. An error because the caller's context is done is of neither
// kind too.
var (
	// ErrTemporary: the call may succeed later without any change. Retry
	// the source with a growing delay.
	ErrTemporary = errors.New("temporary failure")

	// ErrUnavailable: kopia server cannot be reached or has no repository
	// connected. It is also ErrTemporary, but it concerns every source
	// alike: the reconciler can hold the whole queue behind one delay and
	// report kopia server as down.
	ErrUnavailable = fmt.Errorf("kopia server unavailable: %w", ErrTemporary)

	// ErrConfig: what the watcher asked for is wrong for this source, such
	// as a policy kopia server rejects or a path it does not have. Asking
	// again right away fails the same way, so the source is not retried
	// with a delay; the error is reported on the source (log and metric)
	// and the source is tried again when its desired state changes and at
	// every full reconcile, since some causes are fixed outside the
	// watcher (a volume mounted into kopia server later).
	ErrConfig = errors.New("configuration error")
)

// Errors a Backend reports in Error.Err, with Kind nil.
var (
	// ErrIncompleteSource: the source lacks its host, user name or path.
	ErrIncompleteSource = errors.New("incomplete source")

	// ErrWrongIdentity: the source is not kopia server's own. Its user
	// name or host differs from ServerIdentity, which changes when kopia
	// server connects to the repository under another name. The
	// reconciler asks for the identity again and rebuilds its sources.
	ErrWrongIdentity = errors.New("source does not belong to kopia server's identity")

	// ErrSnapshotNotFound: no snapshot matches. For AnnotateSnapshot this
	// is final: the snapshot was never saved (an identical one was
	// skipped) or has been deleted since.
	ErrSnapshotNotFound = errors.New("snapshot not found")

	// ErrNotImplemented: the backend does not implement the method yet.
	ErrNotImplemented = errors.New("not implemented")
)

// Error is a failed Backend call.
type Error struct {
	// Op is the Backend method, e.g. "ApplyPlan".
	Op string
	// Source is the source the call was about; zero if none.
	Source Source
	// Kind is ErrTemporary, ErrUnavailable, ErrConfig or nil.
	Kind error
	// Err is the cause, e.g. a *kopiaclient.Error or ErrWrongIdentity.
	Err error
}

func (e *Error) Error() string {
	var b strings.Builder

	b.WriteString(e.Op)

	if e.Source != (Source{}) {
		b.WriteString(" ")
		b.WriteString(e.Source.String())
	}

	if e.Kind != nil {
		b.WriteString(": ")
		b.WriteString(e.Kind.Error())
	}

	if e.Err != nil {
		b.WriteString(": ")
		b.WriteString(e.Err.Error())
	}

	return b.String()
}

// Unwrap returns Kind and Err, so that errors.Is and errors.As see both:
// errors.Is(err, ErrConfig) and errors.Is(err, kopiaclient.ErrInvalidPolicy).
func (e *Error) Unwrap() []error {
	out := make([]error, 0, 2)

	if e.Kind != nil {
		out = append(out, e.Kind)
	}

	if e.Err != nil {
		out = append(out, e.Err)
	}

	return out
}

// IsTemporary reports whether err is worth retrying with a delay. It is
// true for ErrUnavailable too.
func IsTemporary(err error) bool { return errors.Is(err, ErrTemporary) }

// IsUnavailable reports whether kopia server could not be reached.
func IsUnavailable(err error) bool { return errors.Is(err, ErrUnavailable) }

// IsConfig reports whether err is a configuration error of the source.
func IsConfig(err error) bool { return errors.Is(err, ErrConfig) }
