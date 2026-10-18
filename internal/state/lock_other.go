//go:build !unix

package state

import (
	"errors"
	"os"
)

// errLockUnsupported: the platform has no file locks this package uses.
var errLockUnsupported = errors.New("file locks are not supported on this platform")

func lockFile(*os.File) error { return errLockUnsupported }
