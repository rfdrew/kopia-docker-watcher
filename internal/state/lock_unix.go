//go:build unix

package state

import (
	"errors"
	"os"
	"syscall"
)

// errLockUnsupported: the file system cannot lock files.
var errLockUnsupported = errors.New("file locks are not supported")

// lockFile takes an exclusive flock on f without waiting. The kernel
// releases it when f is closed or the process ends, also when it is
// killed.
func lockFile(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)

	switch {
	case err == nil:
		return nil
	case errors.Is(err, syscall.EWOULDBLOCK):
		return ErrLocked
	case errors.Is(err, syscall.ENOLCK), errors.Is(err, syscall.EOPNOTSUPP), errors.Is(err, syscall.ENOTSUP):
		return errors.Join(errLockUnsupported, err)
	default:
		return err
	}
}
