package backend

import (
	"cmp"
	"fmt"
)

// Source identifies a snapshot source: user@host:path. The watcher only
// works with sources of kopia server's own identity (ServerIdentity), and
// always with all three fields: without host and user name kopia server
// would match the sources of other users on the same host, and a policy
// without a path is a host or global policy, which the watcher does not
// own.
//
// Its JSON form is stored in STATE_FILE.
type Source struct {
	Host     string `json:"host"`
	UserName string `json:"userName"`
	Path     string `json:"path"`
}

// String formats the source like Kopia does: user@host:path.
func (s Source) String() string {
	return s.UserName + "@" + s.Host + ":" + s.Path
}

// Validate reports a source with an empty field.
func (s Source) Validate() error {
	if s.Host == "" || s.UserName == "" || s.Path == "" {
		return fmt.Errorf("%w: %q needs host, user name and path", ErrIncompleteSource, s.String())
	}

	return nil
}

// Compare orders sources by host, user name and path.
func (s Source) Compare(t Source) int {
	return cmp.Or(cmp.Compare(s.Host, t.Host), cmp.Compare(s.UserName, t.UserName), cmp.Compare(s.Path, t.Path))
}
