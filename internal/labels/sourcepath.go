package labels

import (
	"fmt"
	"path"
	"strings"
)

// SourcePath builds the path of a backup source from one path of the path
// label: hostPrefix (HOST_PREFIX: empty, or a clean absolute path) and the
// label path joined with a slash, then cleaned. The result must be
// absolute; without a prefix, a relative label path is an error. With a
// prefix, the label path counts from it, as in the Python version, which
// concatenated the two, and ".." must not lead out of the prefix.
//
// The result has no trailing slash, which Kopia rejects in a policy path.
// Kopia's own normalization (paths/resolve) can still change it, e.g. on a
// Windows server.
func SourcePath(hostPrefix, p string) (string, error) {
	joined := p
	if hostPrefix != "" {
		joined = hostPrefix + "/" + p
	}

	if !path.IsAbs(joined) {
		return "", fmt.Errorf("%q is not an absolute path; set an absolute path or HOST_PREFIX", p)
	}

	clean := path.Clean(joined)
	if hostPrefix != "" && clean != hostPrefix && !strings.HasPrefix(clean, hostPrefix+"/") {
		return "", fmt.Errorf("%q leads out of HOST_PREFIX %s", p, hostPrefix)
	}

	return clean, nil
}
