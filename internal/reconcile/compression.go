package reconcile

import (
	"slices"
	"strings"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
)

// checkCompression checks the compression algorithm each owner's labels
// ask for against the ones kopia server supports. An owner asking for one
// kopia server lacks has labels that cannot be applied. kopia server is
// asked only when an owner sets an algorithm; the backend keeps its
// answer.
func (ps *pass) checkCompression() error {
	for i := range ps.owners {
		o := &ps.owners[i]

		if name, _ := labels.Compressor(o.patch); !o.valid || name == "" || name == labels.NoCompression {
			continue
		}

		algos, err := ps.backend.CompressionAlgorithms(ps.ctx)
		if err != nil {
			return err
		}

		issues := labels.CheckCompressor(ps.labelPrefix, o.patch, algos, ps.strict)
		if !issues.HasErrors() {
			for _, is := range issues {
				ps.warnOnce("compression "+o.name+" "+is.String(), "the labels of a container ask for a deprecated compression algorithm",
					"container", o.name, "label", is.Label, "warning", is.Message)
			}

			continue
		}

		// The labels cannot be applied: as for an error found when they
		// were parsed, the container's snapshots get its name only.
		o.issues = append(o.issues, issues...)
		slices.SortStableFunc(o.issues, func(a, b labels.Issue) int { return strings.Compare(a.Label, b.Label) })
		o.valid = false
		o.patch = backend.PolicyPatch{}
		o.snap.described.description = ""
		o.snap.described.tags = nil
	}

	return nil
}
