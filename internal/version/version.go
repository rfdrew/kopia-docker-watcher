// Package version holds build information injected at link time.
package version

// Build information, set with -ldflags "-X ...". goreleaser and
// `make build` fill all three; a plain `go build` leaves the defaults.
var (
	// Version is the release version of the binary, for example "v0.2.0".
	Version = "dev"
	// Commit is the full git commit the binary was built from.
	Commit = "unknown"
	// Date is the commit time in RFC 3339, which keeps builds reproducible.
	Date = "unknown"
)

// UserAgent returns the User-Agent sent to kopia server.
func UserAgent() string {
	return "kopia-docker-watcher/" + Version
}
