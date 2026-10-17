// Package version holds build information injected at link time.
package version

// Version is the release version of the binary. It is set with
// -ldflags "-X github.com/rfdrew/kopia-docker-watcher/internal/version.Version=v0.1.0".
var Version = "dev"

// UserAgent returns the User-Agent sent to kopia server.
func UserAgent() string {
	return "kopia-docker-watcher/" + Version
}
