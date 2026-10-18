package version

import "testing"

func TestUserAgent(t *testing.T) {
	old := Version
	t.Cleanup(func() { Version = old })

	Version = "v1.2.3"

	if got, want := UserAgent(), "kopia-docker-watcher/v1.2.3"; got != want {
		t.Fatalf("UserAgent() = %q, want %q", got, want)
	}
}
