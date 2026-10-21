package labels

import (
	"strings"
	"testing"
)

func TestSourcePath(t *testing.T) {
	tests := []struct {
		prefix, path, want, err string
	}{
		{"", "/data", "/data", ""},
		{"", "/data/", "/data", ""},
		{"", "/srv//app/./db/..", "/srv/app", ""},
		{"", "/", "/", ""},
		{"/host", "/data", "/host/data", ""},
		{"/host", "data", "/host/data", ""},
		{"/host", "/data/", "/host/data", ""},
		{"/host", "/", "/host", ""},
		{"/host", "a/../b", "/host/b", ""},
		{"/host", "../etc", "", `"../etc" leads out of HOST_PREFIX /host`},
		{"/host", "/..", "", "leads out of HOST_PREFIX"},
		{"/host", "../hostile", "", "leads out of HOST_PREFIX"},
		{"", "data", "", `"data" is not an absolute path`},
		{"", "~/data", "", `"~/data" is not an absolute path`},
		{"", "./data", "", "not an absolute path"},
	}

	for _, tt := range tests {
		got, err := SourcePath(tt.prefix, tt.path)

		switch {
		case tt.err != "" && (err == nil || !strings.Contains(err.Error(), tt.err)):
			t.Errorf("SourcePath(%q, %q) error = %v, want %q", tt.prefix, tt.path, err, tt.err)
		case tt.err == "" && err != nil:
			t.Errorf("SourcePath(%q, %q) error = %v", tt.prefix, tt.path, err)
		case got != tt.want:
			t.Errorf("SourcePath(%q, %q) = %q, want %q", tt.prefix, tt.path, got, tt.want)
		}
	}
}

func TestSourcePathWithin(t *testing.T) {
	tests := []struct {
		host, watcher, path, want, err string
	}{
		{"", "", "/data", "/data", ""},
		{"", "/", "/data", "/data", ""},
		{"", "/data", "/data", "/data", ""},
		{"", "/data", "/data/app/", "/data/app", ""},
		{"", "/data", "/database", "", `"/database" makes the source path /database, which is outside WATCHER_PATH_PREFIX /data`},
		{"", "/data", "/data/../etc", "", "outside WATCHER_PATH_PREFIX /data"},
		{"", "/data", "/", "", "outside WATCHER_PATH_PREFIX"},
		// The prefix applies after HOST_PREFIX.
		{"/host", "/host/volumes", "volumes/app", "/host/volumes/app", ""},
		{"/host", "/host/volumes", "/etc", "", "the source path /host/etc, which is outside"},
		// A HOST_PREFIX error comes first.
		{"/host", "/host", "../etc", "", "leads out of HOST_PREFIX"},
	}

	for _, tt := range tests {
		got, err := SourcePathWithin(tt.host, tt.watcher, tt.path)

		switch {
		case tt.err != "" && (err == nil || !strings.Contains(err.Error(), tt.err)):
			t.Errorf("SourcePathWithin(%q, %q, %q) error = %v, want %q", tt.host, tt.watcher, tt.path, err, tt.err)
		case tt.err == "" && err != nil:
			t.Errorf("SourcePathWithin(%q, %q, %q) error = %v", tt.host, tt.watcher, tt.path, err)
		case got != tt.want:
			t.Errorf("SourcePathWithin(%q, %q, %q) = %q, want %q", tt.host, tt.watcher, tt.path, got, tt.want)
		}
	}
}

func TestWithinPrefix(t *testing.T) {
	for _, tt := range []struct {
		prefix, path string
		want         bool
	}{
		{"", "/anything", true},
		{"/", "/anything", true},
		{"/data", "/data", true},
		{"/data", "/data/app", true},
		{"/data", "/database", false},
		{"/data", "/", false},
		{"/data/app", "/data", false},
	} {
		if got := WithinPrefix(tt.prefix, tt.path); got != tt.want {
			t.Errorf("WithinPrefix(%q, %q) = %t", tt.prefix, tt.path, got)
		}
	}
}
