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
