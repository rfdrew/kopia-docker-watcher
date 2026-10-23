package labels

import (
	"maps"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
)

// spec returns Default() changed by mod.
func spec(mod func(s *ContainerSpec)) ContainerSpec {
	s := Default()
	if mod != nil {
		mod(&s)
	}

	return s
}

// issueStrings formats issues as "<severity> <label>: <message>".
func issueStrings(is Issues) []string {
	out := make([]string, 0, len(is))
	for _, i := range is {
		out = append(out, i.Severity.String()+" "+i.String())
	}

	return out
}

func checkParse(t *testing.T, labels map[string]string, opts Options, want ContainerSpec, wantIssues []string) {
	t.Helper()

	got, issues := Parse(labels, opts)

	if !reflect.DeepEqual(got, want) {
		t.Errorf("spec:\n got %+v\nwant %+v", got, want)
	}

	if gotIssues := issueStrings(issues); !slices.Equal(gotIssues, wantIssues) {
		t.Errorf("issues:\n got %q\nwant %q", gotIssues, wantIssues)
	}
}

func TestDefault(t *testing.T) {
	want := ContainerSpec{
		SnapshotOnStart: true,
		MinInterval:     10 * time.Minute,
		Policy:          backend.PolicyPatch{Scheduling: backend.SchedulingPatch{Cron: []string{"0 * * * *"}}},
		CleanupOnRemove: CleanupPause,
	}

	if got := Default(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Default() = %+v, want %+v", got, want)
	}

	// Each call returns its own slice.
	a := Default()
	a.Policy.Scheduling.Cron[0] = "changed"

	if Default().Policy.Scheduling.Cron[0] != DefaultSchedule {
		t.Fatal("Default() shares the cron slice")
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   ContainerSpec
		issues []string
	}{
		{
			name: "no labels",
			want: Default(),
		},
		{
			name: "labels of other prefixes are ignored",
			labels: map[string]string{
				"com.docker.compose.project": "app",
				"traefik.enable":             "true",
				"kopiax.enable":              "true",
				"kopia":                      "true",
				"Kopia.enable":               "true",
			},
			want: Default(),
		},
		{
			name:   "defaults of the Python version",
			labels: map[string]string{"kopia.enable": "true", "kopia.path": "/data"},
			want: spec(func(s *ContainerSpec) {
				s.Enable = true
				s.Paths = []string{"/data"}
			}),
		},
		{
			name: "every label of the Python version",
			labels: map[string]string{
				"kopia.enable":            "true",
				"kopia.path":              "/data,/logs",
				"kopia.schedule":          "30 2 * * *",
				"kopia.snapshot-on-start": "false",
				"kopia.snapshot-on-stop":  "true",
				"kopia.description":       " my app ",
				"kopia.tags":              "env:prod,app:myapp",
			},
			want: ContainerSpec{
				Enable:          true,
				Paths:           []string{"/data", "/logs"},
				SnapshotOnStart: false,
				SnapshotOnStop:  true,
				MinInterval:     DefaultMinInterval,
				Description:     "my app",
				Tags:            []Tag{{"env", "prod"}, {"app", "myapp"}},
				Policy:          backend.PolicyPatch{Scheduling: backend.SchedulingPatch{Cron: []string{"30 2 * * *"}}},
				CleanupOnRemove: CleanupPause,
			},
		},
		{
			name:   "empty schedule inherits",
			labels: map[string]string{"kopia.schedule": ""},
			want:   spec(func(s *ContainerSpec) { s.Policy.Scheduling.Cron = nil }),
		},
		{
			name:   "blank schedule inherits",
			labels: map[string]string{"kopia.schedule": "  ; ;"},
			want:   spec(func(s *ContainerSpec) { s.Policy.Scheduling.Cron = nil }),
		},
		{
			name:   "several schedules",
			labels: map[string]string{"kopia.schedule": " 0 2 * * * ;30 3 * * 0;"},
			want:   spec(func(s *ContainerSpec) { s.Policy.Scheduling.Cron = []string{"0 2 * * *", "30 3 * * 0"} }),
		},
		{
			name:   "commas stay inside one schedule",
			labels: map[string]string{"kopia.schedule": "0 0,12 * * 1-5"},
			want:   spec(func(s *ContainerSpec) { s.Policy.Scheduling.Cron = []string{"0 0,12 * * 1-5"} }),
		},
		{
			name:   "empty description means the container name",
			labels: map[string]string{"kopia.description": "  "},
			want:   Default(),
		},
		{
			// The required path error is added after the loop over the
			// labels and still lands in label order.
			name: "the missing path error is ordered too",
			labels: map[string]string{
				"kopia.enable": "true",
				"kopia.tags":   "standalone",
				"kopia.aaa":    "1",
			},
			want: spec(func(s *ContainerSpec) { s.Enable = true }),
			issues: []string{
				"warning kopia.aaa: unknown label, ignored",
				"error kopia.path: required when kopia.enable is true",
				`warning kopia.tags: tag "standalone" is not key:value, ignored`,
			},
		},
		{
			name: "issues are ordered by label",
			labels: map[string]string{
				"kopia.zzz":              "1",
				"kopia.enable":           "maybe",
				"kopia.snapshot-on-stop": "no",
				"kopia.aaa":              "1",
			},
			want: Default(),
			issues: []string{
				"warning kopia.aaa: unknown label, ignored",
				`warning kopia.enable: "maybe" is not a boolean, treated as false; use true or false`,
				`warning kopia.snapshot-on-stop: "no" is not a boolean, treated as false; use true or false`,
				"warning kopia.zzz: unknown label, ignored",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkParse(t, tt.labels, Options{}, tt.want, tt.issues)
		})
	}
}

func TestParsePaths(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		paths  []string
		issues []string
	}{
		{
			name:   "several paths",
			labels: map[string]string{"kopia.enable": "true", "kopia.path": "/data,/logs,/srv/app"},
			paths:  []string{"/data", "/logs", "/srv/app"},
		},
		{
			name:   "spaces and empty items are skipped",
			labels: map[string]string{"kopia.enable": "true", "kopia.path": " /data , ,, /logs ,"},
			paths:  []string{"/data", "/logs"},
		},
		{
			// HOST_PREFIX is joined and the result checked and cleaned
			// where the source path is built.
			name:   "paths are kept as written",
			labels: map[string]string{"kopia.enable": "true", "kopia.path": "/data/,data,/srv//app/../x"},
			paths:  []string{"/data/", "data", "/srv//app/../x"},
		},
		{
			name:   "duplicates",
			labels: map[string]string{"kopia.enable": "true", "kopia.path": "/data,/logs, /data"},
			paths:  []string{"/data", "/logs"},
			issues: []string{`warning kopia.path: "/data" is listed more than once`},
		},
		{
			name:   "required when enabled",
			labels: map[string]string{"kopia.enable": "true"},
			issues: []string{"error kopia.path: required when kopia.enable is true"},
		},
		{
			name:   "empty is missing",
			labels: map[string]string{"kopia.enable": "TRUE", "kopia.path": " , "},
			issues: []string{"error kopia.path: required when kopia.enable is true"},
		},
		{
			name:   "not required when disabled",
			labels: map[string]string{"kopia.enable": "false", "kopia.snapshot-on-stop": "true"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, issues := Parse(tt.labels, Options{})

			if !reflect.DeepEqual(got.Paths, tt.paths) {
				t.Errorf("Paths = %q, want %q", got.Paths, tt.paths)
			}

			if gotIssues := issueStrings(issues); !slices.Equal(gotIssues, tt.issues) {
				t.Errorf("issues:\n got %q\nwant %q", gotIssues, tt.issues)
			}
		})
	}
}

// Booleans follow the Python version: only "true" in any case is true.
func TestParseBooleans(t *testing.T) {
	tests := []struct {
		value string
		want  bool
		issue string // message of the warning, if any
	}{
		{value: "true", want: true},
		{value: "TRUE", want: true},
		{value: "True", want: true},
		{value: "false", want: false},
		{value: "False", want: false},
		// The Python version did not trim: " true" was false.
		{value: " true ", issue: `" true " is not a boolean, treated as false; remove the spaces around it`},
		{value: "\tTRUE", issue: `"\tTRUE" is not a boolean, treated as false; remove the spaces around it`},
		{value: "false ", issue: `"false " is not a boolean, treated as false; remove the spaces around it`},
		{value: "  ", issue: "empty value, treated as false; use true or false"},
		{value: "yes", issue: `"yes" is not a boolean, treated as false; use true or false`},
		{value: "1", issue: `"1" is not a boolean, treated as false; use true or false`},
		{value: "on", issue: `"on" is not a boolean, treated as false; use true or false`},
		{value: "t", issue: `"t" is not a boolean, treated as false; use true or false`},
		{value: "", issue: "empty value, treated as false; use true or false"},
	}

	field := map[string]func(ContainerSpec) bool{
		KeyEnable:          func(s ContainerSpec) bool { return s.Enable },
		KeySnapshotOnStart: func(s ContainerSpec) bool { return s.SnapshotOnStart },
		KeySnapshotOnStop:  func(s ContainerSpec) bool { return s.SnapshotOnStop },
	}

	for key, get := range field {
		for _, tt := range tests {
			t.Run(key+"="+tt.value, func(t *testing.T) {
				label := "kopia." + key
				// kopia.enable=true needs a path; it is not under test.
				got, issues := Parse(map[string]string{label: tt.value, "kopia.path": "/data"}, Options{})

				if get(got) != tt.want {
					t.Errorf("%s = %v, want %v", key, get(got), tt.want)
				}

				var want []string
				if tt.issue != "" {
					want = []string{"warning " + label + ": " + tt.issue}
				}

				if gotIssues := issueStrings(issues); !slices.Equal(gotIssues, want) {
					t.Errorf("issues = %q, want %q", gotIssues, want)
				}
			})
		}
	}
}

func TestParseTags(t *testing.T) {
	tests := []struct {
		name   string
		value  string
		want   []Tag
		issues []string
	}{
		{name: "empty", value: ""},
		{name: "one", value: "env:prod", want: []Tag{{"env", "prod"}}},
		{
			name:  "label order is kept",
			value: "z:1,a:2,m:3",
			want:  []Tag{{"z", "1"}, {"a", "2"}, {"m", "3"}},
		},
		{
			name:  "spaces are trimmed",
			value: " env : prod , app:my app ",
			want:  []Tag{{"env", "prod"}, {"app", "my app"}},
		},
		{
			name:  "value may contain colons",
			value: "url:http://example.com:8080",
			want:  []Tag{{"url", "http://example.com:8080"}},
		},
		{name: "empty value", value: "release:", want: []Tag{{"release", ""}}},
		{name: "empty items are skipped", value: ",env:prod,, ,", want: []Tag{{"env", "prod"}}},
		{
			name:   "not key:value",
			value:  "env:prod,standalone",
			want:   []Tag{{"env", "prod"}},
			issues: []string{`warning kopia.tags: tag "standalone" is not key:value, ignored`},
		},
		{
			name:   "empty key",
			value:  ":prod,app:x",
			want:   []Tag{{"app", "x"}},
			issues: []string{`warning kopia.tags: tag ":prod" has an empty key, ignored`},
		},
		{
			name:   "repeated key keeps the last value in the first place",
			value:  "env:dev,app:x,env:prod",
			want:   []Tag{{"env", "prod"}, {"app", "x"}},
			issues: []string{`warning kopia.tags: tag key "env" is repeated, the last value is used`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkParse(t, map[string]string{"kopia.tags": tt.value}, Options{},
				spec(func(s *ContainerSpec) { s.Tags = tt.want }), tt.issues)
		})
	}
}

func TestParseMinInterval(t *testing.T) {
	tests := []struct {
		value  string
		want   time.Duration
		issues []string
	}{
		{"10m", 10 * time.Minute, nil},
		{" 1h30m ", 90 * time.Minute, nil},
		{"0", 0, nil},
		{"0s", 0, nil},
		{"", DefaultMinInterval, []string{"warning kopia.min-interval: empty value, the default 10m0s is used; " + minIntervalHint}},
		{"10", DefaultMinInterval, []string{`warning kopia.min-interval: invalid duration "10", the default 10m0s is used; ` + minIntervalHint}},
		{"1d", DefaultMinInterval, []string{`warning kopia.min-interval: invalid duration "1d", the default 10m0s is used; ` + minIntervalHint}},
		{"-5m", DefaultMinInterval, []string{`warning kopia.min-interval: negative duration "-5m", the default 10m0s is used; ` + minIntervalHint}},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			checkParse(t, map[string]string{"kopia.min-interval": tt.value}, Options{},
				spec(func(s *ContainerSpec) { s.MinInterval = tt.want }), tt.issues)
		})
	}

	t.Run("strict", func(t *testing.T) {
		checkParse(t, map[string]string{"kopia.min-interval": "1d"}, Options{Strict: true}, Default(),
			[]string{`error kopia.min-interval: invalid duration "1d"; ` + minIntervalHint})
	})
}

func TestParseCleanup(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		mode   CleanupMode
		pinned bool
		issues []string
	}{
		{name: "default", mode: CleanupPause},
		{name: "pause", labels: map[string]string{"kopia.cleanup-on-remove": "pause"}, mode: CleanupPause},
		{name: "policy", labels: map[string]string{"kopia.cleanup-on-remove": "policy"}, mode: CleanupPolicy},
		{name: "all", labels: map[string]string{"kopia.cleanup-on-remove": "all"}, mode: CleanupAll},
		{name: "any case and spaces", labels: map[string]string{"kopia.cleanup-on-remove": " ALL "}, mode: CleanupAll},
		{
			name:   "all with pinned snapshots",
			labels: map[string]string{"kopia.cleanup-on-remove": "all", "kopia.cleanup-delete-pinned": "true"},
			mode:   CleanupAll,
			pinned: true,
		},
		{
			name:   "pinned set to false",
			labels: map[string]string{"kopia.cleanup-on-remove": "all", "kopia.cleanup-delete-pinned": "false"},
			mode:   CleanupAll,
		},
		{
			// A typo must not delete anything: the most cautious mode.
			name:   "unknown mode",
			labels: map[string]string{"kopia.cleanup-on-remove": "everything"},
			mode:   CleanupPause,
			issues: []string{`warning kopia.cleanup-on-remove: unknown cleanup mode "everything", the source is only paused; use pause, policy or all`},
		},
		{
			name:   "empty mode",
			labels: map[string]string{"kopia.cleanup-on-remove": " "},
			mode:   CleanupPause,
			issues: []string{"warning kopia.cleanup-on-remove: empty value, the source is only paused; use pause, policy or all"},
		},
		{
			name:   "pinned without all",
			labels: map[string]string{"kopia.cleanup-on-remove": "policy", "kopia.cleanup-delete-pinned": "true"},
			mode:   CleanupPolicy,
			pinned: true,
			issues: []string{"warning kopia.cleanup-delete-pinned: has no effect unless kopia.cleanup-on-remove is all"},
		},
		{
			// Only "true" is true, as for every boolean.
			name:   "pinned not a boolean",
			labels: map[string]string{"kopia.cleanup-on-remove": "all", "kopia.cleanup-delete-pinned": "yes"},
			mode:   CleanupAll,
			issues: []string{`warning kopia.cleanup-delete-pinned: "yes" is not a boolean, treated as false; use true or false`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := spec(func(s *ContainerSpec) {
				s.CleanupOnRemove = tt.mode
				s.CleanupDeletePinned = tt.pinned
			})

			checkParse(t, tt.labels, Options{}, want, tt.issues)
		})
	}

	t.Run("strict", func(t *testing.T) {
		_, issues := Parse(map[string]string{"kopia.cleanup-on-remove": "everything"}, Options{Strict: true})
		if got := issueStrings(issues); !slices.Equal(got, []string{`error kopia.cleanup-on-remove: unknown cleanup mode "everything"; use pause, policy or all`}) {
			t.Errorf("issues = %q", got)
		}
	})
}

func TestParseUnknownKeys(t *testing.T) {
	tests := []struct {
		label string
		issue string
	}{
		{"kopia.bogus", "unknown label, ignored"},
		{"kopia.path.extra", "unknown label, ignored"},
		{"kopia.Enable", "unknown label, ignored; did you mean kopia.enable?"},
		{"kopia.snapshot_on_stop", "unknown label, ignored; did you mean kopia.snapshot-on-stop?"},
		{"kopia.volumes", "not supported by this version, ignored"},
		{"kopia.pre-exec", "not supported by this version, ignored"},
		{"kopia.Retention.Daily", "unknown label, ignored; did you mean kopia.retention.daily?"},
		{"kopia.compression", "unknown label, ignored; it is a group of labels such as kopia.compression.algorithm"},
		{"kopia.retention", "unknown label, ignored; it is a group of labels such as kopia.retention.latest"},
		{"kopia.Compression", "unknown label, ignored; it is a group of labels such as kopia.compression.algorithm"},
		{"kopia.", "unknown label, ignored"},
		{"kopia..enable", "unknown label, ignored"},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			// An unknown label does not change the spec.
			checkParse(t, map[string]string{tt.label: "1"}, Options{}, Default(), []string{"warning " + tt.label + ": " + tt.issue})
		})
	}
}

func TestParseStrict(t *testing.T) {
	labels := map[string]string{
		"kopia.enable":           "true",
		"kopia.path":             "/data,/data",
		"kopia.snapshot-on-stop": "yes",
		"kopia.tags":             "standalone,env:dev,env:prod",
		"kopia.bogus":            "1",
		"kopia.Enable":           "true",
		"kopia.pre-exec":         "pg_dump",
	}

	want := spec(func(s *ContainerSpec) {
		s.Enable = true
		s.Paths = []string{"/data"}
		s.Tags = []Tag{{"env", "prod"}}
	})

	t.Run("lenient", func(t *testing.T) {
		checkParse(t, labels, Options{}, want, []string{
			"warning kopia.Enable: unknown label, ignored; did you mean kopia.enable?",
			"warning kopia.bogus: unknown label, ignored",
			`warning kopia.path: "/data" is listed more than once`,
			"warning kopia.pre-exec: not supported by this version, ignored",
			`warning kopia.snapshot-on-stop: "yes" is not a boolean, treated as false; use true or false`,
			`warning kopia.tags: tag "standalone" is not key:value, ignored`,
			`warning kopia.tags: tag key "env" is repeated, the last value is used`,
		})
	})

	// Every warning is an error, and the message does not claim a
	// workaround.
	t.Run("strict", func(t *testing.T) {
		checkParse(t, labels, Options{Strict: true}, want, []string{
			"error kopia.Enable: unknown label; did you mean kopia.enable?",
			"error kopia.bogus: unknown label",
			`error kopia.path: "/data" is listed more than once`,
			"error kopia.pre-exec: not supported by this version",
			`error kopia.snapshot-on-stop: "yes" is not a boolean; use true or false`,
			`error kopia.tags: tag "standalone" is not key:value`,
			`error kopia.tags: tag key "env" is repeated`,
		})
	})

	t.Run("strict without problems", func(t *testing.T) {
		_, issues := Parse(map[string]string{"kopia.enable": "true", "kopia.path": "/data"}, Options{Strict: true})
		if len(issues) != 0 {
			t.Fatalf("issues = %q", issueStrings(issues))
		}
	})
}

func TestParseLabelPrefix(t *testing.T) {
	labels := map[string]string{
		"backup.kopia.enable":            "true",
		"backup.kopia.path":              "/data",
		"backup.kopia.schedule":          "",
		"backup.kopia.snapshot-on-start": "false",
		"backup.kopia.bogus":             "1",
		// The default prefix is not read when another one is set.
		"kopia.enable":           "false",
		"kopia.path":             "/other",
		"kopia.snapshot-on-stop": "true",
	}

	want := spec(func(s *ContainerSpec) {
		s.Enable = true
		s.Paths = []string{"/data"}
		s.SnapshotOnStart = false
		s.Policy.Scheduling.Cron = nil
	})

	checkParse(t, labels, Options{Prefix: "backup.kopia"}, want, []string{"warning backup.kopia.bogus: unknown label, ignored"})

	// Issues name the labels with the prefix in use.
	_, issues := Parse(map[string]string{"backup.enable": "true", "backup.Path": "/x"}, Options{Prefix: "backup"})
	if got, want := issueStrings(issues), []string{
		"warning backup.Path: unknown label, ignored; did you mean backup.path?",
		"error backup.path: required when backup.enable is true",
	}; !slices.Equal(got, want) {
		t.Errorf("issues:\n got %q\nwant %q", got, want)
	}

	// No prefix means the default one.
	got, _ := Parse(map[string]string{"kopia.enable": "true", "kopia.path": "/x"}, Options{})
	if !got.Enable {
		t.Error("the default prefix is not kopia")
	}
}

func TestIssues(t *testing.T) {
	if (Issues{{Severity: Warning}}).HasErrors() {
		t.Error("warnings only: HasErrors() = true")
	}

	if !(Issues{{Severity: Warning}, {Severity: Error}}).HasErrors() {
		t.Error("with an error: HasErrors() = false")
	}

	if Issues(nil).HasErrors() {
		t.Error("nil: HasErrors() = true")
	}

	for s, want := range map[Severity]string{Warning: "warning", Error: "error", 0: "Severity(0)"} {
		if s.String() != want {
			t.Errorf("Severity(%d).String() = %q, want %q", int(s), s.String(), want)
		}
	}

	is := Issue{Label: "kopia.path", Severity: Error, Message: "required"}
	if is.String() != "kopia.path: required" {
		t.Errorf("Issue.String() = %q", is.String())
	}

	if Name("kopia", KeySnapshotOnStart) != "kopia.snapshot-on-start" {
		t.Errorf("Name() = %q", Name("kopia", KeySnapshotOnStart))
	}
}

// The parser does not change the map it reads.
func TestParseLeavesInputAlone(t *testing.T) {
	in := map[string]string{"kopia.enable": "true", "kopia.path": "/data/", "kopia.tags": " a : b "}
	before := maps.Clone(in)

	Parse(in, Options{})

	if !maps.Equal(in, before) {
		t.Fatalf("input changed: %v", in)
	}
}
