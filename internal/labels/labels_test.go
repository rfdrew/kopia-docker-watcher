package labels

import (
	"maps"
	"reflect"
	"slices"
	"testing"
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
		Policy:          Policy{Scheduling: Scheduling{Cron: []string{"0 * * * *"}}},
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
				Description:     "my app",
				Tags:            []Tag{{"env", "prod"}, {"app", "myapp"}},
				Policy:          Policy{Scheduling: Scheduling{Cron: []string{"30 2 * * *"}}},
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

func TestParseUnknownKeys(t *testing.T) {
	tests := []struct {
		label string
		issue string
	}{
		{"kopia.bogus", "unknown label, ignored"},
		{"kopia.path.extra", "unknown label, ignored"},
		{"kopia.Enable", "unknown label, ignored; did you mean kopia.enable?"},
		{"kopia.snapshot_on_stop", "unknown label, ignored; did you mean kopia.snapshot-on-stop?"},
		{"kopia.retention.daily", "not supported by this version, ignored"},
		{"kopia.compression.algorithm", "not supported by this version, ignored"},
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
		"kopia.retention.daily":  "7",
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
			"warning kopia.retention.daily: not supported by this version, ignored",
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
			"error kopia.retention.daily: not supported by this version",
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
