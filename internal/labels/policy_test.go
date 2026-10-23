package labels

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
)

func ptr[T any](v T) *T { return &v }

// policy returns the default policy changed by mod.
func policy(mod func(p *backend.PolicyPatch)) ContainerSpec {
	return spec(func(s *ContainerSpec) { mod(&s.Policy) })
}

func TestParsePolicyLabels(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   ContainerSpec
		issues []string
	}{
		// Schedule.
		{
			name:   "interval",
			labels: map[string]string{"kopia.interval": " 6h "},
			want: policy(func(p *backend.PolicyPatch) {
				p.Scheduling = backend.SchedulingPatch{IntervalSeconds: 21600}
			}),
		},
		{
			name:   "interval of whole seconds",
			labels: map[string]string{"kopia.interval": "1h30m15s"},
			want:   policy(func(p *backend.PolicyPatch) { p.Scheduling = backend.SchedulingPatch{IntervalSeconds: 5415} }),
		},
		{
			name:   "interval of the minimum",
			labels: map[string]string{"kopia.interval": "60s"},
			want:   policy(func(p *backend.PolicyPatch) { p.Scheduling = backend.SchedulingPatch{IntervalSeconds: 60} }),
		},
		{
			name:   "interval with a schedule keeps both",
			labels: map[string]string{"kopia.interval": "6h", "kopia.schedule": "0 3 * * *"},
			want: policy(func(p *backend.PolicyPatch) {
				p.Scheduling = backend.SchedulingPatch{Cron: []string{"0 3 * * *"}, IntervalSeconds: 21600}
			}),
		},
		{
			name:   "empty interval inherits and keeps the default schedule",
			labels: map[string]string{"kopia.interval": " "},
			want:   Default(),
		},
		{
			name:   "interval too short",
			labels: map[string]string{"kopia.interval": "30s"},
			want:   Default(),
			issues: []string{`error kopia.interval: "30s" is shorter than 1m0s; use a Go duration such as 30m, 6h or 24h (d is not a unit) of at least 1m`},
		},
		{
			name:   "interval of a fraction of a second",
			labels: map[string]string{"kopia.interval": "90.5s"},
			want:   Default(),
			issues: []string{`error kopia.interval: "90.5s" is not a whole number of seconds`},
		},
		{
			name:   "interval in days",
			labels: map[string]string{"kopia.interval": "1d"},
			want:   Default(),
			issues: []string{`error kopia.interval: invalid duration "1d"; use a Go duration such as 30m, 6h or 24h (d is not a unit) of at least 1m`},
		},
		{
			name:   "negative interval",
			labels: map[string]string{"kopia.interval": "-1h"},
			want:   Default(),
			issues: []string{`error kopia.interval: "-1h" is shorter than 1m0s; use a Go duration such as 30m, 6h or 24h (d is not a unit) of at least 1m`},
		},
		{
			name:   "times of day are sorted",
			labels: map[string]string{"kopia.time-of-day": "14:00, 2:30,,00:05"},
			want: policy(func(p *backend.PolicyPatch) {
				p.Scheduling = backend.SchedulingPatch{TimeOfDay: []backend.TimeOfDay{{Hour: 0, Minute: 5}, {Hour: 2, Minute: 30}, {Hour: 14}}}
			}),
		},
		{
			name:   "repeated time of day",
			labels: map[string]string{"kopia.time-of-day": "02:30,2:30"},
			want: policy(func(p *backend.PolicyPatch) {
				p.Scheduling = backend.SchedulingPatch{TimeOfDay: []backend.TimeOfDay{{Hour: 2, Minute: 30}}}
			}),
			issues: []string{`warning kopia.time-of-day: "2:30" is listed more than once`},
		},
		{
			name:   "bad times of day",
			labels: map[string]string{"kopia.time-of-day": "24:00,12:60,1:5,noon,12:00"},
			want: policy(func(p *backend.PolicyPatch) {
				p.Scheduling = backend.SchedulingPatch{TimeOfDay: []backend.TimeOfDay{{Hour: 12}}}
			}),
			issues: []string{
				`error kopia.time-of-day: "24:00" is not a time of day: the hour goes up to 23 and the minute up to 59`,
				`error kopia.time-of-day: "12:60" is not a time of day: the hour goes up to 23 and the minute up to 59`,
				`error kopia.time-of-day: "1:5" is not a time of day; use HH:MM in 24-hour time, such as 02:30`,
				`error kopia.time-of-day: "noon" is not a time of day; use HH:MM in 24-hour time, such as 02:30`,
			},
		},
		{
			name:   "empty times of day inherit",
			labels: map[string]string{"kopia.time-of-day": " , "},
			want:   Default(),
		},
		{
			name:   "run-missed alone keeps the default schedule",
			labels: map[string]string{"kopia.run-missed": "TRUE"},
			want:   policy(func(p *backend.PolicyPatch) { p.Scheduling.RunMissed = ptr(true) }),
		},
		{
			name:   "run-missed false",
			labels: map[string]string{"kopia.run-missed": "false"},
			want:   policy(func(p *backend.PolicyPatch) { p.Scheduling.RunMissed = ptr(false) }),
		},
		{
			name:   "run-missed that is not a boolean",
			labels: map[string]string{"kopia.run-missed": "yes"},
			want:   policy(func(p *backend.PolicyPatch) { p.Scheduling.RunMissed = ptr(false) }),
			issues: []string{`warning kopia.run-missed: "yes" is not a boolean, treated as false; use true or false`},
		},
		{
			name:   "empty run-missed inherits",
			labels: map[string]string{"kopia.run-missed": ""},
			want:   Default(),
		},
		{
			name:   "an empty schedule with an interval",
			labels: map[string]string{"kopia.schedule": "", "kopia.time-of-day": "03:00"},
			want: policy(func(p *backend.PolicyPatch) {
				p.Scheduling = backend.SchedulingPatch{TimeOfDay: []backend.TimeOfDay{{Hour: 3}}}
			}),
		},

		// Retention.
		{
			name: "retention",
			labels: map[string]string{
				"kopia.retention.latest": "10", "kopia.retention.hourly": "0", "kopia.retention.daily": " 7 ",
				"kopia.retention.weekly": "4", "kopia.retention.monthly": "12", "kopia.retention.annual": "2147483647",
				"kopia.retention.ignore-identical": "true",
			},
			want: policy(func(p *backend.PolicyPatch) {
				p.Retention = backend.RetentionPatch{
					KeepLatest: ptr(10), KeepHourly: ptr(0), KeepDaily: ptr(7), KeepWeekly: ptr(4), KeepMonthly: ptr(12),
					KeepAnnual: ptr(2147483647), IgnoreIdenticalSnapshots: ptr(true),
				}
			}),
		},
		{
			name: "bad retention",
			labels: map[string]string{
				"kopia.retention.latest": "-1", "kopia.retention.daily": "7d", "kopia.retention.weekly": "2147483648",
				"kopia.retention.monthly": "99999999999999999999", "kopia.retention.annual": "1.5",
			},
			want: Default(),
			issues: []string{
				`error kopia.retention.annual: "1.5" is not a whole number`,
				`error kopia.retention.daily: "7d" is not a whole number`,
				`error kopia.retention.latest: "-1" is less than 0`,
				`error kopia.retention.monthly: "99999999999999999999" is too large; the most is 2147483647`,
				`error kopia.retention.weekly: "2147483648" is too large; the most is 2147483647`,
			},
		},
		{
			name:   "empty retention inherits",
			labels: map[string]string{"kopia.retention.daily": "", "kopia.retention.ignore-identical": " "},
			want:   Default(),
		},

		// Files.
		{
			name: "files",
			labels: map[string]string{
				"kopia.ignore": "*.tmp, cache/ ,,/node_modules", "kopia.ignore-files": ".kopiaignore,.gitignore",
				"kopia.ignore-cache-dirs": "true", "kopia.max-file-size": "1GiB", "kopia.one-file-system": "false",
			},
			want: policy(func(p *backend.PolicyPatch) {
				p.Files = backend.FilesPatch{
					Ignore: []string{"*.tmp", "cache/", "/node_modules"}, IgnoreDotFiles: []string{".kopiaignore", ".gitignore"},
					IgnoreCacheDirs: ptr(true), MaxFileSize: 1 << 30, OneFileSystem: ptr(false),
				}
			}),
		},
		{
			name:   "repeated ignore rule",
			labels: map[string]string{"kopia.ignore": "*.tmp,*.tmp"},
			want:   policy(func(p *backend.PolicyPatch) { p.Files.Ignore = []string{"*.tmp"} }),
			issues: []string{`warning kopia.ignore: "*.tmp" is listed more than once`},
		},
		{
			name:   "ignore file with a directory",
			labels: map[string]string{"kopia.ignore-files": ".kopiaignore,conf/.ignore"},
			want:   policy(func(p *backend.PolicyPatch) { p.Files.IgnoreDotFiles = []string{".kopiaignore"} }),
			issues: []string{`error kopia.ignore-files: "conf/.ignore" is not a file name: kopia looks for a file of this name in every directory`},
		},
		{
			name:   "empty lists inherit",
			labels: map[string]string{"kopia.ignore": "", "kopia.ignore-files": " , ", "kopia.compression.only": ""},
			want:   Default(),
		},

		// Error handling.
		{
			name: "error handling",
			labels: map[string]string{
				"kopia.ignore-file-errors": "true", "kopia.ignore-dir-errors": "False", "kopia.ignore-unknown-types": "TRUE",
			},
			want: policy(func(p *backend.PolicyPatch) {
				p.ErrorHandling = backend.ErrorHandlingPatch{
					IgnoreFileErrors: ptr(true), IgnoreDirectoryErrors: ptr(false), IgnoreUnknownTypes: ptr(true),
				}
			}),
		},
		{
			name:   "boolean with spaces",
			labels: map[string]string{"kopia.ignore-file-errors": " true"},
			want:   policy(func(p *backend.PolicyPatch) { p.ErrorHandling.IgnoreFileErrors = ptr(false) }),
			issues: []string{`warning kopia.ignore-file-errors: " true" is not a boolean, treated as false; remove the spaces around it`},
		},

		// Compression.
		{
			name: "compression",
			labels: map[string]string{
				"kopia.compression.algorithm": " zstd-fastest ", "kopia.compression.min-size": "4 KiB",
				"kopia.compression.max-size": "1.5gb", "kopia.compression.only": ".txt,.log",
				"kopia.compression.never": ".zip, .gz",
			},
			want: policy(func(p *backend.PolicyPatch) {
				p.Compression = backend.CompressionPatch{
					CompressorName: "zstd-fastest", MinSize: 4096, MaxSize: 1_500_000_000,
					OnlyCompress: []string{".txt", ".log"}, NeverCompress: []string{".zip", ".gz"},
				}
			}),
		},
		{
			name:   "no compression",
			labels: map[string]string{"kopia.compression.algorithm": "none"},
			want:   policy(func(p *backend.PolicyPatch) { p.Compression.CompressorName = "none" }),
		},
		{
			name:   "algorithm with a space",
			labels: map[string]string{"kopia.compression.algorithm": "zstd fastest"},
			want:   Default(),
			issues: []string{`error kopia.compression.algorithm: "zstd fastest" is not the name of a compression algorithm`},
		},
		{
			name:   "extensions without the dot",
			labels: map[string]string{"kopia.compression.only": "txt,*.log,.,.tar.gz,.md", "kopia.compression.never": "zip"},
			want:   policy(func(p *backend.PolicyPatch) { p.Compression.OnlyCompress = []string{".md"} }),
			issues: []string{
				`error kopia.compression.never: "zip" is not a file extension; use the extension with its dot, such as .txt`,
				`error kopia.compression.only: "txt" is not a file extension; use the extension with its dot, such as .txt`,
				`error kopia.compression.only: "*.log" is not a file extension; use the extension with its dot, such as .txt`,
				`error kopia.compression.only: "." is not a file extension; use the extension with its dot, such as .txt`,
				`error kopia.compression.only: ".tar.gz" is not a file extension; use the extension with its dot, such as .txt`,
			},
		},
		{
			name:   "min-size above max-size",
			labels: map[string]string{"kopia.compression.min-size": "1MB", "kopia.compression.max-size": "1KB"},
			want: policy(func(p *backend.PolicyPatch) {
				p.Compression.MinSize, p.Compression.MaxSize = 1_000_000, 1000
			}),
			issues: []string{`error kopia.compression.max-size: is less than kopia.compression.min-size, so no file would be compressed`},
		},

		// Sizes.
		{
			name: "sizes",
			labels: map[string]string{
				"kopia.max-file-size": "500MB", "kopia.compression.min-size": "1000", "kopia.compression.max-size": "2 TiB",
			},
			want: policy(func(p *backend.PolicyPatch) {
				p.Files.MaxFileSize = 500_000_000
				p.Compression.MinSize, p.Compression.MaxSize = 1000, 2<<40
			}),
		},
		{
			name:   "size in bytes with a unit",
			labels: map[string]string{"kopia.max-file-size": "12b"},
			want:   policy(func(p *backend.PolicyPatch) { p.Files.MaxFileSize = 12 }),
		},
		{
			name: "bad sizes",
			labels: map[string]string{
				"kopia.max-file-size": "0", "kopia.compression.min-size": "1.5B", "kopia.compression.max-size": "10 EB",
			},
			want: Default(),
			issues: []string{
				`error kopia.compression.max-size: "10 EB" has an unknown unit "EB"; ` + sizeHint,
				`error kopia.compression.min-size: "1.5B" is not a whole number of bytes`,
				`error kopia.max-file-size: "0" is zero, which kopia does not store; leave the label empty to inherit the size`,
			},
		},
		{
			name: "sizes that are not sizes",
			labels: map[string]string{
				"kopia.max-file-size": "-1MB", "kopia.compression.min-size": "MB", "kopia.compression.max-size": "9000000TiB",
			},
			want: Default(),
			issues: []string{
				`error kopia.compression.max-size: "9000000TiB" is too large`,
				`error kopia.compression.min-size: "MB" is not a size; ` + sizeHint,
				`error kopia.max-file-size: "-1MB" is not a size; ` + sizeHint,
			},
		},

		// Upload.
		{
			name:   "parallel file reads",
			labels: map[string]string{"kopia.upload.parallel-files": "8"},
			want:   policy(func(p *backend.PolicyPatch) { p.Upload.MaxParallelFileReads = ptr(8) }),
		},
		{
			name:   "no parallel file reads",
			labels: map[string]string{"kopia.upload.parallel-files": "0"},
			want:   Default(),
			issues: []string{`error kopia.upload.parallel-files: "0" is less than 1`},
		},

		// policy-json.
		{
			name:   "policy-json",
			labels: map[string]string{"kopia.policy-json": ` {"files": {"noParentIgnore": true}, "osSnapshots": {"volumeShadowCopy": {"enable": 2}}} `},
			want: policy(func(p *backend.PolicyPatch) {
				p.Raw = json.RawMessage(`{"files":{"noParentIgnore":true},"osSnapshots":{"volumeShadowCopy":{"enable":2}}}`)
			}),
		},
		{
			name:   "policy-json with a schedule drops the default one",
			labels: map[string]string{"kopia.policy-json": `{"scheduling":{"intervalSeconds":600}}`},
			want: policy(func(p *backend.PolicyPatch) {
				p.Scheduling.Cron = nil
				p.Raw = json.RawMessage(`{"scheduling":{"intervalSeconds":600}}`)
			}),
		},
		{
			name:   "policy-json that is empty",
			labels: map[string]string{"kopia.policy-json": "  "},
			want:   Default(),
		},
		{
			name:   "policy-json that is not JSON",
			labels: map[string]string{"kopia.policy-json": `{"files": }`},
			want:   Default(),
			issues: []string{`error kopia.policy-json: not a JSON object: invalid character '}' looking for beginning of value at offset 11`},
		},
		{
			name:   "policy-json that is not an object",
			labels: map[string]string{"kopia.policy-json": `[1]`},
			want:   Default(),
			issues: []string{`error kopia.policy-json: not a JSON object: it is not in braces`},
		},
		{
			name:   "policy-json null",
			labels: map[string]string{"kopia.policy-json": `null`},
			want:   Default(),
			issues: []string{`error kopia.policy-json: not a JSON object: null`},
		},
		{
			name:   "policy-json with two values",
			labels: map[string]string{"kopia.policy-json": `{} {}`},
			want:   Default(),
			issues: []string{`error kopia.policy-json: not a JSON object: more than one JSON value`},
		},
		{
			name:   "policy-json with trailing garbage",
			labels: map[string]string{"kopia.policy-json": `{}}`},
			want:   Default(),
			issues: []string{`error kopia.policy-json: not a JSON object: invalid character '}' after top-level value`},
		},
		{
			name:   "policy-json with actions",
			labels: map[string]string{"kopia.policy-json": `{"actions":{"beforeSnapshotRoot":{"script":"rm -rf /"}}}`},
			want:   Default(),
			issues: []string{`error kopia.policy-json: must not set actions: they run commands on kopia server, which has to allow them; set them on kopia server instead`},
		},
		{
			name:   "policy-json with empty actions",
			labels: map[string]string{"kopia.policy-json": `{"actions":{}}`},
			want:   Default(),
			issues: []string{`error kopia.policy-json: must not set actions: they run commands on kopia server, which has to allow them; set them on kopia server instead`},
		},
		{
			name:   "policy-json with the pause",
			labels: map[string]string{"kopia.policy-json": `{"scheduling":{"manual":false}}`},
			want:   Default(),
			issues: []string{`error kopia.policy-json: must not set scheduling.manual: the watcher pauses the source when all its containers are stopped`},
		},
		{
			name:   "policy-json with actions in another case",
			labels: map[string]string{"kopia.policy-json": `{"Actions":{"beforeSnapshotRoot":{"script":"x"}}}`},
			want:   Default(),
			issues: []string{`error kopia.policy-json: must not set actions: they run commands on kopia server, which has to allow them; set them on kopia server instead`},
		},
		{
			name:   "policy-json with the pause in another case",
			labels: map[string]string{"kopia.policy-json": `{"SCHEDULING":{"Manual":true}}`},
			want:   Default(),
			issues: []string{`error kopia.policy-json: must not set scheduling.manual: the watcher pauses the source when all its containers are stopped`},
		},
		{
			name:   "policy-json with a key in another case",
			labels: map[string]string{"kopia.policy-json": `{"scheduling":{"Cron":["0 3 * * *"]}}`},
			want:   Default(),
			issues: []string{`error kopia.policy-json: key "scheduling.Cron" is spelled "scheduling.cron" in a Kopia policy`},
		},
		{
			name:   "policy-json with a section in another case",
			labels: map[string]string{"kopia.policy-json": `{"Retention":{"keepDaily":3}}`},
			want:   Default(),
			issues: []string{`error kopia.policy-json: key "Retention" is spelled "retention" in a Kopia policy`},
		},
		{
			name:   "policy-json with keys that differ in case",
			labels: map[string]string{"kopia.policy-json": `{"osSnapshots":{"x":1,"X":2}}`},
			want:   Default(),
			issues: []string{`error kopia.policy-json: keys "X" and "x" differ only in case, and kopia server reads them as one`},
		},
		{
			name:   "policy-json with a dotted key",
			labels: map[string]string{"kopia.policy-json": `{"files.ignore":["*.log"]}`},
			want:   Default(),
			issues: []string{`error kopia.policy-json: key "files.ignore" has a dot; nest objects instead, such as {"files":{"ignore":["*.tmp"]}}`},
		},
		{
			name:   "policy-json with a nested dotted key",
			labels: map[string]string{"kopia.policy-json": `{"logging":{"entries.snapshotted":5}}`},
			want:   Default(),
			issues: []string{`error kopia.policy-json: key "logging.entries.snapshotted" has a dot; nest objects instead, such as {"files":{"ignore":["*.tmp"]}}`},
		},
		{
			name:   "policy-json that drops the scheduling section",
			labels: map[string]string{"kopia.policy-json": `{"scheduling":null}`},
			want: policy(func(p *backend.PolicyPatch) {
				p.Scheduling.Cron = nil
				p.Raw = json.RawMessage(`{"scheduling":null}`)
			}),
		},
		{
			name:   "policy-json with unknown keys in any case",
			labels: map[string]string{"kopia.policy-json": `{"osSnapshots":{"VolumeShadowCopy":{"Enable":1}}}`},
			want: policy(func(p *backend.PolicyPatch) {
				p.Raw = json.RawMessage(`{"osSnapshots":{"VolumeShadowCopy":{"Enable":1}}}`)
			}),
		},
		{
			name: "policy-json clashing with a label",
			labels: map[string]string{
				"kopia.retention.daily": "7", "kopia.policy-json": `{"retention":null}`,
			},
			want: policy(func(p *backend.PolicyPatch) {
				p.Retention.KeepDaily = ptr(7)
				p.Raw = json.RawMessage(`{"retention":null}`)
			}),
			issues: []string{`error kopia.policy-json: policy field retention is set both as a value and as an object with retention.keepDaily`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.issues == nil {
				tt.issues = []string{}
			}

			checkParse(t, tt.labels, Options{}, tt.want, tt.issues)
		})
	}
}

// Strict mode makes the warnings of the policy labels errors, like the
// others'.
func TestParsePolicyLabelsStrict(t *testing.T) {
	_, issues := Parse(map[string]string{"kopia.one-file-system": "1", "kopia.ignore": "a,a"}, Options{Strict: true})

	want := []string{
		`error kopia.ignore: "a" is listed more than once`,
		`error kopia.one-file-system: "1" is not a boolean; use true or false`,
	}
	if got := issueStrings(issues); !slices.Equal(got, want) {
		t.Errorf("issues = %q, want %q", got, want)
	}
}

// The policy labels go under LABEL_PREFIX like the others.
func TestParsePolicyLabelsPrefix(t *testing.T) {
	got, issues := Parse(map[string]string{"backup.retention.daily": "3", "kopia.retention.daily": "9"}, Options{Prefix: "backup"})
	if len(issues) != 0 || got.Policy.Retention.KeepDaily == nil || *got.Policy.Retention.KeepDaily != 3 {
		t.Errorf("Parse = %+v, %v", got.Policy.Retention, issues)
	}
}

func TestCompressor(t *testing.T) {
	tests := []struct {
		pol       backend.PolicyPatch
		name, key string
	}{
		{backend.PolicyPatch{}, "", KeyCompressionAlgorithm},
		{backend.PolicyPatch{Compression: backend.CompressionPatch{CompressorName: "zstd"}}, "zstd", KeyCompressionAlgorithm},
		{backend.PolicyPatch{
			Compression: backend.CompressionPatch{CompressorName: "zstd"},
			Raw:         json.RawMessage(`{"compression":{"compressorName":"s2-default"}}`),
		}, "s2-default", KeyPolicyJSON},
		{backend.PolicyPatch{
			Compression: backend.CompressionPatch{CompressorName: "zstd"},
			Raw:         json.RawMessage(`{"compression":{"minSize":5}}`),
		}, "zstd", KeyCompressionAlgorithm},
		// A name that is not a string is kopia server's to reject.
		{backend.PolicyPatch{Raw: json.RawMessage(`{"compression":{"compressorName":5}}`)}, "", KeyCompressionAlgorithm},
	}

	for _, tt := range tests {
		if name, key := Compressor(tt.pol); name != tt.name || key != tt.key {
			t.Errorf("Compressor(%+v) = %q, %q; want %q, %q", tt.pol, name, key, tt.name, tt.key)
		}
	}
}

func TestCheckCompressor(t *testing.T) {
	algos := []backend.Algorithm{{Name: "gzip"}, {Name: "s2-default"}, {Name: "zstd"}, {Name: "deflate-old", Deprecated: true}}
	compress := func(name string) backend.PolicyPatch {
		return backend.PolicyPatch{Compression: backend.CompressionPatch{CompressorName: name}}
	}

	tests := []struct {
		pol  backend.PolicyPatch
		want []string
	}{
		{backend.PolicyPatch{}, []string{}},
		{compress("zstd"), []string{}},
		{compress("none"), []string{}},
		{compress("deflate-old"), []string{`warning kopia.compression.algorithm: compression algorithm "deflate-old" is deprecated by kopia; it still works`}},
		{compress("ZSTD"), []string{`error kopia.compression.algorithm: kopia server does not support the compression algorithm "ZSTD"; did you mean zstd?`}},
		{compress("lzma"), []string{`error kopia.compression.algorithm: kopia server does not support the compression algorithm "lzma"; use one of gzip, s2-default, zstd, none`}},
		{backend.PolicyPatch{Raw: json.RawMessage(`{"compression":{"compressorName":"brotli"}}`)},
			[]string{`error kopia.policy-json: kopia server does not support the compression algorithm "brotli"; use one of gzip, s2-default, zstd, none`}},
	}

	for _, tt := range tests {
		if got := issueStrings(CheckCompressor("kopia", tt.pol, algos, false)); !slices.Equal(got, tt.want) {
			t.Errorf("CheckCompressor(%+v) = %q, want %q", tt.pol, got, tt.want)
		}
	}

	// Strict mode makes the warning an error.
	want := []string{`error kopia.compression.algorithm: compression algorithm "deflate-old" is deprecated by kopia`}
	if got := issueStrings(CheckCompressor("kopia", compress("deflate-old"), algos, true)); !slices.Equal(got, want) {
		t.Errorf("strict: %q, want %q", got, want)
	}
}

func TestScheduleKey(t *testing.T) {
	for _, tt := range []struct {
		pol  backend.PolicyPatch
		want string
	}{
		{backend.PolicyPatch{Scheduling: backend.SchedulingPatch{Cron: []string{"0 * * * *"}}}, KeySchedule},
		{backend.PolicyPatch{Raw: json.RawMessage(`{"scheduling":{"cron":["5 * * * *"]}}`)}, KeyPolicyJSON},
		{backend.PolicyPatch{Raw: json.RawMessage(`{"scheduling":{"intervalSeconds":60}}`)}, KeySchedule},
	} {
		if got := ScheduleKey(tt.pol); got != tt.want {
			t.Errorf("ScheduleKey(%+v) = %q, want %q", tt.pol, got, tt.want)
		}
	}
}
