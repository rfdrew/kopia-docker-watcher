package backend

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// fullPatch sets every typed field.
func fullPatch() PolicyPatch {
	return PolicyPatch{
		Scheduling: SchedulingPatch{
			Cron:            []string{"0 3 * * *"},
			IntervalSeconds: 21600,
			TimeOfDay:       []TimeOfDay{{Hour: 2, Minute: 30}, {Hour: 14, Minute: 0}},
			RunMissed:       ptr(false),
		},
		Retention: RetentionPatch{
			KeepLatest: ptr(10), KeepHourly: ptr(0), KeepDaily: ptr(7), KeepWeekly: ptr(4), KeepMonthly: ptr(12),
			KeepAnnual: ptr(3), IgnoreIdenticalSnapshots: ptr(true),
		},
		Files: FilesPatch{
			Ignore:          []string{"*.tmp", "cache/"},
			IgnoreDotFiles:  []string{".kopiaignore"},
			IgnoreCacheDirs: ptr(true),
			MaxFileSize:     1 << 30,
			OneFileSystem:   ptr(false),
		},
		ErrorHandling: ErrorHandlingPatch{
			IgnoreFileErrors: ptr(true), IgnoreDirectoryErrors: ptr(false), IgnoreUnknownTypes: ptr(true),
		},
		Compression: CompressionPatch{
			CompressorName: "zstd", MinSize: 1000, MaxSize: 500_000_000,
			OnlyCompress: []string{".txt"}, NeverCompress: []string{".zip", ".gz"},
		},
		Upload: UploadPatch{MaxParallelFileReads: ptr(4)},
	}
}

func fieldStrings(fs []Field) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.Name() + "=" + string(f.Value)
	}

	return out
}

func TestTypedPatchFields(t *testing.T) {
	got, err := fullPatch().Fields()
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		`compression.compressorName="zstd"`,
		`compression.maxSize=500000000`,
		`compression.minSize=1000`,
		`compression.neverCompress=[".zip",".gz"]`,
		`compression.onlyCompress=[".txt"]`,
		`errorHandling.ignoreDirectoryErrors=false`,
		`errorHandling.ignoreFileErrors=true`,
		`errorHandling.ignoreUnknownTypes=true`,
		`files.ignore=["*.tmp","cache/"]`,
		`files.ignoreCacheDirs=true`,
		`files.ignoreDotFiles=[".kopiaignore"]`,
		`files.maxFileSize=1073741824`,
		`files.oneFileSystem=false`,
		`retention.ignoreIdenticalSnapshots=true`,
		`retention.keepAnnual=3`,
		`retention.keepDaily=7`,
		`retention.keepHourly=0`,
		`retention.keepLatest=10`,
		`retention.keepMonthly=12`,
		`retention.keepWeekly=4`,
		`scheduling.cron=["0 3 * * *"]`,
		`scheduling.intervalSeconds=21600`,
		`scheduling.runMissed=false`,
		`scheduling.timeOfDay=[{"hour":2,"min":30},{"hour":14,"min":0}]`,
		`upload.maxParallelFileReads=4`,
	}

	if names := fieldStrings(got); !reflect.DeepEqual(names, want) {
		t.Errorf("Fields =\n%s\nwant\n%s", strings.Join(names, "\n"), strings.Join(want, "\n"))
	}
}

// The JSON form is stored in STATE_FILE: the names are Kopia's, unset
// fields and sections are left out, and it reads back the same.
func TestTypedPatchJSON(t *testing.T) {
	p := PolicyPatch{
		Scheduling: SchedulingPatch{IntervalSeconds: 3600, RunMissed: ptr(true)},
		Retention:  RetentionPatch{KeepDaily: ptr(0)},
		Files:      FilesPatch{MaxFileSize: 5},
	}

	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}

	want := `{"scheduling":{"intervalSeconds":3600,"runMissed":true},"retention":{"keepDaily":0},"files":{"maxFileSize":5}}`
	if string(b) != want {
		t.Errorf("JSON = %s, want %s", b, want)
	}

	var back PolicyPatch
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}

	if !back.Equal(p) || !reflect.DeepEqual(back, p) {
		t.Errorf("read back %+v, want %+v", back, p)
	}

	full := fullPatch()

	b, err = json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}

	back = PolicyPatch{}
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(back, full) {
		t.Errorf("read back %+v, want %+v", back, full)
	}
}

func TestTypedPatchIsZero(t *testing.T) {
	if !(PolicyPatch{}).IsZero() {
		t.Error("the zero patch is not zero")
	}

	for name, p := range map[string]PolicyPatch{
		"interval":      {Scheduling: SchedulingPatch{IntervalSeconds: 60}},
		"time of day":   {Scheduling: SchedulingPatch{TimeOfDay: []TimeOfDay{{}}}},
		"run missed":    {Scheduling: SchedulingPatch{RunMissed: ptr(false)}},
		"retention":     {Retention: RetentionPatch{KeepAnnual: ptr(0)}},
		"ignore":        {Files: FilesPatch{Ignore: []string{"x"}}},
		"dot files":     {Files: FilesPatch{IgnoreDotFiles: []string{"x"}}},
		"cache dirs":    {Files: FilesPatch{IgnoreCacheDirs: ptr(false)}},
		"max file size": {Files: FilesPatch{MaxFileSize: 1}},
		"one fs":        {Files: FilesPatch{OneFileSystem: ptr(false)}},
		"errors":        {ErrorHandling: ErrorHandlingPatch{IgnoreUnknownTypes: ptr(false)}},
		"compressor":    {Compression: CompressionPatch{CompressorName: "zstd"}},
		"min size":      {Compression: CompressionPatch{MinSize: 1}},
		"max size":      {Compression: CompressionPatch{MaxSize: 1}},
		"only":          {Compression: CompressionPatch{OnlyCompress: []string{".a"}}},
		"never":         {Compression: CompressionPatch{NeverCompress: []string{".a"}}},
		"upload":        {Upload: UploadPatch{MaxParallelFileReads: ptr(1)}},
	} {
		if p.IsZero() {
			t.Errorf("%s: IsZero = true", name)
		}

		if fs, err := p.Fields(); err != nil || len(fs) != 1 {
			t.Errorf("%s: Fields = %v, %v; want one field", name, fs, err)
		}
	}
}

func TestTypedPatchClone(t *testing.T) {
	orig := fullPatch()
	c := orig.Clone()

	if !reflect.DeepEqual(c, orig) {
		t.Fatalf("Clone = %+v, want %+v", c, orig)
	}

	*c.Scheduling.RunMissed = true
	c.Scheduling.TimeOfDay[0].Hour = 9
	*c.Retention.KeepDaily = 99
	*c.Retention.IgnoreIdenticalSnapshots = false
	c.Files.Ignore[0] = "changed"
	c.Files.IgnoreDotFiles[0] = "changed"
	*c.Files.IgnoreCacheDirs = false
	*c.Files.OneFileSystem = true
	*c.ErrorHandling.IgnoreFileErrors = false
	c.Compression.OnlyCompress[0] = "changed"
	c.Compression.NeverCompress[0] = "changed"
	*c.Upload.MaxParallelFileReads = 99

	if !reflect.DeepEqual(orig, fullPatch()) {
		t.Errorf("changing the clone changed the original: %+v", orig)
	}
}

func TestTypedPatchEqual(t *testing.T) {
	a := PolicyPatch{Retention: RetentionPatch{KeepDaily: ptr(7)}}

	for name, b := range map[string]PolicyPatch{
		"other value":  {Retention: RetentionPatch{KeepDaily: ptr(8)}},
		"zero":         {Retention: RetentionPatch{KeepDaily: ptr(0)}},
		"other field":  {Retention: RetentionPatch{KeepWeekly: ptr(7)}},
		"unset":        {},
		"extra field":  {Retention: RetentionPatch{KeepDaily: ptr(7)}, Upload: UploadPatch{MaxParallelFileReads: ptr(1)}},
		"time of day":  {Retention: RetentionPatch{KeepDaily: ptr(7)}, Scheduling: SchedulingPatch{TimeOfDay: []TimeOfDay{{Hour: 1}}}},
		"run missed":   {Retention: RetentionPatch{KeepDaily: ptr(7)}, Scheduling: SchedulingPatch{RunMissed: ptr(false)}},
		"compression":  {Retention: RetentionPatch{KeepDaily: ptr(7)}, Compression: CompressionPatch{CompressorName: "zstd"}},
		"ignore files": {Retention: RetentionPatch{KeepDaily: ptr(7)}, Files: FilesPatch{IgnoreDotFiles: []string{".x"}}},
	} {
		if a.Equal(b) || b.Equal(a) {
			t.Errorf("%s: patches are equal", name)
		}
	}

	// A typed field and the same field in Raw are the same to Kopia.
	if !a.Equal(PolicyPatch{Raw: json.RawMessage(`{"retention":{"keepDaily":7}}`)}) {
		t.Error("a typed field and the same raw field differ")
	}
}

func TestRawMustNotSetPauseOrActions(t *testing.T) {
	for _, raw := range []string{
		`{"actions":{}}`,
		`{"actions":{"beforeSnapshotRoot":{"script":"rm -rf /"}}}`,
		`{"actions":null}`,
		`{"scheduling":{"manual":true}}`,
		`{"scheduling":{"manual":{}}}`,
		`{"scheduling":{"manual":null}}`,
		`{"Actions":{}}`,
		`{"ACTIONS":null}`,
		`{"Scheduling":{"Manual":true}}`,
		`{"scheduling":{"MANUAL":{}}}`,
		`{"files.ignore":["x"]}`,
		`{"logging":{"entries.snapshotted":5}}`,
	} {
		err := PolicyPatch{Raw: json.RawMessage(raw)}.Validate()
		if !IsConfig(err) {
			t.Errorf("%s: Validate = %v, want ErrConfig", raw, err)
		}
	}

	// Sections next to them are fine.
	if err := (PolicyPatch{Raw: json.RawMessage(`{"logging":{"directories":{}},"scheduling":{"runMissed":true}}`)}).Validate(); err != nil {
		t.Errorf("Validate = %v", err)
	}
}

func TestMergeTypedFields(t *testing.T) {
	retention := func(daily int) PolicyPatch {
		return PolicyPatch{Scheduling: SchedulingPatch{Cron: []string{"0 * * * *"}}, Retention: RetentionPatch{KeepDaily: ptr(daily)}}
	}

	sched := PolicyPatch{Scheduling: SchedulingPatch{
		IntervalSeconds: 21600,
		TimeOfDay:       []TimeOfDay{{Hour: 2, Minute: 30}},
		RunMissed:       ptr(true),
	}}

	tests := []struct {
		name    string
		current string
		want    Desired
		last    Applied

		policy string
		saved  string // "" means nil
	}{
		{
			name:    "every section is written next to the fields set in the web UI",
			current: `{"retention":{"keepLatest":3},"files":{"ignore":["ui"]},"splitter":{"algorithm":"x"}}`,
			want:    Desired{Patch: fullPatch()},
			policy: `{"compression":{"compressorName":"zstd","maxSize":500000000,"minSize":1000,"neverCompress":[".zip",".gz"],"onlyCompress":[".txt"]},` +
				`"errorHandling":{"ignoreDirectoryErrors":false,"ignoreFileErrors":true,"ignoreUnknownTypes":true},` +
				`"files":{"ignore":["*.tmp","cache/"],"ignoreCacheDirs":true,"ignoreDotFiles":[".kopiaignore"],"maxFileSize":1073741824,"oneFileSystem":false},` +
				`"retention":{"ignoreIdenticalSnapshots":true,"keepAnnual":3,"keepDaily":7,"keepHourly":0,"keepLatest":10,"keepMonthly":12,"keepWeekly":4},` +
				`"scheduling":{"cron":["0 3 * * *"],"intervalSeconds":21600,"runMissed":false,"timeOfDay":[{"hour":2,"min":30},{"hour":14,"min":0}]},` +
				`"splitter":{"algorithm":"x"},"upload":{"maxParallelFileReads":4}}`,
		},
		{
			name:    "a removed label takes its field out and keeps the web UI's",
			current: `{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepDaily":7,"keepWeekly":4}}`,
			want:    Desired{Patch: cronPatch("0 * * * *")},
			last:    Applied{Patch: retention(7)},
			policy:  `{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepWeekly":4}}`,
		},
		{
			name:    "a removed label empties its section, which goes",
			current: `{"scheduling":{"cron":["0 * * * *"]},"upload":{"maxParallelFileReads":4}}`,
			want:    Desired{Patch: cronPatch("0 * * * *")},
			last:    Applied{Patch: PolicyPatch{Scheduling: SchedulingPatch{Cron: []string{"0 * * * *"}}, Upload: UploadPatch{MaxParallelFileReads: ptr(4)}}},
			policy:  `{"scheduling":{"cron":["0 * * * *"]}}`,
		},
		{
			name:    "a changed label replaces the value set in the web UI",
			current: `{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepDaily":30}}`,
			want:    Desired{Patch: retention(7)},
			last:    Applied{Patch: retention(7)},
			policy:  `{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepDaily":7}}`,
		},
		{
			name:    "the pause keeps the interval, the times of day and run-missed aside",
			current: `{"scheduling":{"intervalSeconds":21600,"timeOfDay":[{"hour":2,"min":30}],"runMissed":true},"retention":{"keepDaily":7}}`,
			want:    Desired{Patch: sched, Paused: true},
			last:    Applied{Patch: sched},
			policy:  `{"scheduling":{"manual":true},"retention":{"keepDaily":7}}`,
			saved:   `{"intervalSeconds":21600,"timeOfDay":[{"hour":2,"min":30}],"runMissed":true}`,
		},
		{
			name:    "unpausing puts them back",
			current: `{"scheduling":{"manual":true},"retention":{"keepDaily":7}}`,
			want:    Desired{Patch: sched},
			last:    Applied{Patch: sched, SavedScheduling: json.RawMessage(`{"intervalSeconds":21600,"timeOfDay":[{"hour":2,"min":30}],"runMissed":true}`)},
			policy:  `{"scheduling":{"intervalSeconds":21600,"timeOfDay":[{"hour":2,"min":30}],"runMissed":true},"retention":{"keepDaily":7}}`,
		},
		{
			name:    "new labels of a paused source go into the saved section",
			current: `{"scheduling":{"manual":true}}`,
			want: Desired{Patch: PolicyPatch{Scheduling: SchedulingPatch{IntervalSeconds: 3600, TimeOfDay: []TimeOfDay{{Hour: 1}}}},
				Paused: true},
			last:   Applied{Patch: sched, SavedScheduling: json.RawMessage(`{"intervalSeconds":21600,"timeOfDay":[{"hour":2,"min":30}],"runMissed":true,"noParentTimeOfDay":true}`)},
			policy: `{"scheduling":{"manual":true}}`,
			saved:  `{"intervalSeconds":3600,"timeOfDay":[{"hour":1,"min":0}],"noParentTimeOfDay":true}`,
		},
		{
			name:    "a source paused elsewhere gets the labels' schedule alone",
			current: `{"scheduling":{"manual":true},"compression":{"compressorName":"s2-default"}}`,
			want:    Desired{Patch: sched},
			policy:  `{"scheduling":{"intervalSeconds":21600,"timeOfDay":[{"hour":2,"min":30}],"runMissed":true},"compression":{"compressorName":"s2-default"}}`,
		},
		{
			name:    "policy-json is merged last and owns its fields",
			current: `{"files":{"ignore":["ui"],"noParentIgnore":false}}`,
			want: Desired{Patch: PolicyPatch{
				Files: FilesPatch{Ignore: []string{"*.tmp"}},
				Raw:   json.RawMessage(`{"files":{"noParentIgnore":true,"ignore":["*.log"]},"osSnapshots":{"volumeShadowCopy":{"enable":1}}}`),
			}},
			policy: `{"files":{"ignore":["*.log"],"noParentIgnore":true},"osSnapshots":{"volumeShadowCopy":{"enable":1}}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.want.Source = testSource

			got, err := Merge(json.RawMessage(tt.current), tt.want, tt.last)
			if err != nil {
				t.Fatal(err)
			}

			assertJSON(t, "Policy", got.Policy, tt.policy)

			if tt.saved == "" {
				if got.Applied.SavedScheduling != nil {
					t.Errorf("SavedScheduling = %s, want nil", got.Applied.SavedScheduling)
				}
			} else {
				assertJSON(t, "SavedScheduling", got.Applied.SavedScheduling, tt.saved)
			}
		})
	}
}
