//go:build integration

package rest

import (
	"encoding/json"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiatest"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
)

// TestIntegrationPolicyLabels applies the policy fields of container labels
// to a real kopia server (kopiatest.DefaultImage unless KOPIA_IMAGE names
// another, such as kopia/kopia:0.22.3): kopia server stores every field as
// the labels set it, keeps the fields set in the web UI, and loses the
// field of a removed label. The subtests run in order on one source.
func TestIntegrationPolicyLabels(t *testing.T) {
	s := kopiatest.Start(t, kopiatest.Options{})

	c, err := kopiaclient.New(kopiaclient.Options{
		URL:             s.URL,
		Username:        kopiatest.UIUser,
		Password:        kopiatest.UIPassword,
		CertFingerprint: s.CertFingerprint,
		Timeout:         time.Minute,
		UserAgent:       "kopia-docker-watcher-integration-test",
	})
	if err != nil {
		t.Fatal(err)
	}

	waitConnected(t, c)

	b := New(c, slog.New(slog.NewTextHandler(t.Output(), nil)))
	ctx := t.Context()

	s.Exec(t, "mkdir", "-p", "/data/labels", "/data/times")

	user, host, err := b.ServerIdentity(ctx)
	if err != nil {
		t.Fatal(err)
	}

	src := backend.Source{Host: host, UserName: user, Path: "/data/labels"}
	st := memState{}

	// desired parses container labels the way the reconciler does.
	desired := func(t *testing.T, src backend.Source, paused bool, kv ...string) backend.Desired {
		t.Helper()

		in := map[string]string{"kopia.enable": "true", "kopia.path": src.Path}
		for i := 0; i < len(kv); i += 2 {
			in[kv[i]] = kv[i+1]
		}

		spec, issues := labels.Parse(in, labels.Options{})
		if issues.HasErrors() {
			t.Fatalf("labels: %v", issues)
		}

		return backend.Desired{Source: src, Patch: spec.Policy, Paused: paused}
	}

	t.Run("CompressionAlgorithms", func(t *testing.T) {
		algos, err := b.CompressionAlgorithms(ctx)
		if err != nil {
			t.Fatal(err)
		}

		names := make([]string, len(algos))
		for i, a := range algos {
			names[i] = a.Name
		}

		if !slices.Contains(names, "zstd") || !slices.Contains(names, "s2-default") || slices.Contains(names, labels.NoCompression) {
			t.Errorf("algorithms = %v", names)
		}

		// Every name passes the label check.
		for _, a := range algos {
			pol := backend.PolicyPatch{Compression: backend.CompressionPatch{CompressorName: a.Name}}
			if is := labels.CheckCompressor("kopia", pol, algos, false); is.HasErrors() {
				t.Errorf("%s: %v", a.Name, is)
			}
		}
	})

	all := []string{
		"kopia.schedule", "0 3 * * *",
		"kopia.run-missed", "false",
		"kopia.retention.latest", "5",
		"kopia.retention.daily", "7",
		"kopia.retention.hourly", "0",
		"kopia.retention.ignore-identical", "true",
		"kopia.ignore", "*.tmp,cache/",
		"kopia.ignore-files", ".kopiaignore",
		"kopia.ignore-cache-dirs", "true",
		"kopia.max-file-size", "1GiB",
		"kopia.one-file-system", "false",
		"kopia.ignore-file-errors", "true",
		"kopia.ignore-dir-errors", "false",
		"kopia.ignore-unknown-types", "true",
		"kopia.compression.algorithm", "zstd",
		"kopia.compression.min-size", "4KiB",
		"kopia.compression.max-size", "100MB",
		"kopia.compression.never", ".zip,.gz",
		"kopia.upload.parallel-files", "2",
		"kopia.policy-json", `{"files":{"noParentIgnore":true}}`,
	}

	t.Run("every field is stored as the labels set it", func(t *testing.T) {
		if r := mustReconcile(t, b, st, desired(t, src, false, all...)); r.Plan.Action != backend.ActionRegister {
			t.Fatalf("Action = %v", r.Plan.Action)
		}

		got := policyOf(t, c, src)
		assertScheduling(t, got, `{"cron":["0 3 * * *"],"runMissed":false}`)
		assertSection(t, got, "retention", `{"keepLatest":5,"keepDaily":7,"keepHourly":0,"ignoreIdenticalSnapshots":true}`)
		assertSection(t, got, "files", `{"ignore":["*.tmp","cache/"],"noParentIgnore":true,"ignoreDotFiles":[".kopiaignore"],`+
			`"ignoreCacheDirs":true,"maxFileSize":1073741824,"oneFileSystem":false}`)
		assertSection(t, got, "errorHandling", `{"ignoreFileErrors":true,"ignoreDirectoryErrors":false,"ignoreUnknownTypes":true}`)
		assertSection(t, got, "compression", `{"compressorName":"zstd","minSize":4096,"maxSize":100000000,"neverCompress":[".zip",".gz"]}`)
		assertSection(t, got, "upload", `{"maxParallelFileReads":2}`)

		// kopia server's own form of them writes nothing next time.
		if r := mustReconcile(t, b, st, desired(t, src, false, all...)); r.Plan.Action != backend.ActionNone {
			t.Errorf("second pass: Action = %v, %s -> %s", r.Plan.Action, r.Plan.Current, r.Plan.Policy)
		}
	})

	t.Run("fields set in the web UI survive and a removed label's field goes", func(t *testing.T) {
		// The web UI writes the whole policy, with fields of its own in
		// sections the labels set and in one they do not.
		got := policyOf(t, c, src)

		var pol map[string]any
		if err := json.Unmarshal(got, &pol); err != nil {
			t.Fatal(err)
		}

		section := func(name string) map[string]any {
			m, ok := pol[name].(map[string]any)
			if !ok {
				t.Fatalf("%s is not an object in %s", name, got)
			}

			return m
		}

		section("retention")["keepWeekly"] = 4
		section("compression")["onlyCompress"] = []string{".txt"}
		pol["logging"] = map[string]any{"directories": map[string]any{"snapshotted": 5}}

		ui, err := json.Marshal(pol)
		if err != nil {
			t.Fatal(err)
		}

		setPolicy(t, c, src, string(ui))

		if r := mustReconcile(t, b, st, desired(t, src, false, all...)); r.Plan.Action != backend.ActionNone {
			t.Errorf("after the web UI: Action = %v, %s -> %s", r.Plan.Action, r.Plan.Current, r.Plan.Policy)
		}

		// The retention, ignore and compression labels are removed or
		// changed.
		var fewer []string

		for i := 0; i < len(all); i += 2 {
			switch all[i] {
			case "kopia.retention.daily", "kopia.retention.hourly", "kopia.ignore", "kopia.ignore-cache-dirs", "kopia.compression.never":
				continue
			case "kopia.compression.algorithm":
				fewer = append(fewer, all[i], "s2-default")
			default:
				fewer = append(fewer, all[i], all[i+1])
			}
		}

		if r := mustReconcile(t, b, st, desired(t, src, false, fewer...)); r.Plan.Action != backend.ActionUpdate {
			t.Fatalf("Action = %v", r.Plan.Action)
		}

		got = policyOf(t, c, src)
		assertSection(t, got, "retention", `{"keepLatest":5,"keepWeekly":4,"ignoreIdenticalSnapshots":true}`)
		assertSection(t, got, "files", `{"noParentIgnore":true,"ignoreDotFiles":[".kopiaignore"],"maxFileSize":1073741824,"oneFileSystem":false}`)
		assertSection(t, got, "compression", `{"compressorName":"s2-default","minSize":4096,"maxSize":100000000,"onlyCompress":[".txt"]}`)
		assertSection(t, got, "logging", `{"directories":{"snapshotted":5}}`)

		// The policy-json label goes: its field goes too.
		var noJSON []string

		for i := 0; i < len(fewer); i += 2 {
			if fewer[i] != "kopia.policy-json" {
				noJSON = append(noJSON, fewer[i], fewer[i+1])
			}
		}

		mustReconcile(t, b, st, desired(t, src, false, noJSON...))
		assertSection(t, policyOf(t, c, src), "files", `{"ignoreDotFiles":[".kopiaignore"],"maxFileSize":1073741824,"oneFileSystem":false}`)
	})

	t.Run("interval and times of day are previewed and kept by the pause", func(t *testing.T) {
		times := backend.Source{Host: host, UserName: user, Path: "/data/times"}
		tst := memState{}
		sched := []string{"kopia.interval", "12h", "kopia.time-of-day", "02:30", "kopia.run-missed", "false", "kopia.retention.daily", "3"}

		// Registered paused, so kopia server does not snapshot it.
		want := desired(t, times, true, sched...)
		if r := mustReconcile(t, b, tst, want); r.Plan.Action != backend.ActionRegister {
			t.Fatalf("Action = %v", r.Plan.Action)
		}

		got := policyOf(t, c, times)
		assertScheduling(t, got, `{"manual":true}`)
		assertSection(t, got, "retention", `{"keepDaily":3}`)
		assertJSON(t, "saved", tst[times].SavedScheduling, `{"intervalSeconds":43200,"timeOfDay":[{"hour":2,"min":30}],"runMissed":false}`)

		plan, err := b.PlanSource(ctx, want, tst[times])
		if err != nil {
			t.Fatal(err)
		}

		preview, err := b.PreviewSchedule(ctx, times, plan.Unpaused, 4)
		if err != nil || len(preview.Times) != 4 {
			t.Fatalf("PreviewSchedule = %+v, %v", preview, err)
		}

		// The 12h interval counts from no snapshot, so kopia server
		// places runs on the times of day and on the 12h marks.
		var at0230 bool

		for i, tm := range preview.Times {
			if tm.Hour() == 2 && tm.Minute() == 30 {
				at0230 = true
			}

			if i > 0 && !tm.After(preview.Times[i-1]) {
				t.Errorf("times not ascending: %v", preview.Times)
			}
		}

		if !at0230 {
			t.Errorf("no run at 02:30 among %v", preview.Times)
		}

		var effective struct {
			IntervalSeconds int64 `json:"intervalSeconds"`
		}

		if err := json.Unmarshal(preview.Scheduling, &effective); err != nil || effective.IntervalSeconds != 43200 {
			t.Errorf("effective scheduling = %s (%v)", preview.Scheduling, err)
		}

		// Unpaused: the section comes back whole.
		mustReconcile(t, b, tst, desired(t, times, false, sched...))
		assertScheduling(t, policyOf(t, c, times), `{"intervalSeconds":43200,"timeOfDay":[{"hour":2,"min":30}],"runMissed":false}`)
	})
}
