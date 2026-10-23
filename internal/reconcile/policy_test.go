package reconcile

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
)

// The policy labels are written to the path's policy next to the fields
// set in the web UI; a removed label takes its field out again, and the
// web UI's fields stay.
func TestPolicyLabels(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app),
		"kopia.retention.daily=7", "kopia.retention.latest=3", "kopia.compression.algorithm=zstd",
		"kopia.ignore=*.tmp,cache/", "kopia.max-file-size=1GiB", "kopia.upload.parallel-files=2")

	if res := h.mustPass(app); len(res.problems) != 0 {
		t.Fatalf("problems = %v", res.problems)
	}

	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepDaily":7,"keepLatest":3},`+
		`"compression":{"compressorName":"zstd"},"files":{"ignore":["*.tmp","cache/"],"maxFileSize":1073741824},`+
		`"upload":{"maxParallelFileReads":2}}`)

	// Fields set in the web UI, one of them in a section the labels set.
	h.fake.SetPolicy(h.src(app), `{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepDaily":7,"keepLatest":3,"keepWeekly":4},`+
		`"compression":{"compressorName":"zstd"},"files":{"ignore":["*.tmp","cache/"],"maxFileSize":1073741824},`+
		`"upload":{"maxParallelFileReads":2},"errorHandling":{"ignoreFileErrors":true}}`)

	h.mustPass(app)

	if n := h.writes(app); n != 1 {
		t.Errorf("%d writes; the web UI's fields made the watcher write", n)
	}

	// The container is recreated without the retention and upload labels
	// and with another algorithm.
	h.engine.remove(id)
	h.engine.run("app", enable, pathLabel(app), "kopia.compression.algorithm=s2-default",
		"kopia.ignore=*.tmp,cache/", "kopia.max-file-size=1GiB")

	if res := h.mustPass(app); len(res.problems) != 0 {
		t.Fatalf("problems = %v", res.problems)
	}

	h.mustPolicy(app, `{"errorHandling":{"ignoreFileErrors":true},"scheduling":{"cron":["0 * * * *"]},`+
		`"retention":{"keepWeekly":4},"compression":{"compressorName":"s2-default"},`+
		`"files":{"ignore":["*.tmp","cache/"],"maxFileSize":1073741824}}`)
}

// The pause keeps the whole scheduling section aside, interval, times of
// day and run-missed included, and puts it back at the next start; the
// schedule is checked with them while the source is paused, too.
func TestPausePolicySchedule(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app), "kopia.interval=6h", "kopia.time-of-day=02:30,14:00",
		"kopia.run-missed=true", "kopia.retention.daily=7")
	h.mustPass(app)

	schedule := `{"intervalSeconds":21600,"runMissed":true,"timeOfDay":[{"hour":2,"min":30},{"hour":14,"min":0}]}`
	h.mustPolicy(app, `{"scheduling":`+schedule+`,"retention":{"keepDaily":7}}`)

	previews := h.fake.CallsTo(backendtest.PreviewSchedule)
	if len(previews) != 1 || !strings.Contains(string(previews[0].Policy), `"intervalSeconds":21600`) ||
		!strings.Contains(string(previews[0].Policy), `"timeOfDay"`) {
		t.Fatalf("schedule previews = %v", previews)
	}

	h.engine.setState(id, docker.StateExited)
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"manual":true},"retention":{"keepDaily":7}}`)

	if a, _ := h.applied(app); string(a.SavedScheduling) != schedule {
		t.Errorf("saved section = %s, want %s", a.SavedScheduling, schedule)
	}

	// New labels while stopped: they go into the saved section, checked
	// with the schedule as it would be without the pause.
	h.engine.remove(id)
	id = h.engine.create("app", docker.StateExited, enable, pathLabel(app), "kopia.time-of-day=03:00", "kopia.retention.daily=7")
	h.fake.ResetCalls()
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"manual":true},"retention":{"keepDaily":7}}`)

	previews = h.fake.CallsTo(backendtest.PreviewSchedule)
	if len(previews) != 1 {
		t.Fatalf("schedule previews = %v", previews)
	}

	assertJSON(t, "previewed policy", previews[0].Policy, `{"scheduling":{"timeOfDay":[{"hour":3,"min":0}]},"retention":{"keepDaily":7}}`)

	h.engine.setState(id, docker.StateRunning)
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"timeOfDay":[{"hour":3,"min":0}]},"retention":{"keepDaily":7}}`)
}

// A rejected interval or time of day falls back to the last applied patch,
// like a rejected cron expression.
func TestPolicyScheduleRejected(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app), "kopia.interval=1h")
	h.mustPass(app)

	h.fake.SetPolicyCheck(func(policy json.RawMessage) error {
		if strings.Contains(string(policy), `"timeOfDay"`) {
			return errBadSchedule
		}

		return nil
	})

	h.engine.remove(id)
	h.engine.run("app", enable, pathLabel(app), "kopia.time-of-day=01:00")

	res := h.mustPass(app)
	if len(res.problems) != 1 || !strings.Contains(res.problems[0].Error(), errBadSchedule.Error()) {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustPolicy(app, `{"scheduling":{"intervalSeconds":3600}}`)
}

var errBadSchedule = errorString("times of day are not allowed here")

type errorString string

func (e errorString) Error() string { return string(e) }

// Owners must agree on every policy field, the new ones included; the
// error names the fields.
func TestOwnersDisagreeOnPolicyFields(t *testing.T) {
	h := newHarness(t)
	h.engine.run("web", enable, pathLabel(app), "kopia.retention.daily=7", "kopia.interval=6h")
	h.mustPass(app)

	h.engine.run("worker", enable, pathLabel(app), "kopia.retention.daily=14", "kopia.interval=6h")

	res := h.mustPass(app)
	if len(res.problems) != 1 || !strings.Contains(res.problems[0].Error(),
		"web: retention.keepDaily=7, scheduling.intervalSeconds=21600; worker: retention.keepDaily=14, scheduling.intervalSeconds=21600") {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustPolicy(app, `{"scheduling":{"intervalSeconds":21600},"retention":{"keepDaily":7}}`)

	if h.metrics.count(app, ReasonOwnersDisagree) != 1 {
		t.Error("not counted")
	}
}

// Owners that write the same fields differently agree: policy-json and a
// label that set the same value.
func TestOwnersAgreeOnPolicyJSON(t *testing.T) {
	h := newHarness(t)
	h.engine.run("web", enable, pathLabel(app), "kopia.retention.daily=7")
	h.engine.run("worker", enable, pathLabel(app), `kopia.policy-json={"retention":{"keepDaily":7}}`)

	if res := h.mustPass(app); len(res.problems) != 0 {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepDaily":7}}`)
}

// A compression algorithm kopia server lacks is a label error: a new
// source is not registered, a known one keeps its last applied patch.
// kopia server is asked only for labels that name an algorithm.
func TestUnknownCompressionAlgorithm(t *testing.T) {
	h := newHarness(t)
	h.engine.run("db", enable, pathLabel("/data/db"), "kopia.compression.algorithm=brotli")

	res := h.mustPass("/data/db")
	if len(res.problems) != 1 || !strings.Contains(res.problems[0].Error(),
		`kopia.compression.algorithm: kopia server does not support the compression algorithm "brotli"`) {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustNoSource("/data/db")

	if h.metrics.count("/data/db", ReasonLabels) != 1 {
		t.Error("not counted")
	}

	id := h.engine.run("app", enable, pathLabel(app))
	h.fake.ResetCalls()
	h.mustPass(app)

	if calls := h.fake.CallsTo(backendtest.CompressionAlgorithms); len(calls) != 0 {
		t.Errorf("kopia server asked for its algorithms %d times without a compression label", len(calls))
	}

	h.engine.remove(id)
	h.engine.run("app", enable, pathLabel(app), "kopia.retention.daily=7",
		`kopia.policy-json={"compression":{"compressorName":"Zstd"}}`)

	res = h.mustPass(app)
	if len(res.problems) != 1 || !strings.Contains(res.problems[0].Error(), `kopia.policy-json: kopia server does not support the compression algorithm "Zstd"; did you mean zstd?`) {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)
}

// "none" turns compression off and needs no answer of kopia server; a
// deprecated algorithm is applied with a warning.
func TestCompressionNoneAndDeprecated(t *testing.T) {
	h := newHarness(t)
	h.fake.SetCompressionAlgorithms(backend.Algorithm{Name: "zstd"}, backend.Algorithm{Name: "deflate-old", Deprecated: true})
	h.fake.FailNext(backendtest.CompressionAlgorithms, &backend.Error{Op: "CompressionAlgorithms", Kind: backend.ErrUnavailable})

	h.engine.run("app", enable, pathLabel(app), "kopia.compression.algorithm=none")
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]},"compression":{"compressorName":"none"}}`)

	h.engine.run("db", enable, pathLabel("/data/db"), "kopia.compression.algorithm=deflate-old")

	// kopia server cannot be asked: the pass is retried.
	if res := h.pass("/data/db"); res.err == nil || !res.retry {
		t.Fatalf("pass = %+v, want a retry", res)
	}

	h.mustNoSource("/data/db")

	if res := h.mustPass("/data/db"); len(res.problems) != 0 {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustPolicy("/data/db", `{"scheduling":{"cron":["0 * * * *"]},"compression":{"compressorName":"deflate-old"}}`)

	if !h.logged("the labels of a container ask for a deprecated compression algorithm", "deflate-old") {
		t.Errorf("no warning:\n%s", h.logs)
	}
}

// An owner whose algorithm kopia server lacks has labels that cannot be
// applied, so its description and tags are not used either.
func TestUnknownCompressionKeepsDescriptionOut(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app), "kopia.compression.algorithm=brotli", "kopia.description=app data", "kopia.tags=env:prod")

	containers, err := h.r.containers(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	ps := &pass{Reconciler: h.r, ctx: t.Context(), path: app, owners: h.r.owners(app, containers)}
	if err := ps.checkCompression(); err != nil {
		t.Fatal(err)
	}

	o := ps.owners[0]
	if o.valid || !o.patch.IsZero() || o.snap.described.description != "" || o.snap.described.tags != nil || o.snap.described.name != "app" {
		t.Errorf("owner = %+v", o)
	}
}

// In strict mode a deprecated algorithm is an error like the other
// warnings.
func TestDeprecatedCompressionStrict(t *testing.T) {
	h := newHarness(t, withStrict)
	h.fake.SetCompressionAlgorithms(backend.Algorithm{Name: "deflate-old", Deprecated: true})
	h.engine.run("app", enable, pathLabel(app), "kopia.compression.algorithm=deflate-old")

	res := h.mustPass(app)
	if len(res.problems) != 1 || !strings.Contains(res.problems[0].Error(), `compression algorithm "deflate-old" is deprecated by kopia`) {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustNoSource(app)
}
