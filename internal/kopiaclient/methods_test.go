package kopiaclient

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"
)

var testSource = SourceInfo{Host: "nas", UserName: "root", Path: "/data/app"}

// respond returns a handler writing a fixed JSON body.
func respond(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}
}

func assertQuery(t *testing.T, call *recordedCall, want url.Values) {
	t.Helper()

	got, err := url.ParseQuery(call.Query)
	if err != nil {
		t.Fatal(err)
	}

	if got.Encode() != want.Encode() {
		t.Errorf("query = %q, want %q", got.Encode(), want.Encode())
	}
}

func assertJSONBody(t *testing.T, call *recordedCall, want string) {
	t.Helper()

	var got, exp any
	if err := json.Unmarshal(call.Body, &got); err != nil {
		t.Fatalf("request body %q: %v", call.Body, err)
	}

	if err := json.Unmarshal([]byte(want), &exp); err != nil {
		t.Fatal(err)
	}

	g, _ := json.Marshal(got)
	e, _ := json.Marshal(exp)

	if string(g) != string(e) {
		t.Errorf("request body = %s\nwant %s", g, e)
	}

	if ct := call.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
}

func sourceQuery() url.Values {
	return url.Values{"host": {"nas"}, "userName": {"root"}, "path": {"/data/app"}}
}

const sourcesJSON = `{
  "localUsername": "root", "localHost": "nas", "multiUser": true,
  "sources": [
    {
      "source": {"host": "nas", "userName": "root", "path": "/data/app"},
      "status": "IDLE",
      "schedule": {"cron": ["0 * * * *"], "runMissed": true},
      "lastSnapshot": {
        "id": "k5f0e", "source": {"host": "nas", "userName": "root", "path": "/data/app"},
        "description": "", "startTime": "2026-10-18T01:00:00.123456789Z", "endTime": "2026-10-18T01:00:05Z",
        "stats": {"totalSize": 10, "excludedTotalSize": 0, "fileCount": 1, "cachedFiles": 0, "nonCachedFiles": 1,
                  "dirCount": 1, "excludedFileCount": 0, "excludedDirCount": 0, "ignoredErrorCount": 0, "errorCount": 2},
        "incomplete": "checkpoint",
        "rootEntry": {"name": "app", "type": "d", "mode": "0755", "mtime": "2026-10-17T00:00:00Z", "obj": "k123",
                      "summ": {"size": 10, "files": 1, "symlinks": 0, "dirs": 1, "maxTime": "2026-10-17T00:00:00Z", "numFailed": 2}}
      },
      "nextSnapshotTime": "2026-10-18T02:00:00Z"
    },
    {
      "source": {"host": "nas", "userName": "root", "path": "/data/db"},
      "status": "UPLOADING", "schedule": {"manual": true},
      "upload": {"hashedFiles": 3, "hashedBytes": 300, "directory": "/data/db/x", "errors": 0},
      "currentTask": "7"
    }
  ]
}`

func TestListSources(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET sources", respond(sourcesJSON))
	c := newTestClient(t, f)

	resp, err := c.ListSources(t.Context(), SourceInfo{Host: "nas", UserName: "root"})
	if err != nil {
		t.Fatal(err)
	}

	assertQuery(t, f.lastCall(t), url.Values{"host": {"nas"}, "userName": {"root"}})

	if resp.LocalHost != "nas" || resp.LocalUsername != "root" || !resp.MultiUser || len(resp.Sources) != 2 {
		t.Fatalf("resp = %+v", resp)
	}

	s0 := resp.Sources[0]
	if s0.Status != SourceStatusIdle || s0.Source != testSource || s0.Schedule.Cron[0] != "0 * * * *" ||
		s0.Schedule.RunMissed == nil || !*s0.Schedule.RunMissed {
		t.Errorf("source 0 = %+v", s0)
	}

	ls := s0.LastSnapshot
	if ls == nil || ls.ID != "k5f0e" || ls.IncompleteReason != "checkpoint" || ls.Stats.ErrorCount != 2 ||
		ls.RootEntry.DirSummary.FatalErrorCount != 2 || ls.StartTime.Nanosecond() != 123456789 {
		t.Errorf("last snapshot = %+v", ls)
	}

	if s0.NextSnapshotTime == nil || !s0.NextSnapshotTime.Equal(time.Date(2026, 10, 18, 2, 0, 0, 0, time.UTC)) {
		t.Errorf("next snapshot = %v", s0.NextSnapshotTime)
	}

	s1 := resp.Sources[1]
	if s1.Status != SourceStatusUploading || s1.CurrentTask != "7" || !s1.Schedule.Manual ||
		s1.Upload == nil || s1.Upload.HashedFiles != 3 || s1.Upload.CurrentDirectory != "/data/db/x" {
		t.Errorf("source 1 = %+v", s1)
	}

	if _, err := c.ListSources(t.Context(), SourceInfo{}); err != nil {
		t.Fatal(err)
	}

	if q := f.lastCall(t).Query; q != "" {
		t.Errorf("empty filter sent query %q", q)
	}
}

func TestCreateSource(t *testing.T) {
	f := newFakeKopia()
	f.handle("POST sources", respond(`{"snapshotted": true}`))
	c := newTestClient(t, f)

	started, err := c.CreateSource(t.Context(), CreateSourceRequest{
		Path: "/data/app", CreateSnapshot: true, Policy: json.RawMessage(`{"scheduling":{"cron":["0 3 * * *"]}}`),
	})
	if err != nil || !started {
		t.Fatalf("started = %v, err = %v", started, err)
	}

	call := f.lastCall(t)
	if call.Method != http.MethodPost || call.Path != "/api/v1/sources" {
		t.Errorf("call = %s %s", call.Method, call.Path)
	}

	assertJSONBody(t, call, `{"path":"/data/app","createSnapshot":true,"policy":{"scheduling":{"cron":["0 3 * * *"]}}}`)

	f.handle("POST sources", func(w http.ResponseWriter, _ *http.Request) {
		writeAPIError(w, 400, CodePathNotFound, "path does not exist")
	})

	if _, err := c.CreateSource(t.Context(), CreateSourceRequest{Path: "/missing", Policy: json.RawMessage(`{}`)}); !errors.Is(err, ErrPathNotFound) {
		t.Errorf("err = %v, want ErrPathNotFound", err)
	}

	_, calls := f.counts()

	for _, req := range []CreateSourceRequest{
		{Path: "/data/app"},
		{Path: "/data/app", Policy: json.RawMessage(`null`)},
		{Path: "/data/app", Policy: json.RawMessage(`[]`)},
		{Policy: json.RawMessage(`{}`)},
	} {
		if _, err := c.CreateSource(t.Context(), req); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("CreateSource(%+v) err = %v, want ErrInvalidArgument", req, err)
		}
	}

	if _, after := f.counts(); after != calls {
		t.Errorf("invalid requests reached the server: %d calls", after-calls)
	}
}

func TestStartSnapshot(t *testing.T) {
	f := newFakeKopia()
	f.handle("POST sources/upload", respond(`{"sources": {"root@nas:/data/app": {"success": true}}}`))
	c := newTestClient(t, f)

	if err := c.StartSnapshot(t.Context(), testSource); err != nil {
		t.Fatal(err)
	}

	call := f.lastCall(t)
	if call.Method != http.MethodPost || call.Path != "/api/v1/sources/upload" {
		t.Errorf("call = %s %s", call.Method, call.Path)
	}

	assertQuery(t, call, sourceQuery())

	// Without host and userName kopia would also start other users' sources.
	for _, src := range []SourceInfo{{Path: "/data/app"}, {Host: "nas", Path: "/data/app"}, {Host: "nas", UserName: "root"}} {
		err := c.StartSnapshot(t.Context(), src)
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("StartSnapshot(%+v) err = %v, want ErrInvalidArgument", src, err)
		}

		if IsRetryable(err) {
			t.Error("ErrInvalidArgument must not be retryable")
		}
	}

	f.handle("POST sources/upload", func(w http.ResponseWriter, _ *http.Request) {
		writeAPIError(w, 404, CodeNotFound, "no source matching the provided filters")
	})

	if err := c.StartSnapshot(t.Context(), testSource); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestGetPolicyKeepsUnknownFields(t *testing.T) {
	const policy = `{"retention":{"keepLatest":5},"scheduling":{"cron":["0 * * * *"]},"fieldFromTheFuture":{"x":[1,2]}}`

	f := newFakeKopia()
	f.handle("GET policy", respond(policy+"\n"))
	c := newTestClient(t, f)

	raw, err := c.GetPolicy(t.Context(), testSource)
	if err != nil {
		t.Fatal(err)
	}

	assertQuery(t, f.lastCall(t), sourceQuery())

	var got, want map[string]any
	_ = json.Unmarshal(raw, &got)
	_ = json.Unmarshal([]byte(policy), &want)

	if g, w := mustMarshal(t, got), mustMarshal(t, want); g != w {
		t.Errorf("policy = %s, want %s", g, w)
	}
}

func TestGetPolicyErrors(t *testing.T) {
	f := newFakeKopia()
	c := newTestClient(t, f)

	f.handle("GET policy", func(w http.ResponseWriter, _ *http.Request) {
		writeAPIError(w, 400, CodeNotFound, "policy not found")
	})

	if _, err := c.GetPolicy(t.Context(), testSource); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing policy: err = %v, want ErrNotFound", err)
	}

	// Any other repository error is 200 with null.
	f.handle("GET policy", respond("null\n"))

	_, err := c.GetPolicy(t.Context(), testSource)
	if !errors.Is(err, ErrInternal) || !IsRetryable(err) {
		t.Errorf("null policy: err = %v, want retryable ErrInternal", err)
	}

	for _, src := range []SourceInfo{{}, {Host: "nas", UserName: "root"}} {
		if _, err := c.GetPolicy(t.Context(), src); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("GetPolicy(%+v) err = %v; host and global policies are not the watcher's", src, err)
		}
	}
}

func TestSetPolicy(t *testing.T) {
	f := newFakeKopia()
	f.handle("PUT policy", respond(`{}`))
	c := newTestClient(t, f)

	policy := json.RawMessage(`{"scheduling":{"manual":true},"unknown":{"kept":true}}`)
	if err := c.SetPolicy(t.Context(), testSource, policy); err != nil {
		t.Fatal(err)
	}

	call := f.lastCall(t)
	if call.Method != http.MethodPut || call.Path != "/api/v1/policy" {
		t.Errorf("call = %s %s", call.Method, call.Path)
	}

	assertQuery(t, call, sourceQuery())
	assertJSONBody(t, call, string(policy))

	for _, bad := range []string{``, `null`, `[]`, `{"a":`} {
		if err := c.SetPolicy(t.Context(), testSource, json.RawMessage(bad)); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("SetPolicy(%q) err = %v, want ErrInvalidArgument", bad, err)
		}
	}

	f.handle("PUT policy", func(w http.ResponseWriter, _ *http.Request) {
		writeAPIError(w, 500, CodeInternal, `internal server error: unable to set policy: failed to validate policy: invalid cron expression "x"`)
	})

	err := c.SetPolicy(t.Context(), testSource, policy)
	if !errors.Is(err, ErrInvalidPolicy) || IsRetryable(err) {
		t.Errorf("err = %v, want non-retryable ErrInvalidPolicy", err)
	}
}

func TestDeletePolicy(t *testing.T) {
	f := newFakeKopia()
	f.handle("DELETE policy", respond(`{}`))
	c := newTestClient(t, f)

	if err := c.DeletePolicy(t.Context(), testSource); err != nil {
		t.Fatal(err)
	}

	call := f.lastCall(t)
	assertQuery(t, call, sourceQuery())

	if len(call.Body) != 0 {
		t.Errorf("DELETE sent a body: %q", call.Body)
	}

	if err := c.DeletePolicy(t.Context(), SourceInfo{Host: "nas"}); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestResolvePolicy(t *testing.T) {
	f := newFakeKopia()
	f.handle("POST policy/resolve", respond(`{
		"effective": {"scheduling": {"cron": ["0 3 * * *"]}, "retention": {"keepLatest": 10}},
		"definition": {"retention": {"keepLatest": {"host": "", "userName": "", "path": ""}}},
		"defined": {},
		"upcomingSnapshotTimes": ["2026-10-19T03:00:00+03:00", "2026-10-20T03:00:00+03:00"],
		"schedulingError": "invalid cron expression \"bad\""
	}`))
	c := newTestClient(t, f)

	resp, err := c.ResolvePolicy(t.Context(), testSource, ResolvePolicyRequest{
		Updates: json.RawMessage(`{"scheduling":{"cron":["0 3 * * *"]}}`), NumUpcomingSnapshotTimes: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	call := f.lastCall(t)
	assertQuery(t, call, sourceQuery())
	assertJSONBody(t, call, `{"updates":{"scheduling":{"cron":["0 3 * * *"]}},"numUpcomingSnapshotTimes":2}`)

	if len(resp.UpcomingSnapshotTimes) != 2 || !resp.UpcomingSnapshotTimes[0].Equal(time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("upcoming = %v", resp.UpcomingSnapshotTimes)
	}

	if resp.SchedulingError == "" || len(resp.Effective) == 0 || len(resp.Definition) == 0 || string(resp.Defined) != "{}" {
		t.Errorf("resp = %+v", resp)
	}

	if _, err := c.ResolvePolicy(t.Context(), testSource, ResolvePolicyRequest{}); err != nil {
		t.Fatal(err)
	}

	assertJSONBody(t, f.lastCall(t), `{"updates":null,"numUpcomingSnapshotTimes":0}`)
}

func TestResolvePath(t *testing.T) {
	f := newFakeKopia()
	f.handle("POST paths/resolve", respond(`{"source": {"host": "nas", "userName": "root", "path": "/data/app"}}`))
	c := newTestClient(t, f)

	src, err := c.ResolvePath(t.Context(), "/data/app/")
	if err != nil {
		t.Fatal(err)
	}

	assertJSONBody(t, f.lastCall(t), `{"path":"/data/app/"}`)

	if src != testSource {
		t.Errorf("source = %+v", src)
	}

	if _, err := c.ResolvePath(t.Context(), ""); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("err = %v", err)
	}
}

func TestListSnapshots(t *testing.T) {
	f := newFakeKopia()
	f.handle("GET snapshots", respond(`{
		"snapshots": [
			{"id": "k2", "description": "app", "startTime": "2026-10-18T02:00:00Z", "endTime": "2026-10-18T02:00:09Z",
			 "summary": {"size": 5, "files": 2, "symlinks": 0, "dirs": 1, "maxTime": "2026-10-18T01:59:00Z", "numFailed": 0},
			 "rootID": "kroot", "retention": ["latest-1"], "pins": ["before-upgrade"]},
			{"id": "k1", "description": "", "startTime": "2026-10-18T01:00:00Z", "endTime": "2026-10-18T01:30:00Z",
			 "incomplete": "checkpoint", "summary": null, "rootID": "kroot0", "retention": [], "pins": []}
		],
		"unfilteredCount": 2, "uniqueCount": 2
	}`))
	c := newTestClient(t, f)

	resp, err := c.ListSnapshots(t.Context(), testSource, true)
	if err != nil {
		t.Fatal(err)
	}

	q := sourceQuery()
	q.Set("all", "1")
	assertQuery(t, f.lastCall(t), q)

	if len(resp.Snapshots) != 2 || resp.UnfilteredCount != 2 || resp.UniqueCount != 2 {
		t.Fatalf("resp = %+v", resp)
	}

	s := resp.Snapshots[0]
	if s.ID != "k2" || s.Description != "app" || s.RootID != "kroot" || s.Pins[0] != "before-upgrade" ||
		s.Retention[0] != "latest-1" || s.Summary == nil || s.Summary.TotalFileCount != 2 {
		t.Errorf("snapshot 0 = %+v", s)
	}

	if resp.Snapshots[1].IncompleteReason != "checkpoint" || resp.Snapshots[1].Summary != nil {
		t.Errorf("snapshot 1 = %+v", resp.Snapshots[1])
	}

	if _, err := c.ListSnapshots(t.Context(), testSource, false); err != nil {
		t.Fatal(err)
	}

	assertQuery(t, f.lastCall(t), sourceQuery())
}

func TestEditSnapshots(t *testing.T) {
	f := newFakeKopia()
	f.handle("POST snapshots/edit", respond(`[{"id": "k3", "description": "app [env=prod]", "startTime": "2026-10-18T02:00:00Z",
		"endTime": "2026-10-18T02:00:09Z", "summary": null, "rootID": "kroot", "retention": [], "pins": ["keep"]}]`))
	c := newTestClient(t, f)

	desc := "app [env=prod]"

	got, err := c.EditSnapshots(t.Context(), EditSnapshotsRequest{Snapshots: []string{"k2"}, Description: &desc, AddPins: []string{"keep"}})
	if err != nil {
		t.Fatal(err)
	}

	assertJSONBody(t, f.lastCall(t), `{"snapshots":["k2"],"description":"app [env=prod]","addPins":["keep"],"removePins":null}`)

	if len(got) != 1 || got[0].ID != "k3" || got[0].Description != desc {
		t.Errorf("got = %+v", got)
	}

	if _, err := c.EditSnapshots(t.Context(), EditSnapshotsRequest{AddPins: []string{"x"}}); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("err = %v", err)
	}
}

func TestDeleteSnapshots(t *testing.T) {
	f := newFakeKopia()
	f.handle("POST snapshots/delete", respond(`{}`))
	c := newTestClient(t, f)

	if err := c.DeleteSnapshots(t.Context(), DeleteSnapshotsRequest{Source: testSource, SnapshotManifestIDs: []string{"k1", "k2"}}); err != nil {
		t.Fatal(err)
	}

	assertJSONBody(t, f.lastCall(t),
		`{"source":{"host":"nas","userName":"root","path":"/data/app"},"snapshotManifestIds":["k1","k2"],"deleteSourceAndPolicy":false}`)

	if err := c.DeleteSnapshots(t.Context(), DeleteSnapshotsRequest{Source: testSource, DeleteSourceAndPolicy: true}); err != nil {
		t.Fatal(err)
	}

	for _, req := range []DeleteSnapshotsRequest{
		{Source: testSource},
		{Source: testSource, SnapshotManifestIDs: []string{"k1"}, DeleteSourceAndPolicy: true},
		{Source: SourceInfo{Host: "nas", Path: "/data/app"}, DeleteSourceAndPolicy: true},
	} {
		if err := c.DeleteSnapshots(t.Context(), req); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("DeleteSnapshots(%+v) err = %v, want ErrInvalidArgument", req, err)
		}
	}

	f.handle("POST snapshots/delete", func(w http.ResponseWriter, _ *http.Request) {
		writeAPIError(w, 400, CodeNotFound, "unknown source")
	})

	if err := c.DeleteSnapshots(t.Context(), DeleteSnapshotsRequest{Source: testSource, DeleteSourceAndPolicy: true}); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestTasks(t *testing.T) {
	const task = `{"id": "12", "startTime": "2026-10-18T01:00:00Z", "endTime": "2026-10-18T01:00:05Z", "kind": "Snapshot",
		"description": "root@nas:/data/app at 2026-10-18T01:00:00Z", "status": "FAILED", "progressInfo": "",
		"errorMessage": "snapshot task: unable to read directory", "counters": {"Errors": {"value": 1, "level": "error"}}}`

	f := newFakeKopia()
	f.handle("GET tasks", respond(`{"tasks": [`+task+`, {"id": "13", "startTime": "2026-10-18T01:10:00Z", "kind": "Maintenance",
		"description": "Periodic maintenance", "status": "RUNNING", "progressInfo": "", "counters": {}}]}`))
	f.handle("GET tasks/12", respond(task))
	f.handle("GET tasks/12/logs", respond(`{"logs": [
		{"level": 1, "ts": 1792285200.25, "mod": "kopia/server", "msg": "starting upload", "path": "/data/app"},
		{"level": 3, "ts": 1792285205, "mod": "uploader", "msg": "unable to read directory"}
	]}`))
	c := newTestClient(t, f)

	tasks, err := c.ListTasks(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if len(tasks) != 2 || tasks[0].Status != TaskFailed || !tasks[0].Status.IsFinished() || tasks[1].Status.IsFinished() ||
		tasks[0].Kind != TaskKindSnapshot || tasks[0].Counters["Errors"].Level != "error" || tasks[1].EndTime != nil {
		t.Fatalf("tasks = %+v", tasks)
	}

	ti, err := c.GetTask(t.Context(), "12")
	if err != nil {
		t.Fatal(err)
	}

	if f.lastCall(t).Path != "/api/v1/tasks/12" || ti.ErrorMessage == "" || ti.EndTime == nil {
		t.Errorf("task = %+v", ti)
	}

	logs, err := c.TaskLogs(t.Context(), "12")
	if err != nil {
		t.Fatal(err)
	}

	if len(logs) != 2 || logs[0].Level != TaskLogInfo || logs[0].Module != "kopia/server" || logs[0].Message != "starting upload" ||
		!logs[0].Time.Equal(time.Unix(1792285200, 250_000_000)) || logs[1].Level != TaskLogError {
		t.Errorf("logs = %+v", logs)
	}

	var fields map[string]any
	if err := json.Unmarshal(logs[0].Raw, &fields); err != nil || fields["path"] != "/data/app" {
		t.Errorf("raw entry = %s", logs[0].Raw)
	}

	f.handle("GET tasks/99", func(w http.ResponseWriter, _ *http.Request) {
		writeAPIError(w, 404, CodeNotFound, "task not found")
	})

	if _, err := c.GetTask(t.Context(), "99"); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}

	for _, id := range []string{"", "../repo/status", "1/logs"} {
		if _, err := c.GetTask(t.Context(), id); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("GetTask(%q) err = %v", id, err)
		}

		if _, err := c.TaskLogs(t.Context(), id); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("TaskLogs(%q) err = %v", id, err)
		}
	}
}

func TestSourceInfoString(t *testing.T) {
	for want, s := range map[string]SourceInfo{
		"(global)":           {},
		"root@nas":           {Host: "nas", UserName: "root"},
		"root@nas:/data/app": testSource,
	} {
		if got := s.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	}
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()

	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}

	return string(b)
}
