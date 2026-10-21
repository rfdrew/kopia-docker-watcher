package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
)

// fakeKopia is an in-memory kopia server, an http.RoundTripper for
// kopiaclient.Options.Transport, so the backend runs over the real client
// without a network. It follows the UI API of kopia server
// (internal/server at 01938f96) as far as the backend uses it:
//
//   - GET / serves the CSRF token; an API request without it is a 401
//     without WWW-Authenticate.
//   - GET /api/v1/sources filters by host, userName and path and reports
//     the identity; sources/upload filters the same way and is a 404
//     NOT_FOUND without a match.
//   - GET /api/v1/policy returns the stored policy or a 400 NOT_FOUND.
//   - PUT /api/v1/policy decodes, validates (manual combined with anything,
//     a cron marked bad, upload.maxParallelSnapshots on a path, a trailing
//     slash: a 500 INTERNAL with kopia server's texts), stores the policy
//     and refreshes: every policy with a full target becomes a known
//     source, a source without a policy and without snapshots is
//     forgotten. DELETE removes the policy and refreshes.
//   - POST /api/v1/sources checks the path (PATH_NOT_FOUND, or a 500 for
//     another stat error), validates and stores the policy and makes the
//     source known, without a refresh: the source's scheduler does not have
//     the policy (loaded).
//   - POST /api/v1/policy/resolve puts updates in place of the stored
//     policy, reports a scheduling error, and returns the upcoming times
//     set with upcoming unless the effective scheduling is manual.
//   - POST /api/v1/paths/resolve expands ~, resolves a relative path
//     against home and cleans it.
//   - GET /api/v1/tasks lists tasks, GET /api/v1/tasks/{id} returns one
//     or a 404 NOT_FOUND, GET /api/v1/tasks/{id}/logs its log (null for an
//     unknown task).
//   - GET /api/v1/snapshots lists the source's snapshots, oldest first,
//     and without all=1 keeps the oldest of those with the same root.
//   - POST /api/v1/snapshots/edit changes the description and adds pins;
//     a changed snapshot gets a new ID, an unknown ID is a 500.
//   - POST /api/v1/snapshots/delete is a 404 NOT_FOUND for a source kopia
//     server does not know. With deleteSourceAndPolicy it deletes every
//     snapshot and the policy and forgets the source; otherwise the listed
//     manifests, an unknown ID being no error and one of another source a
//     500. It does not refresh.
//
// Requests are recorded. inject queues a response for the next request of
// a route, sent instead of the real one (before) or after the request took
// effect (after), as a lost answer.
type fakeKopia struct {
	mu sync.Mutex

	userName, host, home string
	token                string

	policies map[kopiaclient.SourceInfo]json.RawMessage
	sources  map[kopiaclient.SourceInfo]*fakeSource

	missing   map[string]bool   // paths os.Stat reports as not existing
	statErr   map[string]string // paths os.Stat fails for otherwise
	badCrons  map[string]bool
	normalize func(json.RawMessage) json.RawMessage
	// inherited is the scheduling section of the parent policies, under
	// the path's own in policy/resolve.
	inherited map[string]json.RawMessage
	upcoming  []time.Time
	// tasks are listed as given: kopia server lists the newest first.
	tasks    []kopiaclient.TaskInfo
	taskLogs map[string][]string
	// snapshots are the manifests of each source; nextID numbers the
	// manifests an edit saves.
	snapshots map[kopiaclient.SourceInfo][]kopiaclient.Snapshot
	nextID    int
	// refreshFails makes a refresh fail; kopia server only logs that, and
	// the policy write still succeeds.
	refreshFails bool

	requests   []fakeRequest
	injections map[string][]injection
	// hook runs for every API request before it is served, without the
	// fake locked.
	hook func(fakeRequest)
}

type fakeSource struct {
	// loaded: the source's scheduler has the stored policy.
	loaded       bool
	snapshots    bool
	status       string
	lastSnapshot *kopiaclient.SnapshotManifest
	next         *time.Time
	currentTask  string
	uploads      int
}

// fakeRequest is a recorded API request. Route is the method and the path
// below /api/v1/, e.g. "PUT policy".
type fakeRequest struct {
	Route string
	Query url.Values
	Body  json.RawMessage
}

// injection replaces the answer to one request: with a transport error if
// err is set, otherwise with status and body.
type injection struct {
	after  bool
	err    error
	status int
	body   string
}

const (
	fakeUser = "root"
	fakeHost = "kopia"
	fakeHome = "/root"
)

func newFakeKopia() *fakeKopia {
	return &fakeKopia{
		userName:   fakeUser,
		host:       fakeHost,
		home:       fakeHome,
		token:      "csrf-token-1",
		policies:   map[kopiaclient.SourceInfo]json.RawMessage{},
		sources:    map[kopiaclient.SourceInfo]*fakeSource{},
		missing:    map[string]bool{},
		statErr:    map[string]string{},
		badCrons:   map[string]bool{},
		inherited:  map[string]json.RawMessage{},
		injections: map[string][]injection{},
		taskLogs:   map[string][]string{},
		snapshots:  map[kopiaclient.SourceInfo][]kopiaclient.Snapshot{},
	}
}

func (f *fakeKopia) set(fn func(f *fakeKopia)) {
	f.mu.Lock()
	defer f.mu.Unlock()

	fn(f)
}

// setPolicy stores a policy as the web UI does, with a refresh.
func (f *fakeKopia) setPolicy(si kopiaclient.SourceInfo, policy string) {
	if !json.Valid([]byte(policy)) {
		panic("fakeKopia: invalid policy " + policy)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.policies[si] = json.RawMessage(policy)
	f.refresh()
}

func (f *fakeKopia) policy(si kopiaclient.SourceInfo) (json.RawMessage, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	p, ok := f.policies[si]

	return slices.Clone(p), ok
}

func (f *fakeKopia) known(si kopiaclient.SourceInfo) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, ok := f.sources[si]

	return ok
}

func (f *fakeKopia) loaded(si kopiaclient.SourceInfo) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	s, ok := f.sources[si]

	return ok && s.loaded
}

func (f *fakeKopia) uploads(si kopiaclient.SourceInfo) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	if s, ok := f.sources[si]; ok {
		return s.uploads
	}

	return 0
}

// addSource makes kopia server know si, with snapshots if snap is set.
func (f *fakeKopia) addSource(si kopiaclient.SourceInfo, snap *kopiaclient.SnapshotManifest) *fakeSource {
	f.mu.Lock()
	defer f.mu.Unlock()

	s := &fakeSource{loaded: true, snapshots: snap != nil, status: kopiaclient.SourceStatusIdle, lastSnapshot: snap}
	f.sources[si] = s

	return s
}

func (f *fakeKopia) inject(route string, inj injection) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.injections[route] = append(f.injections[route], inj)
}

func (f *fakeKopia) routes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, 0, len(f.requests))
	for _, r := range f.requests {
		out = append(out, r.Route)
	}

	return out
}

func (f *fakeKopia) requestsTo(route string) []fakeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []fakeRequest

	for _, r := range f.requests {
		if r.Route == route {
			out = append(out, r)
		}
	}

	return out
}

func (f *fakeKopia) resetRequests() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.requests = nil
}

// RoundTrip implements http.RoundTripper.
func (f *fakeKopia) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte

	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		_ = req.Body.Close()

		if err != nil {
			return nil, err
		}

		body = b
	}

	if err := req.Context().Err(); err != nil {
		return nil, err
	}

	f.mu.Lock()
	token := f.token
	f.mu.Unlock()

	if req.URL.Path == "/" {
		return reply(req, http.StatusOK, `<html><head><meta name="kopia-csrf-token" content="`+token+`"></head></html>`), nil
	}

	route, ok := strings.CutPrefix(req.URL.Path, "/api/v1/")
	if !ok {
		return reply(req, http.StatusNotFound, "404 page not found"), nil
	}

	if req.Header.Get(kopiaclient.CSRFTokenHeader) != token {
		return reply(req, http.StatusUnauthorized, "invalid CSRF token"), nil
	}

	r := fakeRequest{Route: req.Method + " " + route, Query: req.URL.Query(), Body: body}

	f.mu.Lock()
	f.requests = append(f.requests, r)
	hook := f.hook

	var inj *injection
	if q := f.injections[r.Route]; len(q) > 0 {
		inj, f.injections[r.Route] = &q[0], q[1:]
	}
	f.mu.Unlock()

	if hook != nil {
		hook(r)
	}

	if inj != nil && !inj.after {
		return inj.reply(req)
	}

	f.mu.Lock()
	status, out := f.serve(r)
	f.mu.Unlock()

	if inj != nil {
		return inj.reply(req)
	}

	return reply(req, status, out), nil
}

func (inj *injection) reply(req *http.Request) (*http.Response, error) {
	if inj.err != nil {
		return nil, inj.err
	}

	return reply(req, inj.status, inj.body), nil
}

func reply(req *http.Request, status int, body string) *http.Response {
	return &http.Response{
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		StatusCode: status,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{"Content-Type": {"text/plain; charset=utf-8"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func apiError(status int, code, msg string) (int, string) {
	b, _ := json.Marshal(map[string]string{"code": code, "error": msg})

	return status, string(b)
}

func ok(v any) (int, string) {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	return http.StatusOK, string(b)
}

func target(q url.Values) kopiaclient.SourceInfo {
	return kopiaclient.SourceInfo{Host: q.Get("host"), UserName: q.Get("userName"), Path: q.Get("path")}
}

func matches(si kopiaclient.SourceInfo, q url.Values) bool {
	for k, v := range map[string]string{"host": si.Host, "userName": si.UserName, "path": si.Path} {
		if want := q.Get(k); want != "" && want != v {
			return false
		}
	}

	return true
}

// serve answers an API request. f.mu is held.
func (f *fakeKopia) serve(r fakeRequest) (int, string) {
	switch r.Route {
	case "GET sources":
		return f.listSources(r.Query)
	case "POST sources":
		return f.createSource(r.Body)
	case "GET policy":
		p, found := f.policies[target(r.Query)]
		if !found {
			return apiError(http.StatusBadRequest, kopiaclient.CodeNotFound, "policy not found")
		}

		return http.StatusOK, string(p)
	case "PUT policy":
		return f.putPolicy(target(r.Query), r.Body)
	case "DELETE policy":
		delete(f.policies, target(r.Query))
		f.refresh()

		return ok(struct{}{})
	case "POST policy/resolve":
		return f.resolvePolicy(target(r.Query), r.Body)
	case "POST paths/resolve":
		return f.resolvePath(r.Body)
	case "POST sources/upload":
		return f.upload(r.Query)
	case "GET tasks":
		return ok(map[string]any{"tasks": f.tasks})
	case "GET snapshots":
		return f.listSnapshots(r.Query)
	case "POST snapshots/edit":
		return f.editSnapshots(r.Body)
	case "POST snapshots/delete":
		return f.deleteSnapshots(r.Body)
	}

	if id, found := strings.CutPrefix(r.Route, "GET tasks/"); found {
		if id, found := strings.CutSuffix(id, "/logs"); found {
			return f.taskLog(id)
		}

		return f.task(id)
	}

	return http.StatusNotFound, "404 page not found"
}

func (f *fakeKopia) task(id string) (int, string) {
	for _, t := range f.tasks {
		if t.ID == id {
			return ok(t)
		}
	}

	return apiError(http.StatusNotFound, kopiaclient.CodeNotFound, "task not found")
}

// taskLog returns the log of a task, every line at the level the test set
// in front of it: "3|message" is an error.
func (f *fakeKopia) taskLog(id string) (int, string) {
	lines, found := f.taskLogs[id]
	if !found {
		return ok(map[string]any{"logs": nil})
	}

	logs := []map[string]any{}

	for i, l := range lines {
		level, msg, _ := strings.Cut(l, "|")
		logs = append(logs, map[string]any{"level": json.Number(level), "ts": 1760778000 + i, "mod": "uploader", "msg": msg})
	}

	return ok(map[string]any{"logs": logs})
}

// listSnapshots is GET /api/v1/snapshots: the snapshots of exactly the
// source, sorted by start and end time like snapshot.SortByTime.
func (f *fakeKopia) listSnapshots(q url.Values) (int, string) {
	snaps := slices.Clone(f.snapshots[target(q)])
	slices.SortStableFunc(snaps, func(a, b kopiaclient.Snapshot) int {
		if c := a.StartTime.Compare(b.StartTime); c != 0 {
			return c
		}

		return a.EndTime.Compare(b.EndTime)
	})

	resp := map[string]any{"unfilteredCount": len(snaps)}

	if q.Get("all") == "" {
		var unique []kopiaclient.Snapshot

		for _, s := range snaps {
			if !slices.ContainsFunc(unique, func(u kopiaclient.Snapshot) bool { return u.RootID == s.RootID }) {
				unique = append(unique, s)
			}
		}

		snaps = unique
	}

	if snaps == nil {
		snaps = []kopiaclient.Snapshot{}
	}

	resp["snapshots"] = snaps

	return ok(resp)
}

// editSnapshots is POST /api/v1/snapshots/edit: a snapshot whose
// description or pins change is saved under a new ID.
func (f *fakeKopia) editSnapshots(body json.RawMessage) (int, string) {
	var req struct {
		Snapshots   []string `json:"snapshots"`
		Description *string  `json:"description"`
		AddPins     []string `json:"addPins"`
		RemovePins  []string `json:"removePins"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return apiError(http.StatusBadRequest, kopiaclient.CodeMalformedRequest, "malformed request")
	}

	var out []kopiaclient.Snapshot

	for _, id := range req.Snapshots {
		snap := f.snapshotByID(id)
		if snap == nil {
			return apiError(http.StatusInternalServerError, kopiaclient.CodeInternal,
				"internal server error: unable to load snapshot: manifest not found")
		}

		changed := req.Description != nil

		for _, p := range req.AddPins {
			if !slices.Contains(snap.Pins, p) {
				snap.Pins = append(snap.Pins, p)
				changed = true
			}
		}

		if req.Description != nil {
			snap.Description = *req.Description
		}

		if changed {
			f.nextID++
			snap.ID = fmt.Sprintf("edited-%d", f.nextID)
		}

		out = append(out, *snap)
	}

	return ok(out)
}

// deleteSnapshots is POST /api/v1/snapshots/delete.
func (f *fakeKopia) deleteSnapshots(body json.RawMessage) (int, string) {
	var req struct {
		Source                kopiaclient.SourceInfo `json:"source"`
		IDs                   []string               `json:"snapshotManifestIds"`
		DeleteSourceAndPolicy bool                   `json:"deleteSourceAndPolicy"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return apiError(http.StatusBadRequest, kopiaclient.CodeMalformedRequest, "malformed request")
	}

	s, found := f.sources[req.Source]
	if !found {
		return apiError(http.StatusNotFound, kopiaclient.CodeNotFound, "unknown source")
	}

	if req.DeleteSourceAndPolicy {
		delete(f.sources, req.Source)
		delete(f.snapshots, req.Source)
		delete(f.policies, req.Source)

		return ok(struct{}{})
	}

	for _, id := range req.IDs {
		if snap := f.snapshotByID(id); snap != nil &&
			!slices.ContainsFunc(f.snapshots[req.Source], func(x kopiaclient.Snapshot) bool { return x.ID == id }) {
			return apiError(http.StatusInternalServerError, kopiaclient.CodeInternal,
				"internal server error: source info does not match snapshot source")
		}
	}

	f.snapshots[req.Source] = slices.DeleteFunc(f.snapshots[req.Source], func(x kopiaclient.Snapshot) bool {
		return slices.Contains(req.IDs, x.ID)
	})
	s.snapshots = len(f.snapshots[req.Source]) > 0

	return ok(struct{}{})
}

func (f *fakeKopia) snapshotByID(id string) *kopiaclient.Snapshot {
	for _, snaps := range f.snapshots {
		for i := range snaps {
			if snaps[i].ID == id {
				return &snaps[i]
			}
		}
	}

	return nil
}

func (f *fakeKopia) listSources(q url.Values) (int, string) {
	resp := kopiaclient.SourcesResponse{LocalUsername: f.userName, LocalHost: f.host, MultiUser: true, Sources: []kopiaclient.SourceStatus{}}

	keys := slices.SortedFunc(maps.Keys(f.sources), func(a, b kopiaclient.SourceInfo) int {
		return strings.Compare(a.String(), b.String())
	})

	for _, si := range keys {
		if !matches(si, q) {
			continue
		}

		s := f.sources[si]
		resp.Sources = append(resp.Sources, kopiaclient.SourceStatus{
			Source: si, Status: s.status, LastSnapshot: s.lastSnapshot, NextSnapshotTime: s.next, CurrentTask: s.currentTask,
		})
	}

	return ok(resp)
}

// kopiaPolicy holds the fields of kopia's policy.Policy the fake checks. A
// value of the wrong type fails to decode, as in kopia server.
type kopiaPolicy struct {
	Scheduling struct {
		IntervalSeconds   int64           `json:"intervalSeconds"`
		TimeOfDay         json.RawMessage `json:"timeOfDay"`
		NoParentTimeOfDay bool            `json:"noParentTimeOfDay"`
		Manual            bool            `json:"manual"`
		Cron              []string        `json:"cron"`
		RunMissed         *bool           `json:"runMissed"`
	} `json:"scheduling"`
	Upload struct {
		MaxParallelSnapshots *int `json:"maxParallelSnapshots"`
	} `json:"upload"`
}

func decodePolicy(raw json.RawMessage) (*kopiaPolicy, error) {
	var p *kopiaPolicy
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}

	return p, nil
}

// validateScheduling is policy.ValidateSchedulingPolicy.
func (f *fakeKopia) validateScheduling(p *kopiaPolicy) error {
	s := p.Scheduling
	if s.Manual && (s.IntervalSeconds != 0 || s.TimeOfDay != nil || s.NoParentTimeOfDay || s.Cron != nil || s.RunMissed != nil) {
		return fmt.Errorf("invalid scheduling policy: manual cannot be combined with other scheduling policies")
	}

	for _, c := range s.Cron {
		if f.badCrons[c] {
			return fmt.Errorf("invalid cron expression %q", c)
		}
	}

	return nil
}

// setPolicyError is policy.SetPolicy's validation; "" if the policy is
// valid.
func (f *fakeKopia) setPolicyError(si kopiaclient.SourceInfo, p *kopiaPolicy) string {
	if err := f.validateScheduling(p); err != nil {
		return "failed to validate policy: invalid scheduling policy: " + err.Error()
	}

	if si.Path != "" && p.Upload.MaxParallelSnapshots != nil {
		return "failed to validate policy: invalid upload policy: max parallel snapshots cannot be specified for paths, only global, username@hostname or @hostname"
	}

	if si.Path != "" && si.Path != "/" && (strings.HasSuffix(si.Path, "/") || strings.HasSuffix(si.Path, `\`)) {
		return "invalid policy path: path cannot end with a slash or a backslash"
	}

	return ""
}

func (f *fakeKopia) store(si kopiaclient.SourceInfo, raw json.RawMessage) {
	stored := slices.Clone(raw)
	if f.normalize != nil {
		stored = f.normalize(stored)
	}

	f.policies[si] = stored
}

func (f *fakeKopia) putPolicy(si kopiaclient.SourceInfo, body json.RawMessage) (int, string) {
	p, err := decodePolicy(body)
	if err != nil || p == nil {
		return apiError(http.StatusBadRequest, kopiaclient.CodeMalformedRequest, "malformed request body")
	}

	if msg := f.setPolicyError(si, p); msg != "" {
		return apiError(http.StatusInternalServerError, kopiaclient.CodeInternal, "internal server error: unable to set policy: "+msg)
	}

	f.store(si, body)
	f.refresh()

	return ok(struct{}{})
}

// refresh is kopia server's Refresh: a source manager for every policy with
// a full target and every source with snapshots, each with its current
// policy. A failed refresh changes nothing.
func (f *fakeKopia) refresh() {
	if f.refreshFails {
		return
	}

	for si := range f.policies {
		if si.Host != "" && si.UserName != "" && si.Path != "" {
			if _, found := f.sources[si]; !found {
				f.sources[si] = &fakeSource{status: kopiaclient.SourceStatusIdle}
			}
		}
	}

	for si, s := range f.sources {
		if _, hasPolicy := f.policies[si]; !hasPolicy && !s.snapshots {
			delete(f.sources, si)

			continue
		}

		s.loaded = true
	}
}

func (f *fakeKopia) createSource(body json.RawMessage) (int, string) {
	var req struct {
		Path   string          `json:"path"`
		Policy json.RawMessage `json:"policy"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return apiError(http.StatusBadRequest, kopiaclient.CodeMalformedRequest, "malformed request body")
	}

	p, err := decodePolicy(req.Policy)
	if err != nil {
		return apiError(http.StatusBadRequest, kopiaclient.CodeMalformedRequest, "malformed request body")
	}

	switch {
	case req.Path == "":
		return apiError(http.StatusBadRequest, kopiaclient.CodeMalformedRequest, "missing path")
	case p == nil:
		return apiError(http.StatusBadRequest, kopiaclient.CodeMalformedRequest, "missing policy")
	}

	resolved := f.userPath(req.Path)

	if f.missing[resolved] {
		return apiError(http.StatusBadRequest, kopiaclient.CodePathNotFound, "path does not exist")
	}

	if msg, failed := f.statErr[resolved]; failed {
		return apiError(http.StatusInternalServerError, kopiaclient.CodeInternal, "internal server error: "+msg)
	}

	si := kopiaclient.SourceInfo{Host: f.host, UserName: f.userName, Path: resolved}

	if msg := f.setPolicyError(si, p); msg != "" {
		return apiError(http.StatusInternalServerError, kopiaclient.CodeInternal, "internal server error: unable to set initial policy: "+msg)
	}

	f.store(si, req.Policy)

	s, found := f.sources[si]
	if !found {
		s = &fakeSource{status: kopiaclient.SourceStatusIdle}
		f.sources[si] = s
	}

	s.loaded = false

	return ok(map[string]bool{"snapshotted": false})
}

// userPath is ospath.ResolveUserFriendlyPath: ~ is home, a relative path
// is resolved against home. It does not clean the path.
func (f *fakeKopia) userPath(p string) string {
	switch {
	case strings.HasPrefix(p, "~"):
		return f.home + p[1:]
	case path.IsAbs(p):
		return p
	default:
		return path.Join(f.home, p)
	}
}

func (f *fakeKopia) resolvePolicy(si kopiaclient.SourceInfo, body json.RawMessage) (int, string) {
	var req struct {
		Updates                  json.RawMessage `json:"updates"`
		NumUpcomingSnapshotTimes int             `json:"numUpcomingSnapshotTimes"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return apiError(http.StatusBadRequest, kopiaclient.CodeMalformedRequest, "unable to decode request: "+err.Error())
	}

	own := f.policies[si]
	if !isNull(req.Updates) {
		own = req.Updates
	}

	p, err := decodePolicy(own)
	if err != nil {
		return apiError(http.StatusBadRequest, kopiaclient.CodeMalformedRequest, "unable to decode request: "+err.Error())
	}

	if p == nil {
		p = &kopiaPolicy{}
	}

	resp := map[string]any{"defined": f.policies[si], "definition": map[string]any{}, "upcomingSnapshotTimes": []time.Time{}}

	if err := f.validateScheduling(p); err != nil {
		resp["schedulingError"] = err.Error()
	}

	var obj map[string]json.RawMessage
	_ = json.Unmarshal(own, &obj)

	if obj == nil {
		obj = map[string]json.RawMessage{}
	}

	section := maps.Clone(f.inherited)

	var ownSection map[string]json.RawMessage
	_ = json.Unmarshal(obj["scheduling"], &ownSection)
	maps.Copy(section, ownSection)

	if section == nil {
		section = map[string]json.RawMessage{}
	}

	enc, _ := json.Marshal(section)
	obj["scheduling"] = enc
	resp["effective"] = obj

	if !bytes.Equal(section["manual"], []byte("true")) {
		resp["upcomingSnapshotTimes"] = f.upcoming[:min(max(req.NumUpcomingSnapshotTimes, 0), len(f.upcoming))]
	}

	return ok(resp)
}

func (f *fakeKopia) resolvePath(body json.RawMessage) (int, string) {
	var req struct {
		Path string `json:"path"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return apiError(http.StatusBadRequest, kopiaclient.CodeMalformedRequest, "malformed request body")
	}

	si := kopiaclient.SourceInfo{Host: f.host, UserName: f.userName, Path: path.Clean(f.userPath(req.Path))}

	return ok(map[string]any{"source": si})
}

func (f *fakeKopia) upload(q url.Values) (int, string) {
	out := map[string]map[string]bool{}

	for si, s := range f.sources {
		if matches(si, q) {
			s.uploads++
			out[si.String()] = map[string]bool{"success": true}
		}
	}

	if len(out) == 0 {
		return apiError(http.StatusNotFound, kopiaclient.CodeNotFound, "no source matching the provided filters")
	}

	return ok(map[string]any{"sources": out})
}
