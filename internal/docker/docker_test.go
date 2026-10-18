package docker

import (
	"maps"
	"testing"
	"time"

	"github.com/moby/moby/api/types/events"
)

func TestStateRunning(t *testing.T) {
	tests := []struct {
		state State
		want  bool
	}{
		{StateRunning, true},
		{StatePaused, true},
		{StateRestarting, true},
		{StateCreated, false},
		{StateExited, false},
		{StateDead, false},
		{StateRemoving, false},
		// Unknown states keep backups going.
		{"hibernating", true},
	}

	for _, tt := range tests {
		if got := tt.state.Running(); got != tt.want {
			t.Errorf("%q.Running() = %v, want %v", tt.state, got, tt.want)
		}
	}
}

func TestStoppedByDie(t *testing.T) {
	tests := []struct {
		name  string
		found bool
		state State
		want  bool
	}{
		{"exited", true, StateExited, true},
		{"dead", true, StateDead, true},
		{"being removed", true, StateRemoving, true},
		{"removed already", false, "", true},
		{"restart policy", true, StateRestarting, false},
		{"running again", true, StateRunning, false},
		{"paused right after a restart", true, StatePaused, false},
	}

	for _, tt := range tests {
		if got := StoppedByDie(tt.found, tt.state); got != tt.want {
			t.Errorf("%s: StoppedByDie(%v, %q) = %v, want %v", tt.name, tt.found, tt.state, got, tt.want)
		}
	}
}

func TestEventFromDocker(t *testing.T) {
	const nano = 1_760_000_000_123_456_789

	labels := map[string]string{"kopia.enable": "true", "com.docker.compose.project": "app"}

	attrs := func(extra ...string) map[string]string {
		a := maps.Clone(labels)
		a["image"] = "postgres:17"
		a["name"] = "db"

		for i := 0; i < len(extra); i += 2 {
			a[extra[i]] = extra[i+1]
		}

		return a
	}

	msg := func(action events.Action, a map[string]string) events.Message {
		return events.Message{
			Type:     events.ContainerEventType,
			Action:   action,
			Actor:    events.Actor{ID: "c1", Attributes: a},
			Scope:    "local",
			Time:     nano / int64(time.Second),
			TimeNano: nano,
		}
	}

	tests := []struct {
		name string
		msg  events.Message
		want Kind // 0: ignored
	}{
		{"create", msg(events.ActionCreate, attrs()), KindChanged},
		{"rename", msg(events.ActionRename, attrs("oldName", "/db-old")), KindChanged},
		{"start", msg(events.ActionStart, attrs()), KindStarted},
		{"die", msg(events.ActionDie, attrs("exitCode", "137", "execDuration", "12")), KindDied},
		{"destroy", msg(events.ActionDestroy, attrs()), KindRemoved},
		// docker stop of a container waiting for its restart policy sends
		// stop without die.
		{"stop", msg(events.ActionStop, attrs()), KindDied},

		// kill is only a signal: docker kill -s HUP keeps the container running.
		{"kill", msg(events.ActionKill, attrs("signal", "1")), 0},
		{"restart", msg(events.ActionRestart, attrs()), 0},
		{"oom", msg(events.ActionOOM, attrs()), 0},
		{"pause", msg(events.ActionPause, attrs()), 0},
		{"unpause", msg(events.ActionUnPause, attrs()), 0},
		{"update", msg(events.ActionUpdate, attrs()), 0},
		{"exec_start", msg("exec_start: pg_isready", attrs()), 0},
		{"health_status", msg(events.ActionHealthStatusHealthy, attrs()), 0},
		{"no actor", events.Message{Type: events.ContainerEventType, Action: events.ActionStart}, 0},
		{"network", events.Message{
			Type:   events.NetworkEventType,
			Action: events.ActionDestroy,
			Actor:  events.Actor{ID: "n1", Attributes: map[string]string{"name": "app_default"}},
		}, 0},
	}

	for _, tt := range tests {
		ev, ok := eventFromDocker(tt.msg)

		if tt.want == 0 {
			if ok {
				t.Errorf("%s: got %+v, want ignored", tt.name, ev)
			}

			continue
		}

		if !ok {
			t.Errorf("%s: ignored, want %v", tt.name, tt.want)
			continue
		}

		if ev.Kind != tt.want || ev.Action != string(tt.msg.Action) || ev.ContainerID != "c1" || ev.Name != "db" {
			t.Errorf("%s: got %+v", tt.name, ev)
		}

		if !ev.Time.Equal(time.Unix(0, nano)) {
			t.Errorf("%s: time = %v", tt.name, ev.Time)
		}

		// Only the labels remain: image, name, exitCode, oldName, ... are
		// Docker's own attributes.
		if !maps.Equal(ev.Labels, labels) {
			t.Errorf("%s: labels = %v, want %v", tt.name, ev.Labels, labels)
		}
	}
}

func TestEventFromDockerWithoutTimeNano(t *testing.T) {
	ev, ok := eventFromDocker(events.Message{
		Type:   events.ContainerEventType,
		Action: events.ActionStart,
		Actor:  events.Actor{ID: "c1"},
		Time:   1_760_000_000,
	})
	if !ok || !ev.Time.Equal(time.Unix(1_760_000_000, 0)) {
		t.Fatalf("event = %+v, %v", ev, ok)
	}
}

func TestPrimaryName(t *testing.T) {
	tests := []struct {
		names []string
		want  string
	}{
		{[]string{"/web"}, "web"},
		// A legacy link adds the alias under the linking container's name.
		{[]string{"/app/db", "/db"}, "db"},
		{[]string{"/a/b"}, "a/b"},
		{nil, ""},
	}

	for _, tt := range tests {
		if got := primaryName(tt.names); got != tt.want {
			t.Errorf("primaryName(%q) = %q, want %q", tt.names, got, tt.want)
		}
	}
}

func TestKindString(t *testing.T) {
	for k, want := range map[Kind]string{
		KindResync:  "resync",
		KindChanged: "changed",
		KindStarted: "started",
		KindDied:    "died",
		KindRemoved: "removed",
		0:           "unknown",
	} {
		if got := k.String(); got != want {
			t.Errorf("Kind(%d).String() = %q, want %q", k, got, want)
		}
	}
}
