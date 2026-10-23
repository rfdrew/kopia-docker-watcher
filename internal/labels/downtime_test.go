package labels

import (
	"testing"
	"time"
)

func TestParseDowntime(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		opts   Options
		want   func(s *ContainerSpec)
		issues []string
	}{
		{
			name:   "stop",
			labels: map[string]string{"kopia.stop-during-backup": "TRUE"},
			want: func(s *ContainerSpec) {
				s.StopDuringBackup = true
				s.Hooks = true
			},
		},
		{
			name:   "pause with a bound",
			labels: map[string]string{"kopia.pause-during-backup": "true", "kopia.max-downtime": " 15m "},
			want: func(s *ContainerSpec) {
				s.PauseDuringBackup = true
				s.MaxDowntime = 15 * time.Minute
				s.Hooks = true
			},
		},
		{
			name:   "false",
			labels: map[string]string{"kopia.stop-during-backup": "false", "kopia.pause-during-backup": "false"},
		},
		{
			name:   "both",
			labels: map[string]string{"kopia.stop-during-backup": "true", "kopia.pause-during-backup": "true"},
			want: func(s *ContainerSpec) {
				s.StopDuringBackup = true
				s.PauseDuringBackup = true
				s.Hooks = true
			},
			issues: []string{"error kopia.pause-during-backup: cannot be used with kopia.stop-during-backup: " +
				"a container is either stopped or paused"},
		},
		{
			name:   "not a boolean",
			labels: map[string]string{"kopia.stop-during-backup": "yes"},
			issues: []string{`warning kopia.stop-during-backup: "yes" is not a boolean, treated as false; use true or false`},
		},
		{
			// In strict mode the snapshots fail rather than run with the
			// container up.
			name:   "not a boolean, strict",
			labels: map[string]string{"kopia.stop-during-backup": "yes"},
			opts:   Options{Strict: true},
			want:   func(s *ContainerSpec) { s.Hooks = true },
			issues: []string{`error kopia.stop-during-backup: "yes" is not a boolean; use true or false`},
		},
		{
			name:   "bad bound",
			labels: map[string]string{"kopia.stop-during-backup": "true", "kopia.max-downtime": "1d"},
			want: func(s *ContainerSpec) {
				s.StopDuringBackup = true
				s.Hooks = true
			},
			issues: []string{`error kopia.max-downtime: invalid duration "1d"; ` + maxDowntimeHint},
		},
		{
			name:   "zero bound",
			labels: map[string]string{"kopia.stop-during-backup": "true", "kopia.max-downtime": "0"},
			want: func(s *ContainerSpec) {
				s.StopDuringBackup = true
				s.Hooks = true
			},
			issues: []string{`error kopia.max-downtime: "0" is not greater than zero; ` + maxDowntimeHint},
		},
		{
			name:   "empty bound",
			labels: map[string]string{"kopia.pause-during-backup": "true", "kopia.max-downtime": ""},
			want: func(s *ContainerSpec) {
				s.PauseDuringBackup = true
				s.Hooks = true
			},
			issues: []string{"error kopia.max-downtime: empty value; " + maxDowntimeHint},
		},
		{
			name:   "bound without a stop",
			labels: map[string]string{"kopia.max-downtime": "2h"},
			want:   func(s *ContainerSpec) { s.MaxDowntime = 2 * time.Hour },
			issues: []string{"warning kopia.max-downtime: has no effect without kopia.stop-during-backup or kopia.pause-during-backup"},
		},
		{
			// The containers that depend on a stopped one carry the label;
			// they need no other.
			name:   "depends on",
			labels: map[string]string{"kopia.depends-on": " db , ,redis-1,shop_db.x,db"},
			want:   func(s *ContainerSpec) { s.DependsOn = []string{"db", "redis-1", "shop_db.x"} },
			issues: []string{`warning kopia.depends-on: "db" is listed more than once`},
		},
		{
			name:   "depends on a bad name",
			labels: map[string]string{"kopia.depends-on": "db,/web,-x"},
			want:   func(s *ContainerSpec) { s.DependsOn = []string{"db"} },
			issues: []string{
				`error kopia.depends-on: "/web" is not a container or service name`,
				`error kopia.depends-on: "-x" is not a container or service name`,
			},
		},
		{
			name:   "depends on nothing",
			labels: map[string]string{"kopia.depends-on": ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := Default()
			if tt.want != nil {
				tt.want(&want)
			}

			checkParse(t, tt.labels, tt.opts, want, tt.issues)
		})
	}
}
