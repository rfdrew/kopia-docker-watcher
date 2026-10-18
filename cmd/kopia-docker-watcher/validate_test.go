package main

import (
	"strings"
	"testing"
)

func runValidateTest(t *testing.T, getenv func(string) string, stdin string, args ...string) (code int, stdout, stderr string) {
	t.Helper()

	td := newTestDeps(getenv)
	td.stdin = strings.NewReader(stdin)

	code = run(t.Context(), append([]string{"validate"}, args...), td.deps)

	return code, td.stdout.String(), td.stderr.String()
}

func TestValidateOutput(t *testing.T) {
	code, stdout, stderr := runValidateTest(t, env(), "",
		"kopia.enable=true",
		"kopia.path=/data/,/logs",
		"kopia.schedule=0 2 * * *; 30 3 * * 0",
		"kopia.snapshot-on-stop=yes",
		"kopia.tags=env:prod,app:web",
		"kopia.bogus=1",
		"com.docker.compose.project=app",
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr: %s", code, stderr)
	}

	want := `kopia.enable             true
kopia.path               /data/, /logs
kopia.schedule           0 2 * * *; 30 3 * * 0
kopia.snapshot-on-start  true
kopia.snapshot-on-stop   false
kopia.description        (container name)
kopia.tags               env:prod, app:web

warning: kopia.bogus: unknown label, ignored
warning: kopia.snapshot-on-stop: "yes" is not a boolean, treated as false; use true or false
`
	if stdout != want {
		t.Errorf("stdout:\n%s\nwant:\n%s", stdout, want)
	}

	if stderr != "" {
		t.Errorf("stderr = %q", stderr)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name   string
		env    func(string) string
		stdin  string
		args   []string
		code   int
		stdout []string // lines expected in stdout, spaces collapsed
		stderr []string // substrings expected in stderr
	}{
		{
			name:   "defaults",
			args:   []string{"kopia.enable=true", "kopia.path=/data"},
			stdout: []string{"kopia.schedule 0 * * * *", "kopia.snapshot-on-start true", "kopia.snapshot-on-stop false", "kopia.tags (none)"},
		},
		{
			name:   "empty schedule",
			args:   []string{"kopia.enable=true", "kopia.path=/data", "kopia.schedule="},
			stdout: []string{"kopia.schedule (inherit from the parent policies)"},
		},
		{
			name:   "error",
			args:   []string{"kopia.enable=true"},
			code:   1,
			stdout: []string{"kopia.path (none)", "error: kopia.path: required when kopia.enable is true"},
		},
		{
			name:   "strict",
			args:   []string{"--strict", "kopia.enable=true", "kopia.path=/data", "kopia.bogus=1"},
			code:   1,
			stdout: []string{"error: kopia.bogus: unknown label"},
		},
		{
			name: "stdin",
			stdin: "# labels of the app container\r\n" +
				"\n" +
				"  kopia.enable=TRUE\r\n" +
				"kopia.path=/srv/app\n" +
				"kopia.description= my app\n" +
				"\t# indented comment\n",
			args:   []string{"-"},
			stdout: []string{"kopia.enable true", "kopia.path /srv/app", "kopia.description my app"},
		},
		{
			name:   "later labels override earlier ones",
			stdin:  "kopia.enable=false\nkopia.path=/a\n",
			args:   []string{"kopia.enable=true", "-", "kopia.path=/b"},
			stdout: []string{"kopia.enable false", "kopia.path /b"},
		},
		{
			name:   "value with equals signs",
			args:   []string{"kopia.description=a=b"},
			stdout: []string{"kopia.description a=b"},
		},
		{
			name:   "label prefix",
			env:    env("LABEL_PREFIX", "backup"),
			args:   []string{"backup.enable=true", "backup.path=/data", "kopia.enable=false"},
			stdout: []string{"backup.enable true", "backup.path /data"},
		},
		{
			name:   "no labels with the prefix",
			env:    env("LABEL_PREFIX", "backup"),
			args:   []string{"kopia.enable=true"},
			stdout: []string{"backup.enable false", "no backup.* labels given"},
		},
		{
			name:   "invalid label prefix",
			env:    env("LABEL_PREFIX", "backup."),
			args:   []string{"backup.enable=true"},
			code:   2,
			stderr: []string{"LABEL_PREFIX", "invalid label prefix"},
		},
		{name: "no labels", code: 2, stderr: []string{"no labels given", "Usage:"}},
		{name: "not label=value", args: []string{"kopia.enable"}, code: 2, stderr: []string{`"kopia.enable" is not label=value`}},
		{name: "empty name", args: []string{"=true"}, code: 2, stderr: []string{"invalid label name"}},
		{name: "space in name", args: []string{"kopia enable=true"}, code: 2, stderr: []string{"invalid label name"}},
		{name: "stdin twice", args: []string{"-", "-"}, code: 2, stderr: []string{`"-" is given more than once`}},
		{name: "option after labels", args: []string{"kopia.enable=true", "--strict"}, code: 2, stderr: []string{"options go before the labels"}},
		{name: "unknown option", args: []string{"--bogus"}, code: 2, stderr: []string{"flag provided but not defined", "Usage:"}},
		{
			// Line numbers count every line, skipped ones included.
			name:   "bad stdin line",
			stdin:  "# app\n\nkopia.enable=true\nkopia.path\n",
			args:   []string{"-"},
			code:   2,
			stderr: []string{"stdin line 4:", `"kopia.path" is not label=value`},
		},
		{
			name:   "byte order mark and unicode spaces on stdin",
			stdin:  "\ufeffkopia.enable=true\n\u00a0kopia.path=/data\n",
			args:   []string{"-"},
			stdout: []string{"kopia.enable true", "kopia.path /data"},
		},
		{
			name:   "byte order mark before a comment",
			stdin:  "\ufeff# app\nkopia.enable=true\nkopia.path=/data\n",
			args:   []string{"-"},
			stdout: []string{"kopia.enable true"},
		},
		{
			name:   "long stdin line",
			stdin:  "kopia.description=" + strings.Repeat("a", 100_000) + "\n",
			args:   []string{"-"},
			stdout: []string{"kopia.description " + strings.Repeat("a", 100_000)},
		},
		{
			name:   "stdin line too long",
			stdin:  "kopia.description=" + strings.Repeat("a", maxLabelLine) + "\n",
			args:   []string{"-"},
			code:   2,
			stderr: []string{"reading stdin", "token too long"},
		},
		{
			// A newline in a value cannot fake an issue line.
			name:   "control characters are quoted",
			args:   []string{"kopia.description=x\nerror: kopia.path: fake"},
			stdout: []string{`kopia.description "x\nerror: kopia.path: fake"`},
		},
		{name: "control character in name", args: []string{"kopia.enable\n=true"}, code: 2, stderr: []string{"invalid label name"}},
		{name: "help", args: []string{"--help"}, code: 0, stdout: []string{"Usage: kopia-docker-watcher validate [--strict] label=value... | -"}},
		{name: "short help", args: []string{"-h"}, code: 0, stdout: []string{"Usage: kopia-docker-watcher validate [--strict] label=value... | -"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getenv := tt.env
			if getenv == nil {
				getenv = env()
			}

			code, stdout, stderr := runValidateTest(t, getenv, tt.stdin, tt.args...)

			if code != tt.code {
				t.Errorf("exit code = %d, want %d\nstdout: %s\nstderr: %s", code, tt.code, stdout, stderr)
			}

			lines := map[string]bool{}
			for _, l := range strings.Split(stdout, "\n") {
				lines[strings.Join(strings.Fields(l), " ")] = true
			}

			for _, want := range tt.stdout {
				if !lines[want] {
					t.Errorf("stdout has no line %q:\n%s", want, stdout)
				}
			}

			for _, want := range tt.stderr {
				if !strings.Contains(stderr, want) {
					t.Errorf("stderr does not contain %q:\n%s", want, stderr)
				}
			}

			if len(tt.stderr) == 0 && stderr != "" {
				t.Errorf("stderr = %q", stderr)
			}
		})
	}
}
