package labels

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Hook label keys, relative to the prefix.
const (
	KeyPreExec     = "pre-exec"
	KeyPostExec    = "post-exec"
	KeyHookTimeout = "hook-timeout"
)

// DefaultHookTimeout bounds each hook command when the hook-timeout label
// is absent.
const DefaultHookTimeout = 10 * time.Minute

// hookKeys are the labels of the commands the watcher runs in a container
// around its snapshots.
var hookKeys = []key{
	{KeyPreExec, func(p *parser, v string) { p.spec.PreExec = p.command(v) }},
	{KeyPostExec, func(p *parser, v string) { p.spec.PostExec = p.command(v) }},
	{KeyHookTimeout, (*parser).hookTimeout},
}

// commandHint names the forms of a hook command.
const commandHint = `a value that starts with [ is a JSON array of strings such as ["pg_dump","-f","/backup/db.sql"]; ` +
	`a shell command that starts with [ can use test instead`

// command parses a hook command: a JSON array of strings, run as it is, or
// any other text, run with sh -c. A value that starts with [ is the array,
// never a shell command, so that a mistake in it is found here rather than
// when the hook runs. An empty value is no hook. A value with an error is
// no command, but it still marks the container as one with hooks: its
// sources stay on the watcher's schedule, and the hook fails when it is due
// to run, so that a pre-exec hook with an error takes no snapshot instead
// of one without the hook.
func (p *parser) command(v string) []string {
	s := strings.TrimSpace(v)
	if s == "" {
		return nil
	}

	p.spec.Hooks = true

	if !strings.HasPrefix(s, "[") {
		return []string{"sh", "-c", s}
	}

	var items []any
	if err := json.Unmarshal([]byte(s), &items); err != nil {
		p.errorf("invalid JSON array: %v; %s", err, commandHint)
		return nil
	}

	args := make([]string, len(items))

	for i, item := range items {
		a, ok := item.(string)
		if !ok {
			p.errorf("item %d of the array is not a string; %s", i+1, commandHint)
			return nil
		}

		args[i] = a
	}

	switch {
	case len(args) == 0:
		p.errorf("empty command; %s", commandHint)
	case strings.TrimSpace(args[0]) == "":
		p.errorf("the command name is empty; %s", commandHint)
	default:
		return args
	}

	return nil
}

// hookTimeoutHint names the accepted values of the hook-timeout label.
const hookTimeoutHint = "use a Go duration greater than zero such as 30s, 10m or 1h"

// hookTimeout parses how long each hook command may run. A value that does
// not parse is an error: a timeout far shorter than meant would fail every
// snapshot of a long dump.
func (p *parser) hookTimeout(v string) {
	p.hookTimeoutLabel = true

	s := strings.TrimSpace(v)
	d, err := time.ParseDuration(s)

	switch {
	case s == "":
		p.errorf("empty value; %s", hookTimeoutHint)
	case err != nil:
		p.errorf("invalid duration %q; %s", v, hookTimeoutHint)
	case d <= 0:
		p.errorf("%q is not greater than zero; %s", v, hookTimeoutHint)
	default:
		p.spec.HookTimeout = d
	}
}

// checkHooks warns about a hook timeout without a hook.
func (p *parser) checkHooks() {
	if p.hookTimeoutLabel && !p.spec.Hooks {
		p.label = Name(p.prefix, KeyHookTimeout)
		p.warn(fmt.Sprintf("has no effect without %s or %s", Name(p.prefix, KeyPreExec), Name(p.prefix, KeyPostExec)), "", "")
	}
}
