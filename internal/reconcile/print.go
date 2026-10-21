package reconcile

import (
	"fmt"
	"strings"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
)

// timeFormat formats snapshot times in logs and plans.
const timeFormat = "2006-01-02 15:04:05 -07:00"

// changes lists the fields a plan changes, e.g.
// `scheduling.cron: (unset) -> ["0 * * * *"]`.
func changes(plan backend.Plan) []string {
	diff, err := backend.Diff(plan.Current, plan.Policy)
	if err != nil {
		return []string{err.Error()}
	}

	out := make([]string, len(diff))

	for i, c := range diff {
		old, nv := "(unset)", "(unset)"
		if c.Old != nil {
			old = string(c.Old)
		}

		if c.New != nil {
			nv = string(c.New)
		}

		out[i] = c.Field + ": " + old + " -> " + nv
	}

	return out
}

// print logs what a pass of a dry run would do and prints it to Output:
// the plan, or why there is none, or the error that ended the pass.
func (ps *pass) print(err error) {
	var b strings.Builder

	name := ps.path
	if ps.src != (backend.Source{}) {
		name = ps.src.String()
	}

	attrs := []any{"source", name, "owners", ps.want.Owners, "paused", ps.want.Paused}

	switch {
	case err != nil && ps.outcome != "":
		fmt.Fprintf(&b, "%s: %s: %v\n", name, ps.outcome, err)
		attrs = append(attrs, "outcome", ps.outcome, "error", err.Error())
	case err != nil:
		fmt.Fprintf(&b, "%s: error: %v\n", name, err)
		attrs = append(attrs, "error", err.Error())
	case ps.plan == nil:
		fmt.Fprintf(&b, "%s: %s\n", name, ps.outcome)
		attrs = append(attrs, "outcome", ps.outcome)
	default:
		fmt.Fprintf(&b, "%s: %s\n", name, actionText(ps.plan.Action))
		attrs = append(attrs, "action", ps.plan.Action.String(), "changes", changes(*ps.plan))
	}

	if ps.plan == nil && err == nil && len(ps.owners) == 0 && !ps.had && !ps.outside {
		// A path nobody backs up: nothing to say.
		return
	}

	if !ps.outside {
		ps.writeOwners(&b)
	}

	if ps.plan != nil {
		ps.writePlan(&b)
	}

	if ps.cleanupNote != "" {
		fmt.Fprintf(&b, "  cleanup: %s\n", ps.cleanupNote)
		attrs = append(attrs, "cleanup", ps.cleanupNote)
	}

	for _, p := range ps.problems {
		fmt.Fprintf(&b, "  problem: %v\n", p)
	}

	ps.log.Info("dry run: nothing applied", attrs...)

	if ps.output == nil {
		return
	}

	ps.printMu.Lock()
	defer ps.printMu.Unlock()

	_, _ = fmt.Fprint(ps.output, b.String())
}

func (ps *pass) writeOwners(b *strings.Builder) {
	if len(ps.owners) == 0 {
		b.WriteString("  owners: none\n")
	} else {
		parts := make([]string, len(ps.owners))
		for i, o := range ps.owners {
			parts[i] = fmt.Sprintf("%s (%s)", o.name, o.state)
		}

		fmt.Fprintf(b, "  owners: %s\n", strings.Join(parts, ", "))
	}

	if ps.src != (backend.Source{}) && ps.want.Source == ps.src {
		fmt.Fprintf(b, "  paused: %t\n", ps.want.Paused)
	}
}

func (ps *pass) writePlan(b *strings.Builder) {
	plan := ps.plan

	for _, c := range changes(*plan) {
		fmt.Fprintf(b, "  change: %s\n", c)
	}

	if !plan.Applied.Patch.Equal(ps.last.Patch) {
		fmt.Fprintf(b, "  labels: %s -> %s", describePatch(ps.last.Patch), describePatch(plan.Applied.Patch))

		if ps.want.Paused {
			b.WriteString(" (applies when a container starts)")
		}

		b.WriteString("\n")
	}

	if plan.Action != backend.ActionNone && ps.last.Pending != nil {
		b.WriteString("  note: the last apply was not confirmed, so the policy is written again\n")
	}

	if ps.preview != nil && !ps.want.Paused && len(ps.preview.Times) > 0 {
		times := make([]string, len(ps.preview.Times))
		for i, t := range ps.preview.Times {
			times[i] = t.Format(timeFormat)
		}

		fmt.Fprintf(b, "  next snapshots: %s\n", strings.Join(times, ", "))
	}
}

func actionText(a backend.Action) string {
	switch a {
	case backend.ActionNone:
		return "no change"
	case backend.ActionRegister:
		return "would register the source"
	case backend.ActionUpdate:
		return "would update the policy"
	default:
		return a.String()
	}
}
