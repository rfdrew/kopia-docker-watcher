package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
)

const validateUsage = `Usage: kopia-docker-watcher validate [--strict] label=value... | -

Parses container labels the way the watcher does and prints the result and
every problem. Labels are given as arguments, or with "-" one label=value per
line on stdin; blank lines and lines starting with # are skipped. Labels
without the LABEL_PREFIX prefix are ignored.

Cron expressions and paths are not checked yet.

Options:
  --strict  treat warnings as errors

Exit status: 0 if the labels can be applied (warnings allowed), 1 if not,
2 on a usage error.
`

// maxLabelLine bounds one line of label input.
const maxLabelLine = 1 << 20

func runValidate(args []string, d deps) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(d.stderr)
	fs.Usage = func() { _, _ = fmt.Fprint(d.stderr, validateUsage) }
	strict := fs.Bool("strict", false, "treat warnings as errors")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}

		return 2
	}

	prefix, err := config.ParseLabelPrefix(d.getenv(config.EnvLabelPrefix))
	if err != nil {
		_, _ = fmt.Fprintf(d.stderr, "validate: %s: %v\n", config.EnvLabelPrefix, err)
		return 2
	}

	input, err := readLabels(fs.Args(), d.stdin)
	if err != nil {
		_, _ = fmt.Fprintf(d.stderr, "validate: %v\n\n%s", err, validateUsage)
		return 2
	}

	spec, issues := labels.Parse(input, labels.Options{Prefix: prefix, Strict: *strict})

	printSpec(d.stdout, prefix, spec)

	if !hasPrefixed(input, prefix) {
		_, _ = fmt.Fprintf(d.stdout, "\nno %s.* labels given\n", prefix)
	}

	if len(issues) > 0 {
		_, _ = fmt.Fprintln(d.stdout)
	}

	for _, is := range issues {
		_, _ = fmt.Fprintf(d.stdout, "%s: %s\n", is.Severity, is)
	}

	if issues.HasErrors() {
		return 1
	}

	return 0
}

// readLabels collects labels from label=value arguments and, for "-", from
// stdin. A later label overrides an earlier one, as in docker run.
func readLabels(args []string, stdin io.Reader) (map[string]string, error) {
	if len(args) == 0 {
		return nil, errors.New("no labels given")
	}

	out := map[string]string{}
	stdinRead := false

	for _, arg := range args {
		if arg != "-" {
			if strings.HasPrefix(arg, "-") {
				return nil, fmt.Errorf("unexpected %q: options go before the labels", arg)
			}

			if err := addLabel(out, arg); err != nil {
				return nil, err
			}

			continue
		}

		if stdinRead {
			return nil, errors.New(`"-" is given more than once`)
		}

		stdinRead = true

		if err := readLabelLines(out, stdin); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func readLabelLines(out map[string]string, r io.Reader) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(nil, maxLabelLine)

	for n := 1; sc.Scan(); n++ {
		line := strings.TrimRight(sc.Text(), "\r")

		if trimmed := strings.TrimSpace(line); trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		if err := addLabel(out, strings.TrimLeft(line, " \t")); err != nil {
			return fmt.Errorf("stdin line %d: %w", n, err)
		}
	}

	if err := sc.Err(); err != nil {
		return fmt.Errorf("reading stdin: %w", err)
	}

	return nil
}

// addLabel adds one label=value. The value is kept verbatim: the parser
// decides which spaces matter.
func addLabel(out map[string]string, kv string) error {
	key, value, ok := strings.Cut(kv, "=")

	switch {
	case !ok:
		return fmt.Errorf("%q is not label=value", kv)
	case key == "" || strings.ContainsAny(key, " \t"):
		return fmt.Errorf("%q has an invalid label name", kv)
	}

	out[key] = value

	return nil
}

func hasPrefixed(input map[string]string, prefix string) bool {
	for k := range input {
		if strings.HasPrefix(k, prefix+".") {
			return true
		}
	}

	return false
}

func printSpec(w io.Writer, prefix string, spec labels.ContainerSpec) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	row := func(key, value string) { _, _ = fmt.Fprintf(tw, "%s\t%s\n", labels.Name(prefix, key), value) }

	row(labels.KeyEnable, strconv.FormatBool(spec.Enable))
	row(labels.KeyPath, orNone(strings.Join(spec.Paths, ", "), "(none)"))
	row(labels.KeySchedule, orNone(strings.Join(spec.Policy.Scheduling.Cron, "; "), "(inherit from the host or global policy)"))
	row(labels.KeySnapshotOnStart, strconv.FormatBool(spec.SnapshotOnStart))
	row(labels.KeySnapshotOnStop, strconv.FormatBool(spec.SnapshotOnStop))
	row(labels.KeyDescription, orNone(spec.Description, "(container name)"))

	tags := make([]string, 0, len(spec.Tags))
	for _, t := range spec.Tags {
		tags = append(tags, t.Key+":"+t.Value)
	}

	row(labels.KeyTags, orNone(strings.Join(tags, ", "), "(none)"))

	_ = tw.Flush()
}

func orNone(s, none string) string {
	if s == "" {
		return none
	}

	return s
}
