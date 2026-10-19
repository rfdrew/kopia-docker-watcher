package main

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"

	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
	"github.com/rfdrew/kopia-docker-watcher/internal/version"
)

const validateUsage = `Usage:
  kopia-docker-watcher validate [--strict] label=value... | -
  kopia-docker-watcher validate [--strict] --container NAME [--container NAME]...
  kopia-docker-watcher validate [--strict] --all

Parses container labels the way the watcher does and prints the result and
every problem. The labels come from one of:

  label=value...    arguments; "-" reads one label=value per line from
                    stdin, skipping blank lines and lines starting with #
  --container NAME  a container, by name or ID; may be repeated
  --all             every container with the LABEL_PREFIX.enable label,
                    stopped ones included

Labels without the LABEL_PREFIX prefix are ignored. Docker is reached like
the docker CLI reaches it, through DOCKER_HOST.

Each path of the path label is put under HOST_PREFIX and must then be
absolute. With KOPIA_SERVER_URL set, kopia server checks more, read-only:
it normalizes every path (paths/resolve) and computes the next snapshot
times of the path's stored policy with the schedule of the labels in place
(policy/resolve), in its own time zone, which rejects a bad cron
expression. Whether a path exists is not checked: only registering a
source does that. Without KOPIA_SERVER_URL only the labels are checked.

Options:
  --strict          treat warnings as errors
  --container NAME  check the labels of a container
  --all             check every container with LABEL_PREFIX.enable

Exit status: 0 if the labels can be applied (warnings allowed), 1 if not or
if Docker or kopia server could not be asked, 2 on a usage or
configuration error.
`

// maxLabelLine bounds one line of label input.
const maxLabelLine = 1 << 20

const (
	// upcomingSnapshots is how many snapshot times kopia server computes
	// for every path.
	upcomingSnapshots = 3
	// dockerTimeout bounds one request to the Docker Engine.
	dockerTimeout = 30 * time.Second
	// timeLayout shows a snapshot time with the offset of kopia server's
	// time zone, in which it computes the schedule.
	timeLayout = "2006-01-02 15:04:05 -07:00"
)

// containerFlag collects the values of a repeated --container.
type containerFlag []string

func (c *containerFlag) String() string { return strings.Join(*c, ",") }

func (c *containerFlag) Set(v string) error {
	if strings.TrimSpace(v) == "" {
		return errors.New("empty container name")
	}

	*c = append(*c, v)

	return nil
}

func runValidate(ctx context.Context, args []string, d deps) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(d.stderr)
	fs.Usage = func() {} // printed below: to stdout on request, to stderr on errors
	strict := fs.Bool("strict", false, "treat warnings as errors")
	all := fs.Bool("all", false, "check every container with LABEL_PREFIX.enable")

	var containers containerFlag

	fs.Var(&containers, "container", "check the labels of a container")

	usageError := func(err error) int {
		_, _ = fmt.Fprintf(d.stderr, "validate: %v\n\n%s", err, validateUsage)
		return 2
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = fmt.Fprint(d.stdout, validateUsage)
			return 0
		}

		_, _ = fmt.Fprintf(d.stderr, "\n%s", validateUsage)

		return 2
	}

	fromDocker := *all || len(containers) > 0

	switch {
	case *all && len(containers) > 0:
		return usageError(errors.New("--all and --container cannot be combined"))
	case fromDocker && fs.NArg() > 0:
		return usageError(fmt.Errorf("unexpected %q: labels cannot be given together with --all or --container", fs.Arg(0)))
	}

	v, code := newValidator(d, *strict)
	if code != 0 {
		return code
	}

	var input map[string]string

	if !fromDocker {
		var err error
		if input, err = readLabels(fs.Args(), d.stdin); err != nil {
			return usageError(err)
		}
	}

	if v.kopia != nil {
		if err := checkKopiaServer(ctx, v.kopia); err != nil {
			_, _ = fmt.Fprintf(d.stderr, "validate: %v\n", err)
			return 1
		}
	}

	var ok bool

	if fromDocker {
		var err error
		if ok, err = v.checkContainers(ctx, d, containers, *all); err != nil {
			_, _ = fmt.Fprintf(d.stderr, "validate: %v\n", err)
			return 1
		}
	} else {
		r := v.check(ctx, input)
		r.print(d.stdout, v.prefix, "given")
		ok = !r.issues.HasErrors()
	}

	v.printFooter(d.stdout)

	if !ok {
		return 1
	}

	return 0
}

// validator checks sets of labels. Nothing it does changes Docker or kopia
// server.
type validator struct {
	prefix     string
	hostPrefix string
	strict     bool
	// kopia is nil without KOPIA_SERVER_URL: only the labels are checked.
	kopia *kopiaclient.Client
	// checked counts the label sets checked, for the footer.
	checked int
}

// newValidator reads the configuration validate needs. Only LABEL_PREFIX
// and HOST_PREFIX matter without KOPIA_SERVER_URL; with it, the whole
// configuration is loaded and must be valid, as for the watcher. A non-zero
// code means validate has to exit with it.
func newValidator(d deps, strict bool) (*validator, int) {
	v := &validator{strict: strict}

	if strings.TrimSpace(d.getenv(config.EnvKopiaServerURL)) == "" {
		var err error
		if v.prefix, err = config.ParseLabelPrefix(d.getenv(config.EnvLabelPrefix)); err != nil {
			_, _ = fmt.Fprintf(d.stderr, "validate: %s: %v\n", config.EnvLabelPrefix, err)
			return nil, 2
		}

		if v.hostPrefix, err = config.ParseHostPrefix(d.getenv(config.EnvHostPrefix)); err != nil {
			_, _ = fmt.Fprintf(d.stderr, "validate: %s: %v\n", config.EnvHostPrefix, err)
			return nil, 2
		}

		return v, 0
	}

	cfg, err := config.Load(d.getenv)
	if err != nil {
		errs := []error{err}
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			errs = joined.Unwrap()
		}

		for _, e := range errs {
			_, _ = fmt.Fprintf(d.stderr, "validate: invalid configuration: %v\n", e)
		}

		return nil, 2
	}

	for _, w := range cfg.Warnings {
		_, _ = fmt.Fprintf(d.stderr, "validate: warning: %s\n", w)
	}

	v.prefix = cfg.LabelPrefix
	v.hostPrefix = cfg.HostPrefix

	v.kopia, err = kopiaclient.New(kopiaclient.Options{
		URL:                cfg.Kopia.URL,
		Username:           cfg.Kopia.Username,
		Password:           cfg.Kopia.Password,
		CertFingerprint:    cfg.Kopia.CertFingerprint,
		CAFile:             cfg.Kopia.CAFile,
		InsecureSkipVerify: cfg.Kopia.InsecureSkipVerify,
		UserAgent:          version.UserAgent(),
		Transport:          d.kopiaTransport,
	})
	if err != nil {
		_, _ = fmt.Fprintf(d.stderr, "validate: %v\n", err)
		return nil, 2
	}

	return v, 0
}

// checkKopiaServer makes sure kopia server can answer before any label is
// checked, so that an unreachable server is one error, not one per path.
func checkKopiaServer(ctx context.Context, kopia *kopiaclient.Client) error {
	st, err := kopia.RepoStatus(ctx)
	if err != nil {
		return err
	}

	if !st.Connected {
		return errors.New("kopia server has no connected repository yet")
	}

	return nil
}

// checkContainers checks the named containers, or with all every container
// with the enable label, and prints a report for each. ok is false if any
// of them cannot be applied or does not exist; err is a Docker failure.
func (v *validator) checkContainers(ctx context.Context, d deps, names []string, all bool) (ok bool, err error) {
	engine, closer, err := d.newDocker()
	if err != nil {
		return false, err
	}

	defer func() { _ = closer.Close() }()

	var list []docker.Container

	if all {
		if list, err = v.enabledContainers(ctx, engine); err != nil {
			return false, err
		}

		if len(list) == 0 {
			_, _ = fmt.Fprintf(d.stdout, "no containers with the %s label\n", labels.Name(v.prefix, labels.KeyEnable))
			return true, nil
		}
	}

	ok = true
	first := true

	// Reports are separated by a blank line.
	begin := func() {
		if !first {
			_, _ = fmt.Fprintln(d.stdout)
		}

		first = false
	}

	report := func(c docker.Container) {
		begin()
		_, _ = fmt.Fprintf(d.stdout, "container %s (%s, %s)\n", printable(c.Name), shortID(c.ID), printable(string(c.State)))

		r := v.check(ctx, c.Labels)
		r.print(d.stdout, v.prefix, "on this container")

		if r.issues.HasErrors() {
			ok = false
		}
	}

	for _, c := range list {
		report(c)
	}

	for _, name := range names {
		c, err := inspect(ctx, engine, name)

		switch {
		case errors.Is(err, docker.ErrNotFound):
			begin()
			_, _ = fmt.Fprintf(d.stdout, "container %s\nerror: no such container\n", printable(name))

			ok = false
		case err != nil:
			return false, err
		default:
			report(c)
		}
	}

	return ok, nil
}

func inspect(ctx context.Context, engine docker.Engine, name string) (docker.Container, error) {
	ctx, cancel := context.WithTimeout(ctx, dockerTimeout)
	defer cancel()

	return engine.Inspect(ctx, name)
}

// enabledContainers lists the containers with the enable label, whatever
// its value, sorted by name.
func (v *validator) enabledContainers(ctx context.Context, engine docker.Engine) ([]docker.Container, error) {
	ctx, cancel := context.WithTimeout(ctx, dockerTimeout)
	defer cancel()

	all, err := engine.Containers(ctx)
	if err != nil {
		return nil, err
	}

	enable := labels.Name(v.prefix, labels.KeyEnable)

	var out []docker.Container

	for _, c := range all {
		if _, ok := c.Labels[enable]; ok {
			out = append(out, c)
		}
	}

	slices.SortFunc(out, func(a, b docker.Container) int {
		return cmp.Or(strings.Compare(a.Name, b.Name), strings.Compare(a.ID, b.ID))
	})

	return out, nil
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}

	return id
}

// report is the result of checking one set of labels.
type report struct {
	spec     labels.ContainerSpec
	issues   labels.Issues
	sources  []sourceReport
	noLabels bool
	// server is true if kopia server checked the sources.
	server bool
}

// sourceReport is what is known about one path of the path label.
type sourceReport struct {
	// label is the path as written in the label.
	label string
	// path is the label path under HOST_PREFIX.
	path string
	// source is the source after kopia server normalized path; empty if it
	// was not asked or failed.
	source kopiaclient.SourceInfo
	// upcoming are the next snapshot times kopia server computed; valid
	// only if scheduled is true.
	upcoming  []time.Time
	scheduled bool
}

// check parses the labels and checks the paths and, with kopia server, the
// schedule.
func (v *validator) check(ctx context.Context, input map[string]string) report {
	v.checked++

	spec, issues := labels.Parse(input, labels.Options{Prefix: v.prefix, Strict: v.strict})
	r := report{spec: spec, issues: issues, noLabels: !hasPrefixed(input, v.prefix), server: v.kopia != nil}
	pathLabel := labels.Name(v.prefix, labels.KeyPath)

	for _, p := range spec.Paths {
		sp, err := labels.SourcePath(v.hostPrefix, p)
		if err != nil {
			r.addError(pathLabel, err.Error())
			continue
		}

		src := sourceReport{label: p, path: sp}

		if v.kopia != nil {
			v.checkSource(ctx, &r, &src)
		}

		r.sources = append(r.sources, src)
	}

	return r
}

// checkSource asks kopia server to normalize the path of src and to compute
// its next snapshot times with the schedule of the labels.
func (v *validator) checkSource(ctx context.Context, r *report, src *sourceReport) {
	pathLabel := labels.Name(v.prefix, labels.KeyPath)

	source, err := v.kopia.ResolvePath(ctx, src.path)
	if err != nil {
		r.addError(pathLabel, fmt.Sprintf("%s: %v", src.path, err))
		return
	}

	src.source = source

	stored, err := v.kopia.GetPolicy(ctx, source)

	switch {
	case errors.Is(err, kopiaclient.ErrNotFound):
		stored = json.RawMessage(`{}`)
	case err != nil:
		r.addError(pathLabel, fmt.Sprintf("%s: %v", source.Path, err))
		return
	}

	updates, err := previewPolicy(stored, r.spec.Policy.Scheduling.Cron)
	if err != nil {
		r.addError(pathLabel, fmt.Sprintf("%s: the stored policy: %v", source.Path, err))
		return
	}

	res, err := v.kopia.ResolvePolicy(ctx, source, kopiaclient.ResolvePolicyRequest{
		Updates:                  updates,
		NumUpcomingSnapshotTimes: upcomingSnapshots,
	})
	if err != nil {
		r.addError(pathLabel, fmt.Sprintf("%s: %v", source.Path, err))
		return
	}

	if res.SchedulingError != "" {
		r.addError(labels.Name(v.prefix, labels.KeySchedule), "kopia server rejects the schedule: "+res.SchedulingError)
		return
	}

	src.upcoming = res.UpcomingSnapshotTimes
	src.scheduled = true
}

// addError adds an error unless the same one is there already: kopia
// server finds the same schedule error for every path.
func (r *report) addError(label, msg string) {
	is := labels.Issue{Label: label, Severity: labels.Error, Message: msg}
	if !slices.Contains(r.issues, is) {
		r.issues = append(r.issues, is)
	}
}

// previewPolicy returns the policy stored for a path with the schedule of
// the labels in place, as the watcher would apply it to a running
// container: the cron expressions replaced, or removed for an empty
// schedule, and no manual flag, which kopia does not accept together with
// cron. Every other field is kept as stored.
func previewPolicy(stored json.RawMessage, cron []string) (json.RawMessage, error) {
	var pol map[string]json.RawMessage
	if err := json.Unmarshal(stored, &pol); err != nil {
		return nil, errors.New("not a JSON object")
	}

	if pol == nil {
		pol = map[string]json.RawMessage{}
	}

	var sched map[string]json.RawMessage

	if raw, ok := pol["scheduling"]; ok {
		if err := json.Unmarshal(raw, &sched); err != nil {
			return nil, errors.New("scheduling is not a JSON object")
		}
	}

	if sched == nil {
		sched = map[string]json.RawMessage{}
	}

	delete(sched, "manual")
	delete(sched, "cron")

	if len(cron) > 0 {
		b, err := json.Marshal(cron)
		if err != nil {
			return nil, err
		}

		sched["cron"] = b
	}

	delete(pol, "scheduling")

	if len(sched) > 0 {
		b, err := json.Marshal(sched)
		if err != nil {
			return nil, err
		}

		pol["scheduling"] = b
	}

	return json.Marshal(pol)
}

// print writes the report: the parsed labels, the sources and the issues.
// where completes "no kopia.* labels ...".
func (r report) print(w io.Writer, prefix, where string) {
	printSpec(w, prefix, r.spec)

	if r.noLabels {
		_, _ = fmt.Fprintf(w, "\nno %s.* labels %s\n", prefix, where)
	}

	if len(r.sources) > 0 {
		_, _ = fmt.Fprintln(w, "\nsources:")
	}

	for _, s := range r.sources {
		switch {
		case s.source.Path != "":
			_, _ = fmt.Fprintf(w, "  %s -> %s\n", printable(s.label), printable(s.source.String()))
		default:
			_, _ = fmt.Fprintf(w, "  %s -> %s\n", printable(s.label), printable(s.path))
		}

		if !s.scheduled {
			continue
		}

		if len(s.upcoming) == 0 {
			_, _ = fmt.Fprintln(w, "    next snapshots: none, the effective policy has no schedule")
			continue
		}

		times := make([]string, 0, len(s.upcoming))
		for _, t := range s.upcoming {
			times = append(times, t.Format(timeLayout))
		}

		_, _ = fmt.Fprintf(w, "    next snapshots (kopia server time): %s\n", strings.Join(times, ", "))
	}

	if len(r.issues) > 0 {
		_, _ = fmt.Fprintln(w)
	}

	for _, is := range r.issues {
		_, _ = fmt.Fprintf(w, "%s: %s: %s\n", is.Severity, printable(is.Label), printable(is.Message))
	}
}

// printFooter says what was not checked.
func (v *validator) printFooter(w io.Writer) {
	if v.checked == 0 {
		return
	}

	if v.kopia == nil {
		_, _ = fmt.Fprintf(w, "\nkopia server checks skipped: %s is not set, so cron expressions and the paths as kopia server sees them were not checked\n", config.EnvKopiaServerURL)
		return
	}

	_, _ = fmt.Fprintln(w, "\nwhether the paths exist is not checked: kopia server checks that only when it registers a source")
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

	// bufio.ScanLines drops the \r of CRLF line ends.
	for n := 1; sc.Scan(); n++ {
		line := sc.Text()
		if n == 1 {
			line = strings.TrimPrefix(line, "\ufeff") // byte order mark, as docker --label-file
		}

		line = strings.TrimLeftFunc(line, unicode.IsSpace)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if err := addLabel(out, line); err != nil {
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
	case key == "" || strings.ContainsFunc(key, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }):
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
	row := func(key, value string) {
		_, _ = fmt.Fprintf(tw, "%s\t%s\n", labels.Name(prefix, key), printable(value))
	}

	row(labels.KeyEnable, strconv.FormatBool(spec.Enable))
	row(labels.KeyPath, orNone(strings.Join(spec.Paths, ", "), "(none)"))
	row(labels.KeySchedule, orNone(strings.Join(spec.Policy.Scheduling.Cron, "; "), "(inherit from the parent policies)"))
	row(labels.KeySnapshotOnStart, strconv.FormatBool(spec.SnapshotOnStart))
	row(labels.KeySnapshotOnStop, strconv.FormatBool(spec.SnapshotOnStop))
	row(labels.KeyMinInterval, minInterval(spec.MinInterval))
	row(labels.KeyDescription, orNone(spec.Description, "(container name)"))

	tags := make([]string, 0, len(spec.Tags))
	for _, t := range spec.Tags {
		tags = append(tags, t.Key+":"+t.Value)
	}

	row(labels.KeyTags, orNone(strings.Join(tags, ", "), "(none)"))

	_ = tw.Flush()
}

// printable quotes a value with control characters, so that a newline or a
// tab in a label value cannot break the table or fake an issue line.
func printable(s string) string {
	if strings.ContainsFunc(s, unicode.IsControl) {
		return strconv.Quote(s)
	}

	return s
}

// minInterval formats the min-interval label's value, e.g. 10m0s.
func minInterval(d time.Duration) string {
	if d == 0 {
		return "0 (no minimum)"
	}

	return d.String()
}

func orNone(s, none string) string {
	if s == "" {
		return none
	}

	return s
}
