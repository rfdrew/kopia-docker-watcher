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

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
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

Each path of the path label, and for a container the source of each mount
the volumes label picks, is put under HOST_PREFIX and must then be
absolute; the sources list says which mount each source comes from. With KOPIA_SERVER_URL set, kopia server checks more, read-only:
it normalizes every path (paths/resolve), computes the next snapshot times
of the path's stored policy with the policy fields of the labels in place
(policy/resolve), in its own time zone, which rejects a bad cron
expression, and lists the compression algorithms it supports
(repo/algorithms). Whether a path exists is not checked: only registering
a source does that. Without KOPIA_SERVER_URL only the labels are checked.

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
		r := v.check(ctx, input, nil)
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
	prefix        string
	hostPrefix    string
	watcherPrefix string
	strict        bool
	// kopia is nil without KOPIA_SERVER_URL: only the labels are checked.
	kopia *kopiaclient.Client
	// algorithms are the compression algorithms of kopia server, asked
	// for the first label set that names one.
	algorithms []backend.Algorithm
	// checked counts the label sets checked, for the footer.
	checked int
}

// newValidator reads the configuration validate needs. Only LABEL_PREFIX,
// HOST_PREFIX and WATCHER_PATH_PREFIX matter without KOPIA_SERVER_URL; with it, the whole
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

		if v.watcherPrefix, err = config.ParseWatcherPathPrefix(d.getenv(config.EnvWatcherPathPrefix)); err != nil {
			_, _ = fmt.Fprintf(d.stderr, "validate: %s: %v\n", config.EnvWatcherPathPrefix, err)
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
	v.watcherPrefix = cfg.WatcherPathPrefix

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

	// The names of depends-on labels are looked up among all containers,
	// listed once.
	var dependsOn map[string][]string

	report := func(c docker.Container) {
		begin()
		_, _ = fmt.Fprintf(d.stdout, "container %s (%s, %s)\n", printable(c.Name), shortID(c.ID), printable(string(c.State)))

		r := v.check(ctx, c.Labels, &c)

		if len(labels.DependsOn(c.Labels, v.prefix)) > 0 {
			if dependsOn == nil {
				listing, err := listContainers(ctx, engine)
				if err != nil {
					listing = nil
					r.addError(labels.Name(v.prefix, labels.KeyDependsOn), "cannot list the containers to check the names: "+err.Error())
				}

				dependsOn = reconcile.DependsOnProblems(listing, v.prefix)
			}

			for _, p := range dependsOn[c.ID] {
				r.addError(labels.Name(v.prefix, labels.KeyDependsOn), p)
			}
		}

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

// listContainers lists all containers within dockerTimeout.
func listContainers(ctx context.Context, engine docker.Engine) ([]docker.Container, error) {
	ctx, cancel := context.WithTimeout(ctx, dockerTimeout)
	defer cancel()

	return engine.Containers(ctx)
}

// enabledContainers lists the containers with the enable label, whatever
// its value, sorted by name.
func (v *validator) enabledContainers(ctx context.Context, engine docker.Engine) ([]docker.Container, error) {
	all, err := listContainers(ctx, engine)
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
	// noMounts is true if the volumes label is set but no container was
	// given, so its mounts are unknown.
	noMounts bool
}

// sourceReport is what is known about one path of the path label.
type sourceReport struct {
	// key is the label the path comes from: the path or the volumes label.
	key string
	// label is the path as written in the label, or the mount the volumes
	// label picks.
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
// schedule. c is the container the labels are of, whose mounts the volumes
// label picks from; nil for labels given on their own.
func (v *validator) check(ctx context.Context, input map[string]string, c *docker.Container) report {
	v.checked++

	opts := labels.Options{Prefix: v.prefix, Strict: v.strict}
	spec, issues := labels.Parse(input, opts)
	r := report{spec: spec, issues: issues, noLabels: !hasPrefixed(input, v.prefix), server: v.kopia != nil}

	if v.kopia != nil {
		v.checkCompressor(ctx, &r)
	}

	add := func(key, label, p string) {
		sp, err := labels.SourcePathWithin(v.hostPrefix, v.watcherPrefix, p)
		if err != nil {
			r.addError(labels.Name(v.prefix, key), err.Error())
			return
		}

		if slices.ContainsFunc(r.sources, func(s sourceReport) bool { return s.path == sp }) {
			return
		}

		src := sourceReport{key: key, label: label, path: sp}

		if v.kopia != nil {
			v.checkSource(ctx, &r, &src)
		}

		r.sources = append(r.sources, src)
	}

	for _, p := range spec.Paths {
		add(labels.KeyPath, p, p)
	}

	if len(spec.Volumes) > 0 && c == nil {
		r.noMounts = true
		return r
	}

	if c == nil {
		return r
	}

	vols, volIssues := labels.VolumePaths(spec, *c, opts)
	if spec.Enable {
		r.issues = append(r.issues, volIssues...)
		slices.SortStableFunc(r.issues, func(a, b labels.Issue) int { return strings.Compare(a.Label, b.Label) })
	}

	for _, vp := range vols {
		add(labels.KeyVolumes, vp.String(), vp.Path)
	}

	return r
}

// checkCompressor checks the compression algorithm of the labels against
// the ones kopia server supports.
func (v *validator) checkCompressor(ctx context.Context, r *report) {
	name, key := labels.Compressor(r.spec.Policy)
	if name == "" || name == labels.NoCompression {
		return
	}

	if v.algorithms == nil {
		res, err := v.kopia.SupportedAlgorithms(ctx)
		if err != nil {
			r.addError(labels.Name(v.prefix, key), fmt.Sprintf("cannot list the compression algorithms of kopia server: %v", err))
			return
		}

		for _, a := range res.Compression {
			v.algorithms = append(v.algorithms, backend.Algorithm{Name: a.ID, Deprecated: a.Deprecated})
		}

		slices.SortFunc(v.algorithms, func(a, b backend.Algorithm) int { return strings.Compare(a.Name, b.Name) })
	}

	for _, is := range labels.CheckCompressor(v.prefix, r.spec.Policy, v.algorithms, v.strict) {
		if !slices.Contains(r.issues, is) {
			r.issues = append(r.issues, is)
		}
	}
}

// checkSource asks kopia server to normalize the path of src and to compute
// its next snapshot times with the schedule of the labels.
func (v *validator) checkSource(ctx context.Context, r *report, src *sourceReport) {
	pathLabel := labels.Name(v.prefix, src.key)

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

	if r.spec.Policy.Validate() != nil {
		// The labels report it; there is no policy to preview.
		return
	}

	updates, err := previewPolicy(stored, r.spec.Policy)
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
		r.addError(labels.Name(v.prefix, labels.ScheduleKey(r.spec.Policy)), "kopia server rejects the schedule: "+res.SchedulingError)
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

// previewPolicy returns the policy stored for a path with the policy
// fields of the labels merged in, as the watcher would apply them to a
// running container: no manual flag, which kopia does not accept together
// with a schedule, the cron expressions of the stored policy replaced or,
// for an empty schedule, removed, and the other fields of the labels set.
// Every other field is kept as stored. validate has no state, so a field a
// removed label left behind is kept too.
func previewPolicy(stored json.RawMessage, patch backend.PolicyPatch) (json.RawMessage, error) {
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

	delete(pol, "scheduling")
	delete(sched, "manual")

	if len(sched) > 0 {
		b, err := json.Marshal(sched)
		if err != nil {
			return nil, err
		}

		pol["scheduling"] = b
	}

	current, err := json.Marshal(pol)
	if err != nil {
		return nil, err
	}

	// The schedule label is always the watcher's: with the cron of the
	// last patch the merge removes a stored cron the labels leave to
	// inheritance.
	last := backend.Applied{Patch: backend.PolicyPatch{Scheduling: backend.SchedulingPatch{Cron: []string{labels.DefaultSchedule}}}}

	m, err := backend.Merge(current, backend.Desired{Patch: patch}, last)
	if err != nil {
		return nil, err
	}

	return m.Unpaused, nil
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

	if r.noMounts {
		_, _ = fmt.Fprintf(w, "\n%s: the paths of the mounts are only known for a container; use --container or --all\n",
			labels.Name(prefix, labels.KeyVolumes))
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
		_, _ = fmt.Fprintf(w, "\nkopia server checks skipped: %s is not set, so schedules, the compression algorithm and the paths as kopia server sees them were not checked\n", config.EnvKopiaServerURL)
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
	if len(spec.Volumes) > 0 {
		row(labels.KeyVolumes, strings.Join(spec.Volumes, ", "))
	}

	if len(spec.ExcludeVolumes) > 0 {
		row(labels.KeyExcludeVolumes, strings.Join(spec.ExcludeVolumes, ", "))
	}
	row(labels.KeySchedule, orNone(strings.Join(scheduleOf(spec.Policy), "; "), "(inherit from the parent policies)"))
	row(labels.KeySnapshotOnStart, strconv.FormatBool(spec.SnapshotOnStart))
	row(labels.KeySnapshotOnStop, strconv.FormatBool(spec.SnapshotOnStop))
	row(labels.KeyMinInterval, minInterval(spec.MinInterval))
	row(labels.KeyDescription, orNone(spec.Description, "(container name)"))

	tags := make([]string, 0, len(spec.Tags))
	for _, t := range spec.Tags {
		tags = append(tags, t.Key+":"+t.Value)
	}

	row(labels.KeyTags, orNone(strings.Join(tags, ", "), "(none)"))
	row(labels.KeyCleanupOnRemove, string(spec.CleanupOnRemove))
	row(labels.KeyCleanupDeletePinned, strconv.FormatBool(spec.CleanupDeletePinned))

	if spec.Hooks {
		row(labels.KeyPreExec, hookCommand(spec.PreExec))
		row(labels.KeyPostExec, hookCommand(spec.PostExec))
		row(labels.KeyHookTimeout, cmp.Or(spec.HookTimeout, labels.DefaultHookTimeout).String())
		row(labels.KeyStopDuringBackup, strconv.FormatBool(spec.StopDuringBackup))
		row(labels.KeyPauseDuringBackup, strconv.FormatBool(spec.PauseDuringBackup))
		row(labels.KeyMaxDowntime, cmp.Or(spec.MaxDowntime, labels.DefaultMaxDowntime).String())
	}

	if len(spec.DependsOn) > 0 {
		row(labels.KeyDependsOn, strings.Join(spec.DependsOn, ", "))
	}

	_ = tw.Flush()

	if spec.Hooks {
		_, _ = fmt.Fprintln(w, "\nhooks: the watcher takes the scheduled snapshots of these sources itself and runs the hooks around them; "+
			"kopia server's schedule of them stays off")
	}

	switch {
	case spec.StopDuringBackup && !spec.PauseDuringBackup:
		_, _ = fmt.Fprintln(w, "downtime: the container, and every running container that depends on it, is stopped for each "+
			"snapshot the watcher takes and started again after it")
	case spec.PauseDuringBackup && !spec.StopDuringBackup:
		_, _ = fmt.Fprintln(w, "downtime: the container is paused for each snapshot the watcher takes and unpaused after it")
	}

	printPolicyFields(w, spec.Policy)
}

// hookCommand shows the arguments of a hook command as a JSON array, with
// <, > and & as they are.
func hookCommand(args []string) string {
	if len(args) == 0 {
		return "(none)"
	}

	var b strings.Builder

	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)

	if err := enc.Encode(args); err != nil {
		return strings.Join(args, " ")
	}

	return strings.TrimSuffix(b.String(), "\n")
}

// scheduleOf returns the cron expressions the policy fields set, those of
// policy-json if it sets them; as written when they are not a list of
// strings.
func scheduleOf(pol backend.PolicyPatch) []string {
	fields, err := pol.Fields()
	if err != nil {
		return pol.Scheduling.Cron
	}

	for _, f := range fields {
		if f.Name() != "scheduling.cron" {
			continue
		}

		var crons []string
		if json.Unmarshal(f.Value, &crons) != nil {
			return []string{string(f.Value)}
		}

		return crons
	}

	return nil
}

// printPolicyFields lists the Kopia policy fields the labels set, but for
// the cron expressions of the schedule row.
func printPolicyFields(w io.Writer, pol backend.PolicyPatch) {
	fields, err := pol.Fields()
	if err != nil {
		// The labels report it.
		return
	}

	fields = slices.DeleteFunc(fields, func(f backend.Field) bool { return f.Name() == "scheduling.cron" })
	if len(fields) == 0 {
		return
	}

	_, _ = fmt.Fprintln(w, "\npolicy fields:")

	for _, f := range fields {
		_, _ = fmt.Fprintf(w, "  %s = %s\n", printable(f.Name()), printable(string(f.Value)))
	}
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
