package labels

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
)

// Label keys of the policy fields, relative to the prefix. Each sets one
// field of the path's Kopia policy; an empty value leaves the field to
// inheritance, like an absent label.
const (
	KeyInterval  = "interval"
	KeyTimeOfDay = "time-of-day"
	KeyRunMissed = "run-missed"

	KeyRetentionLatest          = "retention.latest"
	KeyRetentionHourly          = "retention.hourly"
	KeyRetentionDaily           = "retention.daily"
	KeyRetentionWeekly          = "retention.weekly"
	KeyRetentionMonthly         = "retention.monthly"
	KeyRetentionAnnual          = "retention.annual"
	KeyRetentionIgnoreIdentical = "retention.ignore-identical"

	KeyIgnore          = "ignore"
	KeyIgnoreFiles     = "ignore-files"
	KeyIgnoreCacheDirs = "ignore-cache-dirs"
	KeyMaxFileSize     = "max-file-size"
	KeyOneFileSystem   = "one-file-system"

	KeyIgnoreFileErrors   = "ignore-file-errors"
	KeyIgnoreDirErrors    = "ignore-dir-errors"
	KeyIgnoreUnknownTypes = "ignore-unknown-types"

	KeyCompressionAlgorithm = "compression.algorithm"
	KeyCompressionMinSize   = "compression.min-size"
	KeyCompressionMaxSize   = "compression.max-size"
	KeyCompressionOnly      = "compression.only"
	KeyCompressionNever     = "compression.never"

	KeyUploadParallelFiles = "upload.parallel-files"

	KeyPolicyJSON = "policy-json"
)

// MinInterval is the shortest interval label value. cron, the finest
// schedule kopia server has, runs at most once a minute; a shorter
// interval would start a snapshot as soon as the last one ends.
const MinInterval = time.Minute

// NoCompression is the compression algorithm that turns compression off.
// kopia server accepts it in a policy but does not list it among its
// algorithms.
const NoCompression = "none"

// maxCount bounds counts: kopia keeps them as 32-bit integers.
const maxCount = math.MaxInt32

// policyKeys are the labels of the policy fields.
var policyKeys = []key{
	{KeyInterval, (*parser).interval},
	{KeyTimeOfDay, (*parser).timeOfDay},
	{KeyRunMissed, func(p *parser, v string) { p.spec.Policy.Scheduling.RunMissed = p.optBool(v) }},

	{KeyRetentionLatest, func(p *parser, v string) { p.spec.Policy.Retention.KeepLatest = p.count(v, 0) }},
	{KeyRetentionHourly, func(p *parser, v string) { p.spec.Policy.Retention.KeepHourly = p.count(v, 0) }},
	{KeyRetentionDaily, func(p *parser, v string) { p.spec.Policy.Retention.KeepDaily = p.count(v, 0) }},
	{KeyRetentionWeekly, func(p *parser, v string) { p.spec.Policy.Retention.KeepWeekly = p.count(v, 0) }},
	{KeyRetentionMonthly, func(p *parser, v string) { p.spec.Policy.Retention.KeepMonthly = p.count(v, 0) }},
	{KeyRetentionAnnual, func(p *parser, v string) { p.spec.Policy.Retention.KeepAnnual = p.count(v, 0) }},
	{KeyRetentionIgnoreIdentical, func(p *parser, v string) { p.spec.Policy.Retention.IgnoreIdenticalSnapshots = p.optBool(v) }},

	{KeyIgnore, func(p *parser, v string) { p.spec.Policy.Files.Ignore = p.list(v, nil) }},
	{KeyIgnoreFiles, func(p *parser, v string) { p.spec.Policy.Files.IgnoreDotFiles = p.list(v, checkFileName) }},
	{KeyIgnoreCacheDirs, func(p *parser, v string) { p.spec.Policy.Files.IgnoreCacheDirs = p.optBool(v) }},
	{KeyMaxFileSize, func(p *parser, v string) { p.spec.Policy.Files.MaxFileSize = p.size(v) }},
	{KeyOneFileSystem, func(p *parser, v string) { p.spec.Policy.Files.OneFileSystem = p.optBool(v) }},

	{KeyIgnoreFileErrors, func(p *parser, v string) { p.spec.Policy.ErrorHandling.IgnoreFileErrors = p.optBool(v) }},
	{KeyIgnoreDirErrors, func(p *parser, v string) { p.spec.Policy.ErrorHandling.IgnoreDirectoryErrors = p.optBool(v) }},
	{KeyIgnoreUnknownTypes, func(p *parser, v string) { p.spec.Policy.ErrorHandling.IgnoreUnknownTypes = p.optBool(v) }},

	{KeyCompressionAlgorithm, (*parser).compressor},
	{KeyCompressionMinSize, func(p *parser, v string) { p.spec.Policy.Compression.MinSize = p.size(v) }},
	{KeyCompressionMaxSize, func(p *parser, v string) { p.spec.Policy.Compression.MaxSize = p.size(v) }},
	{KeyCompressionOnly, func(p *parser, v string) { p.spec.Policy.Compression.OnlyCompress = p.list(v, checkExtension) }},
	{KeyCompressionNever, func(p *parser, v string) { p.spec.Policy.Compression.NeverCompress = p.list(v, checkExtension) }},

	{KeyUploadParallelFiles, func(p *parser, v string) { p.spec.Policy.Upload.MaxParallelFileReads = p.count(v, 1) }},

	{KeyPolicyJSON, (*parser).policyJSON},
}

// optBool parses a boolean policy field: empty means inherit, anything
// else is read as the other boolean labels are.
func (p *parser) optBool(v string) *bool {
	if strings.TrimSpace(v) == "" {
		return nil
	}

	b := p.bool(v)

	return &b
}

// count parses a whole number of at least lowest; empty means inherit.
func (p *parser) count(v string, lowest int) *int {
	s := strings.TrimSpace(v)
	if s == "" {
		return nil
	}

	n, err := strconv.Atoi(s)

	switch {
	case err != nil && errors.Is(err, strconv.ErrRange):
		p.errorf("%q is too large; the most is %d", v, maxCount)
	case err != nil:
		p.errorf("%q is not a whole number", v)
	case n < lowest:
		p.errorf("%q is less than %d", v, lowest)
	case n > maxCount:
		p.errorf("%q is too large; the most is %d", v, maxCount)
	default:
		return &n
	}

	return nil
}

// sizeHint names the accepted values of the size labels.
const sizeHint = "use a number of bytes with an optional unit, such as 500MB or 1GiB " +
	"(B, KB, MB, GB, TB in powers of 1000, KiB, MiB, GiB, TiB in powers of 1024)"

var (
	sizePattern = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]*)$`)

	sizeUnits = map[string]int64{
		"": 1, "b": 1,
		"kb": 1e3, "mb": 1e6, "gb": 1e9, "tb": 1e12,
		"kib": 1 << 10, "mib": 1 << 20, "gib": 1 << 30, "tib": 1 << 40,
	}
)

// size parses a size in bytes with an optional unit; empty means inherit.
// kopia drops a size of zero from a policy, so zero is an error rather
// than a value that quietly inherits.
func (p *parser) size(v string) int64 {
	s := strings.TrimSpace(v)
	if s == "" {
		return 0
	}

	m := sizePattern.FindStringSubmatch(s)
	if m == nil {
		p.errorf("%q is not a size; %s", v, sizeHint)
		return 0
	}

	unit, ok := sizeUnits[strings.ToLower(m[2])]
	if !ok {
		p.errorf("%q has an unknown unit %q; %s", v, m[2], sizeHint)
		return 0
	}

	n, ok := new(big.Rat).SetString(m[1])
	if !ok {
		p.errorf("%q is not a size; %s", v, sizeHint)
		return 0
	}

	n.Mul(n, new(big.Rat).SetInt64(unit))

	switch {
	case !n.IsInt():
		p.errorf("%q is not a whole number of bytes", v)
	case n.Sign() == 0:
		p.errorf("%q is zero, which kopia does not store; leave the label empty to inherit the size", v)
	case !n.Num().IsInt64():
		p.errorf("%q is too large", v)
	default:
		return n.Num().Int64()
	}

	return 0
}

// list parses a comma-separated list; items are trimmed, empty ones
// skipped and repeated ones dropped with a warning. No item means
// inherit. check, if not nil, reports a bad item as an error.
func (p *parser) list(v string, check func(string) string) []string {
	var out []string

	for item := range strings.SplitSeq(v, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		if check != nil {
			if msg := check(item); msg != "" {
				p.errorf("%s", msg)
				continue
			}
		}

		if slices.Contains(out, item) {
			p.warn(fmt.Sprintf("%q is listed more than once", item), "", "")
			continue
		}

		out = append(out, item)
	}

	return out
}

// checkFileName reports a name that cannot be the name of an ignore file
// in a directory.
func checkFileName(name string) string {
	if strings.ContainsAny(name, `/\`) {
		return fmt.Sprintf("%q is not a file name: kopia looks for a file of this name in every directory", name)
	}

	return ""
}

// checkExtension reports an item that cannot match a file extension:
// kopia compares the extension with the dot, as filepath.Ext returns it.
func checkExtension(ext string) string {
	if !strings.HasPrefix(ext, ".") || len(ext) == 1 || strings.ContainsAny(ext[1:], `./\*?`) {
		return fmt.Sprintf("%q is not a file extension; use the extension with its dot, such as .txt", ext)
	}

	return ""
}

// intervalHint names the accepted values of the interval label.
const intervalHint = "use a Go duration such as 30m, 6h or 24h (d is not a unit) of at least 1m"

// interval parses a Go duration of whole seconds and at least MinInterval;
// empty means inherit.
func (p *parser) interval(v string) {
	p.spec.Policy.Scheduling.IntervalSeconds = 0

	s := strings.TrimSpace(v)
	if s == "" {
		return
	}

	d, err := time.ParseDuration(s)

	switch {
	case err != nil:
		p.errorf("invalid duration %q; %s", v, intervalHint)
	case d < MinInterval:
		p.errorf("%q is shorter than %s; %s", v, MinInterval, intervalHint)
	case d%time.Second != 0:
		p.errorf("%q is not a whole number of seconds", v)
	default:
		p.spec.Policy.Scheduling.IntervalSeconds = int64(d / time.Second)
		p.setsSchedule = true
	}
}

var timeOfDayPattern = regexp.MustCompile(`^([0-9]{1,2}):([0-9]{2})$`)

// timeOfDay parses comma-separated HH:MM times in 24-hour time, sorted
// and each once; none means inherit.
func (p *parser) timeOfDay(v string) {
	var out []backend.TimeOfDay

	for item := range strings.SplitSeq(v, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		m := timeOfDayPattern.FindStringSubmatch(item)
		if m == nil {
			p.errorf("%q is not a time of day; use HH:MM in 24-hour time, such as 02:30", item)
			continue
		}

		h, _ := strconv.Atoi(m[1])
		mi, _ := strconv.Atoi(m[2])

		if h > 23 || mi > 59 {
			p.errorf("%q is not a time of day: the hour goes up to 23 and the minute up to 59", item)
			continue
		}

		t := backend.TimeOfDay{Hour: h, Minute: mi}
		if slices.Contains(out, t) {
			p.warn(fmt.Sprintf("%q is listed more than once", item), "", "")
			continue
		}

		out = append(out, t)
	}

	slices.SortFunc(out, func(a, b backend.TimeOfDay) int { return (a.Hour*60 + a.Minute) - (b.Hour*60 + b.Minute) })

	p.spec.Policy.Scheduling.TimeOfDay = out
	if len(out) > 0 {
		p.setsSchedule = true
	}
}

// compressor takes the name of a compression algorithm; empty means
// inherit. Only kopia server knows which algorithms it has: the reconciler
// and validate check the name against its list.
func (p *parser) compressor(v string) {
	s := strings.TrimSpace(v)
	p.spec.Policy.Compression.CompressorName = ""

	if strings.ContainsFunc(s, func(r rune) bool { return r <= ' ' || r == 0x7f }) {
		p.errorf("%q is not the name of a compression algorithm", v)
		return
	}

	p.spec.Policy.Compression.CompressorName = s
}

// policyJSON takes a JSON object merged into the policy after the fields
// of the other labels; empty means none. The object must not set
// scheduling.manual, since the watcher pauses a source itself, or
// actions, commands that kopia server would run.
func (p *parser) policyJSON(v string) {
	p.spec.Policy.Raw = nil

	s := strings.TrimSpace(v)
	if s == "" {
		return
	}

	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()

	var obj map[string]any
	if err := dec.Decode(&obj); err != nil || obj == nil {
		p.errorf("not a JSON object: %s", jsonError(s, err))
		return
	}

	if dec.More() {
		p.errorf("not a JSON object: more than one JSON value")
		return
	}

	if _, ok := obj["actions"]; ok {
		p.errorf("must not set actions: they run commands on kopia server, which has to allow them; set them on kopia server instead")
		return
	}

	if sched, ok := obj["scheduling"].(map[string]any); ok {
		if _, ok := sched["manual"]; ok {
			p.errorf("must not set scheduling.manual: the watcher pauses the source when all its containers are stopped")
			return
		}

		for _, k := range []string{"cron", "intervalSeconds", "timeOfDay"} {
			if _, ok := sched[k]; ok {
				p.setsSchedule = true
			}
		}
	}

	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(s)); err != nil {
		p.errorf("not a JSON object: %v", err)
		return
	}

	p.spec.Policy.Raw = json.RawMessage(buf.Bytes())
}

// jsonError describes why s does not decode as a JSON object.
func jsonError(s string, err error) string {
	var se *json.SyntaxError

	switch {
	case err == nil:
		return "null"
	case errors.As(err, &se):
		return fmt.Sprintf("%v at offset %d", err, se.Offset)
	case strings.HasPrefix(s, "{"):
		return err.Error()
	default:
		return "it is not in braces"
	}
}

// checkPolicy reports what the policy labels get wrong together, after
// every label is parsed.
func (p *parser) checkPolicy() {
	c := p.spec.Policy.Compression
	if c.MinSize > 0 && c.MaxSize > 0 && c.MinSize > c.MaxSize {
		p.label = Name(p.prefix, KeyCompressionMaxSize)
		p.errorf("is less than %s, so no file would be compressed", Name(p.prefix, KeyCompressionMinSize))
	}

	if err := p.spec.Policy.Validate(); err != nil {
		// Only policy-json can clash with the other fields.
		var be *backend.Error
		if errors.As(err, &be) {
			err = be.Err
		}

		p.label = Name(p.prefix, KeyPolicyJSON)
		p.errorf("%v", err)
	}
}

// Compressor returns the compression algorithm the policy fields ask for
// and the key of the label that asks: policy-json wins over
// compression.algorithm, as it is merged last. The name is empty when
// neither sets one as a string.
func Compressor(pol backend.PolicyPatch) (name, key string) {
	var raw struct {
		Compression struct {
			CompressorName *string `json:"compressorName"`
		} `json:"compression"`
	}

	if len(pol.Raw) > 0 && json.Unmarshal(pol.Raw, &raw) == nil && raw.Compression.CompressorName != nil {
		return *raw.Compression.CompressorName, KeyPolicyJSON
	}

	return pol.Compression.CompressorName, KeyCompressionAlgorithm
}

// CheckCompressor checks the compression algorithm the policy fields ask
// for against the algorithms kopia server supports: an algorithm it lacks
// is an error, a deprecated one a warning. NoCompression is always
// supported.
func CheckCompressor(prefix string, pol backend.PolicyPatch, algos []backend.Algorithm) Issues {
	name, key := Compressor(pol)
	if name == "" || name == NoCompression {
		return nil
	}

	label := Name(prefix, key)

	if i := slices.IndexFunc(algos, func(a backend.Algorithm) bool { return a.Name == name }); i >= 0 {
		if !algos[i].Deprecated {
			return nil
		}

		return Issues{{Label: label, Severity: Warning,
			Message: fmt.Sprintf("compression algorithm %q is deprecated by kopia; it still works", name)}}
	}

	msg := fmt.Sprintf("kopia server does not support the compression algorithm %q", name)

	if i := slices.IndexFunc(algos, func(a backend.Algorithm) bool { return strings.EqualFold(a.Name, name) }); i >= 0 {
		msg += "; did you mean " + algos[i].Name + "?"
	} else {
		names := make([]string, 0, len(algos)+1)
		for _, a := range algos {
			if !a.Deprecated {
				names = append(names, a.Name)
			}
		}

		msg += "; use one of " + strings.Join(append(names, NoCompression), ", ")
	}

	return Issues{{Label: label, Severity: Error, Message: msg}}
}
