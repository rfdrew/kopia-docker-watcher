package labels

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestParsePinOnStop(t *testing.T) {
	const hint = "; " + pinHint

	onStop := func(pin string) map[string]string {
		return map[string]string{"kopia.snapshot-on-stop": "true", "kopia.pin-on-stop": pin}
	}

	tests := []struct {
		name   string
		labels map[string]string
		pin    string
		issues []string
	}{
		{name: "default"},
		{name: "name", labels: onStop("before-update"), pin: "before-update"},
		{name: "trimmed", labels: onStop(" keep "), pin: "keep"},
		{name: "placeholders", labels: onStop("update-{date}T{time}"), pin: "update-{date}T{time}"},
		{name: "punctuation and letters", labels: onStop("v1.2_ünd:@+/#"), pin: "v1.2_ünd:@+/#"},
		{name: "longest", labels: onStop(strings.Repeat("a", MaxPinLength-10) + "{date}"), pin: strings.Repeat("a", MaxPinLength-10) + "{date}"},
		{
			name:   "too long",
			labels: onStop(strings.Repeat("a", MaxPinLength-9) + "{date}"),
			issues: []string{`warning kopia.pin-on-stop: invalid pin "` + strings.Repeat("a", MaxPinLength-9) + `{date}": it is 129 characters long, ` +
				"at most 128 are allowed, no pin is added" + hint},
		},
		{
			name:   "empty",
			labels: onStop(" "),
			issues: []string{"warning kopia.pin-on-stop: empty value, no pin is added" + hint},
		},
		{
			name:   "space",
			labels: onStop("before update"),
			issues: []string{`warning kopia.pin-on-stop: invalid pin "before update": it contains whitespace, no pin is added` + hint},
		},
		{
			name:   "control character",
			labels: onStop("a\tb"),
			issues: []string{`warning kopia.pin-on-stop: invalid pin "a\tb": it contains whitespace, no pin is added` + hint},
		},
		{
			name:   "unprintable",
			labels: onStop("a\u200bb"),
			issues: []string{`warning kopia.pin-on-stop: invalid pin "a\u200bb": it contains the character U+200B, no pin is added` + hint},
		},
		{
			name:   "unknown placeholder",
			labels: onStop("update-{data}"),
			issues: []string{`warning kopia.pin-on-stop: invalid pin "update-{data}": braces are only allowed in {date} and {time}, no pin is added` + hint},
		},
		{
			name:   "without snapshot-on-stop",
			labels: map[string]string{"kopia.pin-on-stop": "keep"},
			pin:    "keep",
			issues: []string{"warning kopia.pin-on-stop: has no effect unless kopia.snapshot-on-stop is true"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := spec(func(s *ContainerSpec) {
				s.SnapshotOnStop = tt.labels["kopia.snapshot-on-stop"] == "true"
				s.PinOnStop = tt.pin
			})

			checkParse(t, tt.labels, Options{}, want, tt.issues)
		})
	}

	t.Run("strict", func(t *testing.T) {
		_, issues := Parse(onStop("a b"), Options{Strict: true})
		if got := issueStrings(issues); !slices.Equal(got, []string{`error kopia.pin-on-stop: invalid pin "a b": it contains whitespace` + hint}) {
			t.Errorf("issues = %q", got)
		}
	})
}

func TestPinName(t *testing.T) {
	at := time.Date(2026, 10, 23, 23, 5, 9, 0, time.FixedZone("CEST", 2*60*60))

	for template, want := range map[string]string{
		"keep":                        "keep",
		"before-update-{date}":        "before-update-2026-10-23",
		"{date}T{time}":               "2026-10-23T21-05-09",
		"{time}-{time}":               "21-05-09-21-05-09",
		"update-{date}-{time}-{date}": "update-2026-10-23-21-05-09-2026-10-23",
	} {
		if got := PinName(template, at); got != want {
			t.Errorf("PinName(%q) = %q, want %q", template, got, want)
		}
	}
}
