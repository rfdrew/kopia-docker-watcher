package labels

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// MaxPinLength is the longest pin, in characters, that the pin-on-stop
// label may give once its placeholders are filled in. Kopia itself sets no
// limit on a pin; the watcher keeps pins short enough to read in the web UI
// and to type in kopia snapshot pin --remove.
const MaxPinLength = 128

// Pin placeholders of the pin-on-stop label, filled in with the time of the
// stop in UTC.
const (
	// PinDate is the date of the stop, 2006-01-02.
	PinDate = "{date}"
	// PinTime is the time of the stop, 15-04-05: Kopia allows colons in a
	// pin, but a shell or a file name may not.
	PinTime = "{time}"
)

// pinHint describes the accepted values of the pin-on-stop label.
const pinHint = "use letters, digits and punctuation without whitespace, with " + PinDate + " and " + PinTime +
	" for the time of the stop, such as before-update-" + PinDate

// PinName returns the pin a pin-on-stop template gives for a stop at t:
// the template with its placeholders filled in with t in UTC. The parser
// has checked the template, so the pin is valid.
func PinName(template string, t time.Time) string {
	t = t.UTC()

	return strings.NewReplacer(PinDate, t.Format(time.DateOnly), PinTime, t.Format("15-04-05")).Replace(template)
}

// pinOnStop parses the pin-on-stop label. Kopia accepts any string as a
// pin; the watcher accepts printable characters without spaces, at most
// MaxPinLength of them, and the placeholders PinDate and PinTime. Any other
// brace is a mistake, such as {data}, rather than part of the name. The
// label only adds a pin, so an invalid one is ignored with a warning rather
// than keeping the labels from being applied.
func (p *parser) pinOnStop(v string) {
	p.spec.PinOnStop = ""

	s := strings.TrimSpace(v)
	if s == "" {
		p.warn("empty value", "no pin is added", pinHint)
		return
	}

	if err := checkPin(s); err != nil {
		p.warn(fmt.Sprintf("invalid pin %q: %v", s, err), "no pin is added", pinHint)
		return
	}

	p.spec.PinOnStop = s
}

// checkPin checks a pin-on-stop template.
func checkPin(template string) error {
	rest := strings.NewReplacer(PinDate, "", PinTime, "").Replace(template)

	for _, r := range rest {
		switch {
		case r == '{' || r == '}':
			return fmt.Errorf("braces are only allowed in %s and %s", PinDate, PinTime)
		case unicode.IsSpace(r):
			return errors.New("it contains whitespace")
		case !unicode.IsPrint(r):
			return fmt.Errorf("it contains the character %U", r)
		}
	}

	// The placeholders have fixed lengths, so any time gives the length.
	if n := utf8.RuneCountInString(PinName(template, time.Time{})); n > MaxPinLength {
		return fmt.Errorf("it is %d characters long, at most %d are allowed", n, MaxPinLength)
	}

	return nil
}
