package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
	"github.com/rfdrew/kopia-docker-watcher/internal/version"
)

// Kind is what a notification is about.
type Kind string

// Kinds.
const (
	SnapshotFailed     Kind = "snapshot_failed"
	SnapshotRecovered  Kind = "snapshot_recovered"
	ReconcileFailed    Kind = "reconcile_failed"
	ReconcileRecovered Kind = "reconcile_recovered"
	HookFailed         Kind = "hook_failed"
	HookRecovered      Kind = "hook_recovered"
)

// Event is one notification.
type Event struct {
	Kind Kind
	// Path is the source path.
	Path string
	// Containers are those that back the path up, if the watcher manages
	// it.
	Containers []string
	// Host is kopia server's host name; empty if not known yet.
	Host string
	// Reason is the snapshot's outcome, the kind of reconcile error
	// (reconcile.Reason), or the hook (reconcile.HookPreExec or
	// reconcile.HookPostExec).
	Reason string
	// Error tells what went wrong; empty for a recovery.
	Error string
	// Since is when the path started failing; Time is now.
	Since, Time time.Time
}

// Failing reports whether e is about a path that started failing.
func (e Event) Failing() bool {
	return e.Kind == SnapshotFailed || e.Kind == ReconcileFailed || e.Kind == HookFailed
}

// Title is a one-line summary of e.
func (e Event) Title() string {
	switch e.Kind {
	case SnapshotFailed:
		return "Backup failed: " + e.Path
	case SnapshotRecovered:
		return "Backup works again: " + e.Path
	case ReconcileFailed:
		return "Backup labels cannot be applied: " + e.Path
	case HookFailed:
		return "Backup hook failed: " + e.Path
	case HookRecovered:
		return "Backup hook works again: " + e.Path
	default:
		return "Backup labels apply again: " + e.Path
	}
}

// Message tells what happened in a few sentences.
func (e Event) Message() string {
	var b strings.Builder

	where := e.Path
	if e.Host != "" {
		where += " on " + e.Host
	}

	if len(e.Containers) > 0 {
		where += " (" + strings.Join(e.Containers, ", ") + ")"
	}

	switch e.Kind {
	case SnapshotFailed:
		if e.Reason == "unknown" {
			fmt.Fprintf(&b, "The snapshot of %s ended in an unknown state", where)
		} else {
			fmt.Fprintf(&b, "The snapshot of %s failed", where)
		}
	case SnapshotRecovered:
		fmt.Fprintf(&b, "A snapshot of %s succeeded, after a failed one at %s.", where, stamp(e.Since))
	case ReconcileFailed:
		fmt.Fprintf(&b, "kopia-docker-watcher cannot bring the source %s in line with its labels (%s)", where, e.Reason)
	case ReconcileRecovered:
		fmt.Fprintf(&b, "kopia-docker-watcher brought the source %s in line with its labels, after problems since %s.",
			where, stamp(e.Since))
	case HookFailed:
		if e.Reason == reconcile.HookPreExec {
			fmt.Fprintf(&b, "The %s hook of %s failed, so no snapshot was taken", e.Reason, where)
		} else {
			fmt.Fprintf(&b, "The %s hook of %s failed", e.Reason, where)
		}
	case HookRecovered:
		fmt.Fprintf(&b, "The %s hook of %s succeeded, after a failure at %s.", e.Reason, where, stamp(e.Since))
	}

	if e.Failing() {
		if e.Error != "" {
			b.WriteString(": ")
			b.WriteString(strings.TrimRight(e.Error, "."))
		}

		b.WriteString(".")

		if e.Kind == ReconcileFailed && !e.Since.Equal(e.Time) {
			fmt.Fprintf(&b, " It has been failing since %s.", stamp(e.Since))
		}
	}

	return b.String()
}

func stamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// payload is the body of the json format.
type payload struct {
	Event      Kind      `json:"event"`
	Status     string    `json:"status"`
	Title      string    `json:"title"`
	Message    string    `json:"message"`
	Path       string    `json:"path"`
	Containers []string  `json:"containers"`
	Host       string    `json:"host,omitempty"`
	Reason     string    `json:"reason,omitempty"`
	Error      string    `json:"error,omitempty"`
	Since      time.Time `json:"since"`
	Time       time.Time `json:"time"`
	Version    string    `json:"watcher_version"`
}

// ntfy priorities and tags (https://docs.ntfy.sh/publish/).
const (
	ntfyPriorityFailing   = "high"
	ntfyPriorityRecovered = "default"
	ntfyTagFailing        = "warning"
	ntfyTagRecovered      = "white_check_mark"
)

// telegramMaxText is the Bot API's limit on the text of a message.
const telegramMaxText = 4096

// request builds the request that delivers e in the Notifier's format.
func (n *Notifier) request(ctx context.Context, e Event) (*http.Request, error) {
	target := *n.url

	var (
		body        []byte
		contentType = "application/json"
		header      = http.Header{}
		err         error
	)

	switch n.opts.Format {
	case config.WebhookNtfy:
		// https://docs.ntfy.sh/publish/: the message is the body, the rest
		// goes into headers, which carry non-ASCII text RFC 2047 encoded.
		body, contentType = []byte(e.Message()), "text/plain; charset=utf-8"
		priority, tag := ntfyPriorityRecovered, ntfyTagRecovered

		if e.Failing() {
			priority, tag = ntfyPriorityFailing, ntfyTagFailing
		}

		header.Set("Title", mime.BEncoding.Encode("utf-8", e.Title()))
		header.Set("Priority", priority)
		header.Set("Tags", tag)
	case config.WebhookSlack:
		// https://api.slack.com/messaging/webhooks: text in mrkdwn, where
		// &, < and > must be escaped.
		body, err = json.Marshal(map[string]string{"text": "*" + slackEscape(e.Title()) + "*\n" + slackEscape(e.Message())})
	case config.WebhookTelegram:
		// https://core.telegram.org/bots/api#sendmessage: the parameters
		// of the URL's query, chat_id among them, go into the JSON body
		// with the text, as plain text.
		msg := map[string]any{"text": truncate(e.Title()+"\n\n"+e.Message(), telegramMaxText)}

		for k, vs := range target.Query() {
			if len(vs) == 0 {
				continue
			}

			if i, err := strconv.ParseInt(vs[0], 10, 64); err == nil {
				msg[k] = i
			} else {
				msg[k] = vs[0]
			}
		}

		target.RawQuery = ""
		body, err = json.Marshal(msg)
	default:
		status := "resolved"
		if e.Failing() {
			status = "failing"
		}

		containers := e.Containers
		if containers == nil {
			containers = []string{}
		}

		body, err = json.Marshal(payload{
			Event: e.Kind, Status: status, Title: e.Title(), Message: e.Message(), Path: e.Path,
			Containers: containers, Host: e.Host, Reason: e.Reason, Error: e.Error,
			Since: e.Since.UTC(), Time: e.Time.UTC(), Version: version.Version,
		})
	}

	if err != nil {
		return nil, fmt.Errorf("encode the notification: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, n.redact(err)
	}

	req.Header = header
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", version.UserAgent())

	return req, nil
}

func slackEscape(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}
