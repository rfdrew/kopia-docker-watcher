package notify

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/config"
)

// WebhookOptions configure the notifications to NOTIFY_WEBHOOK_URL.
type WebhookOptions struct {
	// URL is NOTIFY_WEBHOOK_URL; required.
	URL string
	// Format is one of config.WebhookFormats; empty means json.
	Format string
	// Transport sends the requests; nil means http.DefaultTransport.
	Transport http.RoundTripper
}

// webhook posts the notifications to a URL. The URL is never logged, nor
// shown in an error, as it may hold a token: only its scheme and host are.
type webhook struct {
	url    *url.URL
	safe   string
	format string
	client *http.Client
}

func newWebhook(opts WebhookOptions) (*webhook, error) {
	u, err := url.Parse(opts.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("notify: the webhook URL is not an http or https URL")
	}

	if opts.Format == "" {
		opts.Format = config.WebhookJSON
	}

	if !slices.Contains(config.WebhookFormats(), opts.Format) {
		return nil, fmt.Errorf("notify: unknown format %q", opts.Format)
	}

	if opts.Transport == nil {
		opts.Transport = http.DefaultTransport
	}

	return &webhook{
		url:    u,
		safe:   u.Scheme + "://" + u.Host,
		format: opts.Format,
		client: &http.Client{
			Transport: opts.Transport,
			// A redirect would send the notification, and maybe the token
			// in the URL, where the URL does not point.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

// send implements sender: a 408, a 429 and a 5xx are tried again, after
// the Retry-After of the answer if it has one; any other answer that is
// not a 2xx is not.
func (w *webhook) send(ctx context.Context, e Event) (wait time.Duration, err error) {
	req, err := w.request(ctx, e)
	if err != nil {
		return -1, err
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return 0, w.redact(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return 0, nil
	}

	err = fmt.Errorf("%s %s: %s", req.Method, w.safe, resp.Status)
	if msg := strings.TrimSpace(string(body)); msg != "" {
		err = fmt.Errorf("%w: %s", err, truncate(msg, 200))
	}

	switch {
	case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode == http.StatusRequestTimeout,
		resp.StatusCode >= 500:
		return retryAfter(resp.Header.Get("Retry-After")), err
	default:
		return -1, err
	}
}

// redact drops the URL, which may hold a token, from an error of the HTTP
// client.
func (w *webhook) redact(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s %s: %w", ue.Op, w.safe, ue.Err)
	}

	return err
}

// retryAfter reads a Retry-After header in seconds; zero if there is none.
func retryAfter(v string) time.Duration {
	s, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || s <= 0 {
		return 0
	}

	return time.Duration(s) * time.Second
}
