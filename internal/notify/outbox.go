package notify

import (
	"context"
	"log/slog"
	"time"
)

// Channel is a way the notifications go out, and the value of the channel
// label of their metric.
type Channel string

// Channels.
const (
	// ChannelWebhook posts to NOTIFY_WEBHOOK_URL.
	ChannelWebhook Channel = "webhook"
	// ChannelSMTP sends an e-mail through NOTIFY_SMTP_HOST.
	ChannelSMTP Channel = "smtp"
)

// Channels returns every Channel.
func Channels() []Channel { return []Channel{ChannelWebhook, ChannelSMTP} }

// sender delivers the notifications over one channel.
type sender interface {
	// send makes one attempt. On failure wait is how long the receiver
	// asked to wait before the next one, zero if it did not ask, negative
	// if trying again cannot help.
	send(ctx context.Context, e Event) (wait time.Duration, err error)
}

// outbox is the queue of one channel and the loop that empties it, so
// that a channel that is slow or failing holds back only its own
// notifications.
type outbox struct {
	channel Channel
	sender  sender
	// timeout bounds one attempt.
	timeout time.Duration
	queue   chan Event
	log     *slog.Logger
	// opts holds the retries and the metrics, the same for every channel.
	opts *Options
}

// enqueue hands e to the loop without waiting; a full queue drops it.
func (o *outbox) enqueue(e Event) {
	select {
	case o.queue <- e:
	default:
		o.log.Warn("dropping a notification: too many are waiting to be sent", "event", string(e.Kind), "path", e.Path)
		o.count(ResultDropped)
	}
}

// count counts a result. It comes after the log line about it, so that
// whoever sees the count finds the reason in the log.
func (o *outbox) count(r Result) {
	if o.opts.Metrics != nil {
		o.opts.Metrics.Notification(o.channel, r)
	}
}

// run sends the queued notifications until ctx is done; those still
// waiting then are dropped.
func (o *outbox) run(ctx context.Context) {
	for {
		select {
		case e := <-o.queue:
			o.deliver(ctx, e)
		case <-ctx.Done():
			dropped := 0

			for {
				select {
				case <-o.queue:
					dropped++

					o.count(ResultDropped)
				default:
					if dropped > 0 {
						o.log.Warn("dropping the notifications not sent before the stop", "count", dropped)
					}

					return
				}
			}
		}
	}
}

// deliver sends e, trying again after a temporary failure.
func (o *outbox) deliver(ctx context.Context, e Event) {
	delay := o.opts.RetryDelay

	for attempt := 1; ; attempt++ {
		wait, err := o.send(ctx, e)

		switch {
		case err == nil:
			o.log.Info("notification sent", "event", string(e.Kind), "path", e.Path)
			o.count(ResultSent)

			return
		case ctx.Err() != nil:
			o.count(ResultDropped)
			return
		case wait < 0 || attempt >= o.opts.Attempts:
			o.log.Warn("cannot send the notification", "event", string(e.Kind), "path", e.Path,
				"attempts", attempt, "error", err.Error())
			o.count(ResultFailed)

			return
		}

		wait = min(max(wait, delay), o.opts.MaxRetryDelay)
		o.log.Debug("cannot send the notification; trying again", "event", string(e.Kind), "path", e.Path,
			"attempt", attempt, "delay", wait.String(), "error", err.Error())

		t := time.NewTimer(wait)

		select {
		case <-ctx.Done():
			t.Stop()
			o.count(ResultDropped)

			return
		case <-t.C:
		}

		delay = min(2*delay, o.opts.MaxRetryDelay)
	}
}

// send makes one attempt within the timeout.
func (o *outbox) send(ctx context.Context, e Event) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	return o.sender.send(ctx, e)
}
