// Package asynq provides an asynq-backed QueueBackend for xnotify.
//
// It gives callers full access to every asynq.Option without leaking asynq
// types into the core xnotify package.
//
// Per-notification options are expressed via the optional [AsynqOpts] interface.
// Notifications that do not implement it receive only the default options
// supplied to [New].
//
// Usage:
//
//	client := asynq.NewClient(asynq.RedisClientOpt{Addr: ":6379"})
//	backend := xnotify_asynq.New(client,
//	    asynq.Queue("notifications"),
//	    asynq.MaxRetry(3),
//	)
//	notify := xnotify.New(logger, backend)
package asynq

import (
	"context"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/imohamedsheta/xnotify"
)

// AsynqOpts is an optional interface a Notification can implement to supply
// per-notification, per-channel asynq options (queue name, retry count,
// deadlines, unique keys, retention, …).
//
// The interface lives here — in the asynq subpackage — so the core xnotify
// package remains free of any asynq dependency. Notifications that need
// asynq-specific control implement it; others are unaffected.
//
//	type InvoiceReady struct{ ... }
//
//	func (i *InvoiceReady) AsynqOpts(channel string) []asynq.Option {
//	    switch channel {
//	    case "email":
//	        return []asynq.Option{asynq.Queue("critical"), asynq.MaxRetry(5)}
//	    case "slack":
//	        return []asynq.Option{asynq.Queue("low")}
//	    }
//	    return nil
//	}
type AsynqOpts interface {
	AsynqOpts(channel string) []asynq.Option
}

// Enqueuer is the subset of *asynq.Client used by Backend.
// Exposing it as an interface makes the backend trivially testable.
type Enqueuer interface {
	Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error)
}

// Backend implements xnotify.QueueBackend using asynq.
type Backend struct {
	client      Enqueuer
	defaultOpts []asynq.Option
}

// New creates a Backend.
// defaultOpts are applied to every enqueue call; per-notification opts
// (from AsynqOpts) are appended after and take precedence on conflicts.
func New(client Enqueuer, defaultOpts ...asynq.Option) *Backend {
	return &Backend{
		client:      client,
		defaultOpts: defaultOpts,
	}
}

// Enqueue implements xnotify.QueueBackend.
// It receives the original Notification so it can call AsynqOpts if the
// notification implements it — before the task is serialised and that
// value is no longer available.
func (b *Backend) Enqueue(ctx context.Context, notification xnotify.Notification, task *xnotify.NotificationTask, scheduleAt *time.Time) error {
	payload, err := task.Marshal()
	if err != nil {
		return fmt.Errorf("xnotify - asynq: %w", err)
	}

	// Build option slice: defaults → per-notification → schedule
	opts := make([]asynq.Option, 0, len(b.defaultOpts)+4)
	opts = append(opts, b.defaultOpts...)

	if a, ok := notification.(AsynqOpts); ok {
		opts = append(opts, a.AsynqOpts(task.Channel)...)
	}

	if scheduleAt != nil {
		opts = append(opts, asynq.ProcessAt(*scheduleAt))
	}

	_, err = b.client.Enqueue(asynq.NewTask(xnotify.TaskType, payload), opts...)
	if err != nil {
		return fmt.Errorf("xnotify - asynq: enqueue: %w", err)
	}

	return nil
}

// Handler implements asynq.Handler, wiring asynq worker processing back
// into xnotify's registered channel handlers.
//
//	mux := asynq.NewServeMux()
//	mux.Handle(xnotify.TaskType, asynq.NewHandler(notify))
type Handler struct {
	notify *xnotify.Notify
}

// NewHandler creates an asynq.Handler that delegates to notify.
func NewHandler(notify *xnotify.Notify) *Handler {
	return &Handler{notify: notify}
}

// ProcessTask implements asynq.Handler.
func (h *Handler) ProcessTask(ctx context.Context, t *asynq.Task) error {
	task, err := xnotify.UnmarshalTask(t.Payload())
	if err != nil {
		return fmt.Errorf("asynq: %w", err)
	}
	return h.notify.HandleSendNotification(ctx, task)
}
