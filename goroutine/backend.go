package goroutine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/imohamedsheta/xnotify"
)

const (
	DefaultWorkers   = 10
	DefaultQueueSize = 512
)

// Logger is a minimal structured logger interface.
// Compatible with slog.Logger, zap.SugaredLogger, and similar.
type Logger interface {
	Info(msg string, fields ...any)
	Error(msg string, fields ...any)
	Warn(msg string, fields ...any)
}

// nopLogger is used when no logger is provided.
type nopLogger struct{}

func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}
func (nopLogger) Warn(string, ...any)  {}

// Options configures the Backend.
type Options struct {
	// Workers is the number of goroutines calling the handler concurrently.
	// Defaults to DefaultWorkers (10).
	Workers int

	// QueueSize is the capacity of the internal buffered channel.
	// Enqueue blocks when the channel is full.
	// Defaults to DefaultQueueSize (512).
	QueueSize int

	// Logger is used to report handler errors. Defaults to a no-op logger.
	Logger Logger
}

func (o Options) workers() int {
	if o.Workers <= 0 {
		return DefaultWorkers
	}
	return o.Workers
}

func (o Options) queueSize() int {
	if o.QueueSize <= 0 {
		return DefaultQueueSize
	}
	return o.QueueSize
}

func (o Options) logger() Logger {
	if o.Logger == nil {
		return nopLogger{}
	}
	return o.Logger
}

type item struct {
	ctx  context.Context
	task *xnotify.NotificationTask
}

// Backend implements xnotify.QueueBackend using an in-process buffered channel.
// It knows nothing about xnotify.Notify — it only stores and forwards tasks.
//
// Call Shutdown to drain and shut down gracefully.
type Backend struct {
	queue  chan item
	stop   chan struct{}
	once   sync.Once
	wg     sync.WaitGroup // tracks both workers and timer goroutines
	logger Logger
}

// New creates a Backend and launches the worker pool.
// handler is called for every dequeued task — pass notify.HandleSendNotification
// from the xnotify side.
func New(opts Options, handler func(ctx context.Context, task *xnotify.NotificationTask) error) *Backend {
	b := &Backend{
		queue:  make(chan item, opts.queueSize()),
		stop:   make(chan struct{}),
		logger: opts.logger(),
	}

	for range opts.workers() {
		b.wg.Add(1)
		go b.worker(handler)
	}

	return b
}

// Enqueue implements xnotify.QueueBackend.
// Scheduled tasks sleep in a lightweight timer goroutine before being
// forwarded to the worker pool.
func (b *Backend) Enqueue(ctx context.Context, _ xnotify.Notification, task *xnotify.NotificationTask, scheduleAt *time.Time) error {
	select {
	case <-b.stop:
		return fmt.Errorf("goroutine backend: backend is stopped")
	default:
	}

	it := item{ctx: ctx, task: task}

	if scheduleAt == nil || !scheduleAt.After(time.Now()) {
		return b.dispatch(it)
	}

	// Timer goroutine is tracked by wg so Shutdown waits for it.
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		t := time.NewTimer(time.Until(*scheduleAt))
		defer t.Stop()
		select {
		case <-t.C:
			_ = b.dispatch(it) // backend may have stopped; error is expected and ignorable
		case <-b.stop:
		}
	}()

	return nil
}

// Shutdown signals workers to stop accepting new items, then waits for all
// in-flight tasks and pending timer goroutines to finish.
// Returns ctx.Err() if the context is cancelled before draining completes —
// any tasks still buffered at that point are abandoned.
func (b *Backend) Shutdown(ctx context.Context) error {
	b.once.Do(func() { close(b.stop) })

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ── internal ──────────────────────────────────────────────────────────────────

func (b *Backend) dispatch(it item) error {
	select {
	case b.queue <- it:
		return nil
	case <-b.stop:
		return fmt.Errorf("goroutine backend: backend is stopped")
	}
}

func (b *Backend) worker(handler func(ctx context.Context, task *xnotify.NotificationTask) error) {
	defer b.wg.Done()
	for {
		select {
		case it := <-b.queue:
			b.handle(handler, it)
		case <-b.stop:
			// Drain buffered items before exiting.
			for {
				select {
				case it := <-b.queue:
					b.handle(handler, it)
				default:
					return
				}
			}
		}
	}
}

func (b *Backend) handle(handler func(ctx context.Context, task *xnotify.NotificationTask) error, it item) {
	if err := handler(it.ctx, it.task); err != nil {
		b.logger.Error("xnotify - goroutine backend: handler error",
			"notification_type", it.task.NotificationType,
			"notifiable_type", it.task.NotifiableType,
			"notifiable_id", it.task.NotifiableID,
			"channel", it.task.Channel,
			"error", err,
		)
	}
}
