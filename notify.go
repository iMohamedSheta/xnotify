package xnotify

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// QueueBackend is the interface a queue implementation must satisfy.
// The Notification value is passed alongside the task so backends can
// inspect it at enqueue time (e.g. to extract backend-specific options)
// before the task is serialised and the Notification value is lost.
type QueueBackend interface {
	Enqueue(ctx context.Context, notification Notification, task *NotificationTask, scheduleAt *time.Time) error
}

// Notification is implemented by every notification type.
type Notification interface {
	// Type returns a unique string identifier, e.g. "user.welcome".
	Type() string
	// Channels returns the delivery channels, e.g. ["email", "slack"].
	Channels() []string
	// ShouldQueue reports whether the notification should be queued
	// rather than sent inline.
	ShouldQueue() bool
	// Data returns the payload for a specific channel and notifiable.
	Data(channel string, notifiableType string, notifiableID int64) map[string]any
	// ScheduledAt optionally delays a queued notification.
	// Return nil to enqueue for immediate processing.
	ScheduledAt() *time.Time
}

// Notifiable is implemented by any entity that can receive notifications
// (users, teams, organisations, …).
type Notifiable interface {
	GetNotifiableID() int64
	GetNotifiableType() string
}

// ChannelHandler processes a single notification task for one channel.
type ChannelHandler func(ctx context.Context, task *NotificationTask) error

// Logger is a minimal structured logger interface.
// Compatible with slog.Logger, zap.SugaredLogger, and similar.
type Logger interface {
	Info(msg string, fields ...any)
	Error(msg string, fields ...any)
	Warn(msg string, fields ...any)
}

// Notify is the central dispatcher. Construct it with New.
type Notify struct {
	queue    QueueBackend
	channels map[string]ChannelHandler
	mu       sync.RWMutex
	log      Logger
}

// New creates a Notify instance.
// queue may be nil when only synchronous (SendNow) delivery is needed.
func New(log Logger, queue QueueBackend) *Notify {
	return &Notify{
		channels: make(map[string]ChannelHandler),
		log:      log,
		queue:    queue,
	}
}

// RegisterChannels registers the handler for each named channel.
// Calling RegisterChannels again replaces all previous registrations.
func (n *Notify) RegisterChannels(channels map[string]ChannelHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.channels = channels
}

// Send delivers the notification according to its own ShouldQueue and
// ScheduledAt decisions.
func (n *Notify) Send(ctx context.Context, notification Notification, notifiables ...Notifiable) error {
	tasks := n.buildTasks(notification, notifiables...)
	if notification.ShouldQueue() {
		return n.enqueueTasks(ctx, notification, tasks, notification.ScheduledAt())
	}
	return n.sendNow(ctx, tasks)
}

// SendNow delivers the notification inline, bypassing the queue entirely.
func (n *Notify) SendNow(ctx context.Context, notification Notification, notifiables ...Notifiable) error {
	return n.sendNow(ctx, n.buildTasks(notification, notifiables...))
}

// SendScheduled enqueues the notification for delivery at a future time.
func (n *Notify) SendScheduled(ctx context.Context, at time.Time, notification Notification, notifiables ...Notifiable) error {
	return n.enqueueTasks(ctx, notification, n.buildTasks(notification, notifiables...), &at)
}

// HandleSendNotification is exported so queue workers (e.g. asynqbackend.Handler)
// can route a deserialised task back through the registered channel handlers.
func (n *Notify) HandleSendNotification(ctx context.Context, task *NotificationTask) error {
	n.mu.RLock()
	handler, ok := n.channels[task.Channel]
	n.mu.RUnlock()

	if !ok {
		return fmt.Errorf("xnotify: no handler registered for channel %q", task.Channel)
	}
	return handler(ctx, task)
}

// ── internal ──────────────────────────────────────────────────────────────────

func (n *Notify) sendNow(ctx context.Context, tasks []*NotificationTask) error {
	for _, task := range tasks {
		if err := n.HandleSendNotification(ctx, task); err != nil {
			n.logErr(fmt.Sprintf("send failed on channel %q: %s", task.Channel, err))
			return err
		}
	}
	return nil
}

func (n *Notify) enqueueTasks(ctx context.Context, notification Notification, tasks []*NotificationTask, scheduleAt *time.Time) error {
	if n.queue == nil {
		return fmt.Errorf("xnotify: no queue backend configured")
	}
	for _, task := range tasks {
		if err := n.queue.Enqueue(ctx, notification, task, scheduleAt); err != nil {
			n.logErr(fmt.Sprintf("enqueue failed on channel %q: %s", task.Channel, err))
			return err
		}
	}
	return nil
}

func (n *Notify) buildTasks(notification Notification, notifiables ...Notifiable) []*NotificationTask {
	var tasks []*NotificationTask
	for _, notifiable := range notifiables {
		for _, ch := range notification.Channels() {
			tasks = append(tasks, &NotificationTask{
				NotificationType: notification.Type(),
				NotifiableType:   notifiable.GetNotifiableType(),
				NotifiableID:     notifiable.GetNotifiableID(),
				Channel:          ch,
				Data:             notification.Data(ch, notifiable.GetNotifiableType(), notifiable.GetNotifiableID()),
			})
		}
	}
	return tasks
}

func (n *Notify) logErr(msg string, fields ...any) {
	n.log.Error("xnotify: "+msg, fields...)
}
