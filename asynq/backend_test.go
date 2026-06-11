package asynq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/imohamedsheta/xnotify"
	xnotify_asynq "github.com/imohamedsheta/xnotify/asynq"
)

type stubEnqueuer struct {
	calls     []enqueueCall
	returnErr error
}

type enqueueCall struct {
	task *asynq.Task
	opts []asynq.Option
}

func (s *stubEnqueuer) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	s.calls = append(s.calls, enqueueCall{task: task, opts: opts})
	if s.returnErr != nil {
		return nil, s.returnErr
	}
	return &asynq.TaskInfo{ID: "test-id", Queue: "default", Type: task.Type()}, nil
}

type basicNotification struct{}

func (b *basicNotification) Type() string                             { return "basic" }
func (b *basicNotification) Channels() []string                       { return []string{"email"} }
func (b *basicNotification) ShouldQueue() bool                        { return true }
func (b *basicNotification) ScheduledAt() *time.Time                  { return nil }
func (b *basicNotification) Data(_, _ string, _ int64) map[string]any { return nil }

// notificationWithOpts implements both Notification and xnotify_asynq.AsynqOpts
type notificationWithOpts struct {
	basicNotification
	opts map[string][]asynq.Option
}

func (n *notificationWithOpts) AsynqOpts(channel string) []asynq.Option {
	return n.opts[channel]
}

func TestBackend_Enqueue_BasicNotification(t *testing.T) {
	stub := &stubEnqueuer{}
	backend := xnotify_asynq.New(stub, asynq.Queue("default"))

	task := &xnotify.NotificationTask{
		NotificationType: "basic",
		Channel:          "email",
		NotifiableID:     1,
		NotifiableType:   "user",
	}

	err := backend.Enqueue(context.Background(), &basicNotification{}, task, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(stub.calls) != 1 {
		t.Fatalf("want 1 enqueue call, got %d", len(stub.calls))
	}
	if stub.calls[0].task.Type() != xnotify.TaskType {
		t.Errorf("task type: want %q, got %q", xnotify.TaskType, stub.calls[0].task.Type())
	}
}

func TestBackend_Enqueue_WithAsynqOpts_AppendsPerNotificationOpts(t *testing.T) {
	stub := &stubEnqueuer{}
	backend := xnotify_asynq.New(stub, asynq.MaxRetry(1))

	notification := &notificationWithOpts{
		opts: map[string][]asynq.Option{
			"email": {asynq.Queue("critical"), asynq.MaxRetry(5)},
		},
	}
	task := &xnotify.NotificationTask{Channel: "email", NotificationType: "invoice.ready"}

	err := backend.Enqueue(context.Background(), notification, task, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// default (1 opt) + per-notification (2 opts) = 3 opts total
	if len(stub.calls[0].opts) != 3 {
		t.Errorf("want 3 opts, got %d", len(stub.calls[0].opts))
	}
}

func TestBackend_Enqueue_WithScheduleAt_AddsProcessAt(t *testing.T) {
	stub := &stubEnqueuer{}
	backend := xnotify_asynq.New(stub)

	at := time.Now().Add(1 * time.Hour)
	task := &xnotify.NotificationTask{Channel: "email", NotificationType: "test"}

	err := backend.Enqueue(context.Background(), &basicNotification{}, task, &at)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 1 opt: ProcessAt
	if len(stub.calls[0].opts) != 1 {
		t.Errorf("want 1 opt (ProcessAt), got %d", len(stub.calls[0].opts))
	}
}

func TestBackend_Enqueue_EnqueuerError_Propagates(t *testing.T) {
	want := errors.New("redis down")
	stub := &stubEnqueuer{returnErr: want}
	backend := xnotify_asynq.New(stub)

	task := &xnotify.NotificationTask{Channel: "email", NotificationType: "test"}

	err := backend.Enqueue(context.Background(), &basicNotification{}, task, nil)
	if !errors.Is(err, want) {
		t.Errorf("want %v, got %v", want, err)
	}
}

func TestBackend_Enqueue_NoAsynqOpts_OnlyDefaultsApplied(t *testing.T) {
	stub := &stubEnqueuer{}
	backend := xnotify_asynq.New(stub, asynq.Queue("low"), asynq.MaxRetry(2))

	// basicNotification does NOT implement AsynqOpts
	task := &xnotify.NotificationTask{Channel: "email", NotificationType: "test"}

	err := backend.Enqueue(context.Background(), &basicNotification{}, task, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(stub.calls[0].opts) != 2 {
		t.Errorf("want 2 default opts, got %d", len(stub.calls[0].opts))
	}
}
