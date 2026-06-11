package xnotify_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/imohamedsheta/xnotify"
)

// ── test doubles ──────────────────────────────────────────────────────────────

type testLogger struct {
	mu   sync.Mutex
	logs []string
}

func (l *testLogger) Info(msg string, _ ...any) {
	l.mu.Lock()
	l.logs = append(l.logs, "INFO:"+msg)
	l.mu.Unlock()
}
func (l *testLogger) Error(msg string, _ ...any) {
	l.mu.Lock()
	l.logs = append(l.logs, "ERR:"+msg)
	l.mu.Unlock()
}
func (l *testLogger) Warn(msg string, _ ...any) {
	l.mu.Lock()
	l.logs = append(l.logs, "WARN:"+msg)
	l.mu.Unlock()
}

type captureBackend struct {
	mu        sync.Mutex
	calls     []backendCall
	returnErr error
}

type backendCall struct {
	notification xnotify.Notification
	task         *xnotify.NotificationTask
	scheduleAt   *time.Time
}

func (b *captureBackend) Enqueue(_ context.Context, n xnotify.Notification, t *xnotify.NotificationTask, at *time.Time) error {
	b.mu.Lock()
	b.calls = append(b.calls, backendCall{notification: n, task: t, scheduleAt: at})
	b.mu.Unlock()
	return b.returnErr
}

// ── fixture notifications ──────────────────────────────────────────────────────

type syncNotification struct {
	channels []string
	data     map[string]any
}

func (s *syncNotification) Type() string            { return "test.sync" }
func (s *syncNotification) Channels() []string      { return s.channels }
func (s *syncNotification) ShouldQueue() bool       { return false }
func (s *syncNotification) ScheduledAt() *time.Time { return nil }
func (s *syncNotification) Data(ch, _ string, _ int64) map[string]any {
	if s.data != nil {
		return s.data
	}
	return map[string]any{"channel": ch}
}

type queuedNotification struct {
	syncNotification
	scheduledAt *time.Time
}

func (q *queuedNotification) Type() string            { return "test.queued" }
func (q *queuedNotification) ShouldQueue() bool       { return true }
func (q *queuedNotification) ScheduledAt() *time.Time { return q.scheduledAt }

type testNotifiable struct {
	id  int64
	typ string
}

func (u *testNotifiable) GetNotifiableID() int64    { return u.id }
func (u *testNotifiable) GetNotifiableType() string { return u.typ }

// ── helpers ───────────────────────────────────────────────────────────────────

func newNotify(t *testing.T, backend xnotify.QueueBackend) (*xnotify.Notify, *testLogger) {
	t.Helper()
	log := &testLogger{}
	n := xnotify.New(log, backend)
	return n, log
}

func noopHandler(_ context.Context, _ *xnotify.NotificationTask) error { return nil }

func errorHandler(err error) xnotify.ChannelHandler {
	return func(_ context.Context, _ *xnotify.NotificationTask) error { return err }
}

func captureHandler(out *[]*xnotify.NotificationTask) xnotify.ChannelHandler {
	var mu sync.Mutex
	return func(_ context.Context, task *xnotify.NotificationTask) error {
		mu.Lock()
		*out = append(*out, task)
		mu.Unlock()
		return nil
	}
}

// ── SendNow ───────────────────────────────────────────────────────────────────

func TestSendNow_SingleChannel(t *testing.T) {
	n, _ := newNotify(t, nil)
	var received []*xnotify.NotificationTask
	n.RegisterChannels(map[string]xnotify.ChannelHandler{
		"email": captureHandler(&received),
	})

	notifiable := &testNotifiable{id: 1, typ: "user"}
	notification := &syncNotification{channels: []string{"email"}}

	if err := n.SendNow(context.Background(), notification, notifiable); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(received) != 1 {
		t.Fatalf("want 1 task, got %d", len(received))
	}
	task := received[0]
	if task.Channel != "email" {
		t.Errorf("channel: want %q, got %q", "email", task.Channel)
	}
	if task.NotifiableID != 1 {
		t.Errorf("notifiable_id: want 1, got %d", task.NotifiableID)
	}
	if task.NotificationType != "test.sync" {
		t.Errorf("notification_type: want %q, got %q", "test.sync", task.NotificationType)
	}
}

func TestSendNow_MultipleChannels(t *testing.T) {
	n, _ := newNotify(t, nil)
	var received []*xnotify.NotificationTask
	n.RegisterChannels(map[string]xnotify.ChannelHandler{
		"email": captureHandler(&received),
		"slack": captureHandler(&received),
		"sms":   captureHandler(&received),
	})

	notification := &syncNotification{channels: []string{"email", "slack", "sms"}}
	notifiable := &testNotifiable{id: 42, typ: "user"}

	if err := n.SendNow(context.Background(), notification, notifiable); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(received) != 3 {
		t.Fatalf("want 3 tasks, got %d", len(received))
	}
}

func TestSendNow_MultipleNotifiables(t *testing.T) {
	n, _ := newNotify(t, nil)
	var received []*xnotify.NotificationTask
	n.RegisterChannels(map[string]xnotify.ChannelHandler{
		"email": captureHandler(&received),
	})

	notification := &syncNotification{channels: []string{"email"}}
	notifiables := []xnotify.Notifiable{
		&testNotifiable{id: 1, typ: "user"},
		&testNotifiable{id: 2, typ: "user"},
		&testNotifiable{id: 3, typ: "user"},
	}

	if err := n.SendNow(context.Background(), notification, notifiables...); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(received) != 3 {
		t.Fatalf("want 3 tasks, got %d", len(received))
	}
	ids := map[int64]bool{}
	for _, task := range received {
		ids[task.NotifiableID] = true
	}
	for _, id := range []int64{1, 2, 3} {
		if !ids[id] {
			t.Errorf("missing task for notifiable id %d", id)
		}
	}
}

func TestSendNow_UnregisteredChannel_ReturnsError(t *testing.T) {
	n, _ := newNotify(t, nil)
	n.RegisterChannels(map[string]xnotify.ChannelHandler{}) // empty

	notification := &syncNotification{channels: []string{"email"}}
	notifiable := &testNotifiable{id: 1, typ: "user"}

	err := n.SendNow(context.Background(), notification, notifiable)
	if err == nil {
		t.Fatal("expected error for unregistered channel, got nil")
	}
}

func TestSendNow_HandlerError_Propagates(t *testing.T) {
	n, _ := newNotify(t, nil)
	want := errors.New("smtp down")
	n.RegisterChannels(map[string]xnotify.ChannelHandler{
		"email": errorHandler(want),
	})

	notification := &syncNotification{channels: []string{"email"}}
	notifiable := &testNotifiable{id: 1, typ: "user"}

	err := n.SendNow(context.Background(), notification, notifiable)
	if !errors.Is(err, want) {
		t.Errorf("want %v, got %v", want, err)
	}
}

// ── Send (ShouldQueue routing) ────────────────────────────────────────────────

func TestSend_SyncNotification_CallsHandlerDirectly(t *testing.T) {
	backend := &captureBackend{}
	n, _ := newNotify(t, backend)
	var received []*xnotify.NotificationTask
	n.RegisterChannels(map[string]xnotify.ChannelHandler{
		"email": captureHandler(&received),
	})

	notification := &syncNotification{channels: []string{"email"}}
	notifiable := &testNotifiable{id: 1, typ: "user"}

	if err := n.Send(context.Background(), notification, notifiable); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(backend.calls) != 0 {
		t.Errorf("expected no backend calls for sync notification, got %d", len(backend.calls))
	}
	if len(received) != 1 {
		t.Errorf("expected 1 direct delivery, got %d", len(received))
	}
}

func TestSend_QueuedNotification_CallsBackend(t *testing.T) {
	backend := &captureBackend{}
	n, _ := newNotify(t, backend)
	n.RegisterChannels(map[string]xnotify.ChannelHandler{
		"email": noopHandler,
	})

	notification := &queuedNotification{
		syncNotification: syncNotification{channels: []string{"email"}},
	}
	notifiable := &testNotifiable{id: 1, typ: "user"}

	if err := n.Send(context.Background(), notification, notifiable); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(backend.calls) != 1 {
		t.Fatalf("want 1 backend call, got %d", len(backend.calls))
	}
	call := backend.calls[0]
	if call.task.Channel != "email" {
		t.Errorf("channel: want %q, got %q", "email", call.task.Channel)
	}
	if call.scheduleAt != nil {
		t.Errorf("scheduleAt: want nil, got %v", call.scheduleAt)
	}
}

func TestSend_NoBackend_QueuedNotification_ReturnsError(t *testing.T) {
	n, _ := newNotify(t, nil) // no backend
	n.RegisterChannels(map[string]xnotify.ChannelHandler{})

	notification := &queuedNotification{
		syncNotification: syncNotification{channels: []string{"email"}},
	}
	notifiable := &testNotifiable{id: 1, typ: "user"}

	err := n.Send(context.Background(), notification, notifiable)
	if err == nil {
		t.Fatal("expected error when backend is nil, got nil")
	}
}

// ── SendScheduled ─────────────────────────────────────────────────────────────

func TestSendScheduled_PassesScheduleAtToBackend(t *testing.T) {
	backend := &captureBackend{}
	n, _ := newNotify(t, backend)
	n.RegisterChannels(map[string]xnotify.ChannelHandler{})

	at := time.Now().Add(1 * time.Hour)
	notification := &syncNotification{channels: []string{"email"}}
	notifiable := &testNotifiable{id: 1, typ: "user"}

	if err := n.SendScheduled(context.Background(), at, notification, notifiable); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(backend.calls) != 1 {
		t.Fatalf("want 1 backend call, got %d", len(backend.calls))
	}
	got := backend.calls[0].scheduleAt
	if got == nil || !got.Equal(at) {
		t.Errorf("scheduleAt: want %v, got %v", at, got)
	}
}

// ── ScheduledAt on Notification ───────────────────────────────────────────────

func TestSend_ScheduledAt_PassedToBackend(t *testing.T) {
	backend := &captureBackend{}
	n, _ := newNotify(t, backend)
	n.RegisterChannels(map[string]xnotify.ChannelHandler{})

	at := time.Now().Add(2 * time.Hour)
	notification := &queuedNotification{
		syncNotification: syncNotification{channels: []string{"email"}},
		scheduledAt:      &at,
	}
	notifiable := &testNotifiable{id: 1, typ: "user"}

	if err := n.Send(context.Background(), notification, notifiable); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(backend.calls) != 1 {
		t.Fatalf("want 1 backend call, got %d", len(backend.calls))
	}
	got := backend.calls[0].scheduleAt
	if got == nil || !got.Equal(at) {
		t.Errorf("scheduleAt: want %v, got %v", at, got)
	}
}

// ── backend error propagation ────────────────────────────────────────────────

func TestSend_BackendError_Propagates(t *testing.T) {
	want := errors.New("redis unavailable")
	backend := &captureBackend{returnErr: want}
	n, _ := newNotify(t, backend)
	n.RegisterChannels(map[string]xnotify.ChannelHandler{})

	notification := &queuedNotification{
		syncNotification: syncNotification{channels: []string{"email"}},
	}
	notifiable := &testNotifiable{id: 1, typ: "user"}

	err := n.Send(context.Background(), notification, notifiable)
	if !errors.Is(err, want) {
		t.Errorf("want %v, got %v", want, err)
	}
}

// ── task data ────────────────────────────────────────────────────────────────

func TestSendNow_TaskData_PopulatedCorrectly(t *testing.T) {
	n, _ := newNotify(t, nil)
	var received []*xnotify.NotificationTask

	n.RegisterChannels(map[string]xnotify.ChannelHandler{
		"email": captureHandler(&received),
	})

	customData := map[string]any{"subject": "Hello", "body": "World"}
	notification := &syncNotification{channels: []string{"email"}, data: customData}
	notifiable := &testNotifiable{id: 99, typ: "admin"}

	if err := n.SendNow(context.Background(), notification, notifiable); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	task := received[0]
	if task.NotifiableID != 99 {
		t.Errorf("notifiable_id: want 99, got %d", task.NotifiableID)
	}
	if task.NotifiableType != "admin" {
		t.Errorf("notifiable_type: want %q, got %q", "admin", task.NotifiableType)
	}
	if fmt.Sprintf("%v", task.Data["subject"]) != "Hello" {
		t.Errorf("data.subject: want %q, got %v", "Hello", task.Data["subject"])
	}
}

// ── RegisterChannels concurrency ──────────────────────────────────────────────

func TestRegisterChannels_ConcurrentAccess(t *testing.T) {
	n, _ := newNotify(t, nil)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			n.RegisterChannels(map[string]xnotify.ChannelHandler{
				fmt.Sprintf("ch%d", i): noopHandler,
			})
		}(i)
	}
	wg.Wait()
}
