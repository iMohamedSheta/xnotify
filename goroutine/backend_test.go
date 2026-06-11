package goroutine_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imohamedsheta/xnotify"
	goroutine "github.com/imohamedsheta/xnotify/goroutine"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// basicNotification satisfies xnotify.Notification with no-op behaviour.
type basicNotification struct{}

func (basicNotification) Type() string                             { return "basic" }
func (basicNotification) Channels() []string                       { return []string{"email"} }
func (basicNotification) ShouldQueue() bool                        { return true }
func (basicNotification) ScheduledAt() *time.Time                  { return nil }
func (basicNotification) Data(_, _ string, _ int64) map[string]any { return nil }

// task returns a minimal NotificationTask for use in tests.
func task(notifType, channel string) *xnotify.NotificationTask {
	return &xnotify.NotificationTask{
		NotificationType: notifType,
		NotifiableType:   "user",
		NotifiableID:     1,
		Channel:          channel,
	}
}

// collectingHandler returns a handler that appends every received task to a
// shared slice and signals done after n tasks have been received.
func collectingHandler(n int) (handler func(context.Context, *xnotify.NotificationTask) error, received func() []*xnotify.NotificationTask, wait func()) {
	var mu sync.Mutex
	var tasks []*xnotify.NotificationTask
	done := make(chan struct{})
	var once sync.Once

	h := func(_ context.Context, t *xnotify.NotificationTask) error {
		mu.Lock()
		tasks = append(tasks, t)
		count := len(tasks)
		mu.Unlock()
		if count >= n {
			once.Do(func() { close(done) })
		}
		return nil
	}

	recv := func() []*xnotify.NotificationTask {
		mu.Lock()
		defer mu.Unlock()
		out := make([]*xnotify.NotificationTask, len(tasks))
		copy(out, tasks)
		return out
	}

	w := func() {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}

	return h, recv, w
}

// countingHandler returns a handler that atomically counts invocations.
func countingHandler() (handler func(context.Context, *xnotify.NotificationTask) error, count func() int) {
	var n atomic.Int64
	h := func(_ context.Context, _ *xnotify.NotificationTask) error {
		n.Add(1)
		return nil
	}
	return h, func() int { return int(n.Load()) }
}

// errorHandler always returns the supplied error.
func errorHandler(err error) func(context.Context, *xnotify.NotificationTask) error {
	return func(_ context.Context, _ *xnotify.NotificationTask) error { return err }
}

// spyLogger records every Error call.
type spyLogger struct {
	mu     sync.Mutex
	errors [][]any
}

func (s *spyLogger) Info(string, ...any) {}
func (s *spyLogger) Warn(string, ...any) {}
func (s *spyLogger) Error(_ string, fields ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors = append(s.errors, fields)
}
func (s *spyLogger) errorCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.errors)
}

// newBackend is a convenience constructor that shuts the backend down when the
// test ends, so goroutine leaks are caught automatically.
func newBackend(t *testing.T, opts goroutine.Options, handler func(context.Context, *xnotify.NotificationTask) error) *goroutine.Backend {
	t.Helper()
	b := goroutine.New(opts, handler)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	})
	return b
}

// ── Enqueue ───────────────────────────────────────────────────────────────────

func TestEnqueue_ImmediateTask_IsHandled(t *testing.T) {
	handler, _, wait := collectingHandler(1)
	b := newBackend(t, goroutine.Options{Workers: 1, QueueSize: 8}, handler)

	err := b.Enqueue(context.Background(), basicNotification{}, task("user.welcome", "email"), nil)
	if err != nil {
		t.Fatalf("Enqueue returned unexpected error: %v", err)
	}

	wait()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}
}

func TestEnqueue_MultipleTasks_AllHandled(t *testing.T) {
	const n = 20
	handler, received, wait := collectingHandler(n)
	b := newBackend(t, goroutine.Options{Workers: 4, QueueSize: 64}, handler)

	for i := range n {
		tk := task("user.welcome", "email")
		tk.NotifiableID = int64(i)
		if err := b.Enqueue(context.Background(), basicNotification{}, tk, nil); err != nil {
			t.Fatalf("Enqueue[%d] error: %v", i, err)
		}
	}

	wait()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = b.Shutdown(ctx)

	if got := len(received()); got != n {
		t.Errorf("want %d tasks handled, got %d", n, got)
	}
}

func TestEnqueue_PastScheduleAt_IsDispatchedImmediately(t *testing.T) {
	handler, _, wait := collectingHandler(1)
	b := newBackend(t, goroutine.Options{Workers: 1, QueueSize: 8}, handler)

	past := time.Now().Add(-1 * time.Second)
	err := b.Enqueue(context.Background(), basicNotification{}, task("test", "email"), &past)
	if err != nil {
		t.Fatalf("Enqueue with past scheduleAt error: %v", err)
	}

	wait()
}

func TestEnqueue_NilScheduleAt_IsDispatchedImmediately(t *testing.T) {
	handler, _, wait := collectingHandler(1)
	b := newBackend(t, goroutine.Options{Workers: 1, QueueSize: 8}, handler)

	err := b.Enqueue(context.Background(), basicNotification{}, task("test", "email"), nil)
	if err != nil {
		t.Fatalf("Enqueue with nil scheduleAt error: %v", err)
	}

	wait()
}

// ── Scheduled tasks ───────────────────────────────────────────────────────────

func TestEnqueue_FutureScheduleAt_IsHandledAfterDelay(t *testing.T) {
	handler, received, wait := collectingHandler(1)
	b := newBackend(t, goroutine.Options{Workers: 1, QueueSize: 8}, handler)

	delay := 100 * time.Millisecond
	at := time.Now().Add(delay)

	before := time.Now()
	if err := b.Enqueue(context.Background(), basicNotification{}, task("scheduled", "email"), &at); err != nil {
		t.Fatalf("Enqueue error: %v", err)
	}

	wait()

	elapsed := time.Since(before)
	if elapsed < delay {
		t.Errorf("task was handled too early: elapsed %v < delay %v", elapsed, delay)
	}
	if len(received()) != 1 {
		t.Errorf("want 1 task handled, got %d", len(received()))
	}
}

func TestEnqueue_FutureScheduleAt_ShutdownCancelsTimer(t *testing.T) {
	var handled atomic.Bool
	handler := func(_ context.Context, _ *xnotify.NotificationTask) error {
		handled.Store(true)
		return nil
	}
	b := goroutine.New(goroutine.Options{Workers: 1, QueueSize: 8}, handler)

	far := time.Now().Add(10 * time.Second)
	if err := b.Enqueue(context.Background(), basicNotification{}, task("far-future", "email"), &far); err != nil {
		t.Fatalf("Enqueue error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}

	if handled.Load() {
		t.Error("task should not have been handled — timer should have been cancelled by Shutdown")
	}
}

// ── Stopped backend ───────────────────────────────────────────────────────────

func TestEnqueue_AfterShutdown_ReturnsError(t *testing.T) {
	handler, _, _ := countingHandler()
	b := goroutine.New(goroutine.Options{Workers: 1, QueueSize: 8}, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}

	err := b.Enqueue(context.Background(), basicNotification{}, task("test", "email"), nil)
	if err == nil {
		t.Fatal("want error after Shutdown, got nil")
	}
}

func TestEnqueue_AfterShutdown_FutureScheduleAt_ReturnsError(t *testing.T) {
	handler, _, _ := countingHandler()
	b := goroutine.New(goroutine.Options{Workers: 1, QueueSize: 8}, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = b.Shutdown(ctx)

	far := time.Now().Add(10 * time.Second)
	err := b.Enqueue(context.Background(), basicNotification{}, task("test", "email"), &far)
	if err == nil {
		t.Fatal("want error when enqueuing scheduled task after Shutdown, got nil")
	}
}

// ── Shutdown ──────────────────────────────────────────────────────────────────

func TestShutdown_Idempotent(t *testing.T) {
	handler, _, _ := countingHandler()
	b := goroutine.New(goroutine.Options{Workers: 2, QueueSize: 8}, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Calling Shutdown twice must not panic (sync.Once guard).
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("first Shutdown error: %v", err)
	}
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown error: %v", err)
	}
}

func TestShutdown_DrainsBufferedTasks(t *testing.T) {
	// Use a slow handler and a large batch so tasks sit in the buffer when
	// Shutdown is called. Workers must drain all of them before returning.
	const n = 30
	var handled atomic.Int64
	ready := make(chan struct{})

	handler := func(_ context.Context, _ *xnotify.NotificationTask) error {
		<-ready // block until we open the gate
		handled.Add(1)
		return nil
	}

	b := goroutine.New(goroutine.Options{Workers: 4, QueueSize: 128}, handler)

	for i := range n {
		tk := task("drain", "email")
		tk.NotifiableID = int64(i)
		if err := b.Enqueue(context.Background(), basicNotification{}, tk, nil); err != nil {
			t.Fatalf("Enqueue[%d] error: %v", i, err)
		}
	}

	// Open the gate so workers can start processing.
	close(ready)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}

	if got := int(handled.Load()); got != n {
		t.Errorf("want %d tasks handled after Shutdown drain, got %d", n, got)
	}
}

func TestShutdown_ContextCancelled_ReturnsCtxErr(t *testing.T) {
	// The handler blocks forever, so Shutdown will never complete naturally.
	block := make(chan struct{})
	handler := func(_ context.Context, _ *xnotify.NotificationTask) error {
		<-block
		return nil
	}

	b := goroutine.New(goroutine.Options{Workers: 1, QueueSize: 4}, handler)

	// Fill the queue with a task so a worker is definitely busy.
	if err := b.Enqueue(context.Background(), basicNotification{}, task("block", "email"), nil); err != nil {
		t.Fatalf("Enqueue error: %v", err)
	}

	// Give the worker a moment to pick up the task.
	time.Sleep(10 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := b.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("want context.DeadlineExceeded, got %v", err)
	}

	// Unblock the worker so the goroutine can exit cleanly.
	close(block)
}

// ── Handler error logging ─────────────────────────────────────────────────────

func TestHandler_Error_IsLogged(t *testing.T) {
	spy := &spyLogger{}
	handlerErr := errors.New("delivery failed")

	b := goroutine.New(
		goroutine.Options{Workers: 1, QueueSize: 8, Logger: spy},
		errorHandler(handlerErr),
	)

	if err := b.Enqueue(context.Background(), basicNotification{}, task("err.test", "email"), nil); err != nil {
		t.Fatalf("Enqueue error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}

	if got := spy.errorCount(); got != 1 {
		t.Errorf("want 1 logged error, got %d", got)
	}
}

func TestHandler_Error_DoesNotStopWorker(t *testing.T) {
	// First call returns an error; subsequent calls succeed.
	var callCount atomic.Int64
	handler := func(_ context.Context, _ *xnotify.NotificationTask) error {
		n := callCount.Add(1)
		if n == 1 {
			return errors.New("first call fails")
		}
		return nil
	}

	b := newBackend(t, goroutine.Options{Workers: 1, QueueSize: 8}, handler)

	for range 3 {
		if err := b.Enqueue(context.Background(), basicNotification{}, task("resilience", "email"), nil); err != nil {
			t.Fatalf("Enqueue error: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = b.Shutdown(ctx)

	if got := int(callCount.Load()); got != 3 {
		t.Errorf("want handler called 3 times, got %d", got)
	}
}

// ── Options defaults ──────────────────────────────────────────────────────────

func TestOptions_ZeroValues_UseDefaults(t *testing.T) {
	// Zero-value Options must not panic — defaults kick in.
	handler, _, _ := countingHandler()
	b := goroutine.New(goroutine.Options{}, handler)

	if err := b.Enqueue(context.Background(), basicNotification{}, task("default-opts", "email"), nil); err != nil {
		t.Fatalf("Enqueue error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}
}

// ── Context propagation ───────────────────────────────────────────────────────

func TestEnqueue_ContextIsForwardedToHandler(t *testing.T) {
	type ctxKey struct{}
	want := "propagated-value"

	var got string
	var mu sync.Mutex
	done := make(chan struct{})

	handler := func(ctx context.Context, _ *xnotify.NotificationTask) error {
		mu.Lock()
		got = ctx.Value(ctxKey{}).(string)
		mu.Unlock()
		close(done)
		return nil
	}

	b := newBackend(t, goroutine.Options{Workers: 1, QueueSize: 8}, handler)

	ctx := context.WithValue(context.Background(), ctxKey{}, want)
	if err := b.Enqueue(ctx, basicNotification{}, task("ctx-prop", "email"), nil); err != nil {
		t.Fatalf("Enqueue error: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for handler")
	}

	mu.Lock()
	defer mu.Unlock()
	if got != want {
		t.Errorf("context value: want %q, got %q", want, got)
	}
}
