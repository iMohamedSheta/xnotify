# xnotify

A clean, backend-agnostic notification dispatcher for Go.

Send notifications across multiple channels (email, Slack, SMS, …) — inline or via a queue — with a single unified API. The core package has **zero non-stdlib dependencies**. Queue support is opt-in via subpackages.

---

## Installation

```sh
go get github.com/imohamedsheta/xnotify
```

With asynq queue backend:

```sh
go get github.com/imohamedsheta/xnotify/asynq
```

---

## Concepts

| Type | Role |
|---|---|
| `Notification` | Describes what to send, to which channels, and how |
| `Notifiable` | The recipient — a user, team, or any entity with an ID |
| `ChannelHandler` | Delivers a task on one channel (email, Slack, SMS, …) |
| `QueueBackend` | Puts tasks on a queue; implemented by `asynq` |
| `Notify` | The central dispatcher — wires everything together |

---

## Quick start

### 1. Implement `Notification`

```go
type WelcomeEmail struct {
    Subject string
    Body    string
}

func (w *WelcomeEmail) Type() string     { return "user.welcome" }
func (w *WelcomeEmail) Channels() []string { return []string{"email"} }
func (w *WelcomeEmail) ShouldQueue() bool  { return false } // send inline
func (w *WelcomeEmail) ScheduledAt() *time.Time { return nil }

func (w *WelcomeEmail) Data(channel, notifiableType string, notifiableID int64) map[string]any {
    return map[string]any{
        "subject": w.Subject,
        "body":    w.Body,
    }
}
```

### 2. Implement `Notifiable`

```go
type User struct {
    ID    int64
    Email string
}

func (u *User) GetNotifiableID() int64    { return u.ID }
func (u *User) GetNotifiableType() string { return "user" }
```

### 3. Register channel handlers and send

```go
notify := xnotify.New(logger, nil) // nil = no queue backend needed

notify.RegisterChannels(map[string]xnotify.ChannelHandler{
    "email": func(ctx context.Context, task *xnotify.NotificationTask) error {
        // task.Data holds whatever Data() returned
        return mailer.Send(task.Data["subject"].(string), task.Data["body"].(string))
    },
})

user := &User{ID: 1, Email: "alice@example.com"}

// Sends inline — ShouldQueue() is false
if err := notify.Send(ctx, &WelcomeEmail{Subject: "Welcome!", Body: "..."}, user); err != nil {
    log.Fatal(err)
}
```

---

## Queued notifications

Set `ShouldQueue() bool { return true }` and provide a `QueueBackend`:

```go
client := asynq.NewClient(asynq.RedisClientOpt{Addr: ":6379"})

backend := asynqbackend.New(client,
    asynq.Queue("notifications"),
    asynq.MaxRetry(3),
)

notify := xnotify.New(logger, backend)
notify.RegisterChannels(channels)

// Enqueued immediately, processed by a worker
notify.Send(ctx, &InvoiceReady{InvoiceID: 42}, user)
```

### Worker setup

```go
srv := asynq.NewServer(
    asynq.RedisClientOpt{Addr: ":6379"},
    asynq.Config{Queues: map[string]int{"notifications": 5}},
)

mux := asynq.NewServeMux()
mux.Handle(xnotify.TaskType, asynqbackend.NewHandler(notify))

srv.Run(mux)
```

---

## Scheduled delivery

```go
// Via SendScheduled
at := time.Now().Add(24 * time.Hour)
notify.SendScheduled(ctx, at, &RenewalReminder{}, user)

// Via ScheduledAt() on the notification itself
func (r *RenewalReminder) ScheduledAt() *time.Time {
    t := time.Now().Add(24 * time.Hour)
    return &t
}
notify.Send(ctx, &RenewalReminder{}, user) // picks up ScheduledAt automatically
```

---

## Per-notification asynq options

The `asynq.AsynqOpts` interface lets individual notifications control queue name, retry count, deadlines, unique keys, and any other asynq option — without leaking asynq types into the core package.

Notifications that don't need custom options simply don't implement it.

```go
type InvoiceReady struct{ InvoiceID int64 }

// core Notification methods …

// AsynqOpts is defined in asynq — xnotify core never sees this
func (i *InvoiceReady) AsynqOpts(channel string) []asynq.Option {
    switch channel {
    case "email":
        return []asynq.Option{
            asynq.Queue("critical"),
            asynq.MaxRetry(5),
            asynq.Timeout(30 * time.Second),
        }
    case "slack":
        return []asynq.Option{asynq.Queue("low")}
    }
    return nil
}
```

Option precedence: **defaults (New) → per-notification (AsynqOpts) → schedule (ProcessAt)**

---

## Sending to multiple notifiables

```go
users := []*User{alice, bob, carol}

notifiables := make([]xnotify.Notifiable, len(users))
for i, u := range users {
    notifiables[i] = u
}

notify.Send(ctx, &NewsletterIssue{}, notifiables...)
```

Each notifiable × each channel = one task. Three users, two channels → six tasks.

---

## Bypass the queue

```go
// Always sends inline, ignores ShouldQueue()
notify.SendNow(ctx, notification, notifiable)
```

---

## Logger interface

`xnotify.Logger` is compatible with `slog`, `zap.SugaredLogger`, and any logger that exposes `Info/Error/Warn`:

```go
// slog adapter
type slogAdapter struct{ l *slog.Logger }
func (a *slogAdapter) Info(msg string, fields ...any)  { a.l.Info(msg, fields...) }
func (a *slogAdapter) Error(msg string, fields ...any) { a.l.Error(msg, fields...) }
func (a *slogAdapter) Warn(msg string, fields ...any)  { a.l.Warn(msg, fields...) }
```