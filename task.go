package xnotify

import (
	"encoding/json"
	"fmt"
)

const TaskType = "xnotify:send"

// NotificationTask is the serialisable unit of work passed to a queue backend
// and later processed by a worker. It carries only plain data — no asynq types.
type NotificationTask struct {
	NotificationType string         `json:"notification_type"`
	NotifiableType   string         `json:"notifiable_type"`
	NotifiableID     int64          `json:"notifiable_id"`
	Channel          string         `json:"channel"`
	Data             map[string]any `json:"data"`
}

// Marshal serialises the task to JSON for queue storage.
func (t *NotificationTask) Marshal() ([]byte, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return nil, fmt.Errorf("xnotify: marshal task: %w", err)
	}
	return b, nil
}

// UnmarshalTask deserialises a JSON payload produced by Marshal back into a
// NotificationTask. Used by queue workers on the consumer side.
func UnmarshalTask(b []byte) (*NotificationTask, error) {
	var t NotificationTask
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, fmt.Errorf("xnotify: unmarshal task: %w", err)
	}
	return &t, nil
}
