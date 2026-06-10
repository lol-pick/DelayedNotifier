package models

import "time"

type Status string

const (
	StatusPending  Status = "pending"
	StatusSent     Status = "sent"
	StatusFailed   Status = "failed"
	StatusCanceled Status = "canceled"
)

type Channel string

const (
	ChannelEmail    Channel = "email"
	ChannelTelegram Channel = "telegram"
)

type Notification struct {
	ID        string    `json:"id"`
	Channel   Channel   `json:"channel"`
	Recipient string    `json:"recipient"`
	Subject   string    `json:"subject,omitempty"`
	Message   string    `json:"message"`
	SendAt    time.Time `json:"send_at"`
	Status    Status    `json:"status"`
	Attempts  int       `json:"attempts"`
	LastError string    `json:"last_error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

func (n *Notification) IsTerminal() bool {
	return n.Status == StatusSent || n.Status == StatusFailed || n.Status == StatusCanceled
}
