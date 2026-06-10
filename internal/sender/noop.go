package sender

import (
	"context"
	"log"

	"l3.1/internal/models"
)

type NoopSender struct {
	channel models.Channel
}

func NewNoopSender(ch models.Channel) *NoopSender {
	return &NoopSender{channel: ch}
}

func (n *NoopSender) Send(_ context.Context, msg *models.Notification) error {
	log.Printf("[NOOP-%s] to=%s subject=%q body=%q", n.channel, msg.Recipient, msg.Subject, msg.Message)
	return nil
}
