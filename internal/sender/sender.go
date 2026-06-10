package sender

import (
	"context"
	"fmt"

	"l3.1/internal/models"
)

type Sender interface {
	Send(ctx context.Context, n *models.Notification) error
}

type Registry struct {
	senders map[models.Channel]Sender
}

var _ Sender = (*Registry)(nil)

func NewRegistry() *Registry {
	return &Registry{senders: make(map[models.Channel]Sender)}
}
func (r *Registry) Register(ch models.Channel, s Sender) {
	r.senders[ch] = s
}

func (r *Registry) Send(ctx context.Context, n *models.Notification) error {
	s, ok := r.senders[n.Channel]
	if !ok {
		return fmt.Errorf("no sender registered for channel %q", n.Channel)
	}
	return s.Send(ctx, n)
}
