package queue

import (
	"context"
	"time"
)

type Message struct {
	ID       string `json:"id"`
	Attempts int    `json:"attempts"`
}

type MessageHandler func(ctx context.Context, msg Message) error

type Publisher interface {
	Publish(ctx context.Context, msg Message, delay time.Duration) error
}

type Consumer interface {
	Consume(ctx context.Context, handler MessageHandler) error
}
