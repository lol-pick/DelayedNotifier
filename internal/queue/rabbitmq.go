package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	_ Publisher = (*RabbitMQ)(nil)
	_ Consumer  = (*RabbitMQ)(nil)
)

type Config struct {
	URL           string
	ExchangeName  string
	QueueName     string
	RoutingKey    string
	PrefetchCount int
}

func DefaultConfig() Config {
	return Config{
		ExchangeName:  "notifications.delayed",
		QueueName:     "notifications.queue",
		RoutingKey:    "notify",
		PrefetchCount: 1,
	}
}

type RabbitMQ struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	cfg     Config
}

func NewRabbitMQ(cfg Config) (*RabbitMQ, error) {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("amqp channel: %w", err)
	}

	if err := ch.ExchangeDeclare(
		cfg.ExchangeName,
		"x-delayed-message",
		true,
		false,
		false,
		false,
		amqp.Table{"x-delayed-type": []byte("direct")},
	); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("declare exchange: %w", err)
	}

	if _, err := ch.QueueDeclare(cfg.QueueName, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("declare queue: %w", err)
	}

	if err := ch.QueueBind(cfg.QueueName, cfg.RoutingKey, cfg.ExchangeName, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("bind queue: %w", err)
	}

	if cfg.PrefetchCount > 0 {
		if err := ch.Qos(cfg.PrefetchCount, 0, false); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return nil, fmt.Errorf("qos: %w", err)
		}
	}

	return &RabbitMQ{conn: conn, channel: ch, cfg: cfg}, nil
}

func (r *RabbitMQ) Close() error {
	if r.channel != nil {
		_ = r.channel.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func (r *RabbitMQ) Publish(ctx context.Context, msg Message, delay time.Duration) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	if delay < 0 {
		delay = 0
	}

	headers := amqp.Table{"x-delay": int32(delay / time.Millisecond)}

	return r.channel.PublishWithContext(
		ctx,
		r.cfg.ExchangeName,
		r.cfg.RoutingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Headers:      headers,
			Timestamp:    time.Now(),
		},
	)
}

func (r *RabbitMQ) Consume(ctx context.Context, handler MessageHandler) error {
	deliveries, err := r.channel.Consume(
		r.cfg.QueueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	log.Printf("[queue] consumer started on %q", r.cfg.QueueName)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}
			r.dispatch(ctx, handler, d)
		}
	}
}

func (r *RabbitMQ) dispatch(ctx context.Context, handler MessageHandler, d amqp.Delivery) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("[queue] handler panic: %v", rec)
			_ = d.Nack(false, false)
		}
	}()

	var msg Message
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		log.Printf("[queue] bad message body: %v", err)
		_ = d.Nack(false, false)
		return
	}

	if err := handler(ctx, msg); err != nil {
		log.Printf("[queue] handler error for msg %s: %v", msg.ID, err)
		_ = d.Nack(false, true)
		return
	}

	_ = d.Ack(false)
}
