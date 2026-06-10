package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"l3.1/internal/models"
)

var _ NotificationRepository = (*RedisStorage)(nil)

type RedisStorage struct {
	client    *redis.Client
	ttl       time.Duration
	keyPrefix string
}

func NewRedisStorage(addr, password string, db int, ttl time.Duration, prefix string) (*RedisStorage, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis ping %w", err)
	}

	if prefix == "" {
		prefix = "notify:"
	}
	return &RedisStorage{client: client, ttl: ttl, keyPrefix: prefix}, nil
}

func (s *RedisStorage) Close() error { return s.client.Close() }

func (s *RedisStorage) key(id string) string {
	return s.keyPrefix + id
}

func (s *RedisStorage) Save(ctx context.Context, n *models.Notification) error {
	data, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("marshal notification: %w", err)
	}
	if err := s.client.Set(ctx, s.key(n.ID), data, s.ttl).Err(); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}

func (s *RedisStorage) Get(ctx context.Context, id string) (*models.Notification, error) {
	raw, err := s.client.Get(ctx, s.key(id)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("redis get: %w", err)
	}
	var n models.Notification
	if err := json.Unmarshal(raw, &n); err != nil {
		return nil, fmt.Errorf("unmarshal notification: %w", err)
	}
	return &n, nil
}

func (s *RedisStorage) UpdateStatus(ctx context.Context, id string, status models.Status, lastErr string, attempts int) error {
	n, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	n.Status = status
	n.LastError = lastErr
	n.Attempts = attempts
	return s.Save(ctx, n)
}
