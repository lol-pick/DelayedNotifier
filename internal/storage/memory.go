package storage

import (
	"context"
	"encoding/json"
	"sync"

	"l3.1/internal/models"
)

var _ NotificationRepository = (*MemoryStorage)(nil)

type MemoryStorage struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{data: make(map[string][]byte)}
}

func (m *MemoryStorage) Get(_ context.Context, id string) (*models.Notification, error) {
	m.mu.RLock()
	raw, ok := m.data[id]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	var n models.Notification
	if err := json.Unmarshal(raw, &n); err != nil {
		return nil, err
	}
	return &n, nil
}

func (m *MemoryStorage) Save(_ context.Context, n *models.Notification) error {
	raw, err := json.Marshal(n)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[n.ID] = raw
	return nil
}

func (m *MemoryStorage) UpdateStatus(ctx context.Context, id string, status models.Status, lastErr string, attempts int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, ok := m.data[id]
	if !ok {
		return ErrNotFound
	}
	var n models.Notification
	if err := json.Unmarshal(raw, &n); err != nil {
		return err
	}
	n.Status = status
	n.LastError = lastErr
	n.Attempts = attempts

	updated, err := json.Marshal(&n)
	if err != nil {
		return err
	}
	m.data[id] = updated
	return nil
}
