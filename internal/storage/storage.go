package storage

import (
	"context"
	"errors"

	"l3.1/internal/models"
)

var ErrNotFound = errors.New("notification not found")

type NotificationRepository interface {
	Save(ctx context.Context, n *models.Notification) error
	Get(ctx context.Context, id string) (*models.Notification, error)
	UpdateStatus(ctx context.Context, id string, status models.Status, lastErr string, attempts int) error
}
