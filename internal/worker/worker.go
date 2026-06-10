package worker

import (
	"context"
	"errors"
	"log"
	"math"
	"time"

	"l3.1/internal/models"
	"l3.1/internal/queue"
	"l3.1/internal/sender"
	"l3.1/internal/storage"
)

type Config struct {
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

func DefaultConfig() Config {
	return Config{
		MaxAttempts: 5,
		BaseBackoff: 5 * time.Second,
		MaxBackoff:  10 * time.Minute,
	}
}

type Worker struct {
	publisher queue.Publisher
	repo      storage.NotificationRepository
	sender    sender.Sender
	cfg       Config
}

func New(pub queue.Publisher, repo storage.NotificationRepository, snd sender.Sender, cfg Config) *Worker {
	return &Worker{publisher: pub, repo: repo, sender: snd, cfg: cfg}
}

func (w *Worker) Run(ctx context.Context, consumer queue.Consumer) error {
	log.Println("[worker] starting")
	return consumer.Consume(ctx, w.Handle)
}

func (w *Worker) Handle(ctx context.Context, msg queue.Message) error {
	n, err := w.repo.Get(ctx, msg.ID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			log.Printf("[worker] notification %s not found, skipping", msg.ID)
			return nil
		}
		return err
	}
	if n.IsTerminal() {
		log.Printf("[worker] notification %s in terminal state %q, skipping", n.ID, n.Status)
		return nil
	}

	sendErr := w.sender.Send(ctx, n)
	if sendErr == nil {
		if err := w.repo.UpdateStatus(ctx, n.ID, models.StatusSent, "", n.Attempts+1); err != nil {
			log.Printf("[worker] sent ok but failed to update status for %s: %v", n.ID, err)
		}
		log.Printf("[worker] notification %s sent", n.ID)
		return nil
	}

	attempts := msg.Attempts + 1
	if attempts >= w.cfg.MaxAttempts {
		_ = w.repo.UpdateStatus(ctx, n.ID, models.StatusFailed, sendErr.Error(), attempts)
		log.Printf("[worker] notification %s permanently failed after %d attempts: %v", n.ID, attempts, sendErr)
		return nil
	}

	delay := w.exponentialDelay(attempts)
	if err := w.publisher.Publish(ctx, queue.Message{ID: n.ID, Attempts: attempts}, delay); err != nil {
		log.Printf("[worker] republish failed for %s: %v", n.ID, err)
		return err
	}

	_ = w.repo.UpdateStatus(ctx, n.ID, models.StatusPending, sendErr.Error(), attempts)
	log.Printf("[worker] notification %s failed (attempt %d), retry in %s: %v", n.ID, attempts, delay, sendErr)
	return nil
}

// exponentialDelay = BaseBackoff * 2^(attempts - 1), с потолком MaxBackoff
func (w *Worker) exponentialDelay(attempts int) time.Duration {
	mul := math.Pow(2, float64(attempts-1))
	delay := time.Duration(float64(w.cfg.BaseBackoff) * mul)
	if delay > w.cfg.MaxBackoff {
		delay = w.cfg.MaxBackoff
	}
	return delay
}
