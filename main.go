package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"l3.1/internal/api"
	"l3.1/internal/models"
	"l3.1/internal/queue"
	"l3.1/internal/sender"
	"l3.1/internal/storage"
	"l3.1/internal/worker"
)

func envOrDefault(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envIntOrDefault(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// envDurationSeconds читает целое количество секунд из env и превращает в time.Duration.
// Удобно для backoff/таймаутов — пользователю не нужно знать про синтаксис "5s".
func envDurationSeconds(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return time.Duration(n) * time.Second
		}
	}
	return def
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// 1. Конфигурация ----------------------------------------------------------
	httpAddr := envOrDefault("HTTP_ADDR", ":8080")
	uiDir := envOrDefault("UI_DIR", "./ui")

	// Redis
	redisAddr := envOrDefault("REDIS_ADDR", "localhost:6379")
	redisPass := envOrDefault("REDIS_PASSWORD", "")
	redisDB := envIntOrDefault("REDIS_DB", 0)
	redisTTL := time.Duration(envIntOrDefault("REDIS_TTL_HOURS", 72)) * time.Hour
	redisPrefix := envOrDefault("REDIS_KEY_PREFIX", "notify:")

	// RabbitMQ
	queueCfg := queue.DefaultConfig()
	queueCfg.URL = envOrDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	queueCfg.ExchangeName = envOrDefault("QUEUE_EXCHANGE", queueCfg.ExchangeName)
	queueCfg.QueueName = envOrDefault("QUEUE_NAME", queueCfg.QueueName)
	queueCfg.RoutingKey = envOrDefault("QUEUE_ROUTING_KEY", queueCfg.RoutingKey)
	queueCfg.PrefetchCount = envIntOrDefault("QUEUE_PREFETCH", queueCfg.PrefetchCount)

	// Worker
	workerCfg := worker.Config{
		MaxAttempts: envIntOrDefault("WORKER_MAX_ATTEMPTS", 5),
		BaseBackoff: envDurationSeconds("WORKER_BASE_BACKOFF_SEC", 5*time.Second),
		MaxBackoff:  envDurationSeconds("WORKER_MAX_BACKOFF_SEC", 10*time.Minute),
	}

	// Senders
	smtpHost := envOrDefault("SMTP_HOST", "")
	smtpPort := envIntOrDefault("SMTP_PORT", 1025)
	smtpUser := envOrDefault("SMTP_USERNAME", "")
	smtpPass := envOrDefault("SMTP_PASSWORD", "")
	smtpFrom := envOrDefault("SMTP_FROM", "noreply@notifier.local")
	tgToken := envOrDefault("TELEGRAM_BOT_TOKEN", "")

	// 2. Хранилище
	repo, err := storage.NewRedisStorage(redisAddr, redisPass, redisDB, redisTTL, redisPrefix)
	if err != nil {
		log.Fatalf("init storage: %v", err)
	}
	defer repo.Close()

	// 3. Очередь
	rabbit, err := dialRabbitWithRetry(queueCfg, 30, 2*time.Second)
	if err != nil {
		log.Fatalf("init queue: %v", err)
	}
	defer rabbit.Close()

	// 4. Отправители
	registry := sender.NewRegistry()
	if smtpHost != "" {
		log.Printf("[startup] email sender: smtp %s:%d (user=%q)", smtpHost, smtpPort, smtpUser)
		registry.Register(models.ChannelEmail, sender.NewEmailSender(smtpHost, smtpPort, smtpUser, smtpPass, smtpFrom))
	} else {
		log.Println("[startup] email sender: SMTP_HOST is empty, using noop")
		registry.Register(models.ChannelEmail, sender.NewNoopSender(models.ChannelEmail))
	}
	if tgToken != "" {
		log.Println("[startup] telegram sender: token configured")
		registry.Register(models.ChannelTelegram, sender.NewTelegramSender(tgToken))
	} else {
		log.Println("[startup] telegram sender: TELEGRAM_BOT_TOKEN is empty, using noop")
		registry.Register(models.ChannelTelegram, sender.NewNoopSender(models.ChannelTelegram))
	}

	// 5. Воркер
	w := worker.New(rabbit, repo, registry, workerCfg)

	// 6. Запуск
	rootCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := w.Run(rootCtx, rabbit); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("worker stopped: %v", err)
		}
	}()

	// 6.2 HTTP-сервер.
	handler := api.New(repo, rabbit).Routes(uiDir)
	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("[http] listening on %s", httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http server: %v", err)
			cancel()
		}
	}()

	// 7. Graceful shutdown
	<-rootCtx.Done()
	log.Println("shutdown signal received")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}

	wg.Wait()
	log.Println("bye")
}

func dialRabbitWithRetry(cfg queue.Config, attempts int, pause time.Duration) (*queue.RabbitMQ, error) {
	var lastErr error
	for i := 1; i <= attempts; i++ {
		c, err := queue.NewRabbitMQ(cfg)
		if err == nil {
			return c, nil
		}
		lastErr = err
		log.Printf("[startup] rabbitmq not ready (attempt %d/%d): %v", i, attempts, err)
		time.Sleep(pause)
	}
	return nil, lastErr
}
