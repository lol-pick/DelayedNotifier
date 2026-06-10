package sender

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"l3.1/internal/models"
)

type TelegramSender struct {
	token  string
	client *http.Client
}

func NewTelegramSender(token string) *TelegramSender {
	return &TelegramSender{
		token: token,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

type telegramRequest struct {
	ChatID string `json:"chat_id"`
	Text   string `json:"text"`
}

type telegramResponse struct {
	OK          bool   `json:"ok"`
	ErrorCode   int    `json:"error_code,omitempty"`
	Description string `json:"description,omitempty"`
}

func (t *TelegramSender) Send(ctx context.Context, n *models.Notification) error {
	body, err := json.Marshal(telegramRequest{
		ChatID: n.Recipient,
		Text:   n.Message,
	})
	if err != nil {
		return fmt.Errorf("marshal telegram request: %w", err)
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.token)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("telegram http: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	var tr telegramResponse
	if err := json.Unmarshal(raw, &tr); err != nil {
		return fmt.Errorf("decode response (http=%d, body=%q): %w", resp.StatusCode, string(raw), err)
	}
	if !tr.OK {
		return fmt.Errorf("telegram api error %d: %s", tr.ErrorCode, tr.Description)
	}
	return nil
}
