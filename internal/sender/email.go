package sender

import (
	"context"
	"fmt"
	"mime"
	"net"
	"net/smtp"
	"strconv"
	"strings"

	"l3.1/internal/models"
)

type EmailSender struct {
	host     string
	port     int
	username string
	password string
	from     string
}

func NewEmailSender(host string, port int, username, password, from string) *EmailSender {
	return &EmailSender{
		host:     host,
		port:     port,
		username: username,
		password: password,
		from:     from,
	}
}

func (e *EmailSender) Send(_ context.Context, n *models.Notification) error {
	addr := net.JoinHostPort(e.host, strconv.Itoa(e.port))
	encodedSubject := mime.QEncoding.Encode("utf-8", n.Subject)

	var msg strings.Builder
	msg.WriteString("From: " + e.from + "\r\n")
	msg.WriteString("To: " + n.Recipient + "\r\n")
	msg.WriteString("Subject: " + encodedSubject + "\r\n")
	msg.WriteString("MIME-Version: 1.0\r\n")
	msg.WriteString("Content-Type: text/plain; charset=\"utf-8\"\r\n")
	msg.WriteString(n.Message)

	var auth smtp.Auth
	if e.username != "" {
		auth = smtp.PlainAuth("", e.username, e.password, e.host)
	}

	if err := smtp.SendMail(addr, auth, e.from, []string{n.Recipient}, []byte(msg.String())); err != nil {
		return fmt.Errorf("smtp send to %s: %w", n.Recipient, err)
	}
	return nil
}
