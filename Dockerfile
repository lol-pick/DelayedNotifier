FROM golang:1.25-alpine AS builder
WORKDIR /src

COPY go.mod ./
COPY go.su[m] ./
RUN go mod download || true

COPY . .
RUN CGO_ENABLED=0 go build -o /out/notifier ./

FROM alpine:3.20
WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /out/notifier /app/notifier
COPY ui /app/ui

ENV UI_DIR=/app/ui
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD wget -qO- http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/app/notifier"]