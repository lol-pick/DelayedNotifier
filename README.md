# DelayedNotifier

Сервис отложенных уведомлений на Go: REST API + RabbitMQ (delayed-message-exchange) + Redis + фоновый воркер с экспоненциальным backoff и простой HTML-UI.

## Структура проекта

```
.
├── main.go                      
├── go.mod
├── Dockerfile                   
├── docker-compose.yml           
├── ui/index.html                
└── internal/
    ├── models/notification.go   
    ├── storage/redis.go         
    ├── queue/rabbitmq.go        
    ├── sender/
    │   ├── sender.go            
    │   ├── email.go             
    │   ├── telegram.go          
    │   └── noop.go              
    ├── worker/worker.go         
    └── api/handlers.go          
```

## Архитектура

1. Клиент шлёт `POST /notify` → API создаёт `Notification`, сохраняет JSON в Redis по ключу `notify:<id>`.
2. API публикует в RabbitMQ маленькое сообщение `{id, attempts}` с заголовком `x-delay`, равным `SendAt - now` в миллисекундах.
3. RabbitMQ-плагин `rabbitmq-delayed-message-exchange` придерживает сообщение в эксчейндже `notifications.delayed` нужное время и потом маршрутизирует в очередь `notifications.queue`.
4. Воркер потребляет очередь, по `id` достаёт уведомление из Redis и:
   - если `status == canceled|sent|failed` — пропускает;
   - иначе вызывает соответствующий `Sender`;
   - при успехе ставит `status=sent`;
   - при ошибке инкрементирует `attempts` и ре-публикует то же сообщение с экспоненциальной задержкой `BaseBackoff * 2^(attempts-1)` (cap = `MaxBackoff`); после `MaxAttempts` — `status=failed`.
5. `DELETE /notify/{id}` помечает запись `canceled` (soft cancel — сама очередь не трогается, воркер просто пропустит).

## Запуск

```bash
# 1. Собрать и поднять весь стек
docker compose up --build

# 2. Открыть UI
open http://localhost:8080

# RabbitMQ Management UI: http://localhost:15672  (guest / guest)
# MailHog (поймать email):  http://localhost:8025
```

### Включить Telegram

1. Открой [@BotFather](https://t.me/BotFather), создай бота, получи токен.
2. Напиши боту любое сообщение со своего аккаунта.
3. Узнай свой `chat_id`: открой `https://api.telegram.org/bot<TOKEN>/getUpdates` и найди поле `message.chat.id`.
4. Прокинь токен в сервис: либо добавь `TELEGRAM_BOT_TOKEN=...` в `docker-compose.yml`, либо `export TELEGRAM_BOT_TOKEN=...` перед `docker compose up`.

После рестарта при `POST /notify` с `"channel": "telegram"` и `"recipient": "<chat_id>"` ты получишь сообщение в Telegram.


## Примеры curl

Создать уведомление через 30 секунд:

```bash
SEND_AT=$(date -u -v+30S +"%Y-%m-%dT%H:%M:%SZ")    # macOS
# SEND_AT=$(date -u -d "+30 seconds" +"%Y-%m-%dT%H:%M:%SZ")  # Linux

curl -s -X POST http://localhost:8080/notify \
  -H 'Content-Type: application/json' \
  -d "{
    \"channel\": \"email\",
    \"recipient\": \"alex@example.com\",
    \"subject\": \"Hi\",
    \"message\": \"Через 30 секунд это придёт\",
    \"send_at\": \"$SEND_AT\"
  }"
```

Получить статус:

```bash
curl -s http://localhost:8080/notify/<id> | jq
```

Отменить:

```bash
curl -s -X DELETE http://localhost:8080/notify/<id> -i
```

## Переменные окружения

| Имя              | Дефолт                                  | Назначение                         |
|------------------|-----------------------------------------|-------------------------------------|
| `HTTP_ADDR`      | `:8080`                                 | адрес HTTP-сервера                  |
| `UI_DIR`         | `./ui`                                  | путь к статическому UI              |
| `REDIS_ADDR`     | `localhost:6379`                        | адрес Redis                         |
| `REDIS_PASSWORD` | пусто                                   | пароль Redis                        |
| `REDIS_DB`       | `0`                                     | номер БД Redis                      |
| `REDIS_TTL_HOURS`| `72`                                    | TTL записи в Redis                  |
| `REDIS_KEY_PREFIX` | `notify:`                             | префикс ключей в Redis              |
| `RABBITMQ_URL`   | `amqp://guest:guest@localhost:5672/`    | URL RabbitMQ                        |
| `QUEUE_EXCHANGE` | `notifications.delayed`                 | имя delayed exchange                |
| `QUEUE_NAME`     | `notifications.queue`                   | имя очереди                         |
| `QUEUE_ROUTING_KEY` | `notify`                             | routing key                         |
| `QUEUE_PREFETCH` | `1`                                     | prefetch count (Qos)                |
| `WORKER_MAX_ATTEMPTS` | `5`                                | максимум попыток отправки           |
| `WORKER_BASE_BACKOFF_SEC` | `5`                            | базовый backoff (секунды)           |
| `WORKER_MAX_BACKOFF_SEC`  | `600`                          | потолок backoff (секунды)           |
| `SMTP_HOST`      | пусто (→ noop)                          | хост SMTP-сервера                   |
| `SMTP_PORT`      | `1025`                                  | порт SMTP                           |
| `SMTP_USERNAME`  | пусто                                   | логин SMTP (для MailHog не нужен)   |
| `SMTP_PASSWORD`  | пусто                                   | пароль SMTP                         |
| `SMTP_FROM`      | `noreply@notifier.local`                | поле From исходящих писем           |
| `TELEGRAM_BOT_TOKEN` | пусто (→ noop)                      | токен бота от @BotFather            |
