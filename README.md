# Payments — асинхронный сервис процессинга платежей

Микросервис принимает платежи по HTTP, обрабатывает их асинхронно через эмулятор платёжного шлюза
и сообщает клиенту результат через webhook.

**Стек:** FastAPI, Pydantic v2, SQLAlchemy 2.0 (async, asyncpg), PostgreSQL, RabbitMQ (FastStream),
Alembic, Docker Compose.

## Быстрый старт

```bash
cp .env.example .env          # необязательно: без файла используются значения по умолчанию
docker compose up --build
```

| Что | Где |
|---|---|
| API и Swagger UI | http://localhost:8000/docs |
| RabbitMQ Management | http://localhost:15672 (guest / guest) |
| Тестовый приёмник webhook | http://localhost:8001/webhook (список полученных уведомлений) |

API-ключ по умолчанию — `dev-secret-key`, переопределяется переменной `API_KEY`.

## Примеры

**Создать платёж**

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-secret-key" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "http://webhook-receiver:8001/webhook"
      }'
```

```http
HTTP/1.1 202 Accepted

{"payment_id": "6f1c…", "status": "pending", "created_at": "2026-10-14T10:00:00.123456Z"}
```

Повторный запрос с тем же `Idempotency-Key` вернёт тот же платёж. Новый платёж и новое событие при этом
не создаются.

**Получить платёж** (через 2–5 секунд статус станет `succeeded` или `failed`)

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: dev-secret-key"
```

```json
{
  "payment_id": "6f1c…",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "http://webhook-receiver:8001/webhook",
  "created_at": "2026-10-14T10:00:00.123456Z",
  "processed_at": "2026-10-14T10:00:03.456789Z"
}
```

**Webhook**, который получит клиент (`POST webhook_url`):

```json
{
  "event": "payment.succeeded",
  "payment_id": "6f1c…",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "metadata": {"order_id": 42},
  "processed_at": "2026-10-14T10:00:03.456789Z"
}
```

Заголовки `X-Event-Type` и `X-Payment-Id` нужны получателю для дедупликации: доставка at-least-once.

### Как посмотреть retry и DLQ

- Тестовый приёмник отвечает `503` на 30% запросов (`WEBHOOK_RECEIVER_FAIL_RATE`). В логах consumer'а видны
  повторы с задержками 2 → 4 → 8 секунд:
  `docker compose logs -f consumer`.
- `"webhook_url": "http://webhook-receiver:8001/webhook/fail"` всегда отвечает `500`. После трёх повторов
  сообщение окажется в очереди `payments.dlq`, а в заголовках будут `x-retry-count` и `x-last-error`
  (RabbitMQ Management → Queues → payments.dlq → Get messages).
- `"webhook_url": "http://webhook-receiver:8001/webhook/flaky?fail_times=2"` отвечает `503` на первые две
  доставки и принимает третью. Число попыток для платежа — `GET http://localhost:8001/webhook/attempts/<payment_id>`.

## Архитектура

```
             ┌──────────── одна транзакция ────────────┐
POST ──► API │ INSERT payments  +  INSERT outbox        │
             └──────────────────────────────────────────┘
                                │
                outbox-relay    │  SELECT … FOR UPDATE SKIP LOCKED
                                ▼
          exchange "payments" ──payments.new──► [payments.new] ──► consumer
                                                     │               │ 1. lock платежа, шлюз (2–5 с, 90% успех)
                                                     │               │ 2. UPDATE status
                                                     │               │ 3. POST webhook
                                                     │               │
                                                     │      ошибка ──┤ повторов < 3: payments.retry.<delay>ms
                                                     │               │ (TTL истёк → обратно в payments.new)
                                                     │               │ иначе:  payments.dlx → [payments.dlq]
                    reject (битое сообщение) ────────┴──► payments.dlx ──► [payments.dlq]
```

Сервисы в `docker-compose.yml`:

| Сервис | Назначение |
|---|---|
| `postgres`, `rabbitmq` | инфраструктура |
| `migrations` | `alembic upgrade head`, запускается один раз до старта остальных |
| `api` | FastAPI: создание и получение платежей |
| `outbox-relay` | публикует события из таблицы `outbox` в RabbitMQ |
| `consumer` | FastStream: обработка платежа и webhook |
| `webhook-receiver` | тестовый приёмник уведомлений (только для демо) |

### Структура кода

```
src/payments/
  core/          настройки (pydantic-settings), логирование
  domain/        перечисления и доменные исключения
  db/            декларативная база, модели Payment и OutboxEvent, фабрика сессий
  repositories/  доступ к данным (SQLAlchemy)
  services/      бизнес-логика: PaymentService, PaymentProcessor, OutboxRelay
  messaging/     топология RabbitMQ, контракты событий, retry/DLQ, publisher
  infra/         эмулятор шлюза, HTTP-клиент webhook
  api/           FastAPI: приложение, зависимости (auth, сессия), роутер v1
  workers/       точки входа consumer и outbox-relay
  dev/           тестовый приёмник webhook
migrations/      Alembic
tests/
```

Сервисы зависят от протоколов (`PaymentGateway`, `WebhookNotifier`, `EventPublisher`), а не от конкретных
реализаций. Поэтому в тестах их легко подменить фейками.

## Конфигурация

Все параметры задаются переменными окружения (см. [.env.example](.env.example)).

| Переменная | По умолчанию | Описание |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` | |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | |
| `API_KEY` | `dev-secret-key` | ключ для `X-API-Key` |
| `RETRY_MAX_ATTEMPTS` | `3` | число повторов до DLQ |
| `RETRY_BASE_DELAY` | `2.0` | базовая задержка, с |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` | `2.0` / `5.0` | время обработки в эмуляторе, с |
| `GATEWAY_SUCCESS_RATE` | `0.9` | доля успешных платежей |
| `WEBHOOK_TIMEOUT` | `10.0` | таймаут HTTP-запроса webhook, с |
| `OUTBOX_POLL_INTERVAL` | `1.0` | пауза relay при пустой очереди, с |
| `OUTBOX_BATCH_SIZE` | `100` | размер пачки relay |
| `CONSUMER_PREFETCH_COUNT` | `1` | prefetch consumer'а |

## Локальная разработка и тесты

```bash
uv sync
uv run pytest            # тесты не требуют Postgres/RabbitMQ: SQLite in-memory + TestRabbitBroker
uv run ruff check src tests
```

Запуск компонентов без Docker (нужны Postgres и RabbitMQ):

```bash
uv run alembic upgrade head
uv run uvicorn payments.api.app:create_app --factory --reload
uv run python -m payments.workers.outbox_relay
uv run faststream run payments.workers.consumer:app
```

Что покрыто тестами:
- API: авторизация, валидация, 202/200/404, идемпотентный повтор, запись в outbox;
- гонка двух запросов с одним `Idempotency-Key`;
- обработка платежа: success/fail, повторная доставка не списывает дважды, падение webhook не откатывает
  статус;
- outbox relay: порядок, батчи, повтор после ошибки брокера;
- retry: экспоненциальные задержки, переход в DLQ, заголовки;
- связка consumer-хендлера через `TestRabbitBroker`.
