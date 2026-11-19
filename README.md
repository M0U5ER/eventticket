# EventTicket — Платформа бронирования билетов на мероприятия

Сквозной учебный проект по дисциплине **«Конструирование программного обеспечения»** (ТПУ, ИШИТР, ОИТ, гр. 8К32).

## Архитектура

- `api-gateway` (`:8000`) — единая точка входа, агрегированный `GET /health`, Swagger UI (`http://localhost:8000/docs`)
- `event-service` (`:8001`, `event_db`) — площадки, мероприятия, схема мест, внутренний атомарный резерв (`POST /internal/seats/{seat_id}/reserve`)
- `booking-service` (`:8002`, `booking_db`) — создание броней (`Idempotency-Key`), ручная отмена (`CONFIRMED` -> `CANCELLED`, `PENDING` -> `409 Conflict`)
- `payment-service` (`:8003`, `payment_db`) — обработка оплат (`simulate_payment_fail`) и возвратов (`REFUNDED`)
- `ticket-notification-service` (`:8004`, `ticket_db`) — выпуск/отмена электронных билетов (`ET-...`) и журнал уведомлений
- `Apache Kafka (KRaft)` + `Kafka UI` (`:8080`) — 6 топиков (`booking.created`, `payment.succeeded`, `payment.failed`, `booking.cancelled`, `ticket.issued`, `eventticket.dlq`)

## Запуск стека в Docker Compose

1. Запустите Docker Desktop.
2. Поднимите все сервисы одной командой:

```powershell
docker compose up -d --build
```

3. Дождитесь статуса `healthy` и откройте:
   - Swagger UI: `http://localhost:8000/docs`
   - Healthcheck шлюза: `http://localhost:8000/health`
   - Kafka UI: `http://localhost:8080`

## Тестирование

### Unit и интеграционные тесты (без Docker)

```powershell
.\.venv\Scripts\python.exe -m pytest tests\ -v --ignore=tests\e2e_demo.py --ignore=tests\test_tools.py
python -m pytest tests\test_tools.py -v
```

### Сквозной E2E-прогон (против поднятого Docker Compose)

```powershell
.\.venv\Scripts\python.exe tests\e2e_demo.py
```

Скрипт проверяет все 6 сценариев защиты (создание мероприятия, успешная бронь + `Idempotency-Key`, конфликт `409` при двойном бронировании, компенсация при отказе оплаты, ручная отмена с возвратом средств, отправка битого JSON в `eventticket.dlq`) и сохраняет протокол в `labs/lab5-microservices/evidence/e2e-log.txt`.
