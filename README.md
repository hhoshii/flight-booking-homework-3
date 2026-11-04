# Flight Booking — ДЗ №3

Учебная система бронирования на Go: Booking Service (REST) вызывает Flight Service по gRPC. У сервисов отдельные PostgreSQL. Flight кеширует рейсы и поиск в Redis с Sentinel. Реализованы пункты 1–10 задания.

## Запуск

Нужен Docker с Compose. Из корня проекта:

```sh
docker compose up --build -d --wait
curl http://localhost:8080/health
```

Ответ: `{"status":"ok"}`. Миграции обоих сервисов применяются автоматически. `.env` для первого запуска не нужен: в Compose есть учебные значения по умолчанию. При необходимости скопируй `.env.example` в `.env` и измени настройки. Эти пароли и API key предназначены для локальной работы.

Публичный адрес — `http://localhost:8080`. PostgreSQL, Redis и gRPC не публикуются на хост.

## Демонстрация

```sh
python3 scripts/demo.py
```

Скрипт ищет рейс, создаёт бронь на два места, получает её и список пользователя, отменяет и проверяет возврат мест: `100 → 98 → 100` на свежей БД. Бронь остаётся в БД со статусом CANCELLED.

Учебный рейс: `11111111-1111-4111-8111-111111111111`, SVO → LED, 1 декабря 2026 года, вылет в 09:00 UTC. Цена — 500000 копеек (5000 RUB). Есть ещё рейс на 2 декабря и отменённый рейс: он не попадает в поиск.

Все цены (`price`, `total_price`) — целые копейки RUB. В REST даты имеют формат ISO 8601, дата поиска — `YYYY-MM-DD`; календарный день определяется по UTC.

```sh
curl 'http://localhost:8080/flights?origin=SVO&destination=LED&date=2026-12-01'

curl -i -X POST http://localhost:8080/bookings \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","flight_id":"11111111-1111-4111-8111-111111111111","passenger_name":"Ivan Ivanov","passenger_email":"ivan@example.com","seat_count":2}'
```

В ответе на создание — HTTP 201, `id` брони, `total_price: 1000000`, `status: CONFIRMED`, время создания и остальные поля. Полученный ID используй ниже вместо `BOOKING_ID`.

```sh
curl http://localhost:8080/bookings/BOOKING_ID
curl 'http://localhost:8080/bookings?user_id=aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
curl -X POST http://localhost:8080/bookings/BOOKING_ID/cancel
curl http://localhost:8080/flights/11111111-1111-4111-8111-111111111111
```

## API и критерии

| REST | Поведение |
| --- | --- |
| `GET /flights?origin=...&destination=...&date=...` | Маршрут обязателен, дата необязательна; массив SCHEDULED-рейсов |
| `GET /flights/{id}` | Рейс; 404, если отсутствует |
| `POST /bookings` | Бронь CONFIRMED после GetFlight и успешного ReserveSeats |
| `GET /bookings/{id}` | Бронь; 404, если отсутствует |
| `GET /bookings?user_id=...` | Массив броней указанного пользователя |
| `POST /bookings/{id}/cancel` | Возврат мест, CANCELLED; повторная отмена — 409 |

Ошибки имеют вид `{"error":{"code":"ResourceExhausted","message":"not enough available seats"}}`. Валидация — 400, отсутствующая сущность — 404, нехватка мест/неверный статус — 409, Flight недоступен или circuit открыт — 503, исчерпаны таймауты — 504. Ошибка внутренних credentials — 502. Пользовательская регистрация и JWT не требуются заданием; `user_id` передаётся клиентом.

| Балл | Реализация |
| --- | --- |
| 1. gRPC-контракт | [flight.proto](api/flight/v1/flight.proto): SearchFlights, GetFlight, ReserveSeats, ReleaseReservation; Timestamp, enum, ошибки. Go-код [сгенерирован protoc](gen/flight/v1); генерация повторяется при Docker-сборке |
| 2. ER и 3НФ | [ER-диаграмма и пояснения](docs/architecture.md), PK/FK/UNIQUE/CHECK в [SQL](db) |
| 3. PostgreSQL и сервисы | Два контейнера PostgreSQL; версионные SQL-миграции при старте; Booking REST и Flight gRPC |
| 4. Взаимодействие | GetFlight → ReserveSeats → сохранение CONFIRMED; отмена через ReleaseReservation |
| 5. Транзакции | SELECT FOR UPDATE; места и резервация в одной транзакции; компенсация при ошибке сохранения Booking |
| 6. Аутентификация | `x-api-key` в metadata; unary/stream interceptors; `GRPC_API_KEY` из environment; неверный ключ → UNAUTHENTICATED |
| 7. Redis | Cache-Aside, TTL 300 секунд, ключи рейса и поиска, инвалидация Reserve/Release/UpdateFlight, логи hit/miss |
| 8. Retry | Всего до трёх попыток, паузы 100/200 мс, только UNAVAILABLE/DEADLINE_EXCEEDED; UNIQUE booking_id и идемпотентные мутации |
| 9. Redis HA | Master, replica, три Sentinel, quorum 2; `go-redis.NewFailoverClient` |
| 10. Circuit Breaker | Клиентский interceptor, CLOSED/OPEN/HALF_OPEN, одна физическая попытка probe без retry, учёт сетевых и серверных ошибок, параметры из environment, логи переходов, быстрый 503 |

`UpdateFlight` — небольшой дополнительный RPC изменения цены и статуса, используемый для проверки инвалидации. TTL резерваций и планировщик их истечения в условии не заданы; EXPIRED предусмотрен enum и SQL.

## Проверки

После запуска системы:

```sh
docker compose --profile test run --rm test
```

Команда запускает тесты с `-race`. Интеграционные сценарии создают отдельные временные схемы в каждой БД и удаляют их после теста; для кеша используют Redis DB 1 и 2. Проверяются полный REST/gRPC-сценарий, цена-снимок, валидация и ошибки, транзакционный rollback, компенсация, гонка за последнее место, идемпотентность, TTL, инвалидация, аутентификация, retry и circuit breaker.

Автоматическое переключение Redis в запущенном Compose:

```sh
python3 scripts/check_failover.py
```

Скрипт временно останавливает `redis-master`, дожидается продвижения `redis-replica`, проверяет REST и запускает исходный Redis обратно. Нужен Python 3. После переключения имена контейнеров прежние, но их роли определяет Sentinel.

С установленным Go 1.27:

```sh
make test
make build
```

Без `TEST_FLIGHT_DATABASE_URL`/`TEST_BOOKING_DATABASE_URL` интеграционные сценарии пропускаются. Для отдельного запуска задай также `TEST_REDIS_ADDR` либо `TEST_REDIS_SENTINELS`. Полный отчёт фактической проверки: [docs/verification.md](docs/verification.md).

Для повторной генерации нужны `protoc`, `protoc-gen-go@v1.36.6` и `protoc-gen-go-grpc@v1.5.1`:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
make generate
```

## Настройки, логи и остановка

`GRPC_ATTEMPT_TIMEOUT` — таймаут одной попытки (1s). Circuit breaker: `CB_FAILURE_THRESHOLD=5`, `CB_OPEN_TIMEOUT=10s`, `CB_FAILURE_WINDOW=30s`. API key, пароли БД и HTTP-порт — в `.env.example`.

```sh
docker compose logs -f booking-service flight-service sentinel-1
docker compose down
```

Обычный `down` сохраняет PostgreSQL volumes. `down -v` удаляет учебные данные.

План, составленный до реализации: [docs/plan.md](docs/plan.md). Границы сервисов, ER, транзакции, кеш и принятые упрощения: [docs/architecture.md](docs/architecture.md). Использованные скиллы: [docs/skills.md](docs/skills.md).
