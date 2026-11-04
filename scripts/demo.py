#!/usr/bin/env python3
"""A narrated REST demonstration against the running Booking Service."""
import json
import os
import shlex
import sys
import urllib.error
import urllib.request
import uuid

BASE = os.environ.get("BASE_URL", "http://localhost:8080").rstrip("/")


def request(method, path, body=None, *, step, expected_status=200):
    url = BASE + path
    print(f"\n{step}")
    command = ["curl", "-i", "-X", method,
               "-H", "Content-Type: application/json"]
    data = None
    if body is not None:
        encoded = json.dumps(body, ensure_ascii=False)
        data = encoded.encode("utf-8")
        command.extend(["--data-raw", encoded])
        print("Тело запроса:")
        print(json.dumps(body, ensure_ascii=False, indent=2))
    command.append(url)
    print(shlex.join(command), flush=True)

    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        response = urllib.request.urlopen(req, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        status = response.getcode()
        raw = response.read().decode("utf-8")
    print(f"Ответ: HTTP {status}")
    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        print(raw, flush=True)
        raise
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    if status != expected_status:
        raise RuntimeError(
            f"{method} {path}: ожидался HTTP {expected_status}, получен HTTP {status}")
    return result


def main():
    print("Демонстрация бронирования и отмены через REST API")
    print(f"Адрес API: {BASE}")
    print("Для каждого шага показаны команда curl и фактический ответ сервиса.")
    print("Все денежные суммы в JSON указаны в копейках.")

    health = request("GET", "/health",
                     step="[1/9] Проверяем доступность Booking Service")
    assert health["status"] == "ok", "Сервис не подтвердил готовность"
    print("Проверка OK: API доступен.")

    flights = request("GET", "/flights?origin=SVO&destination=LED&date=2026-12-01",
                      step="[2/9] Ищем рейсы SVO → LED на 1 декабря 2026 года")
    assert flights, "Учебный рейс не найден"
    flight = flights[0]
    print(f"Проверка OK: найдено рейсов — {len(flights)}; выбран рейс {flight['id']}.")

    before = request("GET", "/flights/" + flight["id"],
                     step="[3/9] Читаем рейс и запоминаем исходное количество мест")["available_seats"]
    print(f"Исходное количество свободных мест: {before}.")

    user = str(uuid.uuid4())
    booking = request("POST", "/bookings", {
        "user_id": user, "flight_id": flight["id"],
        "passenger_name": "Ivan Ivanov", "passenger_email": "ivan@example.com", "seat_count": 2,
    }, step="[4/9] Создаём бронирование на два места", expected_status=201)
    try:
        assert booking["status"] == "CONFIRMED", "Бронирование не подтверждено"
        print(f"Проверка OK: создана подтверждённая бронь {booking['id']}.")

        reserved = request("GET", "/flights/" + flight["id"],
                           step="[5/9] Проверяем, что бронирование заняло ровно два места")["available_seats"]
        assert reserved == before - 2, f"Ожидалось {before - 2} мест, получено {reserved}"
        print(f"Проверка OK: свободные места {before} → {reserved}.")

        saved = request("GET", "/bookings/" + booking["id"],
                        step="[6/9] Читаем сохранённую бронь и проверяем статус и сумму")
        assert saved["id"] == booking["id"], "Вернулась другая бронь"
        assert saved["total_price"] == flight["price"] * 2, "Сумма не равна цене двух мест"
        assert saved["status"] == "CONFIRMED", "Сохранённая бронь не подтверждена"
        print(f"Проверка OK: статус CONFIRMED; сумма {flight['price']} × 2 = {saved['total_price']} копеек.")

        bookings = request("GET", "/bookings?user_id=" + user,
                           step="[7/9] Получаем список бронирований созданного пользователя")
        assert len(bookings) == 1, f"Ожидалась одна бронь, получено {len(bookings)}"
        assert bookings[0]["id"] == booking["id"], "В списке отсутствует созданная бронь"
        print("Проверка OK: в списке пользователя ровно одна созданная бронь.")
    finally:
        cancelled = request("POST", "/bookings/" + booking["id"] + "/cancel",
                            step="[8/9] Отменяем созданную бронь и возвращаем места")
        assert cancelled["status"] == "CANCELLED", "Бронь не перешла в CANCELLED"
        print("Проверка OK: бронь отменена, статус CANCELLED.")

    after = request("GET", "/flights/" + flight["id"],
                    step="[9/9] Проверяем восстановление исходного количества мест")["available_seats"]
    assert after == before, f"После отмены ожидалось {before} мест, получено {after}"
    print(f"Проверка OK: свободные места {reserved} → {after}; исходное количество восстановлено.")

    print("\nСценарий бронирования и отмены успешно завершён.")
    print(f"OK: seats {before} -> {reserved} -> {after}")
    print(f"Booking: {booking['id']}; total_price={booking['total_price']} kopecks")


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, RuntimeError, urllib.error.URLError, TimeoutError, ValueError) as error:
        print(f"\nОШИБКА ДЕМОНСТРАЦИИ: {error}", file=sys.stderr)
        sys.exit(1)
