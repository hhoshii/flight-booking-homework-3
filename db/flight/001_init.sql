CREATE TABLE flights (
    id UUID PRIMARY KEY,
    flight_number TEXT NOT NULL CHECK (length(trim(flight_number)) > 0),
    airline TEXT NOT NULL CHECK (length(trim(airline)) > 0),
    origin TEXT NOT NULL CHECK (origin ~ '^[A-Z]{3}$'),
    destination TEXT NOT NULL CHECK (destination ~ '^[A-Z]{3}$' AND destination <> origin),
    departure_time TIMESTAMPTZ NOT NULL,
    arrival_time TIMESTAMPTZ NOT NULL CHECK (arrival_time > departure_time),
    total_seats INTEGER NOT NULL CHECK (total_seats > 0),
    available_seats INTEGER NOT NULL CHECK (available_seats >= 0 AND available_seats <= total_seats),
    price BIGINT NOT NULL CHECK (price > 0),
    status TEXT NOT NULL CHECK (status IN ('SCHEDULED', 'DEPARTED', 'CANCELLED', 'COMPLETED'))
);
CREATE UNIQUE INDEX flights_number_date ON flights (flight_number, ((departure_time AT TIME ZONE 'UTC')::date));
CREATE INDEX flights_search ON flights (origin, destination, departure_time) WHERE status = 'SCHEDULED';

CREATE TABLE seat_reservations (
    id UUID PRIMARY KEY,
    flight_id UUID NOT NULL REFERENCES flights(id),
    booking_id UUID NOT NULL UNIQUE,
    seat_count INTEGER NOT NULL CHECK (seat_count > 0),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED', 'EXPIRED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
