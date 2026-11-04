CREATE TABLE bookings (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    flight_id UUID NOT NULL,
    passenger_name TEXT NOT NULL CHECK (length(trim(passenger_name)) BETWEEN 1 AND 200),
    passenger_email TEXT NOT NULL CHECK (length(passenger_email) BETWEEN 3 AND 254),
    seat_count INTEGER NOT NULL CHECK (seat_count > 0),
    total_price BIGINT NOT NULL CHECK (total_price > 0),
    status TEXT NOT NULL CHECK (status IN ('CONFIRMED', 'CANCELLED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX bookings_user ON bookings(user_id, created_at);
