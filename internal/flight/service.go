package flight

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"time"

	"github.com/google/uuid"
	pb "github.com/hhoshii/flight-booking-homework-3/gen/flight/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Service struct {
	pb.UnimplementedFlightServiceServer
	pool  *pgxpool.Pool
	cache *Cache
}

func New(pool *pgxpool.Pool, cache *Cache) *Service { return &Service{pool: pool, cache: cache} }

const flightColumns = "id::text,flight_number,airline,origin,destination,departure_time,arrival_time,total_seats,available_seats,price,status"
const reservationColumns = "id::text,flight_id::text,booking_id::text,seat_count,status,created_at"

var iata = regexp.MustCompile(`^[A-Z]{3}$`)

func scanFlight(row pgx.Row) (*pb.Flight, error) {
	f := &pb.Flight{}
	var departure, arrival time.Time
	var state string
	err := row.Scan(&f.Id, &f.FlightNumber, &f.Airline, &f.Origin, &f.Destination, &departure, &arrival, &f.TotalSeats, &f.AvailableSeats, &f.Price, &state)
	if err != nil {
		return nil, dbError(err)
	}
	f.DepartureTime = timestamppb.New(departure)
	f.ArrivalTime = timestamppb.New(arrival)
	f.Status = pb.FlightStatus(pb.FlightStatus_value[state])
	return f, nil
}

func scanReservation(row pgx.Row) (*pb.SeatReservation, error) {
	r := &pb.SeatReservation{}
	var created time.Time
	var state string
	err := row.Scan(&r.Id, &r.FlightId, &r.BookingId, &r.SeatCount, &state, &created)
	if err != nil {
		return nil, dbError(err)
	}
	r.CreatedAt = timestamppb.New(created)
	r.Status = pb.ReservationStatus(pb.ReservationStatus_value[state])
	return r, nil
}

func dbError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return status.Error(codes.NotFound, "record not found")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	slog.Error("flight database operation failed", "error", err)
	return status.Error(codes.Internal, "flight database operation failed")
}

func parseID(value string) (string, error) {
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil {
		return "", status.Error(codes.InvalidArgument, "non-zero UUID required")
	}
	return id.String(), nil
}

func (s *Service) GetFlight(ctx context.Context, req *pb.GetFlightRequest) (*pb.Flight, error) {
	id, err := parseID(req.FlightId)
	if err != nil {
		return nil, err
	}
	s.cache.mu.RLock()
	defer s.cache.mu.RUnlock()
	f := &pb.Flight{}
	if s.cache.get(ctx, "flight:"+id, f) {
		return f, nil
	}
	f, err = scanFlight(s.pool.QueryRow(ctx, "SELECT "+flightColumns+" FROM flights WHERE id=$1", id))
	if err == nil {
		s.cache.put(ctx, "flight:"+id, f)
	}
	return f, err
}

func (s *Service) SearchFlights(ctx context.Context, req *pb.SearchFlightsRequest) (*pb.SearchFlightsResponse, error) {
	if !iata.MatchString(req.Origin) || !iata.MatchString(req.Destination) || req.Origin == req.Destination {
		return nil, status.Error(codes.InvalidArgument, "different uppercase IATA airport codes are required")
	}
	date := "all"
	args := []any{req.Origin, req.Destination}
	query := "SELECT " + flightColumns + " FROM flights WHERE origin=$1 AND destination=$2 AND status='SCHEDULED'"
	if req.Date != nil {
		if req.Date.CheckValid() != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid date")
		}
		day := req.Date.AsTime().UTC()
		if !day.Equal(day.Truncate(24 * time.Hour)) {
			return nil, status.Error(codes.InvalidArgument, "date must be UTC midnight")
		}
		date = day.Format("2006-01-02")
		args = append(args, day, day.AddDate(0, 0, 1))
		query += " AND departure_time >= $3 AND departure_time < $4"
	}
	query += " ORDER BY departure_time,id"
	s.cache.mu.RLock()
	defer s.cache.mu.RUnlock()
	key := searchKey(req.Origin, req.Destination, date)
	result := &pb.SearchFlightsResponse{}
	if s.cache.get(ctx, key, result) {
		return result, nil
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	for rows.Next() {
		f, err := scanFlight(rows)
		if err != nil {
			return nil, err
		}
		result.Flights = append(result.Flights, f)
	}
	if err = rows.Err(); err != nil {
		return nil, dbError(err)
	}
	s.cache.put(ctx, key, result)
	return result, nil
}

func (s *Service) commit(ctx context.Context, tx pgx.Tx, f *pb.Flight) error {
	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()
	err := tx.Commit(ctx)
	s.cache.invalidate(f)
	return dbError(err)
}

func lockBooking(ctx context.Context, tx pgx.Tx, id string) error {
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", id)
	return dbError(err)
}

func (s *Service) ReserveSeats(ctx context.Context, req *pb.ReserveSeatsRequest) (*pb.SeatReservation, error) {
	flightID, err := parseID(req.FlightId)
	if err != nil {
		return nil, err
	}
	bookingID, err := parseID(req.BookingId)
	if err != nil {
		return nil, err
	}
	if req.SeatCount <= 0 {
		return nil, status.Error(codes.InvalidArgument, "seat_count must be positive")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, dbError(err)
	}
	defer tx.Rollback(context.Background())
	if err = lockBooking(ctx, tx, bookingID); err != nil {
		return nil, err
	}
	r, err := scanReservation(tx.QueryRow(ctx, "SELECT "+reservationColumns+" FROM seat_reservations WHERE booking_id=$1", bookingID))
	if err == nil {
		if r.FlightId != flightID || r.SeatCount != req.SeatCount {
			return nil, status.Error(codes.AlreadyExists, "booking_id already has different reservation arguments")
		}
		if r.Status != pb.ReservationStatus_ACTIVE {
			return nil, status.Error(codes.FailedPrecondition, "reservation is no longer active")
		}
		return r, nil
	}
	if status.Code(err) != codes.NotFound {
		return nil, err
	}
	f, err := scanFlight(tx.QueryRow(ctx, "SELECT "+flightColumns+" FROM flights WHERE id=$1 FOR UPDATE", flightID))
	if err != nil {
		return nil, err
	}
	if f.Status != pb.FlightStatus_SCHEDULED {
		return nil, status.Error(codes.FailedPrecondition, "flight is not scheduled")
	}
	if f.AvailableSeats < req.SeatCount {
		return nil, status.Error(codes.ResourceExhausted, "not enough available seats")
	}
	if _, err = tx.Exec(ctx, "UPDATE flights SET available_seats=available_seats-$2 WHERE id=$1", flightID, req.SeatCount); err != nil {
		return nil, dbError(err)
	}
	r, err = scanReservation(tx.QueryRow(ctx, "INSERT INTO seat_reservations(id,flight_id,booking_id,seat_count,status) VALUES($1,$2,$3,$4,'ACTIVE') RETURNING "+reservationColumns, uuid.NewString(), flightID, bookingID, req.SeatCount))
	if err != nil {
		return nil, err
	}
	if err = s.commit(ctx, tx, f); err != nil {
		return nil, err
	}
	return r, nil
}

func (s *Service) ReleaseReservation(ctx context.Context, req *pb.ReleaseReservationRequest) (*pb.SeatReservation, error) {
	bookingID, err := parseID(req.BookingId)
	if err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, dbError(err)
	}
	defer tx.Rollback(context.Background())
	if err = lockBooking(ctx, tx, bookingID); err != nil {
		return nil, err
	}
	r, err := scanReservation(tx.QueryRow(ctx, "SELECT "+reservationColumns+" FROM seat_reservations WHERE booking_id=$1", bookingID))
	if err != nil {
		return nil, err
	}
	if r.Status != pb.ReservationStatus_ACTIVE {
		return r, nil
	}
	f, err := scanFlight(tx.QueryRow(ctx, "SELECT "+flightColumns+" FROM flights WHERE id=$1 FOR UPDATE", r.FlightId))
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, "UPDATE flights SET available_seats=available_seats+$2 WHERE id=$1", r.FlightId, r.SeatCount); err != nil {
		return nil, dbError(err)
	}
	r, err = scanReservation(tx.QueryRow(ctx, "UPDATE seat_reservations SET status='RELEASED' WHERE booking_id=$1 RETURNING "+reservationColumns, bookingID))
	if err != nil {
		return nil, err
	}
	if err = s.commit(ctx, tx, f); err != nil {
		return nil, err
	}
	return r, nil
}

func (s *Service) UpdateFlight(ctx context.Context, req *pb.UpdateFlightRequest) (*pb.Flight, error) {
	id, err := parseID(req.FlightId)
	if err != nil {
		return nil, err
	}
	if req.Price <= 0 || req.Status < pb.FlightStatus_SCHEDULED || req.Status > pb.FlightStatus_COMPLETED {
		return nil, status.Error(codes.InvalidArgument, "positive price and valid flight status required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, dbError(err)
	}
	defer tx.Rollback(context.Background())
	f, err := scanFlight(tx.QueryRow(ctx, "UPDATE flights SET price=$2,status=$3 WHERE id=$1 RETURNING "+flightColumns, id, req.Price, req.Status.String()))
	if err != nil {
		return nil, err
	}
	if err = s.commit(ctx, tx, f); err != nil {
		return nil, err
	}
	return f, nil
}
