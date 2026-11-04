package booking

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
	pb "github.com/hhoshii/flight-booking-homework-3/gen/flight/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	pool    *pgxpool.Pool
	flights pb.FlightServiceClient
}

func New(pool *pgxpool.Pool, flights pb.FlightServiceClient) *Service {
	return &Service{pool: pool, flights: flights}
}

type Booking struct {
	ID             string    `json:"id"`
	UserID         string    `json:"user_id"`
	FlightID       string    `json:"flight_id"`
	PassengerName  string    `json:"passenger_name"`
	PassengerEmail string    `json:"passenger_email"`
	SeatCount      int32     `json:"seat_count"`
	TotalPrice     int64     `json:"total_price"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
}

type CreateRequest struct {
	UserID         string `json:"user_id"`
	FlightID       string `json:"flight_id"`
	PassengerName  string `json:"passenger_name"`
	PassengerEmail string `json:"passenger_email"`
	SeatCount      int32  `json:"seat_count"`
}

const columns = "id::text,user_id::text,flight_id::text,passenger_name,passenger_email,seat_count,total_price,status,created_at"

func scanBooking(row pgx.Row) (*Booking, error) {
	b := &Booking{}
	err := row.Scan(&b.ID, &b.UserID, &b.FlightID, &b.PassengerName, &b.PassengerEmail, &b.SeatCount, &b.TotalPrice, &b.Status, &b.CreatedAt)
	return b, dbError(err)
}

func dbError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return status.Error(codes.NotFound, "booking not found")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	slog.Error("booking database operation failed", "error", err)
	return status.Error(codes.Internal, "booking database operation failed")
}

func (s *Service) create(ctx context.Context, req CreateRequest) (*Booking, error) {
	f, err := s.flights.GetFlight(ctx, &pb.GetFlightRequest{FlightId: req.FlightID})
	if err != nil {
		return nil, err
	}
	if f.Price <= 0 || f.Price > math.MaxInt64/int64(req.SeatCount) {
		return nil, status.Error(codes.InvalidArgument, "total_price is out of range")
	}
	id := uuid.NewString()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, dbError(err)
	}
	needRelease := false
	defer func() {
		tx.Rollback(context.Background())
		if needRelease {
			s.compensate(id)
		}
	}()
	_, err = s.flights.ReserveSeats(ctx, &pb.ReserveSeatsRequest{FlightId: req.FlightID, BookingId: id, SeatCount: req.SeatCount})
	if err != nil {
		code := status.Code(err)
		needRelease = code == codes.Unavailable || code == codes.DeadlineExceeded || code == codes.Canceled
		return nil, err
	}
	needRelease = true
	b, err := scanBooking(tx.QueryRow(ctx, "INSERT INTO bookings(id,user_id,flight_id,passenger_name,passenger_email,seat_count,total_price,status) VALUES($1,$2,$3,$4,$5,$6,$7,'CONFIRMED') RETURNING "+columns, id, req.UserID, req.FlightID, req.PassengerName, req.PassengerEmail, req.SeatCount, f.Price*int64(req.SeatCount)))
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		// If only the COMMIT acknowledgement was lost, preserve the committed booking.
		checkCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if saved, checkErr := s.get(checkCtx, id); checkErr == nil {
			needRelease = false
			return saved, nil
		}
		return nil, dbError(err)
	}
	needRelease = false
	return b, nil
}

func (s *Service) compensate(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := s.flights.ReleaseReservation(ctx, &pb.ReleaseReservationRequest{BookingId: id})
	if err != nil && status.Code(err) != codes.NotFound {
		slog.Error("reservation compensation failed; reconciliation required", "booking_id", id, "error", err)
	}
}

func (s *Service) get(ctx context.Context, id string) (*Booking, error) {
	return scanBooking(s.pool.QueryRow(ctx, "SELECT "+columns+" FROM bookings WHERE id=$1", id))
}

func (s *Service) list(ctx context.Context, user string) ([]*Booking, error) {
	rows, err := s.pool.Query(ctx, "SELECT "+columns+" FROM bookings WHERE user_id=$1 ORDER BY created_at DESC,id", user)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	result := []*Booking{}
	for rows.Next() {
		b, err := scanBooking(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, b)
	}
	return result, dbError(rows.Err())
}

func (s *Service) cancel(ctx context.Context, id string) (*Booking, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, dbError(err)
	}
	defer tx.Rollback(context.Background())
	b, err := scanBooking(tx.QueryRow(ctx, "SELECT "+columns+" FROM bookings WHERE id=$1 FOR UPDATE", id))
	if err != nil {
		return nil, err
	}
	if b.Status != "CONFIRMED" {
		return nil, status.Error(codes.FailedPrecondition, "booking is not confirmed")
	}
	// Execute the local write before returning seats remotely. It is still
	// uncommitted, so an RPC failure rolls it back with the rest of this tx.
	b, err = scanBooking(tx.QueryRow(ctx, "UPDATE bookings SET status='CANCELLED' WHERE id=$1 RETURNING "+columns, id))
	if err != nil {
		return nil, err
	}
	if _, err = s.flights.ReleaseReservation(ctx, &pb.ReleaseReservationRequest{BookingId: id}); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, dbError(err)
	}
	return b, nil
}
