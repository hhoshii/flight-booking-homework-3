package booking

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/mail"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	pb "github.com/hhoshii/flight-booking-homework-3/gen/flight/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := s.pool.Ping(ctx); err != nil {
			writeError(w, status.Error(codes.Unavailable, "booking database is unavailable"))
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /flights", s.searchFlights)
	mux.HandleFunc("GET /flights/{id}", s.getFlight)
	mux.HandleFunc("POST /bookings", s.createBooking)
	mux.HandleFunc("GET /bookings/{id}", s.getBooking)
	mux.HandleFunc("GET /bookings", s.listBookings)
	mux.HandleFunc("POST /bookings/{id}/cancel", s.cancelBooking)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &responseWriter{ResponseWriter: w, code: 200}
		mux.ServeHTTP(rec, r)
		slog.Info("http request", "method", r.Method, "path", r.URL.Path, "status", rec.code, "duration", time.Since(start))
	})
}

type responseWriter struct {
	http.ResponseWriter
	code int
}

func (w *responseWriter) WriteHeader(code int) { w.code = code; w.ResponseWriter.WriteHeader(code) }

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	s := status.Convert(err)
	code := 500
	switch s.Code() {
	case codes.InvalidArgument:
		code = 400
	case codes.NotFound:
		code = 404
	case codes.AlreadyExists, codes.FailedPrecondition, codes.ResourceExhausted:
		code = 409
	case codes.Unauthenticated, codes.PermissionDenied:
		code = 502
	case codes.Unavailable:
		code = 503
	case codes.DeadlineExceeded:
		code = 504
	case codes.Canceled:
		code = 408
	}
	writeJSON(w, code, map[string]any{"error": map[string]string{"code": s.Code().String(), "message": s.Message()}})
}

func validID(value string) (string, error) {
	id, e := uuid.Parse(value)
	if e != nil || id == uuid.Nil {
		return "", status.Error(codes.InvalidArgument, "non-zero UUID required")
	}
	return id.String(), nil
}

func (s *Service) searchFlights(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	req := &pb.SearchFlightsRequest{Origin: q.Get("origin"), Destination: q.Get("destination")}
	if raw := q.Get("date"); raw != "" {
		date, err := time.Parse("2006-01-02", raw)
		if err != nil {
			writeError(w, status.Error(codes.InvalidArgument, "date must be YYYY-MM-DD"))
			return
		}
		req.Date = timestamppb.New(date)
	}
	result, err := s.flights.SearchFlights(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	flights := make([]flightView, 0, len(result.Flights))
	for _, f := range result.Flights {
		flights = append(flights, viewFlight(f))
	}
	writeJSON(w, 200, flights)
}

func (s *Service) getFlight(w http.ResponseWriter, r *http.Request) {
	id, err := validID(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	f, err := s.flights.GetFlight(r.Context(), &pb.GetFlightRequest{FlightId: id})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, viewFlight(f))
}

func (s *Service) createBooking(w http.ResponseWriter, r *http.Request) {
	var req CreateRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(w, status.Error(codes.InvalidArgument, "invalid JSON body"))
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		writeError(w, status.Error(codes.InvalidArgument, "exactly one JSON object is required"))
		return
	}
	var err error
	if req.UserID, err = validID(req.UserID); err != nil {
		writeError(w, err)
		return
	}
	if req.FlightID, err = validID(req.FlightID); err != nil {
		writeError(w, err)
		return
	}
	req.PassengerName = strings.TrimSpace(req.PassengerName)
	req.PassengerEmail = strings.TrimSpace(req.PassengerEmail)
	email, emailErr := mail.ParseAddress(req.PassengerEmail)
	if req.SeatCount <= 0 || utf8.RuneCountInString(req.PassengerName) < 1 || utf8.RuneCountInString(req.PassengerName) > 200 || len(req.PassengerEmail) > 254 || emailErr != nil || email.Address != req.PassengerEmail {
		writeError(w, status.Error(codes.InvalidArgument, "positive seat_count, passenger name (1-200 characters) and valid email are required"))
		return
	}
	b, err := s.create(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Location", "/bookings/"+b.ID)
	writeJSON(w, 201, b)
}

func (s *Service) getBooking(w http.ResponseWriter, r *http.Request) {
	id, err := validID(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	b, err := s.get(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, b)
}

func (s *Service) listBookings(w http.ResponseWriter, r *http.Request) {
	user, err := validID(r.URL.Query().Get("user_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	list, err := s.list(r.Context(), user)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, list)
}

func (s *Service) cancelBooking(w http.ResponseWriter, r *http.Request) {
	id, err := validID(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	b, err := s.cancel(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, b)
}
