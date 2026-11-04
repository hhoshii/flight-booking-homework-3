package flight

import (
	"context"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hhoshii/flight-booking-homework-3/db"
	pb "github.com/hhoshii/flight-booking-homework-3/gen/flight/v1"
	"github.com/hhoshii/flight-booking-homework-3/internal/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFlightIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_FLIGHT_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_FLIGHT_DATABASE_URL and TEST_REDIS_ADDR for integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "test_" + uuid.New().String()[:8]
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = db.Migrate(ctx, pool, "flight"); err != nil {
		t.Fatal(err)
	}
	if err = db.Migrate(ctx, pool, "flight"); err != nil {
		t.Fatal("migration is not repeatable", err)
	}
	var rdb redis.UniversalClient
	if sentinels := os.Getenv("TEST_REDIS_SENTINELS"); sentinels != "" {
		rdb = redis.NewFailoverClient(&redis.FailoverOptions{MasterName: "flight-cache", SentinelAddrs: strings.Split(sentinels, ","), DB: 1})
	} else {
		rdb = redis.NewClient(&redis.Options{Addr: os.Getenv("TEST_REDIS_ADDR"), DB: 1})
	}
	defer rdb.Close()
	if err = rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(rpc.ServerAuth("test-key")))
	pb.RegisterFlightServiceServer(server, New(pool, NewCache(rdb)))
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewFlightServiceClient(conn)
	auth := metadata.AppendToOutgoingContext(ctx, "x-api-key", "test-key")
	id := "11111111-1111-4111-8111-111111111111"

	t.Run("auth all methods", func(t *testing.T) {
		calls := []func() error{
			func() error { _, e := client.GetFlight(ctx, &pb.GetFlightRequest{}); return e },
			func() error { _, e := client.SearchFlights(ctx, &pb.SearchFlightsRequest{}); return e },
			func() error { _, e := client.ReserveSeats(ctx, &pb.ReserveSeatsRequest{}); return e },
			func() error { _, e := client.ReleaseReservation(ctx, &pb.ReleaseReservationRequest{}); return e },
			func() error { _, e := client.UpdateFlight(ctx, &pb.UpdateFlightRequest{}); return e },
		}
		for _, call := range calls {
			if e := call(); status.Code(e) != codes.Unauthenticated {
				t.Fatal(e)
			}
		}
	})
	t.Run("search get and cache ttl", func(t *testing.T) {
		date := timestamppb.New(time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC))
		list, e := client.SearchFlights(auth, &pb.SearchFlightsRequest{Origin: "SVO", Destination: "LED", Date: date})
		if e != nil {
			t.Fatal(e)
		}
		if len(list.Flights) != 1 {
			t.Fatalf("flights=%d", len(list.Flights))
		}
		list, e = client.SearchFlights(auth, &pb.SearchFlightsRequest{Origin: "SVO", Destination: "LED"})
		if e != nil || len(list.Flights) != 2 {
			t.Fatalf("without date: %v %v", list, e)
		}
		for i := 0; i < 2; i++ {
			f, e := client.GetFlight(auth, &pb.GetFlightRequest{FlightId: id})
			if e != nil {
				t.Fatal(e)
			}
			if f.AvailableSeats != 100 || f.Price != 500000 {
				t.Fatal(f)
			}
		}
		for _, key := range []string{"flight:" + id, "search:SVO:LED:2026-12-01", "search:SVO:LED:all"} {
			ttl, e := rdb.TTL(ctx, key).Result()
			if e != nil || ttl <= 0 || ttl > 300*time.Second {
				t.Fatalf("%s ttl=%s error=%v", key, ttl, e)
			}
		}
	})
	t.Run("reserve is idempotent invalidates search and releases once", func(t *testing.T) {
		booking := uuid.NewString()
		req := &pb.ReserveSeatsRequest{FlightId: id, BookingId: booking, SeatCount: 2}
		a, e := client.ReserveSeats(auth, req)
		if e != nil {
			t.Fatal(e)
		}
		for _, key := range []string{"flight:" + id, "search:SVO:LED:all", "search:SVO:LED:2026-12-01"} {
			if count, e := rdb.Exists(ctx, key).Result(); e != nil || count != 0 {
				t.Fatalf("stale key %s: count=%d error=%v", key, count, e)
			}
		}
		b, e := client.ReserveSeats(auth, req)
		if e != nil || a.Id != b.Id {
			t.Fatalf("repeat: %v %v", b, e)
		}
		f, e := client.GetFlight(auth, &pb.GetFlightRequest{FlightId: id})
		if e != nil || f.AvailableSeats != 98 {
			t.Fatalf("%v %v", f, e)
		}
		list, e := client.SearchFlights(auth, &pb.SearchFlightsRequest{Origin: "SVO", Destination: "LED"})
		if e != nil || list.Flights[0].AvailableSeats != 98 {
			t.Fatalf("stale search: %v %v", list, e)
		}
		_, e = client.ReserveSeats(auth, &pb.ReserveSeatsRequest{FlightId: id, BookingId: booking, SeatCount: 3})
		if status.Code(e) != codes.AlreadyExists {
			t.Fatal(e)
		}
		for i := 0; i < 2; i++ {
			r, e := client.ReleaseReservation(auth, &pb.ReleaseReservationRequest{BookingId: booking})
			if e != nil || r.Status != pb.ReservationStatus_RELEASED {
				t.Fatalf("%v %v", r, e)
			}
		}
		f, e = client.GetFlight(auth, &pb.GetFlightRequest{FlightId: id})
		if e != nil || f.AvailableSeats != 100 {
			t.Fatalf("double release: %v %v", f, e)
		}
		_, e = client.ReserveSeats(auth, req)
		if status.Code(e) != codes.FailedPrecondition {
			t.Fatal("resurrected released reservation", e)
		}
	})
	t.Run("reservation insert failure rolls back seats", func(t *testing.T) {
		if _, e := pool.Exec(ctx, "ALTER TABLE seat_reservations ADD CONSTRAINT reject_large CHECK (seat_count < 3) NOT VALID"); e != nil {
			t.Fatal(e)
		}
		defer pool.Exec(ctx, "ALTER TABLE seat_reservations DROP CONSTRAINT reject_large")
		booking := uuid.NewString()
		_, e := client.ReserveSeats(auth, &pb.ReserveSeatsRequest{FlightId: id, BookingId: booking, SeatCount: 3})
		if status.Code(e) != codes.Internal {
			t.Fatalf("expected forced insert failure: %v", e)
		}
		var seats, count int
		if e = pool.QueryRow(ctx, "SELECT available_seats FROM flights WHERE id=$1", id).Scan(&seats); e != nil {
			t.Fatal(e)
		}
		if e = pool.QueryRow(ctx, "SELECT count(*) FROM seat_reservations WHERE booking_id=$1", booking).Scan(&count); e != nil {
			t.Fatal(e)
		}
		if seats != 100 || count != 0 {
			t.Fatalf("partial transaction: seats=%d reservations=%d", seats, count)
		}
	})
	t.Run("last seat race and rollback", func(t *testing.T) {
		last := uuid.NewString()
		_, e := pool.Exec(ctx, `INSERT INTO flights VALUES($1,'LAST1','Test','VKO','LED','2026-12-03T00:00:00Z','2026-12-03T01:00:00Z',1,1,100,'SCHEDULED')`, last)
		if e != nil {
			t.Fatal(e)
		}
		var wins atomic.Int32
		var wg sync.WaitGroup
		for i := 0; i < 12; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, e := client.ReserveSeats(auth, &pb.ReserveSeatsRequest{FlightId: last, BookingId: uuid.NewString(), SeatCount: 1})
				if e == nil {
					wins.Add(1)
				} else if status.Code(e) != codes.ResourceExhausted {
					t.Errorf("unexpected %v", e)
				}
			}()
		}
		wg.Wait()
		var seats, res int
		if e = pool.QueryRow(ctx, "SELECT available_seats FROM flights WHERE id=$1", last).Scan(&seats); e != nil {
			t.Fatal(e)
		}
		if e = pool.QueryRow(ctx, "SELECT count(*) FROM seat_reservations WHERE flight_id=$1", last).Scan(&res); e != nil {
			t.Fatal(e)
		}
		if wins.Load() != 1 || seats != 0 || res != 1 {
			t.Fatalf("wins=%d seats=%d reservations=%d", wins.Load(), seats, res)
		}
	})
	t.Run("update invalidates both search variants", func(t *testing.T) {
		date := timestamppb.New(time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC))
		if _, e := client.SearchFlights(auth, &pb.SearchFlightsRequest{Origin: "SVO", Destination: "LED", Date: date}); e != nil {
			t.Fatal(e)
		}
		_, e := client.UpdateFlight(auth, &pb.UpdateFlightRequest{FlightId: id, Price: 700000, Status: pb.FlightStatus_CANCELLED})
		if e != nil {
			t.Fatal(e)
		}
		f, e := client.GetFlight(auth, &pb.GetFlightRequest{FlightId: id})
		if e != nil || f.Price != 700000 || f.Status != pb.FlightStatus_CANCELLED {
			t.Fatalf("%v %v", f, e)
		}
		list, e := client.SearchFlights(auth, &pb.SearchFlightsRequest{Origin: "SVO", Destination: "LED"})
		if e != nil || len(list.Flights) != 1 {
			t.Fatalf("%v %v", list, e)
		}
		list, e = client.SearchFlights(auth, &pb.SearchFlightsRequest{Origin: "SVO", Destination: "LED", Date: date})
		if e != nil || len(list.Flights) != 0 {
			t.Fatalf("stale dated search: %v %v", list, e)
		}
		_, e = client.ReserveSeats(auth, &pb.ReserveSeatsRequest{FlightId: id, BookingId: uuid.NewString(), SeatCount: 1})
		if status.Code(e) != codes.FailedPrecondition {
			t.Fatal(e)
		}
	})
	t.Run("business errors", func(t *testing.T) {
		_, e := client.GetFlight(auth, &pb.GetFlightRequest{FlightId: uuid.NewString()})
		if status.Code(e) != codes.NotFound {
			t.Fatal(e)
		}
		_, e = client.GetFlight(auth, &pb.GetFlightRequest{FlightId: "bad"})
		if status.Code(e) != codes.InvalidArgument {
			t.Fatal(e)
		}
		_, e = client.ReserveSeats(auth, &pb.ReserveSeatsRequest{FlightId: id, BookingId: uuid.NewString(), SeatCount: 0})
		if status.Code(e) != codes.InvalidArgument {
			t.Fatal(e)
		}
	})
}
